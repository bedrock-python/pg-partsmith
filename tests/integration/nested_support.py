"""Shared helpers for the nested-partitioning integration tests.

Everything here is engine-agnostic: SQL text, config builders, and a DDL
counter that both the aio and sync suites drive through their own engine.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from sqlalchemy import event

from pg_partsmith.boundaries import UUIDv7BoundaryCodec
from pg_partsmith.entities import (
    HashSubpartitionSpec,
    ListGroup,
    ListSubpartitionSpec,
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

# Week 2026-W35 runs Mon 2026-08-24 → Mon 2026-08-31.
FROZEN_WEEK = "2026-08-26"
WEEK_SUFFIX = "__2026_w35"
NEXT_WEEK_SUFFIX = "__2026_w36"
PREVIOUS_WEEK_SUFFIX = "__2026_w34"
WEEK_BOUNDS = ("2026-08-24", "2026-08-31")
PREVIOUS_WEEK_BOUNDS = ("2026-08-17", "2026-08-24")

# Timestamp-keyed root. The primary key carries tenant_id because PostgreSQL
# requires every unique constraint on a partitioned table to include all of its
# partition key columns — including the ones a subpartition adds.
#
# BIGSERIAL here, identity in IDENTITY_TABLE_DDL below: both are supported, and
# the two fixtures keep each column kind covered.
TIMESTAMP_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tenant_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, tenant_id, created_at)
    ) PARTITION BY RANGE (created_at)
"""

# UUIDv7-keyed root: the partition key is a time-sortable identifier, not a
# timestamp, which is the shape a GlitchTip-style event table uses.
UUID7_TABLE_DDL = """
    CREATE TABLE {table} (
        id UUID NOT NULL,
        tenant_id BIGINT NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, tenant_id)
    ) PARTITION BY RANGE (id)
"""

# A root that can carry two levels of hash subpartitioning: every unique
# constraint has to name both hash columns.
TWO_LEVEL_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tenant_id BIGINT NOT NULL,
        shard_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, tenant_id, shard_id, created_at)
    ) PARTITION BY RANGE (created_at)
"""

# A root whose primary key omits tenant_id: PostgreSQL cannot hash-subpartition
# it, and the library should say so before emitting any DDL.
UNCONSTRAINED_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tenant_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, created_at)
    ) PARTITION BY RANGE (created_at)
"""


# A root using GENERATED ALWAYS AS IDENTITY. PostgreSQL refuses to attach a
# partition that carries an identity column of its own, so partitions are built
# with LIKE ... EXCLUDING IDENTITY and inherit the parent's identity on ATTACH.
IDENTITY_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGINT GENERATED ALWAYS AS IDENTITY,
        tenant_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        amount NUMERIC NOT NULL DEFAULT 0,
        doubled NUMERIC GENERATED ALWAYS AS (amount * 2) STORED,
        PRIMARY KEY (id, tenant_id, created_at)
    ) PARTITION BY RANGE (created_at)
"""


# A root that can carry LIST subpartitioning: `region` joins the primary key
# because PostgreSQL needs every partition-key column in every unique
# constraint.
LIST_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        region TEXT NOT NULL,
        tenant_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, region, tenant_id, created_at)
    ) PARTITION BY RANGE (created_at)
"""


# Roots with no time dimension at all: the table itself is divided into a fixed
# set of partitions.
HASH_ROOT_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tenant_id BIGINT NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, tenant_id)
    ) PARTITION BY HASH (tenant_id)
"""

LIST_ROOT_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        region TEXT NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, region)
    ) PARTITION BY LIST (region)
"""


def nested_config(
    table_name: str,
    *,
    modulus: int = 2,
    partition_column: str = "created_at",
    create_ahead: int = 1,
    retention: int = 12,
    hash_column: str = "tenant_id",
    inner_modulus: int | None = None,
) -> TablePartitionConfig:
    """Build a weekly RANGE → HASH configuration."""
    inner = HashSubpartitionSpec(column="shard_id", modulus=inner_modulus) if inner_modulus else None
    return TablePartitionConfig(
        table_name=table_name,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column=partition_column,
        granularity=PartitionGranularity.WEEK,
        create_ahead_count=create_ahead,
        retention_count=retention,
        subpartition=HashSubpartitionSpec(column=hash_column, modulus=modulus, subpartition=inner),
    )


def list_config(
    table_name: str,
    *,
    groups: tuple[tuple[str, tuple[str, ...]], ...] = (("eu", ("de", "fr")), ("us", ("us",))),
    include_default: bool = False,
    create_ahead: int = 1,
    retention: int = 12,
    inner_modulus: int | None = None,
) -> TablePartitionConfig:
    """Build a weekly RANGE -> LIST configuration."""
    inner = HashSubpartitionSpec(column="tenant_id", modulus=inner_modulus) if inner_modulus else None
    return TablePartitionConfig(
        table_name=table_name,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.WEEK,
        create_ahead_count=create_ahead,
        retention_count=retention,
        subpartition=ListSubpartitionSpec(
            column="region",
            groups=tuple(ListGroup(name=name, values=values) for name, values in groups),
            include_default=include_default,
            subpartition=inner,
        ),
    )


def hash_root_config(
    table_name: str,
    *,
    modulus: int = 4,
    inner_modulus: int | None = None,
) -> TablePartitionConfig:
    """Build a static HASH-root configuration."""
    inner = HashSubpartitionSpec(column="id", modulus=inner_modulus) if inner_modulus else None
    return TablePartitionConfig(
        table_name=table_name,
        partition_type=PartitionType.HASH,
        partition_strategy=PartitionStrategy.HASH_BASED,
        partition_column="tenant_id",
        root_layout=HashSubpartitionSpec(column="tenant_id", modulus=modulus, subpartition=inner),
    )


def list_root_config(
    table_name: str,
    *,
    groups: tuple[tuple[str, tuple[str, ...]], ...] = (("eu", ("de", "fr")), ("us", ("us",))),
    include_default: bool = False,
) -> TablePartitionConfig:
    """Build a static LIST-root configuration."""
    return TablePartitionConfig(
        table_name=table_name,
        partition_type=PartitionType.LIST,
        partition_strategy=PartitionStrategy.VALUE_BASED,
        partition_column="region",
        root_layout=ListSubpartitionSpec(
            column="region",
            groups=tuple(ListGroup(name=name, values=values) for name, values in groups),
            include_default=include_default,
        ),
    )


def flat_config(table_name: str, *, create_ahead: int = 1, retention: int = 12) -> TablePartitionConfig:
    """Build the classic one-leaf-per-period configuration."""
    return TablePartitionConfig(
        table_name=table_name,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.WEEK,
        create_ahead_count=create_ahead,
        retention_count=retention,
    )


def uuid7_codec() -> UUIDv7BoundaryCodec:
    """Return the codec used by the UUIDv7-keyed scenarios."""
    return UUIDv7BoundaryCodec()


# ── Catalog assertions, as plain SQL both suites can run ────────────────────────

CHILD_BOUNDS_SQL = """
    SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass(:parent)
    ORDER BY c.relname
"""

RELKIND_SQL = "SELECT relkind FROM pg_class WHERE oid = to_regclass(:name)"

ROUTED_LEAF_SQL = "SELECT tableoid::regclass::text FROM {table} WHERE {column} = :value"


def hash_children(rows: list[Any]) -> dict[str, tuple[int, int]]:
    """Map child relname → ``(modulus, remainder)`` from a CHILD_BOUNDS_SQL result."""
    parsed: dict[str, tuple[int, int]] = {}
    for name, bounds in rows:
        match = re.search(r"modulus\s+(\d+),\s*remainder\s+(\d+)", bounds or "", re.IGNORECASE)
        if match:
            parsed[name] = (int(match.group(1)), int(match.group(2)))
    return parsed


class DdlCounter:
    """Counts DDL statements a block of code causes the engine to execute.

    Scenario B asks for proof that a converged tree costs *zero* DDL — the
    property that keeps maintenance from taking heavy locks on a table that
    needs nothing done to it. Counting statements is the only way to show it.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []

    def __call__(self, _conn: Any, _cursor: Any, statement: str, *_args: Any) -> None:
        """SQLAlchemy ``before_cursor_execute`` hook."""
        normalized = " ".join(statement.split()).upper()
        if normalized.startswith(("CREATE TABLE", "ALTER TABLE", "DROP TABLE", "COMMENT ON TABLE")):
            self.statements.append(normalized)

    @property
    def count(self) -> int:
        """Number of DDL statements recorded."""
        return len(self.statements)


def ddl_counter(sync_engine: Any) -> Iterator[DdlCounter]:
    """Attach a :class:`DdlCounter` to a *sync* engine for the duration of a block."""
    counter = DdlCounter()
    event.listen(sync_engine, "before_cursor_execute", counter)
    try:
        yield counter
    finally:
        event.remove(sync_engine, "before_cursor_execute", counter)


def list_children(rows: list[Any]) -> dict[str, tuple[str, ...]]:
    """Map child relname -> its LIST values from a CHILD_BOUNDS_SQL result."""
    parsed: dict[str, tuple[str, ...]] = {}
    for name, bounds in rows:
        match = re.search(r"FOR VALUES IN \((?P<values>.*)\)", bounds or "", re.IGNORECASE | re.DOTALL)
        if match:
            parsed[name] = tuple(v.strip().strip("'") for v in match.group("values").split(","))
    return parsed
