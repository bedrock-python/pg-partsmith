"""Shared helpers for the integration suites.

Everything here is engine-agnostic: SQL text, table DDL, config builders and a
DDL counter that both the aio and sync suites drive through their own engine.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from sqlalchemy import event

from pg_partsmith.boundaries import (
    CursorSource,
    IntegerSequence,
    NumericBoundaries,
    TimeBoundaries,
    UUIDv7BoundaryCodec,
)
from pg_partsmith.entities import PartitionGranularity, TablePartitionConfig
from pg_partsmith.lifecycle import CreateAhead, CreateNextIf, KeepBehind, KeepNewest, LifecyclePolicy, SqlPredicate
from pg_partsmith.partition_bounds import parse_range_boundaries
from pg_partsmith.scheme import HashPartitioning, ListGroup, ListPartitioning, RangePartitioning
from pg_partsmith.utils import orphan_table_comment

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pg_partsmith.boundaries import RangeBoundaryCodec

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

# The ordinary monthly event table: one leaf per period, nothing nested.
MONTHLY_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        created_at TIMESTAMPTZ NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, created_at)
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

# The same shape keyed the way GlitchTip spells it: weekly UUIDv7 windows, each
# divided by organization.
UUID7_ORG_TABLE_DDL = """
    CREATE TABLE {table} (
        id UUID NOT NULL,
        organization_id BIGINT NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, organization_id)
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

# A task queue divided for parallel workers, the shape Hatchet uses.
TASKS_TABLE_DDL = """
    CREATE TABLE {table} (
        task_id BIGINT NOT NULL,
        payload TEXT
    ) PARTITION BY HASH (task_id)
"""

LIST_ROOT_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        region TEXT NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, region)
    ) PARTITION BY LIST (region)
"""

# A LIST root whose groups each run their own monthly lifecycle inside.
TIERED_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tier TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, tier, created_at)
    ) PARTITION BY LIST (tier)
"""

# An integer-keyed queue, partitioned every ``step`` message ids.
QUEUE_TABLE_DDL = """
    CREATE TABLE {table} (
        msg_id BIGSERIAL,
        payload TEXT,
        PRIMARY KEY (msg_id)
    ) PARTITION BY RANGE (msg_id)
"""

# GitLab's sliding list: one integer value per partition, written by the application.
SLIDING_LIST_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        partition_id BIGINT NOT NULL,
        status TEXT,
        PRIMARY KEY (id, partition_id)
    ) PARTITION BY LIST (partition_id)
"""

# An index-free parent: the only kind a foreign table can be a partition of.
METRICS_TABLE_DDL = """
    CREATE TABLE {table} (
        ts TIMESTAMPTZ NOT NULL,
        v DOUBLE PRECISION
    ) PARTITION BY RANGE (ts)
"""


# A root whose partition key spans two columns. Only the leading one carries
# the period; the trailing one is bounded with MINVALUE at both ends.
COMPOSITE_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tenant_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, tenant_id, created_at)
    ) PARTITION BY RANGE (created_at, tenant_id)
"""


# The same composite key with a *nullable* trailing column. PostgreSQL adds an
# IS NOT NULL test for every key column to a range partition's constraint, so a
# row with a NULL tenant belongs in DEFAULT whatever its timestamp says. No
# primary key here: one would have to include tenant_id, which would make it
# NOT NULL and remove the very case under test.
NULLABLE_COMPOSITE_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tenant_id BIGINT,
        created_at TIMESTAMPTZ NOT NULL
    ) PARTITION BY RANGE (created_at, tenant_id)
"""

# A root keyed on an expression rather than a column. Nothing this library does
# can address it: it builds bounds out of column values.
EXPRESSION_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        created_at TIMESTAMPTZ NOT NULL
    ) PARTITION BY RANGE ((created_at AT TIME ZONE 'UTC'))
"""

# A text-keyed root whose bounds look like dates without being them.
SORTABLE_ID_TABLE_DDL = """
    CREATE TABLE {table} (
        id TEXT NOT NULL,
        payload TEXT
    ) PARTITION BY RANGE (id)
"""

# A LIST root whose key accepts NULL, so a partition can be declared for it.
NULLABLE_LIST_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        region TEXT
    ) PARTITION BY LIST (region)
"""


# A root whose DEFAULT partition is attached with a different physical column
# order. ATTACH PARTITION matches by name, so this is legal -- and anything
# copying rows between the two has to name its columns or transpose them.
TRANSPOSED_DEFAULT_TABLE_DDL = """
    CREATE TABLE {table} (
        created_at TIMESTAMPTZ NOT NULL,
        label TEXT,
        note TEXT
    ) PARTITION BY RANGE (created_at)
"""

# A root whose only uniqueness guarantee is a bare index rather than a named
# constraint. PostgreSQL applies the all-key-columns rule to it just the same.
BARE_UNIQUE_INDEX_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tenant_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    ) PARTITION BY RANGE (created_at)
"""


# A root carrying no uniqueness constraint at all. PostgreSQL refuses a PRIMARY
# KEY on a relation whose partition key holds an expression, so this is the only
# shape an expression-keyed branch can be built on.
NO_CONSTRAINT_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL,
        tenant_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    ) PARTITION BY RANGE (created_at)
"""


# ── Config builders ─────────────────────────────────────────────────────────────


def nested_config(
    table_name: str,
    *,
    modulus: int = 2,
    partition_column: str = "created_at",
    create_ahead: int = 1,
    retention: int = 12,
    hash_column: str = "tenant_id",
    inner_modulus: int | None = None,
    codec: RangeBoundaryCodec | None = None,
) -> TablePartitionConfig:
    """Build a weekly RANGE → HASH configuration in the flat spelling."""
    inner = HashPartitioning(key="shard_id", modulus=inner_modulus) if inner_modulus else None
    fields: dict[str, Any] = {}
    if codec is not None:
        fields["boundary_codec"] = codec
    return TablePartitionConfig(
        table_name=table_name,
        partition_column=partition_column,
        granularity=PartitionGranularity.WEEK,
        create_ahead_count=create_ahead,
        retention_count=retention,
        subpartition=HashPartitioning(key=hash_column, modulus=modulus, child=inner),
        **fields,
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
    inner = HashPartitioning(key="tenant_id", modulus=inner_modulus) if inner_modulus else None
    return TablePartitionConfig(
        table_name=table_name,
        partition_column="created_at",
        granularity=PartitionGranularity.WEEK,
        create_ahead_count=create_ahead,
        retention_count=retention,
        subpartition=ListPartitioning(
            key="region",
            groups=tuple(ListGroup(name=name, values=values) for name, values in groups),
            include_default=include_default,
            child=inner,
        ),
    )


def hash_root_config(
    table_name: str,
    *,
    modulus: int = 4,
    inner_modulus: int | None = None,
) -> TablePartitionConfig:
    """Build a static HASH-root configuration."""
    inner = HashPartitioning(key="id", modulus=inner_modulus) if inner_modulus else None
    return TablePartitionConfig(
        table_name=table_name,
        scheme=HashPartitioning(key="tenant_id", modulus=modulus, child=inner),
    )


def tasks_config(table_name: str, *, modulus: int = 8) -> TablePartitionConfig:
    """Build the Hatchet-style root HASH configuration over ``task_id``."""
    return TablePartitionConfig(table_name=table_name, scheme=HashPartitioning(key="task_id", modulus=modulus))


def list_root_config(
    table_name: str,
    *,
    groups: tuple[tuple[str, tuple[str, ...]], ...] = (("eu", ("de", "fr")), ("us", ("us",))),
    include_default: bool = False,
) -> TablePartitionConfig:
    """Build a static LIST-root configuration."""
    return TablePartitionConfig(
        table_name=table_name,
        scheme=ListPartitioning(
            key="region",
            groups=tuple(ListGroup(name=name, values=values) for name, values in groups),
            include_default=include_default,
        ),
    )


def tiered_config(table_name: str, *, create_ahead: int = 2, retention: int = 2) -> TablePartitionConfig:
    """Build a LIST(tier) → RANGE(created_at) configuration: a monthly lifecycle inside each group."""
    return TablePartitionConfig(
        table_name=table_name,
        scheme=ListPartitioning(
            key="tier",
            groups=(ListGroup(name="free", values=("free",)), ListGroup(name="paid", values=("pro", "enterprise"))),
            child=RangePartitioning(
                key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)
            ),
        ),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=create_ahead), retention=KeepNewest(count=retention)),
    )


def queue_config(
    table_name: str,
    *,
    step: int = 1000,
    create_ahead: int = 2,
    distance: int = 2000,
    cursor_source: CursorSource = CursorSource.MAX_KEY,
) -> TablePartitionConfig:
    """Build a numeric RANGE configuration: fixed-width id windows, retention by distance."""
    return TablePartitionConfig(
        table_name=table_name,
        scheme=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=step, cursor_source=cursor_source)),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=create_ahead), retention=KeepBehind(distance=distance)),
    )


def sliding_list_config(
    table_name: str,
    *,
    start: int = 100,
    rotate_at: int = 3,
    keep: int = 3,
) -> TablePartitionConfig:
    """Build a sliding LIST configuration: open the next value once the newest holds ``rotate_at`` rows."""
    return TablePartitionConfig(
        table_name=table_name,
        scheme=ListPartitioning(key="partition_id", sequence=IntegerSequence(start=start)),
        lifecycle=LifecyclePolicy(
            creation=CreateNextIf(when=SqlPredicate(sql=f"SELECT count(*) >= {rotate_at} FROM {{partition}}")),  # noqa: S608
            retention=KeepNewest(count=keep),
        ),
    )


def glitchtip_config(
    table_name: str,
    *,
    modulus: int = 2,
    create_ahead: int = 1,
    retention: int = 12,
) -> TablePartitionConfig:
    """Build the composed weekly UUIDv7 → HASH(organization_id) configuration."""
    return TablePartitionConfig(
        table_name=table_name,
        scheme=RangePartitioning(
            key="id",
            boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=UUIDv7BoundaryCodec()),
            child=HashPartitioning(key="organization_id", modulus=modulus, name_suffix="_h{remainder}"),
        ),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=create_ahead), retention=KeepNewest(count=retention)),
    )


def composite_config(
    table_name: str,
    *,
    create_ahead: int = 1,
    retention: int = 12,
    trailing: tuple[str, ...] = ("tenant_id",),
) -> TablePartitionConfig:
    """Build a weekly configuration over a composite RANGE key."""
    return TablePartitionConfig(
        table_name=table_name,
        partition_column="created_at",
        trailing_partition_columns=trailing,
        granularity=PartitionGranularity.WEEK,
        create_ahead_count=create_ahead,
        retention_count=retention,
    )


def nullable_composite_config(
    table_name: str,
    *,
    create_ahead: int = 1,
    retention: int = 12,
) -> TablePartitionConfig:
    """Build a weekly configuration over a composite key whose tail is nullable."""
    return composite_config(table_name, create_ahead=create_ahead, retention=retention)


def flat_config(table_name: str, *, create_ahead: int = 1, retention: int = 12) -> TablePartitionConfig:
    """Build the classic one-leaf-per-week configuration."""
    return TablePartitionConfig(
        table_name=table_name,
        partition_column="created_at",
        granularity=PartitionGranularity.WEEK,
        create_ahead_count=create_ahead,
        retention_count=retention,
    )


def monthly_config(
    table_name: str,
    *,
    create_ahead: int = 1,
    retention: int = 12,
    lifecycle: LifecyclePolicy | None = None,
    schema: str | None = None,
    column: str = "created_at",
) -> TablePartitionConfig:
    """Build a monthly one-leaf-per-period configuration.

    The flat spelling when only the counts are given; the composed spelling
    when a whole ``lifecycle`` is.
    """
    if lifecycle is None:
        return TablePartitionConfig(
            schema=schema,
            table_name=table_name,
            partition_column=column,
            granularity=PartitionGranularity.MONTH,
            create_ahead_count=create_ahead,
            retention_count=retention,
        )
    return TablePartitionConfig(
        schema=schema,
        table_name=table_name,
        scheme=RangePartitioning(key=column, boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)),
        lifecycle=lifecycle,
    )


def uuid7_codec() -> UUIDv7BoundaryCodec:
    """Return the codec used by the UUIDv7-keyed scenarios."""
    return UUIDv7BoundaryCodec()


def orphan_marker(parent: str) -> str:
    """The first COMMENT line the library writes on a table it detached from ``parent``."""
    return orphan_table_comment(parent)


# ── Catalog assertions, as plain SQL both suites can run ────────────────────────

CHILD_BOUNDS_SQL = """
    SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass(:parent)
    ORDER BY c.relname
"""

RELKIND_SQL = "SELECT relkind FROM pg_class WHERE oid = to_regclass(:name)"

RELISPARTITION_SQL = "SELECT relispartition FROM pg_class WHERE oid = to_regclass(:name)"

RELATION_OID_SQL = "SELECT oid FROM pg_class WHERE oid = to_regclass(:name)"

TABLE_COMMENT_SQL = "SELECT obj_description(to_regclass(:name), 'pg_class')"

CHILD_COUNT_SQL = "SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(:parent)"

ROUTED_LEAF_SQL = "SELECT tableoid::regclass::text FROM {table} WHERE {column} = :value"


def hash_children(rows: list[Any]) -> dict[str, tuple[int, int]]:
    """Map child relname → ``(modulus, remainder)`` from a CHILD_BOUNDS_SQL result."""
    parsed: dict[str, tuple[int, int]] = {}
    for name, bounds in rows:
        match = re.search(r"modulus\s+(\d+),\s*remainder\s+(\d+)", bounds or "", re.IGNORECASE)
        if match:
            parsed[name] = (int(match.group(1)), int(match.group(2)))
    return parsed


def list_children(rows: list[Any]) -> dict[str, tuple[str, ...]]:
    """Map child relname -> its LIST values from a CHILD_BOUNDS_SQL result."""
    parsed: dict[str, tuple[str, ...]] = {}
    for name, bounds in rows:
        match = re.search(r"FOR VALUES IN \((?P<values>.*)\)", bounds or "", re.IGNORECASE | re.DOTALL)
        if match:
            parsed[name] = tuple(v.strip().strip("'") for v in match.group("values").split(","))
    return parsed


def range_children(rows: list[Any]) -> dict[str, tuple[str, str]]:
    """Map child relname -> ``(from_value, to_value)`` from a CHILD_BOUNDS_SQL result."""
    parsed: dict[str, tuple[str, str]] = {}
    for name, bounds in rows:
        from_value, to_value = parse_range_boundaries(bounds)
        if from_value is not None and to_value is not None:
            parsed[name] = (from_value, to_value)
    return parsed


class DdlCounter:
    """Counts DDL statements a block of code causes the engine to execute.

    A converged tree must cost *zero* DDL — the property that keeps maintenance
    from taking heavy locks on a table that needs nothing done to it. Counting
    statements is the only way to show it.
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
