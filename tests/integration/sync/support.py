"""Async helpers shared by the aio integration suites.

Everything that needs an engine lives here; the SQL, the table DDL and the
config builders are engine-agnostic and live in ``tests.integration.nested_support``.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import freezegun
from sqlalchemy import text

from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.maintainer import PartitionMaintainer
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.sync.service import PartitionLifecycleService
from pg_partsmith.utils import quote_identifier
from tests.integration.nested_support import (
    CHILD_BOUNDS_SQL,
    CHILD_COUNT_SQL,
    FROZEN_WEEK,
    RELATION_OID_SQL,
    RELISPARTITION_SQL,
    RELKIND_SQL,
    TABLE_COMMENT_SQL,
    DdlCounter,
    ddl_counter,
    hash_children,
    list_children,
    range_children,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

    from sqlalchemy import Engine

    from pg_partsmith.boundaries import RangeBoundaryCodec
    from pg_partsmith.entities import MaintenanceResult, TablePartitionConfig
    from pg_partsmith.sync.hooks import PartitionLifecycleHooks


def make_table(engine: Engine, ddl: str, *, prefix: str = "nested") -> Generator[str, None]:
    """Create a uniquely named table from ``ddl`` and drop it afterwards."""
    table = f"{prefix}_{uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(text(ddl.format(table=table)))
    yield table
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))


def make_service(
    engine: Engine,
    *,
    hooks: list[PartitionLifecycleHooks] | None = None,
    codec: RangeBoundaryCodec | None = None,
) -> PartitionLifecycleService:
    """Wire a lifecycle service over ``engine`` with the bundled PostgreSQL components."""
    return PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine, boundary_codec=codec),
        locks=PostgresAdvisoryLockManager(engine),
        hooks=hooks,
    )


def make_maintainer(engine: Engine, *, hooks: list[PartitionLifecycleHooks] | None = None) -> PartitionMaintainer:
    """Wire a maintainer over a fresh service."""
    return PartitionMaintainer(make_service(engine, hooks=hooks))


def run_maintenance(
    engine: Engine,
    config: TablePartitionConfig,
    *,
    at_time: str = FROZEN_WEEK,
    hooks: list[PartitionLifecycleHooks] | None = None,
    skip_create: bool = False,
    skip_detach: bool = False,
    skip_drop: bool = False,
    continue_on_error: bool = False,
) -> MaintenanceResult:
    """Run one maintenance tick with the clock frozen at ``at_time``."""
    with freezegun.freeze_time(at_time):
        return make_maintainer(engine, hooks=hooks).run_maintenance(
            config,
            skip_create=skip_create,
            skip_detach=skip_detach,
            skip_drop=skip_drop,
            continue_on_error=continue_on_error,
        )


def exec_sql(engine: Engine, sql: str, **params: Any) -> None:
    """Run one statement in its own transaction."""
    with engine.begin() as conn:
        conn.execute(text(sql), params)


def exec_sql_autocommit(engine: Engine, sql: str, **params: Any) -> None:
    """Run one statement outside any transaction block (CREATE TABLESPACE, DETACH CONCURRENTLY)."""
    with engine.connect() as base_conn:
        conn = base_conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(sql), params)


def scalar(engine: Engine, sql: str, **params: Any) -> Any:
    """Run one query and return its first column of its first row."""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        return result.scalar()


def relkind(engine: Engine, name: str) -> str | None:
    """``pg_class.relkind`` of a relation (bare or schema-qualified), or None when it does not exist."""
    value = scalar(engine, RELKIND_SQL, name=quote_identifier(name))
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def is_attached(engine: Engine, name: str) -> bool:
    """``pg_class.relispartition`` of a relation (False when it does not exist)."""
    return bool(scalar(engine, RELISPARTITION_SQL, name=quote_identifier(name)))


def relation_oid(engine: Engine, name: str) -> int | None:
    """The OID currently holding ``name``, or None."""
    value = scalar(engine, RELATION_OID_SQL, name=quote_identifier(name))
    return None if value is None else int(value)


def table_comment(engine: Engine, name: str) -> str | None:
    """The COMMENT on a relation, or None."""
    value = scalar(engine, TABLE_COMMENT_SQL, name=quote_identifier(name))
    return None if value is None else str(value)


def child_count(engine: Engine, parent: str) -> int:
    """Number of relations directly attached to ``parent``."""
    return int(scalar(engine, CHILD_COUNT_SQL, parent=quote_identifier(parent)))


def _child_bounds(engine: Engine, parent: str) -> list[Any]:
    # Bounds are rendered in the session time zone. Read in UTC whatever the
    # server defaults to, so a bound is one string in every test environment.
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        result = conn.execute(text(CHILD_BOUNDS_SQL), {"parent": quote_identifier(parent)})
        return list(result.fetchall())


def hash_children_of(engine: Engine, parent: str) -> dict[str, tuple[int, int]]:
    """Map child relname -> ``(modulus, remainder)`` for the hash children of ``parent``."""
    return hash_children(_child_bounds(engine, parent))


def list_children_of(engine: Engine, parent: str) -> dict[str, tuple[str, ...]]:
    """Map child relname -> the LIST values it owns."""
    return list_children(_child_bounds(engine, parent))


def range_children_of(engine: Engine, parent: str) -> dict[str, tuple[str, str]]:
    """Map child relname -> ``(from_value, to_value)`` for the RANGE children of ``parent``."""
    return range_children(_child_bounds(engine, parent))


def routed_leaves(engine: Engine, table: str) -> set[str]:
    """The distinct leaves the rows of ``table`` physically live in."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT DISTINCT tableoid::regclass::text FROM {quote_identifier(table)}")  # noqa: S608
        )
        return {str(row[0]) for row in result.fetchall()}


@contextlib.contextmanager
def count_ddl(engine: Engine) -> Iterator[DdlCounter]:
    """Count the DDL statements issued through ``engine`` inside the block."""
    yield from ddl_counter(engine)
