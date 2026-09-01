"""Batched data movement against a real PostgreSQL (async).

``partition_data`` drains a DEFAULT partition -- the migration path for a
table partitioned around its data -- and ``unpartition`` moves everything back
into one plain table.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.entities import MaintenanceIssueStep, Period
from pg_partsmith.events import PartitionEvent
from pg_partsmith.exceptions import InvalidPartitionConfigError, PlanStaleError
from tests.integration.aio.support import (
    child_count,
    exec_sql,
    hash_children_of,
    is_attached,
    make_service,
    make_table,
    range_children_of,
    relkind,
    scalar,
    table_comment,
)
from tests.integration.nested_support import (
    IDENTITY_TABLE_DDL,
    MONTHLY_TABLE_DDL,
    NULLABLE_COMPOSITE_TABLE_DDL,
    TIMESTAMP_TABLE_DDL,
    monthly_config,
    nested_config,
    nullable_composite_config,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def events(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, MONTHLY_TABLE_DDL, prefix="mig"):
        yield name


@pytest_asyncio.fixture
async def tenants(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, TIMESTAMP_TABLE_DDL, prefix="nmig"):
        yield name


@pytest_asyncio.fixture
async def nullable(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, NULLABLE_COMPOSITE_TABLE_DDL, prefix="cmig"):
        yield name


async def _default_with_rows(engine: AsyncEngine, table: str, *, months: tuple[int, ...], per_month: int) -> str:
    """The migration starting point: the old monolithic table attached as DEFAULT, full of rows."""
    default = f"{table}_legacy"
    await exec_sql(engine, f'CREATE TABLE "{default}" (LIKE "{table}" INCLUDING ALL)')
    for month in months:
        await exec_sql(
            engine,
            f'INSERT INTO "{default}" (created_at, payload) '  # noqa: S608
            f"SELECT make_timestamptz(2026, :month, 1 + (g % 27), 12, 0, 0, 'UTC'), 'row ' || g "
            f"FROM generate_series(1, :rows) g",
            month=month,
            rows=per_month,
        )
    await exec_sql(engine, f'ALTER TABLE "{table}" ATTACH PARTITION "{default}" DEFAULT')
    return default


async def _count(engine: AsyncEngine, table: str) -> int:
    return int(await scalar(engine, f'SELECT count(*) FROM "{table}"'))  # noqa: S608


# ── partition_data ──────────────────────────────────────────────────────────────


async def test__partition_data__drains_default_into_monthly_partitions_in_batches(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange -- 25 rows across three months in the DEFAULT partition
    default = await _default_with_rows(db_engine, events, months=(3, 4, 6), per_month=25)
    config = monthly_config(events, create_ahead=1)

    # Act
    result = await make_service(db_engine).partition_data(config, batch_rows=10)

    # Assert
    assert result.complete
    assert result.rows_moved == 75
    assert result.batches == 9  # 3 windows x (10 + 10 + 5)
    assert result.partitions == (
        f"public.{events}__2026_03",
        f"public.{events}__2026_04",
        f"public.{events}__2026_06",
    )
    assert result.issues == ()
    assert await _count(db_engine, default) == 0
    assert await _count(db_engine, events) == 75
    assert await _count(db_engine, f"{events}__2026_04") == 25
    children = await range_children_of(db_engine, events)
    assert set(children) == {f"{events}__2026_03", f"{events}__2026_04", f"{events}__2026_06"}
    assert await is_attached(db_engine, default)


async def test__partition_data__batch_budget__resumes_where_it_stopped(db_engine: AsyncEngine, events: str) -> None:
    # Arrange
    default = await _default_with_rows(db_engine, events, months=(3,), per_month=25)
    config = monthly_config(events, create_ahead=1)
    service = make_service(db_engine)

    # Act -- two batches: the March partition exists but stays detached and invisible
    first = await service.partition_data(config, batch_rows=10, max_batches=2)
    visible_between = await _count(db_engine, events)
    second = await service.partition_data(config, batch_rows=10)

    # Assert
    assert (first.rows_moved, first.batches, first.complete, first.partitions) == (20, 2, False, ())
    assert await relkind(db_engine, f"{events}__2026_03") == "r"
    assert visible_between == 5  # the 20 moved rows sit in the detached table
    assert (second.rows_moved, second.complete, second.partitions) == (5, True, (f"public.{events}__2026_03",))
    assert await is_attached(db_engine, f"{events}__2026_03")
    assert await _count(db_engine, default) == 0
    assert await _count(db_engine, events) == 25


async def test__partition_data__nested_scheme__rows_land_in_the_buckets(db_engine: AsyncEngine, tenants: str) -> None:
    # Arrange
    default = f"{tenants}_legacy"
    await exec_sql(db_engine, f'CREATE TABLE "{default}" (LIKE "{tenants}" INCLUDING ALL)')
    await exec_sql(
        db_engine,
        f'INSERT INTO "{default}" (tenant_id, created_at, payload) '  # noqa: S608
        f"SELECT g % 5, '2026-08-25 10:00:00+00', 'p' FROM generate_series(1, 20) g",
    )
    await exec_sql(db_engine, f'ALTER TABLE "{tenants}" ATTACH PARTITION "{default}" DEFAULT')
    config = nested_config(tenants, modulus=2)

    # Act
    result = await make_service(db_engine).partition_data(config, batch_rows=7)

    # Assert
    assert result.complete
    assert result.rows_moved == 20
    assert result.partitions == (f"public.{tenants}__2026_w35",)
    buckets = await hash_children_of(db_engine, f"{tenants}__2026_w35")
    assert set(buckets.values()) == {(2, 0), (2, 1)}
    assert await _count(db_engine, default) == 0
    assert await _count(db_engine, f"{tenants}__2026_w35") == 20


async def test__partition_data__no_default_partition__nothing_to_do(db_engine: AsyncEngine, events: str) -> None:
    # Act
    result = await make_service(db_engine).partition_data(monthly_config(events))

    # Assert
    assert result.complete
    assert result.rows_moved == 0


async def test__partition_data__rows_with_a_null_trailing_key__stay_in_default(
    db_engine: AsyncEngine, nullable: str
) -> None:
    # Arrange -- the composite key (created_at, tenant_id); PostgreSQL routes a NULL tenant to DEFAULT
    default = f"{nullable}_legacy"
    await exec_sql(db_engine, f'CREATE TABLE "{default}" (LIKE "{nullable}" INCLUDING ALL)')
    await exec_sql(
        db_engine,
        f'INSERT INTO "{default}" (tenant_id, created_at) '  # noqa: S608
        "VALUES (1, '2026-08-25'), (NULL, '2026-08-25')",
    )
    await exec_sql(db_engine, f'ALTER TABLE "{nullable}" ATTACH PARTITION "{default}" DEFAULT')
    config = nullable_composite_config(nullable)

    # Act
    result = await make_service(db_engine).partition_data(config)

    # Assert
    assert result.complete
    assert result.rows_moved == 1
    assert await _count(db_engine, default) == 1


async def test__partition_data__window_held_by_an_unmanaged_partition__stops_with_an_issue(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange -- a hand-made partition straddles March and April, so neither month can be created;
    # a March row it does not cover sits in DEFAULT
    await exec_sql(
        db_engine,
        f"CREATE TABLE \"{events}_odd\" PARTITION OF \"{events}\" FOR VALUES FROM ('2026-03-15') TO ('2026-04-15')",
    )
    default = f"{events}_legacy"
    await exec_sql(db_engine, f'CREATE TABLE "{default}" (LIKE "{events}" INCLUDING ALL)')
    await exec_sql(db_engine, f"INSERT INTO \"{default}\" (created_at, payload) VALUES ('2026-03-03', 'x')")  # noqa: S608
    await exec_sql(db_engine, f'ALTER TABLE "{events}" ATTACH PARTITION "{default}" DEFAULT')

    # Act
    result = await make_service(db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert
    assert not result.complete
    assert result.rows_moved == 0
    assert [issue.step for issue in result.issues][-1] is MaintenanceIssueStep.MOVE
    assert "no partition can be created" in result.issues[-1].error
    assert await _count(db_engine, default) == 1


async def test__partition_data__non_range_root__refused(db_engine: AsyncEngine, events: str) -> None:
    from tests.integration.nested_support import HASH_ROOT_TABLE_DDL, hash_root_config  # noqa: PLC0415

    async for tasks in make_table(db_engine, HASH_ROOT_TABLE_DDL, prefix="hmig"):
        with pytest.raises(InvalidPartitionConfigError, match="not a RANGE level"):
            await make_service(db_engine).partition_data(hash_root_config(tasks))


# ── unpartition ─────────────────────────────────────────────────────────────────


async def test__unpartition__moves_everything_into_one_table_and_drops_the_partitions(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange -- three partitions with rows, made by the mover itself
    await _default_with_rows(db_engine, events, months=(3, 4, 6), per_month=10)
    config = monthly_config(events, create_ahead=1)
    service = make_service(db_engine)
    await service.partition_data(config)
    flat = f"{events}_flat"

    # Act
    result = await service.unpartition(config, flat, batch_rows=4, drop_emptied=True)

    # Assert
    assert result.complete
    assert result.rows_moved == 30
    assert result.partitions == (
        f"public.{events}__2026_03",
        f"public.{events}__2026_04",
        f"public.{events}__2026_06",
        f"public.{events}_legacy",
    )
    assert await _count(db_engine, flat) == 30
    assert await _count(db_engine, events) == 0
    assert await range_children_of(db_engine, events) == {}
    assert await relkind(db_engine, f"{events}__2026_03") is None
    assert await PostgresMetadataProvider(db_engine).list_partitions(events) == []


async def test__unpartition__without_dropping__partitions_stay_attached_and_empty(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange
    await _default_with_rows(db_engine, events, months=(5,), per_month=6)
    config = monthly_config(events, create_ahead=1)
    service = make_service(db_engine)
    await service.partition_data(config)
    flat = f"{events}_flat"
    await exec_sql(db_engine, f'CREATE TABLE "{flat}" (LIKE "{events}" INCLUDING ALL)')

    # Act
    result = await service.unpartition(config, flat)

    # Assert
    assert result.complete
    assert result.rows_moved == 6
    assert await is_attached(db_engine, f"{events}__2026_05")
    assert await _count(db_engine, f"{events}__2026_05") == 0
    assert await _count(db_engine, flat) == 6


async def test__unpartition__batch_budget__stops_and_resumes(db_engine: AsyncEngine, events: str) -> None:
    # Arrange
    await _default_with_rows(db_engine, events, months=(5,), per_month=9)
    config = monthly_config(events, create_ahead=1)
    service = make_service(db_engine)
    await service.partition_data(config)
    flat = f"{events}_flat"

    # Act
    first = await service.unpartition(config, flat, batch_rows=4, max_batches=1)
    second = await service.unpartition(config, flat, batch_rows=4)

    # Assert
    assert (first.rows_moved, first.complete) == (4, False)
    assert (second.rows_moved, second.complete) == (5, True)
    assert await _count(db_engine, flat) == 9


# ── review follow-ups: FK actions, late rows, generated columns, destinations ───


@pytest_asyncio.fixture
async def ledger(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, IDENTITY_TABLE_DDL, prefix="gmig"):
        yield name


async def test__partition_data__cascading_foreign_key__refused_with_nothing_deleted(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange: rows in DEFAULT are referenced ON DELETE CASCADE
    await _default_with_rows(db_engine, events, months=(3,), per_month=5)
    ref = f"{events}_ref"
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{ref}" (event_id BIGINT, created_at TIMESTAMPTZ, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at) ON DELETE CASCADE)',
    )
    await exec_sql(db_engine, f'INSERT INTO "{ref}" SELECT id, created_at FROM "{events}" LIMIT 2')  # noqa: S608

    # Act
    result = await make_service(db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert: fail closed -- no parent row moved, no referencing row cascaded away
    assert not result.complete
    assert any("ON DELETE CASCADE" in issue.error for issue in result.issues)
    assert await _count(db_engine, events) == 5
    assert await _count(db_engine, ref) == 2


class _LateWriter(BasePartitionLifecycleHooks):
    """Commits one more row through the root just before each partition detaches."""

    def __init__(self, engine: AsyncEngine, table: str, when: datetime) -> None:
        self._engine = engine
        self._table = table
        self._when = when

    async def before_detach(self, event: PartitionEvent) -> None:
        await exec_sql(
            self._engine,
            f"INSERT INTO \"{self._table}\" (created_at, payload) VALUES (:ts, 'late')",  # noqa: S608
            ts=self._when,
        )


async def test__unpartition__row_committed_between_the_last_batch_and_the_drop__is_moved_not_dropped(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange: nine March rows, and a writer that lands one more at every detach
    await _default_with_rows(db_engine, events, months=(3,), per_month=9)
    config = monthly_config(events, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    flat = f"{events}_flat"
    late = _LateWriter(db_engine, events, datetime(2026, 3, 15, 12, tzinfo=UTC))

    # Act
    result = await make_service(db_engine, hooks=[late]).unpartition(config, flat, batch_rows=4, drop_emptied=True)

    # Assert: both late rows (March partition, then DEFAULT) end in the flat table
    assert result.complete
    assert result.rows_moved == 11
    assert await _count(db_engine, flat) == 11
    assert await relkind(db_engine, f"{events}__2026_03") is None
    assert await _count(db_engine, events) == 0


async def test__movers__generated_and_identity_columns__recomputed_not_copied(
    db_engine: AsyncEngine, ledger: str
) -> None:
    # Arrange: the legacy table carries GENERATED ALWAYS columns (identity and stored)
    legacy = f"{ledger}_legacy"
    await exec_sql(db_engine, f'CREATE TABLE "{legacy}" (LIKE "{ledger}" INCLUDING ALL EXCLUDING IDENTITY)')
    await exec_sql(
        db_engine,
        f'INSERT INTO "{legacy}" (id, tenant_id, created_at, amount) '  # noqa: S608
        f"SELECT g, 1, make_timestamptz(2026, 4, 1 + (g % 27), 12, 0, 0, 'UTC'), g FROM generate_series(1, 6) g",
    )
    await exec_sql(db_engine, f'ALTER TABLE "{ledger}" ATTACH PARTITION "{legacy}" DEFAULT')
    config = monthly_config(ledger, create_ahead=1)

    # Act
    forward = await make_service(db_engine).partition_data(config)
    flat = f"{ledger}_flat"
    back = await make_service(db_engine).unpartition(config, flat)

    # Assert: rows moved twice, ids kept, the stored column recomputed each time
    assert forward.complete and back.complete
    assert forward.rows_moved == 6 and back.rows_moved == 6
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{flat}" WHERE doubled = amount * 2')) == 6  # noqa: S608
    assert int(await scalar(db_engine, f'SELECT sum(id) FROM "{flat}"')) == 21  # noqa: S608


async def test__unpartition__into_the_root_itself__refused_before_any_row_moves(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange
    await _default_with_rows(db_engine, events, months=(5,), per_month=3)
    config = monthly_config(events, create_ahead=1)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="itself"):
        await make_service(db_engine).unpartition(config, f"public.{events}")
    assert await _count(db_engine, events) == 3


async def test__partition_data__window_of_a_detached_owned_partition__filled_and_reattached(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange: March exists, was detached by the library, and new March rows sit in DEFAULT
    config = monthly_config(events, create_ahead=1)
    service = make_service(db_engine)
    await service.ensure_partitions(config, [Period(year=2026, month=3)])
    march = f"{events}__2026_03"
    listed = await PostgresMetadataProvider(db_engine).list_partitions(events)
    await service.detach_old_partitions(events, [p for p in listed if p.relname == march])
    assert await is_attached(db_engine, march) is False
    await _default_with_rows(db_engine, events, months=(3,), per_month=4)

    # Act
    result = await service.partition_data(config)

    # Assert: the orphan is filled and re-attached, not recreated or given up on
    assert result.complete
    assert result.partitions == (f"public.{march}",)
    assert await is_attached(db_engine, march)
    assert await _count(db_engine, march) == 4
    assert await table_comment(db_engine, march) is None


async def test__partition_data__no_action_foreign_key_with_referenced_rows__refused_row_safe(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange: rows in DEFAULT are referenced through an ordinary (NO ACTION) key
    await _default_with_rows(db_engine, events, months=(3,), per_month=4)
    ref = f"{events}_noact"
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{ref}" (event_id BIGINT, created_at TIMESTAMPTZ, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at))',
    )
    await exec_sql(db_engine, f'INSERT INTO "{ref}" SELECT id, created_at FROM "{events}" LIMIT 1')  # noqa: S608

    # Act
    result = await make_service(db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert: a referenced row cannot leave the tree during a move; the batch fails whole
    assert not result.complete
    assert any("still referenced" in issue.error for issue in result.issues)
    assert await _count(db_engine, events) == 4
    assert await _count(db_engine, ref) == 1


class _DetachedWriter(BasePartitionLifecycleHooks):
    """Commits one more row straight into the detached table right before its drop."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def before_drop(self, event: PartitionEvent) -> None:
        quoted = ".".join(f'"{part}"' for part in event.partition.name.split("."))
        await exec_sql(
            self._engine,
            f"INSERT INTO {quoted} (created_at, payload) VALUES ('2026-03-20T12:00:00+00:00', 'dropwrite')",  # noqa: S608
        )


async def test__unpartition__row_committed_inside_before_drop__moved_in_the_drop_transaction(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange
    await _default_with_rows(db_engine, events, months=(3,), per_month=5)
    config = monthly_config(events, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    flat = f"{events}_flat"

    # Act
    result = await make_service(db_engine, hooks=[_DetachedWriter(db_engine)]).unpartition(
        config, flat, batch_rows=4, drop_emptied=True
    )

    # Assert: both drop-time rows are moved inside the drop's transaction and counted
    assert result.complete
    assert result.rows_moved == 7
    assert await _count(db_engine, flat) == 7
    assert await _count(db_engine, events) == 0


async def test__unpartition__into_an_identity_table__the_next_ordinary_insert_succeeds(
    db_engine: AsyncEngine, ledger: str
) -> None:
    # Arrange: five rows with explicit ids come through the tree
    legacy = f"{ledger}_legacy"
    await exec_sql(db_engine, f'CREATE TABLE "{legacy}" (LIKE "{ledger}" INCLUDING ALL EXCLUDING IDENTITY)')
    await exec_sql(
        db_engine,
        f'INSERT INTO "{legacy}" (id, tenant_id, created_at, amount) '  # noqa: S608
        f"SELECT g, 1, make_timestamptz(2026, 4, 1 + (g % 27), 12, 0, 0, 'UTC'), g FROM generate_series(1, 5) g",
    )
    await exec_sql(db_engine, f'ALTER TABLE "{ledger}" ATTACH PARTITION "{legacy}" DEFAULT')
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = f"{ledger}_dest"
    await exec_sql(db_engine, f'CREATE TABLE "{dest}" (LIKE "{ledger}" INCLUDING ALL)')

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")
    await exec_sql(
        db_engine,
        f"INSERT INTO \"{dest}\" (tenant_id, created_at, amount) VALUES (1, '2026-05-01T00:00:00+00:00', 9)",  # noqa: S608
    )

    # Assert: the identity sequence was advanced past the moved ids
    assert result.complete
    assert result.rows_moved == 5
    assert int(await scalar(db_engine, f'SELECT max(id) FROM "{dest}"')) == 6  # noqa: S608
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{dest}"')) == 6  # noqa: S608


async def test__unpartition__into_a_leaf_of_another_tree__refused(db_engine: AsyncEngine, events: str) -> None:
    # Arrange: a partition of an unrelated root -- relkind 'r', but routed property of another tree
    other = f"{events}_other"
    await exec_sql(db_engine, f'CREATE TABLE "{other}" (ts TIMESTAMPTZ NOT NULL) PARTITION BY RANGE (ts)')
    await exec_sql(
        db_engine,
        f"CREATE TABLE \"{other}_p1\" PARTITION OF \"{other}\" FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    try:
        # Act / Assert
        with pytest.raises(InvalidPartitionConfigError, match="attached as a partition"):
            await make_service(db_engine).unpartition(monthly_config(events, create_ahead=1), f"public.{other}_p1")
    finally:
        await exec_sql(db_engine, f'DROP TABLE "{other}"')


# sync-mirror: skip
async def test__partition_data__orphan_swapped_during_the_fill__stale_not_misfilled(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange: a matching detached orphan for March, and rows for it in DEFAULT
    config = monthly_config(events, create_ahead=1)
    service = make_service(db_engine)
    await service.ensure_partitions(config, [Period(year=2026, month=3)])
    march = f"{events}__2026_03"
    listed = await PostgresMetadataProvider(db_engine).list_partitions(events)
    await service.detach_old_partitions(events, [p for p in listed if p.relname == march])
    await _default_with_rows(db_engine, events, months=(3,), per_month=4)

    original = PostgresPartitionRepository.reconcile_default_rows

    async def swapping(self: PostgresPartitionRepository, **kwargs: object) -> int:
        target = str(kwargs["target_partition_name"]).split(".")[-1]
        async with db_engine.begin() as conn:
            await conn.execute(text(f'DROP TABLE "{target}"'))
            await conn.execute(text(f'CREATE TABLE "{target}" (LIKE "{events}" INCLUDING ALL)'))
        return await original(self, **kwargs)  # type: ignore[arg-type]

    # Act / Assert: the batch verifies the target under its lock and fails closed
    with (
        patch.object(PostgresPartitionRepository, "reconcile_default_rows", swapping),
        pytest.raises(PlanStaleError),
    ):
        await service.partition_data(config)
    assert await _count(db_engine, events) == 4  # every row still in DEFAULT, visible through the root


async def test__partition_data__deferred_foreign_key_with_referenced_rows__refused_row_safe(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange: the check would otherwise wait for COMMIT, outside any statement handler
    await _default_with_rows(db_engine, events, months=(3,), per_month=4)
    ref = f"{events}_defer"
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{ref}" (event_id BIGINT, created_at TIMESTAMPTZ, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at) '
        f"DEFERRABLE INITIALLY DEFERRED)",
    )
    await exec_sql(db_engine, f'INSERT INTO "{ref}" SELECT id, created_at FROM "{events}" LIMIT 1')  # noqa: S608

    # Act
    result = await make_service(db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert: SET CONSTRAINTS ALL IMMEDIATE pulls the refusal into the statement, translated
    assert not result.complete
    assert any("still referenced" in issue.error for issue in result.issues)
    assert await _count(db_engine, events) == 4
    assert await _count(db_engine, ref) == 1


async def test__unpartition__into_a_descending_identity_table__the_next_ordinary_insert_succeeds(
    db_engine: AsyncEngine, ledger: str
) -> None:
    # Arrange: ids 0, -1, -2 come through the tree; the destination counts downwards
    legacy = f"{ledger}_legacy"
    await exec_sql(db_engine, f'CREATE TABLE "{legacy}" (LIKE "{ledger}" INCLUDING ALL EXCLUDING IDENTITY)')
    await exec_sql(
        db_engine,
        f'INSERT INTO "{legacy}" (id, tenant_id, created_at, amount) '  # noqa: S608
        f"SELECT 1 - g, 1, make_timestamptz(2026, 4, g, 12, 0, 0, 'UTC'), g FROM generate_series(1, 3) g",
    )
    await exec_sql(db_engine, f'ALTER TABLE "{ledger}" ATTACH PARTITION "{legacy}" DEFAULT')
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = f"{ledger}_ndest"
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{dest}" ('
        "id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY -1 MAXVALUE 0), "
        "tenant_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, "
        "amount NUMERIC NOT NULL DEFAULT 0, doubled NUMERIC GENERATED ALWAYS AS (amount * 2) STORED)",
    )

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")
    await exec_sql(
        db_engine,
        f"INSERT INTO \"{dest}\" (tenant_id, created_at, amount) VALUES (1, '2026-05-01T00:00:00+00:00', 9)",  # noqa: S608
    )

    # Assert: the sequence chased the LOW water mark, so the next id is -3
    assert result.complete
    assert result.rows_moved == 3
    assert int(await scalar(db_engine, f'SELECT min(id) FROM "{dest}"')) == -3  # noqa: S608
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{dest}"')) == 4  # noqa: S608


# sync-mirror: skip
async def test__partition_data__created_target_swapped_during_the_fill__stale_not_misfilled(
    db_engine: AsyncEngine, events: str
) -> None:
    # Arrange: the very relation partition_data just created is renamed away mid-fill
    await _default_with_rows(db_engine, events, months=(3,), per_month=4)
    config = monthly_config(events, create_ahead=1)
    service = make_service(db_engine)
    original = PostgresPartitionRepository.reconcile_default_rows
    swapped: list[str] = []

    async def swapping(self: PostgresPartitionRepository, **kwargs: object) -> int:
        target = str(kwargs["target_partition_name"]).split(".")[-1]
        if not swapped:
            swapped.append(target)
            async with db_engine.begin() as conn:
                await conn.execute(text(f'ALTER TABLE "{target}" RENAME TO "{target}_hijacked"'))
                await conn.execute(text(f'CREATE TABLE "{target}" (LIKE "{events}" INCLUDING ALL)'))
        return await original(self, **kwargs)  # type: ignore[arg-type]

    # Act / Assert: the fill batch verifies the OID captured at creation and fails closed
    with (
        patch.object(PostgresPartitionRepository, "reconcile_default_rows", swapping),
        pytest.raises(PlanStaleError),
    ):
        await service.partition_data(config)
    assert await _count(db_engine, events) == 4
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{swapped[0]}"')) == 0  # noqa: S608


# sync-mirror: skip
async def test__partition_data__branch_replaced_before_its_children__children_never_go_live_in_it(
    db_engine: AsyncEngine, tenants: str
) -> None:
    """A nested branch swapped during construction must not gain the subtree."""
    # Arrange: a RANGE -> HASH tree whose branch is renamed right after it is created
    default = f"{tenants}_legacy"
    await exec_sql(db_engine, f'CREATE TABLE "{default}" (LIKE "{tenants}" INCLUDING ALL)')
    await exec_sql(
        db_engine,
        f'INSERT INTO "{default}" (tenant_id, created_at, payload) '  # noqa: S608
        f"SELECT g % 5, '2026-08-25 10:00:00+00', 'p' FROM generate_series(1, 6) g",
    )
    await exec_sql(db_engine, f'ALTER TABLE "{tenants}" ATTACH PARTITION "{default}" DEFAULT')
    config = nested_config(tenants, modulus=2)
    original = PostgresPartitionRepository.create_table_like
    hijacked: list[str] = []

    async def swapping(
        self: PostgresPartitionRepository, template: str, name: str, partition_by: object, **kwargs: object
    ) -> int:
        oid = await original(self, template, name, partition_by, **kwargs)  # type: ignore[arg-type]
        relname = name.rsplit(".", maxsplit=1)[-1]
        if partition_by is not None and not hijacked:
            hijacked.append(relname)
            async with db_engine.begin() as conn:
                await conn.execute(text(f'ALTER TABLE "{relname}" RENAME TO "{relname}_hijacked"'))
                await conn.execute(
                    text(f'CREATE TABLE "{relname}" (LIKE "{tenants}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
                )
        return oid

    # Act / Assert: the child attach checks the parent it was planned for
    with (
        patch.object(PostgresPartitionRepository, "create_table_like", swapping),
        pytest.raises(PlanStaleError),
    ):
        await make_service(db_engine).partition_data(config)
    replacement = hijacked[0]
    assert await child_count(db_engine, replacement) == 0
    await exec_sql(db_engine, f'DROP TABLE "{replacement}"')


# sync-mirror: skip
async def test__ensure_partitions__default_replaced_before_a_failing_attach__rows_stay_where_they_are(
    db_engine: AsyncEngine, events: str
) -> None:
    """A compensating restore never hands rows to a relation that took the DEFAULT's name."""
    # Arrange: March's rows sit in DEFAULT, so the attach reconciles them out first
    await _default_with_rows(db_engine, events, months=(3,), per_month=4)
    default = f"{events}_legacy"
    config = monthly_config(events, create_ahead=1)
    original = PostgresPartitionRepository.attach_partition
    attempts: list[str] = []

    async def failing(
        self: PostgresPartitionRepository, parent: str, name: str, bounds: object, **kwargs: object
    ) -> None:
        attempts.append(name)
        if len(attempts) == 1:
            # The real attach, which PostgreSQL refuses while DEFAULT holds the rows.
            await original(self, parent, name, bounds, **kwargs)  # type: ignore[arg-type]
            return
        # The DEFAULT changes hands while its rows are out of it.
        async with db_engine.begin() as conn:
            await conn.execute(text(f'ALTER TABLE "{events}" DETACH PARTITION "{default}"'))
            await conn.execute(text(f'ALTER TABLE "{default}" RENAME TO "{default}_hijacked"'))
            await conn.execute(text(f'CREATE TABLE "{default}" (LIKE "{events}" INCLUDING ALL)'))
        msg = "attach refused after the reconcile"
        raise SQLAlchemyError(msg)

    # Act / Assert
    with patch.object(PostgresPartitionRepository, "attach_partition", failing), pytest.raises(SQLAlchemyError):
        await make_service(db_engine).ensure_partitions(config, [Period(year=2026, month=3)])

    # The rows are in the partition whose identity was verified, not in the stranger
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{events}__2026_03"')) == 4  # noqa: S608
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{default}"')) == 0  # noqa: S608
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{default}_hijacked"')) == 0  # noqa: S608


async def test__unpartition__into_a_cycling_identity_table__refused_with_nothing_moved(
    db_engine: AsyncEngine, ledger: str
) -> None:
    # Arrange: the destination's identity cycles, so it would reissue the moved ids
    await _identity_rows(db_engine, ledger, ids=(1, 2, 3))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(db_engine, ledger, "cyc", "MINVALUE 1 MAXVALUE 5 CYCLE")

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert: refused whole, nothing half-moved
    assert not result.complete
    assert any("cycles" in issue.error for issue in result.issues)
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{dest}"')) == 0  # noqa: S608
    assert await _count(db_engine, ledger) == 3


async def test__unpartition__identity_range_too_narrow__refused_before_the_destination_breaks(
    db_engine: AsyncEngine, ledger: str
) -> None:
    # Arrange: three ids into a sequence that has exactly three values to give
    await _identity_rows(db_engine, ledger, ids=(1, 2, 3))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(db_engine, ledger, "narrow", "MINVALUE 1 MAXVALUE 3")

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert
    assert not result.complete
    assert any("nothing left to issue" in issue.error for issue in result.issues)
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{dest}"')) == 0  # noqa: S608


async def test__unpartition__ids_off_the_sequences_path__moved_and_the_sequence_left_alone(
    db_engine: AsyncEngine, ledger: str
) -> None:
    # Arrange: id 6 is inside the range but not a value the sequence lands on
    await _identity_rows(db_engine, ledger, ids=(6,))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(db_engine, ledger, "offpath", "START WITH 1 INCREMENT BY 3 MAXVALUE 7")

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")
    await exec_sql(
        db_engine,
        f"INSERT INTO \"{dest}\" (tenant_id, created_at, amount) VALUES (1, '2026-05-01T00:00:00+00:00', 9)",  # noqa: S608
    )

    # Assert: nothing to synchronise, and the sequence still starts where it meant to
    assert result.complete
    assert result.rows_moved == 1
    assert int(await scalar(db_engine, f'SELECT min(id) FROM "{dest}"')) == 1  # noqa: S608


async def _identity_rows(engine: AsyncEngine, table: str, *, ids: tuple[int, ...]) -> None:
    """Attach the legacy table as DEFAULT with rows carrying explicit ids."""
    legacy = f"{table}_legacy"
    await exec_sql(engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL EXCLUDING IDENTITY)')
    for index, value in enumerate(ids, start=1):
        await exec_sql(
            engine,
            f'INSERT INTO "{legacy}" (id, tenant_id, created_at, amount) '  # noqa: S608
            f"VALUES (:id, 1, make_timestamptz(2026, 4, :day, 12, 0, 0, 'UTC'), :amount)",
            id=value,
            day=index,
            amount=index,
        )
    await exec_sql(engine, f'ALTER TABLE "{table}" ATTACH PARTITION "{legacy}" DEFAULT')


async def _identity_destination(engine: AsyncEngine, table: str, suffix: str, identity: str) -> str:
    """A plain destination shaped like the ledger, with a deliberately awkward identity."""
    name = f"{table}_{suffix}"
    await exec_sql(
        engine,
        f'CREATE TABLE "{name}" ('
        f"id BIGINT GENERATED ALWAYS AS IDENTITY ({identity}), "
        "tenant_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, "
        "amount NUMERIC NOT NULL DEFAULT 0, doubled NUMERIC GENERATED ALWAYS AS (amount * 2) STORED)",
    )
    return name


async def test__unpartition__into_a_cached_identity_table__refused_while_a_session_may_hold_ids(
    db_engine: AsyncEngine, ledger: str
) -> None:
    # Arrange: the destination's identity draws blocks of five, and one block is out
    await _identity_rows(db_engine, ledger, ids=(2,))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(db_engine, ledger, "cached", "CACHE 5")
    await scalar(db_engine, f"SELECT nextval(pg_get_serial_sequence('public.{dest}', 'id'))")

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert: id 2 is in the drawn block, where no setval reaches it
    assert not result.complete
    assert any("caches 5 values" in issue.error for issue in result.issues)
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{dest}"')) == 0  # noqa: S608
    assert await _count(db_engine, ledger) == 1


# sync-mirror: skip
async def test__ensure_partitions__reattached_branch_replaced_before_its_children__children_never_go_live_in_it(
    db_engine: AsyncEngine, tenants: str
) -> None:
    """A detached branch coming back must not hand its subtree to a replacement."""
    # Arrange: a managed orphan branch with one bucket missing
    config = nested_config(tenants, modulus=2)
    service = make_service(db_engine)
    when = datetime(2026, 8, 25, 10, tzinfo=UTC)
    await service.ensure_partitions(config, [when])
    branch = f"{tenants}__2026_w35"
    listed = await PostgresMetadataProvider(db_engine).list_partitions(tenants)
    await service.detach_old_partitions(tenants, [p for p in listed if p.relname == branch])
    await exec_sql(db_engine, f'DROP TABLE "{branch}__h1"')
    original = PostgresPartitionRepository.create_table_like
    hijacked: list[str] = []

    async def swapping(
        self: PostgresPartitionRepository, template: str, name: str, partition_by: object, **kwargs: object
    ) -> int:
        if not hijacked:
            hijacked.append(branch)
            async with db_engine.begin() as conn:
                await conn.execute(text(f'ALTER TABLE "{branch}" RENAME TO "{branch}_hijacked"'))
                await conn.execute(
                    text(f'CREATE TABLE "{branch}" (LIKE "{tenants}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
                )
        return await original(self, template, name, partition_by, **kwargs)  # type: ignore[arg-type]

    # Act / Assert: the bucket is attached by the branch's identity, not by its name
    with (
        patch.object(PostgresPartitionRepository, "create_table_like", swapping),
        pytest.raises(PlanStaleError),
    ):
        await service.ensure_partitions(config, [when])
    assert await child_count(db_engine, branch) == 0
    assert await child_count(db_engine, f"{branch}_hijacked") == 1
    await exec_sql(db_engine, f'DROP TABLE "{branch}"')


async def test__unpartition__cached_identity_block_held_by_an_older_session__still_refused(
    db_engine: AsyncEngine, ledger: str
) -> None:
    """The newest allocation is not the only one a session may still be holding."""
    # Arrange: session A draws 1..5 and keeps it; session B draws 6..10, moving the catalog to 10
    await _identity_rows(db_engine, ledger, ids=(2,))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(db_engine, ledger, "twoback", "CACHE 5")
    sequence_of = f"pg_get_serial_sequence('public.{dest}', 'id')"
    async with db_engine.connect() as session_a, db_engine.connect() as session_b:
        first = (await session_a.execute(text(f"SELECT nextval({sequence_of})"))).scalar()
        second = (await session_b.execute(text(f"SELECT nextval({sequence_of})"))).scalar()
        assert (first, second) == (1, 6)
        assert int(await scalar(db_engine, f"SELECT pg_sequence_last_value({sequence_of})")) == 10

        # Act: id 2 is below the newest block, still live in session A
        result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert
    assert not result.complete
    assert any("already handed out" in issue.error for issue in result.issues)
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{dest}"')) == 0  # noqa: S608
    assert await _count(db_engine, ledger) == 1


async def test__unpartition__cycling_cached_identity_that_wrapped__refused(db_engine: AsyncEngine, ledger: str) -> None:
    """A wraparound moves the catalog behind a block a session is still holding."""
    # Arrange: INCREMENT 3 over 1..10 with CYCLE; A keeps 5, B wraps the catalog to 4
    await _identity_rows(db_engine, ledger, ids=(5,))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(
        db_engine, ledger, "wrapped", "START WITH 2 INCREMENT BY 3 MINVALUE 1 MAXVALUE 10 CYCLE CACHE 2"
    )
    sequence_of = f"pg_get_serial_sequence('public.{dest}', 'id')"
    async with db_engine.connect() as session_a, db_engine.connect() as session_b:
        assert (await session_a.execute(text(f"SELECT nextval({sequence_of})"))).scalar() == 2
        for _ in range(2):
            await session_b.execute(text(f"SELECT nextval({sequence_of})"))
        assert int(await scalar(db_engine, f"SELECT pg_sequence_last_value({sequence_of})")) < 5

        # Act: 5 is behind the catalog now, and still session A's to issue
        result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert
    assert not result.complete
    assert any("cycles" in issue.error for issue in result.issues)
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{dest}"')) == 0  # noqa: S608
    assert await _count(db_engine, ledger) == 1


async def test__unpartition__cached_identity__ids_before_its_start_move(db_engine: AsyncEngine, ledger: str) -> None:
    # Arrange: the destination's identity begins at 100; nobody was ever handed 50
    await _identity_rows(db_engine, ledger, ids=(50,))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(db_engine, ledger, "afterstart", "START WITH 100 MINVALUE 1 CACHE 5")
    sequence_of = f"pg_get_serial_sequence('public.{dest}', 'id')"
    async with db_engine.connect() as holder:
        await holder.execute(text(f"SELECT nextval({sequence_of})"))

        # Act
        result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert: a value the sequence never issued is nobody's to reissue
    assert result.complete
    assert result.rows_moved == 1
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{dest}"')) == 1  # noqa: S608


async def test__unpartition__cycling_identity__a_row_it_issued_itself_does_not_refuse_the_move(
    db_engine: AsyncEngine, ledger: str
) -> None:
    """A pre-existing destination row is the destination's own business."""
    # Arrange: the destination cycles over 1..10 and already holds an id it
    # issued itself; the only row moving carries 50, which it can never reach
    await _identity_rows(db_engine, ledger, ids=(50,))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(
        db_engine, ledger, "ownrow", "START WITH 2 INCREMENT BY 3 MINVALUE 1 MAXVALUE 10 CYCLE CACHE 2"
    )
    await exec_sql(
        db_engine,
        f'INSERT INTO "{dest}" (tenant_id, created_at, amount) '  # noqa: S608
        "VALUES (1, '2026-05-01T00:00:00+00:00', 9)",
    )

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert
    assert result.complete
    assert result.rows_moved == 1
    assert await scalar(db_engine, f"SELECT string_agg(id::text, ',' ORDER BY id) FROM \"{dest}\"") == "2,50"  # noqa: S608
    assert await _count(db_engine, ledger) == 0


async def test__unpartition__cached_identity__a_block_it_issued_itself_does_not_refuse_the_move(
    db_engine: AsyncEngine, ledger: str
) -> None:
    # Arrange: an ordinary insert draws the block 100..104; the moved row's 50
    # was never the sequence's to give
    await _identity_rows(db_engine, ledger, ids=(50,))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _identity_destination(db_engine, ledger, "ownblock", "START WITH 100 MINVALUE 1 CACHE 5")
    await exec_sql(
        db_engine,
        f'INSERT INTO "{dest}" (tenant_id, created_at, amount) '  # noqa: S608
        "VALUES (1, '2026-05-01T00:00:00+00:00', 9)",
    )

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert
    assert result.complete
    assert result.rows_moved == 1
    assert await scalar(db_engine, f"SELECT string_agg(id::text, ',' ORDER BY id) FROM \"{dest}\"") == "50,100"  # noqa: S608


async def _two_identity_destination(engine: AsyncEngine, table: str) -> str:
    """A destination whose second identity refuses what the first would have accepted."""
    name = f"{table}_two"
    await exec_sql(
        engine,
        f'CREATE TABLE "{name}" ('
        "id BIGINT GENERATED ALWAYS AS IDENTITY (MAXVALUE 5), "
        "tenant_id BIGINT GENERATED BY DEFAULT AS IDENTITY (MINVALUE 1 MAXVALUE 10 CYCLE), "
        "created_at TIMESTAMPTZ NOT NULL, amount NUMERIC NOT NULL DEFAULT 0, "
        "doubled NUMERIC GENERATED ALWAYS AS (amount * 2) STORED)",
    )
    return name


async def test__unpartition__a_later_identity_refuses__the_first_sequence_never_moved(
    db_engine: AsyncEngine, ledger: str
) -> None:
    """A sequence does not roll back with the transaction, so none may move until all agree."""
    # Arrange: id 4 is a value the first sequence could still reach; tenant_id 1
    # is inside the second's cycling range, which refuses the move
    await _identity_rows(db_engine, ledger, ids=(4,))
    config = monthly_config(ledger, create_ahead=1)
    await make_service(db_engine).partition_data(config)
    dest = await _two_identity_destination(db_engine, ledger)

    # Act
    result = await make_service(db_engine).unpartition(config, f"public.{dest}")

    # Assert: refused whole, and the first sequence is still where it started
    assert not result.complete
    assert any("cycles" in issue.error for issue in result.issues)
    assert await _count(db_engine, ledger) == 1
    assert (
        await scalar(db_engine, f"SELECT pg_sequence_last_value(pg_get_serial_sequence('public.{dest}', 'id'))") is None
    )


def _one_connection_engine(postgres_container: PostgresContainer) -> AsyncEngine:
    """An engine with a single connection, so the caller and the library share a backend."""
    url = postgres_container.get_connection_url()
    if "://" in url:
        _, rest = url.split("://", 1)
        url = f"postgresql+asyncpg://{rest}"
    return create_async_engine(url, echo=False, pool_size=1, max_overflow=0)


async def test__move_rows__a_temporary_table_of_the_callers_own__is_left_alone(
    db_engine: AsyncEngine, ledger: str, postgres_container: PostgresContainer
) -> None:
    """The temporary schema belongs to the session, and the caller may hold names in it."""
    # Arrange: a row to move, and one connection for both the caller and the library
    source = f"{ledger}_donor"
    await exec_sql(db_engine, f'CREATE TABLE "{source}" (LIKE "{ledger}" INCLUDING ALL EXCLUDING IDENTITY)')
    await exec_sql(
        db_engine,
        f'INSERT INTO "{source}" (id, tenant_id, created_at, amount) '  # noqa: S608
        "VALUES (50, 1, make_timestamptz(2026, 4, 1, 12, 0, 0, 'UTC'), 1)",
    )
    dest = await _identity_destination(db_engine, ledger, "callertemp", "START WITH 100 CACHE 5")
    engine = _one_connection_engine(postgres_container)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE TEMP TABLE pg_partsmith_moved_identity (id BIGINT)"))
            await conn.execute(text("INSERT INTO pg_partsmith_moved_identity VALUES (999)"))

        # Act: the same backend now runs the move
        moved = await PostgresPartitionRepository(engine).move_rows(source, dest)

        # Assert: the move parked its ids elsewhere and left the caller's table alone
        assert moved == 1
        async with engine.begin() as conn:
            kept = (await conn.execute(text("SELECT id FROM pg_partsmith_moved_identity"))).scalars().all()
        assert list(kept) == [999]
    finally:
        await engine.dispose()
