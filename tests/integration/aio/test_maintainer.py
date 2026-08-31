"""The maintainer orchestrating the lifecycle service against a real PostgreSQL (async)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import patch

import freezegun
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.maintainer import PartitionMaintainer
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.repositories.remover import PartitionRemover
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.entities import (
    MaintenanceResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import LockAcquisitionError, PlanStaleError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.plan import Reason
from pg_partsmith.topology import RangeBounds
from tests.integration.aio.support import (
    count_ddl,
    exec_sql,
    exec_sql_autocommit,
    is_attached,
    make_table,
    relkind,
    scalar,
    table_comment,
)
from tests.integration.nested_support import MONTHLY_TABLE_DDL, monthly_config

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def partitioned_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, MONTHLY_TABLE_DDL, prefix="maint"):
        yield name


def _make_components(
    engine: AsyncEngine,
) -> tuple[PostgresPartitionRepository, PostgresMetadataProvider, PostgresAdvisoryLockManager]:
    return (
        PostgresPartitionRepository(engine),
        PostgresMetadataProvider(engine),
        PostgresAdvisoryLockManager(engine),
    )


# ── PartitionMaintainer lifecycle ────────────────────────────────────────────────


async def test__maintainer__initial_run__creates_partitions_ahead(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange — the 0.x spelling, type and strategy included, is still accepted
    repo, metadata, locks = _make_components(db_engine)
    service = PartitionLifecycleService(repo, metadata, locks)
    maintainer = PartitionMaintainer(service)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=2,
        retention_count=6,
    )

    # Act
    with freezegun.freeze_time("2024-12-01"):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.created_count == 2
    # list_partitions always returns schema-qualified names
    partitions = await metadata.list_partitions(partitioned_table)
    names = {p.name for p in partitions}
    assert f"public.{partitioned_table}__2024_12" in names
    assert f"public.{partitioned_table}__2025_01" in names


async def test__maintainer__second_run_same_month__creates_zero_partitions(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks = _make_components(db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, create_ahead=1, retention=12)

    # Act
    with freezegun.freeze_time("2024-06-01"):
        r1 = await maintainer.run_maintenance(config)
        with count_ddl(db_engine) as counter:
            r2 = await maintainer.run_maintenance(config)

    # Assert
    assert r1.created_count == 1
    assert r2.created_count == 0
    assert counter.statements == []


async def test__maintainer__partitions_beyond_retention__detaches_and_drops_them(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks = _make_components(db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, create_ahead=2, retention=3)

    with freezegun.freeze_time("2024-12-01"):
        await maintainer.run_maintenance(config)

    # Act — advance to May 2025: Dec 2024 and Jan 2025 fall outside retention
    with freezegun.freeze_time("2025-05-01"):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.detached_count == 2
    assert result.dropped_count == 2
    partitions = await metadata.list_partitions(partitioned_table)
    names = {p.name for p in partitions}
    assert f"public.{partitioned_table}__2024_12" not in names
    assert f"public.{partitioned_table}__2025_01" not in names
    assert f"public.{partitioned_table}__2025_05" in names
    assert f"public.{partitioned_table}__2025_06" in names


async def test__maintainer__still_attached_partition__skips_drop(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks = _make_components(db_engine)
    service = PartitionLifecycleService(repo, metadata, locks)

    p1 = f"{partitioned_table}__2024_01"
    p2 = f"{partitioned_table}__2024_02"
    await repo.create_table_like(partitioned_table, p1, None)
    await repo.attach_partition(partitioned_table, p1, RangeBounds(from_value="2024-01-01", to_value="2024-02-01"))
    await repo.create_table_like(partitioned_table, p2, None)
    await repo.attach_partition(partitioned_table, p2, RangeBounds(from_value="2024-02-01", to_value="2024-03-01"))
    await repo.detach_partition(partitioned_table, p1, mode=DetachMode.BLOCKING)

    # Act — try to drop both; only p1 is an orphan
    dropped = await service.drop_detached_partitions(partitioned_table, [p1, p2])

    # Assert
    assert dropped == 1
    assert not await metadata.partition_exists(p1)
    assert await metadata.partition_exists(p2)
    assert await metadata.is_partition_attached(partitioned_table, p2)


async def test__maintainer__detach_fails_one_run__drops_orphan_on_next_run(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks = _make_components(db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)

    with freezegun.freeze_time("2024-01-01"):
        await maintainer.run_maintenance(config)

    with (
        patch.object(repo, "detach_partition", side_effect=SQLAlchemyError("detach failed")),
        freezegun.freeze_time("2024-03-01"),
        pytest.raises(SQLAlchemyError),
    ):
        await maintainer.run_maintenance(config)

    # Act — retry without the fault injection
    with freezegun.freeze_time("2024-03-01"):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert not await metadata.partition_exists(f"{partitioned_table}__2024_01")


async def test__maintainer__hooks_called_at_lifecycle_points(db_engine: AsyncEngine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks = _make_components(db_engine)
    hook_events: list[str] = []

    class AuditHooks(BasePartitionLifecycleHooks):
        async def after_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
            hook_events.append(f"created:{partition.name}")

        async def before_drop(self, table_name: str, partition_name: str) -> None:
            hook_events.append(f"before_drop:{partition_name}")

        async def after_drop(self, table_name: str, partition_name: str) -> None:
            hook_events.append(f"dropped:{partition_name}")

    service = PartitionLifecycleService(repo, metadata, locks, hooks=[AuditHooks()])
    maintainer = PartitionMaintainer(service)
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)

    # Act — first run creates
    with freezegun.freeze_time("2024-01-01"):
        await maintainer.run_maintenance(config)

    assert hook_events == [f"created:public.{partitioned_table}__2024_01"]

    # Act — second run drops old
    with freezegun.freeze_time("2024-04-01"):
        await maintainer.run_maintenance(config)

    # Assert
    assert hook_events == [
        f"created:public.{partitioned_table}__2024_01",
        f"created:public.{partitioned_table}__2024_04",
        f"before_drop:public.{partitioned_table}__2024_01",
        f"dropped:public.{partitioned_table}__2024_01",
    ]


async def test__maintainer__orphaned_partition__dropped_on_next_run(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks = _make_components(db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)

    partition_name = f"{partitioned_table}__2024_01"
    await repo.create_table_like(partitioned_table, partition_name, None)
    await repo.attach_partition(
        partitioned_table, partition_name, RangeBounds(from_value="2024-01-01", to_value="2024-02-01")
    )
    # Simulate an interrupted previous run: detached (at the time) but not dropped
    with freezegun.freeze_time("2024-02-01"):
        await repo.detach_partition(partitioned_table, partition_name, mode=DetachMode.BLOCKING)
    assert await metadata.partition_exists(partition_name)

    # Act
    with freezegun.freeze_time("2024-04-01"):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.dropped_count == 1
    assert not await metadata.partition_exists(partition_name)


async def test__maintainer__run_maintenance_safe__never_raises_and_reports_the_error(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange — a config that does not match the table
    repo, metadata, locks = _make_components(db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, column="payload")

    # Act
    result = await maintainer.run_maintenance_safe(config)

    # Assert
    assert not result.success
    assert result.error is not None
    assert "InvalidPartitionConfigError" in result.error
    assert result.created_count == 0


# ── Two maintainers on the same table ────────────────────────────────────────────


# sync-mirror: skip
async def test__maintainer__two_concurrent_runs_on_one_table__one_wins_the_lock_and_the_tree_converges(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange — two independent wirings, as two workers would have
    first = PartitionMaintainer(PartitionLifecycleService(*_make_components(db_engine)))
    second = PartitionMaintainer(PartitionLifecycleService(*_make_components(db_engine)))
    config = monthly_config(partitioned_table, create_ahead=3, retention=12)

    # Act — both ticks at once
    with freezegun.freeze_time("2026-08-26"):
        outcomes = await asyncio.gather(
            first.run_maintenance(config),
            second.run_maintenance(config),
            return_exceptions=True,
        )

    # Assert — each run either won the lock or lost it; nothing else may happen
    results = [o for o in outcomes if isinstance(o, MaintenanceResult)]
    losers = [o for o in outcomes if isinstance(o, LockAcquisitionError)]
    assert len(results) + len(losers) == 2
    assert len(results) >= 1
    assert sum(r.created_count for r in results) == 3

    # The tree ends converged either way: nothing left to do
    with freezegun.freeze_time("2026-08-26"), count_ddl(db_engine) as counter:
        again = await first.run_maintenance(config)
    assert again.created_count == 0
    assert counter.statements == []
    names = {p.relname for p in await PostgresMetadataProvider(db_engine).list_partitions(partitioned_table)}
    assert names == {f"{partitioned_table}__2026_08", f"{partitioned_table}__2026_09", f"{partitioned_table}__2026_10"}


# ── An interrupted DETACH CONCURRENTLY ──────────────────────────────────────────


# sync-mirror: skip
async def _leave_detach_pending(engine: AsyncEngine, parent: str, partition: str) -> None:
    """Reproduce a cancelled ``DETACH CONCURRENTLY``.

    Its first transaction commits ``inhdetachpending``; the second waits for
    every transaction holding a lock on the parent. A reader that keeps its
    transaction open makes it wait, and cancelling it there leaves the
    partition half-detached, exactly as a DDL timeout would.
    """
    reader_ready = asyncio.Event()
    release_reader = asyncio.Event()

    async def read_and_hold() -> None:
        async with engine.connect() as conn:
            await conn.execute(text(f'SELECT 1 FROM "{parent}" LIMIT 0'))  # noqa: S608
            reader_ready.set()
            await release_reader.wait()
            await conn.rollback()

    async def detach() -> None:
        await reader_ready.wait()
        with pytest.raises(DBAPIError, match="canceling statement"):
            await exec_sql_autocommit(engine, f'ALTER TABLE "{parent}" DETACH PARTITION "{partition}" CONCURRENTLY')

    async def cancel_once_waiting() -> None:
        await reader_ready.wait()
        for _ in range(200):
            await asyncio.sleep(0.05)
            waiting = await scalar(
                engine,
                "SELECT pid FROM pg_stat_activity "
                "WHERE query ILIKE :pattern AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()",
                pattern=f'%DETACH PARTITION "{partition}" CONCURRENTLY%',
            )
            if waiting is not None:
                await scalar(engine, "SELECT pg_cancel_backend(:pid)", pid=waiting)
                break
        release_reader.set()

    await asyncio.gather(read_and_hold(), detach(), cancel_once_waiting())


# sync-mirror: skip
async def test__maintainer__interrupted_concurrent_detach__finalized_and_retired_in_one_call(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange: April exists, expired, and a cancelled DETACH CONCURRENTLY left it half-detached
    config = monthly_config(partitioned_table, create_ahead=1, retention=2)
    maintainer = PartitionMaintainer(PartitionLifecycleService(*_make_components(db_engine)))
    with freezegun.freeze_time("2026-04-15"):
        await maintainer.run_maintenance(config)
    april = f"{partitioned_table}__2026_04"
    await _leave_detach_pending(db_engine, partitioned_table, april)
    assert await scalar(
        db_engine, "SELECT inhdetachpending FROM pg_inherits WHERE inhrelid = to_regclass(:name)", name=april
    )

    # Act: one tick
    with freezegun.freeze_time("2026-08-26"):
        result = await maintainer.run_maintenance(config)

    # Assert: the detach was completed with FINALIZE, and the same call retired the orphan
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert result.issues == ()
    assert [op.reason for op in result.plan.drops] == [Reason.GRACE_ELAPSED]
    assert await relkind(db_engine, april) is None


# sync-mirror: skip
async def test__maintainer__interrupted_detach_of_a_wanted_window__reattached_in_the_same_call(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange: the current month itself was left half-detached, with a row in it
    config = monthly_config(partitioned_table, create_ahead=1, retention=12)
    maintainer = PartitionMaintainer(PartitionLifecycleService(*_make_components(db_engine)))
    with freezegun.freeze_time("2026-08-10"):
        await maintainer.run_maintenance(config)
    august = f"{partitioned_table}__2026_08"
    await exec_sql(
        db_engine,
        f"INSERT INTO \"{partitioned_table}\" (created_at, payload) VALUES ('2026-08-05T12:00:00+00:00', 'kept')",  # noqa: S608
    )
    await _leave_detach_pending(db_engine, partitioned_table, august)
    assert await scalar(
        db_engine, "SELECT inhdetachpending FROM pg_inherits WHERE inhrelid = to_regclass(:name)", name=august
    )

    # Act: one tick
    with freezegun.freeze_time("2026-08-26"):
        result = await maintainer.run_maintenance(config)

    # Assert: finalized, then re-attached with its data; the marker went with the attach
    assert result.detached_count == 1
    assert result.attached_count == 1
    assert result.dropped_count == 0
    assert await is_attached(db_engine, august)
    assert int(await scalar(db_engine, f'SELECT count(*) FROM "{august}"')) == 1  # noqa: S608
    assert await table_comment(db_engine, august) is None


# ── A hook must not be able to redirect a detach ────────────────────────────────


class _SwapHooks(BasePartitionLifecycleHooks):
    """Replaces the partition at the planned name from inside ``before_detach``."""

    def __init__(self, engine: AsyncEngine, parent: str) -> None:
        self._engine = engine
        self._parent = parent

    async def before_detach(self, table_name: str, partition: object) -> None:
        name = partition.relname  # type: ignore[attr-defined]
        lower, upper = partition.from_value, partition.to_value  # type: ignore[attr-defined]
        async with self._engine.begin() as conn:
            await conn.execute(text(f'ALTER TABLE "{self._parent}" DETACH PARTITION "{name}"'))
            await conn.execute(text(f'DROP TABLE "{name}"'))
            await conn.execute(text(f'CREATE TABLE "{name}" (LIKE "{self._parent}" INCLUDING ALL)'))
            await conn.execute(
                text(
                    f'ALTER TABLE "{self._parent}" ATTACH PARTITION "{name}" '
                    f"FOR VALUES FROM ('{lower}') TO ('{upper}')"
                )
            )


async def test__maintainer__relation_swapped_by_a_hook__detach_refused_as_stale(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange: April expired; a hook swaps in a different relation under its name
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)
    hook = _SwapHooks(db_engine, partitioned_table)
    service = PartitionLifecycleService(*_make_components(db_engine), hooks=[hook])
    with freezegun.freeze_time("2026-04-15"):
        await service.maintain(config)

    # Act / Assert: the repository re-checks identity under its own lock and refuses
    with freezegun.freeze_time("2026-08-26"), pytest.raises(PlanStaleError):
        await service.maintain(config)

    # The replacement is untouched: still attached, never marked
    april = f"{partitioned_table}__2026_04"
    assert await is_attached(db_engine, april)
    assert await table_comment(db_engine, april) is None


# sync-mirror: skip
async def test__maintainer__pinned_concurrent_detach__a_swap_cannot_slip_in_while_it_is_marked(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange: April expired; a saboteur tries to detach it out from under us mid-flight
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)
    maintainer = PartitionMaintainer(PartitionLifecycleService(*_make_components(db_engine)))
    with freezegun.freeze_time("2026-04-15"):
        await maintainer.run_maintenance(config)
    april = f"{partitioned_table}__2026_04"
    swap_refused: list[str] = []
    original = PartitionRemover._mark_orphaned

    async def marking_swap(
        self: PartitionRemover, conn: object, table_name: str, partition_name: str, *, stamp_now: bool = True
    ) -> None:
        # The pin (ACCESS SHARE, identity verified under it) is held right now:
        # a swap needs ACCESS EXCLUSIVE and must time out instead of slipping in.
        try:
            async with db_engine.begin() as other:
                await other.execute(text("SET LOCAL lock_timeout = '300ms'"))
                await other.execute(text(f'ALTER TABLE "{partitioned_table}" DETACH PARTITION "{april}"'))
        except DBAPIError as exc:
            swap_refused.append(str(exc.orig))
        return await original(self, conn, table_name, partition_name, stamp_now=stamp_now)  # type: ignore[arg-type]

    # Act
    with (
        freezegun.freeze_time("2026-08-26"),
        patch.object(PartitionRemover, "_mark_orphaned", marking_swap),
    ):
        result = await maintainer.run_maintenance(config)

    # Assert: the swap timed out on the pin; the right relation was detached, then
    # retired by the same call's drop policy -- exactly what the plan decided.
    assert swap_refused, "the pin must hold the name while the relation is checked and marked"
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert await scalar(db_engine, "SELECT CAST(to_regclass(:n) AS oid)", n=april) is None


# sync-mirror: skip
async def test__detach_partition__pinned_and_timing_out__the_statement_is_cancelled_not_left_running(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    """A timeout must not release the pin under a statement that has yet to run.

    The clock is real here on purpose: freezegun freezes ``time.monotonic``,
    which is what ``asyncio.timeout`` and ``asyncio.sleep`` run on.
    """
    # Arrange: one attached partition, and a DDL delayed past the repository's own timeout
    config = monthly_config(partitioned_table, create_ahead=1, retention=12)
    with freezegun.freeze_time("2026-04-15"):
        await PartitionMaintainer(PartitionLifecycleService(*_make_components(db_engine))).run_maintenance(config)
    april = f"{partitioned_table}__2026_04"
    oid_before = await scalar(db_engine, "SELECT CAST(to_regclass(:n) AS oid)", n=april)
    repo = PostgresPartitionRepository(db_engine, ddl_timeout_seconds=0.3)
    original = PartitionRemover._run_concurrent_detach

    async def delayed(self: PartitionRemover, *args: object, **kwargs: object) -> bool:
        await asyncio.sleep(3)
        return await original(self, *args, **kwargs)  # type: ignore[arg-type]

    # Act / Assert: the wait ends in a timeout, with the statement cancelled under the pin
    with patch.object(PartitionRemover, "_run_concurrent_detach", delayed), pytest.raises(TimeoutError):
        await repo.detach_partition(
            f"public.{partitioned_table}", f"public.{april}", mode=DetachMode.CONCURRENT, expected_oid=oid_before
        )

    # Nothing was detached behind the timeout, and the relation is untouched
    assert await is_attached(db_engine, april)
    assert await scalar(db_engine, "SELECT CAST(to_regclass(:n) AS oid)", n=april) == oid_before
