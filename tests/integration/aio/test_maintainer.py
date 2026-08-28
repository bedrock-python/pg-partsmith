"""The maintainer orchestrating the lifecycle service against a real PostgreSQL (async)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import patch

import freezegun
import pytest
import pytest_asyncio
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.maintainer import PartitionMaintainer
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.entities import (
    MaintenanceResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import LockAcquisitionError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.topology import RangeBounds
from tests.integration.aio.support import count_ddl, make_table
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
