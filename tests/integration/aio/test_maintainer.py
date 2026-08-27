from collections.abc import AsyncGenerator
from unittest.mock import patch

import freezegun
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.maintainer import PartitionMaintainer
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.strategies import MonthPeriodCalculator


@pytest_asyncio.fixture
async def partitioned_table(db_session: AsyncSession) -> AsyncGenerator[str, None]:
    await db_session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS maint_events (
                id BIGSERIAL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                data TEXT,
                PRIMARY KEY (id, created_at)
            ) PARTITION BY RANGE (created_at)
            """
        )
    )
    await db_session.commit()
    yield "maint_events"
    await db_session.execute(text("DROP TABLE IF EXISTS maint_events CASCADE"))
    await db_session.commit()


def _make_components(
    engine: AsyncEngine,
) -> tuple[
    PostgresPartitionRepository,
    PostgresMetadataProvider,
    PostgresAdvisoryLockManager,
    MonthPeriodCalculator,
]:
    return (
        PostgresPartitionRepository(engine),
        PostgresMetadataProvider(engine),
        PostgresAdvisoryLockManager(engine),
        MonthPeriodCalculator(),
    )


# ── PartitionMaintainer lifecycle ────────────────────────────────────────────────


@pytest.mark.integration
async def test__maintainer__initial_run__creates_partitions_ahead(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(db_engine)
    service = PartitionLifecycleService(repo, metadata, locks, calc)
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


@pytest.mark.integration
async def test__maintainer__second_run_same_month__creates_zero_partitions(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(db_engine)
    service = PartitionLifecycleService(repo, metadata, locks, calc)
    maintainer = PartitionMaintainer(service)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=1,
        retention_count=12,
    )

    # Act
    with freezegun.freeze_time("2024-06-01"):
        r1 = await maintainer.run_maintenance(config)
        r2 = await maintainer.run_maintenance(config)

    # Assert
    assert r1.created_count == 1
    assert r2.created_count == 0


@pytest.mark.integration
async def test__maintainer__partitions_beyond_retention__detaches_and_drops_them(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(db_engine)
    service = PartitionLifecycleService(repo, metadata, locks, calc)
    maintainer = PartitionMaintainer(service)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=2,
        retention_count=3,
    )

    with freezegun.freeze_time("2024-12-01"):
        await maintainer.run_maintenance(config)

    # Act — advance to May 2025: Dec 2024 and Jan 2025 fall outside retention
    with freezegun.freeze_time("2025-05-01"):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.dropped_count >= 2
    partitions = await metadata.list_partitions(partitioned_table)
    names = {p.name for p in partitions}
    assert f"public.{partitioned_table}__2024_12" not in names
    assert f"public.{partitioned_table}__2025_01" not in names


@pytest.mark.integration
async def test__maintainer__still_attached_partition__skips_drop(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(db_engine)
    service = PartitionLifecycleService(repo, metadata, locks, calc)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )

    p1 = await repo.create_partition(config, f"{partitioned_table}__2024_01", "2024-01-01", "2024-02-01")
    await repo.attach_partition(partitioned_table, p1.name, "2024-01-01", "2024-02-01")
    p2 = await repo.create_partition(config, f"{partitioned_table}__2024_02", "2024-02-01", "2024-03-01")
    await repo.attach_partition(partitioned_table, p2.name, "2024-02-01", "2024-03-01")
    await repo.detach_partition(partitioned_table, p1.name, concurrent=False)

    # Act — try to drop both; only p1 is an orphan
    dropped = await service.drop_detached_partitions(partitioned_table, [p1.name, p2.name])

    # Assert
    assert dropped == 1
    assert not await metadata.partition_exists(p1.name)
    assert await metadata.partition_exists(p2.name)
    assert await metadata.is_partition_attached(partitioned_table, p2.name)


@pytest.mark.integration
async def test__maintainer__detach_fails_one_run__drops_orphan_on_next_run(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(db_engine)
    service = PartitionLifecycleService(repo, metadata, locks, calc)
    maintainer = PartitionMaintainer(service)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=1,
        retention_count=1,
    )

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
    assert result.dropped_count >= 1


@pytest.mark.integration
async def test__maintainer__hooks_called_at_lifecycle_points(db_engine: AsyncEngine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(db_engine)
    hook_events: list[str] = []

    class AuditHooks(BasePartitionLifecycleHooks):
        async def after_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
            hook_events.append(f"created:{partition.name}")

        async def before_drop(self, table_name: str, partition_name: str) -> None:
            hook_events.append(f"before_drop:{partition_name}")

        async def after_drop(self, table_name: str, partition_name: str) -> None:
            hook_events.append(f"dropped:{partition_name}")

    service = PartitionLifecycleService(repo, metadata, locks, calc, hooks=[AuditHooks()])
    maintainer = PartitionMaintainer(service)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=1,
        retention_count=1,
    )

    # Act — first run creates
    with freezegun.freeze_time("2024-01-01"):
        await maintainer.run_maintenance(config)

    assert any(e.startswith("created:") for e in hook_events)

    # Act — second run drops old
    with freezegun.freeze_time("2024-04-01"):
        await maintainer.run_maintenance(config)

    # Assert
    assert any(e.startswith("before_drop:") for e in hook_events)
    assert any(e.startswith("dropped:") for e in hook_events)


@pytest.mark.integration
async def test__maintainer__orphaned_partition__dropped_on_next_run(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(db_engine)
    service = PartitionLifecycleService(repo, metadata, locks, calc)
    maintainer = PartitionMaintainer(service)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=1,
        retention_count=1,
    )

    partition_name = f"{partitioned_table}__2024_01"
    await repo.create_partition(config, partition_name, "2024-01-01", "2024-02-01")
    await repo.attach_partition(partitioned_table, partition_name, "2024-01-01", "2024-02-01")
    # Simulate interrupted previous run: detached but not dropped
    await repo.detach_partition(partitioned_table, partition_name, concurrent=False)
    assert await metadata.partition_exists(partition_name)

    # Act
    with freezegun.freeze_time("2024-04-01"):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.dropped_count >= 1
    assert not await metadata.partition_exists(partition_name)
