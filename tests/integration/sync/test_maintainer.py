from collections.abc import Generator
from unittest.mock import patch

import freezegun
import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.strategies import MonthPeriodCalculator
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.maintainer import PartitionMaintainer
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.sync.service import PartitionLifecycleService


@pytest.fixture
def partitioned_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sync_maint_events (
                    id BIGSERIAL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    data TEXT,
                    PRIMARY KEY (id, created_at)
                ) PARTITION BY RANGE (created_at)
                """
            )
        )
    yield "sync_maint_events"
    with sync_db_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sync_maint_events CASCADE"))


def _make_components(
    engine: Engine,
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
def test__maintainer__initial_run__creates_partitions_ahead(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(sync_db_engine)
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
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.created_count == 2
    partitions = metadata.list_partitions(partitioned_table)
    names = {p.name for p in partitions}
    assert f"{partitioned_table}__2024_12" in names
    assert f"{partitioned_table}__2025_01" in names


@pytest.mark.integration
def test__maintainer__second_run_same_month__creates_zero_partitions(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(sync_db_engine)
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
        r1 = maintainer.run_maintenance(config)
        r2 = maintainer.run_maintenance(config)

    # Assert
    assert r1.created_count == 1
    assert r2.created_count == 0


@pytest.mark.integration
def test__maintainer__partitions_beyond_retention__detaches_and_drops_them(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(sync_db_engine)
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
        maintainer.run_maintenance(config)

    # Act — advance to May 2025: Dec 2024 and Jan 2025 fall outside retention
    with freezegun.freeze_time("2025-05-01"):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.dropped_count >= 2
    partitions = metadata.list_partitions(partitioned_table)
    names = {p.name for p in partitions}
    assert f"{partitioned_table}__2024_12" not in names
    assert f"{partitioned_table}__2025_01" not in names


@pytest.mark.integration
def test__maintainer__still_attached_partition__skips_drop(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(sync_db_engine)
    service = PartitionLifecycleService(repo, metadata, locks, calc)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )

    p1 = repo.create_partition(config, f"{partitioned_table}__2024_01", "2024-01-01", "2024-02-01")
    repo.attach_partition(partitioned_table, p1.name, "2024-01-01", "2024-02-01")
    p2 = repo.create_partition(config, f"{partitioned_table}__2024_02", "2024-02-01", "2024-03-01")
    repo.attach_partition(partitioned_table, p2.name, "2024-02-01", "2024-03-01")
    repo.detach_partition(partitioned_table, p1.name, concurrent=False)

    # Act — try to drop both; only p1 is an orphan
    dropped = service.drop_detached_partitions(partitioned_table, [p1.name, p2.name])

    # Assert
    assert dropped == 1
    assert not repo.partition_exists(p1.name)
    assert repo.partition_exists(p2.name)
    assert repo.is_partition_attached(partitioned_table, p2.name)


@pytest.mark.integration
def test__maintainer__detach_fails_one_run__drops_orphan_on_next_run(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(sync_db_engine)
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
        maintainer.run_maintenance(config)

    with (
        patch.object(repo, "detach_partition", side_effect=SQLAlchemyError("detach failed")),
        freezegun.freeze_time("2024-03-01"),
        pytest.raises(SQLAlchemyError),
    ):
        maintainer.run_maintenance(config)

    # Act — retry without the fault injection
    with freezegun.freeze_time("2024-03-01"):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.dropped_count >= 1


@pytest.mark.integration
def test__maintainer__hooks_called_at_lifecycle_points(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(sync_db_engine)
    hook_events: list[str] = []

    class AuditHooks(BasePartitionLifecycleHooks):
        def after_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
            hook_events.append(f"created:{partition.name}")

        def before_drop(self, table_name: str, partition_name: str) -> None:
            hook_events.append(f"before_drop:{partition_name}")

        def after_drop(self, table_name: str, partition_name: str) -> None:
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
        maintainer.run_maintenance(config)

    assert any(e.startswith("created:") for e in hook_events)

    # Act — second run drops old
    with freezegun.freeze_time("2024-04-01"):
        maintainer.run_maintenance(config)

    # Assert
    assert any(e.startswith("before_drop:") for e in hook_events)
    assert any(e.startswith("dropped:") for e in hook_events)


@pytest.mark.integration
def test__maintainer__orphaned_partition__dropped_on_next_run(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks, calc = _make_components(sync_db_engine)
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
    repo.create_partition(config, partition_name, "2024-01-01", "2024-02-01")
    repo.attach_partition(partitioned_table, partition_name, "2024-01-01", "2024-02-01")
    # Simulate interrupted previous run: detached but not dropped
    repo.detach_partition(partitioned_table, partition_name, concurrent=False)
    assert repo.partition_exists(partition_name)

    # Act
    with freezegun.freeze_time("2024-04-01"):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.dropped_count >= 1
    assert not repo.partition_exists(partition_name)
