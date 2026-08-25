from datetime import datetime

import pytest
from sqlalchemy import Engine, text

from pg_partsmith.entities import PartitionGranularity, PartitionInfo, TablePartitionConfig
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks
from tests.integration.sync.builder import PartitioningScenarioBuilder


@pytest.mark.integration
def test__scenario__fresh_table__creates_partitions_ahead_as_configured(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = sync_partition_builder.with_create_ahead(2).build()

    # Act
    result = ctx.run_maintenance(at_time="2024-12-01")

    # Assert
    assert result.success
    assert result.created_count == 2
    ctx.assert_partition_exists(f"{ctx.table_name}__2024_12")
    ctx.assert_partition_exists(f"{ctx.table_name}__2025_01")
    ctx.assert_partition_attached(f"{ctx.table_name}__2024_12")
    ctx.assert_partition_attached(f"{ctx.table_name}__2025_01")


@pytest.mark.integration
def test__scenario__second_run_same_time__creates_zero_partitions(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = sync_partition_builder.with_create_ahead(1).build()

    # Act
    r1 = ctx.run_maintenance(at_time="2024-06-01")
    r2 = ctx.run_maintenance(at_time="2024-06-01")

    # Assert
    assert r1.created_count == 1
    assert r2.created_count == 0
    ctx.assert_partition_count(1)


@pytest.mark.integration
def test__scenario__partitions_beyond_retention__pruned(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange — create 2 ahead, keep only 2
    ctx = sync_partition_builder.with_create_ahead(2).with_retention(2).build()
    ctx.run_maintenance(at_time="2024-01-01")
    ctx.assert_partition_exists(f"{ctx.table_name}__2024_01")
    ctx.assert_partition_exists(f"{ctx.table_name}__2024_02")

    # Act — advance to April: Jan and Feb fall outside retention window
    result = ctx.run_maintenance(at_time="2024-04-01")

    # Assert
    assert result.dropped_count >= 2
    ctx.assert_partition_not_exists(f"{ctx.table_name}__2024_01")
    ctx.assert_partition_not_exists(f"{ctx.table_name}__2024_02")


@pytest.mark.integration
def test__scenario__detached_orphan__cleaned_on_next_run(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    partition_name = f"{sync_partition_builder._table_name}__2024_01"
    ctx = (
        sync_partition_builder.with_detached_partition(partition_name, "2024-01-01", "2024-02-01")
        .with_retention(1)
        .build()
    )
    ctx.assert_partition_exists(partition_name)
    ctx.assert_partition_detached(partition_name)

    # Act
    result = ctx.run_maintenance(at_time="2024-04-01")

    # Assert
    assert result.dropped_count >= 1
    ctx.assert_partition_not_exists(partition_name)


@pytest.mark.integration
def test__scenario__orphan_partition_within_retention__auto_attached(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange — partition is needed (Dec) but detached
    partition_name = f"{sync_partition_builder._table_name}__2024_12"
    ctx = (
        sync_partition_builder.with_detached_partition(partition_name, "2024-12-01", "2025-01-01")
        .with_create_ahead(1)
        .build()
    )
    ctx.assert_partition_exists(partition_name)
    ctx.assert_partition_detached(partition_name)

    # Act
    ctx.run_maintenance(at_time="2024-12-01")

    # Assert
    ctx.assert_partition_attached(partition_name)


@pytest.mark.integration
def test__scenario__fk_on_partition__constraint_removed_before_drop(
    sync_partition_builder: PartitioningScenarioBuilder,
    sync_db_engine: Engine,
) -> None:
    # Arrange
    ref_table = "referenced_table"
    with sync_db_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {ref_table} (id BIGINT PRIMARY KEY)"))

    partition_name = f"{sync_partition_builder._table_name}__2024_01"
    ctx = (
        sync_partition_builder.with_attached_partition(partition_name, "2024-01-01", "2024-02-01")
        .with_fk_on_partition(partition_name, ref_table)
        .with_retention(1)
        .build()
    )
    ctx.repo.detach_partition(ctx.table_name, partition_name, concurrent=False)

    # Act
    result = ctx.run_maintenance(at_time="2024-04-01")

    # Assert
    assert result.dropped_count >= 1
    ctx.assert_partition_not_exists(partition_name)

    with sync_db_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE {ref_table}"))


@pytest.mark.integration
def test__scenario__lifecycle_hooks__fired_at_correct_points(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    hook_calls: list[str] = []

    class TrackingHooks(BasePartitionLifecycleHooks):
        def after_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
            hook_calls.append(f"after_create:{partition.name}")

        def after_drop(self, table_name: str, partition_name: str) -> None:
            hook_calls.append(f"after_drop:{partition_name}")

    ctx = sync_partition_builder.with_create_ahead(1).with_retention(1).with_hooks([TrackingHooks()]).build()

    # Act — create run
    ctx.run_maintenance(at_time="2024-01-01")
    assert any(c.startswith("after_create:") for c in hook_calls)

    # Act — drop run
    ctx.run_maintenance(at_time="2024-03-01")

    # Assert
    assert any(c.startswith("after_drop:") for c in hook_calls)


@pytest.mark.integration
def test__scenario__week_granularity__idempotent_second_run(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = sync_partition_builder.with_granularity(PartitionGranularity.WEEK).with_create_ahead(1).build()

    # Act
    first = ctx.run_maintenance(at_time="2024-03-20")
    second = ctx.run_maintenance(at_time="2024-03-20")

    # Assert
    assert first.created_count == 1
    assert second.created_count == 0


@pytest.mark.integration
def test__scenario__week_granularity__prunes_old_weeks(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = (
        sync_partition_builder.with_granularity(PartitionGranularity.WEEK)
        .with_create_ahead(1)
        .with_retention(1)
        .build()
    )
    ctx.run_maintenance(at_time="2024-03-20")  # create w12
    old_partition = f"{ctx.table_name}__2024_w12"
    ctx.assert_partition_exists(old_partition)

    # Act — current week 15, w12 falls outside retention
    result = ctx.run_maintenance(at_time="2024-04-10")

    # Assert
    assert result.success
    assert result.dropped_count >= 1
    ctx.assert_partition_not_exists(old_partition)


@pytest.mark.integration
def test__scenario__default_partition_has_conflicting_rows__reconciles_and_attaches(
    sync_partition_builder: PartitioningScenarioBuilder,
    sync_db_engine: Engine,
) -> None:
    # Arrange
    ctx = sync_partition_builder.with_default_partition().with_create_ahead(1).build()

    # Insert a row into DEFAULT that belongs to the upcoming April partition
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(f'INSERT INTO "{ctx.table_name}_default" (created_at, data) VALUES (:dt, :data)'),  # noqa: S608
            {"dt": datetime(2024, 4, 15), "data": "test"},
        )

    # Act
    result = ctx.run_maintenance(at_time="2024-04-01")

    # Assert
    assert result.success
    assert result.created_count == 1
    ctx.assert_partition_attached(f"{ctx.table_name}__2024_04")

    with sync_db_engine.begin() as conn:
        count_in_default = conn.execute(
            text(f'SELECT COUNT(*) FROM "{ctx.table_name}_default"')  # noqa: S608
        )
        count_in_april = conn.execute(
            text(f'SELECT COUNT(*) FROM "{ctx.table_name}__2024_04"')  # noqa: S608
        )
        assert count_in_default.scalar() == 0
        assert count_in_april.scalar() == 1
