from datetime import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.entities import PartitionGranularity, PartitionInfo, TablePartitionConfig
from tests.integration.aio.builder import PartitioningScenarioBuilder


@pytest.mark.integration
async def test__scenario__fresh_table__creates_partitions_ahead_as_configured(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = await partition_builder.with_create_ahead(2).build()

    # Act
    result = await ctx.run_maintenance(at_time="2024-12-01")

    # Assert
    assert result.success
    assert result.created_count == 2
    await ctx.assert_partition_exists(f"{ctx.table_name}__2024_12")
    await ctx.assert_partition_exists(f"{ctx.table_name}__2025_01")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2024_12")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2025_01")


@pytest.mark.integration
async def test__scenario__second_run_same_time__creates_zero_partitions(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = await partition_builder.with_create_ahead(1).build()

    # Act
    r1 = await ctx.run_maintenance(at_time="2024-06-01")
    r2 = await ctx.run_maintenance(at_time="2024-06-01")

    # Assert
    assert r1.created_count == 1
    assert r2.created_count == 0
    await ctx.assert_partition_count(1)


@pytest.mark.integration
async def test__scenario__partitions_beyond_retention__pruned(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange — create 2 ahead, keep only 2
    ctx = await partition_builder.with_create_ahead(2).with_retention(2).build()
    await ctx.run_maintenance(at_time="2024-01-01")
    await ctx.assert_partition_exists(f"{ctx.table_name}__2024_01")
    await ctx.assert_partition_exists(f"{ctx.table_name}__2024_02")

    # Act — advance to April: Jan and Feb fall outside retention window
    result = await ctx.run_maintenance(at_time="2024-04-01")

    # Assert
    assert result.dropped_count >= 2
    await ctx.assert_partition_not_exists(f"{ctx.table_name}__2024_01")
    await ctx.assert_partition_not_exists(f"{ctx.table_name}__2024_02")


@pytest.mark.integration
async def test__scenario__detached_orphan__cleaned_on_next_run(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    partition_name = f"{partition_builder._table_name}__2024_01"
    ctx = await (
        partition_builder.with_detached_partition(partition_name, "2024-01-01", "2024-02-01").with_retention(1).build()
    )
    await ctx.assert_partition_exists(partition_name)
    await ctx.assert_partition_detached(partition_name)

    # Act
    result = await ctx.run_maintenance(at_time="2024-04-01")

    # Assert
    assert result.dropped_count >= 1
    await ctx.assert_partition_not_exists(partition_name)


@pytest.mark.integration
async def test__scenario__orphan_partition_within_retention__auto_attached(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange — partition is needed (Dec) but detached
    partition_name = f"{partition_builder._table_name}__2024_12"
    ctx = await (
        partition_builder.with_detached_partition(partition_name, "2024-12-01", "2025-01-01")
        .with_create_ahead(1)
        .build()
    )
    await ctx.assert_partition_exists(partition_name)
    await ctx.assert_partition_detached(partition_name)

    # Act
    await ctx.run_maintenance(at_time="2024-12-01")

    # Assert
    await ctx.assert_partition_attached(partition_name)


@pytest.mark.integration
async def test__scenario__fk_on_partition__constraint_removed_before_drop(
    partition_builder: PartitioningScenarioBuilder,
    db_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    # Arrange
    ref_table = "referenced_table"
    await db_session.execute(text(f"CREATE TABLE {ref_table} (id BIGINT PRIMARY KEY)"))
    await db_session.commit()

    partition_name = f"{partition_builder._table_name}__2024_01"
    ctx = await (
        partition_builder.with_attached_partition(partition_name, "2024-01-01", "2024-02-01")
        .with_fk_on_partition(partition_name, ref_table)
        .with_retention(1)
        .build()
    )
    await ctx.repo.detach_partition(ctx.table_name, partition_name, concurrent=False)

    # Act
    result = await ctx.run_maintenance(at_time="2024-04-01")

    # Assert
    assert result.dropped_count >= 1
    await ctx.assert_partition_not_exists(partition_name)

    await db_session.execute(text(f"DROP TABLE {ref_table}"))
    await db_session.commit()


@pytest.mark.integration
async def test__scenario__lifecycle_hooks__fired_at_correct_points(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    hook_calls: list[str] = []

    class TrackingHooks(BasePartitionLifecycleHooks):
        async def after_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
            hook_calls.append(f"after_create:{partition.name}")

        async def after_drop(self, table_name: str, partition_name: str) -> None:
            hook_calls.append(f"after_drop:{partition_name}")

    ctx = await partition_builder.with_create_ahead(1).with_retention(1).with_hooks([TrackingHooks()]).build()

    # Act — create run
    await ctx.run_maintenance(at_time="2024-01-01")
    assert any(c.startswith("after_create:") for c in hook_calls)

    # Act — drop run
    await ctx.run_maintenance(at_time="2024-03-01")

    # Assert
    assert any(c.startswith("after_drop:") for c in hook_calls)


@pytest.mark.integration
async def test__scenario__week_granularity__idempotent_second_run(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = await partition_builder.with_granularity(PartitionGranularity.WEEK).with_create_ahead(1).build()

    # Act
    first = await ctx.run_maintenance(at_time="2024-03-20")
    second = await ctx.run_maintenance(at_time="2024-03-20")

    # Assert
    assert first.created_count == 1
    assert second.created_count == 0


@pytest.mark.integration
async def test__scenario__week_granularity__prunes_old_weeks(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = await (
        partition_builder.with_granularity(PartitionGranularity.WEEK).with_create_ahead(1).with_retention(1).build()
    )
    await ctx.run_maintenance(at_time="2024-03-20")  # create w12
    old_partition = f"{ctx.table_name}__2024_w12"
    await ctx.assert_partition_exists(old_partition)

    # Act — current week 15, w12 falls outside retention
    result = await ctx.run_maintenance(at_time="2024-04-10")

    # Assert
    assert result.success
    assert result.dropped_count >= 1
    await ctx.assert_partition_not_exists(old_partition)


@pytest.mark.integration
async def test__scenario__default_partition_has_conflicting_rows__reconciles_and_attaches(
    partition_builder: PartitioningScenarioBuilder,
    db_session: AsyncSession,
) -> None:
    # Arrange
    ctx = await partition_builder.with_default_partition().with_create_ahead(1).build()

    # Insert a row into DEFAULT that belongs to the upcoming April partition
    await db_session.execute(
        text(f'INSERT INTO "{ctx.table_name}_default" (created_at, data) VALUES (:dt, :data)'),  # noqa: S608
        {"dt": datetime(2024, 4, 15), "data": "test"},
    )
    await db_session.commit()

    # Act
    result = await ctx.run_maintenance(at_time="2024-04-01")

    # Assert
    assert result.success
    assert result.created_count == 1
    await ctx.assert_partition_attached(f"{ctx.table_name}__2024_04")

    count_in_default = await db_session.execute(
        text(f'SELECT COUNT(*) FROM "{ctx.table_name}_default"')  # noqa: S608
    )
    count_in_april = await db_session.execute(
        text(f'SELECT COUNT(*) FROM "{ctx.table_name}__2024_04"')  # noqa: S608
    )
    assert count_in_default.scalar() == 0
    assert count_in_april.scalar() == 1
