from datetime import UTC, datetime

import pytest
from dateutil.parser import isoparse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.entities import (
    MaintenanceIssueStep,
    PartitionGranularity,
    PartitionInfo,
    Period,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import PartitionAttachedError
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


@pytest.mark.integration
async def test__scenario__hour_granularity__creates_ahead_and_prunes_old_hours(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = await (
        partition_builder.with_granularity(PartitionGranularity.HOUR).with_create_ahead(3).with_retention(2).build()
    )

    # Act — frozen mid-hour: current hour plus 2 ahead are created
    result = await ctx.run_maintenance(at_time="2026-08-25 14:30:00+00:00")

    # Assert
    assert result.success
    assert result.created_count == 3
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_14")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_15")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_16")

    # Act — advance to 17:30: hours 14 and 15 fall outside retention, hour 16 stays
    result = await ctx.run_maintenance(at_time="2026-08-25 17:30:00+00:00")

    # Assert
    assert result.success
    assert result.dropped_count >= 2
    await ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_08_25_14")
    await ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_08_25_15")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_16")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_17")

    # Act — advance to 23:30: create-ahead crosses midnight into the next day
    result = await ctx.run_maintenance(at_time="2026-08-25 23:30:00+00:00")

    # Assert
    assert result.success
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_23")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_26_00")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_26_01")
    await ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_08_25_16")


@pytest.mark.integration
async def test__scenario__quarter_granularity__creates_ahead_and_prunes_old_quarters(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = await (
        partition_builder.with_granularity(PartitionGranularity.QUARTER).with_create_ahead(2).with_retention(1).build()
    )

    # Act — frozen in Q3 2026: current quarter plus 1 ahead are created
    result = await ctx.run_maintenance(at_time="2026-08-15")

    # Assert
    assert result.success
    assert result.created_count == 2
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_q3")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2026_q4")

    # Act — advance to Q1 2027: 2026 quarters fall outside retention, 2027 quarters created ahead
    result = await ctx.run_maintenance(at_time="2027-01-10")

    # Assert
    assert result.success
    assert result.dropped_count >= 2
    await ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_q3")
    await ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_q4")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2027_q1")
    await ctx.assert_partition_attached(f"{ctx.table_name}__2027_q2")


@pytest.mark.integration
async def test__scenario__infinity_upper_bound__partition_never_pruned(
    partition_builder: PartitioningScenarioBuilder,
    db_session: AsyncSession,
) -> None:
    # Arrange — partition named like an ancient period but with an unbounded upper boundary
    partition_name = f"{partition_builder._table_name}__1970_01"
    ctx = await partition_builder.with_retention(1).build()
    await ctx.repo.create_partition(ctx.config, partition_name, "1970-01-01", "infinity")
    await ctx.repo.attach_partition(ctx.table_name, partition_name, "1970-01-01", "infinity")

    # A current row lands in the unbounded partition
    await db_session.execute(
        text(f'INSERT INTO "{ctx.table_name}" (created_at, data) VALUES (:dt, :data)'),  # noqa: S608
        {"dt": datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC), "data": "live"},
    )
    await db_session.commit()

    # Act — skip create: any new range would overlap the unbounded partition
    result = await ctx.run_maintenance(at_time="2026-08-15", skip_create=True)

    # Assert — never pruned despite the ancient-looking name; the row survives
    assert result.success
    assert result.detached_count == 0
    assert result.dropped_count == 0
    await ctx.assert_partition_attached(partition_name)
    count = await db_session.execute(text(f'SELECT COUNT(*) FROM "{partition_name}"'))  # noqa: S608
    assert count.scalar() == 1


@pytest.mark.integration
async def test__scenario__subpartitioned_child__detached_and_dropped(
    partition_builder: PartitioningScenarioBuilder,
    db_session: AsyncSession,
) -> None:
    # Arrange — child that is itself PARTITION BY RANGE, attached under an old period
    child = f"{partition_builder._table_name}__2024_01"
    leaf = f"{child}_leaf"
    ctx = await partition_builder.with_create_ahead(1).with_retention(1).build()

    await db_session.execute(
        text(
            f"""
            CREATE TABLE "{child}" (
                id BIGSERIAL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                data TEXT,
                PRIMARY KEY (id, created_at)
            ) PARTITION BY RANGE (created_at)
            """
        )
    )
    await db_session.execute(
        text(f"""CREATE TABLE "{leaf}" PARTITION OF "{child}" FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')""")
    )
    await db_session.commit()
    await ctx.repo.attach_partition(ctx.table_name, child, "2024-01-01", "2024-02-01")
    await ctx.assert_partition_attached(child)

    # Act — advance past retention
    result = await ctx.run_maintenance(at_time="2024-04-01")

    # Assert — the partitioned child was detached and dropped along with its leaf
    assert result.success
    assert result.detached_count >= 1
    assert result.dropped_count >= 1
    await ctx.assert_partition_not_exists(child)
    await ctx.assert_partition_not_exists(leaf)


@pytest.mark.integration
async def test__scenario__attached_partition_with_reattached_race__drop_refused(
    partition_builder: PartitioningScenarioBuilder,
    db_session: AsyncSession,
) -> None:
    # Arrange — detached via the repo (orphan marker set), then re-attached behind its back
    partition_name = f"{partition_builder._table_name}__2024_01"
    ctx = await partition_builder.with_attached_partition(partition_name, "2024-01-01", "2024-02-01").build()

    await db_session.execute(
        text(f'INSERT INTO "{ctx.table_name}" (created_at, data) VALUES (:dt, :data)'),  # noqa: S608
        {"dt": datetime(2024, 1, 15, tzinfo=UTC), "data": "keep-me"},
    )
    await db_session.commit()

    await ctx.repo.detach_partition(ctx.table_name, partition_name, concurrent=False)
    await db_session.execute(
        text(
            f'ALTER TABLE "{ctx.table_name}" ATTACH PARTITION "{partition_name}" '
            f"FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
        )
    )
    await db_session.commit()

    # Act / Assert — drop must refuse: the partition is attached again despite the marker
    with pytest.raises(PartitionAttachedError):
        await ctx.repo.drop_partition(partition_name)

    await ctx.assert_partition_exists(partition_name)
    await ctx.assert_partition_attached(partition_name)
    count = await db_session.execute(text(f'SELECT COUNT(*) FROM "{partition_name}"'))  # noqa: S608
    assert count.scalar() == 1


@pytest.mark.integration
async def test__scenario__ensure_partition__specific_past_period__created_and_attached(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = await partition_builder.build()
    partition_name = f"{ctx.table_name}__2024_02"

    # Act — target one specific past period, independent of "now"
    created = await ctx.service.ensure_partition(ctx.config, Period(year=2024, month=2))

    # Assert — created, attached, with the period's exact boundaries
    assert created is not None
    assert created.is_attached
    assert created.relname == partition_name
    await ctx.assert_partition_exists(partition_name)
    await ctx.assert_partition_attached(partition_name)

    boundaries = await ctx.metadata.get_partition_boundaries(partition_name)
    assert boundaries is not None
    from_value, to_value = boundaries
    assert isoparse(from_value) == datetime(2024, 2, 1, tzinfo=UTC)
    assert isoparse(to_value) == datetime(2024, 3, 1, tzinfo=UTC)

    # Act — second call is a no-op
    again = await ctx.service.ensure_partition(ctx.config, Period(year=2024, month=2))

    # Assert — nothing changed
    assert again is None
    await ctx.assert_partition_count(1)
    await ctx.assert_partition_attached(partition_name)
    assert await ctx.metadata.get_partition_boundaries(partition_name) == boundaries


@pytest.mark.integration
async def test__scenario__adopt_partition__legacy_detached_table__dropped_on_next_run(
    partition_builder: PartitioningScenarioBuilder,
    db_session: AsyncSession,
) -> None:
    # Arrange — a legacy-style detached table carrying no orphan marker
    legacy_name = f"{partition_builder._table_name}__2020_01"
    ctx = await partition_builder.with_create_ahead(1).build()
    await db_session.execute(text(f'CREATE TABLE "{legacy_name}" (LIKE "{ctx.table_name}" INCLUDING ALL)'))
    await db_session.commit()

    # Act — the unmarked table is invisible to marker-based discovery
    result = await ctx.run_maintenance(at_time="2024-06-01")

    # Assert — nothing dropped, the legacy table survives untouched
    assert result.success
    assert result.dropped_count == 0
    await ctx.assert_partition_exists(legacy_name)

    # Act — adopting stamps the orphan marker (idempotent)
    assert await ctx.repo.adopt_partition(ctx.table_name, legacy_name) is True
    assert await ctx.repo.adopt_partition(ctx.table_name, legacy_name) is True

    # Adopting an attached partition is refused; a missing name reports False
    with pytest.raises(PartitionAttachedError):
        await ctx.repo.adopt_partition(ctx.table_name, f"{ctx.table_name}__2024_06")
    assert await ctx.repo.adopt_partition(ctx.table_name, f"{ctx.table_name}__1999_01") is False

    # Act — the next run collects the adopted table like any other orphan
    result = await ctx.run_maintenance(at_time="2024-06-01")

    # Assert
    assert result.success
    assert result.dropped_count >= 1
    await ctx.assert_partition_not_exists(legacy_name)
    await ctx.assert_partition_attached(f"{ctx.table_name}__2024_06")


@pytest.mark.integration
async def test__scenario__is_partition_closed__past_and_current_periods(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange — a long-closed partition, one still open (upper bound in the
    # future on the server clock, which is_partition_closed compares against),
    # and a detached table (attach + detach leaves the orphan marker)
    table = partition_builder._table_name
    past_name = f"{table}__2024_02"
    open_name = f"{table}__2099_01"
    detached_name = f"{table}__2020_01"
    ctx = await (
        partition_builder.with_attached_partition(past_name, "2024-02-01", "2024-03-01")
        .with_attached_partition(open_name, "2099-01-01", "2099-02-01")
        .with_detached_partition(detached_name, "2020-01-01", "2020-02-01")
        .build()
    )

    # Assert — the past partition's upper bound has passed
    assert await ctx.metadata.is_partition_closed(f"public.{past_name}") is True

    # An upper bound still in the future — not closed
    assert await ctx.metadata.is_partition_closed(f"public.{open_name}") is False

    # A large settle buffer keeps even a long-past partition open
    assert await ctx.metadata.is_partition_closed(f"public.{past_name}", settle_seconds=10**9) is False

    # Detached tables are never reported as closed
    assert await ctx.metadata.is_partition_closed(f"public.{detached_name}") is False


@pytest.mark.integration
async def test__scenario__maintain_continue_on_error__create_failure_still_prunes(
    partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange — a manually attached wider partition covers June, so the create
    # step's ATTACH of {table}__2024_06 must fail with a partition-overlap error
    table = partition_builder._table_name
    old_name = f"{table}__2024_01"
    wide_name = f"{table}__wide"
    ctx = await (
        partition_builder.with_attached_partition(old_name, "2024-01-01", "2024-02-01")
        .with_attached_partition(wide_name, "2024-06-01", "2024-08-01")
        .with_create_ahead(1)
        .with_retention(1)
        .build()
    )

    # Act / Assert — by default the create failure aborts the run: nothing pruned
    with pytest.raises(SQLAlchemyError):
        await ctx.run_maintenance(at_time="2024-06-15")
    await ctx.assert_partition_attached(old_name)

    # Act — with continue_on_error the failure is isolated and pruning still runs
    result = await ctx.run_maintenance(at_time="2024-06-15", continue_on_error=True)

    # Assert — non-fatal issue recorded for the create step, prune completed
    assert result.success
    assert result.error is None
    assert result.created_count == 0
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.CREATE]
    assert "overlap" in result.issues[0].error.lower()
    assert result.detached_count == 1
    assert result.dropped_count >= 1
    await ctx.assert_partition_not_exists(old_name)
    await ctx.assert_partition_attached(wide_name)
