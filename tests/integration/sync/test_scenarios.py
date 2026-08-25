from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine, text

from pg_partsmith.entities import PartitionGranularity, PartitionInfo, TablePartitionConfig
from pg_partsmith.exceptions import PartitionAttachedError
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


@pytest.mark.integration
def test__scenario__hour_granularity__creates_ahead_and_prunes_old_hours(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = (
        sync_partition_builder.with_granularity(PartitionGranularity.HOUR)
        .with_create_ahead(3)
        .with_retention(2)
        .build()
    )

    # Act — frozen mid-hour: current hour plus 2 ahead are created
    result = ctx.run_maintenance(at_time="2026-08-25 14:30:00+00:00")

    # Assert
    assert result.success
    assert result.created_count == 3
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_14")
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_15")
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_16")

    # Act — advance to 17:30: hours 14 and 15 fall outside retention, hour 16 stays
    result = ctx.run_maintenance(at_time="2026-08-25 17:30:00+00:00")

    # Assert
    assert result.success
    assert result.dropped_count >= 2
    ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_08_25_14")
    ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_08_25_15")
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_16")
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_17")

    # Act — advance to 23:30: create-ahead crosses midnight into the next day
    result = ctx.run_maintenance(at_time="2026-08-25 23:30:00+00:00")

    # Assert
    assert result.success
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_25_23")
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_26_00")
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_08_26_01")
    ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_08_25_16")


@pytest.mark.integration
def test__scenario__quarter_granularity__creates_ahead_and_prunes_old_quarters(
    sync_partition_builder: PartitioningScenarioBuilder,
) -> None:
    # Arrange
    ctx = (
        sync_partition_builder.with_granularity(PartitionGranularity.QUARTER)
        .with_create_ahead(2)
        .with_retention(1)
        .build()
    )

    # Act — frozen in Q3 2026: current quarter plus 1 ahead are created
    result = ctx.run_maintenance(at_time="2026-08-15")

    # Assert
    assert result.success
    assert result.created_count == 2
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_q3")
    ctx.assert_partition_attached(f"{ctx.table_name}__2026_q4")

    # Act — advance to Q1 2027: 2026 quarters fall outside retention, 2027 quarters created ahead
    result = ctx.run_maintenance(at_time="2027-01-10")

    # Assert
    assert result.success
    assert result.dropped_count >= 2
    ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_q3")
    ctx.assert_partition_not_exists(f"{ctx.table_name}__2026_q4")
    ctx.assert_partition_attached(f"{ctx.table_name}__2027_q1")
    ctx.assert_partition_attached(f"{ctx.table_name}__2027_q2")


@pytest.mark.integration
def test__scenario__infinity_upper_bound__partition_never_pruned(
    sync_partition_builder: PartitioningScenarioBuilder,
    sync_db_engine: Engine,
) -> None:
    # Arrange — partition named like an ancient period but with an unbounded upper boundary
    partition_name = f"{sync_partition_builder._table_name}__1970_01"
    ctx = sync_partition_builder.with_retention(1).build()
    ctx.repo.create_partition(ctx.config, partition_name, "1970-01-01", "infinity")
    ctx.repo.attach_partition(ctx.table_name, partition_name, "1970-01-01", "infinity")

    # A current row lands in the unbounded partition
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(f'INSERT INTO "{ctx.table_name}" (created_at, data) VALUES (:dt, :data)'),  # noqa: S608
            {"dt": datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC), "data": "live"},
        )

    # Act — skip create: any new range would overlap the unbounded partition
    result = ctx.run_maintenance(at_time="2026-08-15", skip_create=True)

    # Assert — never pruned despite the ancient-looking name; the row survives
    assert result.success
    assert result.detached_count == 0
    assert result.dropped_count == 0
    ctx.assert_partition_attached(partition_name)
    with sync_db_engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{partition_name}"'))  # noqa: S608
        assert count.scalar() == 1


@pytest.mark.integration
def test__scenario__subpartitioned_child__detached_and_dropped(
    sync_partition_builder: PartitioningScenarioBuilder,
    sync_db_engine: Engine,
) -> None:
    # Arrange — child that is itself PARTITION BY RANGE, attached under an old period
    child = f"{sync_partition_builder._table_name}__2024_01"
    leaf = f"{child}_leaf"
    ctx = sync_partition_builder.with_create_ahead(1).with_retention(1).build()

    with sync_db_engine.begin() as conn:
        conn.execute(
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
        conn.execute(
            text(f"""CREATE TABLE "{leaf}" PARTITION OF "{child}" FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')""")
        )
    ctx.repo.attach_partition(ctx.table_name, child, "2024-01-01", "2024-02-01")
    ctx.assert_partition_attached(child)

    # Act — advance past retention
    result = ctx.run_maintenance(at_time="2024-04-01")

    # Assert — the partitioned child was detached and dropped along with its leaf
    assert result.success
    assert result.detached_count >= 1
    assert result.dropped_count >= 1
    ctx.assert_partition_not_exists(child)
    ctx.assert_partition_not_exists(leaf)


@pytest.mark.integration
def test__scenario__attached_partition_with_reattached_race__drop_refused(
    sync_partition_builder: PartitioningScenarioBuilder,
    sync_db_engine: Engine,
) -> None:
    # Arrange — detached via the repo (orphan marker set), then re-attached behind its back
    partition_name = f"{sync_partition_builder._table_name}__2024_01"
    ctx = sync_partition_builder.with_attached_partition(partition_name, "2024-01-01", "2024-02-01").build()

    with sync_db_engine.begin() as conn:
        conn.execute(
            text(f'INSERT INTO "{ctx.table_name}" (created_at, data) VALUES (:dt, :data)'),  # noqa: S608
            {"dt": datetime(2024, 1, 15, tzinfo=UTC), "data": "keep-me"},
        )

    ctx.repo.detach_partition(ctx.table_name, partition_name, concurrent=False)
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                f'ALTER TABLE "{ctx.table_name}" ATTACH PARTITION "{partition_name}" '
                f"FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
            )
        )

    # Act / Assert — drop must refuse: the partition is attached again despite the marker
    with pytest.raises(PartitionAttachedError):
        ctx.repo.drop_partition(partition_name)

    ctx.assert_partition_exists(partition_name)
    ctx.assert_partition_attached(partition_name)
    with sync_db_engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{partition_name}"'))  # noqa: S608
        assert count.scalar() == 1
