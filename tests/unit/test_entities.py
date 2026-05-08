from datetime import date

import pytest

from pg_partsmith.entities import (
    MaintenanceResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    Period,
    TablePartitionConfig,
)

# ── Period ──────────────────────────────────────────────────────────────────────


def test__period__year_only__month_week_day_are_none() -> None:
    # Arrange / Act
    p = Period(year=2024)

    # Assert
    assert p.year == 2024
    assert p.month is None
    assert p.week is None
    assert p.day is None


def test__period__year_and_month__sets_month() -> None:
    # Arrange / Act
    p = Period(year=2024, month=3)

    # Assert
    assert p.month == 3


def test__period__year_month_day__sets_day() -> None:
    # Arrange / Act
    p = Period(year=2024, month=3, day=15)

    # Assert
    assert p.day == 15


def test__period__year_and_week__sets_week() -> None:
    # Arrange / Act
    p = Period(year=2024, week=12)

    # Assert
    assert p.week == 12


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"year": 2024, "month": 13}, "(?i)month"),
        ({"year": 2024, "month": 0}, "(?i)month"),
        ({"year": 2024, "month": 1, "day": 32}, "(?i)(day|date)"),
        ({"year": 2024, "month": 1, "day": 0}, "(?i)(day|date)"),
        ({"year": 2024, "week": 54}, "(?i)week"),
        ({"year": 2024, "week": 0}, "(?i)week"),
    ],
)
def test__period__out_of_range_field__raises_value_error(kwargs: dict, match: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match=match):
        Period(**kwargs)


def test__period__week_53_in_non_53_week_year__raises_value_error() -> None:
    # 2021 has no ISO week 53
    # Arrange / Act / Assert
    with pytest.raises(ValueError):
        Period(year=2021, week=53)


def test__period__add_months_crosses_year_boundary__wraps_year() -> None:
    # Arrange
    p = Period(year=2024, month=11)

    # Act
    result = p + 2

    # Assert
    assert result.year == 2025
    assert result.month == 1


def test__period__subtract_months_crosses_year_boundary__wraps_year() -> None:
    # Arrange
    p = Period(year=2024, month=3)

    # Act
    result = p - 2

    # Assert
    assert result.year == 2024
    assert result.month == 1


def test__period__add_years__increments_year() -> None:
    # Arrange
    p = Period(year=2024)

    # Act
    result = p + 1

    # Assert
    assert result.year == 2025


def test__period__same_granularity_comparison__orders_correctly() -> None:
    # Arrange
    p1 = Period(year=2024, month=1)
    p2 = Period(year=2024, month=3)

    # Assert
    assert p1 < p2
    assert p2 > p1
    assert p1 <= p1
    assert p1 >= p1


def test__period__different_granularity_comparison__raises_type_error() -> None:
    # Arrange
    p_month = Period(year=2024, month=1)
    p_week = Period(year=2024, week=1)

    # Act / Assert
    with pytest.raises(TypeError):
        _ = p_month < p_week
    with pytest.raises(TypeError):
        _ = p_week > p_month


def test__period__lt_with_non_period__returns_not_implemented() -> None:
    # Arrange
    p = Period(year=2024, month=1)

    # Act / Assert
    assert p.__lt__(1) == NotImplemented  # type: ignore[operator]
    assert p.__lt__(Period(year=2024, week=1)) == NotImplemented


@pytest.mark.parametrize(
    "period,expected_str",
    [
        (Period(year=2024), "2024"),
        (Period(year=2024, month=3), "2024_03"),
        (Period(year=2024, month=3, day=5), "2024_03_05"),
        (Period(year=2024, week=7), "2024_w07"),
    ],
)
def test__period__str__formats_correctly(period: Period, expected_str: str) -> None:
    # Arrange / Act / Assert
    assert str(period) == expected_str


def test__period__to_date__returns_correct_date_for_month() -> None:
    # Arrange / Act / Assert
    assert Period(year=2024, month=3).to_date() == date(2024, 3, 1)


def test__period__to_date__returns_correct_date_for_day() -> None:
    # Arrange / Act / Assert
    assert Period(year=2024, month=3, day=15).to_date() == date(2024, 3, 15)


# ── PartitionInfo ───────────────────────────────────────────────────────────────


def test__partition_info__range_without_boundaries__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="must have from_value"):
        PartitionInfo(name="p", partition_type=PartitionType.RANGE)


def test__partition_info__default_range_without_boundaries__allowed() -> None:
    # Arrange / Act
    p = PartitionInfo(
        name="events_default",
        partition_type=PartitionType.RANGE,
        from_value=None,
        to_value=None,
        is_attached=True,
        is_default=True,
    )

    # Assert
    assert p.is_default is True


def test__partition_info__range_with_boundaries__creates_attached_by_default() -> None:
    # Arrange / Act
    p = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
    )

    # Assert
    assert p.is_attached is True
    assert p.parent_table is None


def test__partition_info__model_copy_update__produces_new_instance() -> None:
    # Arrange
    p = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
        is_attached=False,
    )

    # Act
    p2 = p.model_copy(update={"is_attached": True})

    # Assert
    assert p2.is_attached is True


def test__partition_info__boundaries_expr__satisfies_range_constraint() -> None:
    # Arrange / Act — should not raise
    PartitionInfo(
        name="test",
        partition_type=PartitionType.RANGE,
        boundaries_expr="FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
        is_attached=True,
        parent_table="parent",
    )


# ── TablePartitionConfig ────────────────────────────────────────────────────────


def test__table_partition_config__time_based_without_granularity__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="requires granularity"):
        TablePartitionConfig(
            table_name="events",
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
        )


def test__table_partition_config__time_based_with_list_type__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="requires RANGE"):
        TablePartitionConfig(
            table_name="events",
            partition_type=PartitionType.LIST,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
        )


@pytest.mark.parametrize(
    "strategy,partition_type",
    [
        (PartitionStrategy.VALUE_BASED, PartitionType.LIST),
        (PartitionStrategy.HASH_BASED, PartitionType.HASH),
    ],
)
def test__table_partition_config__unimplemented_strategy__raises_value_error(
    strategy: PartitionStrategy, partition_type: PartitionType
) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="not yet implemented"):
        TablePartitionConfig(
            table_name="events",
            partition_type=partition_type,
            partition_strategy=strategy,
            partition_column="col",
        )


def test__table_partition_config__table_name_too_long__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="too long"):
        TablePartitionConfig(
            table_name="a" * 64,
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
        )


def test__table_partition_config__table_name_too_long_for_monthly_suffix__raises_value_error() -> None:
    # "a" * 55 + "__0000_00" (9 chars) = 64 > 63
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="too long"):
        TablePartitionConfig(
            table_name="a" * 55,
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
        )


def test__table_partition_config__mixed_case_identifiers__normalised_to_lowercase() -> None:
    # Arrange / Act
    cfg = TablePartitionConfig(
        table_name="Events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="Created_At",
        granularity=PartitionGranularity.MONTH,
    )

    # Assert
    assert cfg.table_name == "events"
    assert cfg.partition_column == "created_at"


def test__table_partition_config__valid_time_based__uses_sensible_defaults() -> None:
    # Arrange / Act
    cfg = TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )

    # Assert
    assert cfg.create_ahead_count == 6
    assert cfg.retention_count == 12
    assert cfg.auto_attach_after_create is True


# ── MaintenanceResult ───────────────────────────────────────────────────────────


def test__maintenance_result__no_error__success_is_true() -> None:
    # Arrange / Act
    r = MaintenanceResult()

    # Assert
    assert r.success is True
    assert r.error is None


def test__maintenance_result__with_error__success_is_false() -> None:
    # Arrange / Act
    r = MaintenanceResult(error="oops")

    # Assert
    assert r.success is False


def test__maintenance_result__with_counts__stores_counts_and_duration() -> None:
    # Arrange / Act
    r = MaintenanceResult(created_count=3, detached_count=2, dropped_count=1, duration_ms=100)

    # Assert
    assert r.created_count == 3
    assert r.detached_count == 2
    assert r.dropped_count == 1
    assert r.duration_ms == 100
