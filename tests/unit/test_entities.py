from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

import pg_partsmith
from pg_partsmith.entities import (
    HashSubpartitionSpec,
    MaintenanceIssue,
    MaintenanceIssueStep,
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


def test__period__year_month_day_hour__sets_hour() -> None:
    # Arrange / Act
    p = Period(year=2024, month=3, day=15, hour=7)

    # Assert
    assert p.hour == 7


def test__period__year_and_quarter__sets_quarter() -> None:
    # Arrange / Act
    p = Period(year=2024, quarter=2)

    # Assert
    assert p.quarter == 2


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"year": 2024, "month": 13}, "(?i)month"),
        ({"year": 2024, "month": 0}, "(?i)month"),
        ({"year": 2024, "month": 1, "day": 32}, "(?i)(day|date)"),
        ({"year": 2024, "month": 1, "day": 0}, "(?i)(day|date)"),
        ({"year": 2024, "week": 54}, "(?i)week"),
        ({"year": 2024, "week": 0}, "(?i)week"),
        ({"year": 2024, "month": 1, "day": 1, "hour": 24}, "(?i)hour"),
        ({"year": 2024, "month": 1, "day": 1, "hour": -1}, "(?i)hour"),
        ({"year": 2024, "quarter": 5}, "(?i)quarter"),
        ({"year": 2024, "quarter": 0}, "(?i)quarter"),
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


def test__period__hour_without_day__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match=r"(?i)day"):
        Period(year=2024, month=3, hour=5)


def test__period__quarter_with_month__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match=r"(?i)quarter"):
        Period(year=2024, month=3, quarter=1)


def test__period__week_with_hour__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match=r"(?i)week"):
        Period(year=2024, week=12, hour=5)


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


def test__period__add_hours_crosses_midnight_and_month__wraps_day_and_month() -> None:
    # Arrange
    p = Period(year=2024, month=3, day=31, hour=23)

    # Act
    result = p + 1

    # Assert
    assert result.year == 2024
    assert result.month == 4
    assert result.day == 1
    assert result.hour == 0


def test__period__subtract_hours_crosses_midnight__wraps_to_previous_day() -> None:
    # Arrange
    p = Period(year=2024, month=3, day=1, hour=0)

    # Act
    result = p - 1

    # Assert — 2024 is a leap year, so 1 March minus one hour lands on 29 February
    assert result.year == 2024
    assert result.month == 2
    assert result.day == 29
    assert result.hour == 23


def test__period__add_quarters_crosses_year_boundary__wraps_year() -> None:
    # Arrange
    p = Period(year=2024, quarter=4)

    # Act
    result = p + 2

    # Assert
    assert result.year == 2025
    assert result.quarter == 2


def test__period__subtract_quarters_crosses_year_boundary__wraps_to_previous_year() -> None:
    # Arrange
    p = Period(year=2024, quarter=1)

    # Act
    result = p - 1

    # Assert
    assert result.year == 2023
    assert result.quarter == 4


def test__period__same_granularity_comparison__orders_correctly() -> None:
    # Arrange
    p1 = Period(year=2024, month=1)
    p2 = Period(year=2024, month=3)

    # Assert
    assert p1 < p2
    assert p2 > p1
    assert p1 <= p1
    assert p1 >= p1


def test__period__hour_comparison__orders_within_same_day() -> None:
    # Arrange
    p1 = Period(year=2024, month=3, day=15, hour=7)
    p2 = Period(year=2024, month=3, day=15, hour=9)

    # Assert
    assert p1 < p2
    assert p2 > p1


def test__period__quarter_comparison__orders_correctly() -> None:
    # Arrange
    p1 = Period(year=2024, quarter=1)
    p2 = Period(year=2024, quarter=3)

    # Assert
    assert p1 < p2
    assert p2 > p1


def test__period__different_granularity_comparison__raises_type_error() -> None:
    # Arrange
    p_month = Period(year=2024, month=1)
    p_week = Period(year=2024, week=1)

    # Act / Assert
    with pytest.raises(TypeError):
        _ = p_month < p_week
    with pytest.raises(TypeError):
        _ = p_week > p_month


def test__period__hour_vs_day_comparison__raises_type_error() -> None:
    # Arrange
    p_hour = Period(year=2024, month=3, day=15, hour=7)
    p_day = Period(year=2024, month=3, day=15)

    # Act / Assert
    with pytest.raises(TypeError):
        _ = p_hour < p_day
    with pytest.raises(TypeError):
        _ = p_day > p_hour


def test__period__quarter_vs_month_comparison__raises_type_error() -> None:
    # Arrange
    p_quarter = Period(year=2024, quarter=1)
    p_month = Period(year=2024, month=1)

    # Act / Assert
    with pytest.raises(TypeError):
        _ = p_quarter < p_month
    with pytest.raises(TypeError):
        _ = p_month > p_quarter


def test__period__lt_with_non_period__returns_not_implemented() -> None:
    # Arrange
    p = Period(year=2024, month=1)

    # Act / Assert
    assert p.__lt__(1) == NotImplemented  # type: ignore[operator]
    assert p.__lt__(Period(year=2024, week=1)) == NotImplemented
    assert p.__lt__(Period(year=2024, quarter=1)) == NotImplemented
    assert p.__lt__(Period(year=2024, month=1, day=1, hour=0)) == NotImplemented


@pytest.mark.parametrize(
    "period,expected_str",
    [
        (Period(year=2024), "2024"),
        (Period(year=2024, month=3), "2024_03"),
        (Period(year=2024, month=3, day=5), "2024_03_05"),
        (Period(year=2024, month=3, day=5, hour=7), "2024_03_05_07"),
        (Period(year=2024, week=7), "2024_w07"),
        (Period(year=2024, quarter=2), "2024_q2"),
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


def test__period__to_date__returns_first_day_of_quarter() -> None:
    # Arrange / Act / Assert
    assert Period(year=2024, quarter=3).to_date() == date(2024, 7, 1)


@pytest.mark.parametrize(
    "period,expected",
    [
        (Period(year=2024, month=3, day=15, hour=7), datetime(2024, 3, 15, 7, tzinfo=UTC)),
        (Period(year=2024, month=3, day=15), datetime(2024, 3, 15, tzinfo=UTC)),
        (Period(year=2024, month=3), datetime(2024, 3, 1, tzinfo=UTC)),
        (Period(year=2024, week=12), datetime(2024, 3, 18, tzinfo=UTC)),  # Monday of ISO week 12
        (Period(year=2024, quarter=4), datetime(2024, 10, 1, tzinfo=UTC)),
    ],
)
def test__period__to_datetime__returns_utc_start_of_period(period: Period, expected: datetime) -> None:
    # Arrange / Act / Assert
    assert period.to_datetime() == expected


# ── PartitionGranularity ────────────────────────────────────────────────────────


def test__partition_granularity__hour_and_quarter__are_members() -> None:
    # Arrange / Act / Assert
    assert PartitionGranularity.HOUR.value == "hour"
    assert PartitionGranularity.QUARTER.value == "quarter"


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


def test__partition_info__qualified_name__splits_into_schema_name_and_relname() -> None:
    # Arrange
    p = PartitionInfo(
        name="public.events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
    )

    # Act / Assert
    assert p.schema_name == "public"
    assert p.relname == "events__2024_01"


def test__partition_info__unqualified_name__schema_name_is_none_and_relname_is_full_name() -> None:
    # Arrange
    p = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
    )

    # Act / Assert
    assert p.schema_name is None
    assert p.relname == "events__2024_01"


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
def test__table_partition_config__static_strategy_without_root_layout__raises_value_error(
    strategy: PartitionStrategy, partition_type: PartitionType
) -> None:
    # Arrange / Act / Assert: a static root has no periods to derive partitions
    # from, so it has to say what it is divided into.
    with pytest.raises(ValueError, match="requires root_layout"):
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


def test__table_partition_config__table_name_too_long_for_hourly_suffix__raises_value_error() -> None:
    # "a" * 49 + "__0000_00_00_00" (15 chars) = 64 > 63
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="too long"):
        TablePartitionConfig(
            table_name="a" * 49,
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=PartitionGranularity.HOUR,
        )


def test__table_partition_config__hour_granularity_with_max_length_name__accepted() -> None:
    # "a" * 48 + "__0000_00_00_00" (15 chars) = 63
    # Arrange / Act
    cfg = TablePartitionConfig(
        table_name="a" * 48,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.HOUR,
    )

    # Assert
    assert cfg.granularity == PartitionGranularity.HOUR


def test__table_partition_config__quarter_granularity_with_max_length_name__accepted() -> None:
    # "a" * 54 + "__0000_q0" (9 chars) = 63
    # Arrange / Act
    cfg = TablePartitionConfig(
        table_name="a" * 54,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.QUARTER,
    )

    # Assert
    assert cfg.granularity == PartitionGranularity.QUARTER


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


def test__maintenance_result__no_issues__defaults_to_empty_tuple() -> None:
    # Arrange / Act
    r = MaintenanceResult()

    # Assert
    assert r.issues == ()


def test__maintenance_result__issues_without_error__success_stays_true() -> None:
    # Arrange
    issue = MaintenanceIssue(step=MaintenanceIssueStep.DETACH, error="SQLAlchemyError: detach failed")

    # Act
    r = MaintenanceResult(created_count=1, issues=(issue,))

    # Assert — non-fatal issues never flip success; only a fatal ``error`` does
    assert r.success is True
    assert r.issues == (issue,)


# ── MaintenanceIssue ────────────────────────────────────────────────────────────


def test__maintenance_issue__construction__stores_step_and_error() -> None:
    # Arrange / Act
    issue = MaintenanceIssue(step=MaintenanceIssueStep.CREATE, error="SQLAlchemyError: create failed")

    # Assert
    assert issue.step is MaintenanceIssueStep.CREATE
    assert issue.error == "SQLAlchemyError: create failed"


# ── package-root exports ────────────────────────────────────────────────────────


def test__package_root__migration_ergonomics_exports__importable_and_functional() -> None:
    # Arrange / Act / Assert
    assert pg_partsmith.MaintenanceIssue is MaintenanceIssue
    assert pg_partsmith.qualify("public", "events") == "public.events"
    assert pg_partsmith.qualify(None, "events") == "events"
    assert pg_partsmith.split_qualified_name("public.events") == ("public", "events")
    assert pg_partsmith.split_qualified_name("events") == (None, "events")


# ── Subpartitioned configuration ────────────────────────────────────────────────


def _config(**overrides: object) -> TablePartitionConfig:
    base: dict[str, object] = {
        "table_name": "events",
        "partition_type": PartitionType.RANGE,
        "partition_strategy": PartitionStrategy.TIME_BASED,
        "partition_column": "created_at",
        "granularity": PartitionGranularity.WEEK,
    }
    base.update(overrides)
    return TablePartitionConfig(**base)  # type: ignore[arg-type]


def test__config__without_subpartition__stays_none() -> None:
    # Arrange / Act
    config = _config()

    # Assert
    assert config.subpartition is None


def test__config__with_hash_subpartition__accepted() -> None:
    # Arrange / Act
    config = _config(subpartition=HashSubpartitionSpec(column="tenant_id", modulus=4))

    # Assert
    assert config.subpartition is not None
    assert config.subpartition.modulus == 4


def test__config__subpartition_on_the_root_partition_column__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="distinct across levels"):
        _config(subpartition=HashSubpartitionSpec(column="created_at", modulus=2))


def test__config__repeated_subpartition_column_across_levels__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="distinct across levels"):
        _config(
            subpartition=HashSubpartitionSpec(
                column="tenant_id",
                modulus=2,
                subpartition=HashSubpartitionSpec(column="tenant_id", modulus=2),
            )
        )


def test__config__subpartition_suffix_pushes_name_over_the_identifier_limit__rejected() -> None:
    # Arrange: 55 + len("__0000_w00") == 65 would already fail, so pick a name
    # that only overflows once the bucket suffix is added.
    table_name = "e" * 51

    # Act: no subpartition still fits (51 + 10 == 61).
    _config(table_name=table_name)

    # Assert
    with pytest.raises(ValidationError, match="subpartition suffix"):
        _config(table_name=table_name, subpartition=HashSubpartitionSpec(column="tenant_id", modulus=4))


def test__config__non_range_root_with_subpartition__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        _config(
            partition_type=PartitionType.LIST,
            subpartition=HashSubpartitionSpec(column="tenant_id", modulus=2),
        )
