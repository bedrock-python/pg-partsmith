import json
from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

import pg_partsmith
from pg_partsmith.entities import (
    HashSubpartitionSpec,
    ListBounds,
    ListGroup,
    ListSubpartitionSpec,
    MaintenanceIssue,
    MaintenanceIssueStep,
    MaintenanceResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    Period,
    RangeBounds,
    TablePartitionConfig,
)
from pg_partsmith.partition_bounds import parse_partition_bounds

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


# ── Composite partition keys ────────────────────────────────────────────────────


def test__config__composite_partition_key__exposes_columns_and_arity() -> None:
    # Arrange / Act
    config = _config(trailing_partition_columns=("tenant_id",))

    # Assert
    assert config.partition_columns == ("created_at", "tenant_id")
    assert config.partition_column == "created_at"
    assert config.key_arity == 2


def test__config__single_partition_column__still_accepted_and_normalised() -> None:
    # Arrange / Act
    config = _config()

    # Assert: the historical spelling keeps working.
    assert config.partition_columns == ("created_at",)
    assert config.partition_column == "created_at"
    assert config.key_arity == 1


def test__config__repeated_partition_key_column__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="distinct"):
        _config(trailing_partition_columns=("created_at",))


def test__config__partition_column_is_a_real_field__so_it_type_checks_and_copies() -> None:
    # Arrange
    config = _config()

    # Act
    moved = config.model_copy(update={"partition_column": "occurred_at"})

    # Assert: a derived value would have silently ignored the update and left
    # `dict(model)` disagreeing with `model_dump()`.
    assert moved.partition_column == "occurred_at"
    assert dict(moved)["partition_column"] == "occurred_at"
    assert moved.model_dump()["partition_column"] == "occurred_at"
    assert "partition_column" in TablePartitionConfig.model_fields


def test__config__composite_key_overlapping_a_subpartition_column__rejected() -> None:
    # Arrange / Act / Assert: the lower level would have nothing left to divide.
    with pytest.raises(ValidationError, match="distinct across levels"):
        _config(
            trailing_partition_columns=("tenant_id",),
            subpartition=HashSubpartitionSpec(column="tenant_id", modulus=2),
        )


def test__config__composite_list_root__rejected() -> None:
    # Arrange / Act / Assert: PostgreSQL has no composite LIST key.
    with pytest.raises(ValidationError, match="exactly one column"):
        TablePartitionConfig(
            table_name="events",
            partition_type=PartitionType.LIST,
            partition_strategy=PartitionStrategy.VALUE_BASED,
            partition_column="region",
            trailing_partition_columns=("tier",),
            root_layout=ListSubpartitionSpec(column="region", groups=(ListGroup(name="eu", values=("de",)),)),
        )


def test__hash_spec__composite_columns__accepted() -> None:
    # Arrange / Act
    spec = HashSubpartitionSpec(column="tenant_id", trailing_columns=("shard_id",), modulus=4)

    # Assert
    assert spec.columns == ("tenant_id", "shard_id")


def test__hash_spec__composite_columns__leading_column_still_readable() -> None:
    # Arrange
    spec = HashSubpartitionSpec(column="tenant_id", trailing_columns=("shard_id",), modulus=4)

    # Act / Assert: reading it must never raise — validation and diagnostics do
    # exactly this, and `hasattr` only swallows AttributeError.
    assert spec.column == "tenant_id"
    assert hasattr(spec, "column")
    assert spec.model_copy(update={"column": "account_id"}).column == "account_id"


def test__hash_spec__repeated_key_column__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="distinct"):
        HashSubpartitionSpec(column="tenant_id", trailing_columns=("tenant_id",), modulus=4)


def test__list_spec__composite_columns__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="exactly one column"):
        ListSubpartitionSpec(
            column="region", trailing_columns=("tier",), groups=(ListGroup(name="eu", values=("de",)),)
        )


def test__parse_partition_bounds__composite_range__returns_the_leading_value() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds(
        "FOR VALUES FROM ('2024-01-01 00:00:00+00', MINVALUE) TO ('2024-02-01 00:00:00+00', MINVALUE)"
    )

    # Assert: trailing columns are MINVALUE at both ends, so the leading value
    # is what the partition actually selects on.
    assert parsed == RangeBounds(from_value="2024-01-01 00:00:00+00", to_value="2024-02-01 00:00:00+00")


def test__parse_partition_bounds__composite_numeric_range__returns_the_leading_value() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds("FOR VALUES FROM (100, MINVALUE) TO (200, MINVALUE)")

    # Assert
    assert parsed == RangeBounds(from_value="100", to_value="200")


# ── Static-root configuration refusals ──────────────────────────────────────────


def _static(**overrides: object) -> TablePartitionConfig:
    base: dict[str, object] = {
        "table_name": "issue_index",
        "partition_type": PartitionType.HASH,
        "partition_strategy": PartitionStrategy.HASH_BASED,
        "partition_column": "tenant_id",
        "root_layout": HashSubpartitionSpec(column="tenant_id", modulus=4),
    }
    base.update(overrides)
    return TablePartitionConfig(**base)  # type: ignore[arg-type]


def test__config__static_root__accepted() -> None:
    # Arrange / Act
    config = _static()

    # Assert
    assert config.is_time_based is False
    assert config.key_arity == 1


def test__config__time_based_with_root_layout__rejected() -> None:
    # Arrange / Act / Assert: a time-based table's partitions come from periods.
    with pytest.raises(ValidationError, match="only for HASH_BASED"):
        _config(root_layout=HashSubpartitionSpec(column="tenant_id", modulus=2))


def test__config__static_root__partition_type_disagreeing_with_strategy__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="requires HASH partition type"):
        _static(partition_type=PartitionType.RANGE)


def test__config__static_root__layout_of_the_wrong_strategy__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="root_layout describes"):
        TablePartitionConfig(
            table_name="regions",
            partition_type=PartitionType.LIST,
            partition_strategy=PartitionStrategy.VALUE_BASED,
            partition_column="region",
            root_layout=HashSubpartitionSpec(column="region", modulus=2),
        )


def test__config__static_root__layout_on_another_column__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="must be the table's own partition key"):
        _static(root_layout=HashSubpartitionSpec(column="other_col", modulus=2))


def test__config__static_root__with_granularity__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="no periods"):
        _static(granularity=PartitionGranularity.WEEK)


def test__config__static_root__with_a_sibling_subpartition__rejected() -> None:
    # Arrange / Act / Assert: deeper levels nest inside root_layout instead.
    with pytest.raises(ValidationError, match="inside root_layout"):
        _static(subpartition=HashSubpartitionSpec(column="shard_id", modulus=2))


def test__config__static_root__name_too_long_for_its_buckets__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="too long for this layout"):
        _static(
            table_name="i" * 60,
            root_layout=HashSubpartitionSpec(column="tenant_id", modulus=100),
        )


def test__config__list_root__accepted() -> None:
    # Arrange / Act
    config = TablePartitionConfig(
        table_name="regions",
        partition_type=PartitionType.LIST,
        partition_strategy=PartitionStrategy.VALUE_BASED,
        partition_column="region",
        root_layout=ListSubpartitionSpec(column="region", groups=(ListGroup(name="eu", values=("de",)),)),
    )

    # Assert
    assert config.is_time_based is False


# ── Bound parsing edge cases ────────────────────────────────────────────────────


def test__parse_partition_bounds__quoted_comma_in_a_composite_bound__not_split() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds("FOR VALUES FROM ('a,b', MINVALUE) TO ('c,d', MINVALUE)")

    # Assert
    assert parsed == RangeBounds(from_value="a,b", to_value="c,d")


def test__parse_partition_bounds__doubled_quote_inside_a_value__preserved() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds("FOR VALUES IN ('O''Brien', 'other')")

    # Assert
    assert parsed == ListBounds(values=("O'Brien", "other"))


def test__parse_partition_bounds__hash_bounds_with_an_unreadable_number__returns_none() -> None:
    # Arrange / Act / Assert
    assert parse_partition_bounds("FOR VALUES WITH (modulus x, remainder y)") is None


# ── Serialization compatibility ─────────────────────────────────────────────────


def test__config__dump__still_carries_partition_column() -> None:
    # Arrange
    config = _config()

    # Act
    dumped = config.model_dump()

    # Assert: the dump is 0.4.0's shape plus one new key, so a consumer written
    # against the old one still finds what it reads.
    assert dumped["partition_column"] == "created_at"
    assert dumped["trailing_partition_columns"] == ()
    assert "partition_columns" not in dumped


def test__config__dump__round_trips() -> None:
    # Arrange
    config = _config()

    # Act / Assert: the dump carries both spellings, and reading it back is a no-op.
    assert TablePartitionConfig(**config.model_dump()) == config


def test__config__json_dump__round_trips() -> None:
    # Arrange
    config = _config()

    # Act / Assert
    assert TablePartitionConfig(**json.loads(config.model_dump_json())) == config


def test__config__composite_dump__round_trips_without_collapsing_the_key() -> None:
    # Arrange: the dump names both the whole key and its leading column.
    config = _config(trailing_partition_columns=("tenant_id",))

    # Act
    restored = TablePartitionConfig(**config.model_dump())

    # Assert: the explicit key wins over the derived single column.
    assert restored.partition_columns == ("created_at", "tenant_id")


def test__config__dump_from_before_composite_keys__still_parses() -> None:
    # Arrange: exactly what 0.4.0 would have written.
    legacy = {
        "schema_name": None,
        "table_name": "events",
        "partition_type": "range",
        "partition_strategy": "time_based",
        "partition_column": "created_at",
        "granularity": "month",
        "create_ahead_count": 6,
        "retention_count": 12,
        "auto_attach_after_create": True,
    }

    # Act / Assert
    assert TablePartitionConfig(**legacy).partition_columns == ("created_at",)


def test__parse_partition_bounds__list_value_naming_modulus__stays_a_list_bound() -> None:
    # Arrange / Act: an unanchored search would read this as HashBounds, and a
    # partition whose bounds are misread is invisible to the planner -- which
    # then plans a duplicate that PostgreSQL refuses on every run.
    parsed = parse_partition_bounds("FOR VALUES IN ('modulus 4, remainder 1')")

    # Assert
    assert parsed == ListBounds(values=("modulus 4, remainder 1",))


def test__parse_partition_bounds__range_literal_naming_modulus__stays_a_range_bound() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds("FOR VALUES FROM ('modulus 2, remainder 0') TO ('z')")

    # Assert
    assert parsed == RangeBounds(from_value="modulus 2, remainder 0", to_value="z")


# ── PartitionInfo bound derivation ──────────────────────────────────────────────


def test__partition_info__validation__leaves_the_callers_dict_untouched() -> None:
    # Arrange -- a dict the caller intends to reuse for a second partition.
    payload = {
        "name": "events__2024_01",
        "partition_type": PartitionType.RANGE,
        "from_value": "2024-01-01",
        "to_value": "2024-02-01",
    }
    original = dict(payload)

    # Act
    PartitionInfo.model_validate(payload)

    # Assert -- validating must not write a derived field back into the input.
    assert payload == original


def test__partition_info__round_trip_through_model_dump__keeps_both_spellings() -> None:
    # Arrange
    info = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        bounds=RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
    )

    # Act -- model_dump renders the bound as a plain dict, and the pair has to
    # be derivable from that shape too or a dump-and-reload loses it.
    reloaded = PartitionInfo.model_validate(info.model_dump())

    # Assert
    assert reloaded == info
    assert reloaded.from_value == "2024-01-01"
    assert reloaded.to_value == "2024-02-01"


def test__parse_partition_bounds__null_keyword__is_not_the_string_null() -> None:
    # Arrange / Act
    keyword = parse_partition_bounds("FOR VALUES IN (NULL)")
    literal = parse_partition_bounds("FOR VALUES IN ('NULL')")

    # Assert -- reading them as the same bound would make the planner propose a
    # partition PostgreSQL already has, and fail on the conflict every run.
    assert keyword == ListBounds(values=(), includes_null=True)
    assert literal == ListBounds(values=("NULL",))
    assert keyword != literal


def test__parse_partition_bounds__null_alongside_values__keeps_both() -> None:
    # Arrange / Act
    parsed = parse_partition_bounds("FOR VALUES IN ('eu', NULL, 'us')")

    # Assert
    assert parsed == ListBounds(values=("eu", "us"), includes_null=True)


def test__parse_partition_bounds__cast_null__is_still_the_keyword() -> None:
    # Arrange / Act -- older servers render the element with its type cast.
    parsed = parse_partition_bounds("FOR VALUES IN (NULL::text)")

    # Assert
    assert parsed == ListBounds(values=(), includes_null=True)
