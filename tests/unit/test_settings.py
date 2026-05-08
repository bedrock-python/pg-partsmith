import pytest

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.settings import PartitionTableSettings
from pg_partsmith.strategies import MonthPeriodCalculator, WeekPeriodCalculator


def _make_settings(**overrides: object) -> PartitionTableSettings:
    defaults: dict[str, object] = {
        "table_name": "events",
        "partition_type": PartitionType.RANGE,
        "partition_strategy": PartitionStrategy.TIME_BASED,
        "partition_column": "created_at",
        "granularity": PartitionGranularity.MONTH,
    }
    defaults.update(overrides)
    return PartitionTableSettings(**defaults)  # type: ignore[arg-type]


# ── to_config ───────────────────────────────────────────────────────────────────


def test__settings__to_config__all_fields__maps_to_table_partition_config() -> None:
    # Arrange
    settings = _make_settings(
        schema_name="analytics",
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=3,
        retention_count=6,
        auto_attach_after_create=False,
    )

    # Act
    config = settings.to_config()

    # Assert
    assert isinstance(config, TablePartitionConfig)
    assert config.db_schema == "analytics"
    assert config.table_name == "events"
    assert config.partition_type == PartitionType.RANGE
    assert config.partition_strategy == PartitionStrategy.TIME_BASED
    assert config.partition_column == "created_at"
    assert config.granularity == PartitionGranularity.MONTH
    assert config.create_ahead_count == 3
    assert config.retention_count == 6
    assert config.auto_attach_after_create is False


def test__settings__to_config__no_schema__passes_none() -> None:
    # Arrange
    settings = _make_settings(schema_name=None)

    # Act
    config = settings.to_config()

    # Assert
    assert config.db_schema is None


# ── get_period_calculator ───────────────────────────────────────────────────────


def test__settings__get_period_calculator__month__returns_month_calculator() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.MONTH)

    # Act
    calc = settings.get_period_calculator()

    # Assert
    assert isinstance(calc, MonthPeriodCalculator)


def test__settings__get_period_calculator__week__returns_week_calculator() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.WEEK)

    # Act
    calc = settings.get_period_calculator()

    # Assert
    assert isinstance(calc, WeekPeriodCalculator)


def test__settings__get_period_calculator__granularity_none__raises_value_error() -> None:
    # Arrange
    settings = _make_settings(granularity=None)

    # Act / Assert
    with pytest.raises(ValueError, match="granularity is not set"):
        settings.get_period_calculator()
