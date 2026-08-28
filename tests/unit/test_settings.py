from datetime import UTC
from zoneinfo import ZoneInfo

import pytest
from pydantic_settings import SettingsConfigDict

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


def test__settings__get_period_calculator__custom_tz__forwards_tz_to_calculator() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.MONTH)
    moscow = ZoneInfo("Europe/Moscow")

    # Act
    calc = settings.get_period_calculator(tz=moscow)

    # Assert
    assert calc.tz is moscow


def test__settings__get_period_calculator__no_tz_argument__defaults_to_utc() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.MONTH)

    # Act
    calc = settings.get_period_calculator()

    # Assert
    assert calc.tz is UTC


def test__settings__subpartition_from_env_json__reaches_the_config() -> None:
    # Arrange
    class NestedSettings(PartitionTableSettings):
        model_config = SettingsConfigDict(env_prefix="NESTED_")

    settings = NestedSettings(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.WEEK,
        subpartition={"strategy": "hash", "column": "tenant_id", "modulus": 4},
    )

    # Act
    config = settings.to_config()

    # Assert
    assert config.subpartition is not None
    assert config.subpartition.column == "tenant_id"
    assert config.subpartition.modulus == 4


def test__settings__without_subpartition__config_stays_flat() -> None:
    # Arrange
    class FlatSettings(PartitionTableSettings):
        model_config = SettingsConfigDict(env_prefix="FLAT_")

    settings = FlatSettings(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )

    # Act / Assert
    assert settings.to_config().subpartition is None
