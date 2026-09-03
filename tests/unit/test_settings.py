"""PartitionTableSettings: env-driven configuration mapped onto TablePartitionConfig."""

from datetime import UTC, timedelta
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict

from pg_partsmith.boundaries import UUIDv7BoundaryCodec
from pg_partsmith.entities import PartitionGranularity, PartitionStrategy, PartitionType, TablePartitionConfig
from pg_partsmith.leaves import ForeignLeaves, LocalLeaves
from pg_partsmith.lifecycle import CreateAhead, DropNever, KeepFor
from pg_partsmith.scheme import HashPartitioning, RangePartitioning
from pg_partsmith.settings import PartitionTableSettings
from pg_partsmith.strategies import HourPeriodCalculator, MonthPeriodCalculator, WeekPeriodCalculator

_MOSCOW = ZoneInfo("Europe/Moscow")


class _Settings(PartitionTableSettings):
    """A prefix no real environment sets, so the process environment cannot leak in."""

    model_config = SettingsConfigDict(env_prefix="PGPS_UNIT_TEST_")


def _make_settings(**overrides: object) -> PartitionTableSettings:
    defaults: dict[str, object] = {
        "table_name": "events",
        "partition_column": "created_at",
        "granularity": PartitionGranularity.MONTH,
    }
    defaults.update(overrides)
    return _Settings(**defaults)  # type: ignore[arg-type]


# -- to_config: flat fields ------------------------------------------------------------------


def test__settings__to_config__flat_fields__map_onto_the_config() -> None:
    # Arrange
    settings = _make_settings(
        schema_name="analytics",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        trailing_partition_columns=("tenant_id",),
        tz="Europe/Moscow",
        boundary_codec="uuidv7",
        create_ahead_count=3,
        retention_count=6,
    )

    # Act
    config = settings.to_config()

    # Assert
    assert isinstance(config, TablePartitionConfig)
    assert config.db_schema == "analytics"
    assert config.table_name == "events"
    assert config.partition_type is PartitionType.RANGE
    assert config.partition_strategy is PartitionStrategy.TIME_BASED
    assert config.partition_columns == ("created_at", "tenant_id")
    assert config.granularity is PartitionGranularity.MONTH
    assert config.time_boundaries is not None
    assert config.time_boundaries.tz is _MOSCOW
    assert config.time_boundaries.codec == UUIDv7BoundaryCodec()
    assert config.create_ahead_count == 3
    assert config.retention_count == 6


def test__settings__to_config__defaults__utc_no_codec_six_ahead_twelve_kept() -> None:
    # Arrange / Act
    config = _make_settings().to_config()

    # Assert
    assert config.db_schema is None
    assert config.time_boundaries is not None
    assert config.time_boundaries.tz is UTC
    assert config.time_boundaries.codec is None
    assert config.create_ahead_count == 6
    assert config.retention_count == 12
    assert config.subpartition is None


def test__settings__to_config__without_granularity__rejected_by_the_config() -> None:
    # Arrange
    settings = _make_settings(granularity=None)

    # Act / Assert
    with pytest.raises(ValidationError, match="requires granularity"):
        settings.to_config()


def test__settings__to_config__declared_type_disagreeing_with_the_scheme__rejected() -> None:
    # Arrange
    settings = _make_settings(partition_type=PartitionType.HASH)

    # Act / Assert
    with pytest.raises(ValidationError, match="does not match the scheme's root"):
        settings.to_config()


def test__settings__to_config__declared_strategy_disagreeing_with_the_scheme__rejected() -> None:
    # Arrange
    settings = _make_settings(partition_strategy=PartitionStrategy.HASH_BASED)

    # Act / Assert
    with pytest.raises(ValidationError, match="scheme="):
        settings.to_config()


def test__settings__unknown_granularity__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="granularity"):
        _make_settings(granularity="fortnight")


@pytest.mark.parametrize("field", ["create_ahead_count", "retention_count"])
def test__settings__non_positive_count__rejected(field: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match=field):
        _make_settings(**{field: 0})


# -- to_config: scheme and lifecycle JSON ---------------------------------------------------


def test__settings__to_config__scheme__takes_precedence_over_the_flat_fields() -> None:
    # Arrange -- the flat fields describe a monthly table; the scheme says daily with a hash level
    settings = _make_settings(
        scheme={
            "method": "range",
            "key": "occurred_at",
            "boundaries": {"kind": "time", "granularity": "day"},
            "child": {"method": "hash", "key": "tenant_id", "modulus": 4},
        }
    )

    # Act
    config = settings.to_config()

    # Assert
    assert isinstance(config.scheme, RangePartitioning)
    assert config.partition_column == "occurred_at"
    assert config.granularity is PartitionGranularity.DAY
    assert isinstance(config.subpartition, HashPartitioning)
    assert config.subpartition.modulus == 4


def test__settings__to_config__scheme_without_flat_fields__builds_a_static_root() -> None:
    # Arrange
    settings = _Settings(
        table_name="tasks",
        partition_type=PartitionType.HASH,
        partition_strategy=PartitionStrategy.HASH_BASED,
        scheme={"method": "hash", "key": "task_id", "modulus": 16},
    )

    # Act
    config = settings.to_config()

    # Assert
    assert isinstance(config.scheme, HashPartitioning)
    assert config.scheme.modulus == 16
    assert config.is_time_based is False


def test__settings__to_config__lifecycle__takes_precedence_over_the_counts() -> None:
    # Arrange
    settings = _make_settings(
        create_ahead_count=9,
        retention_count=9,
        lifecycle={
            "creation": {"kind": "create_ahead", "count": 2},
            "retention": {"kind": "keep_for", "age": "P30D"},
            "drop": {"kind": "drop_never"},
        },
    )

    # Act
    config = settings.to_config()

    # Assert
    assert config.lifecycle.creation == CreateAhead(count=2)
    assert config.lifecycle.retention == KeepFor(age=timedelta(days=30))
    assert config.lifecycle.drop == DropNever()
    assert config.create_ahead_count == 2
    assert config.retention_count is None


def test__settings__to_config__declared_type_checked_against_the_scheme_json() -> None:
    # Arrange
    settings = _Settings(
        table_name="tasks",
        partition_type=PartitionType.RANGE,
        scheme={"method": "hash", "key": "task_id", "modulus": 2},
    )

    # Act / Assert
    with pytest.raises(ValidationError, match="does not match the scheme's root, which is HASH"):
        settings.to_config()


# -- get_period_calculator -------------------------------------------------------------------


def test__settings__get_period_calculator__month__returns_month_calculator() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.MONTH)

    # Act / Assert
    assert isinstance(settings.get_period_calculator(), MonthPeriodCalculator)


def test__settings__get_period_calculator__week__returns_week_calculator() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.WEEK)

    # Act / Assert
    assert isinstance(settings.get_period_calculator(), WeekPeriodCalculator)


def test__settings__get_period_calculator__granularity_none__raises_value_error() -> None:
    # Arrange
    settings = _make_settings(granularity=None)

    # Act / Assert
    with pytest.raises(ValueError, match="granularity is not set"):
        settings.get_period_calculator()


def test__settings__get_period_calculator__custom_tz__forwards_tz_to_calculator() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.MONTH)

    # Act
    calc = settings.get_period_calculator(tz=_MOSCOW)

    # Assert
    assert calc.tz is _MOSCOW


def test__settings__get_period_calculator__no_tz_argument__defaults_to_utc() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.MONTH)

    # Act / Assert
    assert settings.get_period_calculator().tz is UTC


def test__settings__get_period_calculator__hour_in_a_local_zone__raises_value_error() -> None:
    # Arrange
    settings = _make_settings(granularity=PartitionGranularity.HOUR)

    # Act / Assert
    assert isinstance(settings.get_period_calculator(), HourPeriodCalculator)
    with pytest.raises(ValueError, match="only UTC"):
        settings.get_period_calculator(tz=_MOSCOW)


# -- loading from the environment ----------------------------------------------------------------


def test__settings__from_environment__reads_every_flat_field(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange -- env loading is the whole reason this class exists
    class OutboxSettings(PartitionTableSettings):
        model_config = SettingsConfigDict(env_prefix="OUTBOX_")

    monkeypatch.setenv("OUTBOX_SCHEMA_NAME", "analytics")
    monkeypatch.setenv("OUTBOX_TABLE_NAME", "outbox")
    monkeypatch.setenv("OUTBOX_PARTITION_TYPE", "range")
    monkeypatch.setenv("OUTBOX_PARTITION_STRATEGY", "time_based")
    monkeypatch.setenv("OUTBOX_PARTITION_COLUMN", "created_at")
    monkeypatch.setenv("OUTBOX_TRAILING_PARTITION_COLUMNS", '["tenant_id"]')
    monkeypatch.setenv("OUTBOX_GRANULARITY", "week")
    monkeypatch.setenv("OUTBOX_TZ", "Europe/Moscow")
    monkeypatch.setenv("OUTBOX_BOUNDARY_CODEC", "epoch_milliseconds")
    monkeypatch.setenv("OUTBOX_CREATE_AHEAD_COUNT", "3")
    monkeypatch.setenv("OUTBOX_RETENTION_COUNT", "4")

    # Act
    config = OutboxSettings().to_config()

    # Assert
    assert config.qualified_name == "analytics.outbox"
    assert config.partition_columns == ("created_at", "tenant_id")
    assert config.granularity is PartitionGranularity.WEEK
    assert config.time_boundaries is not None
    assert config.time_boundaries.timezone_name == "Europe/Moscow"
    assert config.time_boundaries.model_dump()["codec"] == "epoch_milliseconds"
    assert config.create_ahead_count == 3
    assert config.retention_count == 4


def test__settings__from_environment__scheme_and_lifecycle_json__reach_the_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    class TenantSettings(PartitionTableSettings):
        model_config = SettingsConfigDict(env_prefix="TENANT_")

    monkeypatch.setenv("TENANT_TABLE_NAME", "events")
    monkeypatch.setenv("TENANT_PARTITION_STRATEGY", "time_based")
    monkeypatch.setenv(
        "TENANT_SCHEME",
        '{"method": "range", "key": "created_at", "boundaries": {"kind": "time", "granularity": "week"}, '
        '"child": {"method": "hash", "key": "tenant_id", "modulus": 4}}',
    )
    monkeypatch.setenv(
        "TENANT_LIFECYCLE",
        '{"creation": {"kind": "create_ahead", "count": 3}, "retention": {"kind": "keep_newest", "count": 8}}',
    )

    # Act
    config = TenantSettings().to_config()

    # Assert
    assert config.granularity is PartitionGranularity.WEEK
    assert isinstance(config.subpartition, HashPartitioning)
    assert config.subpartition.modulus == 4
    assert config.create_ahead_count == 3
    assert config.retention_count == 8


def test__settings__from_environment__missing_table_name__rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    class BareSettings(PartitionTableSettings):
        model_config = SettingsConfigDict(env_prefix="BARE_")

    monkeypatch.delenv("BARE_TABLE_NAME", raising=False)

    # Act / Assert
    with pytest.raises(ValidationError, match="table_name"):
        BareSettings()


# -- to_config: leaves JSON ------------------------------------------------------------------


def test__settings__to_config__leaves__local_backend_from_json() -> None:
    # Arrange
    class Settings(PartitionTableSettings):
        model_config = SettingsConfigDict(env_prefix="LEAF_")

    settings = Settings(
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        leaves={"kind": "local", "tablespace": "fast", "storage_parameters": {"fillfactor": 70}},
    )

    # Act
    config = settings.to_config()

    # Assert
    assert config.leaves == LocalLeaves(tablespace="fast", storage_parameters={"fillfactor": 70})


def test__settings__to_config__leaves__foreign_backend_from_json() -> None:
    # Arrange
    settings = PartitionTableSettings(
        table_name="metrics",
        partition_column="ts",
        granularity=PartitionGranularity.MONTH,
        leaves={"kind": "foreign", "server": "archive", "options": {"table_name": "{relname}"}},
    )

    # Act / Assert
    assert settings.to_config().leaves == ForeignLeaves(server="archive", options={"table_name": "{relname}"})


def test__settings__to_config__without_leaves__plain_local_tables() -> None:
    settings = PartitionTableSettings(table_name="events", partition_column="created_at", granularity="month")

    assert settings.to_config().leaves == LocalLeaves()


def test__settings__used_as_they_are__read_the_package_prefix_and_not_the_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: a host's TZ is a fact about the machine, not about the calendar
    monkeypatch.setenv("TZ", "Pacific/Kiritimati")
    monkeypatch.setenv("PG_PARTSMITH_TABLE_NAME", "events")
    monkeypatch.setenv("PG_PARTSMITH_PARTITION_COLUMN", "created_at")
    monkeypatch.setenv("PG_PARTSMITH_GRANULARITY", "month")

    # Act
    settings = PartitionTableSettings()

    # Assert
    assert settings.table_name == "events"
    assert str(settings.tz) == "UTC"
