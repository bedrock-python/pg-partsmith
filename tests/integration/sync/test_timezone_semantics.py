"""Integration tests for deterministic TIMESTAMPTZ partition boundaries (UTC and configurable tz, sync API)."""

from __future__ import annotations

from uuid import uuid4
from zoneinfo import ZoneInfo

import freezegun
import pytest
from sqlalchemy import Engine, create_engine, text
from testcontainers.postgres import PostgresContainer

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    Period,
    TablePartitionConfig,
)
from pg_partsmith.strategies import MonthPeriodCalculator
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.sync.service import PartitionLifecycleService
from pg_partsmith.utils import qualify


def _create_sync_engine(postgres_container: PostgresContainer, session_timezone: str | None = None) -> Engine:
    """Create a sync engine, optionally pinning the default session TimeZone."""
    url = postgres_container.get_connection_url()
    if "://" in url:
        _, rest = url.split("://", 1)
        url = f"postgresql+psycopg2://{rest}"

    connect_args: dict[str, str] = {}
    if session_timezone is not None:
        # psycopg2 startup options: pin the default session TimeZone.
        connect_args["options"] = f"-c TimeZone={session_timezone}"

    return create_engine(url, echo=False, pool_pre_ping=True, connect_args=connect_args)


def _create_parent_table(engine: Engine, parent: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {parent} (
                    id BIGSERIAL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    data TEXT,
                    PRIMARY KEY (id, created_at)
                ) PARTITION BY RANGE (created_at)
                """
            )
        )


@pytest.mark.integration
def test__attach_partition__non_utc_session_timezone__boundaries_stored_in_utc(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange
    url = postgres_container.get_connection_url()
    if "://" in url:
        _, rest = url.split("://", 1)
        url = f"postgresql+psycopg2://{rest}"

    engine = create_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        # psycopg2 startup options: ensure the default session TimeZone is NOT UTC.
        connect_args={"options": "-c TimeZone=America/Los_Angeles"},
    )

    table_relname = f"tz_events_{uuid4().hex[:8]}"
    parent = qualify("public", table_relname)
    partition_relname = f"{table_relname}__2024_01"
    partition = qualify("public", partition_relname)

    try:
        with engine.begin() as conn:
            tz = conn.execute(text("SHOW TimeZone")).scalar()
            assert tz is not None
            assert "los_angeles" in str(tz).lower()

            conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {parent} (
                        id BIGSERIAL,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        data TEXT,
                        PRIMARY KEY (id, created_at)
                    ) PARTITION BY RANGE (created_at)
                    """
                )
            )

        repo = PostgresPartitionRepository(engine)  # ddl_timezone="UTC" by default
        config = TablePartitionConfig(
            schema="public",
            table_name=table_relname,
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
        )

        # Act
        repo.create_partition(config, partition, "2024-01-01", "2024-02-01")
        repo.attach_partition(parent, partition, "2024-01-01", "2024-02-01")

        with engine.connect() as conn:
            # pg_get_expr renders timestamptz constants using the current session TimeZone.
            conn.execute(text("SET TIME ZONE 'UTC'"))
            expr = conn.execute(
                text(
                    """
                    SELECT pg_get_expr(c.relpartbound, c.oid)
                    FROM pg_class c
                    WHERE c.oid = to_regclass(:partition_name)
                    """
                ),
                {"partition_name": partition.lower()},
            ).scalar()

        # Assert — if ATTACH used session TZ (America/Los_Angeles), boundaries would show
        # 08:00:00+00 (winter offset) instead of 00:00:00+00
        assert expr is not None
        assert "2024-01-01 00:00:00+00" in str(expr)
        assert "2024-02-01 00:00:00+00" in str(expr)
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {parent} CASCADE"))
        engine.dispose()


@pytest.mark.integration
def test__timezone__moscow_calculator__boundaries_are_moscow_midnights(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange — the session TimeZone is unrelated to the configured DDL timezone.
    engine = _create_sync_engine(postgres_container, session_timezone="America/Los_Angeles")

    table_relname = f"tz_events_{uuid4().hex[:8]}"
    parent = qualify("public", table_relname)

    calc = MonthPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))
    assert calc.timezone_name == "Europe/Moscow"

    period = Period(year=2024, month=1)
    from_value, to_value = calc.get_boundaries(period)
    partition = qualify("public", calc.format_partition_name(table_relname, period))

    try:
        with engine.begin() as conn:
            tz = conn.execute(text("SHOW TimeZone")).scalar()
            assert tz is not None
            assert "los_angeles" in str(tz).lower()

        _create_parent_table(engine, parent)

        repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Moscow")
        assert repo.ddl_timezone == "Europe/Moscow"

        config = TablePartitionConfig(
            schema="public",
            table_name=table_relname,
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
        )

        # Act
        repo.create_partition(config, partition, from_value, to_value)
        repo.attach_partition(parent, partition, from_value, to_value)

        with engine.connect() as conn:
            # pg_get_expr renders timestamptz constants using the current session TimeZone.
            conn.execute(text("SET TIME ZONE 'UTC'"))
            expr = conn.execute(
                text(
                    """
                    SELECT pg_get_expr(c.relpartbound, c.oid)
                    FROM pg_class c
                    WHERE c.oid = to_regclass(:partition_name)
                    """
                ),
                {"partition_name": partition.lower()},
            ).scalar()

        # Assert — naive '2024-01-01'/'2024-02-01' literals attached under
        # SET LOCAL TIME ZONE 'Europe/Moscow' are Moscow midnights, i.e. 21:00
        # UTC the previous day (Moscow is UTC+3 year-round, no DST).
        assert expr is not None
        assert "2023-12-31 21:00:00+00" in str(expr)
        assert "2024-01-31 21:00:00+00" in str(expr)
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {parent} CASCADE"))
        engine.dispose()


@pytest.mark.integration
def test__timezone__moscow_calculator__pruning_selects_correct_set(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange — the session TimeZone is unrelated to the configured DDL timezone.
    engine = _create_sync_engine(postgres_container, session_timezone="America/Los_Angeles")

    table_relname = f"tz_events_{uuid4().hex[:8]}"
    parent = qualify("public", table_relname)

    calc = MonthPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))
    periods = [Period(year=2024, month=4), Period(year=2024, month=5), Period(year=2024, month=6)]

    try:
        _create_parent_table(engine, parent)

        repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Moscow")
        metadata = PostgresMetadataProvider(engine)
        locks = PostgresAdvisoryLockManager(engine)
        service = PartitionLifecycleService(repo, metadata, locks, calc)

        config = TablePartitionConfig(
            schema="public",
            table_name=table_relname,
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
            retention_count=2,
        )

        for period in periods:
            from_value, to_value = calc.get_boundaries(period)
            partition = qualify("public", calc.format_partition_name(table_relname, period))
            repo.create_partition(config, partition, from_value, to_value)
            repo.attach_partition(parent, partition, from_value, to_value)

        # Act — freeze mid-month so UTC and Moscow agree on the current month.
        with freezegun.freeze_time("2024-06-15 12:00:00"):
            to_prune = service.get_partitions_for_pruning(config)

        # Assert — retention keeps 2024-06 (current) and 2024-05; only the
        # 2024-04 partition ends at/before the cutoff, whose Moscow-midnight
        # boundary (2024-05-01 Moscow == 2024-04-30 21:00:00+00) must parse to
        # the same UTC instant as the attached partition bounds.
        expected = qualify("public", calc.format_partition_name(table_relname, Period(year=2024, month=4)))
        assert [p.name for p in to_prune] == [expected]
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {parent} CASCADE"))
        engine.dispose()


@pytest.mark.integration
def test__timezone__mismatched_pair__service_construction_fails(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange
    engine = _create_sync_engine(postgres_container)

    try:
        repo = PostgresPartitionRepository(engine, ddl_timezone="UTC")
        metadata = PostgresMetadataProvider(engine)
        locks = PostgresAdvisoryLockManager(engine)
        calc = MonthPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))

        # Act / Assert
        with pytest.raises(ValueError, match="Timezone mismatch"):
            PartitionLifecycleService(repo, metadata, locks, calc)
    finally:
        engine.dispose()
