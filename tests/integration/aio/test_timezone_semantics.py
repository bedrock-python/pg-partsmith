"""Integration tests for deterministic TIMESTAMPTZ partition boundaries (UTC and configurable tz)."""

from __future__ import annotations

from uuid import uuid4
from zoneinfo import ZoneInfo

import freezegun
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.boundaries import TimeBoundaries
from pg_partsmith.entities import PartitionGranularity, Period, TablePartitionConfig
from pg_partsmith.topology import RangeBounds
from pg_partsmith.utils import qualify

pytestmark = pytest.mark.integration


def _create_async_engine(postgres_container: PostgresContainer, session_timezone: str | None = None) -> AsyncEngine:
    """Create an async engine, optionally pinning the default session TimeZone."""
    url = postgres_container.get_connection_url()
    if "://" in url:
        _, rest = url.split("://", 1)
        url = f"postgresql+asyncpg://{rest}"

    connect_args: dict[str, dict[str, str]] = {}
    if session_timezone is not None:
        # asyncpg startup parameter: pin the default session TimeZone.
        connect_args["server_settings"] = {"TimeZone": session_timezone}

    return create_async_engine(url, echo=False, pool_pre_ping=True, connect_args=connect_args)


async def _create_parent_table(engine: AsyncEngine, parent: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(
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


async def _partition_bound_expr(engine: AsyncEngine, partition: str) -> str:
    async with engine.connect() as conn:
        # pg_get_expr renders timestamptz constants using the current session TimeZone.
        await conn.execute(text("SET TIME ZONE 'UTC'"))
        expr = (
            await conn.execute(
                text(
                    """
                    SELECT pg_get_expr(c.relpartbound, c.oid)
                    FROM pg_class c
                    WHERE c.oid = to_regclass(:partition_name)
                    """
                ),
                {"partition_name": partition.lower()},
            )
        ).scalar()
    assert expr is not None
    return str(expr)


def _moscow_config(table_relname: str, *, retention: int = 12) -> TablePartitionConfig:
    return TablePartitionConfig(
        schema="public",
        table_name=table_relname,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        tz=ZoneInfo("Europe/Moscow"),
        create_ahead_count=1,
        retention_count=retention,
    )


async def test__attach_partition__non_utc_session_timezone__boundaries_stored_in_utc(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange — the default session TimeZone is NOT UTC.
    engine = _create_async_engine(postgres_container, session_timezone="America/Los_Angeles")

    table_relname = f"tz_events_{uuid4().hex[:8]}"
    parent = qualify("public", table_relname)
    partition = qualify("public", f"{table_relname}__2024_01")

    try:
        async with engine.begin() as conn:
            tz = (await conn.execute(text("SHOW TimeZone"))).scalar()
            assert tz is not None
            assert "los_angeles" in str(tz).lower()

        await _create_parent_table(engine, parent)
        repo = PostgresPartitionRepository(engine)  # ddl_timezone="UTC" by default

        # Act
        await repo.create_table_like(parent, partition, None)
        await repo.attach_partition(parent, partition, RangeBounds(from_value="2024-01-01", to_value="2024-02-01"))

        # Assert — if ATTACH used session TZ (America/Los_Angeles), boundaries would show
        # 08:00:00+00 (winter offset) instead of 00:00:00+00
        expr = await _partition_bound_expr(engine, partition)
        assert "2024-01-01 00:00:00+00" in expr
        assert "2024-02-01 00:00:00+00" in expr
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {parent} CASCADE"))
        await engine.dispose()


async def test__timezone__moscow_calendar__boundaries_are_moscow_midnights(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange — the session TimeZone is unrelated to the configured DDL timezone.
    engine = _create_async_engine(postgres_container, session_timezone="America/Los_Angeles")

    table_relname = f"tz_events_{uuid4().hex[:8]}"
    parent = qualify("public", table_relname)

    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH, tz=ZoneInfo("Europe/Moscow"))
    calc = boundaries.period_calculator
    assert boundaries.timezone_name == "Europe/Moscow"

    period = Period(year=2024, month=1)
    from_value, to_value = calc.get_boundaries(period)
    partition = qualify("public", calc.format_partition_name(table_relname, period))

    try:
        async with engine.begin() as conn:
            tz = (await conn.execute(text("SHOW TimeZone"))).scalar()
            assert tz is not None
            assert "los_angeles" in str(tz).lower()

        await _create_parent_table(engine, parent)

        repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Moscow")
        assert repo.ddl_timezone == "Europe/Moscow"

        # Act
        await repo.create_table_like(parent, partition, None)
        await repo.attach_partition(parent, partition, RangeBounds(from_value=from_value, to_value=to_value))

        # Assert — naive '2024-01-01'/'2024-02-01' literals attached under
        # SET LOCAL TIME ZONE 'Europe/Moscow' are Moscow midnights, i.e. 21:00
        # UTC the previous day (Moscow is UTC+3 year-round, no DST).
        expr = await _partition_bound_expr(engine, partition)
        assert "2023-12-31 21:00:00+00" in expr
        assert "2024-01-31 21:00:00+00" in expr
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {parent} CASCADE"))
        await engine.dispose()


async def test__timezone__moscow_calendar__pruning_selects_correct_set(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange — the session TimeZone is unrelated to the configured DDL timezone.
    engine = _create_async_engine(postgres_container, session_timezone="America/Los_Angeles")

    table_relname = f"tz_events_{uuid4().hex[:8]}"
    parent = qualify("public", table_relname)

    config = _moscow_config(table_relname, retention=2)
    boundaries = config.time_boundaries
    assert boundaries is not None
    calc = boundaries.period_calculator
    periods = [Period(year=2024, month=4), Period(year=2024, month=5), Period(year=2024, month=6)]

    try:
        await _create_parent_table(engine, parent)

        repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Moscow")
        metadata = PostgresMetadataProvider(engine)
        locks = PostgresAdvisoryLockManager(engine)
        service = PartitionLifecycleService(repo, metadata, locks)

        for period in periods:
            from_value, to_value = calc.get_boundaries(period)
            partition = qualify("public", calc.format_partition_name(table_relname, period))
            await repo.create_table_like(parent, partition, None)
            await repo.attach_partition(parent, partition, RangeBounds(from_value=from_value, to_value=to_value))

        # Act — freeze mid-month so UTC and Moscow agree on the current month.
        with freezegun.freeze_time("2024-06-15 12:00:00"):
            to_prune = await service.get_partitions_for_pruning(config)

        # Assert — retention keeps 2024-06 (current) and 2024-05; only the
        # 2024-04 partition ends at/before the cutoff, whose Moscow-midnight
        # boundary (2024-05-01 Moscow == 2024-04-30 21:00:00+00) must parse to
        # the same UTC instant as the attached partition bounds.
        expected = qualify("public", calc.format_partition_name(table_relname, Period(year=2024, month=4)))
        assert [p.name for p in to_prune] == [expected]
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {parent} CASCADE"))
        await engine.dispose()


async def test__timezone__moscow_calendar__maintenance_creates_and_prunes_on_moscow_midnights(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange — an end-to-end run with the calendar and the DDL session aligned.
    engine = _create_async_engine(postgres_container, session_timezone="America/Los_Angeles")

    table_relname = f"tz_events_{uuid4().hex[:8]}"
    parent = qualify("public", table_relname)
    config = _moscow_config(table_relname, retention=1)

    try:
        await _create_parent_table(engine, parent)
        repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Moscow")
        service = PartitionLifecycleService(repo, PostgresMetadataProvider(engine), PostgresAdvisoryLockManager(engine))

        # Act — 2024-05-31 22:00 UTC is already June in Moscow
        with freezegun.freeze_time("2024-05-31 22:00:00"):
            first = await service.maintain(config)

        # Assert — the Moscow calendar decided both the name and the instant
        assert first.created_count == 1
        expr = await _partition_bound_expr(engine, f"{parent}__2024_06")
        assert "2024-05-31 21:00:00+00" in expr
        assert "2024-06-30 21:00:00+00" in expr

        # Act — mid-July, June has aged out under a one-month retention
        with freezegun.freeze_time("2024-07-15"):
            second = await service.maintain(config)

        # Assert — pruned by the same Moscow-midnight bound it was created with
        assert second.detached_count == 1
        assert second.dropped_count == 1
        assert second.created_count == 1
        july = await _partition_bound_expr(engine, f"{parent}__2024_07")
        assert "2024-06-30 21:00:00+00" in july
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {parent} CASCADE"))
        await engine.dispose()


async def test__timezone__mismatched_pair__planning_is_refused(
    postgres_container: PostgresContainer,
) -> None:
    # Arrange — the calendar works in Moscow, the repository writes DDL in UTC.
    engine = _create_async_engine(postgres_container)
    table_relname = f"tz_events_{uuid4().hex[:8]}"
    parent = qualify("public", table_relname)

    try:
        await _create_parent_table(engine, parent)
        repo = PostgresPartitionRepository(engine, ddl_timezone="UTC")
        service = PartitionLifecycleService(repo, PostgresMetadataProvider(engine), PostgresAdvisoryLockManager(engine))

        # Act / Assert — refused before any catalog read or DDL
        with pytest.raises(ValueError, match="Timezone mismatch"):
            await service.plan(_moscow_config(table_relname))
        with pytest.raises(ValueError, match="Timezone mismatch"):
            await service.maintain(_moscow_config(table_relname))
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {parent} CASCADE"))
        await engine.dispose()
