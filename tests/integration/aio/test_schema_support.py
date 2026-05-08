"""Integration tests for schema-qualified operation."""

import freezegun
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.maintainer import PartitionMaintainer
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.strategies import MonthPeriodCalculator


@pytest.mark.integration
async def test__schema_support__two_schemas__maintenance_isolated_per_schema(db_engine: AsyncEngine) -> None:
    # Arrange
    async with db_engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS s1"))
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS s2"))
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS s1.events (
                    id BIGSERIAL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    data TEXT,
                    PRIMARY KEY (id, created_at)
                ) PARTITION BY RANGE (created_at)
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS s2.events (
                    id BIGSERIAL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    data TEXT,
                    PRIMARY KEY (id, created_at)
                ) PARTITION BY RANGE (created_at)
                """
            )
        )

    try:
        repo = PostgresPartitionRepository(db_engine)
        metadata = PostgresMetadataProvider(db_engine)
        locks = PostgresAdvisoryLockManager(db_engine)
        calc = MonthPeriodCalculator()
        service = PartitionLifecycleService(repo, metadata, locks, calc)
        maintainer = PartitionMaintainer(service)

        config_s1 = TablePartitionConfig(
            schema="s1",
            table_name="events",
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=PartitionGranularity.MONTH,
            create_ahead_count=1,
            retention_count=12,
        )

        with freezegun.freeze_time("2024-01-01"):
            result = await maintainer.run_maintenance(config_s1)

        assert result.success
        assert await metadata.partition_exists("s1.events__2024_01") is True
        assert await metadata.partition_exists("s2.events__2024_01") is False
    finally:
        async with db_engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA IF EXISTS s1 CASCADE"))
            await conn.execute(text("DROP SCHEMA IF EXISTS s2 CASCADE"))
