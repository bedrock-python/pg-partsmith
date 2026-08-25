"""Integration tests for schema-qualified operation (sync API)."""

import freezegun
import pytest
from sqlalchemy import Engine, text

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.strategies import MonthPeriodCalculator
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.maintainer import PartitionMaintainer
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.sync.service import PartitionLifecycleService


@pytest.mark.integration
def test__schema_support__two_schemas__maintenance_isolated_per_schema(sync_db_engine: Engine) -> None:
    # Arrange
    with sync_db_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS s1"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS s2"))
        conn.execute(
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
        conn.execute(
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
        repo = PostgresPartitionRepository(sync_db_engine)
        metadata = PostgresMetadataProvider(sync_db_engine)
        locks = PostgresAdvisoryLockManager(sync_db_engine)
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
            result = maintainer.run_maintenance(config_s1)

        assert result.success
        assert metadata.partition_exists("s1.events__2024_01") is True
        assert metadata.partition_exists("s2.events__2024_01") is False
    finally:
        with sync_db_engine.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS s1 CASCADE"))
            conn.execute(text("DROP SCHEMA IF EXISTS s2 CASCADE"))
