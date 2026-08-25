"""Integration test for deterministic TIMESTAMPTZ partition boundaries (UTC semantics, sync API)."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.utils import qualify


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
