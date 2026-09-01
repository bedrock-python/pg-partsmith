"""Integration tests for schema-qualified operation (async)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import freezegun
import pytest
import pytest_asyncio
from sqlalchemy import text

from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.maintainer import PartitionMaintainer
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.boundaries import TimeBoundaries
from pg_partsmith.entities import PartitionGranularity, TablePartitionConfig
from pg_partsmith.lifecycle import CreateAhead, KeepNewest, LifecyclePolicy
from pg_partsmith.scheme import HashPartitioning, RangePartitioning
from tests.integration.aio.support import hash_children_of, relkind
from tests.integration.nested_support import monthly_config

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

_EVENTS_DDL = """
    CREATE TABLE {schema}.events (
        id BIGSERIAL,
        tenant_id BIGINT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
        data TEXT,
        PRIMARY KEY (id, tenant_id, created_at)
    ) PARTITION BY RANGE (created_at)
"""


@pytest_asyncio.fixture
async def two_schemas(db_engine: AsyncEngine) -> AsyncGenerator[tuple[str, str], None]:
    """Two schemas holding an identically named partitioned table each."""
    suffix = uuid4().hex[:6]
    schemas = (f"s1_{suffix}", f"s2_{suffix}")
    async with db_engine.begin() as conn:
        for schema in schemas:
            await conn.execute(text(f"CREATE SCHEMA {schema}"))
            await conn.execute(text(_EVENTS_DDL.format(schema=schema)))
    yield schemas
    async with db_engine.begin() as conn:
        for schema in schemas:
            await conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def _maintainer(engine: AsyncEngine) -> PartitionMaintainer:
    service = PartitionLifecycleService(
        PostgresPartitionRepository(engine), PostgresMetadataProvider(engine), PostgresAdvisoryLockManager(engine)
    )
    return PartitionMaintainer(service)


async def test__schema_support__two_schemas__maintenance_isolated_per_schema(
    db_engine: AsyncEngine, two_schemas: tuple[str, str]
) -> None:
    # Arrange
    s1, s2 = two_schemas
    metadata = PostgresMetadataProvider(db_engine)
    config_s1 = monthly_config("events", schema=s1, create_ahead=1, retention=12)

    # Act
    with freezegun.freeze_time("2024-01-01"):
        result = await _maintainer(db_engine).run_maintenance(config_s1)

    # Assert
    assert result.success
    assert result.created_count == 1
    assert await metadata.partition_exists(f"{s1}.events__2024_01") is True
    assert await metadata.partition_exists(f"{s2}.events__2024_01") is False
    assert [p.name for p in await metadata.list_partitions(f"{s1}.events")] == [f"{s1}.events__2024_01"]
    assert await metadata.list_partitions(f"{s2}.events") == []


async def test__schema_support__composed_config_with_schema__subtree_lands_in_that_schema(
    db_engine: AsyncEngine, two_schemas: tuple[str, str]
) -> None:
    # Arrange — the composed spelling, schema included, on the second schema only
    s1, s2 = two_schemas
    metadata = PostgresMetadataProvider(db_engine)
    config = TablePartitionConfig(
        schema=s2,
        table_name="events",
        scheme=RangePartitioning(
            key="created_at",
            boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH),
            child=HashPartitioning(key="tenant_id", modulus=2),
        ),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=12)),
    )
    assert config.qualified_name == f"{s2}.events"

    # Act
    with freezegun.freeze_time("2024-01-01"):
        result = await _maintainer(db_engine).run_maintenance(config)

    # Assert — the branch and its buckets live in s2, and s1 is untouched
    assert result.success
    assert result.created_count == 1
    assert await metadata.partition_exists(f"{s2}.events__2024_01") is True
    assert await metadata.partition_exists(f"{s1}.events__2024_01") is False
    assert await relkind(db_engine, f"{s2}.events__2024_01") == "p"
    assert await hash_children_of(db_engine, f"{s2}.events__2024_01") == {
        "events__2024_01__h0": (2, 0),
        "events__2024_01__h1": (2, 1),
    }
    tree = await metadata.get_partition_tree(f"{s2}.events")
    assert tree is not None
    assert [c.name for c in tree.children] == [f"{s2}.events__2024_01"]
    assert {c.name for c in tree.children[0].children} == {
        f"{s2}.events__2024_01__h0",
        f"{s2}.events__2024_01__h1",
    }


async def test__schema_support__orphans__are_matched_to_their_own_schema(
    db_engine: AsyncEngine, two_schemas: tuple[str, str]
) -> None:
    # Arrange — both schemas get a January partition; s1's expires and is detached
    s1, s2 = two_schemas
    metadata = PostgresMetadataProvider(db_engine)
    config_s1 = monthly_config("events", schema=s1, create_ahead=1, retention=1)
    config_s2 = monthly_config("events", schema=s2, create_ahead=1, retention=1)
    maintainer = _maintainer(db_engine)
    with freezegun.freeze_time("2024-01-01"):
        await maintainer.run_maintenance(config_s1)
        await maintainer.run_maintenance(config_s2)

    # Act — only s1 moves on to March
    with freezegun.freeze_time("2024-03-01"):
        result = await maintainer.run_maintenance(config_s1, skip_drop=True)

    # Assert — s1's January is an orphan of s1.events alone
    assert result.detached_count == 1
    tree_s1 = await metadata.get_actual_tree(f"{s1}.events")
    tree_s2 = await metadata.get_actual_tree(f"{s2}.events")
    assert tree_s1 is not None and tree_s2 is not None
    assert [o.name for o in tree_s1.orphans] == [f"{s1}.events__2024_01"]
    assert tree_s2.orphans == ()
    assert [c.name for c in tree_s2.root.children] == [f"{s2}.events__2024_01"]
