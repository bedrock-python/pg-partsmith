from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import LockAcquisitionError, UnmanagedPartitionDropError


@pytest_asyncio.fixture
async def partitioned_table(db_session: AsyncSession) -> AsyncGenerator[str, None]:
    # Arrange
    await db_session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS repo_events (
                id BIGSERIAL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                data TEXT,
                PRIMARY KEY (id, created_at)
            ) PARTITION BY RANGE (created_at)
            """
        )
    )
    await db_session.commit()
    yield "repo_events"
    await db_session.execute(text("DROP TABLE IF EXISTS repo_events CASCADE"))
    await db_session.commit()


@pytest.fixture
def config(partitioned_table: str) -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )


# ── PostgresPartitionRepository ──────────────────────────────────────────────────


@pytest.mark.integration
async def test__repository__partition_not_created__exists_returns_false(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)

    # Act / Assert
    assert not await repo.partition_exists(f"{partitioned_table}__2024_01")


@pytest.mark.integration
async def test__repository__create_partition__creates_table_and_returns_info(
    db_engine: AsyncEngine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)

    # Act
    info = await repo.create_partition(config, f"{config.table_name}__2024_01", "2024-01-01", "2024-02-01")

    # Assert
    assert info.name == f"{config.table_name}__2024_01"
    assert info.from_value == "2024-01-01"
    assert info.to_value == "2024-02-01"
    assert info.is_attached is False
    assert await repo.partition_exists(info.name)


@pytest.mark.integration
async def test__repository__attach_partition__makes_it_visible_in_list(
    db_engine: AsyncEngine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)
    metadata = PostgresMetadataProvider(db_engine)
    partition_name = f"{config.table_name}__2024_02"
    info = await repo.create_partition(config, partition_name, "2024-02-01", "2024-03-01")

    # Not yet attached — not visible as attached
    partitions = await metadata.list_partitions(config.table_name)
    assert all(p.name != partition_name or not p.is_attached for p in partitions)

    # Act
    await repo.attach_partition(config.table_name, info.name, "2024-02-01", "2024-03-01")

    # Assert
    partitions = await metadata.list_partitions(config.table_name)
    attached = [p for p in partitions if p.name == partition_name and p.is_attached]
    assert len(attached) == 1
    assert attached[0].from_value is not None
    assert "2024-02-01" in attached[0].from_value


@pytest.mark.integration
async def test__repository__detach_and_drop__removes_partition_from_catalog(
    db_engine: AsyncEngine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)
    partition_name = f"{config.table_name}__2024_03"
    info = await repo.create_partition(config, partition_name, "2024-03-01", "2024-04-01")
    await repo.attach_partition(config.table_name, info.name, "2024-03-01", "2024-04-01")
    assert await repo.is_partition_attached(config.table_name, partition_name)

    # Act
    await repo.detach_partition(config.table_name, partition_name, concurrent=True)

    # Assert — detached but still exists
    assert not await repo.is_partition_attached(config.table_name, partition_name)
    assert await repo.partition_exists(partition_name)

    # Act — drop
    await repo.drop_partition(partition_name)

    # Assert — gone
    assert not await repo.partition_exists(partition_name)


@pytest.mark.integration
async def test__repository__drop_nonexistent_partition__is_noop(db_engine: AsyncEngine) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)

    # Act / Assert — must not raise
    await repo.drop_partition("nonexistent_partition_xyz")


@pytest.mark.integration
async def test__repository__drop_unattached_without_orphan_marker__raises_unmanaged_drop_error(
    db_engine: AsyncEngine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)
    partition_name = f"{config.table_name}__2024_04"
    await repo.create_partition(config, partition_name, "2024-04-01", "2024-05-01")
    assert not await repo.is_partition_attached(config.table_name, partition_name)

    # Act / Assert — safe-by-default blocks the drop
    with pytest.raises(UnmanagedPartitionDropError):
        await repo.drop_partition(partition_name)

    # Opt-in bypass
    unsafe_repo = PostgresPartitionRepository(db_engine, drop_allow_unmanaged=True)
    await unsafe_repo.drop_partition(partition_name)


@pytest.mark.integration
async def test__repository__list_partitions__includes_orphan_after_detach(
    db_engine: AsyncEngine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)
    metadata = PostgresMetadataProvider(db_engine)
    partition_name = f"{config.table_name}__2024_06"

    await repo.create_partition(config, partition_name, "2024-06-01", "2024-07-01")
    await repo.attach_partition(config.table_name, partition_name, "2024-06-01", "2024-07-01")
    await repo.detach_partition(config.table_name, partition_name, concurrent=True)

    # Act
    partitions = await metadata.list_partitions(config.table_name)

    # Assert
    orphan = next((p for p in partitions if p.name == partition_name), None)
    assert orphan is not None
    assert orphan.is_attached is False

    await repo.drop_partition(partition_name)


@pytest.mark.integration
async def test__repository__list_partitions__ignores_similarly_named_table_without_marker(
    db_engine: AsyncEngine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)
    metadata = PostgresMetadataProvider(db_engine)

    orphan_name = f"{config.table_name}__2024_07"
    similar_name = f"{config.table_name}__2024_01"

    await repo.create_partition(config, orphan_name, "2024-07-01", "2024-08-01")
    await repo.attach_partition(config.table_name, orphan_name, "2024-07-01", "2024-08-01")
    await repo.detach_partition(config.table_name, orphan_name, concurrent=True)

    async with db_engine.begin() as conn:
        await conn.execute(text(f'CREATE TABLE IF NOT EXISTS "{similar_name}" (id INT)'))

    # Act
    partitions = await metadata.list_partitions(config.table_name)
    names = {p.name for p in partitions}

    # Assert
    assert orphan_name in names
    assert similar_name not in names

    await repo.drop_partition(orphan_name)
    async with db_engine.begin() as conn:
        await conn.execute(text(f'DROP TABLE IF EXISTS "{similar_name}"'))


# ── PostgresMetadataProvider ──────────────────────────────────────────────────────


@pytest.mark.integration
async def test__metadata_provider__partition_type__returns_range(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    provider = PostgresMetadataProvider(db_engine)

    # Act / Assert
    assert await provider.get_partition_type(partitioned_table) == PartitionType.RANGE


@pytest.mark.integration
async def test__metadata_provider__partition_column__returns_created_at(
    db_engine: AsyncEngine, partitioned_table: str
) -> None:
    # Arrange
    provider = PostgresMetadataProvider(db_engine)

    # Act / Assert
    assert await provider.get_partition_column(partitioned_table) == "created_at"


@pytest.mark.integration
async def test__metadata_provider__get_partition_boundaries__returns_correct_range(
    db_engine: AsyncEngine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)
    provider = PostgresMetadataProvider(db_engine)
    partition_name = f"{config.table_name}__2024_05"
    info = await repo.create_partition(config, partition_name, "2024-05-01", "2024-06-01")
    await repo.attach_partition(config.table_name, info.name, "2024-05-01", "2024-06-01")

    # Act
    boundaries = await provider.get_partition_boundaries(partition_name)

    # Assert
    assert boundaries is not None
    assert "2024-05-01" in boundaries[0]
    assert "2024-06-01" in boundaries[1]

    await repo.detach_partition(config.table_name, partition_name, concurrent=True)
    await repo.drop_partition(partition_name)


# ── PostgresAdvisoryLockManager ───────────────────────────────────────────────────


@pytest.mark.integration
async def test__advisory_lock__acquire_and_release__lock_is_held_then_freed(
    db_engine: AsyncEngine,
) -> None:
    # Arrange
    manager = PostgresAdvisoryLockManager(db_engine)

    # Act / Assert
    async with manager.acquire_lock("events"):
        assert await manager.is_locked("events")


@pytest.mark.integration
async def test__advisory_lock__two_sessions_same_table__second_raises_lock_acquisition_error(
    db_engine: AsyncEngine,
) -> None:
    # Arrange
    manager1 = PostgresAdvisoryLockManager(db_engine)
    manager2 = PostgresAdvisoryLockManager(db_engine)

    # Act / Assert
    async with manager1.acquire_lock("events_double"):
        with pytest.raises(LockAcquisitionError):
            async with manager2.acquire_lock("events_double"):
                pass
