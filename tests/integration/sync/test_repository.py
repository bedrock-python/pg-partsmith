from collections.abc import Generator

import pytest
from sqlalchemy import Engine, text

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import LockAcquisitionError, UnmanagedPartitionDropError
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository


@pytest.fixture
def partitioned_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    # Arrange
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sync_repo_events (
                    id BIGSERIAL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    data TEXT,
                    PRIMARY KEY (id, created_at)
                ) PARTITION BY RANGE (created_at)
                """
            )
        )
    yield "sync_repo_events"
    with sync_db_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sync_repo_events CASCADE"))


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
def test__repository__partition_not_created__exists_returns_false(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act / Assert
    assert not repo.partition_exists(f"{partitioned_table}__2024_01")


@pytest.mark.integration
def test__repository__create_partition__creates_table_and_returns_info(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act
    info = repo.create_partition(config, f"{config.table_name}__2024_01", "2024-01-01", "2024-02-01")

    # Assert
    assert info.name == f"{config.table_name}__2024_01"
    assert info.from_value == "2024-01-01"
    assert info.to_value == "2024-02-01"
    assert info.is_attached is False
    assert repo.partition_exists(info.name)


@pytest.mark.integration
def test__repository__attach_partition__makes_it_visible_in_list(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    partition_name = f"{config.table_name}__2024_02"
    # list_partitions always returns schema-qualified names
    qualified_name = f"public.{partition_name}"
    info = repo.create_partition(config, partition_name, "2024-02-01", "2024-03-01")

    # Not yet attached — not visible as attached
    partitions = metadata.list_partitions(config.table_name)
    assert all(p.name != qualified_name or not p.is_attached for p in partitions)

    # Act
    repo.attach_partition(config.table_name, info.name, "2024-02-01", "2024-03-01")

    # Assert
    partitions = metadata.list_partitions(config.table_name)
    attached = [p for p in partitions if p.name == qualified_name and p.is_attached]
    assert len(attached) == 1
    assert attached[0].from_value is not None
    assert "2024-02-01" in attached[0].from_value


@pytest.mark.integration
def test__repository__detach_and_drop__removes_partition_from_catalog(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    partition_name = f"{config.table_name}__2024_03"
    info = repo.create_partition(config, partition_name, "2024-03-01", "2024-04-01")
    repo.attach_partition(config.table_name, info.name, "2024-03-01", "2024-04-01")
    assert repo.is_partition_attached(config.table_name, partition_name)

    # Act
    repo.detach_partition(config.table_name, partition_name, concurrent=True)

    # Assert — detached but still exists
    assert not repo.is_partition_attached(config.table_name, partition_name)
    assert repo.partition_exists(partition_name)

    # Act — drop
    repo.drop_partition(partition_name)

    # Assert — gone
    assert not repo.partition_exists(partition_name)


@pytest.mark.integration
def test__repository__drop_nonexistent_partition__is_noop(sync_db_engine: Engine) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act / Assert — must not raise
    repo.drop_partition("nonexistent_partition_xyz")


@pytest.mark.integration
def test__repository__drop_unattached_without_orphan_marker__raises_unmanaged_drop_error(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    partition_name = f"{config.table_name}__2024_04"
    repo.create_partition(config, partition_name, "2024-04-01", "2024-05-01")
    assert not repo.is_partition_attached(config.table_name, partition_name)

    # Act / Assert — safe-by-default blocks the drop
    with pytest.raises(UnmanagedPartitionDropError):
        repo.drop_partition(partition_name)

    # Opt-in bypass
    unsafe_repo = PostgresPartitionRepository(sync_db_engine, drop_allow_unmanaged=True)
    unsafe_repo.drop_partition(partition_name)


@pytest.mark.integration
def test__repository__list_partitions__includes_orphan_after_detach(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    partition_name = f"{config.table_name}__2024_06"

    repo.create_partition(config, partition_name, "2024-06-01", "2024-07-01")
    repo.attach_partition(config.table_name, partition_name, "2024-06-01", "2024-07-01")
    repo.detach_partition(config.table_name, partition_name, concurrent=True)

    # Act
    partitions = metadata.list_partitions(config.table_name)

    # Assert — orphan is listed under its schema-qualified name
    orphan = next((p for p in partitions if p.name == f"public.{partition_name}"), None)
    assert orphan is not None
    assert orphan.is_attached is False

    repo.drop_partition(partition_name)


@pytest.mark.integration
def test__repository__list_partitions__ignores_similarly_named_table_without_marker(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)

    orphan_name = f"{config.table_name}__2024_07"
    similar_name = f"{config.table_name}__2024_01"

    repo.create_partition(config, orphan_name, "2024-07-01", "2024-08-01")
    repo.attach_partition(config.table_name, orphan_name, "2024-07-01", "2024-08-01")
    repo.detach_partition(config.table_name, orphan_name, concurrent=True)

    with sync_db_engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE IF NOT EXISTS "{similar_name}" (id INT)'))

    # Act
    partitions = metadata.list_partitions(config.table_name)
    names = {p.name for p in partitions}

    # Assert — names are schema-qualified; the unmarked look-alike is not listed
    assert f"public.{orphan_name}" in names
    assert f"public.{similar_name}" not in names

    repo.drop_partition(orphan_name)
    with sync_db_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{similar_name}"'))


# ── PostgresMetadataProvider ──────────────────────────────────────────────────────


@pytest.mark.integration
def test__metadata_provider__partition_type__returns_range(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    provider = PostgresMetadataProvider(sync_db_engine)

    # Act / Assert
    assert provider.get_partition_type(partitioned_table) == PartitionType.RANGE


@pytest.mark.integration
def test__metadata_provider__partition_column__returns_created_at(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    provider = PostgresMetadataProvider(sync_db_engine)

    # Act / Assert
    assert provider.get_partition_column(partitioned_table) == "created_at"


@pytest.mark.integration
def test__metadata_provider__get_partition_boundaries__returns_correct_range(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    provider = PostgresMetadataProvider(sync_db_engine)
    partition_name = f"{config.table_name}__2024_05"
    info = repo.create_partition(config, partition_name, "2024-05-01", "2024-06-01")
    repo.attach_partition(config.table_name, info.name, "2024-05-01", "2024-06-01")

    # Act
    boundaries = provider.get_partition_boundaries(partition_name)

    # Assert
    assert boundaries is not None
    assert "2024-05-01" in boundaries[0]
    assert "2024-06-01" in boundaries[1]

    repo.detach_partition(config.table_name, partition_name, concurrent=True)
    repo.drop_partition(partition_name)


# ── PostgresAdvisoryLockManager ───────────────────────────────────────────────────


@pytest.mark.integration
def test__advisory_lock__acquire_and_release__lock_is_held_then_freed(
    sync_db_engine: Engine,
) -> None:
    # Arrange
    manager = PostgresAdvisoryLockManager(sync_db_engine)

    # Act / Assert
    with manager.acquire_lock("sync_events"):
        assert manager.is_locked("sync_events")


@pytest.mark.integration
def test__advisory_lock__two_sessions_same_table__second_raises_lock_acquisition_error(
    sync_db_engine: Engine,
) -> None:
    # Arrange
    manager1 = PostgresAdvisoryLockManager(sync_db_engine)
    manager2 = PostgresAdvisoryLockManager(sync_db_engine)

    # Act / Assert
    with manager1.acquire_lock("sync_events_double"):  # noqa: SIM117
        with pytest.raises(LockAcquisitionError), manager2.acquire_lock("sync_events_double"):
            pass
