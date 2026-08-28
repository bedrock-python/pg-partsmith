"""Repository, metadata provider and advisory locks against a real PostgreSQL (sync)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

from pg_partsmith.entities import PartitionType, TablePartitionConfig
from pg_partsmith.exceptions import LockAcquisitionError, PartitionAlreadyExistsError, UnmanagedPartitionDropError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.plan import PartitionBy
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.topology import HashBounds, RangeBounds
from tests.integration.nested_support import MONTHLY_TABLE_DDL, monthly_config, orphan_marker
from tests.integration.sync.support import make_table, table_comment

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration


@pytest.fixture
def partitioned_table(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="repo_events")


@pytest.fixture
def config(partitioned_table: str) -> TablePartitionConfig:
    return monthly_config(partitioned_table)


# ── PostgresPartitionRepository ──────────────────────────────────────────────────


def test__repository__create_table_like__creates_a_detached_table(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    name = f"{config.table_name}__2024_01"

    # Act
    repo.create_table_like(config.table_name, name, None)

    # Assert
    assert metadata.partition_exists(name)
    assert not metadata.is_partition_attached(config.table_name, name)


def test__repository__create_table_like__existing_name__raises_already_exists(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    name = f"{config.table_name}__2024_01"
    repo.create_table_like(config.table_name, name, None)

    # Act / Assert
    with pytest.raises(PartitionAlreadyExistsError):
        repo.create_table_like(config.table_name, name, None)


def test__repository__create_table_like_with_partition_by__creates_a_branch(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    branch = f"{config.table_name}__2024_01"

    # Act
    repo.create_table_like(config.table_name, branch, PartitionBy(method=PartitionType.HASH, columns=("id",)))
    repo.create_table_like(branch, f"{branch}__h0", None)
    repo.attach_partition(branch, f"{branch}__h0", HashBounds(modulus=1, remainder=0))

    # Assert
    tree = metadata.get_partition_tree(branch)
    assert tree is not None
    assert tree.partition_type == PartitionType.HASH
    assert tree.partition_columns == ("id",)
    assert [c.bounds for c in tree.children] == [HashBounds(modulus=1, remainder=0)]


def test__repository__attach_partition__makes_it_visible_in_list(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    partition_name = f"{config.table_name}__2024_02"
    # list_partitions always returns schema-qualified names
    qualified_name = f"public.{partition_name}"
    repo.create_table_like(config.table_name, partition_name, None)

    # Not yet attached — not visible as attached
    partitions = metadata.list_partitions(config.table_name)
    assert all(p.name != qualified_name or not p.is_attached for p in partitions)

    # Act
    repo.attach_partition(
        config.table_name, partition_name, RangeBounds(from_value="2024-02-01", to_value="2024-03-01")
    )

    # Assert
    partitions = metadata.list_partitions(config.table_name)
    attached = [p for p in partitions if p.name == qualified_name and p.is_attached]
    assert len(attached) == 1
    assert attached[0].from_value is not None
    assert "2024-02-01" in attached[0].from_value
    assert attached[0].oid is not None
    assert attached[0].subpartition_type is None


def test__repository__detach_and_drop__removes_partition_from_catalog(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    partition_name = f"{config.table_name}__2024_03"
    repo.create_table_like(config.table_name, partition_name, None)
    repo.attach_partition(
        config.table_name, partition_name, RangeBounds(from_value="2024-03-01", to_value="2024-04-01")
    )
    assert metadata.is_partition_attached(config.table_name, partition_name)

    # Act
    repo.detach_partition(config.table_name, partition_name, mode=DetachMode.CONCURRENT)

    # Assert — detached but still exists, and marked as ours
    assert not metadata.is_partition_attached(config.table_name, partition_name)
    assert metadata.partition_exists(partition_name)
    comment = table_comment(sync_db_engine, partition_name)
    assert comment is not None
    assert comment.splitlines()[0] == orphan_marker(f"public.{config.table_name}")

    # Act — drop
    repo.drop_partition(partition_name)

    # Assert — gone
    assert not metadata.partition_exists(partition_name)


def test__repository__drop_nonexistent_partition__is_noop(sync_db_engine: Engine) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act / Assert — must not raise
    repo.drop_partition("nonexistent_partition_xyz")


def test__repository__drop_unattached_without_orphan_marker__raises_unmanaged_drop_error(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    partition_name = f"{config.table_name}__2024_04"
    repo.create_table_like(config.table_name, partition_name, None)
    assert not metadata.is_partition_attached(config.table_name, partition_name)

    # Act / Assert — safe-by-default blocks the drop
    with pytest.raises(UnmanagedPartitionDropError):
        repo.drop_partition(partition_name)

    # Opt-in bypass
    unsafe_repo = PostgresPartitionRepository(sync_db_engine, drop_allow_unmanaged=True)
    unsafe_repo.drop_partition(partition_name)
    assert not metadata.partition_exists(partition_name)


def test__repository__list_partitions__includes_orphan_after_detach(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    partition_name = f"{config.table_name}__2024_06"

    repo.create_table_like(config.table_name, partition_name, None)
    repo.attach_partition(
        config.table_name, partition_name, RangeBounds(from_value="2024-06-01", to_value="2024-07-01")
    )
    repo.detach_partition(config.table_name, partition_name, mode=DetachMode.CONCURRENT)

    # Act
    partitions = metadata.list_partitions(config.table_name)

    # Assert — orphan is listed under its schema-qualified name
    orphan = next((p for p in partitions if p.name == f"public.{partition_name}"), None)
    assert orphan is not None
    assert orphan.is_attached is False

    repo.drop_partition(partition_name)


def test__repository__list_partitions__ignores_similarly_named_table_without_marker(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)

    orphan_name = f"{config.table_name}__2024_07"
    similar_name = f"{config.table_name}__2024_01"

    repo.create_table_like(config.table_name, orphan_name, None)
    repo.attach_partition(config.table_name, orphan_name, RangeBounds(from_value="2024-07-01", to_value="2024-08-01"))
    repo.detach_partition(config.table_name, orphan_name, mode=DetachMode.CONCURRENT)

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


def test__metadata_provider__partition_not_created__exists_returns_false(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act / Assert
    assert not metadata.partition_exists(f"{partitioned_table}__2024_01")


def test__metadata_provider__partition_type__returns_range(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    provider = PostgresMetadataProvider(sync_db_engine)

    # Act / Assert
    assert provider.get_partition_type(partitioned_table) == PartitionType.RANGE


def test__metadata_provider__partition_column__returns_created_at(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    provider = PostgresMetadataProvider(sync_db_engine)

    # Act / Assert
    assert provider.get_partition_column(partitioned_table) == "created_at"
    assert provider.get_partition_columns(partitioned_table) == ("created_at",)


def test__metadata_provider__get_partition_boundaries__returns_correct_range(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    provider = PostgresMetadataProvider(sync_db_engine)
    partition_name = f"{config.table_name}__2024_05"
    repo.create_table_like(config.table_name, partition_name, None)
    repo.attach_partition(
        config.table_name, partition_name, RangeBounds(from_value="2024-05-01", to_value="2024-06-01")
    )

    # Act
    boundaries = provider.get_partition_boundaries(partition_name)

    # Assert
    assert boundaries is not None
    assert "2024-05-01" in boundaries[0]
    assert "2024-06-01" in boundaries[1]

    repo.detach_partition(config.table_name, partition_name, mode=DetachMode.CONCURRENT)
    repo.drop_partition(partition_name)


def test__metadata_provider__get_actual_tree__reports_children_oids_and_orphans(
    sync_db_engine: Engine, config: TablePartitionConfig
) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)
    provider = PostgresMetadataProvider(sync_db_engine)
    attached = f"{config.table_name}__2024_05"
    detached = f"{config.table_name}__2024_04"
    for name, bounds in ((attached, ("2024-05-01", "2024-06-01")), (detached, ("2024-04-01", "2024-05-01"))):
        repo.create_table_like(config.table_name, name, None)
        repo.attach_partition(config.table_name, name, RangeBounds(from_value=bounds[0], to_value=bounds[1]))
    repo.detach_partition(config.table_name, detached, mode=DetachMode.BLOCKING)

    # Act
    tree = provider.get_actual_tree(config.table_name)

    # Assert
    assert tree is not None
    assert tree.root.name == f"public.{config.table_name}"
    assert tree.root.partition_type == PartitionType.RANGE
    assert [c.name for c in tree.root.children] == [f"public.{attached}"]
    assert tree.root.children[0].oid == provider.get_relation_oid(attached)
    assert [o.name for o in tree.orphans] == [f"public.{detached}"]
    assert tree.orphans[0].parent_name == f"public.{config.table_name}"
    assert tree.orphans[0].detached_at is not None


def test__metadata_provider__get_actual_tree__unpartitioned_table__returns_none(sync_db_engine: Engine) -> None:
    # Arrange
    provider = PostgresMetadataProvider(sync_db_engine)
    with sync_db_engine.begin() as conn:
        conn.execute(text("CREATE TABLE plain_repo_table (i INT)"))

    # Act / Assert
    try:
        assert provider.get_actual_tree("plain_repo_table") is None
    finally:
        with sync_db_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS plain_repo_table"))


def test__metadata_provider__get_key_high_water_mark__reads_max_key_and_the_sequence(
    sync_db_engine: Engine,
) -> None:
    # Arrange
    provider = PostgresMetadataProvider(sync_db_engine)
    with sync_db_engine.begin() as conn:
        conn.execute(text("CREATE TABLE hwm_queue (msg_id BIGSERIAL PRIMARY KEY, payload TEXT)"))

    try:
        # Assert — empty: neither source has a value
        assert provider.get_key_high_water_mark("hwm_queue", "msg_id") is None
        assert provider.get_key_high_water_mark("hwm_queue", "msg_id", sequence=True) is None

        with sync_db_engine.begin() as conn:
            conn.execute(text("INSERT INTO hwm_queue (payload) SELECT 'x' FROM generate_series(1, 5)"))
            conn.execute(text("DELETE FROM hwm_queue WHERE msg_id > 3"))

        # Act / Assert — max(key) follows the rows, the sequence what was handed out
        assert provider.get_key_high_water_mark("hwm_queue", "msg_id") == 3
        assert provider.get_key_high_water_mark("hwm_queue", "msg_id", sequence=True) == 5
    finally:
        with sync_db_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS hwm_queue"))


# ── PostgresAdvisoryLockManager ───────────────────────────────────────────────────


def test__advisory_lock__acquire_and_release__lock_is_held_then_freed(
    sync_db_engine: Engine,
) -> None:
    # Arrange
    manager = PostgresAdvisoryLockManager(sync_db_engine)

    # Act / Assert
    with manager.acquire_lock("events"):
        assert manager.is_locked("events")
    assert not manager.is_locked("events")


def test__advisory_lock__two_sessions_same_table__second_raises_lock_acquisition_error(
    sync_db_engine: Engine,
) -> None:
    # Arrange
    manager1 = PostgresAdvisoryLockManager(sync_db_engine)
    manager2 = PostgresAdvisoryLockManager(sync_db_engine)

    def contend() -> None:
        with manager2.acquire_lock("events_double"):
            pytest.fail("the second session must not get the lock")

    # Act / Assert
    with manager1.acquire_lock("events_double"):
        assert manager1.is_locked("events_double")
        with pytest.raises(LockAcquisitionError):
            contend()
