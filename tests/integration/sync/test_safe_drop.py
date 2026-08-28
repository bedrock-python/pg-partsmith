"""Safe drop: FK cleanup, idempotency, attachment guard and lock-contention retries (sync)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

from pg_partsmith.exceptions import PartitionAttachedError, PlanStaleError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.topology import RangeBounds
from tests.integration.sync.support import make_table

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration

_ORDERS_TABLE_DDL = """
    CREATE TABLE {table} (
        id BIGSERIAL PRIMARY KEY,
        name TEXT NOT NULL DEFAULT ''
    )
"""

_EVENTS_TABLE_DDL = """
    CREATE TABLE {table} (
        id        BIGSERIAL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
        order_id  BIGINT,
        data      TEXT,
        PRIMARY KEY (id, created_at)
    ) PARTITION BY RANGE (created_at)
"""


@pytest.fixture
def referenced_table(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, _ORDERS_TABLE_DDL, prefix="sd_orders")


@pytest.fixture
def partitioned_table(sync_db_engine: Engine, referenced_table: str) -> Generator[str, None]:
    yield from make_table(sync_db_engine, _EVENTS_TABLE_DDL, prefix="sd_events")


def _create_detached(engine: Engine, parent: str, partition_name: str, from_val: str, to_val: str) -> None:
    repo = PostgresPartitionRepository(engine)
    repo.create_table_like(parent, partition_name, None)
    repo.attach_partition(parent, partition_name, RangeBounds(from_value=from_val, to_value=to_val))
    repo.detach_partition(parent, partition_name, mode=DetachMode.BLOCKING)


# ── happy path ───────────────────────────────────────────────────────────────────


def test__drop_partition__detached_no_fk__drops_cleanly(
    sync_db_engine: Engine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_07"
    _create_detached(sync_db_engine, partitioned_table, name, "2024-07-01", "2024-08-01")
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act
    repo.drop_partition(name)

    # Assert
    assert not PostgresMetadataProvider(sync_db_engine).partition_exists(name)


# ── FK cleanup ────────────────────────────────────────────────────────────────────


def test__drop_partition__single_fk__removes_constraint_and_drops_table(
    sync_db_engine: Engine,
    sync_db_session: Session,
    partitioned_table: str,
    referenced_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_08"
    _create_detached(sync_db_engine, partitioned_table, name, "2024-08-01", "2024-09-01")

    fk_name = f"fk_{name}_order_id"
    sync_db_session.execute(
        text(
            f'ALTER TABLE "{name}" '
            f'ADD CONSTRAINT "{fk_name}" '
            f'FOREIGN KEY (order_id) REFERENCES "{referenced_table}"(id)'
        )
    )
    sync_db_session.commit()

    # Verify FK exists before drop
    result = sync_db_session.execute(
        text(
            "SELECT conname FROM pg_constraint con "
            "JOIN pg_class rel ON con.conrelid = rel.oid "
            "WHERE rel.relname = :p AND con.contype = 'f'"
        ),
        {"p": name},
    )
    assert result.scalar() == fk_name

    # Act
    repo = PostgresPartitionRepository(sync_db_engine)
    repo.drop_partition(name)

    # Assert — partition gone, referenced table survives
    assert not PostgresMetadataProvider(sync_db_engine).partition_exists(name)
    result = sync_db_session.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = :t AND relkind = 'r')"),
        {"t": referenced_table},
    )
    assert result.scalar() is True


def test__drop_partition__multiple_fks__removes_all_constraints(
    sync_db_engine: Engine,
    sync_db_session: Session,
    partitioned_table: str,
    referenced_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_09"
    _create_detached(sync_db_engine, partitioned_table, name, "2024-09-01", "2024-10-01")

    for i in range(1, 3):
        sync_db_session.execute(
            text(
                f'ALTER TABLE "{name}" '
                f'ADD CONSTRAINT "fk_{name}_order_{i}" '
                f'FOREIGN KEY (order_id) REFERENCES "{referenced_table}"(id)'
            )
        )
    sync_db_session.commit()

    # Act
    repo = PostgresPartitionRepository(sync_db_engine)
    repo.drop_partition(name)

    # Assert
    assert not PostgresMetadataProvider(sync_db_engine).partition_exists(name)


# ── idempotency ───────────────────────────────────────────────────────────────────


def test__drop_partition__double_drop__second_call_is_noop(
    sync_db_engine: Engine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_10"
    _create_detached(sync_db_engine, partitioned_table, name, "2024-10-01", "2024-11-01")
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act
    repo.drop_partition(name)
    repo.drop_partition(name)  # must be a no-op

    # Assert
    assert not PostgresMetadataProvider(sync_db_engine).partition_exists(name)


def test__drop_partition__completely_nonexistent__is_noop(sync_db_engine: Engine) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act / Assert — must not raise
    repo.drop_partition("sd_events__totally_nonexistent_xyz")


# ── safety ────────────────────────────────────────────────────────────────────────


def test__drop_partition__still_attached__raises_partition_attached_error(
    sync_db_engine: Engine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_11"
    repo = PostgresPartitionRepository(sync_db_engine)
    metadata = PostgresMetadataProvider(sync_db_engine)
    repo.create_table_like(partitioned_table, name, None)
    repo.attach_partition(partitioned_table, name, RangeBounds(from_value="2024-11-01", to_value="2024-12-01"))

    try:
        # Act / Assert
        with pytest.raises(PartitionAttachedError):
            repo.drop_partition(name)

        assert metadata.partition_exists(name)
        assert metadata.is_partition_attached(partitioned_table, name)
    finally:
        if metadata.is_partition_attached(partitioned_table, name):
            repo.detach_partition(partitioned_table, name, mode=DetachMode.BLOCKING)
        repo.drop_partition(name)


def test__drop_partition__expected_oid_of_another_relation__refused_and_the_table_survives(
    sync_db_engine: Engine,
    partitioned_table: str,
) -> None:
    # Arrange — the decision was made about a relation that has since been
    # dropped and recreated under the same name
    name = f"{partitioned_table}__2024_12"
    _create_detached(sync_db_engine, partitioned_table, name, "2024-12-01", "2025-01-01")
    metadata = PostgresMetadataProvider(sync_db_engine)
    real_oid = metadata.get_relation_oid(name)
    assert real_oid is not None
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act / Assert
    with pytest.raises(PlanStaleError):
        repo.drop_partition(name, expected_oid=real_oid + 1)
    assert metadata.partition_exists(name)

    # The right identity still drops it
    repo.drop_partition(name, expected_oid=real_oid)
    assert not metadata.partition_exists(name)


# ── retry on lock contention ──────────────────────────────────────────────────────
