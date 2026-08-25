from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from sqlalchemy import text

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import PartitionAttachedError
from pg_partsmith.sync.repositories import PostgresPartitionRepository

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

_ORDERS_TABLE = "ssd_orders"
_EVENTS_TABLE = "ssd_events"


@pytest.fixture
def referenced_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {_ORDERS_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT ''
                )
                """
            )
        )
    yield _ORDERS_TABLE
    with sync_db_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {_ORDERS_TABLE} CASCADE"))


@pytest.fixture
def partitioned_table(
    sync_db_engine: Engine,
    referenced_table: str,
) -> Generator[str, None, None]:
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {_EVENTS_TABLE} (
                    id        BIGSERIAL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    order_id  BIGINT,
                    data      TEXT,
                    PRIMARY KEY (id, created_at)
                ) PARTITION BY RANGE (created_at)
                """
            )
        )
    yield _EVENTS_TABLE
    with sync_db_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {_EVENTS_TABLE} CASCADE"))


def _config(table_name: str) -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name=table_name,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )


def _create_detached(engine: Engine, parent: str, partition_name: str, from_val: str, to_val: str) -> None:
    repo = PostgresPartitionRepository(engine)
    repo.create_partition(_config(parent), partition_name, from_val, to_val)
    repo.attach_partition(parent, partition_name, from_val, to_val)
    repo.detach_partition(parent, partition_name, concurrent=False)


# ── happy path ───────────────────────────────────────────────────────────────────


@pytest.mark.integration
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
    assert not repo.partition_exists(name)


# ── FK cleanup ────────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test__drop_partition__single_fk__removes_constraint_and_drops_table(
    sync_db_engine: Engine,
    partitioned_table: str,
    referenced_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_08"
    _create_detached(sync_db_engine, partitioned_table, name, "2024-08-01", "2024-09-01")

    fk_name = f"fk_{name}_order_id"
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                f'ALTER TABLE "{name}" '
                f'ADD CONSTRAINT "{fk_name}" '
                f'FOREIGN KEY (order_id) REFERENCES "{referenced_table}"(id)'
            )
        )

    # Verify FK exists before drop
    with sync_db_engine.begin() as conn:
        result = conn.execute(
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
    assert not repo.partition_exists(name)
    with sync_db_engine.begin() as conn:
        result = conn.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = :t AND relkind = 'r')"),
            {"t": referenced_table},
        )
        assert result.scalar() is True


@pytest.mark.integration
def test__drop_partition__multiple_fks__removes_all_constraints(
    sync_db_engine: Engine,
    partitioned_table: str,
    referenced_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_09"
    _create_detached(sync_db_engine, partitioned_table, name, "2024-09-01", "2024-10-01")

    with sync_db_engine.begin() as conn:
        for i in range(1, 3):
            conn.execute(
                text(
                    f'ALTER TABLE "{name}" '
                    f'ADD CONSTRAINT "fk_{name}_order_{i}" '
                    f'FOREIGN KEY (order_id) REFERENCES "{referenced_table}"(id)'
                )
            )

    # Act
    repo = PostgresPartitionRepository(sync_db_engine)
    repo.drop_partition(name)

    # Assert
    assert not repo.partition_exists(name)


# ── idempotency ───────────────────────────────────────────────────────────────────


@pytest.mark.integration
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
    assert not repo.partition_exists(name)


@pytest.mark.integration
def test__drop_partition__completely_nonexistent__is_noop(sync_db_engine: Engine) -> None:
    # Arrange
    repo = PostgresPartitionRepository(sync_db_engine)

    # Act / Assert — must not raise
    repo.drop_partition("ssd_events__totally_nonexistent_xyz")


# ── safety ────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test__drop_partition__still_attached__raises_partition_attached_error(
    sync_db_engine: Engine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_11"
    repo = PostgresPartitionRepository(sync_db_engine)
    repo.create_partition(_config(partitioned_table), name, "2024-11-01", "2024-12-01")
    repo.attach_partition(partitioned_table, name, "2024-11-01", "2024-12-01")

    try:
        # Act / Assert
        with pytest.raises(PartitionAttachedError):
            repo.drop_partition(name)

        assert repo.partition_exists(name)
        assert repo.is_partition_attached(partitioned_table, name)
    finally:
        if repo.is_partition_attached(partitioned_table, name):
            repo.detach_partition(partitioned_table, name, concurrent=False)
        repo.drop_partition(name)


# ── retry on lock contention ──────────────────────────────────────────────────────


@pytest.mark.integration
def test__drop_partition__lock_contention__retries_and_succeeds_after_release(
    sync_db_engine: Engine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_12"
    _create_detached(sync_db_engine, partitioned_table, name, "2024-12-01", "2025-01-01")

    lock_acquired = threading.Event()

    def hold_exclusive_lock() -> None:
        with sync_db_engine.begin() as conn:
            conn.execute(text(f'LOCK TABLE "{name}" IN EXCLUSIVE MODE'))
            lock_acquired.set()
            time.sleep(0.8)

    retry_msgs: list[str] = []

    class CapturingLogger:
        def debug(self, event: str, **kwargs: object) -> None: ...
        def info(self, event: str, **kwargs: object) -> None: ...
        def warning(self, event: str, **kwargs: object) -> None:
            retry_msgs.append(event)

        def error(self, event: str, **kwargs: object) -> None: ...
        def fatal(self, event: str, **kwargs: object) -> None: ...
        def exception(self, event: str, **kwargs: object) -> None: ...
        def critical(self, event: str, **kwargs: object) -> None: ...

    drop_repo = PostgresPartitionRepository(
        sync_db_engine,
        drop_lock_timeout_ms=150,
        drop_max_retries=12,
        drop_retry_delay=0.2,
    )

    holder = threading.Thread(target=hold_exclusive_lock)

    # Act
    holder.start()
    try:
        assert lock_acquired.wait(timeout=10), "Lock holder thread failed to acquire the exclusive lock"
        with patch("pg_partsmith.sync.repositories.remover.logger", CapturingLogger()):
            drop_repo.drop_partition(name)
    finally:
        holder.join()

    # Assert
    repo = PostgresPartitionRepository(sync_db_engine)
    assert not repo.partition_exists(name)
    assert len(retry_msgs) >= 1, (
        "Expected at least one retry warning; drop succeeded on first attempt without contention"
    )
