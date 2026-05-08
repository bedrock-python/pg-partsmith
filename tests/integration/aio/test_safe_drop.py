from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import PartitionAttachedError

_ORDERS_TABLE = "sd_orders"
_EVENTS_TABLE = "sd_events"


@pytest_asyncio.fixture
async def referenced_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async with db_engine.begin() as conn:
        await conn.execute(
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
    async with db_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {_ORDERS_TABLE} CASCADE"))


@pytest_asyncio.fixture
async def partitioned_table(
    db_engine: AsyncEngine,
    referenced_table: str,
) -> AsyncGenerator[str, None]:
    async with db_engine.begin() as conn:
        await conn.execute(
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
    async with db_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {_EVENTS_TABLE} CASCADE"))


def _config(table_name: str) -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name=table_name,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )


async def _create_detached(engine: AsyncEngine, parent: str, partition_name: str, from_val: str, to_val: str) -> None:
    repo = PostgresPartitionRepository(engine)
    await repo.create_partition(_config(parent), partition_name, from_val, to_val)
    await repo.attach_partition(parent, partition_name, from_val, to_val)
    await repo.detach_partition(parent, partition_name, concurrent=False)


# ── happy path ───────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test__drop_partition__detached_no_fk__drops_cleanly(
    db_engine: AsyncEngine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_07"
    await _create_detached(db_engine, partitioned_table, name, "2024-07-01", "2024-08-01")
    repo = PostgresPartitionRepository(db_engine)

    # Act
    await repo.drop_partition(name)

    # Assert
    assert not await repo.partition_exists(name)


# ── FK cleanup ────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test__drop_partition__single_fk__removes_constraint_and_drops_table(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    partitioned_table: str,
    referenced_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_08"
    await _create_detached(db_engine, partitioned_table, name, "2024-08-01", "2024-09-01")

    fk_name = f"fk_{name}_order_id"
    await db_session.execute(
        text(
            f'ALTER TABLE "{name}" '
            f'ADD CONSTRAINT "{fk_name}" '
            f'FOREIGN KEY (order_id) REFERENCES "{referenced_table}"(id)'
        )
    )
    await db_session.commit()

    # Verify FK exists before drop
    result = await db_session.execute(
        text(
            "SELECT conname FROM pg_constraint con "
            "JOIN pg_class rel ON con.conrelid = rel.oid "
            "WHERE rel.relname = :p AND con.contype = 'f'"
        ),
        {"p": name},
    )
    assert result.scalar() == fk_name

    # Act
    repo = PostgresPartitionRepository(db_engine)
    await repo.drop_partition(name)

    # Assert — partition gone, referenced table survives
    assert not await repo.partition_exists(name)
    result = await db_session.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = :t AND relkind = 'r')"),
        {"t": referenced_table},
    )
    assert result.scalar() is True


@pytest.mark.integration
async def test__drop_partition__multiple_fks__removes_all_constraints(
    db_engine: AsyncEngine,
    db_session: AsyncSession,
    partitioned_table: str,
    referenced_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_09"
    await _create_detached(db_engine, partitioned_table, name, "2024-09-01", "2024-10-01")

    for i in range(1, 3):
        await db_session.execute(
            text(
                f'ALTER TABLE "{name}" '
                f'ADD CONSTRAINT "fk_{name}_order_{i}" '
                f'FOREIGN KEY (order_id) REFERENCES "{referenced_table}"(id)'
            )
        )
    await db_session.commit()

    # Act
    repo = PostgresPartitionRepository(db_engine)
    await repo.drop_partition(name)

    # Assert
    assert not await repo.partition_exists(name)


# ── idempotency ───────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test__drop_partition__double_drop__second_call_is_noop(
    db_engine: AsyncEngine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_10"
    await _create_detached(db_engine, partitioned_table, name, "2024-10-01", "2024-11-01")
    repo = PostgresPartitionRepository(db_engine)

    # Act
    await repo.drop_partition(name)
    await repo.drop_partition(name)  # must be a no-op

    # Assert
    assert not await repo.partition_exists(name)


@pytest.mark.integration
async def test__drop_partition__completely_nonexistent__is_noop(db_engine: AsyncEngine) -> None:
    # Arrange
    repo = PostgresPartitionRepository(db_engine)

    # Act / Assert — must not raise
    await repo.drop_partition("sd_events__totally_nonexistent_xyz")


# ── safety ────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test__drop_partition__still_attached__raises_partition_attached_error(
    db_engine: AsyncEngine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_11"
    repo = PostgresPartitionRepository(db_engine)
    await repo.create_partition(_config(partitioned_table), name, "2024-11-01", "2024-12-01")
    await repo.attach_partition(partitioned_table, name, "2024-11-01", "2024-12-01")

    try:
        # Act / Assert
        with pytest.raises(PartitionAttachedError):
            await repo.drop_partition(name)

        assert await repo.partition_exists(name)
        assert await repo.is_partition_attached(partitioned_table, name)
    finally:
        if await repo.is_partition_attached(partitioned_table, name):
            await repo.detach_partition(partitioned_table, name, concurrent=False)
        await repo.drop_partition(name)


# ── retry on lock contention ──────────────────────────────────────────────────────


@pytest.mark.integration
async def test__drop_partition__lock_contention__retries_and_succeeds_after_release(
    db_engine: AsyncEngine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2024_12"
    await _create_detached(db_engine, partitioned_table, name, "2024-12-01", "2025-01-01")

    lock_acquired = asyncio.Event()

    async def hold_exclusive_lock() -> None:
        async with db_engine.begin() as conn:
            await conn.execute(text(f'LOCK TABLE "{name}" IN EXCLUSIVE MODE'))
            lock_acquired.set()
            await asyncio.sleep(0.8)

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
        db_engine,
        drop_lock_timeout_ms=150,
        drop_max_retries=12,
        drop_retry_delay=0.2,
    )

    async def drop_after_lock() -> None:
        await lock_acquired.wait()
        with patch("pg_partsmith.aio.repositories.remover.logger", CapturingLogger()):
            await drop_repo.drop_partition(name)

    # Act
    await asyncio.gather(hold_exclusive_lock(), drop_after_lock())

    # Assert
    repo = PostgresPartitionRepository(db_engine)
    assert not await repo.partition_exists(name)
    assert len(retry_msgs) >= 1, (
        "Expected at least one retry warning; drop succeeded on first attempt without contention"
    )
