"""Scenarios that need two threads against one PostgreSQL (sync).

This is the one module of the sync integration suite that is not generated
from the aio suite: its aio twins drive two coroutines with ``asyncio.gather``
and are marked ``# sync-mirror: skip`` there.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING
from unittest.mock import patch

import freezegun
import pytest
from sqlalchemy import text

from pg_partsmith.entities import MaintenanceResult
from pg_partsmith.exceptions import LockAcquisitionError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.sync.maintainer import PartitionMaintainer
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.topology import RangeBounds
from tests.integration.nested_support import MONTHLY_TABLE_DDL, monthly_config
from tests.integration.sync.support import count_ddl, make_service, make_table

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration


@pytest.fixture
def partitioned_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="conc")


# ── Two maintainers on the same table ────────────────────────────────────────────


def test__maintainer__two_concurrent_runs_on_one_table__one_wins_the_lock_and_the_tree_converges(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange — two independent wirings, as two workers would have
    first = PartitionMaintainer(make_service(sync_db_engine))
    second = PartitionMaintainer(make_service(sync_db_engine))
    config = monthly_config(partitioned_table, create_ahead=3, retention=12)

    def tick(maintainer: PartitionMaintainer) -> MaintenanceResult | LockAcquisitionError:
        try:
            return maintainer.run_maintenance(config)
        except LockAcquisitionError as exc:
            return exc

    # Act — both ticks at once
    with freezegun.freeze_time("2026-08-26"), ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(tick, (first, second)))

    # Assert — each run either won the lock or lost it; nothing else may happen
    results = [o for o in outcomes if isinstance(o, MaintenanceResult)]
    losers = [o for o in outcomes if isinstance(o, LockAcquisitionError)]
    assert len(results) + len(losers) == 2
    assert len(results) >= 1
    assert sum(r.created_count for r in results) == 3

    # The tree ends converged either way: nothing left to do
    with freezegun.freeze_time("2026-08-26"), count_ddl(sync_db_engine) as counter:
        again = first.run_maintenance(config)
    assert again.created_count == 0
    assert counter.statements == []
    names = {p.relname for p in PostgresMetadataProvider(sync_db_engine).list_partitions(partitioned_table)}
    assert names == {f"{partitioned_table}__2026_08", f"{partitioned_table}__2026_09", f"{partitioned_table}__2026_10"}


# ── retry on lock contention ──────────────────────────────────────────────────────


def _create_detached(engine: Engine, parent: str, partition_name: str, from_val: str, to_val: str) -> None:
    repo = PostgresPartitionRepository(engine)
    repo.create_table_like(parent, partition_name, None)
    repo.attach_partition(parent, partition_name, RangeBounds(from_value=from_val, to_value=to_val))
    repo.detach_partition(parent, partition_name, mode=DetachMode.BLOCKING)


def test__drop_partition__lock_contention__retries_and_succeeds_after_release(
    sync_db_engine: Engine,
    partitioned_table: str,
) -> None:
    # Arrange
    name = f"{partitioned_table}__2025_01"
    _create_detached(sync_db_engine, partitioned_table, name, "2025-01-01", "2025-02-01")

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
    assert not PostgresMetadataProvider(sync_db_engine).partition_exists(name)
    assert len(retry_msgs) >= 1, (
        "Expected at least one retry warning; drop succeeded on first attempt without contention"
    )
