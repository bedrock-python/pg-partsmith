"""The maintainer orchestrating the lifecycle service against a real PostgreSQL (sync)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import freezegun
import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.events import PartitionEvent
from pg_partsmith.exceptions import PlanStaleError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.maintainer import PartitionMaintainer
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.sync.service import PartitionLifecycleService
from pg_partsmith.topology import RangeBounds
from tests.integration.nested_support import MONTHLY_TABLE_DDL, monthly_config
from tests.integration.sync.support import (
    count_ddl,
    is_attached,
    make_table,
    table_comment,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration


@pytest.fixture
def partitioned_table(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="maint")


def _monthly(table_name: str) -> TablePartitionConfig:
    """The plain monthly config, for the methods that take one to build hook events."""
    return TablePartitionConfig(
        table_name=table_name,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )


def _make_components(
    engine: Engine,
) -> tuple[PostgresPartitionRepository, PostgresMetadataProvider, PostgresAdvisoryLockManager]:
    return (
        PostgresPartitionRepository(engine),
        PostgresMetadataProvider(engine),
        PostgresAdvisoryLockManager(engine),
    )


# ── PartitionMaintainer lifecycle ────────────────────────────────────────────────


def test__maintainer__initial_run__creates_partitions_ahead(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange — the 0.x spelling, type and strategy included, is still accepted
    repo, metadata, locks = _make_components(sync_db_engine)
    service = PartitionLifecycleService(repo, metadata, locks)
    maintainer = PartitionMaintainer(service)
    config = TablePartitionConfig(
        table_name=partitioned_table,
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=2,
        retention_count=6,
    )

    # Act
    with freezegun.freeze_time("2024-12-01"):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.created_count == 2
    # list_partitions always returns schema-qualified names
    partitions = metadata.list_partitions(partitioned_table)
    names = {p.name for p in partitions}
    assert f"public.{partitioned_table}__2024_12" in names
    assert f"public.{partitioned_table}__2025_01" in names


def test__maintainer__second_run_same_month__creates_zero_partitions(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks = _make_components(sync_db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, create_ahead=1, retention=12)

    # Act
    with freezegun.freeze_time("2024-06-01"):
        r1 = maintainer.run_maintenance(config)
        with count_ddl(sync_db_engine) as counter:
            r2 = maintainer.run_maintenance(config)

    # Assert
    assert r1.created_count == 1
    assert r2.created_count == 0
    assert counter.statements == []


def test__maintainer__partitions_beyond_retention__detaches_and_drops_them(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks = _make_components(sync_db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, create_ahead=2, retention=3)

    with freezegun.freeze_time("2024-12-01"):
        maintainer.run_maintenance(config)

    # Act — advance to May 2025: Dec 2024 and Jan 2025 fall outside retention
    with freezegun.freeze_time("2025-05-01"):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.detached_count == 2
    assert result.dropped_count == 2
    partitions = metadata.list_partitions(partitioned_table)
    names = {p.name for p in partitions}
    assert f"public.{partitioned_table}__2024_12" not in names
    assert f"public.{partitioned_table}__2025_01" not in names
    assert f"public.{partitioned_table}__2025_05" in names
    assert f"public.{partitioned_table}__2025_06" in names


def test__maintainer__still_attached_partition__skips_drop(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks = _make_components(sync_db_engine)
    service = PartitionLifecycleService(repo, metadata, locks)

    p1 = f"{partitioned_table}__2024_01"
    p2 = f"{partitioned_table}__2024_02"
    repo.create_table_like(partitioned_table, p1, None)
    repo.attach_partition(partitioned_table, p1, RangeBounds(from_value="2024-01-01", to_value="2024-02-01"))
    repo.create_table_like(partitioned_table, p2, None)
    repo.attach_partition(partitioned_table, p2, RangeBounds(from_value="2024-02-01", to_value="2024-03-01"))
    repo.detach_partition(partitioned_table, p1, mode=DetachMode.BLOCKING)

    # Act — try to drop both; only p1 is an orphan
    dropped = service.drop_detached_partitions(_monthly(partitioned_table), [p1, p2])

    # Assert
    assert dropped == 1
    assert not metadata.partition_exists(p1)
    assert metadata.partition_exists(p2)
    assert metadata.is_partition_attached(partitioned_table, p2)


def test__maintainer__detach_fails_one_run__drops_orphan_on_next_run(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange
    repo, metadata, locks = _make_components(sync_db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)

    with freezegun.freeze_time("2024-01-01"):
        maintainer.run_maintenance(config)

    with (
        patch.object(repo, "detach_partition", side_effect=SQLAlchemyError("detach failed")),
        freezegun.freeze_time("2024-03-01"),
        pytest.raises(SQLAlchemyError),
    ):
        maintainer.run_maintenance(config)

    # Act — retry without the fault injection
    with freezegun.freeze_time("2024-03-01"):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert not metadata.partition_exists(f"{partitioned_table}__2024_01")


def test__maintainer__hooks_called_at_lifecycle_points(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks = _make_components(sync_db_engine)
    hook_events: list[str] = []

    class AuditHooks(BasePartitionLifecycleHooks):
        def after_create(self, event: PartitionEvent) -> None:
            hook_events.append(f"created:{event.partition.name}")

        def before_drop(self, event: PartitionEvent) -> None:
            hook_events.append(f"before_drop:{event.partition.name}")

        def after_drop(self, event: PartitionEvent) -> None:
            hook_events.append(f"dropped:{event.partition.name}")

    service = PartitionLifecycleService(repo, metadata, locks, hooks=[AuditHooks()])
    maintainer = PartitionMaintainer(service)
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)

    # Act — first run creates
    with freezegun.freeze_time("2024-01-01"):
        maintainer.run_maintenance(config)

    assert hook_events == [f"created:public.{partitioned_table}__2024_01"]

    # Act — second run drops old
    with freezegun.freeze_time("2024-04-01"):
        maintainer.run_maintenance(config)

    # Assert
    assert hook_events == [
        f"created:public.{partitioned_table}__2024_01",
        f"created:public.{partitioned_table}__2024_04",
        f"before_drop:public.{partitioned_table}__2024_01",
        f"dropped:public.{partitioned_table}__2024_01",
    ]


def test__maintainer__orphaned_partition__dropped_on_next_run(sync_db_engine: Engine, partitioned_table: str) -> None:
    # Arrange
    repo, metadata, locks = _make_components(sync_db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)

    partition_name = f"{partitioned_table}__2024_01"
    repo.create_table_like(partitioned_table, partition_name, None)
    repo.attach_partition(
        partitioned_table, partition_name, RangeBounds(from_value="2024-01-01", to_value="2024-02-01")
    )
    # Simulate an interrupted previous run: detached (at the time) but not dropped
    with freezegun.freeze_time("2024-02-01"):
        repo.detach_partition(partitioned_table, partition_name, mode=DetachMode.BLOCKING)
    assert metadata.partition_exists(partition_name)

    # Act
    with freezegun.freeze_time("2024-04-01"):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success
    assert result.dropped_count == 1
    assert not metadata.partition_exists(partition_name)


def test__maintainer__run_maintenance_safe__never_raises_and_reports_the_error(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange — a config that does not match the table
    repo, metadata, locks = _make_components(sync_db_engine)
    maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
    config = monthly_config(partitioned_table, column="payload")

    # Act
    result = maintainer.run_maintenance_safe(config)

    # Assert
    assert not result.success
    assert result.error is not None
    assert "InvalidPartitionConfigError" in result.error
    assert result.created_count == 0


# ── Two maintainers on the same table ────────────────────────────────────────────


# ── An interrupted DETACH CONCURRENTLY ──────────────────────────────────────────


# ── A hook must not be able to redirect a detach ────────────────────────────────


class _SwapHooks(BasePartitionLifecycleHooks):
    """Replaces the partition at the planned name from inside ``before_detach``."""

    def __init__(self, engine: Engine, parent: str) -> None:
        self._engine = engine
        self._parent = parent

    def before_detach(self, event: PartitionEvent) -> None:
        name = event.partition.relname  # type: ignore[attr-defined]
        lower, upper = event.partition.from_value, event.partition.to_value  # type: ignore[attr-defined]
        with self._engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{self._parent}" DETACH PARTITION "{name}"'))
            conn.execute(text(f'DROP TABLE "{name}"'))
            conn.execute(text(f'CREATE TABLE "{name}" (LIKE "{self._parent}" INCLUDING ALL)'))
            conn.execute(
                text(
                    f'ALTER TABLE "{self._parent}" ATTACH PARTITION "{name}" '
                    f"FOR VALUES FROM ('{lower}') TO ('{upper}')"
                )
            )


def test__maintainer__relation_swapped_by_a_hook__detach_refused_as_stale(
    sync_db_engine: Engine, partitioned_table: str
) -> None:
    # Arrange: April expired; a hook swaps in a different relation under its name
    config = monthly_config(partitioned_table, create_ahead=1, retention=1)
    hook = _SwapHooks(sync_db_engine, partitioned_table)
    service = PartitionLifecycleService(*_make_components(sync_db_engine), hooks=[hook])
    with freezegun.freeze_time("2026-04-15"):
        service.maintain(config)

    # Act / Assert: the repository re-checks identity under its own lock and refuses
    with freezegun.freeze_time("2026-08-26"), pytest.raises(PlanStaleError):
        service.maintain(config)

    # The replacement is untouched: still attached, never marked
    april = f"{partitioned_table}__2026_04"
    assert is_attached(sync_db_engine, april)
    assert table_comment(sync_db_engine, april) is None
