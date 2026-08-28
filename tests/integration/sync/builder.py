"""Builder pattern for partitioning integration tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import freezegun
from sqlalchemy import text

from pg_partsmith.entities import PartitionGranularity, Period, TablePartitionConfig
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.maintainer import PartitionMaintainer
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.sync.service import PartitionLifecycleService
from pg_partsmith.topology import RangeBounds

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from pg_partsmith.entities import MaintenanceResult
    from pg_partsmith.protocols import PeriodCalculator
    from pg_partsmith.sync.hooks import PartitionLifecycleHooks


@dataclass
class PreCreatedPartition:
    name: str
    from_value: str
    to_value: str
    attached: bool


@dataclass
class FKConfig:
    partition_name: str
    referenced_table: str


class PartitioningScenarioBuilder:
    """Builder for configuring initial database state for partitioning tests."""

    def __init__(self, engine: Engine, table_name: str):
        self._engine = engine
        self._table_name = table_name
        self._granularity = PartitionGranularity.MONTH
        self._create_ahead = 1
        self._retention = 12
        self._partitions: list[PreCreatedPartition] = []
        self._fks: list[FKConfig] = []
        self._hooks: list[PartitionLifecycleHooks] = []
        self._create_default = False

    def with_granularity(self, granularity: PartitionGranularity) -> PartitioningScenarioBuilder:
        self._granularity = granularity
        return self

    def with_create_ahead(self, n: int) -> PartitioningScenarioBuilder:
        self._create_ahead = n
        return self

    def with_retention(self, n: int) -> PartitioningScenarioBuilder:
        self._retention = n
        return self

    def with_attached_partition(self, name: str, from_val: str, to_val: str) -> PartitioningScenarioBuilder:
        self._partitions.append(PreCreatedPartition(name, from_val, to_val, attached=True))
        return self

    def with_detached_partition(self, name: str, from_val: str, to_val: str) -> PartitioningScenarioBuilder:
        """Add a partition the library detached when its window closed.

        The orphan marker records the detach instant, so the detach runs with
        the clock frozen at the partition's upper bound: any maintenance run
        frozen after the window then sees the (zero) grace elapsed.
        """
        self._partitions.append(PreCreatedPartition(name, from_val, to_val, attached=False))
        return self

    def with_fk_on_partition(self, partition_name: str, referenced_table: str) -> PartitioningScenarioBuilder:
        self._fks.append(FKConfig(partition_name, referenced_table))
        return self

    def with_hooks(self, hooks: list[PartitionLifecycleHooks]) -> PartitioningScenarioBuilder:
        self._hooks.extend(hooks)
        return self

    def with_default_partition(self) -> PartitioningScenarioBuilder:
        """Configure test to create a DEFAULT partition."""
        self._create_default = True
        return self

    def build(self) -> PartitioningTestContext:
        """Builds the test context and sets up the database state."""
        repo = PostgresPartitionRepository(self._engine)
        metadata = PostgresMetadataProvider(self._engine)
        locks = PostgresAdvisoryLockManager(self._engine)

        config = TablePartitionConfig(
            table_name=self._table_name,
            partition_column="created_at",
            granularity=self._granularity,
            create_ahead_count=self._create_ahead,
            retention_count=self._retention,
        )
        boundaries = config.time_boundaries
        assert boundaries is not None  # the builder always describes a time-partitioned root
        calc = boundaries.period_calculator

        # Create DEFAULT partition if requested
        if self._create_default:
            with self._engine.begin() as conn:
                conn.execute(
                    text(f'CREATE TABLE "{self._table_name}_default" PARTITION OF "{self._table_name}" DEFAULT')
                )

        # Setup pre-created partitions through the library's own DDL path.
        for p in self._partitions:
            repo.create_table_like(self._table_name, p.name, None)
            repo.attach_partition(self._table_name, p.name, RangeBounds(from_value=p.from_value, to_value=p.to_value))
            if not p.attached:
                with freezegun.freeze_time(p.to_value):
                    repo.detach_partition(self._table_name, p.name, mode=DetachMode.BLOCKING)

        # Setup FKs
        with self._engine.begin() as conn:
            for fk in self._fks:
                conn.execute(
                    text(
                        f'ALTER TABLE "{fk.partition_name}" '
                        f'ADD CONSTRAINT "fk_{fk.partition_name}_ref" '
                        f'FOREIGN KEY (id) REFERENCES "{fk.referenced_table}"(id)'
                    )
                )

        service = PartitionLifecycleService(repo, metadata, locks, hooks=self._hooks)
        maintainer = PartitionMaintainer(service)

        return PartitioningTestContext(
            engine=self._engine,
            table_name=self._table_name,
            config=config,
            repo=repo,
            metadata=metadata,
            locks=locks,
            calc=calc,
            service=service,
            maintainer=maintainer,
        )


@dataclass
class PartitioningTestContext:
    """Test context with components and assertion helpers."""

    engine: Engine
    table_name: str
    config: TablePartitionConfig
    repo: PostgresPartitionRepository
    metadata: PostgresMetadataProvider
    locks: PostgresAdvisoryLockManager
    calc: PeriodCalculator[Period]
    service: PartitionLifecycleService
    maintainer: PartitionMaintainer

    def run_maintenance(
        self,
        at_time: str | datetime | None = None,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Runs maintenance, optionally at a specific time."""
        if at_time:
            with freezegun.freeze_time(at_time):
                return self.maintainer.run_maintenance(
                    self.config,
                    skip_create=skip_create,
                    skip_detach=skip_detach,
                    skip_drop=skip_drop,
                    continue_on_error=continue_on_error,
                )
        return self.maintainer.run_maintenance(
            self.config,
            skip_create=skip_create,
            skip_detach=skip_detach,
            skip_drop=skip_drop,
            continue_on_error=continue_on_error,
        )

    def assert_partition_exists(self, name: str) -> None:
        """Asserts that a partition table exists in the database."""
        exists = self.metadata.partition_exists(name)
        assert exists, f"Partition {name} does not exist"

    def assert_partition_not_exists(self, name: str) -> None:
        """Asserts that a partition table does not exist in the database."""
        exists = self.metadata.partition_exists(name)
        assert not exists, f"Partition {name} exists, but should not"

    def assert_partition_attached(self, name: str) -> None:
        """Asserts that a partition is attached to the parent table."""
        attached = self.metadata.is_partition_attached(self.table_name, name)
        assert attached, f"Partition {name} is not attached to {self.table_name}"

    def assert_partition_detached(self, name: str) -> None:
        """Asserts that a partition is NOT attached to the parent table."""
        attached = self.metadata.is_partition_attached(self.table_name, name)
        assert not attached, f"Partition {name} is attached to {self.table_name}, but should be detached"

    def assert_partition_count(self, expected: int) -> None:
        """Asserts the number of attached partitions."""
        partitions = self.metadata.list_partitions(self.table_name)
        attached = [p for p in partitions if p.is_attached]
        assert len(attached) == expected, f"Expected {expected} attached partitions, got {len(attached)}"

    def list_partition_names(self, attached_only: bool = True) -> list[str]:
        """Returns a list of schema-qualified partition names."""
        partitions = self.metadata.list_partitions(self.table_name)
        if attached_only:
            return [p.name for p in partitions if p.is_attached]
        return [p.name for p in partitions]
