"""Builder pattern for partitioning integration tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import freezegun
from sqlalchemy import text

from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.maintainer import PartitionMaintainer
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    Period,
    TablePartitionConfig,
)
from pg_partsmith.protocols import PeriodCalculator
from pg_partsmith.strategies.selector import get_period_calculator

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from pg_partsmith.aio.hooks import PartitionLifecycleHooks
    from pg_partsmith.entities import MaintenanceResult


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

    def __init__(self, engine: AsyncEngine, table_name: str):
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

    async def build(self) -> PartitioningTestContext:
        """Builds the test context and sets up the database state."""
        repo = PostgresPartitionRepository(self._engine)
        metadata = PostgresMetadataProvider(self._engine)
        locks = PostgresAdvisoryLockManager(self._engine)

        calc = get_period_calculator(self._granularity)

        config = TablePartitionConfig(
            table_name=self._table_name,
            partition_type=PartitionType.RANGE,
            partition_strategy=PartitionStrategy.TIME_BASED,
            partition_column="created_at",
            granularity=self._granularity,
            create_ahead_count=self._create_ahead,
            retention_count=self._retention,
        )

        # Create DEFAULT partition if requested
        if self._create_default:
            async with self._engine.begin() as conn:
                await conn.execute(
                    text(f'CREATE TABLE "{self._table_name}_default" PARTITION OF "{self._table_name}" DEFAULT')
                )

        # Setup pre-created partitions
        for p in self._partitions:
            await repo.create_partition(config, p.name, p.from_value, p.to_value)
            if p.attached:
                await repo.attach_partition(self._table_name, p.name, p.from_value, p.to_value)
            else:
                # To make it an orphan, we need to add the comment marker if detach_partition does it
                # Actually repo.create_partition + repo.attach_partition + repo.detach_partition is better
                # but if it's already detached we just need the marker.
                # Let's use the repo methods to be sure.
                await repo.attach_partition(self._table_name, p.name, p.from_value, p.to_value)
                await repo.detach_partition(self._table_name, p.name, concurrent=False)

        # Setup FKs
        async with self._engine.begin() as conn:
            for fk in self._fks:
                await conn.execute(
                    text(
                        f'ALTER TABLE "{fk.partition_name}" '
                        f'ADD CONSTRAINT "fk_{fk.partition_name}_ref" '
                        f'FOREIGN KEY (id) REFERENCES "{fk.referenced_table}"(id)'
                    )
                )

        service = PartitionLifecycleService(repo, metadata, locks, calc, hooks=self._hooks)
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

    engine: AsyncEngine
    table_name: str
    config: TablePartitionConfig
    repo: PostgresPartitionRepository
    metadata: PostgresMetadataProvider
    locks: PostgresAdvisoryLockManager
    calc: PeriodCalculator[Period]
    service: PartitionLifecycleService
    maintainer: PartitionMaintainer

    async def run_maintenance(
        self,
        at_time: str | datetime | None = None,
        *,
        skip_create: bool = False,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Runs maintenance, optionally at a specific time."""
        if at_time:
            with freezegun.freeze_time(at_time):
                return await self.maintainer.run_maintenance(
                    self.config, skip_create=skip_create, continue_on_error=continue_on_error
                )
        return await self.maintainer.run_maintenance(
            self.config, skip_create=skip_create, continue_on_error=continue_on_error
        )

    async def assert_partition_exists(self, name: str) -> None:
        """Asserts that a partition table exists in the database."""
        exists = await self.repo.partition_exists(name)
        assert exists, f"Partition {name} does not exist"

    async def assert_partition_not_exists(self, name: str) -> None:
        """Asserts that a partition table does not exist in the database."""
        exists = await self.repo.partition_exists(name)
        assert not exists, f"Partition {name} exists, but should not"

    async def assert_partition_attached(self, name: str) -> None:
        """Asserts that a partition is attached to the parent table."""
        attached = await self.repo.is_partition_attached(self.table_name, name)
        assert attached, f"Partition {name} is not attached to {self.table_name}"

    async def assert_partition_detached(self, name: str) -> None:
        """Asserts that a partition is NOT attached to the parent table."""
        attached = await self.repo.is_partition_attached(self.table_name, name)
        assert not attached, f"Partition {name} is attached to {self.table_name}, but should be detached"

    async def assert_partition_count(self, expected: int) -> None:
        """Asserts the number of attached partitions."""
        partitions = await self.metadata.list_partitions(self.table_name)
        attached = [p for p in partitions if p.is_attached]
        assert len(attached) == expected, f"Expected {expected} attached partitions, got {len(attached)}"

    async def list_partition_names(self, attached_only: bool = True) -> list[str]:
        """Returns a list of schema-qualified partition names."""
        partitions = await self.metadata.list_partitions(self.table_name)
        if attached_only:
            return [p.name for p in partitions if p.is_attached]
        return [p.name for p in partitions]
