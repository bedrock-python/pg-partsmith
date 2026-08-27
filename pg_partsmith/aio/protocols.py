"""Async protocols (interfaces) for partition management."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Protocol, runtime_checkable

from pg_partsmith.entities import MaintenanceResult, PartitionInfo, PartitionType, TablePartitionConfig

__all__ = [
    "LockManager",
    "PartitionLifecycle",
    "PartitionMetadataProvider",
    "PartitionRepository",
]


@runtime_checkable
class PartitionLifecycle(Protocol):
    """Interface for running a full maintenance lifecycle.

    This protocol exists so orchestrators (e.g. ``PartitionMaintainer``) depend
    on behaviour rather than a concrete implementation class.
    """

    async def maintain_lifecycle(
        self,
        config: TablePartitionConfig,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
        continue_on_error: bool = False,
    ) -> MaintenanceResult: ...


@runtime_checkable
class PartitionRepository(Protocol):
    """Repository for partition DDL operations.

    This protocol is intentionally limited to write operations.  All read
    operations (listing partitions, checking existence) live in
    ``PartitionMetadataProvider`` so that the two concerns can be mocked,
    swapped, or overridden independently.
    """

    async def create_partition(
        self, config: TablePartitionConfig, partition_name: str, from_value: str, to_value: str
    ) -> PartitionInfo:
        """Create a new partition table.

        Args:
            config: Table partition configuration.
            partition_name: Name for the new partition table.
            from_value: Start boundary value.
            to_value: End boundary value.

        Returns:
            Created partition info.

        Raises:
            PartitionAlreadyExistsError: If partition already exists.
        """
        ...

    async def attach_partition(self, table_name: str, partition_name: str, from_value: str, to_value: str) -> None:
        """Attach partition to parent table.

        Args:
            table_name: Parent table name.
            partition_name: Partition table name.
            from_value: Start boundary value.
            to_value: End boundary value.
        """
        ...

    async def detach_partition(self, table_name: str, partition_name: str, *, concurrent: bool = True) -> None:
        """Detach partition from parent table.

        Args:
            table_name: Parent table name.
            partition_name: Partition table name.
            concurrent: Use DETACH PARTITION CONCURRENTLY if supported.

        Raises:
            PartitionNotFoundError: If partition doesn't exist.
        """
        ...

    async def drop_partition(self, partition_name: str) -> None:
        """Drop a partition table.

        Args:
            partition_name: Partition table name.

        Raises:
            PartitionAttachedError: If partition is still attached.
        """
        ...

    async def reconcile_default_rows(
        self,
        *,
        default_partition_name: str,
        target_partition_name: str,
        partition_column: str,
        from_value: str,
        to_value: str,
    ) -> int:
        """Move rows from DEFAULT partition to target partition for given range.

        Args:
            default_partition_name: Qualified name of DEFAULT partition.
            target_partition_name: Qualified name of target partition.
            partition_column: Column used for partitioning.
            from_value: Range start boundary (inclusive).
            to_value: Range end boundary (exclusive).

        Returns:
            Number of rows moved.

        Raises:
            SQLAlchemyError: On database errors.
        """
        ...


@runtime_checkable
class PartitionMetadataProvider(Protocol):
    """Provider for reading partition metadata from the database catalogue.

    This protocol owns *all* read operations so that the service layer depends
    on a single injectable read interface.  Implement this protocol to support a
    different database, a caching layer, or a stub for testing.
    """

    async def get_partition_type(self, table_name: str) -> PartitionType | None:
        """Get partition type for a table.

        Args:
            table_name: Table name.

        Returns:
            Partition type or None if table is not partitioned.
        """
        ...

    async def get_partition_column(self, table_name: str) -> str | None:
        """Get partition column for a table.

        Args:
            table_name: Table name.

        Returns:
            Partition column name or None if table is not partitioned.

        Raises:
            ValueError: If the table uses a composite (multi-column) partition key.
        """
        ...

    async def get_partition_boundaries(self, partition_name: str) -> tuple[str, str] | None:
        """Get partition boundaries.

        Args:
            partition_name: Partition table name.

        Returns:
            Tuple of (from_value, to_value) or None if not a range partition.
        """
        ...

    async def list_partitions(self, table_name: str) -> list[PartitionInfo]:
        """List all partitions for a table, including orphaned detached ones.

        Orphaned partitions are tables that were detached in a previous
        maintenance run but never dropped.  They are returned with
        ``is_attached=False`` and ``None`` boundaries so that the service can
        schedule them for cleanup on the next run.

        Args:
            table_name: Parent table name.

        Returns:
            List of partition metadata.
        """
        ...

    async def partition_exists(self, partition_name: str) -> bool:
        """Check if a partition table exists in the catalogue.

        Args:
            partition_name: Partition table name.

        Returns:
            True if the table exists.
        """
        ...

    async def is_partition_attached(self, table_name: str, partition_name: str) -> bool:
        """Check if a partition is currently attached to its parent table.

        Args:
            table_name: Parent table name.
            partition_name: Partition table name.

        Returns:
            True if the partition is attached via pg_inherits.
        """
        ...

    async def get_default_partition(self, table_name: str) -> PartitionInfo | None:
        """Get DEFAULT partition for a table if it exists and is attached.

        Args:
            table_name: Parent table name.

        Returns:
            PartitionInfo with is_default=True, or None if no default partition exists.
        """
        ...


@runtime_checkable
class LockManager(Protocol):
    """Lock manager for coordinating partition operations.

    This protocol defines the interface for acquiring and releasing locks
    to prevent concurrent partition operations on the same table.
    Implement this to use a different locking backend (e.g. Zookeeper).
    """

    def acquire_lock(self, table_name: str) -> AbstractAsyncContextManager[None]:
        """Acquire lock for partition operations on a table.

        Args:
            table_name: Table name to lock.

        Returns:
            Async context manager for the lock.

        Raises:
            LockAcquisitionError: If unable to acquire lock.
        """
        ...

    async def is_locked(self, table_name: str) -> bool:
        """Check if table is currently locked.

        Args:
            table_name: Table name.

        Returns:
            True if table is locked.
        """
        ...
