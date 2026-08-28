"""Async protocols (interfaces) for partition management."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Protocol, runtime_checkable

from pg_partsmith.entities import (
    HashBounds,
    MaintenanceResult,
    PartitionInfo,
    PartitionNode,
    PartitionType,
    SubpartitionSpec,
    TablePartitionConfig,
)

__all__ = [
    "LockManager",
    "NestedPartitionMetadata",
    "PartitionLifecycle",
    "PartitionMetadataProvider",
    "PartitionRepository",
    "SubpartitionRepository",
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
    ) -> MaintenanceResult:
        """Run create + detach + drop in a single locked maintenance window.

        Args:
            config: Table partitioning configuration.
            skip_create: Skip the create-ahead step.
            skip_detach: Skip detaching old partitions (orphans are still dropped).
            skip_drop: Skip dropping detached partitions.
            continue_on_error: Collect step failures into ``MaintenanceResult.issues``
                and keep going instead of aborting the run.

        Returns:
            MaintenanceResult with the per-step counters and any collected issues.

        Raises:
            LockAcquisitionError: If the table-level maintenance lock is unavailable.
            InvalidPartitionConfigError: If ``config`` does not match the parent table.
        """
        ...


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


@runtime_checkable
class SubpartitionRepository(Protocol):
    """DDL for partitions that are themselves partitioned tables.

    Kept separate from :class:`PartitionRepository` on purpose: a repository
    written against the flat protocol keeps satisfying it, and is only required
    to grow these three methods once a config actually asks for subpartitioning.
    """

    async def create_branch(
        self,
        config: TablePartitionConfig,
        branch_name: str,
        from_value: str,
        to_value: str,
        spec: SubpartitionSpec,
    ) -> PartitionInfo:
        """Create a detached time partition that is itself partitioned.

        Args:
            config: Table partition configuration.
            branch_name: Name for the new branch table.
            from_value: Start boundary value.
            to_value: End boundary value.
            spec: Subpartitioning the branch applies to its own children.

        Returns:
            Info about the created (still detached) branch.

        Raises:
            PartitionAlreadyExistsError: If a relation of that name exists.
        """
        ...

    async def create_subpartition_table(self, parent_name: str, child_name: str, spec: SubpartitionSpec | None) -> None:
        """Create a detached table shaped like ``parent_name``.

        Args:
            parent_name: Relation the table will later be attached to.
            child_name: Name for the new table.
            spec: Subpartitioning the table applies to its own children, or None.

        Raises:
            PartitionAlreadyExistsError: If a relation of that name exists.
        """
        ...

    async def attach_subpartition(self, parent_name: str, child_name: str, bounds: HashBounds) -> None:
        """Attach a hash bucket to its parent.

        Args:
            parent_name: Partitioned relation to attach to.
            child_name: Table to attach.
            bounds: Modulus and remainder the bucket owns.
        """
        ...


@runtime_checkable
class NestedPartitionMetadata(Protocol):
    """Structural introspection a nested configuration needs.

    Also separate from :class:`PartitionMetadataProvider` so flat setups keep
    working with providers that predate subpartitioning.
    """

    async def get_partition_tree(self, table_name: str) -> PartitionNode | None:
        """Return the whole partition tree rooted at ``table_name``.

        Args:
            table_name: Root of the tree, schema-qualified.

        Returns:
            The root node with its descendants, or None when the relation is
            neither partitioned nor a partition.
        """
        ...

    async def get_unique_constraint_columns(self, table_name: str) -> tuple[tuple[str, ...], ...]:
        """Return the column tuples of every UNIQUE / PRIMARY KEY constraint.

        Args:
            table_name: Table to inspect, schema-qualified.

        Returns:
            One tuple of column names per constraint.
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
