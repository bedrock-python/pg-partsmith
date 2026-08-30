"""Sync protocols (interfaces) for partition management.

Three of them talk to a database and one runs a lifecycle:

* :class:`PartitionRepository` — the DDL: create, attach, detach, drop, mark.
* :class:`PartitionMetadataProvider` — the catalog: what exists, how big it
  is, where the cursor of an integer axis stands.
* :class:`LockManager` — mutual exclusion between maintainers.
* :class:`PartitionLifecycle` — what an orchestrator depends on.

Every method takes and returns plain domain objects, so a custom
implementation — another driver, a caching layer, a stub for tests — never
has to know how the planner works.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Protocol, runtime_checkable

from pg_partsmith.entities import MaintenanceResult, PartitionInfo, TablePartitionConfig
from pg_partsmith.leaves import LocalLeaves
from pg_partsmith.lifecycle import DetachMode, SqlPredicate
from pg_partsmith.plan import PartitionBy
from pg_partsmith.topology import ActualTree, FactKind, PartitionBounds, PartitionNode, PartitionType, RelationKind

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

    def maintain_lifecycle(
        self,
        config: TablePartitionConfig,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Plan and apply one maintenance run under the table's lock.

        Args:
            config: Table partitioning configuration.
            skip_create: Leave out creations and re-attachments.
            skip_detach: Leave out detaches (and the drops that follow them).
            skip_drop: Leave out drops.
            continue_on_error: Collect operation failures into
                ``MaintenanceResult.issues`` and keep going instead of aborting.

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

    This protocol is intentionally limited to write operations. All read
    operations live in :class:`PartitionMetadataProvider` so that the two
    concerns can be mocked, swapped, or overridden independently.
    """

    def create_table_like(
        self,
        template_name: str,
        table_name: str,
        partition_by: PartitionBy | None,
        *,
        physical: LocalLeaves | None = None,
    ) -> None:
        """Create a detached table shaped like ``template_name``.

        Args:
            template_name: Relation whose columns, indexes and constraints are copied.
            table_name: Schema-qualified name for the new table.
            partition_by: How the new table partitions its own children, or
                None for a plain leaf.
            physical: Tablespace, storage parameters and privileges for the
                new table; None for the database defaults.

        Raises:
            PartitionAlreadyExistsError: If a relation of that name exists.
        """
        ...

    def create_foreign_table_like(
        self,
        template_name: str,
        table_name: str,
        *,
        server: str,
        options: dict[str, str],
    ) -> None:
        """Create a detached foreign table with ``template_name``'s columns.

        Args:
            template_name: Relation whose columns are copied.
            table_name: Schema-qualified name for the new foreign table.
            server: The foreign server.
            options: Foreign table options, rendered for this leaf.

        Raises:
            PartitionAlreadyExistsError: If a relation of that name exists.
        """
        ...

    def attach_partition(
        self,
        parent_name: str,
        partition_name: str,
        bounds: PartitionBounds,
        *,
        key_arity: int = 1,
    ) -> None:
        """Attach a table to a partitioned parent.

        Args:
            parent_name: Partitioned relation to attach to.
            partition_name: Table to attach.
            bounds: What the partition owns: a RANGE window, a hash bucket, a
                set of LIST values, or DEFAULT. A RANGE bound under a composite
                key has its trailing columns padded with ``MINVALUE``.
            key_arity: Number of columns in ``parent_name``'s partition key.
        """
        ...

    def detach_partition(
        self,
        parent_name: str,
        partition_name: str,
        *,
        mode: DetachMode = DetachMode.AUTO,
        expected_oid: int | None = None,
    ) -> None:
        """Detach a partition, writing the orphan marker first.

        Args:
            parent_name: Parent table name.
            partition_name: Partition table name.
            mode: Concurrent, blocking, or concurrent with a blocking fallback.
            expected_oid: The catalog identity the caller decided on, checked
                again right before the marker and the statement. When the
                relation now holding the name has another, or is no longer
                attached, nothing is detached.

        Raises:
            PartitionNotFoundError: If partition doesn't exist.
            PartitionDetachInProgressError: If another detach is pending.
            PlanStaleError: If the relation is not the one ``expected_oid`` named.
        """
        ...

    def drop_partition(
        self, partition_name: str, *, expected_oid: int | None = None, drain_into: str | None = None
    ) -> None:
        """Drop a detached, marker-tagged partition table.

        Args:
            partition_name: Partition table name.
            expected_oid: The catalog identity the caller decided on. When the
                relation now holding the name has another, nothing is dropped.
            drain_into: Move the rows the table still holds into this relation
                in the same transaction as the drop, under the drop's lock.

        Raises:
            PartitionAttachedError: If partition is still attached.
            UnmanagedPartitionDropError: If the table carries no orphan marker.
            PlanStaleError: If the relation is not the one ``expected_oid`` named.
            RowMoveRefusedError: If the remaining rows cannot be moved safely.
        """
        ...

    def adopt_partition(self, table_name: str, partition_name: str) -> bool:
        """Stamp the orphan marker on a detached table this library did not detach.

        Args:
            table_name: The parent the table used to belong to.
            partition_name: The detached table.

        Returns:
            True when the marker is present after the call, False when the
            table does not exist.
        """
        ...

    def reconcile_default_rows(
        self,
        *,
        default_partition_name: str,
        target_partition_name: str,
        key_columns: tuple[str, ...],
        from_value: str,
        to_value: str,
        limit: int | None = None,
    ) -> int:
        """Move rows from a DEFAULT partition to the partition for a window.

        Args:
            default_partition_name: Qualified name of DEFAULT partition.
            target_partition_name: Qualified name of target partition.
            key_columns: The parent's partition key, leading column first. Rows
                with a NULL in a trailing column stay in DEFAULT, where
                PostgreSQL routes them.
            from_value: Window start (inclusive) on the leading column.
            to_value: Window end (exclusive) on the leading column.
            limit: Move at most this many rows; None moves them all.

        Returns:
            Number of rows moved.
        """
        ...

    def move_rows(self, source_name: str, target_name: str, *, limit: int | None = None) -> int:
        """Move rows from one relation into another, whatever their keys.

        Args:
            source_name: Qualified name of the relation to take rows from.
            target_name: Qualified name of the relation to put them in.
            limit: Move at most this many rows; None moves them all.

        Returns:
            Number of rows moved.
        """
        ...


@runtime_checkable
class PartitionMetadataProvider(Protocol):
    """Provider for reading partition metadata from the database catalogue.

    This protocol owns *all* read operations so that the service layer depends
    on a single injectable read interface. Implement this protocol to support a
    different database, a caching layer, or a stub for testing.
    """

    def get_partition_type(self, table_name: str) -> PartitionType | None:
        """Get partition type for a table, or None if it is not partitioned."""
        ...

    def get_partition_columns(self, table_name: str) -> tuple[str, ...]:
        """Return a table's own partition key columns, in key order.

        Raises:
            InvalidPartitionConfigError: If any key position is an expression.
        """
        ...

    def get_actual_tree(self, table_name: str) -> ActualTree | None:
        """Return the whole tree below ``table_name`` plus its orphans.

        Args:
            table_name: Root of the tree, schema-qualified.

        Returns:
            The tree, or None when the relation is not partitioned.
        """
        ...

    def measure(
        self,
        tree: ActualTree,
        *,
        targets: tuple[str, ...],
        facts: frozenset[FactKind] = frozenset(),
        sql_predicates: tuple[SqlPredicate, ...] = (),
    ) -> ActualTree:
        """Return ``tree`` with facts attached to the named targets.

        Args:
            tree: The tree to annotate.
            targets: Schema-qualified names the facts and predicates are
                gathered for; everything else stays unmeasured.
            facts: What to measure.
            sql_predicates: Questions to ask about each target.
        """
        ...

    def get_partition_tree(self, table_name: str) -> PartitionNode | None:
        """Return the tree rooted at ``table_name`` without orphans, or None.

        Works for a detached relation too, which is how a half-built branch is
        inspected before it is attached.
        """
        ...

    def get_default_partition(self, table_name: str) -> PartitionInfo | None:
        """Get the attached DEFAULT partition of a table, if any."""
        ...

    def partition_exists(self, partition_name: str) -> bool:
        """True when a table or partitioned table of that name exists."""
        ...

    def is_partition_attached(self, table_name: str, partition_name: str) -> bool:
        """True when ``partition_name`` is attached to ``table_name`` via pg_inherits."""
        ...

    def get_relation_oid(self, name: str) -> int | None:
        """Return the OID of the relation currently holding ``name``, or None."""
        ...

    def get_relation_kind(self, name: str) -> RelationKind | None:
        """What the relation holding ``name`` physically is, or None when there is none."""
        ...

    def get_unique_constraint_columns(self, table_name: str) -> tuple[tuple[str, ...], ...]:
        """Return the column tuples of every UNIQUE / PRIMARY KEY constraint on a table."""
        ...

    def get_key_high_water_mark(self, table_name: str, column: str, *, sequence: bool = False) -> int | None:
        """Return the newest value of an integer partition key.

        Args:
            table_name: The partitioned table.
            column: The key column.
            sequence: Read the key's serial/identity sequence instead of ``max(column)``.

        Returns:
            The value, or None when the table is empty (or the sequence unused).
        """
        ...

    def get_leading_key_minimum(self, table_name: str, key_columns: tuple[str, ...]) -> Any:
        """Return the smallest leading-key value of the rows a partition could take.

        Rows with a NULL anywhere in the key are left out: PostgreSQL routes
        them to DEFAULT whatever their other values.

        Args:
            table_name: The relation to probe -- a DEFAULT partition, usually.
            key_columns: The parent's partition key, leading column first.

        Returns:
            The value as the driver returns it, or None when no such row exists.
        """
        ...

    def list_partitions(self, table_name: str) -> list[PartitionInfo]:
        """List the direct partitions of a table, including its marker-tagged orphans."""
        ...


@runtime_checkable
class LockManager(Protocol):
    """Lock manager for coordinating partition operations.

    This protocol defines the interface for acquiring and releasing locks
    to prevent concurrent partition operations on the same table.
    Implement this to use a different locking backend (e.g. Zookeeper).
    """

    def acquire_lock(self, table_name: str) -> AbstractContextManager[None]:
        """Acquire lock for partition operations on a table.

        Args:
            table_name: Table name to lock.

        Returns:
            Context manager for the lock.

        Raises:
            LockAcquisitionError: If unable to acquire lock.
        """
        ...

    def is_locked(self, table_name: str) -> bool:
        """Check if table is currently locked."""
        ...
