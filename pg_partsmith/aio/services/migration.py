"""Moving rows in batches: out of a DEFAULT partition into lifecycle partitions, and back into one table.

Both directions are the ``pg_partman`` movers (``partition_data_proc``,
``undo_partition``) recast over the plan: every partition is created through
the same executor path as a scheduled one, and every batch is one
``DELETE ... RETURNING`` / ``INSERT`` statement that commits on its own, so
a row exists in exactly one place at every commit point.

What a batch cannot hide: while a window's rows are being moved out of the
DEFAULT partition, they sit in a table that is not yet attached and are
invisible through the parent. PostgreSQL leaves no other order -- a partition
cannot be attached while the DEFAULT partition still holds rows for it -- so
``partition_data`` is a maintenance operation, not a transparent one.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from functools import partial
from typing import TYPE_CHECKING, Any

from pg_partsmith.boundaries import Window
from pg_partsmith.constants import DEFAULT_MOVE_BATCH_ROWS
from pg_partsmith.entities import MaintenanceIssue, MaintenanceIssueStep, MigrationResult
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.plan import DetachPartition, DropPartition, MaintenancePlan, Reason
from pg_partsmith.planner import to_maintenance_issue
from pg_partsmith.scheme import RangePartitioning
from pg_partsmith.topology import DefaultBounds, PartitionNode, RangeBounds

if TYPE_CHECKING:
    from pg_partsmith.aio.protocols import PartitionMetadataProvider, PartitionRepository
    from pg_partsmith.aio.services.execution import PlanExecutor
    from pg_partsmith.entities import TablePartitionConfig

logger = logging.getLogger(__name__)

PlanForWindow = Callable[[Window], Awaitable[MaintenancePlan]]


class DataMover:
    """Moves rows between a table's partitions in bounded batches."""

    def __init__(self, repo: PartitionRepository, metadata: PartitionMetadataProvider, executor: PlanExecutor) -> None:
        self._repo = repo
        self._metadata = metadata
        self._executor = executor

    async def partition_data(
        self,
        config: TablePartitionConfig,
        plan_for: PlanForWindow,
        *,
        batch_rows: int = DEFAULT_MOVE_BATCH_ROWS,
        max_batches: int | None = None,
    ) -> MigrationResult:
        """Move the rows of the root's DEFAULT partition into the partitions they belong to.

        Window by window, oldest first: the partition for the oldest window
        still in DEFAULT is created detached (subtree included), filled from
        DEFAULT in batches of ``batch_rows``, and attached once DEFAULT holds
        nothing more for it. A partition left detached when ``max_batches``
        runs out is picked up and finished by the next call.

        Args:
            config: The table's configuration; its root must be a RANGE level.
            plan_for: Plans the partition for one window (the service's
                EXPLICIT plan).
            batch_rows: Rows moved per statement.
            max_batches: Stop after this many statements, leaving the rest
                for another call.

        Returns:
            What was moved and created, and whether DEFAULT is drained.
        """
        _require_positive(batch_rows, "batch_rows")
        root = config.scheme
        if not isinstance(root, RangePartitioning):
            msg = "partition_data drains a DEFAULT partition into RANGE windows; this root is not a RANGE level"
            raise InvalidPartitionConfigError(msg)
        boundaries = root.range_boundaries

        default = await self._metadata.get_default_partition(config.qualified_name)
        if default is None:
            return MigrationResult(complete=True)

        tally = _Tally(max_batches)
        while True:
            probe = await self._metadata.get_leading_key_minimum(default.name, root.key)
            if probe is None:
                return tally.result(complete=True)

            position = boundaries.decode(str(probe))
            window = boundaries.window_at(probe if position is None else position)
            plan = await plan_for(window)
            tally.issues.extend(to_maintenance_issue(f) for f in plan.actionable_findings)
            op = next((c for c in plan.creates if c.counts_as == "created" and isinstance(c.bounds, RangeBounds)), None)
            if op is None:
                tally.issue(
                    default.name,
                    f"rows for {boundaries.describe(window)} stay in {default.name}: no partition can be created for "
                    "that window (see the findings above)",
                )
                return tally.result(complete=False)

            moved_before = tally.rows_moved
            bounds = op.bounds
            assert isinstance(bounds, RangeBounds)
            fill = partial(self._drain_window, default.name, root.key, bounds, batch_rows, tally)
            attached = await self._executor.create_partition(config, plan, op, issues=tally.issues, fill=fill)
            if not attached:
                logger.info(
                    "Batch budget exhausted; the partition stays detached until the next call",
                    extra={"partition_name": op.target, "batches": tally.batches},
                )
                return tally.result(complete=False)
            tally.partitions.append(op.target)

            if tally.rows_moved == moved_before:
                # The probe found a row this window's literals do not select
                # -- a bound rendered in another timezone, say. Looping would
                # create the same partition forever.
                tally.issue(
                    default.name,
                    f"a row of {default.name} falls in {boundaries.describe(window)} but was not selected by its "
                    f"bounds {bounds.from_value!r} .. {bounds.to_value!r}; left in place",
                )
                return tally.result(complete=False)

    async def _drain_window(
        self,
        default_name: str,
        key_columns: tuple[str, ...],
        bounds: RangeBounds,
        batch_rows: int,
        tally: _Tally,
        target: str,
    ) -> bool:
        """Move a window's rows from DEFAULT into ``target``; False when the batch budget ran out first."""
        while not tally.exhausted:
            moved = await self._repo.reconcile_default_rows(
                default_partition_name=default_name,
                target_partition_name=target,
                key_columns=key_columns,
                from_value=bounds.from_value,
                to_value=bounds.to_value,
                limit=batch_rows,
            )
            tally.batch(moved)
            if moved < batch_rows:
                return True
        return False

    async def unpartition(
        self,
        config: TablePartitionConfig,
        into: str,
        *,
        batch_rows: int = DEFAULT_MOVE_BATCH_ROWS,
        max_batches: int | None = None,
        drop_emptied: bool = False,
    ) -> MigrationResult:
        """Move every partition's rows into one plain table, oldest partition first.

        ``into`` is created ``LIKE`` the root when it does not exist. A
        partition is emptied in batches of ``batch_rows``; with
        ``drop_emptied`` it is then detached and dropped through the ordinary
        path (marker, hooks, revalidation). Foreign partitions are skipped and
        reported: their rows are not this database's to move.

        Args:
            config: The table's configuration.
            into: Schema-qualified name of the receiving table.
            batch_rows: Rows moved per statement.
            max_batches: Stop after this many statements, leaving the rest
                for another call.
            drop_emptied: Detach and drop each partition once it is empty.

        Returns:
            What was moved and emptied, and whether every partition is empty.
        """
        _require_positive(batch_rows, "batch_rows")
        tree = await self._metadata.get_actual_tree(config.qualified_name)
        if tree is None:
            msg = f"Table {config.qualified_name!r} is not partitioned"
            raise InvalidPartitionConfigError(msg)
        if not await self._metadata.partition_exists(into):
            await self._repo.create_table_like(config.qualified_name, into, None)

        tally = _Tally(max_batches)
        for child in _oldest_first(config, tree.root.children):
            if child.is_foreign:
                tally.issue(child.name, f"{child.name} is a foreign table; its rows live elsewhere and are not moved")
                continue
            while True:
                if tally.exhausted:
                    return tally.result(complete=False)
                moved = await self._repo.move_rows(child.name, into, limit=batch_rows)
                tally.batch(moved)
                if moved < batch_rows:
                    break
            tally.partitions.append(child.name)
            if drop_emptied:
                await self._remove(config, child)
        return tally.result(complete=True)

    async def _remove(self, config: TablePartitionConfig, child: PartitionNode) -> None:
        detail = "emptied by unpartition"
        detach = DetachPartition(
            target=child.name,
            oid=child.oid,
            parent_name=config.qualified_name,
            mode=config.lifecycle.detach,
            bounds=child.bounds,
            reason=Reason.EXPLICIT,
            detail=detail,
        )
        await self._executor.detach_single_partition(config.qualified_name, detach)
        drop = DropPartition(target=child.name, oid=child.oid, reason=Reason.EXPLICIT, detail=detail)
        await self._executor.drop_single_partition(config.qualified_name, drop)


class _Tally:
    """Counters of one move, and the batch budget."""

    def __init__(self, max_batches: int | None) -> None:
        if max_batches is not None:
            _require_positive(max_batches, "max_batches")
        self._max_batches = max_batches
        self.rows_moved = 0
        self.batches = 0
        self.partitions: list[str] = []
        self.issues: list[MaintenanceIssue] = []

    @property
    def exhausted(self) -> bool:
        return self._max_batches is not None and self.batches >= self._max_batches

    def batch(self, moved: int) -> None:
        self.batches += 1
        self.rows_moved += moved

    def issue(self, partition_name: str, error: str) -> None:
        self.issues.append(MaintenanceIssue(step=MaintenanceIssueStep.MOVE, error=error, partition_name=partition_name))

    def result(self, *, complete: bool) -> MigrationResult:
        return MigrationResult(
            rows_moved=self.rows_moved,
            batches=self.batches,
            partitions=tuple(self.partitions),
            complete=complete,
            issues=tuple(self.issues),
        )


def _oldest_first(config: TablePartitionConfig, children: tuple[PartitionNode, ...]) -> list[PartitionNode]:
    """The root's partitions in the order their rows should move: by window, DEFAULT last."""
    root = config.scheme

    def key(child: PartitionNode) -> tuple[int, Any, str]:
        if isinstance(child.bounds, DefaultBounds):
            return (2, None, child.name)
        window = root.window_of(child.bounds) if isinstance(root, RangePartitioning) and child.bounds else None
        if window is None:
            return (1, None, child.name)
        return (0, window.start, child.name)

    return sorted(children, key=key)


def _require_positive(value: int, name: str) -> None:
    if value < 1:
        msg = f"{name} must be a positive integer, got {value!r}"
        raise ValueError(msg)
