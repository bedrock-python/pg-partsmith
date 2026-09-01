"""Domain service for partition lifecycle management."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pg_partsmith.boundaries import Window
from pg_partsmith.constants import DEFAULT_MOVE_BATCH_ROWS
from pg_partsmith.entities import MaintenanceResult, MigrationResult, PartitionInfo, Period, TablePartitionConfig
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.plan import CreatePartition, DetachPartition, DropPartition, MaintenancePlan, OperationKind, Reason
from pg_partsmith.planner import PlanMode, plan_maintenance
from pg_partsmith.scheme import RangePartitioning
from pg_partsmith.utils import validate_timezone_alignment

from .services.execution import PlanExecutor
from .services.inspection import PartitionInspector
from .services.migration import DataMover
from .services.validation import PartitionValidationService

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pg_partsmith.aio.hooks import PartitionLifecycleHooks
    from pg_partsmith.aio.protocols import LockManager, PartitionMetadataProvider, PartitionRepository
    from pg_partsmith.topology import ActualTree

logger = logging.getLogger(__name__)


class PartitionLifecycleService:
    """Plans and applies partition maintenance for one table at a time.

    Three verbs cover the lifecycle:

    * :meth:`plan` — read the catalog and decide what to do, without doing it;
    * :meth:`apply` — execute a plan under the table's lock, revalidating
      every destructive operation first;
    * :meth:`maintain` — both, under one lock, which is what a scheduled tick
      runs.

    The remaining methods are conveniences over the same three.
    """

    def __init__(
        self,
        repo: PartitionRepository,
        metadata: PartitionMetadataProvider,
        locks: LockManager,
        hooks: list[PartitionLifecycleHooks] | None = None,
    ) -> None:
        """Initialize the partition lifecycle service.

        Args:
            repo: DDL operations on partitions (create / attach / detach / drop).
            metadata: Read-only access to PostgreSQL catalog data.
            locks: Distributed lock manager preventing concurrent maintenance runs.
            hooks: Optional list of lifecycle hooks called around each step.
        """
        self._repo = repo
        self._metadata = metadata
        self._locks = locks
        self._validation = PartitionValidationService(metadata)
        self._inspector = PartitionInspector(metadata)
        self._executor = PlanExecutor(repo, metadata, hooks)
        self._mover = DataMover(repo, metadata, self._executor)

    # ── The three verbs ─────────────────────────────────────────────────────────

    async def inspect(self, config: TablePartitionConfig) -> ActualTree | None:
        """Read the table's whole partition tree and its orphans.

        Args:
            config: Table partitioning configuration.

        Returns:
            The tree, or None when the table is not partitioned.
        """
        return await self._inspector.inspect(config, measure=False)

    async def plan(
        self,
        config: TablePartitionConfig,
        *,
        mode: PlanMode = PlanMode.MAINTAIN,
        now: datetime | None = None,
        windows: dict[str, tuple[Window, ...]] | None = None,
    ) -> MaintenancePlan:
        """Decide what maintenance would do, without doing any of it.

        Reads the catalog and gathers whatever the lifecycle policy needs; takes
        no lock and issues no DDL. The result can be shown, serialized, filtered
        with :meth:`~pg_partsmith.MaintenancePlan.without`, and handed to
        :meth:`apply`.

        Args:
            config: Table partitioning configuration.
            mode: What the plan is for: the scheduled tick (``MAINTAIN``),
                converging the existing tree only (``RECONCILE``), or ensuring
                specific windows exist (``EXPLICIT``, with ``windows``).
            now: The instant to plan against; the current time by default.
            windows: In ``EXPLICIT`` mode, the windows to ensure at each
                progression level, keyed by leading column.

        Returns:
            The plan.

        Raises:
            InvalidPartitionConfigError: If ``config`` does not match the table.
        """
        self._check_timezones(config)
        await self._validation.validate_config(config)

        tree = await self._inspector.inspect(config, measure=mode is PlanMode.MAINTAIN)
        if tree is None:
            msg = f"Table {config.qualified_name!r} is not partitioned"
            raise InvalidPartitionConfigError(msg)

        context = await self._inspector.context(config, now=now, mode=mode, explicit_windows=windows)
        return plan_maintenance(config, tree, context)

    async def apply(
        self,
        config: TablePartitionConfig,
        plan: MaintenancePlan,
        *,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Execute a plan under the table's lock.

        Every destructive operation is revalidated against the catalog first:
        a relation that no longer has the OID the plan saw is left alone.

        Args:
            config: The configuration the plan was made from.
            plan: The plan, possibly filtered.
            continue_on_error: Isolate operation failures into
                ``MaintenanceResult.issues`` instead of aborting.

        Raises:
            LockAcquisitionError: If the table-level maintenance lock is unavailable.
        """
        async with self._locks.acquire_lock(config.qualified_name):
            return await self._executor.apply(config, plan, continue_on_error=continue_on_error)

    async def maintain(
        self,
        config: TablePartitionConfig,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Plan and apply one maintenance run under a single lock.

        Args:
            config: Table partitioning configuration.
            skip_create: Leave out creations and re-attachments.
            skip_detach: Leave out detaches, and the drops that follow them.
            skip_drop: Leave out drops.
            continue_on_error: Isolate operation failures instead of aborting
                the run: a failed create still prunes (which may free the space
                create needs), a failed detach still drops existing orphans.
                Findings the planner refused to act on are reported through
                ``MaintenanceResult.issues`` regardless, since leaving them
                silent would hide writes PostgreSQL is rejecting. Validation and
                lock failures are always fatal.

        Returns:
            ``MaintenanceResult`` with the per-step counters and the plan.

        Raises:
            LockAcquisitionError: If the table-level maintenance lock is unavailable.
            InvalidPartitionConfigError: If ``config`` does not match the parent table.
        """
        excluded: list[OperationKind] = []
        if skip_create:
            excluded.extend((OperationKind.CREATE, OperationKind.ATTACH))
        if skip_detach:
            excluded.append(OperationKind.DETACH)
        if skip_drop:
            excluded.append(OperationKind.DROP)

        async with self._locks.acquire_lock(config.qualified_name):
            plan = await self.plan(config)
            finalized: MaintenanceResult | None = None
            if not skip_detach and any(op.reason is Reason.DETACH_FINALIZE for op in plan.detaches):
                # An interrupted DETACH CONCURRENTLY is completed first and the
                # run re-planned: the finalized table is an orphan this same
                # call re-attaches (its window is still wanted) or retires
                # under the drop policy, instead of waiting one more tick.
                pending_only = plan.model_copy(
                    update={
                        "operations": tuple(op for op in plan.detaches if op.reason is Reason.DETACH_FINALIZE),
                        "findings": (),
                    }
                )
                finalized = await self._executor.apply(config, pending_only, continue_on_error=continue_on_error)
                plan = await self.plan(config)
            result = await self._executor.apply(config, plan.without(*excluded), continue_on_error=continue_on_error)
            return result if finalized is None else _merged(finalized, result)

    async def maintain_lifecycle(
        self,
        config: TablePartitionConfig,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Alias of :meth:`maintain`, the name orchestrators call it by."""
        return await self.maintain(
            config,
            skip_create=skip_create,
            skip_detach=skip_detach,
            skip_drop=skip_drop,
            continue_on_error=continue_on_error,
        )

    # ── Conveniences ────────────────────────────────────────────────────────────

    async def reconcile(self, config: TablePartitionConfig) -> MaintenanceResult:
        """Converge the existing tree towards the scheme; create nothing ahead, expire nothing.

        Idempotent and safe to call on its own: it creates only the members a
        set level is genuinely missing, and reports rather than "repairs" any
        branch whose shape it cannot converge without risk.

        It takes **no distributed lock of its own** -- unlike :meth:`maintain`.
        Two workers calling this concurrently is safe: a lost race on a member
        is recognised by its bounds and reported, not retried into a failure.
        Wrap it in your own lock if you would rather they queued.
        """
        plan = await self.plan(config, mode=PlanMode.RECONCILE)
        return await self._executor.apply(config, plan)

    async def ensure_partition(
        self, config: TablePartitionConfig, period: Period | Window | Any
    ) -> PartitionInfo | None:
        """Create and attach the partition for one specific window (idempotent).

        Useful for writers that must guarantee a partition exists before an
        insert. Runs the same DEFAULT reconciliation and attach-race handling
        as the scheduled path, and completes the subtree of a partition that
        already exists.

        Args:
            config: Table partitioning configuration.
            period: The period (time axis), the :class:`~pg_partsmith.Window`,
                or a position on the root's axis -- an instant, an integer key
                value, a sliding-list value -- the partition must cover.

        Returns:
            The created partition, or None when it already existed -- also when
            its subtree was completed for it.
        """
        created = await self.ensure_partitions(config, (period,))
        return created[0] if created else None

    async def ensure_partitions(
        self,
        config: TablePartitionConfig,
        periods: Iterable[Period | Window | Any],
    ) -> list[PartitionInfo]:
        """Create and attach partitions for an explicit set of windows (idempotent).

        The backfill counterpart of the scheduled create-ahead: the caller
        chooses the windows, so data that already sits in the table can be
        given partitions without waiting for create-ahead to reach it. The
        catalog is read once for the whole batch, and every window is built
        with its complete subtree before it is attached.

        Takes no lock of its own.

        Args:
            config: Table partitioning configuration.
            periods: Periods (time axis), windows, or positions on the root's
                axis that must have a partition. Duplicates are ignored.

        Returns:
            The partitions created by this call, in chronological order;
            windows that already had one are absent from the list, and so are
            the members created to complete an existing partition's subtree.

        Raises:
            InvalidPartitionConfigError: If the root is not a progression
                level, or a period is given for a non-time axis.
        """
        windows = self._windows_for(config, periods)
        plan = await self.plan(config, mode=PlanMode.EXPLICIT, windows=windows)
        result = await self._executor.apply(config, plan)
        return _created_partitions(config, plan, result)

    # ── Moving rows ─────────────────────────────────────────────────────────────

    async def partition_data(
        self,
        config: TablePartitionConfig,
        *,
        batch_rows: int = DEFAULT_MOVE_BATCH_ROWS,
        max_batches: int | None = None,
    ) -> MigrationResult:
        """Move the rows of the DEFAULT partition into the partitions they belong to, in batches.

        The migration path for a table that was partitioned around its data:
        attach the old table as the DEFAULT partition of the new parent, then
        call this until ``result.complete``. Window by window, oldest first,
        the partition is created detached, filled in batches of
        ``batch_rows`` and attached once nothing of its window is left in
        DEFAULT. Until that attach the window's rows are invisible through the
        parent -- PostgreSQL cannot attach a partition while DEFAULT holds rows
        for it, so there is no order that keeps them visible throughout.

        Takes the table's lock.

        Args:
            config: Table partitioning configuration; the root must be a RANGE level.
            batch_rows: Rows moved per statement.
            max_batches: Stop after this many statements and report
                ``complete=False``; the next call carries on.

        Returns:
            What was moved and created, and whether DEFAULT is drained.

        Raises:
            InvalidPartitionConfigError: If the root is not a RANGE level or
                the configuration does not match the table.
            LockAcquisitionError: If the table-level maintenance lock is unavailable.
        """

        async def plan_for(window: Window) -> MaintenancePlan:
            return await self.plan(config, mode=PlanMode.EXPLICIT, windows={config.scheme.leading_column: (window,)})

        async with self._locks.acquire_lock(config.qualified_name):
            await self._validation.validate_config(config)
            return await self._mover.partition_data(config, plan_for, batch_rows=batch_rows, max_batches=max_batches)

    async def unpartition(
        self,
        config: TablePartitionConfig,
        into: str,
        *,
        batch_rows: int = DEFAULT_MOVE_BATCH_ROWS,
        max_batches: int | None = None,
        drop_emptied: bool = False,
    ) -> MigrationResult:
        """Move every partition's rows into one plain table, in batches.

        The way back: ``into`` is created ``LIKE`` the root when it does not
        exist, each partition is emptied oldest first, and with ``drop_emptied``
        every emptied partition is detached and dropped through the ordinary
        path -- marker, hooks, revalidation. Foreign partitions are skipped
        and reported. Rows already moved are in ``into`` at every commit point,
        never in two places.

        Takes the table's lock.

        Args:
            config: Table partitioning configuration.
            into: Schema-qualified name of the receiving table.
            batch_rows: Rows moved per statement.
            max_batches: Stop after this many statements and report
                ``complete=False``; the next call carries on.
            drop_emptied: Detach and drop each partition once it is empty.

        Returns:
            What was moved and emptied, and whether every partition is empty.

        Raises:
            InvalidPartitionConfigError: If the configuration does not match the table.
            LockAcquisitionError: If the table-level maintenance lock is unavailable.
        """
        async with self._locks.acquire_lock(config.qualified_name):
            await self._validation.validate_config(config)
            return await self._mover.unpartition(
                config, into, batch_rows=batch_rows, max_batches=max_batches, drop_emptied=drop_emptied
            )

    # ── Granular steps ──────────────────────────────────────────────────────────

    async def create_future_partitions(self, config: TablePartitionConfig) -> list[PartitionInfo]:
        """Run the creation half of a maintenance plan: create ahead and re-attach.

        Takes no lock of its own.

        Returns:
            List of newly created partitions (empty if all already existed).
        """
        plan = (await self.plan(config)).only(OperationKind.CREATE, OperationKind.ATTACH)
        result = await self._executor.apply(config, plan)
        return _created_partitions(config, plan, result)

    async def get_partitions_for_pruning(self, config: TablePartitionConfig) -> list[PartitionInfo]:
        """Return the partitions a maintenance run would detach or drop, oldest first.

        Returns:
            Attached partitions retention has expired, followed by detached
            orphans past their grace.
        """
        plan = await self.plan(config)
        infos: list[PartitionInfo] = []
        for op in plan.operations:
            if isinstance(op, DetachPartition):
                infos.append(
                    PartitionInfo(
                        name=op.target,
                        oid=op.oid,
                        partition_type=config.partition_type,
                        bounds=op.bounds,
                        boundaries_expr=None if op.bounds is None else op.bounds.kind,
                        is_attached=True,
                        parent_table=op.parent_name,
                    )
                )
            elif isinstance(op, DropPartition) and not op.follows_detach:
                infos.append(
                    PartitionInfo(
                        name=op.target,
                        oid=op.oid,
                        partition_type=config.partition_type,
                        is_attached=False,
                        parent_table=config.qualified_name,
                    )
                )
        return infos

    async def detach_old_partitions(self, table_name: str, partitions: list[PartitionInfo]) -> list[str]:
        """Detach attached partitions from their parent table.

        Takes no lock of its own.

        Args:
            table_name: Qualified parent table name.
            partitions: Attached partitions to detach. Inputs that are already
                detached are counted without any DDL.

        Returns:
            Names of successfully detached partitions.
        """
        detached: list[str] = []
        for partition in partitions:
            if not partition.is_attached:
                detached.append(partition.name)
                continue
            op = DetachPartition(
                target=partition.name,
                oid=partition.oid,
                parent_name=partition.parent_table or table_name,
                bounds=partition.bounds,
                reason=Reason.RETENTION_EXPIRED,
                detail="requested explicitly",
            )
            await self._executor.detach_single_partition(table_name, op)
            detached.append(partition.name)
        return detached

    async def drop_detached_partitions(self, table_name: str, partition_names: list[str]) -> int:
        """Drop previously detached, marker-tagged partitions.

        Attached partitions are skipped with a warning. Unmanaged tables are
        refused unless the underlying repository is configured otherwise.

        Args:
            table_name: Qualified parent table name (used for hook context).
            partition_names: Names of partitions to drop.

        Returns:
            Number of partitions actually dropped.
        """
        dropped = 0
        for name in partition_names:
            op = DropPartition(target=name, reason=Reason.GRACE_ELAPSED, detail="requested explicitly")
            if await self._executor.drop_single_partition(table_name, op) is not None:
                dropped += 1
        return dropped

    # ── Helpers ─────────────────────────────────────────────────────────────────

    def _check_timezones(self, config: TablePartitionConfig) -> None:
        """Refuse a wiring whose calendar and DDL timezones disagree."""
        for level in config.levels:
            if isinstance(level, RangePartitioning) and level.time_boundaries is not None:
                validate_timezone_alignment(self._repo, level.time_boundaries.period_calculator)

    @staticmethod
    def _windows_for(
        config: TablePartitionConfig, periods: Iterable[Period | Window | Any]
    ) -> dict[str, tuple[Window, ...]]:
        root = config.scheme
        boundaries = root.progression
        if boundaries is None:
            msg = "ensure_partitions needs a progression root: a HASH or grouped LIST root has a fixed partition set"
            raise InvalidPartitionConfigError(msg)
        time_boundaries = root.time_boundaries if isinstance(root, RangePartitioning) else None
        windows: list[Window] = []
        for item in periods:
            if isinstance(item, Window):
                windows.append(item)
            elif isinstance(item, Period):
                if time_boundaries is None:
                    msg = (
                        f"{config.qualified_name!r} is not partitioned by time; pass windows or key values, not periods"
                    )
                    raise InvalidPartitionConfigError(msg)
                windows.append(time_boundaries.window_for(item))
            else:
                windows.append(boundaries.window_at(item))
        return {root.leading_column: tuple(windows)}


def _created_partitions(
    config: TablePartitionConfig, plan: MaintenancePlan, result: MaintenanceResult
) -> list[PartitionInfo]:
    """The partitions a plan created directly under the root, in the order it made them.

    Gaps filled inside a partition that already existed are repairs of that
    partition, not partitions of their own, and are left out.
    """
    failed = _failed(result)
    return [_created_info(config, op) for op in plan.creates if op.counts_as == "created" and op.target not in failed]


def _created_info(config: TablePartitionConfig, op: CreatePartition) -> PartitionInfo:
    return PartitionInfo(
        name=op.target,
        partition_type=config.partition_type,
        bounds=op.bounds,
        boundaries_expr=op.bounds.kind,
        is_attached=True,
        is_default=op.bounds.kind == "default",
        subpartition_type=None if op.partition_by is None else op.partition_by.method,
        parent_table=op.parent_name,
    )


def _failed(result: MaintenanceResult) -> set[str]:
    return {issue.partition_name for issue in result.issues if issue.partition_name is not None}


def _merged(first: MaintenanceResult, second: MaintenanceResult) -> MaintenanceResult:
    """One result over two applies of the same call: counters add up, the last plan stands."""
    return second.model_copy(
        update={
            "created_count": first.created_count + second.created_count,
            "repaired_count": first.repaired_count + second.repaired_count,
            "attached_count": first.attached_count + second.attached_count,
            "detached_count": first.detached_count + second.detached_count,
            "dropped_count": first.dropped_count + second.dropped_count,
            "issues": first.issues + second.issues,
        }
    )
