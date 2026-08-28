"""Domain service for partition lifecycle management."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from pg_partsmith.boundaries import Window
from pg_partsmith.entities import MaintenanceResult, PartitionInfo, Period, TablePartitionConfig
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.plan import CreatePartition, DetachPartition, DropPartition, MaintenancePlan, OperationKind, Reason
from pg_partsmith.planner import PlanMode, plan_maintenance
from pg_partsmith.scheme import RangePartitioning
from pg_partsmith.utils import validate_timezone_alignment

from .services.execution import PlanExecutor
from .services.inspection import PartitionInspector
from .services.validation import PartitionValidationService

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pg_partsmith.sync.hooks import PartitionLifecycleHooks
    from pg_partsmith.sync.protocols import LockManager, PartitionMetadataProvider, PartitionRepository
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

    # ── The three verbs ─────────────────────────────────────────────────────────

    def inspect(self, config: TablePartitionConfig) -> ActualTree | None:
        """Read the table's whole partition tree and its orphans.

        Args:
            config: Table partitioning configuration.

        Returns:
            The tree, or None when the table is not partitioned.
        """
        return self._inspector.inspect(config, measure=False)

    def plan(
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
        self._validation.validate_config(config)

        tree = self._inspector.inspect(config, measure=mode is PlanMode.MAINTAIN)
        if tree is None:
            msg = f"Table {config.qualified_name!r} is not partitioned"
            raise InvalidPartitionConfigError(msg)

        context = self._inspector.context(config, now=now, mode=mode, explicit_windows=windows)
        return plan_maintenance(config, tree, context)

    def apply(
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
        with self._locks.acquire_lock(config.qualified_name):
            return self._executor.apply(config, plan, continue_on_error=continue_on_error)

    def maintain(
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

        with self._locks.acquire_lock(config.qualified_name):
            plan = self.plan(config)
            return self._executor.apply(config, plan.without(*excluded), continue_on_error=continue_on_error)

    def maintain_lifecycle(
        self,
        config: TablePartitionConfig,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Alias of :meth:`maintain`, the name orchestrators call it by."""
        return self.maintain(
            config,
            skip_create=skip_create,
            skip_detach=skip_detach,
            skip_drop=skip_drop,
            continue_on_error=continue_on_error,
        )

    # ── Conveniences ────────────────────────────────────────────────────────────

    def reconcile(self, config: TablePartitionConfig) -> MaintenanceResult:
        """Converge the existing tree towards the scheme; create nothing ahead, expire nothing.

        Idempotent and safe to call on its own: it creates only the members a
        set level is genuinely missing, and reports rather than "repairs" any
        branch whose shape it cannot converge without risk.

        It takes **no distributed lock of its own** -- unlike :meth:`maintain`.
        Two workers calling this concurrently is safe: a lost race on a member
        is recognised by its bounds and reported, not retried into a failure.
        Wrap it in your own lock if you would rather they queued.
        """
        plan = self.plan(config, mode=PlanMode.RECONCILE)
        return self._executor.apply(config, plan)

    def ensure_partition(self, config: TablePartitionConfig, period: Period | Window) -> PartitionInfo | None:
        """Create and attach the partition for one specific window (idempotent).

        Useful for writers that must guarantee a partition exists before an
        insert. Runs the same DEFAULT reconciliation and attach-race handling
        as the scheduled path, and completes the subtree of a partition that
        already exists.

        Args:
            config: Table partitioning configuration.
            period: The period (time axis) or :class:`~pg_partsmith.Window`
                the partition must cover.

        Returns:
            The created partition, or None when it already existed -- also when
            its subtree was completed for it.
        """
        created = self.ensure_partitions(config, (period,))
        return created[0] if created else None

    def ensure_partitions(
        self,
        config: TablePartitionConfig,
        periods: Iterable[Period | Window],
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
            periods: Periods (time axis) or windows that must have a partition.
                Duplicates are ignored.

        Returns:
            The partitions created by this call, in chronological order;
            windows that already had one are absent from the list, and so are
            the members created to complete an existing partition's subtree.

        Raises:
            InvalidPartitionConfigError: If the root is not a RANGE level, or a
                period is given for a non-time axis.
        """
        windows = self._windows_for(config, periods)
        plan = self.plan(config, mode=PlanMode.EXPLICIT, windows=windows)
        result = self._executor.apply(config, plan)
        return _created_partitions(config, plan, result)

    # ── Granular steps ──────────────────────────────────────────────────────────

    def create_future_partitions(self, config: TablePartitionConfig) -> list[PartitionInfo]:
        """Run the creation half of a maintenance plan: create ahead and re-attach.

        Takes no lock of its own.

        Returns:
            List of newly created partitions (empty if all already existed).
        """
        plan = (self.plan(config)).only(OperationKind.CREATE, OperationKind.ATTACH)
        result = self._executor.apply(config, plan)
        return _created_partitions(config, plan, result)

    def get_partitions_for_pruning(self, config: TablePartitionConfig) -> list[PartitionInfo]:
        """Return the partitions a maintenance run would detach or drop, oldest first.

        Returns:
            Attached partitions retention has expired, followed by detached
            orphans past their grace.
        """
        plan = self.plan(config)
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

    def detach_old_partitions(self, table_name: str, partitions: list[PartitionInfo]) -> list[str]:
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
            self._executor.detach_single_partition(table_name, op)
            detached.append(partition.name)
        return detached

    def drop_detached_partitions(self, table_name: str, partition_names: list[str]) -> int:
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
            if self._executor.drop_single_partition(table_name, op):
                dropped += 1
        return dropped

    # ── Helpers ─────────────────────────────────────────────────────────────────

    def _check_timezones(self, config: TablePartitionConfig) -> None:
        """Refuse a wiring whose calendar and DDL timezones disagree."""
        for level in config.levels:
            if isinstance(level, RangePartitioning) and level.time_boundaries is not None:
                validate_timezone_alignment(self._repo, level.time_boundaries.period_calculator)

    @staticmethod
    def _windows_for(config: TablePartitionConfig, periods: Iterable[Period | Window]) -> dict[str, tuple[Window, ...]]:
        root = config.scheme
        if not isinstance(root, RangePartitioning):
            msg = "ensure_partitions needs a RANGE root: a HASH or LIST root has a fixed partition set"
            raise InvalidPartitionConfigError(msg)
        time_boundaries = root.time_boundaries
        windows: list[Window] = []
        for item in periods:
            if isinstance(item, Window):
                windows.append(item)
            elif time_boundaries is not None:
                windows.append(time_boundaries.window_for(item))
            else:
                msg = f"{config.qualified_name!r} is not partitioned by time; pass Window objects, not periods"
                raise InvalidPartitionConfigError(msg)
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
