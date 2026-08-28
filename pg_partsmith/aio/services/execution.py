"""Executes a maintenance plan, one operation at a time.

The executor keeps the transaction semantics the library has always had --
every statement commits on its own -- and adds two things on top of the
plan's order:

* **attach last.** A partition is created detached, its subtree is built
  inside it, and only then is it attached to its parent. Until that attach
  commits the partition is invisible to row routing, so a crash anywhere
  before it leaves a detached table that no writer can reach -- never a live
  branch that rejects part of its keyspace. The next run finds the table,
  completes it, and attaches it.
* **revalidate before destroying.** A detach or drop is executed only if the
  relation still has the OID the plan decided about; a table dropped and
  recreated under the same name between plan and apply is left alone.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.boundaries import Window
from pg_partsmith.constants import ATTACH_CONFLICT_SQLSTATES, DEFAULT_CONFLICT_MAX_RETRIES
from pg_partsmith.entities import MaintenanceIssue, MaintenanceIssueStep, MaintenanceResult, PartitionInfo
from pg_partsmith.exceptions import (
    PartitionAlreadyExistsError,
    PartitionAttachedError,
    PartitionTopologyError,
    PlanStaleError,
)
from pg_partsmith.plan import (
    AttachPartition,
    CreatePartition,
    DetachPartition,
    DropPartition,
    FindingReason,
    MaintenancePlan,
    Operation,
)
from pg_partsmith.planner import PlanMode, PlanningContext, plan_maintenance, to_maintenance_issue
from pg_partsmith.scheme import RangePartitioning
from pg_partsmith.topology import ActualTree, PartitionBounds, PartitionType, RangeBounds, RelationKind
from pg_partsmith.utils import describe_exception, is_default_partition_conflict, pg_sqlstate, split_qualified_name

if TYPE_CHECKING:
    from pg_partsmith.aio.hooks import PartitionLifecycleHooks
    from pg_partsmith.aio.protocols import PartitionMetadataProvider, PartitionRepository
    from pg_partsmith.entities import TablePartitionConfig

logger = logging.getLogger(__name__)


@dataclass
class _Tally:
    """Counters accumulated while applying a plan."""

    created: int = 0
    repaired: int = 0
    attached: int = 0
    detached: int = 0
    dropped: int = 0


class PlanExecutor:
    """Applies a :class:`~pg_partsmith.plan.MaintenancePlan` to the database."""

    def __init__(
        self,
        repo: PartitionRepository,
        metadata: PartitionMetadataProvider,
        hooks: list[PartitionLifecycleHooks] | None = None,
    ) -> None:
        self._repo = repo
        self._metadata = metadata
        self._hooks = hooks or []

    async def apply(
        self,
        config: TablePartitionConfig,
        plan: MaintenancePlan,
        *,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Execute every operation of ``plan`` in order.

        A topology conflict discovered while executing -- a DEFAULT sibling
        holding rows, a name held by a relation with other bounds -- is
        recorded as an issue and never aborts the run: one odd partition must
        not stop every other one from being maintained. Any other failure
        aborts the run unless ``continue_on_error`` is set, in which case it
        is recorded and the next operation runs.

        Args:
            config: The configuration the plan was made from.
            plan: The plan.
            continue_on_error: Isolate operation failures into
                ``MaintenanceResult.issues`` instead of aborting.

        Returns:
            The counters, the issues, and the plan itself.
        """
        tally = _Tally()
        issues: list[MaintenanceIssue] = [to_maintenance_issue(f) for f in plan.actionable_findings]
        detached: set[str] = set()

        for op in plan.operations:
            step = _step_of(op)
            try:
                if isinstance(op, CreatePartition):
                    await self._create(config, plan, op, depth=0, tally=tally, issues=issues)
                elif isinstance(op, AttachPartition):
                    await self._reattach(config, plan, op, tally=tally, issues=issues)
                elif isinstance(op, DetachPartition):
                    await self.detach_single_partition(config.qualified_name, op)
                    detached.add(op.target)
                    tally.detached += 1
                elif isinstance(op, DropPartition):
                    if op.follows_detach and op.target not in detached:
                        logger.info(
                            "Skipping drop because the detach it follows did not happen",
                            extra={"partition_name": op.target},
                        )
                        continue
                    if await self.drop_single_partition(config.qualified_name, op):
                        tally.dropped += 1
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                raise
            except PartitionTopologyError as exc:
                issues.append(MaintenanceIssue(step=step, error=describe_exception(exc), partition_name=op.target))
                logger.warning(exc.detail, extra={"partition_name": op.target, "reason": exc.reason})
            except Exception as exc:
                if not continue_on_error:
                    raise
                error = describe_exception(exc)
                issues.append(MaintenanceIssue(step=step, error=error, partition_name=op.target))
                logger.warning(
                    "Maintenance operation failed; continuing with the remaining ones",
                    extra={"table_name": config.qualified_name, "step": step.value, "error": error},
                )

        return MaintenanceResult(
            created_count=tally.created,
            repaired_count=tally.repaired,
            attached_count=tally.attached,
            detached_count=tally.detached,
            dropped_count=tally.dropped,
            issues=tuple(issues),
            plan=plan,
        )

    # ── Creation ────────────────────────────────────────────────────────────────

    async def _create(
        self,
        config: TablePartitionConfig,
        plan: MaintenancePlan,
        op: CreatePartition,
        *,
        depth: int,
        tally: _Tally,
        issues: list[MaintenanceIssue],
    ) -> None:
        """Create one partition with its subtree, then attach it.

        ``depth`` is the index into ``config.levels`` of the level the new
        partition belongs to: the root's own partitions are at depth 0.
        """
        fires_hooks = op.counts_as == "created"
        info = _partition_info(config, op)
        if fires_hooks:
            await self._run_hooks(lambda h: h.before_create(config, info), "before_create", partition_name=op.target)

        try:
            await self._repo.create_table_like(op.parent_name, op.target, op.partition_by)
        except PartitionAlreadyExistsError:
            # The relation exists but the plan did not see it attached: either
            # a previous run stopped between creating it and attaching it, or
            # another worker created it since the plan was made.
            if await self._metadata.is_partition_attached(op.parent_name, op.target):
                await self._require_planned_bounds(op.parent_name, op.target, op.bounds)
                logger.debug(
                    "Partition already attached (race with another worker)", extra={"partition_name": op.target}
                )
                return
            logger.info(
                "Relation already exists but is not attached; completing its subtree before attaching it",
                extra={"partition_name": op.target},
            )
            tally.repaired += await self._converge_detached(config, plan, op, depth=depth, issues=issues)
        else:
            for child in op.children:
                await self._create(config, plan, child, depth=depth + 1, tally=tally, issues=issues)

        await self._attach_with_reconcile(config, op.parent_name, op.target, op.bounds, key_columns=op.key_columns)

        if op.counts_as == "created":
            tally.created += 1
        elif op.counts_as == "repaired":
            tally.repaired += 1

        if fires_hooks:
            attached = info.model_copy(update={"is_attached": True})
            await self._run_hooks(lambda h: h.after_create(config, attached), "after_create", partition_name=op.target)

    async def _converge_detached(
        self,
        config: TablePartitionConfig,
        plan: MaintenancePlan,
        op: CreatePartition | AttachPartition,
        *,
        depth: int,
        issues: list[MaintenanceIssue],
    ) -> int:
        """Complete the subtree of a detached relation before it goes live.

        The relation is the root of its own tree, so the planner is pointed at
        it with the scheme below the operation's level, and every gap it finds
        is filled the same way any other creation is. Returns the number of
        relations created.
        """
        levels = config.levels
        if depth + 1 >= len(levels):
            return 0
        tree = await self._metadata.get_partition_tree(op.target)
        if tree is None:
            return 0

        below = levels[depth + 1]
        schema, relname = split_qualified_name(op.target)
        sub_config = config.model_copy(update={"scheme": below, "schema_name": schema, "table_name": relname})
        children = op.children if isinstance(op, CreatePartition) else ()
        context = PlanningContext(
            now=plan.generated_at,
            cursors=dict(plan.cursors),
            mode=PlanMode.EXPLICIT,
            explicit_windows=_windows_of(children, below),
        )
        sub_plan = plan_maintenance(sub_config, ActualTree(root=tree), context)
        issues.extend(to_maintenance_issue(f) for f in sub_plan.actionable_findings)

        created = 0
        for child in sub_plan.creates:
            # Gaps filled inside a relation that already exists are repairs:
            # they count as such and, like every other repair, fire no hooks.
            repair = child.model_copy(update={"counts_as": "repaired"})
            nested = _Tally()
            await self._create(config, plan, repair, depth=depth + 1, tally=nested, issues=issues)
            created += child.count()
        return created

    async def _reattach(
        self,
        config: TablePartitionConfig,
        plan: MaintenancePlan,
        op: AttachPartition,
        *,
        tally: _Tally,
        issues: list[MaintenanceIssue],
    ) -> None:
        """Bring a detached orphan back, completing its subtree first."""
        await self._require_same_relation(op.target, op.oid)
        depth = _depth_of(config, op.parent_name)
        tally.repaired += await self._converge_detached(config, plan, op, depth=depth, issues=issues)
        await self._attach_with_reconcile(config, op.parent_name, op.target, op.bounds, key_columns=op.key_columns)
        tally.attached += 1

    async def _attach_with_reconcile(
        self,
        config: TablePartitionConfig,
        parent_name: str,
        partition_name: str,
        bounds: PartitionBounds,
        *,
        key_columns: tuple[str, ...],
    ) -> None:
        """Attach a partition, moving DEFAULT rows out of the way for a RANGE window.

        If the attach ultimately fails after rows were reconciled out of the
        DEFAULT partition, the moved rows are returned to DEFAULT (best effort)
        so they do not end up stranded in a table that is invisible through the
        parent. A lost race with another worker that attached the same
        partition first is benign; the same name attached with other bounds is
        a conflict.
        """
        key_arity = max(1, len(key_columns))
        reconciled_from: str | None = None
        window = bounds if isinstance(bounds, RangeBounds) else None

        for attempt in range(1, DEFAULT_CONFLICT_MAX_RETRIES + 1):
            try:
                await self._repo.attach_partition(parent_name, partition_name, bounds, key_arity=key_arity)
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                # Shielded so the compensating move-back completes even mid-cancellation.
                await asyncio.shield(
                    self._restore_reconciled_rows(reconciled_from, partition_name, window, key_columns)
                )
                raise
            except (OSError, TimeoutError):
                await self._restore_reconciled_rows(reconciled_from, partition_name, window, key_columns)
                raise
            except SQLAlchemyError as exc:
                if pg_sqlstate(exc) in ATTACH_CONFLICT_SQLSTATES:
                    await self._restore_reconciled_rows(reconciled_from, partition_name, window, key_columns)
                    if await self._metadata.is_partition_attached(parent_name, partition_name):
                        await self._require_planned_bounds(parent_name, partition_name, bounds)
                        logger.debug(
                            "Partition already attached (race with another worker)",
                            extra={"partition_name": partition_name, "sqlstate": pg_sqlstate(exc)},
                        )
                        return
                    raise

                if not is_default_partition_conflict(exc):
                    await self._restore_reconciled_rows(reconciled_from, partition_name, window, key_columns)
                    raise

                if window is None:
                    # Rows already sitting in a DEFAULT sibling belong to the
                    # partition being attached, and PostgreSQL will not let it
                    # in until they move. Only a RANGE window can be moved by
                    # its key; anything else is reported for a human.
                    raise PartitionTopologyError(
                        parent_name,
                        FindingReason.DEFAULT_HOLDS_ROWS.value,
                        f"{parent_name} cannot gain {partition_name!r} while its DEFAULT partition holds rows that "
                        f"belong to it; move them out and the next run will attach it ({describe_exception(exc)}).",
                    ) from exc

                if attempt == DEFAULT_CONFLICT_MAX_RETRIES:
                    logger.exception(
                        "Failed to attach after reconciliation retries",
                        extra={"partition_name": partition_name, "attempts": attempt},
                    )
                    await self._restore_reconciled_rows(reconciled_from, partition_name, window, key_columns)
                    raise

                default_partition = await self._metadata.get_default_partition(parent_name)
                if not default_partition:
                    logger.warning(
                        "DEFAULT conflict detected but no DEFAULT partition found",
                        extra={"partition_name": partition_name},
                    )
                    await self._restore_reconciled_rows(reconciled_from, partition_name, window, key_columns)
                    raise

                logger.info(
                    "Reconciling DEFAULT partition before attach",
                    extra={
                        "partition_name": partition_name,
                        "default_partition": default_partition.name,
                        "attempt": attempt,
                    },
                )
                moved = await self._repo.reconcile_default_rows(
                    default_partition_name=default_partition.name,
                    target_partition_name=partition_name,
                    key_columns=key_columns,
                    from_value=window.from_value,
                    to_value=window.to_value,
                )
                if moved:
                    reconciled_from = default_partition.name
                logger.info("Reconciliation completed", extra={"partition_name": partition_name, "moved_rows": moved})
            else:
                return

    async def _restore_reconciled_rows(
        self,
        default_partition_name: str | None,
        partition_name: str,
        window: RangeBounds | None,
        key_columns: tuple[str, ...],
    ) -> None:
        """Return previously reconciled rows to the DEFAULT partition (best effort).

        Reconciliation commits independently of ATTACH, so a final attach
        failure would otherwise leave the moved rows in a standalone table
        that no query against the parent can see. Failures here are logged,
        never raised, so the original attach error stays the primary one.
        """
        if default_partition_name is None or window is None:
            return
        try:
            restored = await self._repo.reconcile_default_rows(
                default_partition_name=partition_name,
                target_partition_name=default_partition_name,
                key_columns=key_columns,
                from_value=window.from_value,
                to_value=window.to_value,
            )
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            logger.exception(
                "Failed to return reconciled rows to DEFAULT partition; rows remain in the detached table",
                extra={"partition_name": partition_name, "default_partition": default_partition_name},
            )
        else:
            logger.warning(
                "Attach failed after reconciliation; returned rows to DEFAULT partition",
                extra={
                    "partition_name": partition_name,
                    "default_partition": default_partition_name,
                    "restored_rows": restored,
                },
            )

    async def _require_planned_bounds(self, parent_name: str, partition_name: str, bounds: PartitionBounds) -> None:
        """A conflict is a lost race only when the relation owns the planned bounds.

        PostgreSQL reports the same SQLSTATE whether another worker just
        created this partition or an unrelated relation happens to hold the
        name. Only matching bounds make it benign.
        """
        existing = await self._metadata.get_partition_tree(partition_name)
        if existing is not None and existing.bounds == bounds:
            return
        raise PartitionTopologyError(
            parent_name,
            FindingReason.NAME_UNUSABLE.value,
            f"{parent_name} already has a relation named {partition_name!r} whose bounds are not the planned ones, "
            "so the partition could not be created.",
        )

    # ── Removal ─────────────────────────────────────────────────────────────────

    async def detach_single_partition(self, table_name: str, op: DetachPartition) -> None:
        """Detach one partition, running the detach hooks around it.

        Extension point for callers that manage partitions one at a time.

        Raises:
            PlanStaleError: If the relation is no longer the one the plan saw,
                or is no longer attached.
        """
        await self._require_same_relation(op.target, op.oid)
        if not await self._metadata.is_partition_attached(op.parent_name, op.target):
            raise PlanStaleError(op.target, f"it is no longer attached to {op.parent_name}")

        info = _detach_info(op)
        await self._run_hooks(lambda h: h.before_detach(table_name, info), "before_detach", partition_name=op.target)
        await self._repo.detach_partition(op.parent_name, op.target, mode=op.mode)
        await self._run_hooks(lambda h: h.after_detach(table_name, op.target), "after_detach", partition_name=op.target)

    async def drop_single_partition(self, table_name: str, op: DropPartition) -> bool:
        """Drop one detached partition, running the drop hooks around it.

        Extension point for callers that manage partitions one at a time.

        Returns:
            True when the partition was dropped; False when it is still
            attached (logged and skipped).
        """
        await self._run_hooks(lambda h: h.before_drop(table_name, op.target), "before_drop", partition_name=op.target)
        try:
            await self._repo.drop_partition(op.target, expected_oid=op.oid)
        except PartitionAttachedError:
            logger.warning("Refusing to drop attached partition", extra={"partition_name": op.target})
            return False
        await self._run_hooks(lambda h: h.after_drop(table_name, op.target), "after_drop", partition_name=op.target)
        return True

    async def _require_same_relation(self, name: str, expected_oid: int | None) -> None:
        """Refuse to act on a relation the plan did not see."""
        if expected_oid is None:
            return
        actual = await self._metadata.get_relation_oid(name)
        if actual is None:
            raise PlanStaleError(name, "the relation no longer exists")
        if actual != expected_oid:
            raise PlanStaleError(
                name, f"the relation now holding the name has OID {actual}, the plan saw {expected_oid}"
            )

    # ── Hooks ───────────────────────────────────────────────────────────────────

    async def _run_hooks(
        self,
        hook_caller: Callable[[PartitionLifecycleHooks], Awaitable[None]],
        hook_name: str,
        *,
        partition_name: str,
    ) -> None:
        """Execute a specific hook across all registered lifecycle hooks."""
        for hook in self._hooks:
            try:
                await hook_caller(hook)
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                raise
            except (ValueError, TypeError, RuntimeError) as e:
                logger.warning(
                    f"{hook_name} hook failed",
                    extra={"partition_name": partition_name, "hook_type": type(hook).__name__, "error": str(e)},
                )
                raise
            except Exception:
                logger.exception(
                    f"{hook_name} hook failed with unexpected error",
                    extra={"partition_name": partition_name, "hook_type": type(hook).__name__},
                )
                raise


# ── Module helpers ──────────────────────────────────────────────────────────────


def _step_of(op: Operation) -> MaintenanceIssueStep:
    if isinstance(op, CreatePartition):
        return MaintenanceIssueStep.RECONCILE if op.counts_as == "repaired" else MaintenanceIssueStep.CREATE
    if isinstance(op, AttachPartition):
        return MaintenanceIssueStep.ATTACH
    if isinstance(op, DetachPartition):
        return MaintenanceIssueStep.DETACH
    return MaintenanceIssueStep.DROP


def _detach_info(op: DetachPartition) -> PartitionInfo:
    """What hooks see of a partition about to be detached.

    Built without validation: the plan may know the partition only by name and
    OID (a caller handing in a listing that carried a raw bound, say), and a
    hook context must not be refused for lacking a bound the operation does
    not need.
    """
    bounds = op.bounds
    from_value = to_value = None
    if isinstance(bounds, RangeBounds):
        from_value, to_value = bounds.from_value, bounds.to_value
    return PartitionInfo.model_construct(
        name=op.target,
        oid=op.oid,
        partition_type=_parent_method(bounds),
        from_value=from_value,
        to_value=to_value,
        boundaries_expr=None if bounds is None else bounds.kind,
        bounds=bounds,
        is_attached=True,
        is_default=bounds is not None and bounds.kind == "default",
        relkind=RelationKind.TABLE,
        subpartition_type=None,
        parent_table=op.parent_name,
    )


def _partition_info(config: TablePartitionConfig, op: CreatePartition) -> PartitionInfo:
    """What hooks see of a partition about to be created."""
    bounds = op.bounds
    return PartitionInfo(
        name=op.target,
        partition_type=_parent_method(bounds) or config.partition_type,
        bounds=bounds,
        boundaries_expr=bounds.kind,
        is_attached=False,
        is_default=bounds.kind == "default",
        subpartition_type=None if op.partition_by is None else op.partition_by.method,
        parent_table=op.parent_name,
    )


def _parent_method(bounds: PartitionBounds | None) -> PartitionType:
    if bounds is None or bounds.kind in {"range", "default"}:
        return PartitionType.RANGE
    return PartitionType.HASH if bounds.kind == "hash" else PartitionType.LIST


def _depth_of(config: TablePartitionConfig, parent_name: str) -> int:
    """Index of the level whose members ``parent_name`` holds.

    Re-attachments are planned for progression levels; the root is the
    common case and the only one the name alone decides. Deeper parents fall
    back to the first progression level below the root.
    """
    if parent_name == config.qualified_name:
        return 0
    for index, level in enumerate(config.levels):
        if index > 0 and isinstance(level, RangePartitioning):
            return index
    return 0


def _windows_of(children: tuple[CreatePartition, ...], level: object) -> dict[str, tuple[Window, ...]]:
    """The RANGE windows a planned subtree was going to create, for EXPLICIT planning."""
    if not isinstance(level, RangePartitioning):
        return {}
    boundaries = level.range_boundaries
    windows: list[Window] = []
    for child in children:
        if isinstance(child.bounds, RangeBounds):
            start = boundaries.decode(child.bounds.from_value)
            end = boundaries.decode(child.bounds.to_value)
            if start is not None and end is not None:
                windows.append(Window(start=start, end=end))
    return {level.leading_column: tuple(windows)} if windows else {}
