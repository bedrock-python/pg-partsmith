"""Subpartition reconciliation domain service."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.protocols import NestedPartitionMetadata, SubpartitionRepository
from pg_partsmith.constants import ATTACH_CONFLICT_SQLSTATES, PG_CHECK_VIOLATION
from pg_partsmith.entities import DefaultBounds, PartitionNode
from pg_partsmith.exceptions import PartitionAlreadyExistsError, UnsupportedCapabilityError
from pg_partsmith.subpartition_plan import (
    SubpartitionAction,
    SubpartitionReconcileResult,
    TopologyFinding,
    TopologyReason,
    plan_new_subtree,
    plan_subpartitions,
)
from pg_partsmith.utils import describe_exception, pg_sqlstate, qualify

if TYPE_CHECKING:
    from collections.abc import Collection

    from pg_partsmith.entities import SubpartitionSpec, TablePartitionConfig

logger = logging.getLogger(__name__)


class PartitionSubpartitionService:
    """Converges the subtree of each time partition towards the configured spec.

    The time partition stays the lifecycle unit — it is created, detached and
    dropped as a whole — and this service owns the dimension *inside* it: the
    buckets that must exist for every row to have somewhere to land.

    Two callers, one set of rules. The creation service uses it to build a
    brand-new branch complete before it is ever attached; maintenance uses it to
    close gaps that opened in branches which already exist, whether from an
    interrupted run or from a partition that predates the current policy.
    """

    def __init__(self, repo: SubpartitionRepository, metadata: NestedPartitionMetadata) -> None:
        """Initialize the service.

        Args:
            repo: Repository providing subpartition DDL.
            metadata: Provider able to introspect a whole partition tree.
        """
        self._repo = repo
        self._metadata = metadata

    async def build_new_branch(self, config: TablePartitionConfig, branch_name: str) -> int:
        """Create every bucket a freshly created branch needs.

        Called while the branch is still detached from the root, so the whole
        subtree is in place before any row can route into it.

        Args:
            config: Table partitioning configuration; must declare a subpartition.
            branch_name: Schema-qualified name of the new branch.

        Returns:
            Number of subpartitions created.
        """
        spec = self._require_spec(config)
        return await self.materialize(plan_new_subtree(spec, branch_name))

    async def converge_branch(self, config: TablePartitionConfig, branch_name: str) -> SubpartitionReconcileResult:
        """Reconcile one existing branch against the configured spec.

        Args:
            config: Table partitioning configuration; must declare a subpartition.
            branch_name: Schema-qualified name of the branch to converge.

        Returns:
            The subpartitions created and the divergences left alone. An empty
            result when the branch does not exist.
        """
        spec = self._require_spec(config)
        node = await self._metadata.get_partition_tree(branch_name)
        if node is None:
            return SubpartitionReconcileResult()
        return await self._apply(spec, node)

    async def reconcile(
        self,
        config: TablePartitionConfig,
        *,
        exclude: Collection[str] = (),
    ) -> SubpartitionReconcileResult:
        """Converge every attached partition of the root table.

        Reconciliation deliberately covers the whole table rather than only the
        create-ahead window: a gap in a historical branch rejects writes for the
        slice of the keyspace it should own, and that does not become harmless
        just because the period is old.

        For a static (HASH_BASED / VALUE_BASED) root the table's own partitions
        are what gets converged; for a time-based one they are the subtrees
        inside each period.

        Args:
            config: Table partitioning configuration. A config that declares
                neither a root layout nor a subpartition spec reconciles to an
                empty result.
            exclude: Schema-qualified names to skip — typically the partitions
                this run is about to prune, which it would be wasteful to
                repair. Not meaningful for a static root, which has one level.

        Returns:
            The subpartitions created and the divergences left alone.
        """
        if config.root_layout is None and config.subpartition is None:
            return SubpartitionReconcileResult()

        self._require_support(config)
        root = qualify(config.db_schema, config.table_name)
        tree = await self._metadata.get_partition_tree(root)
        if tree is None:
            return SubpartitionReconcileResult()

        if config.root_layout is not None:
            # A static root *is* the level to converge: its own partitions are
            # the ones the config describes, and deeper levels come from the
            # layout's own `subpartition`, which the planner already recurses
            # into. There is nothing per-branch to iterate.
            return await self._apply(config.root_layout, tree)

        spec = self._require_spec(config)
        result = SubpartitionReconcileResult()
        for branch in tree.children:
            if not self._is_reconcilable(branch, exclude):
                continue
            result = result.merge(await self._apply_isolated(spec, branch))

        return result

    async def _apply_isolated(self, spec: SubpartitionSpec, node: PartitionNode) -> SubpartitionReconcileResult:
        """Converge one branch, keeping its failure from reaching its siblings.

        Reconciliation runs before pruning, so letting one unconvergeable
        branch propagate would also stop the table reclaiming disk -- on every
        run, forever. A branch that cannot be converged is reported and the
        others still get their turn.
        """
        try:
            return await self._apply(spec, node)
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            logger.warning(
                "Could not converge one branch; continuing with the rest of the table",
                extra={"partition_name": node.name, "error": describe_exception(exc)},
            )
            return SubpartitionReconcileResult(
                findings=(
                    TopologyFinding(
                        partition_name=node.name,
                        reason=TopologyReason.UNCONVERGEABLE,
                        detail=f"{node.name} could not be converged: {describe_exception(exc)}.",
                    ),
                )
            )

    async def materialize(
        self,
        actions: tuple[SubpartitionAction, ...],
        findings: list[TopologyFinding] | None = None,
    ) -> int:
        """Execute create-actions, deepest-first within each subtree.

        Args:
            actions: Actions from the planner.
            findings: Collector for conflicts that are reported rather than
                raised; omit it and such conflicts are only logged.

        Returns:
            Number of subpartitions attached.
        """
        collected = [] if findings is None else findings
        created = 0
        for action in actions:
            created += await self._materialize_one(action, collected)
        return created

    async def _apply(self, spec: SubpartitionSpec, node: PartitionNode) -> SubpartitionReconcileResult:
        """Plan one branch, log what it refused, and execute what it planned."""
        plan = plan_subpartitions(spec, node)

        for finding in plan.findings:
            _log_finding(finding)

        if plan.is_noop:
            return SubpartitionReconcileResult(findings=plan.findings)

        logger.info(
            "Creating missing subpartitions",
            extra={"partition_name": node.name, "subpartition_count": plan.count()},
        )
        execution: list[TopologyFinding] = []
        created = await self.materialize(plan.actions, execution)
        for finding in execution:
            _log_finding(finding)
        return SubpartitionReconcileResult(created_count=created, findings=plan.findings + tuple(execution))

    async def _materialize_one(self, action: SubpartitionAction, findings: list[TopologyFinding]) -> int:
        """Create one node, build its children, then attach it.

        Attaching last is what makes an interrupted run recoverable rather than
        harmful: until the attach commits, the node is invisible to row routing,
        so a half-built subtree can never reject a write. The next run finds the
        detached table, completes it, and attaches it.
        """
        created = 0
        try:
            await self._repo.create_subpartition_table(action.parent_name, action.child_name, action.subpartition)
        except PartitionAlreadyExistsError:
            # Left behind by an interrupted run: the table was created but never
            # attached, so it is invisible to the tree query that planned this.
            logger.info(
                "Subpartition table already exists; completing its attachment",
                extra={"partition_name": action.child_name, "parent_name": action.parent_name},
            )

        created += await self.materialize(action.children, findings)

        if await self._attach(action, findings):
            created += 1
        return created

    async def _attach(self, action: SubpartitionAction, findings: list[TopologyFinding]) -> bool:
        """Attach one node, distinguishing a lost race from a real conflict.

        Returns:
            True when this call attached the node.
        """
        try:
            await self._repo.attach_subpartition(action.parent_name, action.child_name, action.bounds)
        except SQLAlchemyError as exc:
            sqlstate = pg_sqlstate(exc)

            if sqlstate == PG_CHECK_VIOLATION:
                # Rows already sitting in a DEFAULT sibling belong to the
                # partition being attached, and PostgreSQL will not let it in
                # until they move. Reporting beats raising: one branch in this
                # state must not stop the rest of the table being maintained.
                findings.append(_default_conflict_finding(action, exc))
                return False

            if sqlstate not in ATTACH_CONFLICT_SQLSTATES:
                raise

            # A conflict SQLSTATE alone proves nothing: PostgreSQL reports the
            # same code whether another worker just created this partition or
            # an unrelated relation happens to hold the name. Only matching
            # bounds make it a lost race.
            if not await self._matches_planned_bounds(action):
                findings.append(_name_conflict_finding(action, exc))
                return False

            logger.debug(
                "Subpartition already attached (race with another worker)",
                extra={"partition_name": action.child_name, "sqlstate": sqlstate},
            )
            return False
        return True

    async def _matches_planned_bounds(self, action: SubpartitionAction) -> bool:
        """True when the relation holding this name owns the bounds we planned.

        Bounds are read from ``relpartbound``, which is only set on an attached
        partition -- so matching bounds prove both that the name belongs to a
        partition and that it is the one this action was going to create.
        """
        existing = await self._metadata.get_partition_tree(action.child_name)
        return existing is not None and existing.bounds == action.bounds

    def _require_spec(self, config: TablePartitionConfig) -> SubpartitionSpec:
        """Return the config's subpartition spec, refusing an unsupported wiring."""
        if config.subpartition is None:
            msg = "Subpartition reconciliation requires a config with a subpartition spec"
            raise ValueError(msg)

        self._require_support(config)
        return config.subpartition

    def _require_support(self, config: TablePartitionConfig) -> None:
        """Refuse collaborators that cannot serve a nested configuration."""
        del config  # the check is about the wiring, not the config

        for component, obj, protocol in (
            ("Repository", self._repo, SubpartitionRepository),
            ("Metadata provider", self._metadata, NestedPartitionMetadata),
        ):
            if not isinstance(obj, protocol):
                raise UnsupportedCapabilityError(
                    f"{component} {type(obj).__name__}", "subpartitioning", protocol.__name__
                )

    @staticmethod
    def _is_reconcilable(branch: PartitionNode, exclude: Collection[str]) -> bool:
        """True when a direct child of the root should be converged this run."""
        if branch.name in exclude or not branch.is_attached:
            return False
        # The DEFAULT partition is a catch-all leaf by design, not a time branch;
        # subpartitioning it would change where overflow rows land.
        return not isinstance(branch.bounds, DefaultBounds)


# ── Module helpers ──────────────────────────────────────────────────────────────

# A legacy leaf is a normal, indefinitely-repeating steady state during a policy
# change, so it is logged quietly; every other informational reason describes a
# one-off decision worth seeing in the log.
_QUIET_REASONS = frozenset({TopologyReason.LEGACY_LEAF})


def _log_finding(finding: TopologyFinding) -> None:
    """Log a planner finding at a level matching how much it matters."""
    extra = {"partition_name": finding.partition_name, "reason": finding.reason.value}
    if finding.is_actionable:
        logger.warning(finding.detail, extra=extra)
    elif finding.reason in _QUIET_REASONS:
        logger.debug(finding.detail, extra=extra)
    else:
        logger.info(finding.detail, extra=extra)


def _name_conflict_finding(action: SubpartitionAction, exc: Exception) -> TopologyFinding:
    """Report a name held by a relation that is not the one we planned."""
    return TopologyFinding(
        partition_name=action.parent_name,
        reason=TopologyReason.NAME_UNUSABLE,
        detail=(
            f"{action.parent_name} already has a relation named {action.child_name!r} whose bounds are not "
            f"the configured ones, so the partition could not be created ({describe_exception(exc)})."
        ),
    )


def _default_conflict_finding(action: SubpartitionAction, exc: Exception) -> TopologyFinding:
    """Report rows in a DEFAULT sibling blocking a new partition."""
    return TopologyFinding(
        partition_name=action.parent_name,
        reason=TopologyReason.DEFAULT_HOLDS_ROWS,
        detail=(
            f"{action.parent_name} cannot gain {action.child_name!r} while its DEFAULT partition holds rows "
            f"that belong to it; move them out and the next run will create it ({describe_exception(exc)})."
        ),
    )
