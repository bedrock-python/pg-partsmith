"""Subpartition reconciliation domain service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.constants import ATTACH_CONFLICT_SQLSTATES
from pg_partsmith.entities import DefaultBounds, PartitionNode
from pg_partsmith.exceptions import PartitionAlreadyExistsError, SubpartitioningNotSupportedError
from pg_partsmith.subpartition_plan import (
    SubpartitionAction,
    SubpartitionReconcileResult,
    TopologyFinding,
    TopologyReason,
    plan_new_subtree,
    plan_subpartitions,
)
from pg_partsmith.utils import pg_sqlstate, qualify

if TYPE_CHECKING:
    from collections.abc import Collection

    from pg_partsmith.aio.protocols import NestedPartitionMetadata, SubpartitionRepository
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

        Args:
            config: Table partitioning configuration. A config without a
                subpartition spec reconciles to an empty result.
            exclude: Schema-qualified names to skip — typically the partitions
                this run is about to prune, which it would be wasteful to repair.

        Returns:
            The subpartitions created and the divergences left alone.
        """
        if config.subpartition is None:
            return SubpartitionReconcileResult()

        spec = self._require_spec(config)
        root = qualify(config.db_schema, config.table_name)
        tree = await self._metadata.get_partition_tree(root)
        if tree is None:
            return SubpartitionReconcileResult()

        result = SubpartitionReconcileResult()
        for branch in tree.children:
            if not self._is_reconcilable(branch, exclude):
                continue
            result = result.merge(await self._apply(spec, branch))

        return result

    async def materialize(self, actions: tuple[SubpartitionAction, ...]) -> int:
        """Execute create-actions, deepest-first within each subtree.

        Args:
            actions: Actions from the planner.

        Returns:
            Number of subpartitions attached.
        """
        created = 0
        for action in actions:
            created += await self._materialize_one(action)
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
        created = await self.materialize(plan.actions)
        return SubpartitionReconcileResult(created_count=created, findings=plan.findings)

    async def _materialize_one(self, action: SubpartitionAction) -> int:
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

        created += await self.materialize(action.children)

        if await self._attach(action):
            created += 1
        return created

    async def _attach(self, action: SubpartitionAction) -> bool:
        """Attach one node, tolerating a concurrent worker having won the race.

        Returns:
            True when this call attached the node.
        """
        try:
            await self._repo.attach_subpartition(action.parent_name, action.child_name, action.bounds)
        except SQLAlchemyError as exc:
            if pg_sqlstate(exc) not in ATTACH_CONFLICT_SQLSTATES:
                raise
            # A conflict SQLSTATE only proves a lost race if the postcondition
            # actually holds; 42809 also fires for unrelated object mismatches.
            if not await self._metadata.is_partition_attached(action.parent_name, action.child_name):
                raise
            logger.debug(
                "Subpartition already attached (race with another worker)",
                extra={"partition_name": action.child_name, "sqlstate": pg_sqlstate(exc)},
            )
            return False
        return True

    def _require_spec(self, config: TablePartitionConfig) -> SubpartitionSpec:
        """Return the config's subpartition spec, refusing an unsupported wiring."""
        if config.subpartition is None:
            msg = "Subpartition reconciliation requires a config with a subpartition spec"
            raise ValueError(msg)

        for component, obj, expected in (
            ("Repository", self._repo, "SubpartitionRepository"),
            ("Metadata provider", self._metadata, "NestedPartitionMetadata"),
        ):
            if not _supports(obj, expected):
                raise SubpartitioningNotSupportedError(f"{component} {type(obj).__name__}", expected)

        return config.subpartition

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

_REQUIRED_METHODS = {
    "SubpartitionRepository": ("create_branch", "create_subpartition_table", "attach_subpartition"),
    "NestedPartitionMetadata": ("get_partition_tree", "get_unique_constraint_columns", "is_partition_attached"),
}


def _supports(obj: object, protocol_name: str) -> bool:
    """Check an injected component against the methods a nested config needs."""
    return all(callable(getattr(obj, name, None)) for name in _REQUIRED_METHODS[protocol_name])


def _log_finding(finding: TopologyFinding) -> None:
    """Log a planner finding at a level matching how much it matters."""
    extra = {"partition_name": finding.partition_name, "reason": finding.reason.value}
    if finding.is_actionable:
        logger.warning(finding.detail, extra=extra)
    elif finding.reason in _QUIET_REASONS:
        logger.debug(finding.detail, extra=extra)
    else:
        logger.info(finding.detail, extra=extra)
