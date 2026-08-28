"""Desired-vs-actual planning for subpartitioned branches.

Pure and IO-free, so one implementation serves both the aio and sync mirrors
and every convergence rule is unit-testable without a database.

The planner never mutates and never guesses. Given the subpartitioning a
config asks for and the subtree that actually exists, it returns:

* :attr:`SubpartitionPlan.actions` — the nodes that are safe to create, nested
  so a branch is only attached once its own children exist, and
* :attr:`SubpartitionPlan.findings` — everything it deliberately refused to
  touch, each with the reason a human needs to act on it.

The refusals matter as much as the creations. A hash set cannot change modulus
online, an existing partition may predate the current policy, and a partition
that PostgreSQL is happy with must never be "fixed" into one it would reject.
"""

from __future__ import annotations

import logging
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from .entities import MaintenanceIssue, MaintenanceIssueStep
from .exceptions import PartitionTopologyError
from .topology import (
    DefaultBounds,
    HashBounds,
    HashSubpartitionSpec,
    ListBounds,
    ListGroup,
    ListSubpartitionSpec,
    PartitionNode,
    SubpartitionBounds,
    SubpartitionSpec,
    hash_keyspace_covered,
    missing_remainders,
    uniform_modulus,
)
from .types import NonNegativeInt, StrippedNonEmptyStr
from .utils import describe_exception, qualify, split_qualified_name

logger = logging.getLogger(__name__)


class TopologyReason(StrEnum):
    """Why the planner left an existing subtree alone.

    Attributes:
        LEGACY_LEAF: The branch is a plain table created before the current
            subpartitioning policy. PostgreSQL cannot add partitions to it.
        STRATEGY_MISMATCH: The branch is subpartitioned by a different strategy
            than the config asks for.
        COLUMN_MISMATCH: The branch is subpartitioned by the right strategy but
            on a different column.
        MODULUS_PRESERVED: The branch has a complete hash set at a modulus the
            config no longer uses. It already tiles the keyspace, so it stays.
        MODULUS_REPAIRED: The branch has an *incomplete* hash set at a modulus
            the config no longer uses; the gaps were filled at the branch's own
            modulus, which is the only modulus that cannot overlap it.
        NON_UNIFORM_COMPLETE: Hash siblings disagree on modulus but still tile
            the keyspace — legal, and left untouched.
        NON_UNIFORM_INCOMPLETE: Hash siblings disagree on modulus and leave a
            gap. Rows hashing into it are rejected, and no repair is provably
            safe, so this needs a human.
        COVERAGE_UNKNOWN: The moduli are too coarse to enumerate, so coverage
            could not be verified.
        LIST_VALUES_CONFLICT: A configured LIST group claims a value another
            partition already owns. A value belongs to exactly one partition,
            so this needs a human.
    """

    LEGACY_LEAF = "legacy_leaf"
    STRATEGY_MISMATCH = "strategy_mismatch"
    COLUMN_MISMATCH = "column_mismatch"
    MODULUS_PRESERVED = "modulus_preserved"
    MODULUS_REPAIRED = "modulus_repaired"
    NON_UNIFORM_COMPLETE = "non_uniform_complete"
    NON_UNIFORM_INCOMPLETE = "non_uniform_incomplete"
    COVERAGE_UNKNOWN = "coverage_unknown"
    LIST_VALUES_CONFLICT = "list_values_conflict"


# Reasons that describe a healthy, deliberate state (policy evolution) rather
# than something an operator has to act on. Everything else is surfaced through
# ``MaintenanceResult.issues``.
_INFORMATIONAL_REASONS = frozenset(
    {
        TopologyReason.LEGACY_LEAF,
        TopologyReason.MODULUS_PRESERVED,
        TopologyReason.MODULUS_REPAIRED,
        TopologyReason.NON_UNIFORM_COMPLETE,
    }
)


class TopologyFinding(BaseModel):
    """Something the planner observed and chose not to change.

    Attributes:
        partition_name: Schema-qualified name of the branch concerned.
        reason: Which convergence rule applied.
        detail: Human-readable explanation, safe to log or surface verbatim.
    """

    model_config = ConfigDict(frozen=True)

    partition_name: StrippedNonEmptyStr
    reason: TopologyReason
    detail: StrippedNonEmptyStr

    @property
    def is_actionable(self) -> bool:
        """True when an operator has to do something about this finding."""
        return self.reason not in _INFORMATIONAL_REASONS


class SubpartitionAction(BaseModel):
    """One partition to create, together with the subtree to build inside it.

    Executed depth-first: create ``child_name`` detached, build its own
    ``children``, and only then attach it to ``parent_name``. A subtree
    therefore becomes reachable from the parent only once it is complete, so a
    crash mid-way can never expose a branch that rejects rows.

    Attributes:
        parent_name: Schema-qualified relation the new partition attaches to.
        child_name: Schema-qualified name of the partition to create.
        bounds: Bounds to attach ``child_name`` with (hash bucket, list
            values, or DEFAULT).
        subpartition: How ``child_name`` partitions its own children, if at all.
        children: Partitions to create inside ``child_name`` before attaching it.
    """

    model_config = ConfigDict(frozen=True)

    parent_name: StrippedNonEmptyStr
    child_name: StrippedNonEmptyStr
    bounds: SubpartitionBounds
    subpartition: SubpartitionSpec | None = None
    children: tuple[SubpartitionAction, ...] = ()

    def count(self) -> int:
        """Total number of relations this action and its descendants create."""
        return 1 + sum(child.count() for child in self.children)


class SubpartitionPlan(BaseModel):
    """What to create under one branch, and what was deliberately left alone.

    Attributes:
        actions: Partitions to create, nested parent-before-child.
        findings: Divergences the planner refused to repair automatically.
    """

    model_config = ConfigDict(frozen=True)

    actions: tuple[SubpartitionAction, ...] = ()
    findings: tuple[TopologyFinding, ...] = ()

    @property
    def is_noop(self) -> bool:
        """True when converging this branch requires no DDL at all."""
        return not self.actions

    def count(self) -> NonNegativeInt:
        """Total number of relations this plan creates."""
        return sum(action.count() for action in self.actions)

    @property
    def actionable_findings(self) -> tuple[TopologyFinding, ...]:
        """Findings an operator has to act on."""
        return tuple(f for f in self.findings if f.is_actionable)


def plan_subpartitions(spec: SubpartitionSpec, node: PartitionNode) -> SubpartitionPlan:
    """Plan the DDL that converges ``node``'s subtree towards ``spec``.

    Args:
        spec: The subpartitioning the config asks for at ``node``'s level.
        node: The branch as it currently exists, with its children populated.

    Returns:
        A plan whose actions are safe to execute in order, plus findings for
        everything left untouched.
    """
    actions: list[SubpartitionAction] = []
    findings: list[TopologyFinding] = []
    _plan_into(spec, node, actions, findings)
    return SubpartitionPlan(actions=tuple(actions), findings=tuple(findings))


def plan_new_subtree(spec: SubpartitionSpec, branch_name: str) -> tuple[SubpartitionAction, ...]:
    """Plan the complete subtree of a branch that does not exist yet.

    Used on the creation path, where there is nothing to reconcile against and
    every child the spec describes has to be built.

    Args:
        spec: The subpartitioning to materialise.
        branch_name: Schema-qualified name of the branch being created.

    Returns:
        Actions creating every child described by ``spec``, nested.
    """
    if isinstance(spec, HashSubpartitionSpec):
        return _hash_actions(spec, branch_name, range(spec.modulus))
    return _list_actions(spec, branch_name, spec.groups, include_default=spec.include_default)


def _plan_into(
    spec: SubpartitionSpec,
    node: PartitionNode,
    actions: list[SubpartitionAction],
    findings: list[TopologyFinding],
) -> None:
    """Append the plan for one branch, recursing into children that match."""
    incompatibility = _incompatibility(spec, node)
    if incompatibility is not None:
        findings.append(incompatibility)
        return

    if isinstance(spec, HashSubpartitionSpec):
        recurse_into = _plan_hash_level(spec, node, actions, findings)
    else:
        recurse_into = _plan_list_level(spec, node, actions, findings)

    # Existing children may themselves be missing a level below.
    if spec.subpartition is not None:
        for child in recurse_into:
            _plan_into(spec.subpartition, child, actions, findings)


def _incompatibility(spec: SubpartitionSpec, node: PartitionNode) -> TopologyFinding | None:
    """Return a finding when ``node`` cannot host ``spec``, else None."""
    if node.is_leaf:
        return TopologyFinding(
            partition_name=node.name,
            reason=TopologyReason.LEGACY_LEAF,
            detail=(
                f"{node.name} is a plain leaf table and cannot hold "
                f"{spec.partition_type.value.upper()} subpartitions; leaving it as-is. "
                "Partitions created before the subpartitioning policy stay valid; "
                "new periods are created with the current topology."
            ),
        )

    if node.partition_type != spec.partition_type:
        return TopologyFinding(
            partition_name=node.name,
            reason=TopologyReason.STRATEGY_MISMATCH,
            detail=(
                f"{node.name} is {node.describe_topology()} but the configuration asks for "
                f"{spec.partition_type.value.upper()} ({spec.column}); leaving it as-is. "
                "Repartitioning an existing branch requires a rewrite and is not attempted."
            ),
        )

    if node.partition_columns != (spec.column,):
        actual = ", ".join(node.partition_columns) or "?"
        return TopologyFinding(
            partition_name=node.name,
            reason=TopologyReason.COLUMN_MISMATCH,
            detail=(
                f"{node.name} is partitioned by {spec.partition_type.value.upper()} ({actual}) but the "
                f"configuration asks for ({spec.column}); leaving it as-is."
            ),
        )

    return None


# ── HASH levels ─────────────────────────────────────────────────────────────────


def _plan_hash_level(
    spec: HashSubpartitionSpec,
    node: PartitionNode,
    actions: list[SubpartitionAction],
    findings: list[TopologyFinding],
) -> tuple[PartitionNode, ...]:
    """Fill the gaps in a hash bucket set; return the children worth recursing into."""
    hash_children = node.hash_children
    modulus = _effective_modulus(spec, node, hash_children, findings)
    if modulus is None:
        return ()

    bounds = tuple(c.bounds for c in hash_children if isinstance(c.bounds, HashBounds))
    gaps = missing_remainders(modulus, bounds)
    actions.extend(_hash_actions(spec, node.name, gaps, modulus=modulus))
    return hash_children


def _effective_modulus(
    spec: HashSubpartitionSpec,
    node: PartitionNode,
    hash_children: tuple[PartitionNode, ...],
    findings: list[TopologyFinding],
) -> int | None:
    """Decide which modulus to fill gaps at, or None to leave the branch alone.

    A hash set tiles the keyspace at whatever moduli its members already use,
    and those cannot be changed without a rewrite. So a branch that disagrees
    with the configured bucket count is repaired *at its own modulus* when it
    still has gaps — rows hashing into a gap are rejected outright — and left
    completely alone when it does not.
    """
    if not hash_children:
        # Either a brand-new branch or one whose children were lost before they
        # were created; the configured count is the only sensible choice.
        return spec.modulus

    bounds = tuple(c.bounds for c in hash_children if isinstance(c.bounds, HashBounds))
    existing = uniform_modulus(bounds)

    if existing is None:
        findings.append(_non_uniform_finding(node, bounds, spec))
        return None

    if existing == spec.modulus:
        return existing

    gaps = missing_remainders(existing, bounds)
    if not gaps:
        findings.append(
            TopologyFinding(
                partition_name=node.name,
                reason=TopologyReason.MODULUS_PRESERVED,
                detail=(
                    f"{node.name} has a complete {existing}-bucket hash set and the configuration now asks "
                    f"for {spec.modulus}; leaving it as-is. It already tiles the whole keyspace, and changing "
                    "a hash modulus requires a rewrite — new periods use the configured count."
                ),
            )
        )
        return None

    findings.append(
        TopologyFinding(
            partition_name=node.name,
            reason=TopologyReason.MODULUS_REPAIRED,
            detail=(
                f"{node.name} has an incomplete {existing}-bucket hash set "
                f"({len(bounds)} of {existing} present); filling the gaps at that modulus because the "
                f"configured count ({spec.modulus}) would overlap the existing buckets."
            ),
        )
    )
    return existing


def _non_uniform_finding(
    node: PartitionNode,
    bounds: tuple[HashBounds, ...],
    spec: HashSubpartitionSpec,
) -> TopologyFinding:
    """Classify a branch whose hash siblings disagree on modulus.

    Mixed moduli are legal in PostgreSQL as long as the residue classes do not
    overlap, so the question is not whether the layout is uniform but whether
    it is *complete*. Only a genuine gap is actionable.
    """
    moduli = sorted({b.modulus for b in bounds})
    covered = hash_keyspace_covered(bounds)

    if covered is None:
        return TopologyFinding(
            partition_name=node.name,
            reason=TopologyReason.COVERAGE_UNKNOWN,
            detail=(
                f"{node.name} has hash children with moduli {moduli} whose least common multiple is too "
                "large to verify coverage for; leaving it as-is."
            ),
        )

    if covered:
        return TopologyFinding(
            partition_name=node.name,
            reason=TopologyReason.NON_UNIFORM_COMPLETE,
            detail=(
                f"{node.name} has hash children with mixed moduli {moduli} that still tile the whole "
                f"keyspace; leaving it as-is (the configuration asks for {spec.modulus} buckets)."
            ),
        )

    return TopologyFinding(
        partition_name=node.name,
        reason=TopologyReason.NON_UNIFORM_INCOMPLETE,
        detail=(
            f"{node.name} has hash children with inconsistent moduli {moduli} that leave part of the hash "
            "keyspace uncovered; rows hashing into the gap are rejected. No repair is provably safe here, "
            "so the branch is left untouched for manual inspection."
        ),
    )


def _hash_actions(
    spec: HashSubpartitionSpec,
    parent_name: str,
    remainders: range | tuple[int, ...],
    *,
    modulus: int | None = None,
) -> tuple[SubpartitionAction, ...]:
    """Build create-actions for ``remainders`` under ``parent_name``.

    ``modulus`` overrides the spec's bucket count when repairing a branch that
    already uses a different one; the names still come from the spec so a
    repaired branch stays addressable by the same convention.
    """
    schema, parent_relname = split_qualified_name(parent_name)
    effective = spec.modulus if modulus is None else modulus

    return tuple(
        _action(
            spec,
            parent_name,
            qualify(schema, spec.child_name(parent_relname, remainder)),
            HashBounds(modulus=effective, remainder=remainder),
        )
        for remainder in remainders
    )


# ── LIST levels ─────────────────────────────────────────────────────────────────


def _plan_list_level(
    spec: ListSubpartitionSpec,
    node: PartitionNode,
    actions: list[SubpartitionAction],
    findings: list[TopologyFinding],
) -> tuple[PartitionNode, ...]:
    """Create the LIST partitions a branch is missing; return children to recurse into.

    Unlike a hash set, a LIST level is never complete — there is always another
    value the world could produce — so there is no "gap" to detect, only groups
    that are not there yet. Groups are matched by the values they own rather
    than by name, so a tree an earlier tool named differently is recognised
    instead of duplicated.
    """
    claimed: dict[str, str] = {}
    present: set[frozenset[str]] = set()
    has_default = False

    for child in node.children:
        if isinstance(child.bounds, ListBounds):
            present.add(frozenset(child.bounds.values))
            for value in child.bounds.values:
                claimed[value] = child.name
        elif isinstance(child.bounds, DefaultBounds):
            has_default = True

    missing = []
    for group in spec.groups:
        if frozenset(group.values) in present:
            continue

        conflicts = {v: claimed[v] for v in group.values if v in claimed}
        if conflicts:
            findings.append(_list_conflict_finding(node, group, conflicts))
            continue

        missing.append(group)
        for value in group.values:
            claimed[value] = "(pending)"

    actions.extend(
        _list_actions(spec, node.name, tuple(missing), include_default=spec.include_default and not has_default)
    )
    return tuple(c for c in node.children if isinstance(c.bounds, (ListBounds, DefaultBounds)))


def _list_conflict_finding(
    node: PartitionNode,
    group: ListGroup,
    conflicts: dict[str, str],
) -> TopologyFinding:
    """Report a configured group whose values another partition already owns."""
    detail = ", ".join(f"{value!r} in {owner}" for value, owner in sorted(conflicts.items()))
    return TopologyFinding(
        partition_name=node.name,
        reason=TopologyReason.LIST_VALUES_CONFLICT,
        detail=(
            f"{node.name} cannot gain the configured LIST partition {group.name!r}: PostgreSQL already "
            f"routes {detail}. A value belongs to exactly one partition, and moving one requires detaching "
            "the partition that holds it, so this is left for manual inspection."
        ),
    )


def _list_actions(
    spec: ListSubpartitionSpec,
    parent_name: str,
    groups: tuple[ListGroup, ...],
    *,
    include_default: bool,
) -> tuple[SubpartitionAction, ...]:
    """Build create-actions for ``groups`` (and optionally DEFAULT) under a parent."""
    schema, parent_relname = split_qualified_name(parent_name)

    actions = [
        _action(
            spec,
            parent_name,
            qualify(schema, spec.child_name(parent_relname, group.name)),
            group.bounds(),
        )
        for group in groups
    ]

    if include_default:
        actions.append(
            _action(
                spec,
                parent_name,
                qualify(schema, spec.child_name(parent_relname, spec.default_name)),
                DefaultBounds(),
            )
        )

    return tuple(actions)


def _action(
    spec: SubpartitionSpec,
    parent_name: str,
    child_name: str,
    bounds: SubpartitionBounds,
) -> SubpartitionAction:
    """Build one create-action, with the subtree that must exist inside it."""
    return SubpartitionAction(
        parent_name=parent_name,
        child_name=child_name,
        bounds=bounds,
        subpartition=spec.subpartition,
        children=plan_new_subtree(spec.subpartition, child_name) if spec.subpartition is not None else (),
    )


class SubpartitionReconcileResult(BaseModel):
    """Outcome of converging one or more branches towards their spec.

    Attributes:
        created_count: Subpartitions actually attached during this run.
        findings: Divergences the planner refused to repair automatically.
    """

    model_config = ConfigDict(frozen=True)

    created_count: NonNegativeInt = 0
    findings: tuple[TopologyFinding, ...] = ()

    def merge(self, other: SubpartitionReconcileResult) -> SubpartitionReconcileResult:
        """Combine two results, preserving finding order."""
        return SubpartitionReconcileResult(
            created_count=self.created_count + other.created_count,
            findings=self.findings + other.findings,
        )


def to_maintenance_issue(finding: TopologyFinding) -> MaintenanceIssue:
    """Render an actionable finding as a ``MaintenanceResult.issues`` entry.

    Routed through :class:`~pg_partsmith.exceptions.PartitionTopologyError` and
    ``describe_exception`` so topology problems read exactly like every other
    recorded issue instead of introducing a second reporting format.
    """
    error = PartitionTopologyError(finding.partition_name, finding.reason.value, finding.detail)
    return MaintenanceIssue(
        step=MaintenanceIssueStep.RECONCILE,
        error=describe_exception(error),
        partition_name=finding.partition_name,
    )
