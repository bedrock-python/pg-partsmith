"""Desired-vs-actual planning for a whole partition tree.

Pure and IO-free, so one implementation serves both the aio and sync mirrors
and every convergence rule is unit-testable without a database.

:func:`plan_maintenance` walks the configured :data:`~pg_partsmith.scheme.PartitionScheme`
and the introspected :class:`~pg_partsmith.topology.ActualTree` side by side.
At a **progression level** (RANGE, or a LIST over an integer sequence) it
decides which windows must exist ahead of the cursor, which existing ones have
expired, and which orphans may be dropped; at a **set level** (HASH, LIST with
explicit groups) it fills the gaps in the member set.
Either way it returns a :class:`~pg_partsmith.plan.MaintenancePlan`: what to
do, in order, with a reason on every operation — and what it deliberately
refused to touch, with a reason on every finding.

The planner never mutates and never guesses. Ownership is derived from the
catalog: an attached partition whose bounds lie on the scheme's grid is a
lifecycle partition; one whose bounds do not is reported and left alone.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .boundaries import Axis, CursorSource, RangeBoundaries, Window
from .constants import MAX_IDENTIFIER_LENGTH
from .entities import MaintenanceIssue, MaintenanceIssueStep, TablePartitionConfig
from .exceptions import PartitionTopologyError
from .lifecycle import Candidate, DropAfter, LifecyclePolicy
from .plan import (
    AttachPartition,
    CreatePartition,
    DetachPartition,
    DropPartition,
    Finding,
    FindingReason,
    MaintenancePlan,
    Operation,
    PartitionBy,
    Reason,
)
from .scheme import HashPartitioning, ListGroup, ListPartitioning, RangePartitioning, SchemeBase
from .topology import (
    ActualTree,
    DefaultBounds,
    DetachedPartition,
    HashBounds,
    ListBounds,
    PartitionBounds,
    PartitionFacts,
    PartitionNode,
    RangeBounds,
    RelationKind,
    hash_keyspace_covered,
    missing_remainders,
    uniform_modulus,
)
from .utils import describe_exception, qualify, split_qualified_name

__all__ = ["PlanMode", "PlanningContext", "fact_targets", "plan_maintenance", "to_maintenance_issue"]


class PlanMode(StrEnum):
    """What a plan is for.

    Attributes:
        MAINTAIN: The scheduled tick: create ahead, reconcile, expire, drop.
        RECONCILE: Converge the existing tree only; create nothing ahead,
            expire nothing.
        EXPLICIT: Ensure the windows the caller named exist, subtree included;
            expire nothing.
    """

    MAINTAIN = "maintain"
    RECONCILE = "reconcile"
    EXPLICIT = "explicit"


class PlanningContext(BaseModel):
    """What the planner knows beyond the config and the tree.

    Attributes:
        now: The instant the plan is made; every policy is evaluated against
            it, and it is the cursor of every time axis.
        cursors: Cursor position of every integer axis, keyed by the level's
            leading column. A missing entry means an empty table, whose
            cursor is the origin.
        mode: What the plan is for.
        explicit_windows: In :attr:`PlanMode.EXPLICIT` mode, the windows to
            ensure at each progression level, keyed by leading column.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    now: datetime
    cursors: dict[str, Any] = Field(default_factory=dict)
    mode: PlanMode = PlanMode.MAINTAIN
    explicit_windows: dict[str, tuple[Window, ...]] = Field(default_factory=dict)


def plan_maintenance(config: TablePartitionConfig, actual: ActualTree, context: PlanningContext) -> MaintenancePlan:
    """Plan the DDL that converges ``actual`` towards ``config``.

    Args:
        config: The table's configuration.
        actual: The tree as it exists, orphans included.
        context: The clock, the cursors, and what the plan is for.

    Returns:
        A plan whose operations are safe to execute in order, plus findings for
        everything left untouched.
    """
    planner = _Planner(config, actual, context)
    planner.run()
    return MaintenancePlan(
        table_name=config.qualified_name,
        generated_at=context.now,
        cursors=dict(context.cursors),
        operations=tuple(planner.creates + planner.attaches + planner.detaches + planner.drops),
        findings=tuple(planner.findings),
    )


def fact_targets(config: TablePartitionConfig, actual: ActualTree) -> tuple[str, ...]:
    """Names the lifecycle policy may need facts about.

    That is every member of every progression level -- the partitions
    retention decides over -- and every orphan below such a level. Set-level
    members are never candidates, so they are never measured.
    """
    targets: list[str] = []

    def visit(level: SchemeBase, node: PartitionNode) -> None:
        if level.progression is not None:
            targets.extend(child.name for child in node.children if not child.is_default)
            targets.extend(orphan.name for orphan in actual.orphans if orphan.parent_name == node.name)
        if level.child is not None:
            for child in node.children:
                if child.partition_type is not None:
                    visit(level.child, child)

    visit(config.scheme, actual.root)
    return tuple(dict.fromkeys(targets))


def to_maintenance_issue(finding: Finding) -> MaintenanceIssue:
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


# Upper-bound spellings that mean "no upper limit".
_UNBOUNDED_UPPER = frozenset({"MAXVALUE", "INFINITY", "+INFINITY"})
_UNBOUNDED_LOWER = frozenset({"MINVALUE", "-INFINITY"})


_ProgressionLevel = RangePartitioning | ListPartitioning


@dataclass(frozen=True)
class _Member:
    """One attached child of a progression level, positioned on its axis.

    ``managed`` is True only for a bounded, readable window that lies on the
    scheme's grid — the only kind the lifecycle may act on. A LIST member
    owning several values has no window; the values it owns are kept in
    ``claimed`` so a wanted value it holds is recognised as taken.
    """

    node: PartitionNode
    start: Any | None
    end: Any | None
    window: Window | None
    managed: bool
    claimed: frozenset[Any] = frozenset()

    def overlaps(self, window: Window) -> bool:
        """True when this member covers any position of ``window``."""
        if self.claimed:
            return any(window.start <= value < window.end for value in self.claimed)
        if self.start is None and self.end is None and self.window is None:
            return False  # unreadable: unknown position, cannot be tested
        lower_ok = self.start is None or self.start < window.end
        upper_ok = self.end is None or window.start < self.end
        return bool(lower_ok and upper_ok)


@dataclass
class _Planner:
    config: TablePartitionConfig
    actual: ActualTree
    ctx: PlanningContext
    creates: list[Operation] = field(default_factory=list)
    attaches: list[Operation] = field(default_factory=list)
    detaches: list[Operation] = field(default_factory=list)
    drops: list[Operation] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)

    @property
    def policy(self) -> LifecyclePolicy:
        return self.config.lifecycle

    def _owns(self, relkind: RelationKind) -> bool:
        """True when the lifecycle may detach and drop a relation of this kind.

        Tables always; foreign tables only when the configuration realises its
        leaves as foreign tables, since a foreign partition someone else
        attached -- an archive behind ``postgres_fdw``, say -- is not ours.
        """
        if relkind is RelationKind.FOREIGN:
            return self.config.manages_foreign_leaves
        return relkind.is_droppable_table

    def run(self) -> None:
        self._plan_level(self.config.scheme, self.actual.root, depth=0)

    # ── Dispatch ────────────────────────────────────────────────────────────────

    def _plan_level(self, level: SchemeBase, node: PartitionNode, *, depth: int) -> None:
        """Plan one level of the tree, recursing into children that match."""
        if node.has_unaddressable_children:
            # Some of this node's children were left out of the tree because their
            # names cannot be reached by qualified-name DDL. What is left is a subset
            # of the real child set, so every apparent gap in it may already be
            # filled -- planning from it would propose partitions that exist.
            self._record(
                node.name,
                FindingReason.COVERAGE_UNKNOWN,
                f"{node.name} has a child whose name cannot be addressed by qualified-name DDL, so its child "
                "set is incomplete and nothing can be planned for it.",
            )
            return

        incompatibility = _incompatibility(level, node)
        if incompatibility is not None:
            self.findings.append(incompatibility)
            return

        if isinstance(level, HashPartitioning):
            self._plan_hash_level(level, node, depth=depth)
        elif isinstance(level, ListPartitioning) and level.sequence is None:
            self._plan_list_level(level, node, depth=depth)
        else:
            assert isinstance(level, (RangePartitioning, ListPartitioning))
            self._plan_progression_level(level, node, depth=depth)

    # ── The progression level: RANGE windows, or a sliding LIST ────────────────

    def _plan_progression_level(self, level: _ProgressionLevel, node: PartitionNode, *, depth: int) -> None:
        boundaries = level.progression
        assert boundaries is not None  # dispatch sends only progression levels here
        members, pending = self._classify_members(level, node)
        managed = {m.window: m for m in members if m.managed and m.window is not None}
        cursor_window = self._cursor_window(level, managed)

        newest = max(managed.values(), key=lambda m: m.window.start, default=None)  # type: ignore[union-attr]
        newest_candidate = None if newest is None else self._candidate(newest, cursor_window, boundaries)

        desired = self._desired_windows(level, cursor_window, newest_candidate)
        orphans = [o for o in self.actual.orphans if o.parent_name == node.name]
        consumed: set[str] = set()
        recurse_into: list[PartitionNode] = []

        for window in desired:
            if window in pending:
                # The window's partition is half-detached: this plan finalizes
                # the detach, and the same maintenance call re-plans -- the
                # finalized table comes back as an orphan and is re-attached
                # or retired. Creating another partition under its name now
                # would only collide with it.
                continue
            existing = managed.get(window)
            if existing is not None:
                recurse_into.append(existing.node)
                continue

            if any(m.window == window for m in members):
                # Held by a relation the lifecycle may not touch -- a foreign
                # table, say. The window exists; there is nothing to create and
                # nothing to complain about.
                continue

            blocking = [m for m in members if m.overlaps(window)]
            if blocking:
                names = ", ".join(sorted(m.node.name for m in blocking))
                self._record(
                    node.name,
                    FindingReason.RANGE_OVERLAP,
                    f"{node.name} needs a partition for {boundaries.describe(window)} but {names} already "
                    "covers part of it with bounds the scheme did not produce; creating it would fail, and "
                    "detaching the other is not this library's decision.",
                )
                continue

            orphan = self._matching_orphan(orphans, consumed, boundaries, window)
            if orphan is not None:
                consumed.add(orphan.name)
                if isinstance(self.policy.drop, DropAfter):
                    self.attaches.append(self._reattach(level, node, orphan, window))
                else:
                    # Under DropNever a detached table belongs to whatever
                    # process the policy handed it to; neither re-attaching it
                    # nor creating a partition under its name is this
                    # library's call.
                    self._record(
                        node.name,
                        FindingReason.NAME_UNUSABLE,
                        f"{node.name} needs a partition for {boundaries.describe(window)}, but the detached table "
                        f"{orphan.name} holds that name and, under '{self.policy.drop.describe()}', belongs to "
                        "whatever process the policy hands detached tables to; attach it yourself, or rename it "
                        "and the next run creates the partition.",
                    )
                continue

            op = self._new_member(level, node, window, reason=self._creation_reason(), depth=depth)
            if op is not None:
                self.creates.append(op)

        desired_set = set(desired)
        if self.ctx.mode is PlanMode.MAINTAIN:
            for member in managed.values():
                if member.window in desired_set:
                    continue
                if self._expire(level, node, member, cursor_window, boundaries):
                    continue
                recurse_into.append(member.node)
            occupied = {m.window for m in members if m.window is not None}
            for orphan in orphans:
                if orphan.name in consumed:
                    continue
                wanted = self._reattachable_window(level, orphan, occupied, cursor_window, boundaries)
                if wanted is not None:
                    consumed.add(orphan.name)
                    occupied.add(wanted)
                    self.attaches.append(self._reattach(level, node, orphan, wanted))
                    continue
                self._plan_orphan_drop(orphan, cursor_window, boundaries)
        elif self.ctx.mode is PlanMode.RECONCILE:
            recurse_into.extend(m.node for m in managed.values() if m.window not in desired_set)

        if level.child is not None:
            for child_node in recurse_into:
                self._plan_level(level.child, child_node, depth=depth + 1)

    def _cursor_window(self, level: _ProgressionLevel, managed: dict[Window, _Member]) -> Window:
        """The window holding the level's "now".

        The clock for a time axis, the recorded high-water mark for an integer
        one -- or, for a level whose cursor is its newest member, that member's
        own window; an empty level starts at the sequence's first value.
        """
        boundaries = level.progression
        assert boundaries is not None
        if boundaries.cursor_source is CursorSource.NEWEST_MEMBER:
            newest = max(managed, default=None)
            return boundaries.window_at(None if newest is None else newest.start)
        return boundaries.window_at(self._cursor_position(level))

    def _cursor_position(self, level: _ProgressionLevel) -> Any:
        """The level's "now": the clock for time, the recorded high-water mark otherwise."""
        boundaries = level.progression
        assert boundaries is not None
        if boundaries.axis is Axis.TIME:
            return self.ctx.now
        return self.ctx.cursors.get(level.leading_column)

    def _desired_windows(
        self,
        level: _ProgressionLevel,
        cursor_window: Window,
        newest: Candidate | None,
    ) -> list[Window]:
        boundaries = level.progression
        assert boundaries is not None
        if self.ctx.mode is PlanMode.RECONCILE:
            return []
        if self.ctx.mode is PlanMode.EXPLICIT:
            windows = list(self.ctx.explicit_windows.get(level.leading_column, ()))
        else:
            windows = self.policy.creation.desired_windows(cursor_window, boundaries, newest)
        # dict.fromkeys de-duplicates while preserving order; sorting by start
        # then keeps creations chronological, which is what a reader expects.
        return sorted(dict.fromkeys(windows), key=lambda w: w.start)

    def _creation_reason(self) -> Reason:
        if self.ctx.mode is PlanMode.EXPLICIT:
            return Reason.EXPLICIT
        creation = self.policy.creation
        return {
            "create_ahead": Reason.CREATE_AHEAD,
            "create_until": Reason.CREATE_UNTIL,
            "create_next_if": Reason.CREATE_NEXT,
        }.get(getattr(creation, "kind", ""), Reason.CREATE_AHEAD)

    def _classify_members(self, level: _ProgressionLevel, node: PartitionNode) -> tuple[list[_Member], set[Window]]:
        """Position every attached child on the axis and decide which are ours.

        Returns the members, and the windows of half-detached children whose
        finalize this plan carries -- windows nothing else may claim yet.
        """
        boundaries = level.progression
        assert boundaries is not None
        members: list[_Member] = []
        pending: set[Window] = set()

        for child in node.children:
            if child.is_default:
                continue
            if isinstance(level, RangePartitioning):
                if not isinstance(child.bounds, RangeBounds):
                    continue
            elif not isinstance(child.bounds, ListBounds):
                continue

            if child.detach_pending:
                window = self._finalize_pending(level, node, child)
                if window is not None:
                    pending.add(window)
                continue

            if isinstance(child.bounds, RangeBounds):
                members.append(self._range_member(boundaries, child, child.bounds))
            else:
                assert isinstance(child.bounds, ListBounds)
                members.append(self._sequence_member(boundaries, child, child.bounds))

        return members, pending

    def _finalize_pending(self, level: _ProgressionLevel, node: PartitionNode, child: PartitionNode) -> Window | None:
        """Complete a detach an interrupted ``DETACH CONCURRENTLY`` left half-done.

        The partition is still in the catalog but invisible through the parent
        and rejecting its own rows; the decision to detach it was already
        taken, so the maintenance tick finishes it rather than waiting for a
        human. The same call then re-plans: the finalized table is an orphan
        that is re-attached when its window is still wanted, and otherwise
        retired under the drop policy. Returns the window the child held, so
        the plan does not try to fill it while the table still owns the name.
        """
        self._record(
            child.name,
            FindingReason.DETACH_PENDING,
            f"{child.name} is pending detach: a DETACH CONCURRENTLY was interrupted, so it rejects its own rows "
            "and is invisible through the parent; the detach is completed with FINALIZE.",
        )
        window = None if child.bounds is None else level.window_of(child.bounds)
        if self.ctx.mode is not PlanMode.MAINTAIN:
            return window
        self.detaches.append(
            DetachPartition(
                target=child.name,
                oid=child.oid,
                parent_name=node.name,
                mode=self.policy.detach,
                bounds=child.bounds,
                reason=Reason.DETACH_FINALIZE,
                detail="an interrupted DETACH CONCURRENTLY is completed with FINALIZE",
            )
        )
        return window

    def _range_member(self, boundaries: RangeBoundaries, child: PartitionNode, bounds: RangeBounds) -> _Member:
        lower_unbounded = bounds.from_value.strip().upper() in _UNBOUNDED_LOWER
        upper_unbounded = bounds.to_value.strip().upper() in _UNBOUNDED_UPPER
        start = None if lower_unbounded else boundaries.decode(bounds.from_value)
        end = None if upper_unbounded else boundaries.decode(bounds.to_value)

        if child.is_foreign and not self._owns(child.relkind):
            self._record(
                child.name,
                FindingReason.FOREIGN_PARTITION,
                f"{child.name} is a foreign table; it is inspected but never created, detached or dropped.",
            )
            bounded = start is not None and end is not None
            return _Member(child, start, end, Window(start=start, end=end) if bounded else None, managed=False)

        if (not lower_unbounded and start is None) or (not upper_unbounded and end is None):
            self._record(
                child.name,
                FindingReason.UNREADABLE_BOUND,
                f"{child.name} has bounds {bounds.from_value!r} .. {bounds.to_value!r} that cannot "
                "be read on this level's axis; it is never pruned, because guessing risks dropping live data.",
            )
            return _Member(child, None, None, None, managed=False)

        if lower_unbounded or upper_unbounded:
            self._record(
                child.name,
                FindingReason.UNBOUNDED_PARTITION,
                f"{child.name} is open-ended ({bounds.from_value} .. {bounds.to_value}); it holds "
                "current data by definition and is never pruned.",
            )
            return _Member(child, start, end, None, managed=False)

        window = Window(start=start, end=end)
        if _on_grid(boundaries, window):
            return _Member(child, start, end, window, managed=True)

        self._record(
            child.name,
            FindingReason.UNMANAGED_PARTITION,
            f"{child.name} covers {bounds.from_value} .. {bounds.to_value}, which is not a "
            "window of the configured scheme; it is not a lifecycle partition and is left alone.",
        )
        return _Member(child, start, end, window, managed=False)

    def _sequence_member(self, boundaries: RangeBoundaries, child: PartitionNode, bounds: ListBounds) -> _Member:
        """Position a LIST child of a sliding list: one readable value is a window of the sequence."""
        decoded = [boundaries.decode(value) for value in bounds.values]
        readable = frozenset(value for value in decoded if value is not None)
        single = len(bounds.values) == 1 and len(readable) == 1 and not bounds.includes_null
        window = Window(start=next(iter(readable)), end=next(iter(readable)) + 1) if single else None

        if child.is_foreign and not self._owns(child.relkind):
            self._record(
                child.name,
                FindingReason.FOREIGN_PARTITION,
                f"{child.name} is a foreign table; it is inspected but never created, detached or dropped.",
            )
            return _Member(child, None, None, window, managed=False, claimed=readable)

        if window is not None:
            return _Member(child, window.start, window.end, window, managed=True)

        spelled = ", ".join(bounds.values) + (", NULL" if bounds.includes_null else "")
        self._record(
            child.name,
            FindingReason.UNMANAGED_PARTITION,
            f"{child.name} owns the values ({spelled}), which is not a single value of the configured "
            "sequence; it is not a lifecycle partition and is left alone.",
        )
        return _Member(child, None, None, None, managed=False, claimed=readable)

    def _candidate(self, member: _Member, cursor_window: Window, boundaries: RangeBoundaries) -> Candidate:
        return Candidate(
            window=member.window,
            node=member.node,
            now=self.ctx.now,
            cursor_window=cursor_window,
            boundaries=boundaries,
            facts=member.node.facts or PartitionFacts(),
        )

    def _expire(
        self,
        level: _ProgressionLevel,
        node: PartitionNode,
        member: _Member,
        cursor_window: Window,
        boundaries: RangeBoundaries,
    ) -> bool:
        """Plan the detach (and maybe the drop) of an expired member; True when it expired."""
        assert member.window is not None  # managed members always carry one
        if not member.window.end <= cursor_window.start:
            # The cursor's own window and everything ahead of it receive rows;
            # no retention rule may take them out of service.
            return False

        candidate = self._candidate(member, cursor_window, boundaries)
        if not self.policy.retention.evaluate(candidate):
            return False

        detail = f"{boundaries.describe(member.window)} expired under '{self.policy.retention.describe()}'"
        self.detaches.append(
            DetachPartition(
                target=member.node.name,
                oid=member.node.oid,
                parent_name=node.name,
                mode=self.policy.detach,
                bounds=member.node.bounds,
                reason=Reason.RETENTION_EXPIRED,
                detail=detail,
                size_bytes=candidate.facts.size_bytes,
                row_estimate=candidate.facts.row_estimate,
            )
        )

        drop = self.policy.drop
        if (
            isinstance(drop, DropAfter)
            and drop.grace_elapsed(self.ctx.now, self.ctx.now)
            and (drop.when is None or drop.when.evaluate(candidate))
        ):
            self.drops.append(
                DropPartition(
                    target=member.node.name,
                    oid=member.node.oid,
                    reason=Reason.FOLLOWS_DETACH,
                    detail=f"dropped in the same run as its detach ('{drop.describe()}')",
                    follows_detach=True,
                    size_bytes=candidate.facts.size_bytes,
                    row_estimate=candidate.facts.row_estimate,
                )
            )
        return True

    def _reattachable_window(
        self,
        level: _ProgressionLevel,
        orphan: DetachedPartition,
        occupied: set[Window],
        cursor_window: Window,
        boundaries: RangeBoundaries,
    ) -> Window | None:
        """The window an orphan should come back for, or None to leave it detached.

        A partition is detached because retention expired it; a retention that
        has since grown wants its window again, and re-attaching restores the
        data instead of waiting out a grace period and dropping it. Under
        ``DropNever`` detached tables belong to whatever process the policy hands
        them to -- an archiver, say -- so they are never brought back.
        """
        if not isinstance(self.policy.drop, DropAfter) or not self._owns(orphan.relkind):
            return None
        window = boundaries.parse_child_name(orphan.relname)
        if window is None or window in occupied or not _on_grid(boundaries, window):
            return None
        candidate = Candidate(
            window=window,
            now=self.ctx.now,
            cursor_window=cursor_window,
            boundaries=boundaries,
            facts=orphan.facts or PartitionFacts(),
        )
        if window.end > cursor_window.start:
            # The cursor's window and everything ahead of it receive rows:
            # a detached table for such a window is always wanted back.
            return window
        return None if self.policy.retention.evaluate(candidate) else window

    def _plan_orphan_drop(self, orphan: DetachedPartition, cursor_window: Window, boundaries: RangeBoundaries) -> None:
        drop = self.policy.drop
        if not isinstance(drop, DropAfter):
            return  # DropNever: something else owns the drop

        if not self._owns(orphan.relkind):
            self._record(
                orphan.name,
                FindingReason.FOREIGN_PARTITION,
                f"{orphan.name} is a detached foreign table and this configuration does not realise its leaves as "
                "foreign tables, so it is not this library's to drop.",
            )
            return

        if not drop.grace_elapsed(orphan.detached_at, self.ctx.now):
            assert orphan.detached_at is not None  # grace can only be pending with a known instant
            self._record(
                orphan.name,
                FindingReason.GRACE_PENDING,
                f"{orphan.name} was detached at {orphan.detached_at.isoformat()} and is kept until "
                f"{(orphan.detached_at + drop.grace).isoformat()} ('{drop.describe()}').",
            )
            return

        facts = orphan.facts or PartitionFacts()
        if drop.when is not None:
            candidate = Candidate(
                window=boundaries.parse_child_name(orphan.relname),
                now=self.ctx.now,
                cursor_window=cursor_window,
                boundaries=boundaries,
                facts=facts,
            )
            if not drop.when.evaluate(candidate):
                self._record(
                    orphan.name,
                    FindingReason.DROP_DEFERRED,
                    f"{orphan.name} is past its grace but '{drop.when.describe()}' does not hold yet.",
                )
                return

        detail = (
            f"detached at {orphan.detached_at.isoformat()}; grace of {drop.grace} elapsed"
            if orphan.detached_at is not None
            else "detached at an unknown instant (marked by an older version or adopted); treated as past its grace"
        )
        self.drops.append(
            DropPartition(
                target=orphan.name,
                oid=orphan.oid,
                reason=Reason.GRACE_ELAPSED,
                detail=detail,
                detached_at=orphan.detached_at,
                size_bytes=facts.size_bytes,
                row_estimate=facts.row_estimate,
            )
        )

    def _reattach(
        self,
        level: _ProgressionLevel,
        node: PartitionNode,
        orphan: DetachedPartition,
        window: Window,
    ) -> AttachPartition:
        boundaries = level.progression
        assert boundaries is not None
        return AttachPartition(
            target=orphan.name,
            oid=orphan.oid,
            parent_name=node.name,
            bounds=level.bounds_for(window),
            key_columns=level.key,
            partition_by=_partition_by(level.child),
            reason=Reason.REATTACH,
            detail=f"detached partition covers {boundaries.describe(window)}, which is wanted again",
        )

    def _new_member(
        self,
        level: _ProgressionLevel,
        node: PartitionNode,
        window: Window,
        *,
        reason: Reason,
        depth: int,
    ) -> CreatePartition | None:
        boundaries = level.progression
        assert boundaries is not None
        schema, parent_relname = split_qualified_name(node.name)
        relname = boundaries.child_name(parent_relname, window)
        child_name = qualify(schema, relname)
        if self._is_unusable_name(relname, child_name, node):
            return None

        children = self._complete_subtree(level.child, child_name)
        if children is None:
            return None

        return CreatePartition(
            target=child_name,
            parent_name=node.name,
            bounds=level.bounds_for(window),
            partition_by=_partition_by(level.child),
            key_columns=level.key,
            children=children,
            lifecycle_unit=True,
            counts_as="created",
            reason=reason,
            detail=f"{boundaries.describe(window)} under '{self._creation_detail()}'",
        )

    def _creation_detail(self) -> str:
        if self.ctx.mode is PlanMode.EXPLICIT:
            return "explicitly requested"
        return self.policy.creation.describe()

    def _matching_orphan(
        self,
        orphans: list[DetachedPartition],
        consumed: set[str],
        boundaries: RangeBoundaries,
        window: Window,
    ) -> DetachedPartition | None:
        for orphan in orphans:
            if orphan.name in consumed or not self._owns(orphan.relkind):
                continue
            parsed = boundaries.parse_child_name(orphan.relname)
            if parsed is not None and parsed == window:
                return orphan
        return None

    # ── HASH: a set level ───────────────────────────────────────────────────────

    def _plan_hash_level(self, level: HashPartitioning, node: PartitionNode, *, depth: int) -> None:
        hash_children = node.hash_children
        modulus = self._effective_modulus(level, node, hash_children)

        if modulus is not None:
            bounds = tuple(c.bounds for c in hash_children if isinstance(c.bounds, HashBounds))
            gaps = missing_remainders(modulus, bounds)
            reason = Reason.HASH_GAP if modulus == level.modulus else Reason.HASH_GAP_HISTORICAL_MODULUS
            self.creates.extend(
                self._hash_members(level, node.name, gaps, modulus=modulus, node=node, reason=reason, depth=depth)
            )

        # This level's own bucket set may be left alone -- it already tiles the
        # keyspace at another modulus, or its layout is one we refuse to guess
        # at. That decision is about *this* level only: the buckets that do
        # exist may still be missing children of their own.
        if level.child is not None:
            for child in hash_children:
                self._plan_level(level.child, child, depth=depth + 1)

    def _effective_modulus(
        self,
        level: HashPartitioning,
        node: PartitionNode,
        hash_children: tuple[PartitionNode, ...],
    ) -> int | None:
        """Decide which modulus to fill gaps at, or None to leave the set alone.

        A hash set tiles the keyspace at whatever moduli its members already use,
        and those cannot be changed without a rewrite. So a set that disagrees
        with the configured bucket count is repaired *at its own modulus* when it
        still has gaps -- rows hashing into a gap are rejected outright -- and left
        completely alone when it does not.
        """
        if not hash_children:
            return level.modulus

        bounds = tuple(c.bounds for c in hash_children if isinstance(c.bounds, HashBounds))
        existing = uniform_modulus(bounds)

        if existing is None:
            self.findings.append(_non_uniform_finding(node, bounds, level))
            return None

        if existing == level.modulus:
            return existing

        gaps = missing_remainders(existing, bounds)
        if not gaps:
            self._record(
                node.name,
                FindingReason.MODULUS_PRESERVED,
                f"{node.name} has a complete {existing}-bucket hash set and the scheme now asks for "
                f"{level.modulus}; leaving it as-is. It already tiles the whole keyspace, and changing a hash "
                "modulus requires a rewrite — new partitions use the configured count.",
            )
            return None

        self._record(
            node.name,
            FindingReason.MODULUS_REPAIRED,
            f"{node.name} has an incomplete {existing}-bucket hash set ({len(bounds)} of {existing} present); "
            f"filling the gaps at that modulus because the configured count ({level.modulus}) would overlap the "
            "existing buckets.",
        )
        return existing

    def _hash_members(
        self,
        level: HashPartitioning,
        parent_name: str,
        remainders: range | tuple[int, ...],
        *,
        modulus: int,
        node: PartitionNode | None,
        reason: Reason,
        depth: int,
    ) -> list[CreatePartition]:
        """Build create-operations for ``remainders`` under ``parent_name``.

        ``modulus`` may differ from the level's bucket count when repairing a
        set that already uses another one; the names still come from the level
        so a repaired set stays addressable by the same convention.
        """
        schema, parent_relname = split_qualified_name(parent_name)
        ops = []
        for remainder in remainders:
            relname = level.child_name(parent_relname, remainder)
            child_name = qualify(schema, relname)
            if self._is_unusable_name(relname, child_name, node, parent_name=parent_name):
                continue
            op = self._new_set_member(
                level,
                parent_name,
                child_name,
                HashBounds(modulus=modulus, remainder=remainder),
                reason=reason,
                detail=f"bucket {remainder} of {modulus}",
                depth=depth,
            )
            if op is not None:
                ops.append(op)
        return ops

    # ── LIST: a set level ───────────────────────────────────────────────────────

    def _plan_list_level(self, level: ListPartitioning, node: PartitionNode, *, depth: int) -> None:
        """Create the LIST partitions a node is missing; recurse into the rest.

        Unlike a hash set, a LIST level is never complete — there is always
        another value the world could produce — so there is no "gap" to
        detect, only groups that are not there yet. Groups are matched by the
        values they own rather than by name, so a tree an earlier tool named
        differently is recognised instead of duplicated.
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

        missing: list[ListGroup] = []
        for group in level.groups:
            if frozenset(group.values) in present:
                continue
            conflicts = {v: claimed[v] for v in group.values if v in claimed}
            if conflicts:
                self.findings.append(_list_conflict_finding(node, group, conflicts))
                continue
            missing.append(group)
            for value in group.values:
                claimed[value] = "(pending)"

        self.creates.extend(
            self._list_members(
                level,
                node.name,
                tuple(missing),
                include_default=level.include_default and not has_default,
                node=node,
                depth=depth,
            )
        )

        if level.child is not None:
            for child in node.children:
                if isinstance(child.bounds, (ListBounds, DefaultBounds)):
                    self._plan_level(level.child, child, depth=depth + 1)

    def _list_members(
        self,
        level: ListPartitioning,
        parent_name: str,
        groups: tuple[ListGroup, ...],
        *,
        include_default: bool,
        node: PartitionNode | None,
        depth: int,
    ) -> list[CreatePartition]:
        schema, parent_relname = split_qualified_name(parent_name)
        planned: list[tuple[str, PartitionBounds, Reason, str]] = [
            (
                level.child_name(parent_relname, group.name),
                group.bounds(),
                Reason.LIST_GROUP_MISSING,
                f"group {group.name!r}",
            )
            for group in groups
        ]
        if include_default:
            planned.append(
                (
                    level.child_name(parent_relname, level.default_name),
                    level.default_bounds(),
                    Reason.LIST_DEFAULT_MISSING,
                    "DEFAULT catch-all",
                )
            )

        ops = []
        for relname, bounds, reason, detail in planned:
            child_name = qualify(schema, relname)
            if self._is_unusable_name(relname, child_name, node, parent_name=parent_name):
                continue
            op = self._new_set_member(level, parent_name, child_name, bounds, reason=reason, detail=detail, depth=depth)
            if op is not None:
                ops.append(op)
        return ops

    # ── Building new nodes ──────────────────────────────────────────────────────

    def _new_set_member(
        self,
        level: SchemeBase,
        parent_name: str,
        child_name: str,
        bounds: PartitionBounds,
        *,
        reason: Reason,
        detail: str,
        depth: int,
    ) -> CreatePartition | None:
        children = self._complete_subtree(level.child, child_name)
        if children is None:
            return None
        return CreatePartition(
            target=child_name,
            parent_name=parent_name,
            bounds=bounds,
            partition_by=_partition_by(level.child),
            key_columns=level.key,
            children=children,
            lifecycle_unit=False,
            counts_as="created" if depth == 0 else "repaired",
            reason=reason,
            detail=detail,
        )

    def _complete_subtree(self, level: SchemeBase | None, parent_name: str) -> tuple[CreatePartition, ...] | None:
        """Plan a new partition's whole subtree, or None when it cannot be planned in full.

        A partitioned relation attached with a hole in its child set rejects
        every row of the hole, and one attached with no children at all rejects
        every row it receives. When any member of the subtree was refused --
        the refusal is already on record against the parent -- the partition
        must not be created either, so the caller gets None and moves on.
        """
        if level is None:
            return ()
        refused_before = len(self.findings)
        ops = self._new_subtree(level, parent_name)
        if len(self.findings) > refused_before or not ops:
            self._record(
                parent_name,
                FindingReason.UNCONVERGEABLE,
                f"{parent_name} cannot be created: part of the subtree the scheme describes for it could not be "
                "planned, and attaching a partitioned relation with a hole in its child set would reject rows.",
            )
            return None
        return ops

    def _new_subtree(self, level: SchemeBase, parent_name: str) -> tuple[CreatePartition, ...]:
        """Plan the complete subtree of a partition that does not exist yet.

        There is nothing to reconcile against: every member the scheme
        describes has to be built, and a name the planner refuses is reported
        against the parent -- dropping it silently would build a partitioned
        relation with a hole in its child set and attach it.
        """
        ops: list[CreatePartition]
        if isinstance(level, HashPartitioning):
            ops = self._hash_members(
                level,
                parent_name,
                range(level.modulus),
                modulus=level.modulus,
                node=None,
                reason=Reason.SUBTREE,
                depth=-1,
            )
        elif isinstance(level, ListPartitioning) and level.sequence is None:
            ops = self._list_members(
                level, parent_name, level.groups, include_default=level.include_default, node=None, depth=-1
            )
        else:
            assert isinstance(level, (RangePartitioning, ListPartitioning))
            cursor_window = self._cursor_window(level, {})
            ops = []
            for window in self._windows_for_new_level(level, cursor_window):
                op = self._new_member(level, _phantom(parent_name), window, reason=Reason.SUBTREE, depth=-1)
                if op is not None:
                    ops.append(op)
        return tuple(op.model_copy(update={"counts_as": "subtree", "reason": Reason.SUBTREE}) for op in ops)

    def _windows_for_new_level(self, level: _ProgressionLevel, cursor_window: Window) -> list[Window]:
        """The windows a progression level gets inside a partition being created.

        Whatever the plan is for, a new branch must be able to route the rows
        it will receive: the windows named for this level in EXPLICIT mode, and
        otherwise what the creation policy wants ahead of the cursor -- a
        RECONCILE run included, since a branch created with no windows at all
        would reject everything until the next scheduled tick.
        """
        boundaries = level.progression
        assert boundaries is not None
        explicit = self.ctx.explicit_windows.get(level.leading_column)
        windows = list(explicit) if explicit else self.policy.creation.desired_windows(cursor_window, boundaries, None)
        return sorted(dict.fromkeys(windows), key=lambda w: w.start)

    # ── Names ───────────────────────────────────────────────────────────────────

    def _is_unusable_name(
        self,
        relname: str,
        child_name: str,
        node: PartitionNode | None,
        *,
        parent_name: str | None = None,
    ) -> bool:
        """True when a planned name cannot safely be created, recording why.

        Two ways a name goes wrong, both of which would produce a partition
        that silently never appears:

        * Another relation already holds it. The executor would read the
          resulting "already exists" / "already a partition" errors as a lost
          race and report success, leaving that slice of the keyspace rejecting
          rows forever.
        * It exceeds PostgreSQL's 63-byte identifier limit, which truncates
          *silently* -- so two children collapse onto one name and collide.
        """
        owner = parent_name if parent_name is not None else (node.name if node is not None else child_name)
        if len(relname.encode("utf-8")) > MAX_IDENTIFIER_LENGTH:
            self._record(
                owner,
                FindingReason.NAME_UNUSABLE,
                f"{owner} would need a partition named {relname!r}, which exceeds PostgreSQL's "
                f"{MAX_IDENTIFIER_LENGTH}-byte identifier limit and would be truncated into a collision.",
            )
            return True

        if node is not None and any(child.name == child_name for child in node.children):
            self._record(
                node.name,
                FindingReason.NAME_UNUSABLE,
                f"{node.name} already has a partition named {child_name!r} that does not match the configured "
                "one; creating it would collide, and the collision is indistinguishable from a lost race.",
            )
            return True

        return False

    def _record(self, partition_name: str, reason: FindingReason, detail: str) -> None:
        self.findings.append(Finding(partition_name=partition_name, reason=reason, detail=detail))


# ── Module helpers ──────────────────────────────────────────────────────────────


def _on_grid(boundaries: RangeBoundaries, window: Window) -> bool:
    """True when ``window`` is a window of the grid, or lies inside one.

    A partition finer than the grid -- a day inside a month, left behind by an
    earlier, finer configuration -- still holds exactly the data its window
    says, so retention may act on it by its upper bound. A partition coarser
    than the grid, or straddling two of its windows, is not something the
    scheme would ever have produced, and is not this library's to touch.
    """
    try:
        cell = boundaries.window_at(window.start)
    except (ValueError, TypeError, OverflowError):
        return False
    return bool(cell.start <= window.start and window.end <= cell.end)


def _partition_by(level: SchemeBase | None) -> PartitionBy | None:
    return None if level is None else PartitionBy(method=level.method, columns=level.key)


def _phantom(name: str) -> PartitionNode:
    """A node standing in for a partition that does not exist yet."""
    return PartitionNode(name=name)


def _incompatibility(level: SchemeBase, node: PartitionNode) -> Finding | None:
    """Return a finding when ``node`` cannot host ``level``, else None."""
    if node.is_foreign:
        return Finding(
            partition_name=node.name,
            reason=FindingReason.FOREIGN_PARTITION,
            detail=f"{node.name} is a foreign table; it cannot hold {level.describe()} partitions and is left as-is.",
        )

    if node.is_leaf:
        return Finding(
            partition_name=node.name,
            reason=FindingReason.LEGACY_LEAF,
            detail=(
                f"{node.name} is a plain leaf table and cannot hold {level.describe()} partitions; leaving it "
                "as-is. Partitions created before the current scheme stay valid; new partitions are created with "
                "the current topology."
            ),
        )

    if node.partition_type != level.method:
        return Finding(
            partition_name=node.name,
            reason=FindingReason.STRATEGY_MISMATCH,
            detail=(
                f"{node.name} is {node.describe_topology()} but the scheme asks for {level.describe()}; leaving it "
                "as-is. Repartitioning an existing branch requires a rewrite and is not attempted."
            ),
        )

    if node.has_expression_key:
        return Finding(
            partition_name=node.name,
            reason=FindingReason.COLUMN_MISMATCH,
            detail=(
                f"{node.name} partitions on an expression, which cannot be compared against the configured key "
                f"({', '.join(level.key)}) or addressed by a bound; leaving it as-is."
            ),
        )

    if node.partition_columns != level.key:
        actual = ", ".join(node.partition_columns) or "?"
        return Finding(
            partition_name=node.name,
            reason=FindingReason.COLUMN_MISMATCH,
            detail=(
                f"{node.name} is partitioned by {level.method.value.upper()} ({actual}) but the scheme asks for "
                f"({', '.join(level.key)}); leaving it as-is."
            ),
        )

    return None


def _non_uniform_finding(node: PartitionNode, bounds: tuple[HashBounds, ...], level: HashPartitioning) -> Finding:
    """Classify a set whose hash siblings disagree on modulus.

    Mixed moduli are legal in PostgreSQL as long as the residue classes do not
    overlap, so the question is not whether the layout is uniform but whether
    it is *complete*. Only a genuine gap is actionable.
    """
    moduli = sorted({b.modulus for b in bounds})
    covered = hash_keyspace_covered(bounds)

    if covered is None:
        return Finding(
            partition_name=node.name,
            reason=FindingReason.COVERAGE_UNKNOWN,
            detail=(
                f"{node.name} has hash children with moduli {moduli} whose least common multiple is too large to "
                "verify coverage for; leaving it as-is."
            ),
        )

    if covered:
        return Finding(
            partition_name=node.name,
            reason=FindingReason.NON_UNIFORM_COMPLETE,
            detail=(
                f"{node.name} has hash children with mixed moduli {moduli} that still tile the whole keyspace; "
                f"leaving it as-is (the scheme asks for {level.modulus} buckets)."
            ),
        )

    return Finding(
        partition_name=node.name,
        reason=FindingReason.NON_UNIFORM_INCOMPLETE,
        detail=(
            f"{node.name} has hash children with inconsistent moduli {moduli} that leave part of the hash keyspace "
            "uncovered; rows hashing into the gap are rejected. No repair is provably safe here, so the branch is "
            "left untouched for manual inspection."
        ),
    )


def _list_conflict_finding(node: PartitionNode, group: ListGroup, conflicts: dict[str, str]) -> Finding:
    """Report a configured group whose values another partition already owns."""
    detail = ", ".join(f"{value!r} in {owner}" for value, owner in sorted(conflicts.items()))
    return Finding(
        partition_name=node.name,
        reason=FindingReason.LIST_VALUES_CONFLICT,
        detail=(
            f"{node.name} cannot gain the configured LIST partition {group.name!r}: PostgreSQL already routes "
            f"{detail}. A value belongs to exactly one partition, and moving one requires detaching the partition "
            "that holds it, so this is left for manual inspection."
        ),
    )
