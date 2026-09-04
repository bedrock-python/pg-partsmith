"""What maintenance is going to do, and why — before it does it.

A :class:`MaintenancePlan` is the planner's whole answer: the typed, ordered
operations that converge the actual tree towards the configured one, and the
findings it deliberately left alone. It is a plain data structure — no SQL,
no IO — so it can be shown to an operator, serialized for an audit log, or
filtered before it is applied.

The refusals matter as much as the operations. A hash set cannot change
modulus online, an existing partition may predate the current policy, a
partition attached by someone else may not be ours to drop, and a partition
PostgreSQL is happy with must never be "fixed" into one it would reject.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field

from .exceptions import PlanConfigMismatchError
from .lifecycle import DetachMode
from .topology import PartitionBounds, PartitionType
from .types import NonNegativeInt, StrippedNonEmptyStr

if TYPE_CHECKING:
    from .entities import TablePartitionConfig

__all__ = [
    "AttachPartition",
    "CreatePartition",
    "DetachPartition",
    "DropPartition",
    "Finding",
    "FindingReason",
    "MaintenancePlan",
    "Operation",
    "OperationCapabilities",
    "OperationKind",
    "PartitionBy",
    "Reason",
    "Severity",
    "validate_plan_for_config",
]


class OperationKind(StrEnum):
    """The DDL family an operation belongs to."""

    CREATE = "create"
    ATTACH = "attach"
    DETACH = "detach"
    DROP = "drop"


class Reason(StrEnum):
    """Why an operation is in the plan.

    Attributes:
        CREATE_AHEAD: A window the creation policy wants ahead of the cursor.
        CREATE_UNTIL: A window before the configured horizon.
        CREATE_NEXT: The window after the newest, because its predicate held.
        EXPLICIT: A window the caller named (``ensure_partitions``).
        SUBTREE: A member of the subtree of a partition being created.
        HASH_GAP: A missing bucket at the configured modulus.
        HASH_GAP_HISTORICAL_MODULUS: A missing bucket at the modulus the set was built with.
        LIST_GROUP_MISSING: A configured LIST group with no partition.
        LIST_DEFAULT_MISSING: The configured LIST catch-all with no partition.
        REATTACH: A detached orphan whose window is wanted again.
        RETENTION_EXPIRED: The retention policy declared the window expired.
        DETACH_FINALIZE: An interrupted ``DETACH CONCURRENTLY`` is completed
            with ``FINALIZE``; the partition was already leaving.
        FOLLOWS_DETACH: Dropped in the same run as its detach (no grace).
        GRACE_ELAPSED: An orphan past its grace period.
    """

    CREATE_AHEAD = "create_ahead"
    CREATE_UNTIL = "create_until"
    CREATE_NEXT = "create_next"
    EXPLICIT = "explicit"
    SUBTREE = "subtree"
    HASH_GAP = "hash_gap"
    HASH_GAP_HISTORICAL_MODULUS = "hash_gap_historical_modulus"
    LIST_GROUP_MISSING = "list_group_missing"
    LIST_DEFAULT_MISSING = "list_default_missing"
    REATTACH = "reattach"
    RETENTION_EXPIRED = "retention_expired"
    DETACH_FINALIZE = "detach_finalize"
    FOLLOWS_DETACH = "follows_detach"
    GRACE_ELAPSED = "grace_elapsed"


class Severity(StrEnum):
    """How much a finding matters.

    Attributes:
        INFO: An expected steady state the planner recognised and chose
            correctly for: a legacy leaf, a preserved older modulus, an
            orphan still in its grace period.
        WARNING: Something a human has to act on: a gap no repair is safe for,
            a partition that overlaps a wanted window, a bound that cannot be
            read. Surfaced through ``MaintenanceResult.issues``.
    """

    INFO = "info"
    WARNING = "warning"


class FindingReason(StrEnum):
    """Why the planner left something alone.

    Attributes:
        LEGACY_LEAF: The branch is a plain table created before the current
            scheme. PostgreSQL cannot add partitions to it.
        STRATEGY_MISMATCH: The branch is partitioned by a different method
            than the scheme asks for.
        COLUMN_MISMATCH: The branch is partitioned by the right method but on
            a different key, or on an expression.
        MODULUS_PRESERVED: The branch has a complete hash set at a modulus the
            scheme no longer uses. It already tiles the keyspace, so it stays.
        MODULUS_REPAIRED: The branch has an *incomplete* hash set at a modulus
            the scheme no longer uses; the gaps are filled at the branch's own
            modulus, which is the only modulus that cannot overlap it.
        NON_UNIFORM_COMPLETE: Hash siblings disagree on modulus but still tile
            the keyspace — legal, and left untouched.
        NON_UNIFORM_INCOMPLETE: Hash siblings disagree on modulus and leave a
            gap. Rows hashing into it are rejected, and no repair is provably
            safe, so this needs a human.
        COVERAGE_UNKNOWN: The child set could not be read completely, so
            nothing can be planned from it.
        LIST_VALUES_CONFLICT: A configured LIST group claims a value another
            partition already owns.
        NAME_UNUSABLE: The partition the scheme asks for cannot be given a
            usable name — taken by a relation that does not match it, or over
            PostgreSQL's identifier limit.
        DEFAULT_HOLDS_ROWS: A DEFAULT sibling holds rows belonging to the
            partition being created, so PostgreSQL refuses to attach it.
        UNCONVERGEABLE: Converging this branch failed outright; the rest of the
            table was still maintained.
        RANGE_OVERLAP: A wanted window overlaps an existing partition that is
            not that window. Creating it would fail, and detaching the other
            is not ours to decide.
        UNMANAGED_PARTITION: An attached partition whose bounds are not on the
            scheme's grid. It is not a lifecycle partition: never detached,
            never dropped, never counted.
        UNREADABLE_BOUND: An attached RANGE partition whose bound cannot be
            read on this level's axis. It is never pruned, because guessing
            risks dropping live data.
        UNBOUNDED_PARTITION: A partition open on one side (``MINVALUE`` /
            ``MAXVALUE`` / ``infinity``). It holds current data by definition
            and is never pruned.
        FOREIGN_PARTITION: A foreign table in a tree whose configuration does
            not realise its leaves as foreign tables. It is inspected and
            never created, detached or dropped.
        DETACH_PENDING: A ``DETACH CONCURRENTLY`` was interrupted; the
            partition rejects its rows until ``DETACH … FINALIZE`` runs, which
            the same plan does.
        GRACE_PENDING: A detached orphan still within its grace period.
        DROP_DEFERRED: A detached orphan past its grace whose drop condition
            does not hold yet.
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
    NAME_UNUSABLE = "name_unusable"
    DEFAULT_HOLDS_ROWS = "default_holds_rows"
    UNCONVERGEABLE = "unconvergeable"
    RANGE_OVERLAP = "range_overlap"
    UNMANAGED_PARTITION = "unmanaged_partition"
    UNREADABLE_BOUND = "unreadable_bound"
    UNBOUNDED_PARTITION = "unbounded_partition"
    FOREIGN_PARTITION = "foreign_partition"
    DETACH_PENDING = "detach_pending"
    GRACE_PENDING = "grace_pending"
    DROP_DEFERRED = "drop_deferred"


# Reasons that describe a healthy, deliberate state (policy evolution, a
# partition that is simply not ours, an orphan waiting out its grace) rather
# than something an operator has to act on.
_INFORMATIONAL_REASONS = frozenset(
    {
        FindingReason.LEGACY_LEAF,
        FindingReason.MODULUS_PRESERVED,
        FindingReason.MODULUS_REPAIRED,
        FindingReason.NON_UNIFORM_COMPLETE,
        FindingReason.UNMANAGED_PARTITION,
        FindingReason.UNBOUNDED_PARTITION,
        FindingReason.FOREIGN_PARTITION,
        FindingReason.DETACH_PENDING,
        FindingReason.GRACE_PENDING,
        FindingReason.DROP_DEFERRED,
    }
)


class Finding(BaseModel):
    """Something the planner observed and chose not to change.

    Attributes:
        partition_name: Schema-qualified name of the relation concerned.
        reason: Which rule applied.
        severity: Whether an operator has to act on it.
        detail: Human-readable explanation, safe to log or surface verbatim.
    """

    model_config = ConfigDict(frozen=True)

    partition_name: StrippedNonEmptyStr
    reason: FindingReason
    detail: StrippedNonEmptyStr
    severity: Severity | None = None

    def model_post_init(self, __context: object) -> None:
        """Default the severity from the reason."""
        if self.severity is None:
            severity = Severity.INFO if self.reason in _INFORMATIONAL_REASONS else Severity.WARNING
            object.__setattr__(self, "severity", severity)

    @property
    def is_actionable(self) -> bool:
        """True when an operator has to do something about this finding."""
        return self.severity is Severity.WARNING


class OperationCapabilities(BaseModel):
    """How an operation may be executed.

    Attributes:
        transactional: Whether the statement may run inside a transaction
            block. ``DETACH … CONCURRENTLY`` may not (``25001``).
        lock: The heaviest lock the statement takes, and on what, as
            measured on PostgreSQL 15 and 17.
    """

    model_config = ConfigDict(frozen=True)

    transactional: bool = True
    lock: str = ""


class PartitionBy(BaseModel):
    """How a partition being created partitions its own children.

    Attributes:
        method: The PostgreSQL method.
        columns: The key, in key order.
    """

    model_config = ConfigDict(frozen=True)

    method: PartitionType
    columns: tuple[StrippedNonEmptyStr, ...]

    def describe(self) -> str:
        """Render as PostgreSQL would spell it."""
        return f"{self.method.value.upper()} ({', '.join(self.columns)})"


class OperationBase(BaseModel):
    """What every operation carries.

    Attributes:
        kind: The DDL family.
        target: Schema-qualified name of the relation the operation is about.
        oid: The relation's catalog identity when it exists; revalidated
            before a destructive operation is executed.
        reason: Why the operation is in the plan.
        detail: Human-readable explanation.
        size_bytes: Total size of the relation and its subtree, when measured.
        row_estimate: Estimated live rows, when measured.
    """

    model_config = ConfigDict(frozen=True)

    target: StrippedNonEmptyStr
    oid: int | None = None
    reason: Reason
    detail: str = ""
    size_bytes: NonNegativeInt | None = None
    row_estimate: NonNegativeInt | None = None

    @property
    def kind(self) -> OperationKind:
        """The DDL family."""
        raise NotImplementedError

    # Serialized, not only computed: the lock an operation takes is what an
    # operator reads before scheduling a window, and a plan written to a file
    # is exactly the plan they read it off. Each subclass overrides the
    # property; the dump takes the override.
    @computed_field  # type: ignore[prop-decorator]
    @property
    def capabilities(self) -> OperationCapabilities:
        """How the operation may be executed."""
        raise NotImplementedError

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_destructive(self) -> bool:
        """True for operations that remove a partition from service."""
        return False

    def describe(self) -> str:
        """One line for a human."""
        return f"{self.kind.value.upper()} {self.target} ({self.reason.value})"


class CreatePartition(OperationBase):
    """Create a partition — a leaf or a branch with its whole subtree — and attach it.

    Executed depth-first: the relation is created detached, its ``children``
    are built inside it, and only then is it attached to ``parent_name``. A
    subtree therefore becomes reachable only once it is complete, so a crash
    mid-way can never expose a branch that rejects rows.

    Attributes:
        parent_name: Schema-qualified relation the new partition attaches to.
        bounds: Bounds to attach ``target`` with.
        partition_by: How ``target`` partitions its own children, if at all.
        key_columns: ``parent_name``'s partition key, for DEFAULT reconciliation.
        children: Partitions to create inside ``target`` before attaching it.
        lifecycle_unit: True when this is a partition of a progression level —
            the unit hooks fire for and counters count.
        counts_as: What the result counter this operation increments is
            called: ``created`` for a new member directly under the root,
            ``repaired`` for a gap filled inside an existing branch, and
            ``subtree`` for a member of a partition being created.
    """

    kind_name: Literal["create"] = Field(default="create", alias="kind")
    parent_name: StrippedNonEmptyStr
    bounds: PartitionBounds
    partition_by: PartitionBy | None = None
    key_columns: tuple[StrippedNonEmptyStr, ...] = ()
    children: tuple[CreatePartition, ...] = ()
    lifecycle_unit: bool = False
    counts_as: Literal["created", "repaired", "subtree"] = "created"

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    @property
    def kind(self) -> OperationKind:
        """CREATE."""
        return OperationKind.CREATE

    @property
    def capabilities(self) -> OperationCapabilities:
        """Transactional; ATTACH takes SHARE UPDATE EXCLUSIVE on the parent."""
        return OperationCapabilities(
            transactional=True,
            lock="ACCESS SHARE on the template during CREATE; SHARE UPDATE EXCLUSIVE on the parent "
            "and ACCESS EXCLUSIVE on the new partition (and on a DEFAULT sibling) during ATTACH; "
            "SHARE ROW EXCLUSIVE on every table referencing the parent through a foreign key",
        )

    def count(self) -> int:
        """Total number of relations this operation and its descendants create."""
        return 1 + sum(child.count() for child in self.children)

    def walk(self) -> list[CreatePartition]:
        """This operation and every nested one, parent first."""
        ops = [self]
        for child in self.children:
            ops.extend(child.walk())
        return ops


class AttachPartition(OperationBase):
    """Re-attach a detached orphan whose window the policy wants again.

    Attributes:
        parent_name: Schema-qualified relation to attach to.
        bounds: Bounds to attach with.
        key_columns: ``parent_name``'s partition key, for DEFAULT reconciliation.
        partition_by: How the relation is expected to partition its own
            children, so its subtree can be completed before it goes live.
    """

    kind_name: Literal["attach"] = Field(default="attach", alias="kind")
    parent_name: StrippedNonEmptyStr
    bounds: PartitionBounds
    key_columns: tuple[StrippedNonEmptyStr, ...] = ()
    partition_by: PartitionBy | None = None

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    @property
    def kind(self) -> OperationKind:
        """ATTACH."""
        return OperationKind.ATTACH

    @property
    def capabilities(self) -> OperationCapabilities:
        """Transactional; SHARE UPDATE EXCLUSIVE on the parent."""
        return OperationCapabilities(
            transactional=True,
            lock="SHARE UPDATE EXCLUSIVE on the parent; ACCESS EXCLUSIVE on the partition and on a DEFAULT "
            "sibling; SHARE ROW EXCLUSIVE on every table referencing the parent through a foreign key",
        )


class DetachPartition(OperationBase):
    """Detach an expired partition, subtree included.

    Attributes:
        parent_name: The parent it is detached from.
        mode: How.
        bounds: How it was bound in the parent, for hook context.
    """

    kind_name: Literal["detach"] = Field(default="detach", alias="kind")
    parent_name: StrippedNonEmptyStr
    mode: DetachMode = DetachMode.AUTO
    bounds: PartitionBounds | None = None

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    @property
    def kind(self) -> OperationKind:
        """DETACH."""
        return OperationKind.DETACH

    @property
    def capabilities(self) -> OperationCapabilities:
        """Concurrent detach cannot run in a transaction block."""
        if self.mode is DetachMode.BLOCKING:
            return OperationCapabilities(
                transactional=True,
                lock="ACCESS EXCLUSIVE on the parent and the partition, and on every table referencing the parent "
                "through a foreign key",
            )
        return OperationCapabilities(
            transactional=False,
            lock="SHARE UPDATE EXCLUSIVE on the parent (CONCURRENTLY), ACCESS EXCLUSIVE on the partition and, in "
            "the second transaction, on every table referencing the parent through a foreign key; ACCESS EXCLUSIVE "
            "on the parent should the server refuse the concurrent form",
        )

    @property
    def is_destructive(self) -> bool:
        """It removes the partition from service."""
        return True


class DropPartition(OperationBase):
    """Drop a detached, marker-tagged orphan.

    Attributes:
        detached_at: When it was detached, when known.
        follows_detach: True when the detach happens in the same plan; the
            drop is skipped if that detach did not go through.
        bounds: How the partition was bound in its parent, when that is known.
            ``DETACH`` clears ``relpartbound``, so an orphan detached by an
            earlier run has no bounds left in the catalog; what is reported
            then is the window its name decodes to, which is the same reading
            the drop policy was decided on. None when the name does not decode.
    """

    kind_name: Literal["drop"] = Field(default="drop", alias="kind")
    detached_at: datetime | None = None
    follows_detach: bool = False
    bounds: PartitionBounds | None = None

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    @property
    def kind(self) -> OperationKind:
        """DROP."""
        return OperationKind.DROP

    @property
    def capabilities(self) -> OperationCapabilities:
        """Transactional; ACCESS EXCLUSIVE on the table being dropped."""
        return OperationCapabilities(transactional=True, lock="ACCESS EXCLUSIVE on the dropped table only")

    @property
    def is_destructive(self) -> bool:
        """It destroys data."""
        return True


Operation = Annotated[
    CreatePartition | AttachPartition | DetachPartition | DropPartition,
    Field(discriminator="kind_name"),
]
"""Any planned operation, discriminated on ``kind``."""


class MaintenancePlan(BaseModel):
    """Everything one maintenance run is going to do to one table.

    Operations are ordered as they must execute: creations and re-attachments
    first (each with its subtree nested inside it), then detaches, then drops.

    Attributes:
        table_name: Schema-qualified root.
        generated_at: The instant the plan was made; every policy was
            evaluated against it.
        cursors: Cursor of every integer axis the plan was made against,
            keyed by leading column. Integers, because only an integer axis
            records one -- a time axis is cursored by ``generated_at`` -- so
            the plan survives a round trip through JSON unchanged.
        config_fingerprint: :attr:`~pg_partsmith.TablePartitionConfig.fingerprint`
            of the configuration the plan was made from, so a plan read back
            from a file can say whether that configuration still holds. None
            on a plan built by hand.
        operations: What to do, in order.
        findings: What was deliberately left alone, and why.
    """

    model_config = ConfigDict(frozen=True)

    table_name: StrippedNonEmptyStr
    generated_at: datetime
    cursors: dict[str, int] = Field(default_factory=dict)
    config_fingerprint: str | None = None
    operations: tuple[Operation, ...] = ()
    findings: tuple[Finding, ...] = ()

    @property
    def is_noop(self) -> bool:
        """True when applying the plan issues no DDL at all."""
        return not self.operations

    @property
    def creates(self) -> tuple[CreatePartition, ...]:
        """Top-level creations."""
        return tuple(op for op in self.operations if isinstance(op, CreatePartition))

    @property
    def attaches(self) -> tuple[AttachPartition, ...]:
        """Re-attachments."""
        return tuple(op for op in self.operations if isinstance(op, AttachPartition))

    @property
    def detaches(self) -> tuple[DetachPartition, ...]:
        """Detaches."""
        return tuple(op for op in self.operations if isinstance(op, DetachPartition))

    @property
    def drops(self) -> tuple[DropPartition, ...]:
        """Drops."""
        return tuple(op for op in self.operations if isinstance(op, DropPartition))

    @property
    def actionable_findings(self) -> tuple[Finding, ...]:
        """Findings an operator has to act on."""
        return tuple(f for f in self.findings if f.is_actionable)

    @property
    def relation_count(self) -> int:
        """Number of relations the plan creates, subtrees included."""
        return sum(op.count() for op in self.creates)

    def without(self, *kinds: OperationKind) -> MaintenancePlan:
        """Return the plan minus every operation of the given kinds.

        Dropping the detaches also drops the drops that follow them: a
        partition that is not detached this run cannot be dropped this run.
        """
        excluded = set(kinds)
        kept: list[Operation] = []
        for op in self.operations:
            if op.kind in excluded:
                continue
            if isinstance(op, DropPartition) and op.follows_detach and OperationKind.DETACH in excluded:
                continue
            kept.append(op)
        return self.model_copy(update={"operations": tuple(kept)})

    def only(self, *kinds: OperationKind) -> MaintenancePlan:
        """Return the plan reduced to the given kinds (see :meth:`without`)."""
        return self.without(*(kind for kind in OperationKind if kind not in kinds))

    def describe(self) -> str:
        """Render the plan as one line per operation and finding."""
        lines = [f"plan for {self.table_name} at {self.generated_at.isoformat()}"]
        for op in self.operations:
            lines.extend(_describe_operation(op, indent=1))
        for finding in self.findings:
            lines.append(
                f"  [{finding.severity.value if finding.severity else '?'}] {finding.reason.value}: {finding.detail}"
            )
        if len(lines) == 1:
            lines.append("  nothing to do")
        return "\n".join(lines)


def validate_plan_for_config(
    config: TablePartitionConfig,
    plan: MaintenancePlan,
    *,
    allow_config_drift: bool = False,
) -> None:
    """Refuse a plan that was not made from ``config``.

    Two questions the identity revalidation on each destructive operation does
    not answer: whether this is the plan for this table, and whether it was
    planned under the policy in force now. Both become reachable the moment a
    plan is written to a file and applied by a later process -- against the
    wrong table of a document describing several, or after the retention it
    was planned under was edited. The plan would still be applied to exactly
    the relations it named, for reasons that had stopped being true.

    A plan carrying no fingerprint -- built by hand, or by a version that did
    not record one -- is checked for its table alone; there is nothing to
    compare it against, and inventing a refusal from that would be guessing.

    Args:
        config: The configuration the plan is about to be applied with.
        plan: The plan.
        allow_config_drift: Apply a plan whose fingerprint no longer matches
            the configuration.

    Raises:
        PlanConfigMismatchError: If the plan is for another table, or was made
            under another configuration and ``allow_config_drift`` is not set.
    """
    if plan.table_name != config.qualified_name:
        raise PlanConfigMismatchError(plan.table_name, f"the configuration describes {config.qualified_name!r}")
    if allow_config_drift or plan.config_fingerprint is None:
        return
    current = config.fingerprint
    if plan.config_fingerprint != current:
        msg = (
            f"the configuration changed after the plan was made ({plan.config_fingerprint} -> {current}); "
            "plan again, or pass allow_config_drift=True to apply it as it stands"
        )
        raise PlanConfigMismatchError(plan.table_name, msg)


def _describe_operation(op: Operation, indent: int) -> list[str]:
    prefix = "  " * indent
    line = f"{prefix}{op.describe()}"
    if op.size_bytes is not None:
        line += f" size={op.size_bytes}"
    if op.row_estimate is not None:
        line += f" rows~{op.row_estimate}"
    lines = [line]
    if isinstance(op, CreatePartition):
        for child in op.children:
            lines.extend(_describe_operation(child, indent + 1))
    return lines


CreatePartition.model_rebuild()
MaintenancePlan.model_rebuild()
