"""The partition tree that actually exists: bounds, nodes, orphans, facts.

Everything here is IO-free and shared by the aio and sync mirrors:

* :class:`PartitionType` — how a relation partitions its children.
* ``*Bounds`` — how a relation is bound *inside* its parent, as PostgreSQL
  renders it (``FOR VALUES FROM … TO …`` / ``WITH (MODULUS … REMAINDER …)`` /
  ``IN (…)`` / ``DEFAULT``).
* :class:`PartitionNode` — one relation of an introspected tree, and
  :class:`ActualTree` — the tree plus the marker-tagged orphans below its root.
* :class:`PartitionFacts` — what the introspector measured about a relation
  when a policy asked for it.

The desired shape lives in :mod:`pg_partsmith.scheme`; the planner that turns
the difference between the two into a :class:`~pg_partsmith.plan.MaintenancePlan`
lives in :mod:`pg_partsmith.planner`.
"""

from __future__ import annotations

import math
import re
from collections.abc import Collection, Sequence
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .constants import MAX_HASH_KEYSPACE_LCM, MAX_IDENTIFIER_LENGTH
from .types import NonNegativeInt, PositiveInt, StrippedNonEmptyStr

__all__ = [
    "ActualTree",
    "DefaultBounds",
    "DetachedPartition",
    "FactKind",
    "HashBounds",
    "ListBounds",
    "PartitionBounds",
    "PartitionFacts",
    "PartitionNode",
    "PartitionTreeRow",
    "PartitionType",
    "RangeBounds",
    "RelationKind",
    "build_partition_tree",
    "hash_keyspace_covered",
    "missing_remainders",
    "uniform_modulus",
    "validate_pg_identifier",
]


class PartitionType(StrEnum):
    """PostgreSQL partitioning method.

    Attributes:
        RANGE: Range partitioning (e.g., by date ranges).
        LIST: List partitioning (e.g., by specific values).
        HASH: Hash partitioning (e.g., by hash of key).
    """

    RANGE = "range"
    LIST = "list"
    HASH = "hash"

    @classmethod
    def from_partstrat(cls, strat: str | None) -> PartitionType | None:
        """Map a ``pg_partitioned_table.partstrat`` code to a partition type."""
        return {"r": cls.RANGE, "l": cls.LIST, "h": cls.HASH}.get(strat or "")


class RelationKind(StrEnum):
    """What a member of a partition tree physically is (``pg_class.relkind``).

    Attributes:
        TABLE: An ordinary table — a leaf that stores rows.
        PARTITIONED: A partitioned table — a branch with children of its own.
        FOREIGN: A foreign table — a leaf whose rows live elsewhere. The
            library never creates, drops, or comments on one.
        OTHER: Anything else PostgreSQL may put in a tree in a future version.
    """

    TABLE = "table"
    PARTITIONED = "partitioned"
    FOREIGN = "foreign"
    OTHER = "other"

    @classmethod
    def from_relkind(cls, relkind: str | None) -> RelationKind:
        """Map a ``pg_class.relkind`` code to a kind."""
        return {"r": cls.TABLE, "p": cls.PARTITIONED, "f": cls.FOREIGN}.get(relkind or "", cls.OTHER)

    @property
    def is_droppable_table(self) -> bool:
        """True when ``DROP TABLE`` is the statement that removes it."""
        return self in {RelationKind.TABLE, RelationKind.PARTITIONED}


# ── Partition bounds ────────────────────────────────────────────────────────────
#
# One model per PostgreSQL bound spelling, discriminated on ``kind`` so a
# partition's bounds can be pattern-matched instead of string-parsed twice.


class RangeBounds(BaseModel):
    """``FOR VALUES FROM (from_value) TO (to_value)``.

    Under a composite key only the leading element of each tuple is kept: the
    trailing columns are bounded with ``MINVALUE`` at both ends.

    Attributes:
        from_value: Lower bound, inclusive. ``MINVALUE`` when unbounded.
        to_value: Upper bound, exclusive. ``MAXVALUE`` when unbounded.
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal["range"] = "range"
    from_value: StrippedNonEmptyStr
    to_value: StrippedNonEmptyStr


class ListBounds(BaseModel):
    """``FOR VALUES IN (values…)``.

    Attributes:
        values: The literal values routed to this partition.
        includes_null: Whether ``NULL`` itself is one of them. It is kept apart
            from :attr:`values` because ``IN (NULL)`` and ``IN ('NULL')`` are
            different partitions, and reading them as the same one would make
            the planner propose a partition PostgreSQL already has.
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal["list"] = "list"
    values: tuple[str, ...]
    includes_null: bool = False


class HashBounds(BaseModel):
    """``FOR VALUES WITH (MODULUS modulus, REMAINDER remainder)``.

    A hash partition owns the rows whose key hash is congruent to
    ``remainder`` modulo ``modulus``; a set of them is complete only when the
    owned residue classes tile the whole keyspace (see
    :func:`hash_keyspace_covered`).

    Attributes:
        modulus: Number of buckets this partition's residue class is taken from.
        remainder: Residue this partition owns; always ``< modulus``.
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal["hash"] = "hash"
    modulus: PositiveInt
    remainder: NonNegativeInt

    @model_validator(mode="after")
    def validate_remainder_in_range(self) -> HashBounds:
        """Reject a remainder outside ``[0, modulus)`` — PostgreSQL would too."""
        if self.remainder >= self.modulus:
            msg = f"remainder must be < modulus, got remainder={self.remainder} modulus={self.modulus}"
            raise ValueError(msg)
        return self


class DefaultBounds(BaseModel):
    """``DEFAULT`` — the catch-all partition of a RANGE or LIST parent."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["default"] = "default"


PartitionBounds = Annotated[
    RangeBounds | ListBounds | HashBounds | DefaultBounds,
    Field(discriminator="kind"),
]
"""Any partition bound description, discriminated on ``kind``."""


# ── Facts ───────────────────────────────────────────────────────────────────────


class FactKind(StrEnum):
    """Something the introspector can measure about a relation, on request.

    Attributes:
        SIZE: Total on-disk size of the relation and its subtree, in bytes.
        ROWS: Estimated live rows (planner statistics, never ``COUNT(*)``).
    """

    SIZE = "size"
    ROWS = "rows"


class PartitionFacts(BaseModel):
    """What was measured about one relation.

    Only what a policy asked for is populated; everything else stays None so a
    plan for a simple monthly table never pays for ``pg_total_relation_size``.

    Attributes:
        size_bytes: Total size of the relation and its subtree.
        row_estimate: Estimated live rows across the subtree, from planner
            statistics. ``0`` before the first ``ANALYZE`` / stats flush.
        predicates: Result of each :class:`~pg_partsmith.lifecycle.SqlPredicate`
            evaluated against the relation, keyed by the predicate's id.
    """

    model_config = ConfigDict(frozen=True)

    size_bytes: NonNegativeInt | None = None
    row_estimate: NonNegativeInt | None = None
    predicates: dict[str, bool] = Field(default_factory=dict)


# ── Introspected tree ───────────────────────────────────────────────────────────


class PartitionNode(BaseModel):
    """One relation in an introspected partition tree.

    A node describes both how it sits in its parent (:attr:`bounds`) and how it
    partitions its own children (:attr:`partition_type`) — the two are
    independent, which is exactly what makes a nested tree expressible: a
    branch is a partition *and* a partitioned table at once.

    Attributes:
        name: Schema-qualified relation name.
        oid: ``pg_class.oid`` — the relation's identity across renames, and what
            a destructive operation is revalidated against.
        parent_name: Schema-qualified parent name; None for the queried root.
        level: Depth below the queried root (0 for the root itself).
        relkind: What the relation physically is.
        partition_type: How this relation partitions its *children*; None when
            it is a leaf. Note that
            :attr:`~pg_partsmith.PartitionInfo.partition_type` means the
            opposite -- how the relation's *parent* partitions it -- and that
            the equivalent of this field there is ``subpartition_type``.
        partition_columns: This relation's own partition key columns.
        bounds: How this relation is bound inside its parent; None for the root.
        bounds_expr: The bound as ``pg_get_expr(relpartbound, oid)`` rendered it,
            kept for the shapes the parser does not understand.
        is_attached: ``pg_class.relispartition``. Descendants reached through a
            parent are attached by construction — a detached relation is not in
            anyone's tree — so this is informative mainly for the root itself.
        detach_pending: ``pg_inherits.inhdetachpending`` — a
            ``DETACH CONCURRENTLY`` was interrupted; the partition is invisible
            through its parent and rejects its own rows until finalized.
        children: Direct children, ordered by name.
        has_unaddressable_children: Whether a child was left out of
            :attr:`children` because its name cannot be addressed by
            qualified-name DDL. The child set is then a subset of the real one,
            so nothing may be planned from it: a partition that looks missing
            may be one of the omitted ones.
        has_expression_key: Whether any position of this relation's own
            partition key is an expression rather than a column. Such a
            position has no name, so :attr:`partition_columns` is shorter than
            the real key and must not be compared against a scheme as if it
            were complete.
        facts: What the introspector measured, when something asked for it.
    """

    model_config = ConfigDict(frozen=True)

    name: StrippedNonEmptyStr
    oid: int | None = None
    parent_name: StrippedNonEmptyStr | None = None
    level: NonNegativeInt = 0
    relkind: RelationKind = RelationKind.TABLE
    partition_type: PartitionType | None = None
    partition_columns: tuple[str, ...] = ()
    bounds: PartitionBounds | None = None
    bounds_expr: str | None = None
    is_attached: bool = True
    detach_pending: bool = False
    children: tuple[PartitionNode, ...] = ()
    has_unaddressable_children: bool = False
    has_expression_key: bool = False
    facts: PartitionFacts | None = None

    @model_validator(mode="after")
    def derive_relkind(self) -> PartitionNode:
        """A node that partitions children is a partitioned table, whatever was said."""
        if self.partition_type is not None and self.relkind is RelationKind.TABLE:
            object.__setattr__(self, "relkind", RelationKind.PARTITIONED)
        return self

    @property
    def is_leaf(self) -> bool:
        """True when this relation cannot hold partitions of its own."""
        return self.partition_type is None

    @property
    def is_default(self) -> bool:
        """True for a DEFAULT partition."""
        return isinstance(self.bounds, DefaultBounds)

    @property
    def is_foreign(self) -> bool:
        """True for a foreign table."""
        return self.relkind is RelationKind.FOREIGN

    @property
    def relname(self) -> str:
        """Bare relation name without the schema qualifier."""
        _, _, relname = self.name.rpartition(".")
        return relname or self.name

    @property
    def hash_children(self) -> tuple[PartitionNode, ...]:
        """Children bound by ``MODULUS``/``REMAINDER``."""
        return tuple(c for c in self.children if isinstance(c.bounds, HashBounds))

    def walk(self) -> list[PartitionNode]:
        """Return this node and every node below it, depth-first."""
        nodes = [self]
        for child in self.children:
            nodes.extend(child.walk())
        return nodes

    def find(self, name: str) -> PartitionNode | None:
        """Return the node with schema-qualified ``name``, or None."""
        return next((n for n in self.walk() if n.name == name), None)

    def describe_topology(self) -> str:
        """Render a one-line summary used in topology diagnostics."""
        if self.is_foreign:
            return "a foreign table"
        if self.is_leaf:
            return "a plain leaf table"
        columns = ", ".join(self.partition_columns) or "?"
        assert self.partition_type is not None  # guarded by is_leaf above
        return f"partitioned by {self.partition_type.value.upper()} ({columns})"


class DetachedPartition(BaseModel):
    """A table this library detached (or adopted) and has not dropped yet.

    Found by its ``COMMENT`` marker, not by name: the marker is the only
    evidence that cleanup of the table is ours to do.

    Attributes:
        name: Schema-qualified relation name.
        oid: ``pg_class.oid``, revalidated before the drop.
        relkind: What the relation physically is.
        parent_name: The parent the marker names.
        detached_at: When the marker was written, when the marker records it.
            Orphans marked by an older version carry no instant.
        facts: What the introspector measured, when something asked for it.
    """

    model_config = ConfigDict(frozen=True)

    name: StrippedNonEmptyStr
    oid: int | None = None
    relkind: RelationKind = RelationKind.TABLE
    parent_name: StrippedNonEmptyStr
    detached_at: datetime | None = None
    facts: PartitionFacts | None = None

    @property
    def relname(self) -> str:
        """Bare relation name without the schema qualifier."""
        _, _, relname = self.name.rpartition(".")
        return relname or self.name


class ActualTree(BaseModel):
    """Everything below one root that maintenance may act on.

    Attributes:
        root: The partitioned table and its whole attached subtree.
        orphans: Marker-tagged detached tables whose marker names the root.
    """

    model_config = ConfigDict(frozen=True)

    root: PartitionNode
    orphans: tuple[DetachedPartition, ...] = ()

    def find(self, name: str) -> PartitionNode | None:
        """Return the attached node with schema-qualified ``name``, or None."""
        return self.root.find(name)


def uniform_modulus(bounds: tuple[HashBounds, ...]) -> int | None:
    """Return the single modulus shared by ``bounds``, or None when they differ.

    An empty set has no modulus and also returns None; callers distinguish the
    two cases by checking ``bounds`` themselves.
    """
    moduli = {b.modulus for b in bounds}
    return moduli.pop() if len(moduli) == 1 else None


def hash_keyspace_covered(bounds: tuple[HashBounds, ...]) -> bool | None:
    """True when ``bounds`` tile the whole hash keyspace.

    PostgreSQL allows hash siblings at *different* moduli as long as their
    residue classes do not overlap — ``(2, 1)`` and ``(4, 0)`` coexist happily.
    Such a set is complete only if every residue modulo the least common
    multiple of the moduli is owned by someone; a gap means rows hashing there
    are rejected outright with a check violation, so it must be detected rather
    than assumed.

    Returns:
        True/False, or None when the moduli are too coarse to enumerate within
        :data:`~pg_partsmith.constants.MAX_HASH_KEYSPACE_LCM` (coverage is then
        unknown and must not be guessed at).
    """
    if not bounds:
        return False

    span = math.lcm(*(b.modulus for b in bounds))
    if span > MAX_HASH_KEYSPACE_LCM:
        return None

    covered: set[int] = set()
    for b in bounds:
        covered.update(range(b.remainder, span, b.modulus))
    return len(covered) == span


def missing_remainders(modulus: int, bounds: tuple[HashBounds, ...]) -> tuple[int, ...]:
    """Return the remainders at ``modulus`` that ``bounds`` do not already own."""
    present = {b.remainder for b in bounds if b.modulus == modulus}
    return tuple(r for r in range(modulus) if r not in present)


def validate_pg_identifier(v: str) -> str:
    """Validate and normalise a PostgreSQL identifier to lowercase.

    PostgreSQL folds unquoted identifiers to lower-case; normalising here
    ensures that metadata catalogue queries and quoted DDL identifiers always
    refer to the same object.

    Raises:
        ValueError: If ``v`` is not a plain identifier or exceeds the 63-byte
            limit PostgreSQL truncates at.
    """
    v = v.lower()
    if not _IDENTIFIER_PATTERN.match(v):
        msg = f"Invalid SQL identifier: {v!r}"
        raise ValueError(msg)
    if len(v) > MAX_IDENTIFIER_LENGTH:
        msg = f"SQL identifier too long (max {MAX_IDENTIFIER_LENGTH} chars): {v!r}"
        raise ValueError(msg)
    return v


_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


class PartitionTreeRow(BaseModel):
    """One flat catalog row on its way to becoming a :class:`PartitionNode`.

    The metadata providers parse bounds and strategy codes; assembling the rows
    into a tree is pure, so both the aio and sync mirrors share one
    implementation via :func:`build_partition_tree`.

    Attributes:
        level: Depth below the queried root, as ``pg_partition_tree`` reports it.
        name: Schema-qualified relation name.
        oid: ``pg_class.oid``.
        parent_name: Schema-qualified parent name; None for the queried root.
        relkind: What the relation physically is.
        bounds: How this relation is bound inside its parent.
        bounds_expr: The bound as the catalog rendered it.
        is_attached: ``pg_class.relispartition``.
        detach_pending: ``pg_inherits.inhdetachpending``.
        partition_type: How this relation partitions its own children.
        partition_columns: This relation's own partition key columns.
        has_expression_key: Whether the key holds an expression position.
        facts: Measurements taken alongside, if any.
    """

    model_config = ConfigDict(frozen=True)

    level: NonNegativeInt
    name: StrippedNonEmptyStr
    oid: int | None = None
    parent_name: StrippedNonEmptyStr | None = None
    relkind: RelationKind = RelationKind.TABLE
    bounds: PartitionBounds | None = None
    bounds_expr: str | None = None
    is_attached: bool = True
    detach_pending: bool = False
    partition_type: PartitionType | None = None
    partition_columns: tuple[str, ...] = ()
    has_expression_key: bool = False
    facts: PartitionFacts | None = None


def build_partition_tree(
    rows: Sequence[PartitionTreeRow],
    unaddressable_parents: Collection[str] = (),
) -> PartitionNode | None:
    """Assemble flat catalog rows into a tree, rooted at the level-0 row.

    Rows whose parent is missing from the input are dropped rather than
    re-parented: a partial tree would silently misreport a branch's child set,
    and reconciliation reads that set to decide what to create.

    Args:
        rows: Catalog rows for one tree, in any order.
        unaddressable_parents: Names of parents whose child rows the caller
            dropped before calling. Their nodes are marked so the planner can
            refuse to read a shortened child set as a set of gaps.

    Returns:
        The root node with its descendants attached, or None when ``rows``
        contains no level-0 row.
    """
    children_by_parent: dict[str, list[PartitionTreeRow]] = {}
    root: PartitionTreeRow | None = None

    for row in rows:
        if row.level == 0:
            root = row
        elif row.parent_name is not None:
            children_by_parent.setdefault(row.parent_name, []).append(row)

    if root is None:
        return None

    return _to_node(root, children_by_parent, frozenset(unaddressable_parents))


def _to_node(
    row: PartitionTreeRow,
    children_by_parent: dict[str, list[PartitionTreeRow]],
    unaddressable_parents: frozenset[str],
) -> PartitionNode:
    """Build one node and, recursively, everything below it."""
    children = tuple(
        _to_node(child, children_by_parent, unaddressable_parents)
        for child in sorted(children_by_parent.get(row.name, ()), key=_row_name)
    )
    return PartitionNode(
        name=row.name,
        oid=row.oid,
        parent_name=row.parent_name,
        level=row.level,
        relkind=row.relkind,
        partition_type=row.partition_type,
        partition_columns=row.partition_columns,
        has_expression_key=row.has_expression_key,
        bounds=row.bounds,
        bounds_expr=row.bounds_expr,
        is_attached=row.is_attached,
        detach_pending=row.detach_pending,
        children=children,
        has_unaddressable_children=row.name in unaddressable_parents,
        facts=row.facts,
    )


def _row_name(row: PartitionTreeRow) -> str:
    return row.name
