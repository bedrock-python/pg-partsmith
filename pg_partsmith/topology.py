"""Shape of a PostgreSQL partition tree: bounds, subpartition specs, and nodes.

Everything here is IO-free and shared by the aio and sync mirrors:

* :class:`PartitionType` — how a relation partitions its children.
* ``*Bounds`` — how a relation is bound *inside* its parent, as PostgreSQL
  renders it (``FOR VALUES FROM … TO …`` / ``WITH (MODULUS … REMAINDER …)`` /
  ``IN (…)`` / ``DEFAULT``).
* :class:`HashSubpartitionSpec` — the subpartitioning a user *asks for*.
* :class:`PartitionNode` — the tree that actually *exists*, as introspected
  from ``pg_partition_tree`` and friends.

The planner that turns the difference between the last two into DDL intentions
lives in :mod:`pg_partsmith.subpartition_plan`.
"""

from __future__ import annotations

import math
import re
from collections.abc import Collection, Sequence
from enum import StrEnum
from typing import Annotated, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .constants import (
    DEFAULT_HASH_NAME_SUFFIX,
    DEFAULT_LIST_DEFAULT_NAME,
    DEFAULT_LIST_NAME_SUFFIX,
    MAX_HASH_KEYSPACE_LCM,
    MAX_IDENTIFIER_LENGTH,
    MAX_SUBPARTITION_DEPTH,
)
from .types import NonNegativeInt, PositiveInt, StrippedNonEmptyStr


class PartitionType(StrEnum):
    """PostgreSQL partition type.

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


# ── Partition bounds ────────────────────────────────────────────────────────────
#
# One model per PostgreSQL bound spelling, discriminated on ``kind`` so a
# partition's bounds can be pattern-matched instead of string-parsed twice.


class RangeBounds(BaseModel):
    """``FOR VALUES FROM (from_value) TO (to_value)``.

    Attributes:
        from_value: Lower bound, inclusive.
        to_value: Upper bound, exclusive.
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

SubpartitionBounds = Annotated[
    HashBounds | ListBounds | DefaultBounds,
    Field(discriminator="kind"),
]
"""How a subpartition is bound in its parent.

Narrower than :data:`PartitionBounds`: a RANGE bound belongs to the time
dimension at the root, never to a level a subpartition spec creates. Keeping it
out makes the DDL renderer total over the cases it can actually receive.
"""


# ── Desired subpartitioning ─────────────────────────────────────────────────────
#
# A spec is declarative: it says what the tree *should* look like, and
# ``pg_partsmith.subpartition_plan`` works out which nodes are missing. The
# strategies differ in how a level is divided, so what they share — the column,
# the naming template, the level below — lives on a common base.


class SubpartitionSpecBase(BaseModel):
    """Fields and tree arithmetic shared by every subpartitioning strategy.

    A key is spelled as one leading column plus an optional tail, rather than a
    single tuple, so that ``column`` stays an ordinary field: it can be read
    without ever raising, and ``model_copy(update={"column": ...})`` does what
    it says. :attr:`columns` derives the whole key from the two.

    Attributes:
        column: The leading column this level partitions on.
        trailing_columns: The rest of the key, in key order; empty for the usual
            single-column case. Every column named here and above must be part
            of every UNIQUE/PRIMARY KEY constraint on the root table, or
            PostgreSQL refuses the subtree.
        name_suffix: Template appended to the parent's name to name each child.
        subpartition: Optional further level of subpartitioning.
    """

    model_config = ConfigDict(frozen=True)

    column: StrippedNonEmptyStr
    trailing_columns: tuple[StrippedNonEmptyStr, ...] = ()
    name_suffix: str
    subpartition: SubpartitionSpec | None = None

    @property
    def columns(self) -> tuple[str, ...]:
        """The whole partition key of this level, in key order."""
        return (self.column, *self.trailing_columns)

    @property
    def partition_type(self) -> PartitionType:
        """PostgreSQL partition type this spec describes."""
        raise NotImplementedError

    @field_validator("column")
    @classmethod
    def validate_column(cls, v: str) -> str:
        """Validate and normalise the leading partition key identifier."""
        return validate_pg_identifier(v)

    @field_validator("trailing_columns")
    @classmethod
    def validate_trailing_columns(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        """Validate and normalise the rest of the partition key."""
        return tuple(validate_pg_identifier(column) for column in v)

    @model_validator(mode="after")
    def validate_key_is_distinct(self) -> SubpartitionSpecBase:
        """A column repeated in the key would leave one position doing nothing."""
        if len(set(self.columns)) != len(self.columns):
            msg = f"Partition key columns must be distinct, got {self.columns!r}"
            raise ValueError(msg)
        return self

    def own_name_budget(self) -> int:
        """Bytes this level alone adds to a child's name."""
        raise NotImplementedError

    def name_length_budget(self) -> int:
        """Bytes this level and everything below it add to a partition name.

        Used to keep generated names inside PostgreSQL's 63-byte identifier
        limit, which truncates silently — two children could otherwise collapse
        onto one name.
        """
        below = self.subpartition.name_length_budget() if self.subpartition is not None else 0
        return self.own_name_budget() + below

    def depth(self) -> int:
        """Number of subpartition levels this spec describes, including itself."""
        return 1 + (self.subpartition.depth() if self.subpartition is not None else 0)

    def walk(self) -> list[SubpartitionSpec]:
        """Return this spec and every spec below it, outermost first."""
        specs: list[SubpartitionSpec] = [self]  # type: ignore[list-item]
        if self.subpartition is not None:
            specs.extend(self.subpartition.walk())
        return specs

    @model_validator(mode="after")
    def validate_depth(self) -> SubpartitionSpecBase:
        """Bound the tree depth so a typo cannot fan out into thousands of tables."""
        if self.depth() > MAX_SUBPARTITION_DEPTH:
            msg = f"Subpartitioning is limited to {MAX_SUBPARTITION_DEPTH} levels, got {self.depth()}"
            raise ValueError(msg)
        return self


class HashSubpartitionSpec(SubpartitionSpecBase):
    """Divide each partition of the level above into HASH buckets.

    ``modulus`` is the bucket count for *newly created* branches only. Existing
    branches keep the modulus they were built with — a hash set cannot change
    modulus without a rewrite — so lowering or raising it changes future
    periods and leaves history alone. See the reconciliation guide.

    Attributes:
        strategy: Discriminator; always ``"hash"``.
        modulus: Number of hash buckets to create per branch.
        name_suffix: Must contain ``{remainder}`` and otherwise only lowercase
            identifier characters.
    """

    _NAME_SUFFIX_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^[a-z0-9_]*\{remainder\}[a-z0-9_]*$")

    strategy: Literal["hash"] = "hash"
    modulus: PositiveInt
    name_suffix: str = DEFAULT_HASH_NAME_SUFFIX

    @property
    def partition_type(self) -> PartitionType:
        """PostgreSQL partition type this spec describes."""
        return PartitionType.HASH

    @field_validator("name_suffix")
    @classmethod
    def validate_name_suffix(cls, v: str) -> str:
        """Reject templates that could not produce a safe, unique identifier."""
        if not cls._NAME_SUFFIX_PATTERN.match(v):
            msg = (
                f"name_suffix {v!r} must contain '{{remainder}}' and otherwise only "
                "lowercase letters, digits, and underscores"
            )
            raise ValueError(msg)
        return v

    def child_name(self, parent_relname: str, remainder: int) -> str:
        """Return the bare relation name of one bucket under ``parent_relname``."""
        return f"{parent_relname}{self.name_suffix.format(remainder=remainder)}"

    def bounds_for(self, remainder: int) -> HashBounds:
        """Return the bounds of bucket ``remainder`` at this spec's modulus."""
        return HashBounds(modulus=self.modulus, remainder=remainder)

    def own_name_budget(self) -> int:
        """Bytes this level adds, sized for the widest remainder."""
        widest = len(str(self.modulus - 1))
        return len(self.name_suffix) - len("{remainder}") + widest


class ListGroup(BaseModel):
    """One named LIST partition and the key values it owns.

    Attributes:
        name: Identifier fragment used to name the partition.
        values: Values routed to it. Rendered as SQL string literals, which
            PostgreSQL coerces to the partition key's type, so numeric and
            textual keys are both written as strings here.
    """

    model_config = ConfigDict(frozen=True)

    name: StrippedNonEmptyStr
    values: tuple[StrippedNonEmptyStr, ...]

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Keep the fragment safe to splice into an identifier."""
        return validate_pg_identifier(v)

    @model_validator(mode="after")
    def validate_values(self) -> ListGroup:
        """A LIST partition owning no values could never route a row."""
        if not self.values:
            msg = f"LIST group {self.name!r} must own at least one value"
            raise ValueError(msg)
        if len(set(self.values)) != len(self.values):
            msg = f"LIST group {self.name!r} repeats a value: {self.values!r}"
            raise ValueError(msg)
        return self

    def bounds(self) -> ListBounds:
        """Return this group's partition bounds."""
        return ListBounds(values=self.values)


class ListSubpartitionSpec(SubpartitionSpecBase):
    """Divide each partition of the level above into named LIST partitions.

    Unlike HASH, a LIST level is never "complete": there is always another
    value the world could produce. That is what ``include_default`` is for — a
    catch-all partition so an unknown value is stored rather than rejected.

    Because groups are matched by the values they own rather than by name, a
    tree built by another tool is recognised and left alone instead of being
    duplicated under different names.

    Attributes:
        strategy: Discriminator; always ``"list"``.
        groups: The partitions to maintain, each owning an explicit value set.
        include_default: Maintain a DEFAULT catch-all partition alongside them.
        default_name: Identifier fragment for that DEFAULT partition.
        name_suffix: Must contain ``{name}`` and otherwise only lowercase
            identifier characters.
    """

    _NAME_SUFFIX_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^[a-z0-9_]*\{name\}[a-z0-9_]*$")

    strategy: Literal["list"] = "list"
    groups: tuple[ListGroup, ...]
    include_default: bool = False
    default_name: StrippedNonEmptyStr = DEFAULT_LIST_DEFAULT_NAME
    name_suffix: str = DEFAULT_LIST_NAME_SUFFIX

    @property
    def partition_type(self) -> PartitionType:
        """PostgreSQL partition type this spec describes."""
        return PartitionType.LIST

    @field_validator("name_suffix")
    @classmethod
    def validate_name_suffix(cls, v: str) -> str:
        """Reject templates that could not produce a safe, unique identifier."""
        if not cls._NAME_SUFFIX_PATTERN.match(v):
            msg = (
                f"name_suffix {v!r} must contain '{{name}}' and otherwise only "
                "lowercase letters, digits, and underscores"
            )
            raise ValueError(msg)
        return v

    @field_validator("default_name")
    @classmethod
    def validate_default_name(cls, v: str) -> str:
        """Keep the DEFAULT partition's fragment safe to splice into a name."""
        return validate_pg_identifier(v)

    @model_validator(mode="after")
    def validate_groups(self) -> ListSubpartitionSpec:
        """Reject a spec PostgreSQL would refuse or that names two partitions alike."""
        if self.trailing_columns:
            msg = (
                f"LIST partitioning takes exactly one column, got {self.columns!r}. "
                "PostgreSQL rejects a composite LIST key."
            )
            raise ValueError(msg)
        if not self.groups:
            msg = "LIST subpartitioning requires at least one group"
            raise ValueError(msg)

        names = [g.name for g in self.groups]
        if self.include_default:
            names.append(self.default_name)
        if len(set(names)) != len(names):
            msg = f"LIST group names must be distinct, got {names!r}"
            raise ValueError(msg)

        seen: dict[str, str] = {}
        for group in self.groups:
            for value in group.values:
                if value in seen:
                    msg = f"LIST value {value!r} is claimed by both {seen[value]!r} and {group.name!r}"
                    raise ValueError(msg)
                seen[value] = group.name

        return self

    def child_name(self, parent_relname: str, name: str) -> str:
        """Return the bare relation name of one child under ``parent_relname``."""
        return f"{parent_relname}{self.name_suffix.format(name=name)}"

    def own_name_budget(self) -> int:
        """Bytes this level adds, sized for the longest group name."""
        names = [g.name for g in self.groups]
        if self.include_default:
            names.append(self.default_name)
        return len(self.name_suffix) - len("{name}") + max(len(n) for n in names)


SubpartitionSpec = Annotated[
    HashSubpartitionSpec | ListSubpartitionSpec,
    Field(discriminator="strategy"),
]
"""The subpartitioning of one level, discriminated on ``strategy``."""


# ── Introspected tree ───────────────────────────────────────────────────────────


class PartitionNode(BaseModel):
    """One relation in an introspected partition tree.

    A node describes both how it sits in its parent (:attr:`bounds`) and how it
    partitions its own children (:attr:`partition_type`) — the two are
    independent, which is exactly what makes a nested tree expressible: a
    branch is a partition *and* a partitioned table at once.

    Attributes:
        name: Schema-qualified relation name.
        parent_name: Schema-qualified parent name; None for the queried root.
        level: Depth below the queried root (0 for the root itself).
        partition_type: How this relation partitions its *children*; None when
            it is a plain (leaf) table. Note that
            :attr:`~pg_partsmith.PartitionInfo.partition_type` means the
            opposite -- how the relation's *parent* partitions it -- and that
            the equivalent of this field there is ``subpartition_type``.
        partition_columns: This relation's own partition key columns.
        bounds: How this relation is bound inside its parent; None for the root.
        is_attached: ``pg_class.relispartition``. Descendants reached through a
            parent are attached by construction — a detached relation is not in
            anyone's tree — so this is informative mainly for the root itself.
        children: Direct children, ordered by name.
        has_unaddressable_children: Whether a child was left out of
            :attr:`children` because its name cannot be addressed by
            qualified-name DDL. The child set is then a subset of the real one,
            so nothing may be planned from it: a partition that looks missing
            may be one of the omitted ones.
        has_expression_key: Whether any position of this relation's own
            partition key is an expression rather than a column. Such a
            position has no name, so :attr:`partition_columns` is shorter than
            the real key and must not be compared against a spec as if it were
            complete.
    """

    model_config = ConfigDict(frozen=True)

    name: StrippedNonEmptyStr
    parent_name: StrippedNonEmptyStr | None = None
    level: NonNegativeInt = 0
    partition_type: PartitionType | None = None
    partition_columns: tuple[str, ...] = ()
    bounds: PartitionBounds | None = None
    is_attached: bool = True
    children: tuple[PartitionNode, ...] = ()
    has_unaddressable_children: bool = False
    has_expression_key: bool = False

    @property
    def is_leaf(self) -> bool:
        """True when this relation is a plain table that cannot hold partitions."""
        return self.partition_type is None

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
        if self.is_leaf:
            return "a plain leaf table"
        columns = ", ".join(self.partition_columns) or "?"
        assert self.partition_type is not None  # guarded by is_leaf above
        return f"partitioned by {self.partition_type.value.upper()} ({columns})"


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
        parent_name: Schema-qualified parent name; None for the queried root.
        bounds: How this relation is bound inside its parent.
        is_attached: ``pg_class.relispartition``.
        partition_type: How this relation partitions its own children.
        partition_columns: This relation's own partition key columns.
    """

    model_config = ConfigDict(frozen=True)

    level: NonNegativeInt
    name: StrippedNonEmptyStr
    parent_name: StrippedNonEmptyStr | None = None
    bounds: PartitionBounds | None = None
    is_attached: bool = True
    partition_type: PartitionType | None = None
    partition_columns: tuple[str, ...] = ()
    has_expression_key: bool = False


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
        parent_name=row.parent_name,
        level=row.level,
        partition_type=row.partition_type,
        partition_columns=row.partition_columns,
        has_expression_key=row.has_expression_key,
        bounds=row.bounds,
        is_attached=row.is_attached,
        children=children,
        has_unaddressable_children=row.name in unaddressable_parents,
    )


def _row_name(row: PartitionTreeRow) -> str:
    return row.name
