"""The shape of a partition tree, level by level.

A :data:`PartitionScheme` describes one level of a partitioned table — the
PostgreSQL method, the key, how children are named — and, optionally, the
level below it. The same three classes describe a root and a nested level:
:class:`HashPartitioning` at the root is a task table divided for parallel
workers; the same class as the ``child`` of a :class:`RangePartitioning` is a
weekly event table divided by tenant inside each week.

Levels come in two kinds, and the planner treats them differently:

* a **progression level** — :class:`RangePartitioning`, or a
  :class:`ListPartitioning` over an :class:`~pg_partsmith.boundaries.IntegerSequence`
  — divides an ordered, open-ended axis into windows. It is the *lifecycle
  dimension*: partitions are created ahead of a cursor and expire behind it,
  subtree included.
* a **set level** — :class:`HashPartitioning`, :class:`ListPartitioning` with
  explicit groups — divides a level into a fixed, complete set of members. It
  is reconciled (missing members created) and never expires.

Everything here is IO-free and serializable, apart from a custom
:class:`~pg_partsmith.boundaries.RangeBoundaries` object a user may pass in.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from enum import StrEnum
from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .boundaries import IntegerSequence, RangeBoundaries, TimeBoundaries, Window, parse_boundaries
from .constants import (
    DEFAULT_HASH_NAME_SUFFIX,
    DEFAULT_LIST_DEFAULT_NAME,
    DEFAULT_LIST_NAME_SUFFIX,
    MAX_IDENTIFIER_LENGTH,
    MAX_SCHEME_DEPTH,
)
from .topology import (
    DefaultBounds,
    HashBounds,
    ListBounds,
    PartitionBounds,
    PartitionType,
    RangeBounds,
    validate_pg_identifier,
)
from .types import PositiveInt, StrippedNonEmptyStr

__all__ = [
    "HashPartitioning",
    "LevelKind",
    "ListGroup",
    "ListPartitioning",
    "PartitionScheme",
    "RangePartitioning",
    "SchemeBase",
]


class LevelKind(StrEnum):
    """How the planner treats a level.

    Attributes:
        PROGRESSION: An ordered, open-ended sequence of windows with a cursor;
            the lifecycle dimension.
        SET: A fixed, complete set of members that is reconciled and never
            expires.
    """

    PROGRESSION = "progression"
    SET = "set"


class SchemeBase(BaseModel):
    """Fields and tree arithmetic shared by every partitioning method.

    Attributes:
        key: The level's partition key, in key order. A single column may be
            given as a plain string. Every column named here must be part of
            every UNIQUE / PRIMARY KEY constraint on the root table, or
            PostgreSQL refuses the level.
        child: Optional further level of partitioning inside every partition of
            this one.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    key: tuple[StrippedNonEmptyStr, ...]
    child: PartitionScheme | None = None

    @field_validator("key", mode="before")
    @classmethod
    def coerce_key(cls, v: object) -> tuple[str, ...]:
        """Accept a single column as a plain string."""
        if isinstance(v, str):
            return (v,)
        if isinstance(v, Sequence) and not isinstance(v, bytes):
            return tuple(str(c) for c in v)
        msg = f"key must be a column name or a sequence of them, got {type(v).__name__}"
        raise TypeError(msg)

    @field_validator("key")
    @classmethod
    def validate_key(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        """Validate and normalise every key column; reject a repeated one."""
        if not v:
            msg = "key must name at least one column"
            raise ValueError(msg)
        columns = tuple(validate_pg_identifier(column) for column in v)
        if len(set(columns)) != len(columns):
            msg = f"Partition key columns must be distinct, got {columns!r}"
            raise ValueError(msg)
        return columns

    @property
    def method(self) -> PartitionType:
        """PostgreSQL partitioning method of this level."""
        raise NotImplementedError

    @property
    def kind(self) -> LevelKind:
        """Whether this level is a progression or a set."""
        raise NotImplementedError

    @property
    def progression(self) -> RangeBoundaries | None:
        """The rule dividing this level's axis into windows; None for a set level."""
        return None

    @property
    def columns(self) -> tuple[str, ...]:
        """The level's partition key, in key order (alias of :attr:`key`)."""
        return self.key

    @property
    def leading_column(self) -> str:
        """The first key column."""
        return self.key[0]

    @property
    def key_arity(self) -> int:
        """Number of columns in the key."""
        return len(self.key)

    def own_name_budget(self) -> int:
        """Bytes this level alone adds to a child's name."""
        raise NotImplementedError

    def name_length_budget(self) -> int:
        """Bytes this level and everything below it add to a partition name.

        Used to keep generated names inside PostgreSQL's 63-byte identifier
        limit, which truncates silently — two children could otherwise collapse
        onto one name.
        """
        below = self.child.name_length_budget() if self.child is not None else 0
        return self.own_name_budget() + below

    def depth(self) -> int:
        """Number of levels this scheme describes, including itself."""
        return 1 + (self.child.depth() if self.child is not None else 0)

    def walk(self) -> list[PartitionScheme]:
        """Return this level and every level below it, outermost first."""
        levels: list[PartitionScheme] = [self]  # type: ignore[list-item]
        if self.child is not None:
            levels.extend(self.child.walk())
        return levels

    def all_columns(self) -> list[str]:
        """Every key column of every level, outermost first."""
        return [column for level in self.walk() for column in level.key]

    @model_validator(mode="after")
    def validate_tree(self) -> SchemeBase:
        """Bound the depth and keep every level on a fresh dimension."""
        if self.depth() > MAX_SCHEME_DEPTH:
            msg = f"A partition scheme is limited to {MAX_SCHEME_DEPTH} levels, got {self.depth()}"
            raise ValueError(msg)
        columns = self.all_columns()
        duplicates = sorted({column for column in columns if columns.count(column) > 1})
        if duplicates:
            msg = f"Partition columns must be distinct across levels; {duplicates!r} appears more than once"
            raise ValueError(msg)
        return self

    def describe(self) -> str:
        """Render the level as PostgreSQL would spell it."""
        return f"{self.method.value.upper()} ({', '.join(self.key)})"


class HashPartitioning(SchemeBase):
    """Divide a level into HASH buckets.

    ``modulus`` is the bucket count for *newly created* sets only. Existing sets
    keep the modulus they were built with — a hash set cannot change modulus
    without a rewrite — so lowering or raising it changes future partitions and
    leaves history alone.

    Attributes:
        method_name: Discriminator; always ``"hash"``.
        modulus: Number of buckets to create per set.
        name_suffix: Template appended to the parent's name; must contain
            ``{remainder}`` and otherwise only lowercase identifier characters.
    """

    _NAME_SUFFIX_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^[a-z0-9_]*\{remainder\}[a-z0-9_]*$")

    method_name: Literal["hash"] = Field(default="hash", alias="method")
    modulus: PositiveInt
    name_suffix: str = DEFAULT_HASH_NAME_SUFFIX

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, populate_by_name=True)

    @property
    def method(self) -> PartitionType:
        """HASH."""
        return PartitionType.HASH

    @property
    def kind(self) -> LevelKind:
        """A set."""
        return LevelKind.SET

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
        """Return the bounds of bucket ``remainder`` at this level's modulus."""
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

    model_config = ConfigDict(frozen=True, extra="forbid")

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


class ListPartitioning(SchemeBase):
    """Divide a level into LIST partitions: named value sets, or a sliding sequence.

    With ``groups``, the level is a **set**: every group is a partition owning
    an explicit set of values. Unlike HASH, such a level is never "complete" —
    there is always another value the world could produce — which is what
    ``include_default`` is for: a catch-all partition so an unknown value is
    stored rather than rejected. Groups are matched by the values they own
    rather than by name, so a tree built by another tool is recognised and left
    alone instead of being duplicated under different names.

    With ``sequence``, the level is a **progression**: every partition owns one
    integer value, the newest one is where the application writes, and the
    lifecycle policy opens the next value and expires old ones — GitLab's
    sliding list. The creation rule must be state-driven
    (:class:`~pg_partsmith.lifecycle.CreateNextIf`) or a horizon
    (:class:`~pg_partsmith.lifecycle.CreateUntil`): creating "ahead" of a
    cursor that *is* the newest partition would never converge.

    Attributes:
        method_name: Discriminator; always ``"list"``.
        groups: The partitions to maintain, each owning an explicit value set.
            Mutually exclusive with ``sequence``.
        sequence: The value sequence of a sliding list. Mutually exclusive
            with ``groups``.
        include_default: Maintain a DEFAULT catch-all partition alongside the
            groups. Not available with ``sequence``.
        default_name: Identifier fragment for that DEFAULT partition.
        name_suffix: Template appended to the parent's name for a group; must
            contain ``{name}`` and otherwise only lowercase identifier
            characters. A sequence names its partitions itself.
    """

    _NAME_SUFFIX_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^[a-z0-9_]*\{name\}[a-z0-9_]*$")

    method_name: Literal["list"] = Field(default="list", alias="method")
    groups: tuple[ListGroup, ...] = ()
    sequence: IntegerSequence | None = None
    include_default: bool = False
    default_name: StrippedNonEmptyStr = DEFAULT_LIST_DEFAULT_NAME
    name_suffix: str = DEFAULT_LIST_NAME_SUFFIX

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, populate_by_name=True)

    @property
    def method(self) -> PartitionType:
        """LIST."""
        return PartitionType.LIST

    @property
    def kind(self) -> LevelKind:
        """A progression over a sequence, a set of groups otherwise."""
        return LevelKind.PROGRESSION if self.sequence is not None else LevelKind.SET

    @property
    def progression(self) -> RangeBoundaries | None:
        """The sequence, when this is a sliding list."""
        return self.sequence

    def bounds_for(self, window: Window) -> PartitionBounds:
        """The LIST bound of the partition owning ``window``'s value."""
        assert self.sequence is not None  # only a progression level has windows
        return ListBounds(values=(str(self.sequence.value_of(window)),))

    def window_of(self, bounds: PartitionBounds) -> Window | None:
        """The window a single-value LIST bound stands for, or None."""
        if self.sequence is None or not isinstance(bounds, ListBounds) or bounds.includes_null:
            return None
        if len(bounds.values) != 1:
            return None
        value = self.sequence.decode(bounds.values[0])
        return None if value is None else Window(start=value, end=value + 1)

    @field_validator("name_suffix")
    @classmethod
    def validate_name_suffix(cls, v: str) -> str:
        """Reject templates that could not produce a safe, unique identifier."""
        if not cls._NAME_SUFFIX_PATTERN.match(v):
            msg = (
                f"name_suffix {v!r} must contain '{{name}}' and otherwise only lowercase letters, digits, "
                "and underscores"
            )
            raise ValueError(msg)
        return v

    @field_validator("default_name")
    @classmethod
    def validate_default_name(cls, v: str) -> str:
        """Keep the DEFAULT partition's fragment safe to splice into a name."""
        return validate_pg_identifier(v)

    @model_validator(mode="after")
    def validate_groups(self) -> ListPartitioning:
        """Reject a level PostgreSQL would refuse or that names two partitions alike."""
        if len(self.key) > 1:
            msg = (
                f"LIST partitioning takes exactly one column, got {self.key!r}. "
                "PostgreSQL rejects a composite LIST key."
            )
            raise ValueError(msg)
        if self.sequence is not None:
            if self.groups:
                msg = "LIST partitioning takes either groups or a sequence, not both"
                raise ValueError(msg)
            if self.include_default:
                msg = (
                    "A sliding LIST has no DEFAULT partition: the application writes the newest value, "
                    "which always has a partition"
                )
                raise ValueError(msg)
            return self
        if not self.groups:
            msg = "LIST partitioning requires at least one group, or a sequence"
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

    def default_bounds(self) -> DefaultBounds:
        """Bounds of the catch-all partition."""
        return DefaultBounds()

    def own_name_budget(self) -> int:
        """Bytes this level adds, sized for the longest group name (or the widest value)."""
        if self.sequence is not None:
            return self.sequence.own_name_budget()
        names = [g.name for g in self.groups]
        if self.include_default:
            names.append(self.default_name)
        return len(self.name_suffix) - len("{name}") + max(len(n) for n in names)


class RangePartitioning(SchemeBase):
    """Divide a level into RANGE windows along an ordered axis.

    This is the lifecycle dimension: :attr:`boundaries` decides where windows
    begin and end, the config's :class:`~pg_partsmith.lifecycle.LifecyclePolicy`
    decides which of them exist.

    Only the leading key column carries the window. A composite key's trailing
    columns are bounded with ``MINVALUE`` at both ends, so a partition holds
    exactly the rows whose leading column falls in its window — for rows whose
    trailing columns are all non-NULL; PostgreSQL routes a NULL trailing value
    to DEFAULT whatever the leading value.

    Attributes:
        method_name: Discriminator; always ``"range"``.
        boundaries: The rule dividing the axis: :class:`~pg_partsmith.boundaries.TimeBoundaries`,
            :class:`~pg_partsmith.boundaries.NumericBoundaries`, or any object
            implementing :class:`~pg_partsmith.boundaries.RangeBoundaries`.
            Serialized as a dict discriminated on ``kind``.
    """

    method_name: Literal["range"] = Field(default="range", alias="method")
    boundaries: Any

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, populate_by_name=True)

    @property
    def method(self) -> PartitionType:
        """RANGE."""
        return PartitionType.RANGE

    @property
    def kind(self) -> LevelKind:
        """A progression."""
        return LevelKind.PROGRESSION

    @field_validator("boundaries", mode="before")
    @classmethod
    def validate_boundaries(cls, v: object) -> RangeBoundaries:
        """Accept serialized boundaries as well as strategy objects."""
        return parse_boundaries(v)

    @property
    def range_boundaries(self) -> RangeBoundaries:
        """:attr:`boundaries`, typed."""
        return self.boundaries  # type: ignore[no-any-return]

    @property
    def progression(self) -> RangeBoundaries | None:
        """:attr:`boundaries`."""
        return self.range_boundaries

    def bounds_for(self, window: Window) -> PartitionBounds:
        """The RANGE bound of ``window``'s partition."""
        from_value, to_value = self.range_boundaries.literals(window)
        return RangeBounds(from_value=from_value, to_value=to_value)

    def window_of(self, bounds: PartitionBounds) -> Window | None:
        """The window a bounded, readable RANGE bound stands for, or None."""
        if not isinstance(bounds, RangeBounds):
            return None
        boundaries = self.range_boundaries
        start = boundaries.decode(bounds.from_value)
        end = boundaries.decode(bounds.to_value)
        return None if start is None or end is None else Window(start=start, end=end)

    @property
    def time_boundaries(self) -> TimeBoundaries | None:
        """:attr:`boundaries` when the axis is time, else None."""
        return self.boundaries if isinstance(self.boundaries, TimeBoundaries) else None

    def own_name_budget(self) -> int:
        """Bytes this level adds to a partition name.

        Built-in boundaries know their suffix; a custom strategy is asked
        through ``own_name_budget`` when it has one and otherwise assumed to
        fit in a generous fixed allowance.
        """
        budget = getattr(self.boundaries, "own_name_budget", None)
        if callable(budget):
            return int(budget())
        return _RANGE_NAME_ALLOWANCE


# Partition-name suffix an unknown boundaries strategy is assumed to need.
_RANGE_NAME_ALLOWANCE = len("__0000_00_00_00")


PartitionScheme = Annotated[
    RangePartitioning | ListPartitioning | HashPartitioning,
    Field(discriminator="method_name"),
]
"""One level of a partition tree, discriminated on ``method``."""


def name_fits(table_name: str, scheme: SchemeBase) -> tuple[bool, int]:
    """Return whether every generated name fits the identifier limit, and the worst case."""
    total = len(table_name.encode("utf-8")) + scheme.name_length_budget()
    return total <= MAX_IDENTIFIER_LENGTH, total


# Resolve the forward references (``child: PartitionScheme``) now that every
# member of the union exists.
HashPartitioning.model_rebuild()
ListPartitioning.model_rebuild()
RangePartitioning.model_rebuild()
