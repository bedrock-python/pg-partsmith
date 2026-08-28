"""When partitions are created, and when they expire, detach and drop.

A :class:`LifecyclePolicy` answers *when*; the scheme answers *what*. Both
are pure: every policy is a rule evaluated by the planner over a
:class:`Candidate` it already knows everything about. What a rule needs to
know beyond the catalog — a size, a row estimate, the answer to a SQL
question — it declares through :attr:`Predicate.required_facts`, and the
introspector gathers exactly that before planning. A monthly table with
``KeepNewest`` never pays for ``pg_total_relation_size``.

A policy decides eligibility; it never executes DDL. Ownership, safety and
locking stay with the core, which is what keeps a user predicate from turning
into an accidental ``DROP TABLE``.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .boundaries import Axis, RangeBoundaries, Window
from .constants import DEFAULT_CREATE_AHEAD_COUNT, DEFAULT_RETENTION_COUNT
from .topology import FactKind, PartitionFacts, PartitionNode
from .types import NonNegativeInt, PositiveInt

_INTEGER_PATTERN = re.compile(r"-?\d+")

__all__ = [
    "AllOf",
    "AnyOf",
    "Callback",
    "Candidate",
    "CreateAhead",
    "CreateNextIf",
    "CreateUntil",
    "CreationPolicy",
    "DetachMode",
    "DropAfter",
    "DropNever",
    "DropPolicy",
    "ExpireIf",
    "KeepBehind",
    "KeepFor",
    "KeepNewest",
    "LifecyclePolicy",
    "Not",
    "Predicate",
    "RetentionPolicy",
    "RowsAbove",
    "SizeAbove",
    "SqlPredicate",
    "Unreferenced",
    "WindowAgeAbove",
]


class Candidate(BaseModel):
    """One partition — existing or planned — as a policy sees it.

    Attributes:
        window: The window the partition covers on its level's axis.
        node: The relation, when it exists.
        now: The instant the plan is being made.
        cursor_window: The window holding the level's cursor ("now" on its axis).
        boundaries: The level's boundaries, for axis arithmetic.
        facts: What was measured about the relation, if anything.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    window: Window | None = None
    node: PartitionNode | None = None
    now: datetime
    cursor_window: Window
    boundaries: Any
    facts: PartitionFacts = Field(default_factory=PartitionFacts)

    @property
    def range_boundaries(self) -> RangeBoundaries:
        """:attr:`boundaries`, typed."""
        return self.boundaries  # type: ignore[no-any-return]

    @property
    def name(self) -> str | None:
        """The relation's name, when it exists."""
        return None if self.node is None else self.node.name


class PredicateBase(BaseModel):
    """A yes/no question about a candidate.

    Attributes:
        required_facts: What the introspector has to measure for this
            predicate to be answerable.
    """

    model_config = ConfigDict(frozen=True)

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Facts this predicate reads."""
        return frozenset()

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """SQL questions this predicate needs answered per candidate."""
        return ()

    def evaluate(self, candidate: Candidate) -> bool:
        """Answer the question for ``candidate``."""
        raise NotImplementedError

    def describe(self) -> str:
        """Render the rule for a human."""
        raise NotImplementedError


class SizeAbove(PredicateBase):
    """True when the partition and its subtree exceed ``bytes`` on disk.

    A partition that does not exist yet has no size and never satisfies this.
    """

    kind: Literal["size_above"] = "size_above"
    bytes: PositiveInt

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Size."""
        return frozenset({FactKind.SIZE})

    def evaluate(self, candidate: Candidate) -> bool:
        """Compare the measured size."""
        size = candidate.facts.size_bytes
        return size is not None and size > self.bytes

    def describe(self) -> str:
        """Render the rule."""
        return f"size > {self.bytes} bytes"


class RowsAbove(PredicateBase):
    """True when the partition's estimated live rows exceed ``rows``.

    Uses planner statistics, never ``COUNT(*)``; a fresh partition reads as
    empty until the statistics collector catches up.
    """

    kind: Literal["rows_above"] = "rows_above"
    rows: NonNegativeInt

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Rows."""
        return frozenset({FactKind.ROWS})

    def evaluate(self, candidate: Candidate) -> bool:
        """Compare the estimate."""
        rows = candidate.facts.row_estimate
        return rows is not None and rows > self.rows

    def describe(self) -> str:
        """Render the rule."""
        return f"rows > {self.rows}"


class WindowAgeAbove(PredicateBase):
    """True when the window *ended* more than ``age`` ago (time axis only).

    On an integer axis the window has no age and this is never true.
    """

    kind: Literal["window_age_above"] = "window_age_above"
    age: timedelta

    def evaluate(self, candidate: Candidate) -> bool:
        """Compare the window's end against ``now - age``."""
        if candidate.range_boundaries.axis is not Axis.TIME or candidate.window is None:
            return False
        end = candidate.window.end
        return isinstance(end, datetime) and end <= candidate.now - self.age

    def describe(self) -> str:
        """Render the rule."""
        return f"window ended more than {self.age} ago"


class Unreferenced(PredicateBase):
    """True when no row of another table references a row of the partition.

    The condition PostgreSQL enforces on ``DETACH PARTITION`` when a foreign
    key points at the parent: a partition whose rows are still referenced
    cannot be taken out of service, and the statement fails with ``23503``.
    Putting this in the retention rule keeps such partitions attached until
    the referencing rows are gone -- the GitLab rule for ``ci_builds`` --
    instead of failing the run.

    Foreign keys on the parent and on the partition itself are both checked.
    A partition that was not measured reads as referenced, so it is kept.
    """

    kind: Literal["unreferenced"] = "unreferenced"

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """References."""
        return frozenset({FactKind.REFERENCES})

    def evaluate(self, candidate: Candidate) -> bool:
        """True only when the introspector found nothing referencing the partition."""
        return candidate.facts.referenced is False

    def describe(self) -> str:
        """Render the rule."""
        return "no row of another table references it"


class SqlPredicate(PredicateBase):
    """A boolean SQL question the introspector asks about each candidate.

    The statement must yield one boolean. ``{partition}`` is replaced with the
    candidate's quoted, schema-qualified name; nothing else is interpolated.
    A partition that does not exist yet cannot be asked and reads as False.

    Example::

        SqlPredicate("SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')")
    """

    kind: Literal["sql"] = "sql"
    sql: str

    @field_validator("sql")
    @classmethod
    def validate_sql(cls, v: str) -> str:
        """The placeholder is the only way the statement can reach the partition."""
        if "{partition}" not in v:
            msg = "SqlPredicate.sql must reference the candidate as {partition}"
            raise ValueError(msg)
        return v

    @property
    def id(self) -> str:
        """Stable key the result is stored under in :attr:`PartitionFacts.predicates`."""
        return hashlib.sha256(self.sql.encode()).hexdigest()[:16]

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """Itself."""
        return (self,)

    def evaluate(self, candidate: Candidate) -> bool:
        """Read the answer the introspector stored."""
        return candidate.facts.predicates.get(self.id, False)

    def describe(self) -> str:
        """Render the rule."""
        return f"SQL: {self.sql}"


class Callback(PredicateBase):
    """A rule written in Python, evaluated over the candidate's facts.

    The callable receives a :class:`Candidate` and returns a bool. It must be
    pure: whatever it needs measured is declared in ``facts`` and gathered
    before planning, so the same rule serves the aio and sync mirrors alike.
    Excluded from serialization.

    Attributes:
        fn: The rule.
        facts: Facts the rule reads.
        label: How the rule is described in plans and logs.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["callback"] = "callback"
    fn: Callable[[Candidate], bool] = Field(exclude=True)
    facts: frozenset[FactKind] = frozenset()
    label: str = "callback"

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """What the rule declared."""
        return self.facts

    def evaluate(self, candidate: Candidate) -> bool:
        """Call the rule."""
        return bool(self.fn(candidate))

    def describe(self) -> str:
        """Render the rule."""
        return self.label


class AllOf(PredicateBase):
    """True when every member is true."""

    kind: Literal["all_of"] = "all_of"
    members: tuple[Predicate, ...]

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Union of the members' facts."""
        return frozenset().union(*(m.required_facts for m in self.members))

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """Every member's SQL questions."""
        return tuple(p for m in self.members for p in m.sql_predicates)

    def evaluate(self, candidate: Candidate) -> bool:
        """Conjunction."""
        return all(m.evaluate(candidate) for m in self.members)

    def describe(self) -> str:
        """Render the rule."""
        return "(" + " AND ".join(m.describe() for m in self.members) + ")"


class AnyOf(PredicateBase):
    """True when at least one member is true."""

    kind: Literal["any_of"] = "any_of"
    members: tuple[Predicate, ...]

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Union of the members' facts."""
        return frozenset().union(*(m.required_facts for m in self.members))

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """Every member's SQL questions."""
        return tuple(p for m in self.members for p in m.sql_predicates)

    def evaluate(self, candidate: Candidate) -> bool:
        """Disjunction."""
        return any(m.evaluate(candidate) for m in self.members)

    def describe(self) -> str:
        """Render the rule."""
        return "(" + " OR ".join(m.describe() for m in self.members) + ")"


class Not(PredicateBase):
    """True when the member is false."""

    kind: Literal["not"] = "not"
    member: Predicate

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """The member's facts."""
        return self.member.required_facts

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """The member's SQL questions."""
        return self.member.sql_predicates

    def evaluate(self, candidate: Candidate) -> bool:
        """Negation."""
        return not self.member.evaluate(candidate)

    def describe(self) -> str:
        """Render the rule."""
        return f"NOT {self.member.describe()}"


# ── Creation ────────────────────────────────────────────────────────────────────


class CreateAhead(PredicateBase):
    """Keep ``count`` windows in existence starting from the cursor's, inclusive.

    ``CreateAhead(3)`` on a monthly table in June means June, July and August.
    """

    kind: Literal["create_ahead"] = "create_ahead"
    count: PositiveInt = DEFAULT_CREATE_AHEAD_COUNT

    def desired_windows(
        self, cursor_window: Window, boundaries: RangeBoundaries, newest: Candidate | None
    ) -> list[Window]:
        """The cursor's window and the ``count - 1`` after it."""
        return [boundaries.shift(cursor_window, offset) for offset in range(self.count)]

    def evaluate(self, candidate: Candidate) -> bool:
        """Creation policies are not candidate predicates."""
        raise NotImplementedError

    def describe(self) -> str:
        """Render the rule."""
        return f"create {self.count} ahead"


class CreateUntil(PredicateBase):
    """Keep every window from the cursor's up to the one holding ``position``.

    ``CreateUntil(datetime(2028, 1, 1, tzinfo=UTC))`` is "partitions through the
    end of next year"; ``CreateUntil(5_000_000)`` on an integer axis is
    "partitions for the first five million ids". A position behind the cursor
    yields the cursor's window alone.
    """

    kind: Literal["create_until"] = "create_until"
    position: Any

    @field_validator("position", mode="before")
    @classmethod
    def parse_position(cls, v: object) -> object:
        """Read a horizon back from its serialized form.

        A datetime dumps to an ISO string and an integer to a digit string;
        either is turned back into the position it stood for so a policy that
        went through JSON still plans.
        """
        if not isinstance(v, str):
            return v
        text = v.strip()
        if _INTEGER_PATTERN.fullmatch(text):
            return int(text)
        try:
            instant = datetime.fromisoformat(text)
        except ValueError:
            msg = f"CreateUntil.position must be a datetime, an integer, or their ISO/decimal spelling, got {v!r}"
            raise ValueError(msg) from None
        return instant if instant.tzinfo is not None else instant.replace(tzinfo=UTC)

    def desired_windows(
        self, cursor_window: Window, boundaries: RangeBoundaries, newest: Candidate | None
    ) -> list[Window]:
        """Every window from the cursor's to the horizon's."""
        horizon = boundaries.window_at(self.position)
        windows = [cursor_window]
        while windows[-1].start < horizon.start:
            windows.append(boundaries.shift(windows[-1], 1))
        return windows

    def evaluate(self, candidate: Candidate) -> bool:
        """Creation policies are not candidate predicates."""
        raise NotImplementedError

    def describe(self) -> str:
        """Render the rule."""
        return f"create until {self.position}"


class CreateNextIf(PredicateBase):
    """Create the window after the newest existing one when ``when`` holds for it.

    The cursor's window always exists. Beyond it, the next window is created
    only once the newest partition satisfies the predicate — "when it holds
    more than 10 GB", "when its oldest row is older than a day". This is how
    a sequence of partitions is rotated by application state rather than by
    the calendar.
    """

    kind: Literal["create_next_if"] = "create_next_if"
    when: Predicate

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Whatever the predicate reads."""
        return self.when.required_facts

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """Whatever the predicate asks."""
        return self.when.sql_predicates

    def desired_windows(
        self, cursor_window: Window, boundaries: RangeBoundaries, newest: Candidate | None
    ) -> list[Window]:
        """The cursor's window, plus the next one when the newest qualifies."""
        windows = [cursor_window]
        if newest is not None and newest.window is not None and self.when.evaluate(newest):
            windows.append(boundaries.shift(newest.window, 1))
        return windows

    def evaluate(self, candidate: Candidate) -> bool:
        """Creation policies are not candidate predicates."""
        raise NotImplementedError

    def describe(self) -> str:
        """Render the rule."""
        return f"create next when {self.when.describe()}"


CreationPolicy = Annotated[CreateAhead | CreateUntil | CreateNextIf, Field(discriminator="kind")]
"""Which windows ahead of the cursor must exist."""


# ── Retention ───────────────────────────────────────────────────────────────────


class KeepNewest(PredicateBase):
    """Keep the ``count`` newest windows, the cursor's included; older ones expire.

    A window is expired when it ends at or before the start of the window
    ``count - 1`` steps behind the cursor's. Windows ahead of the cursor are
    never expired.
    """

    kind: Literal["keep_newest"] = "keep_newest"
    count: PositiveInt = DEFAULT_RETENTION_COUNT

    def evaluate(self, candidate: Candidate) -> bool:
        """True when the candidate lies behind the retention cutoff."""
        if candidate.window is None:
            return False
        cutoff = candidate.range_boundaries.shift(candidate.cursor_window, -(self.count - 1))
        return bool(candidate.window.end <= cutoff.start)

    def describe(self) -> str:
        """Render the rule."""
        return f"keep newest {self.count}"


class KeepFor(PredicateBase):
    """Keep a window until it has been over for ``age`` (time axis only).

    ``KeepFor(timedelta(days=90))`` expires a partition ninety days after its
    last instant. On an integer axis nothing ever expires under this rule.
    """

    kind: Literal["keep_for"] = "keep_for"
    age: timedelta

    def evaluate(self, candidate: Candidate) -> bool:
        """True when the window ended more than ``age`` ago."""
        return WindowAgeAbove(age=self.age).evaluate(candidate)

    def describe(self) -> str:
        """Render the rule."""
        return f"keep for {self.age}"


class KeepBehind(PredicateBase):
    """Keep a window while it ends within ``distance`` of the cursor (integer axis).

    ``KeepBehind(10_000_000)`` on a queue partitioned by message id expires a
    partition once the newest id is ten million past its upper bound — the
    rule ``pg_partman`` applies to id-based sets. On a time axis nothing ever
    expires under this rule.
    """

    kind: Literal["keep_behind"] = "keep_behind"
    distance: PositiveInt

    def evaluate(self, candidate: Candidate) -> bool:
        """True when the cursor is ``distance`` or more past the window's end."""
        if candidate.range_boundaries.axis is not Axis.INTEGER or candidate.window is None:
            return False
        cursor = candidate.cursor_window.start
        return bool(int(cursor) - int(candidate.window.end) >= self.distance)

    def describe(self) -> str:
        """Render the rule."""
        return f"keep within {self.distance} of the cursor"


class ExpireIf(PredicateBase):
    """Expire a window when an arbitrary predicate holds for it.

    Combine with the calendar rules for "older than N *and* fully processed"::

        ExpireIf(AllOf((KeepNewest(12), SqlPredicate("... {partition} ..."))))
    """

    kind: Literal["expire_if"] = "expire_if"
    when: Predicate

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Whatever the predicate reads."""
        return self.when.required_facts

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """Whatever the predicate asks."""
        return self.when.sql_predicates

    def evaluate(self, candidate: Candidate) -> bool:
        """Delegate."""
        return self.when.evaluate(candidate)

    def describe(self) -> str:
        """Render the rule."""
        return f"expire when {self.when.describe()}"


Predicate = Annotated[
    SizeAbove
    | RowsAbove
    | WindowAgeAbove
    | Unreferenced
    | SqlPredicate
    | Callback
    | KeepNewest
    | KeepFor
    | KeepBehind
    | ExpireIf
    | AllOf
    | AnyOf
    | Not,
    Field(discriminator="kind"),
]
"""Any yes/no question about a candidate, discriminated on ``kind``.

The retention rules are predicates too, so they combine with everything else:
``AllOf((KeepNewest(count=12), SqlPredicate(...)))`` is "older than twelve
periods *and* nothing pending".
"""

RetentionPolicy = Predicate
"""When a window behind the cursor has expired: any predicate."""


# ── Detach and drop ─────────────────────────────────────────────────────────────


class DetachMode(StrEnum):
    """How an expired partition is detached.

    Attributes:
        AUTO: ``DETACH … CONCURRENTLY``, falling back to a blocking detach when
            PostgreSQL refuses it (a DEFAULT partition exists).
        CONCURRENT: ``DETACH … CONCURRENTLY`` only; fails when refused.
        BLOCKING: Plain ``DETACH``, taking ``ACCESS EXCLUSIVE`` on the parent.
    """

    AUTO = "auto"
    CONCURRENT = "concurrent"
    BLOCKING = "blocking"


class DropAfter(BaseModel):
    """Drop a detached partition once ``grace`` has passed since it was detached.

    ``DropAfter(timedelta(0))`` drops in the same run as the detach. An orphan
    whose detach instant is unknown (marked by an older version, or adopted
    without one) is treated as past its grace.

    Attributes:
        grace: How long a detached partition is kept before it is dropped.
        when: An extra condition on the orphan — "not while bigger than 150 GB
            on a weekday", say — evaluated when the grace has passed.
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal["drop_after"] = "drop_after"
    grace: timedelta = timedelta(0)
    when: Predicate | None = None

    @field_validator("grace")
    @classmethod
    def validate_grace(cls, v: timedelta) -> timedelta:
        """A negative grace is a typo."""
        if v < timedelta(0):
            msg = f"grace must not be negative, got {v}"
            raise ValueError(msg)
        return v

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Whatever the condition reads."""
        return frozenset() if self.when is None else self.when.required_facts

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """Whatever the condition asks."""
        return () if self.when is None else self.when.sql_predicates

    def grace_elapsed(self, detached_at: datetime | None, now: datetime) -> bool:
        """True when the orphan may be dropped as far as the grace is concerned."""
        return detached_at is None or detached_at + self.grace <= now

    def describe(self) -> str:
        """Render the rule."""
        text = "drop immediately" if self.grace == timedelta(0) else f"drop after {self.grace}"
        return text if self.when is None else f"{text} when {self.when.describe()}"


class DropNever(BaseModel):
    """Detach expired partitions and keep them: something else owns the drop."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["drop_never"] = "drop_never"

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Nothing."""
        return frozenset()

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """Nothing."""
        return ()

    def describe(self) -> str:
        """Render the rule."""
        return "never drop"


DropPolicy = Annotated[DropAfter | DropNever, Field(discriminator="kind")]
"""What happens to a partition after it was detached."""


class LifecyclePolicy(BaseModel):
    """When partitions of a progression level are created, detached and dropped.

    Attributes:
        creation: Which windows ahead of the cursor must exist.
        retention: When a window behind the cursor has expired.
        detach: How an expired partition is detached.
        drop: What happens to it afterwards.
    """

    model_config = ConfigDict(frozen=True)

    creation: CreationPolicy = Field(default_factory=CreateAhead)
    retention: RetentionPolicy = Field(default_factory=KeepNewest)
    detach: DetachMode = DetachMode.AUTO
    drop: DropPolicy = Field(default_factory=DropAfter)

    @property
    def required_facts(self) -> frozenset[FactKind]:
        """Everything any rule needs measured."""
        return self.creation.required_facts | self.retention.required_facts | self.drop.required_facts

    @property
    def sql_predicates(self) -> tuple[SqlPredicate, ...]:
        """Every SQL question any rule asks."""
        return self.creation.sql_predicates + self.retention.sql_predicates + self.drop.sql_predicates

    @property
    def needs_facts(self) -> bool:
        """True when planning has to measure something beyond the catalog."""
        return bool(self.required_facts or self.sql_predicates)


# Resolve the recursive combinator references now that every member exists.
Callback.model_rebuild()
KeepNewest.model_rebuild()
KeepFor.model_rebuild()
KeepBehind.model_rebuild()
ExpireIf.model_rebuild()
AllOf.model_rebuild()
AnyOf.model_rebuild()
Not.model_rebuild()
CreateNextIf.model_rebuild()
DropAfter.model_rebuild()
LifecyclePolicy.model_rebuild()
