"""Lifecycle policies and predicates, evaluated over hand-built candidates.

Every rule here is pure: a :class:`Candidate` carries everything a policy may
read, so each test pins one rule against one candidate without a database.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from pg_partsmith.boundaries import NumericBoundaries, TimeBoundaries, Window
from pg_partsmith.constants import DEFAULT_CREATE_AHEAD_COUNT, DEFAULT_RETENTION_COUNT
from pg_partsmith.lifecycle import (
    AllOf,
    AnyOf,
    Callback,
    Candidate,
    CreateAhead,
    CreateNextIf,
    CreateUntil,
    DetachMode,
    DropAfter,
    DropNever,
    ExpireIf,
    KeepBehind,
    KeepFor,
    KeepNewest,
    LifecyclePolicy,
    Not,
    Predicate,
    PredicateBase,
    RetentionPolicy,
    RowsAbove,
    SizeAbove,
    SqlPredicate,
    Unreferenced,
    WindowAgeAbove,
)
from pg_partsmith.periods import PartitionGranularity
from pg_partsmith.topology import FactKind, PartitionFacts, PartitionNode

NOW = datetime(2026, 8, 28, tzinfo=UTC)
MONTHS = TimeBoundaries(granularity=PartitionGranularity.MONTH)
STEPS = NumericBoundaries(step=100_000)
CURSOR_MONTH = MONTHS.window_at(NOW)
CURSOR_STEP = STEPS.window_at(1_250_000)
SQL = "SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')"


def _month(year: int, month: int) -> Window:
    return MONTHS.window_at(datetime(year, month, 1, tzinfo=UTC))


def _step(start: int) -> Window:
    return STEPS.window_at(start)


def _candidate(
    window: Window | None = None,
    *,
    facts: PartitionFacts | None = None,
    boundaries: TimeBoundaries | NumericBoundaries = MONTHS,
    cursor_window: Window | None = None,
    node: PartitionNode | None = None,
) -> Candidate:
    if cursor_window is None:
        cursor_window = CURSOR_STEP if isinstance(boundaries, NumericBoundaries) else CURSOR_MONTH
    return Candidate(
        window=window,
        node=node,
        now=NOW,
        cursor_window=cursor_window,
        boundaries=boundaries,
        facts=facts if facts is not None else PartitionFacts(),
    )


def _facts(**values: Any) -> PartitionFacts:
    return PartitionFacts(**values)


# ── Candidate ───────────────────────────────────────────────────────────────────


def test__candidate__with_a_node__exposes_its_name_and_typed_boundaries() -> None:
    # Arrange
    node = PartitionNode(name="public.events__2026_08")

    # Act
    candidate = _candidate(_month(2026, 8), node=node)

    # Assert
    assert candidate.name == "public.events__2026_08"
    assert candidate.range_boundaries is MONTHS


def test__candidate__without_a_node__has_no_name_and_empty_facts() -> None:
    # Arrange / Act
    candidate = _candidate()

    # Assert
    assert candidate.name is None
    assert candidate.facts == PartitionFacts()


# ── SizeAbove ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("size", "expected"),
    [(200, True), (101, True), (100, False), (0, False), (None, False)],
    ids=["above", "just-above", "equal", "zero", "unmeasured"],
)
def test__size_above__evaluate__true_only_strictly_above_the_threshold(size: int | None, expected: bool) -> None:
    # Arrange
    predicate = SizeAbove(bytes=100)
    candidate = _candidate(_month(2026, 8), facts=_facts(size_bytes=size))

    # Act / Assert
    assert predicate.evaluate(candidate) is expected


def test__size_above__metadata__declares_size_and_no_sql() -> None:
    # Arrange
    predicate = SizeAbove(bytes=100)

    # Act / Assert
    assert predicate.required_facts == frozenset({FactKind.SIZE})
    assert predicate.sql_predicates == ()
    assert predicate.describe() == "size > 100 bytes"


def test__size_above__non_positive_threshold__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        SizeAbove(bytes=0)


# ── RowsAbove ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("rows", "expected"),
    [(1, True), (0, False), (None, False)],
    ids=["above", "equal", "unmeasured"],
)
def test__rows_above__evaluate__true_only_strictly_above_the_threshold(rows: int | None, expected: bool) -> None:
    # Arrange
    predicate = RowsAbove(rows=0)
    candidate = _candidate(_month(2026, 8), facts=_facts(row_estimate=rows))

    # Act / Assert
    assert predicate.evaluate(candidate) is expected


def test__rows_above__metadata__declares_rows_and_no_sql() -> None:
    # Arrange
    predicate = RowsAbove(rows=0)

    # Act / Assert
    assert predicate.required_facts == frozenset({FactKind.ROWS})
    assert predicate.sql_predicates == ()
    assert predicate.describe() == "rows > 0"


def test__rows_above__negative_threshold__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        RowsAbove(rows=-1)


# ── WindowAgeAbove ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("window", "expected"),
    [
        (_month(2026, 4), True),  # ended 2026-05-01, well over 90 days ago
        (_month(2026, 5), False),  # ended 2026-06-01, 88 days ago
        (_month(2026, 8), False),  # the cursor's own window
        (_month(2026, 9), False),  # ahead of the cursor
    ],
    ids=["old", "recent", "current", "future"],
)
def test__window_age_above__time_axis__compares_the_window_end_against_now_minus_age(
    window: Window, expected: bool
) -> None:
    # Arrange
    predicate = WindowAgeAbove(age=timedelta(days=90))

    # Act / Assert
    assert predicate.evaluate(_candidate(window)) is expected


def test__window_age_above__window_ended_exactly_age_ago__counts_as_over() -> None:
    # Arrange
    predicate = WindowAgeAbove(age=NOW - datetime(2026, 5, 1, tzinfo=UTC))

    # Act / Assert
    assert predicate.evaluate(_candidate(_month(2026, 4))) is True


def test__window_age_above__integer_axis__never_true() -> None:
    # Arrange
    predicate = WindowAgeAbove(age=timedelta(0))

    # Act / Assert
    assert predicate.evaluate(_candidate(_step(0), boundaries=STEPS)) is False


def test__window_age_above__no_window__never_true() -> None:
    # Arrange
    predicate = WindowAgeAbove(age=timedelta(0))

    # Act / Assert
    assert predicate.evaluate(_candidate(None)) is False


def test__window_age_above__metadata__needs_nothing_measured() -> None:
    # Arrange
    predicate = WindowAgeAbove(age=timedelta(days=90))

    # Act / Assert
    assert predicate.required_facts == frozenset()
    assert predicate.sql_predicates == ()
    assert predicate.describe() == "window ended more than 90 days, 0:00:00 ago"


# ── SqlPredicate ────────────────────────────────────────────────────────────────


def test__sql_predicate__without_the_partition_placeholder__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match=r"must reference the candidate as \{partition\}"):
        SqlPredicate(sql="SELECT true")


def test__sql_predicate__id__stable_and_derived_from_the_statement() -> None:
    # Arrange
    first = SqlPredicate(sql=SQL)
    second = SqlPredicate(sql=SQL)
    other = SqlPredicate(sql="SELECT false FROM {partition}")

    # Act / Assert
    assert first.id == second.id
    assert first.id != other.id
    assert len(first.id) == 16
    assert int(first.id, 16) >= 0


@pytest.mark.parametrize(("answer", "expected"), [(True, True), (False, False)], ids=["holds", "fails"])
def test__sql_predicate__evaluate__reads_the_answer_stored_under_its_id(answer: bool, expected: bool) -> None:
    # Arrange
    predicate = SqlPredicate(sql=SQL)
    candidate = _candidate(_month(2026, 8), facts=_facts(predicates={predicate.id: answer}))

    # Act / Assert
    assert predicate.evaluate(candidate) is expected


def test__sql_predicate__evaluate__unanswered_reads_as_false() -> None:
    # Arrange
    predicate = SqlPredicate(sql=SQL)

    # Act / Assert
    assert predicate.evaluate(_candidate(_month(2026, 8))) is False


def test__sql_predicate__metadata__asks_itself_and_needs_no_catalog_fact() -> None:
    # Arrange
    predicate = SqlPredicate(sql=SQL)

    # Act / Assert
    assert predicate.sql_predicates == (predicate,)
    assert predicate.required_facts == frozenset()
    assert predicate.describe() == f"SQL: {SQL}"


# ── Callback ────────────────────────────────────────────────────────────────────


def test__callback__evaluate__calls_the_rule_with_the_candidate_and_coerces_to_bool() -> None:
    # Arrange
    seen: list[Candidate] = []

    def rule(candidate: Candidate) -> int:
        seen.append(candidate)
        return 1

    predicate = Callback(fn=rule)
    candidate = _candidate(_month(2026, 8))

    # Act
    result = predicate.evaluate(candidate)

    # Assert
    assert result is True
    assert seen == [candidate]


def test__callback__metadata__declares_its_facts_and_label() -> None:
    # Arrange
    predicate = Callback(fn=lambda _: False, facts=frozenset({FactKind.SIZE, FactKind.ROWS}), label="weekday only")

    # Act / Assert
    assert predicate.required_facts == frozenset({FactKind.SIZE, FactKind.ROWS})
    assert predicate.sql_predicates == ()
    assert predicate.describe() == "weekday only"


def test__callback__defaults__no_facts_and_generic_label() -> None:
    # Arrange / Act
    predicate = Callback(fn=lambda _: True)

    # Assert
    assert predicate.required_facts == frozenset()
    assert predicate.describe() == "callback"


def test__callback__model_dump__excludes_the_callable() -> None:
    # Arrange
    predicate = Callback(fn=lambda _: True, facts=frozenset({FactKind.SIZE}), label="x")

    # Act
    dumped = predicate.model_dump(mode="json")

    # Assert
    assert dumped == {"kind": "callback", "facts": ["size"], "label": "x"}


# ── Combinators ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("size", "rows", "expected"),
    [(200, 5, True), (200, 0, False), (50, 5, False), (50, 0, False)],
    ids=["both", "size-only", "rows-only", "neither"],
)
def test__all_of__evaluate__conjunction(size: int, rows: int, expected: bool) -> None:
    # Arrange
    predicate = AllOf(members=(SizeAbove(bytes=100), RowsAbove(rows=0)))
    candidate = _candidate(_month(2026, 8), facts=_facts(size_bytes=size, row_estimate=rows))

    # Act / Assert
    assert predicate.evaluate(candidate) is expected


@pytest.mark.parametrize(
    ("size", "rows", "expected"),
    [(200, 5, True), (200, 0, True), (50, 5, True), (50, 0, False)],
    ids=["both", "size-only", "rows-only", "neither"],
)
def test__any_of__evaluate__disjunction(size: int, rows: int, expected: bool) -> None:
    # Arrange
    predicate = AnyOf(members=(SizeAbove(bytes=100), RowsAbove(rows=0)))
    candidate = _candidate(_month(2026, 8), facts=_facts(size_bytes=size, row_estimate=rows))

    # Act / Assert
    assert predicate.evaluate(candidate) is expected


@pytest.mark.parametrize(("rows", "expected"), [(0, True), (5, False)], ids=["empty", "populated"])
def test__not__evaluate__negation(rows: int, expected: bool) -> None:
    # Arrange
    predicate = Not(member=RowsAbove(rows=0))
    candidate = _candidate(_month(2026, 8), facts=_facts(row_estimate=rows))

    # Act / Assert
    assert predicate.evaluate(candidate) is expected


def test__all_of__required_facts__union_of_the_members() -> None:
    # Arrange
    predicate = AllOf(members=(SizeAbove(bytes=1), Not(member=RowsAbove(rows=0)), WindowAgeAbove(age=timedelta(1))))

    # Act / Assert
    assert predicate.required_facts == frozenset({FactKind.SIZE, FactKind.ROWS})


def test__any_of__required_facts__union_of_the_members() -> None:
    # Arrange
    predicate = AnyOf(members=(Callback(fn=lambda _: True, facts=frozenset({FactKind.ROWS})), SizeAbove(bytes=1)))

    # Act / Assert
    assert predicate.required_facts == frozenset({FactKind.SIZE, FactKind.ROWS})


def test__not__required_facts__the_members_facts() -> None:
    # Arrange
    predicate = Not(member=SizeAbove(bytes=1))

    # Act / Assert
    assert predicate.required_facts == frozenset({FactKind.SIZE})


def test__combinators__sql_predicates__collected_in_member_order_through_every_level() -> None:
    # Arrange
    first = SqlPredicate(sql="SELECT true FROM {partition}")
    second = SqlPredicate(sql="SELECT false FROM {partition}")
    third = SqlPredicate(sql="SELECT 1 = 1 FROM {partition}")
    predicate = AllOf(members=(first, AnyOf(members=(SizeAbove(bytes=1), second)), Not(member=third)))

    # Act / Assert
    assert predicate.sql_predicates == (first, second, third)
    assert Not(member=SizeAbove(bytes=1)).sql_predicates == ()


def test__combinators__describe__renders_infix_notation() -> None:
    # Arrange
    predicate = AllOf(
        members=(SizeAbove(bytes=100), AnyOf(members=(RowsAbove(rows=0), Not(member=WindowAgeAbove(age=timedelta(1))))))
    )

    # Act / Assert
    assert predicate.describe() == "(size > 100 bytes AND (rows > 0 OR NOT window ended more than 1 day, 0:00:00 ago))"


def test__all_of__empty__vacuously_true_and_any_of_empty_false() -> None:
    # Arrange
    candidate = _candidate(_month(2026, 8))

    # Act / Assert
    assert AllOf(members=()).evaluate(candidate) is True
    assert AnyOf(members=()).evaluate(candidate) is False


def test__all_of__keep_newest_member__accepted_as_documented_by_expire_if() -> None:
    # Arrange / Act
    predicate = AllOf(members=(KeepNewest(count=3), SqlPredicate(sql=SQL)))

    # Assert
    assert predicate.evaluate(_candidate(_month(2026, 4), facts=_facts(predicates={SqlPredicate(sql=SQL).id: True})))


# ── Creation policies ───────────────────────────────────────────────────────────


def test__create_ahead__desired_windows__cursor_window_and_the_next_count_minus_one() -> None:
    # Arrange
    policy = CreateAhead(count=3)

    # Act
    windows = policy.desired_windows(CURSOR_MONTH, MONTHS, None)

    # Assert
    assert windows == [_month(2026, 8), _month(2026, 9), _month(2026, 10)]


def test__create_ahead__defaults__library_default_count_and_no_facts() -> None:
    # Arrange / Act
    policy = CreateAhead()

    # Assert
    assert policy.count == DEFAULT_CREATE_AHEAD_COUNT
    assert policy.required_facts == frozenset()
    assert policy.sql_predicates == ()
    assert policy.describe() == f"create {DEFAULT_CREATE_AHEAD_COUNT} ahead"


def test__create_ahead__count_of_one__only_the_cursor_window() -> None:
    # Arrange / Act
    windows = CreateAhead(count=1).desired_windows(CURSOR_STEP, STEPS, None)

    # Assert
    assert windows == [_step(1_200_000)]


def test__create_ahead__non_positive_count__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        CreateAhead(count=0)


def test__create_until__horizon_ahead__every_window_up_to_the_one_holding_it() -> None:
    # Arrange
    policy = CreateUntil(position=datetime(2026, 10, 15, tzinfo=UTC))

    # Act
    windows = policy.desired_windows(CURSOR_MONTH, MONTHS, None)

    # Assert
    assert windows == [_month(2026, 8), _month(2026, 9), _month(2026, 10)]


def test__create_until__horizon_behind_the_cursor__cursor_window_alone() -> None:
    # Arrange
    policy = CreateUntil(position=datetime(2025, 1, 1, tzinfo=UTC))

    # Act
    windows = policy.desired_windows(CURSOR_MONTH, MONTHS, None)

    # Assert
    assert windows == [_month(2026, 8)]


def test__create_until__horizon_inside_the_cursor_window__cursor_window_alone() -> None:
    # Arrange
    policy = CreateUntil(position=datetime(2026, 8, 31, 23, tzinfo=UTC))

    # Act / Assert
    assert policy.desired_windows(CURSOR_MONTH, MONTHS, None) == [_month(2026, 8)]


def test__create_until__integer_axis__walks_steps_up_to_the_horizon() -> None:
    # Arrange
    policy = CreateUntil(position=1_450_000)

    # Act
    windows = policy.desired_windows(CURSOR_STEP, STEPS, None)

    # Assert
    assert windows == [_step(1_200_000), _step(1_300_000), _step(1_400_000)]
    assert policy.describe() == "create until 1450000"


def test__create_next_if__newest_qualifies__adds_the_window_after_the_newest() -> None:
    # Arrange
    policy = CreateNextIf(when=SizeAbove(bytes=10))
    newest = _candidate(_month(2026, 9), facts=_facts(size_bytes=20))

    # Act
    windows = policy.desired_windows(CURSOR_MONTH, MONTHS, newest)

    # Assert: the next window follows the newest, not the cursor.
    assert windows == [_month(2026, 8), _month(2026, 10)]


def test__create_next_if__newest_does_not_qualify__cursor_window_alone() -> None:
    # Arrange
    policy = CreateNextIf(when=SizeAbove(bytes=10))
    newest = _candidate(_month(2026, 8), facts=_facts(size_bytes=5))

    # Act / Assert
    assert policy.desired_windows(CURSOR_MONTH, MONTHS, newest) == [_month(2026, 8)]


def test__create_next_if__no_newest__cursor_window_alone() -> None:
    # Arrange
    policy = CreateNextIf(when=SizeAbove(bytes=10))

    # Act / Assert
    assert policy.desired_windows(CURSOR_MONTH, MONTHS, None) == [_month(2026, 8)]


def test__create_next_if__newest_without_a_window__cursor_window_alone() -> None:
    # Arrange
    policy = CreateNextIf(when=Callback(fn=lambda _: True))
    newest = _candidate(None, facts=_facts(size_bytes=20))

    # Act / Assert
    assert policy.desired_windows(CURSOR_MONTH, MONTHS, newest) == [_month(2026, 8)]


def test__create_next_if__metadata__delegates_to_its_predicate() -> None:
    # Arrange
    sql = SqlPredicate(sql=SQL)
    policy = CreateNextIf(when=AllOf(members=(SizeAbove(bytes=10), sql)))

    # Act / Assert
    assert policy.required_facts == frozenset({FactKind.SIZE})
    assert policy.sql_predicates == (sql,)
    assert policy.describe() == f"create next when (size > 10 bytes AND SQL: {SQL})"


@pytest.mark.parametrize(
    "policy",
    [CreateAhead(count=1), CreateUntil(position=5), CreateNextIf(when=SizeAbove(bytes=1))],
    ids=["ahead", "until", "next-if"],
)
def test__creation_policies__evaluate__not_candidate_predicates(
    policy: CreateAhead | CreateUntil | CreateNextIf,
) -> None:
    # Arrange / Act / Assert
    with pytest.raises(NotImplementedError):
        policy.evaluate(_candidate(_month(2026, 8)))


# ── Retention policies ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("window", "expected"),
    [
        (_month(2026, 4), True),  # ends 2026-05-01, before the cutoff
        (_month(2026, 5), True),  # ends 2026-06-01, exactly at the cutoff
        (_month(2026, 6), False),  # the third-newest kept window
        (_month(2026, 8), False),  # the cursor's window
        (_month(2026, 9), False),  # ahead of the cursor
    ],
    ids=["behind", "at-cutoff", "kept", "current", "future"],
)
def test__keep_newest__evaluate__expires_windows_ending_at_or_before_the_cutoff(window: Window, expected: bool) -> None:
    # Arrange: count=3 keeps June, July and August; the cutoff is June's start.
    policy = KeepNewest(count=3)

    # Act / Assert
    assert policy.evaluate(_candidate(window)) is expected


def test__keep_newest__integer_axis__counts_steps_behind_the_cursor_window() -> None:
    # Arrange: cursor window [1.2M, 1.3M); keeping 2 makes [1.1M, 1.2M) the cutoff.
    policy = KeepNewest(count=2)

    # Act / Assert
    assert policy.evaluate(_candidate(_step(1_000_000), boundaries=STEPS)) is True
    assert policy.evaluate(_candidate(_step(1_100_000), boundaries=STEPS)) is False


def test__keep_newest__no_window__never_expires() -> None:
    # Arrange / Act / Assert
    assert KeepNewest(count=1).evaluate(_candidate(None)) is False


def test__keep_newest__defaults__library_default_count_and_description() -> None:
    # Arrange / Act
    policy = KeepNewest()

    # Assert
    assert policy.count == DEFAULT_RETENTION_COUNT
    assert policy.describe() == f"keep newest {DEFAULT_RETENTION_COUNT}"
    assert policy.required_facts == frozenset()
    assert policy.sql_predicates == ()


@pytest.mark.parametrize(
    ("window", "expected"),
    [(_month(2026, 4), True), (_month(2026, 5), False)],
    ids=["over-for-119-days", "over-for-88-days"],
)
def test__keep_for__time_axis__expires_once_the_window_has_been_over_for_the_age(
    window: Window, expected: bool
) -> None:
    # Arrange
    policy = KeepFor(age=timedelta(days=90))

    # Act / Assert
    assert policy.evaluate(_candidate(window)) is expected


def test__keep_for__integer_axis__never_expires() -> None:
    # Arrange
    policy = KeepFor(age=timedelta(0))

    # Act / Assert
    assert policy.evaluate(_candidate(_step(0), boundaries=STEPS)) is False


def test__keep_for__describe__names_the_age() -> None:
    # Arrange / Act / Assert
    assert KeepFor(age=timedelta(days=90)).describe() == "keep for 90 days, 0:00:00"


@pytest.mark.parametrize(
    ("window", "expected"),
    [
        (_step(100_000), True),  # cursor window start 1.2M is 1.0M past its end
        (_step(200_000), False),  # only 0.9M past its end
        (_step(1_200_000), False),  # the cursor's own window
    ],
    ids=["far-behind", "within-distance", "current"],
)
def test__keep_behind__integer_axis__expires_once_the_cursor_is_distance_past_the_end(
    window: Window, expected: bool
) -> None:
    # Arrange
    policy = KeepBehind(distance=1_000_000)

    # Act / Assert
    assert policy.evaluate(_candidate(window, boundaries=STEPS)) is expected


def test__keep_behind__time_axis__never_expires() -> None:
    # Arrange
    policy = KeepBehind(distance=1)

    # Act / Assert
    assert policy.evaluate(_candidate(_month(2020, 1))) is False


def test__keep_behind__no_window__never_expires() -> None:
    # Arrange / Act / Assert
    assert KeepBehind(distance=1).evaluate(_candidate(None, boundaries=STEPS)) is False


def test__keep_behind__non_positive_distance__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        KeepBehind(distance=0)


def test__keep_behind__describe__names_the_distance() -> None:
    # Arrange / Act / Assert
    assert KeepBehind(distance=1_000_000).describe() == "keep within 1000000 of the cursor"


def test__expire_if__evaluate_and_metadata__delegate_to_the_predicate() -> None:
    # Arrange
    sql = SqlPredicate(sql=SQL)
    policy = ExpireIf(when=AllOf(members=(sql, RowsAbove(rows=0))))
    expired = _candidate(_month(2026, 1), facts=_facts(row_estimate=1, predicates={sql.id: True}))
    kept = _candidate(_month(2026, 1), facts=_facts(row_estimate=1, predicates={sql.id: False}))

    # Act / Assert
    assert policy.evaluate(expired) is True
    assert policy.evaluate(kept) is False
    assert policy.required_facts == frozenset({FactKind.ROWS})
    assert policy.sql_predicates == (sql,)
    assert policy.describe() == f"expire when (SQL: {SQL} AND rows > 0)"


def test__retention_policy__combinator_at_the_top_level__accepted() -> None:
    # Arrange
    retention = AllOf(members=(WindowAgeAbove(age=timedelta(days=30)), Not(member=RowsAbove(rows=0))))

    # Act
    policy = LifecyclePolicy(retention=retention)

    # Assert
    assert policy.retention == retention
    assert policy.retention.evaluate(_candidate(_month(2026, 1), facts=_facts(row_estimate=0))) is True
    assert policy.retention.evaluate(_candidate(_month(2026, 1), facts=_facts(row_estimate=3))) is False


# ── Detach and drop ─────────────────────────────────────────────────────────────


def test__drop_after__negative_grace__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="grace must not be negative"):
        DropAfter(grace=timedelta(days=-1))


def test__drop_after__defaults__zero_grace_and_no_condition() -> None:
    # Arrange / Act
    policy = DropAfter()

    # Assert
    assert policy.grace == timedelta(0)
    assert policy.when is None
    assert policy.required_facts == frozenset()
    assert policy.sql_predicates == ()


@pytest.mark.parametrize(
    ("detached_at", "expected"),
    [
        (None, True),
        (NOW - timedelta(days=8), True),
        (NOW - timedelta(days=7), True),
        (NOW - timedelta(days=6, hours=23), False),
    ],
    ids=["unknown-instant", "past", "exactly", "pending"],
)
def test__drop_after__grace_elapsed__unknown_instant_is_eligible_and_known_ones_wait(
    detached_at: datetime | None, expected: bool
) -> None:
    # Arrange
    policy = DropAfter(grace=timedelta(days=7))

    # Act / Assert
    assert policy.grace_elapsed(detached_at, NOW) is expected


def test__drop_after__zero_grace__elapsed_the_instant_it_was_detached() -> None:
    # Arrange / Act / Assert
    assert DropAfter().grace_elapsed(NOW, NOW) is True


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (DropAfter(), "drop immediately"),
        (DropAfter(grace=timedelta(days=7)), "drop after 7 days, 0:00:00"),
        (DropAfter(when=SizeAbove(bytes=5)), "drop immediately when size > 5 bytes"),
        (DropAfter(grace=timedelta(hours=1), when=RowsAbove(rows=0)), "drop after 1:00:00 when rows > 0"),
    ],
    ids=["immediate", "grace", "condition", "grace-and-condition"],
)
def test__drop_after__describe__renders_grace_and_condition(policy: DropAfter, expected: str) -> None:
    # Arrange / Act / Assert
    assert policy.describe() == expected


def test__drop_after__with_a_condition__declares_its_facts_and_sql() -> None:
    # Arrange
    sql = SqlPredicate(sql=SQL)
    policy = DropAfter(when=AnyOf(members=(SizeAbove(bytes=1), sql)))

    # Act / Assert
    assert policy.required_facts == frozenset({FactKind.SIZE})
    assert policy.sql_predicates == (sql,)


def test__drop_never__metadata__needs_nothing_and_says_so() -> None:
    # Arrange / Act
    policy = DropNever()

    # Assert
    assert policy.describe() == "never drop"
    assert policy.required_facts == frozenset()
    assert policy.sql_predicates == ()


def test__detach_mode__values__spell_the_three_modes() -> None:
    # Arrange / Act / Assert
    assert [mode.value for mode in DetachMode] == ["auto", "concurrent", "blocking"]


# ── LifecyclePolicy ─────────────────────────────────────────────────────────────


def test__lifecycle_policy__defaults__create_ahead_keep_newest_auto_detach_immediate_drop() -> None:
    # Arrange / Act
    policy = LifecyclePolicy()

    # Assert
    assert policy.creation == CreateAhead(count=DEFAULT_CREATE_AHEAD_COUNT)
    assert policy.retention == KeepNewest(count=DEFAULT_RETENTION_COUNT)
    assert policy.detach is DetachMode.AUTO
    assert policy.drop == DropAfter()
    assert policy.required_facts == frozenset()
    assert policy.sql_predicates == ()
    assert policy.needs_facts is False


def test__lifecycle_policy__required_facts__union_over_creation_retention_and_drop() -> None:
    # Arrange
    policy = LifecyclePolicy(
        creation=CreateNextIf(when=SizeAbove(bytes=1)),
        retention=ExpireIf(when=RowsAbove(rows=0)),
        drop=DropAfter(when=Callback(fn=lambda _: True, facts=frozenset({FactKind.SIZE}))),
    )

    # Act / Assert
    assert policy.required_facts == frozenset({FactKind.SIZE, FactKind.ROWS})
    assert policy.needs_facts is True


def test__lifecycle_policy__sql_predicates__creation_then_retention_then_drop() -> None:
    # Arrange
    creation_sql = SqlPredicate(sql="SELECT 1 = 1 FROM {partition}")
    retention_sql = SqlPredicate(sql="SELECT 2 = 2 FROM {partition}")
    drop_sql = SqlPredicate(sql="SELECT 3 = 3 FROM {partition}")
    policy = LifecyclePolicy(
        creation=CreateNextIf(when=creation_sql),
        retention=ExpireIf(when=retention_sql),
        drop=DropAfter(when=drop_sql),
    )

    # Act / Assert
    assert policy.sql_predicates == (creation_sql, retention_sql, drop_sql)
    assert policy.required_facts == frozenset()
    assert policy.needs_facts is True


def test__lifecycle_policy__json_round_trip__with_combinators_survives_intact() -> None:
    # Arrange
    policy = LifecyclePolicy(
        creation=CreateUntil(position=5_000_000),
        retention=AllOf(
            members=(
                WindowAgeAbove(age=timedelta(days=90)),
                Not(member=RowsAbove(rows=0)),
                AnyOf(members=(SizeAbove(bytes=10), SqlPredicate(sql=SQL))),
            )
        ),
        detach=DetachMode.BLOCKING,
        drop=DropAfter(grace=timedelta(days=7), when=Not(member=SizeAbove(bytes=1))),
    )

    # Act
    dumped = policy.model_dump(mode="json")
    restored = LifecyclePolicy.model_validate(dumped)

    # Assert
    assert restored == policy
    assert dumped["retention"]["kind"] == "all_of"
    assert dumped["retention"]["members"][0] == {"kind": "window_age_above", "age": "P90D"}
    assert dumped["drop"] == {
        "kind": "drop_after",
        "grace": "P7D",
        "when": {"kind": "not", "member": {"kind": "size_above", "bytes": 1}},
    }


def test__lifecycle_policy__json_round_trip__keeps_a_time_horizon_usable() -> None:
    # Arrange
    policy = LifecyclePolicy(creation=CreateUntil(position=datetime(2026, 10, 15, tzinfo=UTC)))

    # Act
    restored = LifecyclePolicy.model_validate(policy.model_dump(mode="json"))

    # Assert
    assert restored.creation.desired_windows(CURSOR_MONTH, MONTHS, None) == [
        _month(2026, 8),
        _month(2026, 9),
        _month(2026, 10),
    ]


def test__lifecycle_policy__json_dump__leaves_the_callback_out() -> None:
    # Arrange
    policy = LifecyclePolicy(retention=AnyOf(members=(Callback(fn=lambda _: True, label="rule"), RowsAbove(rows=0))))

    # Act
    dumped = policy.model_dump(mode="json")

    # Assert: the callable cannot be serialized, so its rule is dumped without it.
    assert dumped["retention"]["members"][0] == {"kind": "callback", "facts": [], "label": "rule"}
    with pytest.raises(ValidationError):
        LifecyclePolicy.model_validate(dumped)


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"kind": "keep_for", "age": "P90D"}, KeepFor(age=timedelta(days=90))),
        ({"kind": "keep_newest", "count": 4}, KeepNewest(count=4)),
        ({"kind": "keep_behind", "distance": 10}, KeepBehind(distance=10)),
        (
            {"kind": "expire_if", "when": {"kind": "rows_above", "rows": 0}},
            ExpireIf(when=RowsAbove(rows=0)),
        ),
        (
            {"kind": "not", "member": {"kind": "sql", "sql": SQL}},
            Not(member=SqlPredicate(sql=SQL)),
        ),
    ],
    ids=["keep-for", "keep-newest", "keep-behind", "expire-if", "not-sql"],
)
def test__retention_policy__parsed_from_a_dict__discriminated_on_kind(
    data: dict[str, Any], expected: KeepFor | KeepNewest | KeepBehind | ExpireIf | Not
) -> None:
    # Arrange / Act
    policy = LifecyclePolicy.model_validate({"retention": data})

    # Assert
    assert policy.retention == expected


def test__lifecycle_policy__parsed_from_a_dict__every_section_discriminated() -> None:
    # Arrange
    data = {
        "creation": {"kind": "create_next_if", "when": {"kind": "size_above", "bytes": 1024}},
        "retention": {"kind": "keep_for", "age": "P90D"},
        "detach": "concurrent",
        "drop": {"kind": "drop_never"},
    }

    # Act
    policy = LifecyclePolicy.model_validate(data)

    # Assert
    assert policy == LifecyclePolicy(
        creation=CreateNextIf(when=SizeAbove(bytes=1024)),
        retention=KeepFor(age=timedelta(days=90)),
        detach=DetachMode.CONCURRENT,
        drop=DropNever(),
    )


def test__predicate_union__unknown_kind__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="union_tag_invalid"):
        TypeAdapter(Predicate).validate_python({"kind": "bogus"})


def test__retention_policy_union__creation_rule__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="union_tag_invalid"):
        TypeAdapter(RetentionPolicy).validate_python({"kind": "create_ahead", "count": 3})


def test__predicate_base__evaluate_and_describe__left_to_subclasses() -> None:
    # Arrange
    base = PredicateBase()

    # Act / Assert
    with pytest.raises(NotImplementedError):
        base.evaluate(_candidate(_month(2026, 8)))
    with pytest.raises(NotImplementedError):
        base.describe()
    assert base.required_facts == frozenset()
    assert base.sql_predicates == ()


def test__predicates__frozen__cannot_be_mutated() -> None:
    # Arrange
    predicate = SizeAbove(bytes=1)

    # Act / Assert
    with pytest.raises(ValidationError):
        predicate.bytes = 2  # type: ignore[misc]


# ── Unreferenced ────────────────────────────────────────────────────────────────


def test__unreferenced__asks_for_the_references_fact() -> None:
    assert Unreferenced().required_facts == {FactKind.REFERENCES}
    assert Unreferenced().sql_predicates == ()
    assert Unreferenced().describe() == "no row of another table references it"


@pytest.mark.parametrize(("referenced", "expected"), [(False, True), (True, False), (None, False)])
def test__unreferenced__true_only_when_measured_unreferenced(referenced: bool | None, expected: bool) -> None:
    candidate = _candidate(_month(2026, 5), facts=_facts(referenced=referenced))

    assert Unreferenced().evaluate(candidate) is expected


def test__unreferenced__combines_with_the_calendar_rules() -> None:
    rule = ExpireIf(when=AllOf(members=(KeepNewest(count=1), Unreferenced())))
    old_and_free = _candidate(_month(2026, 5), facts=_facts(referenced=False))
    old_but_held = _candidate(_month(2026, 5), facts=_facts(referenced=True))

    assert rule.required_facts == {FactKind.REFERENCES}
    assert rule.evaluate(old_and_free)
    assert not rule.evaluate(old_but_held)


def test__unreferenced__survives_json() -> None:
    policy = LifecyclePolicy(retention=ExpireIf(when=Unreferenced()))

    assert LifecyclePolicy.model_validate(policy.model_dump(mode="json")) == policy


# ── one positional argument per rule ────────────────────────────────────────────


@pytest.mark.parametrize(
    ("positional", "keyword"),
    [
        (CreateAhead(3), CreateAhead(count=3)),
        (CreateUntil(5), CreateUntil(position=5)),
        (CreateNextIf(RowsAbove(1)), CreateNextIf(when=RowsAbove(rows=1))),
        (KeepNewest(4), KeepNewest(count=4)),
        (KeepFor(timedelta(days=2)), KeepFor(age=timedelta(days=2))),
        (KeepBehind(10), KeepBehind(distance=10)),
        (ExpireIf(SizeAbove(9)), ExpireIf(when=SizeAbove(bytes=9))),
        (WindowAgeAbove(timedelta(hours=1)), WindowAgeAbove(age=timedelta(hours=1))),
        (SqlPredicate(SQL), SqlPredicate(sql=SQL)),
        (AllOf((KeepNewest(1), RowsAbove(0))), AllOf(members=(KeepNewest(count=1), RowsAbove(rows=0)))),
        (AnyOf((KeepNewest(1),)), AnyOf(members=(KeepNewest(count=1),))),
        (Not(RowsAbove(2)), Not(member=RowsAbove(rows=2))),
        (DropAfter(timedelta(days=7)), DropAfter(grace=timedelta(days=7))),
    ],
)
def test__rules__one_positional_argument__same_as_the_keyword(positional: Any, keyword: Any) -> None:
    assert positional == keyword


def test__callback__function_positionally() -> None:
    def always(candidate: Candidate) -> bool:
        return True

    assert Callback(always).fn is always


def test__rules__two_positional_arguments__refused() -> None:
    with pytest.raises(TypeError, match="at most one positional argument"):
        CreateAhead(3, 4)  # type: ignore[call-arg]


def test__rules__value_given_twice__refused() -> None:
    with pytest.raises(TypeError, match="both positionally and by keyword"):
        KeepNewest(3, count=3)


def test__rules__without_a_defining_value__refuse_a_positional_argument() -> None:
    with pytest.raises(TypeError, match="at most one positional argument"):
        Unreferenced(True)  # type: ignore[call-arg]
