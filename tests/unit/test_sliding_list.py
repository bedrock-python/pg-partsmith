"""The sliding LIST progression: ``IntegerSequence``, ``ListPartitioning(sequence=...)`` and the planner over them."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pg_partsmith.boundaries import CursorSource, IntegerSequence, RangeBoundaries, Window, parse_boundaries
from pg_partsmith.entities import PartitionStrategy, TablePartitionConfig
from pg_partsmith.lifecycle import (
    CreateAhead,
    CreateNextIf,
    CreateUntil,
    DropAfter,
    DropNever,
    ExpireIf,
    KeepFor,
    KeepNewest,
    LifecyclePolicy,
    RowsAbove,
    SqlPredicate,
)
from pg_partsmith.plan import AttachPartition, FindingReason, MaintenancePlan, Reason
from pg_partsmith.planner import PlanMode, PlanningContext, fact_targets, plan_maintenance
from pg_partsmith.scheme import HashPartitioning, LevelKind, ListGroup, ListPartitioning, RangePartitioning
from pg_partsmith.topology import (
    ActualTree,
    DefaultBounds,
    DetachedPartition,
    ListBounds,
    PartitionFacts,
    PartitionNode,
    PartitionType,
    RangeBounds,
    RelationKind,
)

NOW = datetime(2026, 8, 28, tzinfo=UTC)
ROOT = "public.ci_builds"
ROTATE = SqlPredicate(sql="SELECT count(*) >= 3 FROM {partition}")


# ── builders ────────────────────────────────────────────────────────────────────


def _sequence(**overrides: Any) -> IntegerSequence:
    return IntegerSequence(start=100, **overrides)


def _scheme(child: HashPartitioning | None = None, **overrides: Any) -> ListPartitioning:
    return ListPartitioning(key="partition_id", sequence=_sequence(**overrides), child=child)


def _policy(
    *,
    creation: Any = None,
    retention: Any = None,
    drop: DropAfter | DropNever | None = None,
) -> LifecyclePolicy:
    return LifecyclePolicy(
        creation=creation if creation is not None else CreateNextIf(when=RowsAbove(rows=1000)),
        retention=retention if retention is not None else KeepNewest(count=3),
        drop=drop if drop is not None else DropAfter(),
    )


def _config(scheme: ListPartitioning | None = None, lifecycle: LifecyclePolicy | None = None) -> TablePartitionConfig:
    return TablePartitionConfig(
        schema="public",
        table_name="ci_builds",
        scheme=scheme if scheme is not None else _scheme(),
        lifecycle=lifecycle if lifecycle is not None else _policy(),
    )


def _root(*children: PartitionNode, partition_type: PartitionType = PartitionType.LIST) -> PartitionNode:
    return PartitionNode(
        name=ROOT, partition_type=partition_type, partition_columns=("partition_id",), children=children
    )


def _value(
    value: int,
    *,
    rows: int | None = None,
    facts: PartitionFacts | None = None,
    name: str | None = None,
    **overrides: Any,
) -> PartitionNode:
    if rows is not None:
        facts = PartitionFacts(row_estimate=rows)
    return PartitionNode(
        name=name or f"{ROOT}__{value}",
        parent_name=ROOT,
        level=1,
        oid=overrides.pop("oid", value),
        bounds=ListBounds(values=(str(value),)),
        facts=facts,
        **overrides,
    )


def _values(*values: int) -> tuple[PartitionNode, ...]:
    return tuple(_value(value) for value in values)


def _orphan(value: int, *, detached_at: datetime | None = None, **overrides: Any) -> DetachedPartition:
    return DetachedPartition(
        name=f"{ROOT}__{value}",
        oid=overrides.pop("oid", 1000 + value),
        parent_name=ROOT,
        detached_at=detached_at,
        **overrides,
    )


def _plan(
    config: TablePartitionConfig,
    root: PartitionNode,
    *,
    orphans: tuple[DetachedPartition, ...] = (),
    mode: PlanMode = PlanMode.MAINTAIN,
    cursors: dict[str, Any] | None = None,
    windows: dict[str, tuple[Window, ...]] | None = None,
) -> MaintenancePlan:
    context = PlanningContext(now=NOW, mode=mode, cursors=cursors or {}, explicit_windows=windows or {})
    return plan_maintenance(config, ActualTree(root=root, orphans=orphans), context)


def _targets(operations: tuple[Any, ...]) -> list[str]:
    return [op.target for op in operations]


def _reasons(plan: MaintenancePlan) -> list[FindingReason]:
    return [finding.reason for finding in plan.findings]


def _window(value: int) -> Window:
    return Window(start=value, end=value + 1)


# ── IntegerSequence ─────────────────────────────────────────────────────────────


def test__integer_sequence__is_a_progression_rule() -> None:
    assert isinstance(_sequence(), RangeBoundaries)
    assert _sequence().cursor_source is CursorSource.NEWEST_MEMBER


def test__integer_sequence__window_at__one_value_per_window_starting_at_start() -> None:
    sequence = _sequence()

    assert sequence.window_at(None) == _window(100)
    assert sequence.window_at(7) == _window(7)
    assert sequence.window_at("42") == _window(42)


def test__integer_sequence__shift_literals_and_value() -> None:
    sequence = _sequence()

    assert sequence.shift(_window(100), 2) == _window(102)
    assert sequence.shift(_window(100), -1) == _window(99)
    assert sequence.literals(_window(100)) == ("100", "101")
    assert sequence.value_of(_window(100)) == 100
    assert sequence.describe(_window(100)) == "value 100"


@pytest.mark.parametrize(("literal", "expected"), [("42", 42), (" -7 ", -7), ("MAXVALUE", None), ("abc", None)])
def test__integer_sequence__decode(literal: str, expected: int | None) -> None:
    assert _sequence().decode(literal) == expected


@pytest.mark.parametrize("value", [100, 0, -3])
def test__integer_sequence__child_name__round_trips_through_parse(value: int) -> None:
    sequence = _sequence()

    name = sequence.child_name("ci_builds", _window(value))

    assert name == f"ci_builds__{'m3' if value < 0 else value}"
    assert sequence.parse_child_name(name) == _window(value)


def test__integer_sequence__custom_suffix__names_and_parses() -> None:
    sequence = _sequence(name_suffix="_p{value}")

    assert sequence.child_name("ci_builds", _window(101)) == "ci_builds_p101"
    assert sequence.parse_child_name("ci_builds_p101") == _window(101)
    assert sequence.parse_child_name("ci_builds__101") is None
    assert sequence.own_name_budget() == len("_p") + 1 + 19


@pytest.mark.parametrize("suffix", ["__{start}", "__{value}X", "{value}-"])
def test__integer_sequence__bad_suffix__refused(suffix: str) -> None:
    with pytest.raises(ValueError, match="name_suffix"):
        _sequence(name_suffix=suffix)


def test__integer_sequence__clock_cursor__refused() -> None:
    with pytest.raises(ValueError, match="cannot read its cursor from the clock"):
        _sequence(cursor_source=CursorSource.CLOCK)


def test__integer_sequence__serialized_form__parses_back() -> None:
    sequence = _sequence(name_suffix="_p{value}", cursor_source=CursorSource.MAX_KEY)

    parsed = parse_boundaries(sequence.model_dump(mode="json"))

    assert parsed == sequence
    assert parse_boundaries({"kind": "sequence"}) == IntegerSequence()


def test__integer_sequence__foreign_kind__refused() -> None:
    with pytest.raises(ValueError, match="kind must be 'sequence'"):
        IntegerSequence(kind="integer")


# ── ListPartitioning over a sequence ────────────────────────────────────────────


def test__list_partitioning__sequence__is_a_progression_level() -> None:
    level = _scheme()

    assert level.kind is LevelKind.PROGRESSION
    assert level.progression == _sequence()
    assert level.method is PartitionType.LIST


def test__list_partitioning__groups__is_a_set_level() -> None:
    level = ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("eu",)),))

    assert level.kind is LevelKind.SET
    assert level.progression is None


def test__list_partitioning__groups_and_sequence__refused() -> None:
    with pytest.raises(ValueError, match="either groups or a sequence"):
        ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("eu",)),), sequence=_sequence())


def test__list_partitioning__default_with_sequence__refused() -> None:
    with pytest.raises(ValueError, match="no DEFAULT partition"):
        ListPartitioning(key="partition_id", sequence=_sequence(), include_default=True)


def test__list_partitioning__neither_groups_nor_sequence__refused() -> None:
    with pytest.raises(ValueError, match="at least one group, or a sequence"):
        ListPartitioning(key="partition_id")


def test__list_partitioning__bounds_for__single_value_list_bound() -> None:
    assert _scheme().bounds_for(_window(101)) == ListBounds(values=("101",))


@pytest.mark.parametrize(
    ("bounds", "expected"),
    [
        (ListBounds(values=("101",)), _window(101)),
        (ListBounds(values=("101", "102")), None),
        (ListBounds(values=("101",), includes_null=True), None),
        (ListBounds(values=("eu",)), None),
        (RangeBounds(from_value="1", to_value="2"), None),
        (DefaultBounds(), None),
    ],
)
def test__list_partitioning__window_of(bounds: Any, expected: Window | None) -> None:
    assert _scheme().window_of(bounds) == expected


def test__list_partitioning__grouped_level__has_no_windows() -> None:
    level = ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("eu",)),))

    assert level.window_of(ListBounds(values=("eu",))) is None


def test__list_partitioning__name_budget__sized_for_the_widest_value() -> None:
    level = _scheme(child=HashPartitioning(key="tenant_id", modulus=4))

    assert level.own_name_budget() == _sequence().own_name_budget()
    assert level.name_length_budget() == _sequence().own_name_budget() + len("__h0")


def test__list_partitioning__sequence__survives_json() -> None:
    level = _scheme(child=HashPartitioning(key="tenant_id", modulus=4), name_suffix="_p{value}")

    assert ListPartitioning.model_validate(level.model_dump(mode="json", by_alias=True)) == level


def test__range_partitioning__bounds_for_and_window_of__round_trip() -> None:
    level = RangePartitioning(key="msg_id", boundaries={"kind": "integer", "step": 1000})

    bounds = level.bounds_for(Window(start=1000, end=2000))

    assert bounds == RangeBounds(from_value="1000", to_value="2000")
    assert level.window_of(bounds) == Window(start=1000, end=2000)
    assert level.window_of(RangeBounds(from_value="MINVALUE", to_value="2000")) is None
    assert level.window_of(ListBounds(values=("1",))) is None
    assert level.progression == level.range_boundaries


# ── TablePartitionConfig ────────────────────────────────────────────────────────


def test__config__sliding_list__is_a_progression_root() -> None:
    config = _config()

    assert config.is_progression_root
    assert config.has_progression_level
    assert config.partition_type is PartitionType.LIST
    assert config.partition_strategy is PartitionStrategy.VALUE_BASED
    assert config.partition_column == "partition_id"


def test__config__sliding_list_with_create_ahead__refused() -> None:
    with pytest.raises(ValueError, match="CreateAhead would open another partition on every run"):
        _config(lifecycle=LifecyclePolicy(creation=CreateAhead(count=2)))


@pytest.mark.parametrize("creation", [CreateNextIf(when=ROTATE), CreateUntil(position=105)])
def test__config__sliding_list__state_driven_and_bounded_creation_accepted(creation: Any) -> None:
    config = _config(lifecycle=LifecyclePolicy(creation=creation))

    assert config.lifecycle.creation == creation


def test__config__sequence_read_from_the_data__may_create_ahead() -> None:
    config = _config(
        scheme=_scheme(cursor_source=CursorSource.MAX_KEY), lifecycle=LifecyclePolicy(creation=CreateAhead(count=2))
    )

    assert config.create_ahead_count == 2


def test__config__sequence_below_a_range_level__is_found() -> None:
    scheme = RangePartitioning(
        key="created_at",
        boundaries={"kind": "time", "granularity": "month"},
        child=ListPartitioning(key="partition_id", sequence=_sequence()),
    )
    config = TablePartitionConfig(table_name="ci_builds", scheme=scheme, lifecycle=_policy())

    assert config.has_progression_level
    assert [level.kind for level in config.levels] == [LevelKind.PROGRESSION, LevelKind.PROGRESSION]


# ── Planner: creation ───────────────────────────────────────────────────────────


def test__plan__empty_level__opens_the_first_value() -> None:
    plan = _plan(_config(), _root())

    (create,) = plan.creates
    assert create.target == f"{ROOT}__100"
    assert create.bounds == ListBounds(values=("100",))
    assert create.key_columns == ("partition_id",)
    assert create.partition_by is None
    assert create.lifecycle_unit
    assert create.reason is Reason.CREATE_NEXT
    assert create.detail == "value 100 under 'create next when rows > 1000'"
    assert plan.findings == ()


def test__plan__newest_satisfies_the_rule__opens_the_next_value() -> None:
    root = _root(_value(100, rows=5000), _value(101, rows=5000))

    plan = _plan(_config(), root)

    assert _targets(plan.creates) == [f"{ROOT}__102"]
    assert plan.creates[0].bounds == ListBounds(values=("102",))


def test__plan__newest_does_not_satisfy_the_rule__nothing_to_do() -> None:
    root = _root(_value(100, rows=5000), _value(101, rows=10))

    plan = _plan(_config(), root)

    assert plan.is_noop
    assert plan.findings == ()


def test__plan__the_newest_is_the_cursor__whatever_the_gaps_behind_it() -> None:
    # 100 and 104 exist; the active partition is 104, so only 105 may follow.
    root = _root(_value(100, rows=9), _value(104, rows=5000))

    plan = _plan(_config(), root)

    assert _targets(plan.creates) == [f"{ROOT}__105"]


def test__plan__create_until__fills_every_value_up_to_the_horizon() -> None:
    config = _config(lifecycle=_policy(creation=CreateUntil(position=104)))

    plan = _plan(config, _root(*_values(100, 101)))

    assert _targets(plan.creates) == [f"{ROOT}__102", f"{ROOT}__103", f"{ROOT}__104"]
    assert all(op.reason is Reason.CREATE_UNTIL for op in plan.creates)


def test__plan__create_until_behind_the_newest__nothing_ahead() -> None:
    config = _config(lifecycle=_policy(creation=CreateUntil(position=101)))

    plan = _plan(config, _root(*_values(100, 101, 102)))

    assert plan.creates == ()


def test__plan__cursor_from_the_data__create_ahead_counts_from_the_written_value() -> None:
    config = _config(
        scheme=_scheme(cursor_source=CursorSource.MAX_KEY), lifecycle=LifecyclePolicy(creation=CreateAhead(count=2))
    )

    plan = _plan(config, _root(*_values(100, 101)), cursors={"partition_id": 101})

    assert _targets(plan.creates) == [f"{ROOT}__102"]


def test__plan__cursor_from_the_data_unknown__starts_the_sequence() -> None:
    config = _config(
        scheme=_scheme(cursor_source=CursorSource.MAX_KEY), lifecycle=LifecyclePolicy(creation=CreateAhead(count=1))
    )

    plan = _plan(config, _root())

    assert _targets(plan.creates) == [f"{ROOT}__100"]


def test__plan__reconcile_mode__creates_nothing() -> None:
    plan = _plan(_config(), _root(_value(100, rows=5000)), mode=PlanMode.RECONCILE)

    assert plan.is_noop


def test__plan__explicit_window__creates_the_named_value_only() -> None:
    plan = _plan(
        _config(), _root(_value(100, rows=5000)), mode=PlanMode.EXPLICIT, windows={"partition_id": (_window(107),)}
    )

    assert _targets(plan.creates) == [f"{ROOT}__107"]
    assert plan.creates[0].reason is Reason.EXPLICIT


def test__plan__nested_scheme__new_value_carries_its_subtree() -> None:
    config = _config(scheme=_scheme(child=HashPartitioning(key="tenant_id", modulus=2)))

    plan = _plan(config, _root())

    (create,) = plan.creates
    assert create.partition_by is not None
    assert create.partition_by.method is PartitionType.HASH
    assert _targets(create.children) == [f"{ROOT}__100__h0", f"{ROOT}__100__h1"]


# ── Planner: retention and orphans ──────────────────────────────────────────────


def test__plan__keep_newest__expires_values_behind_the_cutoff() -> None:
    root = _root(*_values(100, 101, 102, 103), _value(104, rows=10))

    plan = _plan(_config(), root)

    assert _targets(plan.detaches) == [f"{ROOT}__100", f"{ROOT}__101"]
    assert _targets(plan.drops) == [f"{ROOT}__100", f"{ROOT}__101"]
    assert plan.detaches[0].bounds == ListBounds(values=("100",))
    assert plan.detaches[0].detail == "value 100 expired under 'keep newest 3'"
    assert all(op.follows_detach for op in plan.drops)


def test__plan__keep_for__never_expires_a_value() -> None:
    config = _config(lifecycle=_policy(retention=KeepFor(age=timedelta(days=1))))

    plan = _plan(config, _root(*_values(100, 101, 102, 103, 104)))

    assert plan.detaches == ()


def test__plan__expire_if_sql__reads_the_answer_measured_for_the_value() -> None:
    done = SqlPredicate(sql="SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'running')")
    config = _config(lifecycle=_policy(retention=ExpireIf(when=done)))
    root = _root(
        _value(100, facts=PartitionFacts(predicates={done.id: True})),
        _value(101, facts=PartitionFacts(predicates={done.id: False})),
        _value(102, rows=1),
    )

    plan = _plan(config, root)

    assert _targets(plan.detaches) == [f"{ROOT}__100"]


def test__plan__orphan_inside_retention__reattached_with_its_value() -> None:
    root = _root(*_values(102, 104))

    plan = _plan(_config(), root, orphans=(_orphan(103, detached_at=NOW - timedelta(days=1)),))

    (attach,) = plan.attaches
    assert attach == AttachPartition(
        target=f"{ROOT}__103",
        oid=1103,
        parent_name=ROOT,
        bounds=ListBounds(values=("103",)),
        key_columns=("partition_id",),
        partition_by=None,
        reason=Reason.REATTACH,
        detail="detached partition covers value 103, which is wanted again",
    )
    assert plan.drops == ()


def test__plan__orphan_behind_retention__dropped_after_its_grace() -> None:
    root = _root(*_values(102, 103, 104))

    plan = _plan(_config(), root, orphans=(_orphan(100, detached_at=NOW - timedelta(days=30)),))

    assert plan.attaches == ()
    assert _targets(plan.drops) == [f"{ROOT}__100"]
    assert plan.drops[0].reason is Reason.GRACE_ELAPSED


def test__plan__orphan_named_outside_the_sequence__left_alone() -> None:
    orphan = DetachedPartition(name=f"{ROOT}_archive", oid=7, parent_name=ROOT, detached_at=NOW - timedelta(days=30))

    plan = _plan(_config(), _root(*_values(102, 103, 104)), orphans=(orphan,))

    assert plan.attaches == ()
    assert _targets(plan.drops) == [f"{ROOT}_archive"]


# ── Planner: what it refuses to touch ───────────────────────────────────────────


def test__plan__multi_value_partition__unmanaged_and_never_the_cursor() -> None:
    legacy = PartitionNode(
        name=f"{ROOT}_legacy",
        parent_name=ROOT,
        level=1,
        oid=5,
        bounds=ListBounds(values=("1", "2"), includes_null=True),
    )
    root = _root(legacy, _value(100, rows=5000))

    plan = _plan(_config(), root)

    assert _reasons(plan) == [FindingReason.UNMANAGED_PARTITION]
    assert "(1, 2, NULL)" in plan.findings[0].detail
    assert _targets(plan.creates) == [f"{ROOT}__101"]
    assert plan.detaches == ()


def test__plan__wanted_value_held_by_a_multi_value_partition__reported_not_created() -> None:
    legacy = PartitionNode(
        name=f"{ROOT}_legacy", parent_name=ROOT, level=1, oid=5, bounds=ListBounds(values=("100", "101"))
    )
    config = _config(lifecycle=_policy(creation=CreateUntil(position=101)))

    plan = _plan(config, _root(legacy))

    assert plan.creates == ()
    assert _reasons(plan) == [
        FindingReason.UNMANAGED_PARTITION,
        FindingReason.RANGE_OVERLAP,
        FindingReason.RANGE_OVERLAP,
    ]
    assert "needs a partition for value 100" in plan.findings[1].detail


def test__plan__non_integer_value_partition__unmanaged() -> None:
    other = PartitionNode(name=f"{ROOT}_eu", parent_name=ROOT, level=1, oid=5, bounds=ListBounds(values=("eu",)))

    plan = _plan(_config(), _root(other, _value(100, rows=1)))

    assert _reasons(plan) == [FindingReason.UNMANAGED_PARTITION]
    assert plan.is_noop


def test__plan__default_partition__ignored_by_the_progression() -> None:
    default = PartitionNode(name=f"{ROOT}_default", parent_name=ROOT, level=1, oid=5, bounds=DefaultBounds())

    plan = _plan(_config(), _root(default, _value(100, rows=5000)))

    assert _targets(plan.creates) == [f"{ROOT}__101"]
    assert plan.findings == ()


def test__plan__foreign_single_value_partition__satisfies_its_value_but_is_never_touched() -> None:
    config = _config(lifecycle=_policy(creation=CreateUntil(position=101)))
    foreign = _value(101, relkind=RelationKind.FOREIGN)

    plan = _plan(config, _root(_value(100, rows=1), foreign))

    assert plan.creates == ()
    assert plan.detaches == ()
    assert _reasons(plan) == [FindingReason.FOREIGN_PARTITION]


def test__plan__pending_detach__reported_and_skipped() -> None:
    pending = _value(100, detach_pending=True)

    plan = _plan(_config(), _root(pending, _value(101, rows=1)))

    assert _reasons(plan) == [FindingReason.DETACH_PENDING]
    assert plan.is_noop


def test__plan__root_partitioned_otherwise__strategy_mismatch() -> None:
    plan = _plan(_config(), _root(partition_type=PartitionType.RANGE))

    assert _reasons(plan) == [FindingReason.STRATEGY_MISMATCH]
    assert plan.is_noop


def test__fact_targets__sequence_members_and_orphans_are_candidates() -> None:
    tree = ActualTree(root=_root(*_values(100, 101)), orphans=(_orphan(99),))

    assert fact_targets(_config(), tree) == (f"{ROOT}__100", f"{ROOT}__101", f"{ROOT}__99")
