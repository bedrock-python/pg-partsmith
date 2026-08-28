"""Convergence rules of the pure planner, exercised without a database.

Every test builds the actual tree by hand, plans against an explicit clock,
and pins down both what the plan does and what it deliberately refuses to
touch. The hash and list rules are the ones ``test_subpartition_plan.py``
pinned before the planner became one function over the whole tree.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pg_partsmith.boundaries import NumericBoundaries, TimeBoundaries, Window
from pg_partsmith.constants import MAX_HASH_KEYSPACE_LCM
from pg_partsmith.entities import MaintenanceIssueStep, TablePartitionConfig
from pg_partsmith.lifecycle import (
    AllOf,
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
    RowsAbove,
    SizeAbove,
    SqlPredicate,
    WindowAgeAbove,
)
from pg_partsmith.periods import PartitionGranularity
from pg_partsmith.plan import (
    AttachPartition,
    CreatePartition,
    DetachPartition,
    DropPartition,
    Finding,
    FindingReason,
    MaintenancePlan,
    OperationKind,
    PartitionBy,
    Reason,
    Severity,
)
from pg_partsmith.planner import PlanMode, PlanningContext, fact_targets, plan_maintenance, to_maintenance_issue
from pg_partsmith.scheme import HashPartitioning, ListGroup, ListPartitioning, RangePartitioning
from pg_partsmith.topology import (
    ActualTree,
    DefaultBounds,
    DetachedPartition,
    HashBounds,
    ListBounds,
    PartitionFacts,
    PartitionNode,
    PartitionType,
    RangeBounds,
    RelationKind,
)
from pg_partsmith.utils import qualify, split_qualified_name

NOW = datetime(2026, 8, 28, tzinfo=UTC)
SCHEMA = "public"
ROOT = "public.events"
BRANCH = f"{ROOT}__2026_08"  # the cursor window's partition
MONTHS = TimeBoundaries(granularity=PartitionGranularity.MONTH)
STEPS = NumericBoundaries(step=100_000)
EU = ListGroup(name="eu", values=("de", "fr"))
US = ListGroup(name="us", values=("us",))
SQL = "SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')"


# ── Configuration ───────────────────────────────────────────────────────────────


def _policy(
    *,
    creation: CreateAhead | CreateUntil | CreateNextIf | None = None,
    retention: Any = None,
    drop: DropAfter | DropNever | None = None,
    detach: DetachMode = DetachMode.AUTO,
) -> LifecyclePolicy:
    return LifecyclePolicy(
        creation=creation if creation is not None else CreateAhead(count=1),
        retention=retention if retention is not None else KeepNewest(count=12),
        detach=detach,
        drop=drop if drop is not None else DropAfter(),
    )


def _range_level(
    child: HashPartitioning | ListPartitioning | None = None,
    *,
    key: str = "created_at",
    boundaries: TimeBoundaries | NumericBoundaries = MONTHS,
) -> RangePartitioning:
    return RangePartitioning(key=key, boundaries=boundaries, child=child)


def _hash_level(
    modulus: int = 2,
    child: HashPartitioning | ListPartitioning | RangePartitioning | None = None,
    *,
    key: str = "tenant_id",
    **overrides: Any,
) -> HashPartitioning:
    return HashPartitioning(key=key, modulus=modulus, child=child, **overrides)


def _list_level(
    *,
    groups: tuple[ListGroup, ...] = (EU, US),
    child: HashPartitioning | ListPartitioning | RangePartitioning | None = None,
    **overrides: Any,
) -> ListPartitioning:
    return ListPartitioning(key="region", groups=groups, child=child, **overrides)


def _config(
    scheme: RangePartitioning | HashPartitioning | ListPartitioning | None = None,
    *,
    lifecycle: LifecyclePolicy | None = None,
    table_name: str = "events",
    schema: str | None = SCHEMA,
) -> TablePartitionConfig:
    return TablePartitionConfig(
        schema=schema,
        table_name=table_name,
        scheme=scheme if scheme is not None else _range_level(),
        lifecycle=lifecycle if lifecycle is not None else _policy(),
    )


def _context(**overrides: Any) -> PlanningContext:
    return PlanningContext(now=NOW, **overrides)


# ── Actual trees ────────────────────────────────────────────────────────────────


def _window(year: int, month: int) -> Window:
    return MONTHS.window_at(datetime(year, month, 1, tzinfo=UTC))


def _root(
    *children: PartitionNode,
    name: str = ROOT,
    partition_type: PartitionType | None = PartitionType.RANGE,
    columns: tuple[str, ...] = ("created_at",),
    **overrides: Any,
) -> PartitionNode:
    return PartitionNode(
        name=name,
        partition_type=partition_type,
        partition_columns=columns if partition_type is not None else (),
        children=children,
        **overrides,
    )


def _month(year: int, month: int, *, parent: str = ROOT, **overrides: Any) -> PartitionNode:
    """An attached leaf for one window of the monthly grid, named by the scheme."""
    window = _window(year, month)
    schema, parent_relname = split_qualified_name(parent)
    from_value, to_value = MONTHS.literals(window)
    return PartitionNode(
        name=qualify(schema, MONTHS.child_name(parent_relname, window)),
        parent_name=parent,
        level=1,
        bounds=RangeBounds(from_value=from_value, to_value=to_value),
        **overrides,
    )


def _range_child(name: str, from_value: str, to_value: str, *, parent: str = ROOT, **overrides: Any) -> PartitionNode:
    """An attached RANGE child with arbitrary bounds, under ``parent``."""
    return PartitionNode(
        name=f"{parent}__{name}",
        parent_name=parent,
        level=1,
        bounds=RangeBounds(from_value=from_value, to_value=to_value),
        **overrides,
    )


def _bucket(parent: str, modulus: int, remainder: int, **overrides: Any) -> PartitionNode:
    return PartitionNode(
        name=f"{parent}__h{remainder}",
        parent_name=parent,
        bounds=HashBounds(modulus=modulus, remainder=remainder),
        **overrides,
    )


def _branch(
    *remainders_at_modulus: tuple[int, int],
    name: str = BRANCH,
    partition_type: PartitionType | None = PartitionType.HASH,
    columns: tuple[str, ...] = ("tenant_id",),
    **overrides: Any,
) -> PartitionNode:
    """The cursor window as a branch with the given ``(modulus, remainder)`` buckets."""
    from_value, to_value = MONTHS.literals(_window(2026, 8))
    return PartitionNode(
        name=name,
        parent_name=ROOT,
        level=1,
        bounds=RangeBounds(from_value=from_value, to_value=to_value),
        partition_type=partition_type,
        partition_columns=columns if partition_type is not None else (),
        children=tuple(_bucket(name, modulus, remainder, level=2) for modulus, remainder in remainders_at_modulus),
        **overrides,
    )


def _list_child(parent: str, name: str, *values: str, **overrides: Any) -> PartitionNode:
    return PartitionNode(name=f"{parent}__{name}", parent_name=parent, bounds=ListBounds(values=values), **overrides)


def _list_branch(*children: PartitionNode, name: str = BRANCH, **overrides: Any) -> PartitionNode:
    """The cursor window as a LIST-partitioned branch."""
    return _branch(name=name, partition_type=PartitionType.LIST, columns=("region",), **overrides).model_copy(
        update={"children": children}
    )


def _orphan(
    relname: str,
    *,
    parent: str = ROOT,
    detached_at: datetime | None = None,
    **overrides: Any,
) -> DetachedPartition:
    schema, _ = split_qualified_name(parent)
    return DetachedPartition(name=qualify(schema, relname), parent_name=parent, detached_at=detached_at, **overrides)


def _plan(
    config: TablePartitionConfig,
    root: PartitionNode,
    *,
    orphans: tuple[DetachedPartition, ...] = (),
    context: PlanningContext | None = None,
) -> MaintenancePlan:
    return plan_maintenance(config, ActualTree(root=root, orphans=orphans), context or _context())


def _reasons(plan: MaintenancePlan) -> list[FindingReason]:
    return [finding.reason for finding in plan.findings]


def _targets(operations: tuple[Any, ...]) -> list[str]:
    return [op.target for op in operations]


def _hash_created(plan: MaintenancePlan) -> list[tuple[str, int, int]]:
    created: list[tuple[str, int, int]] = []
    for op in plan.creates:
        assert isinstance(op.bounds, HashBounds)
        created.append((op.target, op.bounds.modulus, op.bounds.remainder))
    return created


# ── RANGE root over time: creation ──────────────────────────────────────────────


def test__plan_maintenance__fresh_table__creates_windows_ahead_in_chronological_order() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=3)))

    # Act
    plan = _plan(config, _root())

    # Assert
    assert _targets(plan.operations) == [f"{ROOT}__2026_08", f"{ROOT}__2026_09", f"{ROOT}__2026_10"]
    assert plan.findings == ()
    assert plan.table_name == ROOT
    assert plan.generated_at == NOW
    first = plan.creates[0]
    assert first == CreatePartition(
        target=f"{ROOT}__2026_08",
        parent_name=ROOT,
        bounds=RangeBounds(from_value="2026-08-01", to_value="2026-09-01"),
        partition_by=None,
        key_columns=("created_at",),
        children=(),
        lifecycle_unit=True,
        counts_as="created",
        reason=Reason.CREATE_AHEAD,
        detail="2026_08 under 'create 3 ahead'",
    )
    assert plan.creates[2].bounds == RangeBounds(from_value="2026-10-01", to_value="2026-11-01")


def test__plan_maintenance__aligned_partitions_exist__not_recreated() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=3)))
    root = _root(_month(2026, 8, oid=1), _month(2026, 9, oid=2))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.operations) == [f"{ROOT}__2026_10"]


def test__plan_maintenance__converged_tree__is_noop_with_no_findings() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=2)))
    root = _root(_month(2026, 7, oid=1), _month(2026, 8, oid=2), _month(2026, 9, oid=3))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__bounds_spelled_as_timestamps__recognised_as_the_same_window() -> None:
    # Arrange: the catalog renders a timestamptz key with a time and an offset.
    root = _root(_range_child("2026_08", "2026-08-01 00:00:00+00", "2026-09-01 00:00:00+00", oid=1))

    # Act
    plan = _plan(_config(), root)

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__create_until_ahead__creates_through_the_horizon() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateUntil(position=datetime(2026, 10, 15, tzinfo=UTC))))

    # Act
    plan = _plan(config, _root())

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_08", f"{ROOT}__2026_09", f"{ROOT}__2026_10"]
    assert {op.reason for op in plan.creates} == {Reason.CREATE_UNTIL}
    assert plan.creates[0].detail == "2026_08 under 'create until 2026-10-15 00:00:00+00:00'"


def test__plan_maintenance__create_until_behind_the_cursor__cursor_window_only() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateUntil(position=datetime(2024, 1, 1, tzinfo=UTC))))

    # Act
    plan = _plan(config, _root())

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_08"]


def test__plan_maintenance__create_next_if_newest_qualifies__creates_the_window_after_it() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateNextIf(when=SizeAbove(bytes=10))))
    root = _root(_month(2026, 8, oid=1, facts=PartitionFacts(size_bytes=20)))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_09"]
    assert plan.creates[0].reason is Reason.CREATE_NEXT
    assert plan.creates[0].detail == "2026_09 under 'create next when size > 10 bytes'"


def test__plan_maintenance__create_next_if_newest_is_ahead_of_the_cursor__extends_from_the_newest() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateNextIf(when=SizeAbove(bytes=10))))
    root = _root(
        _month(2026, 8, oid=1, facts=PartitionFacts(size_bytes=50)),
        _month(2026, 9, oid=2, facts=PartitionFacts(size_bytes=20)),
    )

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_10"]


def test__plan_maintenance__create_next_if_predicate_false__nothing_beyond_the_cursor() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateNextIf(when=SizeAbove(bytes=10))))
    root = _root(_month(2026, 8, oid=1, facts=PartitionFacts(size_bytes=5)))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop


def test__plan_maintenance__create_next_if_on_a_fresh_table__cursor_window_only() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateNextIf(when=SizeAbove(bytes=10))))

    # Act
    plan = _plan(config, _root())

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_08"]


# ── Nested schemes ──────────────────────────────────────────────────────────────


def test__plan_maintenance__range_over_hash__new_window_carries_its_buckets() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root())

    # Assert
    (op,) = plan.creates
    assert op.partition_by == PartitionBy(method=PartitionType.HASH, columns=("tenant_id",))
    assert op.lifecycle_unit is True
    assert op.counts_as == "created"
    assert op.children == (
        CreatePartition(
            target=f"{BRANCH}__h0",
            parent_name=BRANCH,
            bounds=HashBounds(modulus=2, remainder=0),
            partition_by=None,
            key_columns=("tenant_id",),
            children=(),
            lifecycle_unit=False,
            counts_as="subtree",
            reason=Reason.SUBTREE,
            detail="bucket 0 of 2",
        ),
        CreatePartition(
            target=f"{BRANCH}__h1",
            parent_name=BRANCH,
            bounds=HashBounds(modulus=2, remainder=1),
            partition_by=None,
            key_columns=("tenant_id",),
            children=(),
            lifecycle_unit=False,
            counts_as="subtree",
            reason=Reason.SUBTREE,
            detail="bucket 1 of 2",
        ),
    )
    assert op.count() == 3
    assert plan.relation_count == 3


def test__plan_maintenance__range_over_list_over_hash__builds_three_levels_deep() -> None:
    # Arrange
    config = _config(_range_level(_list_level(child=_hash_level(modulus=2), include_default=True)))

    # Act
    plan = _plan(config, _root())

    # Assert
    (op,) = plan.creates
    assert op.partition_by == PartitionBy(method=PartitionType.LIST, columns=("region",))
    assert _targets(op.children) == [f"{BRANCH}__eu", f"{BRANCH}__us", f"{BRANCH}__other"]
    eu, _, other = op.children
    assert eu.bounds == ListBounds(values=("de", "fr"))
    assert eu.partition_by == PartitionBy(method=PartitionType.HASH, columns=("tenant_id",))
    assert eu.key_columns == ("region",)
    assert _targets(eu.children) == [f"{BRANCH}__eu__h0", f"{BRANCH}__eu__h1"]
    assert other.bounds == DefaultBounds()
    assert _targets(other.children) == [f"{BRANCH}__other__h0", f"{BRANCH}__other__h1"]
    assert {(nested.counts_as, nested.reason) for nested in op.walk()[1:]} == {("subtree", Reason.SUBTREE)}
    assert op.count() == 10


def test__plan_maintenance__list_root_over_range__new_group_carries_the_cursor_windows() -> None:
    # Arrange
    config = _config(
        _list_level(child=_range_level()), table_name="regions", lifecycle=_policy(creation=CreateAhead(count=2))
    )
    root = _root(name="public.regions", partition_type=PartitionType.LIST, columns=("region",))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == ["public.regions__eu", "public.regions__us"]
    eu = plan.creates[0]
    assert eu.reason is Reason.LIST_GROUP_MISSING
    assert eu.counts_as == "created"
    assert eu.lifecycle_unit is False
    assert eu.partition_by == PartitionBy(method=PartitionType.RANGE, columns=("created_at",))
    assert _targets(eu.children) == ["public.regions__eu__2026_08", "public.regions__eu__2026_09"]
    assert eu.children[0].bounds == RangeBounds(from_value="2026-08-01", to_value="2026-09-01")
    assert eu.children[0].parent_name == "public.regions__eu"
    assert eu.children[0].key_columns == ("created_at",)
    assert {(nested.counts_as, nested.reason) for nested in eu.children} == {("subtree", Reason.SUBTREE)}


def test__plan_maintenance__list_root_over_range__existing_group_gets_windows_planned_inside_it() -> None:
    # Arrange
    config = _config(
        _list_level(child=_range_level()), table_name="regions", lifecycle=_policy(creation=CreateAhead(count=2))
    )
    group = PartitionNode(
        name="public.regions__eu",
        parent_name="public.regions",
        bounds=ListBounds(values=("de", "fr")),
        partition_type=PartitionType.RANGE,
        partition_columns=("created_at",),
        children=(_month(2026, 8, parent="public.regions__eu", oid=5),),
    )
    root = _root(group, name="public.regions", partition_type=PartitionType.LIST, columns=("region",))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == ["public.regions__us", "public.regions__eu__2026_09"]
    window = plan.creates[1]
    assert window.parent_name == "public.regions__eu"
    assert window.lifecycle_unit is True
    assert window.reason is Reason.CREATE_AHEAD
    assert window.key_columns == ("created_at",)
    assert window.partition_by is None


def test__plan_maintenance__existing_window_missing_a_bucket__repaired_in_place() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))
    root = _root(_branch((2, 0)))

    # Act
    plan = _plan(config, root)

    # Assert
    (op,) = plan.creates
    assert op == CreatePartition(
        target=f"{BRANCH}__h1",
        parent_name=BRANCH,
        bounds=HashBounds(modulus=2, remainder=1),
        partition_by=None,
        key_columns=("tenant_id",),
        children=(),
        lifecycle_unit=False,
        counts_as="repaired",
        reason=Reason.HASH_GAP,
        detail="bucket 1 of 2",
    )


# ── HASH levels: the convergence rules ──────────────────────────────────────────


def test__plan_maintenance__branch_with_no_buckets__creates_the_configured_set() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch()))

    # Assert
    assert _hash_created(plan) == [(f"{BRANCH}__h0", 2, 0), (f"{BRANCH}__h1", 2, 1)]
    assert plan.findings == ()


def test__plan_maintenance__complete_set_at_the_configured_modulus__plans_nothing() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch((2, 0), (2, 1))))

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__one_bucket_missing__creates_only_that_bucket() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=4)))

    # Act
    plan = _plan(config, _root(_branch((4, 0), (4, 1), (4, 3))))

    # Assert
    assert _hash_created(plan) == [(f"{BRANCH}__h2", 4, 2)]
    assert plan.creates[0].reason is Reason.HASH_GAP
    assert plan.findings == ()


def test__plan_maintenance__complete_set_at_another_modulus__left_untouched() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch(*[(4, r) for r in range(4)])))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.MODULUS_PRESERVED]
    assert plan.findings[0].partition_name == BRANCH
    assert plan.actionable_findings == ()


def test__plan_maintenance__incomplete_set_at_another_modulus__repaired_at_its_own_modulus() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch((4, 0), (4, 1), (4, 3))))

    # Assert: modulus 2 buckets would overlap the existing ones.
    assert _hash_created(plan) == [(f"{BRANCH}__h2", 4, 2)]
    assert plan.creates[0].reason is Reason.HASH_GAP_HISTORICAL_MODULUS
    assert _reasons(plan) == [FindingReason.MODULUS_REPAIRED]
    assert plan.actionable_findings == ()


def test__plan_maintenance__mixed_moduli_leaving_a_gap__reported_and_untouched() -> None:
    # Arrange: (2,0) owns even residues, (4,1) owns one odd class; 3 is orphaned.
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch((2, 0), (4, 1))))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.NON_UNIFORM_INCOMPLETE]
    assert plan.actionable_findings != ()


def test__plan_maintenance__mixed_moduli_that_still_tile__left_alone_without_alarm() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch((2, 1), (4, 0), (4, 2))))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.NON_UNIFORM_COMPLETE]
    assert plan.actionable_findings == ()


def test__plan_maintenance__moduli_too_coarse_to_verify__coverage_unknown() -> None:
    # Arrange: the least common multiple is beyond what the library enumerates.
    config = _config(_range_level(_hash_level(modulus=2)))
    root = _root(_branch((MAX_HASH_KEYSPACE_LCM + 1, 0), (2, 1)))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.COVERAGE_UNKNOWN]
    assert plan.actionable_findings != ()


def test__plan_maintenance__branch_subpartitioned_by_list_under_a_hash_spec__strategy_mismatch() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))
    root = _root(_list_branch(_list_child(BRANCH, "eu", "eu")))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.STRATEGY_MISMATCH]
    assert plan.findings[0].severity is Severity.WARNING
    assert "partitioned by LIST (region)" in plan.findings[0].detail


def test__plan_maintenance__hash_branch_under_a_list_spec__strategy_mismatch() -> None:
    # Arrange
    config = _config(_range_level(_list_level()))

    # Act
    plan = _plan(config, _root(_branch((2, 0), (2, 1))))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.STRATEGY_MISMATCH]


def test__plan_maintenance__hash_on_a_different_column__column_mismatch() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch((2, 0), (2, 1), columns=("region_id",))))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.COLUMN_MISMATCH]
    assert plan.actionable_findings != ()
    assert "HASH (region_id)" in plan.findings[0].detail


def test__plan_maintenance__branch_partitioned_on_an_expression__column_mismatch() -> None:
    # Arrange: the catalog names the columns only, so the key looks complete.
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch(has_expression_key=True)))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.COLUMN_MISMATCH]
    assert "expression" in plan.findings[0].detail


def test__plan_maintenance__legacy_leaf_partition__left_valid_and_untouched() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))

    # Act
    plan = _plan(config, _root(_branch(partition_type=None)))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.LEGACY_LEAF]
    assert plan.actionable_findings == ()


def test__plan_maintenance__legacy_leaf_under_a_list_spec__left_untouched() -> None:
    # Arrange
    config = _config(_range_level(_list_level()))

    # Act
    plan = _plan(config, _root(_branch(partition_type=None)))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.LEGACY_LEAF]


def test__plan_maintenance__gap_one_level_down__repaired_in_place() -> None:
    # Arrange: the outer set is complete, but one inner bucket is missing.
    config = _config(_range_level(_hash_level(modulus=2, child=_hash_level(modulus=2, key="shard_id"))))
    outer0 = _bucket(BRANCH, 2, 0, partition_type=PartitionType.HASH, partition_columns=("shard_id",)).model_copy(
        update={"children": (_bucket(f"{BRANCH}__h0", 2, 0), _bucket(f"{BRANCH}__h0", 2, 1))}
    )
    outer1 = _bucket(BRANCH, 2, 1, partition_type=PartitionType.HASH, partition_columns=("shard_id",)).model_copy(
        update={"children": (_bucket(f"{BRANCH}__h1", 2, 0),)}
    )
    root = _root(_branch().model_copy(update={"children": (outer0, outer1)}))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _hash_created(plan) == [(f"{BRANCH}__h1__h1", 2, 1)]
    assert plan.creates[0].counts_as == "repaired"


def test__plan_maintenance__custom_name_suffix__used_for_repairs_too() -> None:
    # Arrange: a branch adopted from a partitioner that named buckets _h0/_h1.
    config = _config(_range_level(_hash_level(modulus=4, name_suffix="_h{remainder}")))
    from_value, to_value = MONTHS.literals(_window(2026, 8))
    branch = PartitionNode(
        name="public.events_202608",
        parent_name=ROOT,
        bounds=RangeBounds(from_value=from_value, to_value=to_value),
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=tuple(
            PartitionNode(name=f"public.events_202608_h{r}", bounds=HashBounds(modulus=4, remainder=r))
            for r in (0, 1, 3)
        ),
    )

    # Act
    plan = _plan(config, _root(branch))

    # Assert
    assert _hash_created(plan) == [("public.events_202608_h2", 4, 2)]


def test__plan_maintenance__unqualified_table__keeps_every_generated_name_unqualified() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=1)), schema=None)
    root = _root(_branch(name="events__2026_08"), name="events")

    # Act
    plan = _plan(config, root)

    # Assert
    assert _hash_created(plan) == [("events__2026_08__h0", 1, 0)]
    assert plan.table_name == "events"


def test__plan_maintenance__modulus_preserved__still_converges_the_levels_below() -> None:
    # Arrange: the branch tiles the keyspace at 4 while the config says 2, so
    # this level is left alone -- but its buckets have no children at all.
    config = _config(_range_level(_hash_level(modulus=2, child=_hash_level(modulus=2, key="shard_id"))))
    buckets = tuple(
        _bucket(BRANCH, 4, r, partition_type=PartitionType.HASH, partition_columns=("shard_id",)) for r in range(4)
    )
    root = _root(_branch().model_copy(update={"children": buckets}))

    # Act
    plan = _plan(config, root)

    # Assert: leaving this level's modulus alone says nothing about the ones
    # below it; abandoning them leaves the branch rejecting rows.
    assert _targets(plan.creates) == [f"{BRANCH}__h{r}__h{i}" for r in range(4) for i in range(2)]
    assert _reasons(plan) == [FindingReason.MODULUS_PRESERVED]


def test__plan_maintenance__non_uniform_but_complete__still_converges_the_levels_below() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2, child=_hash_level(modulus=1, key="shard_id"))))
    buckets = tuple(
        _bucket(BRANCH, modulus, r, partition_type=PartitionType.HASH, partition_columns=("shard_id",))
        for modulus, r in ((2, 1), (4, 0), (4, 2))
    )
    root = _root(_branch().model_copy(update={"children": buckets}))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == [f"{BRANCH}__h1__h0", f"{BRANCH}__h0__h0", f"{BRANCH}__h2__h0"]
    assert _reasons(plan) == [FindingReason.NON_UNIFORM_COMPLETE]


def test__plan_maintenance__branch_hiding_a_child__plans_nothing_and_says_so() -> None:
    # Arrange: the branch really has four buckets, but one of them has a dot
    # in its name and was left out of the tree.
    config = _config(_range_level(_hash_level(modulus=4)))
    root = _root(_branch((4, 0), (4, 1), (4, 2), has_unaddressable_children=True))

    # Act
    plan = _plan(config, root)

    # Assert: the missing bucket may be the hidden one, so proposing it would
    # conflict with a partition that already exists, on every single run.
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.COVERAGE_UNKNOWN]
    assert plan.findings[0].partition_name == BRANCH


def test__plan_maintenance__root_hiding_a_child__plans_nothing_at_all() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=3)))

    # Act
    plan = _plan(config, _root(has_unaddressable_children=True))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.COVERAGE_UNKNOWN]
    assert plan.findings[0].partition_name == ROOT


# ── Names that cannot be used ───────────────────────────────────────────────────


def test__plan_maintenance__target_name_held_by_a_mismatched_partition__reported_not_planned() -> None:
    # Arrange: the name the config would generate is taken by a LIST partition
    # owning different values, so no value overlap makes the planner want it.
    config = _config(_range_level(_list_level(groups=(ListGroup(name="eu", values=("eu",)),))))
    root = _root(_list_branch(_list_child(BRANCH, "eu", "europe")))

    # Act
    plan = _plan(config, root)

    # Assert: creating it would collide, and the collision is indistinguishable
    # from a lost race once PostgreSQL reports it.
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.NAME_UNUSABLE]
    assert plan.findings[0].partition_name == BRANCH
    assert plan.actionable_findings != ()


def test__plan_maintenance__window_name_held_by_a_partition_with_other_bounds__reported_not_planned() -> None:
    # Arrange: a partition carries August's name but holds a window years away.
    from_value, to_value = MONTHS.literals(_window(2030, 1))
    root = _root(
        PartitionNode(name=BRANCH, parent_name=ROOT, bounds=RangeBounds(from_value=from_value, to_value=to_value))
    )

    # Act
    plan = _plan(_config(), root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.NAME_UNUSABLE]
    assert plan.findings[0].partition_name == ROOT
    assert f"'{BRANCH}'" in plan.findings[0].detail


def test__plan_maintenance__repair_name_would_exceed_the_identifier_limit__reported_against_the_branch() -> None:
    # Arrange: repairing at the branch's own larger modulus needs more digits
    # than the config validator sized the name for.
    config = _config(_range_level(_hash_level(modulus=8)))
    long_branch = "public." + "e" * 59
    root = _root(_branch(*[(16, r) for r in range(10)], name=long_branch))

    # Act
    plan = _plan(config, root)

    # Assert: PostgreSQL truncates at 63 bytes silently, so planning these
    # would collapse six buckets onto existing names.
    assert plan.is_noop
    assert set(_reasons(plan)) == {FindingReason.MODULUS_REPAIRED, FindingReason.NAME_UNUSABLE}
    unusable = [f for f in plan.findings if f.reason is FindingReason.NAME_UNUSABLE]
    assert len(unusable) == 6
    assert {f.partition_name for f in unusable} == {long_branch}
    assert all(f.is_actionable for f in unusable)


def test__plan_maintenance__new_subtree_name_over_the_identifier_limit__reported_against_the_new_parent() -> None:
    # Arrange: the config validator's name budget is bypassed on purpose -- a
    # custom RangeBoundaries can under-report its budget -- so the planner's
    # own guard is what is under test. The window's own name fits (61 bytes);
    # its buckets would not (65).
    table = "e" * 52
    config = TablePartitionConfig.model_construct(
        schema_name=SCHEMA, table_name=table, scheme=_range_level(_hash_level(modulus=2)), lifecycle=_policy()
    )
    new_window = f"{SCHEMA}.{table}__2026_08"

    # Act
    plan = _plan(config, _root(name=f"{SCHEMA}.{table}"))

    # Assert: there is no node to hang the finding on, so it names the parent
    # that does not exist yet -- and the parent itself is then refused, because
    # a partitioned relation with a hole in its child set would reject rows.
    assert _reasons(plan) == [
        FindingReason.NAME_UNUSABLE,
        FindingReason.NAME_UNUSABLE,
        FindingReason.UNCONVERGEABLE,
    ]
    assert {f.partition_name for f in plan.findings} == {new_window}
    assert "63-byte identifier limit" in plan.findings[0].detail
    assert plan.is_noop


def test__plan_maintenance__subtree_names_all_unusable__does_not_plan_a_childless_branch() -> None:
    # Arrange
    table = "e" * 52
    config = TablePartitionConfig.model_construct(
        schema_name=SCHEMA, table_name=table, scheme=_range_level(_hash_level(modulus=2)), lifecycle=_policy()
    )

    # Act
    plan = _plan(config, _root(name=f"{SCHEMA}.{table}"))

    # Assert
    assert FindingReason.NAME_UNUSABLE in _reasons(plan)
    assert plan.creates == ()


def test__plan_maintenance__window_name_over_the_identifier_limit__reported_against_the_root() -> None:
    # Arrange: 55 bytes of table plus the 9-byte month suffix is 64.
    table = "e" * 55
    config = TablePartitionConfig.model_construct(
        schema_name=SCHEMA, table_name=table, scheme=_range_level(), lifecycle=_policy()
    )

    # Act
    plan = _plan(config, _root(name=f"{SCHEMA}.{table}"))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.NAME_UNUSABLE]
    assert plan.findings[0].partition_name == f"{SCHEMA}.{table}"


# ── LIST levels ─────────────────────────────────────────────────────────────────


def test__plan_maintenance__list_branch_missing_a_group__creates_only_that_group() -> None:
    # Arrange
    config = _config(_range_level(_list_level()))
    root = _root(_list_branch(_list_child(BRANCH, "eu", "de", "fr")))

    # Act
    plan = _plan(config, root)

    # Assert
    (op,) = plan.creates
    assert op == CreatePartition(
        target=f"{BRANCH}__us",
        parent_name=BRANCH,
        bounds=ListBounds(values=("us",)),
        partition_by=None,
        key_columns=("region",),
        children=(),
        lifecycle_unit=False,
        counts_as="repaired",
        reason=Reason.LIST_GROUP_MISSING,
        detail="group 'us'",
    )
    assert plan.findings == ()


def test__plan_maintenance__list_branch_already_complete__plans_nothing() -> None:
    # Arrange
    config = _config(_range_level(_list_level()))
    root = _root(_list_branch(_list_child(BRANCH, "eu", "de", "fr"), _list_child(BRANCH, "us", "us")))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__list_group_matched_by_values_not_name__left_alone() -> None:
    # Arrange: a partition another tool named differently owns the same values.
    config = _config(_range_level(_list_level()))
    root = _root(_list_branch(_list_child(BRANCH, "europe", "fr", "de"), _list_child(BRANCH, "us", "us")))

    # Act
    plan = _plan(config, root)

    # Assert: recognised, not duplicated under our own naming.
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__list_value_already_owned_elsewhere__reported_not_created() -> None:
    # Arrange: "de" sits in a partition that does not match the configured group.
    config = _config(_range_level(_list_level()))
    root = _root(_list_branch(_list_child(BRANCH, "dach", "de", "at")))

    # Act
    plan = _plan(config, root)

    # Assert: only the non-conflicting group is created.
    assert _targets(plan.creates) == [f"{BRANCH}__us"]
    assert _reasons(plan) == [FindingReason.LIST_VALUES_CONFLICT]
    assert plan.findings[0].partition_name == BRANCH
    assert f"'de' in {BRANCH}__dach" in plan.findings[0].detail
    assert plan.actionable_findings != ()


def test__plan_maintenance__list_default_missing__created_last() -> None:
    # Arrange
    config = _config(_range_level(_list_level(include_default=True)))
    root = _root(_list_branch(_list_child(BRANCH, "eu", "de", "fr")))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == [f"{BRANCH}__us", f"{BRANCH}__other"]
    default = plan.creates[-1]
    assert default.bounds == DefaultBounds()
    assert default.reason is Reason.LIST_DEFAULT_MISSING
    assert default.detail == "DEFAULT catch-all"


def test__plan_maintenance__list_default_present__not_created_again() -> None:
    # Arrange
    config = _config(_range_level(_list_level(include_default=True)))
    root = _root(
        _list_branch(
            _list_child(BRANCH, "eu", "de", "fr"),
            _list_child(BRANCH, "us", "us"),
            PartitionNode(name=f"{BRANCH}__other", parent_name=BRANCH, bounds=DefaultBounds()),
        )
    )

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop


def test__plan_maintenance__list_default_under_a_foreign_name__not_created_again() -> None:
    # Arrange: matching the DEFAULT by name alone would plan a duplicate that
    # PostgreSQL refuses on every run, forever.
    config = _config(_range_level(_list_level(groups=(ListGroup(name="eu", values=("eu",)),), include_default=True)))
    root = _root(
        _list_branch(
            _list_child(BRANCH, "eu", "eu"),
            PartitionNode(name=f"{BRANCH}__catch_all", parent_name=BRANCH, bounds=DefaultBounds()),
        )
    )

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop


def test__plan_maintenance__list_group_missing_under_a_custom_suffix__named_by_the_suffix() -> None:
    # Arrange
    config = _config(_range_level(_list_level(name_suffix="_{name}_p", include_default=True, default_name="rest")))
    root = _root(_list_branch(_list_child(BRANCH, "eu", "de", "fr")))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == [f"{BRANCH}_us_p", f"{BRANCH}_rest_p"]


def test__plan_maintenance__hash_under_list__gap_one_level_down_is_repaired() -> None:
    # Arrange: RANGE -> LIST(region) -> HASH(tenant_id), with one bucket missing.
    config = _config(_range_level(_list_level(child=_hash_level(modulus=2))))
    eu = _list_child(
        BRANCH, "eu", "de", "fr", partition_type=PartitionType.HASH, partition_columns=("tenant_id",)
    ).model_copy(update={"children": (_bucket(f"{BRANCH}__eu", 2, 0),)})
    us = _list_child(
        BRANCH, "us", "us", partition_type=PartitionType.HASH, partition_columns=("tenant_id",)
    ).model_copy(update={"children": (_bucket(f"{BRANCH}__us", 2, 0), _bucket(f"{BRANCH}__us", 2, 1))})
    root = _root(_list_branch(eu, us))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _hash_created(plan) == [(f"{BRANCH}__eu__h1", 2, 1)]
    assert plan.findings == ()


def test__plan_maintenance__list_default_partitioned_below__recursed_into_like_a_group() -> None:
    # Arrange: the DEFAULT this library creates under LIST -> HASH is itself a
    # hash-partitioned branch, so its own gaps are repaired too.
    config = _config(_range_level(_list_level(child=_hash_level(modulus=2), include_default=True)))
    default = PartitionNode(
        name=f"{BRANCH}__other",
        parent_name=BRANCH,
        bounds=DefaultBounds(),
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=(_bucket(f"{BRANCH}__other", 2, 1),),
    )
    eu = _list_child(
        BRANCH, "eu", "de", "fr", partition_type=PartitionType.HASH, partition_columns=("tenant_id",)
    ).model_copy(update={"children": (_bucket(f"{BRANCH}__eu", 2, 0), _bucket(f"{BRANCH}__eu", 2, 1))})
    us = _list_child(
        BRANCH, "us", "us", partition_type=PartitionType.HASH, partition_columns=("tenant_id",)
    ).model_copy(update={"children": (_bucket(f"{BRANCH}__us", 2, 0), _bucket(f"{BRANCH}__us", 2, 1))})
    root = _root(_list_branch(eu, us, default))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _hash_created(plan) == [(f"{BRANCH}__other__h0", 2, 0)]


# ── Static roots ────────────────────────────────────────────────────────────────


def test__plan_maintenance__static_hash_root__members_created_and_counted_as_created() -> None:
    # Arrange
    config = _config(_hash_level(modulus=4, key="task_id"), table_name="tasks")
    root = _root(name="public.tasks", partition_type=PartitionType.HASH, columns=("task_id",))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _hash_created(plan) == [(f"public.tasks__h{r}", 4, r) for r in range(4)]
    assert {(op.counts_as, op.lifecycle_unit, op.reason, op.parent_name) for op in plan.creates} == {
        ("created", False, Reason.HASH_GAP, "public.tasks")
    }
    assert plan.findings == ()


def test__plan_maintenance__static_hash_root__complete_set_is_noop_and_never_detached() -> None:
    # Arrange: a retention rule is meaningless for a set; the members are the table.
    config = _config(
        _hash_level(modulus=2, key="task_id"), table_name="tasks", lifecycle=_policy(retention=KeepNewest(count=1))
    )
    root = _root(
        _bucket("public.tasks", 2, 0, oid=1),
        _bucket("public.tasks", 2, 1, oid=2),
        name="public.tasks",
        partition_type=PartitionType.HASH,
        columns=("task_id",),
    )

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop


def test__plan_maintenance__static_list_root__members_created_and_counted_as_created() -> None:
    # Arrange
    config = _config(_list_level(include_default=True), table_name="regions")
    root = _root(name="public.regions", partition_type=PartitionType.LIST, columns=("region",))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == ["public.regions__eu", "public.regions__us", "public.regions__other"]
    assert {op.counts_as for op in plan.creates} == {"created"}
    assert [op.reason for op in plan.creates] == [
        Reason.LIST_GROUP_MISSING,
        Reason.LIST_GROUP_MISSING,
        Reason.LIST_DEFAULT_MISSING,
    ]
    assert plan.detaches == ()
    assert plan.drops == ()


def test__plan_maintenance__static_list_root__orphans_never_dropped_because_nothing_expires_there() -> None:
    # Arrange
    config = _config(_list_level(), table_name="regions")
    root = _root(
        _list_child("public.regions", "eu", "de", "fr"),
        _list_child("public.regions", "us", "us"),
        name="public.regions",
        partition_type=PartitionType.LIST,
        columns=("region",),
    )
    orphan = _orphan("regions__asia", parent="public.regions", detached_at=NOW - timedelta(days=400), oid=9)

    # Act
    plan = _plan(config, root, orphans=(orphan,))

    # Assert
    assert plan.is_noop


# ── Retention ───────────────────────────────────────────────────────────────────


def _months(*months: int, **overrides: Any) -> tuple[PartitionNode, ...]:
    return tuple(_month(2026, month, oid=month, **overrides) for month in months)


def test__plan_maintenance__keep_newest__expires_windows_at_or_behind_the_cutoff_only() -> None:
    # Arrange: count=3 keeps June, July and August; September is ahead.
    config = _config(lifecycle=_policy(retention=KeepNewest(count=3), drop=DropAfter(grace=timedelta(days=7))))
    root = _root(*_months(4, 5, 6, 7, 8, 9))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.operations) == [f"{ROOT}__2026_04", f"{ROOT}__2026_05"]
    assert plan.detaches[0] == DetachPartition(
        target=f"{ROOT}__2026_04",
        oid=4,
        parent_name=ROOT,
        mode=DetachMode.AUTO,
        bounds=RangeBounds(from_value="2026-04-01", to_value="2026-05-01"),
        reason=Reason.RETENTION_EXPIRED,
        detail="2026_04 expired under 'keep newest 3'",
    )


def test__plan_maintenance__keep_newest_of_one__never_touches_the_cursor_window_or_the_future() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=3), retention=KeepNewest(count=1)))
    root = _root(*_months(8, 9, 10))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop


def test__plan_maintenance__drop_after_zero_grace__drop_follows_each_detach() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=2)))
    root = _root(*_months(5, 6, 7, 8))

    # Act
    plan = _plan(config, root)

    # Assert: every detach comes before any drop, and each drop is bound to its detach.
    assert [op.kind for op in plan.operations] == [
        OperationKind.DETACH,
        OperationKind.DETACH,
        OperationKind.DROP,
        OperationKind.DROP,
    ]
    assert plan.drops == (
        DropPartition(
            target=f"{ROOT}__2026_05",
            oid=5,
            reason=Reason.FOLLOWS_DETACH,
            detail="dropped in the same run as its detach ('drop immediately')",
            follows_detach=True,
        ),
        DropPartition(
            target=f"{ROOT}__2026_06",
            oid=6,
            reason=Reason.FOLLOWS_DETACH,
            detail="dropped in the same run as its detach ('drop immediately')",
            follows_detach=True,
        ),
    )


def test__plan_maintenance__drop_after_positive_grace__detach_without_a_drop() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1), drop=DropAfter(grace=timedelta(minutes=1))))

    # Act
    plan = _plan(config, _root(*_months(7, 8)))

    # Assert
    assert _targets(plan.detaches) == [f"{ROOT}__2026_07"]
    assert plan.drops == ()


@pytest.mark.parametrize(("size", "dropped"), [(10, False), (200, True)], ids=["condition-false", "condition-true"])
def test__plan_maintenance__drop_after_with_a_condition__drop_follows_only_when_it_holds(
    size: int, dropped: bool
) -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1), drop=DropAfter(when=SizeAbove(bytes=100))))
    root = _root(_month(2026, 7, oid=7, facts=PartitionFacts(size_bytes=size, row_estimate=3)), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.detaches) == [f"{ROOT}__2026_07"]
    assert plan.detaches[0].size_bytes == size
    assert plan.detaches[0].row_estimate == 3
    assert _targets(plan.drops) == ([f"{ROOT}__2026_07"] if dropped else [])
    if dropped:
        assert plan.drops[0].size_bytes == size
        assert (
            plan.drops[0].detail == "dropped in the same run as its detach ('drop immediately when size > 100 bytes')"
        )


def test__plan_maintenance__drop_never__detach_only() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1), drop=DropNever()))

    # Act
    plan = _plan(config, _root(*_months(7, 8)))

    # Assert
    assert _targets(plan.detaches) == [f"{ROOT}__2026_07"]
    assert plan.drops == ()


def test__plan_maintenance__detach_mode__taken_from_the_policy() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1), detach=DetachMode.BLOCKING))

    # Act
    plan = _plan(config, _root(*_months(7, 8)))

    # Assert
    assert plan.detaches[0].mode is DetachMode.BLOCKING


def test__plan_maintenance__keep_for__expires_windows_over_for_longer_than_the_age() -> None:
    # Arrange: 90 days before the 28th of August is the 30th of May.
    config = _config(lifecycle=_policy(retention=KeepFor(age=timedelta(days=90))))

    # Act
    plan = _plan(config, _root(*_months(4, 5, 6, 8)))

    # Assert: April ended on the 1st of May; May ended on the 1st of June.
    assert _targets(plan.detaches) == [f"{ROOT}__2026_04"]
    assert plan.detaches[0].detail == "2026_04 expired under 'keep for 90 days, 0:00:00'"


def test__plan_maintenance__keep_behind_on_a_time_axis__never_expires() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepBehind(distance=1)))

    # Act
    plan = _plan(config, _root(*_months(1, 2, 8)))

    # Assert
    assert plan.is_noop


def test__plan_maintenance__expire_if_sql_predicate__reads_the_measured_answer() -> None:
    # Arrange
    predicate = SqlPredicate(sql=SQL)
    config = _config(lifecycle=_policy(retention=ExpireIf(when=predicate)))
    root = _root(
        _month(2026, 4, oid=4, facts=PartitionFacts(predicates={predicate.id: True})),
        _month(2026, 5, oid=5, facts=PartitionFacts(predicates={predicate.id: False})),
        _month(2026, 6, oid=6),
        _month(2026, 8, oid=8, facts=PartitionFacts(predicates={predicate.id: True})),
    )

    # Act
    plan = _plan(config, root)

    # Assert: unanswered reads as false; the cursor's window is never a candidate.
    assert _targets(plan.detaches) == [f"{ROOT}__2026_04"]


def test__plan_maintenance__all_of_age_and_emptiness__expires_only_old_empty_windows() -> None:
    # Arrange
    retention = AllOf(members=(WindowAgeAbove(age=timedelta(days=30)), Not(member=RowsAbove(rows=0))))
    config = _config(lifecycle=_policy(retention=retention))
    root = _root(
        _month(2026, 4, oid=4, facts=PartitionFacts(row_estimate=0)),
        _month(2026, 5, oid=5, facts=PartitionFacts(row_estimate=120)),
        _month(2026, 7, oid=7, facts=PartitionFacts(row_estimate=0)),
        _month(2026, 8, oid=8),
    )

    # Act
    plan = _plan(config, root)

    # Assert: May holds rows; July ended less than 30 days ago.
    assert _targets(plan.detaches) == [f"{ROOT}__2026_04"]
    assert plan.detaches[0].row_estimate == 0


def test__plan_maintenance__operations__ordered_creates_attaches_detaches_drops() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=3), retention=KeepNewest(count=2)))
    root = _root(*_months(6, 7, 8))
    orphans = (
        _orphan("events__2026_10", oid=10, detached_at=NOW - timedelta(days=1)),
        _orphan("events__2025_12", oid=12, detached_at=NOW - timedelta(days=30)),
    )

    # Act
    plan = _plan(config, root, orphans=orphans)

    # Assert
    assert [(op.kind, op.target) for op in plan.operations] == [
        (OperationKind.CREATE, f"{ROOT}__2026_09"),
        (OperationKind.ATTACH, f"{ROOT}__2026_10"),
        (OperationKind.DETACH, f"{ROOT}__2026_06"),
        (OperationKind.DROP, f"{ROOT}__2026_06"),
        (OperationKind.DROP, f"{ROOT}__2025_12"),
    ]


def test__plan_maintenance__plan__records_the_cursors_it_was_made_against() -> None:
    # Arrange
    context = _context(cursors={"msg_id": 1_250_000})

    # Act
    plan = _plan(_config(), _root(), context=context)

    # Assert
    assert plan.cursors == {"msg_id": 1_250_000}
    assert plan.generated_at == NOW


# ── Ownership by alignment ──────────────────────────────────────────────────────


def test__plan_maintenance__partition_off_the_grid__reported_unmanaged_and_never_detached() -> None:
    # Arrange: a twenty-year archive attached by hand, older than any retention.
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1)))
    root = _root(_range_child("archive", "2000-01-01", "2020-01-01", oid=1), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.UNMANAGED_PARTITION]
    assert plan.findings[0].partition_name == f"{ROOT}__archive"
    assert plan.findings[0].severity is Severity.INFO


def test__plan_maintenance__partition_finer_than_the_grid__managed_and_expired_by_its_upper_bound() -> None:
    # Arrange: a single day inside May, left behind by an earlier daily scheme.
    config = _config(lifecycle=_policy(retention=KeepNewest(count=2)))
    root = _root(_range_child("2026_05_03", "2026-05-03", "2026-05-04", oid=1), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.detaches) == [f"{ROOT}__2026_05_03"]
    assert plan.findings == ()


def test__plan_maintenance__partition_finer_than_the_grid_inside_a_kept_window__left_alone() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=12)))
    root = _root(_range_child("2026_07_03", "2026-07-03", "2026-07-04", oid=1), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__wanted_window_overlapping_an_unmanaged_partition__reported_not_created() -> None:
    # Arrange: a half-year partition covers both windows the policy wants.
    config = _config(lifecycle=_policy(creation=CreateAhead(count=2)))
    root = _root(_range_child("h2", "2026-07-01", "2027-01-01", oid=1))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [
        FindingReason.UNMANAGED_PARTITION,
        FindingReason.RANGE_OVERLAP,
        FindingReason.RANGE_OVERLAP,
    ]
    overlap = plan.findings[1]
    assert overlap.partition_name == ROOT
    assert overlap.is_actionable
    assert f"2026_08 but {ROOT}__h2 already covers" in overlap.detail


def test__plan_maintenance__wanted_window_overlapping_a_finer_partition__reported_not_created() -> None:
    # Arrange: a day inside the cursor's month is managed, yet the month cannot be created around it.
    root = _root(_range_child("2026_08_03", "2026-08-03", "2026-08-04", oid=1))

    # Act
    plan = _plan(_config(), root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.RANGE_OVERLAP]


@pytest.mark.parametrize(
    ("from_value", "to_value"),
    [("MINVALUE", "2020-01-01"), ("-infinity", "2020-01-01"), ("minvalue", "2020-01-01")],
    ids=["MINVALUE", "-infinity", "lowercase"],
)
def test__plan_maintenance__lower_unbounded_partition__reported_and_never_pruned(
    from_value: str, to_value: str
) -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1)))
    root = _root(_range_child("history", from_value, to_value, oid=1), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.UNBOUNDED_PARTITION]
    assert plan.findings[0].severity is Severity.INFO
    assert plan.findings[0].partition_name == f"{ROOT}__history"


@pytest.mark.parametrize("to_value", ["MAXVALUE", "infinity", "+infinity"], ids=["MAXVALUE", "infinity", "+infinity"])
def test__plan_maintenance__upper_unbounded_partition__blocks_the_windows_it_covers(to_value: str) -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=2)))
    root = _root(_range_child("future", "2026-09-01", to_value, oid=1))

    # Act
    plan = _plan(config, root)

    # Assert: August is still created; September collides with the open end.
    assert _targets(plan.creates) == [f"{ROOT}__2026_08"]
    assert _reasons(plan) == [FindingReason.UNBOUNDED_PARTITION, FindingReason.RANGE_OVERLAP]


def test__plan_maintenance__unreadable_bound__reported_never_pruned_and_never_blocking() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1)))
    root = _root(_range_child("bad", "abc", "def", oid=1))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_08"]
    assert plan.detaches == ()
    assert _reasons(plan) == [FindingReason.UNREADABLE_BOUND]
    assert plan.findings[0].is_actionable
    assert "'abc' .. 'def'" in plan.findings[0].detail


def test__plan_maintenance__window_at_the_edge_of_the_calendar__reported_unmanaged_rather_than_failing() -> None:
    # Arrange: the month after this one has no representable start.
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1)))
    root = _root(_range_child("far", "9999-12-01", "9999-12-31", oid=1), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.UNMANAGED_PARTITION]


def test__plan_maintenance__foreign_leaf__reported_and_never_detached() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1)))
    root = _root(_month(2026, 4, oid=4, relkind=RelationKind.FOREIGN), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.FOREIGN_PARTITION]
    assert plan.findings[0].severity is Severity.INFO
    assert plan.findings[0].partition_name == f"{ROOT}__2026_04"


def test__plan_maintenance__foreign_leaf_holding_a_wanted_window__satisfies_it_without_a_warning() -> None:
    # Arrange
    root = _root(_month(2026, 8, oid=8, relkind=RelationKind.FOREIGN))

    # Act
    plan = _plan(_config(), root)

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.FOREIGN_PARTITION]


def test__plan_maintenance__foreign_group_under_a_nested_level__reported_not_planned_into() -> None:
    # Arrange: LIST -> HASH, where one group is a foreign table.
    config = _config(_list_level(child=_hash_level(modulus=2)), table_name="regions")
    eu = _list_child("public.regions", "eu", "de", "fr", relkind=RelationKind.FOREIGN)
    root = _root(eu, name="public.regions", partition_type=PartitionType.LIST, columns=("region",))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.creates) == ["public.regions__us"]
    assert _reasons(plan) == [FindingReason.FOREIGN_PARTITION]
    assert plan.findings[0].partition_name == "public.regions__eu"
    assert "cannot hold HASH (tenant_id) partitions" in plan.findings[0].detail


def test__plan_maintenance__detach_pending_child__finalized_and_reported() -> None:
    # Arrange: an interrupted DETACH CONCURRENTLY left April half-detached
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1)))
    root = _root(_month(2026, 4, oid=4, detach_pending=True), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root)

    # Assert -- the detach is completed; nothing else is planned for the window
    assert [op.target for op in plan.detaches] == [f"{ROOT}__2026_04"]
    assert plan.detaches[0].reason is Reason.DETACH_FINALIZE
    assert plan.detaches[0].oid == 4
    assert plan.creates == ()
    assert plan.drops == ()
    assert _reasons(plan) == [FindingReason.DETACH_PENDING]
    assert plan.findings[0].severity is Severity.INFO
    assert plan.findings[0].partition_name == f"{ROOT}__2026_04"
    assert "completed with FINALIZE" in plan.findings[0].detail


def test__plan_maintenance__detach_pending_child__not_finalized_outside_maintain_mode() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1)))
    root = _root(_month(2026, 4, oid=4, detach_pending=True), _month(2026, 8, oid=8))

    # Act
    plan = _plan(config, root, context=_context(mode=PlanMode.RECONCILE))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.DETACH_PENDING]


def test__plan_maintenance__default_partition__ignored_for_windows() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=1)))
    root = _root(PartitionNode(name=f"{ROOT}__default", parent_name=ROOT, bounds=DefaultBounds(), oid=1))

    # Act
    plan = _plan(config, root)

    # Assert
    assert _targets(plan.operations) == [f"{ROOT}__2026_08"]
    assert plan.findings == ()


def test__plan_maintenance__root_that_is_a_plain_table__legacy_leaf() -> None:
    # Arrange / Act
    plan = _plan(_config(), _root(partition_type=None))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.LEGACY_LEAF]
    assert plan.findings[0].partition_name == ROOT


def test__plan_maintenance__root_partitioned_on_another_column__column_mismatch() -> None:
    # Arrange / Act
    plan = _plan(_config(), _root(columns=("other",)))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.COLUMN_MISMATCH]
    assert "RANGE (other) but the scheme asks for (created_at)" in plan.findings[0].detail


def test__plan_maintenance__root_partitioned_by_another_method__strategy_mismatch() -> None:
    # Arrange / Act
    plan = _plan(_config(), _root(partition_type=PartitionType.HASH))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.STRATEGY_MISMATCH]


def test__plan_maintenance__composite_key__root_key_must_match_in_order() -> None:
    # Arrange
    config = _config(RangePartitioning(key=("created_at", "id"), boundaries=MONTHS))

    # Act
    matching = _plan(config, _root(columns=("created_at", "id")))
    mismatched = _plan(config, _root(columns=("id", "created_at")))

    # Assert
    assert _targets(matching.creates) == [f"{ROOT}__2026_08"]
    assert matching.creates[0].key_columns == ("created_at", "id")
    assert _reasons(mismatched) == [FindingReason.COLUMN_MISMATCH]


# ── Orphans ─────────────────────────────────────────────────────────────────────


def test__plan_maintenance__orphan_named_for_a_wanted_window__reattached_instead_of_created() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=2)))
    orphan = _orphan("events__2026_09", oid=9, detached_at=NOW - timedelta(days=1))

    # Act
    plan = _plan(config, _root(), orphans=(orphan,))

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_08"]
    assert plan.attaches == (
        AttachPartition(
            target=f"{ROOT}__2026_09",
            oid=9,
            parent_name=ROOT,
            bounds=RangeBounds(from_value="2026-09-01", to_value="2026-10-01"),
            key_columns=("created_at",),
            partition_by=None,
            reason=Reason.REATTACH,
            detail="detached partition covers 2026_09, which is wanted again",
        ),
    )
    assert plan.drops == ()


def test__plan_maintenance__orphan_reattached_under_a_nested_scheme__carries_how_it_partitions() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))
    orphan = _orphan("events__2026_08", oid=8)

    # Act
    plan = _plan(config, _root(), orphans=(orphan,))

    # Assert
    assert plan.creates == ()
    assert plan.attaches[0].partition_by == PartitionBy(method=PartitionType.HASH, columns=("tenant_id",))


def test__plan_maintenance__orphan_inside_retention__reattached_instead_of_dropped() -> None:
    # Arrange: retention grew from 3 to 12 months after May was detached.
    config = _config(lifecycle=_policy(retention=KeepNewest(count=12), drop=DropAfter(grace=timedelta(days=7))))
    orphan = _orphan("events__2026_05", oid=5, detached_at=NOW - timedelta(days=30))

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert plan.operations == (
        AttachPartition(
            target=f"{ROOT}__2026_05",
            oid=5,
            parent_name=ROOT,
            bounds=RangeBounds(from_value="2026-05-01", to_value="2026-06-01"),
            key_columns=("created_at",),
            partition_by=None,
            reason=Reason.REATTACH,
            detail="detached partition covers 2026_05, which is wanted again",
        ),
    )
    assert plan.findings == ()


def test__plan_maintenance__orphan_inside_retention__measured_by_a_retention_predicate() -> None:
    # Arrange: size-based retention; the orphan is small enough to be wanted back.
    config = _config(lifecycle=_policy(retention=SizeAbove(bytes=100), drop=DropAfter()))
    small = _orphan("events__2025_01", oid=1, facts=PartitionFacts(size_bytes=10))
    large = _orphan("events__2025_02", oid=2, facts=PartitionFacts(size_bytes=500))

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(small, large))

    # Assert
    assert _targets(plan.attaches) == [f"{ROOT}__2025_01"]
    assert _targets(plan.drops) == [f"{ROOT}__2025_02"]


def test__plan_maintenance__orphan_inside_retention_under_drop_never__left_alone() -> None:
    # Arrange
    config = _config(lifecycle=_policy(retention=KeepNewest(count=12), drop=DropNever()))
    orphan = _orphan("events__2026_05", oid=5, detached_at=NOW - timedelta(days=30))

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__orphan_whose_window_is_attached_already__not_reattached() -> None:
    # Arrange: May is attached under a legacy name; the orphan spells the same window.
    config = _config(lifecycle=_policy(retention=KeepNewest(count=12), drop=DropAfter()))
    may = _range_child("events_2026_may", "2026-05-01", "2026-06-01", oid=5)
    orphan = _orphan("events__2026_05", oid=55, detached_at=NOW - timedelta(days=30))

    # Act
    plan = _plan(config, _root(may, _month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert plan.attaches == ()
    assert _targets(plan.drops) == [f"{ROOT}__2026_05"]


def test__plan_maintenance__orphan_off_the_grid__never_reattached() -> None:
    # Arrange: a daily orphan left by an earlier finer config, inside retention.
    config = _config(lifecycle=_policy(retention=KeepNewest(count=12), drop=DropAfter()))
    orphan = _orphan("events__2026_05_10", oid=5, detached_at=NOW - timedelta(days=30))

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert plan.attaches == ()
    assert _targets(plan.drops) == [f"{ROOT}__2026_05_10"]


def test__plan_maintenance__orphan_past_its_grace__dropped_with_its_detach_instant() -> None:
    # Arrange
    detached_at = NOW - timedelta(days=30)
    config = _config(lifecycle=_policy(drop=DropAfter(grace=timedelta(days=7))))
    orphan = _orphan(
        "events__2025_01", oid=11, detached_at=detached_at, facts=PartitionFacts(size_bytes=99, row_estimate=2)
    )

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert plan.operations == (
        DropPartition(
            target=f"{ROOT}__2025_01",
            oid=11,
            reason=Reason.GRACE_ELAPSED,
            detail=f"detached at {detached_at.isoformat()}; grace of 7 days, 0:00:00 elapsed",
            detached_at=detached_at,
            follows_detach=False,
            size_bytes=99,
            row_estimate=2,
        ),
    )
    assert plan.findings == ()


def test__plan_maintenance__orphan_within_its_grace__reported_pending_not_dropped() -> None:
    # Arrange
    config = _config(lifecycle=_policy(drop=DropAfter(grace=timedelta(days=7))))
    orphan = _orphan("events__2025_02", oid=12, detached_at=NOW - timedelta(days=1))

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert plan.is_noop
    assert _reasons(plan) == [FindingReason.GRACE_PENDING]
    assert plan.findings[0].severity is Severity.INFO
    assert "is kept until 2026-09-03T00:00:00+00:00" in plan.findings[0].detail


def test__plan_maintenance__orphan_with_an_unknown_detach_instant__treated_as_past_its_grace() -> None:
    # Arrange
    config = _config(lifecycle=_policy(drop=DropAfter(grace=timedelta(days=365))))
    orphan = _orphan("events__2025_03", oid=13, detached_at=None)

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    (drop,) = plan.drops
    assert drop.reason is Reason.GRACE_ELAPSED
    assert drop.detached_at is None
    assert "unknown instant" in drop.detail


def test__plan_maintenance__zero_grace__orphan_dropped_the_run_it_is_found() -> None:
    # Arrange
    orphan = _orphan("events__2025_03", oid=13, detached_at=NOW - timedelta(minutes=1))

    # Act
    plan = _plan(_config(), _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert _targets(plan.drops) == [f"{ROOT}__2025_03"]


@pytest.mark.parametrize(("size", "dropped"), [(10, False), (200, True)], ids=["deferred", "dropped"])
def test__plan_maintenance__orphan_drop_condition__deferred_until_it_holds(size: int, dropped: bool) -> None:
    # Arrange
    config = _config(lifecycle=_policy(drop=DropAfter(when=SizeAbove(bytes=100))))
    orphan = _orphan(
        "events__2025_03", oid=13, detached_at=NOW - timedelta(days=1), facts=PartitionFacts(size_bytes=size)
    )

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert _targets(plan.drops) == ([f"{ROOT}__2025_03"] if dropped else [])
    assert _reasons(plan) == ([] if dropped else [FindingReason.DROP_DEFERRED])
    if not dropped:
        assert plan.findings[0].severity is Severity.INFO
        assert "'size > 100 bytes' does not hold yet" in plan.findings[0].detail


def test__plan_maintenance__drop_never__orphans_left_alone_without_findings() -> None:
    # Arrange
    config = _config(lifecycle=_policy(drop=DropNever()))
    orphans = (
        _orphan("events__2026_03", oid=13, detached_at=NOW - timedelta(days=400)),
        _orphan("events__2025_01", oid=14, relkind=RelationKind.FOREIGN),
    )

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=orphans)

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__foreign_orphan__reported_not_dropped_and_not_reattached() -> None:
    # Arrange: its name encodes a wanted window, but a foreign table is never ours to attach.
    config = _config(lifecycle=_policy(creation=CreateAhead(count=2)))
    orphan = _orphan("events__2026_09", oid=14, relkind=RelationKind.FOREIGN)

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_09"]
    assert plan.attaches == ()
    assert plan.drops == ()
    assert _reasons(plan) == [FindingReason.FOREIGN_PARTITION]
    assert "not this library's to drop" in plan.findings[0].detail


def test__plan_maintenance__orphans_of_another_parent__ignored() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=2)))
    orphans = (
        _orphan("other__2026_09", parent="public.other", oid=1),
        _orphan("other__2026_01", parent="public.other", oid=2, detached_at=NOW - timedelta(days=400)),
    )

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=orphans)

    # Assert
    assert _targets(plan.operations) == [f"{ROOT}__2026_09"]
    assert plan.findings == ()


def test__plan_maintenance__orphan_whose_name_encodes_no_window__dropped_once_past_its_grace() -> None:
    # Arrange
    orphan = _orphan("events_archive_2019", oid=3, detached_at=NOW - timedelta(days=2))

    # Act
    plan = _plan(_config(), _root(_month(2026, 8, oid=8)), orphans=(orphan,))

    # Assert
    assert _targets(plan.drops) == [f"{ROOT}_archive_2019"]
    assert plan.attaches == ()


def test__plan_maintenance__two_orphans_for_one_window__only_the_first_is_reattached() -> None:
    # Arrange: two detached tables both claim September; one under a different spelling cannot.
    config = _config(lifecycle=_policy(creation=CreateAhead(count=2)))
    orphans = (
        _orphan("events__2026_09", oid=1, detached_at=NOW - timedelta(days=1)),
        _orphan("events__2026_09", oid=2, detached_at=NOW - timedelta(days=1)),
    )

    # Act
    plan = _plan(config, _root(_month(2026, 8, oid=8)), orphans=orphans)

    # Assert
    assert [op.oid for op in plan.attaches] == [1]
    assert plan.drops == ()


# ── Modes ───────────────────────────────────────────────────────────────────────


def test__plan_maintenance__reconcile_mode__creates_nothing_ahead_and_expires_nothing() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=3), retention=KeepNewest(count=1)))
    root = _root(*_months(4, 5))
    orphan = _orphan("events__2026_01", oid=1, detached_at=NOW - timedelta(days=400))

    # Act
    plan = _plan(config, root, orphans=(orphan,), context=_context(mode=PlanMode.RECONCILE))

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_maintenance__reconcile_mode__fills_set_level_gaps_in_every_existing_window() -> None:
    # Arrange: an expired window and a future one both miss a bucket.
    config = _config(_range_level(_hash_level(modulus=2)), lifecycle=_policy(retention=KeepNewest(count=1)))
    april = _branch((2, 0), name=f"{ROOT}__2026_04", oid=4).model_copy(
        update={"bounds": RangeBounds(from_value="2026-04-01", to_value="2026-05-01")}
    )
    october = _branch((2, 1), name=f"{ROOT}__2026_10", oid=10).model_copy(
        update={"bounds": RangeBounds(from_value="2026-10-01", to_value="2026-11-01")}
    )
    root = _root(april, october)

    # Act
    plan = _plan(config, root, context=_context(mode=PlanMode.RECONCILE))

    # Assert
    assert _hash_created(plan) == [(f"{ROOT}__2026_04__h1", 2, 1), (f"{ROOT}__2026_10__h0", 2, 0)]
    assert {op.counts_as for op in plan.creates} == {"repaired"}
    assert plan.detaches == ()


def test__plan_maintenance__explicit_mode__creates_exactly_the_named_windows_and_expires_nothing() -> None:
    # Arrange
    config = _config(lifecycle=_policy(creation=CreateAhead(count=3), retention=KeepNewest(count=1)))
    root = _root(*_months(4, 8))
    context = _context(mode=PlanMode.EXPLICIT, explicit_windows={"created_at": (_window(2026, 8), _window(2027, 1))})

    # Act
    plan = _plan(config, root, context=context)

    # Assert
    assert _targets(plan.operations) == [f"{ROOT}__2027_01"]
    assert plan.creates[0].reason is Reason.EXPLICIT
    assert plan.creates[0].detail == "2027_01 under 'explicitly requested'"
    assert plan.findings == ()


def test__plan_maintenance__explicit_mode__windows_without_a_token__still_named_and_bounded() -> None:
    # Arrange: a window rebuilt from catalog bounds carries no Period.
    window = Window(start=datetime(2026, 11, 1, tzinfo=UTC), end=datetime(2026, 12, 1, tzinfo=UTC))
    context = _context(mode=PlanMode.EXPLICIT, explicit_windows={"created_at": (window,)})

    # Act
    plan = _plan(_config(), _root(), context=context)

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_11"]
    assert plan.creates[0].bounds == RangeBounds(from_value="2026-11-01", to_value="2026-12-01")


def test__plan_maintenance__explicit_mode__duplicate_windows__created_once_in_order() -> None:
    # Arrange
    windows = (_window(2026, 10), _window(2026, 9), _window(2026, 9))
    context = _context(mode=PlanMode.EXPLICIT, explicit_windows={"created_at": windows})

    # Act
    plan = _plan(_config(), _root(), context=context)

    # Assert
    assert _targets(plan.creates) == [f"{ROOT}__2026_09", f"{ROOT}__2026_10"]


def test__plan_maintenance__explicit_mode__windows_for_another_column__nothing_requested() -> None:
    # Arrange
    context = _context(mode=PlanMode.EXPLICIT, explicit_windows={"other": (_window(2026, 9),)})

    # Act
    plan = _plan(_config(), _root(), context=context)

    # Assert
    assert plan.is_noop


def test__plan_maintenance__explicit_mode__recurses_only_into_the_requested_windows() -> None:
    # Arrange: both windows miss a bucket; only August is requested.
    config = _config(_range_level(_hash_level(modulus=2)))
    april = _branch((2, 0), name=f"{ROOT}__2026_04", oid=4).model_copy(
        update={"bounds": RangeBounds(from_value="2026-04-01", to_value="2026-05-01")}
    )
    root = _root(april, _branch((2, 0), oid=8))
    context = _context(mode=PlanMode.EXPLICIT, explicit_windows={"created_at": (_window(2026, 8),)})

    # Act
    plan = _plan(config, root, context=context)

    # Assert
    assert _hash_created(plan) == [(f"{BRANCH}__h1", 2, 1)]


def test__plan_maintenance__explicit_mode__orphan_for_a_requested_window__reattached() -> None:
    # Arrange
    orphan = _orphan("events__2026_11", oid=11)
    context = _context(mode=PlanMode.EXPLICIT, explicit_windows={"created_at": (_window(2026, 11),)})

    # Act
    plan = _plan(_config(), _root(), orphans=(orphan,), context=context)

    # Assert
    assert _targets(plan.attaches) == [f"{ROOT}__2026_11"]
    assert plan.creates == ()


def test__plan_maintenance__explicit_mode__new_group_under_a_list_root__carries_the_requested_windows() -> None:
    # Arrange
    config = _config(_list_level(child=_range_level()), table_name="regions")
    root = _root(name="public.regions", partition_type=PartitionType.LIST, columns=("region",))
    context = _context(mode=PlanMode.EXPLICIT, explicit_windows={"created_at": (_window(2027, 1),)})

    # Act
    plan = _plan(config, root, context=context)

    # Assert
    assert _targets(plan.creates) == ["public.regions__eu", "public.regions__us"]
    assert _targets(plan.creates[0].children) == ["public.regions__eu__2027_01"]


def test__planning_context__defaults__maintain_mode_with_no_cursors_or_windows() -> None:
    # Arrange / Act
    context = PlanningContext(now=NOW)

    # Assert
    assert context.mode is PlanMode.MAINTAIN
    assert context.cursors == {}
    assert context.explicit_windows == {}


# ── Integer axis ────────────────────────────────────────────────────────────────


def _queue_config(**policy: Any) -> TablePartitionConfig:
    return _config(_range_level(key="msg_id", boundaries=STEPS), table_name="queue", lifecycle=_policy(**policy))


def _queue_root(*starts: int) -> PartitionNode:
    return _root(
        *(
            PartitionNode(
                name=f"public.queue__{start}",
                parent_name="public.queue",
                bounds=RangeBounds(from_value=str(start), to_value=str(start + 100_000)),
                oid=start // 100_000 + 1,
            )
            for start in starts
        ),
        name="public.queue",
        columns=("msg_id",),
    )


def test__plan_maintenance__integer_axis__cursor_read_from_the_context_by_leading_column() -> None:
    # Arrange
    config = _queue_config(creation=CreateAhead(count=2))
    context = _context(cursors={"msg_id": 450_000, "other": 1})

    # Act
    plan = _plan(config, _queue_root(), context=context)

    # Assert
    assert _targets(plan.creates) == ["public.queue__400000", "public.queue__500000"]
    assert plan.creates[0].bounds == RangeBounds(from_value="400000", to_value="500000")
    assert plan.creates[0].detail == "[400000, 500000) under 'create 2 ahead'"
    assert plan.cursors == {"msg_id": 450_000, "other": 1}


def test__plan_maintenance__integer_axis__missing_cursor__starts_at_the_origin() -> None:
    # Arrange
    config = _queue_config(creation=CreateAhead(count=2))

    # Act
    plan = _plan(config, _queue_root())

    # Assert
    assert _targets(plan.creates) == ["public.queue__0", "public.queue__100000"]


def test__plan_maintenance__integer_axis__negative_origin__spelled_with_a_leading_m() -> None:
    # Arrange
    boundaries = NumericBoundaries(step=100, origin=-1000)
    config = _config(_range_level(key="msg_id", boundaries=boundaries), table_name="queue")
    root = _root(name="public.queue", columns=("msg_id",))

    # Act
    plan = _plan(config, root, context=_context(cursors={"msg_id": -950}))

    # Assert
    assert _targets(plan.creates) == ["public.queue__m1000"]
    assert plan.creates[0].bounds == RangeBounds(from_value="-1000", to_value="-900")


def test__plan_maintenance__integer_axis__keep_behind_expires_windows_far_behind_the_cursor() -> None:
    # Arrange: the cursor window starts at 400 000; anything ending 200 000 or more before it expires.
    config = _queue_config(creation=CreateAhead(count=1), retention=KeepBehind(distance=200_000))

    # Act
    plan = _plan(
        config, _queue_root(0, 100_000, 200_000, 300_000, 400_000), context=_context(cursors={"msg_id": 450_000})
    )

    # Assert
    assert _targets(plan.detaches) == ["public.queue__0", "public.queue__100000"]
    assert _targets(plan.drops) == ["public.queue__0", "public.queue__100000"]
    assert plan.detaches[0].detail == "[0, 100000) expired under 'keep within 200000 of the cursor'"


def test__plan_maintenance__integer_axis__keep_for_never_expires() -> None:
    # Arrange
    config = _queue_config(creation=CreateAhead(count=1), retention=KeepFor(age=timedelta(0)))

    # Act
    plan = _plan(config, _queue_root(0, 400_000), context=_context(cursors={"msg_id": 450_000}))

    # Assert
    assert plan.is_noop


def test__plan_maintenance__integer_axis__orphan_named_by_its_start__reattached() -> None:
    # Arrange
    config = _queue_config(creation=CreateAhead(count=2))
    orphan = DetachedPartition(name="public.queue__500000", oid=6, parent_name="public.queue")

    # Act
    plan = _plan(config, _queue_root(400_000), orphans=(orphan,), context=_context(cursors={"msg_id": 450_000}))

    # Assert
    assert plan.attaches[0].bounds == RangeBounds(from_value="500000", to_value="600000")
    assert plan.creates == ()


def test__plan_maintenance__integer_axis__partition_off_the_step_grid__unmanaged() -> None:
    # Arrange
    config = _queue_config(creation=CreateAhead(count=1))
    root = _root(
        PartitionNode(
            name="public.queue__odd",
            parent_name="public.queue",
            bounds=RangeBounds(from_value="50000", to_value="150000"),
        ),
        name="public.queue",
        columns=("msg_id",),
    )

    # Act
    plan = _plan(config, root, context=_context(cursors={"msg_id": 450_000}))

    # Assert
    assert _targets(plan.creates) == ["public.queue__400000"]
    assert _reasons(plan) == [FindingReason.UNMANAGED_PARTITION]


# ── fact_targets and to_maintenance_issue ───────────────────────────────────────


def test__fact_targets__range_root__members_and_their_orphans_without_the_default() -> None:
    # Arrange
    root = _root(
        _month(2026, 7, oid=7),
        _month(2026, 8, oid=8),
        PartitionNode(name=f"{ROOT}__default", parent_name=ROOT, bounds=DefaultBounds()),
    )
    orphans = (
        _orphan("events__2026_01", oid=1),
        _orphan("other__2026_01", parent="public.other", oid=2),
        _orphan("events__2026_01", oid=1),
    )

    # Act
    targets = fact_targets(_config(), ActualTree(root=root, orphans=orphans))

    # Assert
    assert targets == (f"{ROOT}__2026_07", f"{ROOT}__2026_08", f"{ROOT}__2026_01")


def test__fact_targets__set_level_members__never_measured() -> None:
    # Arrange
    config = _config(_range_level(_hash_level(modulus=2)))
    tree = ActualTree(root=_root(_branch((2, 0), (2, 1), oid=8)))

    # Act / Assert
    assert fact_targets(config, tree) == (BRANCH,)


def test__fact_targets__progression_level_under_a_list_root__group_members_and_their_orphans() -> None:
    # Arrange
    config = _config(_list_level(child=_range_level()), table_name="regions")
    eu = PartitionNode(
        name="public.regions__eu",
        parent_name="public.regions",
        bounds=ListBounds(values=("de", "fr")),
        partition_type=PartitionType.RANGE,
        partition_columns=("created_at",),
        children=(_month(2026, 8, parent="public.regions__eu", oid=5),),
    )
    legacy = _list_child("public.regions", "us", "us")
    root = _root(eu, legacy, name="public.regions", partition_type=PartitionType.LIST, columns=("region",))
    orphans = (
        _orphan("regions__eu__2026_01", parent="public.regions__eu", oid=1),
        _orphan("regions__asia", parent="public.regions", oid=2),
    )

    # Act
    targets = fact_targets(config, ActualTree(root=root, orphans=orphans))

    # Assert: the root's own orphan sits under a set level and is never a candidate.
    assert targets == ("public.regions__eu__2026_08", "public.regions__eu__2026_01")


def test__fact_targets__static_root__nothing_to_measure() -> None:
    # Arrange
    config = _config(_hash_level(modulus=2, key="task_id"), table_name="tasks")
    root = _root(
        _bucket("public.tasks", 2, 0), name="public.tasks", partition_type=PartitionType.HASH, columns=("task_id",)
    )

    # Act / Assert
    assert fact_targets(config, ActualTree(root=root)) == ()


def test__to_maintenance_issue__actionable_finding__renders_as_a_reconcile_issue() -> None:
    # Arrange
    plan = _plan(_config(_range_level(_hash_level(modulus=2))), _root(_branch((2, 0), (4, 1))))
    finding = plan.findings[0]

    # Act
    issue = to_maintenance_issue(finding)

    # Assert
    assert issue.step is MaintenanceIssueStep.RECONCILE
    assert issue.partition_name == BRANCH
    assert issue.error == f"PartitionTopologyError: {finding.detail}"
    assert "inconsistent moduli" in issue.error


def test__to_maintenance_issue__any_finding__keeps_its_partition_name() -> None:
    # Arrange
    finding = Finding(partition_name=f"{ROOT}__2026_04", reason=FindingReason.GRACE_PENDING, detail="kept a while")

    # Act
    issue = to_maintenance_issue(finding)

    # Assert
    assert issue.partition_name == f"{ROOT}__2026_04"
    assert issue.error == "PartitionTopologyError: kept a while"
