"""Convergence rules for subpartitioned branches, exercised without a database.

Each test names the desired-vs-actual state it pins down; the same states are
re-checked against a real PostgreSQL in
``tests/integration/*/test_nested_partitioning.py``.
"""

import pytest

from pg_partsmith.entities import MaintenanceIssueStep
from pg_partsmith.subpartition_plan import (
    TopologyFinding,
    TopologyReason,
    plan_new_subtree,
    plan_subpartitions,
    to_maintenance_issue,
)
from pg_partsmith.topology import (
    DefaultBounds,
    HashBounds,
    HashSubpartitionSpec,
    ListBounds,
    ListGroup,
    ListSubpartitionSpec,
    PartitionNode,
    PartitionType,
    RangeBounds,
)

BRANCH = "public.events__2026_w35"


def _spec(modulus: int = 4, **overrides: object) -> HashSubpartitionSpec:
    return HashSubpartitionSpec(column="tenant_id", modulus=modulus, **overrides)  # type: ignore[arg-type]


def _branch(
    *remainders_at_modulus: tuple[int, int],
    partition_type: PartitionType | None = PartitionType.HASH,
    columns: tuple[str, ...] = ("tenant_id",),
) -> PartitionNode:
    """Build a branch node with the given ``(modulus, remainder)`` children."""
    return PartitionNode(
        name=BRANCH,
        parent_name="public.events",
        level=1,
        bounds=RangeBounds(from_value="2026-08-24", to_value="2026-08-31"),
        partition_type=partition_type,
        partition_columns=columns if partition_type is not None else (),
        children=tuple(
            PartitionNode(
                name=f"{BRANCH}__h{remainder}",
                parent_name=BRANCH,
                level=2,
                bounds=HashBounds(modulus=modulus, remainder=remainder),
            )
            for modulus, remainder in remainders_at_modulus
        ),
    )


def _created(plan: object) -> list[tuple[str, int, int]]:
    return [(a.child_name, a.bounds.modulus, a.bounds.remainder) for a in plan.actions]  # type: ignore[attr-defined]


# ── A. Fresh creation ───────────────────────────────────────────────────────────


def test__plan_new_subtree__brand_new_branch__creates_every_bucket() -> None:
    # Arrange / Act
    actions = plan_new_subtree(_spec(modulus=2), BRANCH)

    # Assert
    assert [(a.child_name, a.bounds.modulus, a.bounds.remainder) for a in actions] == [
        (f"{BRANCH}__h0", 2, 0),
        (f"{BRANCH}__h1", 2, 1),
    ]


def test__plan_new_subtree__nested_spec__builds_children_before_their_parent_attaches() -> None:
    # Arrange
    spec = _spec(modulus=2, subpartition=HashSubpartitionSpec(column="shard_id", modulus=2))

    # Act
    actions = plan_new_subtree(spec, BRANCH)

    # Assert
    assert [a.child_name for a in actions] == [f"{BRANCH}__h0", f"{BRANCH}__h1"]
    assert [c.child_name for c in actions[0].children] == [f"{BRANCH}__h0__h0", f"{BRANCH}__h0__h1"]
    assert actions[0].count() == 3


def test__plan_subpartitions__branch_with_no_children__creates_the_configured_set() -> None:
    # Arrange
    node = _branch()

    # Act
    plan = plan_subpartitions(_spec(modulus=2), node)

    # Assert
    assert _created(plan) == [(f"{BRANCH}__h0", 2, 0), (f"{BRANCH}__h1", 2, 1)]
    assert plan.findings == ()


# ── B. Already complete ─────────────────────────────────────────────────────────


def test__plan_subpartitions__complete_set_at_the_configured_modulus__plans_nothing() -> None:
    # Arrange
    node = _branch((2, 0), (2, 1))

    # Act
    plan = plan_subpartitions(_spec(modulus=2), node)

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


# ── C. Missing hash child ───────────────────────────────────────────────────────


def test__plan_subpartitions__one_bucket_missing__creates_only_that_bucket() -> None:
    # Arrange
    node = _branch((4, 0), (4, 1), (4, 3))

    # Act
    plan = plan_subpartitions(_spec(modulus=4), node)

    # Assert
    assert _created(plan) == [(f"{BRANCH}__h2", 4, 2)]
    assert plan.findings == ()


# ── D. Config modulus changed, historical set complete ──────────────────────────


def test__plan_subpartitions__complete_set_at_another_modulus__left_untouched() -> None:
    # Arrange
    node = _branch(*[(4, r) for r in range(4)])

    # Act
    plan = plan_subpartitions(_spec(modulus=2), node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.MODULUS_PRESERVED]
    assert plan.actionable_findings == ()


# ── E. Config modulus changed, historical set incomplete ────────────────────────


def test__plan_subpartitions__incomplete_set_at_another_modulus__repaired_at_its_own_modulus() -> None:
    # Arrange
    node = _branch((4, 0), (4, 1), (4, 3))

    # Act
    plan = plan_subpartitions(_spec(modulus=2), node)

    # Assert: modulus 2 buckets would overlap the existing ones.
    assert _created(plan) == [(f"{BRANCH}__h2", 4, 2)]
    assert [f.reason for f in plan.findings] == [TopologyReason.MODULUS_REPAIRED]
    assert plan.actionable_findings == ()


# ── F. Inconsistent moduli ──────────────────────────────────────────────────────


def test__plan_subpartitions__mixed_moduli_leaving_a_gap__reported_and_untouched() -> None:
    # Arrange: (2,0) owns even residues, (4,1) owns one odd class; 3 is orphaned.
    node = _branch((2, 0), (4, 1))

    # Act
    plan = plan_subpartitions(_spec(modulus=2), node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.NON_UNIFORM_INCOMPLETE]
    assert plan.actionable_findings != ()


def test__plan_subpartitions__mixed_moduli_that_still_tile__left_alone_without_alarm() -> None:
    # Arrange
    node = _branch((2, 1), (4, 0), (4, 2))

    # Act
    plan = plan_subpartitions(_spec(modulus=2), node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.NON_UNIFORM_COMPLETE]
    assert plan.actionable_findings == ()


# ── G. Unexpected subpartition strategy ─────────────────────────────────────────


def test__plan_subpartitions__branch_subpartitioned_by_list__reported_and_untouched() -> None:
    # Arrange
    node = PartitionNode(
        name=BRANCH,
        partition_type=PartitionType.LIST,
        partition_columns=("region",),
        children=(PartitionNode(name=f"{BRANCH}__eu", bounds=ListBounds(values=("eu",))),),
    )

    # Act
    plan = plan_subpartitions(_spec(), node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.STRATEGY_MISMATCH]
    assert plan.actionable_findings != ()


def test__plan_subpartitions__hash_on_a_different_column__reported_and_untouched() -> None:
    # Arrange
    node = _branch((2, 0), (2, 1), columns=("region_id",))

    # Act
    plan = plan_subpartitions(_spec(modulus=2), node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.COLUMN_MISMATCH]
    assert plan.actionable_findings != ()


# ── H. Legacy leaf ──────────────────────────────────────────────────────────────


def test__plan_subpartitions__legacy_leaf_partition__left_valid_and_untouched() -> None:
    # Arrange
    node = _branch(partition_type=None)

    # Act
    plan = plan_subpartitions(_spec(), node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.LEGACY_LEAF]
    assert plan.actionable_findings == ()


# ── Nested levels ───────────────────────────────────────────────────────────────


def test__plan_subpartitions__gap_one_level_down__repaired_in_place() -> None:
    # Arrange: the outer set is complete, but one inner bucket is missing.
    inner = HashSubpartitionSpec(column="shard_id", modulus=2)
    spec = _spec(modulus=2, subpartition=inner)
    node = PartitionNode(
        name=BRANCH,
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=(
            PartitionNode(
                name=f"{BRANCH}__h0",
                bounds=HashBounds(modulus=2, remainder=0),
                partition_type=PartitionType.HASH,
                partition_columns=("shard_id",),
                children=(
                    PartitionNode(name=f"{BRANCH}__h0__h0", bounds=HashBounds(modulus=2, remainder=0)),
                    PartitionNode(name=f"{BRANCH}__h0__h1", bounds=HashBounds(modulus=2, remainder=1)),
                ),
            ),
            PartitionNode(
                name=f"{BRANCH}__h1",
                bounds=HashBounds(modulus=2, remainder=1),
                partition_type=PartitionType.HASH,
                partition_columns=("shard_id",),
                children=(PartitionNode(name=f"{BRANCH}__h1__h0", bounds=HashBounds(modulus=2, remainder=0)),),
            ),
        ),
    )

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert
    assert _created(plan) == [(f"{BRANCH}__h1__h1", 2, 1)]


def test__plan_subpartitions__custom_name_suffix__used_for_repairs_too() -> None:
    # Arrange: a branch adopted from a partitioner that named buckets _h0/_h1.
    spec = _spec(modulus=4, name_suffix="_h{remainder}")
    node = PartitionNode(
        name="public.events_20260824",
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=tuple(
            PartitionNode(name=f"public.events_20260824_h{r}", bounds=HashBounds(modulus=4, remainder=r))
            for r in (0, 1, 3)
        ),
    )

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert
    assert _created(plan) == [("public.events_20260824_h2", 4, 2)]


def test__plan_subpartitions__unqualified_branch_name__keeps_children_unqualified() -> None:
    # Arrange
    node = PartitionNode(name="events__2026_w35", partition_type=PartitionType.HASH, partition_columns=("tenant_id",))

    # Act
    plan = plan_subpartitions(_spec(modulus=1), node)

    # Assert
    assert _created(plan) == [("events__2026_w35__h0", 1, 0)]


# ── Reporting ───────────────────────────────────────────────────────────────────


def test__to_maintenance_issue__actionable_finding__renders_as_a_reconcile_issue() -> None:
    # Arrange
    plan = plan_subpartitions(_spec(modulus=2), _branch((2, 0), (4, 1)))

    # Act
    issue = to_maintenance_issue(plan.findings[0])

    # Assert
    assert issue.step == MaintenanceIssueStep.RECONCILE
    assert issue.partition_name == BRANCH
    assert issue.error.startswith("PartitionTopologyError: ")
    assert "inconsistent moduli" in issue.error


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        (TopologyReason.LEGACY_LEAF, False),
        (TopologyReason.MODULUS_PRESERVED, False),
        (TopologyReason.MODULUS_REPAIRED, False),
        (TopologyReason.NON_UNIFORM_COMPLETE, False),
        (TopologyReason.NON_UNIFORM_INCOMPLETE, True),
        (TopologyReason.STRATEGY_MISMATCH, True),
        (TopologyReason.COLUMN_MISMATCH, True),
        (TopologyReason.COVERAGE_UNKNOWN, True),
    ],
)
def test__topology_reason__actionability__matches_whether_a_human_must_intervene(
    reason: TopologyReason, expected: bool
) -> None:
    # Arrange
    finding = TopologyFinding(partition_name=BRANCH, reason=reason, detail="…")

    # Act / Assert
    assert finding.is_actionable is expected


# ── LIST levels ─────────────────────────────────────────────────────────────────


def _list_spec(**overrides: object) -> ListSubpartitionSpec:
    base: dict[str, object] = {
        "column": "region",
        "groups": (ListGroup(name="eu", values=("de", "fr")), ListGroup(name="us", values=("us",))),
    }
    base.update(overrides)
    return ListSubpartitionSpec(**base)  # type: ignore[arg-type]


def _list_branch(*children: PartitionNode) -> PartitionNode:
    return PartitionNode(
        name=BRANCH,
        parent_name="public.events",
        level=1,
        bounds=RangeBounds(from_value="2026-08-24", to_value="2026-08-31"),
        partition_type=PartitionType.LIST,
        partition_columns=("region",),
        children=children,
    )


def _list_child(name: str, *values: str) -> PartitionNode:
    return PartitionNode(name=f"{BRANCH}__{name}", bounds=ListBounds(values=values))


def test__plan_new_subtree__list_spec__creates_every_group() -> None:
    # Arrange / Act
    actions = plan_new_subtree(_list_spec(), BRANCH)

    # Assert
    assert [(a.child_name, a.bounds) for a in actions] == [
        (f"{BRANCH}__eu", ListBounds(values=("de", "fr"))),
        (f"{BRANCH}__us", ListBounds(values=("us",))),
    ]


def test__plan_new_subtree__list_spec_with_default__adds_the_catch_all_last() -> None:
    # Arrange / Act
    actions = plan_new_subtree(_list_spec(include_default=True), BRANCH)

    # Assert
    assert actions[-1].child_name == f"{BRANCH}__other"
    assert actions[-1].bounds == DefaultBounds()


def test__plan_subpartitions__list_branch_missing_a_group__creates_only_that_group() -> None:
    # Arrange
    node = _list_branch(_list_child("eu", "de", "fr"))

    # Act
    plan = plan_subpartitions(_list_spec(), node)

    # Assert
    assert [a.child_name for a in plan.actions] == [f"{BRANCH}__us"]
    assert plan.findings == ()


def test__plan_subpartitions__list_branch_already_complete__plans_nothing() -> None:
    # Arrange
    node = _list_branch(_list_child("eu", "de", "fr"), _list_child("us", "us"))

    # Act
    plan = plan_subpartitions(_list_spec(), node)

    # Assert
    assert plan.is_noop
    assert plan.findings == ()


def test__plan_subpartitions__list_group_matched_by_values_not_name__left_alone() -> None:
    # Arrange: a partition another tool named differently owns the same values.
    node = _list_branch(_list_child("europe", "de", "fr"), _list_child("us", "us"))

    # Act
    plan = plan_subpartitions(_list_spec(), node)

    # Assert: recognised, not duplicated under our own naming.
    assert plan.is_noop


def test__plan_subpartitions__list_value_already_owned_elsewhere__reported_not_created() -> None:
    # Arrange: "de" sits in a partition that does not match the configured group.
    node = _list_branch(_list_child("dach", "de", "at"))

    # Act
    plan = plan_subpartitions(_list_spec(), node)

    # Assert: only the non-conflicting group is created.
    assert [a.child_name for a in plan.actions] == [f"{BRANCH}__us"]
    assert [f.reason for f in plan.findings] == [TopologyReason.LIST_VALUES_CONFLICT]
    assert plan.actionable_findings != ()


def test__plan_subpartitions__list_default_missing__is_created() -> None:
    # Arrange
    node = _list_branch(_list_child("eu", "de", "fr"), _list_child("us", "us"))

    # Act
    plan = plan_subpartitions(_list_spec(include_default=True), node)

    # Assert
    assert [a.bounds for a in plan.actions] == [DefaultBounds()]


def test__plan_subpartitions__list_default_present__not_created_again() -> None:
    # Arrange
    node = _list_branch(
        _list_child("eu", "de", "fr"),
        _list_child("us", "us"),
        PartitionNode(name=f"{BRANCH}__other", bounds=DefaultBounds()),
    )

    # Act
    plan = plan_subpartitions(_list_spec(include_default=True), node)

    # Assert
    assert plan.is_noop


def test__plan_subpartitions__list_spec_against_a_hash_branch__reports_strategy_mismatch() -> None:
    # Arrange
    node = _branch((2, 0), (2, 1))

    # Act
    plan = plan_subpartitions(_list_spec(), node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.STRATEGY_MISMATCH]


def test__plan_subpartitions__hash_spec_against_a_list_branch__reports_strategy_mismatch() -> None:
    # Arrange
    node = _list_branch(_list_child("eu", "de"))

    # Act
    plan = plan_subpartitions(_spec(modulus=2), node)

    # Assert
    assert [f.reason for f in plan.findings] == [TopologyReason.STRATEGY_MISMATCH]


def test__plan_subpartitions__list_branch_that_is_a_legacy_leaf__left_untouched() -> None:
    # Arrange
    node = PartitionNode(name=BRANCH, bounds=RangeBounds(from_value="a", to_value="b"))

    # Act
    plan = plan_subpartitions(_list_spec(), node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.LEGACY_LEAF]


def test__plan_subpartitions__hash_under_list__gap_one_level_down_is_repaired() -> None:
    # Arrange: RANGE -> LIST(region) -> HASH(tenant_id), with one bucket missing.
    inner = HashSubpartitionSpec(column="tenant_id", modulus=2)
    spec = _list_spec(subpartition=inner)
    node = _list_branch(
        PartitionNode(
            name=f"{BRANCH}__eu",
            bounds=ListBounds(values=("de", "fr")),
            partition_type=PartitionType.HASH,
            partition_columns=("tenant_id",),
            children=(PartitionNode(name=f"{BRANCH}__eu__h0", bounds=HashBounds(modulus=2, remainder=0)),),
        ),
        PartitionNode(
            name=f"{BRANCH}__us",
            bounds=ListBounds(values=("us",)),
            partition_type=PartitionType.HASH,
            partition_columns=("tenant_id",),
            children=tuple(
                PartitionNode(name=f"{BRANCH}__us__h{r}", bounds=HashBounds(modulus=2, remainder=r)) for r in (0, 1)
            ),
        ),
    )

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert
    assert [a.child_name for a in plan.actions] == [f"{BRANCH}__eu__h1"]


def test__plan_new_subtree__list_over_hash__builds_both_levels() -> None:
    # Arrange
    spec = _list_spec(subpartition=HashSubpartitionSpec(column="tenant_id", modulus=2))

    # Act
    actions = plan_new_subtree(spec, BRANCH)

    # Assert
    assert [a.child_name for a in actions] == [f"{BRANCH}__eu", f"{BRANCH}__us"]
    assert [c.child_name for c in actions[0].children] == [f"{BRANCH}__eu__h0", f"{BRANCH}__eu__h1"]


# ── Levels below a preserved one ────────────────────────────────────────────────


def _nested_branch(*bucket_children: tuple[int, int, tuple[PartitionNode, ...]]) -> PartitionNode:
    """A hash branch whose buckets are themselves HASH-partitioned."""
    return PartitionNode(
        name=BRANCH,
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=tuple(
            PartitionNode(
                name=f"{BRANCH}__h{remainder}",
                bounds=HashBounds(modulus=modulus, remainder=remainder),
                partition_type=PartitionType.HASH,
                partition_columns=("shard_id",),
                children=grandchildren,
            )
            for modulus, remainder, grandchildren in bucket_children
        ),
    )


def test__plan_subpartitions__modulus_preserved__still_converges_the_levels_below() -> None:
    # Arrange: the branch tiles the keyspace at 4 while the config says 2, so
    # this level is left alone -- but its buckets have no children at all.
    inner = HashSubpartitionSpec(column="shard_id", modulus=2)
    spec = _spec(modulus=2, subpartition=inner)
    node = _nested_branch(*((4, r, ()) for r in range(4)))

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert: leaving *this* level's modulus alone says nothing about the ones
    # below it; abandoning them leaves the branch rejecting rows.
    assert [a.child_name for a in plan.actions] == [f"{BRANCH}__h{r}__h{i}" for r in range(4) for i in range(2)]
    assert [f.reason for f in plan.findings] == [TopologyReason.MODULUS_PRESERVED]


def test__plan_subpartitions__non_uniform_but_complete__still_converges_the_levels_below() -> None:
    # Arrange
    inner = HashSubpartitionSpec(column="shard_id", modulus=1)
    spec = _spec(modulus=2, subpartition=inner)
    node = _nested_branch((2, 1, ()), (4, 0, ()), (4, 2, ()))

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert
    assert len(plan.actions) == 3
    assert [f.reason for f in plan.findings] == [TopologyReason.NON_UNIFORM_COMPLETE]


# ── Names that cannot be used ───────────────────────────────────────────────────


def test__plan_subpartitions__target_name_held_by_a_mismatched_partition__reported_not_planned() -> None:
    # Arrange: the name the config would generate is taken by a LIST partition
    # owning different values, so no value overlap makes the planner want it.
    spec = _list_spec(groups=(ListGroup(name="eu", values=("eu",)),))
    node = _list_branch(_list_child("eu", "europe"))

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert: creating it would collide, and the collision is indistinguishable
    # from a lost race once PostgreSQL reports it.
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.NAME_UNUSABLE]
    assert plan.actionable_findings != ()


def test__plan_subpartitions__repair_name_would_exceed_the_identifier_limit__reported() -> None:
    # Arrange: repairing at the branch's own larger modulus needs more digits
    # than the config validator sized the name for.
    long_branch = "public." + "e" * 59
    spec = _spec(modulus=8)
    node = PartitionNode(
        name=long_branch,
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=tuple(
            PartitionNode(name=f"{long_branch}__h{r}", bounds=HashBounds(modulus=16, remainder=r)) for r in range(10)
        ),
    )

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert: PostgreSQL truncates at 63 bytes silently, so planning these
    # would collapse six buckets onto existing names.
    assert plan.is_noop
    assert {f.reason for f in plan.findings} == {TopologyReason.MODULUS_REPAIRED, TopologyReason.NAME_UNUSABLE}
    assert any(f.reason is TopologyReason.NAME_UNUSABLE and f.is_actionable for f in plan.findings)


def test__plan_subpartitions__branch_hiding_a_child__plans_nothing_and_says_so() -> None:
    # Arrange -- the branch really has four buckets, but one of them has a dot
    # in its name and was left out of the tree.
    spec = _spec(modulus=4)
    node = PartitionNode(
        name=BRANCH,
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        has_unaddressable_children=True,
        children=tuple(
            PartitionNode(name=f"{BRANCH}__h{r}", bounds=HashBounds(modulus=4, remainder=r)) for r in range(3)
        ),
    )

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert -- the missing bucket may be the hidden one, so proposing it would
    # conflict with a partition that already exists, on every single run.
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.COVERAGE_UNKNOWN]


def test__plan_new_subtree__name_would_exceed_the_identifier_limit__reported_not_dropped() -> None:
    # Arrange -- a repaired branch can need wider names than the config
    # validator budgeted for, and there is no node to hang the finding on.
    long_branch = "public." + "e" * 61
    spec = HashSubpartitionSpec(column="tenant_id", modulus=2, subpartition=None)
    findings: list[TopologyFinding] = []

    # Act
    actions = plan_new_subtree(spec, long_branch, findings)

    # Assert -- dropping this silently produced a partitioned relation with no
    # children at all, which was then attached and rejected every row.
    assert actions == ()
    assert [f.reason for f in findings] == [TopologyReason.NAME_UNUSABLE, TopologyReason.NAME_UNUSABLE]
    assert all(f.partition_name == long_branch for f in findings)


def test__plan_new_subtree__names_fit__plans_the_whole_subtree_with_no_findings() -> None:
    # Arrange
    spec = HashSubpartitionSpec(column="tenant_id", modulus=2)
    findings: list[TopologyFinding] = []

    # Act
    actions = plan_new_subtree(spec, BRANCH, findings)

    # Assert
    assert [a.child_name for a in actions] == [f"{BRANCH}__h0", f"{BRANCH}__h1"]
    assert findings == []


def test__plan_subpartitions__branch_partitioned_on_an_expression__reported_not_matched() -> None:
    # Arrange -- the catalog reports the named columns only, so a one-column
    # spec would compare equal to a two-position key and the mismatch guard
    # would never fire.
    spec = _spec(modulus=2)
    node = PartitionNode(
        name=BRANCH,
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        has_expression_key=True,
    )

    # Act
    plan = plan_subpartitions(spec, node)

    # Assert
    assert plan.is_noop
    assert [f.reason for f in plan.findings] == [TopologyReason.COLUMN_MISMATCH]
    assert "expression" in plan.findings[0].detail
