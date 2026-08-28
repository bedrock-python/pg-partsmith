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
    HashBounds,
    HashSubpartitionSpec,
    ListBounds,
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
