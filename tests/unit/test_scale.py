"""What reading and planning a very large partition tree costs.

Three claims the architecture rests on, pinned here so that a regression shows up
as a failing test rather than as a slow maintenance run in production:

* the catalog is read in a fixed number of statements, whatever the tree's size --
  never one query per partition (the sync provider is generated from the aio one,
  so one test covers both);
* planning is linear in the number of nodes, so a table with tens of thousands of
  leaves plans in well under a second;
* facts are gathered per lifecycle unit, never per leaf, so a nested tree does not
  multiply the metadata a policy asks for.

The time budget is deliberately loose -- some sixty times the measured cost on a
developer machine. It is here to catch a quadratic planner, not to benchmark CI.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.boundaries import TimeBoundaries, Window
from pg_partsmith.entities import TablePartitionConfig
from pg_partsmith.lifecycle import CreateAhead, DetachMode, DropAfter, KeepNewest, LifecyclePolicy
from pg_partsmith.periods import PartitionGranularity
from pg_partsmith.plan import MaintenancePlan
from pg_partsmith.planner import PlanningContext, fact_targets, plan_maintenance
from pg_partsmith.scheme import HashPartitioning, RangePartitioning
from pg_partsmith.topology import ActualTree, HashBounds, PartitionNode, PartitionType, RangeBounds

NOW = datetime(2026, 8, 28, tzinfo=UTC)
SCHEMA = "public"
RELNAME = "events"
ROOT = f"{SCHEMA}.{RELNAME}"
MONTHS = TimeBoundaries(granularity=PartitionGranularity.MONTH)
BUCKETS = HashPartitioning(key="tenant_id", modulus=8)
SCHEME = RangePartitioning(key="created_at", boundaries=MONTHS, child=BUCKETS)

BRANCHES = 2_000  # a monthly history reaching back to 1860
NODES = 1 + BRANCHES * (1 + BUCKETS.modulus)  # 18 001
BUDGET_SECONDS = 10.0


# ── helpers ─────────────────────────────────────────────────────────────────────


def _config(*, retention: int) -> TablePartitionConfig:
    return TablePartitionConfig(
        schema=SCHEMA,
        table_name=RELNAME,
        scheme=SCHEME,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1),
            retention=KeepNewest(count=retention),
            detach=DetachMode.AUTO,
            drop=DropAfter(),
        ),
    )


def _windows() -> list[Window]:
    """``BRANCHES`` monthly windows, ending with the one holding ``NOW``."""
    windows = [MONTHS.window_at(NOW)]
    for _ in range(BRANCHES - 1):
        windows.append(MONTHS.shift(windows[-1], -1))
    windows.reverse()
    return windows


def _tree(*, skip: tuple[str, int] | None = None) -> PartitionNode:
    """The whole grid as the catalog would report it, optionally one bucket short.

    Args:
        skip: ``(branch relname, remainder)`` of a bucket to leave out.
    """
    branches: list[PartitionNode] = []
    for oid, window in enumerate(_windows(), start=2):
        relname = MONTHS.child_name(RELNAME, window)
        name = f"{SCHEMA}.{relname}"
        from_value, to_value = MONTHS.literals(window)
        buckets = tuple(
            PartitionNode(
                name=f"{SCHEMA}.{BUCKETS.child_name(relname, remainder)}",
                parent_name=name,
                level=2,
                oid=oid * 100 + remainder,
                bounds=HashBounds(modulus=BUCKETS.modulus, remainder=remainder),
            )
            for remainder in range(BUCKETS.modulus)
            if skip != (relname, remainder)
        )
        branches.append(
            PartitionNode(
                name=name,
                parent_name=ROOT,
                level=1,
                oid=oid,
                bounds=RangeBounds(from_value=from_value, to_value=to_value),
                partition_type=PartitionType.HASH,
                partition_columns=("tenant_id",),
                children=buckets,
            )
        )
    return PartitionNode(
        name=ROOT,
        oid=1,
        partition_type=PartitionType.RANGE,
        partition_columns=("created_at",),
        children=tuple(branches),
    )


def _timed_plan(config: TablePartitionConfig, root: PartitionNode) -> tuple[MaintenancePlan, float]:
    started = time.perf_counter()
    plan = plan_maintenance(config, ActualTree(root=root), PlanningContext(now=NOW))
    return plan, time.perf_counter() - started


def _tree_rows(branches: int, modulus: int) -> list[SimpleNamespace]:
    """Catalog rows for a root of ``branches`` branches with ``modulus`` buckets each."""
    rows = [
        SimpleNamespace(
            level=0,
            oid=1,
            relkind="p",
            partition_schema=SCHEMA,
            partition_name=RELNAME,
            parent_schema=None,
            parent_name=None,
            boundaries=None,
            is_attached=True,
            detach_pending=False,
            partstrat="r",
            partition_columns=["created_at"],
            key_arity=1,
        )
    ]
    for index in range(branches):
        relname = f"{RELNAME}__b{index}"
        rows.append(
            SimpleNamespace(
                level=1,
                oid=1000 + index,
                relkind="p",
                partition_schema=SCHEMA,
                partition_name=relname,
                parent_schema=SCHEMA,
                parent_name=RELNAME,
                boundaries=f"FOR VALUES FROM ('{2000 + index}-01-01') TO ('{2001 + index}-01-01')",
                is_attached=True,
                detach_pending=False,
                partstrat="h",
                partition_columns=["tenant_id"],
                key_arity=1,
            )
        )
        rows.extend(
            SimpleNamespace(
                level=2,
                oid=1_000_000 + index * modulus + remainder,
                relkind="r",
                partition_schema=SCHEMA,
                partition_name=f"{relname}__h{remainder}",
                parent_schema=SCHEMA,
                parent_name=relname,
                boundaries=f"FOR VALUES WITH (modulus {modulus}, remainder {remainder})",
                is_attached=True,
                detach_pending=False,
                partstrat=None,
                partition_columns=None,
                key_arity=None,
            )
            for remainder in range(modulus)
        )
    return rows


def _engine_answering(*values: list[SimpleNamespace]) -> MagicMock:
    """An engine mock whose successive ``execute`` calls answer with ``values``."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.execution_options = AsyncMock(return_value=conn)
    results = []
    for value in values:
        result = MagicMock()
        result.fetchall.return_value = value
        results.append(result)
    conn.execute.side_effect = results

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = cm
    engine.begin.return_value = cm
    return engine


# ── reading the catalog ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("branches", [1, 2_000])
async def test__get_actual_tree__any_number_of_partitions__read_in_two_statements(branches: int) -> None:
    # Arrange -- one bulk query for the tree, one for the detached orphans, and that is all
    engine = _engine_answering(_tree_rows(branches, modulus=8), [])
    provider = PostgresMetadataProvider(engine)

    # Act
    tree = await provider.get_actual_tree(ROOT)

    # Assert
    assert tree is not None
    assert len(tree.root.children) == branches
    assert engine.connect.return_value.__aenter__.return_value.execute.await_count == 2


# ── planning ────────────────────────────────────────────────────────────────────


def test__plan_maintenance__converged_tree_of_eighteen_thousand_nodes__is_a_noop_within_the_budget() -> None:
    # Arrange -- nothing is missing and nothing has expired
    config = _config(retention=BRANCHES)
    root = _tree()
    assert 1 + len(root.children) + sum(len(branch.children) for branch in root.children) == NODES

    # Act
    plan, elapsed = _timed_plan(config, root)

    # Assert
    assert plan.is_noop
    assert plan.findings == ()
    assert elapsed < BUDGET_SECONDS


def test__plan_maintenance__one_missing_bucket_among_eighteen_thousand_nodes__creates_only_that_one() -> None:
    # Arrange -- a run that died between two CREATEs, thousands of periods ago
    config = _config(retention=BRANCHES)
    windows = _windows()
    stale = MONTHS.child_name(RELNAME, windows[len(windows) // 2])
    root = _tree(skip=(stale, 5))

    # Act
    plan, elapsed = _timed_plan(config, root)

    # Assert
    assert [op.target for op in plan.operations] == [f"{SCHEMA}.{BUCKETS.child_name(stale, 5)}"]
    assert plan.creates[0].bounds == HashBounds(modulus=BUCKETS.modulus, remainder=5)
    assert elapsed < BUDGET_SECONDS


def test__plan_maintenance__retention_over_a_large_tree__retires_branches_not_leaves() -> None:
    # Arrange -- retention counts lifecycle units, so a nested tree is not eight times as busy
    keep = 12
    config = _config(retention=keep)

    # Act
    plan, elapsed = _timed_plan(config, _tree())

    # Assert -- every expired branch is detached, and only the branch, never its buckets
    assert len(plan.detaches) == BRANCHES - keep
    assert all("__h" not in op.target for op in plan.detaches)
    assert elapsed < BUDGET_SECONDS


def test__fact_targets__large_tree__names_one_target_per_lifecycle_unit() -> None:
    # Arrange -- a size or row policy must never have to measure every leaf of every period
    config = _config(retention=12)

    # Act
    targets = fact_targets(config, ActualTree(root=_tree()))

    # Assert
    assert len(targets) == BRANCHES
    assert all("__h" not in target for target in targets)
