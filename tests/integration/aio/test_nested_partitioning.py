"""Nested partition trees against a real PostgreSQL (async).

RANGE → HASH and RANGE → LIST subtrees, static HASH and LIST roots, LIST → RANGE
progressions inside groups, UUIDv7 boundaries, composite keys, and every shape
of existing tree the planner has to recognise rather than rebuild.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from itertools import pairwise
from typing import TYPE_CHECKING

import freezegun
import pytest
import pytest_asyncio
from sqlalchemy import text

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.boundaries import TimeBoundaries
from pg_partsmith.entities import MaintenanceIssueStep, PartitionType, Period, TablePartitionConfig
from pg_partsmith.events import PartitionEvent
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.lifecycle import CreateAhead, KeepNewest, LifecyclePolicy
from pg_partsmith.plan import FindingReason, Reason
from pg_partsmith.scheme import HashPartitioning, RangePartitioning
from pg_partsmith.strategies import WeekPeriodCalculator
from pg_partsmith.topology import ListBounds
from tests.integration.aio.support import (
    child_count,
    count_ddl,
    exec_sql,
    hash_children_of,
    is_attached,
    list_children_of,
    make_maintainer,
    make_service,
    make_table,
    range_children_of,
    relkind,
    routed_leaves,
    run_maintenance,
    scalar,
)
from tests.integration.nested_support import (
    BARE_UNIQUE_INDEX_TABLE_DDL,
    COMPOSITE_TABLE_DDL,
    EXPRESSION_TABLE_DDL,
    FROZEN_WEEK,
    HASH_ROOT_TABLE_DDL,
    IDENTITY_TABLE_DDL,
    LIST_ROOT_TABLE_DDL,
    LIST_TABLE_DDL,
    NEXT_WEEK_SUFFIX,
    NO_CONSTRAINT_TABLE_DDL,
    NULLABLE_COMPOSITE_TABLE_DDL,
    NULLABLE_LIST_TABLE_DDL,
    PREVIOUS_WEEK_BOUNDS,
    PREVIOUS_WEEK_SUFFIX,
    SORTABLE_ID_TABLE_DDL,
    TASKS_TABLE_DDL,
    TIERED_TABLE_DDL,
    TIMESTAMP_TABLE_DDL,
    TRANSPOSED_DEFAULT_TABLE_DDL,
    TWO_LEVEL_TABLE_DDL,
    UNCONSTRAINED_TABLE_DDL,
    UUID7_ORG_TABLE_DDL,
    UUID7_TABLE_DDL,
    WEEK_BOUNDS,
    WEEK_SUFFIX,
    composite_config,
    flat_config,
    glitchtip_config,
    hash_root_config,
    list_config,
    list_root_config,
    nested_config,
    nullable_composite_config,
    tasks_config,
    tiered_config,
    uuid7_codec,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


# ── Fixtures ────────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, TIMESTAMP_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def uuid_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, UUID7_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def uuid_org_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, UUID7_ORG_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def two_level_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, TWO_LEVEL_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def unconstrained_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, UNCONSTRAINED_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def identity_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, IDENTITY_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def list_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, LIST_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def hash_root_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, HASH_ROOT_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def tasks_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, TASKS_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def list_root_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, LIST_ROOT_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def tiered_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, TIERED_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def composite_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, COMPOSITE_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def nullable_composite_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, NULLABLE_COMPOSITE_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def expression_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, EXPRESSION_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def sortable_id_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, SORTABLE_ID_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def nullable_list_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, NULLABLE_LIST_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def transposed_default_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, TRANSPOSED_DEFAULT_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def bare_unique_index_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, BARE_UNIQUE_INDEX_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def no_constraint_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, NO_CONSTRAINT_TABLE_DDL):
        yield name


async def _build_branch(
    engine: AsyncEngine,
    table: str,
    *,
    modulus: int,
    remainders: tuple[int, ...],
    suffix: str = WEEK_SUFFIX,
    bounds: tuple[str, str] = WEEK_BOUNDS,
    attach: bool = True,
    hash_column: str = "tenant_id",
) -> str:
    """Create a branch with an arbitrary (possibly incomplete) bucket set."""
    branch = f"{table}{suffix}"
    await exec_sql(engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH ({hash_column})')
    for remainder in remainders:
        await exec_sql(
            engine,
            f'CREATE TABLE "{branch}__h{remainder}" PARTITION OF "{branch}" '
            f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})",
        )
    if attach:
        await exec_sql(
            engine,
            f"ALTER TABLE \"{table}\" ATTACH PARTITION \"{branch}\" FOR VALUES FROM ('{bounds[0]}') TO ('{bounds[1]}')",
        )
    return branch


async def _attach(engine: AsyncEngine, table: str, partition: str, bounds: tuple[str, str]) -> None:
    await exec_sql(
        engine,
        f"ALTER TABLE \"{table}\" ATTACH PARTITION \"{partition}\" FOR VALUES FROM ('{bounds[0]}') TO ('{bounds[1]}')",
    )


# ── A. Fresh creation ───────────────────────────────────────────────────────────


async def test__nested__fresh_table__creates_the_branch_and_every_bucket(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert result.success
    assert result.created_count == 1
    assert result.repaired_count == 0
    assert await relkind(db_engine, branch) == "p"
    assert await hash_children_of(db_engine, branch) == {
        f"{branch}__h0": (2, 0),
        f"{branch}__h1": (2, 1),
    }


async def test__nested__fresh_table__plan_nests_the_buckets_inside_the_branch(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    config = nested_config(table, modulus=2)

    # Act
    with freezegun.freeze_time(FROZEN_WEEK):
        plan = await make_service(db_engine).plan(config)

    # Assert: one lifecycle unit, its subtree nested and counted as such.
    branch = f"public.{table}{WEEK_SUFFIX}"
    assert [op.target for op in plan.creates] == [branch]
    assert plan.creates[0].lifecycle_unit is True
    assert plan.creates[0].reason is Reason.CREATE_AHEAD
    assert [(c.target, c.reason, c.counts_as) for c in plan.creates[0].children] == [
        (f"{branch}__h0", Reason.SUBTREE, "subtree"),
        (f"{branch}__h1", Reason.SUBTREE, "subtree"),
    ]
    assert plan.relation_count == 3


async def test__nested__fresh_table__branch_is_attached_to_the_root(db_engine: AsyncEngine, table: str) -> None:
    # Arrange / Act
    await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert
    assert await is_attached(db_engine, f"{table}{WEEK_SUFFIX}") is True


async def test__nested__fresh_table__rows_route_through_the_branch_into_a_leaf(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Act
    async with db_engine.begin() as conn:
        for tenant in range(1, 9):
            await conn.execute(
                text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )
    leaves = await routed_leaves(db_engine, table)

    # Assert: every row landed in one of the branch's own leaves.
    branch = f"{table}{WEEK_SUFFIX}"
    assert leaves <= {f"{branch}__h0", f"{branch}__h1"}
    assert leaves


async def test__nested__deeper_spec__builds_the_whole_two_level_subtree(
    db_engine: AsyncEngine, two_level_table: str
) -> None:
    # Arrange
    table = two_level_table
    config = nested_config(table, modulus=2, inner_modulus=2)

    # Act
    await run_maintenance(db_engine, config)

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert set(await hash_children_of(db_engine, branch)) == {f"{branch}__h0", f"{branch}__h1"}
    assert set(await hash_children_of(db_engine, f"{branch}__h0")) == {f"{branch}__h0__h0", f"{branch}__h0__h1"}


# ── B. Already complete ─────────────────────────────────────────────────────────


async def test__nested__second_run_on_a_converged_tree__executes_zero_ddl(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    await run_maintenance(db_engine, config)

    # Act
    with count_ddl(db_engine) as counter:
        result = await run_maintenance(db_engine, config)

    # Assert: nothing to do must cost nothing, so no heavy locks are taken.
    assert result.created_count == 0
    assert result.repaired_count == 0
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.is_noop
    assert counter.statements == []


# ── C. Missing hash child ───────────────────────────────────────────────────────


async def test__nested__branch_missing_one_bucket__creates_only_that_bucket(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    branch = await _build_branch(db_engine, table, modulus=4, remainders=(0, 1, 3))

    # Act
    with count_ddl(db_engine) as counter:
        result = await run_maintenance(db_engine, nested_config(table, modulus=4))

    # Assert
    assert result.repaired_count == 1
    assert result.created_count == 0
    assert await hash_children_of(db_engine, branch) == {
        f"{branch}__h0": (4, 0),
        f"{branch}__h1": (4, 1),
        f"{branch}__h2": (4, 2),
        f"{branch}__h3": (4, 3),
    }
    created = [s for s in counter.statements if s.startswith("CREATE TABLE")]
    assert len(created) == 1
    assert result.maintenance_plan is not None
    assert [(op.target, op.reason) for op in result.maintenance_plan.creates] == [
        (f"public.{branch}__h2", Reason.HASH_GAP)
    ]


async def test__nested__branch_missing_a_bucket__ingest_recovers_for_the_orphaned_hash_slice(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: find a tenant that hashes into the missing bucket.
    branch = await _build_branch(db_engine, table, modulus=4, remainders=(0, 1, 3))
    stranded_tenant = await scalar(
        db_engine,
        f"SELECT g FROM generate_series(1, 200) g WHERE satisfies_hash_partition("  # noqa: S608
        f"to_regclass('\"{branch}\"'), 4, 2, g::bigint) LIMIT 1",
    )
    assert stranded_tenant is not None

    # Act
    await run_maintenance(db_engine, nested_config(table, modulus=4))

    # Assert: the previously rejected row now has somewhere to go.
    async with db_engine.begin() as conn:
        await conn.execute(
            text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
            {"t": stranded_tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
        )
        routed = await conn.execute(
            text(f'SELECT tableoid::regclass::text FROM "{table}" WHERE tenant_id = :t'),  # noqa: S608
            {"t": stranded_tenant},
        )

    assert str(routed.scalar()) == f"{branch}__h2"


# ── D. Config modulus changed, historical set complete ──────────────────────────


async def test__nested__historical_complete_set_at_another_modulus__left_untouched(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: last week was built with 4 buckets; the config now asks for 2.
    old_branch = await _build_branch(
        db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 2, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert: an expected steady state, reported as information rather than an issue.
    assert result.repaired_count == 0
    assert result.issues == ()
    assert result.maintenance_plan is not None
    findings = {f.partition_name: f.reason for f in result.maintenance_plan.findings}
    assert findings == {f"public.{old_branch}": FindingReason.MODULUS_PRESERVED}
    assert await hash_children_of(db_engine, old_branch) == {f"{old_branch}__h{r}": (4, r) for r in range(4)}


async def test__nested__historical_complete_set_at_another_modulus__new_period_uses_the_new_count(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _build_branch(
        db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 2, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert: rolling the bucket count forward never rewrites history.
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert await hash_children_of(db_engine, new_branch) == {
        f"{new_branch}__h0": (2, 0),
        f"{new_branch}__h1": (2, 1),
    }


# ── E. Config modulus changed, historical set incomplete ────────────────────────


async def test__nested__historical_incomplete_set_at_another_modulus__repaired_at_its_own_modulus(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    old_branch = await _build_branch(
        db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert: filled at modulus 4, never at the configured 2.
    assert result.repaired_count == 1
    assert await hash_children_of(db_engine, old_branch) == {f"{old_branch}__h{r}": (4, r) for r in range(4)}
    assert result.maintenance_plan is not None
    repairs = [op for op in result.maintenance_plan.creates if op.target == f"public.{old_branch}__h2"]
    assert [op.reason for op in repairs] == [Reason.HASH_GAP_HISTORICAL_MODULUS]


async def test__nested__historical_incomplete_set__repair_is_not_reported_as_a_problem(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    old_branch = await _build_branch(
        db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.issues == ()
    assert result.maintenance_plan is not None
    findings = {f.partition_name: f.reason for f in result.maintenance_plan.findings}
    assert findings == {f"public.{old_branch}": FindingReason.MODULUS_REPAIRED}


# ── F. Inconsistent moduli ──────────────────────────────────────────────────────


async def _build_gapped_mixed_branch(engine: AsyncEngine, table: str) -> str:
    """(2,0) plus (4,1) leaves residue 3 (mod 4) unowned."""
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await exec_sql(engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    await exec_sql(
        engine,
        f'CREATE TABLE "{branch}__h0" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 2, REMAINDER 0)',
    )
    await exec_sql(
        engine,
        f'CREATE TABLE "{branch}__h1" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 4, REMAINDER 1)',
    )
    await _attach(engine, table, branch, PREVIOUS_WEEK_BOUNDS)
    return branch


async def test__nested__hash_children_with_a_gap_across_moduli__reported_and_not_mutated(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    branch = await _build_gapped_mixed_branch(db_engine, table)
    before = await hash_children_of(db_engine, branch)

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert
    assert await hash_children_of(db_engine, branch) == before
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]
    assert len(issues) == 1
    assert issues[0].step == MaintenanceIssueStep.RECONCILE
    assert "inconsistent moduli" in issues[0].error
    assert result.maintenance_plan is not None
    reasons = {f.reason for f in result.maintenance_plan.findings if f.partition_name == f"public.{branch}"}
    assert reasons == {FindingReason.NON_UNIFORM_INCOMPLETE}


async def test__nested__inconsistent_branch__does_not_stop_the_rest_of_the_run(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _build_gapped_mixed_branch(db_engine, table)

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert: the current week is still created normally.
    assert result.success
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert len(await hash_children_of(db_engine, new_branch)) == 2


async def test__nested__mixed_moduli_that_still_tile__left_alone_without_an_issue(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: (2,1) plus (4,0) and (4,2) covers the whole keyspace.
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await exec_sql(db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    for modulus, remainder in ((2, 1), (4, 0), (4, 2)):
        await exec_sql(
            db_engine,
            f'CREATE TABLE "{branch}__h{modulus}_{remainder}" PARTITION OF "{branch}" '
            f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})",
        )
    await _attach(db_engine, table, branch, PREVIOUS_WEEK_BOUNDS)

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert
    assert [i for i in result.issues if i.partition_name == f"public.{branch}"] == []
    assert len(await hash_children_of(db_engine, branch)) == 3
    assert result.maintenance_plan is not None
    reasons = {f.reason for f in result.maintenance_plan.findings if f.partition_name == f"public.{branch}"}
    assert reasons == {FindingReason.NON_UNIFORM_COMPLETE}


# ── G. Unexpected subpartition strategy ─────────────────────────────────────────


async def test__nested__branch_subpartitioned_by_list__reported_without_hash_ddl(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await exec_sql(db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY LIST (tenant_id)')
    await exec_sql(db_engine, f'CREATE TABLE "{branch}__eu" PARTITION OF "{branch}" FOR VALUES IN (1, 2)')
    await _attach(db_engine, table, branch, PREVIOUS_WEEK_BOUNDS)

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.success
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]
    assert len(issues) == 1
    assert "LIST" in issues[0].error
    assert result.maintenance_plan is not None
    reasons = {f.reason for f in result.maintenance_plan.findings if f.partition_name == f"public.{branch}"}
    assert reasons == {FindingReason.STRATEGY_MISMATCH}
    assert set(await hash_children_of(db_engine, branch)) == set()  # no hash children were added


# ── H. Legacy leaf ──────────────────────────────────────────────────────────────


async def test__nested__legacy_leaf_partition__left_valid_and_untouched(db_engine: AsyncEngine, table: str) -> None:
    # Arrange: a partition created under the old flat policy.
    legacy = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await exec_sql(db_engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL)')
    await _attach(db_engine, table, legacy, PREVIOUS_WEEK_BOUNDS)

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert: still a plain, writable leaf; recognised, not reported as a problem.
    assert result.success
    assert result.issues == ()
    assert result.maintenance_plan is not None
    findings = {f.partition_name: f.reason for f in result.maintenance_plan.findings}
    assert findings == {f"public.{legacy}": FindingReason.LEGACY_LEAF}
    assert await relkind(db_engine, legacy) == "r"

    async with db_engine.begin() as conn:
        await conn.execute(
            text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (1, :d)'),  # noqa: S608
            {"d": datetime(2026, 8, 18, 10, tzinfo=UTC)},
        )
        routed = await conn.execute(text(f'SELECT tableoid::regclass::text FROM "{table}"'))  # noqa: S608
    assert str(routed.scalar()) == legacy


async def test__nested__legacy_leaf_present__new_periods_still_get_the_new_topology(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    legacy = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await exec_sql(db_engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL)')
    await _attach(db_engine, table, legacy, PREVIOUS_WEEK_BOUNDS)

    # Act
    await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert await relkind(db_engine, new_branch) == "p"
    assert len(await hash_children_of(db_engine, new_branch)) == 2


# ── I. Partial failure ──────────────────────────────────────────────────────────


async def test__nested__branch_created_but_never_attached__next_run_completes_and_attaches_it(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: exactly what an interrupted run leaves behind.
    branch = await _build_branch(db_engine, table, modulus=2, remainders=(0,), attach=False)

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.success
    assert result.created_count == 1
    assert result.repaired_count == 1
    assert await hash_children_of(db_engine, branch) == {f"{branch}__h0": (2, 0), f"{branch}__h1": (2, 1)}
    assert await is_attached(db_engine, branch) is True


async def test__nested__branch_is_attached_only_after_all_its_buckets_exist(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=4)
    branch = f"{table}{WEEK_SUFFIX}".upper()

    # Act
    with count_ddl(db_engine) as ddl:
        await run_maintenance(db_engine, config)

    # Assert: the ordering is the whole recovery story. A branch becomes
    # reachable at the moment it is attached, and a reachable branch missing a
    # bucket rejects every row that hashes into it -- so the attach has to come
    # after the last bucket, not before the first.
    attach = next(
        index
        for index, stmt in enumerate(ddl.statements)
        if "ATTACH PARTITION" in stmt and branch in stmt and "__H" not in stmt.split("ATTACH PARTITION")[1]
    )
    buckets = [index for index, stmt in enumerate(ddl.statements) if f"{branch}__H" in stmt]
    assert len(buckets) >= 4
    assert max(buckets) < attach


# ── J/K. UUIDv7 boundaries (flat spelling, tenant-keyed) ────────────────────────


async def _partition_bound_expr(engine: AsyncEngine, name: str) -> str:
    value = await scalar(
        engine,
        "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = to_regclass(:n)",
        n=f'"{name}"',
    )
    return str(value)


async def test__uuid7__weekly_periods__branch_bounds_are_uuid_literals(db_engine: AsyncEngine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    config = nested_config(uuid_table, modulus=2, partition_column="id", codec=codec)

    # Act
    await run_maintenance(db_engine, config)

    # Assert
    bounds = await _partition_bound_expr(db_engine, f"{uuid_table}{WEEK_SUFFIX}")
    assert str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))) in bounds
    assert str(codec.min_uuid_for(datetime(2026, 8, 31, tzinfo=UTC))) in bounds


async def test__uuid7__adjacent_periods__bounds_meet_without_a_gap(db_engine: AsyncEngine, uuid_table: str) -> None:
    # Arrange
    config = nested_config(uuid_table, modulus=1, partition_column="id", create_ahead=2, codec=uuid7_codec())

    # Act
    await run_maintenance(db_engine, config)

    # Assert
    rows = await range_children_of(db_engine, uuid_table)
    assert rows[f"{uuid_table}{WEEK_SUFFIX}"][1] == rows[f"{uuid_table}{NEXT_WEEK_SUFFIX}"][0]


async def test__uuid7_with_hash__rows_route_to_the_expected_leaf(db_engine: AsyncEngine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    await run_maintenance(db_engine, nested_config(uuid_table, modulus=2, partition_column="id", codec=codec))

    branch = f"{uuid_table}{WEEK_SUFFIX}"

    # Act
    async with db_engine.begin() as conn:
        for tenant in (1, 2, 3, 4):
            row_id = codec.min_uuid_for(datetime(2026, 8, 26, 12, 0, tenant, tzinfo=UTC))
            await conn.execute(
                text(f'INSERT INTO "{uuid_table}" (id, tenant_id, occurred_at) VALUES (:i, :t, :d)'),  # noqa: S608
                {"i": row_id, "t": tenant, "d": datetime(2026, 8, 26, 12, tzinfo=UTC)},
            )
        result = await conn.execute(
            text(
                f'SELECT tenant_id, tableoid::regclass::text FROM "{uuid_table}" ORDER BY tenant_id'  # noqa: S608
            )
        )
        routed = {int(r[0]): str(r[1]) for r in result.fetchall()}

        expected = {}
        for tenant in (1, 2, 3, 4):
            check = await conn.execute(
                text("SELECT satisfies_hash_partition(to_regclass(:b), 2, 0, CAST(:t AS bigint))"),
                {"b": f'"{branch}"', "t": tenant},
            )
            expected[tenant] = f"{branch}__h0" if check.scalar() else f"{branch}__h1"

    # Assert: the time dimension picked the branch, the hash dimension the leaf.
    assert routed == expected


async def test__uuid7__retention__prunes_by_decoding_the_uuid_upper_bound(
    db_engine: AsyncEngine, uuid_table: str
) -> None:
    # Arrange: a branch two weeks old, with retention of one period.
    config = nested_config(uuid_table, modulus=1, partition_column="id", retention=1, codec=uuid7_codec())
    await run_maintenance(db_engine, config, at_time="2026-08-12")
    old_branch = f"{uuid_table}__2026_w33"
    assert await relkind(db_engine, old_branch) == "p"

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert: retention works on encoded bounds, which needs the codec to decode.
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert await relkind(db_engine, old_branch) is None


async def test__uuid7__is_partition_closed__decodes_the_encoded_upper_bound(
    db_engine: AsyncEngine, uuid_table: str
) -> None:
    # Arrange
    codec = uuid7_codec()
    await run_maintenance(
        db_engine, nested_config(uuid_table, modulus=1, partition_column="id", codec=codec), at_time="2026-08-12"
    )
    metadata = PostgresMetadataProvider(db_engine, boundary_codec=codec)

    # Act / Assert: the 2026-W33 branch closed long ago in real time.
    assert await metadata.is_partition_closed(f"{uuid_table}__2026_w33") is True


async def test__uuid7_without_codec__is_partition_closed__reports_false_instead_of_raising(
    db_engine: AsyncEngine, uuid_table: str
) -> None:
    # Arrange
    await run_maintenance(
        db_engine,
        nested_config(uuid_table, modulus=1, partition_column="id", codec=uuid7_codec()),
        at_time="2026-08-12",
    )

    # Act: a provider with no codec cannot read a UUID bound.
    plain = PostgresMetadataProvider(db_engine)

    # Assert: it must not try to cast one to a timestamp.
    assert await plain.is_partition_closed(f"{uuid_table}__2026_w33") is False


# ── UUIDv7 weekly root → HASH(organization_id), composed spelling ───────────────


async def test__uuid7_org__fresh_table__bounds_are_uuid_literals_on_week_boundaries(
    db_engine: AsyncEngine, uuid_org_table: str
) -> None:
    # Arrange
    codec = uuid7_codec()
    config = glitchtip_config(uuid_org_table, modulus=2)

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert
    branch = f"{uuid_org_table}{WEEK_SUFFIX}"
    assert result.created_count == 1
    bounds = await range_children_of(db_engine, uuid_org_table)
    assert bounds == {
        branch: (
            str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))),
            str(codec.min_uuid_for(datetime(2026, 8, 31, tzinfo=UTC))),
        )
    }
    assert await hash_children_of(db_engine, branch) == {f"{branch}_h0": (2, 0), f"{branch}_h1": (2, 1)}


async def test__uuid7_org__adjacent_periods__are_contiguous(db_engine: AsyncEngine, uuid_org_table: str) -> None:
    # Arrange / Act
    await run_maintenance(db_engine, glitchtip_config(uuid_org_table, create_ahead=3))

    # Assert: each window's upper bound is the next window's lower bound.
    bounds = await range_children_of(db_engine, uuid_org_table)
    ordered = sorted(bounds.values())
    assert len(ordered) == 3
    for (_, upper), (lower, _) in pairwise(ordered):
        assert upper == lower


async def test__uuid7_org__rows__route_by_tableoid_into_the_organization_bucket(
    db_engine: AsyncEngine, uuid_org_table: str
) -> None:
    # Arrange
    codec = uuid7_codec()
    await run_maintenance(db_engine, glitchtip_config(uuid_org_table, modulus=2))
    branch = f"{uuid_org_table}{WEEK_SUFFIX}"

    # Act
    async with db_engine.begin() as conn:
        for org in (10, 11, 12, 13):
            row_id = codec.min_uuid_for(datetime(2026, 8, 27, 9, 0, org, tzinfo=UTC))
            await conn.execute(
                text(f'INSERT INTO "{uuid_org_table}" (id, organization_id) VALUES (:i, :o)'),  # noqa: S608
                {"i": row_id, "o": org},
            )
        result = await conn.execute(
            text(f'SELECT organization_id, tableoid::regclass::text FROM "{uuid_org_table}"')  # noqa: S608
        )
        routed = {int(r[0]): str(r[1]) for r in result.fetchall()}
        expected = {}
        for org in (10, 11, 12, 13):
            check = await conn.execute(
                text("SELECT satisfies_hash_partition(to_regclass(:b), 2, 0, CAST(:o AS bigint))"),
                {"b": f'"{branch}"', "o": org},
            )
            expected[org] = f"{branch}_h0" if check.scalar() else f"{branch}_h1"

    # Assert
    assert routed == expected


async def test__uuid7_org__retention__decodes_the_uuid_upper_bound(db_engine: AsyncEngine, uuid_org_table: str) -> None:
    # Arrange
    config = glitchtip_config(uuid_org_table, retention=1)
    await run_maintenance(db_engine, config, at_time="2026-08-12")
    old_branch = f"{uuid_org_table}__2026_w33"
    assert await relkind(db_engine, old_branch) == "p"

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert await relkind(db_engine, old_branch) is None
    assert await relkind(db_engine, f"{old_branch}_h0") is None


async def test__uuid7_org__is_partition_closed__with_the_codec_on_the_provider(
    db_engine: AsyncEngine, uuid_org_table: str
) -> None:
    # Arrange
    await run_maintenance(db_engine, glitchtip_config(uuid_org_table), at_time="2026-08-12")
    metadata = PostgresMetadataProvider(db_engine, boundary_codec=uuid7_codec())

    # Act / Assert
    assert await metadata.is_partition_closed(f"{uuid_org_table}__2026_w33") is True
    assert await metadata.is_partition_closed(f"{uuid_org_table}__2026_w33", settle_seconds=10**9) is False


# ── Lifecycle of a whole branch ─────────────────────────────────────────────────


async def test__nested__expired_branch__detached_and_dropped_with_its_whole_subtree(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    config = nested_config(table, modulus=2, retention=1)
    await run_maintenance(db_engine, config, at_time="2026-08-10")
    old_branch = f"{table}__2026_w33"
    assert await relkind(db_engine, old_branch) == "p"

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert: the branch is the lifecycle unit; its leaves go with it.
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert await relkind(db_engine, old_branch) is None
    assert await relkind(db_engine, f"{old_branch}__h0") is None
    assert await relkind(db_engine, f"{old_branch}__h1") is None


async def test__nested__retention_counts_time_periods_not_leaves(db_engine: AsyncEngine, table: str) -> None:
    # Arrange: 4 buckets per week would exhaust a leaf-counted retention of 2.
    config = nested_config(table, modulus=4, retention=2)
    for week in ("2026-08-10", "2026-08-17", "2026-08-24"):
        await run_maintenance(db_engine, config, at_time=week)

    # Act / Assert: two weekly branches retained, not two hash leaves.
    assert await child_count(db_engine, table) == 2


async def test__nested__hooks__fire_once_for_the_branch_not_per_leaf(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    events: list[str] = []

    class RecordingHooks(BasePartitionLifecycleHooks):
        async def before_create(self, event: PartitionEvent) -> None:
            events.append(f"before_create:{event.partition.name}:{event.partition.subpartition_type}")

        async def after_create(self, event: PartitionEvent) -> None:
            events.append(f"after_create:{event.partition.name}")

        async def before_drop(self, event: PartitionEvent) -> None:
            events.append(f"before_drop:{event.partition.name}")

    config = nested_config(table, modulus=4, retention=1)
    maintainer = make_maintainer(db_engine, hooks=[RecordingHooks()])
    with freezegun.freeze_time("2026-08-10"):
        await maintainer.run_maintenance(config)

    # Act
    with freezegun.freeze_time(FROZEN_WEEK):
        await maintainer.run_maintenance(config)

    # Assert: cold-storage export sees one time slice, not four fragments.
    assert events == [
        f"before_create:public.{table}__2026_w33:hash",
        f"after_create:public.{table}__2026_w33",
        f"before_create:public.{table}{WEEK_SUFFIX}:hash",
        f"after_create:public.{table}{WEEK_SUFFIX}",
        f"before_drop:public.{table}__2026_w33",
    ]


# ── DEFAULT partition reconciliation ────────────────────────────────────────────


async def test__nested__rows_in_default__moved_into_the_new_branch_and_routed_to_leaves(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await exec_sql(db_engine, f'CREATE TABLE "{table}_default" PARTITION OF "{table}" DEFAULT')
    async with db_engine.begin() as conn:
        for tenant in range(1, 7):
            await conn.execute(
                text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert result.success
    async with db_engine.connect() as conn:
        rows = await conn.execute(
            text(f'SELECT tableoid::regclass::text, count(*) FROM "{table}" GROUP BY 1')  # noqa: S608
        )
        placement = {str(r[0]): int(r[1]) for r in rows.fetchall()}

    assert set(placement) <= {f"{branch}__h0", f"{branch}__h1"}
    assert sum(placement.values()) == 6


# ── Configuration validation ────────────────────────────────────────────────────


async def test__nested__hash_column_missing_from_primary_key__refused_before_any_ddl(
    db_engine: AsyncEngine, unconstrained_table: str
) -> None:
    # Arrange
    config = nested_config(unconstrained_table, modulus=2)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="tenant_id"):
        await run_maintenance(db_engine, config)

    assert await relkind(db_engine, f"{unconstrained_table}{WEEK_SUFFIX}") is None


# ── Backward compatibility ──────────────────────────────────────────────────────


async def test__flat_config_on_the_same_table__still_creates_a_plain_leaf(db_engine: AsyncEngine, table: str) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, flat_config(table))

    # Assert
    assert result.created_count == 1
    assert await relkind(db_engine, f"{table}{WEEK_SUFFIX}") == "r"


async def test__flat_config__maintenance_result_reports_no_repairs(db_engine: AsyncEngine, table: str) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, flat_config(table))

    # Assert
    assert result.repaired_count == 0
    assert result.issues == ()


# ── Tree introspection ──────────────────────────────────────────────────────────


async def test__get_partition_tree__nested_table__reports_levels_bounds_and_strategies(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await run_maintenance(db_engine, nested_config(table, modulus=2))
    metadata = PostgresMetadataProvider(db_engine)

    # Act
    tree = await metadata.get_partition_tree(table)

    # Assert
    assert tree is not None
    assert tree.partition_type == PartitionType.RANGE
    assert tree.partition_columns == ("created_at",)
    assert tree.oid is not None

    branch = tree.children[0]
    assert branch.level == 1
    assert branch.partition_type == PartitionType.HASH
    assert branch.partition_columns == ("tenant_id",)
    assert branch.bounds is not None
    assert branch.bounds.kind == "range"
    assert [c.level for c in branch.children] == [2, 2]
    assert all(c.is_leaf for c in branch.children)


async def test__get_partition_tree__unpartitioned_relation__returns_none(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    await exec_sql(db_engine, f'CREATE TABLE "{table}_plain" (i int)')
    metadata = PostgresMetadataProvider(db_engine)

    # Act / Assert
    try:
        assert await metadata.get_partition_tree(f"{table}_plain") is None
    finally:
        await exec_sql(db_engine, f'DROP TABLE IF EXISTS "{table}_plain"')


async def test__list_partitions__nested_table__reports_the_branch_as_subpartitioned(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await run_maintenance(db_engine, nested_config(table, modulus=2))
    metadata = PostgresMetadataProvider(db_engine)

    # Act
    partitions = await metadata.list_partitions(table)

    # Assert: the lifecycle still sees one partition per period.
    assert len(partitions) == 1
    assert partitions[0].subpartition_type == PartitionType.HASH
    assert partitions[0].is_subpartitioned is True


async def test__ensure_partition__existing_branch_with_a_gap__completes_it_for_the_writer(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    branch = await _build_branch(db_engine, table, modulus=4, remainders=(0, 1, 3))
    service = make_service(db_engine)

    # Act
    await service.ensure_partition(nested_config(table, modulus=4), Period(year=2026, week=35))

    # Assert
    assert len(await hash_children_of(db_engine, branch)) == 4


async def test__ensure_partition__existing_branch_with_a_gap__reports_nothing_created(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _build_branch(db_engine, table, modulus=4, remainders=(0, 1, 3))
    service = make_service(db_engine)

    # Act
    created = await service.ensure_partition(nested_config(table, modulus=4), Period(year=2026, week=35))

    # Assert: the window already had its partition; the repair is not a created window.
    assert created is None


# ── Adopting a tree built by another partitioner ────────────────────────────────


class LegacyNamedWeekCalculator(WeekPeriodCalculator):
    """Names weeks by their Monday's date, as a foreign partitioner would."""

    _NAME_PATTERN = re.compile(r"^(.+)_(\d{4})(\d{2})(\d{2})$")

    def format_partition_name(self, table_name: str, period: Period) -> str:
        return f"{table_name}_{period.to_date():%Y%m%d}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        monday = date(int(match.group(2)), int(match.group(3)), int(match.group(4)))
        iso_year, iso_week, _ = monday.isocalendar()
        return Period(year=iso_year, week=iso_week)


async def _adopt_foreign_branch(engine: AsyncEngine, table: str) -> str:
    """Create a branch named the way another tool would, with one bucket missing."""
    branch = f"{table}_20260824"
    await exec_sql(engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    await exec_sql(
        engine,
        f'CREATE TABLE "{branch}_h0" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 2, REMAINDER 0)',
    )
    await _attach(engine, table, branch, WEEK_BOUNDS)
    return branch


def _legacy_named_config(table: str) -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name=table,
        scheme=RangePartitioning(
            key="created_at",
            boundaries=TimeBoundaries(calculator=LegacyNamedWeekCalculator()),
            child=HashPartitioning(key="tenant_id", modulus=2, name_suffix="_h{remainder}"),
        ),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=12)),
    )


async def test__adoption__foreign_named_tree_with_a_custom_calculator__reconciled_without_recreating_anything(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: an existing tree this library did not create.
    branch = await _adopt_foreign_branch(db_engine, table)
    config = _legacy_named_config(table)

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert: the gap is filled in place, under the foreign naming convention.
    assert result.success
    assert result.created_count == 0
    assert result.repaired_count == 1
    assert set(await hash_children_of(db_engine, branch)) == {f"{branch}_h0", f"{branch}_h1"}


async def test__adoption__custom_calculator__names_new_periods_its_own_way(db_engine: AsyncEngine, table: str) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, _legacy_named_config(table))

    # Assert
    assert result.created_count == 1
    assert await relkind(db_engine, f"{table}_20260824") == "p"
    assert set(await hash_children_of(db_engine, f"{table}_20260824")) == {
        f"{table}_20260824_h0",
        f"{table}_20260824_h1",
    }


async def test__adoption__foreign_named_tree_with_the_default_calculator__recognised_by_its_bounds(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: the same tree, under names the default calculator cannot parse.
    branch = await _adopt_foreign_branch(db_engine, table)

    # Act
    result = await run_maintenance(db_engine, nested_config(table, modulus=2))

    # Assert: ownership is decided by the catalog bounds, not by the name, so
    # the week is recognised as covered and only its missing bucket is added --
    # under the configured naming -- rather than a second partition for the same
    # period being attempted.
    assert result.success
    assert result.created_count == 0
    assert result.repaired_count == 1
    assert await relkind(db_engine, f"{table}{WEEK_SUFFIX}") is None
    assert set(await hash_children_of(db_engine, branch)) == {f"{branch}_h0", f"{branch}__h1"}


# ── Backfill ────────────────────────────────────────────────────────────────────


async def test__backfill__past_periods__creates_each_branch_with_a_complete_bucket_set(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: data already in the table predates create-ahead's window.
    config = nested_config(table, modulus=2)
    boundaries = config.time_boundaries
    assert boundaries is not None
    calculator = boundaries.period_calculator
    service = make_service(db_engine)

    with freezegun.freeze_time(FROZEN_WEEK):
        current = calculator.current_period()
        past = [calculator.period_before(current, n) for n in (1, 2, 3)]

    # Act
    created = await service.ensure_partitions(config, past)

    # Assert: reported in chronological order, each with its whole subtree.
    assert [p.relname for p in created] == [
        f"{table}__2026_w32",
        f"{table}__2026_w33",
        f"{table}__2026_w34",
    ]
    assert all(p.subpartition_type == PartitionType.HASH for p in created)
    for suffix in ("__2026_w34", "__2026_w33", "__2026_w32"):
        branch = f"{table}{suffix}"
        assert await relkind(db_engine, branch) == "p"
        assert len(await hash_children_of(db_engine, branch)) == 2


async def test__backfill__rerun__is_idempotent(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    service = make_service(db_engine)
    periods = [Period(year=2026, week=30), Period(year=2026, week=31)]
    await service.ensure_partitions(config, periods)

    # Act
    with count_ddl(db_engine) as counter:
        created = await service.ensure_partitions(config, periods)

    # Assert
    assert created == []
    assert counter.statements == []


async def test__backfill__then_maintenance__past_and_future_coexist(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    await make_service(db_engine).ensure_partitions(config, [Period(year=2026, week=34)])

    # Act: create-ahead now runs over the current week.
    result = await run_maintenance(db_engine, config)

    # Assert: backfilled history is untouched, the current week is added.
    assert result.success
    assert result.created_count == 1
    assert await relkind(db_engine, f"{table}__2026_w34") == "p"
    assert await relkind(db_engine, f"{table}{WEEK_SUFFIX}") == "p"


# ── Identity columns ────────────────────────────────────────────────────────────


async def test__identity_root__flat_config__partition_is_created_and_attached(
    db_engine: AsyncEngine, identity_table: str
) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, flat_config(identity_table))

    # Assert
    assert result.success
    assert await relkind(db_engine, f"{identity_table}{WEEK_SUFFIX}") == "r"
    assert await is_attached(db_engine, f"{identity_table}{WEEK_SUFFIX}") is True


async def test__identity_root__nested_config__whole_branch_is_created(
    db_engine: AsyncEngine, identity_table: str
) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, nested_config(identity_table, modulus=2))

    # Assert
    branch = f"{identity_table}{WEEK_SUFFIX}"
    assert result.success
    assert await relkind(db_engine, branch) == "p"
    assert len(await hash_children_of(db_engine, branch)) == 2


async def test__identity_root__inserts__generate_ids_and_keep_generated_columns(
    db_engine: AsyncEngine, identity_table: str
) -> None:
    # Arrange
    await run_maintenance(db_engine, nested_config(identity_table, modulus=2))

    # Act
    async with db_engine.begin() as conn:
        for tenant in (1, 2, 3):
            await conn.execute(
                text(f'INSERT INTO "{identity_table}" (tenant_id, created_at, amount) VALUES (:t, :d, :a)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC), "a": tenant},
            )
        rows = await conn.execute(
            text(f'SELECT id, amount, doubled FROM "{identity_table}" ORDER BY id')  # noqa: S608
        )
        result = [(int(r[0]), int(r[1]), int(r[2])) for r in rows.fetchall()]

    # Assert: the parent's identity supplies ids through the partition, and
    # INCLUDING ALL still carried the generated column over.
    assert [r[0] for r in result] == [1, 2, 3]
    assert all(doubled == amount * 2 for _, amount, doubled in result)


# ── LIST subpartitioning ────────────────────────────────────────────────────────


async def test__list__fresh_table__creates_one_partition_per_group(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, list_config(list_table))

    # Assert
    branch = f"{list_table}{WEEK_SUFFIX}"
    assert result.success
    assert await relkind(db_engine, branch) == "p"
    assert await list_children_of(db_engine, branch) == {
        f"{branch}__eu": ("de", "fr"),
        f"{branch}__us": ("us",),
    }


async def test__list__include_default__adds_the_catch_all(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange / Act
    await run_maintenance(db_engine, list_config(list_table, include_default=True))

    # Assert
    branch = f"{list_table}{WEEK_SUFFIX}"
    assert await relkind(db_engine, f"{branch}__other") == "r"


async def test__list__rows_route_to_the_partition_owning_their_value(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange
    await run_maintenance(db_engine, list_config(list_table, include_default=True))
    branch = f"{list_table}{WEEK_SUFFIX}"

    # Act
    async with db_engine.begin() as conn:
        for region in ("de", "fr", "us", "jp"):
            await conn.execute(
                text(f'INSERT INTO "{list_table}" (region, tenant_id, created_at) VALUES (:r, 1, :d)'),  # noqa: S608
                {"r": region, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )
        rows = await conn.execute(
            text(f'SELECT region, tableoid::regclass::text FROM "{list_table}" ORDER BY region')  # noqa: S608
        )
        routed = {str(r[0]): str(r[1]) for r in rows.fetchall()}

    # Assert: an unconfigured value lands in DEFAULT rather than being rejected.
    assert routed == {
        "de": f"{branch}__eu",
        "fr": f"{branch}__eu",
        "us": f"{branch}__us",
        "jp": f"{branch}__other",
    }


async def test__list__second_run_on_a_converged_tree__executes_zero_ddl(
    db_engine: AsyncEngine, list_table: str
) -> None:
    # Arrange
    config = list_config(list_table, include_default=True)
    await run_maintenance(db_engine, config)

    # Act
    with count_ddl(db_engine) as counter:
        result = await run_maintenance(db_engine, config)

    # Assert
    assert result.repaired_count == 0
    assert counter.statements == []


async def test__list__branch_missing_a_group__creates_only_that_group(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange: only the EU partition exists so far.
    branch = f"{list_table}{WEEK_SUFFIX}"
    await exec_sql(db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    await exec_sql(db_engine, f"""CREATE TABLE "{branch}__eu" PARTITION OF "{branch}" FOR VALUES IN ('de', 'fr')""")
    await _attach(db_engine, list_table, branch, WEEK_BOUNDS)

    # Act
    result = await run_maintenance(db_engine, list_config(list_table))

    # Assert
    assert result.repaired_count == 1
    assert set(await list_children_of(db_engine, branch)) == {f"{branch}__eu", f"{branch}__us"}
    assert result.maintenance_plan is not None
    assert [op.reason for op in result.maintenance_plan.creates] == [Reason.LIST_GROUP_MISSING]


async def test__list__group_matched_by_values_under_a_foreign_name__left_alone(
    db_engine: AsyncEngine, list_table: str
) -> None:
    # Arrange: another tool created the same value set under a different name.
    branch = f"{list_table}{WEEK_SUFFIX}"
    await exec_sql(db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    await exec_sql(db_engine, f"""CREATE TABLE "{branch}__europe" PARTITION OF "{branch}" FOR VALUES IN ('de', 'fr')""")
    await exec_sql(db_engine, f"""CREATE TABLE "{branch}__usa" PARTITION OF "{branch}" FOR VALUES IN ('us')""")
    await _attach(db_engine, list_table, branch, WEEK_BOUNDS)

    # Act
    result = await run_maintenance(db_engine, list_config(list_table))

    # Assert: matched by the values they own, so nothing is duplicated.
    assert result.repaired_count == 0
    assert set(await list_children_of(db_engine, branch)) == {f"{branch}__europe", f"{branch}__usa"}


async def test__list__value_owned_by_another_partition__reported_and_not_mutated(
    db_engine: AsyncEngine, list_table: str
) -> None:
    # Arrange: "de" sits in a partition that is not the configured EU group.
    branch = f"{list_table}{WEEK_SUFFIX}"
    await exec_sql(db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    await exec_sql(db_engine, f"""CREATE TABLE "{branch}__dach" PARTITION OF "{branch}" FOR VALUES IN ('de', 'at')""")
    await _attach(db_engine, list_table, branch, WEEK_BOUNDS)

    # Act
    result = await run_maintenance(db_engine, list_config(list_table))

    # Assert: the non-conflicting group is still created; the clash is reported.
    assert result.success
    assert set(await list_children_of(db_engine, branch)) == {f"{branch}__dach", f"{branch}__us"}
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]
    assert len(issues) == 1
    assert "'de'" in issues[0].error
    assert result.maintenance_plan is not None
    reasons = {f.reason for f in result.maintenance_plan.findings if f.partition_name == f"public.{branch}"}
    assert reasons == {FindingReason.LIST_VALUES_CONFLICT}


async def test__list__over_hash__builds_and_routes_through_both_levels(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange: RANGE(created_at) -> LIST(region) -> HASH(tenant_id)
    await run_maintenance(db_engine, list_config(list_table, inner_modulus=2))
    branch = f"{list_table}{WEEK_SUFFIX}"

    # Act
    async with db_engine.begin() as conn:
        await conn.execute(
            text(f'INSERT INTO "{list_table}" (region, tenant_id, created_at) VALUES (:r, 1, :d)'),  # noqa: S608
            {"r": "de", "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
        )
        routed = await conn.execute(
            text(f'SELECT tableoid::regclass::text FROM "{list_table}"')  # noqa: S608
        )
        leaf = str(routed.scalar())

    # Assert
    assert set(await hash_children_of(db_engine, f"{branch}__eu")) == {f"{branch}__eu__h0", f"{branch}__eu__h1"}
    assert leaf in {f"{branch}__eu__h0", f"{branch}__eu__h1"}


async def test__list__expired_branch__dropped_with_its_whole_subtree(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange
    config = list_config(list_table, retention=1)
    await run_maintenance(db_engine, config, at_time="2026-08-10")
    old_branch = f"{list_table}__2026_w33"
    assert await relkind(db_engine, old_branch) == "p"

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert
    assert result.dropped_count == 1
    assert await relkind(db_engine, old_branch) is None
    assert await relkind(db_engine, f"{old_branch}__eu") is None


# ── Static roots (no time dimension) ────────────────────────────────────────────


async def test__hash_root__fresh_table__creates_every_bucket(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, hash_root_config(hash_root_table, modulus=4))

    # Assert: the table's own partitions, not a subtree inside a period.
    assert result.success
    assert result.created_count == 4
    assert await hash_children_of(db_engine, hash_root_table) == {f"{hash_root_table}__h{r}": (4, r) for r in range(4)}


async def test__hash_root__second_run__executes_zero_ddl(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange
    config = hash_root_config(hash_root_table, modulus=2)
    await run_maintenance(db_engine, config)

    # Act
    with count_ddl(db_engine) as counter:
        result = await run_maintenance(db_engine, config)

    # Assert
    assert result.created_count == 0
    assert counter.statements == []


async def test__hash_root__missing_bucket__is_repaired(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange: an incomplete set, as a partial migration would leave.
    for remainder in (0, 1, 3):
        await exec_sql(
            db_engine,
            f'CREATE TABLE "{hash_root_table}__h{remainder}" PARTITION OF "{hash_root_table}" '
            f"FOR VALUES WITH (MODULUS 4, REMAINDER {remainder})",
        )

    # Act
    result = await run_maintenance(db_engine, hash_root_config(hash_root_table, modulus=4))

    # Assert
    assert result.created_count == 1
    assert len(await hash_children_of(db_engine, hash_root_table)) == 4


async def test__hash_root__nothing_is_ever_pruned(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, hash_root_config(hash_root_table, modulus=2))

    # Assert: a static root has no periods, so nothing ages out of it.
    assert result.detached_count == 0
    assert result.dropped_count == 0


async def test__hash_root__rows_route_into_the_buckets(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange
    await run_maintenance(db_engine, hash_root_config(hash_root_table, modulus=2))

    # Act
    async with db_engine.begin() as conn:
        for tenant in range(1, 9):
            await conn.execute(
                text(f'INSERT INTO "{hash_root_table}" (tenant_id) VALUES (:t)'),  # noqa: S608
                {"t": tenant},
            )
    leaves = await routed_leaves(db_engine, hash_root_table)

    # Assert
    assert leaves <= {f"{hash_root_table}__h0", f"{hash_root_table}__h1"}
    assert leaves


async def test__hash_root__with_a_nested_level__builds_both(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange / Act: HASH(tenant_id) -> HASH(id)
    await run_maintenance(db_engine, hash_root_config(hash_root_table, modulus=2, inner_modulus=2))

    # Assert
    assert set(await hash_children_of(db_engine, hash_root_table)) == {
        f"{hash_root_table}__h0",
        f"{hash_root_table}__h1",
    }
    assert set(await hash_children_of(db_engine, f"{hash_root_table}__h0")) == {
        f"{hash_root_table}__h0__h0",
        f"{hash_root_table}__h0__h1",
    }


async def test__tasks_root__hash_by_task_id_modulus_8__creates_every_bucket(
    db_engine: AsyncEngine, tasks_table: str
) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, tasks_config(tasks_table, modulus=8))

    # Assert
    assert result.success
    assert result.created_count == 8
    assert result.detached_count == 0
    assert await hash_children_of(db_engine, tasks_table) == {f"{tasks_table}__h{r}": (8, r) for r in range(8)}


async def test__tasks_root__rows__route_into_the_buckets(db_engine: AsyncEngine, tasks_table: str) -> None:
    # Arrange
    await run_maintenance(db_engine, tasks_config(tasks_table, modulus=8))

    # Act
    await exec_sql(
        db_engine,
        f"INSERT INTO \"{tasks_table}\" (task_id, payload) SELECT g, 'x' FROM generate_series(1, 200) g",  # noqa: S608
    )
    leaves = await routed_leaves(db_engine, tasks_table)

    # Assert: two hundred ids spread over every bucket.
    assert leaves == {f"{tasks_table}__h{r}" for r in range(8)}


async def test__tasks_root__second_run__executes_zero_ddl(db_engine: AsyncEngine, tasks_table: str) -> None:
    # Arrange
    config = tasks_config(tasks_table, modulus=8)
    await run_maintenance(db_engine, config)

    # Act
    with count_ddl(db_engine) as counter:
        result = await run_maintenance(db_engine, config)

    # Assert
    assert result.created_count == 0
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.is_noop
    assert counter.statements == []


async def test__tasks_root__dropped_bucket__is_repaired_on_the_next_run(
    db_engine: AsyncEngine, tasks_table: str
) -> None:
    # Arrange
    config = tasks_config(tasks_table, modulus=8)
    await run_maintenance(db_engine, config)
    await exec_sql(db_engine, f'DROP TABLE "{tasks_table}__h3"')

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert
    assert result.created_count == 1
    assert result.maintenance_plan is not None
    assert [(op.target, op.reason) for op in result.maintenance_plan.creates] == [
        (f"public.{tasks_table}__h3", Reason.HASH_GAP)
    ]
    assert await hash_children_of(db_engine, tasks_table) == {f"{tasks_table}__h{r}": (8, r) for r in range(8)}


async def test__list_root__fresh_table__creates_one_partition_per_group(
    db_engine: AsyncEngine, list_root_table: str
) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, list_root_config(list_root_table, include_default=True))

    # Assert
    assert result.success
    assert result.created_count == 3
    assert await list_children_of(db_engine, list_root_table) == {
        f"{list_root_table}__eu": ("de", "fr"),
        f"{list_root_table}__us": ("us",),
    }
    assert await relkind(db_engine, f"{list_root_table}__other") == "r"


async def test__list_root__rows_route_by_value(db_engine: AsyncEngine, list_root_table: str) -> None:
    # Arrange
    await run_maintenance(db_engine, list_root_config(list_root_table, include_default=True))

    # Act
    async with db_engine.begin() as conn:
        for region in ("de", "us", "jp"):
            await conn.execute(
                text(f'INSERT INTO "{list_root_table}" (region) VALUES (:r)'),  # noqa: S608
                {"r": region},
            )
        rows = await conn.execute(
            text(f'SELECT region, tableoid::regclass::text FROM "{list_root_table}" ORDER BY region')  # noqa: S608
        )
        routed = {str(r[0]): str(r[1]) for r in rows.fetchall()}

    # Assert
    assert routed == {
        "de": f"{list_root_table}__eu",
        "us": f"{list_root_table}__us",
        "jp": f"{list_root_table}__other",
    }


async def test__static_root__time_based_config__refused_as_a_type_mismatch(
    db_engine: AsyncEngine, hash_root_table: str
) -> None:
    # Arrange
    service = make_service(db_engine)

    # Act / Assert: the config describes a RANGE root; the table is HASH.
    with pytest.raises(InvalidPartitionConfigError, match="type mismatch"):
        await service.create_future_partitions(flat_config(hash_root_table))
    with pytest.raises(InvalidPartitionConfigError, match="progression root"):
        await service.ensure_partitions(hash_root_config(hash_root_table), [Period(year=2026, week=35)])


# ── LIST root with a monthly progression inside each group ──────────────────────


async def test__tiered__fresh_table__creates_each_group_with_the_current_and_ahead_months_inside(
    db_engine: AsyncEngine, tiered_table: str
) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, tiered_config(tiered_table, create_ahead=2))

    # Assert: two group branches, each carrying its own two months.
    assert result.success
    assert result.created_count == 2
    for group in ("free", "paid"):
        branch = f"{tiered_table}__{group}"
        assert await relkind(db_engine, branch) == "p"
        assert set(await range_children_of(db_engine, branch)) == {f"{branch}__2026_08", f"{branch}__2026_09"}
    assert await list_children_of(db_engine, tiered_table) == {
        f"{tiered_table}__free": ("free",),
        f"{tiered_table}__paid": ("pro", "enterprise"),
    }


async def test__tiered__rows__route_into_the_group_then_the_month(db_engine: AsyncEngine, tiered_table: str) -> None:
    # Arrange
    await run_maintenance(db_engine, tiered_config(tiered_table, create_ahead=2))

    # Act
    async with db_engine.begin() as conn:
        for tier, day in (("free", 25), ("pro", 26), ("enterprise", 28)):
            await conn.execute(
                text(f'INSERT INTO "{tiered_table}" (tier, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tier, "d": datetime(2026, 8, day, 10, tzinfo=UTC)},
            )
        await conn.execute(
            text(f'INSERT INTO "{tiered_table}" (tier, created_at) VALUES (:t, :d)'),  # noqa: S608
            {"t": "free", "d": datetime(2026, 9, 3, 10, tzinfo=UTC)},
        )
        rows = await conn.execute(
            text(f'SELECT tier, created_at, tableoid::regclass::text FROM "{tiered_table}" ORDER BY created_at')  # noqa: S608
        )
        routed = [(str(r[0]), str(r[2])) for r in rows.fetchall()]

    # Assert
    assert routed == [
        ("free", f"{tiered_table}__free__2026_08"),
        ("pro", f"{tiered_table}__paid__2026_08"),
        ("enterprise", f"{tiered_table}__paid__2026_08"),
        ("free", f"{tiered_table}__free__2026_09"),
    ]


async def test__tiered__later_month__appears_inside_each_existing_group(
    db_engine: AsyncEngine, tiered_table: str
) -> None:
    # Arrange
    config = tiered_config(tiered_table, create_ahead=2)
    await run_maintenance(db_engine, config)

    # Act: a month later the horizon moves; the groups themselves already exist.
    result = await run_maintenance(db_engine, config, at_time="2026-09-15")

    # Assert
    assert result.success
    assert result.created_count == 2
    for group in ("free", "paid"):
        branch = f"{tiered_table}__{group}"
        assert set(await range_children_of(db_engine, branch)) == {
            f"{branch}__2026_08",
            f"{branch}__2026_09",
            f"{branch}__2026_10",
        }
    assert await child_count(db_engine, tiered_table) == 2


async def test__tiered__second_run__executes_zero_ddl(db_engine: AsyncEngine, tiered_table: str) -> None:
    # Arrange
    config = tiered_config(tiered_table)
    await run_maintenance(db_engine, config)

    # Act
    with count_ddl(db_engine) as counter:
        result = await run_maintenance(db_engine, config)

    # Assert
    assert result.created_count == 0
    assert counter.statements == []


async def test__tiered__old_month_inside_a_group__detached_and_dropped_by_retention(
    db_engine: AsyncEngine, tiered_table: str
) -> None:
    # Arrange: August and September exist in both groups; retention keeps two months.
    config = tiered_config(tiered_table, create_ahead=2, retention=2)
    await run_maintenance(db_engine, config)

    # Act: in October, August has aged out of every group.
    result = await run_maintenance(db_engine, config, at_time="2026-10-15")

    # Assert
    assert result.success
    assert result.detached_count == 2
    assert result.dropped_count == 2
    for group in ("free", "paid"):
        branch = f"{tiered_table}__{group}"
        assert await relkind(db_engine, f"{branch}__2026_08") is None
        assert set(await range_children_of(db_engine, branch)) == {
            f"{branch}__2026_09",
            f"{branch}__2026_10",
            f"{branch}__2026_11",
        }
    assert result.maintenance_plan is not None
    assert {op.parent_name for op in result.maintenance_plan.detaches} == {
        f"public.{tiered_table}__free",
        f"public.{tiered_table}__paid",
    }


# ── Composite partition keys ────────────────────────────────────────────────────


async def test__composite_key__fresh_table__creates_the_period_partition(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, composite_config(composite_table))

    # Assert
    assert result.success
    assert result.created_count == 1
    assert await relkind(db_engine, f"{composite_table}{WEEK_SUFFIX}") == "r"


async def test__composite_key__bounds_pad_trailing_columns_with_minvalue(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange
    await run_maintenance(db_engine, composite_config(composite_table))

    # Act
    bounds = await _partition_bound_expr(db_engine, f"{composite_table}{WEEK_SUFFIX}")

    # Assert
    assert bounds.count("MINVALUE") == 2
    assert "2026-08-24" in bounds
    assert "2026-08-31" in bounds


async def test__composite_key__rows_route_by_the_leading_column_alone(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange
    await run_maintenance(db_engine, composite_config(composite_table, create_ahead=2))
    branch = f"{composite_table}{WEEK_SUFFIX}"

    # Act: wildly different trailing values, same period.
    async with db_engine.begin() as conn:
        for tenant in (-9999, 0, 1, 999999):
            await conn.execute(
                text(f'INSERT INTO "{composite_table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )
    leaves = await routed_leaves(db_engine, composite_table)

    # Assert: the trailing column does not affect placement.
    assert leaves == {branch}


async def test__composite_key__second_run__executes_zero_ddl(db_engine: AsyncEngine, composite_table: str) -> None:
    # Arrange
    config = composite_config(composite_table)
    await run_maintenance(db_engine, config)

    # Act
    with count_ddl(db_engine) as counter:
        result = await run_maintenance(db_engine, config)

    # Assert
    assert result.created_count == 0
    assert counter.statements == []


async def test__composite_key__retention__prunes_by_the_leading_bound(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange
    config = composite_config(composite_table, retention=1)
    await run_maintenance(db_engine, config, at_time="2026-08-10")
    old = f"{composite_table}__2026_w33"
    assert await relkind(db_engine, old) == "r"

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert: parsing a composite bound yields the leading value, so retention
    # compares the same instant it would for a single-column key.
    assert result.dropped_count == 1
    assert await relkind(db_engine, old) is None


async def test__composite_key__introspection__reports_the_key_in_key_order(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange
    metadata = PostgresMetadataProvider(db_engine)

    # Act
    columns = await metadata.get_partition_columns(composite_table)

    # Assert: key order, which is not column order.
    assert columns == ("created_at", "tenant_id")


async def test__composite_key__config_disagreeing_with_the_table__refused(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange: the real key is (created_at, tenant_id).
    config = composite_config(composite_table, trailing=("id",))

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="key mismatch"):
        await run_maintenance(db_engine, config)


# ── Keys and bounds the catalog spells differently from the config ──────────────


async def test__nullable_trailing_key__null_row_in_default__attach_still_succeeds(
    db_engine: AsyncEngine, nullable_composite_table: str
) -> None:
    # Arrange: a DEFAULT partition holding one row that belongs to the upcoming
    # week and one whose NULL tenant keeps it in DEFAULT whatever its week.
    async with db_engine.begin() as conn:
        await conn.execute(
            text(f'CREATE TABLE "{nullable_composite_table}_default" PARTITION OF "{nullable_composite_table}" DEFAULT')
        )
        await conn.execute(
            text(f'INSERT INTO "{nullable_composite_table}" (tenant_id, created_at) VALUES (7, :d), (NULL, :d)'),  # noqa: S608
            {"d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
        )

    # Act
    result = await run_maintenance(db_engine, nullable_composite_config(nullable_composite_table))

    # Assert: moving the NULL row out would be rejected with the very error the
    # move exists to clear, and the retry would never converge.
    assert result.success
    assert await relkind(db_engine, f"{nullable_composite_table}{WEEK_SUFFIX}") == "r"

    async with db_engine.begin() as conn:
        rows = await conn.execute(
            text(
                f'SELECT tableoid::regclass::text, tenant_id FROM "{nullable_composite_table}" '  # noqa: S608
                f"ORDER BY tenant_id NULLS LAST"
            )
        )
        placed = [(str(r[0]), r[1]) for r in rows.fetchall()]

    assert placed == [
        (f"{nullable_composite_table}{WEEK_SUFFIX}", 7),
        (f"{nullable_composite_table}_default", None),
    ]


async def test__expression_partition_key__is_refused_rather_than_silently_shortened(
    db_engine: AsyncEngine, expression_table: str
) -> None:
    # Arrange
    metadata = PostgresMetadataProvider(db_engine)

    # Act / Assert: an expression key is recorded as attnum 0, and dropping that
    # position would report a shorter key than the table really has.
    with pytest.raises(InvalidPartitionConfigError, match="expression"):
        await metadata.get_partition_columns(expression_table)
    with pytest.raises(InvalidPartitionConfigError, match="expression"):
        await run_maintenance(db_engine, flat_config(expression_table))


async def test__date_shaped_bound_that_is_not_a_date__reports_not_closed(
    db_engine: AsyncEngine, sortable_id_table: str
) -> None:
    # Arrange: a sortable identifier whose prefix gets past any regex guard
    # worth writing and still fails the cast.
    partition = f"{sortable_id_table}__early"
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{partition}" PARTITION OF "{sortable_id_table}" '
        f"FOR VALUES FROM ('2020-01-01-aaaa') TO ('2026-08-28-a1b2c3')",
    )
    metadata = PostgresMetadataProvider(db_engine)

    # Act
    closed = await metadata.is_partition_closed(partition)

    # Assert: "not closed" is the documented answer for a bound this provider
    # cannot read. Raising out of a predicate is not.
    assert closed is False


async def test__list_partition_holding_null__is_not_read_as_the_string_null(
    db_engine: AsyncEngine, nullable_list_table: str
) -> None:
    # Arrange
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{nullable_list_table}__unknown" PARTITION OF "{nullable_list_table}" FOR VALUES IN (NULL)',
    )
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{nullable_list_table}__literal" PARTITION OF "{nullable_list_table}" FOR VALUES IN (\'NULL\')',
    )
    metadata = PostgresMetadataProvider(db_engine)

    # Act
    tree = await metadata.get_partition_tree(nullable_list_table)

    # Assert: reading NULL as the three-character string would make the planner
    # propose a partition PostgreSQL already has, on every run.
    assert tree is not None
    by_name = {c.name: c.bounds for c in tree.children}
    assert by_name[f"public.{nullable_list_table}__unknown"] == ListBounds(values=(), includes_null=True)
    assert by_name[f"public.{nullable_list_table}__literal"] == ListBounds(values=("NULL",))


# ── Fixes confirmed against the server, not against a mock of it ────────────────


async def test__default_conflict__default_partition_ordered_differently__values_are_not_transposed(
    db_engine: AsyncEngine, transposed_default_table: str
) -> None:
    # Arrange: a DEFAULT partition whose physical column order differs from the
    # root's. ATTACH permits it, because it matches columns by name.
    table = transposed_default_table
    async with db_engine.begin() as conn:
        await conn.execute(
            text(f'CREATE TABLE "{table}_default" (created_at TIMESTAMPTZ NOT NULL, note TEXT, label TEXT)')
        )
        await conn.execute(text(f'ALTER TABLE "{table}" ATTACH PARTITION "{table}_default" DEFAULT'))
        await conn.execute(
            text(f'INSERT INTO "{table}" VALUES (:d, :label, :note)'),  # noqa: S608
            {"d": datetime(2026, 8, 25, tzinfo=UTC), "label": "LABEL-A", "note": "NOTE-A"},
        )

    # Act: the DEFAULT holds a row belonging to the period being created, so
    # the reconcile-and-retry path runs unattended.
    result = await run_maintenance(db_engine, flat_config(table))

    # Assert: moving by position would put NOTE-A in label with the source row
    # already deleted, and report success while doing it.
    assert result.success
    async with db_engine.connect() as conn:
        row = (await conn.execute(text(f'SELECT label, note FROM "{table}{WEEK_SUFFIX}"'))).fetchone()  # noqa: S608
    assert row is not None
    assert (row[0], row[1]) == ("LABEL-A", "NOTE-A")


async def test__bare_unique_index__missing_the_hash_column__is_refused_before_any_ddl(
    db_engine: AsyncEngine, bare_unique_index_table: str
) -> None:
    # Arrange: uniqueness comes from an index with no constraint behind it.
    table = bare_unique_index_table
    await exec_sql(db_engine, f'CREATE UNIQUE INDEX ON "{table}" (id, created_at)')

    # Act / Assert: LIKE ... INCLUDING ALL copies the index, so PostgreSQL
    # rejects the branch exactly as it would a named constraint -- mid-run,
    # after other tables have already been changed.
    with pytest.raises(InvalidPartitionConfigError, match="tenant_id"):
        await run_maintenance(db_engine, nested_config(table, modulus=2))

    assert await relkind(db_engine, f"{table}{WEEK_SUFFIX}") is None


async def test__expression_key_branch__is_reported_rather_than_treated_as_a_match(
    db_engine: AsyncEngine, no_constraint_table: str
) -> None:
    # Arrange: a branch partitioned by HASH over an expression as well as the
    # configured column. The catalog names only the column, so a shortened key
    # compares equal to a one-column spec.
    branch = f"{no_constraint_table}{WEEK_SUFFIX}"
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{branch}" (LIKE "{no_constraint_table}" INCLUDING ALL EXCLUDING IDENTITY) '
        f"PARTITION BY HASH (tenant_id, (id + 1))",
    )
    await _attach(db_engine, no_constraint_table, branch, WEEK_BOUNDS)

    # Act
    result = await run_maintenance(db_engine, nested_config(no_constraint_table, modulus=2))

    # Assert: planning against it would build bounds of the wrong arity.
    assert result.success
    reasons = {issue.error for issue in result.issues}
    assert any("expression" in reason for reason in reasons)
    assert result.maintenance_plan is not None
    findings = {f.reason for f in result.maintenance_plan.findings if f.partition_name == f"public.{branch}"}
    assert findings == {FindingReason.COLUMN_MISMATCH}
