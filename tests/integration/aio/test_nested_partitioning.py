"""Nested RANGE → HASH partitioning against a real PostgreSQL (async)."""

from __future__ import annotations

import contextlib
import re
from collections.abc import AsyncGenerator
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import freezegun
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.maintainer import PartitionMaintainer
from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.entities import MaintenanceIssueStep, PartitionType, Period, TablePartitionConfig
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.strategies import WeekPeriodCalculator
from pg_partsmith.subpartition_plan import TopologyReason
from tests.integration.nested_support import (
    CHILD_BOUNDS_SQL,
    COMPOSITE_TABLE_DDL,
    FROZEN_WEEK,
    HASH_ROOT_TABLE_DDL,
    IDENTITY_TABLE_DDL,
    LIST_ROOT_TABLE_DDL,
    LIST_TABLE_DDL,
    NEXT_WEEK_SUFFIX,
    PREVIOUS_WEEK_BOUNDS,
    PREVIOUS_WEEK_SUFFIX,
    RELKIND_SQL,
    TIMESTAMP_TABLE_DDL,
    TWO_LEVEL_TABLE_DDL,
    UNCONSTRAINED_TABLE_DDL,
    UUID7_TABLE_DDL,
    WEEK_BOUNDS,
    WEEK_SUFFIX,
    DdlCounter,
    composite_config,
    ddl_counter,
    flat_config,
    hash_children,
    hash_root_config,
    list_children,
    list_config,
    list_root_config,
    nested_config,
    uuid7_codec,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pg_partsmith.boundaries import RangeBoundaryCodec

pytestmark = pytest.mark.integration


# ── Fixtures ────────────────────────────────────────────────────────────────────


async def _make_table(engine: AsyncEngine, ddl: str) -> AsyncGenerator[str, None]:
    table = f"nested_{uuid4().hex[:8]}"
    async with engine.begin() as conn:
        await conn.execute(text(ddl.format(table=table)))
    yield table
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))


@pytest_asyncio.fixture
async def table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, TIMESTAMP_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def uuid_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, UUID7_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def two_level_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, TWO_LEVEL_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def unconstrained_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, UNCONSTRAINED_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def identity_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, IDENTITY_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def list_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, LIST_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def hash_root_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, HASH_ROOT_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def list_root_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, LIST_ROOT_TABLE_DDL):
        yield name


@pytest_asyncio.fixture
async def composite_table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in _make_table(db_engine, COMPOSITE_TABLE_DDL):
        yield name


def _maintainer(engine: AsyncEngine, *, codec: RangeBoundaryCodec | None = None) -> PartitionMaintainer:
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine, boundary_codec=codec),
        locks=PostgresAdvisoryLockManager(engine),
        period_calculator=WeekPeriodCalculator(boundary_codec=codec),
    )
    return PartitionMaintainer(service)


async def _run(
    engine: AsyncEngine,
    config: TablePartitionConfig,
    *,
    at_time: str = FROZEN_WEEK,
    codec: RangeBoundaryCodec | None = None,
) -> object:
    with freezegun.freeze_time(at_time):
        return await _maintainer(engine, codec=codec).run_maintenance(config)


async def _children(engine: AsyncEngine, parent: str) -> dict[str, tuple[int, int]]:
    async with engine.connect() as conn:
        result = await conn.execute(text(CHILD_BOUNDS_SQL), {"parent": f'"{parent}"'})
        return hash_children(list(result.fetchall()))


async def _relkind(engine: AsyncEngine, name: str) -> str | None:
    async with engine.connect() as conn:
        result = await conn.execute(text(RELKIND_SQL), {"name": f'"{name}"'})
        value = result.scalar()
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


async def _exec(engine: AsyncEngine, sql: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(sql))


@contextlib.contextmanager
def _count_ddl(engine: AsyncEngine) -> Iterator[DdlCounter]:
    yield from ddl_counter(engine.sync_engine)


async def _list_children(engine: AsyncEngine, parent: str) -> dict[str, tuple[str, ...]]:
    """Map child relname -> the LIST values it owns."""
    async with engine.connect() as conn:
        result = await conn.execute(text(CHILD_BOUNDS_SQL), {"parent": f'"{parent}"'})
        rows = list(result.fetchall())
    return list_children(rows)


# ── A. Fresh creation ───────────────────────────────────────────────────────────


async def test__nested__fresh_table__creates_the_branch_and_every_bucket(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)

    # Act
    result = await _run(db_engine, config)

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert result.success  # type: ignore[attr-defined]
    assert result.created_count == 1  # type: ignore[attr-defined]
    assert await _relkind(db_engine, branch) == "p"
    assert await _children(db_engine, branch) == {
        f"{branch}__h0": (2, 0),
        f"{branch}__h1": (2, 1),
    }


async def test__nested__fresh_table__branch_is_attached_to_the_root(db_engine: AsyncEngine, table: str) -> None:
    # Arrange / Act
    await _run(db_engine, nested_config(table, modulus=2))

    # Act
    async with db_engine.connect() as conn:
        result = await conn.execute(
            text("SELECT relispartition FROM pg_class WHERE oid = to_regclass(:n)"),
            {"n": f'"{table}{WEEK_SUFFIX}"'},
        )

    # Assert
    assert result.scalar() is True


async def test__nested__fresh_table__rows_route_through_the_branch_into_a_leaf(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _run(db_engine, nested_config(table, modulus=2))

    # Act
    async with db_engine.begin() as conn:
        for tenant in range(1, 9):
            await conn.execute(
                text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )
        result = await conn.execute(text(f'SELECT DISTINCT tableoid::regclass::text FROM "{table}"'))  # noqa: S608
        leaves = {str(r[0]) for r in result.fetchall()}

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
    await _run(db_engine, config)

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert set(await _children(db_engine, branch)) == {f"{branch}__h0", f"{branch}__h1"}
    assert set(await _children(db_engine, f"{branch}__h0")) == {f"{branch}__h0__h0", f"{branch}__h0__h1"}


# ── B. Already complete ─────────────────────────────────────────────────────────


async def test__nested__second_run_on_a_converged_tree__executes_zero_ddl(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    await _run(db_engine, config)

    # Act
    with _count_ddl(db_engine) as counter:
        result = await _run(db_engine, config)

    # Assert: nothing to do must cost nothing, so no heavy locks are taken.
    assert result.created_count == 0  # type: ignore[attr-defined]
    assert result.repaired_count == 0  # type: ignore[attr-defined]
    assert counter.statements == []


# ── C. Missing hash child ───────────────────────────────────────────────────────


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
    await _exec(engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH ({hash_column})')
    for remainder in remainders:
        await _exec(
            engine,
            f'CREATE TABLE "{branch}__h{remainder}" PARTITION OF "{branch}" '
            f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})",
        )
    if attach:
        await _exec(
            engine,
            f"ALTER TABLE \"{table}\" ATTACH PARTITION \"{branch}\" FOR VALUES FROM ('{bounds[0]}') TO ('{bounds[1]}')",
        )
    return branch


async def test__nested__branch_missing_one_bucket__creates_only_that_bucket(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    branch = await _build_branch(db_engine, table, modulus=4, remainders=(0, 1, 3))

    # Act
    with _count_ddl(db_engine) as counter:
        result = await _run(db_engine, nested_config(table, modulus=4))

    # Assert
    assert result.repaired_count == 1  # type: ignore[attr-defined]
    assert await _children(db_engine, branch) == {
        f"{branch}__h0": (4, 0),
        f"{branch}__h1": (4, 1),
        f"{branch}__h2": (4, 2),
        f"{branch}__h3": (4, 3),
    }
    created = [s for s in counter.statements if s.startswith("CREATE TABLE")]
    assert len(created) == 1


async def test__nested__branch_missing_a_bucket__ingest_recovers_for_the_orphaned_hash_slice(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: find a tenant that hashes into the missing bucket.
    branch = await _build_branch(db_engine, table, modulus=4, remainders=(0, 1, 3))
    async with db_engine.connect() as conn:
        result = await conn.execute(
            text(
                f"SELECT g FROM generate_series(1, 200) g WHERE satisfies_hash_partition("  # noqa: S608
                f"to_regclass('\"{branch}\"'), 4, 2, g::bigint) LIMIT 1"
            )
        )
        stranded_tenant = result.scalar()
    assert stranded_tenant is not None

    # Act
    await _run(db_engine, nested_config(table, modulus=4))

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
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.repaired_count == 0  # type: ignore[attr-defined]
    assert result.issues == ()  # type: ignore[attr-defined]
    assert await _children(db_engine, old_branch) == {f"{old_branch}__h{r}": (4, r) for r in range(4)}


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
    await _run(db_engine, nested_config(table, modulus=2))

    # Assert: rolling the bucket count forward never rewrites history.
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert await _children(db_engine, new_branch) == {
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
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert: filled at modulus 4, never at the configured 2.
    assert result.repaired_count == 1  # type: ignore[attr-defined]
    assert await _children(db_engine, old_branch) == {f"{old_branch}__h{r}": (4, r) for r in range(4)}


async def test__nested__historical_incomplete_set__repair_is_not_reported_as_a_problem(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _build_branch(
        db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.issues == ()  # type: ignore[attr-defined]


# ── F. Inconsistent moduli ──────────────────────────────────────────────────────


async def test__nested__hash_children_with_a_gap_across_moduli__reported_and_not_mutated(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: (2,0) plus (4,1) leaves residue 3 (mod 4) unowned.
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await _exec(db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    await _exec(
        db_engine,
        f'CREATE TABLE "{branch}__h0" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 2, REMAINDER 0)',
    )
    await _exec(
        db_engine,
        f'CREATE TABLE "{branch}__h1" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 4, REMAINDER 1)',
    )
    await _exec(
        db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )
    before = await _children(db_engine, branch)

    # Act
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert
    assert await _children(db_engine, branch) == before
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]  # type: ignore[attr-defined]
    assert len(issues) == 1
    assert issues[0].step == MaintenanceIssueStep.RECONCILE
    assert TopologyReason.NON_UNIFORM_INCOMPLETE.value in issues[0].error or "inconsistent moduli" in issues[0].error


async def test__nested__inconsistent_branch__does_not_stop_the_rest_of_the_run(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await _exec(db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    await _exec(
        db_engine,
        f'CREATE TABLE "{branch}__h0" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 2, REMAINDER 0)',
    )
    await _exec(
        db_engine,
        f'CREATE TABLE "{branch}__h1" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 4, REMAINDER 1)',
    )
    await _exec(
        db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert: the current week is still created normally.
    assert result.success  # type: ignore[attr-defined]
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert len(await _children(db_engine, new_branch)) == 2


async def test__nested__mixed_moduli_that_still_tile__left_alone_without_an_issue(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: (2,1) plus (4,0) and (4,2) covers the whole keyspace.
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await _exec(db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    for modulus, remainder in ((2, 1), (4, 0), (4, 2)):
        await _exec(
            db_engine,
            f'CREATE TABLE "{branch}__h{modulus}_{remainder}" PARTITION OF "{branch}" '
            f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})",
        )
    await _exec(
        db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert
    assert [i for i in result.issues if i.partition_name == f"public.{branch}"] == []  # type: ignore[attr-defined]
    assert len(await _children(db_engine, branch)) == 3


# ── G. Unexpected subpartition strategy ─────────────────────────────────────────


async def test__nested__branch_subpartitioned_by_list__reported_without_hash_ddl(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await _exec(db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY LIST (tenant_id)')
    await _exec(db_engine, f'CREATE TABLE "{branch}__eu" PARTITION OF "{branch}" FOR VALUES IN (1, 2)')
    await _exec(
        db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.success  # type: ignore[attr-defined]
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]  # type: ignore[attr-defined]
    assert len(issues) == 1
    assert "LIST" in issues[0].error
    assert set(await _children(db_engine, branch)) == set()  # no hash children were added


# ── H. Legacy leaf ──────────────────────────────────────────────────────────────


async def test__nested__legacy_leaf_partition__left_valid_and_untouched(db_engine: AsyncEngine, table: str) -> None:
    # Arrange: a partition created under the old flat policy.
    legacy = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    await _exec(db_engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL)')
    await _exec(
        db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{legacy}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert: still a plain, writable leaf; no issue raised.
    assert result.success  # type: ignore[attr-defined]
    assert result.issues == ()  # type: ignore[attr-defined]
    assert await _relkind(db_engine, legacy) == "r"

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
    await _exec(db_engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL)')
    await _exec(
        db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{legacy}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    await _run(db_engine, nested_config(table, modulus=2))

    # Assert
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert await _relkind(db_engine, new_branch) == "p"
    assert len(await _children(db_engine, new_branch)) == 2


# ── I. Partial failure ──────────────────────────────────────────────────────────


async def test__nested__branch_created_but_never_attached__next_run_completes_and_attaches_it(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: exactly what an interrupted run leaves behind.
    branch = await _build_branch(db_engine, table, modulus=2, remainders=(0,), attach=False)

    # Act
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.success  # type: ignore[attr-defined]
    assert await _children(db_engine, branch) == {f"{branch}__h0": (2, 0), f"{branch}__h1": (2, 1)}
    async with db_engine.connect() as conn:
        attached = await conn.execute(
            text("SELECT relispartition FROM pg_class WHERE oid = to_regclass(:n)"),
            {"n": f'"{branch}"'},
        )
    assert attached.scalar() is True


async def test__nested__creation_interrupted_before_attach__root_never_sees_a_partial_branch(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _build_branch(db_engine, table, modulus=2, remainders=(0,), attach=False)

    # Act: before maintenance runs, the incomplete branch is not reachable.
    async with db_engine.connect() as conn:
        result = await conn.execute(
            text("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(:n)"),
            {"n": f'"{table}"'},
        )

    # Assert: attaching last is what buys this — no row can be rejected meanwhile.
    assert result.scalar() == 0


# ── J/K. UUIDv7 boundaries ──────────────────────────────────────────────────────


async def test__uuid7__weekly_periods__branch_bounds_are_uuid_literals(db_engine: AsyncEngine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    config = nested_config(uuid_table, modulus=2, partition_column="id")

    # Act
    await _run(db_engine, config, codec=codec)

    # Assert
    branch = f"{uuid_table}{WEEK_SUFFIX}"
    async with db_engine.connect() as conn:
        result = await conn.execute(
            text("SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = to_regclass(:n)"),
            {"n": f'"{branch}"'},
        )
        bounds = str(result.scalar())

    assert str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))) in bounds
    assert str(codec.min_uuid_for(datetime(2026, 8, 31, tzinfo=UTC))) in bounds


async def test__uuid7__adjacent_periods__bounds_meet_without_a_gap(db_engine: AsyncEngine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    config = nested_config(uuid_table, modulus=1, partition_column="id", create_ahead=2)

    # Act
    await _run(db_engine, config, codec=codec)

    # Assert
    async with db_engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) FROM pg_inherits i "
                "JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = to_regclass(:n) ORDER BY c.relname"
            ),
            {"n": f'"{uuid_table}"'},
        )
        rows = {str(r[0]): str(r[1]) for r in result.fetchall()}

    def upper(expr: str) -> str:
        match = re.search(r"TO \('([^']+)'\)", expr)
        assert match
        return match.group(1)

    def lower(expr: str) -> str:
        match = re.search(r"FROM \('([^']+)'\)", expr)
        assert match
        return match.group(1)

    assert upper(rows[f"{uuid_table}{WEEK_SUFFIX}"]) == lower(rows[f"{uuid_table}{NEXT_WEEK_SUFFIX}"])


async def test__uuid7_with_hash__rows_route_to_the_expected_leaf(db_engine: AsyncEngine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    await _run(db_engine, nested_config(uuid_table, modulus=2, partition_column="id"), codec=codec)

    branch = f"{uuid_table}{WEEK_SUFFIX}"
    inside_week = codec.min_uuid_for(datetime(2026, 8, 26, 12, tzinfo=UTC))

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
    assert str(inside_week) != ""
    assert routed == expected


async def test__uuid7__retention__prunes_by_decoding_the_uuid_upper_bound(
    db_engine: AsyncEngine, uuid_table: str
) -> None:
    # Arrange: a branch two weeks old, with retention of one period.
    codec = uuid7_codec()
    config = nested_config(uuid_table, modulus=1, partition_column="id", retention=1)
    with freezegun.freeze_time("2026-08-12"):
        await _maintainer(db_engine, codec=codec).run_maintenance(config)
    old_branch = f"{uuid_table}__2026_w33"
    assert await _relkind(db_engine, old_branch) == "p"

    # Act
    result = await _run(db_engine, config, codec=codec)

    # Assert: retention works on encoded bounds, which needs the codec to decode.
    assert result.dropped_count == 1  # type: ignore[attr-defined]
    assert await _relkind(db_engine, old_branch) is None


async def test__uuid7__is_partition_closed__decodes_the_encoded_upper_bound(
    db_engine: AsyncEngine, uuid_table: str
) -> None:
    # Arrange
    codec = uuid7_codec()
    with freezegun.freeze_time("2026-08-12"):
        await _maintainer(db_engine, codec=codec).run_maintenance(
            nested_config(uuid_table, modulus=1, partition_column="id")
        )
    metadata = PostgresMetadataProvider(db_engine, boundary_codec=codec)

    # Act / Assert: the 2026-W33 branch closed long ago in real time.
    assert await metadata.is_partition_closed(f"{uuid_table}__2026_w33") is True


async def test__uuid7_without_codec__is_partition_closed__reports_false_instead_of_raising(
    db_engine: AsyncEngine, uuid_table: str
) -> None:
    # Arrange
    codec = uuid7_codec()
    with freezegun.freeze_time("2026-08-12"):
        await _maintainer(db_engine, codec=codec).run_maintenance(
            nested_config(uuid_table, modulus=1, partition_column="id")
        )

    # Act: a provider with no codec cannot read a UUID bound.
    plain = PostgresMetadataProvider(db_engine)

    # Assert: it must not try to cast one to a timestamp.
    assert await plain.is_partition_closed(f"{uuid_table}__2026_w33") is False


# ── Lifecycle of a whole branch ─────────────────────────────────────────────────


async def test__nested__expired_branch__detached_and_dropped_with_its_whole_subtree(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    config = nested_config(table, modulus=2, retention=1)
    with freezegun.freeze_time("2026-08-10"):
        await _maintainer(db_engine).run_maintenance(config)
    old_branch = f"{table}__2026_w33"
    assert await _relkind(db_engine, old_branch) == "p"

    # Act
    result = await _run(db_engine, config)

    # Assert: the branch is the lifecycle unit; its leaves go with it.
    assert result.dropped_count == 1  # type: ignore[attr-defined]
    assert await _relkind(db_engine, old_branch) is None
    assert await _relkind(db_engine, f"{old_branch}__h0") is None
    assert await _relkind(db_engine, f"{old_branch}__h1") is None


async def test__nested__retention_counts_time_periods_not_leaves(db_engine: AsyncEngine, table: str) -> None:
    # Arrange: 4 buckets per week would exhaust a leaf-counted retention of 2.
    config = nested_config(table, modulus=4, retention=2)
    for week in ("2026-08-10", "2026-08-17", "2026-08-24"):
        with freezegun.freeze_time(week):
            await _maintainer(db_engine).run_maintenance(config)

    # Act
    async with db_engine.connect() as conn:
        result = await conn.execute(
            text("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(:n)"),
            {"n": f'"{table}"'},
        )

    # Assert: two weekly branches retained, not two hash leaves.
    assert result.scalar() == 2


async def test__nested__before_drop_hook__fires_once_for_the_branch_not_per_leaf(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    dropped: list[str] = []

    class RecordingHooks(BasePartitionLifecycleHooks):
        async def before_drop(self, table_name: str, partition_name: str) -> None:
            dropped.append(partition_name)

    config = nested_config(table, modulus=4, retention=1)
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(db_engine),
        metadata=PostgresMetadataProvider(db_engine),
        locks=PostgresAdvisoryLockManager(db_engine),
        period_calculator=WeekPeriodCalculator(),
        hooks=[RecordingHooks()],
    )
    maintainer = PartitionMaintainer(service)
    with freezegun.freeze_time("2026-08-10"):
        await maintainer.run_maintenance(config)

    # Act
    with freezegun.freeze_time(FROZEN_WEEK):
        await maintainer.run_maintenance(config)

    # Assert: cold-storage export sees one time slice, not four fragments.
    assert dropped == [f"public.{table}__2026_w33"]


# ── DEFAULT partition reconciliation ────────────────────────────────────────────


async def test__nested__rows_in_default__moved_into_the_new_branch_and_routed_to_leaves(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _exec(db_engine, f'CREATE TABLE "{table}_default" PARTITION OF "{table}" DEFAULT')
    async with db_engine.begin() as conn:
        for tenant in range(1, 7):
            await conn.execute(
                text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )

    # Act
    result = await _run(db_engine, nested_config(table, modulus=2))

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert result.success  # type: ignore[attr-defined]
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
    with freezegun.freeze_time(FROZEN_WEEK), pytest.raises(InvalidPartitionConfigError, match="tenant_id"):
        await _maintainer(db_engine).run_maintenance(config)

    assert await _relkind(db_engine, f"{unconstrained_table}{WEEK_SUFFIX}") is None


# ── Backward compatibility ──────────────────────────────────────────────────────


async def test__flat_config_on_the_same_table__still_creates_a_plain_leaf(db_engine: AsyncEngine, table: str) -> None:
    # Arrange / Act
    result = await _run(db_engine, flat_config(table))

    # Assert
    assert result.created_count == 1  # type: ignore[attr-defined]
    assert await _relkind(db_engine, f"{table}{WEEK_SUFFIX}") == "r"


async def test__flat_config__maintenance_result_reports_no_repairs(db_engine: AsyncEngine, table: str) -> None:
    # Arrange / Act
    result = await _run(db_engine, flat_config(table))

    # Assert
    assert result.repaired_count == 0  # type: ignore[attr-defined]
    assert result.issues == ()  # type: ignore[attr-defined]


# ── Tree introspection ──────────────────────────────────────────────────────────


async def test__get_partition_tree__nested_table__reports_levels_bounds_and_strategies(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _run(db_engine, nested_config(table, modulus=2))
    metadata = PostgresMetadataProvider(db_engine)

    # Act
    tree = await metadata.get_partition_tree(table)

    # Assert
    assert tree is not None
    assert tree.partition_type == PartitionType.RANGE
    assert tree.partition_columns == ("created_at",)

    branch = tree.children[0]
    assert branch.level == 1
    assert branch.partition_type == PartitionType.HASH
    assert branch.partition_columns == ("tenant_id",)
    assert branch.bounds is not None
    assert [c.level for c in branch.children] == [2, 2]
    assert all(c.is_leaf for c in branch.children)


async def test__get_partition_tree__unpartitioned_relation__returns_none(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    await _exec(db_engine, f'CREATE TABLE "{table}_plain" (i int)')
    metadata = PostgresMetadataProvider(db_engine)

    # Act / Assert
    try:
        assert await metadata.get_partition_tree(f"{table}_plain") is None
    finally:
        await _exec(db_engine, f'DROP TABLE IF EXISTS "{table}_plain"')


async def test__list_partitions__nested_table__reports_the_branch_as_subpartitioned(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _run(db_engine, nested_config(table, modulus=2))
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
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(db_engine),
        metadata=PostgresMetadataProvider(db_engine),
        locks=PostgresAdvisoryLockManager(db_engine),
        period_calculator=WeekPeriodCalculator(),
    )

    # Act
    await service.ensure_partition(nested_config(table, modulus=4), Period(year=2026, week=35))

    # Assert
    assert len(await _children(db_engine, branch)) == 4


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
    await _exec(engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    await _exec(
        engine,
        f'CREATE TABLE "{branch}_h0" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 2, REMAINDER 0)',
    )
    await _exec(
        engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')",
    )
    return branch


async def test__adoption__foreign_named_tree__reconciled_without_recreating_anything(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: an existing tree this library did not create.
    branch = await _adopt_foreign_branch(db_engine, table)
    config = nested_config(table, modulus=2)
    config = config.model_copy(
        update={"subpartition": config.subpartition.model_copy(update={"name_suffix": "_h{remainder}"})}
    )
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(db_engine),
        metadata=PostgresMetadataProvider(db_engine),
        locks=PostgresAdvisoryLockManager(db_engine),
        period_calculator=LegacyNamedWeekCalculator(),
    )

    # Act
    with freezegun.freeze_time(FROZEN_WEEK):
        result = await PartitionMaintainer(service).run_maintenance(config)

    # Assert: the gap is filled in place, under the foreign naming convention.
    assert result.success
    assert result.created_count == 0
    assert result.repaired_count == 1
    assert set(await _children(db_engine, branch)) == {f"{branch}_h0", f"{branch}_h1"}


async def test__adoption__foreign_names_with_the_default_calculator__fails_loudly_not_silently(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: the same tree, but the calculator cannot recognise its names.
    await _adopt_foreign_branch(db_engine, table)

    # Act
    result = await _run_safe(db_engine, nested_config(table, modulus=2))

    # Assert: PostgreSQL refuses the overlapping ATTACH and the run reports it,
    # rather than quietly creating a second partition for the same period.
    assert not result.success
    assert "overlap" in str(result.error)


async def _run_safe(
    engine: AsyncEngine,
    config: TablePartitionConfig,
    *,
    at_time: str = FROZEN_WEEK,
) -> Any:
    """Run maintenance without raising, for the cases that are expected to fail."""
    with freezegun.freeze_time(at_time):
        return await _maintainer(engine).run_maintenance_safe(config)


# ── Backfill ────────────────────────────────────────────────────────────────────


async def test__backfill__past_periods__creates_each_branch_with_a_complete_bucket_set(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: data already in the table predates create-ahead's window.
    config = nested_config(table, modulus=2)
    calculator = WeekPeriodCalculator()
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(db_engine),
        metadata=PostgresMetadataProvider(db_engine),
        locks=PostgresAdvisoryLockManager(db_engine),
        period_calculator=calculator,
    )

    with freezegun.freeze_time(FROZEN_WEEK):
        current = calculator.current_period()
        past = [calculator.period_before(current, n) for n in (1, 2, 3)]

    # Act
    created = await service.ensure_partitions(config, past)

    # Assert
    assert [p.relname for p in created] == [
        f"{table}__2026_w34",
        f"{table}__2026_w33",
        f"{table}__2026_w32",
    ]
    for suffix in ("__2026_w34", "__2026_w33", "__2026_w32"):
        branch = f"{table}{suffix}"
        assert await _relkind(db_engine, branch) == "p"
        assert len(await _children(db_engine, branch)) == 2


async def test__backfill__rerun__is_idempotent(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    calculator = WeekPeriodCalculator()
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(db_engine),
        metadata=PostgresMetadataProvider(db_engine),
        locks=PostgresAdvisoryLockManager(db_engine),
        period_calculator=calculator,
    )
    periods = [Period(year=2026, week=30), Period(year=2026, week=31)]
    await service.ensure_partitions(config, periods)

    # Act
    with _count_ddl(db_engine) as counter:
        created = await service.ensure_partitions(config, periods)

    # Assert
    assert created == []
    assert counter.statements == []


async def test__backfill__then_maintenance__past_and_future_coexist(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    calculator = WeekPeriodCalculator()
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(db_engine),
        metadata=PostgresMetadataProvider(db_engine),
        locks=PostgresAdvisoryLockManager(db_engine),
        period_calculator=calculator,
    )
    await service.ensure_partitions(config, [Period(year=2026, week=34)])

    # Act: create-ahead now runs over the current week.
    result = await _run(db_engine, config)

    # Assert: backfilled history is untouched, the current week is added.
    assert result.success
    assert await _relkind(db_engine, f"{table}__2026_w34") == "p"
    assert await _relkind(db_engine, f"{table}{WEEK_SUFFIX}") == "p"


# ── Identity columns ────────────────────────────────────────────────────────────


async def test__identity_root__flat_config__partition_is_created_and_attached(
    db_engine: AsyncEngine, identity_table: str
) -> None:
    # Arrange / Act
    result = await _run(db_engine, flat_config(identity_table))

    # Assert
    assert result.success
    assert await _relkind(db_engine, f"{identity_table}{WEEK_SUFFIX}") == "r"


async def test__identity_root__nested_config__whole_branch_is_created(
    db_engine: AsyncEngine, identity_table: str
) -> None:
    # Arrange / Act
    result = await _run(db_engine, nested_config(identity_table, modulus=2))

    # Assert
    branch = f"{identity_table}{WEEK_SUFFIX}"
    assert result.success
    assert await _relkind(db_engine, branch) == "p"
    assert len(await _children(db_engine, branch)) == 2


async def test__identity_root__inserts__generate_ids_and_keep_generated_columns(
    db_engine: AsyncEngine, identity_table: str
) -> None:
    # Arrange
    await _run(db_engine, nested_config(identity_table, modulus=2))

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
    result = await _run(db_engine, list_config(list_table))

    # Assert
    branch = f"{list_table}{WEEK_SUFFIX}"
    assert result.success
    assert await _relkind(db_engine, branch) == "p"
    assert await _list_children(db_engine, branch) == {
        f"{branch}__eu": ("de", "fr"),
        f"{branch}__us": ("us",),
    }


async def test__list__include_default__adds_the_catch_all(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange / Act
    await _run(db_engine, list_config(list_table, include_default=True))

    # Assert
    branch = f"{list_table}{WEEK_SUFFIX}"
    assert await _relkind(db_engine, f"{branch}__other") == "r"


async def test__list__rows_route_to_the_partition_owning_their_value(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange
    await _run(db_engine, list_config(list_table, include_default=True))
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
    await _run(db_engine, config)

    # Act
    with _count_ddl(db_engine) as counter:
        result = await _run(db_engine, config)

    # Assert
    assert result.repaired_count == 0
    assert counter.statements == []


async def test__list__branch_missing_a_group__creates_only_that_group(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange: only the EU partition exists so far.
    branch = f"{list_table}{WEEK_SUFFIX}"
    await _exec(db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    await _exec(db_engine, f"""CREATE TABLE "{branch}__eu" PARTITION OF "{branch}" FOR VALUES IN ('de', 'fr')""")
    await _exec(
        db_engine,
        f'ALTER TABLE "{list_table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')",
    )

    # Act
    result = await _run(db_engine, list_config(list_table))

    # Assert
    assert result.repaired_count == 1
    assert set(await _list_children(db_engine, branch)) == {f"{branch}__eu", f"{branch}__us"}


async def test__list__group_matched_by_values_under_a_foreign_name__left_alone(
    db_engine: AsyncEngine, list_table: str
) -> None:
    # Arrange: another tool created the same value set under a different name.
    branch = f"{list_table}{WEEK_SUFFIX}"
    await _exec(db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    await _exec(db_engine, f"""CREATE TABLE "{branch}__europe" PARTITION OF "{branch}" FOR VALUES IN ('de', 'fr')""")
    await _exec(db_engine, f"""CREATE TABLE "{branch}__usa" PARTITION OF "{branch}" FOR VALUES IN ('us')""")
    await _exec(
        db_engine,
        f'ALTER TABLE "{list_table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')",
    )

    # Act
    result = await _run(db_engine, list_config(list_table))

    # Assert: matched by the values they own, so nothing is duplicated.
    assert result.repaired_count == 0
    assert set(await _list_children(db_engine, branch)) == {f"{branch}__europe", f"{branch}__usa"}


async def test__list__value_owned_by_another_partition__reported_and_not_mutated(
    db_engine: AsyncEngine, list_table: str
) -> None:
    # Arrange: "de" sits in a partition that is not the configured EU group.
    branch = f"{list_table}{WEEK_SUFFIX}"
    await _exec(db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    await _exec(db_engine, f"""CREATE TABLE "{branch}__dach" PARTITION OF "{branch}" FOR VALUES IN ('de', 'at')""")
    await _exec(
        db_engine,
        f'ALTER TABLE "{list_table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')",
    )

    # Act
    result = await _run(db_engine, list_config(list_table))

    # Assert: the non-conflicting group is still created; the clash is reported.
    assert result.success
    assert set(await _list_children(db_engine, branch)) == {f"{branch}__dach", f"{branch}__us"}
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]
    assert len(issues) == 1
    assert "'de'" in issues[0].error


async def test__list__over_hash__builds_and_routes_through_both_levels(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange: RANGE(created_at) -> LIST(region) -> HASH(tenant_id)
    await _run(db_engine, list_config(list_table, inner_modulus=2))
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
    assert set(await _children(db_engine, f"{branch}__eu")) == {f"{branch}__eu__h0", f"{branch}__eu__h1"}
    assert leaf in {f"{branch}__eu__h0", f"{branch}__eu__h1"}


async def test__list__expired_branch__dropped_with_its_whole_subtree(db_engine: AsyncEngine, list_table: str) -> None:
    # Arrange
    config = list_config(list_table, retention=1)
    with freezegun.freeze_time("2026-08-10"):
        await _maintainer(db_engine).run_maintenance(config)
    old_branch = f"{list_table}__2026_w33"
    assert await _relkind(db_engine, old_branch) == "p"

    # Act
    result = await _run(db_engine, config)

    # Assert
    assert result.dropped_count == 1
    assert await _relkind(db_engine, old_branch) is None
    assert await _relkind(db_engine, f"{old_branch}__eu") is None


# ── Static roots (no time dimension) ────────────────────────────────────────────


def _static_maintainer(engine: AsyncEngine) -> PartitionMaintainer:
    """A lifecycle service with no period calculator, as a static root needs."""
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine),
        locks=PostgresAdvisoryLockManager(engine),
    )
    return PartitionMaintainer(service)


async def test__hash_root__fresh_table__creates_every_bucket(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange / Act
    result = await _static_maintainer(db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=4))

    # Assert: the table's own partitions, not a subtree inside a period.
    assert result.success
    assert result.created_count == 4
    assert await _children(db_engine, hash_root_table) == {f"{hash_root_table}__h{r}": (4, r) for r in range(4)}


async def test__hash_root__second_run__executes_zero_ddl(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange
    config = hash_root_config(hash_root_table, modulus=2)
    await _static_maintainer(db_engine).run_maintenance(config)

    # Act
    with _count_ddl(db_engine) as counter:
        result = await _static_maintainer(db_engine).run_maintenance(config)

    # Assert
    assert result.created_count == 0
    assert counter.statements == []


async def test__hash_root__missing_bucket__is_repaired(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange: an incomplete set, as a partial migration would leave.
    for remainder in (0, 1, 3):
        await _exec(
            db_engine,
            f'CREATE TABLE "{hash_root_table}__h{remainder}" PARTITION OF "{hash_root_table}" '
            f"FOR VALUES WITH (MODULUS 4, REMAINDER {remainder})",
        )

    # Act
    result = await _static_maintainer(db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=4))

    # Assert
    assert result.created_count == 1
    assert len(await _children(db_engine, hash_root_table)) == 4


async def test__hash_root__nothing_is_ever_pruned(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange / Act
    result = await _static_maintainer(db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=2))

    # Assert: a static root has no periods, so nothing ages out of it.
    assert result.detached_count == 0
    assert result.dropped_count == 0


async def test__hash_root__rows_route_into_the_buckets(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange
    await _static_maintainer(db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=2))

    # Act
    async with db_engine.begin() as conn:
        for tenant in range(1, 9):
            await conn.execute(
                text(f'INSERT INTO "{hash_root_table}" (tenant_id) VALUES (:t)'),  # noqa: S608
                {"t": tenant},
            )
        rows = await conn.execute(
            text(f'SELECT DISTINCT tableoid::regclass::text FROM "{hash_root_table}"')  # noqa: S608
        )
        leaves = {str(r[0]) for r in rows.fetchall()}

    # Assert
    assert leaves <= {f"{hash_root_table}__h0", f"{hash_root_table}__h1"}
    assert leaves


async def test__hash_root__with_a_nested_level__builds_both(db_engine: AsyncEngine, hash_root_table: str) -> None:
    # Arrange / Act: HASH(tenant_id) -> HASH(id)
    await _static_maintainer(db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=2, inner_modulus=2))

    # Assert
    assert set(await _children(db_engine, hash_root_table)) == {
        f"{hash_root_table}__h0",
        f"{hash_root_table}__h1",
    }
    assert set(await _children(db_engine, f"{hash_root_table}__h0")) == {
        f"{hash_root_table}__h0__h0",
        f"{hash_root_table}__h0__h1",
    }


async def test__list_root__fresh_table__creates_one_partition_per_group(
    db_engine: AsyncEngine, list_root_table: str
) -> None:
    # Arrange / Act
    result = await _static_maintainer(db_engine).run_maintenance(
        list_root_config(list_root_table, include_default=True)
    )

    # Assert
    assert result.success
    assert await _list_children(db_engine, list_root_table) == {
        f"{list_root_table}__eu": ("de", "fr"),
        f"{list_root_table}__us": ("us",),
    }
    assert await _relkind(db_engine, f"{list_root_table}__other") == "r"


async def test__list_root__rows_route_by_value(db_engine: AsyncEngine, list_root_table: str) -> None:
    # Arrange
    await _static_maintainer(db_engine).run_maintenance(list_root_config(list_root_table, include_default=True))

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


async def test__static_root__time_based_api_without_a_calculator__refused_clearly(
    db_engine: AsyncEngine, hash_root_table: str
) -> None:
    # Arrange
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(db_engine),
        metadata=PostgresMetadataProvider(db_engine),
        locks=PostgresAdvisoryLockManager(db_engine),
    )

    # Act / Assert: create-ahead is period arithmetic and there are no periods.
    with pytest.raises(InvalidPartitionConfigError, match="period_calculator"):
        await service.create_future_partitions(flat_config(hash_root_table))


# ── Composite partition keys ────────────────────────────────────────────────────


async def test__composite_key__fresh_table__creates_the_period_partition(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange / Act
    result = await _run(db_engine, composite_config(composite_table))

    # Assert
    assert result.success
    assert result.created_count == 1
    assert await _relkind(db_engine, f"{composite_table}{WEEK_SUFFIX}") == "r"


async def test__composite_key__bounds_pad_trailing_columns_with_minvalue(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange
    await _run(db_engine, composite_config(composite_table))

    # Act
    async with db_engine.connect() as conn:
        result = await conn.execute(
            text("SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = to_regclass(:n)"),
            {"n": f'"{composite_table}{WEEK_SUFFIX}"'},
        )
        bounds = str(result.scalar())

    # Assert
    assert bounds.count("MINVALUE") == 2
    assert "2026-08-24" in bounds
    assert "2026-08-31" in bounds


async def test__composite_key__rows_route_by_the_leading_column_alone(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange
    await _run(db_engine, composite_config(composite_table, create_ahead=2))
    branch = f"{composite_table}{WEEK_SUFFIX}"

    # Act: wildly different trailing values, same period.
    async with db_engine.begin() as conn:
        for tenant in (-9999, 0, 1, 999999):
            await conn.execute(
                text(f'INSERT INTO "{composite_table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )
        rows = await conn.execute(
            text(f'SELECT DISTINCT tableoid::regclass::text FROM "{composite_table}"')  # noqa: S608
        )
        leaves = {str(r[0]) for r in rows.fetchall()}

    # Assert: the trailing column does not affect placement.
    assert leaves == {branch}


async def test__composite_key__second_run__executes_zero_ddl(db_engine: AsyncEngine, composite_table: str) -> None:
    # Arrange
    config = composite_config(composite_table)
    await _run(db_engine, config)

    # Act
    with _count_ddl(db_engine) as counter:
        result = await _run(db_engine, config)

    # Assert
    assert result.created_count == 0
    assert counter.statements == []


async def test__composite_key__retention__prunes_by_the_leading_bound(
    db_engine: AsyncEngine, composite_table: str
) -> None:
    # Arrange
    config = composite_config(composite_table, retention=1)
    with freezegun.freeze_time("2026-08-10"):
        await _maintainer(db_engine).run_maintenance(config)
    old = f"{composite_table}__2026_w33"
    assert await _relkind(db_engine, old) == "r"

    # Act
    result = await _run(db_engine, config)

    # Assert: parsing a composite bound yields the leading value, so retention
    # compares the same instant it would for a single-column key.
    assert result.dropped_count == 1
    assert await _relkind(db_engine, old) is None


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
    config = composite_config(composite_table).model_copy(update={"trailing_partition_columns": ("id",)})

    # Act / Assert
    with freezegun.freeze_time(FROZEN_WEEK), pytest.raises(InvalidPartitionConfigError, match="key mismatch"):
        await _maintainer(db_engine).run_maintenance(config)
