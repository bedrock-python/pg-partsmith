"""Nested RANGE → HASH partitioning against a real PostgreSQL (sync)."""

from __future__ import annotations

import contextlib
import re
from collections.abc import Generator
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import freezegun
import pytest
from sqlalchemy import Engine, text

from pg_partsmith.entities import MaintenanceIssueStep, PartitionType, Period, TablePartitionConfig
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.strategies import WeekPeriodCalculator
from pg_partsmith.subpartition_plan import TopologyReason
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.maintainer import PartitionMaintainer
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.sync.service import PartitionLifecycleService
from pg_partsmith.topology import ListBounds
from tests.integration.nested_support import (
    BARE_UNIQUE_INDEX_TABLE_DDL,
    CHILD_BOUNDS_SQL,
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
    RELKIND_SQL,
    SORTABLE_ID_TABLE_DDL,
    TIMESTAMP_TABLE_DDL,
    TRANSPOSED_DEFAULT_TABLE_DDL,
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
    nullable_composite_config,
    uuid7_codec,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pg_partsmith.boundaries import RangeBoundaryCodec

pytestmark = pytest.mark.integration


# ── Fixtures ────────────────────────────────────────────────────────────────────


def _make_table(engine: Engine, ddl: str) -> Generator[str, None, None]:
    table = f"nested_{uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(text(ddl.format(table=table)))
    yield table
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))


@pytest.fixture
def table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, TIMESTAMP_TABLE_DDL)


@pytest.fixture
def uuid_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, UUID7_TABLE_DDL)


@pytest.fixture
def two_level_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, TWO_LEVEL_TABLE_DDL)


@pytest.fixture
def unconstrained_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, UNCONSTRAINED_TABLE_DDL)


@pytest.fixture
def identity_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, IDENTITY_TABLE_DDL)


@pytest.fixture
def list_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, LIST_TABLE_DDL)


@pytest.fixture
def hash_root_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, HASH_ROOT_TABLE_DDL)


@pytest.fixture
def list_root_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, LIST_ROOT_TABLE_DDL)


@pytest.fixture
def composite_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, COMPOSITE_TABLE_DDL)


@pytest.fixture
def nullable_composite_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, NULLABLE_COMPOSITE_TABLE_DDL)


@pytest.fixture
def expression_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, EXPRESSION_TABLE_DDL)


@pytest.fixture
def sortable_id_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, SORTABLE_ID_TABLE_DDL)


@pytest.fixture
def nullable_list_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, NULLABLE_LIST_TABLE_DDL)


@pytest.fixture
def transposed_default_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, TRANSPOSED_DEFAULT_TABLE_DDL)


@pytest.fixture
def bare_unique_index_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, BARE_UNIQUE_INDEX_TABLE_DDL)


@pytest.fixture
def no_constraint_table(sync_db_engine: Engine) -> Generator[str, None, None]:
    yield from _make_table(sync_db_engine, NO_CONSTRAINT_TABLE_DDL)


def _maintainer(engine: Engine, *, codec: RangeBoundaryCodec | None = None) -> PartitionMaintainer:
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine, boundary_codec=codec),
        locks=PostgresAdvisoryLockManager(engine),
        period_calculator=WeekPeriodCalculator(boundary_codec=codec),
    )
    return PartitionMaintainer(service)


def _run(
    engine: Engine,
    config: TablePartitionConfig,
    *,
    at_time: str = FROZEN_WEEK,
    codec: RangeBoundaryCodec | None = None,
) -> object:
    with freezegun.freeze_time(at_time):
        return _maintainer(engine, codec=codec).run_maintenance(config)


def _children(engine: Engine, parent: str) -> dict[str, tuple[int, int]]:
    with engine.connect() as conn:
        result = conn.execute(text(CHILD_BOUNDS_SQL), {"parent": f'"{parent}"'})
        return hash_children(list(result.fetchall()))


def _relkind(engine: Engine, name: str) -> str | None:
    with engine.connect() as conn:
        result = conn.execute(text(RELKIND_SQL), {"name": f'"{name}"'})
        value = result.scalar()
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def _exec(engine: Engine, sql: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql))


@contextlib.contextmanager
def _count_ddl(engine: Engine) -> Iterator[DdlCounter]:
    yield from ddl_counter(engine)


def _list_children(engine: Engine, parent: str) -> dict[str, tuple[str, ...]]:
    """Map child relname -> the LIST values it owns."""
    with engine.connect() as conn:
        result = conn.execute(text(CHILD_BOUNDS_SQL), {"parent": f'"{parent}"'})
        rows = list(result.fetchall())
    return list_children(rows)


# ── A. Fresh creation ───────────────────────────────────────────────────────────


def test__nested__fresh_table__creates_the_branch_and_every_bucket(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)

    # Act
    result = _run(sync_db_engine, config)

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert result.success  # type: ignore[attr-defined]
    assert result.created_count == 1  # type: ignore[attr-defined]
    assert _relkind(sync_db_engine, branch) == "p"
    assert _children(sync_db_engine, branch) == {
        f"{branch}__h0": (2, 0),
        f"{branch}__h1": (2, 1),
    }


def test__nested__fresh_table__branch_is_attached_to_the_root(sync_db_engine: Engine, table: str) -> None:
    # Arrange / Act
    _run(sync_db_engine, nested_config(table, modulus=2))

    # Act
    with sync_db_engine.connect() as conn:
        result = conn.execute(
            text("SELECT relispartition FROM pg_class WHERE oid = to_regclass(:n)"),
            {"n": f'"{table}{WEEK_SUFFIX}"'},
        )

    # Assert
    assert result.scalar() is True


def test__nested__fresh_table__rows_route_through_the_branch_into_a_leaf(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    _run(sync_db_engine, nested_config(table, modulus=2))

    # Act
    with sync_db_engine.begin() as conn:
        for tenant in range(1, 9):
            conn.execute(
                text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )
        result = conn.execute(text(f'SELECT DISTINCT tableoid::regclass::text FROM "{table}"'))  # noqa: S608
        leaves = {str(r[0]) for r in result.fetchall()}

    # Assert: every row landed in one of the branch's own leaves.
    branch = f"{table}{WEEK_SUFFIX}"
    assert leaves <= {f"{branch}__h0", f"{branch}__h1"}
    assert leaves


def test__nested__deeper_spec__builds_the_whole_two_level_subtree(sync_db_engine: Engine, two_level_table: str) -> None:
    # Arrange
    table = two_level_table
    config = nested_config(table, modulus=2, inner_modulus=2)

    # Act
    _run(sync_db_engine, config)

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert set(_children(sync_db_engine, branch)) == {f"{branch}__h0", f"{branch}__h1"}
    assert set(_children(sync_db_engine, f"{branch}__h0")) == {f"{branch}__h0__h0", f"{branch}__h0__h1"}


# ── B. Already complete ─────────────────────────────────────────────────────────


def test__nested__second_run_on_a_converged_tree__executes_zero_ddl(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    _run(sync_db_engine, config)

    # Act
    with _count_ddl(sync_db_engine) as counter:
        result = _run(sync_db_engine, config)

    # Assert: nothing to do must cost nothing, so no heavy locks are taken.
    assert result.created_count == 0  # type: ignore[attr-defined]
    assert result.repaired_count == 0  # type: ignore[attr-defined]
    assert counter.statements == []


# ── C. Missing hash child ───────────────────────────────────────────────────────


def _build_branch(
    engine: Engine,
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
    _exec(engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH ({hash_column})')
    for remainder in remainders:
        _exec(
            engine,
            f'CREATE TABLE "{branch}__h{remainder}" PARTITION OF "{branch}" '
            f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})",
        )
    if attach:
        _exec(
            engine,
            f"ALTER TABLE \"{table}\" ATTACH PARTITION \"{branch}\" FOR VALUES FROM ('{bounds[0]}') TO ('{bounds[1]}')",
        )
    return branch


def test__nested__branch_missing_one_bucket__creates_only_that_bucket(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    branch = _build_branch(sync_db_engine, table, modulus=4, remainders=(0, 1, 3))

    # Act
    with _count_ddl(sync_db_engine) as counter:
        result = _run(sync_db_engine, nested_config(table, modulus=4))

    # Assert
    assert result.repaired_count == 1  # type: ignore[attr-defined]
    assert _children(sync_db_engine, branch) == {
        f"{branch}__h0": (4, 0),
        f"{branch}__h1": (4, 1),
        f"{branch}__h2": (4, 2),
        f"{branch}__h3": (4, 3),
    }
    created = [s for s in counter.statements if s.startswith("CREATE TABLE")]
    assert len(created) == 1


def test__nested__branch_missing_a_bucket__ingest_recovers_for_the_orphaned_hash_slice(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: find a tenant that hashes into the missing bucket.
    branch = _build_branch(sync_db_engine, table, modulus=4, remainders=(0, 1, 3))
    with sync_db_engine.connect() as conn:
        result = conn.execute(
            text(
                f"SELECT g FROM generate_series(1, 200) g WHERE satisfies_hash_partition("  # noqa: S608
                f"to_regclass('\"{branch}\"'), 4, 2, g::bigint) LIMIT 1"
            )
        )
        stranded_tenant = result.scalar()
    assert stranded_tenant is not None

    # Act
    _run(sync_db_engine, nested_config(table, modulus=4))

    # Assert: the previously rejected row now has somewhere to go.
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
            {"t": stranded_tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
        )
        routed = conn.execute(
            text(f'SELECT tableoid::regclass::text FROM "{table}" WHERE tenant_id = :t'),  # noqa: S608
            {"t": stranded_tenant},
        )

    assert str(routed.scalar()) == f"{branch}__h2"


# ── D. Config modulus changed, historical set complete ──────────────────────────


def test__nested__historical_complete_set_at_another_modulus__left_untouched(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: last week was built with 4 buckets; the config now asks for 2.
    old_branch = _build_branch(
        sync_db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 2, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.repaired_count == 0  # type: ignore[attr-defined]
    assert result.issues == ()  # type: ignore[attr-defined]
    assert _children(sync_db_engine, old_branch) == {f"{old_branch}__h{r}": (4, r) for r in range(4)}


def test__nested__historical_complete_set_at_another_modulus__new_period_uses_the_new_count(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    _build_branch(
        sync_db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 2, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert: rolling the bucket count forward never rewrites history.
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert _children(sync_db_engine, new_branch) == {
        f"{new_branch}__h0": (2, 0),
        f"{new_branch}__h1": (2, 1),
    }


# ── E. Config modulus changed, historical set incomplete ────────────────────────


def test__nested__historical_incomplete_set_at_another_modulus__repaired_at_its_own_modulus(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    old_branch = _build_branch(
        sync_db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert: filled at modulus 4, never at the configured 2.
    assert result.repaired_count == 1  # type: ignore[attr-defined]
    assert _children(sync_db_engine, old_branch) == {f"{old_branch}__h{r}": (4, r) for r in range(4)}


def test__nested__historical_incomplete_set__repair_is_not_reported_as_a_problem(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    _build_branch(
        sync_db_engine,
        table,
        modulus=4,
        remainders=(0, 1, 3),
        suffix=PREVIOUS_WEEK_SUFFIX,
        bounds=PREVIOUS_WEEK_BOUNDS,
    )

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.issues == ()  # type: ignore[attr-defined]


# ── F. Inconsistent moduli ──────────────────────────────────────────────────────


def test__nested__hash_children_with_a_gap_across_moduli__reported_and_not_mutated(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: (2,0) plus (4,1) leaves residue 3 (mod 4) unowned.
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    _exec(
        sync_db_engine,
        f'CREATE TABLE "{branch}__h0" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 2, REMAINDER 0)',
    )
    _exec(
        sync_db_engine,
        f'CREATE TABLE "{branch}__h1" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 4, REMAINDER 1)',
    )
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )
    before = _children(sync_db_engine, branch)

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert
    assert _children(sync_db_engine, branch) == before
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]  # type: ignore[attr-defined]
    assert len(issues) == 1
    assert issues[0].step == MaintenanceIssueStep.RECONCILE
    assert TopologyReason.NON_UNIFORM_INCOMPLETE.value in issues[0].error or "inconsistent moduli" in issues[0].error


def test__nested__inconsistent_branch__does_not_stop_the_rest_of_the_run(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    _exec(
        sync_db_engine,
        f'CREATE TABLE "{branch}__h0" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 2, REMAINDER 0)',
    )
    _exec(
        sync_db_engine,
        f'CREATE TABLE "{branch}__h1" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 4, REMAINDER 1)',
    )
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert: the current week is still created normally.
    assert result.success  # type: ignore[attr-defined]
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert len(_children(sync_db_engine, new_branch)) == 2


def test__nested__mixed_moduli_that_still_tile__left_alone_without_an_issue(sync_db_engine: Engine, table: str) -> None:
    # Arrange: (2,1) plus (4,0) and (4,2) covers the whole keyspace.
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    for modulus, remainder in ((2, 1), (4, 0), (4, 2)):
        _exec(
            sync_db_engine,
            f'CREATE TABLE "{branch}__h{modulus}_{remainder}" PARTITION OF "{branch}" '
            f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})",
        )
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert
    assert [i for i in result.issues if i.partition_name == f"public.{branch}"] == []  # type: ignore[attr-defined]
    assert len(_children(sync_db_engine, branch)) == 3


# ── G. Unexpected subpartition strategy ─────────────────────────────────────────


def test__nested__branch_subpartitioned_by_list__reported_without_hash_ddl(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    branch = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY LIST (tenant_id)')
    _exec(sync_db_engine, f'CREATE TABLE "{branch}__eu" PARTITION OF "{branch}" FOR VALUES IN (1, 2)')
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.success  # type: ignore[attr-defined]
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]  # type: ignore[attr-defined]
    assert len(issues) == 1
    assert "LIST" in issues[0].error
    assert set(_children(sync_db_engine, branch)) == set()  # no hash children were added


# ── H. Legacy leaf ──────────────────────────────────────────────────────────────


def test__nested__legacy_leaf_partition__left_valid_and_untouched(sync_db_engine: Engine, table: str) -> None:
    # Arrange: a partition created under the old flat policy.
    legacy = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL)')
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{legacy}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert: still a plain, writable leaf; no issue raised.
    assert result.success  # type: ignore[attr-defined]
    assert result.issues == ()  # type: ignore[attr-defined]
    assert _relkind(sync_db_engine, legacy) == "r"

    with sync_db_engine.begin() as conn:
        conn.execute(
            text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (1, :d)'),  # noqa: S608
            {"d": datetime(2026, 8, 18, 10, tzinfo=UTC)},
        )
        routed = conn.execute(text(f'SELECT tableoid::regclass::text FROM "{table}"'))  # noqa: S608
    assert str(routed.scalar()) == legacy


def test__nested__legacy_leaf_present__new_periods_still_get_the_new_topology(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    legacy = f"{table}{PREVIOUS_WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL)')
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{legacy}" '
        f"FOR VALUES FROM ('{PREVIOUS_WEEK_BOUNDS[0]}') TO ('{PREVIOUS_WEEK_BOUNDS[1]}')",
    )

    # Act
    _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert
    new_branch = f"{table}{WEEK_SUFFIX}"
    assert _relkind(sync_db_engine, new_branch) == "p"
    assert len(_children(sync_db_engine, new_branch)) == 2


# ── I. Partial failure ──────────────────────────────────────────────────────────


def test__nested__branch_created_but_never_attached__next_run_completes_and_attaches_it(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: exactly what an interrupted run leaves behind.
    branch = _build_branch(sync_db_engine, table, modulus=2, remainders=(0,), attach=False)

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert
    assert result.success  # type: ignore[attr-defined]
    assert _children(sync_db_engine, branch) == {f"{branch}__h0": (2, 0), f"{branch}__h1": (2, 1)}
    with sync_db_engine.connect() as conn:
        attached = conn.execute(
            text("SELECT relispartition FROM pg_class WHERE oid = to_regclass(:n)"),
            {"n": f'"{branch}"'},
        )
    assert attached.scalar() is True


def test__nested__branch_is_attached_only_after_all_its_buckets_exist(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=4)
    branch = f"{table}{WEEK_SUFFIX}".upper()

    # Act
    with _count_ddl(sync_db_engine) as ddl:
        _run(sync_db_engine, config)

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


# ── J/K. UUIDv7 boundaries ──────────────────────────────────────────────────────


def test__uuid7__weekly_periods__branch_bounds_are_uuid_literals(sync_db_engine: Engine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    config = nested_config(uuid_table, modulus=2, partition_column="id")

    # Act
    _run(sync_db_engine, config, codec=codec)

    # Assert
    branch = f"{uuid_table}{WEEK_SUFFIX}"
    with sync_db_engine.connect() as conn:
        result = conn.execute(
            text("SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = to_regclass(:n)"),
            {"n": f'"{branch}"'},
        )
        bounds = str(result.scalar())

    assert str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))) in bounds
    assert str(codec.min_uuid_for(datetime(2026, 8, 31, tzinfo=UTC))) in bounds


def test__uuid7__adjacent_periods__bounds_meet_without_a_gap(sync_db_engine: Engine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    config = nested_config(uuid_table, modulus=1, partition_column="id", create_ahead=2)

    # Act
    _run(sync_db_engine, config, codec=codec)

    # Assert
    with sync_db_engine.connect() as conn:
        result = conn.execute(
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


def test__uuid7_with_hash__rows_route_to_the_expected_leaf(sync_db_engine: Engine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    _run(sync_db_engine, nested_config(uuid_table, modulus=2, partition_column="id"), codec=codec)

    branch = f"{uuid_table}{WEEK_SUFFIX}"
    inside_week = codec.min_uuid_for(datetime(2026, 8, 26, 12, tzinfo=UTC))

    # Act
    with sync_db_engine.begin() as conn:
        for tenant in (1, 2, 3, 4):
            row_id = codec.min_uuid_for(datetime(2026, 8, 26, 12, 0, tenant, tzinfo=UTC))
            conn.execute(
                text(f'INSERT INTO "{uuid_table}" (id, tenant_id, occurred_at) VALUES (:i, :t, :d)'),  # noqa: S608
                {"i": row_id, "t": tenant, "d": datetime(2026, 8, 26, 12, tzinfo=UTC)},
            )
        result = conn.execute(
            text(
                f'SELECT tenant_id, tableoid::regclass::text FROM "{uuid_table}" ORDER BY tenant_id'  # noqa: S608
            )
        )
        routed = {int(r[0]): str(r[1]) for r in result.fetchall()}

        expected = {}
        for tenant in (1, 2, 3, 4):
            check = conn.execute(
                text("SELECT satisfies_hash_partition(to_regclass(:b), 2, 0, CAST(:t AS bigint))"),
                {"b": f'"{branch}"', "t": tenant},
            )
            expected[tenant] = f"{branch}__h0" if check.scalar() else f"{branch}__h1"

    # Assert: the time dimension picked the branch, the hash dimension the leaf.
    assert str(inside_week) != ""
    assert routed == expected


def test__uuid7__retention__prunes_by_decoding_the_uuid_upper_bound(sync_db_engine: Engine, uuid_table: str) -> None:
    # Arrange: a branch two weeks old, with retention of one period.
    codec = uuid7_codec()
    config = nested_config(uuid_table, modulus=1, partition_column="id", retention=1)
    with freezegun.freeze_time("2026-08-12"):
        _maintainer(sync_db_engine, codec=codec).run_maintenance(config)
    old_branch = f"{uuid_table}__2026_w33"
    assert _relkind(sync_db_engine, old_branch) == "p"

    # Act
    result = _run(sync_db_engine, config, codec=codec)

    # Assert: retention works on encoded bounds, which needs the codec to decode.
    assert result.dropped_count == 1  # type: ignore[attr-defined]
    assert _relkind(sync_db_engine, old_branch) is None


def test__uuid7__is_partition_closed__decodes_the_encoded_upper_bound(sync_db_engine: Engine, uuid_table: str) -> None:
    # Arrange
    codec = uuid7_codec()
    with freezegun.freeze_time("2026-08-12"):
        _maintainer(sync_db_engine, codec=codec).run_maintenance(
            nested_config(uuid_table, modulus=1, partition_column="id")
        )
    metadata = PostgresMetadataProvider(sync_db_engine, boundary_codec=codec)

    # Act / Assert: the 2026-W33 branch closed long ago in real time.
    assert metadata.is_partition_closed(f"{uuid_table}__2026_w33") is True


def test__uuid7_without_codec__is_partition_closed__reports_false_instead_of_raising(
    sync_db_engine: Engine, uuid_table: str
) -> None:
    # Arrange
    codec = uuid7_codec()
    with freezegun.freeze_time("2026-08-12"):
        _maintainer(sync_db_engine, codec=codec).run_maintenance(
            nested_config(uuid_table, modulus=1, partition_column="id")
        )

    # Act: a provider with no codec cannot read a UUID bound.
    plain = PostgresMetadataProvider(sync_db_engine)

    # Assert: it must not try to cast one to a timestamp.
    assert plain.is_partition_closed(f"{uuid_table}__2026_w33") is False


# ── Lifecycle of a whole branch ─────────────────────────────────────────────────


def test__nested__expired_branch__detached_and_dropped_with_its_whole_subtree(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    config = nested_config(table, modulus=2, retention=1)
    with freezegun.freeze_time("2026-08-10"):
        _maintainer(sync_db_engine).run_maintenance(config)
    old_branch = f"{table}__2026_w33"
    assert _relkind(sync_db_engine, old_branch) == "p"

    # Act
    result = _run(sync_db_engine, config)

    # Assert: the branch is the lifecycle unit; its leaves go with it.
    assert result.dropped_count == 1  # type: ignore[attr-defined]
    assert _relkind(sync_db_engine, old_branch) is None
    assert _relkind(sync_db_engine, f"{old_branch}__h0") is None
    assert _relkind(sync_db_engine, f"{old_branch}__h1") is None


def test__nested__retention_counts_time_periods_not_leaves(sync_db_engine: Engine, table: str) -> None:
    # Arrange: 4 buckets per week would exhaust a leaf-counted retention of 2.
    config = nested_config(table, modulus=4, retention=2)
    for week in ("2026-08-10", "2026-08-17", "2026-08-24"):
        with freezegun.freeze_time(week):
            _maintainer(sync_db_engine).run_maintenance(config)

    # Act
    with sync_db_engine.connect() as conn:
        result = conn.execute(
            text("SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass(:n)"),
            {"n": f'"{table}"'},
        )

    # Assert: two weekly branches retained, not two hash leaves.
    assert result.scalar() == 2


def test__nested__before_drop_hook__fires_once_for_the_branch_not_per_leaf(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    dropped: list[str] = []

    class RecordingHooks(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            dropped.append(partition_name)

    config = nested_config(table, modulus=4, retention=1)
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(sync_db_engine),
        metadata=PostgresMetadataProvider(sync_db_engine),
        locks=PostgresAdvisoryLockManager(sync_db_engine),
        period_calculator=WeekPeriodCalculator(),
        hooks=[RecordingHooks()],
    )
    maintainer = PartitionMaintainer(service)
    with freezegun.freeze_time("2026-08-10"):
        maintainer.run_maintenance(config)

    # Act
    with freezegun.freeze_time(FROZEN_WEEK):
        maintainer.run_maintenance(config)

    # Assert: cold-storage export sees one time slice, not four fragments.
    assert dropped == [f"public.{table}__2026_w33"]


# ── DEFAULT partition reconciliation ────────────────────────────────────────────


def test__nested__rows_in_default__moved_into_the_new_branch_and_routed_to_leaves(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    _exec(sync_db_engine, f'CREATE TABLE "{table}_default" PARTITION OF "{table}" DEFAULT')
    with sync_db_engine.begin() as conn:
        for tenant in range(1, 7):
            conn.execute(
                text(f'INSERT INTO "{table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )

    # Act
    result = _run(sync_db_engine, nested_config(table, modulus=2))

    # Assert
    branch = f"{table}{WEEK_SUFFIX}"
    assert result.success  # type: ignore[attr-defined]
    with sync_db_engine.connect() as conn:
        rows = conn.execute(
            text(f'SELECT tableoid::regclass::text, count(*) FROM "{table}" GROUP BY 1')  # noqa: S608
        )
        placement = {str(r[0]): int(r[1]) for r in rows.fetchall()}

    assert set(placement) <= {f"{branch}__h0", f"{branch}__h1"}
    assert sum(placement.values()) == 6


# ── Configuration validation ────────────────────────────────────────────────────


def test__nested__hash_column_missing_from_primary_key__refused_before_any_ddl(
    sync_db_engine: Engine, unconstrained_table: str
) -> None:
    # Arrange
    config = nested_config(unconstrained_table, modulus=2)

    # Act / Assert
    with freezegun.freeze_time(FROZEN_WEEK), pytest.raises(InvalidPartitionConfigError, match="tenant_id"):
        _maintainer(sync_db_engine).run_maintenance(config)

    assert _relkind(sync_db_engine, f"{unconstrained_table}{WEEK_SUFFIX}") is None


# ── Backward compatibility ──────────────────────────────────────────────────────


def test__flat_config_on_the_same_table__still_creates_a_plain_leaf(sync_db_engine: Engine, table: str) -> None:
    # Arrange / Act
    result = _run(sync_db_engine, flat_config(table))

    # Assert
    assert result.created_count == 1  # type: ignore[attr-defined]
    assert _relkind(sync_db_engine, f"{table}{WEEK_SUFFIX}") == "r"


def test__flat_config__maintenance_result_reports_no_repairs(sync_db_engine: Engine, table: str) -> None:
    # Arrange / Act
    result = _run(sync_db_engine, flat_config(table))

    # Assert
    assert result.repaired_count == 0  # type: ignore[attr-defined]
    assert result.issues == ()  # type: ignore[attr-defined]


# ── Tree introspection ──────────────────────────────────────────────────────────


def test__get_partition_tree__nested_table__reports_levels_bounds_and_strategies(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    _run(sync_db_engine, nested_config(table, modulus=2))
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act
    tree = metadata.get_partition_tree(table)

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


def test__get_partition_tree__unpartitioned_relation__returns_none(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    _exec(sync_db_engine, f'CREATE TABLE "{table}_plain" (i int)')
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act / Assert
    try:
        assert metadata.get_partition_tree(f"{table}_plain") is None
    finally:
        _exec(sync_db_engine, f'DROP TABLE IF EXISTS "{table}_plain"')


def test__list_partitions__nested_table__reports_the_branch_as_subpartitioned(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    _run(sync_db_engine, nested_config(table, modulus=2))
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act
    partitions = metadata.list_partitions(table)

    # Assert: the lifecycle still sees one partition per period.
    assert len(partitions) == 1
    assert partitions[0].subpartition_type == PartitionType.HASH
    assert partitions[0].is_subpartitioned is True


def test__ensure_partition__existing_branch_with_a_gap__completes_it_for_the_writer(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    branch = _build_branch(sync_db_engine, table, modulus=4, remainders=(0, 1, 3))
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(sync_db_engine),
        metadata=PostgresMetadataProvider(sync_db_engine),
        locks=PostgresAdvisoryLockManager(sync_db_engine),
        period_calculator=WeekPeriodCalculator(),
    )

    # Act
    service.ensure_partition(nested_config(table, modulus=4), Period(year=2026, week=35))

    # Assert
    assert len(_children(sync_db_engine, branch)) == 4


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


def _adopt_foreign_branch(engine: Engine, table: str) -> str:
    """Create a branch named the way another tool would, with one bucket missing."""
    branch = f"{table}_20260824"
    _exec(engine, f'CREATE TABLE "{branch}" (LIKE "{table}" INCLUDING ALL) PARTITION BY HASH (tenant_id)')
    _exec(
        engine,
        f'CREATE TABLE "{branch}_h0" PARTITION OF "{branch}" FOR VALUES WITH (MODULUS 2, REMAINDER 0)',
    )
    _exec(
        engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')",
    )
    return branch


def test__adoption__foreign_named_tree__reconciled_without_recreating_anything(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: an existing tree this library did not create.
    branch = _adopt_foreign_branch(sync_db_engine, table)
    config = nested_config(table, modulus=2)
    config = config.model_copy(
        update={"subpartition": config.subpartition.model_copy(update={"name_suffix": "_h{remainder}"})}
    )
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(sync_db_engine),
        metadata=PostgresMetadataProvider(sync_db_engine),
        locks=PostgresAdvisoryLockManager(sync_db_engine),
        period_calculator=LegacyNamedWeekCalculator(),
    )

    # Act
    with freezegun.freeze_time(FROZEN_WEEK):
        result = PartitionMaintainer(service).run_maintenance(config)

    # Assert: the gap is filled in place, under the foreign naming convention.
    assert result.success
    assert result.created_count == 0
    assert result.repaired_count == 1
    assert set(_children(sync_db_engine, branch)) == {f"{branch}_h0", f"{branch}_h1"}


def test__adoption__foreign_names_with_the_default_calculator__fails_loudly_not_silently(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: the same tree, but the calculator cannot recognise its names.
    _adopt_foreign_branch(sync_db_engine, table)

    # Act
    result = _run_safe(sync_db_engine, nested_config(table, modulus=2))

    # Assert: PostgreSQL refuses the overlapping ATTACH and the run reports it,
    # rather than quietly creating a second partition for the same period.
    assert not result.success
    assert "overlap" in str(result.error)


def _run_safe(
    engine: Engine,
    config: TablePartitionConfig,
    *,
    at_time: str = FROZEN_WEEK,
) -> Any:
    """Run maintenance without raising, for the cases that are expected to fail."""
    with freezegun.freeze_time(at_time):
        return _maintainer(engine).run_maintenance_safe(config)


# ── Backfill ────────────────────────────────────────────────────────────────────


def test__backfill__past_periods__creates_each_branch_with_a_complete_bucket_set(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: data already in the table predates create-ahead's window.
    config = nested_config(table, modulus=2)
    calculator = WeekPeriodCalculator()
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(sync_db_engine),
        metadata=PostgresMetadataProvider(sync_db_engine),
        locks=PostgresAdvisoryLockManager(sync_db_engine),
        period_calculator=calculator,
    )

    with freezegun.freeze_time(FROZEN_WEEK):
        current = calculator.current_period()
        past = [calculator.period_before(current, n) for n in (1, 2, 3)]

    # Act
    created = service.ensure_partitions(config, past)

    # Assert
    assert [p.relname for p in created] == [
        f"{table}__2026_w34",
        f"{table}__2026_w33",
        f"{table}__2026_w32",
    ]
    for suffix in ("__2026_w34", "__2026_w33", "__2026_w32"):
        branch = f"{table}{suffix}"
        assert _relkind(sync_db_engine, branch) == "p"
        assert len(_children(sync_db_engine, branch)) == 2


def test__backfill__rerun__is_idempotent(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    calculator = WeekPeriodCalculator()
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(sync_db_engine),
        metadata=PostgresMetadataProvider(sync_db_engine),
        locks=PostgresAdvisoryLockManager(sync_db_engine),
        period_calculator=calculator,
    )
    periods = [Period(year=2026, week=30), Period(year=2026, week=31)]
    service.ensure_partitions(config, periods)

    # Act
    with _count_ddl(sync_db_engine) as counter:
        created = service.ensure_partitions(config, periods)

    # Assert
    assert created == []
    assert counter.statements == []


def test__backfill__then_maintenance__past_and_future_coexist(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = nested_config(table, modulus=2)
    calculator = WeekPeriodCalculator()
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(sync_db_engine),
        metadata=PostgresMetadataProvider(sync_db_engine),
        locks=PostgresAdvisoryLockManager(sync_db_engine),
        period_calculator=calculator,
    )
    service.ensure_partitions(config, [Period(year=2026, week=34)])

    # Act: create-ahead now runs over the current week.
    result = _run(sync_db_engine, config)

    # Assert: backfilled history is untouched, the current week is added.
    assert result.success
    assert _relkind(sync_db_engine, f"{table}__2026_w34") == "p"
    assert _relkind(sync_db_engine, f"{table}{WEEK_SUFFIX}") == "p"


# ── Identity columns ────────────────────────────────────────────────────────────


def test__identity_root__flat_config__partition_is_created_and_attached(
    sync_db_engine: Engine, identity_table: str
) -> None:
    # Arrange / Act
    result = _run(sync_db_engine, flat_config(identity_table))

    # Assert
    assert result.success
    assert _relkind(sync_db_engine, f"{identity_table}{WEEK_SUFFIX}") == "r"


def test__identity_root__nested_config__whole_branch_is_created(sync_db_engine: Engine, identity_table: str) -> None:
    # Arrange / Act
    result = _run(sync_db_engine, nested_config(identity_table, modulus=2))

    # Assert
    branch = f"{identity_table}{WEEK_SUFFIX}"
    assert result.success
    assert _relkind(sync_db_engine, branch) == "p"
    assert len(_children(sync_db_engine, branch)) == 2


def test__identity_root__inserts__generate_ids_and_keep_generated_columns(
    sync_db_engine: Engine, identity_table: str
) -> None:
    # Arrange
    _run(sync_db_engine, nested_config(identity_table, modulus=2))

    # Act
    with sync_db_engine.begin() as conn:
        for tenant in (1, 2, 3):
            conn.execute(
                text(f'INSERT INTO "{identity_table}" (tenant_id, created_at, amount) VALUES (:t, :d, :a)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC), "a": tenant},
            )
        rows = conn.execute(
            text(f'SELECT id, amount, doubled FROM "{identity_table}" ORDER BY id')  # noqa: S608
        )
        result = [(int(r[0]), int(r[1]), int(r[2])) for r in rows.fetchall()]

    # Assert: the parent's identity supplies ids through the partition, and
    # INCLUDING ALL still carried the generated column over.
    assert [r[0] for r in result] == [1, 2, 3]
    assert all(doubled == amount * 2 for _, amount, doubled in result)


# ── LIST subpartitioning ────────────────────────────────────────────────────────


def test__list__fresh_table__creates_one_partition_per_group(sync_db_engine: Engine, list_table: str) -> None:
    # Arrange / Act
    result = _run(sync_db_engine, list_config(list_table))

    # Assert
    branch = f"{list_table}{WEEK_SUFFIX}"
    assert result.success
    assert _relkind(sync_db_engine, branch) == "p"
    assert _list_children(sync_db_engine, branch) == {
        f"{branch}__eu": ("de", "fr"),
        f"{branch}__us": ("us",),
    }


def test__list__include_default__adds_the_catch_all(sync_db_engine: Engine, list_table: str) -> None:
    # Arrange / Act
    _run(sync_db_engine, list_config(list_table, include_default=True))

    # Assert
    branch = f"{list_table}{WEEK_SUFFIX}"
    assert _relkind(sync_db_engine, f"{branch}__other") == "r"


def test__list__rows_route_to_the_partition_owning_their_value(sync_db_engine: Engine, list_table: str) -> None:
    # Arrange
    _run(sync_db_engine, list_config(list_table, include_default=True))
    branch = f"{list_table}{WEEK_SUFFIX}"

    # Act
    with sync_db_engine.begin() as conn:
        for region in ("de", "fr", "us", "jp"):
            conn.execute(
                text(f'INSERT INTO "{list_table}" (region, tenant_id, created_at) VALUES (:r, 1, :d)'),  # noqa: S608
                {"r": region, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )
        rows = conn.execute(
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


def test__list__second_run_on_a_converged_tree__executes_zero_ddl(sync_db_engine: Engine, list_table: str) -> None:
    # Arrange
    config = list_config(list_table, include_default=True)
    _run(sync_db_engine, config)

    # Act
    with _count_ddl(sync_db_engine) as counter:
        result = _run(sync_db_engine, config)

    # Assert
    assert result.repaired_count == 0
    assert counter.statements == []


def test__list__branch_missing_a_group__creates_only_that_group(sync_db_engine: Engine, list_table: str) -> None:
    # Arrange: only the EU partition exists so far.
    branch = f"{list_table}{WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    _exec(sync_db_engine, f"""CREATE TABLE "{branch}__eu" PARTITION OF "{branch}" FOR VALUES IN ('de', 'fr')""")
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{list_table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')",
    )

    # Act
    result = _run(sync_db_engine, list_config(list_table))

    # Assert
    assert result.repaired_count == 1
    assert set(_list_children(sync_db_engine, branch)) == {f"{branch}__eu", f"{branch}__us"}


def test__list__group_matched_by_values_under_a_foreign_name__left_alone(
    sync_db_engine: Engine, list_table: str
) -> None:
    # Arrange: another tool created the same value set under a different name.
    branch = f"{list_table}{WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    _exec(sync_db_engine, f"""CREATE TABLE "{branch}__europe" PARTITION OF "{branch}" FOR VALUES IN ('de', 'fr')""")
    _exec(sync_db_engine, f"""CREATE TABLE "{branch}__usa" PARTITION OF "{branch}" FOR VALUES IN ('us')""")
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{list_table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')",
    )

    # Act
    result = _run(sync_db_engine, list_config(list_table))

    # Assert: matched by the values they own, so nothing is duplicated.
    assert result.repaired_count == 0
    assert set(_list_children(sync_db_engine, branch)) == {f"{branch}__europe", f"{branch}__usa"}


def test__list__value_owned_by_another_partition__reported_and_not_mutated(
    sync_db_engine: Engine, list_table: str
) -> None:
    # Arrange: "de" sits in a partition that is not the configured EU group.
    branch = f"{list_table}{WEEK_SUFFIX}"
    _exec(sync_db_engine, f'CREATE TABLE "{branch}" (LIKE "{list_table}" INCLUDING ALL) PARTITION BY LIST (region)')
    _exec(sync_db_engine, f"""CREATE TABLE "{branch}__dach" PARTITION OF "{branch}" FOR VALUES IN ('de', 'at')""")
    _exec(
        sync_db_engine,
        f'ALTER TABLE "{list_table}" ATTACH PARTITION "{branch}" '
        f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')",
    )

    # Act
    result = _run(sync_db_engine, list_config(list_table))

    # Assert: the non-conflicting group is still created; the clash is reported.
    assert result.success
    assert set(_list_children(sync_db_engine, branch)) == {f"{branch}__dach", f"{branch}__us"}
    issues = [i for i in result.issues if i.partition_name == f"public.{branch}"]
    assert len(issues) == 1
    assert "'de'" in issues[0].error


def test__list__over_hash__builds_and_routes_through_both_levels(sync_db_engine: Engine, list_table: str) -> None:
    # Arrange: RANGE(created_at) -> LIST(region) -> HASH(tenant_id)
    _run(sync_db_engine, list_config(list_table, inner_modulus=2))
    branch = f"{list_table}{WEEK_SUFFIX}"

    # Act
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(f'INSERT INTO "{list_table}" (region, tenant_id, created_at) VALUES (:r, 1, :d)'),  # noqa: S608
            {"r": "de", "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
        )
        routed = conn.execute(
            text(f'SELECT tableoid::regclass::text FROM "{list_table}"')  # noqa: S608
        )
        leaf = str(routed.scalar())

    # Assert
    assert set(_children(sync_db_engine, f"{branch}__eu")) == {f"{branch}__eu__h0", f"{branch}__eu__h1"}
    assert leaf in {f"{branch}__eu__h0", f"{branch}__eu__h1"}


def test__list__expired_branch__dropped_with_its_whole_subtree(sync_db_engine: Engine, list_table: str) -> None:
    # Arrange
    config = list_config(list_table, retention=1)
    with freezegun.freeze_time("2026-08-10"):
        _maintainer(sync_db_engine).run_maintenance(config)
    old_branch = f"{list_table}__2026_w33"
    assert _relkind(sync_db_engine, old_branch) == "p"

    # Act
    result = _run(sync_db_engine, config)

    # Assert
    assert result.dropped_count == 1
    assert _relkind(sync_db_engine, old_branch) is None
    assert _relkind(sync_db_engine, f"{old_branch}__eu") is None


# ── Static roots (no time dimension) ────────────────────────────────────────────


def _static_maintainer(engine: Engine) -> PartitionMaintainer:
    """A lifecycle service with no period calculator, as a static root needs."""
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine),
        locks=PostgresAdvisoryLockManager(engine),
    )
    return PartitionMaintainer(service)


def test__hash_root__fresh_table__creates_every_bucket(sync_db_engine: Engine, hash_root_table: str) -> None:
    # Arrange / Act
    result = _static_maintainer(sync_db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=4))

    # Assert: the table's own partitions, not a subtree inside a period.
    assert result.success
    assert result.created_count == 4
    assert _children(sync_db_engine, hash_root_table) == {f"{hash_root_table}__h{r}": (4, r) for r in range(4)}


def test__hash_root__second_run__executes_zero_ddl(sync_db_engine: Engine, hash_root_table: str) -> None:
    # Arrange
    config = hash_root_config(hash_root_table, modulus=2)
    _static_maintainer(sync_db_engine).run_maintenance(config)

    # Act
    with _count_ddl(sync_db_engine) as counter:
        result = _static_maintainer(sync_db_engine).run_maintenance(config)

    # Assert
    assert result.created_count == 0
    assert counter.statements == []


def test__hash_root__missing_bucket__is_repaired(sync_db_engine: Engine, hash_root_table: str) -> None:
    # Arrange: an incomplete set, as a partial migration would leave.
    for remainder in (0, 1, 3):
        _exec(
            sync_db_engine,
            f'CREATE TABLE "{hash_root_table}__h{remainder}" PARTITION OF "{hash_root_table}" '
            f"FOR VALUES WITH (MODULUS 4, REMAINDER {remainder})",
        )

    # Act
    result = _static_maintainer(sync_db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=4))

    # Assert
    assert result.created_count == 1
    assert len(_children(sync_db_engine, hash_root_table)) == 4


def test__hash_root__nothing_is_ever_pruned(sync_db_engine: Engine, hash_root_table: str) -> None:
    # Arrange / Act
    result = _static_maintainer(sync_db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=2))

    # Assert: a static root has no periods, so nothing ages out of it.
    assert result.detached_count == 0
    assert result.dropped_count == 0


def test__hash_root__rows_route_into_the_buckets(sync_db_engine: Engine, hash_root_table: str) -> None:
    # Arrange
    _static_maintainer(sync_db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=2))

    # Act
    with sync_db_engine.begin() as conn:
        for tenant in range(1, 9):
            conn.execute(
                text(f'INSERT INTO "{hash_root_table}" (tenant_id) VALUES (:t)'),  # noqa: S608
                {"t": tenant},
            )
        rows = conn.execute(
            text(f'SELECT DISTINCT tableoid::regclass::text FROM "{hash_root_table}"')  # noqa: S608
        )
        leaves = {str(r[0]) for r in rows.fetchall()}

    # Assert
    assert leaves <= {f"{hash_root_table}__h0", f"{hash_root_table}__h1"}
    assert leaves


def test__hash_root__with_a_nested_level__builds_both(sync_db_engine: Engine, hash_root_table: str) -> None:
    # Arrange / Act: HASH(tenant_id) -> HASH(id)
    _static_maintainer(sync_db_engine).run_maintenance(hash_root_config(hash_root_table, modulus=2, inner_modulus=2))

    # Assert
    assert set(_children(sync_db_engine, hash_root_table)) == {
        f"{hash_root_table}__h0",
        f"{hash_root_table}__h1",
    }
    assert set(_children(sync_db_engine, f"{hash_root_table}__h0")) == {
        f"{hash_root_table}__h0__h0",
        f"{hash_root_table}__h0__h1",
    }


def test__list_root__fresh_table__creates_one_partition_per_group(sync_db_engine: Engine, list_root_table: str) -> None:
    # Arrange / Act
    result = _static_maintainer(sync_db_engine).run_maintenance(list_root_config(list_root_table, include_default=True))

    # Assert
    assert result.success
    assert _list_children(sync_db_engine, list_root_table) == {
        f"{list_root_table}__eu": ("de", "fr"),
        f"{list_root_table}__us": ("us",),
    }
    assert _relkind(sync_db_engine, f"{list_root_table}__other") == "r"


def test__list_root__rows_route_by_value(sync_db_engine: Engine, list_root_table: str) -> None:
    # Arrange
    _static_maintainer(sync_db_engine).run_maintenance(list_root_config(list_root_table, include_default=True))

    # Act
    with sync_db_engine.begin() as conn:
        for region in ("de", "us", "jp"):
            conn.execute(
                text(f'INSERT INTO "{list_root_table}" (region) VALUES (:r)'),  # noqa: S608
                {"r": region},
            )
        rows = conn.execute(
            text(f'SELECT region, tableoid::regclass::text FROM "{list_root_table}" ORDER BY region')  # noqa: S608
        )
        routed = {str(r[0]): str(r[1]) for r in rows.fetchall()}

    # Assert
    assert routed == {
        "de": f"{list_root_table}__eu",
        "us": f"{list_root_table}__us",
        "jp": f"{list_root_table}__other",
    }


def test__static_root__time_based_api_without_a_calculator__refused_clearly(
    sync_db_engine: Engine, hash_root_table: str
) -> None:
    # Arrange
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(sync_db_engine),
        metadata=PostgresMetadataProvider(sync_db_engine),
        locks=PostgresAdvisoryLockManager(sync_db_engine),
    )

    # Act / Assert: create-ahead is period arithmetic and there are no periods.
    with pytest.raises(InvalidPartitionConfigError, match="period_calculator"):
        service.create_future_partitions(flat_config(hash_root_table))


# ── Composite partition keys ────────────────────────────────────────────────────


def test__composite_key__fresh_table__creates_the_period_partition(
    sync_db_engine: Engine, composite_table: str
) -> None:
    # Arrange / Act
    result = _run(sync_db_engine, composite_config(composite_table))

    # Assert
    assert result.success
    assert result.created_count == 1
    assert _relkind(sync_db_engine, f"{composite_table}{WEEK_SUFFIX}") == "r"


def test__composite_key__bounds_pad_trailing_columns_with_minvalue(
    sync_db_engine: Engine, composite_table: str
) -> None:
    # Arrange
    _run(sync_db_engine, composite_config(composite_table))

    # Act
    with sync_db_engine.connect() as conn:
        result = conn.execute(
            text("SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = to_regclass(:n)"),
            {"n": f'"{composite_table}{WEEK_SUFFIX}"'},
        )
        bounds = str(result.scalar())

    # Assert
    assert bounds.count("MINVALUE") == 2
    assert "2026-08-24" in bounds
    assert "2026-08-31" in bounds


def test__composite_key__rows_route_by_the_leading_column_alone(sync_db_engine: Engine, composite_table: str) -> None:
    # Arrange
    _run(sync_db_engine, composite_config(composite_table, create_ahead=2))
    branch = f"{composite_table}{WEEK_SUFFIX}"

    # Act: wildly different trailing values, same period.
    with sync_db_engine.begin() as conn:
        for tenant in (-9999, 0, 1, 999999):
            conn.execute(
                text(f'INSERT INTO "{composite_table}" (tenant_id, created_at) VALUES (:t, :d)'),  # noqa: S608
                {"t": tenant, "d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
            )
        rows = conn.execute(
            text(f'SELECT DISTINCT tableoid::regclass::text FROM "{composite_table}"')  # noqa: S608
        )
        leaves = {str(r[0]) for r in rows.fetchall()}

    # Assert: the trailing column does not affect placement.
    assert leaves == {branch}


def test__composite_key__second_run__executes_zero_ddl(sync_db_engine: Engine, composite_table: str) -> None:
    # Arrange
    config = composite_config(composite_table)
    _run(sync_db_engine, config)

    # Act
    with _count_ddl(sync_db_engine) as counter:
        result = _run(sync_db_engine, config)

    # Assert
    assert result.created_count == 0
    assert counter.statements == []


def test__composite_key__retention__prunes_by_the_leading_bound(sync_db_engine: Engine, composite_table: str) -> None:
    # Arrange
    config = composite_config(composite_table, retention=1)
    with freezegun.freeze_time("2026-08-10"):
        _maintainer(sync_db_engine).run_maintenance(config)
    old = f"{composite_table}__2026_w33"
    assert _relkind(sync_db_engine, old) == "r"

    # Act
    result = _run(sync_db_engine, config)

    # Assert: parsing a composite bound yields the leading value, so retention
    # compares the same instant it would for a single-column key.
    assert result.dropped_count == 1
    assert _relkind(sync_db_engine, old) is None


def test__composite_key__introspection__reports_the_key_in_key_order(
    sync_db_engine: Engine, composite_table: str
) -> None:
    # Arrange
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act
    columns = metadata.get_partition_columns(composite_table)

    # Assert: key order, which is not column order.
    assert columns == ("created_at", "tenant_id")


def test__composite_key__config_disagreeing_with_the_table__refused(
    sync_db_engine: Engine, composite_table: str
) -> None:
    # Arrange: the real key is (created_at, tenant_id).
    config = composite_config(composite_table).model_copy(update={"trailing_partition_columns": ("id",)})

    # Act / Assert
    with freezegun.freeze_time(FROZEN_WEEK), pytest.raises(InvalidPartitionConfigError, match="key mismatch"):
        _maintainer(sync_db_engine).run_maintenance(config)


# ── Keys and bounds the catalog spells differently from the config ──────────────


def test__nullable_trailing_key__null_row_in_default__attach_still_succeeds(
    sync_db_engine: Engine, nullable_composite_table: str
) -> None:
    # Arrange: a DEFAULT partition holding one row that belongs to the upcoming
    # week and one whose NULL tenant keeps it in DEFAULT whatever its week.
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(f'CREATE TABLE "{nullable_composite_table}_default" PARTITION OF "{nullable_composite_table}" DEFAULT')
        )
        conn.execute(
            text(f'INSERT INTO "{nullable_composite_table}" (tenant_id, created_at) VALUES (7, :d), (NULL, :d)'),  # noqa: S608
            {"d": datetime(2026, 8, 25, 10, tzinfo=UTC)},
        )

    # Act
    result = _run(sync_db_engine, nullable_composite_config(nullable_composite_table))

    # Assert: moving the NULL row out would be rejected with the very error the
    # move exists to clear, and the retry would never converge.
    assert result.success
    assert _relkind(sync_db_engine, f"{nullable_composite_table}{WEEK_SUFFIX}") == "r"

    with sync_db_engine.begin() as conn:
        rows = conn.execute(
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


def test__expression_partition_key__is_refused_rather_than_silently_shortened(
    sync_db_engine: Engine, expression_table: str
) -> None:
    # Arrange
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act / Assert: an expression key is recorded as attnum 0, and dropping that
    # position would report a shorter key than the table really has.
    with pytest.raises(InvalidPartitionConfigError, match="expression"):
        metadata.get_partition_columns(expression_table)


def test__date_shaped_bound_that_is_not_a_date__reports_not_closed(
    sync_db_engine: Engine, sortable_id_table: str
) -> None:
    # Arrange: a sortable identifier whose prefix gets past any regex guard
    # worth writing and still fails the cast.
    partition = f"{sortable_id_table}__early"
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE TABLE "{partition}" PARTITION OF "{sortable_id_table}" '
                f"FOR VALUES FROM ('2020-01-01-aaaa') TO ('2026-08-28-a1b2c3')"
            )
        )
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act
    closed = metadata.is_partition_closed(partition)

    # Assert: "not closed" is the documented answer for a bound this provider
    # cannot read. Raising out of a predicate is not.
    assert closed is False


def test__list_partition_holding_null__is_not_read_as_the_string_null(
    sync_db_engine: Engine, nullable_list_table: str
) -> None:
    # Arrange
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE TABLE "{nullable_list_table}__unknown" '
                f'PARTITION OF "{nullable_list_table}" FOR VALUES IN (NULL)'
            )
        )
        conn.execute(
            text(
                f'CREATE TABLE "{nullable_list_table}__literal" '
                f"""PARTITION OF "{nullable_list_table}" FOR VALUES IN ('NULL')"""
            )
        )
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act
    tree = metadata.get_partition_tree(nullable_list_table)

    # Assert: reading NULL as the three-character string would make the planner
    # propose a partition PostgreSQL already has, on every run.
    assert tree is not None
    by_name = {c.name: c.bounds for c in tree.children}
    assert by_name[f"public.{nullable_list_table}__unknown"] == ListBounds(values=(), includes_null=True)
    assert by_name[f"public.{nullable_list_table}__literal"] == ListBounds(values=("NULL",))


# ── Fixes confirmed against the server, not against a mock of it ────────────────


def test__default_conflict__default_partition_ordered_differently__values_are_not_transposed(
    sync_db_engine: Engine, transposed_default_table: str
) -> None:
    # Arrange: a DEFAULT partition whose physical column order differs from the
    # root's. ATTACH permits it, because it matches columns by name.
    table = transposed_default_table
    with sync_db_engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE "{table}_default" (created_at TIMESTAMPTZ NOT NULL, note TEXT, label TEXT)'))
        conn.execute(text(f'ALTER TABLE "{table}" ATTACH PARTITION "{table}_default" DEFAULT'))
        conn.execute(
            text(f'INSERT INTO "{table}" VALUES (:d, :label, :note)'),  # noqa: S608
            {"d": datetime(2026, 8, 25, tzinfo=UTC), "label": "LABEL-A", "note": "NOTE-A"},
        )

    # Act: the DEFAULT holds a row belonging to the period being created, so
    # the reconcile-and-retry path runs unattended.
    result = _run(sync_db_engine, flat_config(table))

    # Assert: moving by position would put NOTE-A in label with the source row
    # already deleted, and report success while doing it.
    assert result.success
    with sync_db_engine.connect() as conn:
        row = (conn.execute(text(f'SELECT label, note FROM "{table}{WEEK_SUFFIX}"'))).fetchone()  # noqa: S608
    assert row is not None
    assert (row[0], row[1]) == ("LABEL-A", "NOTE-A")


def test__bare_unique_index__missing_the_hash_column__is_refused_before_any_ddl(
    sync_db_engine: Engine, bare_unique_index_table: str
) -> None:
    # Arrange: uniqueness comes from an index with no constraint behind it.
    table = bare_unique_index_table
    with sync_db_engine.begin() as conn:
        conn.execute(text(f'CREATE UNIQUE INDEX ON "{table}" (id, created_at)'))

    # Act / Assert: LIKE ... INCLUDING ALL copies the index, so PostgreSQL
    # rejects the branch exactly as it would a named constraint -- mid-run,
    # after other tables have already been changed.
    with pytest.raises(InvalidPartitionConfigError, match="tenant_id"):
        _run(sync_db_engine, nested_config(table, modulus=2))

    assert _relkind(sync_db_engine, f"{table}{WEEK_SUFFIX}") is None


def test__expression_key_branch__is_reported_rather_than_treated_as_a_match(
    sync_db_engine: Engine, no_constraint_table: str
) -> None:
    # Arrange: a branch partitioned by HASH over an expression as well as the
    # configured column. The catalog names only the column, so a shortened key
    # compares equal to a one-column spec.
    branch = f"{no_constraint_table}{WEEK_SUFFIX}"
    with sync_db_engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE TABLE "{branch}" (LIKE "{no_constraint_table}" INCLUDING ALL EXCLUDING IDENTITY) '
                f"PARTITION BY HASH (tenant_id, (id + 1))"
            )
        )
        conn.execute(
            text(
                f'ALTER TABLE "{no_constraint_table}" ATTACH PARTITION "{branch}" '
                f"FOR VALUES FROM ('{WEEK_BOUNDS[0]}') TO ('{WEEK_BOUNDS[1]}')"
            )
        )

    # Act
    result = _run(sync_db_engine, nested_config(no_constraint_table, modulus=2))

    # Assert: planning against it would build bounds of the wrong arity.
    assert result.success
    reasons = {issue.error for issue in result.issues}
    assert any("expression" in reason for reason in reasons)
