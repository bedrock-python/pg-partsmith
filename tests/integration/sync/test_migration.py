"""Batched data movement against a real PostgreSQL (sync).

``partition_data`` drains a DEFAULT partition -- the migration path for a
table partitioned around its data -- and ``unpartition`` moves everything back
into one plain table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pg_partsmith.entities import MaintenanceIssueStep
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from tests.integration.nested_support import (
    MONTHLY_TABLE_DDL,
    NULLABLE_COMPOSITE_TABLE_DDL,
    TIMESTAMP_TABLE_DDL,
    monthly_config,
    nested_config,
    nullable_composite_config,
)
from tests.integration.sync.support import (
    exec_sql,
    hash_children_of,
    is_attached,
    make_service,
    make_table,
    range_children_of,
    relkind,
    scalar,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration


@pytest.fixture
def events(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="mig")


@pytest.fixture
def tenants(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, TIMESTAMP_TABLE_DDL, prefix="nmig")


@pytest.fixture
def nullable(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, NULLABLE_COMPOSITE_TABLE_DDL, prefix="cmig")


def _default_with_rows(engine: Engine, table: str, *, months: tuple[int, ...], per_month: int) -> str:
    """The migration starting point: the old monolithic table attached as DEFAULT, full of rows."""
    default = f"{table}_legacy"
    exec_sql(engine, f'CREATE TABLE "{default}" (LIKE "{table}" INCLUDING ALL)')
    for month in months:
        exec_sql(
            engine,
            f'INSERT INTO "{default}" (created_at, payload) '  # noqa: S608
            f"SELECT make_timestamptz(2026, :month, 1 + (g % 27), 12, 0, 0, 'UTC'), 'row ' || g "
            f"FROM generate_series(1, :rows) g",
            month=month,
            rows=per_month,
        )
    exec_sql(engine, f'ALTER TABLE "{table}" ATTACH PARTITION "{default}" DEFAULT')
    return default


def _count(engine: Engine, table: str) -> int:
    return int(scalar(engine, f'SELECT count(*) FROM "{table}"'))  # noqa: S608


# ── partition_data ──────────────────────────────────────────────────────────────


def test__partition_data__drains_default_into_monthly_partitions_in_batches(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- 25 rows across three months in the DEFAULT partition
    default = _default_with_rows(sync_db_engine, events, months=(3, 4, 6), per_month=25)
    config = monthly_config(events, create_ahead=1)

    # Act
    result = make_service(sync_db_engine).partition_data(config, batch_rows=10)

    # Assert
    assert result.complete
    assert result.rows_moved == 75
    assert result.batches == 9  # 3 windows x (10 + 10 + 5)
    assert result.partitions == (
        f"public.{events}__2026_03",
        f"public.{events}__2026_04",
        f"public.{events}__2026_06",
    )
    assert result.issues == ()
    assert _count(sync_db_engine, default) == 0
    assert _count(sync_db_engine, events) == 75
    assert _count(sync_db_engine, f"{events}__2026_04") == 25
    children = range_children_of(sync_db_engine, events)
    assert set(children) == {f"{events}__2026_03", f"{events}__2026_04", f"{events}__2026_06"}
    assert is_attached(sync_db_engine, default)


def test__partition_data__batch_budget__resumes_where_it_stopped(sync_db_engine: Engine, events: str) -> None:
    # Arrange
    default = _default_with_rows(sync_db_engine, events, months=(3,), per_month=25)
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)

    # Act -- two batches: the March partition exists but stays detached and invisible
    first = service.partition_data(config, batch_rows=10, max_batches=2)
    visible_between = _count(sync_db_engine, events)
    second = service.partition_data(config, batch_rows=10)

    # Assert
    assert (first.rows_moved, first.batches, first.complete, first.partitions) == (20, 2, False, ())
    assert relkind(sync_db_engine, f"{events}__2026_03") == "r"
    assert visible_between == 5  # the 20 moved rows sit in the detached table
    assert (second.rows_moved, second.complete, second.partitions) == (5, True, (f"public.{events}__2026_03",))
    assert is_attached(sync_db_engine, f"{events}__2026_03")
    assert _count(sync_db_engine, default) == 0
    assert _count(sync_db_engine, events) == 25


def test__partition_data__nested_scheme__rows_land_in_the_buckets(sync_db_engine: Engine, tenants: str) -> None:
    # Arrange
    default = f"{tenants}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{default}" (LIKE "{tenants}" INCLUDING ALL)')
    exec_sql(
        sync_db_engine,
        f'INSERT INTO "{default}" (tenant_id, created_at, payload) '  # noqa: S608
        f"SELECT g % 5, '2026-08-25 10:00:00+00', 'p' FROM generate_series(1, 20) g",
    )
    exec_sql(sync_db_engine, f'ALTER TABLE "{tenants}" ATTACH PARTITION "{default}" DEFAULT')
    config = nested_config(tenants, modulus=2)

    # Act
    result = make_service(sync_db_engine).partition_data(config, batch_rows=7)

    # Assert
    assert result.complete
    assert result.rows_moved == 20
    assert result.partitions == (f"public.{tenants}__2026_w35",)
    buckets = hash_children_of(sync_db_engine, f"{tenants}__2026_w35")
    assert set(buckets.values()) == {(2, 0), (2, 1)}
    assert _count(sync_db_engine, default) == 0
    assert _count(sync_db_engine, f"{tenants}__2026_w35") == 20


def test__partition_data__no_default_partition__nothing_to_do(sync_db_engine: Engine, events: str) -> None:
    # Act
    result = make_service(sync_db_engine).partition_data(monthly_config(events))

    # Assert
    assert result.complete
    assert result.rows_moved == 0


def test__partition_data__rows_with_a_null_trailing_key__stay_in_default(sync_db_engine: Engine, nullable: str) -> None:
    # Arrange -- the composite key (created_at, tenant_id); PostgreSQL routes a NULL tenant to DEFAULT
    default = f"{nullable}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{default}" (LIKE "{nullable}" INCLUDING ALL)')
    exec_sql(
        sync_db_engine,
        f'INSERT INTO "{default}" (tenant_id, created_at) '  # noqa: S608
        "VALUES (1, '2026-08-25'), (NULL, '2026-08-25')",
    )
    exec_sql(sync_db_engine, f'ALTER TABLE "{nullable}" ATTACH PARTITION "{default}" DEFAULT')
    config = nullable_composite_config(nullable)

    # Act
    result = make_service(sync_db_engine).partition_data(config)

    # Assert
    assert result.complete
    assert result.rows_moved == 1
    assert _count(sync_db_engine, default) == 1


def test__partition_data__window_held_by_an_unmanaged_partition__stops_with_an_issue(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- a hand-made partition straddles March and April, so neither month can be created;
    # a March row it does not cover sits in DEFAULT
    exec_sql(
        sync_db_engine,
        f"CREATE TABLE \"{events}_odd\" PARTITION OF \"{events}\" FOR VALUES FROM ('2026-03-15') TO ('2026-04-15')",
    )
    default = f"{events}_legacy"
    exec_sql(sync_db_engine, f'CREATE TABLE "{default}" (LIKE "{events}" INCLUDING ALL)')
    exec_sql(sync_db_engine, f"INSERT INTO \"{default}\" (created_at, payload) VALUES ('2026-03-03', 'x')")  # noqa: S608
    exec_sql(sync_db_engine, f'ALTER TABLE "{events}" ATTACH PARTITION "{default}" DEFAULT')

    # Act
    result = make_service(sync_db_engine).partition_data(monthly_config(events, create_ahead=1))

    # Assert
    assert not result.complete
    assert result.rows_moved == 0
    assert [issue.step for issue in result.issues][-1] is MaintenanceIssueStep.MOVE
    assert "no partition can be created" in result.issues[-1].error
    assert _count(sync_db_engine, default) == 1


def test__partition_data__non_range_root__refused(sync_db_engine: Engine, events: str) -> None:
    from tests.integration.nested_support import HASH_ROOT_TABLE_DDL, hash_root_config  # noqa: PLC0415

    for tasks in make_table(sync_db_engine, HASH_ROOT_TABLE_DDL, prefix="hmig"):
        with pytest.raises(InvalidPartitionConfigError, match="not a RANGE level"):
            make_service(sync_db_engine).partition_data(hash_root_config(tasks))


# ── unpartition ─────────────────────────────────────────────────────────────────


def test__unpartition__moves_everything_into_one_table_and_drops_the_partitions(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- three partitions with rows, made by the mover itself
    _default_with_rows(sync_db_engine, events, months=(3, 4, 6), per_month=10)
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)
    service.partition_data(config)
    flat = f"{events}_flat"

    # Act
    result = service.unpartition(config, flat, batch_rows=4, drop_emptied=True)

    # Assert
    assert result.complete
    assert result.rows_moved == 30
    assert result.partitions == (
        f"public.{events}__2026_03",
        f"public.{events}__2026_04",
        f"public.{events}__2026_06",
        f"public.{events}_legacy",
    )
    assert _count(sync_db_engine, flat) == 30
    assert _count(sync_db_engine, events) == 0
    assert range_children_of(sync_db_engine, events) == {}
    assert relkind(sync_db_engine, f"{events}__2026_03") is None
    assert PostgresMetadataProvider(sync_db_engine).list_partitions(events) == []


def test__unpartition__without_dropping__partitions_stay_attached_and_empty(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange
    _default_with_rows(sync_db_engine, events, months=(5,), per_month=6)
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)
    service.partition_data(config)
    flat = f"{events}_flat"
    exec_sql(sync_db_engine, f'CREATE TABLE "{flat}" (LIKE "{events}" INCLUDING ALL)')

    # Act
    result = service.unpartition(config, flat)

    # Assert
    assert result.complete
    assert result.rows_moved == 6
    assert is_attached(sync_db_engine, f"{events}__2026_05")
    assert _count(sync_db_engine, f"{events}__2026_05") == 0
    assert _count(sync_db_engine, flat) == 6


def test__unpartition__batch_budget__stops_and_resumes(sync_db_engine: Engine, events: str) -> None:
    # Arrange
    _default_with_rows(sync_db_engine, events, months=(5,), per_month=9)
    config = monthly_config(events, create_ahead=1)
    service = make_service(sync_db_engine)
    service.partition_data(config)
    flat = f"{events}_flat"

    # Act
    first = service.unpartition(config, flat, batch_rows=4, max_batches=1)
    second = service.unpartition(config, flat, batch_rows=4)

    # Assert
    assert (first.rows_moved, first.complete) == (4, False)
    assert (second.rows_moved, second.complete) == (5, True)
    assert _count(sync_db_engine, flat) == 9
