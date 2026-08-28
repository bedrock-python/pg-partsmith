"""A sliding LIST rotated by application state, against a real PostgreSQL (sync).

GitLab's ``ci_builds`` pattern: ``LIST (partition_id)``, one integer value per
partition, the application writes the newest value, and maintenance opens the
next value once the newest partition is full enough.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.lifecycle import CreateNextIf, DetachMode, KeepNewest, LifecyclePolicy, SqlPredicate
from pg_partsmith.plan import FindingReason, Reason
from pg_partsmith.topology import ListBounds
from tests.integration.sync.support import (
    count_ddl,
    exec_sql,
    is_attached,
    list_children_of,
    make_service,
    make_table,
    run_maintenance,
    scalar,
)
from tests.integration.nested_support import SLIDING_LIST_TABLE_DDL, sliding_list_config

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration


@pytest.fixture
def builds(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, SLIDING_LIST_TABLE_DDL, prefix="builds")


def _fill(engine: Engine, table: str, value: int, rows: int) -> None:
    exec_sql(
        engine,
        f"INSERT INTO \"{table}\" (partition_id, status) SELECT :value, 'done' FROM generate_series(1, :rows)",  # noqa: S608
        value=value,
        rows=rows,
    )


def test__sliding_list__empty_table__opens_the_first_value_and_then_rests(sync_db_engine: Engine, builds: str) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100)

    # Act
    first = run_maintenance(sync_db_engine, config)
    with count_ddl(sync_db_engine) as counter:
        second = run_maintenance(sync_db_engine, config)

    # Assert
    assert first.created_count == 1
    assert list_children_of(sync_db_engine, builds) == {f"{builds}__100": ("100",)}
    assert second.created_count == 0
    assert counter.statements == []


def test__sliding_list__newest_partition_fills_up__the_next_value_is_opened(
    sync_db_engine: Engine, builds: str
) -> None:
    # Arrange -- rotate once the active partition holds three rows
    config = sliding_list_config(builds, start=100, rotate_at=3)
    run_maintenance(sync_db_engine, config)
    _fill(sync_db_engine, builds, 100, rows=2)

    # Act
    unchanged = run_maintenance(sync_db_engine, config)
    _fill(sync_db_engine, builds, 100, rows=1)
    rotated = run_maintenance(sync_db_engine, config)

    # Assert
    assert unchanged.created_count == 0
    assert rotated.created_count == 1
    assert rotated.maintenance_plan is not None
    assert rotated.maintenance_plan.creates[0].reason is Reason.CREATE_NEXT
    assert list_children_of(sync_db_engine, builds) == {f"{builds}__100": ("100",), f"{builds}__101": ("101",)}
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{builds}__100"') == 3  # noqa: S608


def test__sliding_list__retention__expires_the_oldest_values_behind_the_active_one(
    sync_db_engine: Engine, builds: str
) -> None:
    # Arrange -- keep the two newest values
    config = sliding_list_config(builds, start=100, rotate_at=1, keep=2)
    for value in (100, 101, 102):
        run_maintenance(sync_db_engine, config)
        _fill(sync_db_engine, builds, value, rows=1)

    # Act -- 103 opens; the two newest *existing* values are 101 and 102, so 100 expires
    result = run_maintenance(sync_db_engine, config)
    children = list_children_of(sync_db_engine, builds)
    # the next tick sees 103 as the newest and lets 101 go
    again = run_maintenance(sync_db_engine, config)

    # Assert
    assert (result.created_count, result.detached_count, result.dropped_count) == (1, 1, 1)
    assert children == {f"{builds}__101": ("101",), f"{builds}__102": ("102",), f"{builds}__103": ("103",)}
    assert (again.created_count, again.detached_count, again.dropped_count) == (0, 1, 1)
    assert list_children_of(sync_db_engine, builds) == {f"{builds}__102": ("102",), f"{builds}__103": ("103",)}


def test__sliding_list__rows_route_by_value__the_active_partition_receives_them(
    sync_db_engine: Engine, builds: str
) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100, rotate_at=1)
    run_maintenance(sync_db_engine, config)
    _fill(sync_db_engine, builds, 100, rows=1)
    run_maintenance(sync_db_engine, config)

    # Act
    _fill(sync_db_engine, builds, 101, rows=5)

    # Assert
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{builds}__101"') == 5  # noqa: S608
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{builds}"') == 6  # noqa: S608


def test__sliding_list__ensure_partition__creates_the_named_value(sync_db_engine: Engine, builds: str) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100)

    # Act
    created = make_service(sync_db_engine).ensure_partition(config, 250)
    again = make_service(sync_db_engine).ensure_partition(config, 250)

    # Assert
    assert created is not None
    assert created.name == f"public.{builds}__250"
    assert created.bounds == ListBounds(values=("250",))
    assert again is None
    assert is_attached(sync_db_engine, f"{builds}__250")


def test__sliding_list__detached_value_wanted_again__reattached_not_recreated(
    sync_db_engine: Engine, builds: str
) -> None:
    # Arrange -- 100..102 exist; 101 was detached by hand through the repository
    config = sliding_list_config(builds, start=100, rotate_at=1, keep=5)
    for value in (100, 101):
        run_maintenance(sync_db_engine, config)
        _fill(sync_db_engine, builds, value, rows=1)
    run_maintenance(sync_db_engine, config)
    repo = PostgresPartitionRepository(sync_db_engine)
    repo.detach_partition(builds, f"{builds}__101", mode=DetachMode.BLOCKING)
    assert not is_attached(sync_db_engine, f"{builds}__101")

    # Act
    result = run_maintenance(sync_db_engine, config)

    # Assert
    assert result.attached_count == 1
    assert result.created_count == 0
    assert is_attached(sync_db_engine, f"{builds}__101")
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{builds}__101"') == 1  # noqa: S608


def test__sliding_list__hand_made_multi_value_partition__reported_and_left_alone(
    sync_db_engine: Engine, builds: str
) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100, rotate_at=1)
    exec_sql(sync_db_engine, f'CREATE TABLE "{builds}_legacy" PARTITION OF "{builds}" FOR VALUES IN (1, 2)')

    # Act
    plan = make_service(sync_db_engine).plan(config)
    result = run_maintenance(sync_db_engine, config)

    # Assert
    assert [f.reason for f in plan.findings] == [FindingReason.UNMANAGED_PARTITION]
    assert result.created_count == 1
    children = list_children_of(sync_db_engine, builds)
    assert children[f"{builds}_legacy"] == ("1", "2")
    assert children[f"{builds}__100"] == ("100",)


def test__sliding_list__list_partitions__reports_the_values(sync_db_engine: Engine, builds: str) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100)
    run_maintenance(sync_db_engine, config)

    # Act
    partitions = PostgresMetadataProvider(sync_db_engine).list_partitions(builds)

    # Assert
    (info,) = partitions
    assert info.name == f"public.{builds}__100"
    assert info.bounds == ListBounds(values=("100",))
    assert info.boundaries_expr == "FOR VALUES IN ('100')"


def test__sliding_list_config__is_state_driven() -> None:
    config = sliding_list_config("builds", start=100, rotate_at=3, keep=2)

    assert config.lifecycle == LifecyclePolicy(
        creation=CreateNextIf(when=SqlPredicate(sql="SELECT count(*) >= 3 FROM {partition}")),
        retention=KeepNewest(count=2),
    )
