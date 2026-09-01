"""A sliding LIST rotated by application state, against a real PostgreSQL (async).

GitLab's ``ci_builds`` pattern: ``LIST (partition_id)``, one integer value per
partition, the application writes the newest value, and maintenance opens the
next value once the newest partition is full enough.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.lifecycle import CreateNextIf, DetachMode, KeepNewest, LifecyclePolicy, SqlPredicate
from pg_partsmith.plan import FindingReason, Reason
from pg_partsmith.topology import ListBounds
from tests.integration.aio.support import (
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
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def builds(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, SLIDING_LIST_TABLE_DDL, prefix="builds"):
        yield name


async def _fill(engine: AsyncEngine, table: str, value: int, rows: int) -> None:
    await exec_sql(
        engine,
        f"INSERT INTO \"{table}\" (partition_id, status) SELECT :value, 'done' FROM generate_series(1, :rows)",  # noqa: S608
        value=value,
        rows=rows,
    )


async def test__sliding_list__empty_table__opens_the_first_value_and_then_rests(
    db_engine: AsyncEngine, builds: str
) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100)

    # Act
    first = await run_maintenance(db_engine, config)
    with count_ddl(db_engine) as counter:
        second = await run_maintenance(db_engine, config)

    # Assert
    assert first.created_count == 1
    assert await list_children_of(db_engine, builds) == {f"{builds}__100": ("100",)}
    assert second.created_count == 0
    assert counter.statements == []


async def test__sliding_list__newest_partition_fills_up__the_next_value_is_opened(
    db_engine: AsyncEngine, builds: str
) -> None:
    # Arrange -- rotate once the active partition holds three rows
    config = sliding_list_config(builds, start=100, rotate_at=3)
    await run_maintenance(db_engine, config)
    await _fill(db_engine, builds, 100, rows=2)

    # Act
    unchanged = await run_maintenance(db_engine, config)
    await _fill(db_engine, builds, 100, rows=1)
    rotated = await run_maintenance(db_engine, config)

    # Assert
    assert unchanged.created_count == 0
    assert rotated.created_count == 1
    assert rotated.maintenance_plan is not None
    assert rotated.maintenance_plan.creates[0].reason is Reason.CREATE_NEXT
    assert await list_children_of(db_engine, builds) == {f"{builds}__100": ("100",), f"{builds}__101": ("101",)}
    assert await scalar(db_engine, f'SELECT count(*) FROM "{builds}__100"') == 3  # noqa: S608


async def test__sliding_list__retention__expires_the_oldest_values_behind_the_active_one(
    db_engine: AsyncEngine, builds: str
) -> None:
    # Arrange -- keep the two newest values
    config = sliding_list_config(builds, start=100, rotate_at=1, keep=2)
    for value in (100, 101, 102):
        await run_maintenance(db_engine, config)
        await _fill(db_engine, builds, value, rows=1)

    # Act -- 103 opens; the two newest *existing* values are 101 and 102, so 100 expires
    result = await run_maintenance(db_engine, config)
    children = await list_children_of(db_engine, builds)
    # the next tick sees 103 as the newest and lets 101 go
    again = await run_maintenance(db_engine, config)

    # Assert
    assert (result.created_count, result.detached_count, result.dropped_count) == (1, 1, 1)
    assert children == {f"{builds}__101": ("101",), f"{builds}__102": ("102",), f"{builds}__103": ("103",)}
    assert (again.created_count, again.detached_count, again.dropped_count) == (0, 1, 1)
    assert await list_children_of(db_engine, builds) == {f"{builds}__102": ("102",), f"{builds}__103": ("103",)}


async def test__sliding_list__rows_route_by_value__the_active_partition_receives_them(
    db_engine: AsyncEngine, builds: str
) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100, rotate_at=1)
    await run_maintenance(db_engine, config)
    await _fill(db_engine, builds, 100, rows=1)
    await run_maintenance(db_engine, config)

    # Act
    await _fill(db_engine, builds, 101, rows=5)

    # Assert
    assert await scalar(db_engine, f'SELECT count(*) FROM "{builds}__101"') == 5  # noqa: S608
    assert await scalar(db_engine, f'SELECT count(*) FROM "{builds}"') == 6  # noqa: S608


async def test__sliding_list__ensure_partition__creates_the_named_value(db_engine: AsyncEngine, builds: str) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100)

    # Act
    created = await make_service(db_engine).ensure_partition(config, 250)
    again = await make_service(db_engine).ensure_partition(config, 250)

    # Assert
    assert created is not None
    assert created.name == f"public.{builds}__250"
    assert created.bounds == ListBounds(values=("250",))
    assert again is None
    assert await is_attached(db_engine, f"{builds}__250")


async def test__sliding_list__detached_value_wanted_again__reattached_not_recreated(
    db_engine: AsyncEngine, builds: str
) -> None:
    # Arrange -- 100..102 exist; 101 was detached by hand through the repository
    config = sliding_list_config(builds, start=100, rotate_at=1, keep=5)
    for value in (100, 101):
        await run_maintenance(db_engine, config)
        await _fill(db_engine, builds, value, rows=1)
    await run_maintenance(db_engine, config)
    repo = PostgresPartitionRepository(db_engine)
    await repo.detach_partition(builds, f"{builds}__101", mode=DetachMode.BLOCKING)
    assert not await is_attached(db_engine, f"{builds}__101")

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert
    assert result.attached_count == 1
    assert result.created_count == 0
    assert await is_attached(db_engine, f"{builds}__101")
    assert await scalar(db_engine, f'SELECT count(*) FROM "{builds}__101"') == 1  # noqa: S608


async def test__sliding_list__hand_made_multi_value_partition__reported_and_left_alone(
    db_engine: AsyncEngine, builds: str
) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100, rotate_at=1)
    await exec_sql(db_engine, f'CREATE TABLE "{builds}_legacy" PARTITION OF "{builds}" FOR VALUES IN (1, 2)')

    # Act
    plan = await make_service(db_engine).plan(config)
    result = await run_maintenance(db_engine, config)

    # Assert
    assert [f.reason for f in plan.findings] == [FindingReason.UNMANAGED_PARTITION]
    assert result.created_count == 1
    children = await list_children_of(db_engine, builds)
    assert children[f"{builds}_legacy"] == ("1", "2")
    assert children[f"{builds}__100"] == ("100",)


async def test__sliding_list__list_partitions__reports_the_values(db_engine: AsyncEngine, builds: str) -> None:
    # Arrange
    config = sliding_list_config(builds, start=100)
    await run_maintenance(db_engine, config)

    # Act
    partitions = await PostgresMetadataProvider(db_engine).list_partitions(builds)

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
