"""Foreign keys and the lifecycle, against a real PostgreSQL (async).

PostgreSQL refuses to detach a partition whose rows another table still
references through a foreign key on the parent (``23503``, verified identical
on 15 and 17). ``Unreferenced()`` keeps such partitions out of the plan; a
refused detach that does get planned is an issue, not the end of the run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from pg_partsmith.entities import MaintenanceIssueStep
from pg_partsmith.lifecycle import AllOf, CreateAhead, ExpireIf, KeepNewest, LifecyclePolicy, Unreferenced
from pg_partsmith.topology import FactKind
from tests.integration.aio.support import (
    exec_sql,
    is_attached,
    make_service,
    make_table,
    range_children_of,
    run_maintenance,
    scalar,
)
from tests.integration.nested_support import MONTHLY_TABLE_DDL, monthly_config

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

    from pg_partsmith.entities import TablePartitionConfig

pytestmark = pytest.mark.integration

NOW = "2026-08-26"


@pytest_asyncio.fixture
async def events(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, MONTHLY_TABLE_DDL, prefix="fk"):
        yield name


@pytest_asyncio.fixture
async def refs(db_engine: AsyncEngine, events: str) -> AsyncGenerator[str, None]:
    """A table whose foreign key points at the partitioned parent."""
    name = f"{events}_refs"
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{name}" (event_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at))',
    )
    yield name
    await exec_sql(db_engine, f'DROP TABLE IF EXISTS "{name}"')


def _keep_unreferenced(table: str, *, keep: int = 1) -> TablePartitionConfig:
    return monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1),
            retention=ExpireIf(when=AllOf(members=(KeepNewest(count=keep), Unreferenced()))),
        ),
    )


async def _seed(engine: AsyncEngine, events: str, refs: str) -> None:
    """June and July partitions with one row each; June's row is referenced."""
    config = monthly_config(events, create_ahead=1)
    await run_maintenance(engine, config, at_time="2026-06-15")
    await run_maintenance(engine, config, at_time="2026-07-15")
    await exec_sql(engine, f"INSERT INTO \"{events}\" (id, created_at, payload) VALUES (1, '2026-06-10', 'june')")  # noqa: S608
    await exec_sql(engine, f"INSERT INTO \"{events}\" (id, created_at, payload) VALUES (2, '2026-07-10', 'july')")  # noqa: S608
    await exec_sql(engine, f"INSERT INTO \"{refs}\" VALUES (1, '2026-06-10')")  # noqa: S608


async def test__unreferenced__referenced_partition_is_kept_and_the_other_expires(
    db_engine: AsyncEngine, events: str, refs: str
) -> None:
    # Arrange -- keep the newest month only; June is still referenced, July is not
    await _seed(db_engine, events, refs)
    config = _keep_unreferenced(events)

    # Act
    plan = await make_service(db_engine).plan(config, now=None)
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert [op.target for op in plan.detaches] == [f"public.{events}__2026_07"]
    assert result.detached_count == 1
    assert result.issues == ()
    assert await is_attached(db_engine, f"{events}__2026_06")
    assert not await is_attached(db_engine, f"{events}__2026_07")


async def test__unreferenced__referencing_rows_gone__the_partition_expires_on_the_next_run(
    db_engine: AsyncEngine, events: str, refs: str
) -> None:
    # Arrange
    await _seed(db_engine, events, refs)
    config = _keep_unreferenced(events)
    await run_maintenance(db_engine, config, at_time=NOW)

    # Act
    await exec_sql(db_engine, f'DELETE FROM "{refs}"')  # noqa: S608
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.detached_count == 1
    assert not await is_attached(db_engine, f"{events}__2026_06")


async def test__unreferenced__measured_only_when_a_rule_asks(db_engine: AsyncEngine, events: str, refs: str) -> None:
    # Arrange
    await _seed(db_engine, events, refs)
    asking = _keep_unreferenced(events)
    silent = monthly_config(events, create_ahead=1, retention=1)

    # Act
    measured = await make_service(db_engine).inspect(asking)
    assert measured is not None
    tree_asking = await make_service(db_engine)._inspector.inspect(asking, measure=True)
    tree_silent = await make_service(db_engine)._inspector.inspect(silent, measure=True)

    # Assert
    assert asking.lifecycle.required_facts == {FactKind.REFERENCES}
    assert tree_asking is not None and tree_silent is not None
    june = tree_asking.find(f"public.{events}__2026_06")
    july = tree_asking.find(f"public.{events}__2026_07")
    assert june is not None and june.facts is not None and june.facts.referenced is True
    assert july is not None and july.facts is not None and july.facts.referenced is False
    assert all(child.facts is None for child in tree_silent.root.children)


async def test__without_unreferenced__refused_detach_is_an_issue_and_the_run_goes_on(
    db_engine: AsyncEngine, events: str, refs: str
) -> None:
    # Arrange -- plain retention wants June and July gone; June is referenced
    await _seed(db_engine, events, refs)
    config = monthly_config(events, create_ahead=1, retention=1)

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert -- June stays (23503 became an issue), July went, August was created
    assert result.success
    assert result.created_count == 1
    assert result.detached_count == 1
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.DETACH]
    assert "still referenced by rows of another table" in result.issues[0].error
    assert result.issues[0].partition_name == f"public.{events}__2026_06"
    assert await is_attached(db_engine, f"{events}__2026_06")
    assert not await is_attached(db_engine, f"{events}__2026_07")
    assert set(await range_children_of(db_engine, events)) == {f"{events}__2026_06", f"{events}__2026_08"}
    assert await scalar(db_engine, f'SELECT count(*) FROM "{refs}"') == 1  # noqa: S608
