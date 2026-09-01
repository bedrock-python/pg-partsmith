"""Foreign keys and the lifecycle, against a real PostgreSQL (sync).

PostgreSQL refuses to detach a partition whose rows another table still
references through a foreign key on the parent (``23503``, verified identical
on 15 and 17). ``Unreferenced()`` keeps such partitions out of the plan; a
refused detach that does get planned is an issue, not the end of the run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pg_partsmith.entities import MaintenanceIssueStep
from pg_partsmith.lifecycle import AllOf, CreateAhead, ExpireIf, KeepNewest, LifecyclePolicy, Unreferenced
from pg_partsmith.topology import FactKind
from tests.integration.nested_support import MONTHLY_TABLE_DDL, monthly_config
from tests.integration.sync.support import (
    exec_sql,
    is_attached,
    make_service,
    make_table,
    range_children_of,
    run_maintenance,
    scalar,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

    from pg_partsmith.entities import TablePartitionConfig

pytestmark = pytest.mark.integration

NOW = "2026-08-26"


@pytest.fixture
def events(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="fk")


@pytest.fixture
def refs(sync_db_engine: Engine, events: str) -> Generator[str, None]:
    """A table whose foreign key points at the partitioned parent."""
    name = f"{events}_refs"
    exec_sql(
        sync_db_engine,
        f'CREATE TABLE "{name}" (event_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at))',
    )
    yield name
    exec_sql(sync_db_engine, f'DROP TABLE IF EXISTS "{name}"')


def _keep_unreferenced(table: str, *, keep: int = 1) -> TablePartitionConfig:
    return monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1),
            retention=ExpireIf(when=AllOf(members=(KeepNewest(count=keep), Unreferenced()))),
        ),
    )


def _seed(engine: Engine, events: str, refs: str) -> None:
    """June and July partitions with one row each; June's row is referenced."""
    config = monthly_config(events, create_ahead=1)
    run_maintenance(engine, config, at_time="2026-06-15")
    run_maintenance(engine, config, at_time="2026-07-15")
    exec_sql(engine, f"INSERT INTO \"{events}\" (id, created_at, payload) VALUES (1, '2026-06-10', 'june')")  # noqa: S608
    exec_sql(engine, f"INSERT INTO \"{events}\" (id, created_at, payload) VALUES (2, '2026-07-10', 'july')")  # noqa: S608
    exec_sql(engine, f"INSERT INTO \"{refs}\" VALUES (1, '2026-06-10')")  # noqa: S608


def test__unreferenced__referenced_partition_is_kept_and_the_other_expires(
    sync_db_engine: Engine, events: str, refs: str
) -> None:
    # Arrange -- keep the newest month only; June is still referenced, July is not
    _seed(sync_db_engine, events, refs)
    config = _keep_unreferenced(events)

    # Act
    plan = make_service(sync_db_engine).plan(config, now=None)
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert [op.target for op in plan.detaches] == [f"public.{events}__2026_07"]
    assert result.detached_count == 1
    assert result.issues == ()
    assert is_attached(sync_db_engine, f"{events}__2026_06")
    assert not is_attached(sync_db_engine, f"{events}__2026_07")


def test__unreferenced__referencing_rows_gone__the_partition_expires_on_the_next_run(
    sync_db_engine: Engine, events: str, refs: str
) -> None:
    # Arrange
    _seed(sync_db_engine, events, refs)
    config = _keep_unreferenced(events)
    run_maintenance(sync_db_engine, config, at_time=NOW)

    # Act
    exec_sql(sync_db_engine, f'DELETE FROM "{refs}"')  # noqa: S608
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert result.detached_count == 1
    assert not is_attached(sync_db_engine, f"{events}__2026_06")


def test__unreferenced__measured_only_when_a_rule_asks(sync_db_engine: Engine, events: str, refs: str) -> None:
    # Arrange
    _seed(sync_db_engine, events, refs)
    asking = _keep_unreferenced(events)
    silent = monthly_config(events, create_ahead=1, retention=1)

    # Act
    measured = make_service(sync_db_engine).inspect(asking)
    assert measured is not None
    tree_asking = make_service(sync_db_engine)._inspector.inspect(asking, measure=True)
    tree_silent = make_service(sync_db_engine)._inspector.inspect(silent, measure=True)

    # Assert
    assert asking.lifecycle.required_facts == {FactKind.REFERENCES}
    assert tree_asking is not None and tree_silent is not None
    june = tree_asking.find(f"public.{events}__2026_06")
    july = tree_asking.find(f"public.{events}__2026_07")
    assert june is not None and june.facts is not None and june.facts.referenced is True
    assert july is not None and july.facts is not None and july.facts.referenced is False
    assert all(child.facts is None for child in tree_silent.root.children)


def test__without_unreferenced__refused_detach_is_an_issue_and_the_run_goes_on(
    sync_db_engine: Engine, events: str, refs: str
) -> None:
    # Arrange -- plain retention wants June and July gone; June is referenced
    _seed(sync_db_engine, events, refs)
    config = monthly_config(events, create_ahead=1, retention=1)

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert -- June stays (23503 became an issue), July went, August was created
    assert result.success
    assert result.created_count == 1
    assert result.detached_count == 1
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.DETACH]
    assert "still referenced by rows of another table" in result.issues[0].error
    assert result.issues[0].partition_name == f"public.{events}__2026_06"
    assert is_attached(sync_db_engine, f"{events}__2026_06")
    assert not is_attached(sync_db_engine, f"{events}__2026_07")
    assert set(range_children_of(sync_db_engine, events)) == {f"{events}__2026_06", f"{events}__2026_08"}
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{refs}"') == 1  # noqa: S608
