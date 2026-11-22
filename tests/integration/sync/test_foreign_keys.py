"""Foreign keys and the lifecycle, against a real PostgreSQL (sync).

PostgreSQL refuses to detach a partition whose rows another table still
references through a foreign key on the parent (``23503``, verified identical
on 15 and 17). ``Unreferenced()`` keeps such partitions out of the plan; a
refused detach that does get planned is an issue, not the end of the run.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from pg_partsmith.boundaries import TimeBoundaries
from pg_partsmith.entities import MaintenanceIssueStep, PartitionGranularity, TablePartitionConfig
from pg_partsmith.lifecycle import (
    AllOf,
    CreateAhead,
    DropAfter,
    ExpireAction,
    ExpireIf,
    KeepNewest,
    LifecyclePolicy,
    Unreferenced,
)
from pg_partsmith.plan import FindingReason
from pg_partsmith.scheme import HashPartitioning, RangePartitioning
from pg_partsmith.topology import FactKind
from pg_partsmith.utils import parse_orphan_comment
from tests.integration.nested_support import MONTHLY_TABLE_DDL, TIMESTAMP_TABLE_DDL, monthly_config
from tests.integration.sync.support import (
    count_ddl,
    exec_sql,
    is_attached,
    make_service,
    make_table,
    range_children_of,
    run_maintenance,
    scalar,
    table_comment,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration

NOW = "2026-08-26"


@pytest.fixture
def events(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="fk")


@pytest.fixture
def hashed(sync_db_engine: Engine) -> Generator[str, None]:
    """A monthly RANGE root whose windows are HASH branches by tenant."""
    yield from make_table(sync_db_engine, TIMESTAMP_TABLE_DDL, prefix="fkh")


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


def _june_and_july(engine: Engine, events: str) -> None:
    """June and July partitions with one row each."""
    config = monthly_config(events, create_ahead=1)
    run_maintenance(engine, config, at_time="2026-06-15")
    run_maintenance(engine, config, at_time="2026-07-15")
    exec_sql(engine, f"INSERT INTO \"{events}\" (id, created_at, payload) VALUES (1, '2026-06-10', 'june')")  # noqa: S608
    exec_sql(engine, f"INSERT INTO \"{events}\" (id, created_at, payload) VALUES (2, '2026-07-10', 'july')")  # noqa: S608


def _seed(engine: Engine, events: str, refs: str) -> None:
    """June and July partitions with one row each; June's row is referenced."""
    _june_and_july(engine, events)
    exec_sql(engine, f"INSERT INTO \"{refs}\" VALUES (1, '2026-06-10')")  # noqa: S608


def _emptying_in_place(table: str) -> TablePartitionConfig:
    return monthly_config(
        table,
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(1), expire=ExpireAction.TRUNCATE),
    )


def test__unreferenced__referenced_partition_is_kept_and_the_other_expires(
    sync_db_engine: Engine, events: str, refs: str
) -> None:
    # Arrange -- keep the newest month only; June is still referenced, July is not
    _seed(sync_db_engine, events, refs)
    config = _keep_unreferenced(events)

    # Act
    plan = make_service(sync_db_engine).plan(config, now=datetime.fromisoformat(NOW).replace(tzinfo=UTC))
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


def test__expire_truncate__parent_referenced_by_a_foreign_key__a_warning_and_the_rows_stay(
    sync_db_engine: Engine, events: str, refs: str
) -> None:
    # Arrange -- June is referenced, July is not; PostgreSQL refuses TRUNCATE of either,
    # because the key on the parent is cloned onto every partition
    _seed(sync_db_engine, events, refs)
    config = _emptying_in_place(events)

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert -- a finding per partition with rows, no failed statement, nothing emptied
    assert result.success
    assert result.truncated_count == 0
    plan = result.maintenance_plan
    assert plan is not None
    assert plan.truncates == ()
    blocked = [f for f in plan.findings if f.reason is FindingReason.TRUNCATE_BLOCKED]
    assert [f.partition_name for f in blocked] == [f"public.{events}__2026_06", f"public.{events}__2026_07"]
    assert f"references it through a foreign key (public.{refs})" in blocked[0].detail
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.RECONCILE] * 2
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{events}"') == 2  # noqa: S608
    assert is_attached(sync_db_engine, f"{events}__2026_06")


def test__expire_truncate__key_pointing_at_one_partition__blocks_that_partition_only(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- a key on June itself, none on the parent
    _june_and_july(sync_db_engine, events)
    june_refs = f"{events}_june_refs"
    exec_sql(
        sync_db_engine,
        f'CREATE TABLE "{june_refs}" (event_id BIGINT, created_at TIMESTAMPTZ, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}__2026_06" (id, created_at))',
    )
    try:
        # Act
        result = run_maintenance(sync_db_engine, _emptying_in_place(events), at_time=NOW)
    finally:
        exec_sql(sync_db_engine, f'DROP TABLE "{june_refs}"')

    # Assert -- July is emptied, June keeps its row and says why
    plan = result.maintenance_plan
    assert plan is not None
    assert [op.target for op in plan.truncates] == [f"public.{events}__2026_07"]
    blocked = [f for f in plan.findings if f.reason is FindingReason.TRUNCATE_BLOCKED]
    assert [f.partition_name for f in blocked] == [f"public.{events}__2026_06"]
    assert f"(public.{june_refs})" in blocked[0].detail
    assert result.truncated_count == 1
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{events}"') == 1  # noqa: S608


def test__expire_truncate__self_referencing_key_on_the_parent__blocks_every_partition(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- the referencing side is the parent itself, which the truncate of a partition does not empty
    _june_and_july(sync_db_engine, events)
    exec_sql(
        sync_db_engine,
        f'ALTER TABLE "{events}" ADD COLUMN parent_id BIGINT, ADD COLUMN parent_at TIMESTAMPTZ, '
        f'ADD FOREIGN KEY (parent_id, parent_at) REFERENCES "{events}" (id, created_at)',
    )

    # Act
    result = run_maintenance(sync_db_engine, _emptying_in_place(events), at_time=NOW)

    # Assert
    assert result.success
    plan = result.maintenance_plan
    assert plan is not None
    assert plan.truncates == ()
    blocked = [f for f in plan.findings if f.reason is FindingReason.TRUNCATE_BLOCKED]
    assert [f.partition_name for f in blocked] == [f"public.{events}__2026_06", f"public.{events}__2026_07"]
    assert f"(public.{events})" in blocked[0].detail
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{events}"') == 2  # noqa: S608


def _emptying_hashed(table: str) -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name=table,
        scheme=RangePartitioning(
            key="created_at",
            boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH),
            child=HashPartitioning(key="tenant_id", modulus=2),
        ),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(1), expire=ExpireAction.TRUNCATE),
    )


def _fill_hashed_june(engine: Engine, table: str) -> None:
    """June as a branch of two buckets, with rows in both."""
    run_maintenance(engine, _emptying_hashed(table), at_time="2026-06-15")
    exec_sql(
        engine,
        f"INSERT INTO \"{table}\" (tenant_id, created_at) SELECT g, '2026-06-10' FROM generate_series(1, 20) g",  # noqa: S608
    )


def test__expire_truncate__key_pointing_at_one_bucket__blocks_the_whole_branch(
    sync_db_engine: Engine, hashed: str
) -> None:
    # Arrange -- the key reaches one leaf below the expired June branch, and TRUNCATE of the branch reaches it too
    _fill_hashed_june(sync_db_engine, hashed)
    leaf_refs = f"{hashed}_leaf_refs"
    exec_sql(
        sync_db_engine,
        f'CREATE TABLE "{leaf_refs}" (event_id BIGINT, tenant_id BIGINT, created_at TIMESTAMPTZ, '
        f"FOREIGN KEY (event_id, tenant_id, created_at) "
        f'REFERENCES "{hashed}__2026_06__h0" (id, tenant_id, created_at))',
    )
    try:
        # Act
        result = run_maintenance(sync_db_engine, _emptying_hashed(hashed), at_time=NOW)
    finally:
        exec_sql(sync_db_engine, f'DROP TABLE "{leaf_refs}"')

    # Assert
    assert result.success
    plan = result.maintenance_plan
    assert plan is not None
    assert plan.truncates == ()
    blocked = [f for f in plan.findings if f.reason is FindingReason.TRUNCATE_BLOCKED]
    assert [f.partition_name for f in blocked] == [f"public.{hashed}__2026_06"]
    assert f"(public.{leaf_refs})" in blocked[0].detail
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{hashed}"') == 20  # noqa: S608


def test__expire_truncate__expired_branch__emptied_whole_then_left_alone(sync_db_engine: Engine, hashed: str) -> None:
    # Arrange
    _fill_hashed_june(sync_db_engine, hashed)
    config = _emptying_hashed(hashed)
    june = f"{hashed}__2026_06"
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{june}__h0"') > 0  # noqa: S608
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{june}__h1"') > 0  # noqa: S608

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)
    with count_ddl(sync_db_engine) as counter:
        again = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert -- one statement for the branch, every bucket empty and still in place; then nothing to do
    assert result.truncated_count == 1
    assert is_attached(sync_db_engine, june)
    assert is_attached(sync_db_engine, f"{june}__h0")
    assert is_attached(sync_db_engine, f"{june}__h1")
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{june}"') == 0  # noqa: S608
    assert counter.statements == []
    assert again.maintenance_plan is not None
    assert [(f.reason, f.partition_name) for f in again.maintenance_plan.findings] == [
        (FindingReason.EXPIRED_EMPTY, f"public.{hashed}")
    ]


def test__expire_truncate__a_partition_referencing_itself__is_still_emptied(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- a key from June to June: the truncate empties both sides of it, so PostgreSQL allows it
    _june_and_july(sync_db_engine, events)
    june = f"{events}__2026_06"
    exec_sql(sync_db_engine, f'ALTER TABLE "{events}" ADD COLUMN parent_id BIGINT, ADD COLUMN parent_at TIMESTAMPTZ')
    exec_sql(
        sync_db_engine,
        f'ALTER TABLE "{june}" ADD FOREIGN KEY (parent_id, parent_at) REFERENCES "{june}" (id, created_at)',
    )

    # Act
    result = run_maintenance(sync_db_engine, _emptying_in_place(events), at_time=NOW)

    # Assert
    assert result.issues == ()
    assert result.maintenance_plan is not None
    assert [op.target for op in result.maintenance_plan.truncates] == [f"public.{june}", f"public.{events}__2026_07"]
    assert result.truncated_count == 2
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{events}"') == 0  # noqa: S608


def test__expire_truncate__key_added_after_the_plan__an_issue_and_the_run_goes_on(
    sync_db_engine: Engine, events: str
) -> None:
    # Arrange -- planned with nothing referencing the table, then a key on the parent appears
    _june_and_july(sync_db_engine, events)
    config = _emptying_in_place(events)
    service = make_service(sync_db_engine)
    plan = service.plan(config, now=datetime.fromisoformat(NOW).replace(tzinfo=UTC))
    assert len(plan.truncates) == 2
    late_refs = f"{events}_late_refs"
    exec_sql(
        sync_db_engine,
        f'CREATE TABLE "{late_refs}" (event_id BIGINT, created_at TIMESTAMPTZ, '
        f'FOREIGN KEY (event_id, created_at) REFERENCES "{events}" (id, created_at))',
    )
    try:
        # Act
        result = service.apply(config, plan)
    finally:
        exec_sql(sync_db_engine, f'DROP TABLE "{late_refs}"')

    # Assert -- found under the truncate's lock, recorded, and nothing emptied
    assert result.truncated_count == 0
    assert [(issue.step, issue.partition_name) for issue in result.issues] == [
        (MaintenanceIssueStep.TRUNCATE, f"public.{events}__2026_06"),
        (MaintenanceIssueStep.TRUNCATE, f"public.{events}__2026_07"),
    ]
    assert f"(public.{late_refs})" in result.issues[0].error
    assert scalar(sync_db_engine, f'SELECT count(*) FROM "{events}"') == 2  # noqa: S608


def test__refused_detach__leaves_no_marker_and_the_later_detach_stamps_its_own_instant(
    sync_db_engine: Engine, events: str, refs: str
) -> None:
    """A refused detach records no detach, so the drop grace counts from the detach that happens."""
    # Arrange -- June is referenced and carries a comment of its own; detached tables wait a month
    _seed(sync_db_engine, events, refs)
    june = f"{events}__2026_06"
    exec_sql(sync_db_engine, f"COMMENT ON TABLE \"{june}\" IS 'june archive'")
    config = monthly_config(
        events,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1), retention=KeepNewest(count=1), drop=DropAfter(grace=timedelta(days=30))
        ),
    )
    refused = run_maintenance(sync_db_engine, config, at_time="2026-08-26")
    comment_after_refusal = table_comment(sync_db_engine, june)

    # Act -- the reference goes, and a run two days later detaches June
    exec_sql(sync_db_engine, f'DELETE FROM "{refs}"')  # noqa: S608
    detached = run_maintenance(sync_db_engine, config, at_time="2026-08-28")

    # Assert
    assert [issue.step for issue in refused.issues] == [MaintenanceIssueStep.DETACH]
    assert comment_after_refusal == "june archive"
    assert detached.detached_count == 1
    assert not is_attached(sync_db_engine, june)
    comment = table_comment(sync_db_engine, june)
    marker = parse_orphan_comment(comment)
    assert marker is not None
    assert marker[1] == datetime(2026, 8, 28, tzinfo=UTC)
    assert (comment or "").endswith("june archive")
