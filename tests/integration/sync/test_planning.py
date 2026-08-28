"""Plans, dry runs, ownership and revalidation against a real PostgreSQL (sync)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from uuid import uuid4

import freezegun
import pytest
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.entities import MaintenanceIssueStep
from pg_partsmith.exceptions import PlanStaleError
from pg_partsmith.lifecycle import CreateAhead, DetachMode, KeepNewest, LifecyclePolicy
from pg_partsmith.plan import FindingReason, MaintenancePlan, OperationKind, Reason, Severity
from pg_partsmith.planner import PlanMode
from pg_partsmith.topology import RangeBounds, RelationKind
from tests.integration.sync.support import (
    count_ddl,
    exec_sql,
    is_attached,
    make_service,
    make_table,
    range_children_of,
    relation_oid,
    relkind,
    run_maintenance,
)
from tests.integration.nested_support import METRICS_TABLE_DDL, MONTHLY_TABLE_DDL, monthly_config, orphan_marker

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration

NOW = "2026-08-26"


@pytest.fixture
def table(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="plan")


@pytest.fixture
def metrics_table(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, METRICS_TABLE_DDL, prefix="metrics")


# ── plan() / apply() ────────────────────────────────────────────────────────────


def test__plan__fresh_table__lists_the_creations_and_issues_zero_ddl(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = monthly_config(table, create_ahead=3, retention=12)
    service = make_service(sync_db_engine)

    # Act
    with freezegun.freeze_time(NOW), count_ddl(sync_db_engine) as counter:
        plan = service.plan(config)

    # Assert: the whole answer, and not a single statement to get it
    assert counter.statements == []
    assert plan.table_name == config.qualified_name == table
    assert [(op.kind, op.target, op.reason) for op in plan.operations] == [
        (OperationKind.CREATE, f"public.{table}__2026_08", Reason.CREATE_AHEAD),
        (OperationKind.CREATE, f"public.{table}__2026_09", Reason.CREATE_AHEAD),
        (OperationKind.CREATE, f"public.{table}__2026_10", Reason.CREATE_AHEAD),
    ]
    windows = [op.bounds for op in plan.creates if isinstance(op.bounds, RangeBounds)]
    assert [(w.from_value, w.to_value) for w in windows] == [
        ("2026-08-01", "2026-09-01"),
        ("2026-09-01", "2026-10-01"),
        ("2026-10-01", "2026-11-01"),
    ]
    assert plan.findings == ()
    assert plan.describe().startswith(f"plan for {table} at 2026-08-26T00:00:00+00:00")
    assert relkind(sync_db_engine, f"{table}__2026_08") is None


def test__maintain__after_plan__executes_exactly_that_plan_and_converges(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = monthly_config(table, create_ahead=3, retention=12)
    service = make_service(sync_db_engine)
    with freezegun.freeze_time(NOW):
        plan = service.plan(config)

    # Act
    with freezegun.freeze_time(NOW):
        result = service.maintain(config)
    with freezegun.freeze_time(NOW), count_ddl(sync_db_engine) as counter:
        again = service.maintain(config)

    # Assert: the executed plan is the one shown beforehand; the next tick is free
    assert result.maintenance_plan == plan
    assert result.created_count == len(plan.creates) == 3
    assert set(range_children_of(sync_db_engine, table)) == {
        f"{table}__2026_08",
        f"{table}__2026_09",
        f"{table}__2026_10",
    }
    assert again.created_count == 0
    assert again.maintenance_plan is not None
    assert again.maintenance_plan.is_noop
    assert counter.statements == []


def test__apply__of_a_plan_made_earlier__executes_it_under_the_lock(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = monthly_config(table, create_ahead=2, retention=12)
    service = make_service(sync_db_engine)
    with freezegun.freeze_time(NOW):
        plan = service.plan(config)

    # Act
    result = service.apply(config, plan.only(OperationKind.CREATE))

    # Assert
    assert result.created_count == 2
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.operations == plan.operations
    assert is_attached(sync_db_engine, f"{table}__2026_08")
    assert is_attached(sync_db_engine, f"{table}__2026_09")


def test__plan__json_dump__round_trips_through_model_validate(sync_db_engine: Engine, table: str) -> None:
    # Arrange: a plan with a creation, a detach, a same-run drop and a finding
    config = monthly_config(table, create_ahead=1, retention=1)
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")
    exec_sql(sync_db_engine, f'CREATE TABLE "{table}_archive" (LIKE "{table}" INCLUDING ALL)')
    exec_sql(
        sync_db_engine,
        f'ALTER TABLE "{table}" ATTACH PARTITION "{table}_archive" '
        "FOR VALUES FROM ('2000-01-01') TO ('2020-01-01')",
    )
    with freezegun.freeze_time(NOW):
        plan = make_service(sync_db_engine).plan(config)
    assert {op.kind for op in plan.operations} == {OperationKind.CREATE, OperationKind.DETACH, OperationKind.DROP}
    assert plan.findings != ()

    # Act
    wire = json.dumps(plan.model_dump(mode="json"))
    restored = MaintenancePlan.model_validate(json.loads(wire))

    # Assert
    assert restored == plan
    assert restored.model_dump(mode="json") == plan.model_dump(mode="json")
    assert [op.oid for op in restored.detaches] == [op.oid for op in plan.detaches]


def test__plan__modes__reconcile_and_explicit_create_nothing_ahead(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = monthly_config(table, create_ahead=2, retention=1)
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")  # June and July exist
    service = make_service(sync_db_engine)

    # Act
    with freezegun.freeze_time(NOW):
        maintain = service.plan(config)
        reconcile = service.plan(config, mode=PlanMode.RECONCILE)

    # Assert: the tick would create ahead and expire; a reconcile touches neither
    assert [op.target for op in maintain.creates] == [f"public.{table}__2026_08", f"public.{table}__2026_09"]
    assert [op.target for op in maintain.detaches] == [f"public.{table}__2026_06", f"public.{table}__2026_07"]
    assert reconcile.is_noop


# ── Ownership by alignment ──────────────────────────────────────────────────────


def test__ownership__hand_attached_off_grid_partition__reported_as_unmanaged_and_never_touched(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: a DBA's archive spanning twenty years, on a monthly grid
    archive = f"{table}_archive"
    exec_sql(sync_db_engine, f'CREATE TABLE "{archive}" (LIKE "{table}" INCLUDING ALL)')
    exec_sql(
        sync_db_engine,
        f"ALTER TABLE \"{table}\" ATTACH PARTITION \"{archive}\" FOR VALUES FROM ('2000-01-01') TO ('2020-01-01')",
    )
    config = monthly_config(table, create_ahead=1, retention=1)

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert: an informational finding, not an issue, and no operation about it
    assert result.success
    assert result.issues == ()
    plan = result.maintenance_plan
    assert plan is not None
    findings = [f for f in plan.findings if f.partition_name == f"public.{archive}"]
    assert [(f.reason, f.severity) for f in findings] == [(FindingReason.UNMANAGED_PARTITION, Severity.INFO)]
    assert all(op.target != f"public.{archive}" for op in plan.operations)
    assert result.detached_count == 0
    assert result.dropped_count == 0

    # Act: many runs later it still has not been touched, while retention keeps rolling
    for at_time in ("2026-09-15", "2026-10-15", "2027-01-15", "2027-06-15"):
        later = run_maintenance(sync_db_engine, config, at_time=at_time)
        assert later.issues == ()
        assert later.maintenance_plan is not None
        assert all(op.target != f"public.{archive}" for op in later.maintenance_plan.operations)

    # Assert
    assert is_attached(sync_db_engine, archive) is True
    assert set(range_children_of(sync_db_engine, table)) == {archive, f"{table}__2027_06"}


def test__ownership__hand_attached_partition_finer_than_the_grid__managed_and_expired_by_its_bound(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: a single day inside the monthly grid, left by an earlier daily config
    day = f"{table}_june_10"
    exec_sql(sync_db_engine, f'CREATE TABLE "{day}" (LIKE "{table}" INCLUDING ALL)')
    exec_sql(
        sync_db_engine,
        f"ALTER TABLE \"{table}\" ATTACH PARTITION \"{day}\" FOR VALUES FROM ('2026-06-10') TO ('2026-06-11')",
    )
    config = monthly_config(table, create_ahead=1, retention=1)

    # Act
    with freezegun.freeze_time(NOW):
        plan = make_service(sync_db_engine).plan(config)
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert: it lies inside a window of the grid, so it is ours and it has aged out
    assert [f for f in plan.findings if f.partition_name == f"public.{day}"] == []
    assert [(op.target, op.reason) for op in plan.detaches] == [(f"public.{day}", Reason.RETENTION_EXPIRED)]
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert relkind(sync_db_engine, day) is None


def test__ownership__wanted_window_overlapping_an_off_grid_partition__range_overlap_issue_and_no_ddl(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: a hand-attached window straddling two months, covering "now"
    wide = f"{table}_wide"
    exec_sql(sync_db_engine, f'CREATE TABLE "{wide}" (LIKE "{table}" INCLUDING ALL)')
    exec_sql(
        sync_db_engine,
        f"ALTER TABLE \"{table}\" ATTACH PARTITION \"{wide}\" FOR VALUES FROM ('2026-08-15') TO ('2026-09-15')",
    )
    config = monthly_config(table, create_ahead=2, retention=12)

    # Act
    with count_ddl(sync_db_engine) as counter:
        result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert: both wanted windows are blocked; nothing is created, nothing is detached
    assert result.success
    assert counter.statements == []
    assert result.created_count == 0
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.RECONCILE] * 2
    assert all(wide in issue.error for issue in result.issues)
    plan = result.maintenance_plan
    assert plan is not None
    assert plan.is_noop
    overlaps = [f for f in plan.findings if f.reason is FindingReason.RANGE_OVERLAP]
    assert len(overlaps) == 2
    assert all(f.is_actionable for f in overlaps)
    unmanaged = [f for f in plan.findings if f.reason is FindingReason.UNMANAGED_PARTITION]
    assert [f.partition_name for f in unmanaged] == [f"public.{wide}"]
    assert is_attached(sync_db_engine, wide) is True


# ── Destructive revalidation ────────────────────────────────────────────────────


def _replace_orphan(engine: Engine, table: str, orphan: str) -> None:
    """Drop an orphan and recreate a same-named, same-marked table in its place."""
    exec_sql(engine, f'DROP TABLE "{orphan}"')
    exec_sql(engine, f'CREATE TABLE "{orphan}" (LIKE "{table}" INCLUDING ALL)')
    exec_sql(engine, f"COMMENT ON TABLE \"{orphan}\" IS '{orphan_marker(f'public.{table}')}'")


def test__apply__drop_of_a_relation_recreated_since_the_plan__refused_and_recorded_with_continue_on_error(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: June expired and was detached; a plan decides to drop it by OID
    config = monthly_config(table, create_ahead=1, retention=1)
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")
    run_maintenance(sync_db_engine, config, at_time="2026-08-01", skip_drop=True)
    june = f"{table}__2026_06"
    service = make_service(sync_db_engine)
    with freezegun.freeze_time("2026-08-02"):
        plan = service.plan(config)
    assert [op.target for op in plan.drops] == [f"public.{june}"]
    planned_oid = plan.drops[0].oid
    assert planned_oid == relation_oid(sync_db_engine, june)

    # A same-named, same-marked table takes its place before the plan is applied
    _replace_orphan(sync_db_engine, table, june)
    replacement_oid = relation_oid(sync_db_engine, june)
    assert replacement_oid is not None
    assert replacement_oid != planned_oid

    # Act
    result = service.apply(config, plan, continue_on_error=True)

    # Assert: the replacement is left alone and the refusal is on record
    assert result.dropped_count == 0
    assert [(issue.step, issue.partition_name) for issue in result.issues] == [
        (MaintenanceIssueStep.DROP, f"public.{june}")
    ]
    assert "PlanStaleError" in result.issues[0].error
    assert relation_oid(sync_db_engine, june) == replacement_oid


def test__apply__drop_of_a_relation_recreated_since_the_plan__raises_without_continue_on_error(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    config = monthly_config(table, create_ahead=1, retention=1)
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")
    run_maintenance(sync_db_engine, config, at_time="2026-08-01", skip_drop=True)
    june = f"{table}__2026_06"
    service = make_service(sync_db_engine)
    with freezegun.freeze_time("2026-08-02"):
        plan = service.plan(config)
    _replace_orphan(sync_db_engine, table, june)
    replacement_oid = relation_oid(sync_db_engine, june)

    # Act / Assert
    with pytest.raises(PlanStaleError, match=june):
        service.apply(config, plan)
    assert relation_oid(sync_db_engine, june) == replacement_oid


def test__apply__detach_of_a_partition_detached_since_the_plan__refused_as_stale(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: the plan wants June detached; someone detaches it first
    config = monthly_config(table, create_ahead=1, retention=1)
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")
    june = f"{table}__2026_06"
    service = make_service(sync_db_engine)
    with freezegun.freeze_time(NOW):
        plan = service.plan(config)
    assert [op.target for op in plan.detaches] == [f"public.{june}"]
    exec_sql(sync_db_engine, f'ALTER TABLE "{table}" DETACH PARTITION "{june}"')

    # Act
    with freezegun.freeze_time(NOW):
        result = service.apply(config, plan.without(OperationKind.CREATE), continue_on_error=True)

    # Assert: neither the detach nor the drop that followed it ran
    assert result.detached_count == 0
    assert result.dropped_count == 0
    assert [(issue.step, issue.partition_name) for issue in result.issues] == [
        (MaintenanceIssueStep.DETACH, f"public.{june}")
    ]
    assert "PlanStaleError" in result.issues[0].error
    assert relkind(sync_db_engine, june) == "r"


# ── Foreign leaves ──────────────────────────────────────────────────────────────


def _foreign_leaf(
    sync_db_engine: Engine,
    metrics_table: str,
    postgres_container: PostgresContainer,
    *,
    suffix: str,
    bounds: tuple[str, str],
    row_ts: str,
) -> Generator[str, None]:
    """A postgres_fdw loopback foreign table attached as one monthly partition."""
    server = f"loop_{uuid4().hex[:8]}"
    remote = f"{metrics_table}_remote{suffix}"
    leaf = f"{metrics_table}{suffix}"
    with sync_db_engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgres_fdw"))
        conn.execute(
            text(
                f"CREATE SERVER {server} FOREIGN DATA WRAPPER postgres_fdw "
                f"OPTIONS (host 'localhost', dbname '{postgres_container.dbname}', port '5432')"
            )
        )
        conn.execute(
            text(
                f"CREATE USER MAPPING FOR CURRENT_USER SERVER {server} "
                f"OPTIONS (user '{postgres_container.username}', password '{postgres_container.password}')"
            )
        )
        conn.execute(text(f'CREATE TABLE "{remote}" (ts TIMESTAMPTZ NOT NULL, v DOUBLE PRECISION)'))
        conn.execute(text(f"INSERT INTO \"{remote}\" VALUES ('{row_ts}', 1.5)"))  # noqa: S608
        conn.execute(
            text(
                f'CREATE FOREIGN TABLE "{leaf}" PARTITION OF "{metrics_table}" '
                f"FOR VALUES FROM ('{bounds[0]}') TO ('{bounds[1]}') SERVER {server} OPTIONS (table_name '{remote}')"
            )
        )
    yield leaf
    with sync_db_engine.begin() as conn:
        conn.execute(text(f"DROP SERVER IF EXISTS {server} CASCADE"))
        conn.execute(text(f'DROP TABLE IF EXISTS "{remote}"'))


@pytest.fixture
def foreign_leaf(
    sync_db_engine: Engine, metrics_table: str, postgres_container: PostgresContainer
) -> Generator[str, None]:
    """The January 2025 partition, long behind every retention rule in these tests."""
    yield from _foreign_leaf(
        sync_db_engine,
        metrics_table,
        postgres_container,
        suffix="__2025_01",
        bounds=("2025-01-01", "2025-02-01"),
        row_ts="2025-01-15",
    )


@pytest.fixture
def foreign_leaf_now(
    sync_db_engine: Engine, metrics_table: str, postgres_container: PostgresContainer
) -> Generator[str, None]:
    """The August 2026 partition: the very window the cursor sits in."""
    yield from _foreign_leaf(
        sync_db_engine,
        metrics_table,
        postgres_container,
        suffix="__2026_08",
        bounds=("2026-08-01", "2026-09-01"),
        row_ts="2026-08-15",
    )


def test__foreign_leaf__covering_a_wanted_window__satisfies_it_without_an_overlap_or_a_create(
    sync_db_engine: Engine, metrics_table: str, foreign_leaf_now: str
) -> None:
    # Arrange
    config = monthly_config(metrics_table, create_ahead=2, retention=12, column="ts")

    # Act
    with count_ddl(sync_db_engine) as counter:
        result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert: the window exists, held by a relation the lifecycle may not touch;
    # only September is created, and nothing is reported as a conflict
    assert result.issues == ()
    assert result.created_count == 1
    assert [op.target for op in (result.maintenance_plan.creates if result.maintenance_plan else ())] == [
        f"public.{metrics_table}__2026_09"
    ]
    assert not any("ATTACH" in s and foreign_leaf_now.upper() in s for s in counter.statements)
    plan = result.maintenance_plan
    assert plan is not None
    assert {f.reason for f in plan.findings} == {FindingReason.FOREIGN_PARTITION}
    assert is_attached(sync_db_engine, foreign_leaf_now) is True


def test__foreign_leaf__get_actual_tree__reports_the_foreign_relkind(
    sync_db_engine: Engine, metrics_table: str, foreign_leaf: str
) -> None:
    # Arrange
    metadata = PostgresMetadataProvider(sync_db_engine)

    # Act
    tree = metadata.get_actual_tree(metrics_table)

    # Assert
    assert tree is not None
    assert [c.name for c in tree.root.children] == [f"public.{foreign_leaf}"]
    leaf = tree.root.children[0]
    assert leaf.relkind is RelationKind.FOREIGN
    assert leaf.is_foreign
    assert leaf.is_leaf
    assert leaf.bounds is not None
    assert leaf.bounds.kind == "range"


def test__foreign_leaf__plan__lists_a_foreign_partition_finding_and_no_operation_about_it(
    sync_db_engine: Engine, metrics_table: str, foreign_leaf: str
) -> None:
    # Arrange: retention that would have expired a local partition of that window
    config = monthly_config(metrics_table, create_ahead=1, retention=1, column="ts")

    # Act
    with freezegun.freeze_time(NOW):
        plan = make_service(sync_db_engine).plan(config)

    # Assert
    findings = [f for f in plan.findings if f.partition_name == f"public.{foreign_leaf}"]
    assert [(f.reason, f.severity) for f in findings] == [(FindingReason.FOREIGN_PARTITION, Severity.INFO)]
    assert all(op.target != f"public.{foreign_leaf}" for op in plan.operations)
    assert [op.target for op in plan.creates] == [f"public.{metrics_table}__2026_08"]


def test__foreign_leaf__maintain__never_detaches_or_drops_it(
    sync_db_engine: Engine, metrics_table: str, foreign_leaf: str
) -> None:
    # Arrange
    config = monthly_config(metrics_table, create_ahead=1, retention=1, column="ts")

    # Act
    first = run_maintenance(sync_db_engine, config, at_time=NOW)
    second = run_maintenance(sync_db_engine, config, at_time="2026-10-15")

    # Assert: local windows come and go around it; the foreign one stays attached and readable
    assert first.created_count == 1
    assert first.issues == ()
    assert second.detached_count == 1
    assert second.dropped_count == 1
    assert relkind(sync_db_engine, foreign_leaf) == "f"
    assert is_attached(sync_db_engine, foreign_leaf) is True
    with sync_db_engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT tableoid::regclass::text, v FROM \"{metrics_table}\" WHERE ts < '2026-01-01'")  # noqa: S608
        )
        assert [(str(r[0]), float(r[1])) for r in rows.fetchall()] == [(foreign_leaf, 1.5)]


def test__foreign_leaf__detached_by_hand_and_marked__reported_and_never_dropped(
    sync_db_engine: Engine, metrics_table: str, foreign_leaf: str
) -> None:
    # Arrange: a DBA detached it and stamped the marker (COMMENT ON TABLE is refused
    # for a foreign table, so the library itself never writes one here)
    config = monthly_config(metrics_table, create_ahead=1, retention=1, column="ts")
    exec_sql(sync_db_engine, f'ALTER TABLE "{metrics_table}" DETACH PARTITION "{foreign_leaf}"')
    exec_sql(
        sync_db_engine, f"COMMENT ON FOREIGN TABLE \"{foreign_leaf}\" IS '{orphan_marker(f'public.{metrics_table}')}'"
    )
    tree = PostgresMetadataProvider(sync_db_engine).get_actual_tree(metrics_table)
    assert tree is not None
    assert [(o.name, o.relkind) for o in tree.orphans] == [(f"public.{foreign_leaf}", RelationKind.FOREIGN)]

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert: DROP TABLE cannot remove a foreign table, so nothing is attempted
    assert result.dropped_count == 0
    assert result.issues == ()
    plan = result.maintenance_plan
    assert plan is not None
    assert plan.drops == ()
    findings = [f for f in plan.findings if f.partition_name == f"public.{foreign_leaf}"]
    assert [f.reason for f in findings] == [FindingReason.FOREIGN_PARTITION]
    assert relkind(sync_db_engine, foreign_leaf) == "f"


def test__plan__detach_mode__is_carried_on_the_operation(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1), retention=KeepNewest(count=1), detach=DetachMode.BLOCKING
        ),
    )
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")

    # Act
    with freezegun.freeze_time(NOW):
        plan = make_service(sync_db_engine).plan(config)

    # Assert
    assert [op.mode for op in plan.detaches] == [DetachMode.BLOCKING]
    assert plan.detaches[0].capabilities.transactional is True
    assert plan.detaches[0].is_destructive
