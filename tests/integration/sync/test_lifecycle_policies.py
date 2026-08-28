"""Lifecycle policies against a real PostgreSQL (sync).

Numeric windows and their cursor, detach grace and drop policies, detach
modes, fact-driven predicates, horizon and age based rules, and orphans that
come back.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING

import freezegun
import pytest
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.boundaries import CursorSource, Window
from pg_partsmith.entities import MaintenanceIssueStep, Period
from pg_partsmith.lifecycle import (
    AllOf,
    CreateAhead,
    CreateUntil,
    DetachMode,
    DropAfter,
    DropNever,
    ExpireIf,
    KeepFor,
    KeepNewest,
    LifecyclePolicy,
    Not,
    SizeAbove,
    SqlPredicate,
    WindowAgeAbove,
)
from pg_partsmith.plan import FindingReason, Reason
from pg_partsmith.sync.metadata import PostgresMetadataProvider
from pg_partsmith.utils import DETACHED_AT_MARKER
from tests.integration.nested_support import (
    MONTHLY_TABLE_DDL,
    QUEUE_TABLE_DDL,
    monthly_config,
    orphan_marker,
    queue_config,
)
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
    scalar,
    table_comment,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy import Engine

pytestmark = pytest.mark.integration

NOW = "2026-08-26"


@pytest.fixture
def table(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, MONTHLY_TABLE_DDL, prefix="policy")


@pytest.fixture
def queue(sync_db_engine: Engine) -> Generator[str, None]:
    yield from make_table(sync_db_engine, QUEUE_TABLE_DDL, prefix="queue")


def _fill_queue(engine: Engine, queue: str, rows: int) -> None:
    exec_sql(engine, f"INSERT INTO \"{queue}\" (payload) SELECT 'x' FROM generate_series(1, {rows})")  # noqa: S608


def _seed_month(engine: Engine, table: str, year: int, month: int, *, rows: int = 0) -> str:
    """Create one monthly partition through the library and optionally fill it."""
    name = f"{table}__{year:04d}_{month:02d}"
    make_service(engine).ensure_partitions(monthly_config(table), [Period(year=year, month=month)])
    if rows:
        exec_sql(
            engine,
            f'INSERT INTO "{table}" (created_at, payload) '  # noqa: S608
            f"SELECT '{year:04d}-{month:02d}-15'::timestamptz, repeat('x', 200) FROM generate_series(1, {rows})",
        )
    return name


# ── Numeric RANGE root ──────────────────────────────────────────────────────────


def test__numeric_root__empty_table__creates_ahead_from_the_origin(sync_db_engine: Engine, queue: str) -> None:
    # Arrange / Act
    result = run_maintenance(sync_db_engine, queue_config(queue, step=1000, create_ahead=2))

    # Assert
    assert result.success
    assert result.created_count == 2
    assert range_children_of(sync_db_engine, queue) == {
        f"{queue}__0": ("0", "1000"),
        f"{queue}__1000": ("1000", "2000"),
    }
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.cursors == {}


def test__numeric_root__rows_inserted__cursor_moves_to_max_key_and_the_next_window_appears(
    sync_db_engine: Engine, queue: str
) -> None:
    # Arrange
    config = queue_config(queue, step=1000, create_ahead=2)
    run_maintenance(sync_db_engine, config)
    _fill_queue(sync_db_engine, queue, 1500)

    # Act
    result = run_maintenance(sync_db_engine, config)

    # Assert: the cursor's window is [1000, 2000), so [2000, 3000) is created ahead of it
    assert result.created_count == 1
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.cursors == {"msg_id": 1500}
    assert [op.target for op in result.maintenance_plan.creates] == [f"public.{queue}__2000"]
    assert set(range_children_of(sync_db_engine, queue)) == {f"{queue}__0", f"{queue}__1000", f"{queue}__2000"}


def test__numeric_root__windows__are_contiguous_without_gaps_or_overlaps(sync_db_engine: Engine, queue: str) -> None:
    # Arrange
    config = queue_config(queue, step=1000, create_ahead=3)
    run_maintenance(sync_db_engine, config)
    _fill_queue(sync_db_engine, queue, 2500)
    run_maintenance(sync_db_engine, config)

    # Act
    bounds = sorted((int(lo), int(hi)) for lo, hi in (range_children_of(sync_db_engine, queue)).values())

    # Assert
    assert bounds[0] == (0, 1000)
    assert bounds[-1] == (4000, 5000)
    for (_, upper), (lower, _) in pairwise(bounds):
        assert upper == lower


def test__numeric_root__rows__route_by_tableoid_into_their_window(sync_db_engine: Engine, queue: str) -> None:
    # Arrange
    config = queue_config(queue, step=1000, create_ahead=2)
    run_maintenance(sync_db_engine, config)
    _fill_queue(sync_db_engine, queue, 1500)

    # Act / Assert
    assert scalar(sync_db_engine, f'SELECT tableoid::regclass::text FROM "{queue}" WHERE msg_id = 1') == f"{queue}__0"  # noqa: S608
    assert (
        scalar(sync_db_engine, f'SELECT tableoid::regclass::text FROM "{queue}" WHERE msg_id = 1500')  # noqa: S608
        == f"{queue}__1000"
    )


def test__numeric_root__keep_behind__detaches_and_drops_windows_far_behind_the_cursor(
    sync_db_engine: Engine, queue: str
) -> None:
    # Arrange: the queue fills up one window at a time, maintenance keeping two ahead
    config = queue_config(queue, step=1000, create_ahead=2, distance=2000)
    run_maintenance(sync_db_engine, config)
    for batch in (1500, 1000):
        _fill_queue(sync_db_engine, queue, batch)
        run_maintenance(sync_db_engine, config)
    _fill_queue(sync_db_engine, queue, 1000)  # the cursor reaches 3500

    # Act
    result = run_maintenance(sync_db_engine, config)

    # Assert: [0, 1000) ends 2000 before the cursor's window and expires; [1000, 2000) is kept
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert result.maintenance_plan is not None
    assert [(op.target, op.reason) for op in result.maintenance_plan.detaches] == [
        (f"public.{queue}__0", Reason.RETENTION_EXPIRED)
    ]
    assert set(range_children_of(sync_db_engine, queue)) == {
        f"{queue}__1000",
        f"{queue}__2000",
        f"{queue}__3000",
        f"{queue}__4000",
    }
    assert relkind(sync_db_engine, f"{queue}__0") is None


def test__numeric_root__sequence_cursor__follows_the_sequence_not_the_surviving_rows(
    sync_db_engine: Engine, queue: str
) -> None:
    # Arrange: ids were handed out and consumed, leaving the table empty
    config = queue_config(queue, step=1000, create_ahead=2, cursor_source=CursorSource.SEQUENCE)
    run_maintenance(sync_db_engine, config)
    _fill_queue(sync_db_engine, queue, 1500)
    exec_sql(sync_db_engine, f'DELETE FROM "{queue}"')  # noqa: S608

    # Act
    result = run_maintenance(sync_db_engine, config)
    by_max_key = run_maintenance(sync_db_engine, queue_config(queue, step=1000, create_ahead=2))

    # Assert: the sequence still says 1500; max(key) says empty
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.cursors == {"msg_id": 1500}
    assert [op.target for op in result.maintenance_plan.creates] == [f"public.{queue}__2000"]
    assert by_max_key.maintenance_plan is not None
    assert by_max_key.maintenance_plan.cursors == {}
    assert by_max_key.created_count == 0


def test__numeric_root__ensure_partitions_with_a_window__backfills_that_window(
    sync_db_engine: Engine, queue: str
) -> None:
    # Arrange
    config = queue_config(queue, step=1000)

    # Act
    created = make_service(sync_db_engine).ensure_partitions(config, [Window(start=5000, end=6000)])

    # Assert
    assert [p.relname for p in created] == [f"{queue}__5000"]
    assert range_children_of(sync_db_engine, queue) == {f"{queue}__5000": ("5000", "6000")}


# ── Detach grace and drop policies ──────────────────────────────────────────────


def _grace_config(table: str, grace: timedelta) -> LifecyclePolicy:
    return LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=1), drop=DropAfter(grace=grace))


def test__drop_after_grace__detach_now_drop_a_week_later(sync_db_engine: Engine, table: str) -> None:
    # Arrange: June exists; by August it has expired
    config = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=7)))
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")
    june = f"{table}__2026_06"

    # Act — day 0
    day0 = run_maintenance(sync_db_engine, config, at_time="2026-08-01")

    # Assert — detached, not dropped, and the marker records when
    assert day0.detached_count == 1
    assert day0.dropped_count == 0
    assert day0.maintenance_plan is not None
    assert day0.maintenance_plan.drops == ()
    assert is_attached(sync_db_engine, june) is False
    comment = table_comment(sync_db_engine, june)
    assert comment is not None
    assert comment.splitlines() == [
        orphan_marker(f"public.{table}"),
        f"{DETACHED_AT_MARKER}2026-08-01T00:00:00+00:00",
    ]

    # Act — day 3
    day3 = run_maintenance(sync_db_engine, config, at_time="2026-08-04")

    # Assert — still there, reported as waiting, not as a problem
    assert day3.dropped_count == 0
    assert day3.issues == ()
    assert day3.maintenance_plan is not None
    findings = {f.partition_name: f.reason for f in day3.maintenance_plan.findings}
    assert findings == {f"public.{june}": FindingReason.GRACE_PENDING}
    assert relkind(sync_db_engine, june) == "r"

    # Act — day 8
    day8 = run_maintenance(sync_db_engine, config, at_time="2026-08-09")

    # Assert — gone
    assert day8.dropped_count == 1
    assert day8.maintenance_plan is not None
    assert [(op.reason, op.detached_at) for op in day8.maintenance_plan.drops] == [
        (Reason.GRACE_ELAPSED, datetime(2026, 8, 1, tzinfo=UTC))
    ]
    assert relkind(sync_db_engine, june) is None


def test__drop_never__keeps_the_detached_table_forever(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = monthly_config(
        table, lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=1), drop=DropNever())
    )
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")
    june = f"{table}__2026_06"

    # Act
    first = run_maintenance(sync_db_engine, config, at_time="2026-08-01")
    much_later = run_maintenance(sync_db_engine, config, at_time="2027-08-01")

    # Assert
    assert first.detached_count == 1
    assert first.dropped_count == 0
    assert much_later.dropped_count == 0
    assert much_later.maintenance_plan is not None
    assert much_later.maintenance_plan.drops == ()
    assert relkind(sync_db_engine, june) == "r"
    assert is_attached(sync_db_engine, june) is False


def test__drop_after_grace__orphan_marked_before_1_0__dropped_immediately(sync_db_engine: Engine, table: str) -> None:
    # Arrange: a marker with the ownership line only, as older versions wrote it
    config = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=7)))
    legacy = f"{table}__2026_01"
    exec_sql(sync_db_engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL)')
    exec_sql(sync_db_engine, f"COMMENT ON TABLE \"{legacy}\" IS '{orphan_marker(f'public.{table}')}'")

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert: an unknown instant cannot delay a table that has already waited
    assert result.dropped_count == 1
    assert result.maintenance_plan is not None
    assert [(op.reason, op.detached_at) for op in result.maintenance_plan.drops] == [(Reason.GRACE_ELAPSED, None)]
    assert relkind(sync_db_engine, legacy) is None


# ── Detach modes ────────────────────────────────────────────────────────────────


def _detach_config(table: str, mode: DetachMode) -> LifecyclePolicy:
    return LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=1), detach=mode)


def _seed_june_and_default(engine: Engine, table: str, mode: DetachMode, *, default: bool) -> None:
    run_maintenance(engine, monthly_config(table, lifecycle=_detach_config(table, mode)), at_time="2026-06-15")
    if default:
        exec_sql(engine, f'CREATE TABLE "{table}_default" PARTITION OF "{table}" DEFAULT')


def test__detach_mode_blocking__with_a_default_partition__detaches(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    _seed_june_and_default(sync_db_engine, table, DetachMode.BLOCKING, default=True)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.BLOCKING))

    # Act
    result = run_maintenance(sync_db_engine, config, at_time="2026-08-01")

    # Assert
    assert result.issues == ()
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert relkind(sync_db_engine, f"{table}__2026_06") is None


def test__detach_mode_concurrent__with_a_default_partition__fails_and_is_recorded(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: PostgreSQL refuses DETACH CONCURRENTLY while a DEFAULT partition exists
    _seed_june_and_default(sync_db_engine, table, DetachMode.CONCURRENT, default=True)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.CONCURRENT))

    # Act
    result = run_maintenance(sync_db_engine, config, at_time="2026-08-01", continue_on_error=True)

    # Assert: the refusal is isolated; the drop that followed the detach is skipped
    assert result.detached_count == 0
    assert result.dropped_count == 0
    assert [(issue.step, issue.partition_name) for issue in result.issues] == [
        (MaintenanceIssueStep.DETACH, f"public.{table}__2026_06")
    ]
    assert "default partition" in result.issues[0].error
    assert is_attached(sync_db_engine, f"{table}__2026_06") is True


def test__detach_mode_concurrent__with_a_default_partition__raises_without_continue_on_error(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    _seed_june_and_default(sync_db_engine, table, DetachMode.CONCURRENT, default=True)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.CONCURRENT))

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="default partition"):
        run_maintenance(sync_db_engine, config, at_time="2026-08-01")
    assert is_attached(sync_db_engine, f"{table}__2026_06") is True


def test__detach_mode_concurrent__without_a_default_partition__detaches(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    _seed_june_and_default(sync_db_engine, table, DetachMode.CONCURRENT, default=False)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.CONCURRENT))

    # Act
    result = run_maintenance(sync_db_engine, config, at_time="2026-08-01")

    # Assert
    assert result.issues == ()
    assert result.detached_count == 1
    assert result.dropped_count == 1


def test__detach_mode_auto__with_a_default_partition__falls_back_to_the_blocking_form(
    sync_db_engine: Engine, table: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange
    _seed_june_and_default(sync_db_engine, table, DetachMode.AUTO, default=True)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.AUTO))

    # Act
    with caplog.at_level(logging.WARNING, logger="pg_partsmith.sync.repositories.remover"):
        result = run_maintenance(sync_db_engine, config, at_time="2026-08-01")

    # Assert
    assert result.issues == ()
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert any("falling back to non-concurrent DETACH" in record.getMessage() for record in caplog.records)


# ── Policies over facts ─────────────────────────────────────────────────────────

_NO_PENDING = SqlPredicate(sql="SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE payload = 'pending')")


def _seed_may_and_june_with_a_pending_row(engine: Engine, table: str) -> tuple[str, str]:
    may = _seed_month(engine, table, 2026, 5, rows=3)
    june = _seed_month(engine, table, 2026, 6, rows=3)
    exec_sql(engine, f"INSERT INTO \"{table}\" (created_at, payload) VALUES ('2026-06-20', 'pending')")  # noqa: S608
    return may, june


def test__expire_if__keep_newest_and_sql_predicate__detaches_only_the_partition_with_no_pending_rows(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    may, june = _seed_may_and_june_with_a_pending_row(sync_db_engine, table)
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1), retention=ExpireIf(when=AllOf(members=(KeepNewest(count=1), _NO_PENDING)))
        ),
    )

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert result.detached_count == 1
    assert relkind(sync_db_engine, may) is None
    assert is_attached(sync_db_engine, june) is True


def test__expire_if__age_and_sql_predicate__detaches_only_the_partition_with_no_pending_rows(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: both months are older than thirty days; only June holds pending work
    may, june = _seed_may_and_june_with_a_pending_row(sync_db_engine, table)
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1),
            retention=ExpireIf(when=AllOf(members=(WindowAgeAbove(age=timedelta(days=30)), _NO_PENDING))),
        ),
    )

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert result.maintenance_plan is not None
    assert [op.target for op in result.maintenance_plan.detaches] == [f"public.{may}"]
    assert relkind(sync_db_engine, may) is None
    assert is_attached(sync_db_engine, june) is True

    # Act: once the pending row is gone, June expires too
    exec_sql(sync_db_engine, f"DELETE FROM \"{june}\" WHERE payload = 'pending'")  # noqa: S608
    later = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert later.detached_count == 1
    assert relkind(sync_db_engine, june) is None


_DROP_UNLESS_LARGE = LifecyclePolicy(
    creation=CreateAhead(count=1),
    retention=KeepNewest(count=1),
    drop=DropAfter(when=Not(member=SizeAbove(bytes=256 * 1024))),
)


def test__drop_after__not_size_above__same_run_drop_skips_the_large_partition(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: May is empty, June carries a megabyte; both have expired
    may = _seed_month(sync_db_engine, table, 2026, 5)
    june = _seed_month(sync_db_engine, table, 2026, 6, rows=5000)
    config = monthly_config(table, lifecycle=_DROP_UNLESS_LARGE)

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert: both detached, only the small one dropped, and the sizes are on the plan
    assert result.detached_count == 2
    assert result.dropped_count == 1
    assert result.maintenance_plan is not None
    sizes = {op.target: op.size_bytes for op in result.maintenance_plan.detaches}
    assert sizes[f"public.{may}"] is not None and sizes[f"public.{may}"] <= 256 * 1024
    assert sizes[f"public.{june}"] is not None and sizes[f"public.{june}"] > 256 * 1024
    assert [op.target for op in result.maintenance_plan.drops] == [f"public.{may}"]
    assert relkind(sync_db_engine, may) is None
    assert relkind(sync_db_engine, june) == "r"
    assert is_attached(sync_db_engine, june) is False


def test__drop_after__not_size_above__keeps_deferring_the_large_orphan_until_it_shrinks(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: June was detached last run and is still a megabyte
    june = _seed_month(sync_db_engine, table, 2026, 6, rows=5000)
    config = monthly_config(table, lifecycle=_DROP_UNLESS_LARGE)
    first = run_maintenance(sync_db_engine, config, at_time=NOW)
    assert first.detached_count == 1
    assert first.dropped_count == 0

    # Act: the next run keeps deferring while it is still big
    deferred = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert deferred.dropped_count == 0
    assert deferred.maintenance_plan is not None
    findings = {f.partition_name: f.reason for f in deferred.maintenance_plan.findings}
    assert findings == {f"public.{june}": FindingReason.DROP_DEFERRED}
    assert relkind(sync_db_engine, june) == "r"

    # Act: emptied, it is finally dropped
    exec_sql(sync_db_engine, f'TRUNCATE "{june}"')
    emptied = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert emptied.dropped_count == 1
    assert relkind(sync_db_engine, june) is None


def test__create_until__creates_every_window_through_the_horizon(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateUntil(position=datetime(2026, 12, 1, tzinfo=UTC)), retention=KeepNewest(count=12)
        ),
    )

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)
    with count_ddl(sync_db_engine) as counter:
        again = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert result.created_count == 5
    assert result.maintenance_plan is not None
    assert {op.reason for op in result.maintenance_plan.creates} == {Reason.CREATE_UNTIL}
    assert set(range_children_of(sync_db_engine, table)) == {f"{table}__2026_{m:02d}" for m in range(8, 13)}
    assert again.created_count == 0
    assert counter.statements == []


def test__create_until__horizon_behind_the_cursor__creates_the_cursor_window_alone(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(creation=CreateUntil(position=datetime(2020, 1, 1, tzinfo=UTC))),
    )

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert result.created_count == 1
    assert set(range_children_of(sync_db_engine, table)) == {f"{table}__2026_08"}


def test__keep_for__expires_windows_that_ended_more_than_the_age_ago(sync_db_engine: Engine, table: str) -> None:
    # Arrange: May ended 86 days before "now", June 56 days before
    for month in (5, 6, 7, 8):
        _seed_month(sync_db_engine, table, 2026, month)
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepFor(age=timedelta(days=60))),
    )

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert relkind(sync_db_engine, f"{table}__2026_05") is None
    assert set(range_children_of(sync_db_engine, table)) == {
        f"{table}__2026_06",
        f"{table}__2026_07",
        f"{table}__2026_08",
    }


# ── Orphans that come back ──────────────────────────────────────────────────────


def test__orphan_reattach__window_in_the_create_ahead_set__reattached_not_recreated(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: September was created ahead, then detached through the library
    config = monthly_config(table, create_ahead=2, retention=12)
    run_maintenance(sync_db_engine, config, at_time=NOW)
    september = f"{table}__2026_09"
    oid_before = relation_oid(sync_db_engine, september)
    listed = PostgresMetadataProvider(sync_db_engine).list_partitions(table)
    with freezegun.freeze_time(NOW):
        make_service(sync_db_engine).detach_old_partitions(table, [p for p in listed if p.relname == september])
    assert is_attached(sync_db_engine, september) is False

    # Act
    result = run_maintenance(sync_db_engine, config, at_time=NOW)

    # Assert
    assert result.attached_count == 1
    assert result.created_count == 0
    assert result.maintenance_plan is not None
    assert [(op.target, op.reason) for op in result.maintenance_plan.attaches] == [
        (f"public.{september}", Reason.REATTACH)
    ]
    assert is_attached(sync_db_engine, september) is True
    assert relation_oid(sync_db_engine, september) == oid_before


def test__orphan_reattach__ensure_partitions_naming_an_orphaned_window__reattaches_it(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: an expired, still-present June (a month of grace)
    config = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=30)))
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")
    run_maintenance(sync_db_engine, config, at_time="2026-08-01")
    june = f"{table}__2026_06"
    oid_before = relation_oid(sync_db_engine, june)
    assert is_attached(sync_db_engine, june) is False

    # Act: a backfill names June explicitly
    created = make_service(sync_db_engine).ensure_partitions(config, [Period(year=2026, month=6)])

    # Assert: nothing new; the orphan is back with its data
    assert created == []
    assert is_attached(sync_db_engine, june) is True
    assert relation_oid(sync_db_engine, june) == oid_before


def test__orphan_reattach__retention_grew__the_orphan_is_reattached_rather_than_kept_waiting(
    sync_db_engine: Engine, table: str
) -> None:
    # Arrange: July expired under KeepNewest(1) and sits in its grace period
    narrow = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=30)))
    run_maintenance(sync_db_engine, narrow, at_time="2026-07-15")
    run_maintenance(sync_db_engine, narrow, at_time="2026-08-10")
    july = f"{table}__2026_07"
    assert is_attached(sync_db_engine, july) is False

    # Act: retention grows to three months, which wants July again
    wide = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1), retention=KeepNewest(count=3), drop=DropAfter(grace=timedelta(days=30))
        ),
    )
    result = run_maintenance(sync_db_engine, wide, at_time="2026-08-11")

    # Assert
    assert result.attached_count == 1
    assert result.created_count == 0
    assert is_attached(sync_db_engine, july) is True


def test__orphan__not_wanted_again__waits_out_its_grace_then_drops(sync_db_engine: Engine, table: str) -> None:
    # Arrange
    config = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=3)))
    run_maintenance(sync_db_engine, config, at_time="2026-06-15")
    run_maintenance(sync_db_engine, config, at_time="2026-08-01")
    june = f"{table}__2026_06"

    # Act
    pending = run_maintenance(sync_db_engine, config, at_time="2026-08-02")
    dropped = run_maintenance(sync_db_engine, config, at_time="2026-08-05")

    # Assert
    assert pending.attached_count == 0
    assert pending.dropped_count == 0
    assert dropped.dropped_count == 1
    assert relkind(sync_db_engine, june) is None
