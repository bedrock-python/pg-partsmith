"""Lifecycle policies against a real PostgreSQL (async).

Numeric windows and their cursor, detach grace and drop policies, detach
modes, fact-driven predicates, horizon and age based rules, and orphans that
come back.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING

import freezegun
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.metadata import PostgresMetadataProvider
from pg_partsmith.boundaries import CursorSource, Window
from pg_partsmith.entities import MaintenanceIssueStep, Period
from pg_partsmith.lifecycle import (
    AllOf,
    CreateAhead,
    CreateUntil,
    DetachMode,
    DropAfter,
    DropNever,
    ExpireAction,
    ExpireIf,
    KeepFor,
    KeepNewest,
    LifecyclePolicy,
    Not,
    SizeAbove,
    SqlPredicate,
    WindowAgeAbove,
)
from pg_partsmith.plan import FindingReason, OperationKind, Reason
from pg_partsmith.utils import DETACHED_AT_MARKER
from tests.integration.aio.support import (
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
from tests.integration.nested_support import (
    MONTHLY_TABLE_DDL,
    QUEUE_TABLE_DDL,
    monthly_config,
    orphan_marker,
    queue_config,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

NOW = "2026-08-26"


@pytest_asyncio.fixture
async def table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, MONTHLY_TABLE_DDL, prefix="policy"):
        yield name


@pytest_asyncio.fixture
async def queue(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, QUEUE_TABLE_DDL, prefix="queue"):
        yield name


async def _fill_queue(engine: AsyncEngine, queue: str, rows: int) -> None:
    await exec_sql(engine, f"INSERT INTO \"{queue}\" (payload) SELECT 'x' FROM generate_series(1, {rows})")  # noqa: S608


async def _seed_month(engine: AsyncEngine, table: str, year: int, month: int, *, rows: int = 0) -> str:
    """Create one monthly partition through the library and optionally fill it."""
    name = f"{table}__{year:04d}_{month:02d}"
    await make_service(engine).ensure_partitions(monthly_config(table), [Period(year=year, month=month)])
    if rows:
        await exec_sql(
            engine,
            f'INSERT INTO "{table}" (created_at, payload) '  # noqa: S608
            f"SELECT '{year:04d}-{month:02d}-15'::timestamptz, repeat('x', 200) FROM generate_series(1, {rows})",
        )
    return name


# ── Numeric RANGE root ──────────────────────────────────────────────────────────


async def test__numeric_root__empty_table__creates_ahead_from_the_origin(db_engine: AsyncEngine, queue: str) -> None:
    # Arrange / Act
    result = await run_maintenance(db_engine, queue_config(queue, step=1000, create_ahead=2))

    # Assert
    assert result.success
    assert result.created_count == 2
    assert await range_children_of(db_engine, queue) == {
        f"{queue}__0": ("0", "1000"),
        f"{queue}__1000": ("1000", "2000"),
    }
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.cursors == {}


async def test__numeric_root__rows_inserted__cursor_moves_to_max_key_and_the_next_window_appears(
    db_engine: AsyncEngine, queue: str
) -> None:
    # Arrange
    config = queue_config(queue, step=1000, create_ahead=2)
    await run_maintenance(db_engine, config)
    await _fill_queue(db_engine, queue, 1500)

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert: the cursor's window is [1000, 2000), so [2000, 3000) is created ahead of it
    assert result.created_count == 1
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.cursors == {"msg_id": 1500}
    assert [op.target for op in result.maintenance_plan.creates] == [f"public.{queue}__2000"]
    assert set(await range_children_of(db_engine, queue)) == {f"{queue}__0", f"{queue}__1000", f"{queue}__2000"}


async def test__numeric_root__windows__are_contiguous_without_gaps_or_overlaps(
    db_engine: AsyncEngine, queue: str
) -> None:
    # Arrange
    config = queue_config(queue, step=1000, create_ahead=3)
    await run_maintenance(db_engine, config)
    await _fill_queue(db_engine, queue, 2500)
    await run_maintenance(db_engine, config)

    # Act
    bounds = sorted((int(lo), int(hi)) for lo, hi in (await range_children_of(db_engine, queue)).values())

    # Assert
    assert bounds[0] == (0, 1000)
    assert bounds[-1] == (4000, 5000)
    for (_, upper), (lower, _) in pairwise(bounds):
        assert upper == lower


async def test__numeric_root__rows__route_by_tableoid_into_their_window(db_engine: AsyncEngine, queue: str) -> None:
    # Arrange
    config = queue_config(queue, step=1000, create_ahead=2)
    await run_maintenance(db_engine, config)
    await _fill_queue(db_engine, queue, 1500)

    # Act / Assert
    assert await scalar(db_engine, f'SELECT tableoid::regclass::text FROM "{queue}" WHERE msg_id = 1') == f"{queue}__0"  # noqa: S608
    assert (
        await scalar(db_engine, f'SELECT tableoid::regclass::text FROM "{queue}" WHERE msg_id = 1500')  # noqa: S608
        == f"{queue}__1000"
    )


async def test__numeric_root__keep_behind__detaches_and_drops_windows_far_behind_the_cursor(
    db_engine: AsyncEngine, queue: str
) -> None:
    # Arrange: the queue fills up one window at a time, maintenance keeping two ahead
    config = queue_config(queue, step=1000, create_ahead=2, distance=2000)
    await run_maintenance(db_engine, config)
    for batch in (1500, 1000):
        await _fill_queue(db_engine, queue, batch)
        await run_maintenance(db_engine, config)
    await _fill_queue(db_engine, queue, 1000)  # the cursor reaches 3500

    # Act
    result = await run_maintenance(db_engine, config)

    # Assert: [0, 1000) ends 2000 before the cursor's window and expires; [1000, 2000) is kept
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert result.maintenance_plan is not None
    assert [(op.target, op.reason) for op in result.maintenance_plan.detaches] == [
        (f"public.{queue}__0", Reason.RETENTION_EXPIRED)
    ]
    assert set(await range_children_of(db_engine, queue)) == {
        f"{queue}__1000",
        f"{queue}__2000",
        f"{queue}__3000",
        f"{queue}__4000",
    }
    assert await relkind(db_engine, f"{queue}__0") is None


async def test__numeric_root__sequence_cursor__follows_the_sequence_not_the_surviving_rows(
    db_engine: AsyncEngine, queue: str
) -> None:
    # Arrange: ids were handed out and consumed, leaving the table empty
    config = queue_config(queue, step=1000, create_ahead=2, cursor_source=CursorSource.SEQUENCE)
    await run_maintenance(db_engine, config)
    await _fill_queue(db_engine, queue, 1500)
    await exec_sql(db_engine, f'DELETE FROM "{queue}"')  # noqa: S608

    # Act
    result = await run_maintenance(db_engine, config)
    by_max_key = await run_maintenance(db_engine, queue_config(queue, step=1000, create_ahead=2))

    # Assert: the sequence still says 1500; max(key) says empty
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.cursors == {"msg_id": 1500}
    assert [op.target for op in result.maintenance_plan.creates] == [f"public.{queue}__2000"]
    assert by_max_key.maintenance_plan is not None
    assert by_max_key.maintenance_plan.cursors == {}
    assert by_max_key.created_count == 0


async def test__numeric_root__ensure_partitions_with_a_window__backfills_that_window(
    db_engine: AsyncEngine, queue: str
) -> None:
    # Arrange
    config = queue_config(queue, step=1000)

    # Act
    created = await make_service(db_engine).ensure_partitions(config, [Window(start=5000, end=6000)])

    # Assert
    assert [p.relname for p in created] == [f"{queue}__5000"]
    assert await range_children_of(db_engine, queue) == {f"{queue}__5000": ("5000", "6000")}


# ── Detach grace and drop policies ──────────────────────────────────────────────


def _grace_config(table: str, grace: timedelta) -> LifecyclePolicy:
    return LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=1), drop=DropAfter(grace=grace))


async def test__drop_after_grace__detach_now_drop_a_week_later(db_engine: AsyncEngine, table: str) -> None:
    # Arrange: June exists; by August it has expired
    config = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=7)))
    await run_maintenance(db_engine, config, at_time="2026-06-15")
    june = f"{table}__2026_06"

    # Act — day 0
    day0 = await run_maintenance(db_engine, config, at_time="2026-08-01")

    # Assert — detached, not dropped, and the marker records when
    assert day0.detached_count == 1
    assert day0.dropped_count == 0
    assert day0.maintenance_plan is not None
    assert day0.maintenance_plan.drops == ()
    assert await is_attached(db_engine, june) is False
    comment = await table_comment(db_engine, june)
    assert comment is not None
    assert comment.splitlines() == [
        orphan_marker(f"public.{table}"),
        f"{DETACHED_AT_MARKER}2026-08-01T00:00:00+00:00",
    ]

    # Act — day 3
    day3 = await run_maintenance(db_engine, config, at_time="2026-08-04")

    # Assert — still there, reported as waiting, not as a problem
    assert day3.dropped_count == 0
    assert day3.issues == ()
    assert day3.maintenance_plan is not None
    findings = {f.partition_name: f.reason for f in day3.maintenance_plan.findings}
    assert findings == {f"public.{june}": FindingReason.GRACE_PENDING}
    assert await relkind(db_engine, june) == "r"

    # Act — day 8
    day8 = await run_maintenance(db_engine, config, at_time="2026-08-09")

    # Assert — gone
    assert day8.dropped_count == 1
    assert day8.maintenance_plan is not None
    assert [(op.reason, op.detached_at) for op in day8.maintenance_plan.drops] == [
        (Reason.GRACE_ELAPSED, datetime(2026, 8, 1, tzinfo=UTC))
    ]
    assert await relkind(db_engine, june) is None


async def test__drop_never__keeps_the_detached_table_forever(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = monthly_config(
        table, lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=1), drop=DropNever())
    )
    await run_maintenance(db_engine, config, at_time="2026-06-15")
    june = f"{table}__2026_06"

    # Act
    first = await run_maintenance(db_engine, config, at_time="2026-08-01")
    much_later = await run_maintenance(db_engine, config, at_time="2027-08-01")

    # Assert
    assert first.detached_count == 1
    assert first.dropped_count == 0
    assert much_later.dropped_count == 0
    assert much_later.maintenance_plan is not None
    assert much_later.maintenance_plan.drops == ()
    assert await relkind(db_engine, june) == "r"
    assert await is_attached(db_engine, june) is False


async def test__drop_after_grace__orphan_marked_before_1_0__dropped_immediately(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: a marker with the ownership line only, as older versions wrote it
    config = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=7)))
    legacy = f"{table}__2026_01"
    await exec_sql(db_engine, f'CREATE TABLE "{legacy}" (LIKE "{table}" INCLUDING ALL)')
    await exec_sql(db_engine, f"COMMENT ON TABLE \"{legacy}\" IS '{orphan_marker(f'public.{table}')}'")

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert: an unknown instant cannot delay a table that has already waited
    assert result.dropped_count == 1
    assert result.maintenance_plan is not None
    assert [(op.reason, op.detached_at) for op in result.maintenance_plan.drops] == [(Reason.GRACE_ELAPSED, None)]
    assert await relkind(db_engine, legacy) is None


# ── Detach modes ────────────────────────────────────────────────────────────────


def _detach_config(table: str, mode: DetachMode) -> LifecyclePolicy:
    return LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=1), detach=mode)


async def _seed_june_and_default(engine: AsyncEngine, table: str, mode: DetachMode, *, default: bool) -> None:
    await run_maintenance(engine, monthly_config(table, lifecycle=_detach_config(table, mode)), at_time="2026-06-15")
    if default:
        await exec_sql(engine, f'CREATE TABLE "{table}_default" PARTITION OF "{table}" DEFAULT')


async def test__detach_mode_blocking__with_a_default_partition__detaches(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    await _seed_june_and_default(db_engine, table, DetachMode.BLOCKING, default=True)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.BLOCKING))

    # Act
    result = await run_maintenance(db_engine, config, at_time="2026-08-01")

    # Assert
    assert result.issues == ()
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert await relkind(db_engine, f"{table}__2026_06") is None


async def test__detach_mode_concurrent__with_a_default_partition__fails_and_is_recorded(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: PostgreSQL refuses DETACH CONCURRENTLY while a DEFAULT partition exists
    await _seed_june_and_default(db_engine, table, DetachMode.CONCURRENT, default=True)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.CONCURRENT))

    # Act
    result = await run_maintenance(db_engine, config, at_time="2026-08-01", continue_on_error=True)

    # Assert: the refusal is isolated; the drop that followed the detach is skipped
    assert result.detached_count == 0
    assert result.dropped_count == 0
    assert [(issue.step, issue.partition_name) for issue in result.issues] == [
        (MaintenanceIssueStep.DETACH, f"public.{table}__2026_06")
    ]
    assert "default partition" in result.issues[0].error
    assert await is_attached(db_engine, f"{table}__2026_06") is True


async def test__detach_mode_concurrent__with_a_default_partition__raises_without_continue_on_error(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _seed_june_and_default(db_engine, table, DetachMode.CONCURRENT, default=True)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.CONCURRENT))

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="default partition"):
        await run_maintenance(db_engine, config, at_time="2026-08-01")
    assert await is_attached(db_engine, f"{table}__2026_06") is True


async def test__detach_mode_concurrent__without_a_default_partition__detaches(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    await _seed_june_and_default(db_engine, table, DetachMode.CONCURRENT, default=False)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.CONCURRENT))

    # Act
    result = await run_maintenance(db_engine, config, at_time="2026-08-01")

    # Assert
    assert result.issues == ()
    assert result.detached_count == 1
    assert result.dropped_count == 1


async def test__detach_mode_auto__with_a_default_partition__is_planned_as_the_blocking_form(
    db_engine: AsyncEngine, table: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange: the server refuses the concurrent form outright while a DEFAULT
    # exists, and the planner reads the DEFAULT from the same catalog snapshot
    # it plans from -- so the plan says blocking, and no statement is issued
    # that could only fail.
    await _seed_june_and_default(db_engine, table, DetachMode.AUTO, default=True)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.AUTO))

    # Act
    with caplog.at_level(logging.WARNING, logger="pg_partsmith.aio.repositories.remover"):
        result = await run_maintenance(db_engine, config, at_time="2026-08-01")

    # Assert: the outcome is what the fallback used to reach, without the failure
    assert result.issues == ()
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert not any("falling back to non-concurrent DETACH" in record.getMessage() for record in caplog.records)
    assert result.maintenance_plan is not None
    detach = result.maintenance_plan.detaches[0]
    assert detach.mode is DetachMode.BLOCKING
    assert "DEFAULT partition" in detach.detail
    assert "ACCESS EXCLUSIVE on the parent" in detach.capabilities.lock


async def test__detach_mode_auto__without_a_default_partition__stays_concurrent(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: nothing forces the blocking form here, so AUTO is what it says.
    await _seed_june_and_default(db_engine, table, DetachMode.AUTO, default=False)
    config = monthly_config(table, lifecycle=_detach_config(table, DetachMode.AUTO))

    # Act
    result = await run_maintenance(db_engine, config, at_time="2026-08-01")

    # Assert
    assert result.issues == ()
    assert result.detached_count == 1
    assert result.maintenance_plan is not None
    assert result.maintenance_plan.detaches[0].mode is DetachMode.AUTO


# ── Policies over facts ─────────────────────────────────────────────────────────

_NO_PENDING = SqlPredicate(sql="SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE payload = 'pending')")


async def _seed_may_and_june_with_a_pending_row(engine: AsyncEngine, table: str) -> tuple[str, str]:
    may = await _seed_month(engine, table, 2026, 5, rows=3)
    june = await _seed_month(engine, table, 2026, 6, rows=3)
    await exec_sql(engine, f"INSERT INTO \"{table}\" (created_at, payload) VALUES ('2026-06-20', 'pending')")  # noqa: S608
    return may, june


async def test__expire_if__keep_newest_and_sql_predicate__detaches_only_the_partition_with_no_pending_rows(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    may, june = await _seed_may_and_june_with_a_pending_row(db_engine, table)
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1), retention=ExpireIf(when=AllOf(members=(KeepNewest(count=1), _NO_PENDING)))
        ),
    )

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.detached_count == 1
    assert await relkind(db_engine, may) is None
    assert await is_attached(db_engine, june) is True


async def test__expire_if__age_and_sql_predicate__detaches_only_the_partition_with_no_pending_rows(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: both months are older than thirty days; only June holds pending work
    may, june = await _seed_may_and_june_with_a_pending_row(db_engine, table)
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1),
            retention=ExpireIf(when=AllOf(members=(WindowAgeAbove(age=timedelta(days=30)), _NO_PENDING))),
        ),
    )

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert result.maintenance_plan is not None
    assert [op.target for op in result.maintenance_plan.detaches] == [f"public.{may}"]
    assert await relkind(db_engine, may) is None
    assert await is_attached(db_engine, june) is True

    # Act: once the pending row is gone, June expires too
    await exec_sql(db_engine, f"DELETE FROM \"{june}\" WHERE payload = 'pending'")  # noqa: S608
    later = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert later.detached_count == 1
    assert await relkind(db_engine, june) is None


_DROP_UNLESS_LARGE = LifecyclePolicy(
    creation=CreateAhead(count=1),
    retention=KeepNewest(count=1),
    drop=DropAfter(when=Not(member=SizeAbove(bytes=256 * 1024))),
)


async def test__drop_after__not_size_above__same_run_drop_skips_the_large_partition(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: May is empty, June carries a megabyte; both have expired
    may = await _seed_month(db_engine, table, 2026, 5)
    june = await _seed_month(db_engine, table, 2026, 6, rows=5000)
    config = monthly_config(table, lifecycle=_DROP_UNLESS_LARGE)

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert: both detached, only the small one dropped, and the sizes are on the plan
    assert result.detached_count == 2
    assert result.dropped_count == 1
    assert result.maintenance_plan is not None
    sizes = {op.target: op.size_bytes for op in result.maintenance_plan.detaches}
    assert sizes[f"public.{may}"] is not None and sizes[f"public.{may}"] <= 256 * 1024
    assert sizes[f"public.{june}"] is not None and sizes[f"public.{june}"] > 256 * 1024
    assert [op.target for op in result.maintenance_plan.drops] == [f"public.{may}"]
    assert await relkind(db_engine, may) is None
    assert await relkind(db_engine, june) == "r"
    assert await is_attached(db_engine, june) is False


async def test__drop_after__not_size_above__keeps_deferring_the_large_orphan_until_it_shrinks(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: June was detached last run and is still a megabyte
    june = await _seed_month(db_engine, table, 2026, 6, rows=5000)
    config = monthly_config(table, lifecycle=_DROP_UNLESS_LARGE)
    first = await run_maintenance(db_engine, config, at_time=NOW)
    assert first.detached_count == 1
    assert first.dropped_count == 0

    # Act: the next run keeps deferring while it is still big
    deferred = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert deferred.dropped_count == 0
    assert deferred.maintenance_plan is not None
    findings = {f.partition_name: f.reason for f in deferred.maintenance_plan.findings}
    assert findings == {f"public.{june}": FindingReason.DROP_DEFERRED}
    assert await relkind(db_engine, june) == "r"

    # Act: emptied, it is finally dropped
    await exec_sql(db_engine, f'TRUNCATE "{june}"')
    emptied = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert emptied.dropped_count == 1
    assert await relkind(db_engine, june) is None


async def test__create_until__creates_every_window_through_the_horizon(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateUntil(position=datetime(2026, 12, 1, tzinfo=UTC)), retention=KeepNewest(count=12)
        ),
    )

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)
    with count_ddl(db_engine) as counter:
        again = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.created_count == 5
    assert result.maintenance_plan is not None
    assert {op.reason for op in result.maintenance_plan.creates} == {Reason.CREATE_UNTIL}
    assert set(await range_children_of(db_engine, table)) == {f"{table}__2026_{m:02d}" for m in range(8, 13)}
    assert again.created_count == 0
    assert counter.statements == []


async def test__create_until__horizon_behind_the_cursor__creates_the_cursor_window_alone(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(creation=CreateUntil(position=datetime(2020, 1, 1, tzinfo=UTC))),
    )

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.created_count == 1
    assert set(await range_children_of(db_engine, table)) == {f"{table}__2026_08"}


async def test__keep_for__expires_windows_that_ended_more_than_the_age_ago(db_engine: AsyncEngine, table: str) -> None:
    # Arrange: May ended 86 days before "now", June 56 days before
    for month in (5, 6, 7, 8):
        await _seed_month(db_engine, table, 2026, month)
    config = monthly_config(
        table,
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepFor(age=timedelta(days=60))),
    )

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert await relkind(db_engine, f"{table}__2026_05") is None
    assert set(await range_children_of(db_engine, table)) == {
        f"{table}__2026_06",
        f"{table}__2026_07",
        f"{table}__2026_08",
    }


# ── Emptying expired partitions in place ────────────────────────────────────────


def _truncating(retention: KeepFor | KeepNewest) -> LifecyclePolicy:
    return LifecyclePolicy(creation=CreateAhead(count=1), retention=retention, expire=ExpireAction.TRUNCATE)


async def test__expire_truncate__a_year_old_partition__emptied_in_place_and_left_attached(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: the reporter's setup -- a year of retention, and a DEFAULT partition, which
    # would force a detach into its blocking form
    july = await _seed_month(db_engine, table, 2025, 7, rows=40)
    await _seed_month(db_engine, table, 2026, 8, rows=5)
    await exec_sql(db_engine, f'CREATE TABLE "{table}_default" PARTITION OF "{table}" DEFAULT')
    oid_before = await relation_oid(db_engine, july)
    config = monthly_config(table, lifecycle=_truncating(KeepFor(age=timedelta(days=365))))

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert: the same table, still attached and now empty; nothing else touched
    assert result.issues == ()
    assert (result.truncated_count, result.detached_count, result.dropped_count) == (1, 0, 0)
    assert await is_attached(db_engine, july)
    assert await relation_oid(db_engine, july) == oid_before
    assert await table_comment(db_engine, july) is None
    assert await scalar(db_engine, f'SELECT count(*) FROM "{july}"') == 0  # noqa: S608
    assert await scalar(db_engine, f'SELECT count(*) FROM "{table}"') == 5  # noqa: S608
    assert set(await range_children_of(db_engine, table)) == {july, f"{table}__2026_08"}


async def test__expire_truncate__second_run__issues_no_ddl_and_reports_what_it_keeps(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: one run has emptied July 2025 already
    await _seed_month(db_engine, table, 2025, 7, rows=40)
    await _seed_month(db_engine, table, 2026, 8, rows=5)
    config = monthly_config(table, lifecycle=_truncating(KeepFor(age=timedelta(days=365))))
    await run_maintenance(db_engine, config, at_time=NOW)

    # Act
    with count_ddl(db_engine) as counter:
        result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert counter.statements == []
    assert result.truncated_count == 0
    assert result.maintenance_plan is not None
    assert [(f.reason, f.partition_name) for f in result.maintenance_plan.findings] == [
        (FindingReason.EXPIRED_EMPTY, f"public.{table}")
    ]
    assert result.issues == ()


async def test__expire_truncate__a_late_row_in_an_emptied_partition__emptied_again(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: emptied once, then a late write lands in the expired window
    june = await _seed_month(db_engine, table, 2026, 6, rows=10)
    await _seed_month(db_engine, table, 2026, 8)
    config = monthly_config(table, lifecycle=_truncating(KeepNewest(count=1)))
    await run_maintenance(db_engine, config, at_time=NOW)
    await exec_sql(db_engine, f"INSERT INTO \"{table}\" (created_at, payload) VALUES ('2026-06-20', 'late')")  # noqa: S608

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.truncated_count == 1
    assert await scalar(db_engine, f'SELECT count(*) FROM "{june}"') == 0  # noqa: S608


# sync-mirror: skip
async def test__expire_truncate__waiting_for_a_reader_of_the_partition__writers_to_the_others_go_through(
    db_engine: AsyncEngine, table: str
) -> None:
    """The truncate locks the expired partition and nothing above it.

    A reader of the expired partition makes the truncate wait for its lock. That wait is
    where a blocking detach would already hold the parent in ACCESS EXCLUSIVE, with every
    writer queued behind it; here the writers keep landing in the current month.
    """
    # Arrange: June expired with rows, August current, and a DEFAULT, so a detach would be the blocking form
    june = await _seed_month(db_engine, table, 2026, 6, rows=10)
    august = await _seed_month(db_engine, table, 2026, 8)
    await exec_sql(db_engine, f'CREATE TABLE "{table}_default" PARTITION OF "{table}" DEFAULT')
    config = monthly_config(table, lifecycle=_truncating(KeepNewest(count=1)))
    service = make_service(db_engine, ddl_lock_timeout_ms=30_000)
    plan = await service.plan(config, now=datetime(2026, 8, 26, tzinfo=UTC))
    assert [op.kind for op in plan.operations] == [OperationKind.TRUNCATE]
    insert = text(f"INSERT INTO \"{table}\" (created_at, payload) VALUES ('2026-08-20', 'live')")  # noqa: S608

    async with db_engine.connect() as reader:
        await reader.execute(text(f'SELECT count(*) FROM "{june}"'))  # noqa: S608
        applying = asyncio.create_task(service.apply(config, plan))
        for _ in range(200):
            await asyncio.sleep(0.05)
            waiting = await scalar(
                db_engine,
                "SELECT pid FROM pg_locks WHERE NOT granted AND relation = to_regclass(:name)",
                name=june,
            )
            if waiting is not None:
                break
        else:
            pytest.fail("the truncate never queued for the lock on June")
        held_on_parent = await scalar(
            db_engine,
            "SELECT count(*) FROM pg_locks WHERE pid = :pid AND relation = to_regclass(:name)",
            pid=waiting,
            name=table,
        )

        # Act: a live writer goes through the parent into August while the truncate waits
        async with db_engine.connect() as writer:
            await writer.execute(text("SET lock_timeout = '1s'"))
            for _ in range(10):
                await writer.execute(insert)
            await writer.commit()
        still_waiting = not applying.done()
        await reader.rollback()
    result = await applying

    # Assert
    assert held_on_parent == 0
    assert still_waiting
    assert result.truncated_count == 1
    assert await scalar(db_engine, f'SELECT count(*) FROM "{june}"') == 0  # noqa: S608
    assert await scalar(db_engine, f'SELECT count(*) FROM "{august}"') == 10  # noqa: S608


async def test__expire_truncate__switched_back_to_detach__the_emptied_partitions_are_retired(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: what replaces a later drop -- retention still says they expired
    june = await _seed_month(db_engine, table, 2026, 6, rows=10)
    await _seed_month(db_engine, table, 2026, 8)
    await run_maintenance(db_engine, monthly_config(table, lifecycle=_truncating(KeepNewest(count=1))), at_time=NOW)
    detaching = LifecyclePolicy(creation=CreateAhead(count=1), retention=KeepNewest(count=1), drop=DropAfter())

    # Act
    result = await run_maintenance(db_engine, monthly_config(table, lifecycle=detaching), at_time=NOW)

    # Assert
    assert (result.detached_count, result.dropped_count) == (1, 1)
    assert await relkind(db_engine, june) is None


# ── Orphans that come back ──────────────────────────────────────────────────────


async def test__orphan_reattach__window_in_the_create_ahead_set__reattached_not_recreated(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: September was created ahead, then detached through the library
    config = monthly_config(table, create_ahead=2, retention=12)
    await run_maintenance(db_engine, config, at_time=NOW)
    september = f"{table}__2026_09"
    oid_before = await relation_oid(db_engine, september)
    listed = await PostgresMetadataProvider(db_engine).list_partitions(table)
    with freezegun.freeze_time(NOW):
        await make_service(db_engine).detach_old_partitions(config, [p for p in listed if p.relname == september])
    assert await is_attached(db_engine, september) is False

    # Act
    result = await run_maintenance(db_engine, config, at_time=NOW)

    # Assert
    assert result.attached_count == 1
    assert result.created_count == 0
    assert result.maintenance_plan is not None
    assert [(op.target, op.reason) for op in result.maintenance_plan.attaches] == [
        (f"public.{september}", Reason.REATTACH)
    ]
    assert await is_attached(db_engine, september) is True
    assert await relation_oid(db_engine, september) == oid_before


async def test__orphan_reattach__ensure_partitions_naming_an_orphaned_window__reattaches_it(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: an expired, still-present June (a month of grace)
    config = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=30)))
    await run_maintenance(db_engine, config, at_time="2026-06-15")
    await run_maintenance(db_engine, config, at_time="2026-08-01")
    june = f"{table}__2026_06"
    oid_before = await relation_oid(db_engine, june)
    assert await is_attached(db_engine, june) is False

    # Act: a backfill names June explicitly
    created = await make_service(db_engine).ensure_partitions(config, [Period(year=2026, month=6)])

    # Assert: nothing new; the orphan is back with its data
    assert created == []
    assert await is_attached(db_engine, june) is True
    assert await relation_oid(db_engine, june) == oid_before


async def test__orphan_reattach__retention_grew__the_orphan_is_reattached_rather_than_kept_waiting(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: July expired under KeepNewest(1) and sits in its grace period
    narrow = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=30)))
    await run_maintenance(db_engine, narrow, at_time="2026-07-15")
    await run_maintenance(db_engine, narrow, at_time="2026-08-10")
    july = f"{table}__2026_07"
    assert await is_attached(db_engine, july) is False

    # Act: retention grows to three months, which wants July again
    wide = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1), retention=KeepNewest(count=3), drop=DropAfter(grace=timedelta(days=30))
        ),
    )
    result = await run_maintenance(db_engine, wide, at_time="2026-08-11")

    # Assert
    assert result.attached_count == 1
    assert result.created_count == 0
    assert await is_attached(db_engine, july) is True


async def test__orphan__not_wanted_again__waits_out_its_grace_then_drops(db_engine: AsyncEngine, table: str) -> None:
    # Arrange
    config = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=3)))
    await run_maintenance(db_engine, config, at_time="2026-06-15")
    await run_maintenance(db_engine, config, at_time="2026-08-01")
    june = f"{table}__2026_06"

    # Act
    pending = await run_maintenance(db_engine, config, at_time="2026-08-02")
    dropped = await run_maintenance(db_engine, config, at_time="2026-08-05")

    # Assert
    assert pending.attached_count == 0
    assert pending.dropped_count == 0
    assert dropped.dropped_count == 1
    assert await relkind(db_engine, june) is None


async def test__orphan_reattach__marker_removed_and_a_later_detach_starts_a_fresh_grace(
    db_engine: AsyncEngine, table: str
) -> None:
    # Arrange: July detached under KeepNewest(1) with a week of grace
    narrow = monthly_config(table, lifecycle=_grace_config(table, timedelta(days=7)))
    await run_maintenance(db_engine, narrow, at_time="2026-07-15")
    await run_maintenance(db_engine, narrow, at_time="2026-08-01")
    july = f"{table}__2026_07"
    assert (await table_comment(db_engine, july) or "").startswith(orphan_marker(f"public.{table}"))

    # Act 1: retention grows; the orphan comes back and stops being an orphan
    wide = monthly_config(
        table,
        lifecycle=LifecyclePolicy(
            creation=CreateAhead(count=1), retention=KeepNewest(count=3), drop=DropAfter(grace=timedelta(days=7))
        ),
    )
    await run_maintenance(db_engine, wide, at_time="2026-08-02")
    assert await is_attached(db_engine, july)
    assert await table_comment(db_engine, july) is None

    # Act 2: it expires again weeks later -- the grace runs from the *new* detach
    await run_maintenance(db_engine, narrow, at_time="2026-08-20")
    comment = await table_comment(db_engine, july) or ""
    assert f"{DETACHED_AT_MARKER}2026-08-20" in comment
    pending = await run_maintenance(db_engine, narrow, at_time="2026-08-21")
    assert pending.dropped_count == 0
    assert await relkind(db_engine, july) == "r"
    dropped = await run_maintenance(db_engine, narrow, at_time="2026-08-28")
    assert dropped.dropped_count == 1
