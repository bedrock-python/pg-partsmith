"""Unit tests for the aio ``PostgresPartitionRepository`` and its creator / remover helpers against a mocked engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.repositories.fk_manager import PartitionForeignKeyManager
from pg_partsmith.aio.repositories.resolver import PartitionRelationResolver
from pg_partsmith.entities import DefaultBounds, HashBounds, ListBounds, PartitionType, RangeBounds
from pg_partsmith.exceptions import (
    DropRetryExhaustedError,
    PartitionAlreadyExistsError,
    PartitionAttachedError,
    PartitionDetachInProgressError,
    PartitionNotFoundError,
    PlanStaleError,
    UnmanagedPartitionDropError,
)
from pg_partsmith.leaves import LocalLeaves
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.plan import PartitionBy
from pg_partsmith.utils import DETACHED_AT_MARKER, orphan_comment, orphan_table_comment

# ── a catalog the fake connection answers from ──────────────────────────────────


@dataclass
class _Catalog:
    """What the mocked PostgreSQL knows; a list is consumed one value per read."""

    exists: object = True
    attached_to: object = None
    comment: object = None
    oid: object = 4242
    fqn: str | None = "public.events"
    detach_pending: bool = False
    fk_constraints: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=lambda: ["created_at", "tenant_id", "data"])
    column_defs: list[tuple[str, str, bool]] = field(
        default_factory=lambda: [("ts", "timestamp with time zone", True), ("v", "double precision", False)]
    )
    privileges: list[tuple[str | None, str | None, str | None, bool]] = field(default_factory=list)
    relkind: str = "r"
    moved_rows: int | None = 0
    failures: dict[str, object] = field(default_factory=dict)


def _next(value: object) -> object:
    if isinstance(value, list):
        return value.pop(0) if value else None
    return value


def _scalar(value: object) -> MagicMock:
    result = MagicMock()
    result.scalar.return_value = value
    result.fetchall.return_value = []
    return result


def _rows(values: list[tuple[object, ...]]) -> MagicMock:
    result = MagicMock()
    result.fetchall.return_value = values
    return result


def _answer(catalog: _Catalog, sql: str) -> MagicMock:
    """Answer one statement from the catalog, raising a configured failure first."""
    for needle, failure in catalog.failures.items():
        if needle in sql:
            error = _next(failure)
            if error is not None:
                raise error  # type: ignore[misc]
    if "inhdetachpending" in sql:
        return _scalar(catalog.detach_pending)
    if "obj_description" in sql:
        return _scalar(_next(catalog.comment))
    if "relispartition = true" in sql:
        return _scalar(_next(catalog.attached_to))
    if "relkind IN ('r', 'p', 'f')" in sql:
        return _scalar(_next(catalog.exists))
    if "SELECT c.oid" in sql:
        return _scalar(_next(catalog.oid))
    if "ns.nspname || '.' || c.relname" in sql:
        return _scalar(catalog.fqn)
    if "contype = 'f'" in sql:
        return _rows([(name,) for name in catalog.fk_constraints])
    if "format_type(" in sql:
        return _rows(list(catalog.column_defs))
    if "pg_attribute a" in sql:
        return _rows([(column,) for column in catalog.columns])
    if "aclexplode" in sql:
        return _rows(list(catalog.privileges))
    if "SELECT c.relkind" in sql:
        return _scalar(catalog.relkind)
    if "WITH moved AS" in sql:
        result = MagicMock()
        result.rowcount = catalog.moved_rows
        return result
    return _scalar(None)


def _engine(catalog: _Catalog | None = None) -> tuple[MagicMock, AsyncMock]:
    """An engine whose single connection answers from ``catalog`` and records every statement."""
    catalog = catalog or _Catalog()
    conn = AsyncMock()
    conn.execute.side_effect = lambda stmt, params=None: _answer(catalog, str(stmt))
    conn.execution_options = AsyncMock(return_value=conn)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = cm
    engine.begin.return_value = cm
    return engine, conn


def _statements(conn: AsyncMock) -> list[str]:
    return [str(call.args[0]) for call in conn.execute.call_args_list]


def _sqlstate_error(sqlstate: str, message: str = "pg error") -> SQLAlchemyError:
    exc = SQLAlchemyError(message)
    orig = MagicMock()
    orig.sqlstate = sqlstate
    exc.orig = orig  # type: ignore[attr-defined]
    return exc


def _comment_statement(conn: AsyncMock) -> str:
    (statement,) = [s for s in _statements(conn) if s.startswith("COMMENT ON TABLE")]
    return statement


# ── constructor ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "kwargs,exc_type,match",
    [
        ({"ddl_timeout_seconds": 0}, ValueError, "ddl_timeout_seconds"),
        ({"ddl_timeout_seconds": -1}, ValueError, "ddl_timeout_seconds"),
        ({"ddl_timezone": ""}, ValueError, "ddl_timezone"),
        ({"ddl_timezone": "Europe/Moscow; DROP"}, ValueError, "ddl_timezone"),
        ({"marker_prefix": "bad prefix"}, ValueError, "marker_prefix"),
        ({"drop_lock_timeout_ms": -1}, ValueError, "drop_lock_timeout_ms"),
        ({"drop_lock_timeout_ms": "nope"}, TypeError, "drop_lock_timeout_ms"),
        ({"drop_lock_timeout_ms": True}, TypeError, "drop_lock_timeout_ms"),
        ({"drop_max_retries": 0}, ValueError, "drop_max_retries"),
        ({"drop_max_retries": 1.5}, TypeError, "drop_max_retries"),
        ({"drop_retry_delay": -0.1}, ValueError, "drop_retry_delay"),
        ({"drop_retry_delay": "slow"}, TypeError, "drop_retry_delay"),
        ({"drop_retry_delay": False}, TypeError, "drop_retry_delay"),
        ({"drop_max_backoff": -1}, ValueError, "drop_max_backoff"),
    ],
)
def test__constructor__invalid_argument__raises(
    kwargs: dict[str, object], exc_type: type[Exception], match: str
) -> None:
    # Arrange / Act / Assert
    with pytest.raises(exc_type, match=match):
        PostgresPartitionRepository(MagicMock(), **kwargs)  # type: ignore[arg-type]


def test__ddl_timezone__defaults_to_utc() -> None:
    # Arrange / Act / Assert
    assert PostgresPartitionRepository(MagicMock()).ddl_timezone == "UTC"


def test__ddl_timezone__is_stripped_and_kept() -> None:
    # Arrange / Act / Assert
    assert PostgresPartitionRepository(MagicMock(), ddl_timezone=" Europe/Moscow ").ddl_timezone == "Europe/Moscow"


def test__ddl_timezone__none_trusts_the_session() -> None:
    # Arrange / Act / Assert
    assert PostgresPartitionRepository(MagicMock(), ddl_timezone=None).ddl_timezone is None


# ── create_table_like ───────────────────────────────────────────────────────────


async def test__create_table_like__leaf__copies_the_template_without_its_identity() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_table_like("public.events", "public.events__2024_01", None)

    # Assert
    assert _statements(conn) == [
        'CREATE TABLE "public"."events__2024_01" (LIKE "public"."events" INCLUDING ALL EXCLUDING IDENTITY)'
    ]
    engine.begin.assert_called_once()


async def test__create_table_like__branch__partitions_by_every_quoted_column() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)
    partition_by = PartitionBy(method=PartitionType.HASH, columns=("tenant_id", "shard_id"))

    # Act
    await repo.create_table_like("events__2026_w35", "events__2026_w35__h0", partition_by)

    # Assert
    assert _statements(conn) == [
        'CREATE TABLE "events__2026_w35__h0" (LIKE "events__2026_w35" INCLUDING ALL EXCLUDING IDENTITY) '
        'PARTITION BY HASH ("tenant_id", "shard_id")'
    ]


async def test__create_table_like__range_branch__spells_the_method_in_upper_case() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_table_like(
        "events", "events__h0", PartitionBy(method=PartitionType.RANGE, columns=("created_at",))
    )

    # Assert
    assert _statements(conn)[0].endswith('PARTITION BY RANGE ("created_at")')


async def test__create_table_like__name_taken__raises_already_exists() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"CREATE TABLE": _sqlstate_error("42P07")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAlreadyExistsError) as excinfo:
        await repo.create_table_like("events", "events__2024_01", None)

    assert excinfo.value.partition_name == "events__2024_01"


async def test__create_table_like__other_database_error__propagates() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"CREATE TABLE": _sqlstate_error("42501", "permission denied")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="permission denied"):
        await repo.create_table_like("events", "events__2024_01", None)


async def test__create_table_like__transport_error__propagates() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"CREATE TABLE": OSError("connection reset")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(OSError, match="connection reset"):
        await repo.create_table_like("events", "events__2024_01", None)


# ── attach_partition ────────────────────────────────────────────────────────────


async def test__attach_partition__range_bounds__sets_the_ddl_timezone_then_attaches() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.attach_partition(
        "events", "events__2024_01", RangeBounds(from_value="2024-01-01", to_value="2024-02-01")
    )

    # Assert
    assert _statements(conn) == [
        "SET LOCAL TIME ZONE 'UTC'",
        "ALTER TABLE \"events\" ATTACH PARTITION \"events__2024_01\" FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
    ]


async def test__attach_partition__custom_ddl_timezone__is_the_one_set() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Moscow")

    # Act
    await repo.attach_partition(
        "events", "events__2024_01", RangeBounds(from_value="2024-01-01", to_value="2024-02-01")
    )

    # Assert
    assert _statements(conn)[0] == "SET LOCAL TIME ZONE 'Europe/Moscow'"


async def test__attach_partition__no_ddl_timezone__attaches_without_touching_the_session() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine, ddl_timezone=None)

    # Act
    await repo.attach_partition(
        "events", "events__2024_01", RangeBounds(from_value="2024-01-01", to_value="2024-02-01")
    )

    # Assert
    assert len(_statements(conn)) == 1
    assert _statements(conn)[0].startswith("ALTER TABLE")


async def test__attach_partition__composite_key__pads_every_trailing_column_with_minvalue() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.attach_partition(
        "events", "events__2026_w35", RangeBounds(from_value="2026-08-24", to_value="2026-08-31"), key_arity=3
    )

    # Assert -- one MINVALUE per trailing column, on both ends
    stmt = _statements(conn)[-1]
    assert "FROM ('2026-08-24', MINVALUE, MINVALUE)" in stmt
    assert "TO ('2026-08-31', MINVALUE, MINVALUE)" in stmt


@pytest.mark.parametrize(
    "bounds,expected",
    [
        (RangeBounds(from_value="MINVALUE", to_value="2024-02-01"), "FOR VALUES FROM (MINVALUE) TO ('2024-02-01')"),
        (RangeBounds(from_value="2024-01-01", to_value="maxvalue"), "FOR VALUES FROM ('2024-01-01') TO (MAXVALUE)"),
        (RangeBounds(from_value="o'clock", to_value="p'clock"), "FOR VALUES FROM ('o''clock') TO ('p''clock')"),
        (RangeBounds(from_value="{a}", to_value="[b]"), "FOR VALUES FROM ('{a}') TO ('[b]')"),
    ],
)
async def test__attach_partition__range_literals__unbounded_ends_are_keywords_and_values_are_quoted(
    bounds: RangeBounds, expected: str
) -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.attach_partition("events", "events__x", bounds)

    # Assert
    assert _statements(conn)[-1].endswith(expected)


async def test__attach_partition__hash_bounds__renders_modulus_then_remainder_without_a_timezone() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.attach_partition("events__2026_w35", "events__2026_w35__h1", HashBounds(modulus=4, remainder=1))

    # Assert
    assert _statements(conn) == [
        'ALTER TABLE "events__2026_w35" ATTACH PARTITION "events__2026_w35__h1" '
        "FOR VALUES WITH (MODULUS 4, REMAINDER 1)"
    ]


async def test__attach_partition__list_bounds__quotes_values_and_keeps_null_a_keyword() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.attach_partition("regions", "regions__eu", ListBounds(values=("de", "l'x"), includes_null=True))

    # Assert
    assert _statements(conn) == [
        "ALTER TABLE \"regions\" ATTACH PARTITION \"regions__eu\" FOR VALUES IN ('de', 'l''x', NULL)"
    ]


async def test__attach_partition__list_bounds_with_the_string_null__quotes_it() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.attach_partition("regions", "regions__x", ListBounds(values=("NULL",)))

    # Assert
    assert _statements(conn)[-1].endswith("FOR VALUES IN ('NULL')")


async def test__attach_partition__default_bounds__renders_default() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.attach_partition("regions", "regions__other", DefaultBounds())

    # Assert
    assert _statements(conn) == ['ALTER TABLE "regions" ATTACH PARTITION "regions__other" DEFAULT']


async def test__attach_partition__database_error__propagates() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"ATTACH PARTITION": _sqlstate_error("42P07", "already a partition")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="already a partition"):
        await repo.attach_partition("events", "events__x", HashBounds(modulus=2, remainder=0))


# ── reconcile_default_rows ──────────────────────────────────────────────────────


async def test__reconcile_default_rows__names_the_columns_on_both_sides_and_returns_the_count() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(moved_rows=42))
    repo = PostgresPartitionRepository(engine)

    # Act
    moved = await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        key_columns=("created_at",),
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    assert moved == 42
    statements = _statements(conn)
    assert statements[0] == "SET LOCAL TIME ZONE 'UTC'"
    assert statements[1] == 'LOCK TABLE "events_default" IN SHARE ROW EXCLUSIVE MODE'
    assert statements[2] == 'LOCK TABLE "events__2024_04" IN SHARE ROW EXCLUSIVE MODE'
    assert "pg_attribute" in statements[3]
    assert statements[4] == (
        'WITH moved AS (DELETE FROM "events_default" '
        "WHERE \"created_at\" >= '2024-04-01' AND \"created_at\" < '2024-05-01' "
        'RETURNING "created_at", "tenant_id", "data") '
        'INSERT INTO "events__2024_04" ("created_at", "tenant_id", "data") '
        'SELECT "created_at", "tenant_id", "data" FROM moved'
    )
    columns_lookup = next(c for c in conn.execute.call_args_list if "pg_attribute" in str(c.args[0]))
    assert columns_lookup.args[1] == {"table_name": '"events_default"'}


async def test__reconcile_default_rows__composite_key__leaves_rows_with_a_null_trailing_key_in_default() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(moved_rows=3))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        key_columns=("created_at", "tenant_id", "shard_id"),
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    move = _statements(conn)[-1]
    assert 'AND "created_at" < \'2024-05-01\' AND "tenant_id" IS NOT NULL AND "shard_id" IS NOT NULL RETURNING' in move


async def test__reconcile_default_rows__single_column_key__adds_no_null_test() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        key_columns=("created_at",),
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    assert "IS NOT NULL" not in _statements(conn)[-1]


async def test__reconcile_default_rows__empty_key__is_rejected_before_any_sql() -> None:
    # Arrange
    engine, _ = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(ValueError, match="partition key"):
        await repo.reconcile_default_rows(
            default_partition_name="events_default",
            target_partition_name="events__2024_04",
            key_columns=(),
            from_value="2024-04-01",
            to_value="2024-05-01",
        )

    engine.begin.assert_not_called()


async def test__reconcile_default_rows__relation_without_columns__is_reported_not_guessed() -> None:
    # Arrange -- to_regclass resolved nothing, so the column lookup is empty
    engine, conn = _engine(_Catalog(columns=[]))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionNotFoundError, match="no readable columns"):
        await repo.reconcile_default_rows(
            default_partition_name="events_default",
            target_partition_name="events__2024_04",
            key_columns=("created_at",),
            from_value="2024-04-01",
            to_value="2024-05-01",
        )

    assert not any("WITH moved AS" in s for s in _statements(conn))


async def test__reconcile_default_rows__no_ddl_timezone__skips_the_session_setting() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine, ddl_timezone=None)

    # Act
    await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        key_columns=("created_at",),
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    assert not any("TIME ZONE" in s for s in _statements(conn))
    assert _statements(conn)[0].startswith("LOCK TABLE")


async def test__reconcile_default_rows__unknown_rowcount__reads_as_zero() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(moved_rows=None))
    repo = PostgresPartitionRepository(engine)

    # Act
    moved = await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        key_columns=("created_at",),
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    assert moved == 0


# ── detach_partition ────────────────────────────────────────────────────────────


async def test__detach_partition__auto__marks_the_orphan_then_detaches_concurrently() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01")

    # Assert -- the pending pre-check and the concurrent detach each use an autocommit connection
    statements = _statements(conn)
    assert "inhdetachpending" in statements[0]
    assert statements[-1] == 'ALTER TABLE "events" DETACH PARTITION "events__2024_01" CONCURRENTLY'
    assert statements.index(_comment_statement(conn)) < len(statements) - 1
    assert engine.connect.call_count == 2
    engine.begin.assert_not_called()
    assert conn.execution_options.await_count == 2
    assert conn.execution_options.call_args.kwargs == {"isolation_level": "AUTOCOMMIT"}


async def test__detach_partition__marker__names_the_resolved_parent_and_the_detach_instant() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(fqn="public.events", comment="keep me"))
    repo = PostgresPartitionRepository(engine)
    before = datetime.now(UTC)

    # Act
    await repo.detach_partition("events", "events__2024_01")

    # Assert -- line 1 is the ownership marker, line 2 the instant, the user's own comment below them
    comment = _comment_statement(conn)
    assert comment.startswith('COMMENT ON TABLE "events__2024_01" IS \'')
    marker, stamped, rest = comment[len('COMMENT ON TABLE "events__2024_01" IS \'') : -1].split("\n")
    assert marker == orphan_table_comment("public.events")
    assert stamped.startswith(DETACHED_AT_MARKER)
    assert before <= datetime.fromisoformat(stamped[len(DETACHED_AT_MARKER) :]) <= datetime.now(UTC)
    assert rest == "keep me"


async def test__detach_partition__parent_cannot_be_resolved__marker_falls_back_to_the_given_name() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(fqn=None))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01")

    # Assert
    assert orphan_table_comment("events") in _comment_statement(conn)


async def test__detach_partition__already_marked_with_an_instant__keeps_it_and_writes_nothing() -> None:
    # Arrange -- a repeated detach must not restart the grace period
    existing = orphan_comment("public.events", detached_at=datetime(2024, 1, 1, tzinfo=UTC), existing_comment=None)
    engine, conn = _engine(_Catalog(comment=existing))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01")

    # Assert
    assert not any(s.startswith("COMMENT ON TABLE") for s in _statements(conn))


async def test__detach_partition__custom_marker_prefix__is_used_in_the_comment() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine, marker_prefix="myapp:parent=")

    # Act
    await repo.detach_partition("events", "events__2024_01")

    # Assert
    assert "'myapp:parent=public.events\n" in _comment_statement(conn)


async def test__detach_partition__non_ascii_existing_comment__does_not_raise() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment="существующий комментарий".encode()))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01")

    # Assert
    assert "существующий комментарий" in _comment_statement(conn)


@pytest.mark.parametrize("sqlstate", ["0A000", "42601", "55000"])
async def test__detach_partition__auto_and_concurrent_refused__falls_back_to_the_blocking_form(sqlstate: str) -> None:
    # Arrange
    engine, conn = _engine(_Catalog(failures={"CONCURRENTLY": _sqlstate_error(sqlstate)}))
    repo = PostgresPartitionRepository(engine)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.aio.repositories.remover.logger", logger):
        await repo.detach_partition("events", "events__2024_01", mode=DetachMode.AUTO)

    # Assert
    assert _statements(conn)[-1] == 'ALTER TABLE "events" DETACH PARTITION "events__2024_01"'
    assert engine.connect.call_count == 2
    assert engine.begin.call_count == 1
    logger.warning.assert_called_once()
    assert logger.warning.call_args.kwargs["extra"]["sqlstate"] == sqlstate


async def test__detach_partition__auto_and_unrelated_error__propagates_without_a_fallback() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"CONCURRENTLY": _sqlstate_error("42501", "permission denied")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="permission denied"):
        await repo.detach_partition("events", "events__2024_01")

    engine.begin.assert_not_called()


@pytest.mark.parametrize("sqlstate", ["0A000", "55000"])
async def test__detach_partition__concurrent_mode_refused__propagates_without_a_fallback(sqlstate: str) -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"CONCURRENTLY": _sqlstate_error(sqlstate, "not possible")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="not possible"):
        await repo.detach_partition("events", "events__2024_01", mode=DetachMode.CONCURRENT)

    engine.begin.assert_not_called()


async def test__detach_partition__concurrent_mode__succeeds_with_the_concurrent_form() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01", mode=DetachMode.CONCURRENT)

    # Assert
    assert _statements(conn)[-1].endswith("CONCURRENTLY")
    engine.begin.assert_not_called()


async def test__detach_partition__blocking_mode__runs_only_the_plain_form_in_a_transaction() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01", mode=DetachMode.BLOCKING)

    # Assert
    statements = _statements(conn)
    assert statements[-1] == 'ALTER TABLE "events" DETACH PARTITION "events__2024_01"'
    assert not any("CONCURRENTLY" in s for s in statements)
    assert statements.index(_comment_statement(conn)) < len(statements) - 1
    assert engine.connect.call_count == 1
    assert engine.begin.call_count == 1


@pytest.mark.parametrize("mode", [DetachMode.AUTO, DetachMode.BLOCKING])
async def test__detach_partition__table_missing__raises_partition_not_found(mode: DetachMode) -> None:
    # Arrange -- the first marker query fails with undefined_table
    engine, _ = _engine(_Catalog(failures={"obj_description": _sqlstate_error("42P01")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionNotFoundError) as excinfo:
        await repo.detach_partition("events", "events__2024_01", mode=mode)

    assert excinfo.value.partition_name == "events__2024_01"


@pytest.mark.parametrize("mode", [DetachMode.AUTO, DetachMode.CONCURRENT, DetachMode.BLOCKING])
async def test__detach_partition__another_detach_pending__raises_detach_in_progress(mode: DetachMode) -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"DETACH PARTITION": _sqlstate_error("55006")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionDetachInProgressError):
        await repo.detach_partition("events", "events__2024_01", mode=mode)


async def test__detach_partition__blocking_and_unrelated_error__propagates() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"DETACH PARTITION": _sqlstate_error("42501", "permission denied")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="permission denied"):
        await repo.detach_partition("events", "events__2024_01", mode=DetachMode.BLOCKING)


async def test__detach_partition__marker_write_fails__aborts_before_the_detach() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(failures={"COMMENT ON TABLE": RuntimeError("cannot comment table")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(RuntimeError, match="cannot comment table"):
        await repo.detach_partition("events", "events__2024_01")

    assert not any("DETACH" in s for s in _statements(conn))


async def test__detach_partition__pending_detach__is_finalized_instead() -> None:
    # Arrange -- a cancelled DETACH CONCURRENTLY left the partition half detached
    engine, conn = _engine(_Catalog(detach_pending=True))
    repo = PostgresPartitionRepository(engine)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.aio.repositories.remover.logger", logger):
        await repo.detach_partition("events", "events__2024_01")

    # Assert
    statements = _statements(conn)
    assert statements[-1] == 'ALTER TABLE "events" DETACH PARTITION "events__2024_01" FINALIZE'
    assert any(s.startswith("COMMENT ON TABLE") for s in statements)
    assert not any("CONCURRENTLY" in s for s in statements)
    assert engine.connect.call_count == 1
    engine.begin.assert_not_called()
    logger.warning.assert_called_once()


# ── adopt_partition ─────────────────────────────────────────────────────────────


async def test__adopt_partition__detached_table__writes_the_marker_without_a_detach_instant() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment="legacy note"))
    repo = PostgresPartitionRepository(engine)

    # Act
    adopted = await repo.adopt_partition("events", "events__2023_01")

    # Assert
    assert adopted is True
    engine.begin.assert_called_once()
    engine.connect.assert_not_called()
    comment = _comment_statement(conn)
    assert comment == f"COMMENT ON TABLE \"events__2023_01\" IS '{orphan_table_comment('public.events')}\nlegacy note'"
    assert DETACHED_AT_MARKER not in comment


async def test__adopt_partition__table_missing__returns_false_without_a_comment() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(exists=False))
    repo = PostgresPartitionRepository(engine)

    # Act
    adopted = await repo.adopt_partition("events", "events__2023_01")

    # Assert
    assert adopted is False
    assert not any(s.startswith("COMMENT ON TABLE") for s in _statements(conn))


async def test__adopt_partition__still_attached__raises_without_a_comment() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(attached_to="public.events"))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAttachedError) as excinfo:
        await repo.adopt_partition("events", "events__2023_01")

    assert excinfo.value.table_name == "public.events"
    assert not any(s.startswith("COMMENT ON TABLE") for s in _statements(conn))


async def test__adopt_partition__marker_already_present__returns_true_without_rewriting() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment=orphan_table_comment("public.events")))
    repo = PostgresPartitionRepository(engine)

    # Act
    adopted = await repo.adopt_partition("events", "events__2023_01")

    # Assert
    assert adopted is True
    assert not any(s.startswith("COMMENT ON TABLE") for s in _statements(conn))


# ── drop_partition ──────────────────────────────────────────────────────────────


async def test__drop_partition__missing_table__is_a_no_op() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(exists=False))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_not_called()


async def test__drop_partition__marked_orphan__is_dropped_under_an_exclusive_lock() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment=orphan_table_comment("public.events")))
    repo = PostgresPartitionRepository(engine, drop_lock_timeout_ms=1500)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert -- the lock precedes the revalidation, which precedes the drop
    engine.begin.assert_called_once()
    statements = _statements(conn)
    lock_timeout = next(i for i, s in enumerate(statements) if "set_config('lock_timeout'" in s)
    lock = next(i for i, s in enumerate(statements) if s == 'LOCK TABLE "events__2024_01" IN ACCESS EXCLUSIVE MODE')
    revalidate = next(i for i, s in enumerate(statements) if i > lock and "relispartition" in s)
    marker = next(i for i, s in enumerate(statements) if i > lock and "obj_description" in s)
    drop = statements.index('DROP TABLE IF EXISTS "events__2024_01"')
    assert lock_timeout < lock < revalidate < marker < drop
    lock_timeout_call = next(c for c in conn.execute.call_args_list if "set_config('lock_timeout'" in str(c.args[0]))
    assert lock_timeout_call.args[1] == {"timeout": "1500"}


async def test__drop_partition__still_attached__raises_without_a_transaction() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(attached_to="public.events"))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAttachedError):
        await repo.drop_partition("events__2024_01")

    engine.begin.assert_not_called()


async def test__drop_partition__no_marker__refuses_an_unmanaged_table() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(comment=None))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(UnmanagedPartitionDropError, match="drop_allow_unmanaged"):
        await repo.drop_partition("events__2024_01")

    engine.begin.assert_not_called()


async def test__drop_partition__foreign_comment__refuses_an_unmanaged_table() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(comment="somebody else's table"))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(UnmanagedPartitionDropError):
        await repo.drop_partition("events__2024_01")


async def test__drop_partition__unmanaged_with_opt_in__is_dropped_without_reading_the_comment() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment=None))
    repo = PostgresPartitionRepository(engine, drop_allow_unmanaged=True)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    assert 'DROP TABLE IF EXISTS "events__2024_01"' in _statements(conn)
    assert not any("obj_description" in s for s in _statements(conn))


async def test__drop_partition__vanished_between_checks__is_a_no_op() -> None:
    # Arrange -- exists at first, no comment, gone by the race check
    engine, _ = _engine(_Catalog(exists=[True, False], comment=None))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_not_called()


async def test__drop_partition__expected_oid_matches__is_dropped() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(oid=4242, comment=orphan_table_comment("public.events")))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01", expected_oid=4242)

    # Assert
    assert 'DROP TABLE IF EXISTS "events__2024_01"' in _statements(conn)
    assert len([s for s in _statements(conn) if "SELECT c.oid" in s]) == 2


async def test__drop_partition__expected_oid_differs__is_stale_before_any_transaction() -> None:
    # Arrange -- the name now belongs to a recreated relation
    engine, _ = _engine(_Catalog(oid=9999, comment=orphan_table_comment("public.events")))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PlanStaleError, match="OID 9999") as excinfo:
        await repo.drop_partition("events__2024_01", expected_oid=4242)

    assert excinfo.value.partition_name == "events__2024_01"
    engine.begin.assert_not_called()


async def test__drop_partition__expected_oid_differs_under_the_lock__is_stale_without_a_drop() -> None:
    # Arrange -- replaced between the pre-check and the lock
    engine, conn = _engine(_Catalog(oid=[4242, 9999], comment=orphan_table_comment("public.events")))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PlanStaleError):
        await repo.drop_partition("events__2024_01", expected_oid=4242)

    assert not any("DROP TABLE" in s for s in _statements(conn))


async def test__drop_partition__relation_gone_when_the_oid_is_read__proceeds() -> None:
    # Arrange -- nothing holds the name, so nothing can be the wrong relation
    engine, conn = _engine(_Catalog(oid=None, comment=orphan_table_comment("public.events")))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01", expected_oid=4242)

    # Assert
    assert 'DROP TABLE IF EXISTS "events__2024_01"' in _statements(conn)


async def test__drop_partition__no_expected_oid__never_reads_the_oid() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment=orphan_table_comment("public.events")))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    assert not any("SELECT c.oid" in s for s in _statements(conn))


async def test__drop_partition__foreign_keys__are_dropped_in_one_statement_before_the_table() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment=orphan_table_comment("public.events"), fk_constraints=["fk_a", "fk_b"]))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    statements = _statements(conn)
    constraint_drop = statements.index(
        'ALTER TABLE "events__2024_01" DROP CONSTRAINT IF EXISTS "fk_a", DROP CONSTRAINT IF EXISTS "fk_b"'
    )
    assert constraint_drop < statements.index('DROP TABLE IF EXISTS "events__2024_01"')


async def test__drop_partition__lock_hits_undefined_table__returns_without_a_drop() -> None:
    # Arrange -- the table vanished between the pre-check and the LOCK TABLE statement
    engine, conn = _engine(
        _Catalog(comment=orphan_table_comment("public.events"), failures={"LOCK TABLE": _sqlstate_error("42P01")})
    )
    repo = PostgresPartitionRepository(engine)

    # Act -- treated as already done
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()
    assert not any("DROP TABLE" in s for s in _statements(conn))


async def test__drop_partition__reattached_after_the_lock__raises_without_a_drop() -> None:
    # Arrange -- detached at the pre-check, attached again under the lock
    engine, conn = _engine(_Catalog(attached_to=[None, "public.events"], comment=orphan_table_comment("public.events")))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAttachedError):
        await repo.drop_partition("events__2024_01")

    assert not any("DROP TABLE" in s for s in _statements(conn))


async def test__drop_partition__marker_gone_after_the_lock__raises_without_a_drop() -> None:
    # Arrange -- the comment was removed (table replaced) between the pre-check and the lock
    engine, conn = _engine(_Catalog(comment=[orphan_table_comment("public.events"), None], exists=[True, True]))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(UnmanagedPartitionDropError):
        await repo.drop_partition("events__2024_01")

    assert not any("DROP TABLE" in s for s in _statements(conn))


async def test__drop_partition__vanished_after_the_lock__returns_without_a_drop() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment=[orphan_table_comment("public.events"), None], exists=[True, False]))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    assert not any("DROP TABLE" in s for s in _statements(conn))


@pytest.mark.parametrize("sqlstate", ["40P01", "55P03", "57014"])
async def test__drop_partition__transient_lock_error__is_retried_after_a_backoff(sqlstate: str) -> None:
    # Arrange
    engine, conn = _engine(
        _Catalog(comment=orphan_table_comment("public.events"), failures={"LOCK TABLE": [_sqlstate_error(sqlstate)]})
    )
    repo = PostgresPartitionRepository(engine, drop_retry_delay=0.5)
    sleep = AsyncMock()
    logger = MagicMock()

    # Act
    with (
        patch("pg_partsmith.aio.repositories.remover.asyncio.sleep", sleep),
        patch("pg_partsmith.aio.repositories.remover.logger", logger),
    ):
        await repo.drop_partition("events__2024_01")

    # Assert
    assert engine.begin.call_count == 2
    assert 'DROP TABLE IF EXISTS "events__2024_01"' in _statements(conn)
    sleep.assert_awaited_once_with(0.5)
    logger.warning.assert_called_once()
    assert logger.warning.call_args.kwargs["extra"]["attempt"] == 2


async def test__drop_partition__transport_error__is_retried_too() -> None:
    # Arrange
    engine, _ = _engine(
        _Catalog(comment=orphan_table_comment("public.events"), failures={"LOCK TABLE": [OSError("reset")]})
    )
    repo = PostgresPartitionRepository(engine, drop_retry_delay=0)

    # Act
    with patch("pg_partsmith.aio.repositories.remover.asyncio.sleep", AsyncMock()):
        await repo.drop_partition("events__2024_01")

    # Assert
    assert engine.begin.call_count == 2


async def test__drop_partition__backoff__doubles_and_is_capped() -> None:
    # Arrange
    deadlock = _sqlstate_error("40P01")
    engine, _ = _engine(
        _Catalog(
            comment=orphan_table_comment("public.events"),
            failures={"LOCK TABLE": [deadlock, deadlock, deadlock]},
        )
    )
    repo = PostgresPartitionRepository(engine, drop_max_retries=4, drop_retry_delay=1.0, drop_max_backoff=3.0)
    sleep = AsyncMock()

    # Act
    with patch("pg_partsmith.aio.repositories.remover.asyncio.sleep", sleep):
        await repo.drop_partition("events__2024_01")

    # Assert
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 2.0, 3.0]


async def test__drop_partition__retries_exhausted__raises_with_the_last_cause() -> None:
    # Arrange
    deadlock = _sqlstate_error("40P01", "deadlock detected")
    engine, _ = _engine(_Catalog(comment=orphan_table_comment("public.events"), failures={"LOCK TABLE": deadlock}))
    repo = PostgresPartitionRepository(engine, drop_max_retries=3, drop_retry_delay=0)

    # Act / Assert
    with (
        patch("pg_partsmith.aio.repositories.remover.asyncio.sleep", AsyncMock()),
        pytest.raises(DropRetryExhaustedError, match="after 3 attempt") as excinfo,
    ):
        await repo.drop_partition("events__2024_01")

    assert excinfo.value.partition_name == "events__2024_01"
    assert excinfo.value.attempts == 3
    assert excinfo.value.cause is deadlock
    assert excinfo.value.__cause__ is deadlock
    assert engine.begin.call_count == 3


async def test__drop_partition__non_retryable_error__fails_at_once() -> None:
    # Arrange
    engine, _ = _engine(
        _Catalog(
            comment=orphan_table_comment("public.events"), failures={"DROP TABLE": _sqlstate_error("42501", "denied")}
        )
    )
    repo = PostgresPartitionRepository(engine, drop_max_retries=3, drop_retry_delay=0)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="denied"):
        await repo.drop_partition("events__2024_01")

    assert engine.begin.call_count == 1


# ── helpers: fk_manager and resolver ────────────────────────────────────────────


async def test__fk_manager__list_constraints_conn__returns_names() -> None:
    # Arrange
    conn = AsyncMock()
    result = MagicMock()
    result.fetchall.return_value = [("fk_a",), ("fk_b",)]
    conn.execute.return_value = result

    # Act
    names = await PartitionForeignKeyManager.list_constraints_conn(conn, "events__2024_01")

    # Assert
    assert names == ["fk_a", "fk_b"]
    assert conn.execute.call_args.args[1] == {"partition_name": '"events__2024_01"'}


async def test__fk_manager__drop_constraints__no_names__issues_nothing() -> None:
    # Arrange
    conn = AsyncMock()

    # Act
    await PartitionForeignKeyManager.drop_constraints(conn, "events__2024_01", [])

    # Assert
    conn.execute.assert_not_awaited()


async def test__resolver__exists_and_is_attached__open_their_own_connections() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(exists=True, attached_to="public.events"))
    resolver = PartitionRelationResolver(engine)

    # Act
    exists = await resolver.exists("events__2024_01")
    attached = await resolver.is_attached("events", "events__2024_01")

    # Assert
    assert exists is True
    assert attached is True
    assert engine.connect.call_count == 2


async def test__resolver__resolve_fqn_conn__returns_none_for_an_unknown_relation() -> None:
    # Arrange
    _, conn = _engine(_Catalog(fqn=None))

    # Act / Assert
    assert await PartitionRelationResolver.resolve_fqn_conn(conn, "nothing") is None


# ── leaf backends: local physical settings ──────────────────────────────────────


async def test__create_table_like__storage_parameters_and_tablespace__spelled_as_literals() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)
    physical = LocalLeaves(tablespace="fast_ssd", storage_parameters={"fillfactor": 70, "autovacuum_enabled": False})

    # Act
    await repo.create_table_like("events", "events__2024_01", None, physical=physical)

    # Assert
    assert _statements(conn) == [
        'CREATE TABLE "events__2024_01" (LIKE "events" INCLUDING ALL EXCLUDING IDENTITY) '
        "WITH (fillfactor = '70', autovacuum_enabled = 'false') TABLESPACE \"fast_ssd\""
    ]


async def test__create_table_like__branch__takes_the_tablespace_but_no_storage_parameters() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)
    physical = LocalLeaves(tablespace="fast_ssd", storage_parameters={"fillfactor": 70})

    # Act
    await repo.create_table_like(
        "events", "events__2024_01", PartitionBy(method=PartitionType.HASH, columns=("tenant_id",)), physical=physical
    )

    # Assert
    assert _statements(conn) == [
        'CREATE TABLE "events__2024_01" (LIKE "events" INCLUDING ALL EXCLUDING IDENTITY) '
        'PARTITION BY HASH ("tenant_id") TABLESPACE "fast_ssd"'
    ]


async def test__create_table_like__plain_physical__adds_nothing() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_table_like("events", "events__2024_01", None, physical=LocalLeaves())

    # Assert
    assert _statements(conn) == ['CREATE TABLE "events__2024_01" (LIKE "events" INCLUDING ALL EXCLUDING IDENTITY)']


async def test__create_table_like__inherit_privileges__replays_owner_and_grants_in_the_same_transaction() -> None:
    # Arrange
    privileges = [
        ("app", "app", "SELECT", False),
        ("app", "app", "INSERT", False),
        ("app", "reader", "SELECT", False),
        ("app", "PUBLIC", "SELECT", False),
        ("app", '"odd role"', "UPDATE", True),
    ]
    engine, conn = _engine(_Catalog(privileges=privileges))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_table_like(
        "public.events", "public.events__2024_01", None, physical=LocalLeaves(inherit_privileges=True)
    )

    # Assert
    statements = _statements(conn)
    assert statements[0].startswith('CREATE TABLE "public"."events__2024_01"')
    assert 'ALTER TABLE "public"."events__2024_01" OWNER TO "app"' in statements
    assert 'GRANT SELECT, INSERT ON TABLE "public"."events__2024_01" TO app' in statements
    assert 'GRANT SELECT ON TABLE "public"."events__2024_01" TO reader' in statements
    assert 'GRANT SELECT ON TABLE "public"."events__2024_01" TO PUBLIC' in statements
    assert 'GRANT UPDATE ON TABLE "public"."events__2024_01" TO "odd role" WITH GRANT OPTION' in statements
    engine.begin.assert_called_once()


async def test__create_table_like__inherit_privileges__empty_acl__sets_the_owner_only() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(privileges=[("app", None, None, False)]))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_table_like("events", "events__2024_01", None, physical=LocalLeaves(inherit_privileges=True))

    # Assert
    assert [s for s in _statements(conn) if s.startswith(("GRANT", "ALTER"))] == [
        'ALTER TABLE "events__2024_01" OWNER TO "app"'
    ]


async def test__create_table_like__inherit_privileges__unknown_privilege_word__is_not_spliced() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(privileges=[("app", "reader", "SELECT; DROP", False)]))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_table_like("events", "events__2024_01", None, physical=LocalLeaves(inherit_privileges=True))

    # Assert
    assert not [s for s in _statements(conn) if s.startswith("GRANT")]


# ── leaf backends: foreign tables ───────────────────────────────────────────────


async def test__create_foreign_table_like__spells_columns_server_and_options() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_foreign_table_like(
        "public.metrics",
        "public.metrics__2026_01",
        server="archive",
        options={"table_name": "metrics__2026_01", "schema_name": "cold"},
    )

    # Assert
    assert _statements(conn)[-1] == (
        'CREATE FOREIGN TABLE "public"."metrics__2026_01" '
        '("ts" timestamp with time zone NOT NULL, "v" double precision) '
        "SERVER \"archive\" OPTIONS (table_name 'metrics__2026_01', schema_name 'cold')"
    )
    engine.begin.assert_called_once()


async def test__create_foreign_table_like__no_options__omits_the_clause() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_foreign_table_like("metrics", "metrics__2026_01", server="archive", options={})

    # Assert
    assert _statements(conn)[-1].endswith('SERVER "archive"')


async def test__create_foreign_table_like__option_value_is_a_literal__quotes_are_escaped() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.create_foreign_table_like(
        "metrics", "metrics__2026_01", server="archive", options={"table_name": "it's"}
    )

    # Assert
    assert "OPTIONS (table_name 'it''s')" in _statements(conn)[-1]


async def test__create_foreign_table_like__template_without_columns__raises_not_found() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(column_defs=[]))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionNotFoundError, match="no readable columns"):
        await repo.create_foreign_table_like("metrics", "metrics__2026_01", server="archive", options={})


async def test__create_foreign_table_like__name_taken__raises_already_exists() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(failures={"CREATE FOREIGN TABLE": _sqlstate_error("42P07")}))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAlreadyExistsError):
        await repo.create_foreign_table_like("metrics", "metrics__2026_01", server="archive", options={})


async def test__detach_partition__foreign_table__marker_is_written_with_comment_on_foreign_table() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(relkind="f"))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("public.metrics", "public.metrics__2026_01", mode=DetachMode.BLOCKING)

    # Assert
    (comment,) = [s for s in _statements(conn) if s.startswith("COMMENT ON")]
    assert comment.startswith('COMMENT ON FOREIGN TABLE "public"."metrics__2026_01" IS')


async def test__drop_partition__foreign_table__uses_drop_foreign_table_and_skips_constraints() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(comment=orphan_table_comment("public.metrics"), relkind="f", fk_constraints=["fk"]))
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("metrics__2026_01")

    # Assert
    statements = _statements(conn)
    assert 'DROP FOREIGN TABLE IF EXISTS "metrics__2026_01"' in statements
    assert not [s for s in statements if s.startswith("DROP TABLE") or "DROP CONSTRAINT" in s]


# ── batched moves ───────────────────────────────────────────────────────────────


async def test__reconcile_default_rows__with_a_limit__picks_a_batch_by_tableoid_and_ctid() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(moved_rows=10))
    repo = PostgresPartitionRepository(engine)

    # Act
    moved = await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_01",
        key_columns=("created_at",),
        from_value="2024-01-01",
        to_value="2024-02-01",
        limit=10,
    )

    # Assert
    assert moved == 10
    (statement,) = [s for s in _statements(conn) if s.startswith("WITH moved AS")]
    assert statement == (
        'WITH moved AS (DELETE FROM "events_default" WHERE (tableoid, ctid) IN ('
        'SELECT tableoid, ctid FROM "events_default" '
        "WHERE \"created_at\" >= '2024-01-01' AND \"created_at\" < '2024-02-01' LIMIT 10) "
        'RETURNING "created_at", "tenant_id", "data") '
        'INSERT INTO "events__2024_01" ("created_at", "tenant_id", "data") '
        'SELECT "created_at", "tenant_id", "data" FROM moved'
    )


async def test__reconcile_default_rows__without_a_limit__moves_the_whole_window_as_before() -> None:
    # Arrange
    engine, conn = _engine()
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.reconcile_default_rows(
        default_partition_name="d", target_partition_name="t", key_columns=("k",), from_value="1", to_value="2"
    )

    # Assert
    (statement,) = [s for s in _statements(conn) if s.startswith("WITH moved AS")]
    assert "LIMIT" not in statement
    assert statement.startswith('WITH moved AS (DELETE FROM "d" WHERE "k" >= \'1\' AND "k" < \'2\' RETURNING')


async def test__move_rows__locks_both_sides_and_moves_a_batch_of_any_rows() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(moved_rows=4))
    repo = PostgresPartitionRepository(engine)

    # Act
    moved = await repo.move_rows("public.events__2024_01", "public.events_flat", limit=4)

    # Assert
    assert moved == 4
    statements = _statements(conn)
    assert statements[0] == 'LOCK TABLE "public"."events__2024_01" IN SHARE ROW EXCLUSIVE MODE'
    assert statements[1] == 'LOCK TABLE "public"."events_flat" IN SHARE ROW EXCLUSIVE MODE'
    assert statements[-1] == (
        'WITH moved AS (DELETE FROM "public"."events__2024_01" WHERE (tableoid, ctid) IN ('
        'SELECT tableoid, ctid FROM "public"."events__2024_01" LIMIT 4) '
        'RETURNING "created_at", "tenant_id", "data") '
        'INSERT INTO "public"."events_flat" ("created_at", "tenant_id", "data") '
        'SELECT "created_at", "tenant_id", "data" FROM moved'
    )
    engine.begin.assert_called_once()


async def test__move_rows__no_limit__moves_everything() -> None:
    # Arrange
    engine, conn = _engine(_Catalog(moved_rows=99))
    repo = PostgresPartitionRepository(engine)

    # Act
    moved = await repo.move_rows("a", "b")

    # Assert
    assert moved == 99
    assert _statements(conn)[-1].startswith('WITH moved AS (DELETE FROM "a" RETURNING')


async def test__move_rows__source_without_columns__raises_not_found() -> None:
    # Arrange
    engine, _ = _engine(_Catalog(columns=[]))
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionNotFoundError):
        await repo.move_rows("gone", "b")
