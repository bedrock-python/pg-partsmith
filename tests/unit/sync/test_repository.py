from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import (
    DropRetryExhaustedError,
    PartitionAlreadyExistsError,
    PartitionAttachedError,
    PartitionDetachInProgressError,
    PartitionNotFoundError,
    UnmanagedPartitionDropError,
)
from pg_partsmith.sync.repositories import PostgresPartitionRepository
from pg_partsmith.utils import orphan_table_comment, pg_sqlstate

# ── helpers ─────────────────────────────────────────────────────────────────────
#
# Unlike the async repository, the sync repository enforces ``ddl_timeout_seconds``
# server-side via extra ``set_config('statement_timeout', ...)`` executes:
#   - every ``engine.begin()`` block and the drop read/FK-listing connections start
#     with one extra ``set_config`` execute;
#   - the autocommit DETACH CONCURRENTLY path wraps in a session-level ``set_config``
#     plus a trailing ``RESET statement_timeout`` execute;
#   - every ``detach`` starts with a pending-detach pre-check on its own autocommit
#     connection: ``set_config``, SELECT inhdetachpending, ``RESET`` (3 executes when
#     not pending).
# Result sequences and call-count assertions below account for those extra calls.


@pytest.fixture
def config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )


def _make_engine(sequence: list[object] | None = None) -> MagicMock:
    """Build an engine mock consuming *sequence* values in order.

    - A list value → result.fetchall.return_value
    - Any other value → result.scalar.return_value
    One conn mock is shared between engine.connect() and engine.begin().
    """
    engine = MagicMock()
    conn = MagicMock()

    results = []
    for value in sequence or []:
        r = MagicMock()
        if isinstance(value, list):
            r.fetchall.return_value = value
        else:
            r.scalar.return_value = value
        results.append(r)

    if results:
        conn.execute.side_effect = results
    else:
        default_result = MagicMock()
        default_result.scalar.return_value = None
        default_result.fetchall.return_value = []
        conn.execute.return_value = default_result

    conn.execution_options = MagicMock(return_value=conn)

    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm

    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=conn)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm

    return engine


def _make_retryable_exc(sqlstate: str) -> SQLAlchemyError:
    orig = MagicMock()
    orig.sqlstate = sqlstate
    exc = SQLAlchemyError("pg error")
    exc.orig = orig  # type: ignore[attr-defined]
    return exc


def _make_retry_engine(fail_with: Exception, fail_attempts: int) -> MagicMock:
    """Engine where connect() always reads OK, begin() fails *fail_attempts* times then succeeds."""
    engine = MagicMock()
    conn_read = MagicMock()

    def _r(v: object) -> MagicMock:
        r = MagicMock()
        if isinstance(v, list):
            r.fetchall.return_value = v
        else:
            r.scalar.return_value = v
        return r

    # read conn: set_config, exists, not attached, marker; FK conn: set_config, FK list
    conn_read.execute.side_effect = [
        _r(None),
        _r(True),
        _r(None),
        _r(orphan_table_comment("events")),
        _r(None),
        _r([]),
    ]

    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=conn_read)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm

    def _make_begin_cm(*, should_fail: bool) -> MagicMock:
        ddl_conn = MagicMock()
        if should_fail:
            ddl_conn.execute.side_effect = fail_with
        begin_cm = MagicMock()
        begin_cm.__enter__ = MagicMock(return_value=ddl_conn)
        begin_cm.__exit__ = MagicMock(return_value=False)
        return begin_cm

    begin_cms = [_make_begin_cm(should_fail=True) for _ in range(fail_attempts)]
    begin_cms.append(_make_begin_cm(should_fail=False))
    engine.begin.side_effect = begin_cms

    return engine


# ── constructor validation ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "kwargs,exc_type,match",
    [
        ({"drop_lock_timeout_ms": -1}, ValueError, "drop_lock_timeout_ms"),
        ({"drop_lock_timeout_ms": "nope"}, TypeError, "drop_lock_timeout_ms"),
        ({"drop_lock_timeout_ms": True}, TypeError, "drop_lock_timeout_ms"),
        ({"drop_max_retries": 0}, ValueError, "drop_max_retries"),
        ({"drop_max_retries": 1.5}, TypeError, "drop_max_retries"),
        ({"drop_retry_delay": -0.1}, ValueError, "drop_retry_delay"),
        ({"drop_retry_delay": "slow"}, TypeError, "drop_retry_delay"),
        ({"drop_retry_delay": False}, TypeError, "drop_retry_delay"),
    ],
)
def test__repository__invalid_constructor_argument__raises_correct_exception(
    kwargs: dict, exc_type: type[Exception], match: str
) -> None:
    # Arrange / Act / Assert
    with pytest.raises(exc_type, match=match):
        PostgresPartitionRepository(MagicMock(), **kwargs)  # type: ignore[arg-type]


# ── partition_exists ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("exists", [True, False])
def test__repository__partition_exists__returns_correct_bool(exists: bool) -> None:
    # Arrange
    engine = _make_engine([exists])
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    assert repo.partition_exists("events__2024_01") is exists


def test__repository__partition_exists__uses_quoted_regclass_argument() -> None:
    # Arrange
    engine = _make_engine([True])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.partition_exists("events__2024_W12")

    # Assert
    conn = engine.connect.return_value.__enter__.return_value
    params = conn.execute.call_args.args[1]
    assert params["partition_name"] == '"events__2024_W12"'


# ── create_partition ────────────────────────────────────────────────────────────


def test__repository__create_partition__new_partition__returns_partition_info(
    config: TablePartitionConfig,
) -> None:
    # Arrange — set_config timeout; CREATE TABLE → success
    engine = _make_engine([None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    info = repo.create_partition(config, "events__2024_01", "2024-01-01", "2024-02-01")

    # Assert
    assert info.name == "events__2024_01"
    assert info.from_value == "2024-01-01"
    assert info.to_value == "2024-02-01"
    assert info.is_attached is False
    assert info.parent_table == "events"


def test__repository__create_partition__already_exists__raises_partition_already_exists(
    config: TablePartitionConfig,
) -> None:
    # Arrange — set_config timeout succeeds; CREATE TABLE raises duplicate_table
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = [MagicMock(), _make_retryable_exc("42P07")]
    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAlreadyExistsError):
        repo.create_partition(config, "events__2024_01", "2024-01-01", "2024-02-01")


# ── attach_partition ────────────────────────────────────────────────────────────


def test__repository__attach_partition__sets_timeout_and_utc_timezone_then_attaches() -> None:
    # Arrange
    engine = _make_engine([None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.attach_partition("events", "events__2024_01", "2024-01-01", "2024-02-01")

    # Assert
    conn = engine.begin.return_value.__enter__.return_value
    assert conn.execute.call_count == 3
    first_stmt = str(conn.execute.call_args_list[0].args[0])
    assert "statement_timeout" in first_stmt
    second_stmt = str(conn.execute.call_args_list[1].args[0])
    assert "time zone" in second_stmt.lower() and "SET LOCAL" in second_stmt
    assert "ATTACH PARTITION" in str(conn.execute.call_args_list[2].args[0])


# ── detach_partition ────────────────────────────────────────────────────────────


def test__repository__detach_partition__table_not_found__raises_partition_not_found() -> None:
    # Arrange — pre-check: set_config, not pending, RESET; then set_config, marker query raises 42P01, RESET
    engine = MagicMock()
    conn = MagicMock()
    not_pending = MagicMock()
    not_pending.scalar.return_value = False
    conn.execute.side_effect = [
        MagicMock(),
        not_pending,
        MagicMock(),
        MagicMock(),
        _make_retryable_exc("42P01"),
        MagicMock(),
    ]
    conn.execution_options = MagicMock(return_value=conn)
    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=conn)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionNotFoundError):
        repo.detach_partition("events", "events__2024_01")


def test__repository__detach_partition_concurrent__uses_connect_not_begin() -> None:
    # Arrange — pre-check: set_config, not pending, RESET;
    # then set_config, resolve_fqn, comment_result, COMMENT, DETACH CONCURRENTLY, RESET
    engine = _make_engine([None, False, None, None, "public.events", None, None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — one connect for the pending pre-check, one for the concurrent detach
    assert engine.connect.call_count == 2
    assert engine.begin.call_count == 0


def test__repository__detach_partition_concurrent__non_ascii_comment__does_not_raise() -> None:
    # Arrange — pre-check (3 executes), then concurrent detach flow
    engine = _make_engine(
        [None, False, None, None, "public.events", "существующий комментарий".encode(), None, None, None]
    )
    repo = PostgresPartitionRepository(engine)

    # Act / Assert — must not raise
    repo.detach_partition("events", "events__2024_01", concurrent=True)
    assert engine.connect.call_count == 2


def test__repository__detach_partition_concurrent__0a000_error__falls_back_to_non_concurrent() -> None:
    # Arrange
    engine = MagicMock()
    concurrent_exc = _make_retryable_exc("0A000")
    concurrent_conn = MagicMock()
    q0, p0, q1 = MagicMock(), MagicMock(), MagicMock()
    p0.scalar.return_value = False  # pending-detach pre-check → not pending
    s0, c1, c2, c3, c5 = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    c1.scalar.return_value = "public.events"
    c2.scalar.return_value = None
    concurrent_conn.execute.side_effect = [q0, p0, q1, s0, c1, c2, c3, concurrent_exc, c5]
    concurrent_conn.execution_options = MagicMock(return_value=concurrent_conn)
    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=concurrent_conn)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm

    fallback_conn = MagicMock()
    r0, r1, r2, r3, r4 = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    r1.scalar.return_value = "public.events"
    r2.scalar.return_value = None
    fallback_conn.execute.side_effect = [r0, r1, r2, r3, r4]
    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=fallback_conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm

    repo = PostgresPartitionRepository(engine)

    # Act
    repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — pre-check connect + concurrent connect, then non-concurrent begin
    assert engine.connect.call_count == 2
    assert engine.begin.call_count == 1


def test__repository__detach_partition_concurrent__55000_error__falls_back_to_non_concurrent() -> None:
    # Arrange
    engine = MagicMock()
    concurrent_exc = _make_retryable_exc("55000")
    concurrent_conn = MagicMock()
    q0, p0, q1 = MagicMock(), MagicMock(), MagicMock()
    p0.scalar.return_value = False  # pending-detach pre-check → not pending
    s0, c1, c2, c3, c5 = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    c1.scalar.return_value = "public.events"
    c2.scalar.return_value = None
    concurrent_conn.execute.side_effect = [q0, p0, q1, s0, c1, c2, c3, concurrent_exc, c5]
    concurrent_conn.execution_options = MagicMock(return_value=concurrent_conn)
    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=concurrent_conn)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm

    fallback_conn = MagicMock()
    r0, r1, r2, r3, r4 = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    r1.scalar.return_value = "public.events"
    r2.scalar.return_value = None
    fallback_conn.execute.side_effect = [r0, r1, r2, r3, r4]
    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=fallback_conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm

    repo = PostgresPartitionRepository(engine)

    # Act
    repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — pre-check connect + concurrent connect, then non-concurrent begin
    assert engine.connect.call_count == 2
    assert engine.begin.call_count == 1


def test__repository__detach_partition_concurrent__55006_error__raises_detach_in_progress() -> None:
    # Arrange
    engine = MagicMock()
    in_progress_exc = _make_retryable_exc("55006")
    concurrent_conn = MagicMock()
    q0, p0, q1 = MagicMock(), MagicMock(), MagicMock()
    p0.scalar.return_value = False  # pending-detach pre-check → not pending
    s0, c1, c2, c3, c5 = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    c1.scalar.return_value = "public.events"
    c2.scalar.return_value = None
    concurrent_conn.execute.side_effect = [q0, p0, q1, s0, c1, c2, c3, in_progress_exc, c5]
    concurrent_conn.execution_options = MagicMock(return_value=concurrent_conn)
    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=concurrent_conn)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionDetachInProgressError):
        repo.detach_partition("events", "events__2024_01", concurrent=True)

    engine.begin.assert_not_called()


def test__repository__detach_partition_concurrent__generic_error__propagates() -> None:
    # Arrange
    engine = MagicMock()
    concurrent_conn = MagicMock()
    q0, p0, q1 = MagicMock(), MagicMock(), MagicMock()
    p0.scalar.return_value = False  # pending-detach pre-check → not pending
    s0, c1, c2, c3, c5 = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    c1.scalar.return_value = "public.events"
    c2.scalar.return_value = None
    concurrent_conn.execute.side_effect = [q0, p0, q1, s0, c1, c2, c3, Exception("permission denied"), c5]
    concurrent_conn.execution_options = MagicMock(return_value=concurrent_conn)
    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=concurrent_conn)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(Exception, match="permission denied"):
        repo.detach_partition("events", "events__2024_01", concurrent=True)

    engine.begin.assert_not_called()


def test__repository__detach_partition_non_concurrent__uses_begin() -> None:
    # Arrange — pre-check: set_config, not pending, RESET; then set_config, resolve_fqn, comment_result, COMMENT, DETACH
    engine = _make_engine([None, False, None, None, None, None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.detach_partition("events", "events__2024_01", concurrent=False)

    # Assert
    engine.begin().__enter__.assert_called()


def test__repository__detach_partition_concurrent__marker_write_fails__aborts_before_detach() -> None:
    # Arrange — pre-check: set_config, not pending, RESET; then set_config, resolve_fqn and comment read
    # succeed, COMMENT write fails; RESET timeout still runs
    engine = MagicMock()
    conn = MagicMock()
    conn.execution_options = MagicMock(return_value=conn)
    not_pending, resolve_result, comment_result = MagicMock(), MagicMock(), MagicMock()
    not_pending.scalar.return_value = False
    resolve_result.scalar.return_value = "public.events"
    comment_result.scalar.return_value = None
    conn.execute.side_effect = [
        MagicMock(),
        not_pending,
        MagicMock(),
        MagicMock(),
        resolve_result,
        comment_result,
        Exception("cannot comment table"),
        MagicMock(),
    ]
    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=conn)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(Exception, match="cannot comment table"):
        repo.detach_partition("events", "events__2024_01", concurrent=True)

    # pre-check (3) + set_config + resolve + comment read + failed COMMENT + RESET — the DETACH itself never ran
    assert conn.execute.call_count == 8
    assert not any("DETACH" in str(call.args[0]) for call in conn.execute.call_args_list)
    engine.begin.assert_not_called()


def test__repository__detach_partition__pending_detach__finalizes_without_plain_or_concurrent_detach() -> None:
    # Arrange — pre-check: set_config, pending → True; then resolve_fqn, comment_result, COMMENT, FINALIZE, RESET
    engine = _make_engine([None, True, "public.events", None, None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — FINALIZE ran, the orphan marker was written, and no other detach was attempted
    conn = engine.connect.return_value.__enter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("DETACH PARTITION" in stmt and "FINALIZE" in stmt for stmt in statements)
    assert any("COMMENT ON TABLE" in stmt for stmt in statements)
    assert not any("CONCURRENTLY" in stmt for stmt in statements)
    assert engine.connect.call_count == 1
    engine.begin.assert_not_called()


def test__repository__detach_partition__not_pending__proceeds_with_concurrent_detach() -> None:
    # Arrange — pre-check: set_config, not pending, RESET;
    # then set_config, resolve_fqn, comment_result, COMMENT, DETACH CONCURRENTLY, RESET
    engine = _make_engine([None, False, None, None, "public.events", None, None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — normal concurrent flow runs; FINALIZE is never issued
    conn = engine.connect.return_value.__enter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("DETACH PARTITION" in stmt and "CONCURRENTLY" in stmt for stmt in statements)
    assert not any("FINALIZE" in stmt for stmt in statements)


# ── drop_partition — happy path ──────────────────────────────────────────────────


def test__repository__drop_partition__not_exists__is_noop() -> None:
    # Arrange — set_config, exists → False
    engine = _make_engine([None, False])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_not_called()


def test__repository__drop_partition__detached_with_orphan_marker__drops_successfully() -> None:
    # Arrange — set_config, exists, not attached, has orphan marker; FK conn: set_config, no FKs;
    # DDL conn: set_config, lock_timeout, DROP
    engine = _make_engine([None, True, None, orphan_table_comment("events"), None, [], None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()


def test__repository__drop_partition__still_attached__raises_partition_attached_error() -> None:
    # Arrange — set_config, exists, is attached
    engine = _make_engine([None, True, "events"])
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAttachedError):
        repo.drop_partition("events__2024_01")

    engine.begin.assert_not_called()


def test__repository__drop_partition__no_orphan_marker__raises_unmanaged_drop_error() -> None:
    # Arrange — set_config, exists, not attached, no marker, still exists (race check)
    engine = _make_engine([None, True, None, None, True])
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(UnmanagedPartitionDropError):
        repo.drop_partition("events__2024_01")

    engine.begin.assert_not_called()


def test__repository__drop_partition__disappeared_between_checks__is_noop() -> None:
    # Arrange — set_config, exists initially, no marker, gone by race check
    engine = _make_engine([None, True, None, None, False])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_not_called()


def test__repository__drop_partition__unmanaged_with_opt_in__drops_successfully() -> None:
    # Arrange — set_config, exists, not attached; FK conn: set_config, no FKs; DDL conn: set_config, lock, DROP
    engine = _make_engine([None, True, None, None, [], None, None, None])
    repo = PostgresPartitionRepository(engine, drop_allow_unmanaged=True)

    # Act
    repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()


# ── drop_partition — FK cleanup ──────────────────────────────────────────────────


def test__repository__drop_partition__single_fk__drops_fk_then_table() -> None:
    # Arrange
    engine = _make_engine(
        [
            None,
            True,
            None,
            orphan_table_comment("events"),
            None,
            [("fk_events__2024_01_order_id",)],
            None,
            None,
            None,
            None,
        ]
    )
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()


def test__repository__drop_partition__multiple_fks__drops_all_fks() -> None:
    # Arrange
    engine = _make_engine(
        [None, True, None, orphan_table_comment("events"), None, [("fk_a",), ("fk_b",)], None, None, None, None]
    )
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()


# ── drop_partition — retry logic ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "sqlstate",
    ["40P01", "55P03", "57014"],
)
def test__repository__drop_partition__retryable_error__retries_and_succeeds(sqlstate: str) -> None:
    # Arrange
    exc = _make_retryable_exc(sqlstate)
    engine = _make_retry_engine(exc, fail_attempts=1)
    repo = PostgresPartitionRepository(engine, drop_retry_delay=0)

    # Act
    repo.drop_partition("events__2024_01")

    # Assert
    assert engine.begin.call_count == 2


def test__repository__drop_partition__all_retries_exhausted__raises_drop_retry_exhausted() -> None:
    # Arrange
    deadlock = _make_retryable_exc("40P01")
    engine = MagicMock()
    conn_read = MagicMock()

    def _r(v: object) -> MagicMock:
        r = MagicMock()
        if isinstance(v, list):
            r.fetchall.return_value = v
        else:
            r.scalar.return_value = v
        return r

    conn_read.execute.side_effect = [_r(None), _r(True), _r(None), _r(orphan_table_comment("events")), _r(None), _r([])]
    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=conn_read)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm

    fail_conn = MagicMock()
    fail_conn.execute.side_effect = deadlock
    fail_cm = MagicMock()
    fail_cm.__enter__ = MagicMock(return_value=fail_conn)
    fail_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = fail_cm

    repo = PostgresPartitionRepository(engine, drop_max_retries=3, drop_retry_delay=0)

    # Act / Assert
    with pytest.raises(DropRetryExhaustedError) as exc_info:
        repo.drop_partition("events__2024_01")

    assert exc_info.value.partition_name == "events__2024_01"
    assert exc_info.value.attempts == 3
    assert engine.begin.call_count == 3


def test__repository__drop_partition__non_retryable_error__fails_without_retry() -> None:
    # Arrange
    engine = MagicMock()
    conn_read = MagicMock()

    def _r(v: object) -> MagicMock:
        r = MagicMock()
        if isinstance(v, list):
            r.fetchall.return_value = v
        else:
            r.scalar.return_value = v
        return r

    conn_read.execute.side_effect = [_r(None), _r(True), _r(None), _r(orphan_table_comment("events")), _r(None), _r([])]
    connect_cm = MagicMock()
    connect_cm.__enter__ = MagicMock(return_value=conn_read)
    connect_cm.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_cm

    fail_conn = MagicMock()
    fail_conn.execute.side_effect = Exception("syntax error")
    fail_cm = MagicMock()
    fail_cm.__enter__ = MagicMock(return_value=fail_conn)
    fail_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = fail_cm

    repo = PostgresPartitionRepository(engine, drop_max_retries=3, drop_retry_delay=0)

    # Act / Assert
    with pytest.raises(Exception, match="syntax error"):
        repo.drop_partition("events__2024_01")

    assert engine.begin.call_count == 1


def test__repository__drop_partition__retry__logs_warning_with_attempt_number() -> None:
    # Arrange
    deadlock = _make_retryable_exc("40P01")
    engine = _make_retry_engine(deadlock, fail_attempts=1)

    logger = MagicMock()
    with patch("pg_partsmith.sync.repositories.remover.logger", logger):
        repo = PostgresPartitionRepository(engine, drop_retry_delay=0)

        # Act
        repo.drop_partition("events__2024_01")

        # Assert
        logger.warning.assert_called_once()

    call_kwargs = logger.warning.call_args
    assert call_kwargs.kwargs.get("extra", {}).get("attempt") == 2


# ── is_partition_attached ───────────────────────────────────────────────────────


@pytest.mark.parametrize("attached", [True, False])
def test__repository__is_partition_attached__returns_correct_bool(attached: bool) -> None:
    # Arrange
    engine = _make_engine([attached])
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    assert repo.is_partition_attached("events", "events__2024_01") is attached


def test__repository__is_partition_attached__uses_quoted_regclass_arguments() -> None:
    # Arrange
    engine = _make_engine([True])
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.is_partition_attached("events", "events__2024_W12")

    # Assert
    conn = engine.connect.return_value.__enter__.return_value
    params = conn.execute.call_args.args[1]
    assert params["table_name"] == '"events"'
    assert params["partition_name"] == '"events__2024_W12"'


# ── pg_sqlstate helper ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "exc_builder,expected",
    [
        (
            lambda: _set_attr(Exception("deadlock"), "orig", _set_attr(MagicMock(), "sqlstate", "40P01")),
            "40P01",
        ),
    ],
)
def test__pg_sqlstate__asyncpg_style_exception__returns_sqlstate(exc_builder: object, expected: str) -> None:
    # Arrange
    orig = MagicMock()
    orig.sqlstate = "40P01"
    exc = Exception("deadlock")
    exc.orig = orig  # type: ignore[attr-defined]

    # Act / Assert
    assert pg_sqlstate(exc) == "40P01"


def test__pg_sqlstate__psycopg2_style_exception__returns_pgcode() -> None:
    # Arrange
    orig = MagicMock()
    orig.sqlstate = None
    orig.pgcode = "55P03"
    exc = Exception("lock")
    exc.orig = orig  # type: ignore[attr-defined]

    # Act / Assert
    assert pg_sqlstate(exc) == "55P03"


def test__pg_sqlstate__plain_exception__returns_none() -> None:
    # Arrange / Act / Assert
    assert pg_sqlstate(ValueError("nope")) is None


# ── reconcile_default_rows ───────────────────────────────────────────────────────


def test__repository__reconcile_default_rows__matching_rows__returns_row_count() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 42
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    count = repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    assert count == 42
    assert conn.execute.call_count == 5  # set_config + SET TIME ZONE + 2 LOCK TABLE + move


def test__repository__reconcile_default_rows__acquires_locks_on_both_tables() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 5
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    calls = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("LOCK TABLE" in call and "events_default" in call for call in calls)
    assert any("LOCK TABLE" in call and "events__2024_04" in call for call in calls)


def test__repository__reconcile_default_rows__no_matching_rows__returns_zero() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 0
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    count = repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    assert count == 0


def test__repository__reconcile_default_rows__sets_timezone_before_locks() -> None:
    # Arrange — default ddl_timezone is 'UTC'
    move_result = MagicMock()
    move_result.rowcount = 1
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert — SET LOCAL TIME ZONE runs right after set_config, before either LOCK TABLE
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert "statement_timeout" in statements[0]
    assert "time zone" in statements[1].lower() and "SET LOCAL" in statements[1]
    assert all("LOCK TABLE" in stmt for stmt in statements[2:4])


def test__repository__reconcile_default_rows__no_ddl_timezone__skips_set_time_zone() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 1
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = MagicMock()
    begin_cm.__enter__ = MagicMock(return_value=conn)
    begin_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine, ddl_timezone=None)

    # Act
    repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert — only set_config + 2 LOCK TABLE + move; no timezone statement issued
    assert conn.execute.call_count == 4
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert not any("time zone" in stmt.lower() for stmt in statements)


# ── helper used by parametrize ──────────────────────────────────────────────────


def _set_attr(obj: object, attr: str, value: object) -> object:
    setattr(obj, attr, value)
    return obj
