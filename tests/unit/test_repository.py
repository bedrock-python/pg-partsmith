from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.repositories import PostgresPartitionRepository
from pg_partsmith.aio.repositories.fk_manager import PartitionForeignKeyManager
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
from pg_partsmith.utils import orphan_table_comment, pg_sqlstate

# ── helpers ─────────────────────────────────────────────────────────────────────


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
    conn = AsyncMock()

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

    conn.execution_options = AsyncMock(return_value=conn)

    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm

    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=conn)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
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
    conn_read = AsyncMock()

    def _r(v: object) -> MagicMock:
        r = MagicMock()
        if isinstance(v, list):
            r.fetchall.return_value = v
        else:
            r.scalar.return_value = v
        return r

    conn_read.execute.side_effect = [
        _r(True),
        _r(None),
        _r(orphan_table_comment("events")),
    ]

    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=conn_read)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm

    def _make_begin_cm(*, should_fail: bool) -> AsyncMock:
        ddl_conn = AsyncMock()
        if should_fail:
            ddl_conn.execute.side_effect = fail_with
        else:
            # lock_timeout, LOCK TABLE, revalidation (not attached, marker), FK list, DROP
            ddl_conn.execute.side_effect = [
                _r(None),
                _r(None),
                _r(None),
                _r(orphan_table_comment("events")),
                _r([]),
                _r(None),
            ]
        begin_cm = AsyncMock()
        begin_cm.__aenter__ = AsyncMock(return_value=ddl_conn)
        begin_cm.__aexit__ = AsyncMock(return_value=False)
        return begin_cm

    begin_cms = [_make_begin_cm(should_fail=True) for _ in range(fail_attempts)]
    begin_cms.append(_make_begin_cm(should_fail=False))
    engine.begin.side_effect = begin_cms

    return engine


def _make_drop_engine(read_values: list[object], ddl_values: list[object]) -> tuple[MagicMock, AsyncMock]:
    """Engine with separate connect() (pre-check) and begin() (DDL) connections for drop tests.

    An Exception instance in a sequence is raised by that execute() call;
    a list becomes result.fetchall(); anything else becomes result.scalar().
    """

    def _to_effect(value: object) -> object:
        if isinstance(value, BaseException):
            return value
        r = MagicMock()
        if isinstance(value, list):
            r.fetchall.return_value = value
        else:
            r.scalar.return_value = value
        return r

    engine = MagicMock()

    conn_read = AsyncMock()
    conn_read.execute.side_effect = [_to_effect(v) for v in read_values]
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=conn_read)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm

    ddl_conn = AsyncMock()
    ddl_conn.execute.side_effect = [_to_effect(v) for v in ddl_values]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=ddl_conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm

    return engine, ddl_conn


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


# ── create_partition ────────────────────────────────────────────────────────────


async def test__repository__create_partition__new_partition__returns_partition_info(
    config: TablePartitionConfig,
) -> None:
    # Arrange — CREATE TABLE → success
    engine = _make_engine([None])
    repo = PostgresPartitionRepository(engine)

    # Act
    info = await repo.create_partition(config, "events__2024_01", "2024-01-01", "2024-02-01")

    # Assert
    assert info.name == "events__2024_01"
    assert info.from_value == "2024-01-01"
    assert info.to_value == "2024-02-01"
    assert info.is_attached is False
    assert info.parent_table == "events"


async def test__repository__create_partition__already_exists__raises_partition_already_exists(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    engine = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = _make_retryable_exc("42P07")
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAlreadyExistsError):
        await repo.create_partition(config, "events__2024_01", "2024-01-01", "2024-02-01")


# ── attach_partition ────────────────────────────────────────────────────────────


async def test__repository__attach_partition__sets_utc_timezone_then_attaches() -> None:
    # Arrange
    engine = _make_engine([None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.attach_partition("events", "events__2024_01", "2024-01-01", "2024-02-01")

    # Assert
    conn = engine.begin.return_value.__aenter__.return_value
    assert conn.execute.call_count == 2
    first_stmt = str(conn.execute.call_args_list[0].args[0])
    assert "time zone" in first_stmt.lower() and "SET LOCAL" in first_stmt
    assert "ATTACH PARTITION" in str(conn.execute.call_args_list[1].args[0])


# ── detach_partition ────────────────────────────────────────────────────────────


async def test__repository__detach_partition__table_not_found__raises_partition_not_found() -> None:
    # Arrange — pending-detach pre-check → not pending; first marker query raises 42P01
    engine = MagicMock()
    conn = AsyncMock()
    not_pending = MagicMock()
    not_pending.scalar.return_value = False
    conn.execute.side_effect = [not_pending, _make_retryable_exc("42P01")]
    conn.execution_options = AsyncMock(return_value=conn)
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=conn)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionNotFoundError):
        await repo.detach_partition("events", "events__2024_01")


async def test__repository__detach_partition_concurrent__uses_connect_not_begin() -> None:
    # Arrange — sequence: pending pre-check → False, then resolve_fqn, comment_result, COMMENT, DETACH CONCURRENTLY
    engine = _make_engine([False, "public.events", None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — one connect for the pending pre-check, one for the concurrent detach
    assert engine.connect.call_count == 2
    assert engine.begin.call_count == 0


async def test__repository__detach_partition_concurrent__non_ascii_comment__does_not_raise() -> None:
    # Arrange — pending pre-check → False, then concurrent detach flow
    engine = _make_engine([False, "public.events", "существующий комментарий".encode(), None, None])
    repo = PostgresPartitionRepository(engine)

    # Act / Assert — must not raise
    await repo.detach_partition("events", "events__2024_01", concurrent=True)
    assert engine.connect.call_count == 2


async def test__repository__detach_partition_concurrent__0a000_error__falls_back_to_non_concurrent() -> None:
    # Arrange
    engine = MagicMock()
    concurrent_exc = _make_retryable_exc("0A000")
    concurrent_conn = AsyncMock()
    p0, c1, c2, c3 = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    p0.scalar.return_value = False  # pending-detach pre-check → not pending
    c1.scalar.return_value = "public.events"
    c2.scalar.return_value = None
    concurrent_conn.execute.side_effect = [p0, c1, c2, c3, concurrent_exc]
    concurrent_conn.execution_options = AsyncMock(return_value=concurrent_conn)
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=concurrent_conn)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm

    fallback_conn = AsyncMock()
    r1, r2, r3, r4 = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    r2.scalar.return_value = None
    r3.scalar.return_value = None
    fallback_conn.execute.side_effect = [r1, r2, r3, r4]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=fallback_conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm

    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — pre-check connect + concurrent connect, then non-concurrent begin
    assert engine.connect.call_count == 2
    assert engine.begin.call_count == 1


async def test__repository__detach_partition_concurrent__55000_error__falls_back_to_non_concurrent() -> None:
    # Arrange
    engine = MagicMock()
    concurrent_exc = _make_retryable_exc("55000")
    concurrent_conn = AsyncMock()
    p0, c1, c2, c3 = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    p0.scalar.return_value = False  # pending-detach pre-check → not pending
    c1.scalar.return_value = "public.events"
    c2.scalar.return_value = None
    concurrent_conn.execute.side_effect = [p0, c1, c2, c3, concurrent_exc]
    concurrent_conn.execution_options = AsyncMock(return_value=concurrent_conn)
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=concurrent_conn)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm

    fallback_conn = AsyncMock()
    r1, r2, r3, r4 = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    r2.scalar.return_value = None
    r3.scalar.return_value = None
    fallback_conn.execute.side_effect = [r1, r2, r3, r4]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=fallback_conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm

    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — pre-check connect + concurrent connect, then non-concurrent begin
    assert engine.connect.call_count == 2
    assert engine.begin.call_count == 1


async def test__repository__detach_partition_concurrent__55006_error__raises_detach_in_progress() -> None:
    # Arrange
    engine = MagicMock()
    in_progress_exc = _make_retryable_exc("55006")
    concurrent_conn = AsyncMock()
    p0, c1, c2, c3 = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    p0.scalar.return_value = False  # pending-detach pre-check → not pending
    c1.scalar.return_value = "public.events"
    c2.scalar.return_value = None
    concurrent_conn.execute.side_effect = [p0, c1, c2, c3, in_progress_exc]
    concurrent_conn.execution_options = AsyncMock(return_value=concurrent_conn)
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=concurrent_conn)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionDetachInProgressError):
        await repo.detach_partition("events", "events__2024_01", concurrent=True)

    engine.begin.assert_not_called()


async def test__repository__detach_partition_concurrent__generic_error__propagates() -> None:
    # Arrange
    engine = MagicMock()
    concurrent_conn = AsyncMock()
    p0, c1, c2, c3 = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    p0.scalar.return_value = False  # pending-detach pre-check → not pending
    c1.scalar.return_value = "public.events"
    c2.scalar.return_value = None
    concurrent_conn.execute.side_effect = [p0, c1, c2, c3, Exception("permission denied")]
    concurrent_conn.execution_options = AsyncMock(return_value=concurrent_conn)
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=concurrent_conn)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(Exception, match="permission denied"):
        await repo.detach_partition("events", "events__2024_01", concurrent=True)

    engine.begin.assert_not_called()


async def test__repository__detach_partition_non_concurrent__uses_begin() -> None:
    # Arrange — sequence: pending pre-check → False, then resolve_fqn, comment_result, COMMENT, DETACH
    engine = _make_engine([False, None, None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01", concurrent=False)

    # Assert
    engine.begin().__aenter__.assert_called()


async def test__repository__detach_partition_concurrent__marker_write_fails__aborts_before_detach() -> None:
    # Arrange — pending pre-check → False; resolve_fqn and comment read succeed; COMMENT write fails
    engine = MagicMock()
    conn = AsyncMock()
    conn.execution_options = AsyncMock(return_value=conn)
    not_pending, resolve_result, comment_result = MagicMock(), MagicMock(), MagicMock()
    not_pending.scalar.return_value = False
    resolve_result.scalar.return_value = "public.events"
    comment_result.scalar.return_value = None
    conn.execute.side_effect = [not_pending, resolve_result, comment_result, Exception("cannot comment table")]
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=conn)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(Exception, match="cannot comment table"):
        await repo.detach_partition("events", "events__2024_01", concurrent=True)

    assert conn.execute.call_count == 4
    assert not any("DETACH" in str(call.args[0]) for call in conn.execute.call_args_list)
    engine.begin.assert_not_called()


async def test__repository__detach_partition__pending_detach__finalizes_without_plain_or_concurrent_detach() -> None:
    # Arrange — pending pre-check → True; then resolve_fqn, comment_result, COMMENT, FINALIZE
    engine = _make_engine([True, "public.events", None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — FINALIZE ran, the orphan marker was written, and no other detach was attempted
    conn = engine.connect.return_value.__aenter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("DETACH PARTITION" in stmt and "FINALIZE" in stmt for stmt in statements)
    assert any("COMMENT ON TABLE" in stmt for stmt in statements)
    assert not any("CONCURRENTLY" in stmt for stmt in statements)
    assert engine.connect.call_count == 1
    engine.begin.assert_not_called()


async def test__repository__detach_partition__not_pending__proceeds_with_concurrent_detach() -> None:
    # Arrange — pending pre-check → False, then resolve_fqn, comment_result, COMMENT, DETACH CONCURRENTLY
    engine = _make_engine([False, "public.events", None, None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.detach_partition("events", "events__2024_01", concurrent=True)

    # Assert — normal concurrent flow runs; FINALIZE is never issued
    conn = engine.connect.return_value.__aenter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("DETACH PARTITION" in stmt and "CONCURRENTLY" in stmt for stmt in statements)
    assert not any("FINALIZE" in stmt for stmt in statements)


# ── drop_partition — happy path ──────────────────────────────────────────────────


async def test__repository__drop_partition__not_exists__is_noop() -> None:
    # Arrange
    engine = _make_engine([False])
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_not_called()


async def test__repository__drop_partition__detached_with_orphan_marker__drops_successfully() -> None:
    # Arrange — pre-check: exists, not attached, has orphan marker;
    # DDL txn: lock_timeout, LOCK, not attached, marker, no FKs, DROP
    engine = _make_engine(
        [True, None, orphan_table_comment("events"), None, None, None, orphan_table_comment("events"), [], None]
    )
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()


async def test__repository__drop_partition__still_attached__raises_partition_attached_error() -> None:
    # Arrange — exists, is attached
    engine = _make_engine([True, "events"])
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAttachedError):
        await repo.drop_partition("events__2024_01")

    engine.begin.assert_not_called()


async def test__repository__drop_partition__no_orphan_marker__raises_unmanaged_drop_error() -> None:
    # Arrange — exists, not attached, no marker, still exists (race check)
    engine = _make_engine([True, None, None, True])
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(UnmanagedPartitionDropError):
        await repo.drop_partition("events__2024_01")

    engine.begin.assert_not_called()


async def test__repository__drop_partition__disappeared_between_checks__is_noop() -> None:
    # Arrange — exists initially, no marker, gone by race check
    engine = _make_engine([True, None, None, False])
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_not_called()


async def test__repository__drop_partition__unmanaged_with_opt_in__drops_successfully() -> None:
    # Arrange — pre-check: exists, not attached; DDL txn: lock_timeout, LOCK, not attached, no FKs, DROP
    engine = _make_engine([True, None, None, None, None, [], None])
    repo = PostgresPartitionRepository(engine, drop_allow_unmanaged=True)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()


# ── drop_partition — FK cleanup ──────────────────────────────────────────────────


async def test__repository__drop_partition__single_fk__drops_fk_then_table() -> None:
    # Arrange — pre-check: exists, not attached, marker; DDL txn: lock_timeout, LOCK, not attached,
    # marker, FK list, DROP CONSTRAINT, DROP TABLE
    engine = _make_engine(
        [
            True,
            None,
            orphan_table_comment("events"),
            None,
            None,
            None,
            orphan_table_comment("events"),
            [("fk_events__2024_01_order_id",)],
            None,
            None,
        ]
    )
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()
    conn = engine.begin.return_value.__aenter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("DROP CONSTRAINT" in stmt and "fk_events__2024_01_order_id" in stmt for stmt in statements)
    assert any("DROP TABLE" in stmt for stmt in statements)


async def test__repository__drop_partition__multiple_fks__drops_all_fks() -> None:
    # Arrange
    engine = _make_engine(
        [
            True,
            None,
            orphan_table_comment("events"),
            None,
            None,
            None,
            orphan_table_comment("events"),
            [("fk_a",), ("fk_b",)],
            None,
            None,
        ]
    )
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()
    conn = engine.begin.return_value.__aenter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("fk_a" in stmt and "fk_b" in stmt and "DROP CONSTRAINT" in stmt for stmt in statements)


# ── drop_partition — retry logic ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "sqlstate",
    ["40P01", "55P03", "57014"],
)
async def test__repository__drop_partition__retryable_error__retries_and_succeeds(sqlstate: str) -> None:
    # Arrange
    exc = _make_retryable_exc(sqlstate)
    engine = _make_retry_engine(exc, fail_attempts=1)
    repo = PostgresPartitionRepository(engine, drop_retry_delay=0)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert
    assert engine.begin.call_count == 2


async def test__repository__drop_partition__all_retries_exhausted__raises_drop_retry_exhausted() -> None:
    # Arrange
    deadlock = _make_retryable_exc("40P01")
    engine = MagicMock()
    conn_read = AsyncMock()

    def _r(v: object) -> MagicMock:
        r = MagicMock()
        if isinstance(v, list):
            r.fetchall.return_value = v
        else:
            r.scalar.return_value = v
        return r

    conn_read.execute.side_effect = [_r(True), _r(None), _r(orphan_table_comment("events")), _r([])]
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=conn_read)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm

    fail_conn = AsyncMock()
    fail_conn.execute.side_effect = deadlock
    fail_cm = AsyncMock()
    fail_cm.__aenter__ = AsyncMock(return_value=fail_conn)
    fail_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = fail_cm

    repo = PostgresPartitionRepository(engine, drop_max_retries=3, drop_retry_delay=0)

    # Act / Assert
    with pytest.raises(DropRetryExhaustedError) as exc_info:
        await repo.drop_partition("events__2024_01")

    assert exc_info.value.partition_name == "events__2024_01"
    assert exc_info.value.attempts == 3
    assert engine.begin.call_count == 3


async def test__repository__drop_partition__non_retryable_error__fails_without_retry() -> None:
    # Arrange
    engine = MagicMock()
    conn_read = AsyncMock()

    def _r(v: object) -> MagicMock:
        r = MagicMock()
        if isinstance(v, list):
            r.fetchall.return_value = v
        else:
            r.scalar.return_value = v
        return r

    conn_read.execute.side_effect = [_r(True), _r(None), _r(orphan_table_comment("events")), _r([])]
    connect_cm = AsyncMock()
    connect_cm.__aenter__ = AsyncMock(return_value=conn_read)
    connect_cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = connect_cm

    fail_conn = AsyncMock()
    fail_conn.execute.side_effect = Exception("syntax error")
    fail_cm = AsyncMock()
    fail_cm.__aenter__ = AsyncMock(return_value=fail_conn)
    fail_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = fail_cm

    repo = PostgresPartitionRepository(engine, drop_max_retries=3, drop_retry_delay=0)

    # Act / Assert
    with pytest.raises(Exception, match="syntax error"):
        await repo.drop_partition("events__2024_01")

    assert engine.begin.call_count == 1


async def test__repository__drop_partition__retry__logs_warning_with_attempt_number() -> None:
    # Arrange
    deadlock = _make_retryable_exc("40P01")
    engine = _make_retry_engine(deadlock, fail_attempts=1)

    logger = MagicMock()
    with patch("pg_partsmith.aio.repositories.remover.logger", logger):
        repo = PostgresPartitionRepository(engine, drop_retry_delay=0)

        # Act
        await repo.drop_partition("events__2024_01")

        # Assert
        logger.warning.assert_called_once()

    call_kwargs = logger.warning.call_args
    assert call_kwargs.kwargs.get("extra", {}).get("attempt") == 2


# ── drop_partition — lock-then-revalidate transaction ────────────────────────────


async def test__repository__drop_partition__lock_precedes_revalidation_and_drop__single_transaction() -> None:
    # Arrange — pre-check passes; DDL txn: lock_timeout, LOCK, not attached, marker, FK list, DROP
    engine, ddl_conn = _make_drop_engine(
        [True, None, orphan_table_comment("events")],
        [None, None, None, orphan_table_comment("events"), [], None],
    )
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.drop_partition("events__2024_01")

    # Assert — one transaction where LOCK TABLE runs before revalidation, which runs before DROP TABLE
    engine.begin.assert_called_once()
    statements = [str(call.args[0]) for call in ddl_conn.execute.call_args_list]
    lock_idx = next(i for i, s in enumerate(statements) if "LOCK TABLE" in s and "ACCESS EXCLUSIVE" in s)
    revalidate_idx = next(i for i, s in enumerate(statements) if "pg_inherits" in s)
    drop_idx = next(i for i, s in enumerate(statements) if "DROP TABLE" in s)
    assert lock_idx < revalidate_idx < drop_idx


async def test__repository__drop_partition__lock_hits_42p01__returns_without_drop() -> None:
    # Arrange — the table vanished between the pre-check and the LOCK TABLE statement
    engine, ddl_conn = _make_drop_engine(
        [True, None, orphan_table_comment("events")],
        [None, _make_retryable_exc("42P01")],
    )
    repo = PostgresPartitionRepository(engine)

    # Act — treated as already-done, not an error and not a retry
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()
    statements = [str(call.args[0]) for call in ddl_conn.execute.call_args_list]
    assert not any("DROP TABLE" in s for s in statements)


async def test__repository__drop_partition__reattached_after_lock__raises_attached_error_without_drop() -> None:
    # Arrange — pre-check saw it detached, but under the lock it is attached again
    engine, ddl_conn = _make_drop_engine(
        [True, None, orphan_table_comment("events")],
        [None, None, "public.events"],
    )
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAttachedError):
        await repo.drop_partition("events__2024_01")

    statements = [str(call.args[0]) for call in ddl_conn.execute.call_args_list]
    assert not any("DROP TABLE" in s for s in statements)


async def test__repository__drop_partition__non_42p01_error_on_lock__propagates() -> None:
    # Arrange — a non-retryable, non-42P01 error on the LOCK TABLE statement (e.g. permission denied)
    engine, ddl_conn = _make_drop_engine(
        [True, None, orphan_table_comment("events")],
        [None, _make_retryable_exc("42501")],
    )
    repo = PostgresPartitionRepository(engine)

    # Act / Assert — must propagate, not be swallowed like 42P01 and not be retried
    with pytest.raises(SQLAlchemyError):
        await repo.drop_partition("events__2024_01")

    engine.begin.assert_called_once()
    statements = [str(call.args[0]) for call in ddl_conn.execute.call_args_list]
    assert not any("DROP TABLE" in s for s in statements)


async def test__repository__drop_partition__vanished_after_lock__returns_without_drop() -> None:
    # Arrange — under the lock the marker is gone AND the table no longer exists (dropped concurrently)
    engine, ddl_conn = _make_drop_engine(
        [True, None, orphan_table_comment("events")],
        [None, None, None, None, False],  # lock_timeout, LOCK, not attached, no marker, gone
    )
    repo = PostgresPartitionRepository(engine)

    # Act — treated as already-done, no error
    await repo.drop_partition("events__2024_01")

    # Assert
    engine.begin.assert_called_once()
    statements = [str(call.args[0]) for call in ddl_conn.execute.call_args_list]
    assert not any("DROP TABLE" in s for s in statements)


async def test__repository__drop_partition__marker_gone_after_lock__raises_unmanaged_error_without_drop() -> None:
    # Arrange — pre-check saw the orphan marker, but under the lock the comment is gone (table replaced)
    engine, ddl_conn = _make_drop_engine(
        [True, None, orphan_table_comment("events")],
        [None, None, None, None, True],  # lock_timeout, LOCK, not attached, no marker, still exists
    )
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(UnmanagedPartitionDropError):
        await repo.drop_partition("events__2024_01")

    statements = [str(call.args[0]) for call in ddl_conn.execute.call_args_list]
    assert not any("DROP TABLE" in s for s in statements)


# ── adopt_partition ─────────────────────────────────────────────────────────────


async def test__repository__adopt_partition__table_missing__returns_false_without_comment() -> None:
    # Arrange — the exists check inside the transaction comes back False
    engine = _make_engine([False])
    repo = PostgresPartitionRepository(engine)

    # Act
    adopted = await repo.adopt_partition("events", "events__2023_01")

    # Assert
    assert adopted is False
    conn = engine.begin.return_value.__aenter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert not any("COMMENT ON TABLE" in stmt for stmt in statements)


async def test__repository__adopt_partition__still_attached__raises_attached_error_without_comment() -> None:
    # Arrange — exists; the attachment check resolves a parent → the table is attached
    engine = _make_engine([True, "public.events"])
    repo = PostgresPartitionRepository(engine)

    # Act / Assert
    with pytest.raises(PartitionAttachedError):
        await repo.adopt_partition("events", "events__2023_01")

    conn = engine.begin.return_value.__aenter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert not any("COMMENT ON TABLE" in stmt for stmt in statements)


async def test__repository__adopt_partition__detached_existing__writes_marker_in_transaction_and_returns_true() -> None:
    # Arrange — exists, not attached, parent resolves to public.events, no existing comment, COMMENT
    engine = _make_engine([True, None, "public.events", None, None])
    repo = PostgresPartitionRepository(engine)

    # Act
    adopted = await repo.adopt_partition("events", "events__2023_01")

    # Assert — the marker COMMENT ran inside a single begin() transaction
    assert adopted is True
    engine.begin.assert_called_once()
    engine.connect.assert_not_called()
    conn = engine.begin.return_value.__aenter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    comment_stmts = [s for s in statements if "COMMENT ON TABLE" in s]
    assert len(comment_stmts) == 1
    assert orphan_table_comment("public.events") in comment_stmts[0]


async def test__repository__adopt_partition__marker_already_present__returns_true_without_rewriting_comment() -> None:
    # Arrange — the table already carries the orphan marker (idempotent re-adopt)
    engine = _make_engine([True, None, "public.events", orphan_table_comment("public.events")])
    repo = PostgresPartitionRepository(engine)

    # Act
    adopted = await repo.adopt_partition("events", "events__2023_01")

    # Assert
    assert adopted is True
    conn = engine.begin.return_value.__aenter__.return_value
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert not any("COMMENT ON TABLE" in stmt for stmt in statements)


# ── fk_manager ──────────────────────────────────────────────────────────────────


async def test__fk_manager__list_constraints_conn__returns_names() -> None:
    # Arrange — a mocked connection returning two FK constraint rows
    conn = AsyncMock()
    result = MagicMock()
    result.fetchall.return_value = [("fk_a",), ("fk_b",)]
    conn.execute.return_value = result

    # Act
    names = await PartitionForeignKeyManager.list_constraints_conn(conn, "events__2024_01")

    # Assert
    assert names == ["fk_a", "fk_b"]
    conn.execute.assert_called_once()
    params = conn.execute.call_args.args[1]
    assert params["partition_name"] == '"events__2024_01"'


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


async def test__repository__reconcile_default_rows__matching_rows__returns_row_count() -> None:
    # Arrange — SET LOCAL TIME ZONE + 2 LOCK TABLE + move
    move_result = MagicMock()
    move_result.rowcount = 42
    engine = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    count = await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    assert count == 42
    assert conn.execute.call_count == 4  # SET TIME ZONE + 2 LOCK TABLE + move


async def test__repository__reconcile_default_rows__acquires_locks_on_both_tables() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 5
    engine = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.reconcile_default_rows(
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


async def test__repository__reconcile_default_rows__no_matching_rows__returns_zero() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 0
    engine = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    count = await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert
    assert count == 0


async def test__repository__reconcile_default_rows__sets_timezone_before_locks() -> None:
    # Arrange — default ddl_timezone is 'UTC'
    move_result = MagicMock()
    move_result.rowcount = 1
    engine = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert — SET LOCAL TIME ZONE runs first, before either LOCK TABLE
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert "time zone" in statements[0].lower() and "SET LOCAL" in statements[0]
    assert all("LOCK TABLE" in stmt for stmt in statements[1:3])


async def test__repository__reconcile_default_rows__no_ddl_timezone__skips_set_time_zone() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 1
    engine = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), move_result]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine, ddl_timezone=None)

    # Act
    await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert — only 2 LOCK TABLE + move; no timezone statement issued
    assert conn.execute.call_count == 3
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert not any("time zone" in stmt.lower() for stmt in statements)


# ── ddl_timezone property ───────────────────────────────────────────────────────


def test__repository__ddl_timezone_property__returns_constructor_value() -> None:
    # Arrange / Act
    repo = PostgresPartitionRepository(MagicMock(), ddl_timezone="Europe/Moscow")

    # Assert
    assert repo.ddl_timezone == "Europe/Moscow"


def test__repository__ddl_timezone_property__defaults_to_utc() -> None:
    # Arrange / Act
    repo = PostgresPartitionRepository(MagicMock())

    # Assert
    assert repo.ddl_timezone == "UTC"


def test__repository__ddl_timezone_property__none_when_disabled() -> None:
    # Arrange / Act
    repo = PostgresPartitionRepository(MagicMock(), ddl_timezone=None)

    # Assert
    assert repo.ddl_timezone is None


# ── helper used by parametrize ──────────────────────────────────────────────────


def _set_attr(obj: object, attr: str, value: object) -> object:
    setattr(obj, attr, value)
    return obj


async def test__repository__reconcile_default_rows__composite_key__leaves_null_trailing_rows_in_default() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 3
    engine = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        trailing_columns=("tenant_id",),
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert -- PostgreSQL adds an IS NOT NULL test for every key column, so a
    # row with a NULL tenant belongs in DEFAULT and moving it would be rejected
    # with the very error this call exists to clear.
    move = str(conn.execute.call_args_list[-1].args[0])
    assert '"tenant_id" IS NOT NULL' in move


async def test__repository__reconcile_default_rows__single_column_key__adds_no_null_test() -> None:
    # Arrange
    move_result = MagicMock()
    move_result.rowcount = 3
    engine = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = [MagicMock(), MagicMock(), MagicMock(), move_result]
    begin_cm = AsyncMock()
    begin_cm.__aenter__ = AsyncMock(return_value=conn)
    begin_cm.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = begin_cm
    repo = PostgresPartitionRepository(engine)

    # Act
    await repo.reconcile_default_rows(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        partition_column="created_at",
        from_value="2024-04-01",
        to_value="2024-05-01",
    )

    # Assert -- the leading column already carries its own NOT NULL implicitly
    # through the range test, so the statement stays what it always was.
    assert "IS NOT NULL" not in str(conn.execute.call_args_list[-1].args[0])
