import threading
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pg_partsmith.exceptions import LockAcquisitionError
from pg_partsmith.sync.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.sync.lock.redis import RedisDistributedLockManager

# ── helpers ─────────────────────────────────────────────────────────────────────


def _make_engine_mock(lock_acquired: bool = True) -> tuple[MagicMock, MagicMock]:
    """Return (engine_mock, conn_mock) pair."""
    result = MagicMock()
    result.scalar.return_value = lock_acquired
    result.fetchall.return_value = []

    conn = MagicMock()
    conn.execute.return_value = result
    conn.execution_options.return_value = conn
    conn.invalidate.return_value = None

    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)

    engine = MagicMock()
    engine.connect.return_value = cm
    engine.begin.return_value = cm

    return engine, conn


def _make_redis_mock(
    acquire_result: bool = True,
) -> tuple[MagicMock, MagicMock, MagicMock]:
    """Return (redis_client, unlock_script, renew_script)."""
    redis_client = MagicMock()
    redis_client.set = MagicMock(return_value=acquire_result)
    redis_client.exists = MagicMock(return_value=1 if acquire_result else 0)
    unlock_script = MagicMock(return_value=1)
    renew_script = MagicMock(return_value=1)
    redis_client.register_script.side_effect = [unlock_script, renew_script]
    return redis_client, unlock_script, renew_script


# ── PostgresAdvisoryLockManager ──────────────────────────────────────────────────


def test__postgres_lock__lock_acquired__calls_lock_and_unlock_sql() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    # Act
    with manager.acquire_lock("events"):
        lock_call_count = conn.execute.call_count

    # Assert
    assert lock_call_count == 1  # advisory_lock call
    assert conn.execute.call_count == 2  # + advisory_unlock call


def test__postgres_lock__lock_not_acquired__raises_lock_acquisition_error() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=False)
    manager = PostgresAdvisoryLockManager(engine)

    # Act / Assert
    with pytest.raises(LockAcquisitionError) as exc_info, manager.acquire_lock("events"):
        pass

    assert "events" in str(exc_info.value)
    assert conn.execute.call_count == 1


def test__postgres_lock__body_raises__unlock_is_still_called() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    # Act
    with pytest.raises(ValueError, match="body error"), manager.acquire_lock("events"):
        raise ValueError("body error")

    # Assert
    assert conn.execute.call_count == 2  # type: ignore[unreachable]


def test__postgres_lock__body_keyboard_interrupt__unlock_is_still_called() -> None:
    """Sync analog of the async cancellation test: an interrupt in the body still releases the lock."""
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    # Act
    with pytest.raises(KeyboardInterrupt), manager.acquire_lock("events"):
        raise KeyboardInterrupt

    # Assert
    assert conn.execute.call_count == 2  # type: ignore[unreachable]


def test__postgres_lock__is_locked_true__queries_pg_locks() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    result = MagicMock()
    result.scalar.return_value = 1
    conn.execute.side_effect = None
    conn.execute.return_value = result
    manager = PostgresAdvisoryLockManager(engine)

    # Act
    locked = manager.is_locked("events")

    # Assert
    assert locked is True
    sql = str(conn.execute.call_args.args[0]).lower()
    assert "granted = true" in sql
    assert "database" in sql
    assert "objsubid" in sql


def test__postgres_lock__is_locked_false__returns_false() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=False)
    result = MagicMock()
    result.scalar.return_value = 0
    conn.execute.side_effect = None
    conn.execute.return_value = result
    manager = PostgresAdvisoryLockManager(engine)

    # Act / Assert
    assert manager.is_locked("events") is False


def test__postgres_lock__different_prefixes__produce_different_lock_ids() -> None:
    # Arrange
    engine = MagicMock()
    m1 = PostgresAdvisoryLockManager(engine, prefix="app1")
    m2 = PostgresAdvisoryLockManager(engine, prefix="app2")

    # Act / Assert
    assert m1._compute_lock_id("events") != m2._compute_lock_id("events")


def test__postgres_lock__unlock_keyboard_interrupt__invalidates_connection() -> None:
    """Sync analog of the async shield-cancellation test: interrupt during unlock invalidates the connection."""
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    def _execute_mock(statement: Any, params: dict[str, Any] | None = None) -> MagicMock:
        sql = str(statement).lower()
        if "pg_try_advisory_lock" in sql:
            r = MagicMock()
            r.scalar.return_value = True
            return r
        if "pg_advisory_unlock" in sql:
            raise KeyboardInterrupt
        return MagicMock()

    conn.execute.side_effect = _execute_mock

    # Act / Assert
    with pytest.raises(KeyboardInterrupt), manager.acquire_lock("test"):
        pass

    conn.invalidate.assert_called()


def test__postgres_lock__unlock_raises__invalidates_connection() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    def _execute_mock(statement: Any, params: dict[str, Any] | None = None) -> MagicMock:
        sql = str(statement).lower()
        if "pg_try_advisory_lock" in sql:
            r = MagicMock()
            r.scalar.return_value = True
            return r
        if "pg_advisory_unlock" in sql:
            raise RuntimeError("unlock failed")
        return MagicMock()

    conn.execute.side_effect = _execute_mock

    # Act / Assert
    with pytest.raises(RuntimeError, match="unlock failed"), manager.acquire_lock("test"):
        pass

    conn.invalidate.assert_called()


# ── RedisDistributedLockManager ──────────────────────────────────────────────────


def test__redis_lock__lock_acquired__calls_set_and_unlock_on_exit() -> None:
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act
    with manager.acquire_lock("events"):
        redis_client.set.assert_called_once()

    # Assert
    assert unlock_script.called


def test__redis_lock__lock_not_acquired__raises_lock_acquisition_error() -> None:
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=False)
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    with pytest.raises(LockAcquisitionError) as exc_info, manager.acquire_lock("events"):
        pass

    assert "events" in str(exc_info.value)
    unlock_script.assert_not_called()


def test__redis_lock__body_raises__unlock_is_still_called() -> None:
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act
    with pytest.raises(RuntimeError, match="body error"), manager.acquire_lock("events"):
        raise RuntimeError("body error")

    # Assert
    assert unlock_script.called  # type: ignore[unreachable]


def test__redis_lock__body_keyboard_interrupt__unlock_is_still_called() -> None:
    """Sync analog of the async cancellation test: an interrupt in the body still releases the lock."""
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, ttl_seconds=3)

    # Act
    with pytest.raises(KeyboardInterrupt), manager.acquire_lock("events"):
        raise KeyboardInterrupt

    # Assert
    assert unlock_script.called  # type: ignore[unreachable]


def test__redis_lock__is_locked_true__returns_true() -> None:
    # Arrange
    redis_client = MagicMock()
    redis_client.exists = MagicMock(return_value=1)
    redis_client.register_script.return_value = MagicMock(return_value=1)
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    assert manager.is_locked("events") is True


def test__redis_lock__is_locked_false__returns_false() -> None:
    # Arrange
    redis_client = MagicMock()
    redis_client.exists = MagicMock(return_value=0)
    redis_client.register_script.return_value = MagicMock(return_value=1)
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    assert manager.is_locked("events") is False


def test__redis_lock__redis_unavailable__raises_import_error_on_instantiation() -> None:
    # Arrange / Act / Assert
    with (
        patch("pg_partsmith.sync.lock.redis._redis_available", False),
        pytest.raises(ImportError, match="redis-locks"),
    ):
        RedisDistributedLockManager(MagicMock())


def test__redis_lock__custom_prefix__lock_key_includes_prefix_and_table() -> None:
    # Arrange
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(MagicMock(), prefix="myapp:lock")

    # Act
    key = manager._get_lock_key("events")

    # Assert
    assert key == "myapp:lock:events"


def test__redis_lock__ttl_below_minimum__raises_value_error() -> None:
    # Arrange / Act / Assert
    with (
        patch("pg_partsmith.sync.lock.redis._redis_available", True),
        pytest.raises(ValueError, match="ttl_seconds"),
    ):
        RedisDistributedLockManager(MagicMock(), ttl_seconds=2)


def test__redis_lock__bool_ttl__raises_value_error() -> None:
    # True == 1, which is below the minimum of 3
    # Arrange / Act / Assert
    with (
        patch("pg_partsmith.sync.lock.redis._redis_available", True),
        pytest.raises(ValueError, match="ttl_seconds"),
    ):
        RedisDistributedLockManager(MagicMock(), ttl_seconds=True)  # type: ignore[arg-type]


def test__redis_lock__watchdog__renews_ttl_until_stopped() -> None:
    """Sync analog of TTL renewal: the watchdog thread renews the TTL while held and exits once stopped."""
    # Arrange
    redis_client, _, renew_script = _make_redis_mock(acquire_result=True)
    renewed = threading.Event()
    stop_event = threading.Event()

    def _renew(*a: Any, **k: Any) -> int:
        renewed.set()
        return 1

    renew_script.side_effect = _renew

    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, ttl_seconds=3)

    # Act — run the watchdog with a tiny renewal interval, then stop it
    with patch("pg_partsmith.sync.lock.redis._RENEW_JITTER_RANGE", (0.001, 0.001)):
        watchdog = threading.Thread(
            target=manager._renewal_watchdog,
            args=("partitioner:lock:events", "token", "events", stop_event),
            daemon=True,
        )
        watchdog.start()
        assert renewed.wait(timeout=1.0)
        stop_event.set()
        watchdog.join(timeout=1.0)

    # Assert
    assert not watchdog.is_alive()
    renew_script.assert_called_with(keys=["partitioner:lock:events"], args=["token", "3"])


def test__redis_lock__watchdog__stop_event_already_set__returns_without_renewing() -> None:
    # Arrange
    redis_client, _, renew_script = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, ttl_seconds=3)

    stop_event = threading.Event()
    stop_event.set()

    # Act
    with patch("pg_partsmith.sync.lock.redis._RENEW_JITTER_RANGE", (0.001, 0.001)):
        manager._renewal_watchdog("partitioner:lock:events", "token", "events", stop_event)

    # Assert
    renew_script.assert_not_called()


def test__redis_lock__watchdog_renewal_fails__logs_warning_and_exits() -> None:
    """Sync analog of the async watchdog-cancels-holder test: the sync watchdog cannot cancel the holder — it
    logs a warning and exits without raising."""
    # Arrange
    redis_client, _, renew_script = _make_redis_mock(acquire_result=True)
    renew_script.side_effect = RuntimeError("redis down")

    logger = MagicMock()
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, ttl_seconds=3)

    stop_event = threading.Event()

    # Act — must not raise
    with (
        patch("pg_partsmith.sync.lock.redis._RENEW_JITTER_RANGE", (0.001, 0.001)),
        patch("pg_partsmith.sync.lock.redis.logger", logger),
    ):
        manager._renewal_watchdog("partitioner:lock:test", "token", "test", stop_event)

    # Assert
    warnings = [
        call for call in logger.warning.call_args_list if "Redis lock renewal failed: recoverable error" in call.args[0]
    ]
    assert len(warnings) == 1
    assert "extra" in logger.warning.call_args.kwargs
    assert logger.warning.call_args.kwargs["extra"]["table_name"] == "test"


def test__redis_lock__watchdog_returns_zero__logs_lock_lost_and_exits() -> None:
    """Sync analog of the async lock-lost test: the watchdog logs "lock lost" and exits without raising."""
    # Arrange
    redis_client, _, renew_script = _make_redis_mock(acquire_result=True)
    renew_script.side_effect = None
    renew_script.return_value = 0

    logger = MagicMock()
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, ttl_seconds=3)

    stop_event = threading.Event()

    # Act — must not raise
    with (
        patch("pg_partsmith.sync.lock.redis._RENEW_JITTER_RANGE", (0.001, 0.001)),
        patch("pg_partsmith.sync.lock.redis.logger", logger),
    ):
        manager._renewal_watchdog("partitioner:lock:test", "token", "test", stop_event)

    # Assert
    warnings = [
        call for call in logger.warning.call_args_list if "Redis lock renewal failed: lock lost" in call.args[0]
    ]
    assert len(warnings) == 1
    assert logger.warning.call_args.kwargs["extra"]["table_name"] == "test"


def test__redis_lock__unlock_raises__logs_warning_without_reraise() -> None:
    # Arrange
    redis = MagicMock()
    redis.set = MagicMock(return_value=True)

    unlock_script = MagicMock(side_effect=RuntimeError("unlock failed"))
    renew_script = MagicMock(return_value=1)
    redis.register_script.side_effect = [unlock_script, renew_script]

    logger = MagicMock()
    with patch("pg_partsmith.sync.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis)

    # Act
    with patch("pg_partsmith.sync.lock.redis.logger", logger), manager.acquire_lock("test"):
        pass

    # Assert
    warnings = [call for call in logger.warning.call_args_list if "Failed to release Redis lock" in call.args[0]]
    assert len(warnings) == 1
    assert logger.warning.call_args.kwargs["extra"]["table_name"] == "test"
