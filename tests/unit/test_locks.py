import asyncio
import contextlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_partsmith.aio.lock.postgres import PostgresAdvisoryLockManager
from pg_partsmith.aio.lock.redis import RedisDistributedLockManager
from pg_partsmith.exceptions import LockAcquisitionError

# ── helpers ─────────────────────────────────────────────────────────────────────


def _make_engine_mock(lock_acquired: bool = True) -> tuple[MagicMock, AsyncMock]:
    """Return (engine_mock, conn_mock) pair."""
    result = MagicMock()
    result.scalar.return_value = lock_acquired
    result.fetchall.return_value = []

    conn = AsyncMock()
    conn.execute.return_value = result
    conn.execution_options.return_value = conn
    conn.invalidate.return_value = None

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)

    engine = MagicMock()
    engine.connect.return_value = cm
    engine.begin.return_value = cm

    return engine, conn


def _make_redis_mock(
    acquire_result: bool = True,
) -> tuple[MagicMock, AsyncMock, AsyncMock]:
    """Return (redis_client, unlock_script, renew_script)."""
    redis_client = MagicMock()
    redis_client.set = AsyncMock(return_value=acquire_result)
    redis_client.exists = AsyncMock(return_value=1 if acquire_result else 0)
    redis_client.get = AsyncMock(return_value=None)
    unlock_script = AsyncMock(return_value=1)
    renew_script = AsyncMock(return_value=1)
    redis_client.register_script.side_effect = [unlock_script, renew_script]
    return redis_client, unlock_script, renew_script


# ── PostgresAdvisoryLockManager ──────────────────────────────────────────────────


async def test__postgres_lock__lock_acquired__calls_lock_and_unlock_sql() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    # Act
    async with manager.acquire_lock("events"):
        lock_call_count = conn.execute.call_count

    # Assert
    assert lock_call_count == 1  # advisory_lock call
    assert conn.execute.call_count == 2  # + advisory_unlock call


async def test__postgres_lock__lock_not_acquired__raises_lock_acquisition_error() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=False)
    manager = PostgresAdvisoryLockManager(engine)

    # Act / Assert
    with pytest.raises(LockAcquisitionError) as exc_info:
        async with manager.acquire_lock("events"):
            pass

    assert "events" in str(exc_info.value)
    assert conn.execute.call_count == 1


async def test__postgres_lock__body_raises__unlock_is_still_called() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    # Act
    with pytest.raises(ValueError, match="body error"):
        async with manager.acquire_lock("events"):
            raise ValueError("body error")

    # Assert
    assert conn.execute.call_count == 2  # type: ignore[unreachable]


async def test__postgres_lock__task_cancelled__unlock_completes_via_shield() -> None:
    # Arrange
    engine, _ = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    unlock_completed = asyncio.Event()
    lock_result = MagicMock()
    lock_result.scalar.return_value = True
    unlock_result = MagicMock()
    unlock_result.scalar.return_value = True

    async def _execute(statement: object, params: dict[str, Any] | None = None) -> MagicMock:
        sql = str(statement).lower()
        if "pg_try_advisory_lock" in sql:
            return lock_result
        if "pg_advisory_unlock" in sql:
            await asyncio.sleep(0)
            unlock_completed.set()
            return unlock_result
        raise AssertionError(f"Unexpected SQL: {statement!r}")

    conn_mock = AsyncMock()
    conn_mock.execute.side_effect = _execute
    conn_mock.execution_options.return_value = conn_mock

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn_mock)
    cm.__aexit__ = AsyncMock(return_value=False)
    engine.connect.return_value = cm

    async def _runner() -> None:
        async with manager.acquire_lock("events"):
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            await asyncio.sleep(0)

    # Act
    task = asyncio.create_task(_runner())
    with pytest.raises(asyncio.CancelledError):
        await task

    # Assert
    assert unlock_completed.is_set()


async def test__postgres_lock__is_locked_true__queries_pg_locks() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    result = MagicMock()
    result.scalar.return_value = 1
    conn.execute.side_effect = None
    conn.execute.return_value = result
    manager = PostgresAdvisoryLockManager(engine)

    # Act
    locked = await manager.is_locked("events")

    # Assert
    assert locked is True
    sql = str(conn.execute.call_args.args[0]).lower()
    assert "granted = true" in sql
    assert "database" in sql
    assert "objsubid" in sql


async def test__postgres_lock__is_locked_false__returns_false() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=False)
    result = MagicMock()
    result.scalar.return_value = 0
    conn.execute.side_effect = None
    conn.execute.return_value = result
    manager = PostgresAdvisoryLockManager(engine)

    # Act / Assert
    assert await manager.is_locked("events") is False


def test__postgres_lock__different_prefixes__produce_different_lock_ids() -> None:
    # Arrange
    engine = MagicMock()
    m1 = PostgresAdvisoryLockManager(engine, prefix="app1")
    m2 = PostgresAdvisoryLockManager(engine, prefix="app2")

    # Act / Assert
    assert m1._compute_lock_id("events") != m2._compute_lock_id("events")


async def test__postgres_lock__shield_raises_cancelled_error__invalidates_connection() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)
    call_count = 0

    async def _shield_mock(arg: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            if asyncio.iscoroutine(arg) or isinstance(arg, asyncio.Task):
                await arg
            raise asyncio.CancelledError()
        return await arg if asyncio.iscoroutine(arg) or isinstance(arg, asyncio.Task) else arg

    # Act
    with (
        patch("asyncio.shield", side_effect=_shield_mock),
        pytest.raises(asyncio.CancelledError),
    ):
        async with manager.acquire_lock("test"):
            pass

    # Assert
    conn.invalidate.assert_called()


async def test__postgres_lock__rate_limit_first_acquire_small_monotonic__does_not_sleep() -> None:
    # Arrange — host uptime (monotonic) is smaller than the interval; a first-ever acquire owes no delay
    engine, _ = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine, acquire_min_interval_seconds=30.0)
    sleep_mock = AsyncMock()

    # Act
    with (
        patch("pg_partsmith.aio.lock.postgres.time.monotonic", return_value=5.0),
        patch("pg_partsmith.aio.lock.postgres.asyncio.sleep", sleep_mock),
    ):
        await manager._respect_rate_limit("events")

    # Assert
    sleep_mock.assert_not_awaited()
    assert manager._last_acquire_time["events"] == 5.0


async def test__postgres_lock__rate_limit_other_table_recent__new_table_not_delayed() -> None:
    # Arrange — an unrelated table has a fresh timestamp; a first-ever table must not owe any delay
    engine, _ = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine, acquire_min_interval_seconds=30.0)
    manager._last_acquire_time["orders"] = 100.0
    sleep_mock = AsyncMock()

    # Act
    with (
        patch("pg_partsmith.aio.lock.postgres.time.monotonic", return_value=100.0),
        patch("pg_partsmith.aio.lock.postgres.asyncio.sleep", sleep_mock),
    ):
        await manager._respect_rate_limit("events")

    # Assert
    sleep_mock.assert_not_awaited()
    assert manager._last_acquire_time["events"] == 100.0
    assert manager._last_acquire_time["orders"] == 100.0


async def test__postgres_lock__rate_limit_same_table_within_interval__sleeps_remainder_outside_mutex() -> None:
    # Arrange
    engine, _ = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine, acquire_min_interval_seconds=2.0)
    manager._last_acquire_time["events"] = 100.0
    observed: list[tuple[float, bool]] = []

    async def _fake_sleep(delay: float) -> None:
        observed.append((delay, manager._rate_limit_lock.locked()))

    # Act — 0.5s since the last acquire, 1.5s still owed
    with (
        patch("pg_partsmith.aio.lock.postgres.time.monotonic", return_value=100.5),
        patch("pg_partsmith.aio.lock.postgres.asyncio.sleep", _fake_sleep),
    ):
        await manager._respect_rate_limit("events")

    # Assert — the owed delay is slept with the mutex already released, and the next slot is reserved
    assert observed == [(1.5, False)]
    assert manager._last_acquire_time["events"] == 102.0


async def test__postgres_lock__unlock_raises__invalidates_connection() -> None:
    # Arrange
    engine, conn = _make_engine_mock(lock_acquired=True)
    manager = PostgresAdvisoryLockManager(engine)

    async def _execute_mock(statement: Any, params: dict[str, Any] | None = None) -> MagicMock:
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
    with pytest.raises(RuntimeError, match="unlock failed"):
        async with manager.acquire_lock("test"):
            pass

    conn.invalidate.assert_called()


# ── RedisDistributedLockManager ──────────────────────────────────────────────────


async def test__redis_lock__lock_acquired__calls_set_and_unlock_on_exit() -> None:
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act
    async with manager.acquire_lock("events"):
        redis_client.set.assert_called_once()

    # Assert
    assert unlock_script.called


async def test__redis_lock__lock_not_acquired__raises_lock_acquisition_error() -> None:
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=False)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    with pytest.raises(LockAcquisitionError) as exc_info:
        async with manager.acquire_lock("events"):
            pass

    assert "events" in str(exc_info.value)
    unlock_script.assert_not_called()


async def test__redis_lock__body_raises__unlock_is_still_called() -> None:
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act
    with pytest.raises(RuntimeError, match="body error"):
        async with manager.acquire_lock("events"):
            raise RuntimeError("body error")

    # Assert
    assert unlock_script.called  # type: ignore[unreachable]


async def test__redis_lock__task_cancelled__unlock_completes_via_shield() -> None:
    # Arrange
    redis_client = MagicMock()
    redis_client.set = AsyncMock(return_value=True)
    unlock_completed = asyncio.Event()

    async def _unlock(*_args: object, **_kwargs: object) -> int:
        await asyncio.sleep(0)
        unlock_completed.set()
        return 1

    unlock_script = AsyncMock(side_effect=_unlock)
    renew_script = AsyncMock(return_value=1)
    redis_client.register_script.side_effect = [unlock_script, renew_script]

    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, ttl_seconds=3)

    async def _runner() -> None:
        async with manager.acquire_lock("events"):
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            await asyncio.sleep(0)

    # Act
    task = asyncio.create_task(_runner())
    with pytest.raises(asyncio.CancelledError):
        await task

    # Assert
    assert unlock_completed.is_set()


async def test__redis_lock__cancelled_during_set__releases_token_and_reraises() -> None:
    # Arrange — the SET may have been applied server-side before the cancellation landed, so the
    # token-checked unlock script must run (safe no-op otherwise) before the cancellation propagates
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=True)
    redis_client.set = AsyncMock(side_effect=asyncio.CancelledError())
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    with pytest.raises(asyncio.CancelledError):
        async with manager.acquire_lock("events"):
            pass

    unlock_script.assert_awaited_once()


async def test__redis_lock__is_locked_true__returns_true() -> None:
    # Arrange
    redis_client = MagicMock()
    redis_client.exists = AsyncMock(return_value=1)
    redis_client.register_script.return_value = AsyncMock(return_value=1)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    assert await manager.is_locked("events") is True


async def test__redis_lock__is_locked_false__returns_false() -> None:
    # Arrange
    redis_client = MagicMock()
    redis_client.exists = AsyncMock(return_value=0)
    redis_client.register_script.return_value = AsyncMock(return_value=1)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    assert await manager.is_locked("events") is False


def test__redis_lock__redis_unavailable__raises_import_error_on_instantiation() -> None:
    # Arrange / Act / Assert
    with (
        patch("pg_partsmith.aio.lock.redis._redis_available", False),
        pytest.raises(ImportError, match="redis-locks"),
    ):
        RedisDistributedLockManager(MagicMock())


def test__redis_lock__custom_prefix__lock_key_includes_prefix_and_table() -> None:
    # Arrange
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(MagicMock(), prefix="myapp:lock")

    # Act
    key = manager._get_lock_key("events")

    # Assert
    assert key == "myapp:lock:events"


def test__redis_lock__ttl_below_minimum__raises_value_error() -> None:
    # Arrange / Act / Assert
    with (
        patch("pg_partsmith.aio.lock.redis._redis_available", True),
        pytest.raises(ValueError, match="ttl_seconds"),
    ):
        RedisDistributedLockManager(MagicMock(), ttl_seconds=2)


def test__redis_lock__bool_ttl__raises_value_error() -> None:
    # True == 1, which is below the minimum of 3
    # Arrange / Act / Assert
    with (
        patch("pg_partsmith.aio.lock.redis._redis_available", True),
        pytest.raises(ValueError, match="ttl_seconds"),
    ):
        RedisDistributedLockManager(MagicMock(), ttl_seconds=True)  # type: ignore[arg-type]


async def test__redis_lock__watchdog_renewal_fails__cancels_holder_task_and_logs_warning() -> None:
    # Arrange
    redis = MagicMock()
    redis.set = AsyncMock(return_value=True)

    async def _renew_fail(*a: Any, **k: Any) -> int:
        raise RuntimeError("redis down")

    renew_script = AsyncMock(side_effect=_renew_fail)
    unlock_script = AsyncMock(return_value=1)
    redis.register_script.side_effect = [unlock_script, renew_script]

    logger = MagicMock()
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis, ttl_seconds=3)

    async def task_body() -> str:
        try:
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            return "cancelled"
        return "not cancelled"

    # Act
    with patch("pg_partsmith.aio.lock.redis.logger", logger):
        async with manager.acquire_lock("test"):
            res = await task_body()

    # Assert
    assert res == "cancelled"
    warnings = [
        call for call in logger.warning.call_args_list if "Redis lock renewal failed: recoverable error" in call.args[0]
    ]
    assert len(warnings) == 1
    assert "extra" in logger.warning.call_args.kwargs
    assert logger.warning.call_args.kwargs["extra"]["table_name"] == "test"


async def test__redis_lock__watchdog_returns_zero__cancels_holder_task_and_logs_lock_lost() -> None:
    # Arrange
    redis = MagicMock()
    redis.set = AsyncMock(return_value=True)

    renew_script = AsyncMock(return_value=0)
    unlock_script = AsyncMock(return_value=1)
    redis.register_script.side_effect = [unlock_script, renew_script]

    logger = MagicMock()
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis, ttl_seconds=3)

    async def task_body() -> str:
        try:
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            return "cancelled"
        return "not cancelled"

    # Act
    with patch("pg_partsmith.aio.lock.redis.logger", logger):
        async with manager.acquire_lock("test"):
            res = await task_body()

    # Assert
    assert res == "cancelled"
    warnings = [
        call for call in logger.warning.call_args_list if "Redis lock renewal failed: lock lost" in call.args[0]
    ]
    assert len(warnings) == 1
    assert logger.warning.call_args.kwargs["extra"]["table_name"] == "test"


async def test__redis_lock__unlock_raises__logs_warning_without_reraise() -> None:
    # Arrange
    redis = MagicMock()
    redis.set = AsyncMock(return_value=True)

    async def _unlock_fail(*a: Any, **k: Any) -> int:
        raise RuntimeError("unlock failed")

    unlock_script = AsyncMock(side_effect=_unlock_fail)
    renew_script = AsyncMock(return_value=1)
    redis.register_script.side_effect = [unlock_script, renew_script]

    logger = MagicMock()
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis)

    # Act
    with patch("pg_partsmith.aio.lock.redis.logger", logger):
        async with manager.acquire_lock("test"):
            pass

    # Assert
    warnings = [call for call in logger.warning.call_args_list if "Failed to release Redis lock" in call.args[0]]
    assert len(warnings) == 1
    assert logger.warning.call_args.kwargs["extra"]["table_name"] == "test"


async def test__redis_lock__rate_limit_first_acquire_small_monotonic__does_not_sleep() -> None:
    # Arrange — host uptime (monotonic) is smaller than the interval; a first-ever acquire owes no delay
    redis_client, _, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, acquire_min_interval_seconds=30.0)
    sleep_mock = AsyncMock()

    # Act
    with (
        patch("pg_partsmith.aio.lock.redis.time.monotonic", return_value=5.0),
        patch("pg_partsmith.aio.lock.redis.asyncio.sleep", sleep_mock),
    ):
        await manager._respect_rate_limit("events")

    # Assert
    sleep_mock.assert_not_awaited()
    assert manager._last_acquire_time["events"] == 5.0


async def test__redis_lock__rate_limit_other_table_recent__new_table_not_delayed() -> None:
    # Arrange — an unrelated table has a fresh timestamp; a first-ever table must not owe any delay
    redis_client, _, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, acquire_min_interval_seconds=30.0)
    manager._last_acquire_time["orders"] = 100.0
    sleep_mock = AsyncMock()

    # Act
    with (
        patch("pg_partsmith.aio.lock.redis.time.monotonic", return_value=100.0),
        patch("pg_partsmith.aio.lock.redis.asyncio.sleep", sleep_mock),
    ):
        await manager._respect_rate_limit("events")

    # Assert
    sleep_mock.assert_not_awaited()
    assert manager._last_acquire_time["events"] == 100.0
    assert manager._last_acquire_time["orders"] == 100.0


async def test__redis_lock__rate_limit_same_table_within_interval__sleeps_remainder_outside_mutex() -> None:
    # Arrange
    redis_client, _, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, acquire_min_interval_seconds=2.0)
    manager._last_acquire_time["events"] = 100.0
    observed: list[tuple[float, bool]] = []

    async def _fake_sleep(delay: float) -> None:
        observed.append((delay, manager._rate_limit_lock.locked()))

    # Act — 0.5s since the last acquire, 1.5s still owed
    with (
        patch("pg_partsmith.aio.lock.redis.time.monotonic", return_value=100.5),
        patch("pg_partsmith.aio.lock.redis.asyncio.sleep", _fake_sleep),
    ):
        await manager._respect_rate_limit("events")

    # Assert — the owed delay is slept with the mutex already released, and the next slot is reserved
    assert observed == [(1.5, False)]
    assert manager._last_acquire_time["events"] == 102.0


async def test__redis_lock__watchdog_task_start_fails__releases_key_and_propagates() -> None:
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    def _failing_create_task(coro: Any, **_kwargs: Any) -> None:
        coro.close()  # avoid an "unawaited coroutine" warning for the abandoned watchdog
        raise RuntimeError("cannot schedule task")

    # Act — the original error must propagate, but the key must be released, not leaked until TTL
    with (
        patch("pg_partsmith.aio.lock.redis.asyncio.create_task", side_effect=_failing_create_task),
        pytest.raises(RuntimeError, match="cannot schedule task"),
    ):
        async with manager.acquire_lock("events"):
            pass

    # Assert
    unlock_script.assert_awaited_once()


async def test__redis_lock__holder_cancelled_while_awaiting_watchdog__cancellation_propagates() -> None:
    """Regression: an external ``task.cancel()`` landing inside ``_cancel_watchdog`` must not be swallowed."""
    # Arrange
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=True)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client, ttl_seconds=3)

    watchdog_started = asyncio.Event()
    watchdog_cancelled = asyncio.Event()

    async def _stubborn_watchdog(*_args: object, **_kwargs: object) -> None:
        try:
            watchdog_started.set()
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            watchdog_cancelled.set()
            # Linger so the holder is parked in ``await watchdog`` when the external cancel lands.
            await asyncio.sleep(0.2)
            raise

    async def _holder() -> None:
        async with manager.acquire_lock("events"):
            # Yield so the watchdog task gets its first step (enters its try block) before teardown.
            await watchdog_started.wait()

    # Act — cancel the holder task while it is awaiting the watchdog inside _cancel_watchdog
    with patch.object(manager, "_renewal_watchdog", _stubborn_watchdog):
        task = asyncio.create_task(_holder())
        await watchdog_cancelled.wait()
        assert task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # Assert — the holder task ends cancelled and the key was still released
    assert task.cancelled()
    assert unlock_script.called

    # Cleanup — let the lingering watchdog task finish so it does not outlive the test
    for pending in [t for t in asyncio.all_tasks() if t.get_name().startswith("redis-lock-watchdog:")]:
        with contextlib.suppress(asyncio.CancelledError):
            await pending


async def test__redis_lock__a_set_the_client_retried__is_held_when_the_key_carries_our_token() -> None:
    # Arrange: SET NX landed on the first try, the retry answered "not set", and the key holds our token
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=False)
    stored: dict[str, str] = {}

    async def remember(name: str, value: str, **_: Any) -> None:
        stored[name] = value

    redis_client.set = AsyncMock(side_effect=remember)
    redis_client.get = AsyncMock(side_effect=stored.get)
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act
    async with manager.acquire_lock("events"):
        redis_client.get.assert_awaited_once()

    # Assert: held, and released like any other
    assert unlock_script.called


async def test__redis_lock__a_key_carrying_another_token__is_not_ours() -> None:
    # Arrange
    redis_client, _, _ = _make_redis_mock(acquire_result=False)
    redis_client.get = AsyncMock(return_value=b"somebody-else")
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    with pytest.raises(LockAcquisitionError):
        async with manager.acquire_lock("events"):
            pass


async def test__redis_lock__a_get_that_fails_after_a_retried_set__releases_and_is_not_acquired() -> None:
    # Arrange: the connection that timed out on the SET fails the GET too
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=False)
    redis_client.get = AsyncMock(side_effect=ConnectionError("gone"))
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert: not acquired, and whatever the SET left behind is released
    with pytest.raises(LockAcquisitionError):
        async with manager.acquire_lock("events"):
            pass
    assert unlock_script.called


async def test__redis_lock__a_cancellation_during_the_token_check__releases_the_key() -> None:
    # Arrange: SIGTERM lands while the GET is in flight
    redis_client, unlock_script, _ = _make_redis_mock(acquire_result=False)
    redis_client.get = AsyncMock(side_effect=asyncio.CancelledError())
    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        manager = RedisDistributedLockManager(redis_client)

    # Act / Assert
    with pytest.raises(asyncio.CancelledError):
        async with manager.acquire_lock("events"):
            pass
    assert unlock_script.called
