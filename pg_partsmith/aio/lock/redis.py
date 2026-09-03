"""Redis-based distributed lock manager.

Requires the ``redis-locks`` optional dependency::

    pip install pg-partsmith[redis-locks]
"""

from __future__ import annotations

import asyncio
import logging
import random
import secrets
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pg_partsmith.constants import DEFAULT_LOCK_PREFIX
from pg_partsmith.exceptions import LockAcquisitionError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager

logger = logging.getLogger(__name__)


try:
    import redis.asyncio  # noqa: F401

    _redis_available = True
except ImportError:
    _redis_available = False

_DEFAULT_REDIS_LOCK_PREFIX = f"{DEFAULT_LOCK_PREFIX}:lock"

# Lua scripts for atomic lock operations.
_UNLOCK_LUA = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
_RENEW_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end"
)

_MIN_TTL_SECONDS = 3
_RENEW_JITTER_RANGE = (0.8, 1.2)


@runtime_checkable
class RedisClientProtocol(Protocol):
    """Protocol for the Redis client used by the lock manager."""

    def register_script(self, script: str) -> Any:
        """Register Lua script."""
        ...

    async def set(self, name: str, value: str | bytes, ex: int | None = None, nx: bool = False) -> Any:
        """Set key in Redis."""
        ...

    async def exists(self, *names: str) -> int:
        """Check if keys exist."""
        ...

    async def get(self, name: str) -> Any:
        """Read a key; None when it is not there."""
        ...


class RedisDistributedLockManager:
    """Lock manager using Redis for distributed coordination.

    Uses ``SET NX EX`` to acquire the lock and a background renewal task to
    extend the TTL while the lock is held, preventing expiry during long DDL
    operations (e.g. ``DETACH PARTITION CONCURRENTLY``). The lock is released
    atomically via a Lua script that checks the ownership token, so it is safe
    even if Redis restarts during the renewal window.

    The renewal interval is ``ttl_seconds // 3`` (with random jitter to avoid
    thundering herds). If renewal fails — e.g. Redis is unreachable or another
    holder takes over — the watchdog logs a warning and cancels the holder
    task, forcing the maintenance run to stop (fail-safe).

    For production use you may want to subclass and override ``acquire_lock``
    to use Redlock or another algorithm with stronger guarantees.

    Raises:
        ImportError: If the ``redis-locks`` optional dependency is not installed.
    """

    def __init__(
        self,
        redis_client: RedisClientProtocol,
        prefix: str = _DEFAULT_REDIS_LOCK_PREFIX,
        ttl_seconds: int = 300,
        acquire_min_interval_seconds: float = 0.0,
    ) -> None:
        """Initialize lock manager.

        Args:
            redis_client: Redis client instance.
            prefix: Prefix for Redis keys.
            ttl_seconds: Lock time-to-live in seconds. The lock is automatically
                renewed every ``ttl_seconds // 3`` seconds so that it does not
                expire during long DDL operations.
            acquire_min_interval_seconds: Minimum seconds between acquire attempts
                per table (rate limiting). 0 disables.

        Raises:
            ImportError: If ``redis-py`` is not installed.
            ValueError: If ``ttl_seconds`` is below the minimum.
        """
        if not _redis_available:
            msg = (
                "redis-py is required for RedisDistributedLockManager. "
                "Install it with: pip install pg-partsmith[redis-locks]"
            )
            raise ImportError(msg)

        if ttl_seconds < _MIN_TTL_SECONDS:
            msg = f"ttl_seconds must be >= {_MIN_TTL_SECONDS}, got {ttl_seconds!r}"
            raise ValueError(msg)

        self._redis = redis_client
        self._prefix = prefix
        self._ttl = ttl_seconds
        self._renew_interval = max(1, ttl_seconds // 3)

        self._unlock_script = self._redis.register_script(_UNLOCK_LUA)
        self._renew_script = self._redis.register_script(_RENEW_LUA)
        self._acquire_min_interval = max(0.0, acquire_min_interval_seconds)
        self._last_acquire_time: dict[str, float] = {}
        self._rate_limit_lock = asyncio.Lock()

    def _get_lock_key(self, table_name: str) -> str:
        return f"{self._prefix}:{table_name}"

    def acquire_lock(self, table_name: str) -> AbstractAsyncContextManager[None]:
        """Acquire Redis lock with automatic TTL renewal.

        Args:
            table_name: Table name.

        Returns:
            Async context manager for the lock.

        Raises:
            LockAcquisitionError: If the lock is already held.
        """
        return self._lock_scope(table_name)

    @asynccontextmanager
    async def _lock_scope(self, table_name: str) -> AsyncIterator[None]:
        """Internal acquire/release flow for a single Redis lock."""
        await self._respect_rate_limit(table_name)

        key = self._get_lock_key(table_name)
        token = secrets.token_hex(16)

        try:
            acquired = await self._redis.set(key, token, ex=self._ttl, nx=True)
        except asyncio.CancelledError:
            # The SET may have been applied server-side before the cancellation
            # landed; the unlock script checks the token, so this is a safe no-op
            # when it was not.
            await asyncio.shield(self._release_safely(key, token, table_name))
            raise

        if not acquired and not await self._holds_our_token(key, token):
            raise LockAcquisitionError(table_name, "Redis lock unavailable")

        # From here on the key is held: any failure must release it rather than leak it until TTL.
        watchdog: asyncio.Task[None] | None = None
        try:
            holder_task = asyncio.current_task()
            if holder_task is None:
                raise RuntimeError("Could not determine current asyncio task")

            watchdog = asyncio.create_task(
                self._renewal_watchdog(key, token, table_name, holder_task),
                name=f"redis-lock-watchdog:{key}",
            )
            yield
        finally:
            try:
                if watchdog is not None:
                    await self._cancel_watchdog(watchdog)
            finally:
                await asyncio.shield(self._release_safely(key, token, table_name))

    async def _respect_rate_limit(self, table_name: str) -> None:
        """Sleep enough to enforce the configured min-interval between acquires.

        The per-table slot is reserved under the mutex; the sleep itself happens
        outside it so one table's owed delay never blocks acquires for other tables.
        """
        if self._acquire_min_interval <= 0:
            return
        async with self._rate_limit_lock:
            now = time.monotonic()
            last = self._last_acquire_time.get(table_name)
            slot = now if last is None else max(now, last + self._acquire_min_interval)
            self._last_acquire_time[table_name] = slot
        delay = slot - now
        if delay > 0:
            await asyncio.sleep(delay)

    async def _renewal_watchdog(
        self,
        key: str,
        token: str,
        table_name: str,
        holder_task: asyncio.Task[Any],
    ) -> None:
        """Periodically extend the lock TTL until cancelled or renewal fails."""
        while True:
            jitter = random.uniform(*_RENEW_JITTER_RANGE)  # noqa: S311
            await asyncio.sleep(self._renew_interval * jitter)
            try:
                renewed = await self._renew_script(keys=[key], args=[token, str(self._ttl)])
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                raise
            except (OSError, ConnectionError, TimeoutError, RuntimeError) as exc:
                self._fail_holder(holder_task, key, table_name, "recoverable error", exc)
                return
            except Exception as exc:
                self._fail_holder(holder_task, key, table_name, "unexpected exception", exc)
                return

            if not renewed:
                self._fail_holder(holder_task, key, table_name, "lock lost", None)
                return

    def _fail_holder(
        self,
        holder_task: asyncio.Task[Any],
        key: str,
        table_name: str,
        reason: str,
        exc: Exception | None,
    ) -> None:
        """Log the renewal failure and cancel the holder task (fail-safe)."""
        extra: dict[str, str] = {"table_name": table_name, "key": key, "reason": reason}
        if exc is not None:
            extra["error"] = str(exc)
            logger.warning(
                f"Redis lock renewal failed: {reason}; cancelling maintenance task",
                extra=extra,
                exc_info=True,
            )
        else:
            logger.warning(
                f"Redis lock renewal failed: {reason}; cancelling maintenance task",
                extra=extra,
            )
        if not holder_task.done():
            holder_task.cancel()

    async def _cancel_watchdog(self, watchdog: asyncio.Task[None]) -> None:
        """Cancel the renewal watchdog and absorb its CancelledError.

        If the holder task itself is being cancelled while awaiting the watchdog
        (e.g. app shutdown calls ``task.cancel()``), the cancellation must
        propagate — only the watchdog's own cancellation is swallowed.
        """
        watchdog.cancel()
        try:
            await watchdog
        except asyncio.CancelledError:
            if (cur := asyncio.current_task()) is not None and cur.cancelling() > 0:
                raise

    async def _holds_our_token(self, key: str, token: str) -> bool:
        """Whether the key carries this attempt's token.

        The client retries a ``SET NX`` that timed out, and the first attempt
        can have landed: the retry then answers "not set" for a lock this run
        holds, which used to be given up on until the TTL expired it.
        """
        held = await self._redis.get(key)
        if isinstance(held, bytes):
            held = held.decode()
        return bool(held == token)

    async def _release_safely(self, key: str, token: str, table_name: str) -> None:
        """Release the lock; failures are logged but do not propagate.

        If unlock fails, the TTL will eventually expire the lock.
        """
        try:
            await self._unlock_script(keys=[key], args=[token])
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except (OSError, ConnectionError, TimeoutError, RuntimeError) as exc:
            logger.warning(
                "Failed to release Redis lock (recoverable); TTL will expire it eventually",
                extra={
                    "table_name": table_name,
                    "key": key,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
            )
        except Exception:
            logger.exception(
                "Unexpected failure while releasing Redis lock",
                extra={"table_name": table_name, "key": key},
            )

    async def is_locked(self, table_name: str) -> bool:
        """Return True if the Redis lock for ``table_name`` is currently held."""
        key = self._get_lock_key(table_name)
        return bool(await self._redis.exists(key))
