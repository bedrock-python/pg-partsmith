"""PostgreSQL advisory lock manager."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.exceptions import LockAcquisitionError
from pg_partsmith.utils import calculate_lock_id

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)


class PostgresAdvisoryLockManager:
    """Lock manager using PostgreSQL advisory locks.

    Holds the advisory lock on a dedicated AUTOCOMMIT connection from the
    engine pool. This guarantees the lock survives any number of commits or
    rollbacks on the caller's session, which is required when the caller
    needs to commit DDL (e.g. ATTACH PARTITION) before running
    DETACH PARTITION CONCURRENTLY.

    Override `_compute_lock_id` to customise the lock ID derivation.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        prefix: str = "partitioner",
        acquire_min_interval_seconds: float = 0.0,
    ) -> None:
        """Initialize lock manager.

        Args:
            engine: SQLAlchemy async engine used to open a dedicated connection
                for the advisory lock.
            prefix: Prefix for lock key generation.
            acquire_min_interval_seconds: Minimum seconds between acquire attempts
                per table (rate limiting). 0 disables.
        """
        self._engine = engine
        self._prefix = prefix
        self._acquire_min_interval = max(0.0, acquire_min_interval_seconds)
        self._last_acquire_time: dict[str, float] = {}
        self._rate_limit_lock = asyncio.Lock()

    def _compute_lock_id(self, table_name: str) -> int:
        """Compute the advisory lock ID for a table name.

        Override this method to customise the ID derivation strategy.

        Args:
            table_name: Table name to lock.

        Returns:
            Advisory lock ID.
        """
        return calculate_lock_id(table_name, prefix=self._prefix)

    def acquire_lock(self, table_name: str) -> AbstractAsyncContextManager[None]:
        """Acquire advisory lock for a table.

        Opens a dedicated AUTOCOMMIT connection from the engine pool and
        acquires a session-level advisory lock on it. The lock is released
        when the context manager exits, with cancellation-safe cleanup.

        Args:
            table_name: Table name to lock.

        Returns:
            Async context manager for the lock.

        Raises:
            LockAcquisitionError: If the lock cannot be acquired.
        """
        return self._lock_scope(table_name)

    @asynccontextmanager
    async def _lock_scope(self, table_name: str) -> AsyncIterator[None]:
        """Internal acquire/release flow for a single advisory lock."""
        await self._respect_rate_limit(table_name)
        lock_id = self._compute_lock_id(table_name)

        async with self._engine.connect() as base_conn:
            conn = await base_conn.execution_options(isolation_level="AUTOCOMMIT")
            await self._try_acquire(conn, lock_id, table_name)

            body_exc: BaseException | None = None
            try:
                yield
            except BaseException as exc:
                body_exc = exc
                raise
            finally:
                await self._release_safely(conn, lock_id, table_name, body_exc)

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

    async def _try_acquire(self, conn: AsyncConnection, lock_id: int, table_name: str) -> None:
        """Run ``pg_try_advisory_lock`` and raise if not granted."""
        result = await conn.execute(text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": lock_id})
        if not result.scalar():
            raise LockAcquisitionError(table_name, "advisory lock unavailable")

    async def _release_safely(
        self,
        conn: AsyncConnection,
        lock_id: int,
        table_name: str,
        body_exc: BaseException | None,
    ) -> None:
        """Release the lock with shielding so cancellation cannot leak a held lock."""
        try:
            await asyncio.shield(self._unlock(conn, lock_id, table_name))
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            # Defensively invalidate so the connection is not returned to the pool with a dangling lock.
            with contextlib.suppress(Exception):
                await asyncio.shield(conn.invalidate())
            raise
        except Exception:
            # Body exception takes precedence; otherwise propagate the unlock failure.
            if body_exc is None:
                raise
            logger.warning(
                "Failed to release advisory lock",
                extra={"table_name": table_name, "lock_id": lock_id},
            )

    async def _unlock(self, conn: AsyncConnection, lock_id: int, table_name: str) -> None:
        """Run ``pg_advisory_unlock``; invalidate the connection on any failure."""
        try:
            await conn.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id})
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except (SQLAlchemyError, OSError) as e:
            logger.warning(
                "Failed to release advisory lock (recoverable)",
                extra={"table_name": table_name, "lock_id": lock_id, "error": str(e)},
            )
            with contextlib.suppress(Exception):
                await conn.invalidate()
            raise
        except Exception:
            logger.exception(
                "Unexpected error while releasing advisory lock",
                extra={"table_name": table_name, "lock_id": lock_id},
            )
            with contextlib.suppress(Exception):
                await conn.invalidate()
            raise

    async def is_locked(self, table_name: str) -> bool:
        """Check if lock is held by any session.

        Args:
            table_name: Table name.

        Returns:
            True if the advisory lock for the given table is currently held.
        """
        lock_id = self._compute_lock_id(table_name)
        # Split 64-bit lock_id into classid and objid as stored in pg_locks for int8 advisory locks (objsubid=1).
        class_id = (lock_id >> 32) & 0xFFFFFFFF
        if class_id > 0x7FFFFFFF:
            class_id -= 0x100000000
        obj_id = lock_id & 0xFFFFFFFF
        if obj_id > 0x7FFFFFFF:
            obj_id -= 0x100000000

        async with self._engine.connect() as base_conn:
            conn = await base_conn.execution_options(isolation_level="AUTOCOMMIT")
            result = await conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND granted = true
                      AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                      AND classid = CAST(:class_id AS int4)
                      AND objid = CAST(:obj_id AS int4)
                      AND objsubid = 1
                    """
                ),
                {"class_id": class_id, "obj_id": obj_id},
            )
            count = result.scalar()
        return bool(count is not None and count > 0)
