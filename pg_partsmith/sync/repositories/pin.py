"""Identity pinning for ``DETACH CONCURRENTLY`` -- the sync flavour.

Same protocol as the aio module, with a worker thread where aio uses a task:
the holder connection takes ``ACCESS SHARE`` and verifies the OID under it,
the marker is written under the pin, the statement runs on its own connection
in a worker thread, and the pin is released once the statement is queued for
the partition's lock (or the pending flag is committed) -- from there a swap
can only make the statement fail on a vanished OID, never redirect it. The
poll paces itself on real round trips. A deadline bounds the wait, but it
cancels the statement's backend and waits for the worker before the pin goes:
releasing it under a statement that is still to run would leave a
name-addressed ``DETACH`` free to fire at whatever holds the name by then.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from sqlalchemy import text

from pg_partsmith.utils import build_ddl_statement

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Connection, Engine

# See the aio module for the release-condition reasoning: only the statement
# backend's own queued lock (or the committed pending flag) releases the pin.
_RELEASE_SQL = (
    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'relation' AND relation = CAST(:oid AS oid) "
    "AND mode = 'AccessExclusiveLock' AND NOT granted AND pid = CAST(:pid AS int)) "
    "OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = CAST(:oid AS oid) AND inhdetachpending)"
)
_PENDING_ONLY_SQL = "SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = CAST(:oid AS oid) AND inhdetachpending)"
_CANCEL_SQL = "SELECT pg_cancel_backend(CAST(:pid AS int))"


def detach_concurrently_pinned(
    engine: Engine,
    partition_name: str,
    expected_oid: int,
    *,
    timeout_seconds: float,
    verify: Callable[[Connection], None],
    mark: Callable[[], None],
    statement: Callable[[], bool],
    statement_pid: Callable[[], int | None],
) -> bool:
    """Run one pinned ``DETACH … CONCURRENTLY``; the return value is ``statement``'s."""
    deadline = time.monotonic() + timeout_seconds
    with engine.connect() as holder:
        holder.execute(
            build_ddl_statement("LOCK TABLE ONLY {partition} IN ACCESS SHARE MODE", partition=partition_name)
        )
        verify(holder)
        mark()
        with ThreadPoolExecutor(max_workers=1) as pool:
            run = pool.submit(statement)
            try:
                while not run.done():
                    pid = statement_pid()
                    if pid is None:
                        release = holder.execute(text(_PENDING_ONLY_SQL), {"oid": expected_oid}).scalar()
                    else:
                        release = holder.execute(text(_RELEASE_SQL), {"oid": expected_oid, "pid": pid}).scalar()
                    if release:
                        break
                    if time.monotonic() > deadline:
                        # Cancel the statement and wait for the worker rather
                        # than releasing the pin under it; the statement's own
                        # server-side timeout bounds the wait when its backend
                        # is not known yet.
                        if pid is not None:
                            holder.execute(text(_CANCEL_SQL), {"pid": pid})
                        run.result()
                        break
            finally:
                # Release the pin: from here the statement either holds the
                # relation by OID or is finished.
                holder.rollback()
            return run.result()
