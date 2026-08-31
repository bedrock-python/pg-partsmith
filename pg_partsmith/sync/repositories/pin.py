"""Identity pinning for ``DETACH CONCURRENTLY`` -- the sync flavour.

Same protocol as the aio module, with a worker thread where aio uses a task:
the holder connection takes ``ACCESS SHARE`` and verifies the OID under it,
the marker is written under the pin, the statement runs on its own connection
in a worker thread, and the pin is released once the statement is queued for
the partition's lock (or the pending flag is committed) -- from there a swap
can only make the statement fail on a vanished OID, never redirect it. The
poll paces itself on real round trips; the deadline is a belt, since the
statement carries its own server-side timeout.
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
                    if release or time.monotonic() > deadline:
                        break
            finally:
                # Release the pin so the statement can take its lock and finish.
                holder.rollback()
            return run.result()
