"""Identity pinning for ``DETACH CONCURRENTLY``.

The concurrent detach cannot run inside a transaction, so its name cannot be
pinned by the statement's own transaction the way the blocking form's can. A
holder connection does it instead: it takes ``ACCESS SHARE`` on the relation
and verifies the OID under it -- while any lock is held, no session can drop
or replace the relation, because either needs ``ACCESS EXCLUSIVE``. The
marker is then written (its ``SHARE UPDATE EXCLUSIVE`` does not conflict with
the pin), the statement starts on its own connection, and the pin is released
the moment the statement holds the relation by OID -- its ACCESS EXCLUSIVE
request is queued on the partition, or the pending flag is committed. From
there a swap can only make the statement fail on a vanished OID; it can never
redirect it to a replacement. The statement's first transaction requests
ACCESS EXCLUSIVE on the partition, which the pin itself blocks, so the
release must happen while the statement is in flight -- a concurrent task
here, a worker thread in the sync mirror's hand-kept twin of this module.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from sqlalchemy import text

from pg_partsmith.utils import build_ddl_statement

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

# The pin may be released once the statement holds the relation by OID: its
# ACCESS EXCLUSIVE request is queued on the partition (behind the pin), or the
# pending flag is already committed. Lock queues are ordered, so from that
# moment a swap can only make the statement fail on a vanished OID -- it can
# never redirect it to a replacement.
_RELEASE_SQL = (
    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'relation' AND relation = CAST(:oid AS oid) "
    "AND mode = 'AccessExclusiveLock' AND NOT granted) "
    "OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = CAST(:oid AS oid) AND inhdetachpending)"
)


async def detach_concurrently_pinned(
    engine: AsyncEngine,
    partition_name: str,
    expected_oid: int,
    *,
    timeout_seconds: float,
    verify: Callable[[AsyncConnection], Awaitable[None]],
    mark: Callable[[], Awaitable[None]],
    statement: Callable[[], Awaitable[bool]],
) -> bool:
    """Run one pinned ``DETACH … CONCURRENTLY``; the return value is ``statement``'s.

    ``verify`` checks identity and attachment on the holder connection, under
    the pin; ``mark`` writes the orphan marker in its own short transaction;
    ``statement`` runs the DDL and returns False when the caller should fall
    back to the blocking form. Exceptions from any of them propagate.
    """
    async with asyncio.timeout(timeout_seconds), engine.connect() as holder:
        await holder.execute(
            build_ddl_statement("LOCK TABLE ONLY {partition} IN ACCESS SHARE MODE", partition=partition_name)
        )
        await verify(holder)
        await mark()
        run = asyncio.ensure_future(statement())
        try:
            while not run.done():
                release = (await holder.execute(text(_RELEASE_SQL), {"oid": expected_oid})).scalar()
                if release:
                    break
                # Each poll is a real round trip, which is both the pacing and
                # the yield the statement task needs; a clock-based sleep would
                # not wake under a frozen test clock.
                await asyncio.sleep(0)
        finally:
            # Release the pin so the statement can take its lock and finish.
            await holder.rollback()
        return await run
