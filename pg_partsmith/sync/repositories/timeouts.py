"""Statement-timeout helpers for sync DDL operations.

The async package bounds DDL waits client-side via ``asyncio.timeout``.  The
sync package has no equivalent client-side mechanism, so the same
``ddl_timeout_seconds`` budget is enforced server-side through PostgreSQL's
``statement_timeout`` instead: each individual statement (rather than the
whole block) is bounded by the configured timeout.
"""

from __future__ import annotations

from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy import Connection


def apply_local_statement_timeout(conn: Connection, timeout_seconds: float) -> None:
    """Set a transaction-scoped ``statement_timeout`` on the current connection."""
    conn.execute(
        text("SELECT set_config('statement_timeout', :timeout, true)"),
        {"timeout": _timeout_ms(timeout_seconds)},
    )


@contextmanager
def session_statement_timeout(conn: Connection, timeout_seconds: float) -> Iterator[None]:
    """Set a session-scoped ``statement_timeout`` and reset it on exit.

    Needed for AUTOCOMMIT connections where ``SET LOCAL`` has no effect.  The
    connection is invalidated if the reset fails so a stale timeout is never
    returned to the pool.
    """
    conn.execute(
        text("SELECT set_config('statement_timeout', :timeout, false)"),
        {"timeout": _timeout_ms(timeout_seconds)},
    )
    try:
        yield
    finally:
        try:
            conn.execute(text("RESET statement_timeout"))
        except Exception:
            with suppress(Exception):
                conn.invalidate()


def _timeout_ms(timeout_seconds: float) -> str:
    return str(max(1, int(timeout_seconds * 1000)))
