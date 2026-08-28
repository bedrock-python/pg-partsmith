"""Regenerate ``pg_partsmith/sync`` from ``pg_partsmith/aio``.

The sync package is a mechanical mirror: plain methods instead of coroutines,
``Engine`` instead of ``AsyncEngine``, and server-side ``statement_timeout``
instead of ``asyncio.timeout``. Run this after changing any of the mirrored
aio modules, then format and review the diff::

    uv run python scripts/sync_mirror.py
    uv run ruff check --fix pg_partsmith/sync && uv run ruff format pg_partsmith/sync

Hand-maintained (not mirrored): ``lock/``, ``maintainer.py`` and
``repositories/{resolver,fk_manager,timeouts}.py``.
"""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
AIO = ROOT / "pg_partsmith" / "aio"
SYNC = ROOT / "pg_partsmith" / "sync"

FILES = [
    "protocols.py",
    "hooks.py",
    "metadata.py",
    "service.py",
    "__init__.py",
    "services/__init__.py",
    "services/validation.py",
    "services/inspection.py",
    "services/execution.py",
    "services/migration.py",
    "repositories/creator.py",
    "repositories/remover.py",
    "repositories/repository.py",
]

RULES: list[tuple[str, str]] = [
    (r"from sqlalchemy\.ext\.asyncio import AsyncConnection, AsyncEngine", "from sqlalchemy import Connection, Engine"),
    (r"from sqlalchemy\.ext\.asyncio import AsyncEngine", "from sqlalchemy import Engine"),
    (r"\bAsyncEngine\b", "Engine"),
    (r"\bAsyncConnection\b", "Connection"),
    (r"\bAbstractAsyncContextManager\b", "AbstractContextManager"),
    (r"pg_partsmith\.aio\b", "pg_partsmith.sync"),
    (r"Async implementations", "Sync implementations"),
    (r'"""Async protocols', '"""Sync protocols'),
    (r"\basync def ", "def "),
    (r"\bawait ", ""),
    (r"\basync with ", "with "),
    (r"\(asyncio\.CancelledError, KeyboardInterrupt, SystemExit\)", "(KeyboardInterrupt, SystemExit)"),
    (r"asyncio\.shield\(", "("),
    (r"asyncio\.sleep\(", "time.sleep("),
    (r"Awaitable\[None\]", "None"),
    (r"Awaitable\[(\w+)\]", r"\1"),
    (r"from collections\.abc import Awaitable, Callable", "from collections.abc import Callable"),
    (r"coroutines", "plain methods"),
    (r"SQLAlchemy async engine", "SQLAlchemy engine"),
    (r"Async context manager for the lock", "Context manager for the lock"),
]

_TIMEOUT_BEGIN = "with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:"
_TIMEOUT_CONNECT = "with asyncio.timeout(self._ddl_timeout), self._engine.connect() as conn:"
_TIMEOUT_AUTOCOMMIT = "with asyncio.timeout(self._ddl_timeout), self._engine.connect() as base_conn:"
_LOCAL_TIMEOUT = "apply_local_statement_timeout(conn, self._ddl_timeout)"
_AUTOCOMMIT_LINE = 'conn = base_conn.execution_options(isolation_level="AUTOCOMMIT")'


def transform(text: str) -> str:
    """Turn one aio module into its sync twin."""
    uses_timeout = "asyncio.timeout(" in text
    uses_sleep = "asyncio.sleep(" in text
    for pattern, repl in RULES:
        text = re.sub(pattern, repl, text)

    if uses_timeout:
        # Per-transaction statement timeout replaces the client-side asyncio.timeout.
        text = text.replace(_TIMEOUT_BEGIN, "with self._engine.begin() as conn:")
        text = text.replace(_TIMEOUT_CONNECT, "with self._engine.connect() as conn:")
        text = text.replace(_TIMEOUT_AUTOCOMMIT, "with self._engine.connect() as base_conn:")
        text = re.sub(
            r"(\n[ ]+with self\._engine\.(?:begin|connect)\(\) as conn:\n)(?P<body_indent>[ ]+)",
            lambda m: f"{m.group(1)}{m.group('body_indent')}{_LOCAL_TIMEOUT}\n{m.group('body_indent')}",
            text,
        )
        # Autocommit connections cannot SET LOCAL: use the session-scoped helper around the body.
        text = _wrap_autocommit_blocks(text)

    text = text.replace("import asyncio\n", "")
    if uses_sleep and "import time\n" not in text:
        text = text.replace("import logging\n", "import logging\nimport time\n", 1)
    if uses_timeout:
        names = "apply_local_statement_timeout, session_statement_timeout"
        if "session_statement_timeout(" not in text:
            names = "apply_local_statement_timeout"
        helper_import = f"from .timeouts import {names}\n"
        if "if TYPE_CHECKING:" in text:
            text = text.replace("\nif TYPE_CHECKING:", f"\n{helper_import}\nif TYPE_CHECKING:", 1)
        else:
            text = text.rstrip("\n") + "\n" + helper_import
    return text


def _wrap_autocommit_blocks(text: str) -> str:
    """Wrap the body following ``conn = ...AUTOCOMMIT`` in the session-timeout context."""
    lines = text.split("\n")
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        if _AUTOCOMMIT_LINE in line:
            indent = len(line) - len(line.lstrip(" "))
            out.append(" " * indent + "with session_statement_timeout(conn, self._ddl_timeout):")
            i += 1
            while i < len(lines):
                nxt = lines[i]
                if nxt.strip() and (len(nxt) - len(nxt.lstrip(" "))) < indent:
                    break
                out.append(("    " + nxt) if nxt.strip() else nxt)
                i += 1
            continue
        i += 1
    return "\n".join(out)


def main() -> None:
    """Write every mirrored module."""
    for rel in FILES:
        source = (AIO / rel).read_text(encoding="utf-8")
        (SYNC / rel).write_text(transform(source), encoding="utf-8")
        sys.stdout.write(f"wrote pg_partsmith/sync/{rel}\n")


if __name__ == "__main__":
    main()
