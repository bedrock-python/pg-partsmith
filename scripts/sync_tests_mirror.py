"""Regenerate ``tests/integration/sync`` from ``tests/integration/aio``.

The sync integration suite is a mechanical mirror of the aio one: plain test
functions instead of coroutines, ``Engine`` instead of ``AsyncEngine``, the
``sync_db_engine`` / ``sync_db_session`` / ``sync_partition_builder`` fixtures
instead of their aio twins, ``redis.Redis`` instead of ``redis.asyncio.Redis``.
Run this after changing the aio suite and review the diff::

    uv run python scripts/sync_tests_mirror.py
    uv run ruff check --fix tests/integration/sync && uv run ruff format tests/integration/sync

A test that cannot be mirrored mechanically (one that drives two coroutines
concurrently, say) is marked in the aio suite with a ``# sync-mirror: skip``
line right above it (above its decorators, if any) and is left out of the sync
suite; its thread-based twin lives in ``tests/integration/sync/test_concurrency.py``,
the one module of the sync suite that is written by hand. A module the aio
suite no longer has is removed from the sync suite, that one excepted.
"""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
AIO = ROOT / "tests" / "integration" / "aio"
SYNC = ROOT / "tests" / "integration" / "sync"

HANDWRITTEN = frozenset({"test_concurrency.py"})
SKIP_MARKER = "# sync-mirror: skip"
TOP_LEVEL = re.compile(r"^(async def |def |class |@|# ──)")
DEFINITION = re.compile(r"^(async )?def ")
FOR_LOOP = re.compile(r"^(?P<indent>\s*)for (?P<var>\w+) in (?P<rest>.*)$")

RULES: list[tuple[str, str]] = [
    (
        r"(?m)^(\s*)from sqlalchemy\.ext\.asyncio import AsyncEngine, AsyncSession",
        r"\1from sqlalchemy import Engine\n\1from sqlalchemy.orm import Session",
    ),
    (
        r"from sqlalchemy\.ext\.asyncio import AsyncEngine, create_async_engine",
        "from sqlalchemy import Engine, create_engine",
    ),
    (r"from sqlalchemy\.ext\.asyncio import AsyncEngine", "from sqlalchemy import Engine"),
    (r"from sqlalchemy\.ext\.asyncio import AsyncConnection, AsyncEngine", "from sqlalchemy import Connection, Engine"),
    # redis-py keeps its async client in ``redis.asyncio``; the sync one is at the package root.
    (r"from redis\.asyncio import", "from redis import"),
    # A mirror-able sleep is spelled ``from asyncio import sleep`` / ``await sleep(...)`` in the aio suite.
    (r"from asyncio import sleep\b", "from time import sleep"),
    (r"\bcreate_async_engine\b", "create_engine"),
    (r"_create_async_engine\b", "_create_sync_engine"),
    (r"postgresql\+asyncpg://", "postgresql+psycopg2://"),
    (r"an async engine", "a sync engine"),
    (r"\bAsyncEngine\b", "Engine"),
    (r"\bAsyncSession\b", "Session"),
    (r"\bAsyncConnection\b", "Connection"),
    (r"\bAsyncGenerator\b", "Generator"),
    (r"\bAsyncIterator\b", "Iterator"),
    # contextlib's async exit stack and its methods, for a context that must outlive a ``with`` block.
    (r"\bAsyncExitStack\b", "ExitStack"),
    (r"\benter_async_context\b", "enter_context"),
    (r"\baclose\b", "close"),
    (r"import pytest_asyncio\n", ""),
    (r"@pytest_asyncio\.fixture", "@pytest.fixture"),
    (r"pg_partsmith\.aio\b", "pg_partsmith.sync"),
    (r"tests\.integration\.aio\b", "tests.integration.sync"),
    (r"\basync def ", "def "),
    (r"\basync for ", "for "),
    (r"\basync with ", "with "),
    (r"\bawait ", ""),
    (r"\bdb_engine\b", "sync_db_engine"),
    (r"\bdb_session\b", "sync_db_session"),
    (r"\bpartition_builder\b", "sync_partition_builder"),
    (r"engine\.sync_engine", "engine"),
    (r"\(async\)", "(sync)"),
    (r"\bAsyncMock\b", "MagicMock"),
    # unittest.mock spells the awaited assertions differently from the called ones.
    (r"\bassert_awaited_once_with\b", "assert_called_once_with"),
    (r"\bassert_awaited_with\b", "assert_called_with"),
    (r"\bassert_awaited_once\b", "assert_called_once"),
    (r"\bassert_not_awaited\b", "assert_not_called"),
    (r"\bawait_count\b", "call_count"),
    (r"\bawait_args_list\b", "call_args_list"),
    (r"\bawait_args\b", "call_args"),
]


def transform(text: str) -> str:
    """Turn one aio test module into its sync twin."""
    text = "\n".join(_without_skipped(text.split("\n")))
    for pattern, repl in RULES:
        text = re.sub(pattern, repl, text)
    text = text.replace("import asyncio\n", "")
    return "\n".join(_yield_from(text.split("\n")))


def _without_skipped(lines: list[str]) -> list[str]:
    """Leave out every function under a ``# sync-mirror: skip`` line.

    The marker sits right above the function -- above its decorators when it
    has any -- and the function ends at the next top-level definition,
    decorator or section heading.
    """
    kept: list[str] = []
    state = "keep"
    for line in lines:
        if line.strip() == SKIP_MARKER:
            state = "marked"
            continue
        top_level = TOP_LEVEL.match(line) is not None
        if state == "marked":
            if top_level:
                state = "body" if DEFINITION.match(line) else "decorators"
            continue
        if state == "decorators":
            if DEFINITION.match(line):
                state = "body"
            continue
        if state == "body":
            if not top_level:
                continue
            state = "keep"
        kept.append(line)
    return kept


def _yield_from(lines: list[str]) -> list[str]:
    """Spell ``for x in gen(...): yield x`` -- what an ``async for`` becomes -- as ``yield from``."""
    out: list[str] = []
    i = 0
    while i < len(lines):
        loop = FOR_LOOP.match(lines[i])
        if loop is None:
            out.append(lines[i])
            i += 1
            continue
        indent, var = loop.group("indent"), loop.group("var")
        end = i
        while end < len(lines) and not lines[end].rstrip().endswith(":"):
            end += 1
        body = end + 1
        after = lines[body + 1] if body + 1 < len(lines) else ""
        body_is_deeper = bool(after.strip()) and len(after) - len(after.lstrip()) > len(indent)
        if body >= len(lines) or lines[body] != f"{indent}    yield {var}" or body_is_deeper:
            out.append(lines[i])
            i += 1
            continue
        header = lines[i : end + 1]
        header[0] = f"{indent}yield from {loop.group('rest')}"
        header[-1] = header[-1].rstrip()[:-1]
        out.extend(header)
        i = body + 1
    return out


def main() -> None:
    """Write every mirrored module and remove the ones that lost their aio source."""
    SYNC.mkdir(parents=True, exist_ok=True)
    generated: set[str] = set()
    for source in sorted(AIO.glob("*.py")):
        target = SYNC / source.name
        target.write_text(transform(source.read_text(encoding="utf-8")), encoding="utf-8")
        generated.add(target.name)
        sys.stdout.write(f"wrote {target.relative_to(ROOT)}\n")
    for stale in sorted(SYNC.glob("*.py")):
        if stale.name not in generated and stale.name not in HANDWRITTEN:
            stale.unlink()
            sys.stdout.write(f"removed {stale.relative_to(ROOT)}\n")


if __name__ == "__main__":
    main()
