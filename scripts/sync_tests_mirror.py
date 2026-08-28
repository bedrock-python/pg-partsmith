"""Regenerate ``tests/integration/sync`` from ``tests/integration/aio``.

The sync integration suite is a mechanical mirror of the aio one: plain test
functions instead of coroutines, ``Engine`` instead of ``AsyncEngine``, the
``sync_db_engine`` / ``sync_partition_builder`` fixtures instead of their aio
twins. Run this after changing the aio suite and review the diff::

    uv run python scripts/sync_tests_mirror.py
    uv run ruff check --fix tests/integration/sync && uv run ruff format tests/integration/sync

A test that cannot be mirrored mechanically (one that drives two coroutines
concurrently, say) is marked in the aio suite with ``# sync-mirror: skip`` on
the line of its ``def`` and is left out of the sync suite.
"""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
AIO = ROOT / "tests" / "integration" / "aio"
SYNC = ROOT / "tests" / "integration" / "sync"

RULES: list[tuple[str, str]] = [
    (r"from sqlalchemy\.ext\.asyncio import AsyncEngine, AsyncSession", "from sqlalchemy import Engine"),
    (r"from sqlalchemy\.ext\.asyncio import AsyncEngine", "from sqlalchemy import Engine"),
    (r"from sqlalchemy\.ext\.asyncio import AsyncConnection, AsyncEngine", "from sqlalchemy import Connection, Engine"),
    (r"\bAsyncEngine\b", "Engine"),
    (r"\bAsyncConnection\b", "Connection"),
    (r"\bAsyncGenerator\b", "Generator"),
    (r"\bAsyncIterator\b", "Iterator"),
    (r"import pytest_asyncio\n", ""),
    (r"@pytest_asyncio\.fixture", "@pytest.fixture"),
    (r"pg_partsmith\.aio\b", "pg_partsmith.sync"),
    (r"tests\.integration\.aio\b", "tests.integration.sync"),
    (r"\basync def ", "def "),
    (r"\basync for ", "for "),
    (r"\basync with ", "with "),
    (r"\bawait ", ""),
    (r"\bdb_engine\b", "sync_db_engine"),
    (r"\bpartition_builder\b", "sync_partition_builder"),
    (r"engine\.sync_engine", "engine"),
    (r"\(async\)", "(sync)"),
    (r"\bAsyncMock\b", "MagicMock"),
]


def transform(text: str) -> str:
    """Turn one aio test module into its sync twin."""
    lines = text.split("\n")
    kept: list[str] = []
    skipping = False
    for line in lines:
        if "# sync-mirror: skip" in line:
            skipping = True
        if skipping:
            # Drop the whole decorated function: until the next top-level definition.
            if re.match(r"^(async def |def |class |@|# ──)", line) and "# sync-mirror: skip" not in line:
                skipping = False
            else:
                continue
        kept.append(line)
    text = "\n".join(kept)
    for pattern, repl in RULES:
        text = re.sub(pattern, repl, text)
    text = text.replace("import asyncio\n", "")
    return text


def main() -> None:
    """Write every mirrored module."""
    SYNC.mkdir(parents=True, exist_ok=True)
    for source in sorted(AIO.glob("*.py")):
        target = SYNC / source.name
        target.write_text(transform(source.read_text(encoding="utf-8")), encoding="utf-8")
        sys.stdout.write(f"wrote {target.relative_to(ROOT)}\n")


if __name__ == "__main__":
    main()
