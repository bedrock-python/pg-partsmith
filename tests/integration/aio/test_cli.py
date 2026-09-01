"""The CLI end to end: a document, a real database, and the codes it exits with."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from pg_partsmith.cli import ExitCode, main
from tests.integration.aio.support import make_service, make_table
from tests.integration.nested_support import MONTHLY_TABLE_DDL, monthly_config

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def table(db_engine: AsyncEngine) -> AsyncGenerator[str, None]:
    async for name in make_table(db_engine, MONTHLY_TABLE_DDL, prefix="cli"):
        yield name


async def _run_cli(*args: str) -> int:
    """Drive the CLI from a worker thread: it owns an event loop of its own."""
    return await asyncio.to_thread(main, list(args))


def _document(tmp_path: Path, table: str, dsn: str, *, retention: int = 12) -> str:
    """A document describing that one table, written where the CLI will read it."""
    schema, _, relname = table.rpartition(".")
    payload = {
        "dsn": dsn,
        "tables": [
            {
                "table_name": relname,
                "schema": schema or None,
                "partition_column": "created_at",
                "granularity": "month",
                "create_ahead_count": 2,
                "retention_count": retention,
            }
        ],
    }
    path = tmp_path / "partitions.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _dsn(db_engine: AsyncEngine) -> str:
    return db_engine.url.render_as_string(hide_password=False)


async def test__validate__a_table_the_document_matches__exits_ok(
    db_engine: AsyncEngine, table: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    config = _document(tmp_path, table, _dsn(db_engine))

    # Act
    code = await _run_cli("validate", "-c", config)

    # Assert
    assert code == ExitCode.OK
    assert "ok" in capsys.readouterr().out


async def test__validate__a_table_that_is_not_partitioned__exits_config(db_engine: AsyncEngine, tmp_path: Path) -> None:
    # Arrange: a table the document describes and PostgreSQL does not have
    config = _document(tmp_path, "public.no_such_table", _dsn(db_engine))

    # Act / Assert
    assert await _run_cli("validate", "-c", config) == ExitCode.CONFIG


async def test__plan__a_fresh_table__reports_drift_under_check_and_issues_no_ddl(
    db_engine: AsyncEngine, table: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    config = _document(tmp_path, table, _dsn(db_engine))

    # Act
    code = await _run_cli("plan", "-c", config, "--check", "--output", "json")
    payload = json.loads(capsys.readouterr().out)

    # Assert: this is what "maintenance has not been running" looks like
    assert code == ExitCode.DRIFT
    assert payload["command"] == "plan"
    assert [op["kind"] for op in payload["tables"][0]["plan"]["operations"]] == ["create", "create"]

    # And nothing was created by asking
    assert await _run_cli("inspect", "-c", config, "--output", "json") == ExitCode.OK
    tree = json.loads(capsys.readouterr().out)["tables"][0]["tree"]
    assert tree["root"]["children"] == []


async def test__plan__after_maintenance__is_converged_and_exits_ok(
    db_engine: AsyncEngine, table: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    # The CLI plans against the real clock, so this tick must run on it too.
    await make_service(db_engine).maintain(monthly_config(table, create_ahead=2, retention=12))
    config = _document(tmp_path, table, _dsn(db_engine))

    # Act
    code = await _run_cli("plan", "-c", config, "--check")

    # Assert
    assert code == ExitCode.OK
    assert "nothing to do" in capsys.readouterr().out


async def test__inspect__a_maintained_table__prints_the_tree_that_exists(
    db_engine: AsyncEngine, table: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    # The CLI plans against the real clock, so this tick must run on it too.
    await make_service(db_engine).maintain(monthly_config(table, create_ahead=2, retention=12))
    config = _document(tmp_path, table, _dsn(db_engine))

    # Act
    code = await _run_cli("inspect", "-c", config)
    printed = capsys.readouterr().out

    # Assert
    assert code == ExitCode.OK
    assert table in printed
    assert "FOR VALUES FROM" in printed


async def test__table__narrows_the_run_to_one_table_of_the_document(
    db_engine: AsyncEngine, table: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    _, _, relname = table.rpartition(".")
    config = _document(tmp_path, table, _dsn(db_engine))

    # Act
    code = await _run_cli("plan", "-c", config, "--table", relname, "--output", "json")

    # Assert
    assert code == ExitCode.OK
    assert len(json.loads(capsys.readouterr().out)["tables"]) == 1


async def test__dsn__a_database_that_is_not_there__exits_connection(tmp_path: Path) -> None:
    # Arrange
    config = _document(tmp_path, "public.events", "postgresql://nobody@127.0.0.1:1/none")

    # Act / Assert: a connection failure is its own code, not a generic failure
    assert await _run_cli("validate", "-c", config) == ExitCode.CONNECTION
