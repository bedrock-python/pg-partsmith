"""The CLI end to end: a document, a real database, and the codes it exits with."""

from __future__ import annotations

import asyncio
import json
import sys
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from sqlalchemy import text

from pg_partsmith.cli import ExitCode, main
from tests.integration.aio.support import exec_sql, make_service, make_table
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


def _document(tmp_path: Path, table: str, dsn: str, *, retention: int = 12, hooks: dict[str, Any] | None = None) -> str:
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
    if hooks is not None:
        payload["hooks"] = hooks
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "partitions.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _dsn(db_engine: AsyncEngine) -> str:
    return db_engine.url.render_as_string(hide_password=False)


async def _with_an_expired_partition(db_engine: AsyncEngine, table: str) -> None:
    """Two live windows, and one long past any retention."""
    await make_service(db_engine).maintain(monthly_config(table, create_ahead=2, retention=12))
    await exec_sql(
        db_engine,
        f'CREATE TABLE "{table}__2020_01" PARTITION OF "{table}" '
        "FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-02-01 00:00:00+00')",
    )


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


async def test__apply__by_default__creates_and_retires_nothing(
    db_engine: AsyncEngine, table: str, tmp_path: Path
) -> None:
    # Arrange: two live windows and one that retention has long expired
    await _with_an_expired_partition(db_engine, table)
    config = _document(tmp_path, table, _dsn(db_engine), retention=1)

    # Act: the safe mode is the default one
    code = await _run_cli("apply", "-c", config)

    # Assert: the expired partition is still attached
    assert code == ExitCode.OK
    async with db_engine.begin() as conn:
        attached = await conn.execute(
            text("SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = :table"),
            {"table": table},
        )
    assert attached.scalar_one() == 3


async def test__apply__allow_destructive__retires_what_retention_expired(
    db_engine: AsyncEngine, table: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    await _with_an_expired_partition(db_engine, table)
    config = _document(tmp_path, table, _dsn(db_engine), retention=1)

    # Act
    code = await _run_cli("apply", "-c", config, "--allow-destructive", "--output", "json")
    payload = json.loads(capsys.readouterr().out)

    # Assert
    assert code == ExitCode.OK
    assert payload["tables"][0]["result"]["detached_count"] == 1


async def test__plan_save__then_apply__is_the_artifact_between_the_two(
    db_engine: AsyncEngine, table: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    config = _document(tmp_path, table, _dsn(db_engine))
    saved = tmp_path / "plan.json"

    # Act: plan it, read it, then apply exactly that
    assert await _run_cli("plan", "-c", config, "--save", str(saved)) == ExitCode.OK
    written = json.loads(saved.read_text(encoding="utf-8"))
    capsys.readouterr()
    code = await _run_cli("apply", "-c", config, "--plan", str(saved))

    # Assert
    assert written["tables"][0]["plan"]["config_fingerprint"]
    assert code == ExitCode.OK
    assert "created 2" in capsys.readouterr().out


async def test__apply__a_plan_made_under_another_configuration__is_refused(
    db_engine: AsyncEngine, table: str, tmp_path: Path
) -> None:
    # Arrange: plan under one retention, then edit the document
    planned_under = _document(tmp_path, table, _dsn(db_engine), retention=12)
    saved = tmp_path / "plan.json"
    assert await _run_cli("plan", "-c", planned_under, "--save", str(saved)) == ExitCode.OK
    edited = _document(tmp_path / "edited", table, _dsn(db_engine), retention=3)

    # Act / Assert: the operations still name the right relations, for reasons
    # that stopped being true
    assert await _run_cli("apply", "-c", edited, "--plan", str(saved)) == ExitCode.CONFIG
    assert await _run_cli("apply", "-c", edited, "--plan", str(saved), "--allow-config-drift") == ExitCode.OK


async def test__apply__a_command_hook__runs_around_the_real_drop(
    db_engine: AsyncEngine, table: str, tmp_path: Path
) -> None:
    # Arrange: the export-before-destroy case, as a non-Python team would write it
    await _with_an_expired_partition(db_engine, table)
    marker = tmp_path / "archived.json"
    body = f"import sys, pathlib; pathlib.Path({str(marker)!r}).write_text(sys.stdin.read(), encoding='utf-8')"
    config = _document(
        tmp_path,
        table,
        _dsn(db_engine),
        retention=1,
        hooks={"before_drop": [sys.executable, "-c", body]},
    )

    # Act
    code = await _run_cli("apply", "-c", config, "--allow-destructive", "--allow-hooks")

    # Assert: it ran, and it was told which partition and why
    assert code == ExitCode.OK
    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["phase"] == "before_drop"
    assert payload["partition"]["name"].endswith(f"{table}__2020_01")
    assert payload["operation"]["reason"] in {"grace_elapsed", "follows_detach"}


async def test__apply__a_hook_that_refuses__leaves_the_partition_alone(
    db_engine: AsyncEngine, table: str, tmp_path: Path
) -> None:
    # Arrange: an archiver that says "not yet"
    await _with_an_expired_partition(db_engine, table)
    config = _document(
        tmp_path,
        table,
        _dsn(db_engine),
        retention=1,
        hooks={"before_drop": [sys.executable, "-c", "import sys; sys.exit(1)"]},
    )

    # Act
    code = await _run_cli("apply", "-c", config, "--allow-destructive", "--allow-hooks", "--continue-on-error")

    # Assert: detached, and still there to be dropped once the archiver agrees
    assert code == ExitCode.FINDINGS
    async with db_engine.begin() as conn:
        exists = await conn.execute(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": f"{table}__2020_01"})
    assert exists.scalar_one() is True


async def test__apply__hooks_declared_but_not_allowed__refuses_before_connecting(
    db_engine: AsyncEngine, table: str, tmp_path: Path
) -> None:
    # Arrange
    config = _document(tmp_path, table, _dsn(db_engine), hooks={"before_drop": [sys.executable, "-c", "pass"]})

    # Act / Assert
    assert await _run_cli("apply", "-c", config) == ExitCode.CONFIG


async def test__output_metrics__a_real_run__is_a_textfile_a_collector_can_serve(
    db_engine: AsyncEngine, table: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    config = _document(tmp_path, table, _dsn(db_engine))

    # Act
    code = await _run_cli("plan", "-c", config, "--check", "--output", "metrics")
    printed = capsys.readouterr().out

    # Assert: the drift the same run reports on its exit code, as a number
    assert code == ExitCode.DRIFT
    assert "# TYPE pg_partsmith_pending_operations gauge" in printed
    assert f'pg_partsmith_pending_operations{{table="{table}",kind="create"}} 2' in printed
    assert f'pg_partsmith_pending_operations{{table="{table}",kind="drop"}} 0' in printed


async def test__apply__a_python_hook__runs_around_the_real_drop(
    db_engine: AsyncEngine, table: str, tmp_path: Path
) -> None:
    # Arrange: the same export-before-destroy case, written as a block rather than a binary
    await _with_an_expired_partition(db_engine, table)
    marker = tmp_path / "archived.txt"
    block = f"""
import pathlib
pathlib.Path({str(marker)!r}).write_text(f"{{event.phase.value}} {{event.partition.name}}", encoding="utf-8")
"""
    config = _document(tmp_path, table, _dsn(db_engine), retention=1, hooks={"before_drop": {"python": block}})

    # Act
    code = await _run_cli("apply", "-c", config, "--allow-destructive", "--allow-hooks")

    # Assert
    assert code == ExitCode.OK
    assert marker.read_text(encoding="utf-8").endswith(f"{table}__2020_01")
    assert marker.read_text(encoding="utf-8").startswith("before_drop")


async def test__apply__a_python_hook_that_raises__leaves_the_partition_alone(
    db_engine: AsyncEngine, table: str, tmp_path: Path
) -> None:
    # Arrange: raising is how a block says "not yet"
    await _with_an_expired_partition(db_engine, table)
    config = _document(
        tmp_path,
        table,
        _dsn(db_engine),
        retention=1,
        hooks={"before_drop": {"python": "raise RuntimeError('archive first')"}},
    )

    # Act
    code = await _run_cli("apply", "-c", config, "--allow-destructive", "--allow-hooks", "--continue-on-error")

    # Assert: detached, and still there to be dropped once the block agrees
    assert code == ExitCode.FINDINGS
    async with db_engine.begin() as conn:
        exists = await conn.execute(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": f"{table}__2020_01"})
    assert exists.scalar_one() is True
