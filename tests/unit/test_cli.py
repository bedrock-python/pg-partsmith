"""The command line: reading a document, choosing a connection, and what it exits with."""

from __future__ import annotations

import asyncio
import importlib
import io
import json
import re
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_partsmith import console
from pg_partsmith.__version__ import __version__
from pg_partsmith.aio.command_hooks import CommandHooks
from pg_partsmith.cli import ExitCode, main
from pg_partsmith.cli.commands import CommandResult, run_apply, run_plan, run_validate
from pg_partsmith.cli.loader import (
    DSN_ENV_VAR,
    DSN_FILE_ENV_VAR,
    ConfigError,
    async_url,
    load_document,
    load_plans,
    load_python_hooks,
    resolve_dsn,
    select_configs,
)
from pg_partsmith.cli.main import _hooks as _cli_hooks
from pg_partsmith.cli.render import describe_locks, envelope, plan_entry
from pg_partsmith.document import PartitionsDocument
from pg_partsmith.entities import MaintenanceIssue, MaintenanceIssueStep, MaintenanceResult
from pg_partsmith.events import HookPhase
from pg_partsmith.exceptions import InvalidPartitionConfigError, LockAcquisitionError
from pg_partsmith.hook_commands import CommandHookError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.plan import (
    CreatePartition,
    DetachPartition,
    DropPartition,
    Finding,
    FindingReason,
    MaintenancePlan,
    Reason,
    Severity,
)
from pg_partsmith.topology import RangeBounds

if TYPE_CHECKING:
    from collections.abc import Sequence

NOW = datetime(2026, 8, 28, tzinfo=UTC)

# The package re-exports a function called main, which shadows the submodule on
# attribute access; importlib reaches the module itself.
cli = importlib.import_module("pg_partsmith.cli.main")

DOCUMENT: dict[str, Any] = {
    "defaults": {"schema": "public", "granularity": "month"},
    "tables": [
        {"table_name": "events", "partition_column": "created_at"},
        {"table_name": "audit", "partition_column": "logged_at"},
    ],
}


def _write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _plan(*, noop: bool = False, findings: tuple[Finding, ...] = ()) -> MaintenancePlan:
    operations = (
        ()
        if noop
        else (
            CreatePartition(
                target="public.events__2026_09",
                parent_name="public.events",
                bounds=RangeBounds(from_value="2026-09-01", to_value="2026-10-01"),
                reason=Reason.CREATE_AHEAD,
            ),
        )
    )
    return MaintenancePlan(table_name="public.events", generated_at=NOW, operations=operations, findings=findings)


def _kit(plan: MaintenancePlan) -> MagicMock:
    kit = MagicMock()
    kit.service.plan = AsyncMock(return_value=plan)
    return kit


def _configs() -> Sequence[Any]:
    return PartitionsDocument.model_validate(DOCUMENT).configs()[:1]


# ── Reading the document ────────────────────────────────────────────────────────


def test__load_document__json__is_read_without_pyyaml(tmp_path: Path) -> None:
    # Arrange
    path = _write(tmp_path, "partitions.json", json.dumps(DOCUMENT))

    # Act
    document = load_document(path)

    # Assert
    assert [config.qualified_name for config in document.configs()] == ["public.events", "public.audit"]


def test__load_document__yaml__is_read_by_the_safe_loader(tmp_path: Path) -> None:
    # Arrange
    yaml = pytest.importorskip("yaml")
    path = _write(tmp_path, "partitions.yaml", yaml.safe_dump(DOCUMENT))

    # Act / Assert
    assert load_document(path).configs()[0].qualified_name == "public.events"


def test__load_document__an_extension_it_does_not_read__says_which_it_does(tmp_path: Path) -> None:
    # Arrange
    path = _write(tmp_path, "partitions.ini", "[events]\n")

    # Act / Assert
    with pytest.raises(ConfigError, match=r"\.json"):
        load_document(path)


def test__load_document__a_file_that_is_not_there__is_a_configuration_error(tmp_path: Path) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ConfigError, match="Cannot read"):
        load_document(tmp_path / "absent.json")


def test__load_document__a_document_that_does_not_validate__names_the_file(tmp_path: Path) -> None:
    # Arrange
    path = _write(tmp_path, "partitions.json", json.dumps({"tables": [{"table_name": "events", "granulaity": "x"}]}))

    # Act / Assert
    with pytest.raises(ConfigError, match=re.escape("partitions.json")):
        load_document(path)


def test__load_document__a_top_level_that_is_not_a_mapping__is_refused(tmp_path: Path) -> None:
    # Arrange
    path = _write(tmp_path, "partitions.json", json.dumps([{"table_name": "events"}]))

    # Act / Assert
    with pytest.raises(ConfigError, match="not a document"):
        load_document(path)


# ── Choosing a connection ───────────────────────────────────────────────────────


def test__resolve_dsn__the_flag_outranks_the_environment_and_the_file(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv(DSN_ENV_VAR, "postgresql://env/db")
    document = PartitionsDocument.model_validate({**DOCUMENT, "dsn": "postgresql://file/db"})

    # Act / Assert
    assert resolve_dsn(document, override="postgresql://flag/db") == "postgresql://flag/db"
    assert resolve_dsn(document) == "postgresql://env/db"
    monkeypatch.delenv(DSN_ENV_VAR)
    assert resolve_dsn(document) == "postgresql://file/db"


def test__resolve_dsn__a_secret_file__is_read_after_the_variable_and_before_the_document(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Arrange: the Docker and Swarm secrets convention, a file under /run/secrets
    secret = _write(tmp_path, "dsn", "postgresql://secret@host/db\n")
    monkeypatch.delenv(DSN_ENV_VAR, raising=False)
    monkeypatch.setenv(DSN_FILE_ENV_VAR, str(secret))
    document = PartitionsDocument.model_validate({**DOCUMENT, "dsn": "postgresql://file/db"})

    # Act / Assert: stripped of its newline, and outranked only by the variable and the flag
    assert resolve_dsn(document) == "postgresql://secret@host/db"
    monkeypatch.setenv(DSN_ENV_VAR, "postgresql://env/db")
    assert resolve_dsn(document) == "postgresql://env/db"


def test__resolve_dsn__a_secret_file_that_cannot_be_read__is_a_configuration_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Arrange
    monkeypatch.delenv(DSN_ENV_VAR, raising=False)
    monkeypatch.setenv(DSN_FILE_ENV_VAR, str(tmp_path / "absent"))

    # Act / Assert
    with pytest.raises(ConfigError, match=DSN_FILE_ENV_VAR):
        resolve_dsn(PartitionsDocument.model_validate(DOCUMENT))


def test__resolve_dsn__nowhere_to_connect__says_all_three_places(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.delenv(DSN_ENV_VAR, raising=False)

    # Act / Assert
    with pytest.raises(ConfigError, match="--dsn"):
        resolve_dsn(PartitionsDocument.model_validate(DOCUMENT))


def test__async_url__a_dsn_naming_no_driver__is_given_the_async_one() -> None:
    # Arrange / Act / Assert
    assert async_url("postgresql://app@host/db") == "postgresql+asyncpg://app@host/db"
    assert async_url("postgres://app@host/db") == "postgresql+asyncpg://app@host/db"


def test__async_url__a_dsn_naming_its_driver__is_left_exactly_as_it_is() -> None:
    # Whoever wrote +psycopg installed psycopg on purpose.
    assert async_url("postgresql+psycopg://app@host/db") == "postgresql+psycopg://app@host/db"
    assert async_url("not a url") == "not a url"


# ── Narrowing to tables ─────────────────────────────────────────────────────────


def test__select_configs__no_table_named__is_every_table_in_document_order() -> None:
    # Arrange
    document = PartitionsDocument.model_validate(DOCUMENT)

    # Act / Assert
    assert [c.table_name for c in select_configs(document, ())] == ["events", "audit"]


def test__select_configs__a_bare_name__finds_the_qualified_table() -> None:
    # Arrange
    document = PartitionsDocument.model_validate(DOCUMENT)

    # Act / Assert
    assert [c.qualified_name for c in select_configs(document, ("audit",))] == ["public.audit"]
    assert [c.qualified_name for c in select_configs(document, ("public.audit",))] == ["public.audit"]


def test__select_configs__a_name_the_document_does_not_have__lists_the_ones_it_does() -> None:
    # Arrange
    document = PartitionsDocument.model_validate(DOCUMENT)

    # Act / Assert
    with pytest.raises(ConfigError, match=re.escape("public.events, public.audit")):
        select_configs(document, ("orders",))


# ── Exit codes ──────────────────────────────────────────────────────────────────


async def test__plan__a_converged_table__exits_ok_even_under_check() -> None:
    # Arrange / Act
    result = await run_plan(_kit(_plan(noop=True)), _configs(), check=True)

    # Assert
    assert result.code is ExitCode.OK


async def test__plan__pending_operations__are_drift_only_when_asked_to_check() -> None:
    # Arrange
    kit = _kit(_plan())

    # Act / Assert: a plan is a report by default; --check is what alerts
    assert (await run_plan(kit, _configs(), check=False)).code is ExitCode.OK
    assert (await run_plan(kit, _configs(), check=True)).code is ExitCode.DRIFT


async def test__plan__an_actionable_finding__outranks_drift() -> None:
    # Arrange: drift is what a run fixes; a finding is what it cannot.
    finding = Finding(
        partition_name="public.events__2026_08",
        reason=FindingReason.RANGE_OVERLAP,
        detail="overlaps a partition that is not this window",
        severity=Severity.WARNING,
    )

    # Act
    result = await run_plan(_kit(_plan(findings=(finding,))), _configs(), check=True)

    # Assert
    assert result.code is ExitCode.FINDINGS


async def test__plan__json__is_the_model_dump_under_a_versioned_envelope() -> None:
    # Arrange / Act
    result = await run_plan(_kit(_plan()), _configs(), check=False)
    payload = json.loads(result.render(output="json"))

    # Assert: the vocabulary a configuration file is written in, not one of ours
    assert payload["version"] == 1
    assert payload["command"] == "plan"
    assert payload["tables"][0]["table"] == "public.events"
    assert payload["tables"][0]["plan"]["operations"][0]["kind"] == "create"


def _validation(monkeypatch: pytest.MonkeyPatch, *, error: Exception | None = None) -> None:
    """Point the command at a validation service that answers as told."""
    service = MagicMock(validate_config=AsyncMock(side_effect=error))
    monkeypatch.setattr("pg_partsmith.cli.commands.PartitionValidationService", MagicMock(return_value=service))


async def test__validate__a_table_the_catalog_disagrees_with__exits_config(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    _validation(monkeypatch, error=InvalidPartitionConfigError("Table 'public.events' is not partitioned"))

    # Act
    result = await run_validate(MagicMock(), _configs())

    # Assert
    assert result.code is ExitCode.CONFIG
    assert json.loads(result.render(output="json"))["tables"][0]["ok"] is False


async def test__validate__a_table_the_catalog_agrees_with__exits_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    _validation(monkeypatch)

    # Act
    result = await run_validate(MagicMock(), _configs())

    # Assert
    assert result.code is ExitCode.OK
    assert json.loads(result.render(output="json"))["tables"][0] == {
        "table": "public.events",
        "ok": True,
        "error": None,
    }


def test__main__a_document_that_is_not_there__exits_config_without_connecting(tmp_path: Path) -> None:
    # Arrange / Act / Assert: no DSN is even resolved, so this is 4 and not 5
    assert main(["plan", "-c", str(tmp_path / "absent.yaml")]) == ExitCode.CONFIG


def test__main__version__exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    # Act / Assert: one number for the library, the CLI and the image
    assert main(["--version"]) == ExitCode.OK
    assert __version__ in capsys.readouterr().out


def test__main__a_misspelled_flag__is_a_usage_error_and_not_drift(capsys: pytest.CaptureFixture[str]) -> None:
    # A CronJob alerting on exit 2 must not page over a typo.
    assert main(["plan", "--bogus", "-c", "partitions.yaml"]) == ExitCode.USAGE
    assert "--bogus" in capsys.readouterr().err


def test__main__no_command__is_a_usage_error_that_shows_the_help(capsys: pytest.CaptureFixture[str]) -> None:
    # Arrange / Act
    code = main([])
    captured = capsys.readouterr()

    # Assert: the help explains itself; the code says it was not a run
    assert code == ExitCode.USAGE
    assert "inspect" in captured.out
    assert captured.err == ""


def test__main__help__exits_zero_and_names_every_command(capsys: pytest.CaptureFixture[str]) -> None:
    # Arrange / Act
    code = main(["--help"])
    printed = capsys.readouterr().out

    # Assert
    assert code == ExitCode.OK
    assert all(command in printed for command in ("inspect", "plan", "validate", "apply"))


# ── Rendering ───────────────────────────────────────────────────────────────────


def test__envelope__carries_its_own_version_and_the_command_that_made_it() -> None:
    # Arrange / Act
    payload = envelope("inspect", [{"table": "public.events"}])

    # Assert
    assert payload["version"] == 1
    assert payload["command"] == "inspect"
    assert payload["tables"] == [{"table": "public.events"}]


def test__plan_entry__names_the_table_beside_the_plan() -> None:
    # Arrange / Act
    entry = plan_entry(_plan())

    # Assert
    assert entry["table"] == "public.events"
    assert entry["plan"]["table_name"] == "public.events"


# ── Applying ────────────────────────────────────────────────────────────────────


def _applying_kit(result: MaintenanceResult | None = None) -> MagicMock:
    kit = MagicMock()
    answer = result if result is not None else MaintenanceResult(created_count=1)
    kit.service.apply = AsyncMock(return_value=answer)
    kit.service.maintain = AsyncMock(return_value=answer)
    return kit


def _destructive_plan() -> MaintenancePlan:
    return MaintenancePlan(
        table_name="public.events",
        generated_at=NOW,
        operations=(
            CreatePartition(
                target="public.events__2026_09",
                parent_name="public.events",
                bounds=RangeBounds(from_value="2026-09-01", to_value="2026-10-01"),
                reason=Reason.CREATE_AHEAD,
            ),
            DropPartition(target="public.events__2025_08", reason=Reason.GRACE_ELAPSED, oid=7),
        ),
    )


async def test__apply__by_default__withholds_every_destructive_operation() -> None:
    # Arrange: the safe mode is the default one, not a second mode to remember
    kit = _applying_kit()

    # Act
    result = await run_apply(kit, _configs(), plans={"public.events": _destructive_plan()})

    # Assert
    applied = kit.service.apply.await_args.args[1]
    assert [op.kind.value for op in applied.operations] == ["create"]
    assert "--allow-destructive" in result.render(output="human")


async def test__apply__allow_destructive__carries_the_drop_out_too() -> None:
    # Arrange
    kit = _applying_kit()

    # Act
    await run_apply(kit, _configs(), plans={"public.events": _destructive_plan()}, allow_destructive=True)

    # Assert
    applied = kit.service.apply.await_args.args[1]
    assert [op.kind.value for op in applied.operations] == ["create", "drop"]


async def test__apply__without_a_plan_file__plans_and_applies_under_one_lock() -> None:
    # Arrange: that is what maintain() is for, and it finalizes a detach that
    # was interrupted before deciding the rest of the run.
    kit = _applying_kit()

    # Act
    await run_apply(kit, _configs())

    # Assert
    kit.service.apply.assert_not_awaited()
    assert kit.service.maintain.await_args.kwargs == {
        "skip_detach": True,
        "skip_drop": True,
        "continue_on_error": False,
    }


async def test__apply__a_plan_file_with_nothing_for_the_table__says_what_it_holds() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ConfigError, match=re.escape("public.audit")):
        await run_apply(_applying_kit(), _configs(), plans={"public.audit": _plan()})


async def test__apply__issues_reported_by_the_run__exit_findings() -> None:
    # Arrange
    issue = MaintenanceIssue(step=MaintenanceIssueStep.DROP, partition_name="public.events__2025_08", error="refused")
    kit = _applying_kit(MaintenanceResult(created_count=1, issues=(issue,)))

    # Act
    result = await run_apply(kit, _configs())

    # Assert
    assert result.code is ExitCode.FINDINGS
    assert "refused" in result.render(output="human")


async def test__apply__json__carries_the_plan_beside_the_result() -> None:
    # The result excludes its plan from serialization, so an audit log would
    # otherwise get counters with nothing saying what they counted.
    kit = _applying_kit()

    # Act
    result = await run_apply(kit, _configs(), plans={"public.events": _plan()})
    payload = json.loads(result.render(output="json"))

    # Assert
    entry = payload["tables"][0]
    assert entry["result"]["created_count"] == 1
    assert entry["plan"]["operations"][0]["kind"] == "create"


def test__load_plans__a_file_plan_save_wrote__reads_back_as_the_plan(tmp_path: Path) -> None:
    # Arrange
    saved = envelope("plan", [plan_entry(_plan())])
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(saved), encoding="utf-8")

    # Act
    plans = load_plans(path)

    # Assert
    assert plans["public.events"] == _plan()


def test__load_plans__a_version_this_does_not_read__is_refused(tmp_path: Path) -> None:
    # Arrange
    saved = {**envelope("plan", [plan_entry(_plan())]), "version": 99}
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(saved), encoding="utf-8")

    # Act / Assert: half-reading a future format is worse than refusing it
    with pytest.raises(ConfigError, match="version 99"):
        load_plans(path)


def test__load_plans__something_that_is_not_a_saved_plan__is_refused(tmp_path: Path) -> None:
    # Arrange
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"version": 1, "command": "plan"}), encoding="utf-8")

    # Act / Assert
    with pytest.raises(ConfigError, match="no 'tables'"):
        load_plans(path)


# ── Hooks are code, and run only when asked for ─────────────────────────────────


def _hooks_document(tmp_path: Path) -> str:
    payload = {**DOCUMENT, "dsn": "postgresql://app@localhost/app", "hooks": {"before_drop": ["/bin/archive"]}}
    return str(_write(tmp_path, "partitions.json", json.dumps(payload)))


def test__apply__a_document_that_runs_commands__is_refused_unless_asked_for(tmp_path: Path) -> None:
    # Arrange: ignoring a configured before_drop silently would be the worst
    # outcome available -- an operator reads the file and believes it ran.
    config = _hooks_document(tmp_path)

    # Act
    code = main(["apply", "-c", config])

    # Assert: refused as configuration, before any connection is made
    assert code == ExitCode.CONFIG


def test__plan__a_document_that_runs_commands__needs_no_permission(tmp_path: Path) -> None:
    # Hooks fire during apply alone, so planning one is not running one. This
    # gets as far as connecting, which is a different failure entirely.
    config = _hooks_document(tmp_path)

    # Act / Assert
    assert main(["plan", "-c", config]) == ExitCode.CONNECTION


def test__hooks__allowed__are_built_from_the_document(tmp_path: Path) -> None:
    # Arrange
    document = load_document(Path(_hooks_document(tmp_path)))

    # Act
    hooks = _cli_hooks(document, command="apply", config=tmp_path / "partitions.json", allow_hooks=True)

    # Assert
    assert hooks is not None
    assert isinstance(hooks[0], CommandHooks)
    assert hooks[0].phases == (HookPhase.BEFORE_DROP,)


# ── What a plan will lock ───────────────────────────────────────────────────────


def test__describe_locks__names_the_heaviest_lock_of_every_top_level_operation() -> None:
    # Arrange
    plan = _destructive_plan()

    # Act
    lines = describe_locks(plan).splitlines()

    # Assert: one header line per operation, its lock on the next
    assert lines[0] == "locks:"
    assert lines[1].startswith("  CREATE public.events__2026_09")
    assert "SHARE UPDATE EXCLUSIVE on the parent" in lines[2]
    assert lines[3].startswith("  DROP public.events__2025_08")
    assert "ACCESS EXCLUSIVE on the dropped table only" in lines[4]


def test__describe_locks__an_operation_that_cannot_run_in_a_transaction__says_so() -> None:
    # The one a crash leaves half-done, which is worth knowing before a window.
    plan = MaintenancePlan(
        table_name="public.events",
        generated_at=NOW,
        operations=(
            DetachPartition(
                target="public.events__2025_08",
                parent_name="public.events",
                reason=Reason.RETENTION_EXPIRED,
                mode=DetachMode.CONCURRENT,
            ),
        ),
    )

    # Act / Assert
    assert "(outside a transaction block)" in describe_locks(plan).splitlines()[1]


def test__describe_locks__a_converged_table__has_nothing_to_lock() -> None:
    # Arrange / Act / Assert
    assert describe_locks(_plan(noop=True)) == "locks: none, nothing to do"


async def test__plan__locks__are_printed_after_the_operations_only_when_asked() -> None:
    # Arrange
    kit = _kit(_plan())

    # Act
    without = (await run_plan(kit, _configs(), check=False)).render(output="human")
    with_locks = (await run_plan(kit, _configs(), check=False, locks=True)).render(output="human")

    # Assert
    assert "locks:" not in without
    assert "locks:" in with_locks
    assert "SHARE UPDATE EXCLUSIVE" in with_locks


# ── Python in the document ──────────────────────────────────────────────────────


def test__hooks__a_python_block__is_built_alongside_the_commands(tmp_path: Path) -> None:
    # Arrange
    payload = {
        **DOCUMENT,
        "dsn": "postgresql://app@localhost/app",
        "hooks": {"after_create": ["/bin/notify"], "before_drop": {"python": "log.info('x')"}},
    }
    document = load_document(_write(tmp_path, "partitions.json", json.dumps(payload)))

    # Act
    hooks = _cli_hooks(document, command="apply", config=tmp_path / "partitions.json", allow_hooks=True)

    # Assert: one object per kind, each knowing its own phases
    assert hooks is not None
    assert [type(hook).__name__ for hook in hooks] == ["CommandHooks", "PythonHooks"]


def test__hooks__a_python_file__is_read_relative_to_the_document(tmp_path: Path) -> None:
    # Arrange
    (tmp_path / "hooks").mkdir()
    (tmp_path / "hooks" / "export.py").write_text("log.info('exporting %s', event.partition.name)", encoding="utf-8")
    payload = {**DOCUMENT, "dsn": "x", "hooks": {"before_drop": {"python_file": "hooks/export.py"}}}
    document = load_document(_write(tmp_path, "partitions.json", json.dumps(payload)))

    # Act
    sources, names = load_python_hooks(document.hooks, tmp_path)  # type: ignore[arg-type]

    # Assert
    assert sources[HookPhase.BEFORE_DROP].startswith("log.info")
    assert names[HookPhase.BEFORE_DROP].endswith("export.py")


def test__hooks__a_python_file_that_does_not_parse__fails_validate_by_name(tmp_path: Path) -> None:
    # Arrange: validate is the command that should find this, not apply at 03:00
    (tmp_path / "export.py").write_text("def broken(:", encoding="utf-8")
    payload = {**DOCUMENT, "dsn": "x", "hooks": {"before_drop": {"python_file": "export.py"}}}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    # Act / Assert: refused before any connection is attempted
    assert main(["validate", "-c", str(config)]) == ExitCode.CONFIG


def test__hooks__a_python_file_that_is_missing__fails_validate_by_name(tmp_path: Path) -> None:
    # Arrange
    payload = {**DOCUMENT, "dsn": "x", "hooks": {"before_drop": {"python_file": "absent.py"}}}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    # Act / Assert
    assert main(["validate", "-c", str(config)]) == ExitCode.CONFIG


# ── Being stopped ───────────────────────────────────────────────────────────────


class _Recording:
    """An engine that remembers being disposed, and is otherwise the real one."""

    def __init__(self, engine: Any) -> None:
        self._engine = engine
        self.disposed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._engine, name)

    async def dispose(self) -> None:
        self.disposed = True
        await self._engine.dispose()


def _stoppable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, signum: int) -> tuple[str, list[_Recording]]:
    """A document that connects nowhere, and a plan that raises ``signum`` mid-run."""
    payload = {**DOCUMENT, "dsn": "postgresql+asyncpg://nobody@127.0.0.1:1/none"}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))
    engines: list[_Recording] = []

    def recording(url: str) -> _Recording:
        engine = _Recording(cli.create_async_engine.__wrapped__(url))  # type: ignore[attr-defined]
        engines.append(engine)
        return engine

    real = cli.create_async_engine
    recording.__wrapped__ = real  # type: ignore[attr-defined]
    monkeypatch.setattr(cli, "create_async_engine", recording)

    async def stopped_mid_run(kit: Any, configs: Any, *, check: bool, locks: bool = False) -> Any:
        signal.raise_signal(signum)  # what the terminal, or the kubelet, does
        await asyncio.sleep(5)  # the statement the run was in the middle of
        raise AssertionError("never reached")

    monkeypatch.setattr(cli, "run_plan", stopped_mid_run)
    return str(config), engines


def test__main__ctrl_c_mid_run__cleans_up_and_says_so(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    config, engines = _stoppable(monkeypatch, tmp_path, signal.SIGINT)

    # Act
    code = main(["plan", "-c", config])

    # Assert: 128 + 2, the engine disposed on the way out, one line saying why
    assert code == ExitCode.INTERRUPTED
    assert engines and engines[0].disposed
    assert "interrupted" in capsys.readouterr().err


@pytest.mark.skipif(sys.platform == "win32", reason="an event loop takes no signal handlers on Windows")
def test__main__sigterm_mid_run__cleans_up_and_exits_143(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange: what a pod is sent at its deadline
    config, engines = _stoppable(monkeypatch, tmp_path, signal.SIGTERM)

    # Act
    code = main(["plan", "-c", config])

    # Assert
    assert code == ExitCode.TERMINATED
    assert engines and engines[0].disposed
    assert "terminated" in capsys.readouterr().err


def test__main__schema__prints_the_document_schema_and_needs_no_database(capsys: pytest.CaptureFixture[str]) -> None:
    # Arrange / Act
    code = main(["schema"])
    printed = json.loads(capsys.readouterr().out)

    # Assert: the vocabulary an editor validates against is the model's own
    assert code == ExitCode.OK
    assert set(printed["properties"]) >= {"tables", "defaults", "runtime", "hooks", "dsn", "version"}


# ── The corners of reading files ────────────────────────────────────────────────


def test__load_document__json_that_does_not_parse__names_the_file(tmp_path: Path) -> None:
    # Arrange
    path = _write(tmp_path, "partitions.json", "{not json")

    # Act / Assert
    with pytest.raises(ConfigError, match="not valid JSON"):
        load_document(path)


def test__load_document__yaml_that_does_not_parse__names_the_file(tmp_path: Path) -> None:
    # Arrange
    pytest.importorskip("yaml")
    path = _write(tmp_path, "partitions.yaml", "tables: [unclosed")

    # Act / Assert
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_document(path)


def test__async_url__a_scheme_that_is_not_postgres__is_left_alone() -> None:
    # Arrange / Act / Assert: not ours to rewrite
    assert async_url("mysql://app@host/db") == "mysql://app@host/db"


def test__load_plans__a_file_that_is_not_there__is_a_configuration_error(tmp_path: Path) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ConfigError, match="Cannot read"):
        load_plans(tmp_path / "absent.json")


def test__load_plans__a_file_that_is_not_json__is_a_configuration_error(tmp_path: Path) -> None:
    # Arrange
    path = _write(tmp_path, "plan.json", "{")

    # Act / Assert
    with pytest.raises(ConfigError, match="not valid JSON"):
        load_plans(path)


def test__load_plans__an_entry_with_no_plan__is_refused(tmp_path: Path) -> None:
    # Arrange
    path = _write(tmp_path, "plan.json", json.dumps({"version": 1, "tables": [{"table": "public.events"}]}))

    # Act / Assert
    with pytest.raises(ConfigError, match="no plan"):
        load_plans(path)


def test__load_plans__a_plan_this_version_cannot_read__is_refused(tmp_path: Path) -> None:
    # Arrange: a plan with an instant no plan could have
    saved = {"version": 1, "tables": [{"table": "public.events", "plan": {"table_name": "x", "generated_at": "no"}}]}
    path = _write(tmp_path, "plan.json", json.dumps(saved))

    # Act / Assert
    with pytest.raises(ConfigError, match="cannot read"):
        load_plans(path)


def test__load_plans__a_file_naming_no_table__is_refused(tmp_path: Path) -> None:
    # Arrange
    path = _write(tmp_path, "plan.json", json.dumps({"version": 1, "tables": []}))

    # Act / Assert
    with pytest.raises(ConfigError, match="names no table"):
        load_plans(path)


def test__as_code__turns_what_the_app_returns_into_an_exit_code() -> None:
    # Arrange / Act / Assert: a command's own code, click's 0 for --help, an
    # integer that is no code of ours, and nothing at all
    assert cli._as_code(ExitCode.DRIFT) is ExitCode.DRIFT
    assert cli._as_code(0) is ExitCode.OK
    assert cli._as_code(99) is ExitCode.FAILED
    assert cli._as_code(None) is ExitCode.OK


def test__plan_save__writes_the_envelope_where_apply_can_read_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Arrange: no database is reached; the plan is handed back by a fake
    payload = {**DOCUMENT, "dsn": "postgresql+asyncpg://nobody@127.0.0.1:1/none"}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    async def planned(kit: Any, configs: Any, *, check: bool, locks: bool = False) -> Any:
        return CommandResult(code=ExitCode.OK, lines=["planned"], payload=envelope("plan", [plan_entry(_plan())]))

    monkeypatch.setattr(cli, "run_plan", planned)
    saved = tmp_path / "plan.json"

    # Act
    code = main(["plan", "-c", str(config), "--save", str(saved)])

    # Assert: the file is the artifact, whatever the terminal was shown
    assert code == ExitCode.OK
    assert load_plans(saved)["public.events"] == _plan()


# ── What a shell wrapper used to be for ─────────────────────────────────────────


def test__write__puts_the_output_in_a_file_and_nothing_on_stdout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange: a textfile collector reads the file; the log should not get a copy
    payload = {**DOCUMENT, "dsn": "postgresql+asyncpg://nobody@127.0.0.1:1/none"}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    async def planned(kit: Any, configs: Any, *, check: bool, locks: bool = False) -> Any:
        return CommandResult(code=ExitCode.OK, lines=["planned"], payload=envelope("plan", [plan_entry(_plan())]))

    monkeypatch.setattr(cli, "run_plan", planned)
    target = tmp_path / "textfile" / "partsmith.prom"
    target.parent.mkdir()

    # Act
    code = main(["plan", "-c", str(config), "--output", "metrics", "--write", str(target)])

    # Assert: written whole, under its final name, and no temporary left beside it
    assert code == ExitCode.OK
    assert "# TYPE pg_partsmith_pending_operations gauge" in target.read_text(encoding="utf-8")
    assert capsys.readouterr().out == ""
    assert [p.name for p in target.parent.iterdir()] == ["partsmith.prom"]


def test__write__a_path_that_cannot_be_written__is_exit_4_and_a_sentence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange: the textfile directory a hostPath names is root's and the
    # container is not root; here, the nearest thing on every platform -- a
    # directory that is not there at all
    payload = {**DOCUMENT, "dsn": "postgresql+asyncpg://nobody@127.0.0.1:1/none"}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    async def planned(kit: Any, configs: Any, *, check: bool, locks: bool = False) -> Any:
        return CommandResult(code=ExitCode.OK, lines=["planned"], payload=envelope("plan", [plan_entry(_plan())]))

    monkeypatch.setattr(cli, "run_plan", planned)
    target = tmp_path / "missing" / "partsmith.prom"

    # Act
    code = main(["plan", "-c", str(config), "--output", "metrics", "--write", str(target)])

    # Assert: the path and the reason, not a traceback, and nothing on stdout
    assert code == ExitCode.CONFIG
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("pg-partsmith: Cannot write ")
    assert "partsmith.prom" in captured.err
    assert "Traceback" not in captured.err


def test__plan_save__a_path_that_cannot_be_written__is_exit_4_and_a_sentence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange
    payload = {**DOCUMENT, "dsn": "postgresql+asyncpg://nobody@127.0.0.1:1/none"}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    async def planned(kit: Any, configs: Any, *, check: bool, locks: bool = False) -> Any:
        return CommandResult(code=ExitCode.OK, lines=["planned"], payload=envelope("plan", [plan_entry(_plan())]))

    monkeypatch.setattr(cli, "run_plan", planned)
    saved = tmp_path / "missing" / "plan.json"

    # Act
    code = main(["plan", "-c", str(config), "--save", str(saved)])

    # Assert: it used to come out as a database error, which it is not
    assert code == ExitCode.CONFIG
    assert capsys.readouterr().err.startswith("pg-partsmith: Cannot write the plan to ")


# ── What the run itself refuses, and what the document refuses first ─────────────


def test__apply__a_hook_that_refuses_without_continue_on_error__is_exit_3_and_a_sentence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange: the archiver said no, and the run stopped there, as it should
    payload = {**DOCUMENT, "dsn": "postgresql+asyncpg://nobody@127.0.0.1:1/none"}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))
    refused = CommandHookError(HookPhase.BEFORE_DROP, "events__2020_01", "exit status 1")
    monkeypatch.setattr(cli, "run_apply", AsyncMock(side_effect=refused))

    # Act
    code = main(["apply", "-c", str(config), "--allow-destructive"])

    # Assert: a finding for a person, the way --continue-on-error would have recorded it
    assert code == ExitCode.FINDINGS
    assert capsys.readouterr().err == "pg-partsmith: before_drop hook for 'events__2020_01' failed: exit status 1\n"


def test__dsn__a_string_that_is_no_url__is_exit_4_before_any_database(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = _write(tmp_path, "partitions.json", json.dumps(DOCUMENT))

    # Act
    code = main(["plan", "-c", str(config), "--dsn", "not a connection string"])

    # Assert: the deployment's mistake, not the database's
    assert code == ExitCode.CONFIG
    err = capsys.readouterr().err
    assert err.startswith("pg-partsmith: the connection string is not usable: ")
    assert "Traceback" not in err


def test__runtime_boundary_codec__that_nothing_resolves__is_refused_by_validate(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange: a typo in the wiring section, which used to surface as a bare ValueError at apply
    payload = {**DOCUMENT, "runtime": {"boundary_codec": "uuid7"}}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    # Act
    code = main(["validate", "-c", str(config), "--dsn", "postgresql://nobody@127.0.0.1:1/none"])

    # Assert: named where it is written, before any connection
    assert code == ExitCode.CONFIG
    err = capsys.readouterr().err
    assert "boundary_codec" in err
    assert "uuid7" in err


def test__hooks__an_empty_command__is_refused_where_it_is_written(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    payload = {**DOCUMENT, "hooks": {"before_drop": []}}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    # Act
    code = main(["validate", "-c", str(config), "--dsn", "postgresql://nobody@127.0.0.1:1/none"])

    # Assert
    assert code == ExitCode.CONFIG
    assert "before_drop" in capsys.readouterr().err


def test__console_script__without_the_cli_extra__is_a_sentence_and_exit_64(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange: typer is what the extra brings; without it the package cannot
    # import, and the console script is on PATH all the same
    monkeypatch.setitem(sys.modules, "typer", None)
    for name in ("pg_partsmith.cli", "pg_partsmith.cli.main"):
        monkeypatch.delitem(sys.modules, name, raising=False)

    # Act
    code = console.main(["--version"])

    # Assert
    assert code == ExitCode.USAGE
    assert capsys.readouterr().err.startswith('pg-partsmith needs the cli extra: pip install "pg-partsmith[cli]"')


def test__console_script__with_the_extra__is_the_command_line() -> None:
    assert console.main(["--version"]) == ExitCode.OK


def test__plan__a_password_the_server_rejects__is_exit_5_and_not_a_traceback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The driver raises its own error for a rejected password, before
    # SQLAlchemy has a DBAPI error to wrap it in; it is still the database.
    asyncpg = pytest.importorskip("asyncpg")
    config = _write(tmp_path, "partitions.json", json.dumps(DOCUMENT))
    rejected = asyncpg.InvalidPasswordError('password authentication failed for user "app"')
    monkeypatch.setattr(cli, "run_plan", AsyncMock(side_effect=rejected))

    # Act / Assert
    assert main(["plan", "-c", str(config), "--dsn", "postgresql://app:wrong@localhost/app"]) == ExitCode.CONNECTION


def test__ok_if_locked__turns_a_held_lock_into_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Arrange: an init container restarts on anything non-zero, and ten replicas
    # starting at once means nine of them find the lock held
    payload = {**DOCUMENT, "dsn": "postgresql+asyncpg://nobody@127.0.0.1:1/none"}
    config = _write(tmp_path, "partitions.json", json.dumps(payload))

    async def locked(kit: Any, configs: Any, **kwargs: Any) -> Any:
        raise LockAcquisitionError("public.events", "held by another maintainer")

    monkeypatch.setattr(cli, "run_apply", locked)

    # Act / Assert
    assert main(["apply", "-c", str(config)]) == ExitCode.LOCKED
    assert main(["apply", "-c", str(config), "--ok-if-locked"]) == ExitCode.OK


# ── The console's encoding ──────────────────────────────────────────────────────


def test__main__survives_a_console_that_cannot_show_an_em_dash(monkeypatch: pytest.MonkeyPatch) -> None:
    """A Windows pipe or a bare POSIX locale may be ASCII, and human output has an em dash in it."""
    # Arrange: an ASCII-only stdout, as strict as a redirected stream is opened
    raw = io.BytesIO()
    monkeypatch.setattr(sys, "stdout", io.TextIOWrapper(raw, encoding="ascii", errors="strict"))

    # Act
    code = main(["--version"])
    sys.stdout.write("events — ok\n")
    sys.stdout.flush()

    # Assert
    assert code == ExitCode.OK
    assert sys.stdout.errors == "backslashreplace"
    assert b"events \\u2014 ok" in raw.getvalue()
