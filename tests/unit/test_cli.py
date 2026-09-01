"""The command line: reading a document, choosing a connection, and what it exits with."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_partsmith.__version__ import __version__
from pg_partsmith.cli import ExitCode, main
from pg_partsmith.cli.commands import run_plan, run_validate
from pg_partsmith.cli.loader import DSN_ENV_VAR, ConfigError, async_url, load_document, resolve_dsn, select_configs
from pg_partsmith.cli.render import envelope, plan_entry
from pg_partsmith.document import PartitionsDocument
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.plan import CreatePartition, Finding, FindingReason, MaintenancePlan, Reason, Severity
from pg_partsmith.topology import RangeBounds

if TYPE_CHECKING:
    from collections.abc import Sequence

NOW = datetime(2026, 8, 28, tzinfo=UTC)

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
    result = await run_plan(_kit(_plan(noop=True)), _configs(), as_json=False, check=True)

    # Assert
    assert result.code is ExitCode.OK


async def test__plan__pending_operations__are_drift_only_when_asked_to_check() -> None:
    # Arrange
    kit = _kit(_plan())

    # Act / Assert: a plan is a report by default; --check is what alerts
    assert (await run_plan(kit, _configs(), as_json=False, check=False)).code is ExitCode.OK
    assert (await run_plan(kit, _configs(), as_json=False, check=True)).code is ExitCode.DRIFT


async def test__plan__an_actionable_finding__outranks_drift() -> None:
    # Arrange: drift is what a run fixes; a finding is what it cannot.
    finding = Finding(
        partition_name="public.events__2026_08",
        reason=FindingReason.RANGE_OVERLAP,
        detail="overlaps a partition that is not this window",
        severity=Severity.WARNING,
    )

    # Act
    result = await run_plan(_kit(_plan(findings=(finding,))), _configs(), as_json=False, check=True)

    # Assert
    assert result.code is ExitCode.FINDINGS


async def test__plan__json__is_the_model_dump_under_a_versioned_envelope() -> None:
    # Arrange / Act
    result = await run_plan(_kit(_plan()), _configs(), as_json=True, check=False)
    payload = json.loads(result.render(as_json=True))

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
    result = await run_validate(MagicMock(), _configs(), as_json=True)

    # Assert
    assert result.code is ExitCode.CONFIG
    assert json.loads(result.render(as_json=True))["tables"][0]["ok"] is False


async def test__validate__a_table_the_catalog_agrees_with__exits_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    _validation(monkeypatch)

    # Act
    result = await run_validate(MagicMock(), _configs(), as_json=True)

    # Assert
    assert result.code is ExitCode.OK
    assert json.loads(result.render(as_json=True))["tables"][0] == {
        "table": "public.events",
        "ok": True,
        "error": None,
    }


def test__main__a_document_that_is_not_there__exits_config_without_connecting(tmp_path: Path) -> None:
    # Arrange / Act / Assert: no DSN is even resolved, so this is 4 and not 5
    assert main(["plan", "-c", str(tmp_path / "absent.yaml")]) == ExitCode.CONFIG


def test__main__version__exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    # Act / Assert: one number for the library, the CLI and the image
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


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
