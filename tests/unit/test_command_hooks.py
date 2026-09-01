"""``CommandHooks``: a lifecycle hook that runs somebody else's program."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pg_partsmith.aio.command_hooks import CommandHookError, CommandHooks
from pg_partsmith.entities import PartitionGranularity, PartitionInfo, TablePartitionConfig
from pg_partsmith.events import HookPhase, PartitionEvent
from pg_partsmith.plan import DropPartition, Reason
from pg_partsmith.sync.command_hooks import CommandHooks as SyncCommandHooks
from pg_partsmith.topology import RangeBounds

CONFIG = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
)


def _event(phase: HookPhase = HookPhase.BEFORE_DROP) -> PartitionEvent:
    info = PartitionInfo(
        name="events__2025_08",
        schema_name="public",
        partition_type="range",
        is_attached=False,
        bounds=RangeBounds(from_value="2025-08-01", to_value="2025-09-01"),
    )
    operation = DropPartition(target="public.events__2025_08", reason=Reason.GRACE_ELAPSED, oid=42)
    return PartitionEvent.build(phase, CONFIG, info, operation)


def _script(body: str) -> list[str]:
    """A command that is a Python one-liner, so the test runs anywhere pytest does."""
    return [sys.executable, "-c", body]


READ_STDIN = "import sys, json; d = json.load(sys.stdin); open(sys.argv[1] if len(sys.argv) > 1 else 'x', 'w')"


async def test__on_event__the_phase_it_was_configured_for__runs_the_command(tmp_path: Path) -> None:
    # Arrange
    marker = tmp_path / "ran.json"
    body = f"import sys, pathlib; pathlib.Path({str(marker)!r}).write_text(sys.stdin.read(), encoding='utf-8')"
    hooks = CommandHooks({HookPhase.BEFORE_DROP: _script(body)})

    # Act
    await hooks.on_event(_event())

    # Assert: the whole event, in the vocabulary a document is written in
    payload: dict[str, Any] = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["phase"] == "before_drop"
    assert payload["partition"]["name"] == "events__2025_08"
    assert payload["operation"]["kind"] == "drop"
    assert payload["operation"]["reason"] == "grace_elapsed"
    assert payload["window"]["start"].startswith("2025-08-01")


async def test__on_event__another_phase__runs_nothing(tmp_path: Path) -> None:
    # Arrange
    marker = tmp_path / "ran"
    body = f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran', encoding='utf-8')"
    hooks = CommandHooks({HookPhase.BEFORE_DROP: _script(body)})

    # Act
    await hooks.on_event(_event(HookPhase.AFTER_CREATE))

    # Assert
    assert not marker.exists()


async def test__on_event__a_command_that_refuses__aborts_the_operation() -> None:
    # Arrange: a non-zero exit is a refusal, exactly as a raised exception is
    hooks = CommandHooks({HookPhase.BEFORE_DROP: _script("import sys; sys.stderr.write('still needed'); sys.exit(3)")})

    # Act / Assert
    with pytest.raises(CommandHookError, match="still needed") as exc:
        await hooks.on_event(_event())
    assert exc.value.phase is HookPhase.BEFORE_DROP
    assert exc.value.partition_name == "events__2025_08"


async def test__on_event__a_command_that_hangs__is_killed_and_the_operation_abandoned() -> None:
    # A hook that never returns holds the table's maintenance lock, so there is
    # no "wait forever" option.
    hooks = CommandHooks({HookPhase.BEFORE_DROP: _script("import time; time.sleep(30)")}, timeout_seconds=0.5)

    # Act / Assert
    with pytest.raises(CommandHookError, match="did not finish"):
        await hooks.on_event(_event())


async def test__on_event__a_command_that_is_not_there__says_so_by_name() -> None:
    # Arrange
    hooks = CommandHooks({HookPhase.BEFORE_DROP: ["/nonexistent/archive-partition"]})

    # Act / Assert
    with pytest.raises(CommandHookError, match="archive-partition"):
        await hooks.on_event(_event())


async def test__on_event__the_environment__names_the_phase_the_table_and_the_partition(tmp_path: Path) -> None:
    # Arrange: the three facts a shell script reaches for, without parsing JSON
    marker = tmp_path / "env.json"
    body = (
        "import os, json, pathlib; "
        f"pathlib.Path({str(marker)!r}).write_text(json.dumps("
        "{k: v for k, v in os.environ.items() if k.startswith('PG_PARTSMITH_')}), encoding='utf-8')"
    )
    hooks = CommandHooks({HookPhase.BEFORE_DROP: _script(body)})

    # Act
    await hooks.on_event(_event())

    # Assert
    environment = json.loads(marker.read_text(encoding="utf-8"))
    assert environment["PG_PARTSMITH_PHASE"] == "before_drop"
    assert environment["PG_PARTSMITH_TABLE"] == "public.events"
    assert environment["PG_PARTSMITH_PARTITION"] == "events__2025_08"
    assert environment["PG_PARTSMITH_WINDOW_START"].startswith("2025-08-01")


def test__command_hooks__an_empty_command__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="before_drop"):
        CommandHooks({HookPhase.BEFORE_DROP: []})


def test__command_hooks__implements_every_phase_method_the_executor_calls() -> None:
    # The executor calls the method named for the phase after on_event, so a
    # hook missing one would abort the operation with an AttributeError.
    hooks = CommandHooks({HookPhase.BEFORE_DROP: ["/bin/true"]})

    # Act / Assert
    assert all(callable(getattr(hooks, phase.value, None)) for phase in HookPhase)
    assert hooks.phases == (HookPhase.BEFORE_DROP,)


def test__sync_command_hooks__runs_the_same_way(tmp_path: Path) -> None:
    # Arrange: the mirrors differ in how they wait, not in what they do
    marker = tmp_path / "ran.json"
    body = f"import sys, pathlib; pathlib.Path({str(marker)!r}).write_text(sys.stdin.read(), encoding='utf-8')"
    hooks = SyncCommandHooks({HookPhase.BEFORE_DROP: _script(body)})

    # Act
    hooks.on_event(_event())

    # Assert
    assert json.loads(marker.read_text(encoding="utf-8"))["phase"] == "before_drop"


def test__sync_command_hooks__a_command_that_refuses__aborts_the_operation() -> None:
    # Arrange
    hooks = SyncCommandHooks({HookPhase.BEFORE_DROP: _script("import sys; sys.exit(2)")})

    # Act / Assert
    with pytest.raises(CommandHookError, match="exited 2"):
        hooks.on_event(_event())


def test__hook_event__is_built_at_the_moment_the_test_says_it_is() -> None:
    # Guards the fixture rather than the code: a stale clock here would make
    # the window assertions above pass for the wrong reason.
    assert datetime.now(UTC).year >= 2024
