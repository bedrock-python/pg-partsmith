"""``PythonHooks``: a lifecycle hook written as a block in a configuration file."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from pg_partsmith.aio.python_hooks import PythonHookError, PythonHooks
from pg_partsmith.entities import PartitionGranularity, PartitionInfo, TablePartitionConfig
from pg_partsmith.events import HookPhase, PartitionEvent
from pg_partsmith.plan import DropPartition, Reason
from pg_partsmith.python_hooks import compile_hook_source, run_hook_source
from pg_partsmith.sync.python_hooks import PythonHooks as SyncPythonHooks
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
    operation = DropPartition(target="public.events__2025_08", reason=Reason.GRACE_ELAPSED, oid=42, size_bytes=4096)
    return PartitionEvent.build(phase, CONFIG, info, operation)


async def test__on_event__the_block_sees_the_event_and_a_logger(tmp_path: Path) -> None:
    # Arrange: a block that writes what it was handed
    marker = tmp_path / "seen.txt"
    source = f"""
import pathlib
pathlib.Path({str(marker)!r}).write_text(
    f"{{event.phase.value}} {{event.partition.name}} {{event.operation.size_bytes}} {{event.window.start.date()}}",
    encoding="utf-8",
)
log.info("seen %s", event.partition.name)
"""
    hooks = PythonHooks({HookPhase.BEFORE_DROP: source})

    # Act
    await hooks.on_event(_event())

    # Assert
    assert marker.read_text(encoding="utf-8") == "before_drop events__2025_08 4096 2025-08-01"


async def test__on_event__another_phase__runs_nothing(tmp_path: Path) -> None:
    # Arrange
    marker = tmp_path / "ran"
    hooks = PythonHooks({HookPhase.BEFORE_DROP: f"import pathlib; pathlib.Path({str(marker)!r}).write_text('x')"})

    # Act
    await hooks.on_event(_event(HookPhase.AFTER_CREATE))

    # Assert
    assert not marker.exists()


async def test__on_event__a_block_that_raises__refuses_the_operation_and_keeps_the_cause() -> None:
    # Arrange: raising is how a block says "not yet"
    hooks = PythonHooks({HookPhase.BEFORE_DROP: "raise RuntimeError('rows still referenced')"})

    # Act / Assert
    with pytest.raises(PythonHookError, match="rows still referenced") as exc:
        await hooks.on_event(_event())
    assert exc.value.phase is HookPhase.BEFORE_DROP
    assert exc.value.partition_name == "events__2025_08"
    assert isinstance(exc.value.__cause__, RuntimeError)


def test__python_hooks__a_block_that_does_not_parse__is_refused_when_built() -> None:
    # A SyntaxError found at 03:00 by a CronJob is the failure mode to design out.
    with pytest.raises(SyntaxError):
        PythonHooks({HookPhase.BEFORE_DROP: "if event.partition.name\n    pass"})


def test__python_hooks__a_block_is_named_after_its_phase_in_tracebacks() -> None:
    # Arrange
    code = compile_hook_source("raise ValueError('x')", name="<hooks.before_drop>")

    # Act / Assert
    with pytest.raises(PythonHookError) as exc:
        run_hook_source(code, _event(), logger=logging.getLogger("test"))
    assert exc.value.__cause__ is not None
    assert exc.value.__cause__.__traceback__ is not None
    frames = []
    trace = exc.value.__cause__.__traceback__
    while trace is not None:
        frames.append(trace.tb_frame.f_code.co_filename)
        trace = trace.tb_next
    assert "<hooks.before_drop>" in frames


def test__python_hooks__an_empty_block__is_refused() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="empty"):
        PythonHooks({HookPhase.BEFORE_DROP: "   "})


def test__python_hooks__implements_every_phase_method_the_executor_calls() -> None:
    # The executor calls the method named for the phase after on_event.
    hooks = PythonHooks({HookPhase.AFTER_CREATE: "pass"})

    # Act / Assert
    assert all(callable(getattr(hooks, phase.value, None)) for phase in HookPhase)
    assert hooks.phases == (HookPhase.AFTER_CREATE,)


def test__sync_python_hooks__runs_the_same_block_the_same_way(tmp_path: Path) -> None:
    # Arrange
    marker = tmp_path / "seen.txt"
    hooks = SyncPythonHooks(
        {HookPhase.BEFORE_DROP: f"import pathlib; pathlib.Path({str(marker)!r}).write_text(event.table_name)"}
    )

    # Act
    hooks.on_event(_event())

    # Assert
    assert marker.read_text() == "public.events"


def test__sync_python_hooks__a_block_that_raises__refuses_the_operation() -> None:
    # Arrange
    hooks = SyncPythonHooks({HookPhase.BEFORE_DROP: "raise KeyError('archive')"})

    # Act / Assert
    with pytest.raises(PythonHookError, match="KeyError"):
        hooks.on_event(_event())


def test__python_hooks__a_phase_that_is_not_one__is_refused_in_both_mirrors() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="not a hook phase"):
        PythonHooks({"before_lunch": "pass"})  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="not a hook phase"):
        SyncPythonHooks({"before_lunch": "pass"})  # type: ignore[dict-item]


def test__sync_python_hooks__the_same_refusals_the_same_phases_and_the_same_silence(tmp_path: Path) -> None:
    # Arrange
    marker = tmp_path / "ran"
    body = f"import pathlib; pathlib.Path({str(marker)!r}).write_text('x')"
    hooks = SyncPythonHooks({HookPhase.BEFORE_DROP: body})

    # Act / Assert
    with pytest.raises(ValueError, match="empty"):
        SyncPythonHooks({HookPhase.BEFORE_DROP: "  "})
    with pytest.raises(SyntaxError):
        SyncPythonHooks({HookPhase.BEFORE_DROP: "def broken(:"})
    assert hooks.phases == (HookPhase.BEFORE_DROP,)
    hooks.on_event(_event(HookPhase.AFTER_CREATE))
    assert not marker.exists()
