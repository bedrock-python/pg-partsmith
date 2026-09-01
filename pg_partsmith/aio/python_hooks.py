"""Lifecycle hooks written as Python blocks in a configuration file.

One hook object holds one compiled block per phase and runs it with ``event``
and ``log`` in scope when that phase fires. Every block is compiled when the
object is built, so a document with a typo in it is refused before any DDL
runs rather than after some of it has.

Usage::

    hooks = PythonHooks({HookPhase.BEFORE_DROP: "log.warning('dropping %s', event.partition.name)"})
    service = PartitionLifecycleService(repo=repo, metadata=metadata, locks=locks, hooks=[hooks])

The block runs inline, in the process and on the loop that fires it. That is
the same footing as any hook written as a class, and the same responsibility:
a block that blocks, blocks maintenance.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pg_partsmith.events import HookPhase
from pg_partsmith.python_hooks import PythonHookError, compile_hook_source, run_hook_source

from .hooks import BasePartitionLifecycleHooks

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import CodeType

    from pg_partsmith.events import PartitionEvent

__all__ = ["PythonHookError", "PythonHooks"]


class PythonHooks(BasePartitionLifecycleHooks):
    """Runs one configured block of Python per phase.

    Overrides :meth:`on_event` alone; the phase is in the event, and the
    per-phase methods come from the no-op base, which the executor calls after
    this one.
    """

    def __init__(self, sources: Mapping[HookPhase, str], *, names: Mapping[HookPhase, str] | None = None) -> None:
        """Compile every block now.

        Args:
            sources: The block to run at each phase.
            names: What tracebacks call each block -- the file it came from,
                when it came from one. The phase by default.

        Raises:
            SyntaxError: If a block does not parse. Its line and column are
                the message, so it is not wrapped.
            ValueError: If a phase is not a hook phase, or a block is empty.
        """
        compiled: dict[HookPhase, CodeType] = {}
        for phase, source in sources.items():
            if phase not in set(HookPhase):
                msg = f"{phase!r} is not a hook phase; expected one of: {', '.join(p.value for p in HookPhase)}"
                raise ValueError(msg)
            if not source or not source.strip():
                msg = f"The {phase.value} block is empty"
                raise ValueError(msg)
            name = (names or {}).get(phase, f"<hooks.{phase.value}>")
            compiled[phase] = compile_hook_source(source, name=name)
        self._code = compiled
        self._loggers = {phase: logging.getLogger(f"pg_partsmith.hook.{phase.value}") for phase in compiled}

    @property
    def phases(self) -> tuple[HookPhase, ...]:
        """The phases a block is configured for, in declaration order."""
        return tuple(self._code)

    async def on_event(self, event: PartitionEvent) -> None:
        """Run the block configured for this event's phase, if there is one.

        Raises:
            PythonHookError: If the block raises, which is how it refuses the
                operation.
        """
        code = self._code.get(event.phase)
        if code is None:
            return
        run_hook_source(code, event, logger=self._loggers[event.phase])
