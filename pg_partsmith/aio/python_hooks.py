"""Lifecycle hooks written as Python blocks in a configuration file.

One hook object holds one compiled block per phase and runs it with ``event``
and ``log`` in scope when that phase fires. Every block is compiled when the
object is built, so a document with a typo in it is refused before any DDL
runs rather than after some of it has.

Usage::

    hooks = PythonHooks({HookPhase.BEFORE_DROP: "log.warning('dropping %s', event.partition.name)"})
    service = PartitionLifecycleService(repo=repo, metadata=metadata, locks=locks, hooks=[hooks])

The block runs in the process, on a thread of its own, so the loop that fires
it stays free to take a stop. The responsibility is the one any hook written as
a class carries: a block that blocks, blocks maintenance until it returns.
"""

from __future__ import annotations

import asyncio
import logging
import threading
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

        The block runs on a thread of its own, so the loop stays free to take a
        stop signal while it runs: the run is cancelled and cleans up, and a
        block still running at that point is abandoned when the process exits.

        Raises:
            PythonHookError: If the block raises, which is how it refuses the
                operation.
        """
        code = self._code.get(event.phase)
        if code is None:
            return
        loop = asyncio.get_running_loop()
        outcome: asyncio.Future[None] = loop.create_future()
        hook_logger = self._loggers[event.phase]

        def run() -> None:
            try:
                run_hook_source(code, event, logger=hook_logger)
            except BaseException as exc:
                _settle(loop, outcome, exc)
            else:
                _settle(loop, outcome, None)

        threading.Thread(target=run, name=f"pg-partsmith-hook-{event.phase.value}", daemon=True).start()
        await outcome


def _settle(loop: asyncio.AbstractEventLoop, outcome: asyncio.Future[None], exc: BaseException | None) -> None:
    """Hand the block's result to the loop; nothing to hand if the run was cancelled or the loop is gone."""

    def deliver() -> None:
        if outcome.done():
            return
        if exc is None:
            outcome.set_result(None)
        else:
            outcome.set_exception(exc)

    try:
        loop.call_soon_threadsafe(deliver)
    except RuntimeError:  # the loop closed while the block ran: the process is on its way out
        pass
