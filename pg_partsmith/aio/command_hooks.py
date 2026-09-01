"""Lifecycle hooks that run a command, for the stacks this library is not written in.

A Go or Ruby team already owns an archiver; what they need is for it to be
invoked at the right moment with the right metadata. That is all this is: one
hook object that runs a configured command per phase, hands it the
:class:`~pg_partsmith.events.PartitionEvent` as JSON on stdin, and treats a
non-zero exit exactly as a raised exception -- the operation is aborted and
left for the next run.

Nothing here needs a new context: the event is already a pydantic model, so the
payload the command reads is the same object a Python hook is handed, dumped
with ``by_alias=True``.

Usage::

    hooks = CommandHooks({HookPhase.BEFORE_DROP: ["/usr/local/bin/archive"]})
    service = PartitionLifecycleService(repo=repo, metadata=metadata, locks=locks, hooks=[hooks])

The command's stdout is logged. Its stderr is carried in the error when it
fails, which is the only place an operator will look at 03:00.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from pg_partsmith.constants import DEFAULT_HOOK_TIMEOUT_SECONDS
from pg_partsmith.events import HookPhase, hook_environment
from pg_partsmith.hook_commands import CommandHookError, run_hook_command, tail

from .hooks import BasePartitionLifecycleHooks

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from pg_partsmith.events import PartitionEvent

logger = logging.getLogger(__name__)

__all__ = ["CommandHookError", "CommandHooks"]


class CommandHooks(BasePartitionLifecycleHooks):
    """Runs one configured command per phase, with the event as JSON on stdin.

    Overrides :meth:`on_event` alone rather than one method per phase: the
    phase is in the event, and a dispatch table reads better than eight
    delegating methods that would each have to be added to again. The
    per-phase methods come from the no-op base, which the executor calls after
    this one.
    """

    def __init__(
        self,
        commands: Mapping[HookPhase, Sequence[str]],
        *,
        timeout_seconds: float = DEFAULT_HOOK_TIMEOUT_SECONDS,
    ) -> None:
        """Wire the commands.

        Args:
            commands: The command to run at each phase, as an argument vector.
                A vector rather than a string: nothing is passed through a
                shell, so a partition name can never be read as syntax.
            timeout_seconds: How long a command may take before it is killed and
                the operation abandoned. A hook that hangs holds the table's
                maintenance lock, so there is no "wait forever" option.

        Raises:
            ValueError: If a command is empty, or its phase is not a hook phase.
        """
        for phase, command in commands.items():
            if phase not in set(HookPhase):
                msg = f"{phase!r} is not a hook phase; expected one of: {', '.join(p.value for p in HookPhase)}"
                raise ValueError(msg)
            if not command or not all(isinstance(part, str) for part in command):
                msg = f"The {phase.value} command must be a non-empty list of strings"
                raise ValueError(msg)
        self._commands = {phase: tuple(command) for phase, command in commands.items()}
        self._timeout = timeout_seconds

    @property
    def phases(self) -> tuple[HookPhase, ...]:
        """The phases a command is configured for, in declaration order."""
        return tuple(self._commands)

    async def on_event(self, event: PartitionEvent) -> None:
        """Run the command configured for this event's phase, if there is one.

        Raises:
            CommandHookError: If the command exits non-zero, cannot be run, or
                outlives its timeout. A ``before_*`` hook failing this way
                abandons the operation; an ``after_*`` one abandons what is left
                of it, and the next run plans it again.
        """
        command = self._commands.get(event.phase)
        if command is None:
            return
        stdout = await asyncio.to_thread(
            run_hook_command,
            command,
            event.model_dump_json(by_alias=True).encode(),
            phase=event.phase,
            partition_name=event.partition.name,
            environment=hook_environment(event),
            timeout_seconds=self._timeout,
        )
        if stdout:
            logger.info(
                "%s hook said: %s",
                event.phase.value,
                tail(stdout),
                extra={"partition_name": event.partition.name, "table_name": event.table_name},
            )
