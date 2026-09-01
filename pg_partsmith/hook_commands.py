"""Running somebody else's program as a lifecycle hook, once, for both mirrors.

The command, its payload and everything that can go wrong with it are the same
question in the aio and the sync worlds; only the waiting differs. So the
running lives here, as an ordinary blocking call, and the aio mirror hands it to
a thread.

That is also what makes it work everywhere. ``asyncio.create_subprocess_exec``
needs a loop that can spawn processes -- on Windows a selector loop, which is
what an asyncpg or psycopg deployment often installs, raises
``NotImplementedError`` instead -- and a hook that runs on Linux but not on a
developer's machine is a hook nobody trusts.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from .events import HookPhase

__all__ = ["CommandHookError", "run_hook_command"]

_OUTPUT_TAIL = 2000


class CommandHookError(RuntimeError):
    """A configured command refused the operation, or could not be run.

    Raised so the executor treats it like any other hook failure: the operation
    is abandoned and planned again next run, and under ``continue_on_error`` the
    rest of the table is still maintained.
    """

    def __init__(self, phase: HookPhase, partition_name: str, detail: str) -> None:
        super().__init__(f"{phase.value} hook for {partition_name!r} failed: {detail}")
        self.phase = phase
        self.partition_name = partition_name
        self.detail = detail


def run_hook_command(
    command: Sequence[str],
    payload: bytes,
    *,
    phase: HookPhase,
    partition_name: str,
    environment: Mapping[str, str],
    timeout_seconds: float,
) -> bytes:
    """Run one command with ``payload`` on its stdin, and answer with its stdout.

    Args:
        command: The argument vector. Never a shell string, so a partition name
            cannot be read as syntax.
        payload: The event, as JSON.
        phase: Which moment this is, for the error.
        partition_name: What the operation is about, for the error.
        environment: The process environment.
        timeout_seconds: How long it may take. A hook that hangs holds the
            table's maintenance lock, so there is no "wait forever".

    Returns:
        Whatever the command wrote to stdout.

    Raises:
        CommandHookError: If it exits non-zero, cannot be run, or outlives the
            timeout -- each of which abandons the operation.
    """
    try:
        completed = subprocess.run(  # noqa: S603 - an argument vector, never a shell
            list(command),
            input=payload,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
            env=dict(environment),
        )
    except OSError as exc:
        raise CommandHookError(phase, partition_name, f"{command[0]}: {exc}") from exc
    except subprocess.TimeoutExpired:
        detail = f"{command[0]} did not finish within {timeout_seconds}s and was killed"
        raise CommandHookError(phase, partition_name, detail) from None

    if completed.returncode != 0:
        detail = f"{command[0]} exited {completed.returncode}: {tail(completed.stderr)}"
        raise CommandHookError(phase, partition_name, detail)
    return completed.stdout


def tail(stream: bytes, limit: int = _OUTPUT_TAIL) -> str:
    """The end of a command's output, which is where it says what went wrong."""
    text = stream.decode(errors="replace").strip()
    return text if len(text) <= limit else f"…{text[-limit:]}"
