"""Running somebody else's program as a lifecycle hook, once, for both mirrors.

The command, its payload and everything that can go wrong with it are the same
question in the aio and the sync worlds; only the waiting differs. So the
running lives here, as ordinary blocking calls, and the aio mirror hands the
waiting to a thread.

That is also what makes it work everywhere. ``asyncio.create_subprocess_exec``
needs a loop that can spawn processes -- on Windows a selector loop, which is
what an asyncpg or psycopg deployment often installs, raises
``NotImplementedError`` instead -- and a hook that runs on Linux but not on a
developer's machine is a hook nobody trusts.

The run is split in three -- start, finish, stop -- rather than one call, so
that a run being cancelled can stop the child it started. A hook process that
outlives the maintenance run that fired it is a process nobody is watching.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from .events import HookPhase

__all__ = ["CommandHookError", "finish_hook_command", "run_hook_command", "start_hook_command", "stop_hook_command"]

_OUTPUT_TAIL = 2000
_STOP_GRACE_SECONDS = 5.0


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


def start_hook_command(
    command: Sequence[str],
    *,
    phase: HookPhase,
    partition_name: str,
    environment: Mapping[str, str],
) -> subprocess.Popen[bytes]:
    """Start one command with its stdin, stdout and stderr piped.

    Args:
        command: The argument vector. Never a shell string, so a partition name
            cannot be read as syntax.
        phase: Which moment this is, for the error.
        partition_name: What the operation is about, for the error.
        environment: The process environment.

    Returns:
        The running process, for :func:`finish_hook_command`.

    Raises:
        CommandHookError: If it cannot be started at all -- not found, not
            executable.
    """
    try:
        return subprocess.Popen(  # noqa: S603 - an argument vector, never a shell
            list(command),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=dict(environment),
        )
    except OSError as exc:
        raise CommandHookError(phase, partition_name, f"{command[0]}: {exc}") from exc


def finish_hook_command(
    process: subprocess.Popen[bytes],
    payload: bytes,
    *,
    phase: HookPhase,
    partition_name: str,
    timeout_seconds: float,
) -> bytes:
    """Hand the payload over, wait for the exit, and answer with stdout.

    Args:
        process: What :func:`start_hook_command` returned.
        payload: The event, as JSON, for the command's stdin.
        phase: Which moment this is, for the error.
        partition_name: What the operation is about, for the error.
        timeout_seconds: How long it may take. A hook that hangs holds the
            table's maintenance lock, so there is no "wait forever".

    Returns:
        Whatever the command wrote to stdout.

    Raises:
        CommandHookError: If it exits non-zero or outlives the timeout -- each
            of which abandons the operation. A child that outlived its timeout,
            or that was still running when the wait was interrupted, is stopped
            before this returns or raises: nothing it started keeps running
            unwatched.
    """
    name = process.args[0] if isinstance(process.args, (list, tuple)) else str(process.args)
    try:
        stdout, stderr = process.communicate(payload, timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        stop_hook_command(process)
        detail = f"{name} did not finish within {timeout_seconds}s and was killed"
        raise CommandHookError(phase, partition_name, detail) from None
    except BaseException:
        # Ctrl+C in the sync mirror lands here; the child goes with the run.
        stop_hook_command(process)
        raise

    if process.returncode != 0:
        detail = f"{name} exited {process.returncode}: {tail(stderr)}"
        raise CommandHookError(phase, partition_name, detail)
    return stdout


def stop_hook_command(process: subprocess.Popen[bytes], *, grace_seconds: float = _STOP_GRACE_SECONDS) -> None:
    """Stop a child the run no longer waits for: ask first, then insist.

    ``terminate`` gives an archiver a moment to close what it was writing;
    ``kill`` follows if it does not take it. Idempotent, and never raises: it
    runs on the way out of a cancellation, where there is nothing left to do
    about a failure but log it.

    Args:
        process: The child.
        grace_seconds: How long to wait between the two.
    """
    if process.poll() is not None:
        return
    try:
        process.terminate()
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    except OSError:  # pragma: no cover - already gone between the poll and the signal
        pass


def run_hook_command(
    command: Sequence[str],
    payload: bytes,
    *,
    phase: HookPhase,
    partition_name: str,
    environment: Mapping[str, str],
    timeout_seconds: float,
) -> bytes:
    """Start, feed and finish one command in one blocking call.

    The sync mirror's whole path, and the aio mirror's when nothing cancels it.
    See :func:`start_hook_command` and :func:`finish_hook_command` for what can
    go wrong at each step.
    """
    process = start_hook_command(command, phase=phase, partition_name=partition_name, environment=environment)
    return finish_hook_command(
        process, payload, phase=phase, partition_name=partition_name, timeout_seconds=timeout_seconds
    )


def tail(stream: bytes, limit: int = _OUTPUT_TAIL) -> str:
    """The end of a command's output, which is where it says what went wrong."""
    text = stream.decode(errors="replace").strip()
    return text if len(text) <= limit else f"…{text[-limit:]}"
