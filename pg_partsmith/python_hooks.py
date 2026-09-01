"""Lifecycle hooks written as Python in a configuration file, once, for both mirrors.

The authentik model: a block of Python in the document, evaluated with a
prepared namespace when the moment comes. Expressive, no build step, and the
natural fit for the Python-adjacent audience -- a team that would rather write
five lines than ship a binary.

Two things this module is careful about, and one it deliberately is not.

It compiles every block when the document is read, not when the hook fires: a
``SyntaxError`` discovered at 03:00 by a CronJob, after some of the run's DDL
has committed, is the failure mode to design out. And it names each block
after its phase, so a traceback says ``before_drop`` and a line number rather
than ``<string>``.

What it does not do is sandbox anything. ``exec`` with a filtered
``__builtins__`` is not a security boundary and is trivially escaped; claiming
one would be worse than having none. The block runs as the process runs, with
every credential the process holds, and the documentation says so. Isolation,
if wanted, is a command hook in a container of its own.
"""

from __future__ import annotations

import logging
from types import CodeType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .events import HookPhase, PartitionEvent

__all__ = ["PythonHookError", "compile_hook_source", "run_hook_source"]


class PythonHookError(RuntimeError):
    """A block refused the operation by raising, or could not be run.

    Raised so the executor treats it like any other hook failure: the operation
    is abandoned and planned again next run, and under ``continue_on_error`` the
    rest of the table is still maintained. The block's own exception is the
    cause, so its traceback is not lost.
    """

    def __init__(self, phase: HookPhase, partition_name: str, detail: str) -> None:
        super().__init__(f"{phase.value} hook for {partition_name!r} failed: {detail}")
        self.phase = phase
        self.partition_name = partition_name
        self.detail = detail


def compile_hook_source(source: str, *, name: str) -> CodeType:
    """Compile one block, so a mistake in it is found now and not at 03:00.

    Args:
        source: The block, as the document gives it.
        name: What tracebacks call it -- the phase, or the file it came from.

    Returns:
        The code, ready for :func:`run_hook_source`.

    Raises:
        SyntaxError: If the block does not parse. Left as it is rather than
            wrapped, because its line and column are the whole message.
    """
    return compile(source, name, "exec", dont_inherit=True)


def run_hook_source(code: CodeType, event: PartitionEvent, *, logger: logging.Logger) -> None:
    """Run one compiled block with the namespace the documentation promises.

    The namespace is ``event`` -- the :class:`~pg_partsmith.events.PartitionEvent`,
    exactly what a Python hook class is handed -- and ``log``, a logger named
    for the phase. Nothing else is injected: a block that needs ``datetime``
    imports it, the way any Python does.

    Args:
        code: What :func:`compile_hook_source` returned.
        event: The event the hook is fired with.
        logger: Where ``log`` writes.

    Raises:
        PythonHookError: If the block raises. Raising is how a block refuses
            the operation, so the error names the phase and the partition and
            keeps the block's own exception as its cause.
    """
    namespace: dict[str, Any] = {
        "__name__": f"pg_partsmith.hook.{event.phase.value}",
        "event": event,
        "log": logger,
    }
    try:
        exec(code, namespace)  # noqa: S102 - the document is trusted code, and the docs say exactly that
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
        raise PythonHookError(event.phase, event.partition.name, detail) from exc
