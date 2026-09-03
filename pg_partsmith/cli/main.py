"""The command line: argument parsing, the engine, and what the process exits with.

``inspect``, ``plan`` and ``validate`` issue no DDL; ``apply`` is the one that
acts, and withholds every destructive operation unless it is asked for them.
Logging goes to stderr so stdout stays parseable; ``--output json`` is the
models' own dump, so it cannot drift from the library.

The arguments are declared as type annotations and parsed by typer, which is
click underneath: every flag has a type, the help is generated from the same
declarations that parse it, and shell completion comes for free. The process
still leaves through :func:`main`, which turns whatever a command returns --
or whatever parsing raised -- into one :class:`~pg_partsmith.cli.exit_codes.ExitCode`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer
from sqlalchemy.exc import ArgumentError, DBAPIError, NoSuchModuleError, SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from pg_partsmith.__version__ import __version__
from pg_partsmith.aio import CommandHooks, PartitionToolkit, PythonHooks
from pg_partsmith.document import PartitionsDocument
from pg_partsmith.exceptions import (
    InvalidPartitionConfigError,
    LockAcquisitionError,
    PartitionError,
    PlanConfigMismatchError,
)
from pg_partsmith.hook_commands import CommandHookError
from pg_partsmith.python_hooks import PythonHookError
from pg_partsmith.utils import pg_sqlstate

from .commands import CommandResult, run_apply, run_inspect, run_plan, run_validate
from .exit_codes import ExitCode
from .loader import (
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
from .render import to_json

try:
    from asyncpg.exceptions import InterfaceError as _DriverInterfaceError
    from asyncpg.exceptions import PostgresError as _DriverPostgresError
except ImportError:  # pragma: no cover - the cli extra brings asyncpg; a bare install may not
    _DRIVER_ERRORS: tuple[type[Exception], ...] = ()
else:
    _DRIVER_ERRORS = (_DriverPostgresError, _DriverInterfaceError)

# Whatever the database answers with, at any depth, so the mapping to exit 5 is one place.
_DATABASE_ERRORS: tuple[type[Exception], ...] = (SQLAlchemyError, OSError, *_DRIVER_ERRORS)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pg_partsmith.aio import PartitionLifecycleHooks

logger = logging.getLogger("pg_partsmith.cli")

__all__ = ["app", "main"]

_EPILOG = (
    "Exit codes: 0 nothing pending, 2 drift found (plan --check), 3 findings need a human, "
    "4 configuration, 5 the database, 6 another maintainer holds the lock, 64 usage, "
    "130 / 143 stopped by a signal, 1 unexpected."
)


class Output(StrEnum):
    """The forms a result can be printed in."""

    human = "human"
    json = "json"
    metrics = "metrics"


app = typer.Typer(
    name="pg-partsmith",
    help="Inspect, plan and run PostgreSQL partition maintenance from a configuration document.",
    epilog=_EPILOG,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    # Tracebacks are ours to print, under --verbose; a rich one for an operator
    # reading a CronJob log is noise dressed up.
    pretty_exceptions_enable=False,
)


# ── Options every command takes ─────────────────────────────────────────────────

ConfigOption = Annotated[
    Path,
    typer.Option("--config", "-c", metavar="FILE", show_default=False, help="the configuration document (.yaml/.json)"),
]
DsnOption = Annotated[
    str | None,
    typer.Option(
        "--dsn",
        show_default=False,
        help=f"connection string; falls back to ${DSN_ENV_VAR}, then to the file ${DSN_FILE_ENV_VAR} names, "
        "then to the document's dsn",
    ),
]
TableOption = Annotated[
    list[str] | None,
    typer.Option(
        "--table",
        metavar="NAME",
        show_default=False,
        help="only this table, qualified or bare; repeatable. Every table in the document otherwise",
    ),
]
OutputOption = Annotated[
    Output,
    typer.Option("--output", "-o", help="human, json, or Prometheus text for a node_exporter textfile"),
]
VerboseOption = Annotated[bool, typer.Option("--verbose", "-v", help="log what it is doing, to stderr")]
WriteOption = Annotated[
    Path | None,
    typer.Option(
        "--write",
        metavar="FILE",
        show_default=False,
        help="write the output to FILE instead of stdout, atomically; for a node_exporter textfile",
    ),
]


@dataclass(frozen=True, slots=True)
class _Invocation:
    """One parsed command, with every flag it could carry."""

    command: str
    config: Path
    dsn: str | None
    tables: tuple[str, ...]
    output: Output
    verbose: bool
    check: bool = False
    save: Path | None = None
    locks: bool = False
    plan: Path | None = None
    allow_destructive: bool = False
    continue_on_error: bool = False
    allow_config_drift: bool = False
    allow_hooks: bool = False
    write: Path | None = None
    ok_if_locked: bool = False


def _version(value: bool) -> None:
    """``--version`` reads the library's own number: there is deliberately no second one."""
    if value:
        typer.echo(f"pg-partsmith {__version__}")
        raise typer.Exit


@app.callback()
def _root(
    version: Annotated[
        bool, typer.Option("--version", callback=_version, is_eager=True, help="show the version and exit")
    ] = False,
) -> None:
    """Inspect, plan and run PostgreSQL partition maintenance from a configuration document."""


# ── The commands ────────────────────────────────────────────────────────────────


@app.command()
def inspect(
    config: ConfigOption,
    dsn: DsnOption = None,
    table: TableOption = None,
    output: OutputOption = Output.human,
    verbose: VerboseOption = False,
    write: WriteOption = None,
) -> int:
    """Read the partition tree that exists and print it."""
    return _execute(
        _Invocation(
            "inspect", config=config, dsn=dsn, tables=tuple(table or ()), output=output, verbose=verbose, write=write
        )
    )


@app.command()
def plan(
    config: ConfigOption,
    dsn: DsnOption = None,
    table: TableOption = None,
    output: OutputOption = Output.human,
    verbose: VerboseOption = False,
    write: WriteOption = None,
    check: Annotated[
        bool,
        typer.Option(
            "--check",
            help="exit 2 when any operation is pending, for alerting on maintenance that has stopped running",
        ),
    ] = False,
    save: Annotated[
        Path | None,
        typer.Option(
            "--save", metavar="FILE", show_default=False, help="write the plan to FILE, for `apply --plan FILE`"
        ),
    ] = None,
    locks: Annotated[
        bool, typer.Option("--locks", help="after each operation, the heaviest lock it takes and on what")
    ] = False,
) -> int:
    """Show what maintenance would do, and why. Issues no DDL."""
    return _execute(
        _Invocation(
            "plan",
            config=config,
            dsn=dsn,
            tables=tuple(table or ()),
            output=output,
            verbose=verbose,
            check=check,
            save=save,
            locks=locks,
            write=write,
        )
    )


@app.command()
def validate(
    config: ConfigOption,
    dsn: DsnOption = None,
    table: TableOption = None,
    output: OutputOption = Output.human,
    verbose: VerboseOption = False,
    write: WriteOption = None,
) -> int:
    """Check the document against the catalog. Exits non-zero on a problem."""
    return _execute(
        _Invocation(
            "validate", config=config, dsn=dsn, tables=tuple(table or ()), output=output, verbose=verbose, write=write
        )
    )


@app.command()
def apply(
    config: ConfigOption,
    dsn: DsnOption = None,
    table: TableOption = None,
    output: OutputOption = Output.human,
    verbose: VerboseOption = False,
    write: WriteOption = None,
    plan: Annotated[
        Path | None,
        typer.Option(
            "--plan",
            metavar="FILE",
            show_default=False,
            help="apply the plan saved in FILE instead of planning now; refused if it was not made from this document",
        ),
    ] = None,
    allow_destructive: Annotated[
        bool,
        typer.Option(
            "--allow-destructive",
            help="carry out detaches and drops as well; without it, only partitions are created",
        ),
    ] = False,
    continue_on_error: Annotated[
        bool,
        typer.Option("--continue-on-error", help="isolate a failed operation into the issues instead of aborting"),
    ] = False,
    allow_config_drift: Annotated[
        bool,
        typer.Option(
            "--allow-config-drift", help="apply a saved plan whose configuration has changed since it was made"
        ),
    ] = False,
    allow_hooks: Annotated[
        bool,
        typer.Option(
            "--allow-hooks",
            help="run what the document's hooks section names; without it, a document declaring any is refused",
        ),
    ] = False,
    ok_if_locked: Annotated[
        bool,
        typer.Option(
            "--ok-if-locked",
            help="exit 0 rather than 6 when another maintainer holds the lock; for an init container",
        ),
    ] = False,
) -> int:
    """Carry out maintenance. Creations only, unless --allow-destructive."""
    return _execute(
        _Invocation(
            "apply",
            config=config,
            dsn=dsn,
            tables=tuple(table or ()),
            output=output,
            verbose=verbose,
            plan=plan,
            allow_destructive=allow_destructive,
            continue_on_error=continue_on_error,
            allow_config_drift=allow_config_drift,
            allow_hooks=allow_hooks,
            write=write,
            ok_if_locked=ok_if_locked,
        )
    )


@app.command()
def schema() -> int:
    """Print the document's JSON Schema, for an editor to validate against."""
    sys.stdout.write(json.dumps(PartitionsDocument.model_json_schema(), indent=2) + "\n")
    return ExitCode.OK


# ── Leaving the process ─────────────────────────────────────────────────────────


def main(argv: Sequence[str] | None = None) -> int:
    """Run one command and answer with its exit status.

    Parsing is left to typer, but the process leaves through here, so that a
    usage error -- a misspelled flag, no command at all -- has a code of its own
    rather than the ``2`` argparse and click both use, which this tool means
    "drift" by. A CronJob alerting on drift must not page over a typo.

    Args:
        argv: Arguments, or None to read ``sys.argv``.

    Returns:
        The exit status, as :class:`~pg_partsmith.cli.exit_codes.ExitCode`.
    """
    _survive_any_console_encoding()
    try:
        outcome = app(args=list(argv) if argv is not None else None, prog_name="pg-partsmith", standalone_mode=False)
    except typer.Abort:  # pragma: no cover - interactive only
        return _failed("interrupted", ExitCode.FAILED)
    except Exception as exc:
        if not _is_usage_error(exc):
            raise
        _usage_error(exc)
        return ExitCode.USAGE
    return _as_code(outcome)


def _execute(invocation: _Invocation) -> ExitCode:
    """Read, connect, dispatch, print, and turn every failure into its code."""
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if invocation.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    try:
        # A Runner with a loop factory rather than asyncio.run: the process is left
        # with the current-loop state it had, which matters to a program that
        # embeds main() next to a loop of its own, and to a test session.
        with asyncio.Runner(loop_factory=asyncio.new_event_loop) as runner:
            result = runner.run(_supervised(invocation))
    except (ConfigError, InvalidPartitionConfigError, PlanConfigMismatchError) as exc:
        return _failed(str(exc), ExitCode.CONFIG)
    except LockAcquisitionError as exc:
        # An overlapping run is ordinary operation, not a failure to page on --
        # and for an init container, which restarts on anything non-zero, it
        # is not even worth a code of its own.
        return _failed(str(exc), ExitCode.OK if invocation.ok_if_locked else ExitCode.LOCKED)
    except (PartitionError, CommandHookError, PythonHookError) as exc:
        # A run that stopped on its own terms -- a hook refused, a plan went
        # stale, a partition is still referenced -- is a finding for a person,
        # the way --continue-on-error would have recorded it. Not a traceback.
        return _failed(str(exc), ExitCode.FINDINGS)
    except DBAPIError as exc:
        # The server answered with a SQLSTATE. One about the statement -- a
        # missing grant, a constraint, a statement timeout -- is a finding for
        # a person, the way --continue-on-error would have recorded it; one
        # about reaching the server at all is the connection's.
        if _refused_by_the_server(exc):
            return _failed(f"the database refused: {_reason(exc)}", ExitCode.FINDINGS)
        return _failed(f"database error: {_reason(exc)}", ExitCode.CONNECTION)
    except (ArgumentError, NoSuchModuleError) as exc:
        # A connection string SQLAlchemy cannot parse, or one naming a driver
        # that is not installed: the deployment's to fix, before any database.
        return _failed(f"the connection string is not usable: {exc}", ExitCode.CONFIG)
    except _DATABASE_ERRORS as exc:
        # A refused connection surfaces as a bare OSError and a rejected
        # password as the driver's own error: both come before SQLAlchemy has
        # a DBAPI error to wrap. Reading the document is already behind
        # ConfigError, so anything here is the database.
        return _failed(f"database error: {exc}", ExitCode.CONNECTION)
    except _StopSignalError as exc:
        word, code = _STOP_SIGNALS[exc.signum]
        return _failed(word, code)
    except KeyboardInterrupt:  # pragma: no cover - the Windows route; CI runs on Linux
        # Where the loop cannot take a signal handler of its own -- Windows --
        # asyncio.run's own Ctrl+C handling cancels the task and raises this
        # once it has cleaned up. The same outcome, by the runner's route.
        return _failed("interrupted", ExitCode.INTERRUPTED)

    text = result.render(output=invocation.output.value)
    if invocation.write is not None:
        try:
            _write_atomically(invocation.write, text + "\n")
        except OSError as exc:
            # A textfile directory the container's user cannot write -- a
            # root-owned hostPath, usually -- is the deployment's to fix. The
            # run has done its work; say which path, and leave the previous
            # file where it was.
            return _failed(f"Cannot write {invocation.write}: {exc.strerror or exc}", ExitCode.CONFIG)
        logger.info("wrote %s output to %s", invocation.output.value, invocation.write)
    elif text:
        sys.stdout.write(text + "\n")
    return result.code


class _StopSignalError(Exception):
    """The run was stopped by a signal, and has cleaned up on the way here."""

    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


_STOP_SIGNALS: dict[int, tuple[str, ExitCode]] = {
    signal.SIGINT: ("interrupted", ExitCode.INTERRUPTED),
    signal.SIGTERM: ("terminated", ExitCode.TERMINATED),
}


async def _supervised(invocation: _Invocation) -> CommandResult:
    """Run the command, with a stop signal turned into a cancellation that cleans up.

    ``SIGTERM`` is what a pod is sent at its deadline, and Python's default for
    it is to die on the spot -- no ``finally``, no lock released by us, no line
    in the log. The database survives that (a dropped connection cancels the
    statement and rolls the transaction back; a session lock goes with the
    session), but "survives" is not "was told". Cancelling the task instead
    takes the same path a raised exception takes: the statement is cancelled
    by the driver, the lock is released, the engine is disposed, a hook's child
    process is stopped, and the exit code says which signal it was.

    Where the loop cannot take signal handlers -- a worker thread, Windows --
    nothing is installed and ``asyncio.run``'s own Ctrl+C handling applies.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    received: list[int] = []

    def stop(signum: int) -> None:
        # A second signal changes nothing: the first already cancelled the run,
        # and what is left is the cleanup, which is worth letting finish.
        if not received and task is not None:
            received.append(signum)
            task.cancel()

    installed: list[int] = []
    for signum in _STOP_SIGNALS:
        try:
            loop.add_signal_handler(signum, stop, signum)
        except (NotImplementedError, RuntimeError, ValueError):  # pragma: no cover - Windows, or a worker thread
            break
        installed.append(signum)
    try:
        return await _run(invocation)
    except asyncio.CancelledError:
        if received:
            raise _StopSignalError(received[0]) from None
        raise
    finally:
        for signum in installed:
            loop.remove_signal_handler(signum)


async def _run(invocation: _Invocation) -> CommandResult:
    """Read the document, bring up the engine, and dispatch one command."""
    document = load_document(invocation.config)
    _check_python_hooks(document, invocation.config)
    configs = select_configs(document, invocation.tables)
    url = async_url(resolve_dsn(document, override=invocation.dsn))

    engine = create_async_engine(url)
    try:
        try:
            hooks = _hooks(
                document, command=invocation.command, config=invocation.config, allow_hooks=invocation.allow_hooks
            )
            kit = PartitionToolkit.from_options(engine, document.runtime, hooks=hooks)
        except ValueError as exc:
            # The models refuse what they can see; whatever the wiring still
            # objects to is the document's fault all the same.
            raise ConfigError(str(exc)) from exc
        if invocation.command == "inspect":
            return await run_inspect(kit, configs)
        if invocation.command == "plan":
            result = await run_plan(kit, configs, check=invocation.check, locks=invocation.locks)
            if invocation.save is not None:
                _save_plan(invocation.save, result)
            return result
        if invocation.command == "apply":
            return await run_apply(
                kit,
                configs,
                plans=load_plans(invocation.plan) if invocation.plan is not None else None,
                allow_destructive=invocation.allow_destructive,
                continue_on_error=invocation.continue_on_error,
                allow_config_drift=invocation.allow_config_drift,
            )
        return await run_validate(kit, configs)
    finally:
        await engine.dispose()


def _hooks(
    document: PartitionsDocument, *, command: str, config: Path, allow_hooks: bool
) -> list[PartitionLifecycleHooks] | None:
    """The hooks this run honours, and a refusal when it was not told to honour any.

    Hooks fire during ``apply`` alone, so nothing else has to decide. Ignoring a
    configured ``before_drop`` silently would be the worst outcome available: an
    operator would read the file, believe their archiver ran, and be wrong. So a
    document declaring hooks is refused rather than quietly stripped.

    Raises:
        ConfigError: If the document names hooks and ``--allow-hooks`` was not given.
    """
    hooks = document.hooks
    if hooks is None or hooks.is_empty:
        return None
    if command != "apply":
        return None
    if not allow_hooks:
        named = ", ".join(phase.value for phase in hooks.actions())
        msg = (
            f"This document runs code at {named}. It executes in a process holding DDL credentials, "
            "so it runs only when asked for: pass --allow-hooks, or remove the hooks section."
        )
        raise ConfigError(msg)
    # Annotated as the protocol, not the class: a list is invariant, and the
    # toolkit takes any hook implementation.
    configured: list[PartitionLifecycleHooks] = []
    if hooks.commands():
        configured.append(CommandHooks(hooks.commands(), timeout_seconds=hooks.timeout_seconds))
    sources, names = load_python_hooks(hooks, config.resolve().parent)
    if sources:
        configured.append(PythonHooks(sources, names=names))
    return configured


def _check_python_hooks(document: PartitionsDocument, config: Path) -> None:
    """Read and compile every file-backed block, whatever the command.

    Inline blocks were compiled when the document validated; a file has to be
    found and read first, and ``validate`` is the command that should find it
    missing or broken -- not ``apply``, and not at 03:00.
    """
    if document.hooks is not None and document.hooks.python_blocks():
        load_python_hooks(document.hooks, config.resolve().parent)


def _write_atomically(path: Path, text: str) -> None:
    """Write next to the target and rename, so a reader never sees half a file.

    A node_exporter textfile collector reads whenever it likes; a partially
    written file parses as a broken one, and a broken one is dropped whole.
    """
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def _save_plan(path: Path, result: CommandResult) -> None:
    """Write the plan envelope where ``apply --plan`` can read it back.

    Written whatever ``--output`` says: the file is the artifact between plan
    and apply, and what a person reads on the terminal is a separate question.
    """
    if result.payload is None:  # pragma: no cover - every command builds one
        msg = "the command produced no plan to save"
        raise ConfigError(msg)
    try:
        path.write_text(to_json(result.payload) + "\n", encoding="utf-8")
    except OSError as exc:
        # Raised inside the run, where a bare OSError would be read as the database's.
        msg = f"Cannot write the plan to {path}: {exc.strerror or exc}"
        raise ConfigError(msg) from exc


def _as_code(outcome: object) -> ExitCode:
    """What ``app()`` handed back, as an exit code.

    A command returns its :class:`ExitCode`; ``--help`` and ``--version`` leave
    through click's own ``Exit`` and come back as ``0``.
    """
    if isinstance(outcome, ExitCode):
        return outcome
    if isinstance(outcome, int):
        return ExitCode(outcome) if outcome in set(ExitCode) else ExitCode.FAILED
    return ExitCode.OK


def _is_usage_error(exc: Exception) -> bool:
    """Whether parsing, not the command, raised this.

    Recognised by click's own ``UsageError`` interface rather than by class:
    typer vendors its copy of click from 0.27 and imports the real one before
    that, so the class lives in two places across the versions this supports,
    one of them private. The interface -- ``exit_code`` of ``2`` and
    ``format_message`` -- is the documented, stable part.
    """
    return getattr(exc, "exit_code", None) == 2 and callable(getattr(exc, "format_message", None))


def _usage_error(exc: Exception) -> None:
    """Say what was wrong with the invocation, and how it is spelled.

    No command at all is the one usage error that has already explained
    itself: click prints the help for it before raising, so there is nothing
    to add but the exit code.
    """
    if type(exc).__name__ == "NoArgsIsHelpError":
        return
    ctx = getattr(exc, "ctx", None)
    if ctx is not None:
        sys.stderr.write(ctx.get_usage() + "\n")
    sys.stderr.write(f"pg-partsmith: {exc.format_message()}\n")  # type: ignore[attr-defined]


# SQLSTATE classes about reaching the server rather than about a statement:
# connection exceptions, authorization, an unknown database, resources the
# server ran out of, and the operator taking it down.
_NOT_THE_STATEMENTS_FAULT = ("08", "28", "3D", "53", "57P")


def _refused_by_the_server(exc: DBAPIError) -> bool:
    """Whether the server answered a statement with a SQLSTATE of its own, rather than failing to answer."""
    state = pg_sqlstate(exc)
    if not state:
        return False
    return not state.startswith(_NOT_THE_STATEMENTS_FAULT)


def _reason(exc: DBAPIError) -> object:
    """The driver's own message: the asyncpg adapter wraps it and prefixes the class name."""
    return getattr(exc.orig, "__cause__", None) or exc.orig or exc


def _failed(message: str, code: ExitCode) -> ExitCode:
    """Say what went wrong on stderr, and answer with the code that names it.

    Not a log record: this is the message a person reads. The traceback behind
    it is kept for ``--verbose``, where the debug stream is already on.
    """
    sys.stderr.write(f"pg-partsmith: {message}\n")
    logger.debug("command failed with %s", code.name, exc_info=True)
    return code


def _survive_any_console_encoding() -> None:
    """Never die on a character the console cannot show.

    Human output has an em dash in it, and a Windows pipe or a bare POSIX
    locale may be encoding in something without one. An escaped character in
    a CronJob log beats a traceback there.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(errors="backslashreplace")
        except (OSError, ValueError):  # pragma: no cover - a closed or exotic stream
            pass
