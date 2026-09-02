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
import logging
import sys
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from pg_partsmith.__version__ import __version__
from pg_partsmith.aio import CommandHooks, PartitionToolkit, PythonHooks
from pg_partsmith.exceptions import InvalidPartitionConfigError, LockAcquisitionError, PlanConfigMismatchError

from .commands import CommandResult, run_apply, run_inspect, run_plan, run_validate
from .exit_codes import ExitCode
from .loader import (
    DSN_ENV_VAR,
    ConfigError,
    async_url,
    load_document,
    load_plans,
    load_python_hooks,
    resolve_dsn,
    select_configs,
)
from .render import to_json

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pg_partsmith.aio import PartitionLifecycleHooks
    from pg_partsmith.document import PartitionsDocument

logger = logging.getLogger("pg_partsmith.cli")

__all__ = ["app", "main"]

_EPILOG = (
    "Exit codes: 0 nothing pending, 2 drift found (plan --check), 3 findings need a human, "
    "4 configuration, 5 connection, 6 another maintainer holds the lock, 64 usage, 1 unexpected."
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
        "--dsn", show_default=False, help=f"connection string; falls back to ${DSN_ENV_VAR}, then to the document's dsn"
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
) -> int:
    """Read the partition tree that exists and print it."""
    return _execute(
        _Invocation("inspect", config=config, dsn=dsn, tables=tuple(table or ()), output=output, verbose=verbose)
    )


@app.command()
def plan(
    config: ConfigOption,
    dsn: DsnOption = None,
    table: TableOption = None,
    output: OutputOption = Output.human,
    verbose: VerboseOption = False,
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
        )
    )


@app.command()
def validate(
    config: ConfigOption,
    dsn: DsnOption = None,
    table: TableOption = None,
    output: OutputOption = Output.human,
    verbose: VerboseOption = False,
) -> int:
    """Check the document against the catalog. Exits non-zero on a problem."""
    return _execute(
        _Invocation("validate", config=config, dsn=dsn, tables=tuple(table or ()), output=output, verbose=verbose)
    )


@app.command()
def apply(
    config: ConfigOption,
    dsn: DsnOption = None,
    table: TableOption = None,
    output: OutputOption = Output.human,
    verbose: VerboseOption = False,
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
        )
    )


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
        result = asyncio.run(_run(invocation))
    except (ConfigError, InvalidPartitionConfigError, PlanConfigMismatchError) as exc:
        return _failed(str(exc), ExitCode.CONFIG)
    except LockAcquisitionError as exc:
        # An overlapping run is ordinary operation, not a failure to page on.
        return _failed(str(exc), ExitCode.LOCKED)
    except (SQLAlchemyError, OSError) as exc:
        # A refused connection surfaces as a bare OSError: the driver raises it
        # before SQLAlchemy has a DBAPI error to wrap. Reading the document is
        # already behind ConfigError, so an OSError here is the database.
        return _failed(f"database error: {exc}", ExitCode.CONNECTION)
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        return _failed("interrupted", ExitCode.FAILED)

    text = result.render(output=invocation.output.value)
    if text:
        sys.stdout.write(text + "\n")
    return result.code


async def _run(invocation: _Invocation) -> CommandResult:
    """Read the document, bring up the engine, and dispatch one command."""
    document = load_document(invocation.config)
    _check_python_hooks(document, invocation.config)
    configs = select_configs(document, invocation.tables)
    url = async_url(resolve_dsn(document, override=invocation.dsn))

    engine = create_async_engine(url)
    try:
        hooks = _hooks(
            document, command=invocation.command, config=invocation.config, allow_hooks=invocation.allow_hooks
        )
        kit = PartitionToolkit.from_options(engine, document.runtime, hooks=hooks)
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


def _save_plan(path: Path, result: CommandResult) -> None:
    """Write the plan envelope where ``apply --plan`` can read it back.

    Written whatever ``--output`` says: the file is the artifact between plan
    and apply, and what a person reads on the terminal is a separate question.
    """
    if result.payload is None:  # pragma: no cover - every command builds one
        msg = "the command produced no plan to save"
        raise ConfigError(msg)
    path.write_text(to_json(result.payload) + "\n", encoding="utf-8")


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


def _failed(message: str, code: ExitCode) -> ExitCode:
    """Say what went wrong on stderr, and answer with the code that names it.

    Not a log record: this is the message a person reads. The traceback behind
    it is kept for ``--verbose``, where the debug stream is already on.
    """
    sys.stderr.write(f"pg-partsmith: {message}\n")
    logger.debug("command failed with %s", code.name, exc_info=True)
    return code
