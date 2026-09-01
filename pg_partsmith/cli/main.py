"""The command line: argument parsing, the engine, and what the process exits with.

``inspect``, ``plan`` and ``validate`` issue no DDL; ``apply`` is the one that
acts, and withholds every destructive operation unless it is asked for them.
Logging goes to stderr so stdout stays parseable; ``--output json`` is the
models' own dump, so it cannot drift from the library.

The argument parser is the standard library's. The image this ships in is
measured, and a dependency is a poor trade for a nicer ``--help``.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from pg_partsmith.__version__ import __version__
from pg_partsmith.aio import CommandHooks, PartitionToolkit
from pg_partsmith.exceptions import InvalidPartitionConfigError, LockAcquisitionError, PlanConfigMismatchError

from .commands import CommandResult, run_apply, run_inspect, run_plan, run_validate
from .exit_codes import ExitCode
from .loader import (
    DSN_ENV_VAR,
    ConfigError,
    async_url,
    load_document,
    load_plans,
    resolve_dsn,
    select_configs,
)
from .render import to_json

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pg_partsmith.aio import PartitionLifecycleHooks
    from pg_partsmith.document import PartitionsDocument

logger = logging.getLogger("pg_partsmith.cli")

__all__ = ["build_parser", "main"]


def build_parser() -> argparse.ArgumentParser:
    """The whole command surface, as a parser."""
    parser = argparse.ArgumentParser(
        prog="pg-partsmith",
        description="Inspect, plan and run PostgreSQL partition maintenance from a configuration document.",
        epilog=(
            "Exit codes: 0 nothing pending, 2 drift found (plan --check), 3 findings need a human, "
            "4 configuration, 5 connection, 6 another maintainer holds the lock, 1 unexpected."
        ),
    )
    parser.add_argument("--version", action="version", version=f"pg-partsmith {__version__}")
    subcommands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    for name, help_text in (
        ("inspect", "read the partition tree that exists and print it"),
        ("plan", "show what maintenance would do, and why; issues no DDL"),
        ("validate", "check the document against the catalog; exits non-zero on a problem"),
        ("apply", "carry out maintenance; creations only unless --allow-destructive"),
    ):
        sub = subcommands.add_parser(name, help=help_text, description=help_text)
        _add_common_arguments(sub)
        if name == "plan":
            sub.add_argument(
                "--check",
                action="store_true",
                help="exit 2 when any operation is pending, for alerting on maintenance that has stopped running",
            )
            sub.add_argument(
                "--save",
                metavar="FILE",
                default=None,
                help="write the plan to FILE, for review and for `apply --plan FILE`",
            )
        if name == "apply":
            _add_apply_arguments(sub)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one command.

    Args:
        argv: Arguments, or None to read ``sys.argv``.

    Returns:
        The exit status, as :class:`~pg_partsmith.cli.exit_codes.ExitCode`.
    """
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    try:
        result = asyncio.run(_run(args))
    except (ConfigError, InvalidPartitionConfigError) as exc:
        return _failed(str(exc), ExitCode.CONFIG)
    except PlanConfigMismatchError as exc:
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

    text = result.render(as_json=args.output == "json")
    if text:
        sys.stdout.write(text + "\n")
    return result.code


async def _run(args: argparse.Namespace) -> CommandResult:
    """Read the document, bring up the engine, and dispatch one command."""
    document = load_document(Path(args.config))
    configs = select_configs(document, tuple(args.table))
    url = async_url(resolve_dsn(document, override=args.dsn))

    engine = create_async_engine(url)
    try:
        kit = PartitionToolkit.from_options(engine, document.runtime, hooks=_hooks(document, args))
        if args.command == "inspect":
            return await run_inspect(kit, configs)
        if args.command == "plan":
            result = await run_plan(kit, configs, check=args.check)
            if args.save:
                _save_plan(Path(args.save), result)
            return result
        if args.command == "apply":
            return await run_apply(
                kit,
                configs,
                plans=load_plans(Path(args.plan)) if args.plan else None,
                allow_destructive=args.allow_destructive,
                continue_on_error=args.continue_on_error,
                allow_config_drift=args.allow_config_drift,
            )
        return await run_validate(kit, configs)
    finally:
        await engine.dispose()


def _hooks(document: PartitionsDocument, args: argparse.Namespace) -> list[PartitionLifecycleHooks] | None:
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
    if args.command != "apply":
        return None
    if not getattr(args, "allow_hooks", False):
        named = ", ".join(phase.value for phase in hooks.commands())
        msg = (
            f"This document runs commands ({named}). They execute in a process holding DDL credentials, "
            "so they run only when asked for: pass --allow-hooks, or remove the hooks section."
        )
        raise ConfigError(msg)
    # Annotated as the protocol, not the class: a list is invariant, and the
    # toolkit takes any hook implementation.
    configured: list[PartitionLifecycleHooks] = [CommandHooks(hooks.commands(), timeout_seconds=hooks.timeout_seconds)]
    return configured


def _add_apply_arguments(parser: argparse.ArgumentParser) -> None:
    """What only ``apply`` takes, including the two that decide how much it may do."""
    parser.add_argument(
        "--plan",
        metavar="FILE",
        default=None,
        help="apply the plan saved in FILE instead of planning now; refused if it was not made from this document",
    )
    parser.add_argument(
        "--allow-destructive",
        action="store_true",
        help="carry out detaches and drops as well; without it, only partitions are created",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="isolate a failed operation into the issues instead of aborting the run",
    )
    parser.add_argument(
        "--allow-config-drift",
        action="store_true",
        help="apply a saved plan whose configuration has changed since it was made",
    )
    parser.add_argument(
        "--allow-hooks",
        action="store_true",
        help="run the commands the document's hooks section names; without it, a document declaring any is refused",
    )


def _save_plan(path: Path, result: CommandResult) -> None:
    """Write the plan envelope where ``apply --plan`` can read it back.

    Written whatever ``--output`` says: the file is the artifact between plan
    and apply, and what a person reads on the terminal is a separate question.
    """
    if result.payload is None:  # pragma: no cover - every command builds one
        msg = "the command produced no plan to save"
        raise ConfigError(msg)
    path.write_text(to_json(result.payload) + "\n", encoding="utf-8")


def _failed(message: str, code: ExitCode) -> ExitCode:
    """Say what went wrong on stderr, and answer with the code that names it.

    Not a log record: this is the message a person reads. The traceback behind
    it is kept for ``--verbose``, where the debug stream is already on.
    """
    sys.stderr.write(f"pg-partsmith: {message}\n")
    logger.debug("command failed with %s", code.name, exc_info=True)
    return code


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Every command reads a document, may be narrowed, and can speak JSON."""
    parser.add_argument(
        "-c", "--config", required=True, metavar="FILE", help="the configuration document (.yaml/.json)"
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help=f"connection string; falls back to ${DSN_ENV_VAR}, then to the document's dsn",
    )
    parser.add_argument(
        "--table",
        action="append",
        default=[],
        metavar="NAME",
        help="only this table, qualified or bare; repeatable (default: every table in the document)",
    )
    parser.add_argument(
        "-o", "--output", choices=("human", "json"), default="human", help="output format (default: human)"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="log what it is doing, to stderr")
