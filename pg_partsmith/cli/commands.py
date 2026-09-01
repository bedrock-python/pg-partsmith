"""What each command does, once the document is read and the engine is up.

Every command here is read-only: it issues no DDL, takes no lock, and fires no
hook. What they differ in is what they say and what they exit with -- a
CronJob and a CI step read the exit code and nothing else.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pg_partsmith.aio import PartitionToolkit, PartitionValidationService
from pg_partsmith.exceptions import InvalidPartitionConfigError

from .exit_codes import ExitCode
from .render import describe_tree, envelope, plan_entry, to_json, tree_entry

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pg_partsmith.entities import TablePartitionConfig

logger = logging.getLogger("pg_partsmith.cli")

__all__ = ["CommandResult", "run_inspect", "run_plan", "run_validate"]


@dataclass(frozen=True, slots=True)
class CommandResult:
    """What a command produced: what to print, and what to exit with.

    Attributes:
        code: The exit status.
        lines: Everything to write to stdout, one entry per block.
        payload: The JSON envelope, when the command was asked for JSON.
    """

    code: ExitCode
    lines: list[str] = field(default_factory=list)
    payload: dict[str, Any] | None = None

    def render(self, *, as_json: bool) -> str:
        """The text to print, in the form that was asked for."""
        if as_json and self.payload is not None:
            return to_json(self.payload)
        return "\n\n".join(self.lines)


async def run_inspect(
    kit: PartitionToolkit, configs: Sequence[TablePartitionConfig], *, as_json: bool
) -> CommandResult:
    """Read the real tree of every selected table and show it.

    A table the document describes but PostgreSQL does not partition is
    reported and makes the command exit ``CONFIG``: the document is describing
    something that is not there.
    """
    blocks: list[str] = []
    entries: list[dict[str, Any]] = []
    code = ExitCode.OK
    for config in configs:
        tree = await kit.service.inspect(config)
        entries.append(tree_entry(config.qualified_name, tree))
        if tree is None:
            code = ExitCode.CONFIG
            blocks.append(f"{config.qualified_name} — not a partitioned table")
            continue
        blocks.append(describe_tree(tree))
    return CommandResult(code=code, lines=blocks, payload=envelope("inspect", entries) if as_json else None)


async def run_plan(
    kit: PartitionToolkit, configs: Sequence[TablePartitionConfig], *, as_json: bool, check: bool
) -> CommandResult:
    """Plan every selected table, and issue no DDL doing it.

    With ``check``, pending operations are drift and are exited on, which is
    what lets an alert say "maintenance has not been running". A finding
    outranks that: drift is what a run fixes, a finding is what it cannot.
    """
    blocks: list[str] = []
    entries: list[dict[str, Any]] = []
    drift = False
    findings = False
    for config in configs:
        plan = await kit.service.plan(config)
        entries.append(plan_entry(plan))
        blocks.append(plan.describe())
        drift = drift or not plan.is_noop
        findings = findings or bool(plan.actionable_findings)

    code = ExitCode.OK
    if findings:
        code = ExitCode.FINDINGS
    elif check and drift:
        code = ExitCode.DRIFT
    return CommandResult(code=code, lines=blocks, payload=envelope("plan", entries) if as_json else None)


async def run_validate(
    kit: PartitionToolkit, configs: Sequence[TablePartitionConfig], *, as_json: bool
) -> CommandResult:
    """Check every selected table's configuration against the catalog.

    This is the one that belongs in CI: the document has already parsed by the
    time this runs, so what is left to answer is whether the tables it
    describes are the tables that exist -- partitioned at all, by the method
    and on the key the scheme claims.
    """
    blocks: list[str] = []
    entries: list[dict[str, Any]] = []
    validation = PartitionValidationService(kit.metadata)
    code = ExitCode.OK
    for config in configs:
        try:
            await validation.validate_config(config)
        except InvalidPartitionConfigError as exc:
            code = ExitCode.CONFIG
            entries.append({"table": config.qualified_name, "ok": False, "error": str(exc)})
            blocks.append(f"{config.qualified_name} — {exc}")
            continue
        entries.append({"table": config.qualified_name, "ok": True, "error": None})
        blocks.append(f"{config.qualified_name} — ok")
    return CommandResult(code=code, lines=blocks, payload=envelope("validate", entries) if as_json else None)
