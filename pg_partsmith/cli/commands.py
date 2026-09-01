"""What each command does, once the document is read and the engine is up.

``inspect``, ``plan`` and ``validate`` are read-only: no DDL, no lock, no hook.
``apply`` is the one that acts, and it withholds every destructive operation
unless it was asked for them. What they all share is that a CronJob and a CI
step read the exit code and nothing else.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pg_partsmith.aio import PartitionToolkit, PartitionValidationService
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.plan import MaintenancePlan, OperationKind

from .exit_codes import ExitCode
from .loader import ConfigError
from .render import describe_tree, envelope, plan_entry, to_json, tree_entry

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pg_partsmith.entities import MaintenanceResult, TablePartitionConfig

logger = logging.getLogger("pg_partsmith.cli")

__all__ = ["CommandResult", "run_apply", "run_inspect", "run_plan", "run_validate"]


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


async def run_inspect(kit: PartitionToolkit, configs: Sequence[TablePartitionConfig]) -> CommandResult:
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
    return CommandResult(code=code, lines=blocks, payload=envelope("inspect", entries))


async def run_plan(kit: PartitionToolkit, configs: Sequence[TablePartitionConfig], *, check: bool) -> CommandResult:
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
    return CommandResult(code=code, lines=blocks, payload=envelope("plan", entries))


async def run_validate(kit: PartitionToolkit, configs: Sequence[TablePartitionConfig]) -> CommandResult:
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
    return CommandResult(code=code, lines=blocks, payload=envelope("validate", entries))


async def run_apply(
    kit: PartitionToolkit,
    configs: Sequence[TablePartitionConfig],
    *,
    plans: dict[str, MaintenancePlan] | None = None,
    allow_destructive: bool = False,
    continue_on_error: bool = False,
    allow_config_drift: bool = False,
) -> CommandResult:
    """Execute maintenance: either a plan read from a file, or one made here.

    Destructive operations are withheld unless they are asked for. That makes
    the safe mode the default one -- an init container that creates what the
    application is about to write into, and retires nothing -- rather than a
    second mode somebody has to remember to select.

    A plan read from a file is applied through the library's own guard: it is
    refused unless it was made for this table, under this configuration.

    Args:
        kit: The wiring.
        configs: The tables to maintain, in order.
        plans: Plans read from a file, keyed by table; None to plan here.
        allow_destructive: Carry out detaches and drops as well as creations.
        continue_on_error: Isolate a failed operation instead of aborting.
        allow_config_drift: Apply a saved plan whose configuration has changed.

    Returns:
        What was done, and the code to exit with.

    Raises:
        ConfigError: If a table was selected and the plan file has nothing for it.
    """
    blocks: list[str] = []
    entries: list[dict[str, Any]] = []
    issues = False
    for config in configs:
        if plans is not None:
            plan = _plan_for(config, plans)
            if not allow_destructive:
                plan = plan.without(OperationKind.DETACH, OperationKind.DROP)
            result = await kit.service.apply(
                config, plan, continue_on_error=continue_on_error, allow_config_drift=allow_config_drift
            )
            applied: MaintenancePlan | None = plan
        else:
            # Planning and applying under one lock is what maintain() is for,
            # and it is also what completes an interrupted DETACH CONCURRENTLY
            # before deciding the rest of the run.
            result = await kit.service.maintain(
                config,
                skip_detach=not allow_destructive,
                skip_drop=not allow_destructive,
                continue_on_error=continue_on_error,
            )
            applied = result.maintenance_plan

        issues = issues or bool(result.issues)
        entries.append(
            {
                "table": config.qualified_name,
                "result": result.model_dump(mode="json", by_alias=True),
                # The result excludes its plan from serialization, so it is
                # dumped beside it: an audit log wants what was done and why.
                "plan": None if applied is None else applied.model_dump(mode="json", by_alias=True),
            }
        )
        blocks.append(_describe_result(config.qualified_name, result))

    if not allow_destructive:
        blocks.append("Creations only: detaches and drops need --allow-destructive.")
    return CommandResult(
        code=ExitCode.FINDINGS if issues else ExitCode.OK,
        lines=blocks,
        payload=envelope("apply", entries),
    )


def _plan_for(config: TablePartitionConfig, plans: dict[str, MaintenancePlan]) -> MaintenancePlan:
    """The saved plan for one table, or a refusal naming what the file does hold."""
    plan = plans.get(config.qualified_name)
    if plan is None:
        known = ", ".join(sorted(plans)) or "nothing"
        msg = f"The plan file has nothing for {config.qualified_name!r}; it holds {known}"
        raise ConfigError(msg)
    return plan


def _describe_result(table_name: str, result: MaintenanceResult) -> str:
    """One block per table: what it did, then anything it wants a human to see."""
    counts = (
        f"created {result.created_count}, repaired {result.repaired_count}, attached {result.attached_count}, "
        f"detached {result.detached_count}, dropped {result.dropped_count}"
    )
    lines = [f"{table_name} — {counts}"]
    lines.extend(f"  [{issue.step.value}] {issue.partition_name}: {issue.error}" for issue in result.issues)
    return "\n".join(lines)
