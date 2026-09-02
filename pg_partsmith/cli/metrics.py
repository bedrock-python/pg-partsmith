"""The same run, as numbers a Prometheus node_exporter can pick up.

Most systems that partition PostgreSQL have no monitoring of it at all -- they
find out at 03:00, from an insert that PostgreSQL rejected. A CronJob already
runs this tool on a schedule, so the cheapest possible monitoring is for that
run to leave a textfile behind:

    pg-partsmith plan -c partitions.yaml --output metrics > /var/lib/node_exporter/partsmith.prom

Every command can do it. The numbers are read off the JSON envelope rather than
collected separately, so a metric cannot disagree with what the same run
printed.

The format is the text exposition one: ``HELP``, ``TYPE``, then samples. Every
value is a gauge, because a one-shot job cannot own a counter -- the next run is
a new process with no memory of this one.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

__all__ = ["render_metrics"]

_PREFIX = "pg_partsmith"


def render_metrics(payload: dict[str, Any]) -> str:
    """Render one command's envelope as Prometheus text exposition.

    Args:
        payload: The envelope the command built.

    Returns:
        The metrics, ending in a newline, ready to write to a textfile
        collector's directory.
    """
    command = str(payload.get("command", "unknown"))
    lines: list[str] = []
    _emit(
        lines,
        f"{_PREFIX}_run_timestamp_seconds",
        "When this run happened, so a stale textfile is visible as one.",
        [({"command": command}, _timestamp(payload.get("generated_at")))],
    )

    tables = [entry for entry in payload.get("tables", []) if isinstance(entry, dict)]
    renderer = _RENDERERS.get(command)
    if renderer is not None:
        renderer(lines, tables)
    return "\n".join(lines) + "\n"


# ── Per command ─────────────────────────────────────────────────────────────────


def _plan_metrics(lines: list[str], tables: list[dict[str, Any]]) -> None:
    """What is waiting to be applied, and what nothing will apply."""
    pending: list[tuple[dict[str, str], float]] = []
    relations: list[tuple[dict[str, str], float]] = []
    findings: list[tuple[dict[str, str], float]] = []
    for entry in tables:
        table = str(entry.get("table", ""))
        plan = entry.get("plan") or {}
        operations = plan.get("operations", [])
        counted = Counter(str(op.get("kind", "unknown")) for op in operations)
        for kind in ("create", "attach", "detach", "drop"):
            pending.append(({"table": table, "kind": kind}, counted.get(kind, 0)))
        relations.append(({"table": table}, _relation_count(operations)))
        by_severity = Counter(str(f.get("severity", "unknown")) for f in plan.get("findings", []))
        for severity in ("info", "warning"):
            findings.append(({"table": table, "severity": severity}, by_severity.get(severity, 0)))

    _emit(lines, f"{_PREFIX}_pending_operations", "Operations a maintenance run would carry out.", pending)
    _emit(
        lines,
        f"{_PREFIX}_pending_relations",
        "Relations those operations would create, subtrees included.",
        relations,
    )
    _emit(
        lines,
        f"{_PREFIX}_findings",
        "What the planner saw and left alone. A warning needs a person.",
        findings,
    )


def _inspect_metrics(lines: list[str], tables: list[dict[str, Any]]) -> None:
    """What exists, and what is waiting to be cleaned up."""
    partitions: list[tuple[dict[str, str], float]] = []
    orphans: list[tuple[dict[str, str], float]] = []
    oldest: list[tuple[dict[str, str], float]] = []
    partitioned: list[tuple[dict[str, str], float]] = []
    now = datetime.now(UTC)
    for entry in tables:
        table = str(entry.get("table", ""))
        tree = entry.get("tree")
        partitioned.append(({"table": table}, 0 if tree is None else 1))
        if tree is None:
            continue
        detached = [o for o in tree.get("orphans", []) if isinstance(o, dict)]
        partitions.append(({"table": table}, _count_nodes(tree.get("root", {}))))
        orphans.append(({"table": table}, len(detached)))
        age = _oldest_age_seconds(detached, now)
        if age is not None:
            oldest.append(({"table": table}, age))

    _emit(lines, f"{_PREFIX}_partitioned", "1 when the table is partitioned at all, 0 when it is not.", partitioned)
    _emit(lines, f"{_PREFIX}_partitions", "Relations attached below the root, at every level.", partitions)
    _emit(lines, f"{_PREFIX}_detached_partitions", "Detached partitions this library has not dropped yet.", orphans)
    _emit(
        lines,
        f"{_PREFIX}_oldest_detached_age_seconds",
        "Age of the oldest detached partition. A number that only grows means drops are not happening.",
        oldest,
    )


def _validate_metrics(lines: list[str], tables: list[dict[str, Any]]) -> None:
    """Whether the document still describes the database."""
    valid = [({"table": str(entry.get("table", ""))}, 1 if entry.get("ok") else 0) for entry in tables]
    _emit(lines, f"{_PREFIX}_config_valid", "1 when the table matches the configuration describing it.", valid)


def _apply_metrics(lines: list[str], tables: list[dict[str, Any]]) -> None:
    """What the run actually did, and what it could not."""
    applied: list[tuple[dict[str, str], float]] = []
    issues: list[tuple[dict[str, str], float]] = []
    for entry in tables:
        table = str(entry.get("table", ""))
        result = entry.get("result") or {}
        for name in ("created", "repaired", "attached", "detached", "dropped"):
            applied.append(({"table": table, "operation": name}, result.get(f"{name}_count", 0)))
        issues.append(({"table": table}, len(result.get("issues", []))))

    _emit(lines, f"{_PREFIX}_applied_operations", "What this run carried out.", applied)
    _emit(lines, f"{_PREFIX}_issues", "Problems this run reported and did not fail on.", issues)


_RENDERERS = {
    "plan": _plan_metrics,
    "inspect": _inspect_metrics,
    "validate": _validate_metrics,
    "apply": _apply_metrics,
}


# ── Module helpers ──────────────────────────────────────────────────────────────


def _emit(lines: list[str], name: str, help_text: str, samples: Sequence[tuple[dict[str, str], float]]) -> None:
    """One metric family: its documentation, its type, and every sample of it."""
    if not samples:
        return
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} gauge")
    for labels, value in samples:
        lines.append(f"{name}{_labels(labels)} {_number(value)}")


def _labels(labels: dict[str, str]) -> str:
    """Render a label set, escaping what the exposition format reserves."""
    if not labels:
        return ""
    rendered = ",".join(f'{key}="{_escape(value)}"' for key, value in labels.items())
    return "{" + rendered + "}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _number(value: float) -> str:
    """Integers without a decimal point, everything else as a plain float."""
    return str(int(value)) if float(value).is_integer() else repr(float(value))


def _timestamp(generated_at: object) -> float:
    """The envelope's instant as a UNIX timestamp, falling back to now."""
    if isinstance(generated_at, str):
        try:
            return datetime.fromisoformat(generated_at).timestamp()
        except ValueError:
            pass
    return datetime.now(UTC).timestamp()


def _relation_count(operations: list[Any]) -> int:
    """Every relation the creations would make, nested children included."""
    total = 0
    for operation in operations:
        if isinstance(operation, dict) and operation.get("kind") == "create":
            total += 1 + _relation_count(operation.get("children", []))
    return total


def _count_nodes(node: dict[str, Any]) -> int:
    """Relations below the root, the root itself not counted."""
    children = node.get("children", [])
    return sum(1 + _count_nodes(child) for child in children if isinstance(child, dict))


def _oldest_age_seconds(orphans: list[dict[str, Any]], now: datetime) -> float | None:
    """How long the longest-waiting orphan has been detached, when any says."""
    ages = []
    for orphan in orphans:
        detached_at = orphan.get("detached_at")
        if not isinstance(detached_at, str):
            continue
        try:
            moment = datetime.fromisoformat(detached_at)
        except ValueError:
            continue
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        ages.append((now - moment).total_seconds())
    return max(ages) if ages else None
