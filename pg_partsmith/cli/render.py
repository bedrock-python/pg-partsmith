"""Two ways to say the same thing: one for a person, one for a pipe.

The JSON is the models' own dump, never a shape assembled here -- a hand-rolled
one drifts from the library the first time a field is added. It is emitted with
``by_alias=True``, which is the vocabulary a configuration file is written in
(``kind``, ``method``, ``schema``), so what comes out of ``plan`` and what goes
into a document are one language.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pg_partsmith.plan import MaintenancePlan
    from pg_partsmith.topology import ActualTree, PartitionNode

__all__ = ["OUTPUT_VERSION", "describe_tree", "envelope", "plan_entry", "to_json", "tree_entry"]

OUTPUT_VERSION = 1
"""Schema version of the JSON envelope, so a reader can tell what it is holding."""


def envelope(command: str, tables: list[dict[str, Any]]) -> dict[str, Any]:
    """The outer shape every JSON output shares.

    Args:
        command: The command that produced it.
        tables: One entry per table, in the order they were handled.

    Returns:
        The envelope, ready to serialize.
    """
    return {
        "version": OUTPUT_VERSION,
        "command": command,
        "generated_at": datetime.now(UTC).isoformat(),
        "tables": tables,
    }


def to_json(payload: dict[str, Any]) -> str:
    """Render an envelope, pretty enough to read and stable enough to diff."""
    return json.dumps(payload, indent=2, sort_keys=False, default=str)


def plan_entry(plan: MaintenancePlan) -> dict[str, Any]:
    """One table's plan, as the model dumps it."""
    return {"table": plan.table_name, "plan": plan.model_dump(mode="json", by_alias=True)}


def tree_entry(table_name: str, tree: ActualTree | None) -> dict[str, Any]:
    """One table's tree, as the model dumps it. ``None`` when it is not partitioned."""
    return {
        "table": table_name,
        "tree": None if tree is None else tree.model_dump(mode="json", by_alias=True),
    }


def describe_tree(tree: ActualTree) -> str:
    """Render a tree the way a person reads one: indented, bounds on each line."""
    lines = [f"{tree.root.name} — {tree.root.describe_topology()}"]
    lines.extend(_describe_children(tree.root, depth=1))
    if tree.orphans:
        lines.append("  detached, ours to clean up:")
        for orphan in tree.orphans:
            when = "" if orphan.detached_at is None else f" detached {orphan.detached_at.isoformat()}"
            lines.append(f"    {orphan.name}{when}")
    return "\n".join(lines)


def _describe_children(node: PartitionNode, depth: int) -> list[str]:
    lines: list[str] = []
    for child in node.children:
        lines.append(f"{'  ' * depth}{child.relname}{_annotations(child)}")
        lines.extend(_describe_children(child, depth + 1))
    return lines


def _annotations(node: PartitionNode) -> str:
    """What is worth saying about one node, after its name.

    The bound is shown as ``pg_get_expr`` rendered it rather than re-spelled
    from the parsed form: an operator comparing the output against
    ``\\d+ events`` should see the same string PostgreSQL shows them.
    """
    parts = []
    if node.bounds_expr:
        parts.append(node.bounds_expr)
    if node.is_foreign:
        parts.append("foreign")
    if node.detach_pending:
        parts.append("detach pending")
    if node.has_unaddressable_children:
        parts.append("children omitted: unaddressable names")
    return f" — {', '.join(parts)}" if parts else ""
