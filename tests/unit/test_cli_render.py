"""What ``inspect`` prints: a tree the way a person reads one, and the JSON beside it."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from pg_partsmith.cli import ExitCode
from pg_partsmith.cli.commands import CommandResult, run_inspect
from pg_partsmith.cli.render import describe_tree, tree_entry
from pg_partsmith.document import PartitionsDocument
from pg_partsmith.topology import ActualTree, DetachedPartition, PartitionNode, PartitionType, RangeBounds, RelationKind

ROOT = "public.events"


def _tree(*, orphans: bool = True) -> ActualTree:
    august = PartitionNode(
        name=f"{ROOT}__2026_08",
        oid=11,
        parent_name=ROOT,
        level=1,
        bounds=RangeBounds(from_value="2026-08-01", to_value="2026-09-01"),
        bounds_expr="FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')",
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=(
            PartitionNode(
                name=f"{ROOT}__2026_08__h0",
                oid=12,
                parent_name=f"{ROOT}__2026_08",
                level=2,
                bounds_expr="FOR VALUES WITH (modulus 2, remainder 0)",
                relkind=RelationKind.FOREIGN,
            ),
        ),
    )
    september = PartitionNode(
        name=f"{ROOT}__2026_09",
        oid=13,
        parent_name=ROOT,
        level=1,
        bounds_expr="FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')",
        detach_pending=True,
        has_unaddressable_children=True,
    )
    root = PartitionNode(
        name=ROOT,
        oid=10,
        partition_type=PartitionType.RANGE,
        partition_columns=("created_at",),
        children=(august, september),
    )
    detached = (
        (
            DetachedPartition(
                name=f"{ROOT}__2026_01", oid=5, parent_name=ROOT, detached_at=datetime(2026, 8, 1, tzinfo=UTC)
            ),
            DetachedPartition(name=f"{ROOT}__2026_02", oid=6, parent_name=ROOT),
        )
        if orphans
        else ()
    )
    return ActualTree(root=root, orphans=detached)


def test__describe_tree__indents_by_level_and_shows_the_bound_as_postgresql_spells_it() -> None:
    # Act
    lines = describe_tree(_tree()).splitlines()

    # Assert: the root names its method; each child its bound, one level deeper
    assert lines[0] == f"{ROOT} — partitioned by RANGE (created_at)"
    assert lines[1] == "  events__2026_08 — FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')"
    assert lines[2] == "    events__2026_08__h0 — FOR VALUES WITH (modulus 2, remainder 0), foreign"
    assert lines[3] == (
        "  events__2026_09 — FOR VALUES FROM ('2026-09-01') TO ('2026-10-01'), "
        "detach pending, children omitted: unaddressable names"
    )


def test__describe_tree__lists_the_orphans_with_when_they_were_detached() -> None:
    # Act
    printed = describe_tree(_tree())

    # Assert: one has an instant, the other was marked by an older version
    assert "detached, ours to clean up:" in printed
    assert f"{ROOT}__2026_01 detached 2026-08-01T00:00:00+00:00" in printed
    assert printed.splitlines()[-1] == f"    {ROOT}__2026_02"


def test__describe_tree__no_orphans__says_nothing_about_cleanup() -> None:
    # Act / Assert
    assert "detached" not in describe_tree(_tree(orphans=False))


def test__tree_entry__a_table_that_is_not_partitioned__is_null_rather_than_absent() -> None:
    # Arrange / Act
    entry = tree_entry(ROOT, None)

    # Assert
    assert entry == {"table": ROOT, "tree": None}


def test__tree_entry__dumps_the_model_in_the_documented_vocabulary() -> None:
    # Arrange / Act
    entry = tree_entry(ROOT, _tree())

    # Assert
    assert entry["tree"]["root"]["name"] == ROOT
    assert entry["tree"]["orphans"][0]["name"] == f"{ROOT}__2026_01"


async def test__run_inspect__prints_every_tree_and_exits_ok() -> None:
    # Arrange
    kit = MagicMock()
    kit.service.inspect = AsyncMock(return_value=_tree())
    configs = PartitionsDocument.model_validate(
        {
            "tables": [
                {"table_name": "events", "schema": "public", "partition_column": "created_at", "granularity": "month"}
            ]
        }
    ).configs()

    # Act
    result = await run_inspect(kit, configs)

    # Assert
    assert result.code is ExitCode.OK
    assert ROOT in result.render(output="human")
    assert result.payload is not None and result.payload["command"] == "inspect"


async def test__run_inspect__a_table_that_is_not_partitioned__is_a_configuration_error() -> None:
    # Arrange: the document describes something that is not there
    kit = MagicMock()
    kit.service.inspect = AsyncMock(return_value=None)
    configs = PartitionsDocument.model_validate(
        {"tables": [{"table_name": "events", "partition_column": "created_at", "granularity": "month"}]}
    ).configs()

    # Act
    result = await run_inspect(kit, configs)

    # Assert
    assert result.code is ExitCode.CONFIG
    assert "not a partitioned table" in result.render(output="human")


def test__command_result__renders_metrics_and_falls_back_to_lines_without_a_payload() -> None:
    # Arrange
    payload: dict[str, Any] = {"version": 1, "command": "validate", "generated_at": "x", "tables": []}
    with_payload = CommandResult(code=ExitCode.OK, lines=["a", "b"], payload=payload)
    without = CommandResult(code=ExitCode.OK, lines=["a", "b"])

    # Act / Assert
    assert with_payload.render(output="metrics").startswith("# HELP pg_partsmith_run_timestamp_seconds")
    assert without.render(output="json") == "a" + chr(10) + chr(10) + "b"
