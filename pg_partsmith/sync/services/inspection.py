"""Reads the actual tree and everything the planner needs to know beside it."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pg_partsmith.boundaries import Axis, CursorSource, Window
from pg_partsmith.planner import PlanMode, PlanningContext, fact_targets, unattached_window
from pg_partsmith.scheme import RangePartitioning

if TYPE_CHECKING:
    from pg_partsmith.entities import TablePartitionConfig
    from pg_partsmith.sync.protocols import PartitionMetadataProvider
    from pg_partsmith.topology import ActualTree, UnattachedTable


class PartitionInspector:
    """Builds the planner's inputs from the catalog.

    Two things beyond the tree itself: the *facts* the lifecycle policy asked
    for, gathered only for the relations a policy can decide over, and the
    *cursors* -- the database's clock for a time axis, the high-water mark
    for an integer axis that reads one.
    """

    def __init__(self, metadata: PartitionMetadataProvider) -> None:
        self._metadata = metadata

    def inspect(self, config: TablePartitionConfig, *, measure: bool = True) -> ActualTree | None:
        """Read the tree, measuring what the policy needs when ``measure`` is set.

        Args:
            config: The table's configuration.
            measure: Whether to gather the facts the lifecycle policy reads.
                A plan that expires nothing does not need them.

        Returns:
            The tree with its orphans, or None when the table is not partitioned.
        """
        tree = self._metadata.get_actual_tree(config.qualified_name)
        if tree is None or not measure or not config.has_progression_level:
            return tree
        unattached = unattached_tables(self._metadata, config)
        if unattached:
            tree = tree.model_copy(update={"unattached": unattached})
        earliest = default_earliest(self._metadata, config, tree)
        if earliest is not None:
            tree = tree.model_copy(update={"default_earliest": earliest})

        policy = config.lifecycle
        if not policy.needs_facts:
            return tree

        targets = fact_targets(config, tree)
        if not targets:
            return tree
        return self._metadata.measure(
            tree,
            targets=targets,
            facts=policy.required_facts,
            sql_predicates=policy.sql_predicates,
        )

    def context(
        self,
        config: TablePartitionConfig,
        *,
        now: datetime | None = None,
        mode: PlanMode = PlanMode.MAINTAIN,
        explicit_windows: dict[str, tuple[Window, ...]] | None = None,
    ) -> PlanningContext:
        """Resolve the clock and the cursors the plan is made against.

        Without ``now`` the clock is the database's, not this process's: a
        maintenance container whose clock runs ahead would otherwise expire
        partitions the data has not finished with.
        """
        instant = self._metadata.current_time() if now is None else now
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)

        cursors: dict[str, int] = {}
        for level in config.levels:
            boundaries = level.progression
            if boundaries is None or boundaries.axis is not Axis.INTEGER:
                continue
            if boundaries.cursor_source is CursorSource.NEWEST_MEMBER:
                continue  # the planner reads it off the tree
            value = self._metadata.get_key_high_water_mark(
                config.qualified_name,
                level.leading_column,
                sequence=boundaries.cursor_source is CursorSource.SEQUENCE,
            )
            if value is not None:
                cursors[level.leading_column] = value

        return PlanningContext(
            now=instant,
            cursors=cursors,
            mode=mode,
            explicit_windows=dict(explicit_windows or {}),
        )


def default_earliest(metadata: PartitionMetadataProvider, config: TablePartitionConfig, tree: ActualTree) -> Any:
    """The smallest leading-key value in the root's DEFAULT partition among rows whose whole key is set.

    A row with a NULL in the key is routed to DEFAULT on purpose and stays
    there; any other row belongs to a window that has no partition. One
    index probe when the key is indexed.
    """
    root = config.scheme
    if not isinstance(root, RangePartitioning):
        return None
    default = next((child for child in tree.root.children if child.is_default), None)
    if default is None:
        return None
    return metadata.get_leading_key_minimum(default.name, root.key)


def unattached_tables(metadata: PartitionMetadataProvider, config: TablePartitionConfig) -> tuple[UnattachedTable, ...]:
    """The tables named under the root and attached to nothing, ``holds_rows`` answered where it matters.

    Only a name the root's scheme reads back as one of its windows is looked
    into, so a table that merely shares the prefix -- the root's pre-migration
    copy, say -- is never read and needs no grant. The question asked is
    whether the table has a first row at all.
    """
    root = config.scheme
    if not isinstance(root, RangePartitioning):
        return ()
    measured: list[UnattachedTable] = []
    for table in metadata.get_unattached_tables(config.qualified_name):
        if unattached_window(config, table.relname) is None:
            measured.append(table)
            continue
        first = metadata.get_leading_key_minimum(table.name, root.key)
        measured.append(table.model_copy(update={"holds_rows": first is not None}))
    return tuple(measured)
