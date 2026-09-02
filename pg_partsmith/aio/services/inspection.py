"""Reads the actual tree and everything the planner needs to know beside it."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pg_partsmith.boundaries import Axis, CursorSource, Window
from pg_partsmith.planner import PlanMode, PlanningContext, fact_targets

if TYPE_CHECKING:
    from pg_partsmith.aio.protocols import PartitionMetadataProvider
    from pg_partsmith.entities import TablePartitionConfig
    from pg_partsmith.topology import ActualTree


class PartitionInspector:
    """Builds the planner's inputs from the catalog.

    Two things beyond the tree itself: the *facts* the lifecycle policy asked
    for, gathered only for the relations a policy can decide over, and the
    *cursor* of every integer axis -- the clock needs no query.
    """

    def __init__(self, metadata: PartitionMetadataProvider) -> None:
        self._metadata = metadata

    async def inspect(self, config: TablePartitionConfig, *, measure: bool = True) -> ActualTree | None:
        """Read the tree, measuring what the policy needs when ``measure`` is set.

        Args:
            config: The table's configuration.
            measure: Whether to gather the facts the lifecycle policy reads.
                A plan that expires nothing does not need them.

        Returns:
            The tree with its orphans, or None when the table is not partitioned.
        """
        tree = await self._metadata.get_actual_tree(config.qualified_name)
        if tree is None or not measure or not config.has_progression_level:
            return tree

        policy = config.lifecycle
        if not policy.needs_facts:
            return tree

        targets = fact_targets(config, tree)
        if not targets:
            return tree
        return await self._metadata.measure(
            tree,
            targets=targets,
            facts=policy.required_facts,
            sql_predicates=policy.sql_predicates,
        )

    async def context(
        self,
        config: TablePartitionConfig,
        *,
        now: datetime | None = None,
        mode: PlanMode = PlanMode.MAINTAIN,
        explicit_windows: dict[str, tuple[Window, ...]] | None = None,
    ) -> PlanningContext:
        """Resolve the clock and the cursors the plan is made against."""
        instant = datetime.now(UTC) if now is None else now
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)

        cursors: dict[str, int] = {}
        for level in config.levels:
            boundaries = level.progression
            if boundaries is None or boundaries.axis is not Axis.INTEGER:
                continue
            if boundaries.cursor_source is CursorSource.NEWEST_MEMBER:
                continue  # the planner reads it off the tree
            value = await self._metadata.get_key_high_water_mark(
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
