"""Partition pruning identification domain service."""

from __future__ import annotations

from datetime import UTC, tzinfo
from typing import TYPE_CHECKING

from pg_partsmith import pruning_rules
from pg_partsmith.entities import PartitionInfo, Period
from pg_partsmith.protocols import TimezoneAwareCalculator
from pg_partsmith.utils import qualify

if TYPE_CHECKING:
    from pg_partsmith.aio.protocols import PartitionMetadataProvider
    from pg_partsmith.entities import TablePartitionConfig
    from pg_partsmith.protocols import PeriodCalculator


class PartitionPruningService:
    """Service for identifying partitions that should be pruned."""

    def __init__(self, metadata: PartitionMetadataProvider, calculator: PeriodCalculator[Period]) -> None:
        self._metadata = metadata
        self._calculator = calculator
        # Naive catalog boundaries mean period starts in the calculator's
        # timezone; calculators without one (custom implementations) keep the
        # historical UTC interpretation.
        calc_tz = calculator.tz if isinstance(calculator, TimezoneAwareCalculator) else None
        self._boundary_tz: tzinfo = calc_tz if isinstance(calc_tz, tzinfo) else UTC

    async def get_partitions_for_pruning(self, config: TablePartitionConfig) -> list[PartitionInfo]:
        """Fetch the table's partitions, then select those outside the retention window.

        Args:
            config: Table partitioning configuration.

        Returns:
            Partitions eligible for detach + drop, sorted oldest first.
        """
        qualified_parent = qualify(config.db_schema, config.table_name)
        all_partitions = await self._metadata.list_partitions(qualified_parent)
        return await self.identify_partitions_to_prune(config, all_partitions)

    async def identify_partitions_to_prune(
        self, config: TablePartitionConfig, all_partitions: list[PartitionInfo]
    ) -> list[PartitionInfo]:
        """Select partitions outside the retention window from an already-fetched list.

        Args:
            config: Table partitioning configuration.
            all_partitions: Partitions of the parent table, as previously listed.

        Returns:
            Partitions eligible for detach + drop, sorted oldest first.
        """
        return pruning_rules.select_partitions_to_prune(self._calculator, self._boundary_tz, config, all_partitions)
