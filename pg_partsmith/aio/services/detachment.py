"""Partition detachment domain service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .base import BasePartitionService

if TYPE_CHECKING:
    from pg_partsmith.aio.hooks import PartitionLifecycleHooks
    from pg_partsmith.aio.protocols import PartitionRepository
    from pg_partsmith.entities import PartitionInfo

logger = logging.getLogger(__name__)


class PartitionDetachmentService(BasePartitionService):
    """Service for detaching old partitions."""

    def __init__(
        self,
        repo: PartitionRepository,
        hooks: list[PartitionLifecycleHooks] | None = None,
    ) -> None:
        super().__init__(hooks=hooks)
        self._repo = repo

    async def detach_old_partitions(
        self,
        table_name: str,
        partitions: list[PartitionInfo],
    ) -> list[str]:
        """Detach old partitions from parent table.

        Returns:
            List of successfully detached partition names.

        Raises:
            Exception: Any error during detachment or hooks.
        """
        successfully_detached: list[str] = []
        for partition in partitions:
            if not partition.is_attached:
                successfully_detached.append(partition.name)
                continue

            await self.detach_single_partition(table_name, partition)
            successfully_detached.append(partition.name)

        return successfully_detached

    async def detach_single_partition(
        self,
        table_name: str,
        partition: PartitionInfo,
    ) -> None:
        """Detach a single partition with hooks."""
        # Hooks: before detach
        await self._run_hooks(
            lambda h: h.before_detach(table_name, partition),
            "before_detach",
            partition_name=partition.name,
        )

        # Detachment
        await self._repo.detach_partition(table_name, partition.name, concurrent=True)

        # Hooks: after detach
        await self._run_hooks(
            lambda h: h.after_detach(table_name, partition.name),
            "after_detach",
            partition_name=partition.name,
        )
