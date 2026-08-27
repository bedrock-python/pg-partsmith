"""Partition deletion domain service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pg_partsmith.exceptions import PartitionAttachedError

from .base import BasePartitionService

if TYPE_CHECKING:
    from pg_partsmith.sync.hooks import PartitionLifecycleHooks
    from pg_partsmith.sync.protocols import PartitionRepository

logger = logging.getLogger(__name__)


class PartitionDeletionService(BasePartitionService):
    """Service for dropping detached partitions."""

    def __init__(
        self,
        repo: PartitionRepository,
        hooks: list[PartitionLifecycleHooks] | None = None,
    ) -> None:
        super().__init__(hooks=hooks)
        self._repo = repo

    def drop_detached_partitions(
        self,
        table_name: str,
        partition_names: list[str],
    ) -> int:
        """Drop detached partitions.

        Returns:
            Number of dropped partitions.

        Raises:
            Exception: Any error during dropping or hooks.
        """
        dropped_count = 0
        for partition_name in partition_names:
            if self.drop_single_partition(table_name, partition_name):
                dropped_count += 1
        return dropped_count

    def drop_single_partition(
        self,
        table_name: str,
        partition_name: str,
    ) -> bool:
        """Drop a single partition, running the drop hooks around it.

        Extension point for callers that manage partitions one at a time.

        Args:
            table_name: Qualified parent table name (hook context).
            partition_name: Name of the detached partition to drop.

        Returns:
            True when the partition was dropped; False when it is still
            attached (logged and skipped).
        """
        # Hooks: before drop
        self._run_hooks(
            lambda h: h.before_drop(table_name, partition_name),
            "before_drop",
            partition_name=partition_name,
        )

        # Deletion
        try:
            self._repo.drop_partition(partition_name)
        except PartitionAttachedError:
            logger.warning("Refusing to drop attached partition", extra={"partition_name": partition_name})
            return False

        # Hooks: after drop
        self._run_hooks(
            lambda h: h.after_drop(table_name, partition_name),
            "after_drop",
            partition_name=partition_name,
        )

        return True
