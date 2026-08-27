"""Partition detachment domain service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import BasePartitionService

if TYPE_CHECKING:
    from pg_partsmith.entities import PartitionInfo
    from pg_partsmith.sync.hooks import PartitionLifecycleHooks
    from pg_partsmith.sync.protocols import PartitionRepository


class PartitionDetachmentService(BasePartitionService):
    """Service for detaching old partitions."""

    def __init__(
        self,
        repo: PartitionRepository,
        hooks: list[PartitionLifecycleHooks] | None = None,
    ) -> None:
        super().__init__(hooks=hooks)
        self._repo = repo

    def detach_old_partitions(
        self,
        table_name: str,
        partitions: list[PartitionInfo],
    ) -> list[str]:
        """Detach old partitions from parent table.

        Returns:
            List of successfully detached partition names; inputs that were
            already detached are included.

        Raises:
            Exception: Any error during detachment or hooks.
        """
        successfully_detached: list[str] = []
        for partition in partitions:
            if not partition.is_attached:
                successfully_detached.append(partition.name)
                continue

            self.detach_single_partition(table_name, partition)
            successfully_detached.append(partition.name)

        return successfully_detached

    def detach_single_partition(
        self,
        table_name: str,
        partition: PartitionInfo,
    ) -> None:
        """Detach a single partition, running the detach hooks around it.

        Extension point for callers that manage partitions one at a time.

        Args:
            table_name: Qualified parent table name (hook context).
            partition: The attached partition to detach.

        Raises:
            PartitionDetachInProgressError: If a concurrent detach is in progress.
            Exception: Any error from the repository or a ``before_detach`` hook.
        """
        # Hooks: before detach
        self._run_hooks(
            lambda h: h.before_detach(table_name, partition),
            "before_detach",
            partition_name=partition.name,
        )

        # Detachment
        self._repo.detach_partition(table_name, partition.name, concurrent=True)

        # Hooks: after detach
        self._run_hooks(
            lambda h: h.after_detach(table_name, partition.name),
            "after_detach",
            partition_name=partition.name,
        )
