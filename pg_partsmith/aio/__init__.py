"""Async implementations for partition management."""

from .command_hooks import CommandHookError, CommandHooks
from .hooks import BasePartitionLifecycleHooks, PartitionLifecycleHooks
from .lock import PostgresAdvisoryLockManager, RedisDistributedLockManager
from .maintainer import PartitionMaintainer, maintain_partitions
from .metadata import PostgresMetadataProvider
from .protocols import (
    LockManager,
    PartitionLifecycle,
    PartitionMetadataProvider,
    PartitionRepository,
)
from .repositories import PostgresPartitionRepository
from .service import PartitionLifecycleService
from .services import PartitionInspector, PartitionValidationService, PlanExecutor
from .toolkit import PartitionToolkit

__all__ = [
    "BasePartitionLifecycleHooks",
    "CommandHookError",
    "CommandHooks",
    "LockManager",
    "PartitionInspector",
    "PartitionLifecycle",
    "PartitionLifecycleHooks",
    "PartitionLifecycleService",
    "PartitionMaintainer",
    "PartitionMetadataProvider",
    "PartitionRepository",
    "PartitionToolkit",
    "PartitionValidationService",
    "PlanExecutor",
    "PostgresAdvisoryLockManager",
    "PostgresMetadataProvider",
    "PostgresPartitionRepository",
    "RedisDistributedLockManager",
    "maintain_partitions",
]
