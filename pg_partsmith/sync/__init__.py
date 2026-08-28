"""Sync implementations for partition management."""

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

__all__ = [
    "BasePartitionLifecycleHooks",
    "LockManager",
    "PartitionInspector",
    "PartitionLifecycle",
    "PartitionLifecycleHooks",
    "PartitionLifecycleService",
    "PartitionMaintainer",
    "PartitionMetadataProvider",
    "PartitionRepository",
    "PartitionValidationService",
    "PlanExecutor",
    "PostgresAdvisoryLockManager",
    "PostgresMetadataProvider",
    "PostgresPartitionRepository",
    "RedisDistributedLockManager",
    "maintain_partitions",
]
