"""Sync implementations for partition management."""

from .hooks import BasePartitionLifecycleHooks, PartitionLifecycleHooks
from .lock import PostgresAdvisoryLockManager, RedisDistributedLockManager
from .maintainer import PartitionMaintainer, maintain_partitions
from .metadata import PostgresMetadataProvider
from .protocols import LockManager, PartitionMetadataProvider, PartitionRepository
from .repositories import PostgresPartitionRepository
from .service import PartitionLifecycleService

__all__ = [
    "BasePartitionLifecycleHooks",
    "LockManager",
    "PartitionLifecycleHooks",
    "PartitionLifecycleService",
    "PartitionMaintainer",
    "PartitionMetadataProvider",
    "PartitionRepository",
    "PostgresAdvisoryLockManager",
    "PostgresMetadataProvider",
    "PostgresPartitionRepository",
    "RedisDistributedLockManager",
    "maintain_partitions",
]
