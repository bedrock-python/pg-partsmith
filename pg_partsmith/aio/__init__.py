"""Async implementations for partition management."""

from .hooks import BasePartitionLifecycleHooks, PartitionLifecycleHooks
from .lock import PostgresAdvisoryLockManager, RedisDistributedLockManager
from .maintainer import PartitionMaintainer, maintain_partitions
from .metadata import PostgresMetadataProvider
from .protocols import (
    CompositeKeyMetadata,
    CompositeKeyRepository,
    LockManager,
    NestedPartitionMetadata,
    PartitionMetadataProvider,
    PartitionRepository,
    SubpartitionRepository,
)
from .repositories import PostgresPartitionRepository
from .service import PartitionLifecycleService

__all__ = [
    "BasePartitionLifecycleHooks",
    "CompositeKeyMetadata",
    "CompositeKeyRepository",
    "LockManager",
    "NestedPartitionMetadata",
    "PartitionLifecycleHooks",
    "PartitionLifecycleService",
    "PartitionMaintainer",
    "PartitionMetadataProvider",
    "PartitionRepository",
    "PostgresAdvisoryLockManager",
    "PostgresMetadataProvider",
    "PostgresPartitionRepository",
    "RedisDistributedLockManager",
    "SubpartitionRepository",
    "maintain_partitions",
]
