"""PostgreSQL partition repository with safe SQL operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pg_partsmith.utils import (
    orphan_comment_prefix,
    validate_ddl_timeout,
    validate_float,
    validate_int,
    validate_timezone,
)

from .creator import PartitionCreator
from .fk_manager import PartitionForeignKeyManager
from .remover import PartitionRemover
from .resolver import PartitionRelationResolver

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from pg_partsmith.entities import PartitionInfo, TablePartitionConfig


# Default values for partition management operations.
DEFAULT_DDL_TIMEOUT_SECONDS = 30.0
DEFAULT_DDL_TIMEZONE = "UTC"
DEFAULT_DROP_LOCK_TIMEOUT_MS = 3000
DEFAULT_DROP_MAX_RETRIES = 3
DEFAULT_DROP_RETRY_DELAY = 0.5
DEFAULT_DROP_MAX_BACKOFF = 300.0


class PostgresPartitionRepository:
    """PostgreSQL implementation of partition repository.

    Facade that delegates to specialized helper classes for improved maintenance and SRP.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        ddl_timezone: str | None = DEFAULT_DDL_TIMEZONE,
        ddl_timeout_seconds: float = DEFAULT_DDL_TIMEOUT_SECONDS,
        marker_prefix: str | None = None,
        drop_allow_unmanaged: bool = False,
        drop_lock_timeout_ms: int = DEFAULT_DROP_LOCK_TIMEOUT_MS,
        drop_max_retries: int = DEFAULT_DROP_MAX_RETRIES,
        drop_retry_delay: float = DEFAULT_DROP_RETRY_DELAY,
        drop_max_backoff: float = DEFAULT_DROP_MAX_BACKOFF,
    ) -> None:
        self._engine = engine
        self._marker_prefix = orphan_comment_prefix(marker_prefix=marker_prefix)

        # Configuration validation
        self._ddl_timeout_seconds = validate_ddl_timeout(ddl_timeout_seconds)
        self._ddl_timezone = validate_timezone(ddl_timezone)
        self._drop_allow_unmanaged = bool(drop_allow_unmanaged)
        self._drop_lock_timeout_ms = validate_int(drop_lock_timeout_ms, "drop_lock_timeout_ms", min_val=0)
        self._drop_max_retries = validate_int(drop_max_retries, "drop_max_retries", min_val=1)
        self._drop_retry_delay = validate_float(drop_retry_delay, "drop_retry_delay", min_val=0.0)
        self._drop_max_backoff = validate_float(drop_max_backoff, "drop_max_backoff", min_val=0.0)

        # Initialize helpers
        self._resolver = PartitionRelationResolver(engine)
        self._fk_manager = PartitionForeignKeyManager(engine, self._ddl_timeout_seconds)
        self._creator = PartitionCreator(engine, self._ddl_timeout_seconds, self._ddl_timezone)
        self._remover = PartitionRemover(
            engine=engine,
            ddl_timeout=self._ddl_timeout_seconds,
            drop_lock_timeout_ms=self._drop_lock_timeout_ms,
            drop_max_retries=self._drop_max_retries,
            drop_retry_delay=self._drop_retry_delay,
            drop_max_backoff=self._drop_max_backoff,
            marker_prefix=self._marker_prefix,
            resolver=self._resolver,
            fk_manager=self._fk_manager,
            allow_unmanaged=self._drop_allow_unmanaged,
        )

    @property
    def ddl_timezone(self) -> str | None:
        """Timezone applied via ``SET LOCAL TIME ZONE`` around boundary-sensitive DDL.

        ``None`` means the session timezone is trusted as-is.
        """
        return self._ddl_timezone

    # Public API delegated to helpers
    async def create_partition(
        self, config: TablePartitionConfig, partition_name: str, from_value: str, to_value: str
    ) -> PartitionInfo:
        return await self._creator.create(config, partition_name, from_value, to_value)

    async def attach_partition(self, table_name: str, partition_name: str, from_value: str, to_value: str) -> None:
        await self._creator.attach(table_name, partition_name, from_value, to_value)

    async def detach_partition(self, table_name: str, partition_name: str, *, concurrent: bool = True) -> None:
        await self._remover.detach(table_name, partition_name, concurrent=concurrent)

    async def drop_partition(self, partition_name: str) -> None:
        await self._remover.drop(partition_name)

    async def adopt_partition(self, table_name: str, partition_name: str) -> bool:
        """Mark a detached legacy table as owned by this library (orphan marker).

        See :meth:`PartitionRemover.adopt`. Use once when migrating an existing
        partitioner instead of enabling ``drop_allow_unmanaged``.
        """
        return await self._remover.adopt(table_name, partition_name)

    async def partition_exists(self, partition_name: str) -> bool:
        return await self._resolver.exists(partition_name)

    async def is_partition_attached(self, table_name: str, partition_name: str) -> bool:
        return await self._resolver.is_attached(table_name, partition_name)

    async def reconcile_default_rows(
        self,
        *,
        default_partition_name: str,
        target_partition_name: str,
        partition_column: str,
        from_value: str,
        to_value: str,
    ) -> int:
        return await self._creator.reconcile_default_rows(
            default_partition_name=default_partition_name,
            target_partition_name=target_partition_name,
            partition_column=partition_column,
            from_value=from_value,
            to_value=to_value,
        )
