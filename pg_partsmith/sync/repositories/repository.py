"""PostgreSQL partition repository with safe SQL operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pg_partsmith.constants import (
    DEFAULT_DDL_TIMEOUT_SECONDS,
    DEFAULT_DDL_TIMEZONE,
    DEFAULT_DROP_LOCK_TIMEOUT_MS,
    DEFAULT_DROP_MAX_BACKOFF,
    DEFAULT_DROP_MAX_RETRIES,
    DEFAULT_DROP_RETRY_DELAY,
)
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
    from sqlalchemy import Engine

    from pg_partsmith.entities import PartitionInfo, SubpartitionBounds, SubpartitionSpec, TablePartitionConfig


class PostgresPartitionRepository:
    """PostgreSQL implementation of partition repository.

    Facade that delegates to specialized helper classes for improved maintenance and SRP.

    Unlike the async implementation, ``ddl_timeout_seconds`` is enforced
    server-side via PostgreSQL ``statement_timeout`` (per statement) rather
    than client-side around the whole operation.
    """

    def __init__(
        self,
        engine: Engine,
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
        marker_prefix = orphan_comment_prefix(marker_prefix=marker_prefix)
        ddl_timeout_seconds = validate_ddl_timeout(ddl_timeout_seconds)
        self._ddl_timezone = validate_timezone(ddl_timezone)
        drop_lock_timeout_ms = validate_int(drop_lock_timeout_ms, "drop_lock_timeout_ms", min_val=0)
        drop_max_retries = validate_int(drop_max_retries, "drop_max_retries", min_val=1)
        drop_retry_delay = validate_float(drop_retry_delay, "drop_retry_delay", min_val=0.0)
        drop_max_backoff = validate_float(drop_max_backoff, "drop_max_backoff", min_val=0.0)

        self._resolver = PartitionRelationResolver(engine)
        self._fk_manager = PartitionForeignKeyManager(engine, ddl_timeout_seconds)
        self._creator = PartitionCreator(
            engine=engine,
            ddl_timeout=ddl_timeout_seconds,
            ddl_timezone=self._ddl_timezone,
        )
        self._remover = PartitionRemover(
            engine=engine,
            ddl_timeout=ddl_timeout_seconds,
            drop_lock_timeout_ms=drop_lock_timeout_ms,
            drop_max_retries=drop_max_retries,
            drop_retry_delay=drop_retry_delay,
            drop_max_backoff=drop_max_backoff,
            marker_prefix=marker_prefix,
            resolver=self._resolver,
            fk_manager=self._fk_manager,
            allow_unmanaged=bool(drop_allow_unmanaged),
        )

    @property
    def ddl_timezone(self) -> str | None:
        """Timezone applied via ``SET LOCAL TIME ZONE`` around boundary-sensitive DDL.

        ``None`` means the session timezone is trusted as-is.
        """
        return self._ddl_timezone

    def create_partition(
        self, config: TablePartitionConfig, partition_name: str, from_value: str, to_value: str
    ) -> PartitionInfo:
        return self._creator.create(config, partition_name, from_value, to_value)

    def attach_partition(self, table_name: str, partition_name: str, from_value: str, to_value: str) -> None:
        self._creator.attach(table_name, partition_name, from_value, to_value)

    def create_branch(
        self,
        config: TablePartitionConfig,
        branch_name: str,
        from_value: str,
        to_value: str,
        spec: SubpartitionSpec,
    ) -> PartitionInfo:
        """Create a detached time partition that is itself partitioned.

        See :meth:`PartitionCreator.create_branch`. Its buckets are created
        separately and the branch is attached last, so an interrupted run can
        never leave a partially-covering branch reachable from the root.
        """
        return self._creator.create_branch(config, branch_name, from_value, to_value, spec)

    def create_subpartition_table(self, parent_name: str, child_name: str, spec: SubpartitionSpec | None) -> None:
        """Create a detached table shaped like ``parent_name``.

        See :meth:`PartitionCreator.create_subpartition_table`.
        """
        self._creator.create_subpartition_table(parent_name, child_name, spec)

    def attach_subpartition(self, parent_name: str, child_name: str, bounds: SubpartitionBounds) -> None:
        """Attach one subpartition to its parent.

        See :meth:`PartitionCreator.attach_subpartition`.
        """
        self._creator.attach_subpartition(parent_name, child_name, bounds)

    def attach_composite_partition(
        self,
        table_name: str,
        partition_name: str,
        from_value: str,
        to_value: str,
        *,
        key_arity: int,
    ) -> None:
        """Attach a partition to a parent with a composite partition key.

        See :meth:`PartitionCreator.attach_composite_partition`.
        """
        self._creator.attach_composite_partition(table_name, partition_name, from_value, to_value, key_arity=key_arity)

    def detach_partition(self, table_name: str, partition_name: str, *, concurrent: bool = True) -> None:
        self._remover.detach(table_name, partition_name, concurrent=concurrent)

    def drop_partition(self, partition_name: str) -> None:
        self._remover.drop(partition_name)

    def adopt_partition(self, table_name: str, partition_name: str) -> bool:
        """Mark a detached legacy table as owned by this library (orphan marker).

        See :meth:`PartitionRemover.adopt`. Use once when migrating an existing
        partitioner instead of enabling ``drop_allow_unmanaged``.
        """
        return self._remover.adopt(table_name, partition_name)

    def reconcile_default_rows(
        self,
        *,
        default_partition_name: str,
        target_partition_name: str,
        partition_column: str,
        trailing_columns: tuple[str, ...] = (),
        from_value: str,
        to_value: str,
    ) -> int:
        return self._creator.reconcile_default_rows(
            default_partition_name=default_partition_name,
            target_partition_name=target_partition_name,
            partition_column=partition_column,
            trailing_columns=trailing_columns,
            from_value=from_value,
            to_value=to_value,
        )
