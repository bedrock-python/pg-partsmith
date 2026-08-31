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
from pg_partsmith.lifecycle import DetachMode
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

    from pg_partsmith.leaves import LocalLeaves
    from pg_partsmith.plan import PartitionBy
    from pg_partsmith.topology import PartitionBounds


class PostgresPartitionRepository:
    """PostgreSQL implementation of partition repository.

    Facade that delegates to specialized helper classes for improved maintenance and SRP.
    Every statement runs in its own transaction and commits immediately.
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
            marker_prefix=marker_prefix,
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
            creator=self._creator,
            allow_unmanaged=bool(drop_allow_unmanaged),
        )

    @property
    def ddl_timezone(self) -> str | None:
        """Timezone applied via ``SET LOCAL TIME ZONE`` around boundary-sensitive DDL.

        ``None`` means the session timezone is trusted as-is.
        """
        return self._ddl_timezone

    def create_table_like(
        self,
        template_name: str,
        table_name: str,
        partition_by: PartitionBy | None,
        *,
        physical: LocalLeaves | None = None,
    ) -> int:
        """Create a detached table shaped like ``template_name``; returns its OID.

        See :meth:`PartitionCreator.create_table_like`.
        """
        return self._creator.create_table_like(template_name, table_name, partition_by, physical=physical)

    def create_foreign_table_like(
        self,
        template_name: str,
        table_name: str,
        *,
        server: str,
        options: dict[str, str],
    ) -> int:
        """Create a detached foreign table with ``template_name``'s columns; returns its OID.

        See :meth:`PartitionCreator.create_foreign_table_like`.
        """
        return self._creator.create_foreign_table_like(template_name, table_name, server=server, options=options)

    def attach_partition(
        self,
        parent_name: str,
        partition_name: str,
        bounds: PartitionBounds,
        *,
        key_arity: int = 1,
        expected_oid: int | None = None,
        expected_parent_oid: int | None = None,
    ) -> None:
        """Attach a table to a partitioned parent.

        See :meth:`PartitionCreator.attach`.
        """
        self._creator.attach(
            parent_name,
            partition_name,
            bounds,
            key_arity=key_arity,
            expected_oid=expected_oid,
            expected_parent_oid=expected_parent_oid,
        )

    def detach_partition(
        self,
        parent_name: str,
        partition_name: str,
        *,
        mode: DetachMode = DetachMode.AUTO,
        expected_oid: int | None = None,
    ) -> None:
        """Detach a partition, writing the orphan marker first.

        See :meth:`PartitionRemover.detach`.
        """
        self._remover.detach(parent_name, partition_name, mode=mode, expected_oid=expected_oid)

    def drop_partition(
        self, partition_name: str, *, expected_oid: int | None = None, drain_into: str | None = None
    ) -> int:
        """Drop a detached, marker-tagged partition; returns the rows moved into ``drain_into``.

        See :meth:`PartitionRemover.drop`.
        """
        return self._remover.drop(partition_name, expected_oid=expected_oid, drain_into=drain_into)

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
        key_columns: tuple[str, ...],
        from_value: str,
        to_value: str,
        limit: int | None = None,
        expected_source_oid: int | None = None,
        expected_target_oid: int | None = None,
    ) -> int:
        """Move rows from a DEFAULT partition to the partition for a window.

        See :meth:`PartitionCreator.reconcile_default_rows`.
        """
        return self._creator.reconcile_default_rows(
            default_partition_name=default_partition_name,
            target_partition_name=target_partition_name,
            key_columns=key_columns,
            from_value=from_value,
            to_value=to_value,
            limit=limit,
            expected_source_oid=expected_source_oid,
            expected_target_oid=expected_target_oid,
        )

    def move_rows(self, source_name: str, target_name: str, *, limit: int | None = None) -> int:
        """Move rows from one relation into another.

        See :meth:`PartitionCreator.move_rows`.
        """
        return self._creator.move_rows(source_name, target_name, limit=limit)
