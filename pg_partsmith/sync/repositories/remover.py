"""Helper for partition detachment and removal."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.exceptions import (
    DropRetryExhaustedError,
    PartitionAttachedError,
    PartitionDetachInProgressError,
    PartitionNotFoundError,
    UnmanagedPartitionDropError,
)
from pg_partsmith.utils import (
    build_ddl_statement,
    orphan_table_comment,
    pg_sqlstate,
    to_regclass_argument,
)

from .timeouts import apply_local_statement_timeout, session_statement_timeout

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine

    from .fk_manager import PartitionForeignKeyManager
    from .resolver import PartitionRelationResolver

logger = logging.getLogger(__name__)

# PostgreSQL SQLSTATE codes that indicate transient lock contention.
_RETRYABLE_PG_STATES: frozenset[str] = frozenset(
    {
        "40P01",  # deadlock_detected
        "55P03",  # lock_not_available  (SET lock_timeout fires this via pg_error_code 55P03)
        "57014",  # query_canceled      (lock_timeout / statement_timeout)
    }
)


class PartitionRemover:
    """Helper for partition detachment and removal."""

    def __init__(
        self,
        engine: Engine,
        ddl_timeout: float,
        drop_lock_timeout_ms: int,
        drop_max_retries: int,
        drop_retry_delay: float,
        marker_prefix: str,
        resolver: PartitionRelationResolver,
        fk_manager: PartitionForeignKeyManager,
        allow_unmanaged: bool = False,
        drop_max_backoff: float = 300.0,
    ) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout
        self._drop_lock_timeout_ms = drop_lock_timeout_ms
        self._drop_max_retries = drop_max_retries
        self._drop_retry_delay = drop_retry_delay
        self._drop_max_backoff = drop_max_backoff
        self._marker_prefix = marker_prefix
        self._resolver = resolver
        self._fk_manager = fk_manager
        self._allow_unmanaged = allow_unmanaged

    def detach(self, table_name: str, partition_name: str, *, concurrent: bool = True) -> None:
        """Detach partition from parent.

        A partition left in ``inhdetachpending`` state by a cancelled
        ``DETACH CONCURRENTLY`` (e.g. our own DDL timeout) is completed with
        ``DETACH PARTITION ... FINALIZE`` instead of failing forever.
        """
        if self._finalize_if_pending(table_name, partition_name):
            return

        if concurrent and self._detach_concurrently(table_name, partition_name):
            return

        stmt = build_ddl_statement(
            "ALTER TABLE {parent} DETACH PARTITION {partition}",
            parent=table_name,
            partition=partition_name,
        )
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            try:
                self._mark_orphaned(conn, table_name, partition_name)
                conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                domain_exc = self._translate_detach_error(exc, partition_name)
                if domain_exc is not None:
                    raise domain_exc from exc
                raise

    def _finalize_if_pending(self, table_name: str, partition_name: str) -> bool:
        """Complete a detach left pending by a cancelled DETACH CONCURRENTLY.

        Returns True if the partition was in pending-detach state and has been
        finalized (i.e. it is now fully detached).
        """
        with self._engine.connect() as base_conn:
            conn = base_conn.execution_options(isolation_level="AUTOCOMMIT")
            with session_statement_timeout(conn, self._ddl_timeout):
                result = conn.execute(
                    text(
                        """
                        SELECT inh.inhdetachpending
                        FROM pg_inherits inh
                        WHERE inh.inhrelid = to_regclass(:partition_name)
                          AND inh.inhparent = to_regclass(:table_name)
                        """
                    ),
                    {
                        "partition_name": to_regclass_argument(partition_name),
                        "table_name": to_regclass_argument(table_name),
                    },
                )
                if not result.scalar():
                    return False

                logger.warning(
                    "Completing pending detach left by a cancelled DETACH CONCURRENTLY",
                    extra={"table_name": table_name, "partition_name": partition_name},
                )
                self._mark_orphaned(conn, table_name, partition_name)
                conn.execute(
                    build_ddl_statement(
                        "ALTER TABLE {parent} DETACH PARTITION {partition} FINALIZE",
                        parent=table_name,
                        partition=partition_name,
                    )
                )
                return True

    def _detach_concurrently(self, table_name: str, partition_name: str) -> bool:
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} DETACH PARTITION {partition} CONCURRENTLY",
            parent=table_name,
            partition=partition_name,
        )
        with self._engine.connect() as base_conn:
            conn = base_conn.execution_options(isolation_level="AUTOCOMMIT")
            with session_statement_timeout(conn, self._ddl_timeout):
                try:
                    self._mark_orphaned(conn, table_name, partition_name)
                    conn.execute(stmt)
                except (SQLAlchemyError, OSError, TimeoutError) as exc:
                    sqlstate = pg_sqlstate(exc)
                    if sqlstate in {"42P01", "55006"}:
                        domain_exc = self._translate_detach_error(exc, partition_name)
                        assert domain_exc is not None  # guaranteed by sqlstate match above
                        raise domain_exc from exc
                    if sqlstate not in {"0A000", "42601", "55000"}:
                        raise
                    logger.warning(
                        "DETACH PARTITION CONCURRENTLY failed; falling back to non-concurrent DETACH",
                        extra={
                            "table_name": table_name,
                            "partition_name": partition_name,
                            "sqlstate": sqlstate,
                            "reason": str(exc),
                        },
                    )
                    return False
                else:
                    return True

    def _mark_orphaned(self, conn: Connection, table_name: str, partition_name: str) -> None:
        parent_fqn = self._resolver.resolve_fqn_conn(conn, table_name) or table_name
        marker = orphan_table_comment(parent_fqn, marker_prefix=self._marker_prefix)

        comment_result = conn.execute(
            text("SELECT obj_description(to_regclass(:partition_name), 'pg_class')"),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        existing_comment = comment_result.scalar()
        if isinstance(existing_comment, bytes):
            existing_comment = existing_comment.decode("utf-8", errors="replace")

        if existing_comment == marker or (existing_comment and existing_comment.startswith(f"{marker}\n")):
            return

        new_comment = marker if not existing_comment else f"{marker}\n{existing_comment}"
        conn.execute(
            build_ddl_statement(
                "COMMENT ON TABLE {partition} IS [comment]",
                partition=partition_name,
                comment=new_comment,
            )
        )

    def _translate_detach_error(self, exc: Exception, partition_name: str) -> Exception | None:
        """Translate a SQL-level detach error into a domain exception.

        Returns ``None`` when the error is not one we can translate; the caller
        is expected to re-raise the original exception in that case.
        """
        sqlstate = pg_sqlstate(exc)
        if sqlstate == "42P01":
            return PartitionNotFoundError(partition_name)
        if sqlstate == "55006":
            return PartitionDetachInProgressError(partition_name)
        return None

    def drop(self, partition_name: str) -> None:
        """Drop a detached partition."""
        with self._engine.connect() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            if not self._resolver.exists_conn(conn, partition_name):
                return
            self._ensure_not_attached(conn, partition_name)
            if not self._ensure_managed(conn, partition_name):
                return

        fk_constraints = self._fk_manager.list_constraints(partition_name)
        last_exc: BaseException | None = None

        for attempt in range(self._drop_max_retries):
            if attempt > 0:
                self._handle_retry_delay(attempt, last_exc, partition_name)

            try:
                self._execute_drop(partition_name, fk_constraints)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if isinstance(exc, SQLAlchemyError) and pg_sqlstate(exc) not in _RETRYABLE_PG_STATES:
                    raise
                last_exc = exc
            else:
                return

        raise DropRetryExhaustedError(partition_name, self._drop_max_retries, last_exc) from last_exc

    def _execute_drop(self, partition_name: str, fk_constraints: list[str]) -> None:
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            conn.execute(
                text("SELECT set_config('lock_timeout', :timeout, true)"),
                {"timeout": str(self._drop_lock_timeout_ms)},
            )
            self._fk_manager.drop_constraints(conn, partition_name, fk_constraints)
            conn.execute(build_ddl_statement("DROP TABLE IF EXISTS {partition}", partition=partition_name))

    def _ensure_not_attached(self, conn: Connection, partition_name: str) -> None:
        result = conn.execute(
            text(
                """
                SELECT parent_ns.nspname || '.' || parent.relname
                FROM pg_inherits inh
                JOIN pg_class child ON inh.inhrelid = child.oid
                JOIN pg_class parent ON inh.inhparent = parent.oid
                JOIN pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid
                WHERE child.oid = to_regclass(:partition_name)
                  AND child.relispartition = true
                """
            ),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        parent_name = result.scalar()
        if parent_name is not None:
            if isinstance(parent_name, bytes):
                parent_name = parent_name.decode("utf-8", errors="replace")
            raise PartitionAttachedError(partition_name, str(parent_name))

    def _ensure_managed(self, conn: Connection, partition_name: str) -> bool:
        if self._allow_unmanaged:
            return True

        comment_result = conn.execute(
            text("SELECT obj_description(to_regclass(:partition_name), 'pg_class')"),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        existing_comment = comment_result.scalar()
        if isinstance(existing_comment, bytes):
            existing_comment = existing_comment.decode("utf-8", errors="replace")
        comment_str = str(existing_comment) if existing_comment is not None else None

        if comment_str is None:
            if not self._resolver.exists_conn(conn, partition_name):
                return False
            raise UnmanagedPartitionDropError(partition_name)

        prefix = self._marker_prefix
        has_parent = len(comment_str) > len(prefix) and comment_str[len(prefix)] != "\n"
        if not comment_str.startswith(prefix) or not has_parent:
            raise UnmanagedPartitionDropError(partition_name)

        return True

    def _handle_retry_delay(self, attempt: int, last_exc: BaseException | None, partition_name: str) -> None:
        delay = min(
            self._drop_retry_delay * (2 ** (attempt - 1)),
            self._drop_max_backoff,
        )
        time.sleep(delay)
        logger.warning(
            "Retrying drop_partition after transient error",
            extra={
                "partition_name": partition_name,
                "attempt": attempt + 1,
                "max_retries": self._drop_max_retries,
                "reason": str(last_exc),
            },
        )
