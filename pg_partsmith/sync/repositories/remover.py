"""Helper for partition detachment and removal."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.catalog_queries import RELATION_KIND_SQL
from pg_partsmith.exceptions import (
    DropRetryExhaustedError,
    PartitionAttachedError,
    PartitionDetachInProgressError,
    PartitionNotFoundError,
    PartitionReferencedError,
    PlanStaleError,
    UnmanagedPartitionDropError,
)
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.utils import (
    build_ddl_statement,
    coerce_str,
    orphan_comment,
    parse_orphan_comment,
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

# SQLSTATEs PostgreSQL raises when DETACH CONCURRENTLY is not possible here:
# feature_not_supported, syntax_error (older servers), and
# object_not_in_prerequisite_state ("cannot detach partitions concurrently
# when a default partition exists").
_CONCURRENT_DETACH_UNAVAILABLE: frozenset[str] = frozenset({"0A000", "42601", "55000"})


class PartitionRemover:
    """Helper for partition detachment and removal."""

    def __init__(
        self,
        *,
        engine: Engine,
        ddl_timeout: float,
        drop_lock_timeout_ms: int,
        drop_max_retries: int,
        drop_retry_delay: float,
        drop_max_backoff: float,
        marker_prefix: str,
        resolver: PartitionRelationResolver,
        fk_manager: PartitionForeignKeyManager,
        allow_unmanaged: bool,
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

    def detach(self, table_name: str, partition_name: str, *, mode: DetachMode = DetachMode.AUTO) -> None:
        """Detach partition from parent, writing the orphan marker first.

        A partition left in ``inhdetachpending`` state by a cancelled
        ``DETACH CONCURRENTLY`` (e.g. our own DDL timeout) is completed with
        ``DETACH PARTITION ... FINALIZE`` instead of failing forever.

        Args:
            table_name: Parent table name.
            partition_name: Partition table name.
            mode: ``CONCURRENT`` runs only the concurrent form and fails when
                PostgreSQL refuses it (a DEFAULT partition exists);
                ``BLOCKING`` runs the plain form; ``AUTO`` tries the concurrent
                form and falls back to the blocking one.
        """
        if self._finalize_if_pending(table_name, partition_name):
            return

        if mode is not DetachMode.BLOCKING and self._detach_concurrently(
            table_name, partition_name, fallback=mode is DetachMode.AUTO
        ):
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

    def adopt(self, table_name: str, partition_name: str) -> bool:
        """Stamp the orphan marker on a detached table this library did not detach.

        Legacy partitions detached before the library was introduced carry no
        marker, so safe-drop refuses them. Adopting marks them as owned by the
        library; the next maintenance run collects and drops them like any
        other orphan. The detach instant is left unknown, so a grace period
        does not delay a table that has already waited. Idempotent.

        Returns:
            True when the marker is present after the call, False when the
            table does not exist.

        Raises:
            PartitionAttachedError: If the table is currently attached —
                attached partitions get their marker automatically at detach.
        """
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            if not self._resolver.exists_conn(conn, partition_name):
                return False
            self._ensure_not_attached(conn, partition_name)
            self._mark_orphaned(conn, table_name, partition_name, stamp_now=False)
            return True

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

    def _detach_concurrently(self, table_name: str, partition_name: str, *, fallback: bool) -> bool:
        """Run ``DETACH … CONCURRENTLY``; False when the blocking form should run instead.

        The statement cannot run inside a transaction block, so it goes out on
        an AUTOCOMMIT connection.
        """
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
                    if sqlstate in {"42P01", "55006", "23503"}:
                        domain_exc = self._translate_detach_error(exc, partition_name)
                        assert domain_exc is not None  # guaranteed by sqlstate match above
                        raise domain_exc from exc
                    if sqlstate not in _CONCURRENT_DETACH_UNAVAILABLE or not fallback:
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

    def _mark_orphaned(
        self,
        conn: Connection,
        table_name: str,
        partition_name: str,
        *,
        stamp_now: bool = True,
    ) -> None:
        """Write the ownership marker, recording the detach instant.

        Written *before* the DETACH so an interrupted run leaves a marked table
        rather than an unmarked one that orphan discovery would never see.

        Args:
            stamp_now: Record the current instant as the detach time. Off for
                adoption, where the table was detached at an unknown time.
        """
        parent_fqn = self._resolver.resolve_fqn_conn(conn, table_name) or table_name

        comment_result = conn.execute(
            text("SELECT obj_description(to_regclass(:partition_name), 'pg_class')"),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        existing_comment = coerce_str(comment_result.scalar())

        instant = datetime.now(UTC) if stamp_now else None
        new_comment = orphan_comment(
            parent_fqn,
            detached_at=instant,
            existing_comment=existing_comment,
            marker_prefix=self._marker_prefix,
        )
        if new_comment == existing_comment:
            return

        # ``COMMENT ON TABLE`` refuses a foreign table ("is not a table").
        relation = "FOREIGN TABLE" if _relkind(conn, partition_name) == "f" else "TABLE"
        conn.execute(
            build_ddl_statement(
                f"COMMENT ON {relation} {{partition}} IS [comment]",
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
        if sqlstate == "23503":
            # "removing partition ... violates foreign key constraint": rows of
            # another table reference rows of this partition through a foreign
            # key on the parent. Verified identical on PostgreSQL 15 and 17,
            # for the plain and the CONCURRENTLY form alike.
            return PartitionReferencedError(partition_name, str(exc).strip().splitlines()[0])
        return None

    def drop(self, partition_name: str, *, expected_oid: int | None = None) -> None:
        """Drop a detached partition.

        Args:
            partition_name: The table to drop.
            expected_oid: The identity the decision to drop was made about.
                A relation that has since been dropped and recreated under the
                same name carries another OID and is left alone.

        Raises:
            PlanStaleError: If the relation holding the name is not the one
                ``expected_oid`` identifies.
        """
        with self._engine.connect() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            if not self._resolver.exists_conn(conn, partition_name):
                return
            self._ensure_expected_oid(conn, partition_name, expected_oid)
            self._ensure_not_attached(conn, partition_name)
            if not self._ensure_managed(conn, partition_name):
                return

        last_exc: BaseException | None = None

        for attempt in range(self._drop_max_retries):
            if attempt > 0:
                self._handle_retry_delay(attempt, last_exc, partition_name)

            try:
                self._execute_drop(partition_name, expected_oid)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if isinstance(exc, SQLAlchemyError) and pg_sqlstate(exc) not in _RETRYABLE_PG_STATES:
                    raise
                last_exc = exc
            else:
                return

        raise DropRetryExhaustedError(partition_name, self._drop_max_retries, last_exc) from last_exc

    def _execute_drop(self, partition_name: str, expected_oid: int | None) -> None:
        """Lock, revalidate, and drop in one transaction.

        The pre-checks in :meth:`drop` run on a different connection, so the
        relation could have been reattached or replaced since. Taking ACCESS
        EXCLUSIVE first and revalidating under it closes that window —
        PostgreSQL happily drops even an attached partition via DROP TABLE.
        """
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            conn.execute(
                text("SELECT set_config('lock_timeout', :timeout, true)"),
                {"timeout": str(self._drop_lock_timeout_ms)},
            )
            # A foreign table cannot be LOCKed ("not supported for foreign
            # tables"); it holds no rows of its own, and DROP takes the lock
            # it needs on the catalog entry itself.
            foreign = _relkind(conn, partition_name) == "f"
            if not foreign:
                try:
                    conn.execute(
                        build_ddl_statement("LOCK TABLE {partition} IN ACCESS EXCLUSIVE MODE", partition=partition_name)
                    )
                except (SQLAlchemyError, OSError, TimeoutError) as exc:
                    if pg_sqlstate(exc) == "42P01":  # dropped concurrently — nothing left to do
                        return
                    raise
            self._ensure_expected_oid(conn, partition_name, expected_oid)
            self._ensure_not_attached(conn, partition_name)
            if not self._ensure_managed(conn, partition_name):
                return
            if foreign:
                # A foreign table carries no constraints of its own, and
                # ``DROP TABLE`` refuses it ("is not a table").
                conn.execute(build_ddl_statement("DROP FOREIGN TABLE IF EXISTS {partition}", partition=partition_name))
                return
            fk_constraints = self._fk_manager.list_constraints_conn(conn, partition_name)
            self._fk_manager.drop_constraints(conn, partition_name, fk_constraints)
            conn.execute(build_ddl_statement("DROP TABLE IF EXISTS {partition}", partition=partition_name))

    def _ensure_expected_oid(self, conn: Connection, partition_name: str, expected_oid: int | None) -> None:
        if expected_oid is None:
            return
        result = conn.execute(
            text("SELECT c.oid FROM pg_class c WHERE c.oid = to_regclass(:name)"),
            {"name": to_regclass_argument(partition_name)},
        )
        actual = result.scalar()
        if actual is not None and int(actual) != expected_oid:
            raise PlanStaleError(
                partition_name,
                f"the relation now holding the name has OID {int(actual)}, the plan decided about OID {expected_oid}",
            )

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
        parent_name = coerce_str(result.scalar())
        if parent_name is not None:
            raise PartitionAttachedError(partition_name, parent_name)

    def _ensure_managed(self, conn: Connection, partition_name: str) -> bool:
        if self._allow_unmanaged:
            return True

        comment_result = conn.execute(
            text("SELECT obj_description(to_regclass(:partition_name), 'pg_class')"),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        comment_str = coerce_str(comment_result.scalar())

        if comment_str is None:
            if not self._resolver.exists_conn(conn, partition_name):
                return False
            raise UnmanagedPartitionDropError(partition_name)

        if parse_orphan_comment(comment_str, marker_prefix=self._marker_prefix) is None:
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


def _relkind(conn: Connection, name: str) -> str | None:
    """``pg_class.relkind`` of the relation holding ``name``, or None when there is none."""
    result = conn.execute(text(RELATION_KIND_SQL), {"name": to_regclass_argument(name)})
    return coerce_str(result.scalar(), encoding="ascii")
