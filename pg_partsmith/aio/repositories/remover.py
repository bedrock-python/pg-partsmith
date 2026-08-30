"""Helper for partition detachment and removal."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

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

from .resolver import relation_kind

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from .creator import PartitionCreator
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
        engine: AsyncEngine,
        ddl_timeout: float,
        drop_lock_timeout_ms: int,
        drop_max_retries: int,
        drop_retry_delay: float,
        drop_max_backoff: float,
        marker_prefix: str,
        resolver: PartitionRelationResolver,
        fk_manager: PartitionForeignKeyManager,
        creator: PartitionCreator,
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
        self._creator = creator
        self._allow_unmanaged = allow_unmanaged

    async def detach(
        self,
        table_name: str,
        partition_name: str,
        *,
        mode: DetachMode = DetachMode.AUTO,
        expected_oid: int | None = None,
    ) -> None:
        """Detach partition from parent, writing the orphan marker first.

        A partition left in ``inhdetachpending`` state by a cancelled
        ``DETACH CONCURRENTLY`` (e.g. our own DDL timeout) is completed with
        ``DETACH PARTITION ... FINALIZE`` instead of failing forever.

        DDL addresses relations by name, and a name can change hands between
        the decision and the statement -- a hook or another session dropping
        the partition and creating another under the same name. With
        ``expected_oid`` the relation's identity and its attachment are
        checked again right before the marker and the statement: in the
        blocking form under the lock the detach itself takes, so nothing can
        change in between; in the concurrent form, which cannot run inside a
        transaction, immediately before and once more after the statement.

        Args:
            table_name: Parent table name.
            partition_name: Partition table name.
            mode: ``CONCURRENT`` runs only the concurrent form and fails when
                PostgreSQL refuses it (a DEFAULT partition exists);
                ``BLOCKING`` runs the plain form; ``AUTO`` tries the concurrent
                form and falls back to the blocking one.
            expected_oid: The identity the decision to detach was made about.

        Raises:
            PlanStaleError: If the relation holding the name is not the one
                ``expected_oid`` identifies, or is not attached to
                ``table_name`` any more.
        """
        if await self._finalize_if_pending(table_name, partition_name, expected_oid):
            return

        if mode is not DetachMode.BLOCKING and await self._detach_concurrently(
            table_name, partition_name, expected_oid, fallback=mode is DetachMode.AUTO
        ):
            return

        stmt = build_ddl_statement(
            "ALTER TABLE {parent} DETACH PARTITION {partition}",
            parent=table_name,
            partition=partition_name,
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            try:
                await self._lock_partition(conn, partition_name)
                await self._ensure_still_the_partition(conn, table_name, partition_name, expected_oid)
                await self._mark_orphaned(conn, table_name, partition_name)
                await conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                domain_exc = self._translate_detach_error(exc, partition_name)
                if domain_exc is not None:
                    raise domain_exc from exc
                raise

    async def _lock_partition(self, conn: AsyncConnection, partition_name: str) -> None:
        """ACCESS EXCLUSIVE on the partition -- what the blocking DETACH takes anyway, taken first.

        Under it the identity check, the marker and the statement see one
        relation. A foreign table cannot be locked and is checked unlocked.
        """
        if await relation_kind(conn, partition_name) == "f":
            return
        await conn.execute(
            build_ddl_statement("LOCK TABLE {partition} IN ACCESS EXCLUSIVE MODE", partition=partition_name)
        )

    async def _ensure_still_the_partition(
        self, conn: AsyncConnection, table_name: str, partition_name: str, expected_oid: int | None
    ) -> None:
        """The relation is the one decided about and is attached to ``table_name``."""
        if expected_oid is None:
            return
        await self._ensure_expected_oid(conn, partition_name, expected_oid)
        if not await self._resolver.is_attached_conn(conn, table_name, partition_name):
            raise PlanStaleError(partition_name, f"it is no longer attached to {table_name}")

    async def adopt(self, table_name: str, partition_name: str) -> bool:
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
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            if not await self._resolver.exists_conn(conn, partition_name):
                return False
            await self._ensure_not_attached(conn, partition_name)
            await self._mark_orphaned(conn, table_name, partition_name, stamp_now=False)
            return True

    async def _finalize_if_pending(self, table_name: str, partition_name: str, expected_oid: int | None) -> bool:
        """Complete a detach left pending by a cancelled DETACH CONCURRENTLY.

        Returns True if the partition was in pending-detach state and has been
        finalized (i.e. it is now fully detached).
        """
        async with asyncio.timeout(self._ddl_timeout), self._engine.connect() as base_conn:
            conn = await base_conn.execution_options(isolation_level="AUTOCOMMIT")
            result = await conn.execute(
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
            await self._ensure_expected_oid(conn, partition_name, expected_oid)
            await self._mark_orphaned(conn, table_name, partition_name)
            await conn.execute(
                build_ddl_statement(
                    "ALTER TABLE {parent} DETACH PARTITION {partition} FINALIZE",
                    parent=table_name,
                    partition=partition_name,
                )
            )
            return True

    async def _detach_concurrently(
        self, table_name: str, partition_name: str, expected_oid: int | None, *, fallback: bool
    ) -> bool:
        """Run ``DETACH … CONCURRENTLY``; False when the blocking form should run instead.

        The statement cannot run inside a transaction block, so it goes out on
        an AUTOCOMMIT connection. Identity is checked right before the marker
        and again after the statement: the name cannot be pinned across an
        autocommit statement, so a relation swapped in between is reported
        rather than silently detached in place of the planned one.
        """
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} DETACH PARTITION {partition} CONCURRENTLY",
            parent=table_name,
            partition=partition_name,
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.connect() as base_conn:
            conn = await base_conn.execution_options(isolation_level="AUTOCOMMIT")
            try:
                await self._ensure_still_the_partition(conn, table_name, partition_name, expected_oid)
                await self._mark_orphaned(conn, table_name, partition_name)
                await conn.execute(stmt)
                await self._ensure_expected_oid(conn, partition_name, expected_oid)
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

    async def _mark_orphaned(
        self,
        conn: AsyncConnection,
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
        parent_fqn = await self._resolver.resolve_fqn_conn(conn, table_name) or table_name

        comment_result = await conn.execute(
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
        relation = "FOREIGN TABLE" if await _relkind(conn, partition_name) == "f" else "TABLE"
        await conn.execute(
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

    async def drop(
        self, partition_name: str, *, expected_oid: int | None = None, drain_into: str | None = None
    ) -> None:
        """Drop a detached partition.

        Args:
            partition_name: The table to drop.
            expected_oid: The identity the decision to drop was made about.
                A relation that has since been dropped and recreated under the
                same name carries another OID and is left alone.
            drain_into: Move whatever rows the table still holds into this
                relation in the transaction that drops it. Under the lock the
                drop takes, so nothing can slip in between the move and the
                drop -- what ``unpartition`` needs to promise that no row is
                lost.

        Raises:
            PlanStaleError: If the relation holding the name is not the one
                ``expected_oid`` identifies.
            RowMoveRefusedError: If ``drain_into`` is given and a foreign key's
                ``ON DELETE`` action would fire on the remaining rows; the
                table is left as it is.
        """
        async with asyncio.timeout(self._ddl_timeout), self._engine.connect() as conn:
            if not await self._resolver.exists_conn(conn, partition_name):
                return
            await self._ensure_expected_oid(conn, partition_name, expected_oid)
            await self._ensure_not_attached(conn, partition_name)
            if not await self._ensure_managed(conn, partition_name):
                return

        last_exc: BaseException | None = None

        for attempt in range(self._drop_max_retries):
            if attempt > 0:
                await self._handle_retry_delay(attempt, last_exc, partition_name)

            try:
                await self._execute_drop(partition_name, expected_oid, drain_into)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if isinstance(exc, SQLAlchemyError) and pg_sqlstate(exc) not in _RETRYABLE_PG_STATES:
                    raise
                last_exc = exc
            else:
                return

        raise DropRetryExhaustedError(partition_name, self._drop_max_retries, last_exc) from last_exc

    async def _execute_drop(self, partition_name: str, expected_oid: int | None, drain_into: str | None) -> None:
        """Lock, revalidate, and drop in one transaction.

        The pre-checks in :meth:`drop` run on a different connection, so the
        relation could have been reattached or replaced since. Taking ACCESS
        EXCLUSIVE first and revalidating under it closes that window —
        PostgreSQL happily drops even an attached partition via DROP TABLE.
        """
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            await conn.execute(
                text("SELECT set_config('lock_timeout', :timeout, true)"),
                {"timeout": str(self._drop_lock_timeout_ms)},
            )
            # A foreign table cannot be LOCKed ("not supported for foreign
            # tables"); it holds no rows of its own, and DROP takes the lock
            # it needs on the catalog entry itself.
            foreign = await _relkind(conn, partition_name) == "f"
            if not foreign:
                try:
                    await conn.execute(
                        build_ddl_statement("LOCK TABLE {partition} IN ACCESS EXCLUSIVE MODE", partition=partition_name)
                    )
                except (SQLAlchemyError, OSError, TimeoutError) as exc:
                    if pg_sqlstate(exc) == "42P01":  # dropped concurrently — nothing left to do
                        return
                    raise
            await self._ensure_expected_oid(conn, partition_name, expected_oid)
            await self._ensure_not_attached(conn, partition_name)
            if not await self._ensure_managed(conn, partition_name):
                return
            if foreign:
                # A foreign table carries no constraints of its own, and
                # ``DROP TABLE`` refuses it ("is not a table").
                await conn.execute(
                    build_ddl_statement("DROP FOREIGN TABLE IF EXISTS {partition}", partition=partition_name)
                )
                return
            if drain_into is not None:
                drained = await self._creator.move_rows_conn(conn, partition_name, drain_into)
                if drained:
                    logger.info(
                        "Moved the rows that arrived after the last batch before dropping the table",
                        extra={"partition_name": partition_name, "target": drain_into, "rows": drained},
                    )
            fk_constraints = await self._fk_manager.list_constraints_conn(conn, partition_name)
            await self._fk_manager.drop_constraints(conn, partition_name, fk_constraints)
            await conn.execute(build_ddl_statement("DROP TABLE IF EXISTS {partition}", partition=partition_name))

    async def _ensure_expected_oid(self, conn: AsyncConnection, partition_name: str, expected_oid: int | None) -> None:
        if expected_oid is None:
            return
        result = await conn.execute(
            text("SELECT c.oid FROM pg_class c WHERE c.oid = to_regclass(:name)"),
            {"name": to_regclass_argument(partition_name)},
        )
        actual = result.scalar()
        if actual is not None and int(actual) != expected_oid:
            raise PlanStaleError(
                partition_name,
                f"the relation now holding the name has OID {int(actual)}, the plan decided about OID {expected_oid}",
            )

    async def _ensure_not_attached(self, conn: AsyncConnection, partition_name: str) -> None:
        result = await conn.execute(
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

    async def _ensure_managed(self, conn: AsyncConnection, partition_name: str) -> bool:
        if self._allow_unmanaged:
            return True

        comment_result = await conn.execute(
            text("SELECT obj_description(to_regclass(:partition_name), 'pg_class')"),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        comment_str = coerce_str(comment_result.scalar())

        if comment_str is None:
            if not await self._resolver.exists_conn(conn, partition_name):
                return False
            raise UnmanagedPartitionDropError(partition_name)

        if parse_orphan_comment(comment_str, marker_prefix=self._marker_prefix) is None:
            raise UnmanagedPartitionDropError(partition_name)

        return True

    async def _handle_retry_delay(self, attempt: int, last_exc: BaseException | None, partition_name: str) -> None:
        delay = min(
            self._drop_retry_delay * (2 ** (attempt - 1)),
            self._drop_max_backoff,
        )
        await asyncio.sleep(delay)
        logger.warning(
            "Retrying drop_partition after transient error",
            extra={
                "partition_name": partition_name,
                "attempt": attempt + 1,
                "max_retries": self._drop_max_retries,
                "reason": str(last_exc),
            },
        )


async def _relkind(conn: AsyncConnection, name: str) -> str | None:
    """``pg_class.relkind`` of the relation holding ``name``, or None when there is none."""
    return await relation_kind(conn, name)
