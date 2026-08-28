"""Helper for partition creation and attachment."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.entities import HashBounds, PartitionInfo
from pg_partsmith.exceptions import PartitionAlreadyExistsError
from pg_partsmith.utils import build_ddl_statement, pg_sqlstate, qualify, quote_identifier, quote_literal

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from pg_partsmith.entities import SubpartitionSpec, TablePartitionConfig


class PartitionCreator:
    """Helper for partition creation and attachment."""

    def __init__(self, *, engine: AsyncEngine, ddl_timeout: float, ddl_timezone: str | None) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout
        self._ddl_timezone = ddl_timezone

    async def create(
        self, config: TablePartitionConfig, partition_name: str, from_value: str, to_value: str
    ) -> PartitionInfo:
        """Create a new partition table."""
        # EXCLUDING IDENTITY: PostgreSQL refuses to attach a partition that
        # carries an identity column ("The new partition may not contain an
        # identity column"), and propagates the parent's own identity on ATTACH
        # instead. Tables without one are unaffected.
        stmt = build_ddl_statement(
            "CREATE TABLE {partition} (LIKE {parent} INCLUDING ALL EXCLUDING IDENTITY)",
            partition=partition_name,
            parent=qualify(config.db_schema, config.table_name),
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            try:
                await conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(partition_name) from exc
                raise

        return PartitionInfo(
            name=partition_name,
            partition_type=config.partition_type,
            from_value=from_value,
            to_value=to_value,
            is_attached=False,
            parent_table=qualify(config.db_schema, config.table_name),
        )

    async def attach(self, table_name: str, partition_name: str, from_value: str, to_value: str) -> None:
        """Attach partition to parent table."""
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} ATTACH PARTITION {partition} FOR VALUES FROM ([from_val]) TO ([to_val])",
            parent=table_name,
            partition=partition_name,
            from_val=from_value,
            to_val=to_value,
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            if self._ddl_timezone is not None:
                await conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            await conn.execute(stmt)

    async def reconcile_default_rows(
        self,
        *,
        default_partition_name: str,
        target_partition_name: str,
        partition_column: str,
        from_value: str,
        to_value: str,
    ) -> int:
        """Move conflicting rows from DEFAULT partition to target partition.

        Args:
            default_partition_name: Qualified name of DEFAULT partition.
            target_partition_name: Qualified name of target partition.
            partition_column: Column used for partitioning.
            from_value: Range start boundary (inclusive).
            to_value: Range end boundary (exclusive).

        Returns:
            Number of rows moved.
        """
        default_quoted = quote_identifier(default_partition_name)
        target_quoted = quote_identifier(target_partition_name)
        column_quoted = quote_identifier(partition_column)
        from_quoted = quote_literal(from_value)
        to_quoted = quote_literal(to_value)

        # All identifiers and literals are properly quoted above, S608 is a false positive
        move_sql = text(
            f"WITH moved AS ("  # noqa: S608
            f"DELETE FROM {default_quoted} "
            f"WHERE {column_quoted} >= {from_quoted} "
            f"AND {column_quoted} < {to_quoted} "
            f"RETURNING *"
            f") "
            f"INSERT INTO {target_quoted} "
            f"SELECT * FROM moved"
        )

        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            # Boundary literals must be interpreted in the same timezone ATTACH uses,
            # otherwise a non-UTC server timezone moves the wrong row range.
            if self._ddl_timezone is not None:
                await conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            # Lock both tables to minimize race conditions
            await conn.execute(text(f"LOCK TABLE {default_quoted} IN SHARE ROW EXCLUSIVE MODE"))
            await conn.execute(text(f"LOCK TABLE {target_quoted} IN SHARE ROW EXCLUSIVE MODE"))

            result = await conn.execute(move_sql)
            moved_count = result.rowcount or 0

        return moved_count

    async def create_branch(
        self,
        config: TablePartitionConfig,
        branch_name: str,
        from_value: str,
        to_value: str,
        spec: SubpartitionSpec,
    ) -> PartitionInfo:
        """Create a time partition that is itself a partitioned table.

        The branch is created standalone — ``LIKE`` the parent, not
        ``PARTITION OF`` it — so this takes only an ACCESS SHARE lock on the
        live root table. Its buckets are built while it is still detached, and
        only the final ATTACH makes the completed subtree reachable for row
        routing. A branch is therefore never visible to writers in a state
        where part of its keyspace has nowhere to go.

        Args:
            config: Table partition configuration.
            branch_name: Name for the new branch table.
            from_value: Start boundary value.
            to_value: End boundary value.
            spec: Subpartitioning the branch itself applies to its children.

        Returns:
            Info about the created (still detached) branch.

        Raises:
            PartitionAlreadyExistsError: If a relation of that name exists.
        """
        stmt = build_ddl_statement(
            "CREATE TABLE {partition} (LIKE {parent} INCLUDING ALL EXCLUDING IDENTITY) " + _partition_by_clause(spec),
            partition=branch_name,
            parent=qualify(config.db_schema, config.table_name),
            column=spec.column,
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            try:
                await conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(branch_name) from exc
                raise

        return PartitionInfo(
            name=branch_name,
            partition_type=config.partition_type,
            from_value=from_value,
            to_value=to_value,
            is_attached=False,
            subpartition_type=spec.partition_type,
            parent_table=qualify(config.db_schema, config.table_name),
        )

    async def create_subpartition_table(self, parent_name: str, child_name: str, spec: SubpartitionSpec | None) -> None:
        """Create a detached table shaped like ``parent_name``.

        Args:
            parent_name: Relation the table will later be attached to.
            child_name: Name for the new table.
            spec: Subpartitioning this table applies to its own children, or
                None to create a leaf.

        Raises:
            PartitionAlreadyExistsError: If a relation of that name exists.
        """
        template = "CREATE TABLE {partition} (LIKE {parent} INCLUDING ALL EXCLUDING IDENTITY)"
        params = {"partition": child_name, "parent": parent_name}
        if spec is not None:
            template = f"{template} {_partition_by_clause(spec)}"
            params["column"] = spec.column

        stmt = build_ddl_statement(template, **params)
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            try:
                await conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(child_name) from exc
                raise

    async def attach_subpartition(self, parent_name: str, child_name: str, bounds: HashBounds) -> None:
        """Attach a hash bucket to its parent.

        ATTACH rather than ``CREATE … PARTITION OF`` on purpose: attaching takes
        SHARE UPDATE EXCLUSIVE on the parent, so filling a gap in a live branch
        does not block reads or writes, where creating in place would take
        ACCESS EXCLUSIVE and stall every writer routing through it.

        Args:
            parent_name: Partitioned relation to attach to.
            child_name: Table to attach.
            bounds: Modulus and remainder the bucket owns.
        """
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} ATTACH PARTITION {partition} " + _hash_values_clause(bounds),
            parent=parent_name,
            partition=child_name,
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            await conn.execute(stmt)


def _partition_by_clause(spec: SubpartitionSpec) -> str:
    """Render the ``PARTITION BY`` clause for a spec.

    The strategy comes from a closed enum and ``{column}`` is substituted by
    :func:`~pg_partsmith.utils.build_ddl_statement` as a quoted identifier, so
    neither half of the clause is unescaped caller text.
    """
    return f"PARTITION BY {spec.partition_type.value.upper()} ({{column}})"


def _hash_values_clause(bounds: HashBounds) -> str:
    """Render ``FOR VALUES WITH (MODULUS …, REMAINDER …)``.

    Both numbers are validated integers on a frozen model, so formatting them
    into the statement cannot inject anything; they also cannot be bound as
    parameters, since PostgreSQL requires literals here.
    """
    return f"FOR VALUES WITH (MODULUS {bounds.modulus:d}, REMAINDER {bounds.remainder:d})"
