"""Helper for partition creation and attachment."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.catalog_queries import RELATION_COLUMNS_SQL
from pg_partsmith.entities import HashBounds, ListBounds, PartitionInfo
from pg_partsmith.exceptions import PartitionAlreadyExistsError, PartitionNotFoundError
from pg_partsmith.utils import (
    _as_text,
    build_ddl_statement,
    coerce_str,
    pg_sqlstate,
    qualify,
    quote_identifier,
    quote_literal,
    to_regclass_argument,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from pg_partsmith.entities import SubpartitionBounds, SubpartitionSpec, TablePartitionConfig


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

    async def attach_composite_partition(
        self,
        table_name: str,
        partition_name: str,
        from_value: str,
        to_value: str,
        *,
        key_arity: int,
    ) -> None:
        """Attach a partition to a parent whose partition key has several columns.

        Only the leading column carries the period; the trailing ones are
        bounded with MINVALUE at both ends, so the partition holds the rows
        whose leading column falls in ``[from_value, to_value)``.

        Not *all* of them: PostgreSQL adds an IS NOT NULL test for every key
        column to the constraint it derives from this bound, so a row with a
        NULL in any trailing column goes to DEFAULT regardless of its leading
        value. Declare the trailing columns NOT NULL if you need every row of a
        period in that period's partition.

        Args:
            table_name: Parent table name.
            partition_name: Partition table name.
            from_value: Start boundary for the leading column.
            to_value: End boundary for the leading column.
            key_arity: Number of columns in the parent's partition key.
        """
        padding = ", MINVALUE" * (key_arity - 1)
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} ATTACH PARTITION {partition} "
            f"FOR VALUES FROM ([from_val]{padding}) TO ([to_val]{padding})",
            parent=table_name,
            partition=partition_name,
            from_val=from_value,
            to_val=to_value,
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            if self._ddl_timezone is not None:
                await conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            await conn.execute(stmt)

    async def _relation_columns(self, conn: AsyncConnection, table_name: str) -> tuple[str, ...]:
        """Read a relation's live column names, in its own physical order.

        Args:
            conn: Connection already inside the caller's transaction, so the
                shape read here is the shape the move will run against.
            table_name: Relation to inspect, schema-qualified.

        Returns:
            The column names, dropped columns excluded.

        Raises:
            PartitionNotFoundError: If the relation has no readable columns,
                which means it is gone rather than empty.
        """
        result = await conn.execute(
            text(RELATION_COLUMNS_SQL),
            {"table_name": to_regclass_argument(table_name)},
        )
        columns = tuple(coerce_str(row[0]) or "" for row in result.fetchall())
        if not columns:
            msg = f"Relation {table_name!r} has no readable columns, so its rows cannot be moved."
            raise PartitionNotFoundError(msg)
        return columns

    async def reconcile_default_rows(
        self,
        *,
        default_partition_name: str,
        target_partition_name: str,
        partition_column: str,
        trailing_columns: tuple[str, ...] = (),
        from_value: str,
        to_value: str,
    ) -> int:
        """Move conflicting rows from DEFAULT partition to target partition.

        Args:
            default_partition_name: Qualified name of DEFAULT partition.
            target_partition_name: Qualified name of target partition.
            partition_column: Leading column of the partition key.
            trailing_columns: The remaining key columns, for a composite key.
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

        # PostgreSQL adds an IS NOT NULL test for *every* key column to a range
        # partition's constraint, so a row with a NULL trailing key value
        # belongs in DEFAULT whatever its leading value is. Moving it out would
        # be rejected -- and the rejection would look exactly like the DEFAULT
        # conflict this call exists to clear, so the retry would never converge.
        not_null = "".join(f" AND {quote_identifier(column)} IS NOT NULL" for column in trailing_columns)

        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            # Boundary literals must be interpreted in the same timezone ATTACH uses,
            # otherwise a non-UTC server timezone moves the wrong row range.
            if self._ddl_timezone is not None:
                await conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            # Lock both tables to minimize race conditions
            await conn.execute(text(f"LOCK TABLE {default_quoted} IN SHARE ROW EXCLUSIVE MODE"))
            await conn.execute(text(f"LOCK TABLE {target_quoted} IN SHARE ROW EXCLUSIVE MODE"))

            columns = await self._relation_columns(conn, default_partition_name)
            column_list = ", ".join(quote_identifier(column) for column in columns)

            # Both sides name their columns. ``RETURNING *`` emits the DEFAULT
            # partition's own physical order, and an unqualified INSERT binds
            # that order positionally to the target's -- silently transposing
            # values whenever the two differ. They can differ: ATTACH PARTITION
            # matches columns by name, so a DEFAULT partition created
            # independently and attached need not share the order of one
            # created with LIKE.
            #
            # All identifiers and literals are properly quoted, S608 is a false positive
            move_sql = _as_text(
                f"WITH moved AS ("  # noqa: S608
                f"DELETE FROM {default_quoted} "
                f"WHERE {column_quoted} >= {from_quoted} "
                f"AND {column_quoted} < {to_quoted}"
                f"{not_null} "
                f"RETURNING {column_list}"
                f") "
                f"INSERT INTO {target_quoted} ({column_list}) "
                f"SELECT {column_list} FROM moved"
            )

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
        clause, columns = _partition_by_clause(spec)
        stmt = build_ddl_statement(
            "CREATE TABLE {partition} (LIKE {parent} INCLUDING ALL EXCLUDING IDENTITY) " + clause,
            partition=branch_name,
            parent=qualify(config.db_schema, config.table_name),
            **columns,
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
            clause, columns = _partition_by_clause(spec)
            template = f"{template} {clause}"
            params.update(columns)

        stmt = build_ddl_statement(template, **params)
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            try:
                await conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(child_name) from exc
                raise

    async def attach_subpartition(self, parent_name: str, child_name: str, bounds: SubpartitionBounds) -> None:
        """Attach a hash bucket to its parent.

        ATTACH rather than ``CREATE … PARTITION OF`` on purpose: attaching takes
        SHARE UPDATE EXCLUSIVE on the parent, so filling a gap in a live branch
        does not block reads or writes, where creating in place would take
        ACCESS EXCLUSIVE and stall every writer routing through it.

        The parent's DEFAULT partition, if it has one, is the exception: ATTACH
        takes ACCESS EXCLUSIVE on that one while scanning it for rows the new
        partition would claim. A fully tiled parent has no DEFAULT partition and
        pays nothing for it.

        Args:
            parent_name: Partitioned relation to attach to.
            child_name: Table to attach.
            bounds: What the child owns — a hash bucket, a set of LIST values,
                or DEFAULT.
        """
        clause, values = _values_clause(bounds)
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} ATTACH PARTITION {partition} " + clause,
            parent=parent_name,
            partition=child_name,
            **values,
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            await conn.execute(stmt)


def _partition_by_clause(spec: SubpartitionSpec) -> tuple[str, dict[str, str]]:
    """Render the ``PARTITION BY`` clause for a spec, plus the identifiers it needs.

    The strategy comes from a closed enum and each column is substituted by
    :func:`~pg_partsmith.utils.build_ddl_statement` as a quoted identifier, so
    no part of the clause is unescaped caller text.

    Returns:
        The clause and the identifier parameters it references.
    """
    columns = {f"key_{index}": column for index, column in enumerate(spec.columns)}
    rendered = ", ".join(f"{{{name}}}" for name in columns)
    return f"PARTITION BY {spec.partition_type.value.upper()} ({rendered})", columns


def _values_clause(bounds: SubpartitionBounds) -> tuple[str, dict[str, str]]:
    """Render the bound clause for a subpartition, plus the literals it needs.

    Hash moduli are validated integers on a frozen model, so formatting them in
    cannot inject anything — and PostgreSQL requires literals there anyway. LIST
    values are caller data, so they go back through ``build_ddl_statement`` as
    ``[placeholder]`` literals rather than into the template: a value containing
    a brace would otherwise be re-interpreted when the template is formatted.

    Returns:
        The clause and the literal parameters it references.
    """
    if isinstance(bounds, HashBounds):
        return f"FOR VALUES WITH (MODULUS {bounds.modulus:d}, REMAINDER {bounds.remainder:d})", {}

    if isinstance(bounds, ListBounds):
        values = {f"value_{index}": value for index, value in enumerate(bounds.values)}
        elements = [f"[{name}]" for name in values]
        if bounds.includes_null:
            # NULL is a keyword here, not a value: quoting it would produce a
            # partition for the three-character string instead.
            elements.append("NULL")
        return f"FOR VALUES IN ({', '.join(elements)})", values

    return "DEFAULT", {}
