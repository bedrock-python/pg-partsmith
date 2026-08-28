"""Helper for partition creation and attachment."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.catalog_queries import RELATION_COLUMNS_SQL
from pg_partsmith.exceptions import PartitionAlreadyExistsError, PartitionNotFoundError
from pg_partsmith.topology import HashBounds, ListBounds, RangeBounds
from pg_partsmith.utils import (
    _as_text,
    build_ddl_statement,
    coerce_str,
    pg_sqlstate,
    quote_identifier,
    quote_literal,
    to_regclass_argument,
)

from .timeouts import apply_local_statement_timeout

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine

    from pg_partsmith.plan import PartitionBy
    from pg_partsmith.topology import PartitionBounds


class PartitionCreator:
    """Helper for partition creation and attachment."""

    def __init__(self, *, engine: Engine, ddl_timeout: float, ddl_timezone: str | None) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout
        self._ddl_timezone = ddl_timezone

    def create_table_like(self, template_name: str, table_name: str, partition_by: PartitionBy | None) -> None:
        """Create a detached table shaped like ``template_name``.

        Standalone — ``LIKE`` the template, not ``PARTITION OF`` it — so this
        takes only an ACCESS SHARE lock on the live template. The table becomes
        reachable for row routing only when it is attached, which the caller
        does last, once its own subtree is complete.

        Args:
            template_name: Relation whose shape is copied.
            table_name: Schema-qualified name for the new table.
            partition_by: How the new table partitions its own children, or
                None for a plain leaf.

        Raises:
            PartitionAlreadyExistsError: If a relation of that name exists.
        """
        # EXCLUDING IDENTITY: PostgreSQL refuses to attach a partition that
        # carries an identity column ("The new partition may not contain an
        # identity column"), and propagates the parent's own identity on ATTACH
        # instead. Tables without one are unaffected.
        template = "CREATE TABLE {partition} (LIKE {parent} INCLUDING ALL EXCLUDING IDENTITY)"
        params = {"partition": table_name, "parent": template_name}
        if partition_by is not None:
            clause, columns = _partition_by_clause(partition_by)
            template = f"{template} {clause}"
            params.update(columns)

        stmt = build_ddl_statement(template, **params)
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            try:
                conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(table_name) from exc
                raise

    def attach(self, parent_name: str, partition_name: str, bounds: PartitionBounds, *, key_arity: int = 1) -> None:
        """Attach a table to a partitioned parent.

        ATTACH rather than ``CREATE … PARTITION OF`` on purpose: attaching takes
        SHARE UPDATE EXCLUSIVE on the parent, so filling a gap in a live parent
        does not block reads or writes, where creating in place would take
        ACCESS EXCLUSIVE and stall every writer routing through it.

        The parent's DEFAULT partition, if it has one, is the exception: ATTACH
        takes ACCESS EXCLUSIVE on that one while scanning it for rows the new
        partition would claim. A fully tiled parent has no DEFAULT partition and
        pays nothing for it.

        A RANGE bound is written under ``SET LOCAL TIME ZONE`` so a naive
        timestamp literal means the same instant every time. Under a composite
        key only the leading column carries the window; the trailing ones are
        bounded with ``MINVALUE`` at both ends.

        Args:
            parent_name: Partitioned relation to attach to.
            partition_name: Table to attach.
            bounds: What the partition owns.
            key_arity: Number of columns in the parent's partition key.
        """
        clause, values = _values_clause(bounds, key_arity)
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} ATTACH PARTITION {partition} " + clause,
            parent=parent_name,
            partition=partition_name,
            **values,
        )
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            if isinstance(bounds, RangeBounds) and self._ddl_timezone is not None:
                conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            conn.execute(stmt)

    def _relation_columns(self, conn: Connection, table_name: str) -> tuple[str, ...]:
        """Read a relation's live column names, in its own physical order.

        Raises:
            PartitionNotFoundError: If the relation has no readable columns,
                which means it is gone rather than empty.
        """
        result = conn.execute(
            text(RELATION_COLUMNS_SQL),
            {"table_name": to_regclass_argument(table_name)},
        )
        columns = tuple(coerce_str(row[0]) or "" for row in result.fetchall())
        if not columns:
            msg = f"Relation {table_name!r} has no readable columns, so its rows cannot be moved."
            raise PartitionNotFoundError(msg)
        return columns

    def reconcile_default_rows(
        self,
        *,
        default_partition_name: str,
        target_partition_name: str,
        key_columns: tuple[str, ...],
        from_value: str,
        to_value: str,
    ) -> int:
        """Move conflicting rows from DEFAULT partition to target partition.

        Args:
            default_partition_name: Qualified name of DEFAULT partition.
            target_partition_name: Qualified name of target partition.
            key_columns: The parent's partition key, leading column first.
            from_value: Range start boundary (inclusive).
            to_value: Range end boundary (exclusive).

        Returns:
            Number of rows moved.
        """
        if not key_columns:
            msg = "reconcile_default_rows needs the parent's partition key"
            raise ValueError(msg)

        default_quoted = quote_identifier(default_partition_name)
        target_quoted = quote_identifier(target_partition_name)
        column_quoted = quote_identifier(key_columns[0])
        from_quoted = quote_literal(from_value)
        to_quoted = quote_literal(to_value)

        # PostgreSQL adds an IS NOT NULL test for *every* key column to a range
        # partition's constraint, so a row with a NULL trailing key value
        # belongs in DEFAULT whatever its leading value is. Moving it out would
        # be rejected -- and the rejection would look exactly like the DEFAULT
        # conflict this call exists to clear, so the retry would never converge.
        not_null = "".join(f" AND {quote_identifier(column)} IS NOT NULL" for column in key_columns[1:])

        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            # Boundary literals must be interpreted in the same timezone ATTACH uses,
            # otherwise a non-UTC server timezone moves the wrong row range.
            if self._ddl_timezone is not None:
                conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            # Lock both tables to minimize race conditions
            conn.execute(text(f"LOCK TABLE {default_quoted} IN SHARE ROW EXCLUSIVE MODE"))
            conn.execute(text(f"LOCK TABLE {target_quoted} IN SHARE ROW EXCLUSIVE MODE"))

            columns = self._relation_columns(conn, default_partition_name)
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

            result = conn.execute(move_sql)
            moved_count = result.rowcount or 0

        return moved_count


def _partition_by_clause(partition_by: PartitionBy) -> tuple[str, dict[str, str]]:
    """Render the ``PARTITION BY`` clause, plus the identifiers it needs.

    The method comes from a closed enum and each column is substituted by
    :func:`~pg_partsmith.utils.build_ddl_statement` as a quoted identifier, so
    no part of the clause is unescaped caller text.
    """
    columns = {f"key_{index}": column for index, column in enumerate(partition_by.columns)}
    rendered = ", ".join(f"{{{name}}}" for name in columns)
    return f"PARTITION BY {partition_by.method.value.upper()} ({rendered})", columns


def _values_clause(bounds: PartitionBounds, key_arity: int) -> tuple[str, dict[str, str]]:
    """Render the bound clause for a partition, plus the literals it needs.

    Hash moduli are validated integers on a frozen model, so formatting them in
    cannot inject anything — and PostgreSQL requires literals there anyway.
    RANGE and LIST values are caller data, so they go back through
    ``build_ddl_statement`` as ``[placeholder]`` literals rather than into the
    template: a value containing a brace would otherwise be re-interpreted when
    the template is formatted.
    """
    if isinstance(bounds, RangeBounds):
        padding = ", MINVALUE" * max(0, key_arity - 1)
        from_part = "MINVALUE" if bounds.from_value.upper() == "MINVALUE" else "[from_val]"
        to_part = "MAXVALUE" if bounds.to_value.upper() == "MAXVALUE" else "[to_val]"
        values = {}
        if from_part != "MINVALUE":
            values["from_val"] = bounds.from_value
        if to_part != "MAXVALUE":
            values["to_val"] = bounds.to_value
        return f"FOR VALUES FROM ({from_part}{padding}) TO ({to_part}{padding})", values

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
