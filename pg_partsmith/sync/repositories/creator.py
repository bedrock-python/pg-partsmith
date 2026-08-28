"""Helper for partition creation and attachment."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.catalog_queries import (
    RELATION_COLUMN_DEFINITIONS_SQL,
    RELATION_COLUMNS_SQL,
    RELATION_PRIVILEGES_SQL,
)
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

    from pg_partsmith.leaves import LocalLeaves
    from pg_partsmith.plan import PartitionBy
    from pg_partsmith.topology import PartitionBounds

# Privilege names ``aclexplode`` reports; anything else is not spliced into a GRANT.
_PRIVILEGE_PATTERN = re.compile(r"^[A-Z]+$")


class PartitionCreator:
    """Helper for partition creation and attachment."""

    def __init__(self, *, engine: Engine, ddl_timeout: float, ddl_timezone: str | None) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout
        self._ddl_timezone = ddl_timezone

    def create_table_like(
        self,
        template_name: str,
        table_name: str,
        partition_by: PartitionBy | None,
        *,
        physical: LocalLeaves | None = None,
    ) -> None:
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
            physical: Tablespace, storage parameters and privileges to give
                the new table. Storage parameters apply to leaves only:
                PostgreSQL refuses them on a partitioned table.

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
        if physical is not None:
            if partition_by is None and physical.storage_parameters:
                clause, values = _storage_parameters_clause(physical)
                template = f"{template} {clause}"
                params.update(values)
            if physical.tablespace is not None:
                template = f"{template} TABLESPACE {{tablespace}}"
                params["tablespace"] = physical.tablespace

        stmt = build_ddl_statement(template, **params)
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            try:
                conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(table_name) from exc
                raise
            if physical is not None and physical.inherit_privileges:
                self._inherit_privileges(conn, template_name, table_name)

    def create_foreign_table_like(
        self,
        template_name: str,
        table_name: str,
        *,
        server: str,
        options: dict[str, str],
    ) -> None:
        """Create a detached foreign table with ``template_name``'s columns.

        ``CREATE FOREIGN TABLE`` has no ``LIKE``, so the columns are read from
        the catalog and spelled out: name, type as ``format_type`` renders it,
        and ``NOT NULL`` where the template has it -- ATTACH requires the
        partition to match the parent's ``NOT NULL`` constraints.

        Args:
            template_name: Relation whose columns are copied.
            table_name: Schema-qualified name for the new foreign table.
            server: The foreign server.
            options: Foreign table options, already rendered.

        Raises:
            PartitionAlreadyExistsError: If a relation of that name exists.
            PartitionNotFoundError: If the template has no readable columns.
        """
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            definitions = self._column_definitions(conn, template_name)
            params: dict[str, str] = {"partition": table_name, "server": server}
            columns: list[str] = []
            for index, (name, type_name, not_null) in enumerate(definitions):
                params[f"col_{index}"] = name
                columns.append(f"{{col_{index}}} {type_name}{' NOT NULL' if not_null else ''}")
            template = f"CREATE FOREIGN TABLE {{partition}} ({', '.join(columns)}) SERVER {{server}}"
            if options:
                rendered: list[str] = []
                for index, (name, value) in enumerate(options.items()):
                    params[f"opt_{index}"] = value
                    rendered.append(f"{name} [opt_{index}]")
                template = f"{template} OPTIONS ({', '.join(rendered)})"
            try:
                conn.execute(build_ddl_statement(template, **params))
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(table_name) from exc
                raise

    def _column_definitions(self, conn: Connection, table_name: str) -> list[tuple[str, str, bool]]:
        """Read ``(name, type, not_null)`` for every live column of a relation."""
        result = conn.execute(
            text(RELATION_COLUMN_DEFINITIONS_SQL),
            {"table_name": to_regclass_argument(table_name)},
        )
        definitions = [(coerce_str(row[0]) or "", coerce_str(row[1]) or "", bool(row[2])) for row in result.fetchall()]
        if not definitions:
            msg = f"Relation {table_name!r} has no readable columns, so nothing can be shaped like it."
            raise PartitionNotFoundError(msg)
        return definitions

    def _inherit_privileges(self, conn: Connection, template_name: str, table_name: str) -> None:
        """Give the new relation the template's owner and grants.

        Runs in the transaction that created the relation, so a grant the
        current role may not make rolls the creation back with it rather than
        leaving a half-configured table behind.
        """
        result = conn.execute(
            text(RELATION_PRIVILEGES_SQL),
            {"table_name": to_regclass_argument(template_name)},
        )
        rows = result.fetchall()
        if not rows:
            return
        owner = coerce_str(rows[0][0])
        if owner:
            conn.execute(
                build_ddl_statement("ALTER TABLE {partition} OWNER TO {owner}", partition=table_name, owner=owner)
            )

        grants: dict[tuple[str, bool], list[str]] = {}
        for row in rows:
            grantee, privilege, grantable = coerce_str(row[1]), coerce_str(row[2]), bool(row[3])
            if not grantee or not privilege or not _PRIVILEGE_PATTERN.match(privilege):
                continue
            grants.setdefault((grantee, grantable), []).append(privilege)
        for (grantee, grantable), privileges in grants.items():
            # The grantee is catalog output (``regrole::text`` quotes what needs
            # quoting; PUBLIC is a keyword) and the privileges are keywords, so
            # neither goes through identifier quoting.
            statement = f"GRANT {', '.join(privileges)} ON TABLE {quote_identifier(table_name)} TO {grantee}"
            if grantable:
                statement = f"{statement} WITH GRANT OPTION"
            conn.execute(_as_text(statement))

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
        limit: int | None = None,
    ) -> int:
        """Move conflicting rows from DEFAULT partition to target partition.

        Args:
            default_partition_name: Qualified name of DEFAULT partition.
            target_partition_name: Qualified name of target partition.
            key_columns: The parent's partition key, leading column first.
            from_value: Range start boundary (inclusive).
            to_value: Range end boundary (exclusive).
            limit: Move at most this many rows; None moves every row of the
                window in one statement.

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
            condition = f"{column_quoted} >= {from_quoted} AND {column_quoted} < {to_quoted}{not_null}"
            move_sql = _as_text(_move_statement(default_quoted, target_quoted, column_list, condition, limit))

            result = conn.execute(move_sql)
            moved_count = result.rowcount or 0

        return moved_count

    def move_rows(self, source_name: str, target_name: str, *, limit: int | None = None) -> int:
        """Move rows from one relation into another, whatever their keys.

        Args:
            source_name: Qualified name of the relation to take rows from; a
                partitioned table works, its leaves are addressed through it.
            target_name: Qualified name of the relation to put them in.
            limit: Move at most this many rows; None moves every row.

        Returns:
            Number of rows moved.
        """
        source_quoted = quote_identifier(source_name)
        target_quoted = quote_identifier(target_name)
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            conn.execute(text(f"LOCK TABLE {source_quoted} IN SHARE ROW EXCLUSIVE MODE"))
            conn.execute(text(f"LOCK TABLE {target_quoted} IN SHARE ROW EXCLUSIVE MODE"))
            columns = self._relation_columns(conn, source_name)
            column_list = ", ".join(quote_identifier(column) for column in columns)
            result = conn.execute(_as_text(_move_statement(source_quoted, target_quoted, column_list, None, limit)))
            return result.rowcount or 0


def _move_statement(source: str, target: str, column_list: str, condition: str | None, limit: int | None) -> str:
    """One ``DELETE ... RETURNING`` / ``INSERT`` statement moving rows from ``source`` to ``target``.

    Both sides name their columns: ``RETURNING *`` would emit the source's
    physical order, and an unqualified INSERT binds it positionally -- silently
    transposing values whenever the two relations were created independently.
    A batch is bounded through ``(tableoid, ctid)`` rather than ``ctid`` alone
    so a partitioned source, whose leaves each number their own tuples, is
    addressed unambiguously. Every identifier and literal is already quoted.
    """
    where = f" WHERE {condition}" if condition else ""
    if limit is not None:
        picked = f"SELECT tableoid, ctid FROM {source}{where} LIMIT {int(limit):d}"  # noqa: S608
        where = f" WHERE (tableoid, ctid) IN ({picked})"
    return (
        f"WITH moved AS ("  # noqa: S608
        f"DELETE FROM {source}{where} RETURNING {column_list}"
        f") "
        f"INSERT INTO {target} ({column_list}) "
        f"SELECT {column_list} FROM moved"
    )


def _storage_parameters_clause(physical: LocalLeaves) -> tuple[str, dict[str, str]]:
    """Render ``WITH (...)``, every value as a literal placeholder.

    PostgreSQL accepts a quoted literal for every storage parameter type, so
    numbers and booleans are spelled as strings and quoted like the rest; the
    names were validated by the model and are spliced as they are.
    """
    values: dict[str, str] = {}
    rendered: list[str] = []
    for index, (name, value) in enumerate(physical.rendered_storage_parameters().items()):
        values[f"with_{index}"] = value
        rendered.append(f"{name} = [with_{index}]")
    return f"WITH ({', '.join(rendered)})", values


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
