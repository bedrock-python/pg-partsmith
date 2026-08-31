"""Helper for partition creation and attachment."""

from __future__ import annotations

import asyncio
import re
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.catalog_queries import (
    DESTRUCTIVE_INCOMING_FOREIGN_KEYS_SQL,
    RELATION_COLUMN_DEFINITIONS_SQL,
    RELATION_COLUMNS_SQL,
    RELATION_HAS_IDENTITY_ALWAYS_SQL,
    RELATION_IDENTITY_COLUMNS_SQL,
    RELATION_PRIVILEGES_SQL,
)
from pg_partsmith.exceptions import (
    PartitionAlreadyExistsError,
    PartitionNotFoundError,
    PlanStaleError,
    RowMoveRefusedError,
)
from pg_partsmith.topology import HashBounds, ListBounds, RangeBounds
from pg_partsmith.utils import (
    _as_text,
    build_ddl_statement,
    coerce_str,
    comment_without_markers,
    parse_orphan_comment,
    pg_sqlstate,
    quote_identifier,
    quote_literal,
    to_regclass_argument,
)

from .resolver import relation_kind

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from pg_partsmith.leaves import LocalLeaves
    from pg_partsmith.plan import PartitionBy
    from pg_partsmith.topology import PartitionBounds

# Privilege names ``aclexplode`` reports; anything else is not spliced into a GRANT.
_PRIVILEGE_PATTERN = re.compile(r"^[A-Z]+$")

# ``pg_constraint.confdeltype`` codes of the actions a row move must not trigger.
_ON_DELETE_ACTIONS = {"c": "CASCADE", "n": "SET NULL", "d": "SET DEFAULT"}


class PartitionCreator:
    """Helper for partition creation, attachment and row movement."""

    def __init__(
        self, *, engine: AsyncEngine, ddl_timeout: float, ddl_timezone: str | None, marker_prefix: str | None = None
    ) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout
        self._ddl_timezone = ddl_timezone
        self._marker_prefix = marker_prefix

    async def create_table_like(
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
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            try:
                await conn.execute(stmt)
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(table_name) from exc
                raise
            if physical is not None and physical.inherit_privileges:
                await self._inherit_privileges(conn, template_name, table_name)

    async def create_foreign_table_like(
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
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            definitions = await self._column_definitions(conn, template_name)
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
                await conn.execute(build_ddl_statement(template, **params))
            except (SQLAlchemyError, OSError, TimeoutError) as exc:
                if pg_sqlstate(exc) == "42P07":  # duplicate_table
                    raise PartitionAlreadyExistsError(table_name) from exc
                raise

    async def _column_definitions(self, conn: AsyncConnection, table_name: str) -> list[tuple[str, str, bool]]:
        """Read ``(name, type, not_null)`` for every live column of a relation."""
        result = await conn.execute(
            text(RELATION_COLUMN_DEFINITIONS_SQL),
            {"table_name": to_regclass_argument(table_name)},
        )
        definitions = [(coerce_str(row[0]) or "", coerce_str(row[1]) or "", bool(row[2])) for row in result.fetchall()]
        if not definitions:
            msg = f"Relation {table_name!r} has no readable columns, so nothing can be shaped like it."
            raise PartitionNotFoundError(msg)
        return definitions

    async def _inherit_privileges(self, conn: AsyncConnection, template_name: str, table_name: str) -> None:
        """Give the new relation the template's owner and grants.

        Runs in the transaction that created the relation, so a grant the
        current role may not make rolls the creation back with it rather than
        leaving a half-configured table behind.
        """
        result = await conn.execute(
            text(RELATION_PRIVILEGES_SQL),
            {"table_name": to_regclass_argument(template_name)},
        )
        rows = result.fetchall()
        if not rows:
            return
        owner = coerce_str(rows[0][0])
        if owner:
            await conn.execute(
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
            await conn.execute(_as_text(statement))

    async def attach(
        self,
        parent_name: str,
        partition_name: str,
        bounds: PartitionBounds,
        *,
        key_arity: int = 1,
        expected_oid: int | None = None,
    ) -> None:
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

        A table that carries the orphan marker -- one this library detached
        and is now bringing back -- loses it in the same transaction: an
        attached partition is nobody's orphan, and a marker left on it would
        hand its old detach instant to the next detach, cutting that grace
        period short. The rest of the comment is kept.

        Args:
            parent_name: Partitioned relation to attach to.
            partition_name: Table to attach.
            bounds: What the partition owns.
            key_arity: Number of columns in the parent's partition key.
            expected_oid: The identity the decision to attach was made about.
                Checked after the ATTACH, under the ACCESS EXCLUSIVE lock it
                took, in the same transaction -- a relation swapped in under
                the name rolls the whole attach back (``PlanStaleError``)
                instead of going live in the planned one's stead.

        Raises:
            PlanStaleError: If the relation holding the name is not the one
                ``expected_oid`` identifies; nothing stays attached.
        """
        clause, values = _values_clause(bounds, key_arity)
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} ATTACH PARTITION {partition} " + clause,
            parent=parent_name,
            partition=partition_name,
            **values,
        )
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            if isinstance(bounds, RangeBounds) and self._ddl_timezone is not None:
                await conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            await conn.execute(stmt)
            await self._require_oid(conn, partition_name, expected_oid)
            await self._clear_orphan_marker(conn, partition_name)

    async def _require_oid(self, conn: AsyncConnection, partition_name: str, expected_oid: int | None) -> None:
        """The relation holding the name is the one the caller decided about."""
        if expected_oid is None:
            return
        result = await conn.execute(
            text("SELECT c.oid FROM pg_class c WHERE c.oid = to_regclass(:name)"),
            {"name": to_regclass_argument(partition_name)},
        )
        actual = result.scalar()
        if actual is None or int(actual) != expected_oid:
            held = "nothing" if actual is None else f"OID {int(actual)}"
            raise PlanStaleError(
                partition_name, f"the name now resolves to {held}, the plan decided about OID {expected_oid}"
            )

    async def _clear_orphan_marker(self, conn: AsyncConnection, partition_name: str) -> None:
        """Take this library's marker lines off a relation that is attached again."""
        result = await conn.execute(
            text("SELECT obj_description(to_regclass(:partition_name), 'pg_class')"),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        existing = coerce_str(result.scalar())
        if parse_orphan_comment(existing, marker_prefix=self._marker_prefix) is None:
            return
        rest = comment_without_markers(existing, marker_prefix=self._marker_prefix)
        relation = "FOREIGN TABLE" if await relation_kind(conn, partition_name) == "f" else "TABLE"
        if rest:
            stmt = build_ddl_statement(
                f"COMMENT ON {relation} {{partition}} IS [comment]", partition=partition_name, comment=rest
            )
        else:
            stmt = build_ddl_statement(f"COMMENT ON {relation} {{partition}} IS NULL", partition=partition_name)
        await conn.execute(stmt)

    async def _relation_columns(self, conn: AsyncConnection, table_name: str) -> tuple[str, ...]:
        """Read a relation's live column names, in its own physical order.

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
        key_columns: tuple[str, ...],
        from_value: str,
        to_value: str,
        limit: int | None = None,
        expected_target_oid: int | None = None,
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
            expected_target_oid: The identity of the relation the rows should
                land in, checked under the target's lock in the move's own
                transaction -- a target swapped at its name refuses the batch
                (``PlanStaleError``) before a single row leaves DEFAULT.

        Returns:
            Number of rows moved.
        """
        if not key_columns:
            msg = "reconcile_default_rows needs the parent's partition key"
            raise ValueError(msg)

        column_quoted = quote_identifier(key_columns[0])
        from_quoted = quote_literal(from_value)
        to_quoted = quote_literal(to_value)

        # PostgreSQL adds an IS NOT NULL test for *every* key column to a range
        # partition's constraint, so a row with a NULL trailing key value
        # belongs in DEFAULT whatever its leading value is. Moving it out would
        # be rejected -- and the rejection would look exactly like the DEFAULT
        # conflict this call exists to clear, so the retry would never converge.
        not_null = "".join(f" AND {quote_identifier(column)} IS NOT NULL" for column in key_columns[1:])

        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            # Boundary literals must be interpreted in the same timezone ATTACH uses,
            # otherwise a non-UTC server timezone moves the wrong row range.
            if self._ddl_timezone is not None:
                await conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            await self._lock_for_move(conn, default_partition_name, target_partition_name)
            await self._require_oid(conn, target_partition_name, expected_target_oid)
            # All identifiers and literals are properly quoted, S608 is a false positive
            condition = f"{column_quoted} >= {from_quoted} AND {column_quoted} < {to_quoted}{not_null}"
            return await self._move(
                conn, default_partition_name, target_partition_name, condition=condition, limit=limit
            )

    async def move_rows(self, source_name: str, target_name: str, *, limit: int | None = None) -> int:
        """Move rows from one relation into another, whatever their keys.

        Args:
            source_name: Qualified name of the relation to take rows from; a
                partitioned table works, its leaves are addressed through it.
            target_name: Qualified name of the relation to put them in.
            limit: Move at most this many rows; None moves every row.

        Returns:
            Number of rows moved.

        Raises:
            RowMoveRefusedError: If a foreign key's ``ON DELETE`` action would
                fire on the rows as they leave ``source_name``.
        """
        async with asyncio.timeout(self._ddl_timeout), self._engine.begin() as conn:
            await self._lock_for_move(conn, source_name, target_name)
            return await self._move(conn, source_name, target_name, condition=None, limit=limit)

    async def move_rows_conn(self, conn: AsyncConnection, source_name: str, target_name: str) -> int:
        """Move every row of ``source_name`` into ``target_name`` on an open transaction.

        For callers that need the move and what follows it -- a drop, say --
        to commit together. Locks are the caller's.
        """
        return await self._move(conn, source_name, target_name, condition=None, limit=None)

    async def _lock_for_move(self, conn: AsyncConnection, *names: str) -> None:
        """Take SHARE ROW EXCLUSIVE on every local relation involved in a move.

        A foreign table cannot be locked (``LOCK TABLE is not supported for
        foreign tables``); it is written through its wrapper like any other
        INSERT, and a detached one is reachable by nobody else anyway.
        """
        for name in names:
            if await relation_kind(conn, name) != "f":
                await conn.execute(text(f"LOCK TABLE {quote_identifier(name)} IN SHARE ROW EXCLUSIVE MODE"))

    async def _move(
        self, conn: AsyncConnection, source_name: str, target_name: str, *, condition: str | None, limit: int | None
    ) -> int:
        """Run one move statement once it is known to be safe."""
        await self._refuse_referential_actions(conn, source_name)
        columns = await self._relation_columns(conn, source_name)
        # Both sides name their columns. ``RETURNING *`` emits the source's
        # own physical order, and an unqualified INSERT binds that order
        # positionally to the target's -- silently transposing values whenever
        # the two differ. They can differ: ATTACH PARTITION matches columns by
        # name, so a DEFAULT partition created independently and attached need
        # not share the order of one created with LIKE.
        column_list = ", ".join(quote_identifier(column) for column in columns)
        overriding = await self._has_identity_always(conn, target_name)
        statement = _move_statement(
            quote_identifier(source_name),
            quote_identifier(target_name),
            column_list,
            condition,
            limit,
            overriding=overriding,
        )
        try:
            result = await conn.execute(_as_text(statement))
        except (SQLAlchemyError, OSError, TimeoutError) as exc:
            if pg_sqlstate(exc) != "23503":
                raise
            # Even ON DELETE NO ACTION refuses here: its end-of-statement check
            # looks for the key through the referenced tree, and a row being
            # moved sits in a table that is not attached (or not attached any
            # more), so a *referenced* row cannot be moved at all. The failure
            # is atomic -- the statement rolls back with every row in place.
            first_line = str(exc).strip().splitlines()[0]
            raise RowMoveRefusedError(
                source_name,
                f"rows are still referenced through a foreign key and a referenced row cannot leave the "
                f"partition tree it is referenced in ({first_line}); delete or repoint the referencing rows "
                "first, or drop the foreign key for the migration and re-create it afterwards",
            ) from exc
        moved = result.rowcount or 0
        if moved:
            await self._advance_identity_sequences(conn, target_name)
        return moved

    async def _advance_identity_sequences(self, conn: AsyncConnection, target_name: str) -> None:
        """Advance the target's identity sequences past the values a move brought in.

        ``OVERRIDING SYSTEM VALUE`` preserves the moved ids but leaves the
        backing sequence where it was, so the target's next ordinary INSERT
        would draw an id a moved row already owns. Runs in the move's
        transaction, under the target's lock; a pre-existing higher sequence
        position is kept (``GREATEST``).
        """
        result = await conn.execute(
            text(RELATION_IDENTITY_COLUMNS_SQL), {"table_name": to_regclass_argument(target_name)}
        )
        columns = [coerce_str(row[0]) or "" for row in result.fetchall()]
        for column in columns:
            maximum = (
                await conn.execute(
                    _as_text(f"SELECT MAX({quote_identifier(column)}) FROM {quote_identifier(target_name)}")  # noqa: S608
                )
            ).scalar()
            if maximum is None:
                continue
            await conn.execute(
                text(
                    "SELECT setval(seq.oid, GREATEST(CAST(:maximum AS bigint), "
                    "COALESCE(pg_sequence_last_value(seq.oid), CAST(:maximum AS bigint)))) "
                    "FROM CAST(CAST(pg_get_serial_sequence(:table_name, :column) AS regclass) AS oid) AS seq(oid) "
                    "WHERE pg_get_serial_sequence(:table_name, :column) IS NOT NULL"
                ),
                {
                    "maximum": int(maximum),
                    "table_name": to_regclass_argument(target_name),
                    "column": column,
                },
            )

    async def _refuse_referential_actions(self, conn: AsyncConnection, source_name: str) -> None:
        """Refuse a move that a foreign key's ``ON DELETE`` action would turn into data loss.

        The move is one statement: ``DELETE ... RETURNING`` into ``INSERT``.
        PostgreSQL's NO ACTION check runs at the end of it, finds the row in
        its new partition and passes; RESTRICT refuses the statement, which is
        safe. CASCADE, SET NULL and SET DEFAULT act on the DELETE alone and
        would delete or rewrite the referencing rows while the moved row lives
        on -- verified on PostgreSQL 15, 16 and 17.
        """
        result = await conn.execute(
            text(DESTRUCTIVE_INCOMING_FOREIGN_KEYS_SQL), {"table_name": to_regclass_argument(source_name)}
        )
        rows = result.fetchall()
        if not rows:
            return
        described = ", ".join(
            f"{coerce_str(row[0])} on {coerce_str(row[1])} (ON DELETE "
            f"{_ON_DELETE_ACTIONS.get(coerce_str(row[2], encoding='ascii') or '', '?')})"
            for row in rows
        )
        raise RowMoveRefusedError(
            source_name,
            f"foreign key {described} would act on the rows as they are deleted from their old partition, "
            "deleting or rewriting the rows that reference them; re-create it ON DELETE NO ACTION "
            "(referenced rows are then refused row-safe instead) or drop it for the migration and "
            "re-create it afterwards",
        )

    async def _has_identity_always(self, conn: AsyncConnection, table_name: str) -> bool:
        result = await conn.execute(
            text(RELATION_HAS_IDENTITY_ALWAYS_SQL), {"table_name": to_regclass_argument(table_name)}
        )
        return bool(result.scalar())


def _move_statement(
    source: str, target: str, column_list: str, condition: str | None, limit: int | None, *, overriding: bool = False
) -> str:
    """One ``DELETE ... RETURNING`` / ``INSERT`` statement moving rows from ``source`` to ``target``.

    Both sides name their columns: ``RETURNING *`` would emit the source's
    physical order, and an unqualified INSERT binds it positionally -- silently
    transposing values whenever the two relations were created independently.
    A batch is bounded through ``(tableoid, ctid)`` rather than ``ctid`` alone
    so a partitioned source, whose leaves each number their own tuples, is
    addressed unambiguously. ``overriding`` keeps the values of a
    ``GENERATED ALWAYS AS IDENTITY`` column on the target. Every identifier
    and literal is already quoted.
    """
    where = f" WHERE {condition}" if condition else ""
    if limit is not None:
        picked = f"SELECT tableoid, ctid FROM {source}{where} LIMIT {int(limit):d}"  # noqa: S608
        where = f" WHERE (tableoid, ctid) IN ({picked})"
    override = " OVERRIDING SYSTEM VALUE" if overriding else ""
    return (
        f"WITH moved AS ("  # noqa: S608
        f"DELETE FROM {source}{where} RETURNING {column_list}"
        f") "
        f"INSERT INTO {target} ({column_list}){override} "
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
