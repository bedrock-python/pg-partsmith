"""PostgreSQL metadata provider."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from pg_partsmith.catalog_queries import (
    INSTANT_HAS_PASSED_SQL,
    ORPHANS_SQL,
    PARTITION_COLUMNS_SQL,
    PARTITION_FACTS_SQL,
    PARTITION_IS_ATTACHED_SQL,
    PARTITION_STRATEGY_SQL,
    PARTITION_TREE_SQL,
    PARTITION_UPPER_BOUND_SQL,
    RELATION_EXISTS_SQL,
    RELATION_OID_SQL,
    SEQUENCE_LAST_VALUE_SQL,
    TEXT_INSTANT_HAS_PASSED_SQL,
    UNIQUE_CONSTRAINT_COLUMNS_SQL,
)
from pg_partsmith.entities import PartitionInfo, PartitionType
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.partition_bounds import is_addressable, parse_partition_bounds, parse_range_boundaries
from pg_partsmith.topology import (
    ActualTree,
    DefaultBounds,
    DetachedPartition,
    FactKind,
    PartitionFacts,
    PartitionNode,
    PartitionTreeRow,
    RelationKind,
    build_partition_tree,
)
from pg_partsmith.utils import (
    coerce_str,
    orphan_comment_prefix,
    orphan_table_comment,
    parse_orphan_comment,
    qualify,
    quote_identifier,
    quote_literal,
    to_regclass_argument,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from pg_partsmith.boundaries import RangeBoundaryCodec
    from pg_partsmith.lifecycle import SqlPredicate


logger = logging.getLogger(__name__)


class PostgresMetadataProvider:
    """Provider for PostgreSQL partition metadata.

    Queries pg_catalog to retrieve information about table partitioning.
    Override any method to customise catalog queries for your schema setup.

    Each method opens its own read-only connection from the engine pool so it
    is safe to call outside any existing transaction.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        marker_prefix: str | None = None,
        boundary_codec: RangeBoundaryCodec | None = None,
        ddl_timezone: str | None = None,
    ) -> None:
        """Initialize provider.

        Args:
            engine: SQLAlchemy async engine.
            marker_prefix: Optional COMMENT marker prefix for orphaned partitions.
                When None, the library default prefix is used. Pass the same
                value to both repository and metadata provider if you override it.
            ddl_timezone: Session timezone :meth:`is_partition_closed` reads
                naive boundary literals in. Pass whatever the repository writes
                partitions with: a ``timestamp``/``date`` key renders its bounds
                without an offset, so reader and writer must agree or a
                partition is reported closed at the wrong moment. A
                ``timestamptz`` key is unaffected -- its literals carry an offset.
            boundary_codec: Codec :meth:`is_partition_closed` reads encoded
                boundary literals with. Required only when the partition key is
                an encoded identifier rather than a timestamp; pass the codec
                the table's ``TimeBoundaries`` was configured with. Planning
                does not need it: the planner decodes through the config.
        """
        self._engine = engine
        self._marker_prefix = orphan_comment_prefix(marker_prefix=marker_prefix)
        self._boundary_codec = boundary_codec
        self._ddl_timezone = ddl_timezone

    # ── The root ────────────────────────────────────────────────────────────────

    async def get_partition_type(self, table_name: str) -> PartitionType | None:
        """Get partition type for a table."""
        async with self._engine.connect() as conn:
            result = await conn.execute(text(PARTITION_STRATEGY_SQL), {"table_name": to_regclass_argument(table_name)})
            strat = coerce_str(result.scalar(), encoding="ascii")

        return PartitionType.from_partstrat(strat)

    async def get_partition_columns(self, table_name: str) -> tuple[str, ...]:
        """Return a table's own partition key columns, in key order.

        Key order is not column order, so it comes from ``partattrs``' own
        ordering.

        Args:
            table_name: Table to inspect, schema-qualified.

        Returns:
            The key columns in order; empty when the table is not partitioned.

        Raises:
            InvalidPartitionConfigError: If any key position is an expression
                rather than a column, which this library cannot address.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(PARTITION_COLUMNS_SQL),
                {"table_name": to_regclass_argument(table_name)},
            )
            rows = result.fetchall()

        key = tuple(coerce_str(row[0]) for row in rows)
        for position, column in enumerate(key, start=1):
            if column is None:
                raise InvalidPartitionConfigError(_expression_key_message(table_name, position))

        return tuple(column for column in key if column is not None)

    async def get_partition_column(self, table_name: str) -> str | None:
        """Get the single partition column of a table.

        Raises:
            ValueError: If the table uses a composite (multi-column) partition key.
        """
        key = await self.get_partition_columns(table_name)
        if not key:
            return None
        if len(key) > 1:
            msg = (
                f"Table {table_name!r} uses a composite partition key {list(key)!r}; "
                "read it with get_partition_columns()."
            )
            raise ValueError(msg)
        return key[0]

    # ── The tree ────────────────────────────────────────────────────────────────

    async def get_actual_tree(self, table_name: str) -> ActualTree | None:
        """Return the whole tree below ``table_name`` plus its orphans.

        The tree is one round-trip regardless of depth; the orphans one more.
        Nothing is measured here -- see :meth:`measure` -- so a plan for a
        simple monthly table never pays for ``pg_total_relation_size``.

        Args:
            table_name: Root of the tree, schema-qualified.

        Returns:
            The tree with its orphans, or None when ``table_name`` is not
            partitioned.
        """
        root = await self.get_partition_tree(table_name)
        if root is None or root.partition_type is None:
            return None

        parents = [node.name for node in root.walk() if node.partition_type is not None]
        orphans = await self._orphans_of(parents)
        return ActualTree(root=root, orphans=orphans)

    async def measure(
        self,
        tree: ActualTree,
        *,
        targets: tuple[str, ...],
        facts: frozenset[FactKind] = frozenset(),
        sql_predicates: tuple[SqlPredicate, ...] = (),
    ) -> ActualTree:
        """Return ``tree`` with facts attached to the named targets.

        Sizes and row estimates come in one query for every target;
        each SQL predicate is asked once per target.

        Args:
            tree: The tree to annotate.
            targets: Schema-qualified names to measure; anything else stays
                unmeasured.
            facts: What to measure.
            sql_predicates: Questions to ask about each target.
        """
        wanted = set(targets)
        if not wanted or not (facts or sql_predicates):
            return tree
        measured = await self._measure(tree.root, tree.orphans, wanted, facts, sql_predicates)
        root = _with_facts(tree.root, measured)
        orphans = tuple(
            o.model_copy(update={"facts": measured[o.name]}) if o.name in measured else o for o in tree.orphans
        )
        return ActualTree(root=root, orphans=orphans)

    async def get_partition_tree(self, table_name: str) -> PartitionNode | None:
        """Return the tree rooted at ``table_name``, without orphans.

        Works for a detached relation as well as a live root: a detached
        branch is the root of its own tree, which is how a half-built branch
        is inspected before it is attached.

        Args:
            table_name: Root of the tree, schema-qualified.

        Returns:
            The root node with its descendants, or None when ``table_name`` is
            not partitioned and is not itself a partition.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(PARTITION_TREE_SQL),
                {"table_name": to_regclass_argument(table_name)},
            )
            rows = result.fetchall()

        tree_rows: list[PartitionTreeRow] = []
        unaddressable_parents: set[str] = set()
        for row in rows:
            schema = coerce_str(row.partition_schema) or ""
            relname = coerce_str(row.partition_name) or ""
            parent_schema_raw = coerce_str(row.parent_schema)
            parent_relname_raw = coerce_str(row.parent_name)
            if not is_addressable(schema, relname):
                # The parent keeps a child the tree cannot show. Recording that
                # is what keeps the planner from reading the shortened child set
                # as a set of gaps to fill.
                if parent_schema_raw and parent_relname_raw:
                    unaddressable_parents.add(qualify(parent_schema_raw, parent_relname_raw))
                continue

            parent_name = (
                qualify(parent_schema_raw, parent_relname_raw) if parent_schema_raw and parent_relname_raw else None
            )

            columns = row.partition_columns or ()
            named = tuple(str(c) for c in columns if c is not None)
            tree_rows.append(
                PartitionTreeRow(
                    level=row.level,
                    name=qualify(schema, relname),
                    oid=int(row.oid),
                    parent_name=parent_name,
                    relkind=RelationKind.from_relkind(coerce_str(row.relkind, encoding="ascii")),
                    bounds=parse_partition_bounds(coerce_str(row.boundaries)),
                    bounds_expr=coerce_str(row.boundaries) or None,
                    is_attached=bool(row.is_attached),
                    detach_pending=bool(row.detach_pending),
                    partition_type=PartitionType.from_partstrat(coerce_str(row.partstrat, encoding="ascii")),
                    partition_columns=named,
                    # An expression key position comes back as NULL and has no
                    # name to report; what matters is that the key is wider than
                    # the names, so nothing compares it as if it were complete.
                    has_expression_key=len(named) != (row.key_arity or len(named)),
                )
            )

        return build_partition_tree(tree_rows, unaddressable_parents)

    async def _orphans_of(self, parents: list[str]) -> tuple[DetachedPartition, ...]:
        """Marker-tagged detached tables whose marker names one of ``parents``."""
        if not parents:
            return ()
        markers = [orphan_table_comment(parent, marker_prefix=self._marker_prefix) for parent in parents]
        async with self._engine.connect() as conn:
            result = await conn.execute(text(ORPHANS_SQL), {"markers": markers})
            rows = result.fetchall()

        orphans: list[DetachedPartition] = []
        for row in rows:
            schema = coerce_str(row.partition_schema) or ""
            relname = coerce_str(row.partition_name) or ""
            if not is_addressable(schema, relname):
                continue
            parsed = parse_orphan_comment(coerce_str(row.description), marker_prefix=self._marker_prefix)
            if parsed is None:
                continue
            parent, detached_at = parsed
            orphans.append(
                DetachedPartition(
                    name=qualify(schema, relname),
                    oid=int(row.oid),
                    relkind=RelationKind.from_relkind(coerce_str(row.relkind, encoding="ascii")),
                    parent_name=parent,
                    detached_at=detached_at,
                )
            )
        return tuple(orphans)

    async def _measure(
        self,
        root: PartitionNode,
        orphans: tuple[DetachedPartition, ...],
        targets: set[str],
        facts: frozenset[FactKind],
        sql_predicates: tuple[SqlPredicate, ...],
    ) -> dict[str, PartitionFacts]:
        """Gather the requested facts for the targets, by name."""
        oids: dict[str, int] = {}
        for node in root.walk():
            if node.name in targets and node.oid is not None:
                oids[node.name] = node.oid
        for orphan in orphans:
            if orphan.name in targets and orphan.oid is not None:
                oids[orphan.name] = orphan.oid
        if not oids:
            return {}

        sizes: dict[int, tuple[int, int]] = {}
        if facts:
            async with self._engine.connect() as conn:
                result = await conn.execute(text(PARTITION_FACTS_SQL), {"oids": list(oids.values())})
                for row in result.fetchall():
                    sizes[int(row.oid)] = (int(row.size_bytes or 0), int(row.row_estimate or 0))

        measured: dict[str, PartitionFacts] = {}
        for name, oid in oids.items():
            size, rows = sizes.get(oid, (0, 0))
            answers: dict[str, bool] = {}
            for predicate in sql_predicates:
                answers[predicate.id] = await self.evaluate_sql_predicate(predicate, name)
            measured[name] = PartitionFacts(
                size_bytes=size if FactKind.SIZE in facts else None,
                row_estimate=rows if FactKind.ROWS in facts else None,
                predicates=answers,
            )
        return measured

    async def evaluate_sql_predicate(self, predicate: SqlPredicate, partition_name: str) -> bool:
        """Ask one :class:`~pg_partsmith.lifecycle.SqlPredicate` about one relation.

        ``{partition}`` is replaced with the quoted, schema-qualified name;
        nothing else is interpolated. An error in the statement propagates: a
        rule that cannot be evaluated must not silently read as False.
        """
        statement = predicate.sql.replace("{partition}", quote_identifier(partition_name))
        async with self._engine.connect() as conn:
            result = await conn.execute(text(statement.replace(":", r"\:")))
            return bool(result.scalar())

    # ── Cursors ─────────────────────────────────────────────────────────────────

    async def get_key_high_water_mark(self, table_name: str, column: str, *, sequence: bool = False) -> int | None:
        """Return the newest value of an integer partition key.

        ``max(column)`` over the whole table is one index probe per leaf when
        the key is indexed -- which a partition key almost always is. The
        sequence form is one catalog read, and right only for a key fed by
        that sequence.
        """
        async with self._engine.connect() as conn:
            if sequence:
                result = await conn.execute(
                    text(SEQUENCE_LAST_VALUE_SQL),
                    {"table_name": to_regclass_argument(table_name), "column": column},
                )
            else:
                result = await conn.execute(
                    text(f"SELECT max({quote_identifier(column)}) FROM {quote_identifier(table_name)}")  # noqa: S608
                )
            value = result.scalar()
        return None if value is None else int(value)

    # ── Single relations ────────────────────────────────────────────────────────

    async def list_partitions(self, table_name: str) -> list[PartitionInfo]:
        """List the direct partitions of a table, including its marker-tagged orphans.

        Orphaned partitions are detached-but-not-dropped tables previously
        detached by this library. They are detected by a COMMENT marker set on
        successful detach and returned with ``is_attached=False`` and ``None``
        boundaries.

        Partition names are always schema-qualified with the child's catalog
        schema — a partition may live in a different schema than its parent,
        and a bare name could resolve to an unrelated table via ``search_path``.
        """
        tree = await self.get_actual_tree(table_name)
        if tree is None or tree.root.partition_type is None:
            return []

        partition_type = tree.root.partition_type
        partitions: list[PartitionInfo] = []
        for child in tree.root.children:
            bounds = child.bounds
            is_default = isinstance(bounds, DefaultBounds)
            from_value = to_value = None
            if bounds is not None and bounds.kind == "range":
                from_value, to_value = bounds.from_value, bounds.to_value
            partitions.append(
                PartitionInfo(
                    name=child.name,
                    oid=child.oid,
                    partition_type=partition_type,
                    from_value=from_value,
                    to_value=to_value,
                    boundaries_expr=_render_bounds(child),
                    bounds=bounds,
                    is_attached=child.is_attached,
                    is_default=is_default,
                    relkind=child.relkind,
                    subpartition_type=child.partition_type,
                    parent_table=table_name,
                )
            )

        for orphan in tree.orphans:
            if orphan.parent_name != tree.root.name:
                continue
            partitions.append(
                PartitionInfo(
                    name=orphan.name,
                    oid=orphan.oid,
                    partition_type=partition_type,
                    is_attached=False,
                    relkind=orphan.relkind,
                    parent_table=table_name,
                )
            )

        return partitions

    async def partition_exists(self, partition_name: str) -> bool:
        """Check if a partition table exists in pg_class.

        Returns:
            True if the table exists as a regular or partitioned table
            (a partition may itself be subpartitioned).
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(RELATION_EXISTS_SQL),
                {"partition_name": to_regclass_argument(partition_name)},
            )
            return bool(result.scalar())

    async def is_partition_attached(self, table_name: str, partition_name: str) -> bool:
        """Check if a partition is currently attached to its parent via pg_inherits."""
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(PARTITION_IS_ATTACHED_SQL),
                {
                    "table_name": to_regclass_argument(table_name),
                    "partition_name": to_regclass_argument(partition_name),
                },
            )
            return bool(result.scalar())

    async def get_relation_oid(self, name: str) -> int | None:
        """Return the OID of the relation currently holding ``name``, or None."""
        async with self._engine.connect() as conn:
            result = await conn.execute(text(RELATION_OID_SQL), {"name": to_regclass_argument(name)})
            value = result.scalar()
        return None if value is None else int(value)

    async def get_partition_boundaries(self, partition_name: str) -> tuple[str, str] | None:
        """Get a RANGE partition's ``(from_value, to_value)``, or None."""
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT pg_get_expr(relpartbound, oid)
                    FROM pg_class
                    WHERE oid = to_regclass(:partition_name)
                    """
                ),
                {"partition_name": to_regclass_argument(partition_name)},
            )
            boundaries_expr = coerce_str(result.scalar())

        if not boundaries_expr:
            return None

        from_val, to_val = parse_range_boundaries(boundaries_expr)
        if from_val is not None and to_val is not None:
            return from_val, to_val

        return None

    async def is_partition_closed(self, partition_name: str, *, settle_seconds: int = 0) -> bool:
        """True when the partition's upper bound (+ settle buffer) has passed.

        ``now()`` is evaluated on the server rather than on the client, so the
        answer tolerates app-clock skew. Useful for export/archive pipelines
        that must only finalize partitions which can no longer receive
        in-range rows.

        Works for a subpartitioned branch exactly as for a plain leaf: what is
        read is the branch's own RANGE bound in the root table, and its whole
        subtree closes with it.

        A naive bound -- which is what a ``timestamp`` or ``date`` key produces
        -- is resolved under this provider's ``ddl_timezone``. Configure it with
        the same value the repository writes partitions with, or the two
        disagree about when the bound falls.

        Args:
            partition_name: Attached partition table name.
            settle_seconds: Extra buffer after the upper bound for late writers
                still holding open transactions.

        Returns:
            True when ``now() >= upper_bound + settle_seconds``. False for the
            DEFAULT partition, non-RANGE partitions, unbounded upper bounds
            (MAXVALUE / infinity), detached tables, unresolvable names, and
            boundaries that carry no instant this provider can read.
        """
        async with self._engine.connect() as conn:
            if self._ddl_timezone is not None:
                # A naive bound is resolved by the session timezone, so this has
                # to be the one the partition was written with. Without it the
                # server default decides, and the two need not agree.
                await conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))

            bound_result = await conn.execute(
                text(PARTITION_UPPER_BOUND_SQL),
                {"partition_name": to_regclass_argument(partition_name)},
            )
            raw_bound = coerce_str(bound_result.scalar())
            if raw_bound is None:
                # No upper bound to read: DEFAULT, non-RANGE, detached, or unknown.
                return False

            if self._boundary_codec is not None:
                instant = self._boundary_codec.decode(raw_bound)
                if instant is None:
                    self._warn_unreadable_bound(partition_name, raw_bound)
                    return False
                query = INSTANT_HAS_PASSED_SQL
                upper_bound: datetime | str = instant
            else:
                query = TEXT_INSTANT_HAS_PASSED_SQL
                upper_bound = raw_bound

            try:
                result = await conn.execute(
                    text(query),
                    {"upper_bound": upper_bound, "settle_seconds": settle_seconds},
                )
            except DBAPIError:
                # A bound can look like a date and still not be one -- a
                # sortable identifier with a date-like prefix, say. Reporting
                # "not closed" is the documented answer; raising out of a
                # predicate is not.
                self._warn_unreadable_bound(partition_name, raw_bound)
                return False

            return bool(result.scalar())

    def _warn_unreadable_bound(self, partition_name: str, raw_bound: str) -> None:
        """Explain a partition that can never report as closed.

        The answer is always False while the bound cannot be read, so an export
        pipeline gated on this would wait forever with nothing to show for it.
        """
        logger.warning(
            "Partition has an upper bound this provider cannot read, so it never reports as closed; "
            "pass the boundary_codec its partitions were created with",
            extra={"partition_name": partition_name, "upper_bound": raw_bound},
        )

    async def get_default_partition(self, table_name: str) -> PartitionInfo | None:
        """Get DEFAULT partition for a table if it exists and is attached."""
        all_partitions = await self.list_partitions(table_name)
        defaults = [p for p in all_partitions if p.is_default and p.is_attached]
        return defaults[0] if defaults else None

    async def get_unique_constraint_columns(self, table_name: str) -> tuple[tuple[str, ...], ...]:
        """Return the column tuples of every UNIQUE / PRIMARY KEY constraint.

        PostgreSQL requires such a constraint on a partitioned table to contain
        all of its partition-key columns. Reading them lets a nested scheme be
        refused with an explanation before any DDL is attempted, instead of
        failing halfway through a maintenance run.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(UNIQUE_CONSTRAINT_COLUMNS_SQL),
                {"table_name": to_regclass_argument(table_name)},
            )
            rows = result.fetchall()

        return tuple(tuple(str(c) for c in (row.columns or ())) for row in rows)


def _with_facts(node: PartitionNode, measured: dict[str, PartitionFacts]) -> PartitionNode:
    """Return ``node`` with facts attached wherever they were measured."""
    children = tuple(_with_facts(child, measured) for child in node.children)
    facts = measured.get(node.name, node.facts)
    if children == node.children and facts is node.facts:
        return node
    return node.model_copy(update={"children": children, "facts": facts})


def _render_bounds(node: PartitionNode) -> str | None:
    """The bound expression for ``boundaries_expr``: the catalog's own text when it was kept."""
    if node.bounds_expr is not None:
        return node.bounds_expr
    bounds = node.bounds
    if bounds is None:
        return None
    if bounds.kind == "range":
        return f"FOR VALUES FROM ({quote_literal(bounds.from_value)}) TO ({quote_literal(bounds.to_value)})"
    if bounds.kind == "hash":
        return f"FOR VALUES WITH (modulus {bounds.modulus}, remainder {bounds.remainder})"
    if bounds.kind == "list":
        values = [quote_literal(v) for v in bounds.values]
        if bounds.includes_null:
            values.append("NULL")
        return f"FOR VALUES IN ({', '.join(values)})"
    return "DEFAULT"


def _expression_key_message(table_name: str, position: int) -> str:
    """Explain a key this library has no way to address."""
    return (
        f"Table {table_name!r} partitions on an expression at key position {position}, which pg-partsmith "
        "cannot address: it builds bounds from column values, and an expression's value is not one. "
        "Partition on plain columns, or manage this table outside pg-partsmith."
    )
