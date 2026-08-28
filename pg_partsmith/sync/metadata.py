"""PostgreSQL metadata provider."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from pg_partsmith.catalog_queries import (
    INSTANT_HAS_PASSED_SQL,
    PARTITION_COLUMNS_SQL,
    PARTITION_IS_ATTACHED_SQL,
    PARTITION_TREE_SQL,
    PARTITION_UPPER_BOUND_SQL,
    RELATION_EXISTS_SQL,
    TEXT_INSTANT_HAS_PASSED_SQL,
    UNIQUE_CONSTRAINT_COLUMNS_SQL,
)
from pg_partsmith.entities import PartitionInfo, PartitionType
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.partition_bounds import is_addressable, parse_partition_bounds, parse_range_boundaries
from pg_partsmith.topology import PartitionNode, PartitionTreeRow, build_partition_tree
from pg_partsmith.utils import (
    coerce_str,
    orphan_comment_prefix,
    orphan_table_comment,
    qualify,
    quote_literal,
    to_regclass_argument,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from pg_partsmith.boundaries import RangeBoundaryCodec


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
        engine: Engine,
        *,
        marker_prefix: str | None = None,
        boundary_codec: RangeBoundaryCodec | None = None,
        ddl_timezone: str | None = None,
    ) -> None:
        """Initialize provider.

        Args:
            engine: SQLAlchemy engine.
            marker_prefix: Optional COMMENT marker prefix for orphaned partitions.
                When None, the library default prefix is used. Pass the same
                value to both repository and metadata provider if you override it.
            ddl_timezone: Session timezone to read naive boundary literals in.
                Pass whatever the repository writes partitions with: a
                ``timestamp``/``date`` key renders its bounds without an offset,
                so reader and writer must agree or a partition is reported
                closed at the wrong moment. A ``timestamptz`` key is unaffected
                -- its literals carry an offset.
            boundary_codec: Codec used to read boundary literals back into
                instants. Required only when the partition key is an encoded
                identifier rather than a timestamp; pass the same codec the
                period calculator was built with.
        """
        self._engine = engine
        self._marker_prefix = orphan_comment_prefix(marker_prefix=marker_prefix)
        self._boundary_codec = boundary_codec
        self._ddl_timezone = ddl_timezone

    def get_partition_type(self, table_name: str) -> PartitionType | None:
        """Get partition type for a table."""
        with self._engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT partstrat
                    FROM pg_partitioned_table t
                    WHERE t.partrelid = to_regclass(:table_name)
                    """
                ),
                {"table_name": to_regclass_argument(table_name)},
            )
            strat = coerce_str(result.scalar(), encoding="ascii")

        return PartitionType.from_partstrat(strat)

    def get_partition_column(self, table_name: str) -> str | None:
        """Get partition column for a table.

        Raises:
            ValueError: If the table uses a composite (multi-column) partition
                key.  Only single-column keys are supported by this library.
        """
        key = self._read_partition_key(table_name)
        if not key:
            return None

        if len(key) > 1:
            msg = (
                f"Table {table_name!r} uses a composite partition key {list(key)!r}. "
                "Only single-column partition keys are supported."
            )
            raise ValueError(msg)

        if key[0] is None:
            raise ValueError(_expression_key_message(table_name, 1))

        return key[0]

    def get_partition_columns(self, table_name: str) -> tuple[str, ...]:
        """Return a table's own partition key columns, in key order.

        Unlike :meth:`get_partition_column`, which predates composite keys and
        refuses them, this reports the whole key. Key order is not column
        order, so it comes from ``partattrs``' own ordering.

        Args:
            table_name: Table to inspect, schema-qualified.

        Returns:
            The key columns in order; empty when the table is not partitioned.
        """
        key = self._read_partition_key(table_name)
        for position, column in enumerate(key, start=1):
            if column is None:
                raise InvalidPartitionConfigError(_expression_key_message(table_name, position))

        return tuple(column for column in key if column is not None)

    def _read_partition_key(self, table_name: str) -> tuple[str | None, ...]:
        """Read a table's partition key in key order, expressions included as None."""
        with self._engine.connect() as conn:
            result = conn.execute(
                text(PARTITION_COLUMNS_SQL),
                {"table_name": to_regclass_argument(table_name)},
            )
            rows = result.fetchall()

        return tuple(coerce_str(row[0]) for row in rows)

    def list_partitions(self, table_name: str) -> list[PartitionInfo]:
        """List all partitions for a table, including orphaned detached ones.

        Orphaned partitions are detached-but-not-dropped tables previously
        detached by this library. They are detected by a COMMENT marker set on
        successful detach and returned with ``is_attached=False`` and ``None``
        boundaries.

        Partition names are always schema-qualified with the child's catalog
        schema — a partition may live in a different schema than its parent,
        and a bare name could resolve to an unrelated table via ``search_path``.
        """
        with self._engine.connect() as conn:
            parent_info_result = conn.execute(
                text(
                    """
                    SELECT
                        pt.partstrat,
                        ns.nspname || '.' || c.relname AS qualified_name
                    FROM pg_class c
                    JOIN pg_namespace ns ON c.relnamespace = ns.oid
                    LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
                    WHERE c.oid = to_regclass(:name)
                    """
                ),
                {"name": to_regclass_argument(table_name)},
            )
            parent_row = parent_info_result.fetchone()
            if not parent_row:
                return []

            strat = coerce_str(parent_row[0], encoding="ascii")
            parent_qualified = coerce_str(parent_row[1]) or table_name

            partition_type = PartitionType.from_partstrat(strat)
            if not partition_type:
                return []

            attached_result = conn.execute(
                text(
                    """
                    SELECT
                        ns.nspname AS partition_schema,
                        child.relname AS partition_name,
                        pg_get_expr(child.relpartbound, child.oid) AS boundaries,
                        child.relispartition AS is_attached,
                        child_pt.partstrat AS subpartstrat
                    FROM pg_inherits inh
                    JOIN pg_class child ON inh.inhrelid = child.oid
                    JOIN pg_namespace ns ON child.relnamespace = ns.oid
                    LEFT JOIN pg_partitioned_table child_pt ON child_pt.partrelid = child.oid
                    WHERE inh.inhparent = to_regclass(:table_name)
                    ORDER BY ns.nspname, child.relname
                    """
                ),
                {"table_name": to_regclass_argument(table_name)},
            )
            attached_rows = attached_result.fetchall()

            orphan_result = conn.execute(
                text(
                    """
                    SELECT
                        ns.nspname AS partition_schema,
                        c.relname AS partition_name
                    FROM pg_class c
                    JOIN pg_namespace ns ON c.relnamespace = ns.oid
                    JOIN pg_description d
                      ON d.objoid = c.oid
                     AND d.classoid = 'pg_class'::regclass
                     AND d.objsubid = 0
                    WHERE c.relkind IN ('r', 'p')
                      AND c.relispartition = false
                      AND split_part(d.description, E'\\n', 1) = :marker
                       AND NOT EXISTS (
                           SELECT 1
                           FROM pg_inherits inh
                           WHERE inh.inhrelid = c.oid
                       )
                    ORDER BY ns.nspname, c.relname
                    """
                ),
                {
                    "marker": orphan_table_comment(parent_qualified, marker_prefix=self._marker_prefix),
                },
            )
            orphan_rows = orphan_result.fetchall()

        partitions: list[PartitionInfo] = []

        for row in attached_rows:
            relname = coerce_str(row.partition_name) or ""
            part_schema = coerce_str(row.partition_schema) or ""

            if not is_addressable(part_schema, relname):
                continue

            name = qualify(part_schema, relname)

            boundaries_str = coerce_str(row.boundaries) or ""
            is_default = boundaries_str.strip().upper() == "DEFAULT"
            from_val, to_val = (None, None) if is_default else self._parse_boundaries(boundaries_str)
            partitions.append(
                PartitionInfo(
                    name=name,
                    partition_type=partition_type,
                    from_value=from_val,
                    to_value=to_val,
                    boundaries_expr=boundaries_str if boundaries_str else None,
                    bounds=parse_partition_bounds(boundaries_str),
                    is_attached=row.is_attached,
                    is_default=is_default,
                    subpartition_type=PartitionType.from_partstrat(coerce_str(row.subpartstrat, encoding="ascii")),
                    parent_table=table_name,
                )
            )

        for row in orphan_rows:
            relname = coerce_str(row.partition_name) or ""
            part_schema = coerce_str(row.partition_schema) or ""

            if not is_addressable(part_schema, relname):
                continue

            name = qualify(part_schema, relname)
            partitions.append(
                PartitionInfo(
                    name=name,
                    partition_type=partition_type,
                    from_value=None,
                    to_value=None,
                    is_attached=False,
                    parent_table=table_name,
                )
            )

        return partitions

    def partition_exists(self, partition_name: str) -> bool:
        """Check if a partition table exists in pg_class.

        Args:
            partition_name: Partition table name.

        Returns:
            True if the table exists as a regular or partitioned table
            (a partition may itself be subpartitioned).
        """
        with self._engine.connect() as conn:
            result = conn.execute(
                text(RELATION_EXISTS_SQL),
                {"partition_name": to_regclass_argument(partition_name)},
            )
            return bool(result.scalar())

    def is_partition_attached(self, table_name: str, partition_name: str) -> bool:
        """Check if a partition is currently attached to its parent via pg_inherits.

        Args:
            table_name: Parent table name.
            partition_name: Partition table name.

        Returns:
            True if the partition is attached.
        """
        with self._engine.connect() as conn:
            result = conn.execute(
                text(PARTITION_IS_ATTACHED_SQL),
                {
                    "table_name": to_regclass_argument(table_name),
                    "partition_name": to_regclass_argument(partition_name),
                },
            )
            return bool(result.scalar())

    def get_partition_boundaries(self, partition_name: str) -> tuple[str, str] | None:
        """Get partition boundaries.

        Args:
            partition_name: Partition table name.

        Returns:
            Tuple of (from_value, to_value) or None if not a range partition.
        """
        with self._engine.connect() as conn:
            result = conn.execute(
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

        from_val, to_val = self._parse_boundaries(boundaries_expr)
        if from_val is not None and to_val is not None:
            return from_val, to_val

        return None

    def _parse_boundaries(self, boundaries_expr: str | None) -> tuple[str | None, str | None]:
        """Delegate to :func:`pg_partsmith.partition_bounds.parse_range_boundaries`; override to customise parsing."""
        return parse_range_boundaries(boundaries_expr)

    def is_partition_closed(self, partition_name: str, *, settle_seconds: int = 0) -> bool:
        """True when the partition's upper bound (+ settle buffer) has passed.

        The comparison runs entirely server-side — ``now()`` and the bound come
        from the same query — so it tolerates replica lag and app-clock skew.
        Useful for export/archive pipelines that must only finalize partitions
        that can no longer receive in-range rows.

        Args:
            partition_name: Attached partition table name.
            settle_seconds: Extra buffer after the upper bound for late writers
                still holding open transactions.

        Works for a subpartitioned branch exactly as for a plain leaf: what is
        read is the branch's own RANGE bound in the root table, and its whole
        subtree closes with it.

        Returns:
            True when ``now() >= upper_bound + settle_seconds``. False for the
            DEFAULT partition, non-RANGE partitions, unbounded upper bounds
            (MAXVALUE / infinity), detached tables, unresolvable names, and
            boundaries that carry no instant this provider can read.
        """
        with self._engine.connect() as conn:
            if self._ddl_timezone is not None:
                # A naive bound is resolved by the session timezone, so this has
                # to be the one the partition was written with. Without it the
                # server default decides, and the two need not agree.
                conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))

            bound_result = conn.execute(
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
                result = conn.execute(
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

    def get_default_partition(self, table_name: str) -> PartitionInfo | None:
        """Get DEFAULT partition for a table if it exists and is attached.

        Args:
            table_name: Parent table name.

        Returns:
            PartitionInfo with is_default=True, or None if no default partition exists.
        """
        all_partitions = self.list_partitions(table_name)
        defaults = [p for p in all_partitions if p.is_default and p.is_attached]
        return defaults[0] if defaults else None

    def get_partition_tree(self, table_name: str) -> PartitionNode | None:
        """Return the whole partition tree rooted at ``table_name``.

        Unlike :meth:`list_partitions`, which reports the direct children a
        lifecycle acts on, this walks the hierarchy to the leaves — the shape
        subpartition reconciliation needs to know which buckets exist. One
        round-trip regardless of depth.

        Detached partitions are absent by construction: a detached branch is no
        longer part of its parent's tree. Query it by name to inspect it.

        Args:
            table_name: Root of the tree, schema-qualified.

        Returns:
            The root node with its descendants, or None when ``table_name`` is
            not partitioned and is not itself a partition.
        """
        with self._engine.connect() as conn:
            result = conn.execute(
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

            parent_schema = coerce_str(row.parent_schema)
            parent_relname = coerce_str(row.parent_name)
            parent_name = qualify(parent_schema, parent_relname) if parent_schema and parent_relname else None

            columns = row.partition_columns or ()
            named = tuple(str(c) for c in columns if c is not None)
            tree_rows.append(
                PartitionTreeRow(
                    level=row.level,
                    name=qualify(schema, relname),
                    parent_name=parent_name,
                    bounds=parse_partition_bounds(coerce_str(row.boundaries)),
                    is_attached=bool(row.is_attached),
                    partition_type=PartitionType.from_partstrat(coerce_str(row.partstrat, encoding="ascii")),
                    partition_columns=named,
                    # An expression key position comes back as NULL and has no
                    # name to report; what matters is that the key is wider than
                    # the names, so nothing compares it as if it were complete.
                    has_expression_key=len(named) != (row.key_arity or len(named)),
                )
            )

        return build_partition_tree(tree_rows, unaddressable_parents)

    def get_unique_constraint_columns(self, table_name: str) -> tuple[tuple[str, ...], ...]:
        """Return the column tuples of every UNIQUE / PRIMARY KEY constraint.

        PostgreSQL requires such a constraint on a partitioned table to contain
        all of its partition-key columns. Reading them lets a subpartitioning
        config be refused with an explanation before any DDL is attempted,
        instead of failing halfway through a maintenance run.

        Args:
            table_name: Table to inspect, schema-qualified.

        Returns:
            One tuple of column names per constraint; empty when the table has
            no unique constraints at all.
        """
        with self._engine.connect() as conn:
            result = conn.execute(
                text(UNIQUE_CONSTRAINT_COLUMNS_SQL),
                {"table_name": to_regclass_argument(table_name)},
            )
            rows = result.fetchall()

        return tuple(tuple(str(c) for c in (row.columns or ())) for row in rows)


def _expression_key_message(table_name: str, position: int) -> str:
    """Explain a key this library has no way to address."""
    return (
        f"Table {table_name!r} partitions on an expression at key position {position}, which pg-partsmith "
        "cannot address: it builds bounds from column values, and an expression's value is not one. "
        "Partition on plain columns, or manage this table outside pg-partsmith."
    )
