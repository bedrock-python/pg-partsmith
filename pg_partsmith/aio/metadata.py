"""PostgreSQL metadata provider."""

import logging
import re

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from pg_partsmith.entities import PartitionInfo, PartitionType
from pg_partsmith.utils import (
    orphan_comment_prefix,
    orphan_table_comment,
    qualify,
    split_qualified_name,
    to_regclass_argument,
)

# Pre-compiled regex patterns for partition boundary parsing.
_RANGE_BOUND_PATTERN = re.compile(r"^\s*FOR\s+VALUES\s+FROM\s*\(", re.IGNORECASE)
_BOUNDARY_SEP_PATTERN = re.compile(r"\)\s+TO\s+\(", re.IGNORECASE)
_FROM_PREFIX_PATTERN = re.compile(r"^.*?FROM\s*\(", re.IGNORECASE | re.DOTALL)
_TRAILING_PAREN_PATTERN = re.compile(r"\)\s*$", re.IGNORECASE | re.DOTALL)
_CAST_PATTERN = re.compile(r"^CAST\((?P<inner>.*)\s+AS\s+.*\)$", re.IGNORECASE | re.DOTALL)
_STR_LITERAL_PATTERN = re.compile(r"'(?P<s>(?:[^']|'')*)'")

logger = logging.getLogger(__name__)


def _is_addressable(schema: str, relname: str) -> bool:
    """Return False for relations whose schema or name contains a dot.

    The library addresses relations as ``schema.relname`` strings, so a dot
    inside either part would be re-split into a different relation by
    ``quote_identifier`` — DDL could then target the wrong table.  Such
    partitions are never created by this library; skip them with a warning.
    """
    if "." in schema or "." in relname:
        logger.warning(
            "Skipping partition with '.' in its schema or name; not addressable by qualified-name DDL",
            extra={"partition_schema": schema, "partition_name": relname},
        )
        return False
    return True


class PostgresMetadataProvider:
    """Provider for PostgreSQL partition metadata.

    Queries pg_catalog to retrieve information about table partitioning.
    Override any method to customise catalog queries for your schema setup.

    Each method opens its own read-only connection from the engine pool so it
    is safe to call outside any existing transaction.
    """

    def __init__(self, engine: AsyncEngine, *, marker_prefix: str | None = None) -> None:
        """Initialize provider.

        Args:
            engine: SQLAlchemy async engine.
            marker_prefix: Optional COMMENT marker prefix for orphaned partitions.
                When None, the library default prefix is used. Pass the same
                value to both repository and metadata provider if you override it.
        """
        self._engine = engine
        self._marker_prefix = orphan_comment_prefix(marker_prefix=marker_prefix)

    @staticmethod
    def _maybe_decode(val: str | bytes | None, encoding: str = "utf-8") -> str | None:
        """Decode bytes to string if needed."""
        if val is None:
            return None
        if isinstance(val, bytes):
            return val.decode(encoding, errors="replace")
        return val

    async def get_partition_type(self, table_name: str) -> PartitionType | None:
        """Get partition type for a table."""
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT partstrat
                    FROM pg_partitioned_table t
                    WHERE t.partrelid = to_regclass(:table_name)
                    """
                ),
                {"table_name": to_regclass_argument(table_name)},
            )
            strat = self._maybe_decode(result.scalar(), encoding="ascii")

        if strat == "r":
            return PartitionType.RANGE
        if strat == "l":
            return PartitionType.LIST
        if strat == "h":
            return PartitionType.HASH
        return None

    async def get_partition_column(self, table_name: str) -> str | None:
        """Get partition column for a table.

        Raises:
            ValueError: If the table uses a composite (multi-column) partition
                key.  Only single-column keys are supported by this library.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT a.attname
                    FROM pg_partitioned_table t
                    JOIN pg_attribute a ON a.attrelid = t.partrelid AND a.attnum = ANY(t.partattrs)
                    WHERE t.partrelid = to_regclass(:table_name)
                    ORDER BY a.attnum
                    """
                ),
                {"table_name": to_regclass_argument(table_name)},
            )
            rows = result.fetchall()

        if not rows:
            return None

        if len(rows) > 1:
            cols = [r[0] for r in rows]
            msg = (
                f"Table {table_name!r} uses a composite partition key {cols!r}. "
                "Only single-column partition keys are supported."
            )
            raise ValueError(msg)

        return self._maybe_decode(rows[0][0])

    async def list_partitions(self, table_name: str) -> list[PartitionInfo]:
        """List all partitions for a table, including orphaned detached ones.

        Orphaned partitions are detached-but-not-dropped tables previously
        detached by this library. They are detected by a COMMENT marker set on
        successful detach and returned with ``is_attached=False`` and ``None``
        boundaries.
        """
        parent_schema, _ = split_qualified_name(table_name)
        want_qualified = parent_schema is not None

        async with self._engine.connect() as conn:
            # 1. Get partition type and canonical parent name in one query
            parent_info_result = await conn.execute(
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

            strat = self._maybe_decode(parent_row[0], encoding="ascii")
            parent_qualified = self._maybe_decode(parent_row[1]) or table_name

            partition_type: PartitionType | None = None
            if strat == "r":
                partition_type = PartitionType.RANGE
            elif strat == "l":
                partition_type = PartitionType.LIST
            elif strat == "h":
                partition_type = PartitionType.HASH

            if not partition_type:
                return []

            # 2. Get attached partitions
            attached_result = await conn.execute(
                text(
                    """
                    SELECT
                        ns.nspname AS partition_schema,
                        child.relname AS partition_name,
                        pg_get_expr(child.relpartbound, child.oid) AS boundaries,
                        child.relispartition AS is_attached
                    FROM pg_inherits inh
                    JOIN pg_class child ON inh.inhrelid = child.oid
                    JOIN pg_namespace ns ON child.relnamespace = ns.oid
                    WHERE inh.inhparent = to_regclass(:table_name)
                    ORDER BY ns.nspname, child.relname
                    """
                ),
                {"table_name": to_regclass_argument(table_name)},
            )
            attached_rows = attached_result.fetchall()

            # 3. Get orphan partitions
            orphan_result = await conn.execute(
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
                    WHERE c.relkind = 'r'
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
            relname: str | bytes = row.partition_name
            if isinstance(relname, bytes):
                relname = relname.decode("utf-8", errors="replace")

            part_schema: str | bytes = row.partition_schema
            if isinstance(part_schema, bytes):
                part_schema = part_schema.decode("utf-8", errors="replace")

            if not _is_addressable(part_schema, relname):
                continue

            name = qualify(part_schema if want_qualified else None, relname)

            boundaries = row.boundaries
            if isinstance(boundaries, bytes):
                boundaries = boundaries.decode("utf-8", errors="replace")

            boundaries_str = str(boundaries) if boundaries is not None else ""
            is_default = boundaries_str.strip().upper() == "DEFAULT"
            from_val, to_val = (None, None) if is_default else self._parse_boundaries(boundaries_str)
            partitions.append(
                PartitionInfo(
                    name=name,
                    partition_type=partition_type,
                    from_value=from_val,
                    to_value=to_val,
                    boundaries_expr=boundaries_str if boundaries_str else None,
                    is_attached=row.is_attached,
                    is_default=is_default,
                    parent_table=table_name,
                )
            )

        for row in orphan_rows:
            orphan_relname: str | bytes = row.partition_name
            if isinstance(orphan_relname, bytes):
                orphan_relname = orphan_relname.decode("utf-8", errors="replace")

            orphan_schema: str | bytes = row.partition_schema
            if isinstance(orphan_schema, bytes):
                orphan_schema = orphan_schema.decode("utf-8", errors="replace")

            if not _is_addressable(orphan_schema, orphan_relname):
                continue

            name = qualify(orphan_schema if want_qualified else None, orphan_relname)
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

    async def partition_exists(self, partition_name: str) -> bool:
        """Check if a partition table exists in pg_class.

        Args:
            partition_name: Partition table name.

        Returns:
            True if the table exists as a regular table.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_class
                        WHERE oid = to_regclass(:partition_name)
                          AND relkind = 'r'
                    )
                    """
                ),
                {"partition_name": to_regclass_argument(partition_name)},
            )
            return bool(result.scalar())

    async def is_partition_attached(self, table_name: str, partition_name: str) -> bool:
        """Check if a partition is currently attached to its parent via pg_inherits.

        Args:
            table_name: Parent table name.
            partition_name: Partition table name.

        Returns:
            True if the partition is attached.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_inherits inh
                        JOIN pg_class child ON inh.inhrelid = child.oid
                        WHERE inh.inhparent = to_regclass(:table_name)
                          AND inh.inhrelid = to_regclass(:partition_name)
                          AND child.relispartition = true
                    )
                    """
                ),
                {
                    "table_name": to_regclass_argument(table_name),
                    "partition_name": to_regclass_argument(partition_name),
                },
            )
            return bool(result.scalar())

    async def get_partition_boundaries(self, partition_name: str) -> tuple[str, str] | None:
        """Get partition boundaries.

        Args:
            partition_name: Partition table name.

        Returns:
            Tuple of (from_value, to_value) or None if not a range partition.
        """
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
            boundaries_expr = result.scalar()

        if not boundaries_expr:
            return None

        if isinstance(boundaries_expr, bytes):
            boundaries_expr = boundaries_expr.decode("utf-8", errors="replace")

        from_val, to_val = self._parse_boundaries(boundaries_expr)
        if from_val is not None and to_val is not None:
            return from_val, to_val

        return None

    def _parse_boundaries(self, boundaries_expr: str | None) -> tuple[str | None, str | None]:
        """Parse boundary values from pg_get_expr output.

        ``pg_get_expr(relpartbound, oid)`` can include casts and varying
        whitespace depending on PostgreSQL version and the partition key type.
        This parser aims to extract stable "boundary values" for the most common
        cases, without trying to fully parse SQL expressions.

        Examples:
          FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
          FOR VALUES FROM ('2024-01-01'::date) TO ('2024-02-01'::date)
          FOR VALUES FROM (1::bigint) TO (5::bigint)
          FOR VALUES FROM (MINVALUE) TO (MAXVALUE)
        """
        if not boundaries_expr:
            return None, None

        if boundaries_expr.strip().upper() == "DEFAULT":
            return None, None

        # Only RANGE bounds carry FROM/TO values; LIST/HASH expressions could
        # contain a ") TO (" inside a string value and must not be mis-parsed.
        if not _RANGE_BOUND_PATTERN.match(boundaries_expr):
            return None, None

        # Split into FROM and TO parts by finding ") TO (" which is the most
        # reliable separator for range boundaries.
        parts = _BOUNDARY_SEP_PATTERN.split(boundaries_expr)
        if len(parts) != 2:
            return None, None

        from_part = parts[0]
        to_part = parts[1]

        # Strip "FOR VALUES FROM (" prefix from the first part
        from_part = _FROM_PREFIX_PATTERN.sub("", from_part)
        # Strip trailing ")" from the second part
        to_part = _TRAILING_PAREN_PATTERN.sub("", to_part)

        def _strip_outer_parens(s: str) -> str:
            s = s.strip()
            # pg_get_expr sometimes wraps constants in extra parentheses
            while s.startswith("(") and s.endswith(")") and len(s) > 1:
                s = s[1:-1].strip()
            return s

        def _normalize(expr: str) -> str:
            expr = _strip_outer_parens(expr)

            # CAST(x AS type) → x
            cast_match = _CAST_PATTERN.match(expr)
            if cast_match:
                expr = _strip_outer_parens(cast_match.group("inner"))

            # Prefer extracting a string literal if present.
            str_match = _STR_LITERAL_PATTERN.search(expr)
            if str_match:
                return str_match.group("s").replace("''", "'")

            # Strip ::type casts.
            if "::" in expr:
                expr = expr.split("::", 1)[0].strip()

            return expr.strip()

        return _normalize(from_part), _normalize(to_part)

    async def get_default_partition(self, table_name: str) -> PartitionInfo | None:
        """Get DEFAULT partition for a table if it exists and is attached.

        Args:
            table_name: Parent table name.

        Returns:
            PartitionInfo with is_default=True, or None if no default partition exists.
        """
        all_partitions = await self.list_partitions(table_name)
        defaults = [p for p in all_partitions if p.is_default and p.is_attached]
        return defaults[0] if defaults else None
