"""Helper for partition creation and attachment."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.entities import PartitionInfo
from pg_partsmith.exceptions import PartitionAlreadyExistsError
from pg_partsmith.utils import build_ddl_statement, pg_sqlstate, qualify, quote_identifier, quote_literal

from .timeouts import apply_local_statement_timeout

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from pg_partsmith.entities import TablePartitionConfig


class PartitionCreator:
    """Helper for partition creation and attachment."""

    def __init__(self, engine: Engine, ddl_timeout: float, ddl_timezone: str | None = None) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout
        self._ddl_timezone = ddl_timezone

    def create(
        self, config: TablePartitionConfig, partition_name: str, from_value: str, to_value: str
    ) -> PartitionInfo:
        """Create a new partition table."""
        stmt = build_ddl_statement(
            "CREATE TABLE {partition} (LIKE {parent} INCLUDING ALL)",
            partition=partition_name,
            parent=qualify(config.db_schema, config.table_name),
        )
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            try:
                conn.execute(stmt)
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

    def attach(self, table_name: str, partition_name: str, from_value: str, to_value: str) -> None:
        """Attach partition to parent table."""
        stmt = build_ddl_statement(
            "ALTER TABLE {parent} ATTACH PARTITION {partition} FOR VALUES FROM ([from_val]) TO ([to_val])",
            parent=table_name,
            partition=partition_name,
            from_val=from_value,
            to_val=to_value,
        )
        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            if self._ddl_timezone is not None:
                conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            conn.execute(stmt)

    def reconcile_default_rows(
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

        with self._engine.begin() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
            # Boundary literals must be interpreted in the same timezone ATTACH uses,
            # otherwise a non-UTC server timezone moves the wrong row range.
            if self._ddl_timezone is not None:
                conn.execute(text(f"SET LOCAL TIME ZONE {quote_literal(self._ddl_timezone)}"))
            # Lock both tables to minimize race conditions
            conn.execute(text(f"LOCK TABLE {default_quoted} IN SHARE ROW EXCLUSIVE MODE"))
            conn.execute(text(f"LOCK TABLE {target_quoted} IN SHARE ROW EXCLUSIVE MODE"))

            result = conn.execute(move_sql)
            moved_count = result.rowcount or 0

        return moved_count
