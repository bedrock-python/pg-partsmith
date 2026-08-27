"""Helper for managing foreign key constraints on partitions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from pg_partsmith.utils import coerce_str, quote_identifier, to_regclass_argument

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine


class PartitionForeignKeyManager:
    """Helper for managing foreign key constraints on partitions."""

    def __init__(self, engine: Engine, ddl_timeout: float) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout

    @staticmethod
    def list_constraints_conn(conn: Connection, partition_name: str) -> list[str]:
        """List FK constraints using an existing connection."""
        result = conn.execute(
            text(
                """
                SELECT con.conname
                FROM pg_constraint con
                WHERE con.conrelid = to_regclass(:partition_name)
                  AND con.contype = 'f'
                ORDER BY con.conname
                """
            ),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        return [coerce_str(row[0]) or "" for row in result.fetchall()]

    @staticmethod
    def drop_constraints(conn: Connection, partition_name: str, names: list[str]) -> None:
        """Drop multiple FK constraints from a partition in a single statement."""
        if not names:
            return

        # PostgreSQL allows dropping multiple constraints in a single ALTER TABLE
        quoted_partition = quote_identifier(partition_name)
        clauses = [f"DROP CONSTRAINT IF EXISTS {quote_identifier(name)}" for name in names]
        stmt = f"ALTER TABLE {quoted_partition} {', '.join(clauses)}"
        conn.execute(text(stmt))
