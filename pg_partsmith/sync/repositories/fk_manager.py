"""Helper for managing foreign key constraints on partitions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from pg_partsmith.utils import quote_identifier, to_regclass_argument

from .timeouts import apply_local_statement_timeout

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine


class PartitionForeignKeyManager:
    """Helper for managing foreign key constraints on partitions."""

    def __init__(self, engine: Engine, ddl_timeout: float) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout

    def list_constraints(self, partition_name: str) -> list[str]:
        """List FK constraints defined ON the partition."""
        with self._engine.connect() as conn:
            apply_local_statement_timeout(conn, self._ddl_timeout)
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
            rows = result.fetchall()
        names: list[str] = []
        for row in rows:
            conname = row[0]
            if isinstance(conname, bytes):
                names.append(conname.decode("utf-8", errors="replace"))
            else:
                names.append(str(conname))
        return names

    def drop_constraints(self, conn: Connection, partition_name: str, names: list[str]) -> None:
        """Drop multiple FK constraints from a partition in a single statement."""
        if not names:
            return

        # PostgreSQL allows dropping multiple constraints in a single ALTER TABLE
        quoted_partition = quote_identifier(partition_name)
        clauses = [f"DROP CONSTRAINT IF EXISTS {quote_identifier(name)}" for name in names]
        stmt = f"ALTER TABLE {quoted_partition} {', '.join(clauses)}"
        conn.execute(text(stmt))
