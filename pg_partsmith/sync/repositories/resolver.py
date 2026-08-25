"""Helper for resolving relation names and checking existence/attachment."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from pg_partsmith.utils import to_regclass_argument

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine


class PartitionRelationResolver:
    """Helper for resolving relation names and checking existence/attachment."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def exists(self, name: str) -> bool:
        """Check if a table exists in pg_class."""
        with self._engine.connect() as conn:
            return self.exists_conn(conn, name)

    @staticmethod
    def exists_conn(conn: Connection, name: str) -> bool:
        """Check existence using an existing connection.

        Accepts regular and partitioned tables — a partition may itself be
        subpartitioned.
        """
        result = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_class
                    WHERE oid = to_regclass(:partition_name)
                      AND relkind IN ('r', 'p')
                )
                """
            ),
            {"partition_name": to_regclass_argument(name)},
        )
        return bool(result.scalar())

    def is_attached(self, parent: str, child: str) -> bool:
        """Check if partition is attached to parent."""
        with self._engine.connect() as conn:
            return self.is_attached_conn(conn, parent, child)

    @staticmethod
    def is_attached_conn(conn: Connection, parent: str, child: str) -> bool:
        """Check attachment using an existing connection."""
        result = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_inherits inh
                    JOIN pg_class c ON inh.inhrelid = c.oid
                    WHERE inh.inhparent = to_regclass(:table_name)
                      AND inh.inhrelid = to_regclass(:partition_name)
                      AND c.relispartition = true
                )
                """
            ),
            {"table_name": to_regclass_argument(parent), "partition_name": to_regclass_argument(child)},
        )
        return bool(result.scalar())

    def resolve_fqn(self, name: str) -> str | None:
        """Resolve canonical schema.name for a relation."""
        with self._engine.connect() as conn:
            return self.resolve_fqn_conn(conn, name)

    @staticmethod
    def resolve_fqn_conn(conn: Connection, name: str) -> str | None:
        """Resolve FQN using an existing connection."""
        result = conn.execute(
            text(
                """
                SELECT ns.nspname || '.' || c.relname
                FROM pg_class c
                JOIN pg_namespace ns ON c.relnamespace = ns.oid
                WHERE c.oid = to_regclass(:name)
                """
            ),
            {"name": to_regclass_argument(name)},
        )
        value = result.scalar()
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)
