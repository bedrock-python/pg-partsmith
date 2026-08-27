"""Helper for resolving relation names and checking existence/attachment."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from pg_partsmith.catalog_queries import PARTITION_IS_ATTACHED_SQL, RELATION_EXISTS_SQL
from pg_partsmith.utils import coerce_str, to_regclass_argument

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine


class PartitionRelationResolver:
    """Helper for resolving relation names and checking existence/attachment."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def exists(self, partition_name: str) -> bool:
        """Check if a table exists in pg_class."""
        with self._engine.connect() as conn:
            return self.exists_conn(conn, partition_name)

    @staticmethod
    def exists_conn(conn: Connection, partition_name: str) -> bool:
        """Check existence using an existing connection.

        Accepts regular and partitioned tables — a partition may itself be
        subpartitioned.
        """
        result = conn.execute(
            text(RELATION_EXISTS_SQL),
            {"partition_name": to_regclass_argument(partition_name)},
        )
        return bool(result.scalar())

    def is_attached(self, table_name: str, partition_name: str) -> bool:
        """Check if partition is attached to parent."""
        with self._engine.connect() as conn:
            return self.is_attached_conn(conn, table_name, partition_name)

    @staticmethod
    def is_attached_conn(conn: Connection, table_name: str, partition_name: str) -> bool:
        """Check attachment using an existing connection."""
        result = conn.execute(
            text(PARTITION_IS_ATTACHED_SQL),
            {
                "table_name": to_regclass_argument(table_name),
                "partition_name": to_regclass_argument(partition_name),
            },
        )
        return bool(result.scalar())

    @staticmethod
    def resolve_fqn_conn(conn: Connection, name: str) -> str | None:
        """Resolve canonical ``schema.name`` using an existing connection."""
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
        return coerce_str(result.scalar())
