"""Helper for resolving relation names and checking existence/attachment."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from pg_partsmith.utils import to_regclass_argument

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class PartitionRelationResolver:
    """Helper for resolving relation names and checking existence/attachment."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def exists(self, name: str) -> bool:
        """Check if a table exists in pg_class."""
        async with self._engine.connect() as conn:
            return await self.exists_conn(conn, name)

    @staticmethod
    async def exists_conn(conn: AsyncConnection, name: str) -> bool:
        """Check existence using an existing connection."""
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
            {"partition_name": to_regclass_argument(name)},
        )
        return bool(result.scalar())

    async def is_attached(self, parent: str, child: str) -> bool:
        """Check if partition is attached to parent."""
        async with self._engine.connect() as conn:
            return await self.is_attached_conn(conn, parent, child)

    @staticmethod
    async def is_attached_conn(conn: AsyncConnection, parent: str, child: str) -> bool:
        """Check attachment using an existing connection."""
        result = await conn.execute(
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

    async def resolve_fqn(self, name: str) -> str | None:
        """Resolve canonical schema.name for a relation."""
        async with self._engine.connect() as conn:
            return await self.resolve_fqn_conn(conn, name)

    @staticmethod
    async def resolve_fqn_conn(conn: AsyncConnection, name: str) -> str | None:
        """Resolve FQN using an existing connection."""
        result = await conn.execute(
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
