"""Helper for managing foreign key constraints on partitions."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from sqlalchemy import text

from pg_partsmith.utils import quote_identifier, to_regclass_argument

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class PartitionForeignKeyManager:
    """Helper for managing foreign key constraints on partitions."""

    def __init__(self, engine: AsyncEngine, ddl_timeout: float) -> None:
        self._engine = engine
        self._ddl_timeout = ddl_timeout

    async def list_constraints(self, partition_name: str) -> list[str]:
        """List FK constraints defined ON the partition."""
        async with asyncio.timeout(self._ddl_timeout), self._engine.connect() as conn:
            return await self.list_constraints_conn(conn, partition_name)

    @staticmethod
    async def list_constraints_conn(conn: AsyncConnection, partition_name: str) -> list[str]:
        """List FK constraints using an existing connection."""
        result = await conn.execute(
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

    async def drop_constraints(self, conn: AsyncConnection, partition_name: str, names: list[str]) -> None:
        """Drop multiple FK constraints from a partition in a single statement."""
        if not names:
            return

        # PostgreSQL allows dropping multiple constraints in a single ALTER TABLE
        quoted_partition = quote_identifier(partition_name)
        clauses = [f"DROP CONSTRAINT IF EXISTS {quote_identifier(name)}" for name in names]
        stmt = f"ALTER TABLE {quoted_partition} {', '.join(clauses)}"
        await conn.execute(text(stmt))
