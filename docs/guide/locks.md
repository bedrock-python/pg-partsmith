# Lock managers

Lock managers prevent concurrent maintenance runs from conflicting with each other.

## PostgreSQL advisory locks (default)

No extra dependencies. Uses PostgreSQL session-level advisory locks tied to a hash of
the table name.

```python
from pg_partsmith.aio import PostgresAdvisoryLockManager

locks = PostgresAdvisoryLockManager(engine, prefix="myapp")
```

!!! warning "Pool sizing"
    Advisory locks hold a dedicated database connection for the full duration of maintenance.
    Make sure your SQLAlchemy pool has spare capacity for DDL operations, or use a separate
    `AsyncEngine` for the lock manager. A pool size of 1 will deadlock.

## Redis distributed locks

For multi-process deployments or when you want an external lock store.

**Install:**
```bash
pip install "pg-partsmith[redis-locks]"
```

**Usage:**
```python
from redis.asyncio import Redis
from pg_partsmith.aio import RedisDistributedLockManager

locks = RedisDistributedLockManager(
    redis_client=Redis.from_url("redis://localhost"),
    prefix="myapp:partitioner",
    ttl_seconds=300,
)
```

## Custom lock manager

Implement the `LockManager` protocol to use any other locking backend (Zookeeper,
etcd, etc.):

```python
from contextlib import asynccontextmanager
from pg_partsmith.aio.protocols import LockManager


class ZookeeperLockManager:
    @asynccontextmanager
    async def acquire_lock(self, table_name: str):
        async with self._zk.lock(f"/partsmith/{table_name}"):
            yield

    async def is_locked(self, table_name: str) -> bool:
        return await self._zk.exists(f"/partsmith/{table_name}") is not None
```

`LockManager`, `PartitionRepository`, `PartitionMetadataProvider`, and
`PartitionLifecycleHooks` are all `@runtime_checkable`, so you can verify a
custom implementation satisfies the protocol with `isinstance()`.

## Lock scope

Each lock is scoped to `{prefix}:{schema}.{table_name}`, so multiple tables can be
maintained in parallel without blocking each other.
