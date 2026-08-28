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

```bash
pip install "pg-partsmith[redis-locks]"
```

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
etcd, a lease table):

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

## Who takes the lock

| Call | Lock |
|---|---|
| `service.plan()`, `service.inspect()` | none — read-only |
| `service.apply()` | the table's lock, for the duration of the apply |
| `service.maintain()` / `maintain_lifecycle()` / the maintainer | one lock around plan **and** apply |
| `service.reconcile()`, `ensure_partition(s)`, `create_future_partitions`, `detach_old_partitions`, `drop_detached_partitions` | none — hold `locks.acquire_lock(table)` yourself when orchestrating by hand |

Acquisition is non-blocking (`pg_try_advisory_lock` / Redis `SET NX`): a tick that
collides with another replica raises `LockAcquisitionError` immediately instead of
queueing. Scheduled jobs typically catch it and skip the tick; the plan is recomputed from
the catalog on the next one, so nothing is lost.

The PostgreSQL manager holds the lock on a dedicated AUTOCOMMIT connection, so it
survives commits/rollbacks on the caller's session and can safely span
`DETACH PARTITION CONCURRENTLY`, which cannot run inside a transaction block.

## Two maintainers, one table

Two processes running `maintain()` on the same config either serialize (Redis lease
expiry, retried ticks) or one loses the lock and skips. Without a lock manager that
actually excludes them, the library still does not corrupt the tree: a partition created by
the other worker is recognised by its bounds as a lost race, not treated as a conflict, and
every destructive operation is revalidated against the catalog before it runs.
