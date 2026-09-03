# Schedule maintenance

pg-partsmith runs when you call it. This guide covers what to call, how often, from how
many places, and how to keep replicas from colliding.

## What to call

`PartitionMaintainer.run_maintenance_safe(config)` is the entry point for anything
scheduled: it plans and applies under the table's lock and **never raises** — a failure
comes back on `result.error`.

```python
from pg_partsmith.aio import PartitionMaintainer

maintainer = PartitionMaintainer(service)
result = await maintainer.run_maintenance_safe(config)
```

`maintain_partitions(maintainer, config)` is the same call as a plain function, for
schedulers that want one:

```python
from pg_partsmith.aio import maintain_partitions

scheduler.add_job(maintain_partitions, "cron", hour=2, kwargs={"maintainer": maintainer, "config": config})
```

Use `run_maintenance()` instead when you *want* exceptions to propagate — in a script you
run by hand, for instance.

## How often

Often enough that the partitions created ahead outlast the longest gap between ticks.
A row with no partition to go to is rejected by PostgreSQL, not buffered.

| Granularity | Typical tick | With `create_ahead_count` |
|---|---|---|
| hour | every 15 minutes | 24 or more |
| day | hourly | 3–7 |
| week / month | daily | 2–3 |
| integer steps (a queue) | proportional to the insert rate | enough windows to cover several ticks of inserts |

There is no reason to be stingy: a tick against a converged table costs one catalog
round-trip and no DDL.

## Where

=== "APScheduler"

    ```python
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    scheduler = AsyncIOScheduler()
    for table_config in CONFIGS:
        scheduler.add_job(
            maintainer.run_maintenance_safe, "cron", hour=2, minute=15,
            args=[table_config], id=f"partitions:{table_config.qualified_name}",
        )
    scheduler.start()
    ```

=== "Celery beat"

    ```python
    from pg_partsmith.sync import PartitionLifecycleService, PartitionMaintainer

    @app.task
    def maintain_partitions() -> None:
        maintainer = PartitionMaintainer(PartitionLifecycleService(repo, metadata, locks))
        for table_config in CONFIGS:
            report(table_config, maintainer.run_maintenance_safe(table_config))

    app.conf.beat_schedule = {
        "partitions": {"task": "maintain_partitions", "schedule": crontab(hour=2, minute=15)},
    }
    ```

=== "Kubernetes CronJob / cron"

    ```python
    # maintain.py — exit non-zero when any run failed, so the job shows red
    import asyncio, sys

    async def main() -> int:
        failed = 0
        for table_config in CONFIGS:
            result = await maintainer.run_maintenance_safe(table_config)
            report(table_config, result)
            failed += not result.success
        await engine.dispose()
        return 1 if failed else 0

    sys.exit(asyncio.run(main()))
    ```

=== "Application start-up"

    ```python
    @app.on_event("startup")
    async def ensure_partitions() -> None:
        # every replica tries; the ones that lose the lock skip
        await maintainer.run_maintenance_safe(events_config)
    ```

## Replicas and locks

Every tick takes the table's lock through the **lock manager**. Acquisition is
non-blocking: a replica that finds the lock taken gets `LockAcquisitionError` at once and
skips the tick — another replica is doing the work, and the next tick plans from the
catalog again.

```text
success=False
error=LockAcquisitionError: Failed to acquire lock for table public.events: advisory lock unavailable
```

Treat that as "skipped", not as a failure.

### PostgreSQL advisory locks

The default; no infrastructure beyond the database. A session-level advisory lock keyed
on the table name, held on a dedicated autocommit connection for the length of the run
(so it survives the statements' own commits and `DETACH CONCURRENTLY`, which cannot run in
a transaction).

```python
from pg_partsmith.aio import PostgresAdvisoryLockManager

locks = PostgresAdvisoryLockManager(engine, prefix="app")
```

`prefix` separates deployments sharing a database. Give the engine a pool of at least
two connections, or a separate engine for the lock manager.

### Redis

For deployments that already run Redis and want a lease with a TTL:

```bash
pip install "pg-partsmith[redis-locks]"
```

```python
from redis.asyncio import Redis

from pg_partsmith.aio import RedisDistributedLockManager

locks = RedisDistributedLockManager(Redis.from_url("redis://cache:6379"), prefix="app:partitions", ttl_seconds=300)
```

redis-py 8 speaks RESP3 on the wire by default; a Redis server older than 6 wants
`redis://cache:6379?protocol=2`.

The async manager renews the lease while the run lasts and cancels the run if the lease
is lost; the sync one renews and warns.

### Locks are scoped per table

`{prefix}:{schema}.{table}` — tables are maintained in parallel without blocking each
other. Which calls take the lock:

| Call | Lock |
|---|---|
| `plan()`, `inspect()` | none — read-only |
| `apply()`, `maintain()`, the maintainer, `partition_data()`, `unpartition()` | the table's lock |
| `reconcile()`, `ensure_partition(s)`, `create_future_partitions()`, `detach_old_partitions()`, `drop_detached_partitions()` | none — hold `locks.acquire_lock(table)` yourself when orchestrating by hand |

### Without a lock

Two maintainers on one table cannot corrupt it even if the lock fails: a partition the
other one created first is recognised by its bounds as a lost race, and every detach and
drop is revalidated against the catalog before it runs. The lock saves wasted work.

## Cancellation

A run cancelled mid-way — a pod killed, a task cancelled — leaves at most a detached,
unattached table (attach is the last step) or a marked, half-detached partition; the next
tick converges both. `run_maintenance_safe()` reports the cancellation on `result.error`
and returns normally; `run_maintenance()` re-raises it.
