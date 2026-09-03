# Tutorial: running it in production

The [first tutorial](first-table.md) ran `plan()` and `apply()` by hand. A production
setup runs them on a schedule, from several replicas, and tells you when something needs
attention. This tutorial builds that: a maintainer, a scheduled tick, a lock that keeps
replicas from colliding, and a result you can alert on.

## 1. The maintainer

`PartitionMaintainer` wraps the service for schedulers: `run_maintenance_safe()` plans and
applies under the table's lock, times the run, and **never raises** — whatever happens
comes back in the result.

```python
from pg_partsmith.aio import PartitionMaintainer

maintainer = PartitionMaintainer(service)

result = await maintainer.run_maintenance_safe(config)
```

```python
result.success          # False only when the run failed as a whole
result.error            # "InvalidPartitionConfigError: …", "LockAcquisitionError: …", or None
result.created_count    # partitions created directly under the root
result.detached_count
result.dropped_count
result.issues           # things that went wrong or were refused, one per partition
result.duration_ms
result.plan             # the MaintenancePlan that was executed
```

A run that could not even start looks like this:

```text
success=False
error=InvalidPartitionConfigError: Invalid partition configuration: Partition column mismatch for table 'public.events': config='occurred_at' actual='created_at'
```

## 2. A tick

A tick is one call per table. Put it wherever your scheduled work already lives —
APScheduler, Celery beat, a Kubernetes CronJob, a `while True: sleep` loop in a worker, an
application start-up hook. pg-partsmith is not a scheduler and does not want to be one.

=== "APScheduler"

    ```python
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    scheduler = AsyncIOScheduler()

    for table_config in (events_config, audit_config):
        scheduler.add_job(
            maintainer.run_maintenance_safe,
            "cron",
            hour=2,
            minute=15,
            args=[table_config],
            id=f"partitions:{table_config.qualified_name}",
        )
    ```

=== "Celery beat"

    ```python
    @app.task
    def maintain_partitions() -> dict[str, int]:
        service = PartitionLifecycleService(repo, metadata, locks)      # the sync mirror
        maintainer = PartitionMaintainer(service)
        counts = {}
        for table_config in (events_config, audit_config):
            result = maintainer.run_maintenance_safe(table_config)
            counts[table_config.qualified_name] = result.created_count
            report(table_config, result)
        return counts

    app.conf.beat_schedule = {"partitions": {"task": "maintain_partitions", "schedule": crontab(hour=2, minute=15)}}
    ```

=== "Kubernetes CronJob"

    ```python
    # maintain.py -- run by a CronJob; exits non-zero when a run fails
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

How often? Often enough that `create_ahead_count` covers the longest gap between ticks
with margin. Daily is the common choice for monthly and weekly tables; hourly tables want
an hourly tick. A tick that finds nothing to do costs one catalog round-trip and no DDL, so
there is no reason to be stingy.

## 3. Replicas and the lock

Two replicas running the same tick must not both create `events__2027_09`. The lock
manager serialises them per table, and acquisition is **non-blocking**: the replica that
loses does not wait, it gets `LockAcquisitionError` and skips — the other one is doing the
work.

```text
success=False
error=LockAcquisitionError: Failed to acquire lock for table public.events: advisory lock unavailable
```

That is a normal outcome, not an incident. Treat it as "skipped this tick"; the next tick
plans from the catalog again and nothing is lost.

`PostgresAdvisoryLockManager` needs no infrastructure: a session-level advisory lock held
on a dedicated connection for the length of the run. Across processes that share a
database it is all you need. `RedisDistributedLockManager` is for deployments that already
run Redis and want a lease with a TTL:

```python
from redis.asyncio import Redis

from pg_partsmith.aio import RedisDistributedLockManager

locks = RedisDistributedLockManager(Redis.from_url("redis://cache:6379"), prefix="app:partitions", ttl_seconds=300)
```

redis-py 8 speaks RESP3 on the wire by default; a Redis server older than 6 wants
`redis://cache:6379?protocol=2`.

Even without a working lock two maintainers cannot corrupt the tree: a partition the
other one created first is recognised by its bounds as a lost race, and every detach and
drop is re-checked against the catalog before it runs. The lock saves wasted work; safety
does not depend on it.

## 4. Reading the result

Three fields carry the signal.

**`result.error`** means the run failed as a whole: a configuration that does not match
the table, the lock, a connection error. Alert on it.

**`result.issues`** are per-partition problems in a run that otherwise went through.
Each has a `step` (`create`, `reconcile`, `attach`, `detach`, `drop`, `move`), the
`partition_name`, and an `error` string. Two kinds land here:

- what the planner *refused* to do and thinks you should know about — a wanted window
  that overlaps a partition it does not own, a hash set with a gap it cannot repair
  safely, a DEFAULT partition holding rows in the way;
- what PostgreSQL refused at execution time — a detach blocked by a foreign key, a name
  taken by another relation.

```text
reconcile: public.events: PartitionTopologyError: public.events needs a partition for 2028_03 but public.events_oddweeks already covers part of it with bounds the scheme did not produce; creating it would fail, and detaching the other is not this library's decision.
```

Alert on issues too, but expect them to repeat until someone acts: the same finding comes
back every tick, by design.

**`result.plan.findings`** is everything the planner noticed, including the harmless
steady states (`grace_pending`, `unmanaged_partition`, `legacy_leaf`) that are kept out of
`issues`. Log them at debug level; they are the full story of a tick.

A reporting function that covers all of it:

```python
import logging

log = logging.getLogger("partitions")


def report(config: TablePartitionConfig, result: MaintenanceResult) -> None:
    table = config.qualified_name
    if not result.success:
        log.error("partition maintenance failed", extra={"table": table, "error": result.error})
        return
    log.info(
        "partition maintenance done",
        extra={
            "table": table,
            "created": result.created_count,
            "detached": result.detached_count,
            "dropped": result.dropped_count,
            "duration_ms": result.duration_ms,
        },
    )
    for issue in result.issues:
        log.warning("partition needs attention", extra={"table": table, "step": issue.step.value, "partition": issue.partition_name, "error": issue.error})
    if result.plan is not None:
        for finding in result.plan.findings:
            log.debug(finding.detail, extra={"table": table, "reason": finding.reason.value, "severity": finding.severity.value})
```

## 5. Keep going on a partial failure

By default a failing step aborts the run: if the create of one partition raises, the
detaches after it do not happen. On a table where the two are independent you may prefer
to isolate failures:

```python
result = await maintainer.run_maintenance_safe(config, continue_on_error=True)
```

Every failed step then lands in `result.issues` and the next operation runs — a failed
create still prunes, which may free the space the create needed. Validation and lock
failures stay fatal either way, and topology refusals are always recorded rather than
raised.

## 6. Dry runs in CI

Because `plan()` needs neither lock nor DDL, it makes a good check against a staging copy
or a freshly migrated schema:

```python
plan = await service.plan(config)
assert not plan.actionable_findings, plan.describe()
```

`actionable_findings` are the `warning`-severity ones — the same set that reaches
`result.issues` on a real run.

## What you have learned

- `PartitionMaintainer.run_maintenance_safe()` is the entry point for a scheduler: it never
  raises, and everything is on the result.
- One tick per table per replica; the loser of the lock skips, and that is fine.
- `error` is the run, `issues` is what needs a human, `plan.findings` is the full account.

## Next

[Tutorial: a multi-tenant event store →](event-store.md) — weekly partitions over a
UUIDv7 key, each split by tenant, with a history that is not uniform.
