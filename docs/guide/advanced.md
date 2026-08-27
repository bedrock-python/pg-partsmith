# Advanced

## Multi-schema databases

Set `schema` in `TablePartitionConfig` to make all operations schema-qualified. This
prevents cross-schema ambiguity and makes behaviour independent of `search_path`.

```python
config = TablePartitionConfig(
    schema="analytics",
    table_name="events",
    ...
)
```

When `schema` is set, the library schema-qualifies all catalog queries, DDL statements,
and lock namespaces.

Set `marker_prefix` consistently across `PostgresPartitionRepository` and
`PostgresMetadataProvider` when working with multiple schemas or deployments:

```python
repo = PostgresPartitionRepository(engine, marker_prefix="myapp")
metadata = PostgresMetadataProvider(engine, marker_prefix="myapp")
```

## Orphan partitions

When a partition is detached, the repository writes a `COMMENT` marker on the detached
table. Only marker-tagged tables are treated as orphan partitions eligible for dropping.
This makes retention cleanup safe even if the database contains similarly named tables
not managed by pg-partsmith.

## DEFAULT partition reconciliation

When creating a new partition, if the DEFAULT partition contains rows belonging to the
new partition's range, pg-partsmith automatically:

1. Detects the conflict (`CheckViolationError 23514`)
2. Moves conflicting rows from DEFAULT to the new partition
3. Retries the `ATTACH PARTITION`

The reconciliation runs in a single transaction and is logged at `INFO` level.

## TIMESTAMPTZ boundary semantics

For `TIMESTAMP WITH TIME ZONE` partition keys, PostgreSQL interprets boundary literals
(e.g. `'2024-01-01'`) using the session `TimeZone`. pg-partsmith defines all boundaries
as **UTC period starts** and enforces this at DDL time: `PostgresPartitionRepository`
runs `SET LOCAL TimeZone='UTC'` before `ATTACH PARTITION` by default
(`ddl_timezone="UTC"`).

To disable UTC enforcement:
```python
repo = PostgresPartitionRepository(engine, ddl_timezone=None)
```

## Safe drops

`drop_partition()` refuses to drop any table that is not a marker-tagged orphan. This
prevents accidental drops of tables not managed by pg-partsmith.

To override (not recommended):
```python
repo = PostgresPartitionRepository(engine, drop_allow_unmanaged=True)
```

Legacy tables detached by a previous partitioner are better handled with
`repo.adopt_partition(table_name, partition_name)` — it stamps the marker once so the
normal safe path applies, instead of disabling the guard for every future drop. See
[Migrating an existing partitioner](migration.md).

An attempt to drop an unmanaged table raises `UnmanagedPartitionDropError`.
Attempting to drop an attached partition raises `PartitionAttachedError` (which
`PartitionLifecycleService` treats as a warning and skips).

## Scheduler integration

`maintain_partitions` is a plain async function ready for any scheduler:

```python
from pg_partsmith.aio import maintain_partitions

# APScheduler
scheduler.add_job(
    maintain_partitions,
    "cron",
    hour=2,
    kwargs={"maintainer": maintainer, "config": config},
)

# Celery Beat
@app.task
async def run_partition_maintenance():
    await maintain_partitions(maintainer=maintainer, config=config)
```

`maintain_partitions` always returns `MaintenanceResult` — it never raises, including
on `asyncio.CancelledError`.

## Cancellation semantics

| Method | On exception |
|--------|-------------|
| `PartitionMaintainer.run_maintenance()` | Logs and re-raises (including `CancelledError`) |
| `PartitionMaintainer.run_maintenance_safe()` | Always returns `MaintenanceResult`; cancellation reported via `result.error` |
| `maintain_partitions()` | Same as `run_maintenance_safe()` |

Use `run_maintenance_safe()` / `maintain_partitions()` in schedulers; use
`run_maintenance()` when you want to propagate exceptions to the caller.
