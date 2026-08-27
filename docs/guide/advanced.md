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

## Timezone semantics

By default everything happens in UTC: partition names encode UTC periods, boundaries are
**UTC period starts**, and pruning compares UTC instants. Three layers are involved, and
they stay aligned through one knob:

1. **Period computation** — each calculator works in a timezone (`tz=datetime.UTC` by
   default): "now" for the current period, and the meaning of naive boundary literals.
2. **DDL** — for `TIMESTAMP WITH TIME ZONE` partition keys PostgreSQL interprets naive
   boundary literals (e.g. `'2024-01-01'`) in the session `TimeZone`, so
   `PostgresPartitionRepository` runs `SET LOCAL TIME ZONE '<ddl_timezone>'` in the same
   transaction as `ATTACH PARTITION` and DEFAULT reconciliation (`ddl_timezone="UTC"` by
   default).
3. **Pruning** — naive boundaries read back from the catalog are interpreted in the
   calculator's timezone before being compared as UTC instants.

### Local-time partitions

For calendar partitions in a business timezone, pass a `ZoneInfo` to the calculator and
the **same** zone name to the repository:

```python
from zoneinfo import ZoneInfo

calc = MonthPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))
repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Moscow")
service = PartitionLifecycleService(repo=repo, ..., period_calculator=calc)
```

`events__2024_01` then means January in Moscow time and its real bounds are Moscow
midnights. Only `datetime.UTC` and keyed `ZoneInfo` objects are accepted — the zone must
have an IANA name usable in `SET LOCAL TIME ZONE`.

`PartitionLifecycleService` **refuses a mismatched pair**: a calculator in one zone with
`ddl_timezone` in another raises `ValueError` at construction — the silent failure mode
where names and real bounds drift apart cannot happen.

### Caveats

- **Hourly granularity is UTC-only.** In a DST zone a local hour can repeat or vanish,
  making `table__YYYY_MM_DD_HH` names ambiguous; `HourPeriodCalculator` raises for any
  `tz` other than UTC.
- **Zones whose midnight can be skipped by DST** (e.g. `America/Santiago`): the naive
  literal is resolved by PostgreSQL under `SET LOCAL TIME ZONE`, which shifts a
  non-existent midnight forward — boundaries stay well-defined and contiguous.
- **`ddl_timezone=None`** trusts the session timezone as-is. In this mode the library
  cannot guarantee that names and real bounds agree (a pooled connection may carry any
  `TimeZone`); pruning still interprets naive boundaries in the calculator's zone. The
  service logs a warning when a non-UTC calculator is combined with it.
- Existing partitions are not reinterpreted: boundaries stored with explicit offsets
  (the normal case for `timestamptz` keys) are converted exactly as before.

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
