# Quick start

This guide walks through setting up partition maintenance for an `events` table partitioned by month.

## 1. Create the partitioned table

```sql
CREATE TABLE events (
    id          BIGSERIAL,
    created_at  TIMESTAMPTZ NOT NULL,
    payload     JSONB
) PARTITION BY RANGE (created_at);
```

## 2. Install pg-partsmith

```bash
pip install pg-partsmith
```

## 3. Define the configuration

```python
from pg_partsmith import PartitionGranularity, TablePartitionConfig

config = TablePartitionConfig(
    schema="public",           # optional but strongly recommended
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,      # current month + next 2
    retention_count=12,        # keep 12 months of data
)
```

## 4. Build the service

```python
from sqlalchemy.ext.asyncio import create_async_engine
from pg_partsmith.aio import (
    PartitionLifecycleService,
    PartitionMaintainer,
    PostgresAdvisoryLockManager,
    PostgresMetadataProvider,
    PostgresPartitionRepository,
)

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")

service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine),
    locks=PostgresAdvisoryLockManager(engine),
)
maintainer = PartitionMaintainer(service)
```

## 5. Look before you leap

```python
plan = await service.plan(config)
print(plan.describe())
```

```text
plan for public.events at 2026-08-28T10:00:00+00:00
  CREATE public.events__2026_08 (create_ahead)
  CREATE public.events__2026_09 (create_ahead)
  CREATE public.events__2026_10 (create_ahead)
```

`plan()` reads the catalog and issues no DDL. Every operation says why it is there; every
finding says what was left alone and why.

## 6. Run maintenance

```python
result = await maintainer.run_maintenance_safe(config)

if result.success:
    print(f"created={result.created_count} detached={result.detached_count} dropped={result.dropped_count}")
    for issue in result.issues:
        print("needs attention:", issue.partition_name, issue.error)
else:
    print(f"error={result.error}")
```

`run_maintenance_safe()` never raises — it returns a `MaintenanceResult` even on
`asyncio.CancelledError`. Use `run_maintenance()` if you want exceptions to propagate.
`result.plan` is the plan that was executed.

## 7. Schedule it

```python
from pg_partsmith.aio import maintain_partitions

# APScheduler example
scheduler.add_job(
    maintain_partitions,
    "cron",
    hour=2,
    kwargs={"maintainer": maintainer, "config": config},
)
```

## Sync variant

Every class above has a synchronous twin in `pg_partsmith.sync` with the same name and
API. Build against the classic SQLAlchemy engine and drop the `await`:

```python
from sqlalchemy import create_engine
from pg_partsmith.sync import (
    PartitionLifecycleService,
    PartitionMaintainer,
    PostgresAdvisoryLockManager,
    PostgresMetadataProvider,
    PostgresPartitionRepository,
)

engine = create_engine("postgresql+psycopg2://user:pass@host/db")

service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine),
    locks=PostgresAdvisoryLockManager(engine),
)
maintainer = PartitionMaintainer(service)
result = maintainer.run_maintenance_safe(config)
```

## What happens each run

1. **Inspect** — one catalog round-trip reads the partition tree and the marker-tagged
   detached orphans.
2. **Plan** — which windows must exist ahead of now, which existing ones have expired,
   which orphans may be dropped; for nested schemes, which buckets are missing.
3. **Apply**, under the table's lock — create (subtree first, attach last), detach, drop.

Each run does only what is needed; a converged table costs zero DDL, so run it as often as
you like.

## Next steps

- [Configuration](configuration.md) — the flat and the composed spelling
- [Partition schemes](partition-schemes.md) — nesting, hash, list, composite keys
- [Lifecycle policies](lifecycle-policies.md) — horizons, ages, predicates, grace periods
- [Planning and dry runs](planning.md) — what the plan tells you
- [Migrating an existing partitioner](migration.md)
