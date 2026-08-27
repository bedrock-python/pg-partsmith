# Quick start

This guide walks through setting up partition maintenance for a `events` table partitioned by month.

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
from pg_partsmith import (
    MonthPeriodCalculator,
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)

config = TablePartitionConfig(
    schema="public",           # optional but strongly recommended
    table_name="events",
    partition_type=PartitionType.RANGE,
    partition_strategy=PartitionStrategy.TIME_BASED,
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
    period_calculator=MonthPeriodCalculator(),
)
maintainer = PartitionMaintainer(service)
```

## 5. Run maintenance

```python
result = await maintainer.run_maintenance_safe(config)

if result.success:
    print(
        f"created={result.created_count} "
        f"detached={result.detached_count} "
        f"dropped={result.dropped_count}"
    )
else:
    print(f"error={result.error}")
```

`run_maintenance_safe()` never raises — it returns a `MaintenanceResult` even on
`asyncio.CancelledError`. Use `run_maintenance()` if you want exceptions to propagate.

## 6. Schedule it

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
    period_calculator=MonthPeriodCalculator(),
)
maintainer = PartitionMaintainer(service)
result = maintainer.run_maintenance_safe(config)
```

## What happens each run

1. **Create** — ensures partitions exist for `create_ahead_count` periods starting from now.
2. **Detach** — detaches partitions older than `retention_count` periods.
3. **Drop** — drops previously detached (marker-tagged) orphan partitions.

Each step runs only what is needed; idempotent re-runs are safe.

## Next steps

- [Configuration](configuration.md) — full `TablePartitionConfig` reference
- [Lifecycle hooks](hooks.md) — react to create / detach / drop events
- [Advanced](advanced.md) — multi-schema, DEFAULT partition handling, timezone semantics
- [Migrating an existing partitioner](migration.md) — retention semantics, adopting legacy
  partitions, lock ownership, per-step error isolation
