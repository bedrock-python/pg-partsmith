# pg-partsmith

PostgreSQL partition lifecycle management with extensible hooks.

Covers the full partition lifecycle: creating partitions ahead of time, detaching expired ones, and dropping orphans — with a middleware system for injecting custom logic at each step.

## Installation

```bash
pip install pg-partsmith

# With Redis distributed locks
pip install "pg-partsmith[redis-locks]"
```

**Requirements:** Python 3.11+, PostgreSQL 15+

## Key concepts

| Concept | Description |
|---------|-------------|
| **Period calculator** | Determines partition names, granularity, and range boundaries |
| **Lifecycle service** | Orchestrates create → detach → drop in order |
| **Lifecycle hooks** | Inject custom logic before/after each DDL step |
| **Lock manager** | Prevents concurrent maintenance runs |
| **Maintainer** | Scheduler-friendly wrapper with safe error handling |
| **Subpartition spec** | Splits each time partition along a second dimension (`RANGE(time) → HASH(column)`) |
| **Boundary codec** | Encodes periods into the partition key's own literals (UUIDv7, sortable ids) |

## Quick start

```python
from sqlalchemy.ext.asyncio import create_async_engine

from pg_partsmith import (
    MonthPeriodCalculator,
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.aio import (
    PartitionLifecycleService,
    PartitionMaintainer,
    PostgresAdvisoryLockManager,
    PostgresMetadataProvider,
    PostgresPartitionRepository,
)

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_type=PartitionType.RANGE,
    partition_strategy=PartitionStrategy.TIME_BASED,
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,  # current month + 2 ahead
    retention_count=12,
)


async def run_maintenance() -> None:
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine),
        locks=PostgresAdvisoryLockManager(engine),
        period_calculator=MonthPeriodCalculator(),
    )
    maintainer = PartitionMaintainer(service)
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

!!! note "Transaction semantics"
    Every DDL operation runs in its own connection and commits immediately.
    Pass `AsyncEngine` — not `AsyncSession`.

## Documentation

- [**Quick start**](guide/quickstart.md) — step-by-step walkthrough
- [**Configuration**](guide/configuration.md) — `TablePartitionConfig` reference
- [**Period strategies**](guide/strategies.md) — day / week / month / year + custom
- [**Lifecycle hooks**](guide/hooks.md) — inject logic at each step
- [**Lock managers**](guide/locks.md) — PostgreSQL advisory and Redis
- [**Advanced**](guide/advanced.md) — multi-schema, scheduler, safe drops, timezone semantics
- [**API Reference**](reference/index.md) — auto-generated from source
