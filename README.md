# pg-partsmith

PostgreSQL partition lifecycle management with extensible hooks.

[![PyPI](https://img.shields.io/pypi/v/pg-partsmith?color=blue)](https://pypi.org/project/pg-partsmith/)
[![Python](https://img.shields.io/pypi/pyversions/pg-partsmith)](https://pypi.org/project/pg-partsmith/)
[![License](https://img.shields.io/github/license/bedrock-python/pg-partsmith)](LICENSE)
[![CI](https://github.com/bedrock-python/pg-partsmith/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/bedrock-python/pg-partsmith/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bedrock-python/pg-partsmith/graph/badge.svg)](https://codecov.io/gh/bedrock-python/pg-partsmith)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://bedrock-python.github.io/pg-partsmith/)

A single library that covers the full PostgreSQL partition lifecycle: creating partitions ahead of time, detaching expired ones, and dropping orphans — with a middleware system for injecting custom logic at each step.

## Features

- **Async and sync** — `pg_partsmith.aio` on the SQLAlchemy async engine, `pg_partsmith.sync` on the classic sync engine
- **Full lifecycle** — create ahead, detach expired, drop orphans in one call
- **Extensible hooks** — 6 hook points (`before`/`after` create, detach, drop)
- **Multiple strategies** — daily, weekly, monthly, yearly + fully custom
- **Distributed locking** — PostgreSQL advisory locks (built-in) or Redis
- **Schema-aware** — multi-schema support, independent of `search_path`
- **Safe by default** — refuses to drop tables not managed by this library
- **Type-safe** — full mypy compliance with Pydantic models
- **Well-tested** — 90%+ coverage with real PostgreSQL via testcontainers

## Installation

```bash
pip install pg-partsmith

# With Redis distributed locks
pip install "pg-partsmith[redis-locks]"
```

**Requirements:** Python 3.11+, PostgreSQL 15+

## Quick start

```python
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

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
    create_ahead_count=3,  # current month + next 2
    retention_count=12,
)


async def run_maintenance(engine: AsyncEngine) -> None:
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

> **Transaction semantics** — every DDL operation (CREATE, ATTACH, DETACH, DROP)
> runs in its own connection and commits immediately. Use `AsyncEngine`, not `AsyncSession`.

> **Cancellation semantics** — `run_maintenance_safe()` (and `maintain_partitions()`)
> always returns `MaintenanceResult`, including on `asyncio.CancelledError`.

## Sync usage

Every class in `pg_partsmith.aio` has a synchronous twin in `pg_partsmith.sync` with the
same name and API — built on the classic SQLAlchemy `Engine` instead of `AsyncEngine`:

```python
from sqlalchemy import create_engine

from pg_partsmith import MonthPeriodCalculator
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

Hooks and custom lock managers implement the sync protocols from `pg_partsmith.sync`
(plain methods instead of coroutines). Two behavioural differences from the async package:

- `ddl_timeout_seconds` is enforced server-side via PostgreSQL `statement_timeout`
  (per statement) rather than client-side around the whole operation.
- The Redis lock renews its TTL from a background thread; on renewal failure it logs a
  warning but cannot cancel the running maintenance (the TTL bounds a stale holder).

## Multi-schema databases

If your database uses multiple schemas, set `schema` in `TablePartitionConfig`. The library
schema-qualifies all catalog queries, DDL statements, and lock namespaces — behaviour
becomes independent of `search_path`.

## Orphan partitions

After detach, the repository writes a `COMMENT` marker on the detached table.
Only marker-tagged tables are eligible for dropping, making cleanup safe even if the
database contains similarly named tables not managed by this library.

Set `marker_prefix` explicitly on both `PostgresPartitionRepository` and
`PostgresMetadataProvider` to ensure consistent orphan marker recognition across deployments.

## DEFAULT partition reconciliation

When creating a new partition, if the DEFAULT partition contains rows belonging to the
new range, pg-partsmith automatically:

1. Detects the conflict (`CheckViolationError 23514`)
2. Moves conflicting rows from DEFAULT to the new partition
3. Retries `ATTACH PARTITION`

The reconciliation is atomic and logged at `INFO` level.

## TIMESTAMPTZ boundary semantics

For `TIMESTAMP WITH TIME ZONE` partition keys, `PostgresPartitionRepository` runs
`SET LOCAL TimeZone='UTC'` before `ATTACH PARTITION` (`ddl_timezone="UTC"` default).
Set `ddl_timezone=None` to disable this enforcement.

## Safe drops

`drop_partition()` refuses to drop tables not tagged as orphans. To override:
```python
repo = PostgresPartitionRepository(engine, drop_allow_unmanaged=True)
```
An attempt to drop an unmanaged table raises `UnmanagedPartitionDropError`.

## Hooks (middleware)

```python
from pg_partsmith.aio import BasePartitionLifecycleHooks, PartitionLifecycleService
from pg_partsmith.entities import PartitionInfo, TablePartitionConfig


class KafkaNotifyHooks(BasePartitionLifecycleHooks):
    def __init__(self, producer: KafkaProducer) -> None:
        self._producer = producer

    async def after_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
        await self._producer.send("partition.created", {"name": partition.name})

    async def before_drop(self, table_name: str, partition_name: str) -> None:
        await export_to_cold_storage(table_name, partition_name)


service = PartitionLifecycleService(
    repo=repo,
    metadata=metadata,
    locks=locks,
    period_calculator=calculator,
    hooks=[KafkaNotifyHooks(producer)],
)
```

### Hook points

| Method | When |
|--------|------|
| `before_create(config, partition_name, from_value, to_value)` | Before partition is created |
| `after_create(config, partition)` | After creation |
| `before_detach(table_name, partition)` | Before detach |
| `after_detach(table_name, partition_name)` | After successful detach |
| `before_drop(table_name, partition_name)` | Before drop — last chance to read data |
| `after_drop(table_name, partition_name)` | After drop |

`before_*` exceptions abort the operation. `after_*` exceptions are logged but do not affect `result.success`.

## Extensibility

```python
from pg_partsmith.aio import PostgresPartitionRepository


class AuditedPartitionRepository(PostgresPartitionRepository):
    async def drop_partition(self, partition_name: str) -> None:
        await self._audit_log.record("drop", partition_name)
        await super().drop_partition(partition_name)
```

## Lock managers

### PostgreSQL advisory locks (default)

```python
from pg_partsmith.aio import PostgresAdvisoryLockManager

locks = PostgresAdvisoryLockManager(engine, prefix="myapp")
```

> **Pool sizing** — advisory locks hold a dedicated connection for the duration of
> maintenance. Ensure your pool has spare capacity, or use a separate `AsyncEngine`
> for the lock manager (a pool of 1 will deadlock).

### Redis distributed locks

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

## Period strategies

| Class | Granularity | Example |
|-------|-------------|---------|
| `DayPeriodCalculator` | Daily | `events__2024_01_15` |
| `WeekPeriodCalculator` | ISO weekly | `events__2024_w03` |
| `MonthPeriodCalculator` | Monthly | `events__2024_01` |
| `YearPeriodCalculator` | Yearly | `events__2024` |

```python
from pg_partsmith.strategies import BasePeriodCalculator
from pg_partsmith.entities import Period


class QuarterPeriodCalculator(BasePeriodCalculator):
    def current_period(self) -> Period: ...
    def format_partition_name(self, table_name: str, period: Period) -> str: ...
    def parse_partition_name(self, partition_name: str) -> Period | None: ...
    def get_boundaries(self, period: Period) -> tuple[str, str]: ...
```

## Scheduler integration

```python
from pg_partsmith.aio import maintain_partitions

scheduler.add_job(
    maintain_partitions,
    "cron",
    hour=2,
    kwargs={"maintainer": maintainer, "config": config},
)
```

## API reference

### `pg_partsmith`

**Entities** — `Period`, `PartitionInfo`, `TablePartitionConfig`, `MaintenanceResult`, `MaintenanceIssueStep`

**Enums** — `PartitionType`, `PartitionGranularity`, `PartitionStrategy`

**Exceptions** — `PartitionError`, `PartitionAlreadyExistsError`, `PartitionNotFoundError`, `PartitionAttachedError`, `PartitionDetachInProgressError`, `InvalidPartitionConfigError`, `LockAcquisitionError`

**Protocols** — `PeriodCalculator`

**Strategies** — `BasePeriodCalculator`, `DayPeriodCalculator`, `WeekPeriodCalculator`, `MonthPeriodCalculator`, `YearPeriodCalculator`

### `pg_partsmith.aio`

**Protocols** — `PartitionRepository`, `PartitionMetadataProvider`, `LockManager`

**Hooks** — `BasePartitionLifecycleHooks`

**Service** — `PartitionLifecycleService`

**PostgreSQL** — `PostgresPartitionRepository`, `PostgresMetadataProvider`

**Lock managers** — `PostgresAdvisoryLockManager`, `RedisDistributedLockManager`

**Orchestration** — `PartitionMaintainer`, `maintain_partitions`

### `pg_partsmith.sync`

Synchronous mirror of `pg_partsmith.aio` — same names, same layout, plain methods
built on the sync SQLAlchemy `Engine`.

## Development

```bash
make install          # uv sync --group dev
make check            # ruff + mypy
make test-unit        # unit tests (no Docker)
make test-integration # integration tests (Docker required)
make test             # all tests with coverage
make docs-serve       # local docs preview
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide.

## License

[Apache 2.0](LICENSE)
