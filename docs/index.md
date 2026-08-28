# pg-partsmith

PostgreSQL partition lifecycle management with a plan you can read before it runs.

A plain-Python engine for native declarative partitioning: it understands a table's
partition **scheme** (`RANGE` / `LIST` / `HASH`, nested), its **lifecycle policy** (create
ahead, expire, detach, drop), reads the tree that actually exists, and turns the difference
into a **maintenance plan** you can inspect, serialize, filter and apply.

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
| **Partition scheme** | The shape of the tree: `RangePartitioning`, `ListPartitioning`, `HashPartitioning`, each with an optional level below |
| **Boundaries** | How a `RANGE` axis is divided: calendar periods (`TimeBoundaries`, optionally over an encoded key) or integer steps (`NumericBoundaries`) |
| **Lifecycle policy** | When partitions are created (`CreateAhead`, `CreateUntil`, `CreateNextIf`), expire (`KeepNewest`, `KeepFor`, `KeepBehind`, `ExpireIf`), are detached (`DetachMode`) and dropped (`DropAfter`, `DropNever`) |
| **Actual tree** | The catalog's view: every node with its bounds, OID and kind, plus the marker-tagged orphans |
| **Maintenance plan** | Typed, ordered operations with reasons and sizes, and the findings the planner refused to act on |
| **Lifecycle service** | `plan()`, `apply()`, `maintain()` — and conveniences over them |
| **Hooks** | Six points around create / detach / drop |
| **Lock manager** | PostgreSQL advisory locks or Redis, around plan + apply |

## Quick start

```python
from sqlalchemy.ext.asyncio import create_async_engine

from pg_partsmith import PartitionGranularity, TablePartitionConfig
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
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,
    retention_count=12,
)

service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine),
    locks=PostgresAdvisoryLockManager(engine),
)


async def run_maintenance() -> None:
    print((await service.plan(config)).describe())          # dry run
    result = await PartitionMaintainer(service).run_maintenance_safe(config)
    print(result.created_count, result.detached_count, result.dropped_count, result.issues)
```

!!! note "Transaction semantics"
    Every DDL statement runs in its own connection and commits immediately; a partition with
    a subtree is attached last. Pass `AsyncEngine` — not `AsyncSession`.

## Documentation

- [**Quick start**](guide/quickstart.md)
- [**Configuration**](guide/configuration.md) — flat and composed spellings, serialization
- [**Partition schemes**](guide/partition-schemes.md) — RANGE / LIST / HASH, nesting, composite keys
- [**Boundaries and codecs**](guide/boundary-codecs.md) — time, integers, UUIDv7 and epoch keys
- [**Lifecycle policies**](guide/lifecycle-policies.md) — creation, retention, predicates, detach, grace
- [**Planning and dry runs**](guide/planning.md) — the plan, ownership, convergence rules
- [**Period strategies**](guide/strategies.md) — built-in calendars and custom ones
- [**Lifecycle hooks**](guide/hooks.md), [**Lock managers**](guide/locks.md), [**Advanced**](guide/advanced.md)
- [**Recipes**](guide/recipes.md) — error monitoring, queues, outboxes, cold tiering
- [**Migrating an existing partitioner**](guide/migration.md), [**Example: TIME → HASH event store**](guide/nested-migration.md)
- [**Design**](design/rfc-0001-partition-schemes.md) — RFC 0001, [OSS research](design/oss-research.md), [verified PostgreSQL semantics](design/postgresql-semantics.md)
- [**API Reference**](reference/index.md)
