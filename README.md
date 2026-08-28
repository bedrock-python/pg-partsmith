# pg-partsmith

PostgreSQL partition lifecycle management with a plan you can read before it runs.

[![PyPI](https://img.shields.io/pypi/v/pg-partsmith?color=blue)](https://pypi.org/project/pg-partsmith/)
[![Python](https://img.shields.io/pypi/pyversions/pg-partsmith)](https://pypi.org/project/pg-partsmith/)
[![License](https://img.shields.io/github/license/bedrock-python/pg-partsmith)](LICENSE)
[![CI](https://github.com/bedrock-python/pg-partsmith/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/bedrock-python/pg-partsmith/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bedrock-python/pg-partsmith/graph/badge.svg)](https://codecov.io/gh/bedrock-python/pg-partsmith)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://bedrock-python.github.io/pg-partsmith/)

A plain-Python engine for native PostgreSQL declarative partitioning. It understands a
table's partition **scheme** (`RANGE`, `LIST`, `HASH`, nested to any depth), its
**lifecycle policy** (what to create ahead, what has expired, when to drop), reads the
tree that actually exists from the catalog, and turns the difference into a
**maintenance plan** — typed operations with a reason on each — that you can inspect,
serialize, filter and apply. No extension, no superuser, no scheduler of its own.

## Features

- **Any topology** — `RANGE(time)`, `RANGE(id)`, root `HASH`, root `LIST`,
  `RANGE → HASH`, `RANGE → LIST → HASH`, `LIST → RANGE`; composite keys
- **Any axis** — calendar periods over timestamps, or over encoded keys (UUIDv7, epoch
  integers, your own codec); fixed-width integer windows for id-partitioned queues
- **Lifecycle policies** — create ahead by count or until a horizon, expire by count, age
  or distance, or by a predicate (size, rows, SQL); detach now, drop after a grace period
- **Plan → apply** — `plan()` issues zero DDL and tells you *what*, *why* and *how big*;
  `apply()` revalidates every destructive operation against the catalog before running it
- **Convergent and safe** — a converged tree costs zero DDL; gaps in hash sets are repaired
  at their own modulus; partitions the scheme did not produce are reported, never touched;
  foreign tables are inspected, never dropped
- **Async and sync** — `pg_partsmith.aio` on `AsyncEngine`, `pg_partsmith.sync` on `Engine`
- **Hooks, locks, schemas** — six lifecycle hooks; PostgreSQL advisory or Redis locks;
  schema-qualified everything
- **Type-safe, tested** — Pydantic models, full mypy, real PostgreSQL 15 and 17 via testcontainers

## Installation

```bash
pip install pg-partsmith

# With Redis distributed locks
pip install "pg-partsmith[redis-locks]"
```

**Requirements:** Python 3.11+, PostgreSQL 15+

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
    create_ahead_count=3,  # current month + next 2
    retention_count=12,
)

service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine),
    locks=PostgresAdvisoryLockManager(engine),
)


async def maintain() -> None:
    plan = await service.plan(config)          # read-only: what would happen, and why
    print(plan.describe())
    result = await PartitionMaintainer(service).run_maintenance_safe(config)
    print(result.created_count, result.detached_count, result.dropped_count, result.issues)
```

`plan.describe()` on a fresh table:

```text
plan for public.events at 2026-08-28T00:00:00+00:00
  CREATE public.events__2026_08 (create_ahead)
  CREATE public.events__2026_09 (create_ahead)
  CREATE public.events__2026_10 (create_ahead)
```

> **Transaction semantics** — every DDL statement runs in its own connection and commits
> immediately. A partition that has a subtree is built detached and attached last, so an
> interrupted run leaves an unreachable table rather than a live partition that rejects
> part of its keyspace. Use `AsyncEngine`, not `AsyncSession`.

## The composed form

The flat fields above are sugar for a **scheme** and a **lifecycle policy**. Spell them out
for anything beyond a time-partitioned root:

```python
from datetime import timedelta

from pg_partsmith import (
    CreateAhead, DropAfter, HashPartitioning, KeepNewest, LifecyclePolicy,
    RangePartitioning, TimeBoundaries, UUIDv7BoundaryCodec,
)

config = TablePartitionConfig(
    table_name="issue_events",
    scheme=RangePartitioning(
        key="id",                                                   # a UUIDv7 column
        boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=UUIDv7BoundaryCodec()),
        child=HashPartitioning(key="organization_id", modulus=4),   # each week split by tenant
    ),
    lifecycle=LifecyclePolicy(
        creation=CreateAhead(count=3),
        retention=KeepNewest(count=12),          # twelve weeks, not twelve leaves
        drop=DropAfter(grace=timedelta(days=7)), # detach now, drop a week later
    ),
)
```

```text
issue_events                          PARTITION BY RANGE (id)
├── issue_events__2026_w35            PARTITION BY HASH (organization_id)
│   ├── issue_events__2026_w35__h0    MODULUS 4, REMAINDER 0
│   └── …
└── issue_events__2026_w36 …
```

More shapes, each a one-liner:

```python
# a queue partitioned every 100 000 message ids, keeping ten million behind the newest
RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=100_000))
LifecyclePolicy(creation=CreateAhead(count=4), retention=KeepBehind(distance=10_000_000))

# a task table hashed for parallel workers: a fixed set, never created ahead, never expired
HashPartitioning(key="task_id", modulus=8)

# partitions through the end of next year, dropped 90 days after their last row could arrive
LifecyclePolicy(creation=CreateUntil(datetime(2028, 1, 1, tzinfo=UTC)), retention=KeepFor(timedelta(days=90)))

# detach when nothing is pending any more, whatever the calendar says
LifecyclePolicy(retention=ExpireIf(AllOf((KeepNewest(count=2),
    SqlPredicate("SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')")))))
```

## What maintenance does

1. **Inspect** — one catalog round-trip reads the whole tree (`pg_partition_tree`), the
   marker-tagged detached orphans, and — only when a policy asks — sizes and row estimates.
2. **Plan** — the planner walks the scheme and the tree together. At a `RANGE` level it
   decides which windows must exist ahead of the cursor (the clock, or `max(key)` for an
   integer axis) and which existing ones have expired; at a `HASH`/`LIST` level it fills the
   gaps in the member set. Everything it refuses to do is a **finding** with a reason.
3. **Apply** — under the table's lock, in order: creations (subtree first, attach last),
   re-attachments, detaches (`CONCURRENTLY` where PostgreSQL allows), drops (revalidated by
   OID and ownership marker).

Repeated on a converged table, maintenance issues **zero DDL** — that is an integration
test, not a promise.

## Ownership and safety

- An attached partition whose bounds are a window of the scheme's grid (or lie inside one)
  is a lifecycle partition. One whose bounds are not — a DBA's hand-attached
  `events_archive_2000_2019` — is reported as `unmanaged_partition` and never detached or
  dropped, no matter how old.
- Only tables carrying the library's `COMMENT` marker are ever dropped. The marker is
  written before the `DETACH` and records when it happened, which is what a grace period is
  measured from. Legacy detached tables are adopted with `repo.adopt_partition(...)`.
- A hash set at a modulus the config no longer uses is preserved if complete and repaired
  *at its own modulus* if not; mixed moduli leaving a gap are reported, never guessed at.
- A plan made at 10:00 and applied at 10:05 refuses to drop a table that was recreated in
  between (`PlanStaleError`, reported as an issue).

## Sync usage

Every class in `pg_partsmith.aio` has a synchronous twin in `pg_partsmith.sync` with the
same name and API, built on the classic SQLAlchemy `Engine`:

```python
from sqlalchemy import create_engine

from pg_partsmith.sync import (
    PartitionLifecycleService, PartitionMaintainer,
    PostgresAdvisoryLockManager, PostgresMetadataProvider, PostgresPartitionRepository,
)

engine = create_engine("postgresql+psycopg2://user:pass@host/db")
service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine),
    locks=PostgresAdvisoryLockManager(engine),
)
result = PartitionMaintainer(service).run_maintenance_safe(config)
```

Two behavioural differences: `ddl_timeout_seconds` is enforced server-side via
`statement_timeout` per statement, and the Redis lock renews from a background thread.

## Hooks

```python
from pg_partsmith.aio import BasePartitionLifecycleHooks


class ColdStorageHooks(BasePartitionLifecycleHooks):
    async def before_drop(self, table_name: str, partition_name: str) -> None:
        await export_to_object_storage(partition_name)   # raising aborts the drop; retried next tick


service = PartitionLifecycleService(repo, metadata, locks, hooks=[ColdStorageHooks()])
```

| Method | When |
|--------|------|
| `before_create(config, partition)` / `after_create(config, partition)` | around the creation of a partition directly under the root (its subtree included) |
| `before_detach(table_name, partition)` / `after_detach(table_name, partition_name)` | around a detach |
| `before_drop(table_name, partition_name)` / `after_drop(table_name, partition_name)` | around a drop — `before_drop` is the last chance to read the data |

`before_*` exceptions abort that operation; `after_*` exceptions are logged.

## Documentation

- [Quick start](https://bedrock-python.github.io/pg-partsmith/guide/quickstart/)
- [Partition schemes](https://bedrock-python.github.io/pg-partsmith/guide/partition-schemes/) — RANGE / LIST / HASH, nesting, composite keys, encoded keys
- [Lifecycle policies](https://bedrock-python.github.io/pg-partsmith/guide/lifecycle-policies/) — creation, retention, predicates, detach modes, grace periods
- [Planning and dry runs](https://bedrock-python.github.io/pg-partsmith/guide/planning/) — the plan, findings, ownership, revalidation
- [Recipes](https://bedrock-python.github.io/pg-partsmith/guide/recipes/) — error monitoring, queues, outboxes, cold tiering
- [Migrating an existing partitioner](https://bedrock-python.github.io/pg-partsmith/guide/migration/)
- [Design: RFC 0001](https://bedrock-python.github.io/pg-partsmith/design/rfc-0001-partition-schemes/), [OSS research](https://bedrock-python.github.io/pg-partsmith/design/oss-research/), [verified PostgreSQL semantics](https://bedrock-python.github.io/pg-partsmith/design/postgresql-semantics/)

## Development

```bash
make install          # uv sync --group dev
make check            # ruff + mypy
make test-unit        # unit tests (no Docker)
make test-integration # integration tests (Docker required)
make test             # all tests with coverage
make docs-serve       # local docs preview
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache 2.0](LICENSE)
