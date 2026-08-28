# Real-world example: an event store with `TIME → HASH`

This walks through replacing a hand-rolled partition manager for a high-volume,
multi-tenant event store — the shape several production Python applications end up
building for themselves:

```text
events                                 PARTITION BY RANGE (id)        ← id is a UUIDv7
├── events_20260824                    PARTITION BY HASH (tenant_id)
│   ├── events_20260824_h0
│   └── events_20260824_h1
└── …
```

Three things make it awkward for generic tooling, and all three are covered:

1. the partition key is a **UUIDv7**, not a timestamp, though partitions are still weekly;
2. each time partition is **itself partitioned** by hash, for tenant distribution and
   query pruning; and
3. the bucket count has **changed over time**, so history is not uniform.

## The schema

The root is `RANGE`-partitioned on a UUIDv7. Because a hash dimension is added below it,
`tenant_id` must appear in the primary key
(see [Partition schemes](partition-schemes.md#required-unique-constraints)):

```sql
CREATE TABLE events (
    id          UUID   NOT NULL,
    tenant_id   BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload     JSONB  NOT NULL,
    CONSTRAINT events_pkey PRIMARY KEY (id, tenant_id)
) PARTITION BY RANGE (id);
```

> **Generating the key.** PostgreSQL gains `uuidv7()` in **18**; there is no built-in
> UUIDv7 before that. On 17 and earlier, generate the value in the application (Python's
> `uuid6` package, or any RFC 9562 implementation) or install a UUIDv7 function of your own.

## The configuration

```python
from datetime import timedelta

from pg_partsmith import (
    CreateAhead, DropAfter, HashPartitioning, KeepNewest, LifecyclePolicy,
    PartitionGranularity, RangePartitioning, TablePartitionConfig, TimeBoundaries, UUIDv7BoundaryCodec,
)
from pg_partsmith.aio import (
    PartitionLifecycleService, PartitionMaintainer,
    PostgresAdvisoryLockManager, PostgresMetadataProvider, PostgresPartitionRepository,
)

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    scheme=RangePartitioning(
        key="id",                                                        # the UUIDv7 column
        boundaries=TimeBoundaries(
            granularity=PartitionGranularity.WEEK,                       # semantic period
            codec="uuidv7",                                              # physical encoding
        ),
        child=HashPartitioning(key="tenant_id", modulus=2, name_suffix="_h{remainder}"),
    ),
    lifecycle=LifecyclePolicy(
        creation=CreateAhead(count=3),
        retention=KeepNewest(count=12),               # 12 weeks, not 12 leaves
        drop=DropAfter(grace=timedelta(days=7)),      # a week between detach and drop
    ),
)

service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine, boundary_codec=UUIDv7BoundaryCodec()),  # for is_partition_closed
    locks=PostgresAdvisoryLockManager(engine),
)
maintainer = PartitionMaintainer(service)

result = await maintainer.run_maintenance_safe(config)
```

That replaces the create-ahead loop, the bucket loop, the modulus-conflict handling, the
existence pre-checks, and the retention pruner.

## Naming and adoption

Two independent questions:

- **Existing partitions** are discovered by reading `pg_catalog`, not by parsing names, so
  a tree built by another tool is introspected and reconciled as-is — provided its bounds
  are windows of the weekly grid (they are: both managers use the minimum UUID of each
  Monday). Nothing needs renaming, nothing needs recreating.
- **New partitions** are named by the calendar. If existing names are `events_20260824`
  rather than `events__2026_w35`, subclass the calculator so old and new stay consistent:

  ```python
  import re
  from datetime import date

  from pg_partsmith import Period, WeekPeriodCalculator


  class LegacyNamedWeekCalculator(WeekPeriodCalculator):
      """Names weeks by their Monday's date, as the previous manager did."""

      _NAME_PATTERN = re.compile(r"^(.+)_(\d{4})(\d{2})(\d{2})$")

      def format_partition_name(self, table_name: str, period: Period) -> str:
          return f"{table_name}_{period.to_date():%Y%m%d}"

      def _period_from_match(self, match: re.Match[str]) -> Period:
          monday = date(int(match.group(2)), int(match.group(3)), int(match.group(4)))
          iso_year, iso_week, _ = monday.isocalendar()
          return Period(year=iso_year, week=iso_week)


  boundaries = TimeBoundaries(calculator=LegacyNamedWeekCalculator(boundary_codec=UUIDv7BoundaryCodec()))
  ```

  and set `name_suffix="_h{remainder}"` on the hash level so buckets match too.

Detached-but-never-dropped tables left by the old manager carry no ownership marker and
are adopted once with `repo.adopt_partition(...)` — see
[Migrating an existing partitioner](migration.md#adopting-legacy-detached-partitions).

## Backfilling the periods your data already occupies

Events already in the table predate the create-ahead window, so give them partitions
explicitly before the first scheduled tick:

```python
calculator = config.scheme.time_boundaries.period_calculator
current = calculator.current_period()
past = [calculator.period_before(current, n) for n in reversed(range(1, 13))]

await service.ensure_partitions(config, past)
```

Idempotent, one catalogue read for the whole batch, and each week is built with its full
bucket set before being attached.

## What happens on the first run

Against a database with a non-uniform history, one tick converges everything it safely
can and reports the rest — see it first with `print((await service.plan(config)).describe())`:

```text
events_20260810   MODULUS 4, complete     → untouched (modulus_preserved, info)
events_20260817   MODULUS 4, missing h2   → h2 created at MODULUS 4 (hash_gap_historical_modulus)
events_20260824   plain leaf, no buckets  → left as a valid legacy leaf (legacy_leaf, info)
events_20260831   absent                  → created with MODULUS 2 and both buckets
```

Re-running the tick is a no-op. None of the informational findings reaches
`MaintenanceResult.issues`; a branch partitioned by the wrong column, a LIST value owned
elsewhere, or a DEFAULT partition blocking an attach would.

## Cold storage before drop

Export hooks fire once per **time slice**, not once per bucket:

```python
class ColdStorageHooks(BasePartitionLifecycleHooks):
    async def after_detach(self, table_name: str, partition_name: str) -> None:
        # "public.events_20260810" — the whole week, readable as one relation across its buckets
        await export_to_object_storage(partition_name)
```

With `DropAfter(grace=timedelta(days=7))` the export has a week before the drop; a
`before_drop` hook that raises defers the drop to a later run.

## What stays in your application

Query-side concerns — in particular, turning a time filter into a UUIDv7 range so the
planner can prune partitions. The codec exposes the same encoding the DDL uses:

```python
codec = UUIDv7BoundaryCodec()
lower, upper = codec.min_uuid_for(window_start), codec.min_uuid_for(window_end)
# WHERE id >= :lower AND id < :upper AND tenant_id = :tenant
```
