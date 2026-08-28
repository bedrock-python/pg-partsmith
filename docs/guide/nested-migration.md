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

1. the partition key is a **UUIDv7**, not a timestamp, though partitions are still
   weekly;
2. each time partition is **itself partitioned** by hash, for tenant distribution and
   query pruning; and
3. the bucket count has **changed over time**, so history is not uniform.

## The schema

The root is `RANGE`-partitioned on a server-generated UUIDv7. Because a hash dimension is
added below it, `tenant_id` must appear in the primary key
(see [Subpartitioning](subpartitioning.md#required-unique-constraints)):

```sql
CREATE TABLE events (
    id          UUID   NOT NULL DEFAULT uuid_generate_v7(),
    tenant_id   BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload     JSONB  NOT NULL,
    CONSTRAINT events_pkey PRIMARY KEY (id, tenant_id)
) PARTITION BY RANGE (id);
```

## The configuration

```python
from pg_partsmith import (
    HashSubpartitionSpec,
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
    WeekPeriodCalculator,
)
from pg_partsmith.aio import (
    PartitionLifecycleService,
    PartitionMaintainer,
    PostgresAdvisoryLockManager,
    PostgresMetadataProvider,
    PostgresPartitionRepository,
)
from pg_partsmith.boundaries import UUIDv7BoundaryCodec

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_type=PartitionType.RANGE,
    partition_strategy=PartitionStrategy.TIME_BASED,
    partition_column="id",                       # the UUIDv7 column
    granularity=PartitionGranularity.WEEK,       # semantic period
    create_ahead_count=3,
    retention_count=12,                          # 12 weeks, not 12 leaves
    subpartition=HashSubpartitionSpec(           # distribution dimension
        column="tenant_id",
        modulus=2,
        name_suffix="_h{remainder}",             # match the existing naming
    ),
)

codec = UUIDv7BoundaryCodec()                    # physical boundary encoding

service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine, boundary_codec=codec),
    locks=PostgresAdvisoryLockManager(engine),
    period_calculator=WeekPeriodCalculator(boundary_codec=codec),
)
maintainer = PartitionMaintainer(service)

result = await maintainer.run_maintenance_safe(config)
```

That replaces the create-ahead loop, the bucket loop, the modulus-conflict handling, the
existence pre-checks, and the retention pruner.

## Naming and adoption

Existing partitions are almost certainly named by a different convention. Two independent
questions:

- **Existing partitions** are discovered by reading `pg_catalog`, not by parsing names, so
  a tree built by another tool is introspected and reconciled as-is. Nothing needs
  renaming, nothing needs recreating.
- **New partitions** are named by the period calculator. If your existing names are
  `events_20260824` rather than pg-partsmith's `events__2026_w35`, subclass the calculator
  so old and new partitions stay consistent:

  ```python
  import re
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
  ```

  Set `name_suffix="_h{remainder}"` on the spec so buckets match too.

Detached-but-never-dropped tables left by the old manager carry no ownership marker and
are adopted once with `repo.adopt_partition(...)` — see
[Migrating an existing partitioner](migration.md#adopting-legacy-detached-partitions).

## Backfilling the periods your data already occupies

The events already in the table predate the create-ahead window, so give them partitions
explicitly before the first scheduled tick:

```python
calculator = WeekPeriodCalculator(boundary_codec=codec)
current = calculator.current_period()
past = [calculator.period_before(current, n) for n in reversed(range(1, 13))]

await service.ensure_partitions(config, past)
```

Idempotent, one catalogue read for the whole batch, and each week is built with its full
bucket set before being attached. Weeks that already exist — under whatever naming the
previous manager used — are recognised and skipped rather than duplicated.

## What happens on the first run

Against a database with a non-uniform history, one tick converges everything it safely
can and reports the rest:

```text
events_20260810   MODULUS 4, complete     → untouched (a preserved older bucket count)
events_20260817   MODULUS 4, missing h2   → h2 created at MODULUS 4, not 2
events_20260824   plain leaf, no buckets  → left as a valid legacy leaf
events_20260831   absent                  → created with MODULUS 2 and both buckets
```

Only the third and fourth lines involve DDL on the live tree, and none of it rewrites
data. Re-running the tick is a no-op.

### `issues` now fills up on a successful run

The first two lines above are reported on `MaintenanceResult.issues`, and the run is
still a success. Before subpartitioning existed, `issues` was only ever populated by a
step that had *failed* under `continue_on_error`, so treating a non-empty `issues` as an
alarm was reasonable. It no longer is: a topology finding is a description of something
the run deliberately left alone, not a failure.

`MaintenanceResult.success` is unchanged — it reports a fatal error and nothing else. If
you page on `issues`, filter by `MaintenanceIssue.step`:

```python
failures = [i for i in result.issues if i.step is not MaintenanceIssueStep.RECONCILE]
```

## Cold storage before drop

Export hooks fire once per **time slice**, not once per bucket — which is what an archival
pipeline wants:

```python
class ColdStorageHooks(BasePartitionLifecycleHooks):
    async def before_drop(self, table_name: str, partition_name: str) -> None:
        # partition_name is "public.events_20260810" — the whole week,
        # readable as one relation across all of its buckets.
        await export_to_object_storage(partition_name)
```

To finalize only partitions that can no longer receive rows, check
`metadata.is_partition_closed(name, settle_seconds=...)`. With a codec configured this
works on UUIDv7 bounds too.

## What stays in your application

pg-partsmith manages the partition tree. Query-side concerns remain yours — in
particular, turning a time filter into a UUIDv7 range so the planner can prune
partitions. The codec exposes the same encoding the DDL uses, so the two cannot drift:

```python
codec = UUIDv7BoundaryCodec()
lower = codec.min_uuid_for(window_start)
upper = codec.min_uuid_for(window_end)

# WHERE id >= :lower AND id < :upper
```
