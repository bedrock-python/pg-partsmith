# Tutorial: your first partitioned table

In this tutorial you partition an `events` table by month, let pg-partsmith create the
partitions, watch rows land in them, and see old months retire a year later. Every output
below was captured against PostgreSQL 17; the clock is frozen at **28 August 2026** so the
names make sense.

You need a database you can create tables in, and the library installed
([Installation](installation.md)).

## 1. The table

PostgreSQL does the partitioning; pg-partsmith manages the partitions. Start with a
partitioned parent and no partitions at all:

```sql
CREATE TABLE events (
    id          BIGSERIAL,
    created_at  TIMESTAMPTZ NOT NULL,
    payload     JSONB,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
```

The primary key includes `created_at` because PostgreSQL requires every unique constraint
on a partitioned table to contain the partition key.

## 2. The configuration

A configuration describes what the tree should look like:

```python
from pg_partsmith import PartitionGranularity, TablePartitionConfig

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,   # this month and the next two must exist
    retention_count=12,     # keep the twelve newest months, this one included
)
```

Read it as: *one partition per calendar month of `created_at`; always have the current
month and two more ready; once a month is older than the twelve newest, retire it.*

## 3. The service

The service wires three things to an engine: a **repository** that runs DDL, a **metadata
provider** that reads the catalog, and a **lock manager** that keeps two maintainers off
the same table.

=== "asyncio"

    ```python
    from sqlalchemy.ext.asyncio import create_async_engine

    from pg_partsmith.aio import (
        PartitionLifecycleService,
        PostgresAdvisoryLockManager,
        PostgresMetadataProvider,
        PostgresPartitionRepository,
    )

    engine = create_async_engine("postgresql+asyncpg://app:secret@localhost/app")
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine),
        locks=PostgresAdvisoryLockManager(engine),
    )
    ```

=== "sync"

    ```python
    from sqlalchemy import create_engine

    from pg_partsmith.sync import (
        PartitionLifecycleService,
        PostgresAdvisoryLockManager,
        PostgresMetadataProvider,
        PostgresPartitionRepository,
    )

    engine = create_engine("postgresql+psycopg2://app:secret@localhost/app")
    service = PartitionLifecycleService(
        repo=PostgresPartitionRepository(engine),
        metadata=PostgresMetadataProvider(engine),
        locks=PostgresAdvisoryLockManager(engine),
    )
    ```

## 4. Look before you leap

`plan()` reads the catalog and decides what to do. It issues no DDL and takes no lock, so
it is safe to call anywhere, as often as you like:

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

Three creations, each tagged with the *reason* it is in the plan: `create_ahead`, because
the policy asked for three months starting with the current one. The plan is a plain
Pydantic model — `plan.operations` is the typed sequence, `plan.model_dump(mode="json")` is
what you would put in a log or hand to a dashboard:

```json
{
  "target": "public.events__2026_08",
  "reason": "create_ahead",
  "detail": "2026_08 under 'create 3 ahead'",
  "kind_name": "create",
  "parent_name": "public.events",
  "bounds": {"kind": "range", "from_value": "2026-08-01", "to_value": "2026-09-01"},
  "key_columns": ["created_at"],
  "lifecycle_unit": true,
  "counts_as": "created"
}
```

## 5. Apply it

```python
result = await service.apply(config, plan)
print(result.created_count, result.detached_count, result.dropped_count, result.issues)
```

```text
3 0 0 ()
```

`apply()` takes the table's lock, runs the operations in order, and returns a
`MaintenanceResult` with counters and any issues. Each partition is created standalone
(`CREATE TABLE … LIKE parent`) and then attached, one statement per transaction. The
catalog now shows:

```text
events  partitioned table  PARTITION BY RANGE (created_at)
  events__2026_08  table  FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00')
  events__2026_09  table  FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00')
  events__2026_10  table  FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00')
```

Names follow the calendar (`events__2026_08`); bounds are calendar months in UTC.

`plan()` followed by `apply()` is what `service.maintain(config)` does under one lock, and
what the scheduler-friendly `PartitionMaintainer` wraps — the [next tutorial](production.md)
uses those.

## 6. Rows go where they belong

PostgreSQL routes each row by its key; pg-partsmith is not involved at write time:

```sql
INSERT INTO events (created_at, payload) VALUES
    ('2026-08-28 09:00+00', '{"kind": "signup"}'),
    ('2026-09-02 12:00+00', '{"kind": "login"}');

SELECT tableoid::regclass AS partition, created_at::date FROM events ORDER BY created_at;
```

```text
events__2026_08  2026-08-28
events__2026_09  2026-09-02
```

A row with no partition to go to is rejected — which is why the policy keeps partitions
*ahead* of now:

```sql
INSERT INTO events (created_at, payload) VALUES ('2026-12-01 00:00+00', '{}');
```

```text
ERROR:  no partition of relation "events" found for row
DETAIL:  Partition key of the failing row contains (created_at) = (2026-12-01 00:00:00+00).
```

Run maintenance often enough that `create_ahead_count` outlasts the gap between runs. A
daily tick with three months ahead is comfortable; the cost of a run that finds nothing to
do is one catalog read.

## 7. A converged table costs nothing

Plan again right away:

```python
print((await service.plan(config)).describe())
```

```text
plan for public.events at 2026-08-28T10:00:00+00:00
  nothing to do
```

## 8. A year later

Move the clock to **15 September 2027** and plan once more. The three-ahead window has
moved on, and the twelve-newest rule has caught up with the first months:

```text
plan for public.events at 2027-09-15T03:00:00+00:00
  CREATE public.events__2027_09 (create_ahead)
  CREATE public.events__2027_10 (create_ahead)
  CREATE public.events__2027_11 (create_ahead)
  DETACH public.events__2026_08 (retention_expired)
  DETACH public.events__2026_09 (retention_expired)
  DROP public.events__2026_08 (follows_detach)
  DROP public.events__2026_09 (follows_detach)
```

Two things to notice. Retirement is a **detach** followed by a **drop**, two separate
statements: the partition leaves the parent first, and only a table that is detached is
ever dropped. And `events__2026_10` survives: the twelve newest months, counted back from
September 2027, are October 2026 through September 2027.

After `apply()`:

```text
events  partitioned table  PARTITION BY RANGE (created_at)
  events__2026_10  table  FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00')
  events__2027_09  table  FOR VALUES FROM ('2027-09-01 00:00:00+00') TO ('2027-10-01 00:00:00+00')
  events__2027_10  table  FOR VALUES FROM ('2027-10-01 00:00:00+00') TO ('2027-11-01 00:00:00+00')
  events__2027_11  table  FOR VALUES FROM ('2027-11-01 00:00:00+00') TO ('2027-12-01 00:00:00+00')
```

(Only three months were ever created between the two runs, which is why the table looks
sparse. A daily tick fills the calendar continuously.)

## 9. A week between detach and drop

Dropping in the same run as the detach is the default. Most teams want a pause — to
export the partition, to be able to put it back. That is the `drop` half of a lifecycle
policy. The flat fields you used so far are sugar for a scheme and a policy; spell the
policy out to change it:

```python
from datetime import timedelta

from pg_partsmith import CreateAhead, DropAfter, KeepNewest, LifecyclePolicy

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    lifecycle=LifecyclePolicy(
        creation=CreateAhead(count=3),
        retention=KeepNewest(count=12),
        drop=DropAfter(grace=timedelta(days=7)),
    ),
)
```

On **2 October 2027** the plan detaches October 2026 and drops nothing:

```text
plan for public.events at 2027-10-02T03:00:00+00:00
  CREATE public.events__2027_12 (create_ahead)
  DETACH public.events__2026_10 (retention_expired)
```

The detached table stays in the database, marked as pg-partsmith's with a `COMMENT`:

```text
pg-partsmith:orphan-parent=public.events
pg-partsmith:detached-at=2027-10-02T03:00:00+00:00
```

Planning again the same day reports why it is still there:

```text
plan for public.events at 2027-10-02T03:00:00+00:00
  [info] grace_pending: public.events__2026_10 was detached at 2027-10-02T03:00:00+00:00 and is kept until 2027-10-09T03:00:00+00:00 ('drop after 7 days, 0:00:00').
```

That line is a **finding**: something the planner saw and deliberately left alone, with
its severity (`info` — an expected steady state) and its reason. A week later the drop is
planned:

```text
plan for public.events at 2027-10-10T03:00:00+00:00
  DROP public.events__2026_10 (grace_elapsed)
```

Only tables carrying the marker are ever dropped. If retention grows again in the
meantime, a marked table whose month is wanted is *re-attached* rather than recreated.

## What you have learned

- A configuration is a **scheme** (monthly partitions of `created_at`) plus a **lifecycle
  policy** (three ahead, twelve kept, drop a week after detach). The flat fields are sugar
  for both.
- `plan()` is read-only and explains itself; `apply()` runs the plan under a lock.
- Retirement is detach, then drop; grace periods live on the marker.
- Findings are the planner telling you what it chose not to do, and why.

## Next

[Tutorial: running it in production →](production.md) — a scheduler, several replicas,
reading results, alerting on issues.
