# Tutorial: a multi-tenant event store

The [first tutorial](first-table.md) covered the ordinary monthly table. This one builds
the shape error-monitoring and analytics products end up with — GlitchTip's
`issue_events`, for instance:

- the partition key is a **UUIDv7**, not a timestamp, though partitions are still weekly;
- each week is **itself partitioned** by tenant, so a tenant's queries touch one bucket;
- the tree has **history**: earlier weeks were built with a different bucket count, one of
  them by hand.

All three are things generic tooling trips over, and all three are covered by the
composed configuration. Outputs are captured against PostgreSQL 17 on 28 August 2026.

## 1. The table

```sql
CREATE TABLE issue_events (
    id               UUID   NOT NULL,     -- a UUIDv7: time-ordered
    organization_id  BIGINT NOT NULL,
    payload          JSONB  NOT NULL,
    PRIMARY KEY (id, organization_id)     -- both partition keys must be in it
) PARTITION BY RANGE (id);
```

`organization_id` is in the primary key because the hash level below will partition on
it, and PostgreSQL requires every unique constraint to contain every partition-key column
of every level. pg-partsmith checks this against the catalog before it runs any DDL and
refuses a configuration that would fail half-way.

!!! note "Generating UUIDv7 values"
    PostgreSQL 18 ships `uuidv7()`. On 17 and earlier, generate the value in the
    application (any RFC 9562 implementation) or with a SQL function of your own.

## 2. The configuration

The flat spelling only covers a time-partitioned root. Everything else is spelled as a
**scheme** — the shape of the tree, level by level — and a **lifecycle policy**:

```python
from datetime import timedelta

from pg_partsmith import (
    CreateAhead,
    DropAfter,
    HashPartitioning,
    KeepNewest,
    LifecyclePolicy,
    PartitionGranularity,
    RangePartitioning,
    TablePartitionConfig,
    TimeBoundaries,
)

config = TablePartitionConfig(
    schema="public",
    table_name="issue_events",
    scheme=RangePartitioning(
        key="id",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="uuidv7"),
        child=HashPartitioning(key="organization_id", modulus=2),
    ),
    lifecycle=LifecyclePolicy(
        creation=CreateAhead(count=2),
        retention=KeepNewest(count=8),
        drop=DropAfter(grace=timedelta(days=7)),
    ),
)
```

Three ideas are packed in here.

**The root is a `RangePartitioning` over `id`.** Its `boundaries` say how the axis is cut
into windows: calendar weeks. The **codec** says how a week is written as `id` literals —
`"uuidv7"` renders each boundary instant as the smallest UUIDv7 of that millisecond, so
the partitions are ordinary `RANGE (id)` partitions and the planner prunes on `id` ranges.

**Each week has a `child`.** `HashPartitioning(key="organization_id", modulus=2)` splits
every week into two buckets. The child is part of the week: created with it, dropped with
it, counted as one.

**The policy talks about weeks, not buckets.** `KeepNewest(count=8)` keeps eight weeks
however many buckets each holds. The lifecycle unit is the partition directly under the
root.

## 3. The plan

```python
print((await service.plan(config)).describe())
```

```text
plan for public.issue_events at 2026-08-28T10:00:00+00:00
  CREATE public.issue_events__2026_w35 (create_ahead)
    CREATE public.issue_events__2026_w35__h0 (subtree)
    CREATE public.issue_events__2026_w35__h1 (subtree)
  CREATE public.issue_events__2026_w36 (create_ahead)
    CREATE public.issue_events__2026_w36__h0 (subtree)
    CREATE public.issue_events__2026_w36__h1 (subtree)
```

A creation carries its subtree. When applied, the week is created as a standalone
partitioned table, its two buckets are created and attached *inside* it, and only then is
the week attached to `issue_events`. Until that last statement the week is invisible to
row routing — so an interruption anywhere before it leaves an unreachable table, never a
live week missing a bucket that would reject half the tenants.

After `apply()`:

```text
issue_events  partitioned table  PARTITION BY RANGE (id)
  issue_events__2026_w35  partitioned table  FOR VALUES FROM ('01a03111-1c00-7000-8000-000000000000') TO ('01a0551d-a000-7000-8000-000000000000')  PARTITION BY HASH (organization_id)
  issue_events__2026_w36  partitioned table  FOR VALUES FROM ('01a0551d-a000-7000-8000-000000000000') TO ('01a0792a-2400-7000-8000-000000000000')  PARTITION BY HASH (organization_id)
    issue_events__2026_w35__h0  table  FOR VALUES WITH (modulus 2, remainder 0)
    issue_events__2026_w35__h1  table  FOR VALUES WITH (modulus 2, remainder 1)
    issue_events__2026_w36__h0  table  FOR VALUES WITH (modulus 2, remainder 0)
    issue_events__2026_w36__h1  table  FOR VALUES WITH (modulus 2, remainder 1)
```

The bounds are UUIDs: the minimum UUIDv7 of Monday 24 August 2026 00:00 UTC, and of the
Monday after. Adjacent weeks share a boundary, so no identifier can fall between them.

## 4. History that does not match

Real tables have a past. Suppose two earlier weeks exist: week 33 was built with **four**
buckets and is missing one of them, and week 34 is a plain table — nobody had split it.

```sql
CREATE TABLE issue_events__2026_w33 PARTITION OF issue_events
    FOR VALUES FROM ('019fe8f8-1400-7000-8000-000000000000') TO ('01a00d04-9800-7000-8000-000000000000')
    PARTITION BY HASH (organization_id);
CREATE TABLE issue_events__2026_w33__h0 PARTITION OF issue_events__2026_w33 FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE issue_events__2026_w33__h1 PARTITION OF issue_events__2026_w33 FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE issue_events__2026_w33__h3 PARTITION OF issue_events__2026_w33 FOR VALUES WITH (MODULUS 4, REMAINDER 3);

CREATE TABLE issue_events__2026_w34 PARTITION OF issue_events
    FOR VALUES FROM ('01a00d04-9800-7000-8000-000000000000') TO ('01a03111-1c00-7000-8000-000000000000');
```

The plan against that:

```text
plan for public.issue_events at 2026-08-28T10:00:00+00:00
  CREATE public.issue_events__2026_w33__h2 (hash_gap_historical_modulus)
  [info] modulus_repaired: public.issue_events__2026_w33 has an incomplete 4-bucket hash set (3 of 4 present); filling the gaps at that modulus because the configured count (2) would overlap the existing buckets.
  [info] legacy_leaf: public.issue_events__2026_w34 is a plain leaf table and cannot hold HASH (organization_id) partitions; leaving it as-is. Partitions created before the current scheme stay valid; new partitions are created with the current topology.
```

Read it line by line:

- Week 33's missing bucket is **repaired at modulus 4**, not 2. A hash set at another
  modulus is legal and complete only at its own modulus; a `MODULUS 2` bucket would
  overlap the existing ones and PostgreSQL would refuse it. Until the gap is filled, a
  quarter of the tenants cannot insert into that week — which is why the repair is
  planned rather than reported.
- Week 34 is a **legacy leaf**: a plain table cannot gain partitions, and it holds valid
  data, so it stays. New weeks follow the configured shape.

Both weeks were recognised by their **bounds**, not their names: the library reads
`pg_get_expr(relpartbound)` and asks whether the window is one the scheme would produce.
A tree built by another tool, or by hand, is reconciled as-is.

## 5. What retention counts

With `KeepNewest(count=8)`, expiry looks at the eight newest *weeks*. When week 33 expires
it is detached as a whole — the branch with its four buckets — kept for the seven-day
grace, and dropped as a whole (`DROP TABLE` of a partitioned table takes its children
with it, no `CASCADE` needed). Hooks fire once per week, not once per bucket: an archive
step gets one relation to export, readable across all its buckets.

## 6. Backfilling weeks that already have data

Events written before the tree existed sit in weeks create-ahead will never reach. Give
them partitions explicitly:

```python
calculator = config.scheme.time_boundaries.period_calculator
current = calculator.current_period()
past = [calculator.period_before(current, n) for n in reversed(range(1, 9))]

created = await service.ensure_partitions(config, past)      # idempotent
```

Each week comes with its full bucket set before it is attached. Windows outside the
retention window are pointless to create: the next tick would retire them.

## 7. Query pruning is yours

The lifecycle does not make queries fast by itself. PostgreSQL skips partitions only when
the query constrains the partition key of every level it should skip. For a UUIDv7 key
that means turning a time filter into an `id` range with the same codec the DDL used:

```python
from pg_partsmith import UUIDv7BoundaryCodec

codec = UUIDv7BoundaryCodec()
lower, upper = codec.min_uuid_for(since), codec.min_uuid_for(until)
# WHERE id >= :lower AND id < :upper AND organization_id = :org
```

## What you have learned

- A scheme is levels: `RangePartitioning(boundaries, child=HashPartitioning(...))`.
- Boundaries decide the windows; a codec decides how a window is written on an encoded
  key.
- The lifecycle unit is the partition under the root, subtree included.
- History is reconciled from the catalog: gaps repaired at their own modulus, legacy
  leaves kept, nothing renamed.

## Next

- [How it works](../concepts/overview.md) — the model behind all of this
- [Partition schemes](../concepts/schemes.md) — every level type, nesting rules, sliding lists
- [Recipes from real systems](../guide/recipes.md) — this shape and others as ready-made configs
