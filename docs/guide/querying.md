# Query a partitioned table

A correct partition tree does not make a single query faster. **PostgreSQL skips
partitions only when the query constrains the partition key**, and pg-partsmith never
rewrites your queries or steers the planner. Partitioning buys you cheap retirement of old
data, smaller indexes and per-partition maintenance; the read speedup is something your
`WHERE` clause has to ask for.

Everything below was measured on PostgreSQL 17 against this table — a monthly `RANGE` over
`created_at` with four `HASH` buckets on `tenant_id` per month, three months created, so
twelve leaves:

```python
scheme = RangePartitioning(
    key="created_at",
    boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH),
    child=HashPartitioning(key="tenant_id", modulus=4),
)
```

## Constrain the key of every level you want pruned

Pruning happens level by level. A predicate on the range key picks the months; a predicate
on the hash key picks the bucket inside each surviving month. `EXPLAIN (COSTS OFF)`, with
the filters trimmed:

```sql
SELECT count(*) FROM events;
--  Append
--    ->  Seq Scan on events__2026_09__h0     ⋮  twelve leaves scanned
--    ->  Seq Scan on events__2026_11__h3

SELECT count(*) FROM events
 WHERE created_at >= TIMESTAMPTZ '2026-10-01'
   AND created_at <  TIMESTAMPTZ '2026-11-01';
--  Append
--    ->  Seq Scan on events__2026_10__h0     ⋮  one month, all four buckets
--    ->  Seq Scan on events__2026_10__h3

SELECT count(*) FROM events
 WHERE created_at >= TIMESTAMPTZ '2026-10-01'
   AND created_at <  TIMESTAMPTZ '2026-11-01'
   AND tenant_id = 3;
--  Seq Scan on events__2026_10__h1          ⋮  one leaf, no Append at all

SELECT count(*) FROM events WHERE tenant_id = 3;
--  Append
--    ->  Seq Scan on events__2026_09__h1     ⋮  one bucket per month, every month
--    ->  Seq Scan on events__2026_11__h1
```

What prunes each method:

| Level | Prunes on | Does not prune on |
|---|---|---|
| `RANGE` | `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN` on the key | a predicate on any other column |
| `LIST` | `=`, `IN`, `IS NULL` on the key | ranges over the values |
| `HASH` | equality only | any range — hashing destroys order |

With a composite key, pruning starts from the leading column, as with an index.

## A function of the key is not the key

`date_trunc('month', created_at) = TIMESTAMPTZ '2026-10-01'` reads every one of the twelve
leaves: the planner compares bounds against the key, not against an expression over it.
`created_at::date = DATE '2026-10-05'` and `extract(...)` are the same story. Ask for the
half-open range instead — it is the shape the partitions themselves have:

```sql
WHERE created_at >= TIMESTAMPTZ '2026-10-01' AND created_at < TIMESTAMPTZ '2026-11-01'
```

## Values the planner does not know yet

A parameter or a `STABLE` expression cannot prune at planning time, so PostgreSQL prunes at
executor start instead. It shows up as `Subplans Removed`:

```sql
EXPLAIN (COSTS OFF) SELECT count(*) FROM events WHERE created_at < now() + interval '3 days';
--  Append
--    Subplans Removed: 8
--    ->  Seq Scan on events__2026_09__h0
```

So `now() - interval '7 days'` is fine, and so is a naive `TIMESTAMP '2026-10-01'`
literal against a `timestamptz` key: the cast depends on the session's `TimeZone`, which
moves the pruning from planning time to executor start (`Subplans Removed: 8` on the same
twelve leaves). Just do not expect to read that pruning off the list of scanned nodes —
look for `Subplans Removed`, or for `(never executed)` under `EXPLAIN ANALYZE`.

## Encoded keys: query in the key's own type

When the physical key is an encoded timestamp — a UUIDv7 id, epoch milliseconds — the
partition bounds are UUIDs or integers, and only predicates in *that* type prune. This is
the one piece the library cannot do for you, so it exposes the codec that made the bounds:

```python
codec = UUIDv7BoundaryCodec()
lower = codec.min_uuid_for(datetime(2026, 8, 31, tzinfo=UTC))
upper = codec.min_uuid_for(datetime(2026, 9, 7, tzinfo=UTC))
rows = session.execute(
    text("SELECT * FROM uevents WHERE id >= :lower AND id < :upper"),
    {"lower": str(lower), "upper": str(upper)},
)
```

```text
Bitmap Heap Scan on uevents__2026_w36
  Recheck Cond: ((id >= '01a0551d-a000-7000-8000-000000000000'::uuid) AND (id < '01a0792a-2400-7000-8000-000000000000'::uuid))
```

One week, one partition. Without the range predicate — `WHERE id = …` aside — every week
is scanned. Note that `min_uuid_for` is the *minimum* UUIDv7 for an instant, which is
exactly what the boundaries use, so the predicate lines up with the partition edges
instead of straddling them.

## Local calendars, absolute predicates

A calendar in a business timezone puts the bounds on local midnights: with
`tz=ZoneInfo("Europe/Berlin")`, the daily partition for 29 March 2026 runs from
`2026-03-28 23:00+00` to `2026-03-29 22:00+00`. A predicate written as a UTC day therefore
covers parts of two partitions — correct results, one extra partition scanned. Write the
range you actually mean, with an offset or a zone-aware value, and let PostgreSQL compare
instants.

## Indexes still matter

Each leaf is created with `LIKE parent INCLUDING ALL`, so the parent's indexes exist on
every partition and new partitions inherit them automatically. Pruning decides *which*
partitions are opened; the index decides how much of one is read. And remember the
PostgreSQL rule that shapes the schema: a primary key or unique constraint on a
partitioned table must include every partition key column — `(created_at, tenant_id, id)`
for the table above.

Two settings are worth knowing: `enable_partition_pruning` (on by default; turning it off
is a debugging tool) and `plan_cache_mode`, which decides whether a prepared statement gets
a generic plan and therefore run-time rather than planning-time pruning.

## What this library does not do

- It does not rewrite queries, add indexes, or route reads to a particular partition.
- It does not need `constraint_exclusion`: declarative partitioning uses partition
  pruning, which reads the catalog bounds directly.
- Querying a detached partition is a plain table read — after `DETACH`, rows are no longer
  visible through the parent. That is the point of the
  [detach-then-drop grace period](../concepts/lifecycle.md), and it is worth checking that
  your reporting queries do not silently lose a month when one is retired.
