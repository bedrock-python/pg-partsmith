# Subpartitioning

A time partition can itself be a partitioned table. The classic layout gives one leaf per
period; subpartitioning splits each period along a second dimension:

```text
events                          PARTITION BY RANGE (created_at)
├── events__2026_w35            PARTITION BY HASH (tenant_id)   ← a partition AND a partitioned table
│   ├── events__2026_w35__h0    MODULUS 4, REMAINDER 0
│   ├── events__2026_w35__h1    MODULUS 4, REMAINDER 1
│   ├── events__2026_w35__h2    MODULUS 4, REMAINDER 2
│   └── events__2026_w35__h3    MODULUS 4, REMAINDER 3
└── events__2026_w36
    └── …
```

The two dimensions do different jobs, and keeping them apart is the whole point:

| Dimension | Purpose |
|-----------|---------|
| Top-level `RANGE` over time | **Lifecycle** — create-ahead, retention, detach, drop, cold-storage export |
| Nested `HASH` | **Distribution and pruning** — spreading a multi-tenant workload, letting the planner skip buckets |
| Nested `LIST` | **Explicit segmentation** — a partition per region, tier or tenant class |

Retention is therefore always counted in *time periods*, never in leaves:
`retention_count=12` keeps twelve weeks, whether each week holds one table or sixteen.

## Configuration

Add a `subpartition` spec to an otherwise ordinary config:

```python
from pg_partsmith import (
    HashSubpartitionSpec,
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_type=PartitionType.RANGE,
    partition_strategy=PartitionStrategy.TIME_BASED,
    partition_column="created_at",
    granularity=PartitionGranularity.WEEK,
    create_ahead_count=3,
    retention_count=12,
    subpartition=HashSubpartitionSpec(column="tenant_id", modulus=4),
)
```

Nothing else changes: the same `PartitionLifecycleService`, the same
`PartitionMaintainer`, the same hooks and lock managers. Omit `subpartition` and you get
the flat layout exactly as before.

### Bucket names

Buckets are named by appending `name_suffix` to the branch's name, giving
`events__2026_w35__h0` by default. Override it when adopting a tree that another tool
named differently:

```python
HashSubpartitionSpec(column="tenant_id", modulus=4, name_suffix="_h{remainder}")
```

The template must contain `{remainder}` and otherwise only lowercase identifier
characters. Because PostgreSQL truncates identifiers at 63 bytes **silently** — which
would collapse two buckets onto one name — `TablePartitionConfig` adds the bucket suffix
to its length check and refuses a table name that would overflow.

### LIST instead of HASH

Where HASH divides a level into anonymous buckets, LIST divides it into named
partitions with explicit value sets:

```python
from pg_partsmith import ListGroup, ListSubpartitionSpec

subpartition=ListSubpartitionSpec(
    column="region",
    groups=(
        ListGroup(name="eu", values=("de", "fr", "es")),
        ListGroup(name="us", values=("us", "ca")),
    ),
    include_default=True,   # a catch-all for values you did not list
)
```

giving `events__2026_w35__eu`, `events__2026_w35__us` and
`events__2026_w35__other`.

The two strategies differ in one way that matters for reconciliation: **a LIST
level is never complete.** There is always another value the world could
produce, so there is no "gap" to detect — only groups that do not exist yet.
That is what `include_default` is for: without a DEFAULT partition, a row
carrying an unconfigured value is *rejected*, exactly as a missing hash bucket
would reject one.

Groups are matched by **the values they own, not by their name**, so a tree
built by another tool under a different naming convention is recognised and
left alone rather than duplicated.

A value belongs to exactly one partition. If a configured group claims a value
that some other partition already owns, moving it would mean detaching that
partition — so reconciliation creates the groups it safely can, and reports the
clash through `MaintenanceResult.issues`:

```text
PartitionTopologyError: public.events__2026_w35 cannot gain the configured LIST
partition 'eu': PostgreSQL already routes 'de' in public.events__2026_w35__dach.
```

Values are written as SQL string literals, which PostgreSQL coerces to the
partition key's type — so a numeric key is configured as `values=("1", "2")`.

### Deeper trees

A spec can carry its own `subpartition`, up to four levels, and the strategies mix freely
(`RANGE(time) → LIST(region) → HASH(tenant_id)`):

```python
HashSubpartitionSpec(
    column="tenant_id",
    modulus=4,
    subpartition=HashSubpartitionSpec(column="shard_id", modulus=2),
)
```

## Tables with no time dimension

Not every partitioned table is partitioned by time. A table divided only by
tenant, or only by region, has a **fixed** set of partitions: nothing is created
ahead of the clock and nothing ages out. Those are configured with
`root_layout` and the matching strategy:

```python
config = TablePartitionConfig(
    table_name="issue_index",
    partition_type=PartitionType.HASH,
    partition_strategy=PartitionStrategy.HASH_BASED,
    partition_column="organization_id",
    root_layout=HashSubpartitionSpec(column="organization_id", modulus=16),
)
```

`VALUE_BASED` works the same way with a `ListSubpartitionSpec`. Either can carry
its own `subpartition` to nest further.

Such a table has no periods, so it needs no period calculator:

```python
service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine),
    locks=PostgresAdvisoryLockManager(engine),
)                                    # no period_calculator
```

Maintenance is then **only** reconciliation: the configured partitions are
created if missing, and `created_count` reports how many were made.
`detached_count` and `dropped_count` are always zero — there is no retention
window, because there is no time. `create_ahead_count` and `retention_count` are
ignored, and `granularity` must be unset.

Calling a period-driven API (`create_future_partitions`, `ensure_partitions`,
`get_partitions_for_pruning`) on a service built without a calculator raises
`InvalidPartitionConfigError` explaining that this wiring has no periods.

The same convergence rules apply as for a nested level — an existing hash set at
another modulus is preserved, a LIST value another partition owns is reported —
because it is the same planner, just pointed at the root.

## Composite partition keys

A partition key may span several columns. `partition_column` keeps naming the
leading one; the rest go in `trailing_partition_columns`, in key order:

```python
config = TablePartitionConfig(
    table_name="events",
    partition_type=PartitionType.RANGE,
    partition_strategy=PartitionStrategy.TIME_BASED,
    partition_column="created_at",
    trailing_partition_columns=("tenant_id",),
    granularity=PartitionGranularity.WEEK,
)
```

`config.partition_columns` reads the whole key back as a tuple, and
`config.key_arity` is its length. Splitting the field this way is what keeps a
single-column config — and every metadata provider and repository written
against one — working unchanged.

Only the **leading** column carries the period. The trailing ones are bounded
with `MINVALUE` at both ends:

```sql
FOR VALUES FROM ('2026-08-24', MINVALUE) TO ('2026-08-31', MINVALUE)
```

Retention reads the leading value back out, so pruning behaves identically.

Subpartition levels take composite keys the same way — `column` plus
`trailing_columns`:

```python
HashSubpartitionSpec(column="tenant_id", trailing_columns=("shard_id",), modulus=8)
```

### NULLs in a trailing column

A composite bound does **not** select the same rows a single-column bound would.
PostgreSQL adds an `IS NOT NULL` test for *every* key column to a range
partition's constraint:

```sql
-- pg_get_partition_constraintdef('events__2026_w35')
(created_at IS NOT NULL) AND (tenant_id IS NOT NULL)
  AND (created_at >= '2026-08-24…') AND (created_at < '2026-08-31…')
```

So a row whose `tenant_id` is NULL is routed to the DEFAULT partition whatever
its `created_at` says. Two consequences worth knowing before you make a trailing
key column nullable:

- **Those rows never age out.** Retention drops period partitions; the DEFAULT
  partition is not one, and is never pruned.
- **They stay in DEFAULT during reconciliation.** When a DEFAULT conflict blocks
  an attach, the rows moved out are only those the new partition can actually
  accept — the NULL-keyed ones are left where they belong. Moving them would be
  rejected with the very error the move exists to clear.

Declaring every trailing key column `NOT NULL` avoids both.

### Limits

Two of them, both PostgreSQL's rather than this library's:

- **LIST takes exactly one column.** A composite LIST key is rejected outright
  by PostgreSQL, and the config refuses it up front.
- **Every key column must appear in every UNIQUE/PRIMARY KEY**, which now means
  all of them, not just the leading one.

> Key order is not column order. `pg_partsmith` reads it from `partattrs`'
> own ordering — sorting by column position would silently transpose a
> composite key.

### Expression keys are refused

`PARTITION BY RANGE ((created_at AT TIME ZONE 'UTC'))` is a valid PostgreSQL
table and not one this library can manage: it builds bounds out of column
values, and an expression's value is not one. The catalog records such a key
position as `attnum 0`, so reading only the columns would report a shorter key
than the table has and every bound built from it would be the wrong arity.
Introspecting one raises `InvalidPartitionConfigError` naming the position.

## Required unique constraints

PostgreSQL requires every `UNIQUE` / `PRIMARY KEY` constraint on a partitioned table to
contain **all** of its partition-key columns. Adding a hash dimension therefore adds a
column to that requirement:

```sql
-- Before: RANGE(created_at) only
PRIMARY KEY (id, created_at)

-- After: RANGE(created_at) → HASH(tenant_id)
PRIMARY KEY (id, tenant_id, created_at)
```

pg-partsmith never changes your schema. It reads the constraints during validation and
refuses a config it knows PostgreSQL would reject, naming the column and the constraints
that need it — before any DDL runs:

```text
InvalidPartitionConfigError: Subpartition column 'tenant_id' is missing from unique
constraint(s) (id, created_at) on table 'public.events'. PostgreSQL requires every
UNIQUE/PRIMARY KEY on a partitioned table to include all partition key columns, so add
'tenant_id' to them before enabling this subpartitioning.
```

## How a branch is built

Creation order is deliberate, and it is what makes an interrupted run harmless:

1. `CREATE TABLE branch (LIKE parent INCLUDING ALL) PARTITION BY HASH (tenant_id)` —
   standalone, taking only an `ACCESS SHARE` lock on the live root table.
2. Every bucket is created and attached **to the still-detached branch**.
3. `ALTER TABLE parent ATTACH PARTITION branch FOR VALUES FROM … TO …`.

Until step 3 commits, the branch is invisible to row routing. A crash anywhere before it
leaves a detached table that no writer can reach — never a branch that is live but cannot
route part of its keyspace. The next maintenance run finds that table, completes it, and
attaches it.

This keeps the library's existing transaction semantics (each DDL statement in its own
transaction, committed immediately) while giving the property that actually matters:
**no partially-covering branch is ever reachable from the root.**

??? note "Why not one transaction per subtree?"

    Three options were considered for making subtree creation crash-safe.

    **A — wrap the whole subtree in one transaction.** All-or-nothing, but it holds DDL
    locks for the duration of every bucket creation, scales that hold time with the bucket
    count, and abandons the per-statement transaction semantics the rest of the library
    (and its `continue_on_error` reporting) is built on.

    **B — commit each node independently and let the next run reconcile.** Matches the
    existing design and keeps transactions small, but between two runs the root can hold a
    branch that rejects writes for part of the hash keyspace. That is the failure mode this
    whole feature exists to prevent.

    **C — commit each node independently, but attach the branch last.** Chosen. Ordering
    substitutes for atomicity: reachability from the root is itself the commit point, so
    every intermediate state is either "not there" or "complete". A crash leaves a detached
    table that no writer can reach and the next run completes; transactions stay per
    statement; locks on the live table stay minimal.

    The same ordering is used one level down, which is why a nested action creates its own
    children before attaching itself.

Repairing an *existing* branch cannot use that trick — the branch is already live — so
attachment is used rather than `CREATE TABLE … PARTITION OF`. Measured on PostgreSQL 17:

| Statement | Lock on the branch |
|---|---|
| `ALTER TABLE branch ATTACH PARTITION leaf …` | `SHARE UPDATE EXCLUSIVE` — reads and writes continue |
| `CREATE TABLE leaf PARTITION OF branch …` | `ACCESS EXCLUSIVE` — every writer through that branch stalls |

Filling a gap therefore does not interrupt ingestion for the tenants already served by the
branch.

**One exception, also measured:** if the branch has a `DEFAULT` partition, `ATTACH` takes
`ACCESS EXCLUSIVE` on *that* partition while it scans it for rows the new one would claim.
Reads and writes routed to the other buckets continue; anything touching the DEFAULT
partition waits. A branch whose keyspace is fully tiled has no DEFAULT partition and pays
nothing for this.

## Reconciliation

Maintenance is a convergent desired-state loop. Between creating and pruning, every
attached partition's subtree is compared against the configured spec, and only genuinely
missing buckets are created. Partitions the run is about to prune are skipped.

An incomplete hash set is not cosmetic: PostgreSQL rejects any row whose key hashes into
a missing remainder with `no partition of relation … found for row`. Repair is what
restores ingestion for that slice of tenants.

### What reconciliation does

| Actual state | Action |
|---|---|
| Branch missing some buckets at the configured modulus | Create exactly the missing ones |
| Branch complete at the configured modulus | Nothing — **zero DDL, zero locks** |
| Branch complete at a *different* modulus | Leave it; it already tiles the keyspace |
| Branch incomplete at a *different* modulus | Fill the gaps **at the branch's own modulus** |
| Branch is a plain leaf from an older policy | Leave it; new periods use the new topology |
| Branch subpartitioned by another strategy or column | Leave it; report an issue |
| Hash siblings at mixed moduli that still tile the keyspace | Leave it; no issue |
| Hash siblings at mixed moduli leaving a gap | Leave it; report an issue |
| LIST group missing | Create it |
| LIST group present under another name but the same values | Leave it |
| LIST group whose value another partition owns | Leave it; report an issue |

### Why a modulus is never changed

A hash partition owns the rows whose key hash is congruent to `remainder` modulo
`modulus`. Adding `MODULUS 2` buckets to a branch built with `MODULUS 4` would overlap
the existing ones, and PostgreSQL rejects it. Changing a modulus means rewriting the
data.

So lowering or raising `modulus` in your config is a **rolling** change: new periods use
the new count, history keeps what it has.

```text
2026-W33 → MODULUS 4   (built under the old config, complete → untouched)
2026-W34 → MODULUS 4   (built under the old config, complete → untouched)
2026-W35 → MODULUS 2   (built under the new config)
```

The one exception is a historical branch that is *incomplete*. Leaving it alone would
leave some tenants unable to write until the period ages out, so its gaps are filled —
at the modulus it already uses, which is the only modulus that cannot overlap it.

### Mixed moduli

PostgreSQL permits hash siblings at different moduli as long as their residue classes do
not overlap: `(MODULUS 2, REMAINDER 1)` alongside `(MODULUS 4, REMAINDER 0)` and
`(MODULUS 4, REMAINDER 2)` is a legal, complete tiling. pg-partsmith computes actual
coverage over the least common multiple of the moduli rather than assuming uniformity, so
such a set is recognised as healthy and left alone. Only a real gap is reported.

### Reported issues

Divergences that reconciliation refuses to repair land in `MaintenanceResult.issues` with
`step=MaintenanceIssueStep.RECONCILE` and the branch in `partition_name`:

```python
result = await maintainer.run_maintenance_safe(config)

for issue in result.issues:
    if issue.step is MaintenanceIssueStep.RECONCILE:
        log.warning("%s: %s", issue.partition_name, issue.error)
```

These are reported **regardless of `continue_on_error`** — a branch rejecting writes must
not stay silent — and they never abort the run, so one odd historical partition cannot
stop every other table from being maintained. Expected steady states (a legacy leaf, a
preserved older modulus) are logged, not reported as issues.

To see what reconciliation would do without running maintenance:

```python
result = await service.reconcile_subpartitions(config)
print(result.created_count, result.findings)
```

### Targeting specific periods

Reconciliation runs over whatever is attached. To *create* a period that create-ahead has
not reached — backfilling history during a migration, or guaranteeing a writer's target
exists — name the periods explicitly:

```python
await service.ensure_partitions(config, [Period(year=2026, week=33), Period(year=2026, week=34)])
await service.ensure_partition(config, Period(year=2026, week=35))
```

Both build the complete bucket set for each period before attaching it, and both are
idempotent.

## Lifecycle of a branch

The **time partition is the lifecycle unit**; buckets are an implementation detail.

- **Detach** — `ALTER TABLE root DETACH PARTITION branch` removes the whole subtree from
  the active tree in one step. `DETACH … CONCURRENTLY` works on a branch just as on a
  leaf.
- **Drop** — `DROP TABLE branch` removes its buckets with it; no `CASCADE` is needed and
  none is used.
- **Orphan marker** — written on the branch root only, since that is the relation
  detach/drop acts on. Safe-drop checks it there.
- **Hooks** — fire once per branch. `before_drop` receives the time slice, which is what
  a cold-storage export wants, rather than one call per bucket.
- **`is_partition_closed`** — reads the branch's own `RANGE` bound in the root table, so
  it answers for the whole subtree.

## Introspection

`list_partitions` still returns one entry per period — the lifecycle's view — now
carrying `subpartition_type` and structured `bounds`:

```python
for p in await metadata.list_partitions("public.events"):
    print(p.name, p.is_subpartitioned, p.bounds)
```

For the full shape, `get_partition_tree` walks the hierarchy in a single query:

```python
tree = await metadata.get_partition_tree("public.events")

for node in tree.walk():
    print("  " * node.level, node.name, node.describe_topology(), node.bounds)
```

Each `PartitionNode` reports both halves of its identity — `bounds` for how it sits in
its parent, `partition_type` / `partition_columns` for how it partitions its own children.
A branch has both; a leaf only the first.

Adoption needs nothing special: the tree is read from `pg_catalog`, so partitions created
by `pg_partman`, a hand-rolled manager, or a migration are discovered and reconciled like
any other. The orphan marker governs only what may be **dropped**, never what may be seen.
