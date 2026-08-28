# Partition schemes

A **scheme** describes the shape of a partition tree, one level at a time: the PostgreSQL
method, the key, how children are named, and — optionally — the level below. The same three
classes describe a root and a nested level.

```python
from pg_partsmith import HashPartitioning, ListGroup, ListPartitioning, RangePartitioning, TimeBoundaries

RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH))
HashPartitioning(key="tenant_id", modulus=16)
ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("de", "fr")),), include_default=True)
```

Every level has a `key` (one column, or several in key order) and an optional `child`.

## Two kinds of level

| Kind | Classes | What the planner does with it |
|---|---|---|
| **progression** | `RangePartitioning` | An ordered, open-ended sequence of windows with a *cursor*. Partitions are created ahead of the cursor and expire behind it — the lifecycle dimension. Its partition is the lifecycle unit, subtree included. |
| **set** | `HashPartitioning`, `ListPartitioning` | A fixed, complete set of members. Missing members are created; nothing ever expires. |

So `retention_count=12` on a `RANGE(time) → HASH(tenant)` table keeps twelve periods,
however many buckets each holds.

## RANGE

```python
RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK))
RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=100_000))
```

`boundaries` is the rule that divides the axis into windows — see
[Boundaries and codecs](boundary-codecs.md). Time boundaries name partitions the way the
period calculators always have (`events__2026_w35`); numeric ones after the window's start
(`queue__100000`).

Only the leading key column carries the window. A composite key's trailing columns are
bounded with `MINVALUE` at both ends:

```python
RangePartitioning(key=("created_at", "tenant_id"), boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK))
```

```sql
FOR VALUES FROM ('2026-08-24', MINVALUE) TO ('2026-08-31', MINVALUE)
```

PostgreSQL adds an `IS NOT NULL` test for every key column, so a row with a NULL trailing
value routes to DEFAULT whatever its leading value — and DEFAULT is never pruned. Declare
trailing key columns `NOT NULL` unless you want that.

## HASH

```python
HashPartitioning(key="tenant_id", modulus=4, name_suffix="__h{remainder}")
```

Members are `MODULUS 4, REMAINDER 0..3`, named by appending `name_suffix` to the parent's
name. `modulus` is the bucket count for **newly created** sets only: existing sets keep
the modulus they were built with, because a hash set cannot change modulus without a
rewrite. Lowering or raising it is a rolling change — new periods use the new count,
history keeps what it has (see [convergence](planning.md#convergence-rules)).

## LIST

```python
ListPartitioning(
    key="region",
    groups=(
        ListGroup(name="eu", values=("de", "fr", "es")),
        ListGroup(name="us", values=("us", "ca")),
    ),
    include_default=True,      # a catch-all for values you did not list
    default_name="other",
)
```

A LIST level is never "complete" — there is always another value the world could produce —
so there is no gap to detect, only groups that do not exist yet. Without a DEFAULT
partition, a row with an unlisted value is rejected. Groups are matched by **the values
they own, not by name**, so a tree another tool named differently is recognised and left
alone. A value belongs to exactly one partition: a configured group claiming a value another
partition owns is reported (`list_values_conflict`), never forced.

Values are written as SQL string literals, which PostgreSQL coerces to the key's type
(`values=("1", "2")` for an integer key). LIST takes exactly one key column.

## Nesting

Any level can carry a `child`:

```python
RangePartitioning(
    key="created_at",
    boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH),
    child=ListPartitioning(
        key="region",
        groups=(ListGroup(name="eu", values=("de", "fr")), ListGroup(name="us", values=("us",))),
        child=HashPartitioning(key="tenant_id", modulus=4),
    ),
)
```

A progression level below a set level is allowed too — `LIST(tier) → RANGE(created_at)`
gives every tier its own monthly lifecycle:

```python
ListPartitioning(
    key="tier",
    groups=(ListGroup(name="gold", values=("gold",)), ListGroup(name="free", values=("free",))),
    child=RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)),
)
```

Rules the model enforces (all PostgreSQL's): every level partitions on a fresh column, depth
is bounded (`MAX_SCHEME_DEPTH`), every generated name fits 63 bytes at every level
(PostgreSQL truncates identifiers silently, which would collapse two siblings onto one
name).

## Required unique constraints

PostgreSQL requires every `UNIQUE` / `PRIMARY KEY` on a partitioned table to contain **all**
partition-key columns of every level. Adding a hash dimension adds a column to that
requirement:

```sql
-- RANGE(created_at) only
PRIMARY KEY (id, created_at)
-- RANGE(created_at) → HASH(tenant_id)
PRIMARY KEY (id, tenant_id, created_at)
```

pg-partsmith never changes your schema. Validation reads the constraints and refuses a
config PostgreSQL would reject, naming the column and the constraints that need it, before
any DDL runs.

## How a partition is built

1. `CREATE TABLE child (LIKE parent INCLUDING ALL EXCLUDING IDENTITY) [PARTITION BY …]` —
   standalone, `ACCESS SHARE` on the live parent only.
2. Its own subtree is created and attached inside it, deepest first.
3. `ALTER TABLE parent ATTACH PARTITION child FOR VALUES …` — `SHARE UPDATE EXCLUSIVE` on the
   parent.

Until step 3 commits the child is invisible to row routing, so a crash anywhere before it
leaves a detached table no writer can reach — never a live branch that rejects part of its
keyspace. The next run finds the table, completes its subtree, and attaches it.

`ATTACH` rather than `CREATE … PARTITION OF` is deliberate: the latter takes
`ACCESS EXCLUSIVE` on the parent and stalls every writer. Measured lock levels are in
[PostgreSQL semantics](../design/postgresql-semantics.md).

## The flat spelling

The ordinary time-partitioned table keeps its five-line form, which is sugar for a
`RangePartitioning` over `TimeBoundaries` and a `LifecyclePolicy` of `CreateAhead` +
`KeepNewest`:

```python
TablePartitionConfig(
    table_name="events",
    partition_column="created_at",
    trailing_partition_columns=(),          # composite key tail, optional
    granularity=PartitionGranularity.MONTH,
    tz="Europe/Helsinki",                   # optional
    boundary_codec="uuidv7",                # optional
    subpartition=HashPartitioning(key="tenant_id", modulus=4),   # optional level below
    create_ahead_count=3,
    retention_count=12,
)
```

`partition_type` and `partition_strategy` are still accepted and checked against the
scheme; they are no longer required. `config.scheme` and `config.lifecycle` expose the
composed form either way.

## Introspection

```python
tree = await service.inspect(config)              # ActualTree: root + marker-tagged orphans
for node in tree.root.walk():
    print("  " * node.level, node.name, node.relkind, node.describe_topology(), node.bounds)
for orphan in tree.orphans:
    print(orphan.name, orphan.detached_at)
```

Each `PartitionNode` reports both halves of its identity — `bounds` for how it sits in its
parent, `partition_type` / `partition_columns` for how it partitions its own children — plus
its `oid`, its `relkind` (a foreign table is inspected, never touched) and whether a
`DETACH CONCURRENTLY` left it pending.
