# Partition schemes

A **scheme** describes the shape of a partition tree, one level at a time: the PostgreSQL
method, the key, how children are named, and — optionally — the level below. The same
three classes describe a root and a nested level.

```python
from pg_partsmith import (
    HashPartitioning,
    IntegerSequence,
    ListGroup,
    ListPartitioning,
    PartitionGranularity,
    RangePartitioning,
    TimeBoundaries,
)

RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH))
HashPartitioning(key="tenant_id", modulus=16)
ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("de", "fr")),), include_default=True)
ListPartitioning(key="partition_id", sequence=IntegerSequence(start=100))     # a sliding list
```

Every level has a `key` — one column, or several in key order — and an optional `child`.

## Progression levels and set levels

The planner treats a level in one of two ways, and the distinction runs through
everything else.

| Kind | Levels | What the planner does |
|---|---|---|
| **progression** | `RangePartitioning`; `ListPartitioning(sequence=…)` | The members form an open-ended sequence of windows with a *cursor*. Partitions are created ahead of the cursor and expire behind it. This is the lifecycle dimension; its partition is the **lifecycle unit**, subtree included. |
| **set** | `HashPartitioning`; `ListPartitioning(groups=…)` | The members form a fixed, complete set. Missing members are created (the set is *reconciled*); nothing ever expires. |

So `retention_count=12` on a `RANGE(time) → HASH(tenant)` table keeps twelve months,
however many buckets each holds. Hooks fire once per month; `created_count` counts
months.

A scheme with no progression level — a root `HASH`, a `LIST` of fixed groups — has a fixed
partition set. Maintenance creates whatever is missing and otherwise issues no DDL; the
lifecycle policy is ignored.

## RANGE

```python
RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK))
RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=100_000))
```

`boundaries` is the rule that divides the axis into windows and names them — see
[Boundaries, cursors and calendars](boundaries.md). Time boundaries name partitions after
the period (`events__2026_w35`); numeric ones after the window's start (`queue__100000`).

```text
events                          PARTITION BY RANGE (created_at)
├── events__2026_08             FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')
├── events__2026_09             FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')
└── events__2026_10             FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')
```

### Composite keys

Only the leading key column carries the window. Trailing columns are bounded with
`MINVALUE` at both ends:

```python
RangePartitioning(key=("created_at", "tenant_id"), boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK))
```

```sql
FOR VALUES FROM ('2026-08-24', MINVALUE) TO ('2026-08-31', MINVALUE)
```

PostgreSQL adds an `IS NOT NULL` test for every key column to a partition's constraint,
so a row with a NULL trailing value goes to the DEFAULT partition whatever its leading
value — and the DEFAULT partition is never pruned. Declare trailing key columns `NOT NULL`
unless you want that.

## HASH

```python
HashPartitioning(key="tenant_id", modulus=4, name_suffix="__h{remainder}")
```

Members are `MODULUS 4, REMAINDER 0..3`, named by appending `name_suffix` to the parent's
name (`events__2026_08__h0`). A hash set is complete when its residue classes tile the
keyspace; a missing bucket means every row hashing into it is rejected outright, which
is why gaps are repaired, not reported.

`modulus` is the bucket count for **newly created** sets only. An existing set keeps the
modulus it was built with — a hash set cannot change modulus without a rewrite, and
PostgreSQL refuses a bucket that would overlap the existing ones. Changing `modulus` is a
rolling change: new periods use the new count, history keeps what it has, and a gap in an
old set is repaired at that set's own modulus. See [Change a scheme safely](../guide/changing-the-scheme.md).

## LIST with groups

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

A LIST level is never "complete" — there is always another value the world could produce
— so there is no gap to detect, only groups that do not exist yet. Without a DEFAULT
partition a row with an unlisted value is rejected; `include_default` maintains one.

Groups are matched by **the values they own, not by name**: a tree another tool named
differently is recognised and left alone. A value belongs to exactly one partition, so a
configured group claiming a value another partition already owns is reported
(`list_values_conflict`), never forced.

Values are written as SQL string literals and coerced by PostgreSQL to the key's type
(`values=("1", "2")` for an integer key). A LIST key is one column.

## LIST with a sequence — the sliding list

```python
ListPartitioning(key="partition_id", sequence=IntegerSequence(start=100))
```

With a `sequence` instead of `groups`, a LIST level is a **progression**: every partition
owns one integer value (`FOR VALUES IN (101)`), the newest partition is where the
application writes, and the lifecycle policy opens the next value and retires old ones.
This is GitLab's sliding list for `ci_builds`.

```text
ci_builds                       PARTITION BY LIST (partition_id)
├── ci_builds__100              FOR VALUES IN (100)
├── ci_builds__101              FOR VALUES IN (101)
└── ci_builds__102              FOR VALUES IN (102)      ← the application writes 102
```

The cursor is the **newest partition** — no query, no clock — which has a consequence for
the creation rule: "create N ahead" would open another partition on every run, so
`CreateAhead` is refused at construction. Rotate the sequence with `CreateNextIf(when)`
(open 103 once 102 satisfies `when`) or bound it with `CreateUntil(position)`. The
application reads the current value from the catalog (`service.inspect`) and writes it.

Windows are `[value, value + 1)`, so every retention rule written for an integer axis
works: `KeepNewest(3)` keeps the three newest values. A hand-made partition owning several
values, a non-integer value or `NULL` is `unmanaged_partition`: inspected, never touched,
and a value it owns is never created. A sliding list has no DEFAULT partition.

`IntegerSequence(cursor_source=CursorSource.MAX_KEY)` reads the cursor from the data
instead, and then `CreateAhead` is allowed.

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

```text
events                                   RANGE (created_at)
└── events__2026_08                      LIST (region)
    ├── events__2026_08__eu              HASH (tenant_id)
    │   ├── events__2026_08__eu__h0
    │   └── …
    └── events__2026_08__us              HASH (tenant_id)
```

A progression below a set level is allowed too. `LIST(tier) → RANGE(created_at)` gives
every tier its own monthly lifecycle:

```python
ListPartitioning(
    key="tier",
    groups=(ListGroup(name="gold", values=("gold",)), ListGroup(name="free", values=("free",))),
    child=RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)),
)
```

Rules the model enforces at construction, all of them PostgreSQL's:

- every level partitions on a fresh column;
- depth is bounded (`MAX_SCHEME_DEPTH`, five levels);
- every generated name fits 63 bytes at every level — PostgreSQL truncates identifiers
  silently, which would collapse two siblings onto one name. `TablePartitionConfig`
  refuses a table name that leaves no room for its scheme's suffixes.

## Required unique constraints

PostgreSQL requires every `UNIQUE` / `PRIMARY KEY` on a partitioned table to contain
**all** partition-key columns of every level. Adding a hash dimension adds a column to
that requirement:

```sql
-- RANGE (created_at) only
PRIMARY KEY (id, created_at)
-- RANGE (created_at) → HASH (tenant_id)
PRIMARY KEY (id, tenant_id, created_at)
```

pg-partsmith never changes your schema. The service reads the constraints at plan time
and refuses a configuration PostgreSQL would reject, naming the column and the
constraints that need it, before any DDL runs:

```text
InvalidPartitionConfigError: Subpartition column(s) 'tenant_id' missing from unique
constraint(s) (id, created_at) on table 'public.events'. PostgreSQL requires every
UNIQUE/PRIMARY KEY on a partitioned table to include all partition key columns, so add
'tenant_id' to them before enabling this partitioning.
```

## Names

Names are derived, never parsed for truth. A time level names partitions after the period
(`__2026_08`, `__2026_w35`, `__2026_q3`, `__2026_08_15`, `__2026_08_15_09`); a numeric
level after the window's start (`__100000`, `__m100` for a negative start); a sliding list
after its value (`__101`); hash and list levels append `name_suffix`
(`__h{remainder}`, `__{name}`). To keep an existing convention, plug it in as a custom
calculator or a `name_suffix` — see [Custom calendars, names and codecs](../guide/calendars-and-codecs.md).

Existing partitions are matched to windows by their **bounds** in the catalog. Names are
read only to recognise a detached orphan.

## The flat spelling

The ordinary time-partitioned table keeps its short form, which is sugar for a
`RangePartitioning` over `TimeBoundaries` and a `LifecyclePolicy` of `CreateAhead` +
`KeepNewest`:

```python
TablePartitionConfig(
    table_name="events",
    partition_column="created_at",
    trailing_partition_columns=(),                                # composite key tail
    granularity=PartitionGranularity.MONTH,
    tz="Europe/Helsinki",                                         # optional
    boundary_codec="uuidv7",                                      # optional
    subpartition=HashPartitioning(key="tenant_id", modulus=4),    # optional level below
    create_ahead_count=3,
    retention_count=12,
)
```

`config.scheme` and `config.lifecycle` expose the composed form either way. See
[Configure a table](../guide/configuration.md) for every field.

## Introspection

```python
tree = await service.inspect(config)            # ActualTree, or None if the table is not partitioned
for node in tree.root.walk():
    print("  " * node.level, node.name, node.relkind.value, node.describe_topology(), node.bounds)
for orphan in tree.orphans:
    print("orphan", orphan.name, orphan.detached_at)
```

Each `PartitionNode` reports both halves of its identity — `bounds` for how it sits in its
parent, `partition_type` / `partition_columns` for how it partitions its own children —
plus its `oid`, its `relkind` (table, partitioned table, foreign table) and whether an
interrupted `DETACH CONCURRENTLY` left it pending.
