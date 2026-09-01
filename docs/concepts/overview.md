# How it works

pg-partsmith is a **desired-state loop** over a partitioned table. You describe the shape
the tree should have; it reads the shape the tree has; the difference becomes a plan; the
plan is applied. This page introduces the four pieces and the vocabulary the rest of the
documentation uses.

```text
        config                          catalog
   scheme + policy                pg_inherits + markers
          │                                │
          ▼                                ▼
   TablePartitionConfig               ActualTree
          │                                │
          └──────────► plan_maintenance ◄──┘        pure Python, no I/O
                              │
                              ▼
                       MaintenancePlan          operations with reasons,
                              │                 findings with severities
                              ▼
                        apply(plan)              one statement per transaction,
                                                 revalidated before anything destructive
```

## Four concerns, kept apart

Most partition managers fuse these into one script. pg-partsmith keeps them separate, so
each can be swapped, tested and reasoned about alone.

### 1. Topology — the partition scheme

*Which partitions exist, by which method, on which key.* A `PartitionScheme` is one level
of the tree — `RangePartitioning`, `ListPartitioning` or `HashPartitioning` — with an
optional level below it. `RANGE(created_at) → HASH(tenant_id)` is a range level whose
child is a hash level.

Levels come in two kinds. A **progression** level (a RANGE, or a sliding LIST) produces an
open-ended sequence of windows along an axis; it is the lifecycle dimension, where
partitions are created ahead and expire behind. A **set** level (HASH, or a LIST with
fixed groups) produces a complete set of members that is kept complete and never expires.

→ [Partition schemes](schemes.md)

### 2. Boundaries — where windows begin and end

*How a progression axis is divided.* `TimeBoundaries` cuts time into calendar periods, in a
timezone, optionally over an encoded key (UUIDv7, epoch) through a codec.
`NumericBoundaries` cuts an integer axis into fixed steps. `IntegerSequence` gives a
sliding LIST one value per partition. Boundaries also decide the partition **names**.

The **cursor** is "now" on the axis: the clock for time, the key's high-water mark for
integers, the newest partition for a sliding list.

→ [Boundaries, cursors and calendars](boundaries.md)

### 3. Lifecycle policy — when

*When partitions of the progression level are created, detached and dropped.* A
`LifecyclePolicy` has four parts: **creation** (`CreateAhead`, `CreateUntil`,
`CreateNextIf`), **retention** (`KeepNewest`, `KeepFor`, `KeepBehind`, or any predicate),
**detach mode** and **drop** (`DropAfter` a grace period, or `DropNever`). Rules are pure
predicates over a candidate partition; what they need to know — a size, whether rows are
still referenced — is gathered up front, and only when a rule asks.

→ [Lifecycle policies](lifecycle.md)

### 4. The plan, and its execution

*What to do, why, and what not to touch.* `plan_maintenance` compares the scheme with the
`ActualTree` read from the catalog and returns a `MaintenancePlan`: typed, ordered
operations (`CreatePartition` with its subtree, `AttachPartition`, `DetachPartition`,
`DropPartition`), each with a **reason**; and **findings** — what the planner deliberately
left alone, with a severity. The executor applies it one statement per transaction,
creating subtrees before attaching and re-checking every destructive operation against
the catalog first.

→ [The maintenance plan](plan.md) · [Executing DDL](execution.md)

## Ownership

The question every partition manager has to answer: *is this partition mine to drop?*
pg-partsmith answers it from the catalog, without a metadata table. An attached partition
whose bounds are a window of the scheme's grid is a lifecycle partition; one whose bounds
are not — an archive spanning years, a week straddling two months, a foreign table under
a local-leaves configuration — is reported as unmanaged and never touched. A detached
table is dropped only if it carries the library's `COMMENT` marker, written at detach.

→ [Ownership and safety](ownership.md)

## Leaves

The deepest members of the tree store the rows. By default they are ordinary tables
`LIKE` their parent; they can also carry a tablespace, storage parameters and the parent's
grants, or be foreign tables on an FDW server.

→ [Leaf backends](leaves.md)

## Vocabulary

| Term | Meaning |
|---|---|
| **scheme** | the shape of the tree, level by level |
| **level** | one `PARTITION BY` in the tree; a root or a nested one |
| **progression level** | a level whose members form an open-ended sequence with a cursor: RANGE windows or a sliding LIST |
| **set level** | a level whose members form a complete set: HASH buckets, LIST groups |
| **window** | one slot of a progression axis, `[start, end)`; a month, a 100 000-id step, one list value |
| **grid** | all the windows a boundaries rule can produce; a partition is *on the grid* when its bounds are one of them |
| **cursor** | "now" on an axis: the clock, `max(key)`, the newest partition |
| **lifecycle unit** | the partition directly under a progression level — what is created, counted, hooked and expired as one, subtree included |
| **candidate** | a partition as a policy rule sees it: its window, its facts, the cursor |
| **facts** | what the introspector measured about a partition because a rule asked: size, rows, references, SQL answers |
| **plan** | the operations and findings for one run |
| **finding** | something the planner saw and chose not to change, with a reason and a severity |
| **orphan** | a detached table carrying the library's marker: ours to drop, once the policy allows |
| **marker** | the `COMMENT` written on a table at detach (`pg-partsmith:orphan-parent=…` / `detached-at=…`) |
| **leaf** | a relation that stores rows: the deepest member of every branch |
| **branch** | a partition that partitions further |

The [glossary](../reference/glossary.md) lists every term with links.

## What it is not

- **Not a scheduler.** It runs when you call it. Cron, Celery, APScheduler, a CronJob, a
  start-up hook — any of them.
- **Not a query rewriter.** Partition pruning depends on your queries constraining the
  partition key; the codec helps you compute the bounds, nothing more. See
  [Query a partitioned table](../guide/querying.md).
- **Not an extension.** Plain SQL through SQLAlchemy, as the table's owner. Nothing to
  install in the database.
- **Not a schema migration tool.** It creates, attaches, detaches and drops partitions of a
  table that already exists and is already partitioned; it never alters your columns,
  indexes or constraints — except to drop a detached partition's own foreign keys before
  dropping it.
