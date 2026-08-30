# Ownership and safety

The question any partition manager has to answer before dropping anything: *is this
mine?* pg-partsmith answers it from the catalog, without a metadata table, and answers
"no" whenever it is not sure. This page is the whole set of rules.

## Attached partitions: alignment with the grid

An attached partition is a lifecycle partition — one the policy may expire, detach and
drop — when its bounds are a window of the scheme's grid, or lie inside one. Otherwise it
is **unmanaged**: inspected, reported, never touched.

| An attached partition whose bounds… | is | The lifecycle may |
|---|---|---|
| are a window of the boundaries' grid — the same month, the same ISO week, the same 100 000-id step | managed | create below it, detach, drop |
| lie inside one window — a day inside a monthly grid, left by an earlier finer configuration | managed | the same; retention acts on it by its upper bound |
| are not on the grid — an archive spanning years, a week straddling two months | `unmanaged_partition` (INFO) | inspect and report only; a wanted window it overlaps is `range_overlap` (WARNING) and is not created |
| are open on one side (`MINVALUE`, `MAXVALUE`, `infinity`) | `unbounded_partition` (INFO) | never prune: it holds current data by definition |
| cannot be read on the level's axis | `unreadable_bound` (WARNING) | never prune — guessing risks dropping live data |
| own several values, a non-integer value or `NULL`, on a sliding list | `unmanaged_partition` (INFO) | inspect and report only |
| belong to a foreign table, under a `LocalLeaves` configuration | `foreign_partition` (INFO) | nothing — it is someone else's; under `ForeignLeaves` a foreign partition is managed like any other |
| are pending an interrupted `DETACH CONCURRENTLY` | `detach_pending` (INFO) | complete the detach with `FINALIZE`; the drop follows the policy |

Alignment is the safe generalisation of "did we create it". A partition whose bounds are
exactly the window the scheme would have produced is indistinguishable from ours and is
treated as ours — which is what lets a tree built by another tool, or by hand, be
adopted without renaming anything. A DBA's `events_archive_2000_2019` never aligns with a
monthly grid and is never touched.

Names play no part. Existing partitions are matched to windows by
`pg_get_expr(relpartbound)`; names are read only to recognise a detached orphan.

## Detached tables: the marker

A partition that is detached stays in the database. What makes it the library's to drop
is a `COMMENT` written on it **before** the detach — in the same transaction for the
blocking form; committed just ahead of it for `CONCURRENTLY`, which cannot run inside a
transaction block:

```text
pg-partsmith:orphan-parent=public.events
pg-partsmith:detached-at=2027-10-02T03:00:00+00:00
<any comment the table already carried>
```

Only tables carrying the marker — **orphans** — are ever dropped. A detached table without
it is invisible to the lifecycle: not dropped, not re-attached, not counted. The second
line is what a grace period is measured from.

Writing the marker first means an interrupted run leaves a *marked* detached table, which
the next run finds and finishes, rather than an unmarked one nobody will ever collect.

The attach is the marker's other end: a relation that comes back — retention grew, a
backfill named its window — loses the marker in the transaction that attaches it. An
attached partition is nobody's orphan, and a marker left on it would hand its old detach
instant to the *next* detach, cutting that grace period short; with the marker gone, the
next detach stamps a fresh one.

The marker survives `pg_dump` and restore (comments are dumped by default), so a restored
copy of a marked table is again eligible for dropping. When repurposing such a table,
clear its comment (`COMMENT ON TABLE … IS NULL`), or restore with `--no-comments`.

### Adopting legacy orphans

Tables a *previous* partitioner detached and never dropped carry no marker. Adopt them
once, and the next tick treats them like any other orphan:

```python
await repo.adopt_partition("public.events", "public.events__2024_01")   # True when marked
```

Adoption records no detach instant, so a grace period does not delay a table that has
already waited. It refuses a table that is still attached.

### Several deployments, one database

The marker's first line names the parent; the prefix is configurable when two
deployments share a database:

```python
repo = PostgresPartitionRepository(engine, marker_prefix="app")
metadata = PostgresMetadataProvider(engine, marker_prefix="app")
```

Pass the same prefix to both.

## Revalidation before anything destructive

A plan is made from a snapshot; the database moves on. Before a detach or a drop runs,
the executor checks that the relation is still the one the plan decided about:

- its **OID** is the one the plan saw — a table dropped and recreated under the same
  name between plan and apply has another OID and is left alone (`PlanStaleError`:
  raised, or recorded as an issue under `continue_on_error`);
- for a detach, it is **still attached** to that parent;
- for a drop, it is **not attached** to anything and **still carries the marker**.

The drop's checks run under `ACCESS EXCLUSIVE` in the same transaction as `DROP TABLE`,
and the detach's under the lock the detach itself takes, in the same transaction as its
marker and its statement — closing the window in which a concurrently replaced relation
(a `before_detach` hook swapping the table at its name, say) could be acted on in the
plan's stead. The concurrent detach form, which cannot run inside a transaction, checks
the identity immediately before the statement and once more after it. `DROP` is never
issued with `CASCADE`, so a drop that would take something else with it fails instead.

## What the library will never do

- Drop an attached partition. Retirement is always detach first, then drop.
- Drop a table without the marker (unless you turn the guard off with
  `drop_allow_unmanaged=True` on the repository — not recommended; adopt instead).
- Create a partition whose window overlaps a partition it does not own. PostgreSQL would
  refuse anyway; the plan says so before trying.
- Rewrite a hash set to a new modulus, re-partition a branch, or change your columns,
  indexes or constraints. (It does drop a detached partition's own foreign keys right
  before dropping the partition.)
- Move rows except where you ask it to: out of a DEFAULT partition into a partition being
  attached, and in the batch movers `partition_data` / `unpartition`.
- Guess. An unreadable bound, an incomplete child set, a hash layout no repair is provably
  safe for — reported, not fixed.

## Two maintainers on one table

The lock manager serialises maintainers per table, and the loser of a non-blocking
acquisition skips its tick. Even without a working lock the tree cannot be corrupted: a
partition the other worker created first is recognised by its bounds as a lost race, not
a conflict; a name taken by a relation with *other* bounds is reported; and every
destructive operation is revalidated as above.
