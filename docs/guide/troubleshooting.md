# Troubleshoot

What a message means, why it happened, and what to do. Findings appear in
`plan.findings` and `plan.describe()`; warnings among them also land in `result.issues`.
Exceptions are raised by `plan()` / `apply()` or reported on `result.error` by the
maintainer.

## The plan says `nothing to do` but I expected partitions

- The windows exist already. Check `service.inspect(config)`.
- The creation rule is `CreateNextIf` and the newest partition does not satisfy it yet.
- The cursor is not where you think: an integer axis reads `max(key)` — an empty table
  starts at `origin`; a time axis reads the clock in the calendar's `tz`.
- The run was planned in `RECONCILE` mode (`reconcile()` creates nothing ahead).

## `range_overlap` — a wanted window overlaps a partition I did not configure

```text
[warning] range_overlap: public.events needs a partition for 2028_03 but public.events_oddweeks already covers part of it with bounds the scheme did not produce; creating it would fail, and detaching the other is not this library's decision.
```

A partition whose bounds are not on the grid sits where a wanted window should go.
PostgreSQL would refuse the overlap, and detaching someone else's partition is not the
library's call. Decide by hand: detach and re-attach it with grid-aligned bounds, split
it, or leave it and accept that this window is not managed. See
[Change a scheme safely](changing-the-scheme.md) for the granularity-change case.

## `unmanaged_partition` (INFO)

The partition's bounds are not a window of the grid, nor inside one. It is inspected and
left alone: never expired, never dropped. This is the normal state of a hand-attached
archive. If it *should* be managed, its bounds have to be the scheme's — check timezone
(a month created under another zone straddles two cells) and granularity.

## `legacy_leaf` (INFO)

The scheme expects a branch (`RANGE → HASH`) but this partition is a plain table, created
before the level below existed. A plain table cannot gain partitions. It holds valid data
and stays; new partitions follow the new shape.

## `modulus_preserved` (INFO), `hash_gap_historical_modulus`, `modulus_repaired` (INFO)

The configured bucket count changed. A complete set at the old modulus is kept; an
incomplete one is repaired *at its own modulus*, because a bucket at the new modulus would
overlap. Rebucketing history is a data migration, not maintenance.

## `non_uniform_incomplete` — hash buckets at mixed moduli leave a gap

Some siblings use one modulus, some another, and together they do not tile the keyspace;
rows hashing into the gap are rejected. No repair is provably safe, so it is reported.
Look at the buckets (`inspect`), work out which residue classes are missing, and add
them by hand at the modulus that fits (each modulus must be a factor of the next larger
one, and residue classes must not overlap).

## `default_holds_rows` — a DEFAULT sibling holds rows for a hash or list member

Only a RANGE window can be selected by its key, so rows belonging to a hash bucket or a
list group are not moved automatically. Move them out of the DEFAULT partition (insert
them through the parent after the member exists, or delete and re-insert) and the next
run attaches the member.

## `name_unusable`

The name the scheme produces is either taken by a relation with *other* bounds, or over
PostgreSQL's 63-byte limit (which truncates silently and would make two partitions
collide). Rename the stray relation, or shorten the table name / `name_suffix`.

## `detach_pending` (INFO)

An earlier `DETACH … CONCURRENTLY` was interrupted. The partition is still attached in the
catalog, invisible through the parent, and rejects its own rows. The same plan completes
it (`DETACH … FINALIZE`, reason `detach_finalize`) and its drop follows the drop policy.
To finish it by hand instead:

```sql
ALTER TABLE events DETACH PARTITION events__2026_08 FINALIZE;
```

## `strategy_mismatch`, `column_mismatch`

A branch is partitioned by another method, or on another key, than the scheme asks for —
`RANGE (created_at) → LIST (region)` where the configuration says `HASH (tenant_id)`.
Repartitioning an existing branch is a rewrite, so it is left alone and reported; new
branches follow the scheme. Fix the scheme if the tree is right, or migrate the branch by
hand.

## `non_uniform_complete` (INFO)

Hash siblings use different moduli (2 and 4, say) but together still tile the keyspace.
Legal, and left as it is.

## `unconvergeable`

A partition was not created because part of its subtree could not be planned — a name
refused, a group in conflict. Attaching a branch with a hole in its child set would reject
rows, so the whole partition waits. The findings for the subtree say why.

## `grace_pending`, `drop_deferred` (INFO)

A detached orphan waiting out its grace period, or one whose `DropAfter(when=…)`
condition does not hold yet. Expected; the drop comes when the policy says.

## `unreadable_bound`, `unbounded_partition`

A bound the level's axis cannot read (a partition keyed differently than configured, or
a codec mismatch), or an open-ended one (`MINVALUE` / `MAXVALUE`). Never pruned. If the
codec is wrong, fix `boundary_codec`; an open-ended partition is by definition current.

## `foreign_partition` (INFO)

A foreign table is in the tree under a `LocalLeaves` configuration. It is someone else's;
nothing is created, detached or dropped. Configure `ForeignLeaves` if it should be
managed.

## `list_values_conflict`

A configured LIST group claims a value another partition already owns. A value belongs to
exactly one partition; detach the other partition first, or change the group.

## `coverage_unknown`

Either a child's name contains a dot and cannot be addressed by qualified-name DDL, so
the child set cannot be read completely; or hash siblings use moduli whose least common
multiple is too large to check coverage. Nothing is planned for that branch. Rename the
child, or simplify the moduli.

## `PartitionReferencedError` — a detach was refused by a foreign key

```text
detach: public.ci_builds__2026_06: PartitionReferencedError: Partition public.ci_builds__2026_06 is still referenced by rows of another table: removing partition "ci_builds__2026_06" violates foreign key constraint "ci_artifacts_build_id_created_at_fkey1"
```

Rows of another table reference rows of this partition through a foreign key on the
parent; PostgreSQL will not detach it. The run goes on; the issue repeats until the
referencing rows are gone. Put `Unreferenced()` in the retention rule to keep such
partitions out of the plan — see [Handle foreign keys](foreign-keys.md).

## `InvalidPartitionConfigError`

Raised by `plan()` before any DDL; reported on `result.error` by the maintainer.

| Message | Cause | Fix |
|---|---|---|
| `Table 'public.events_flat' is not partitioned` | the parent is a plain table | `CREATE TABLE … PARTITION BY`; see [Partition an existing table](partition-existing-table.md) |
| `Partition type mismatch … config='range' actual='list'` | the root's method differs from the scheme's | match the scheme to the table |
| `Partition column mismatch … config='occurred_at' actual='created_at'` / `Partition key mismatch` | the key differs, or composite key order differs | match the scheme's `key` to `PARTITION BY` |
| `Subpartition column(s) 'tenant_id' missing from unique constraint(s) (id, created_at)` | a nested level's column is not in every `UNIQUE` / `PRIMARY KEY` | add the column to the constraints |
| `… PostgreSQL refuses a foreign table as a partition of a table with a unique index or primary key` | `ForeignLeaves` on a parent with a unique index | local leaves, or drop the constraint |
| `Partition column(s) […] are mixed-case` / `partitions on an expression` | a key the library cannot address | use lowercase column keys |
| `ensure_partitions needs a progression root` | `ensure_partition(s)` on a HASH or grouped LIST root | those sets are fixed; use `reconcile()` |
| `partition_data drains a DEFAULT partition into RANGE windows; this root is not a RANGE level` | the mover on a non-RANGE root | only RANGE roots have a DEFAULT to drain |

## `Timezone mismatch`

```text
Timezone mismatch: the period calculator works in 'Europe/Helsinki' but repository DDL runs in 'UTC'. Pass ddl_timezone='Europe/Helsinki' to the repository, or align the calculator's tz.
```

The calendar and the DDL disagree on what midnight is. Pass the same zone to
`TimeBoundaries(tz=…)` (or the flat `tz`) and to `PostgresPartitionRepository(ddl_timezone=…)`.

## `LockAcquisitionError`

```text
Failed to acquire lock for table public.events: advisory lock unavailable
```

Another maintainer holds the table's lock. Not an error: skip the tick. If it persists,
look for a stuck run (`pg_locks` for advisory locks; the Redis key for the Redis manager)
or a pool of size one, which deadlocks the advisory manager against its own DDL.

## `PlanStaleError`

The relation the plan decided about is not the one holding the name any more — it was
dropped and recreated between plan and apply, or re-attached. The operation is skipped
(recorded as an issue with `continue_on_error`, raised otherwise). Plan again.

## `UnmanagedPartitionDropError`, `PartitionAttachedError`

`drop_partition` refused a table without the marker, or one that is still attached. Both
guards are the point of safe drops; adopt legacy tables with `adopt_partition`, and
detach before dropping.

## `PartitionTopologyError`

The execution-time twin of a warning finding: a DEFAULT sibling holding rows for a hash
or list member, a name taken by a relation with other bounds. Recorded as an issue with
the finding's reason; the run goes on. The remedy is the finding's.

## `PartitionAlreadyExistsError`, `PartitionNotFoundError`, `PartitionDetachInProgressError`

Repository-level errors the executor normally absorbs: a name already taken is a lost
race (benign) or a conflict (`name_unusable`); a relation that vanished between plan and
apply is skipped; a detach already pending on another connection is retried next tick.
Seen directly only when calling the repository yourself.

## `DropRetryExhaustedError`

The drop could not take its lock within `drop_max_retries` attempts — a long transaction
holds the table. Find it in `pg_stat_activity`, or raise `drop_lock_timeout_ms`.

## `ValueError` at construction

Refused before touching the database: a `name_suffix` without its placeholder, a
composite LIST key, two levels on one column, a table name too long for the scheme's
suffixes, `CreateAhead` on a sliding list, a negative grace, a foreign-table option
template with an unknown placeholder. The message names the field.

## Rows are being rejected: `no partition of relation "events" found for row`

PostgreSQL's error, not the library's: a row's key falls into a window with no partition.
Either the tick has not run for longer than `create_ahead_count` covers, the table has
no DEFAULT partition and receives out-of-range rows, or a hash set has a gap
(`non_uniform_incomplete`). Run a tick; check `plan.findings`.

## Everything looks right but nothing was created

- Two replicas with *different* configurations for one table undo each other. Deploy one
  configuration everywhere.
- The engine points at a replica (`pg_is_in_recovery()`); DDL needs the primary.
- The maintenance role does not own the table. Partition DDL requires ownership; check
  `result.error` for `permission denied`.
