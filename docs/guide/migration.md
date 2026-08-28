# Migrating an existing partitioner

This guide covers the traps we have seen real projects hit when replacing a hand-rolled
partition maintenance script — or `pg_partman` — with pg-partsmith. Read it before the
first production tick. For the 0.x → 1.0 API changes see the [changelog](../changelog.md).

## Start with a plan, not a tick

```python
plan = await service.plan(config)
print(plan.describe())
```

Nothing runs. The plan lists every creation, detach and drop with its reason and — with a
size-aware policy — its size, and every finding: partitions the scheme did not produce,
hash sets at an older modulus, a DEFAULT partition with rows in the way. If the plan against
a staging copy is what you expect, the tick will do exactly that.

## Retention: count, not distance

Hand-rolled pruners usually express retention as a *distance*: "drop everything older than
`N` periods from now", which keeps `N + 1` partitions on disk. `KeepNewest(count)` (and the
flat `retention_count`) is a *count*: "keep exactly `N` newest periods, current one
included". Passing a distance straight in silently drops one extra period on the first
tick. Convert once at the boundary (`old_distance + 1`), or use `KeepFor(timedelta(...))`
which expresses an age directly.

## Existing partitions are recognised by their bounds

Nothing needs renaming. An attached partition whose bounds are a window of the configured
grid — the same month, the same ISO week, the same 100 000-id step — is a lifecycle
partition whatever its name. One whose bounds are not (a yearly archive under a monthly
config, a week straddling two months under a monthly config) is reported as
`unmanaged_partition` and left alone; decide about those by hand.

New partitions follow the configured naming. To keep old and new names consistent, plug
the old convention in as a calculator (`TimeBoundaries(calculator=...)`) or a `name_suffix`
— see the [event store example](nested-migration.md#naming-and-adoption).

## Adopting legacy detached partitions

pg-partsmith only ever drops tables that carry its `COMMENT` marker. Partitions that are
attached at migration time need nothing: the first tick detaches and marks them itself.

Tables your *old* partitioner already detached and never dropped carry no marker, so they
are invisible to orphan discovery and will sit there forever. **Do not reach for
`drop_allow_unmanaged=True`** — that disables the safe-drop guard for every drop, and does
not even help, because unmarked tables are never discovered. Adopt them once instead:

```python
await repo.adopt_partition("public.events", "public.events__2024_01")   # True when marked
```

Adoption is idempotent, refuses attached partitions (`PartitionAttachedError`), returns
`False` for names that do not resolve, and records no detach instant — so a grace period
does not delay a table that has already waited. The next tick drops adopted tables like any
other orphan, `before_drop` hooks included.

## A DEFAULT partition full of data

Tables that started with only a DEFAULT partition (Hookdeck Outpost's shape) need no
special step: as each new window is attached, rows belonging to it are moved out of DEFAULT
and the attach retried. Rows with a NULL trailing key column stay in DEFAULT, where
PostgreSQL routes them.

## Backfilling partitions for data you already have

`create_future_partitions` walks *forward* from the current period, which is right for a
scheduled tick and wrong for a migration: rows already in the table live in periods
create-ahead will never reach. `ensure_partitions` takes the windows from you:

```python
calculator = config.scheme.time_boundaries.period_calculator
current = calculator.current_period()
past = [calculator.period_before(current, n) for n in reversed(range(1, 25))]
created = await service.ensure_partitions(config, past)          # periods, or Window objects
```

Idempotent, one catalogue read for the batch, complete subtrees before attach. Only create
what retention keeps: partitions outside the retention window are detached and dropped by
the next tick.

## Coming from `pg_partman`

| pg_partman | pg-partsmith |
|---|---|
| `premake` | `CreateAhead(count)` |
| `retention` (interval) | `KeepFor(age)` |
| `retention` (integer, id sets) | `KeepBehind(distance)` |
| `retention_keep_table = true` | `DropNever` |
| `p_time_encoder` / `p_time_decoder` | `TimeBoundaries(codec=...)` |
| `epoch` | `codec="epoch_seconds"` / `"epoch_milliseconds"` |
| `create_sub_parent` | `child=` on the scheme |
| `part_config` rows | the `TablePartitionConfig` objects in your code |
| `run_maintenance()` / BGW | `maintainer.run_maintenance_safe(config)` from your scheduler |

Drop the `part_config` row (`pg_partman` would otherwise keep managing the table) and let
pg-partsmith's first plan show you the tree.

## Partition names are schema-qualified

`list_partitions` and every plan operation use `schema.relname`. Code that works with bare
names should use the accessors:

```python
p.name         # "public.events__2024_01" — for DDL and library calls
p.relname      # "events__2024_01"        — for parsing / external layouts
p.schema_name  # "public"
```

## Who takes the lock

`maintain()` (and the maintainer) takes the distributed lock around plan and apply. The
granular methods — `reconcile`, `ensure_partition(s)`, `create_future_partitions`,
`detach_old_partitions`, `drop_detached_partitions` — do **not**: hold the lock yourself
when orchestrating them.

## One failed step should not stop the tick

```python
result = await maintainer.run_maintenance_safe(config, continue_on_error=True)
for issue in result.issues:
    log.warning("step failed", step=issue.step, error=issue.error, partition=issue.partition_name)
```

Operation failures land in `result.issues` instead of aborting; topology conflicts always
do; validation and lock failures stay fatal.

## Export pipelines: when is a partition finished?

`metadata.is_partition_closed(name, settle_seconds=900)` answers "can this partition still
receive in-range rows?" with one server-side check, so replica lag and app-clock skew do
not skew the answer. With an encoded key pass the codec to the metadata provider.
