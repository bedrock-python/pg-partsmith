# Migrate from pg_partman or a hand-rolled script

Replacing an existing partition manager is mostly a matter of not surprising yourself on
the first tick. This guide is the checklist: what carries over unchanged, what to translate,
and the two things that bite.

## 1. Look at the plan first

```python
plan = await service.plan(config)
print(plan.describe())
```

Nothing runs. The plan lists every creation, detach and drop with its reason, and every
finding: partitions the scheme did not produce, hash sets at another modulus, a DEFAULT
partition with rows in the way. Run it against a staging copy until it says what you
expect.

## 2. Existing partitions carry over as they are

Partitions are recognised by their **bounds**, never by name. An attached partition whose
bounds are a window of the configured grid — the same month, the same ISO week, the same
100 000-id step — is a lifecycle partition whatever it is called. Nothing needs renaming
or recreating.

One whose bounds are not on the grid — a yearly archive under a monthly configuration, a
week that straddles two months — is reported as `unmanaged_partition` and left alone.
Decide about those by hand.

New partitions follow the configured naming. To keep old and new names consistent, plug
the old convention in as a calculator or a `name_suffix` — see
[Custom calendars, names and codecs](calendars-and-codecs.md).

## 3. Retention is a count, not a distance

Hand-rolled pruners usually express retention as a distance: "drop everything older than
`N` months from now", which keeps `N + 1` partitions on disk. `KeepNewest(count)` — and
the flat `retention_count` — is a *count*: "keep exactly the `N` newest periods, the
current one included". Passing a distance straight in drops one extra period on the first
tick. Convert once at the boundary (`old_distance + 1`), or express the age directly with
`KeepFor(age=timedelta(...))`.

## 4. Adopt the tables the old manager detached

pg-partsmith only drops tables that carry its `COMMENT` marker. Partitions that are
*attached* at migration time need nothing: the first tick that expires them writes the
marker itself.

Tables the old manager **detached and never dropped** carry no marker, so they are
invisible to the lifecycle and would sit there forever. Adopt them once:

```python
await repo.adopt_partition("public.events", "public.events__2024_01")   # True when marked
```

Idempotent; refuses attached partitions; returns `False` for names that do not resolve;
records no detach instant, so a grace period does not delay a table that has already
waited. The next tick drops adopted tables like any other orphan, `before_drop` hooks
included.

!!! danger "Do not reach for `drop_allow_unmanaged=True`"
    It disables the safe-drop guard for *every* drop — and does not even solve this
    problem, because unmarked tables are never discovered in the first place.

## 5. Translate the configuration

### From pg_partman

| pg_partman | pg-partsmith |
|---|---|
| `p_interval` (`'1 month'`, `'1 week'`, …) | `granularity=PartitionGranularity.MONTH` / `WEEK` / … |
| `p_interval` (an integer, id sets) | `NumericBoundaries(step=…)` |
| `premake` | `CreateAhead(count)` — the count includes the current period |
| `retention` (an interval) | `KeepFor(age)` |
| `retention` (an integer, id sets) | `KeepBehind(distance)` |
| `retention_keep_table = true` | `DropNever()` |
| `p_time_encoder` / `p_time_decoder` | `TimeBoundaries(codec=…)` |
| `epoch = 'seconds'` / `'milliseconds'` | `boundary_codec="epoch_seconds"` / `"epoch_milliseconds"` |
| `create_sub_parent` | `child=` on the scheme |
| template table (tablespace, reloptions), `inherit_privileges` | `leaves=LocalLeaves(tablespace=…, storage_parameters=…, inherit_privileges=True)` |
| `partition_data_proc` / `partition_data_time` / `partition_data_id` | `service.partition_data(config, batch_rows=…, max_batches=…)` |
| `undo_partition_proc` | `service.unpartition(config, into, drop_emptied=True)` |
| `part_config` rows | the `TablePartitionConfig` objects in your code |
| `run_maintenance()` / the background worker | `maintainer.run_maintenance_safe(config)` from your scheduler |
| `retention_schema`, index or publication drop on detach, `p_analyze` | a hook (`after_detach`, `before_drop`) |

Then remove the `part_config` row — pg_partman would otherwise keep managing the table —
and let pg-partsmith's first plan show you the tree.

### From a script

Most scripts fold three concerns into one loop: compute the next few period names, create
what is missing, drop what is older than N. The equivalents are the flat configuration and
one tick. Two things scripts usually get wrong that the library does differently:

- creating with `CREATE TABLE … PARTITION OF`, which takes `ACCESS EXCLUSIVE` on the
  parent — pg-partsmith creates standalone and attaches;
- dropping attached partitions directly — pg-partsmith detaches first, and never drops a
  table it did not mark.

## 6. Backfill what the data already occupies

`create_ahead` walks forward from now. Rows already in the table live in periods it will
never reach:

```python
calculator = config.scheme.time_boundaries.period_calculator
current = calculator.current_period()
past = [calculator.period_before(current, n) for n in reversed(range(1, 25))]
await service.ensure_partitions(config, past)
```

See [Backfill partitions for existing data](backfill.md). A table whose history sits in a
DEFAULT partition is the job of [`partition_data`](partition-existing-table.md).

## 7. Names are schema-qualified

`list_partitions` and every plan operation name relations as `schema.relname`. Code that
works with bare names should use the accessors:

```python
p.name         # "public.events__2024_01" — for DDL and library calls
p.relname      # "events__2024_01"        — for parsing / external layouts
p.schema_name  # "public"
```

## 8. Export pipelines

`metadata.is_partition_closed(name, settle_seconds=900)` answers "can this partition
still receive in-range rows?" with one server-side check — `now()` is evaluated in the
database, so replica lag and application clock skew do not skew the answer. Hand it the
table's own boundaries, `boundaries=config.time_boundaries`, so the bound is read with the
timezone and codec that wrote it.
