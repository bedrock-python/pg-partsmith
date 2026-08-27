# Migrating an existing partitioner

This guide covers the traps we have seen real projects hit when replacing a hand-rolled
partition maintenance script with pg-partsmith. Read it before the first production tick.

## Retention: count, not distance

Hand-rolled pruners usually express retention as a *distance*: "drop everything older than
`N` periods from now", which keeps `N + 1` partitions on disk (the current one plus `N`
past). pg-partsmith's `retention_count` is a *count*: "keep exactly `N` newest periods,
current one included".

Passing a distance straight into `retention_count` silently drops one extra period of data
on the first tick after deploy. Convert once, at the boundary:

```python
config = TablePartitionConfig(
    ...,
    # old semantics: "keep current + N past periods"
    retention_count=old_retention_distance + 1,
)
```

Verify on a staging copy: run one maintenance tick and compare the set of surviving
partitions with what the old pruner would have kept.

## Adopting legacy detached partitions

pg-partsmith only ever drops tables that carry its `COMMENT` marker (see
[Advanced](advanced.md)). The marker is stamped automatically when *the library* detaches a
partition — so partitions that are attached at migration time need nothing: the first
maintenance tick detaches and marks them itself.

The one case that needs action is tables your *old* partitioner already detached and never
dropped (a failed export, a crashed run). They carry no marker, so they are invisible to
orphan discovery and will sit there forever. **Do not reach for `drop_allow_unmanaged=True`
to handle them** — that disables the safe-drop guard for every drop, permanently, and does
not even help: unmarked detached tables are never discovered in the first place. Adopt them
once instead:

```python
repo.adopt_partition("events", "events__2024_01")   # stamps the marker; True when done
```

Adoption is idempotent, refuses attached partitions (`PartitionAttachedError`), and returns
`False` for names that do not resolve. The next maintenance tick collects adopted tables
like any other orphan: `before_drop` hooks run, then they are dropped.

## Partition names are schema-qualified

`list_partitions` returns names as `schema.relname` (e.g. `public.events__2024_01`) — a
partition may live in a different schema than its parent, and a bare name could resolve to
an unrelated table through `search_path`. Code that works with bare names (period parsing,
export layouts) should use the accessors instead of splitting strings:

```python
for p in metadata.list_partitions("events"):
    p.name         # "public.events__2024_01" — use for DDL and library calls
    p.relname      # "events__2024_01"        — use for parsing / external layouts
    p.schema_name  # "public" (None if the name is unqualified)
```

## Mapping partitions back to periods

There is no need for custom catalogue queries to answer "which period does this partition
hold" — combine `list_partitions` with the calculator you already have:

```python
calc = get_period_calculator(config.granularity)
by_period = {
    period: p
    for p in metadata.list_partitions("events")
    if p.is_attached and (period := calc.parse_partition_name(p.relname)) is not None
}
```

## Who takes the lock

`maintain_lifecycle` (and the maintainer on top of it) takes the distributed lock itself.
The granular service methods — `create_future_partitions`, `ensure_partition`,
`get_partitions_for_pruning`, `detach_old_partitions`, `drop_detached_partitions` — do
**not**: when you orchestrate them yourself, hold the lock around the whole sequence:

```python
with locks.acquire_lock("events"):
    service.create_future_partitions(config)
    ...
```

The advisory lock is non-blocking (`pg_try_advisory_lock`): a tick that collides with
another replica raises `LockAcquisitionError` immediately. Catch it and skip the tick —
the schedule brings it back.

## Creating one specific partition

`create_future_partitions` covers the scheduled create-ahead path. When a *writer* must
guarantee a partition exists before an insert (an hourly buffer, a backfill for a past
period), use `ensure_partition` — it targets exactly one period and runs the same DEFAULT
reconciliation and attach-race handling:

```python
service.ensure_partition(config, Period(year=2026, month=8, day=27, hour=14))
```

Do not hand-roll `create_partition` + `attach_partition` for this: the raw repository
calls skip reconciliation, so rows sitting in a DEFAULT partition or a concurrent worker
will fail the attach.

## One failed step should not stop the tick

By default `maintain_lifecycle` aborts on the first error. For a scheduled tick you
usually want the opposite — a failed create must not prevent pruning (which may free the
very space create needs):

```python
result = maintainer.run_maintenance_safe(config, continue_on_error=True)
for issue in result.issues:
    log.warning("step failed", step=issue.step, error=issue.error)
```

Step failures land in `result.issues` (`MaintenanceIssue`) instead of aborting;
validation and lock failures are still fatal.

## Export pipelines: when is a partition finished?

Incremental exporters need to know when a partition can no longer receive rows so they can
finalize it. `is_partition_closed` answers this with a single server-side check — `now()`
and the partition bound come from the same query, so replica lag and app-clock skew do not
skew the answer:

```python
if metadata.is_partition_closed("public.events__2026_07", settle_seconds=900):
    finalize_export(...)
```

Exports that must happen *before* a partition is dropped belong in a `before_drop` hook:
raising there aborts that partition's drop, and the orphan marker retries it next tick.
