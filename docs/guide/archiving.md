# Archive before dropping

A partition that expires is detached first and dropped later. Everything between those
two moments is yours: export it, verify the export, keep it for a while, or never drop it
at all. This guide combines the three tools for that — hooks, the grace period, and
`DropNever`.

## The hooks

Eight phases fire around create, attach, detach and drop, once per **lifecycle unit** —
the partition directly under the root, never once per leaf of its subtree — and once per
member of a root `HASH` or `LIST`:

| Hook | When |
|---|---|
| `before_create(event)` | before the partition is created |
| `after_create(event)` | after it is created, its subtree built, and attached |
| `before_attach(event)` | before a detached partition goes back into the tree — anything derived from it while it was out is about to go stale |
| `after_attach(event)` | after it is attached and taking rows again |
| `before_detach(event)` | before detaching — the data is still reachable through the parent |
| `after_detach(event)` | after a successful detach — the table exists, standalone |
| `before_drop(event)` | before the table is dropped — the last chance to read it |
| `after_drop(event)` | after the table is gone |
| `on_event(event)` | all of the above, in addition to the method named for the phase |

Every one of them takes the same `PartitionEvent`:

| Field | What it is |
|---|---|
| `phase` | which of the six moments this is |
| `config` | the table's configuration — the calendar, the codec, the policy in force |
| `partition` | the partition itself: `name`, `bounds`, `oid`, `subpartition_type` |
| `window` | the period it covers; `None` for a member of a root `HASH` or `LIST` |
| `operation` | the planned operation: `reason`, `detail`, `oid`, `size_bytes`, `row_estimate`, `detached_at` |
| `table_name` | the root table, derived from the config |

A `before_*` hook that raises **aborts that operation**: the partition stays as it was and
comes back on the next tick. With `continue_on_error` the error lands in
`result.issues`; otherwise it propagates. An `after_*` hook that raises is logged and
re-raised the same way — the operation already happened, but the run aborts (or records
the issue under `continue_on_error`).

```python
from pg_partsmith.aio import BasePartitionLifecycleHooks, PartitionLifecycleService


class ArchiveHooks(BasePartitionLifecycleHooks):
    def __init__(self, archive: Archive) -> None:
        self._archive = archive

    async def after_detach(self, event: PartitionEvent) -> None:
        await self._archive.export(event.partition.name, covering=event.window)   # the whole week

    async def before_drop(self, event: PartitionEvent) -> None:
        if not await self._archive.verified(event.partition.name):
            raise RuntimeError(f"{event.partition.name} is not verified yet")   # dropped on a later tick


service = PartitionLifecycleService(repo, metadata, locks, hooks=[ArchiveHooks(archive)])
```

Hooks are called in the order given; pass several for notifications, metrics and audit
logs.

### One method for every phase

An audit trail or a metrics counter wants all six moments and treats them alike. That is
`on_event`, which fires for every phase in addition to the method named after it — so a
hook implementing both is called twice, on purpose:

```python
class AuditHooks(BasePartitionLifecycleHooks):
    async def on_event(self, event: PartitionEvent) -> None:
        await audit.record(
            phase=event.phase,
            table=event.table_name,
            partition=event.partition.name,
            why=event.operation.reason,
            bytes=event.operation.size_bytes,
        )
```

A phase added in a later version arrives here as a new `phase` value rather than as a
method you have to write.

### What a drop hook knows

`DETACH` clears `relpartbound`, so by drop time — usually a later run, after the grace —
the database no longer records where the partition sat; its name is the only evidence
left. The event carries the reading the planner itself decided the drop on, so a hook does
not have to repeat it:

```python
async def before_drop(self, event: PartitionEvent) -> None:
    await self._archive.finalize(event.partition.name, covering=event.window)
```

`event.window` and `event.partition.bounds` are `None` when the name does not decode — an
adopted table under a name of someone else's making. The same reading is on the plan
(`drop.bounds`) for callers that export between `plan()` and `apply()` rather than from a
hook.

## The grace period

```python
lifecycle=LifecyclePolicy(
    creation=CreateAhead(count=3),
    retention=KeepNewest(count=12),
    drop=DropAfter(grace=timedelta(days=7)),
)
```

An expired partition is detached now and dropped a week later. In between it is an
**orphan**: still in the database, readable by name, carrying the marker with its detach
instant. The plan reports it every tick as `grace_pending` (INFO) and, once the week is
over, plans the drop (`grace_elapsed`). If retention grows in the meantime and the window
is wanted again, the orphan is re-attached instead — the data comes back.

Give the export the whole grace window rather than doing it inline in `after_detach` when
it is slow: kick it off in the hook, verify it in `before_drop`.

## A condition on the drop

`DropAfter(when=…)` adds a predicate the drop has to satisfy after the grace has passed
— GitLab's "no partition above 150 GB is dropped on a weekday":

```python
from pg_partsmith import Callback, Candidate, FactKind


def small_or_weekend(candidate: Candidate) -> bool:
    size = candidate.facts.size_bytes or 0
    return size < 150 * 2**30 or candidate.now.weekday() >= 5


drop=DropAfter(
    grace=timedelta(days=7),
    when=Callback(fn=small_or_weekend, facts=frozenset({FactKind.SIZE}), label="<150GB or weekend"),
)
```

A deferred drop shows up as `drop_deferred` (INFO) with the size on the plan.

## Never dropping

```python
drop=DropNever()
```

Expired partitions are detached, marked, and left alone. A cold-storage pipeline or a DBA
owns them from there: `list_partitions` reports them with `is_attached=False`, and
`drop_detached_partitions(table, names)` drops them on request through the same guarded
path. Under `DropNever` orphans are never re-attached either — they belong to whatever
process the policy hands them to.

## Exporting only closed partitions

An export that must not race in-flight writes can ask whether a partition can still
receive in-range rows:

```python
if await metadata.is_partition_closed(partition_name, settle_seconds=900):
    await export(partition_name)
```

The check runs `now()` on the server, so replica lag and application clock skew do not
skew it. Reading a bound back needs the same timezone and codec that wrote it, so hand it
the table's own boundaries and the question cannot be asked with the wrong ones:

```python
await metadata.is_partition_closed(name, settle_seconds=900, boundaries=config.time_boundaries)
```

Without that argument the provider's own `ddl_timezone` and `boundary_codec` decide, which
is only safe if they were constructed to match the table.

## Cold tiering instead of dropping

When the archive is a foreign server that PostgreSQL can query, consider not exporting at
all: [foreign leaves](cold-tiering.md) keep old windows queryable through the parent.
