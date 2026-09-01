# Archive before dropping

A partition that expires is detached first and dropped later. Everything between those
two moments is yours: export it, verify the export, keep it for a while, or never drop it
at all. This guide combines the three tools for that — hooks, the grace period, and
`DropNever`.

## The hooks

Six hooks fire around create, detach and drop, once per **lifecycle unit** — the partition
directly under the root, never once per leaf of its subtree — and once per member of a
root `HASH` or `LIST`:

| Hook | When |
|---|---|
| `before_create(config, partition)` | before the partition (name, bounds, `subpartition_type`) is created |
| `after_create(config, partition)` | after it is created, its subtree built, and attached |
| `before_detach(table_name, partition)` | before detaching — the data is still reachable through the parent |
| `after_detach(table_name, partition_name)` | after a successful detach — the table exists, standalone |
| `before_drop(table_name, partition_name)` | before the table is dropped — the last chance to read it |
| `after_drop(table_name, partition_name)` | after the table is gone |

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

    async def after_detach(self, table_name: str, partition_name: str) -> None:
        await self._archive.export(partition_name)          # the whole week, buckets included

    async def before_drop(self, table_name: str, partition_name: str) -> None:
        if not await self._archive.verified(partition_name):
            raise RuntimeError(f"{partition_name} is not verified yet")   # dropped on a later tick


service = PartitionLifecycleService(repo, metadata, locks, hooks=[ArchiveHooks(archive)])
```

Hooks are called in the order given; pass several for notifications, metrics and audit
logs.

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
skew it. With an encoded key (UUIDv7, epoch) pass the codec to
`PostgresMetadataProvider(boundary_codec=…)`.

## Cold tiering instead of dropping

When the archive is a foreign server that PostgreSQL can query, consider not exporting at
all: [foreign leaves](cold-tiering.md) keep old windows queryable through the parent.
