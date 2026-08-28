# Recipes

Real-world shapes as configurations. Each was checked against the source of a production
system that hand-rolls it (see [OSS research](../design/oss-research.md)).

## Error monitoring: weekly UUIDv7 events split by organisation

The shape GlitchTip builds for `issue_events`: the partition key is a UUIDv7 `id`, periods
are calendar weeks, each week is hashed by tenant, and the bucket count has changed over
time.

```python
config = TablePartitionConfig(
    schema="public",
    table_name="issue_events",
    scheme=RangePartitioning(
        key="id",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="uuidv7"),
        child=HashPartitioning(key="organization_id", modulus=2, name_suffix="_h{remainder}"),
    ),
    lifecycle=LifecyclePolicy(creation=CreateAhead(count=3), retention=KeepNewest(count=12)),
)
```

Requires `PRIMARY KEY (id, organization_id)`. Existing weeks built with `MODULUS 4` are
preserved; an incomplete historical week is repaired at its own modulus; a legacy plain
leaf stays valid. Cold storage before the drop is a `before_drop` hook; to finalize only
closed weeks, gate on `metadata.is_partition_closed(name, settle_seconds=900)` with the
codec on the provider.

## Queue: partitions every N message ids

PGMQ's partitioned queue, without `pg_partman`:

```python
config = TablePartitionConfig(
    table_name="q_orders",
    scheme=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=10_000)),
    lifecycle=LifecyclePolicy(creation=CreateAhead(count=4), retention=KeepBehind(distance=100_000)),
)
```

The cursor is `max(msg_id)`; `KeepBehind` expires a window once the newest id is 100 000
past its upper bound. Run maintenance often enough that four windows ahead outlast the
insert rate between ticks — a row beyond the last window is rejected by PostgreSQL, not
buffered.

## Outbox / task table: root HASH for parallel workers

pg-trx-outbox and Hatchet's `v1_task_events_olap_tmp`: a fixed set of buckets that workers
address directly with `FOR UPDATE SKIP LOCKED`.

```python
config = TablePartitionConfig(
    table_name="pg_trx_outbox",
    scheme=HashPartitioning(key="key", modulus=3, name_suffix="_{remainder}"),
)
```

No lifecycle: maintenance creates the missing buckets and otherwise issues zero DDL. The
`(modulus, remainder) → name` mapping comes from `service.inspect(config)`.

## Daily stream history with hand-managed neighbours

Centrifugo's outbox tables: daily partitions, a few days ahead, dropped after N days —
next to partitions an operator attached by hand.

```python
config = TablePartitionConfig(
    table_name="cf_stream_history",
    scheme=RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.DAY)),
    lifecycle=LifecyclePolicy(creation=CreateAhead(count=3), retention=KeepFor(timedelta(days=7))),
)
```

A hand-attached partition whose bounds are not a day of the grid is `unmanaged_partition`:
reported, never detached, never dropped.

## Webhook log: partitions through next year, at startup

Hookdeck Outpost's planned workflow:

```python
config = TablePartitionConfig(
    table_name="events",
    scheme=RangePartitioning(key="time", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)),
    lifecycle=LifecyclePolicy(
        creation=CreateUntil(datetime(date.today().year + 2, 1, 1, tzinfo=UTC)),
        retention=KeepFor(timedelta(days=90)),
    ),
)

async def on_startup() -> None:
    try:
        await service.maintain(config)
    except LockAcquisitionError:
        pass   # another replica is doing it
```

Rows already sitting in `events_default` move into each new monthly partition as it is
attached (DEFAULT reconciliation).

## Sliding queue: rotate by state, not by calendar

GitLab's sliding-list pattern on a RANGE axis: open the next partition once the current
one holds more than a day of data, detach a partition only when nothing in it is pending,
keep it a week before dropping.

```python
config = TablePartitionConfig(
    table_name="deleted_records",
    scheme=RangePartitioning(key="partition_no", boundaries=NumericBoundaries(step=1)),
    lifecycle=LifecyclePolicy(
        creation=CreateNextIf(SqlPredicate(
            "SELECT min(created_at) < now() - interval '1 day' FROM {partition}"
        )),
        retention=ExpireIf(SqlPredicate(
            "SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')"
        )),
        drop=DropAfter(grace=timedelta(days=7)),
    ),
)
```

Writers insert with `partition_no = <current>`; the cursor is `max(partition_no)`. A
LIST-valued progression level with the same policies is the next planned step.

## Cold tiering: detach, archive, verify, drop

ColdFront's `detach` strategy plus an archival pipeline:

```python
class ArchiveHooks(BasePartitionLifecycleHooks):
    async def after_detach(self, table_name: str, partition_name: str) -> None:
        await export_to_iceberg(partition_name)

    async def before_drop(self, table_name: str, partition_name: str) -> None:
        if not await archive_verified(partition_name):
            raise RuntimeError("archive not verified yet")   # the drop is retried next tick


config = TablePartitionConfig(
    table_name="metrics",
    partition_column="ts",
    granularity=PartitionGranularity.MONTH,
    lifecycle=LifecyclePolicy(
        creation=CreateAhead(count=2),
        retention=KeepFor(timedelta(days=90)),
        detach=DetachMode.CONCURRENT,
        drop=DropAfter(grace=timedelta(days=7)),
    ),
)
```

Or `drop=DropNever` to hand the detached tables to another process entirely.

## Large partitions on weekends only

GitLab's 150 GB rule as a drop condition:

```python
def small_or_weekend(candidate: Candidate) -> bool:
    size = candidate.facts.size_bytes or 0
    return size < 150 * 2**30 or candidate.now.weekday() >= 5

lifecycle = LifecyclePolicy(
    retention=KeepFor(timedelta(days=30)),
    drop=DropAfter(grace=timedelta(days=7), when=Callback(small_or_weekend, facts=frozenset({FactKind.SIZE}), label="<150GB or weekend")),
)
```

Deferred drops appear as `drop_deferred` findings with the size on the plan.
