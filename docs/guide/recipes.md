# Recipes from real systems

Configurations for the shapes production systems build for themselves. Each was checked
against the source of a project that hand-rolls it (see the
[OSS research](../design/oss-research.md)); each is a complete `TablePartitionConfig` you
can start from. The names used below are all importable from `pg_partsmith`.

## Error monitoring: weekly UUIDv7 events split by organisation

GlitchTip's `issue_events`: the partition key is a UUIDv7 `id`, periods are calendar
weeks, each week is hashed by tenant, and the bucket count has changed over time.

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
leaf stays valid. Walkthrough: [a multi-tenant event store](../getting-started/event-store.md).

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
insert rate between ticks — a row beyond the last window is rejected, not buffered.

## Outbox / task table: root HASH for parallel workers

pg-trx-outbox and Hatchet's task tables: a fixed set of buckets that workers address
directly with `FOR UPDATE SKIP LOCKED`.

```python
config = TablePartitionConfig(
    table_name="pg_trx_outbox",
    scheme=HashPartitioning(key="key", modulus=3, name_suffix="_{remainder}"),
)
```

No lifecycle: maintenance creates the missing buckets and otherwise issues zero DDL. The
`(modulus, remainder) → name` mapping comes from `service.inspect(config)`.

## Daily stream history next to hand-managed neighbours

Centrifugo's outbox tables: daily partitions, a few days ahead, dropped after a week —
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

## Webhook log: partitions through next year, at start-up

Hookdeck Outpost's workflow:

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
    await maintainer.run_maintenance_safe(config)     # replicas that lose the lock skip
```

Rows already sitting in `events_default` move into each new monthly partition as it is
attached.

## Sliding list: rotate by state, not by calendar

GitLab's `ci_builds`: `LIST (partition_id)`, one integer value per partition, the
application writes the newest value, the next value opens once the newest partition holds
more than a day of data, and a partition is retired only when no `ci_pipelines` row
references it any more.

```python
config = TablePartitionConfig(
    table_name="ci_builds",
    scheme=ListPartitioning(key="partition_id", sequence=IntegerSequence(start=100)),
    lifecycle=LifecyclePolicy(
        creation=CreateNextIf(SqlPredicate(
            "SELECT min(created_at) < now() - interval '1 day' FROM {partition}"
        )),
        retention=ExpireIf(AllOf((KeepNewest(count=3), Unreferenced()))),
        drop=DropAfter(grace=timedelta(days=7)),
    ),
)
```

The cursor is the newest partition, so nothing is created "ahead"; the application reads
the current value from `service.inspect(config)` (the highest single-value member) and
writes it. `Unreferenced()` is the condition PostgreSQL itself imposes on the detach — a
partition whose rows are still referenced through the foreign key from `ci_pipelines`
cannot be detached — so the policy never plans a detach the database would refuse.

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

Or `drop=DropNever()` to hand the detached tables to another process entirely.

## Cold tiering to a column store: foreign leaves

pg_clickhouse's shape: an index-free metrics table whose partitions are foreign tables on
a ClickHouse (or any FDW) server, queried through one PostgreSQL parent.

```python
config = TablePartitionConfig(
    table_name="metrics",
    scheme=RangePartitioning(key="ts", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)),
    lifecycle=LifecyclePolicy(creation=CreateAhead(count=2), retention=KeepNewest(count=24)),
    leaves=ForeignLeaves(server="clickhouse", options={"table_name": "{relname}"}),
)
```

The parent must have no unique index (PostgreSQL's rule; checked before any DDL). See
[Tier cold data to a foreign server](cold-tiering.md).

## Hot leaves on fast storage, with the parent's grants

```python
config = TablePartitionConfig(
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.DAY,
    create_ahead_count=3,
    retention_count=30,
    leaves=LocalLeaves(tablespace="nvme", storage_parameters={"fillfactor": 90}, inherit_privileges=True),
)
```

Every new day lands on the `nvme` tablespace with the parent's owner and grants — what
`pg_partman`'s template table and `inherit_privileges` did.

## Large partitions on weekends only

GitLab's 150 GB rule as a drop condition:

```python
def small_or_weekend(candidate: Candidate) -> bool:
    size = candidate.facts.size_bytes or 0
    return size < 150 * 2**30 or candidate.now.weekday() >= 5


lifecycle = LifecyclePolicy(
    retention=KeepFor(timedelta(days=30)),
    drop=DropAfter(
        grace=timedelta(days=7),
        when=Callback(small_or_weekend, facts=frozenset({FactKind.SIZE}), label="<150GB or weekend"),
    ),
)
```

Deferred drops appear as `drop_deferred` findings with the size on the plan.

## From a monolithic table

```python
# once, in SQL:
#   ALTER TABLE events RENAME TO events_legacy;
#   CREATE TABLE events (LIKE events_legacy INCLUDING ALL) PARTITION BY RANGE (created_at);
#   ALTER TABLE events ATTACH PARTITION events_legacy DEFAULT;

while not (result := await service.partition_data(config, batch_rows=50_000, max_batches=100)).complete:
    await asyncio.sleep(1)          # let the writers breathe between rounds
```

See [Partition an existing table](partition-existing-table.md) for what is and is not
visible while it runs.
