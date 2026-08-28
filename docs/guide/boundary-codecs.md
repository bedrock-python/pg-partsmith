# Boundary codecs

A time-based partition has two independent notions of "when":

- the **semantic period** — a week, a month — which decides the partition's name, its
  place in the create-ahead window, and when retention drops it; and
- the **physical boundary** — the literal PostgreSQL compares the partition key against.

For a `timestamptz` key the two coincide, and there is nothing to configure. They come
apart whenever the partition key is a **time-sortable identifier** rather than a
timestamp: a UUIDv7, a ULID, a Snowflake id, an epoch bigint, an encoded day bucket. Such
a table is still partitioned by time — the ordering of the key *is* the ordering of time —
but its `FOR VALUES FROM … TO …` literals are identifiers, not dates.

A `RangeBoundaryCodec` bridges the two, so the lifecycle keeps reasoning in periods while
the DDL speaks the key's own language.

```python
from pg_partsmith import WeekPeriodCalculator
from pg_partsmith.boundaries import UUIDv7BoundaryCodec

calculator = WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())
```

Everything else is unchanged. Periods, partition names, create-ahead and retention all
keep working in calendar terms:

```python
calculator.format_partition_name("events", Period(year=2026, week=35))
# 'events__2026_w35'

calculator.get_boundaries(Period(year=2026, week=35))
# ('01a03111-1c00-7000-8000-000000000000',
#  '01a0551d-a000-7000-8000-000000000000')
```

## Wiring

Pass the same codec to the metadata provider, so it can read boundaries back:

```python
codec = UUIDv7BoundaryCodec()

service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine, boundary_codec=codec),
    locks=PostgresAdvisoryLockManager(engine),
    period_calculator=WeekPeriodCalculator(boundary_codec=codec),
)
```

Codecs are **bidirectional on purpose**. Retention selects partitions by comparing a
partition's *catalog* upper bound against the cutoff instant, and `is_partition_closed`
does the same. A codec that could only encode would create partitions the library could
never prune or finalize.

Without a codec, the metadata provider will not try to read a non-timestamp bound: it
reports `is_partition_closed` as `False` rather than raising, and retention skips the
partition with a warning rather than guessing.

## UUIDv7

`UUIDv7BoundaryCodec` implements RFC 9562: a 48-bit big-endian Unix-milliseconds
timestamp in the leading bits, so UUIDv7 values sort chronologically.

Both boundaries use the **minimum** UUID for their instant — every random bit zero. Using
the minimum on both ends is what makes adjacent periods exactly contiguous:

```text
week 35:  [min_uuid(2026-08-24), min_uuid(2026-08-31))
week 36:  [min_uuid(2026-08-31), min_uuid(2026-09-07))
                ^^^^^^^^^^^^^^^^ the same value
```

No identifier can fall between two partitions, so nothing silently lands in `DEFAULT`.
Encoding is deterministic — the same instant always yields the same boundary — which is
what makes the create path idempotent.

## Writing your own

The protocol is two methods:

```python
from datetime import UTC, datetime


class EpochMillisBoundaryCodec:
    """Partition key is a bigint of milliseconds since the Unix epoch."""

    def encode(self, start: datetime, end: datetime) -> tuple[str, str]:
        return str(int(start.timestamp() * 1000)), str(int(end.timestamp() * 1000))

    def decode(self, literal: str) -> datetime | None:
        try:
            return datetime.fromtimestamp(int(literal) / 1000, tz=UTC)
        except (TypeError, ValueError):
            return None
```

Two rules make a codec correct:

1. **`encode` is monotonic** in its argument — later instants must produce later literals
   in the key type's own ordering, or PostgreSQL's range bounds are meaningless.
2. **Adjacent periods are contiguous** — one period's upper bound is the next period's
   lower bound, with no gap and no overlap.

`decode` should return `None` for anything carrying no instant (`MINVALUE`, `MAXVALUE`, a
literal from a differently-typed key) rather than raising, so a table whose history mixes
key types can still be introspected.

## Combining with subpartitioning

Boundary encoding and [subpartitioning](subpartitioning.md) are orthogonal, and compose:

```python
config = TablePartitionConfig(
    table_name="events",
    partition_type=PartitionType.RANGE,
    partition_strategy=PartitionStrategy.TIME_BASED,
    partition_column="id",              # a UUIDv7 column
    granularity=PartitionGranularity.WEEK,
    subpartition=HashSubpartitionSpec(column="tenant_id", modulus=4),
)
```

That is `RANGE(id UUIDv7)` weekly → `HASH(tenant_id)`: the time dimension picks the
branch, the hash dimension picks the leaf.
