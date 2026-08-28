# Boundaries and codecs

A `RANGE` level needs one rule — its **boundaries** — that turns a position on the axis
into the window holding it, steps between adjacent windows, renders a window as the two
literals PostgreSQL compares the key against, reads such a literal back, and names the
partition for a window.

## Time: `TimeBoundaries`

```python
TimeBoundaries(granularity=PartitionGranularity.MONTH)
TimeBoundaries(granularity=PartitionGranularity.DAY, tz="Europe/Helsinki")
TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=UUIDv7BoundaryCodec())
TimeBoundaries(calculator=FiscalYearPeriodCalculator())      # any PeriodCalculator
```

The period arithmetic is a [period calculator](strategies.md): a built-in granularity
(hour, day, ISO week, month, quarter, year) or a custom one. Names are the calculator's
(`events__2026_w35`); the cursor is the clock in the calendar's timezone.

**Timezones.** The calendar is computed in `tz` (`UTC` by default; a `ZoneInfo` or an IANA
name). Keep the repository's `ddl_timezone` aligned with it — the service refuses a
mismatched pair, so names and real bounds cannot silently drift apart. `HOUR` is UTC-only
(a local hour can repeat or vanish under DST).

## Integers: `NumericBoundaries`

```python
NumericBoundaries(step=100_000)                       # [0, 100000), [100000, 200000), …
NumericBoundaries(step=1_000, origin=500)             # windows anchored on 500
NumericBoundaries(step=100_000, cursor_source=CursorSource.SEQUENCE)
```

Windows are `[origin + k·step, origin + (k+1)·step)`, named after their start
(`queue__100000`; a negative start is spelled `m100`). The cursor is `max(key)` over the
table — one index probe per leaf when the key is indexed — or the key's serial/identity
sequence with `CursorSource.SEQUENCE`, which is one catalog read and right only for a key
fed by that sequence. Retention on this axis is `KeepNewest(count)` or
`KeepBehind(distance)`.

## Codecs: partitioning by time over an encoded key

A time-partitioned table has two notions of "when": the **semantic period** — a week, a
month — which decides names, the create-ahead window and retention; and the **physical
boundary** — the literal PostgreSQL compares the key against. For a `timestamptz` key they
coincide. They come apart when the key is a *time-sortable identifier*: a UUIDv7, a ULID,
an epoch bigint. A `RangeBoundaryCodec` bridges the two:

```python
TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="uuidv7")
TimeBoundaries(granularity=PartitionGranularity.DAY, codec=EpochBoundaryCodec("milliseconds"))
```

```python
config.scheme.range_boundaries.literals(window)
# ('01a03111-1c00-7000-8000-000000000000', '01a0551d-a000-7000-8000-000000000000')
```

Built-in codecs, addressable by name in serialized configs: `uuidv7`, `epoch_seconds`,
`epoch_milliseconds`.

`UUIDv7BoundaryCodec` implements RFC 9562 — a 48-bit big-endian Unix-milliseconds
timestamp in the leading bits. Both boundaries use the **minimum** UUID for their instant
(every random bit zero), which is what makes adjacent periods exactly contiguous: one
period's upper bound is the next one's lower bound, and no identifier can fall between two
partitions. The encoding is deterministic, so the create path stays idempotent.
`min_uuid_for(instant)` is exposed for the query side — turning a time filter into a UUID
range so the planner can prune.

Codecs are **bidirectional on purpose**: retention compares a partition's *catalog* upper
bound against the cutoff, and ownership compares it against the grid, so a codec that could
only encode would create partitions the library could never recognise again.

`PostgresMetadataProvider(engine, boundary_codec=...)` needs the same codec only for
`is_partition_closed`; planning decodes through the config.

## Writing your own

A codec is two methods:

```python
class SnowflakeBoundaryCodec:
    EPOCH_MS = 1_288_834_974_657          # Twitter epoch

    def encode(self, start: datetime, end: datetime) -> tuple[str, str]:
        return str(self._id_at(start)), str(self._id_at(end))

    def decode(self, literal: str) -> datetime | None:
        try:
            ms = (int(literal) >> 22) + self.EPOCH_MS
        except ValueError:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=UTC)

    def _id_at(self, instant: datetime) -> int:
        return (int(instant.timestamp() * 1000) - self.EPOCH_MS) << 22
```

Two rules make a codec correct: `encode` is monotonic in its argument, and adjacent periods
are contiguous — no gap, no overlap. `decode` returns `None` for anything carrying no
instant (`MINVALUE`, `MAXVALUE`, a literal of another type) rather than raising, so a table
with a mixed history can still be introspected.

A whole axis is a `RangeBoundaries` implementation (`window_at`, `shift`, `literals`,
`decode`, `child_name`, `parse_child_name`, `describe`, `axis`, `cursor_source`) — the same
protocol `TimeBoundaries` and `NumericBoundaries` implement.

## Combining with nesting

Boundaries and nesting are orthogonal:

```python
RangePartitioning(
    key="id",
    boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="uuidv7"),
    child=HashPartitioning(key="organization_id", modulus=4),
)
```

`RANGE(id UUIDv7)` weekly → `HASH(organization_id)`: the time dimension picks the branch,
the hash dimension picks the leaf.
