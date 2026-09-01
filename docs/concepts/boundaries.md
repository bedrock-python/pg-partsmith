# Boundaries, cursors and calendars

A progression level needs one rule — its **boundaries** — that answers four questions:
which window holds a given position on the axis; what the next and previous windows are;
how a window is written as the literals PostgreSQL compares the key against; and what the
partition for a window is called. Three implementations ship.

| Boundaries | Axis | Window | Cursor |
|---|---|---|---|
| `TimeBoundaries` | instants | a calendar period | the clock |
| `NumericBoundaries` | integers | a fixed-width step | `max(key)` or the key's sequence |
| `IntegerSequence` | integers | one value | the newest partition |

## Windows and the grid

A **window** is a half-open interval `[start, end)` on the axis. The set of all windows a
rule can produce is its **grid**. The grid is what ownership is decided against: an
attached partition whose bounds are a window of the grid — or lie inside one — is a
lifecycle partition; one whose bounds are not is left alone.

```text
monthly grid:   … │ 2026-07 │ 2026-08 │ 2026-09 │ 2026-10 │ …
partition A:        [2026-08-01, 2026-09-01)            on the grid → managed
partition B:        [2026-08-10, 2026-08-11)            inside a cell → managed (a day left by an earlier daily config)
partition C:        [2026-08-15, 2026-09-15)            straddles two cells → unmanaged
partition D:        [2020-01-01, 2021-01-01)            coarser than the grid → unmanaged
```

## Time: `TimeBoundaries`

```python
TimeBoundaries(granularity=PartitionGranularity.MONTH)
TimeBoundaries(granularity=PartitionGranularity.DAY, tz="Europe/Helsinki")
TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="uuidv7")
TimeBoundaries(calculator=FiscalYearPeriodCalculator())        # any PeriodCalculator
```

The calendar arithmetic is a **period calculator**: one of the six built-in granularities
or a custom one. The calculator names partitions and renders their bounds:

| Granularity | Partition name | Bounds |
|---|---|---|
| `HOUR` | `events__2026_08_28_09` | `'2026-08-28 09:00:00+00' .. '2026-08-28 10:00:00+00'` (UTC only) |
| `DAY` | `events__2026_08_28` | midnight to midnight in `tz` |
| `WEEK` | `events__2026_w35` | ISO week, Monday to Monday |
| `MONTH` | `events__2026_08` | first of the month to first of the next |
| `QUARTER` | `events__2026_q3` | |
| `YEAR` | `events__2026` | |

The cursor is the clock, read in the calendar's timezone.

### Timezones

Three things must agree on what "midnight" means: the calendar that computes periods,
the DDL that writes bounds, and the planner that reads bounds back.

1. `TimeBoundaries(tz=…)` computes the calendar in that zone: `events__2026_08` under
   `Europe/Moscow` is August in Moscow, and its bounds are Moscow midnights.
2. For a `timestamptz` key, PostgreSQL interprets a naive literal in the session
   `TimeZone`, so the repository runs `SET LOCAL TIME ZONE '<ddl_timezone>'` in the same
   transaction as `ATTACH PARTITION` and DEFAULT reconciliation. `ddl_timezone` is `"UTC"`
   by default.
3. Bounds read from the catalog are interpreted in the calendar's zone before being
   compared as UTC instants.

The service **refuses a mismatched pair** at plan time rather than let names and real
bounds drift apart:

```text
Timezone mismatch: the period calculator works in 'Europe/Helsinki' but repository DDL
runs in 'UTC'. Pass ddl_timezone='Europe/Helsinki' to the repository, or align the
calculator's tz.
```

```python
config = TablePartitionConfig(..., granularity=PartitionGranularity.MONTH, tz="Europe/Helsinki")
repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Helsinki")
```

`HOUR` is UTC-only: a local hour can repeat or vanish under daylight-saving changes, and a
name that means two instants is not a name. Existing partitions are never reinterpreted
when the timezone changes.

## Integers: `NumericBoundaries`

```python
NumericBoundaries(step=100_000)                                  # [0, 100000), [100000, 200000), …
NumericBoundaries(step=1_000, origin=500)                        # anchored on 500
NumericBoundaries(step=100_000, cursor_source=CursorSource.SEQUENCE)
```

Windows are `[origin + k·step, origin + (k+1)·step)`, named after their start
(`queue__100000`; a negative start is spelled `m100`). The cursor is `max(key)` over the
table — one index probe per leaf when the key is indexed, which a partition key nearly
always is — or, with `CursorSource.SEQUENCE`, the last value of the key's serial or
identity sequence: one catalog read, right only for a key fed by that sequence. An empty
table's cursor is `origin`.

Retention on an integer axis is a count (`KeepNewest`) or a distance behind the cursor
(`KeepBehind(distance)`, pg_partman's rule for id sets). Age-based rules never fire: an
integer window has no age.

## One value each: `IntegerSequence`

```python
IntegerSequence(start=100)                                       # 100, 101, 102, …
IntegerSequence(start=1, name_suffix="_p{value}")
```

The boundaries of a [sliding list](schemes.md#list-with-a-sequence-the-sliding-list):
every window is one value, `[value, value + 1)`, written as `FOR VALUES IN (value)`. The
cursor is the level's newest partition, read off the tree; with
`cursor_source=CursorSource.MAX_KEY` it is `max(key)` instead.

## Codecs: time partitions over an encoded key

A time-partitioned table has two notions of "when". The **semantic period** — a week, a
month — decides names, the create-ahead window and retention. The **physical boundary**
is the literal PostgreSQL compares the key against. For a `timestamptz` key they
coincide. They come apart when the key is a *time-sortable identifier*: a UUIDv7, a
ULID, an epoch bigint. A `RangeBoundaryCodec` bridges the two:

```python
TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="uuidv7")
TimeBoundaries(granularity=PartitionGranularity.DAY, codec="epoch_milliseconds")
```

```text
week 35 of 2026:   FOR VALUES FROM ('01a03111-1c00-7000-8000-000000000000')
                              TO ('01a0551d-a000-7000-8000-000000000000')
```

Built-in codecs, addressable by name in serialized configs: `uuidv7`, `epoch_seconds`,
`epoch_milliseconds`. `UUIDv7BoundaryCodec` renders each boundary instant as the
**smallest** UUIDv7 of that millisecond, every random bit zero, which makes adjacent
periods exactly contiguous — one period's upper bound is the next one's lower bound, and
no identifier can fall between two partitions. The same encoding serves the query side:
`codec.min_uuid_for(instant)` turns a time filter into an `id` range the planner can
prune on.

Codecs are **bidirectional** on purpose. Retention compares a partition's catalog upper
bound against the cutoff, and ownership compares it against the grid; a codec that could
only encode would create partitions the library could never recognise again.

`PostgresMetadataProvider(engine, boundary_codec=…)` needs the codec only for
`is_partition_closed`; planning decodes through the configuration.

## Cursors, in one place

| Axis | Cursor | Where it comes from |
|---|---|---|
| time | the current instant, in the calendar's zone | the clock (`now=` on `plan()` to override) |
| integer (`NumericBoundaries`) | the key's high-water mark | `max(key)` over the table, or the sequence |
| integer (`IntegerSequence`) | the newest partition's value | the tree (`NEWEST_MEMBER`), or `max(key)` |

The cursor's own window and everything ahead of it receive rows and are never expired,
whatever the retention rule says. The plan records the cursors it was made against
(`plan.cursors`).

## Writing your own

A whole axis is a `RangeBoundaries` implementation — `window_at`, `shift`, `literals`,
`decode`, `child_name`, `parse_child_name`, `describe`, `axis`, `cursor_source` — the same
protocol the three built-ins implement. A custom calendar is usually simpler: a
`PeriodCalculator` plugged into `TimeBoundaries(calculator=…)`; a custom encoding is a
two-method codec. Both are walked through in
[Custom calendars, names and codecs](../guide/calendars-and-codecs.md).
