# Custom calendars, names and codecs

The built-in calendars, names and codecs cover most tables. When they do not — a fiscal
year, a legacy naming convention, a Snowflake id — plug your own in. Three extension
points, from the most common to the rarest.

## A custom calendar or naming: `PeriodCalculator`

`TimeBoundaries(calculator=…)` accepts any period calculator. Subclass a built-in one to
change the names, or `BasePeriodCalculator` to change the periods themselves.

### Keep a legacy naming convention

A previous manager named weeks by their Monday (`events_20260824`). New partitions should
match:

```python
import re
from datetime import date

from pg_partsmith import Period, TimeBoundaries, WeekPeriodCalculator


class MondayNamedWeeks(WeekPeriodCalculator):
    _NAME_PATTERN = re.compile(r"^(.+)_(\d{4})(\d{2})(\d{2})$")

    def format_partition_name(self, table_name: str, period: Period) -> str:
        return f"{table_name}_{period.to_date():%Y%m%d}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        monday = date(int(match.group(2)), int(match.group(3)), int(match.group(4)))
        iso_year, iso_week, _ = monday.isocalendar()
        return Period(year=iso_year, week=iso_week)


boundaries = TimeBoundaries(calculator=MondayNamedWeeks())
```

`_NAME_PATTERN`'s first group is the table name; `parse_partition_name` is what recognises
a detached orphan by name. Existing *attached* partitions are matched by bounds either
way — the naming only decides what new partitions are called and which orphans are ours.

### A fiscal year

```python
import re
from datetime import datetime
from typing import ClassVar

from pg_partsmith import Period
from pg_partsmith.strategies import BasePeriodCalculator


class FiscalYearPeriodCalculator(BasePeriodCalculator):
    """Yearly partitions aligned to a fiscal year starting April 1."""

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__fy(\d{4})$")

    def period_at(self, instant: datetime) -> Period:
        local = self._local(instant)
        return Period(year=local.year if local.month >= 4 else local.year - 1)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        return f"{table_name}__fy{period.year}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        return Period(year=int(match.group(2)))

    def period_start(self, period: Period) -> datetime:
        return datetime(period.year, 4, 1, tzinfo=self.tz)

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        return self._encoded_boundaries(period, lambda d: d.strftime("%Y-%m-%d"))
```

What each method is for:

| Method | Role |
|---|---|
| `period_at(instant)` | which period holds an instant — the cursor, a `CreateUntil` horizon, the start of a partition read from the catalog |
| `period_start(period)` | the instant a period begins; must agree with `get_boundaries`, because the planner compares instants decoded from the catalog with the ones the calendar produces |
| `get_boundaries(period)` | the two literals; `_encoded_boundaries` routes through the codec when one is configured |
| `format_partition_name` / `_period_from_match` | the name and its inverse |
| `own_name_budget()` (optional) | bytes the suffix adds, for the 63-byte check; a generous default is assumed otherwise |

The full protocol, for an implementation that does not subclass `BasePeriodCalculator`:

```python
class MyCalculator:
    def current_period(self) -> Period: ...
    def period_at(self, instant: datetime) -> Period: ...          # optional but recommended
    def next_periods(self, count: int) -> list[Period]: ...
    def period_before(self, reference: Period, offset: int) -> Period: ...
    def format_partition_name(self, table_name: str, period: Period) -> str: ...
    def parse_partition_name(self, partition_name: str) -> Period | None: ...
    def get_boundaries(self, period: Period) -> tuple[str, str]: ...
```

Without `period_at`, positions are found by walking from the current period one step at
a time — fine near now, slow for a horizon years away.

Every built-in calculator takes `tz` (`datetime.UTC`, or a keyed `ZoneInfo`); `HOUR` is
UTC-only. Keep the repository's `ddl_timezone` aligned.

## A custom key encoding: `RangeBoundaryCodec`

A codec turns period instants into the literals of an encoded key and back. Two methods:

```python
from datetime import UTC, datetime

from pg_partsmith import PartitionGranularity, TimeBoundaries


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


TimeBoundaries(granularity=PartitionGranularity.DAY, codec=SnowflakeBoundaryCodec())
```

Two rules make a codec correct: `encode` is monotonic in its argument, and adjacent
periods are contiguous — the upper literal of one period is the lower literal of the
next, no gap, no overlap. `decode` returns `None` for anything carrying no instant
(`MINVALUE`, `MAXVALUE`, a literal of another type) rather than raising, so a table with a
mixed history can still be introspected. Use the same codec on the query side to turn
time filters into key ranges.

Built-in codecs are addressable by name in serialized configs (`"uuidv7"`,
`"epoch_seconds"`, `"epoch_milliseconds"`); a custom one is an object and is left out of
`model_dump`.

## A whole axis: `RangeBoundaries`

When the axis is neither a calendar nor integers, implement the protocol `TimeBoundaries`
and `NumericBoundaries` implement:

```python
class RangeBoundaries(Protocol):
    axis: Axis                                   # TIME or INTEGER
    cursor_source: CursorSource                  # where "now" comes from
    def window_at(self, position) -> Window: ...
    def shift(self, window: Window, offset: int) -> Window: ...
    def literals(self, window: Window) -> tuple[str, str]: ...
    def decode(self, literal: str): ...          # a position, or None
    def child_name(self, parent_relname: str, window: Window) -> str: ...
    def parse_child_name(self, relname: str) -> Window | None: ...
    def describe(self, window: Window) -> str: ...
```

The contract: windows tile the axis with no gap and no overlap, `shift` walks them in
order, `decode` inverts `literals` closely enough for equality, `child_name` is
deterministic. Pass the instance as `RangePartitioning(boundaries=…)`; add
`own_name_budget()` if the names are long.

## Hash and list names

`HashPartitioning(name_suffix="_h{remainder}")` and `ListPartitioning(name_suffix="_{name}")`
change how members are named; `{remainder}` / `{name}` must appear, and the rest is
lowercase identifier characters. List members are matched by the values they own, so a
suffix change only affects new members.
