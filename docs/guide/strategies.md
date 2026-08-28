# Period strategies

Period calculators are the calendar behind `TimeBoundaries`: they decide which period an
instant falls in, how partitions are named, and what range boundaries they get.
pg-partsmith ships six built-in calculators and a base class for custom ones.

## Built-in calculators

| Class | Granularity | Example partition name |
|-------|-------------|------------------------|
| `HourPeriodCalculator` | Hourly (UTC) | `events__2024_01_15_09` |
| `DayPeriodCalculator` | Daily | `events__2024_01_15` |
| `WeekPeriodCalculator` | ISO weekly | `events__2024_w03` |
| `MonthPeriodCalculator` | Monthly | `events__2024_01` |
| `QuarterPeriodCalculator` | Quarterly | `events__2024_q1` |
| `YearPeriodCalculator` | Yearly | `events__2024` |

`TimeBoundaries(granularity=...)` picks one of them; `TimeBoundaries(calculator=...)` takes
any instance, including your own.

`HourPeriodCalculator` works in UTC and emits boundaries with hour precision
(`2024-01-15 09:00:00+00`), so it is suitable for short-lived buffer tables
(e.g. transactional outboxes) where retention is measured in hours.

## Timezone

Every calculator accepts `tz` (`datetime.UTC` by default, or a keyed `zoneinfo.ZoneInfo`):
the current period is derived from "now" in that zone, and naive boundary literals mean
period starts in that zone. `TimeBoundaries(tz=...)` forwards it. Keep the repository's
`ddl_timezone` aligned — the service refuses a mismatched pair. `HourPeriodCalculator` is
UTC-only (local hour names are ambiguous under DST). See
[Advanced → Timezone semantics](advanced.md#timezone-semantics).

```python
from zoneinfo import ZoneInfo

calc = DayPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))
calc = get_period_calculator(PartitionGranularity.MONTH, tz=ZoneInfo("Europe/Moscow"))
```

## Custom calculator

Subclass `BasePeriodCalculator` to define a custom naming scheme or non-standard boundaries.
A subclass defines `_NAME_PATTERN` (a compiled regex whose group 1 is the table name),
implements `period_at` and `_period_from_match`, and renders boundaries in `get_boundaries`:

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

`period_at` is what makes a calculator usable at any position on the axis — a horizon for
`CreateUntil`, the start of an existing partition when the planner checks whether it lies
on the grid. `period_start` must agree with `get_boundaries`, because the planner compares
the instants it decodes from the catalog with the instants the calendar produces.
`_encoded_boundaries` routes through the boundary codec when one is configured.

## Protocol

All calculators implement the `PeriodCalculator` protocol, so a fully custom implementation
without `BasePeriodCalculator` also works:

```python
class MyCalculator:
    def current_period(self) -> Period: ...
    def period_at(self, instant: datetime) -> Period: ...
    def next_periods(self, count: int) -> list[Period]: ...
    def period_before(self, reference: Period, offset: int) -> Period: ...
    def format_partition_name(self, table_name: str, period: Period) -> str: ...
    def parse_partition_name(self, partition_name: str) -> Period | None: ...
    def get_boundaries(self, period: Period) -> tuple[str, str]: ...
```

A calculator that lacks `period_at` still works: `TimeBoundaries` walks from the current
period one step at a time, which is slow only for positions very far from now.

`PeriodCalculator` is `@runtime_checkable`:

```python
assert isinstance(MyCalculator(), PeriodCalculator)
```
