# Period strategies

Period calculators determine how partitions are named and what range boundaries they get.
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

```python
from pg_partsmith import (
    DayPeriodCalculator,
    HourPeriodCalculator,
    MonthPeriodCalculator,
    QuarterPeriodCalculator,
    WeekPeriodCalculator,
    YearPeriodCalculator,
)
```

`WeekPeriodCalculator` uses lowercase `w` and enforces lowercase naming for all existing
partitions to maintain consistency.

`HourPeriodCalculator` works in UTC and emits boundaries with hour precision
(`2024-01-15 09:00:00+00`), so it is suitable for short-lived buffer tables
(e.g. transactional outboxes) where retention is measured in hours.

## Timezone

Every calculator accepts `tz` (`datetime.UTC` by default, or a keyed
`zoneinfo.ZoneInfo`): the current period is derived from "now" in that zone, and naive
boundary literals mean period starts in that zone. Keep the repository's `ddl_timezone`
aligned — `PartitionLifecycleService` refuses a mismatched pair. `HourPeriodCalculator`
is UTC-only (local hour names are ambiguous under DST). See
[Advanced → Timezone semantics](advanced.md#timezone-semantics).

```python
from zoneinfo import ZoneInfo

calc = DayPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))
calc = get_period_calculator(PartitionGranularity.MONTH, tz=ZoneInfo("Europe/Moscow"))
```

## Passing a calculator to the service

```python
from pg_partsmith.aio import PartitionLifecycleService
from pg_partsmith import MonthPeriodCalculator

service = PartitionLifecycleService(
    repo=...,
    metadata=...,
    locks=...,
    period_calculator=MonthPeriodCalculator(),
)
```

## Custom calculator

Subclass `BasePeriodCalculator` to define a custom naming scheme or non-standard boundaries.
A subclass must define `_NAME_PATTERN` (a compiled regex whose group 1 is the table name)
and implement `_period_from_match`; the inherited `parse_partition_name` then matches names
against the pattern and returns `None` for names that do not match or encode invalid values:

```python
import re
from datetime import datetime, timezone
from typing import ClassVar

from pg_partsmith.strategies import BasePeriodCalculator
from pg_partsmith.entities import Period


class FiscalYearPeriodCalculator(BasePeriodCalculator):
    """Yearly partitions aligned to a fiscal year starting April 1."""

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__fy(\d{4})$")

    def current_period(self) -> Period:
        now = datetime.now(timezone.utc)
        fiscal_year = now.year if now.month >= 4 else now.year - 1
        return Period(year=fiscal_year)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        return f"{table_name}__fy{period.year}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        # group 1 is the table name, group 2 encodes the fiscal year
        return Period(year=int(match.group(2)))

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        # fiscal year N runs from 1 April N to 1 April N+1
        return (f"{period.year}-04-01", f"{period.year + 1}-04-01")
```

## Protocol

All calculators implement the `PeriodCalculator` protocol, which means you can also provide
a fully custom implementation without subclassing `BasePeriodCalculator`:

```python
from pg_partsmith import PeriodCalculator
from pg_partsmith.entities import Period


class MyCalculator:
    def current_period(self) -> Period: ...
    def next_periods(self, count: int) -> list[Period]: ...
    def period_before(self, reference: Period, offset: int) -> Period: ...
    def format_partition_name(self, table_name: str, period: Period) -> str: ...
    def parse_partition_name(self, partition_name: str) -> Period | None: ...
    def get_boundaries(self, period: Period) -> tuple[str, str]: ...
```

`PeriodCalculator` is `@runtime_checkable`, so you can validate custom implementations at
runtime:

```python
calc = MyCalculator()
assert isinstance(calc, PeriodCalculator)  # passes if all methods are present
```
