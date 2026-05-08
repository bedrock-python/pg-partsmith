# Period strategies

Period calculators determine how partitions are named and what range boundaries they get.
pg-partsmith ships four built-in calculators and a base class for custom ones.

## Built-in calculators

| Class | Granularity | Example partition name |
|-------|-------------|------------------------|
| `DayPeriodCalculator` | Daily | `events__2024_01_15` |
| `WeekPeriodCalculator` | ISO weekly | `events__2024_w03` |
| `MonthPeriodCalculator` | Monthly | `events__2024_01` |
| `YearPeriodCalculator` | Yearly | `events__2024` |

```python
from pg_partsmith import (
    DayPeriodCalculator,
    MonthPeriodCalculator,
    WeekPeriodCalculator,
    YearPeriodCalculator,
)
```

`WeekPeriodCalculator` uses lowercase `w` and enforces lowercase naming for all existing
partitions to maintain consistency.

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

Subclass `BasePeriodCalculator` to define a custom naming scheme or non-standard boundaries:

```python
from pg_partsmith.strategies import BasePeriodCalculator
from pg_partsmith.entities import Period
from datetime import datetime, timezone


class QuarterPeriodCalculator(BasePeriodCalculator):
    def current_period(self) -> Period:
        now = datetime.now(timezone.utc)
        quarter = (now.month - 1) // 3 + 1
        return Period(year=now.year, quarter=quarter)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        return f"{table_name}__q{period.quarter}_{period.year}"

    def parse_partition_name(self, partition_name: str) -> Period | None:
        # parse back to Period for retention logic
        ...

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        # return (from_value, to_value) as SQL-compatible strings
        ...
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
