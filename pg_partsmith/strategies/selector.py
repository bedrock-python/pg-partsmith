"""Factory for selecting a period calculator by granularity."""

from datetime import UTC, tzinfo

from pg_partsmith.entities import PartitionGranularity

from .base import BasePeriodCalculator
from .day import DayPeriodCalculator
from .hour import HourPeriodCalculator
from .month import MonthPeriodCalculator
from .quarter import QuarterPeriodCalculator
from .week import WeekPeriodCalculator
from .year import YearPeriodCalculator

_CALCULATORS: dict[PartitionGranularity, type[BasePeriodCalculator]] = {
    PartitionGranularity.HOUR: HourPeriodCalculator,
    PartitionGranularity.DAY: DayPeriodCalculator,
    PartitionGranularity.WEEK: WeekPeriodCalculator,
    PartitionGranularity.MONTH: MonthPeriodCalculator,
    PartitionGranularity.QUARTER: QuarterPeriodCalculator,
    PartitionGranularity.YEAR: YearPeriodCalculator,
}


def get_period_calculator(granularity: PartitionGranularity, tz: tzinfo = UTC) -> BasePeriodCalculator:
    """Return the period calculator for the given granularity.

    Args:
        granularity: The partition time granularity.
        tz: Timezone the calculator works in (``datetime.UTC`` or a keyed
            :class:`zoneinfo.ZoneInfo`). HOUR accepts only UTC.

    Returns:
        A fresh calculator instance for the requested granularity.

    Raises:
        ValueError: If *granularity* has no registered calculator, or ``tz``
            is unsupported for it.
    """
    try:
        calculator_cls = _CALCULATORS[granularity]
    except KeyError:
        raise ValueError(f"No calculator for granularity: {granularity!r}") from None
    return calculator_cls(tz=tz)
