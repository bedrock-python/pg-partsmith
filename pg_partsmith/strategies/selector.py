"""Factory for selecting a period calculator by granularity."""

from pg_partsmith.entities import PartitionGranularity

from .base import BasePeriodCalculator
from .day import DayPeriodCalculator
from .month import MonthPeriodCalculator
from .week import WeekPeriodCalculator
from .year import YearPeriodCalculator

_CALCULATORS: dict[PartitionGranularity, type[BasePeriodCalculator]] = {
    PartitionGranularity.DAY: DayPeriodCalculator,
    PartitionGranularity.WEEK: WeekPeriodCalculator,
    PartitionGranularity.MONTH: MonthPeriodCalculator,
    PartitionGranularity.YEAR: YearPeriodCalculator,
}


def get_period_calculator(granularity: PartitionGranularity) -> BasePeriodCalculator:
    """Return the period calculator for the given granularity.

    Args:
        granularity: The partition time granularity.

    Returns:
        A fresh calculator instance for the requested granularity.

    Raises:
        ValueError: If *granularity* has no registered calculator.
    """
    try:
        return _CALCULATORS[granularity]()
    except KeyError:
        raise ValueError(f"No calculator for granularity: {granularity!r}") from None
