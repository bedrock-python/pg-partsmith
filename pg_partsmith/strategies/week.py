"""Week-based period calculator."""

from __future__ import annotations

import re
from typing import ClassVar

from pg_partsmith.entities import Period

from .base import BasePeriodCalculator


class WeekPeriodCalculator(BasePeriodCalculator):
    """Calculator for weekly partitions.

    Generates partitions with ISO-week granularity.
    Partition naming: ``{table}__{YYYY}_w{WW}``
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__(\d{4})_w(\d{2})$")

    def current_period(self) -> Period:
        """Get current ISO-week period."""
        now = self._now()
        iso_year, iso_week, _ = now.isocalendar()
        return Period(year=iso_year, week=iso_week)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        """Format partition name: ``table__YYYY_wWW``."""
        if period.week is None:
            msg = "Week is required for WeekPeriodCalculator"
            raise ValueError(msg)
        return f"{table_name}__{period}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        return Period(year=int(match.group(2)), week=int(match.group(3)))

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        """Get ISO-week boundaries (Monday to Monday) as ISO date strings."""
        if period.week is None:
            msg = "Week is required for WeekPeriodCalculator"
            raise ValueError(msg)

        fmt = "%Y-%m-%d"
        return (period.to_date().strftime(fmt), (period + 1).to_date().strftime(fmt))
