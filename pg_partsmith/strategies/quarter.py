"""Quarter-based period calculator."""

from __future__ import annotations

import re
from typing import ClassVar

from pg_partsmith.entities import Period

from .base import BasePeriodCalculator


class QuarterPeriodCalculator(BasePeriodCalculator):
    """Calculator for quarterly partitions.

    Generates partitions with quarter granularity.
    Partition naming: ``{table}__{YYYY}_q{Q}``
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__(\d{4})_q([1-4])$")

    def current_period(self) -> Period:
        """Get current quarter period."""
        now = self._now()
        return Period(year=now.year, quarter=(now.month - 1) // 3 + 1)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        """Format partition name: ``table__YYYY_qQ``."""
        if period.quarter is None:
            msg = "Quarter is required for QuarterPeriodCalculator"
            raise ValueError(msg)
        return f"{table_name}__{period}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        return Period(year=int(match.group(2)), quarter=int(match.group(3)))

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        """Get quarter boundaries as ``(start_date, end_date)`` in ISO format."""
        if period.quarter is None:
            msg = "Quarter is required for QuarterPeriodCalculator"
            raise ValueError(msg)

        return self._encoded_boundaries(period, lambda d: d.strftime("%Y-%m-%d"))
