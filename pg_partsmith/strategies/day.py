"""Day-based period calculator."""

from __future__ import annotations

import re
from typing import ClassVar

from pg_partsmith.entities import Period

from .base import BasePeriodCalculator


class DayPeriodCalculator(BasePeriodCalculator):
    """Calculator for daily partitions.

    Generates partitions with day granularity.
    Partition naming: ``{table}__{YYYY}_{MM}_{DD}``
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__(\d{4})_(\d{2})_(\d{2})$")

    def current_period(self) -> Period:
        """Get current day period."""
        now = self._now()
        return Period(year=now.year, month=now.month, day=now.day)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        """Format partition name: ``table__YYYY_MM_DD``."""
        if period.month is None or period.day is None:
            msg = "Month and day are required for DayPeriodCalculator"
            raise ValueError(msg)
        return f"{table_name}__{period}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        return Period(
            year=int(match.group(2)),
            month=int(match.group(3)),
            day=int(match.group(4)),
        )

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        """Get day boundaries as ``(start_date, end_date)`` in ISO format."""
        if period.month is None or period.day is None:
            msg = "Month and day are required for DayPeriodCalculator"
            raise ValueError(msg)

        return self._encoded_boundaries(period, lambda d: d.strftime("%Y-%m-%d"))
