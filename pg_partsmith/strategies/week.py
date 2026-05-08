"""Week-based period calculator."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from pg_partsmith.entities import Period
from pg_partsmith.utils import utc_now

from .base import BasePeriodCalculator


class WeekPeriodCalculator(BasePeriodCalculator):
    """Calculator for weekly partitions.

    Generates partitions with ISO-week granularity.
    Partition naming: ``{table}__{YYYY}_w{WW}``
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__(\d{4})_w(\d{2})$")

    def current_period(self) -> Period:
        """Get current ISO-week period."""
        now = utc_now()
        iso_year, iso_week, _ = now.isocalendar()
        return Period(year=iso_year, week=iso_week)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        """Format partition name: ``table__YYYY_wWW``."""
        if period.week is None:
            msg = "Week is required for WeekPeriodCalculator"
            raise ValueError(msg)
        return f"{table_name}__{period.year:04d}_w{period.week:02d}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        return Period(year=int(match.group(2)), week=int(match.group(3)))

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        """Get ISO-week boundaries (Monday to Monday) as ISO date strings."""
        if period.week is None:
            msg = "Week is required for WeekPeriodCalculator"
            raise ValueError(msg)

        date_str = f"{period.year:04d}-W{period.week:02d}-1"
        start_date = datetime.strptime(date_str, "%G-W%V-%u").replace(tzinfo=UTC)
        end_date = start_date + timedelta(weeks=1)

        return (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
