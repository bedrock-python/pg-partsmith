"""Year-based period calculator."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import ClassVar

from dateutil.relativedelta import relativedelta

from pg_partsmith.entities import Period
from pg_partsmith.utils import utc_now

from .base import BasePeriodCalculator


class YearPeriodCalculator(BasePeriodCalculator):
    """Calculator for yearly partitions.

    Generates partitions with year granularity.
    Partition naming: ``{table}__{YYYY}``
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__(\d{4})$")

    def current_period(self) -> Period:
        """Get current year period."""
        now = utc_now()
        return Period(year=now.year)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        """Format partition name: ``table__YYYY``."""
        return f"{table_name}__{period.year:04d}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        return Period(year=int(match.group(2)))

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        """Get year boundaries as ``(start_date, end_date)`` in ISO format."""
        start_date = datetime(period.year, 1, 1, tzinfo=UTC)
        end_date = start_date + relativedelta(years=1)

        return (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
