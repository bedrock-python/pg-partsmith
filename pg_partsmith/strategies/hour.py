"""Hour-based period calculator."""

from __future__ import annotations

import re
from typing import ClassVar

from pg_partsmith.entities import Period
from pg_partsmith.utils import utc_now

from .base import BasePeriodCalculator


class HourPeriodCalculator(BasePeriodCalculator):
    """Calculator for hourly partitions.

    Generates partitions with hour granularity.
    Partition naming: ``{table}__{YYYY}_{MM}_{DD}_{HH}``
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__(\d{4})_(\d{2})_(\d{2})_(\d{2})$")

    def current_period(self) -> Period:
        """Get current hour period."""
        now = utc_now()
        return Period(year=now.year, month=now.month, day=now.day, hour=now.hour)

    def format_partition_name(self, table_name: str, period: Period) -> str:
        """Format partition name: ``table__YYYY_MM_DD_HH``."""
        if period.month is None or period.day is None or period.hour is None:
            msg = "Month, day and hour are required for HourPeriodCalculator"
            raise ValueError(msg)
        return f"{table_name}__{period}"

    def _period_from_match(self, match: re.Match[str]) -> Period:
        return Period(
            year=int(match.group(2)),
            month=int(match.group(3)),
            day=int(match.group(4)),
            hour=int(match.group(5)),
        )

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        """Get hour boundaries as ``(start, end)`` UTC timestamps with hour precision."""
        if period.month is None or period.day is None or period.hour is None:
            msg = "Month, day and hour are required for HourPeriodCalculator"
            raise ValueError(msg)

        fmt = "%Y-%m-%d %H:00:00+00"
        return (period.to_datetime().strftime(fmt), (period + 1).to_datetime().strftime(fmt))
