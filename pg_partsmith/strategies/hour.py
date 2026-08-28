"""Hour-based period calculator."""

from __future__ import annotations

import re
from datetime import UTC, datetime, tzinfo
from typing import TYPE_CHECKING, ClassVar

from pg_partsmith.periods import Period

from .base import BasePeriodCalculator

if TYPE_CHECKING:
    from pg_partsmith.boundaries import RangeBoundaryCodec


class HourPeriodCalculator(BasePeriodCalculator):
    """Calculator for hourly partitions.

    Generates partitions with hour granularity. UTC only: in a zone with DST
    a local hour can repeat or vanish, making ``{table}__YYYY_MM_DD_HH`` names
    ambiguous, so non-UTC timezones are rejected.
    Partition naming: ``{table}__{YYYY}_{MM}_{DD}_{HH}``
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^(.+)__(\d{4})_(\d{2})_(\d{2})_(\d{2})$")

    def __init__(self, tz: tzinfo = UTC, *, boundary_codec: RangeBoundaryCodec | None = None) -> None:
        """Initialize calculator; only ``tz=datetime.UTC`` is accepted.

        Raises:
            ValueError: If ``tz`` is not UTC — local-time hour partition names
                are ambiguous under DST transitions.
        """
        super().__init__(tz=tz, boundary_codec=boundary_codec)
        if self.timezone_name != "UTC":
            msg = (
                f"HourPeriodCalculator supports only UTC, got {self.timezone_name!r}: "
                "local-time hour partition names are ambiguous under DST transitions"
            )
            raise ValueError(msg)

    def period_at(self, instant: datetime) -> Period:
        """Return the hour period holding ``instant``."""
        now = self._local(instant)
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

        return self._encoded_boundaries(period, lambda d: d.strftime("%Y-%m-%d %H:00:00+00"))
