"""Common protocols (interfaces) for partition management."""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

from .entities import Period

__all__ = ["PeriodCalculator"]

PeriodT = TypeVar("PeriodT", bound=Period)


@runtime_checkable
class PeriodCalculator(Protocol[PeriodT]):
    """Calculator for time-based partition periods.

    This protocol defines the interface for calculating partition periods,
    formatting partition names, and parsing period information from names.
    """

    def current_period(self) -> PeriodT:
        """Get current period based on current time.

        Returns:
            Current period.
        """
        ...

    def next_periods(self, count: int) -> list[PeriodT]:
        """Generate next N periods from current period.

        Args:
            count: Number of periods to generate.

        Returns:
            List of future periods.
        """
        ...

    def period_before(self, reference: PeriodT, offset: int) -> PeriodT:
        """Calculate period before reference period.

        Args:
            reference: Reference period.
            offset: Number of periods before reference.

        Returns:
            Past period.
        """
        ...

    def format_partition_name(self, table_name: str, period: PeriodT) -> str:
        """Format partition name for a period.

        Args:
            table_name: Parent table name.
            period: Time period.

        Returns:
            Formatted partition name.
        """
        ...

    def parse_partition_name(self, partition_name: str) -> PeriodT | None:
        """Parse period from partition name.

        Args:
            partition_name: Partition table name.

        Returns:
            Parsed period, or None if name doesn't match the pattern or
            encodes an invalid calendar value.
        """
        ...

    def get_boundaries(self, period: PeriodT) -> tuple[str, str]:
        """Get partition boundaries for a period.

        Args:
            period: Time period.

        Returns:
            Tuple of (from_value, to_value) as SQL-compatible strings.
        """
        ...
