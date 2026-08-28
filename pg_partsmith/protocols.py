"""Common protocols (interfaces) for partition management."""

from __future__ import annotations

from datetime import datetime, tzinfo
from typing import Protocol, TypeVar, runtime_checkable

from .entities import Period

__all__ = [
    "BoundaryDecoder",
    "DdlTimezoneAware",
    "PeriodCalculator",
    "TimezoneAwareCalculator",
]

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
        """Generate N periods starting from the current period (inclusive).

        Args:
            count: Number of periods to generate.

        Returns:
            List of periods, the current one first.
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


@runtime_checkable
class TimezoneAwareCalculator(Protocol):
    """Calculator that declares the timezone its periods are computed in.

    ``PartitionLifecycleService`` uses this to refuse a wiring whose calculator
    and repository DDL timezones disagree, and the pruning services use it to
    interpret naive catalog boundaries. Plain :class:`PeriodCalculator`
    implementations without timezone metadata are checked leniently (assumed
    UTC, no alignment enforcement).
    """

    @property
    def tz(self) -> tzinfo:
        """Timezone the calculator works in."""
        ...

    @property
    def timezone_name(self) -> str:
        """IANA name of :attr:`tz`, usable in ``SET LOCAL TIME ZONE``."""
        ...


@runtime_checkable
class DdlTimezoneAware(Protocol):
    """Repository that declares the timezone its boundary-sensitive DDL runs in."""

    @property
    def ddl_timezone(self) -> str | None:
        """Timezone applied via ``SET LOCAL TIME ZONE``; None trusts the session."""
        ...


@runtime_checkable
class BoundaryDecoder(Protocol):
    """Calculator that can read its own physical boundary literals back.

    Retention selects partitions by comparing a partition's *catalog* upper
    bound against the cutoff instant. When the partition key is not a timestamp
    — a UUIDv7, a ULID, an epoch bigint — that comparison is only possible if
    the component that encoded the boundary can also decode it. Calculators
    without this capability keep the historical timestamp interpretation.
    """

    def decode_boundary(self, literal: str) -> datetime | None:
        """Return the instant a boundary literal stands for, or None."""
        ...
