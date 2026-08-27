"""Base class for period calculators."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import UTC, datetime, tzinfo
from typing import ClassVar

from pg_partsmith.entities import Period
from pg_partsmith.utils import timezone_name


class BasePeriodCalculator(ABC):
    """Base class for all period calculators.

    Implements common logic for period calculations and defines
    the interface for granularity-specific strategies.
    Subclass and override any method to customise behaviour.

    Periods are computed in the calculator's timezone (UTC by default): the
    current period is derived from "now" in that zone, and naive boundary
    literals mean period starts in that zone. Keep the repository's
    ``ddl_timezone`` aligned with it — ``PartitionLifecycleService`` refuses a
    mismatched pair.

    Subclasses must define ``_NAME_PATTERN`` (a compiled regex) and implement
    ``_period_from_match`` to construct a ``Period`` from regex groups.
    Group 1 is conventionally the table name; subsequent groups encode the period.
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]]

    def __init__(self, tz: tzinfo = UTC) -> None:
        """Initialize calculator.

        Args:
            tz: Timezone the calculator works in. Only ``datetime.UTC`` and
                :class:`zoneinfo.ZoneInfo` instances are accepted — the zone
                must have an IANA name usable in ``SET LOCAL TIME ZONE``.

        Raises:
            ValueError: If ``tz`` carries no IANA name.
        """
        self._tz = tz
        self._tz_name = timezone_name(tz)

    @property
    def tz(self) -> tzinfo:
        """Timezone the calculator works in."""
        return self._tz

    @property
    def timezone_name(self) -> str:
        """IANA name of :attr:`tz`, usable in ``SET LOCAL TIME ZONE``."""
        return self._tz_name

    def _now(self) -> datetime:
        """Current time in the calculator's timezone."""
        return datetime.now(self._tz)

    @abstractmethod
    def current_period(self) -> Period:
        """Return the current period based on the current time in :attr:`tz`."""
        ...

    @abstractmethod
    def format_partition_name(self, table_name: str, period: Period) -> str:
        """Format partition name for a given table and period."""
        ...

    @abstractmethod
    def get_boundaries(self, period: Period) -> tuple[str, str]:
        """Return ``(from_value, to_value)`` boundaries for a period as ISO strings."""
        ...

    @abstractmethod
    def _period_from_match(self, match: re.Match[str]) -> Period:
        """Build a ``Period`` from a successful ``_NAME_PATTERN`` match.

        Subclasses may raise ``ValueError`` for invalid calendar values; the
        public ``parse_partition_name`` translates that into ``None``.
        """
        ...

    def parse_partition_name(self, partition_name: str) -> Period | None:
        """Parse period from a partition name.

        Returns ``None`` if the name does not match ``_NAME_PATTERN`` or encodes
        an invalid calendar value (e.g. month 13).
        """
        match = self._NAME_PATTERN.match(partition_name)
        if not match:
            return None
        try:
            return self._period_from_match(match)
        except ValueError:
            return None

    def next_periods(self, count: int) -> list[Period]:
        """Generate N periods starting from the current period (inclusive)."""
        if count <= 0:
            msg = "Count must be positive"
            raise ValueError(msg)

        current = self.current_period()
        return [self.period_after(current, i) for i in range(count)]

    def period_after(self, reference: Period, offset: int) -> Period:
        """Return the period ``offset`` steps after ``reference``."""
        if offset < 0:
            msg = "Offset must be non-negative"
            raise ValueError(msg)

        return reference + offset

    def period_before(self, reference: Period, offset: int) -> Period:
        """Return the period ``offset`` steps before ``reference``."""
        if offset < 0:
            msg = "Offset must be non-negative"
            raise ValueError(msg)

        return reference - offset
