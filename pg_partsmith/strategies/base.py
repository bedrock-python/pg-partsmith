"""Base class for period calculators."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import ClassVar

from pg_partsmith.entities import Period


class BasePeriodCalculator(ABC):
    """Base class for all period calculators.

    Implements common logic for period calculations and defines
    the interface for granularity-specific strategies.
    Subclass and override any method to customise behaviour.

    Subclasses must define ``_NAME_PATTERN`` (a compiled regex) and implement
    ``_period_from_match`` to construct a ``Period`` from regex groups.
    Group 1 is conventionally the table name; subsequent groups encode the period.
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]]

    @abstractmethod
    def current_period(self) -> Period:
        """Return the current period based on the current UTC time."""
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
