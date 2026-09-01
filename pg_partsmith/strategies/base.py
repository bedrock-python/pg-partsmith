"""Base class for period calculators."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import UTC, datetime, tzinfo
from typing import TYPE_CHECKING, ClassVar

from pg_partsmith.partition_bounds import parse_boundary_literal
from pg_partsmith.periods import Period
from pg_partsmith.utils import timezone_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from pg_partsmith.boundaries import RangeBoundaryCodec


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

    Passing ``boundary_codec`` decouples the semantic period from the physical
    partition key: periods, names, create-ahead and retention keep working in
    calendar terms while the ``FOR VALUES FROM … TO …`` literals are whatever
    the key actually stores (a UUIDv7, a sortable id). Without one, boundaries
    are rendered as the calendar literals they have always been.
    """

    _NAME_PATTERN: ClassVar[re.Pattern[str]]

    def __init__(self, tz: tzinfo = UTC, *, boundary_codec: RangeBoundaryCodec | None = None) -> None:
        """Initialize calculator.

        Args:
            tz: Timezone the calculator works in. Only ``datetime.UTC`` and
                :class:`zoneinfo.ZoneInfo` instances are accepted — the zone
                must have an IANA name usable in ``SET LOCAL TIME ZONE``.
            boundary_codec: Optional encoder for the physical partition key.
                When set, period boundaries are encoded through it instead of
                being rendered as calendar literals.

        Raises:
            ValueError: If ``tz`` carries no IANA name.
        """
        self._tz = tz
        self._tz_name = timezone_name(tz)
        self._boundary_codec = boundary_codec

    @property
    def tz(self) -> tzinfo:
        """Timezone the calculator works in."""
        return self._tz

    @property
    def timezone_name(self) -> str:
        """IANA name of :attr:`tz`, usable in ``SET LOCAL TIME ZONE``."""
        return self._tz_name

    @property
    def boundary_codec(self) -> RangeBoundaryCodec | None:
        """Codec used to render and read physical boundary literals, if any."""
        return self._boundary_codec

    def period_start(self, period: Period) -> datetime:
        """Return the instant a period begins, in the calculator's timezone.

        ``Period.to_datetime`` pins UTC; a calculator working in a business
        timezone means the same calendar period starts at a different instant,
        which is what a boundary codec has to encode.
        """
        return period.to_datetime().replace(tzinfo=self._tz)

    def decode_boundary(self, literal: str) -> datetime | None:
        """Return the instant a catalog boundary literal stands for, or None.

        Retention compares partitions by their upper bound, so whatever encoded
        a boundary has to be able to read it back. Falls back to interpreting
        the literal as a timestamp when no codec is configured.
        """
        if self._boundary_codec is not None:
            return self._boundary_codec.decode(literal)
        return parse_boundary_literal(literal, self._tz)

    def _encoded_boundaries(
        self,
        period: Period,
        render: Callable[[datetime], str],
    ) -> tuple[str, str]:
        """Return this period's half-open boundaries as SQL literals.

        Args:
            period: The period to bound.
            render: Formats a boundary instant the way this granularity has
                always rendered it; used only when no codec is configured.
        """
        start, end = self.period_start(period), self.period_start(period + 1)
        if self._boundary_codec is not None:
            return self._boundary_codec.encode(start, end)
        return render(start), render(end)

    def _now(self) -> datetime:
        """Current time in the calculator's timezone."""
        return datetime.now(self._tz)

    def _local(self, instant: datetime) -> datetime:
        """Express an instant in the calculator's timezone (naive values are read as UTC)."""
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)
        return instant.astimezone(self._tz)

    @abstractmethod
    def period_at(self, instant: datetime) -> Period:
        """Return the period holding ``instant``, computed in :attr:`tz`.

        Every position-dependent question -- which period is current, which
        period a horizon falls in, which period an existing bound starts --
        is answered through this one method.
        """
        ...

    def current_period(self) -> Period:
        """Return the current period based on the current time in :attr:`tz`."""
        return self.period_at(self._now())

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
