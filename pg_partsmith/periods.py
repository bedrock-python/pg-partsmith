"""Calendar periods: the semantic units a time-partitioned table is divided into."""

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum

from .constants import (
    MAX_HOUR,
    MAX_ISO_WEEK,
    MAX_MONTH,
    MAX_QUARTER,
    MIN_HOUR,
    MIN_ISO_WEEK,
    MIN_MONTH,
    MIN_QUARTER,
)

__all__ = ["PartitionGranularity", "Period"]


class PartitionGranularity(StrEnum):
    """Time-based partition granularity.

    Attributes:
        HOUR: Hourly partitions.
        DAY: Daily partitions.
        WEEK: Weekly partitions.
        MONTH: Monthly partitions.
        QUARTER: Quarterly partitions.
        YEAR: Yearly partitions.
    """

    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


@dataclass(frozen=True)
@functools.total_ordering
class Period:
    """Represents a time period for partition boundaries.

    Every period has exactly one granularity kind (a
    :class:`PartitionGranularity` member), decided once by
    ``_granularity_key``. All kind-specific behaviour (validation,
    arithmetic, ordering, formatting) lives in the ``_SPECS`` table below,
    so supporting a new kind means adding one ``_GranularitySpec`` entry.

    Attributes:
        year: Year component.
        month: Month component (1-12), optional.
        day: Day component (1-31), optional.
        week: ISO week number (1-53), optional.
        hour: Hour component (0-23), optional; requires day.
        quarter: Quarter component (1-4), optional.
    """

    year: int
    month: int | None = None
    day: int | None = None
    week: int | None = None
    hour: int | None = None
    quarter: int | None = None

    def __post_init__(self) -> None:
        """Validate period components for this period's granularity kind."""
        self._spec.validate(self)

    @property
    def _spec(self) -> _GranularitySpec:
        """Per-kind behaviour for this period."""
        return _SPECS[_granularity_key(self)]

    def to_date(self) -> date:
        """Return the period's start date.

        Weekly periods return the Monday of the ISO week; hourly periods
        return the calendar date (use :meth:`to_datetime` to preserve the
        hour component).
        """
        return self._spec.start_date(self)

    def to_datetime(self) -> datetime:
        """Convert period start to a timezone-aware UTC datetime.

        Unlike :meth:`to_date`, preserves the hour component, so hourly
        periods within one day map to distinct instants.
        """
        base = datetime.combine(self.to_date(), datetime.min.time(), tzinfo=UTC)
        if self.hour is not None:
            base = base.replace(hour=self.hour)
        return base

    def __add__(self, offset: int) -> Period:
        """Add offset to period (implementation depends on granularity).

        Args:
            offset: Number of periods to add.

        Returns:
            New Period object.
        """
        return self._spec.add(self, offset)

    def __sub__(self, offset: int) -> Period:
        """Subtract offset from period.

        Args:
            offset: Number of periods to subtract.

        Returns:
            New Period object.
        """
        return self.__add__(-offset)

    def __lt__(self, other: object) -> bool:
        """Compare periods of the same granularity kind."""
        if not isinstance(other, Period):
            return NotImplemented

        kind = _granularity_key(self)
        if kind != _granularity_key(other):
            return NotImplemented

        spec = _SPECS[kind]
        return spec.sort_key(self) < spec.sort_key(other)

    def __str__(self) -> str:
        """String representation."""
        return self._spec.fmt(self)


# ── Per-granularity dispatch for Period ─────────────────────────────────────────
#
# Each granularity kind is described by one _GranularitySpec entry in _SPECS.
# Period methods dispatch on the kind exactly once; adding a new kind means
# adding one group of handlers plus one _SPECS entry (and teaching
# _granularity_key below to recognise it).
#
# The handlers may assume the invariants enforced by their kind's `validate`
# (e.g. an "hour" period always has month, day and hour set).


@dataclass(frozen=True)
class _GranularitySpec:
    """Kind-specific behaviour of :class:`Period`.

    Attributes:
        validate: Check field consistency and ranges; raise ``ValueError``.
        start_date: Return the period's start date.
        add: Return the period shifted by an offset of whole periods.
        sort_key: Tuple used to order periods of the same kind.
        fmt: Canonical string form (used in partition names).
    """

    validate: Callable[[Period], None]
    start_date: Callable[[Period], date]
    add: Callable[[Period, int], Period]
    sort_key: Callable[[Period], tuple[int | None, ...]]
    fmt: Callable[[Period], str]


def _check_month_range(p: Period) -> None:
    if p.month is None:
        return
    if not MIN_MONTH <= p.month <= MAX_MONTH:
        msg = f"Month must be between {MIN_MONTH} and {MAX_MONTH}, got {p.month}"
        raise ValueError(msg)


def _check_day_resolves_to_real_date(p: Period) -> None:
    if p.day is None:
        return
    if p.month is None:
        msg = "Month is required when day is specified"
        raise ValueError(msg)
    try:
        date(p.year, p.month, p.day)
    except ValueError as exc:
        msg = f"Invalid date: {p.year}-{p.month}-{p.day}"
        raise ValueError(msg) from exc


def _check_hour_requires_day_and_range(p: Period) -> None:
    if p.hour is None:
        return
    if p.day is None:
        msg = "Day is required when hour is specified"
        raise ValueError(msg)
    if not MIN_HOUR <= p.hour <= MAX_HOUR:
        msg = f"Hour must be between {MIN_HOUR} and {MAX_HOUR}, got {p.hour}"
        raise ValueError(msg)


# ── year ──


def _year_validate(p: Period) -> None:
    """A year-only period has no extra components to validate."""


def _year_start(p: Period) -> date:
    return date(p.year, 1, 1)


def _year_add(p: Period, offset: int) -> Period:
    return Period(year=p.year + offset)


def _year_sort(p: Period) -> tuple[int | None, ...]:
    return (p.year,)


def _year_fmt(p: Period) -> str:
    return f"{p.year:04d}"


# ── month ──


def _month_validate(p: Period) -> None:
    _check_month_range(p)


def _month_start(p: Period) -> date:
    month = p.month if p.month is not None else 1
    return date(p.year, month, 1)


def _month_add(p: Period, offset: int) -> Period:
    month = p.month if p.month is not None else 1
    total_months = p.year * 12 + month - 1 + offset
    return Period(year=total_months // 12, month=(total_months % 12) + 1)


def _month_sort(p: Period) -> tuple[int | None, ...]:
    return (p.year, p.month)


def _month_fmt(p: Period) -> str:
    return f"{p.year:04d}_{p.month:02d}"


# ── day ──


def _day_validate(p: Period) -> None:
    _check_month_range(p)
    _check_day_resolves_to_real_date(p)


def _day_start(p: Period) -> date:
    return _day_date(p)


def _day_add(p: Period, offset: int) -> Period:
    d = _day_date(p) + timedelta(days=offset)
    return Period(year=d.year, month=d.month, day=d.day)


def _day_sort(p: Period) -> tuple[int | None, ...]:
    return (p.year, p.month, p.day)


def _day_fmt(p: Period) -> str:
    return f"{p.year:04d}_{p.month:02d}_{p.day:02d}"


def _day_date(p: Period) -> date:
    """Calendar date of a day- or hour-kind period (fields guaranteed set)."""
    month = p.month if p.month is not None else 1
    day = p.day if p.day is not None else 1
    return date(p.year, month, day)


# ── week ──


def _week_validate(p: Period) -> None:
    if p.month is not None or p.day is not None or p.hour is not None:
        msg = "Period cannot have both week and month/day/hour"
        raise ValueError(msg)
    if p.quarter is not None:
        msg = "Period cannot have both quarter and month/day/week/hour"
        raise ValueError(msg)
    week = p.week if p.week is not None else 1
    if not MIN_ISO_WEEK <= week <= MAX_ISO_WEEK:
        msg = f"Week must be between {MIN_ISO_WEEK} and {MAX_ISO_WEEK}, got {week}"
        raise ValueError(msg)
    try:
        date.fromisocalendar(p.year, week, 1)
    except ValueError as exc:
        msg = f"Invalid ISO week: {p.year:04d}-W{week:02d}"
        raise ValueError(msg) from exc


def _week_start(p: Period) -> date:
    week = p.week if p.week is not None else 1
    return date.fromisocalendar(p.year, week, 1)


def _week_add(p: Period, offset: int) -> Period:
    new_date = _week_start(p) + timedelta(weeks=offset)
    iso_year, iso_week, _ = new_date.isocalendar()
    return Period(year=iso_year, week=iso_week)


def _week_sort(p: Period) -> tuple[int | None, ...]:
    return (p.year, p.week)


def _week_fmt(p: Period) -> str:
    return f"{p.year:04d}_w{p.week:02d}"


# ── hour ──


def _hour_validate(p: Period) -> None:
    _check_month_range(p)
    _check_day_resolves_to_real_date(p)
    _check_hour_requires_day_and_range(p)


def _hour_start(p: Period) -> date:
    return _day_date(p)


def _hour_add(p: Period, offset: int) -> Period:
    hour = p.hour if p.hour is not None else 0
    start = datetime.combine(_day_date(p), datetime.min.time(), tzinfo=UTC).replace(hour=hour)
    dt = start + timedelta(hours=offset)
    return Period(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour)


def _hour_sort(p: Period) -> tuple[int | None, ...]:
    return (p.year, p.month, p.day, p.hour)


def _hour_fmt(p: Period) -> str:
    return f"{p.year:04d}_{p.month:02d}_{p.day:02d}_{p.hour:02d}"


# ── quarter ──


def _quarter_validate(p: Period) -> None:
    if p.month is not None or p.day is not None or p.hour is not None:
        msg = "Period cannot have both quarter and month/day/week/hour"
        raise ValueError(msg)
    quarter = p.quarter if p.quarter is not None else 1
    if not MIN_QUARTER <= quarter <= MAX_QUARTER:
        msg = f"Quarter must be between {MIN_QUARTER} and {MAX_QUARTER}, got {quarter}"
        raise ValueError(msg)


def _quarter_start(p: Period) -> date:
    quarter = p.quarter if p.quarter is not None else 1
    return date(p.year, (quarter - 1) * 3 + 1, 1)


def _quarter_add(p: Period, offset: int) -> Period:
    quarter = p.quarter if p.quarter is not None else 1
    total_quarters = p.year * 4 + quarter - 1 + offset
    return Period(year=total_quarters // 4, quarter=(total_quarters % 4) + 1)


def _quarter_sort(p: Period) -> tuple[int | None, ...]:
    return (p.year, p.quarter)


def _quarter_fmt(p: Period) -> str:
    return f"{p.year:04d}_q{p.quarter}"


_SPECS: dict[PartitionGranularity, _GranularitySpec] = {
    PartitionGranularity.YEAR: _GranularitySpec(_year_validate, _year_start, _year_add, _year_sort, _year_fmt),
    PartitionGranularity.MONTH: _GranularitySpec(_month_validate, _month_start, _month_add, _month_sort, _month_fmt),
    PartitionGranularity.DAY: _GranularitySpec(_day_validate, _day_start, _day_add, _day_sort, _day_fmt),
    PartitionGranularity.WEEK: _GranularitySpec(_week_validate, _week_start, _week_add, _week_sort, _week_fmt),
    PartitionGranularity.HOUR: _GranularitySpec(_hour_validate, _hour_start, _hour_add, _hour_sort, _hour_fmt),
    PartitionGranularity.QUARTER: _GranularitySpec(
        _quarter_validate, _quarter_start, _quarter_add, _quarter_sort, _quarter_fmt
    ),
}


def _granularity_key(p: Period) -> PartitionGranularity:
    if p.week is not None:
        return PartitionGranularity.WEEK
    if p.quarter is not None:
        return PartitionGranularity.QUARTER
    if p.hour is not None:
        return PartitionGranularity.HOUR
    if p.day is not None:
        return PartitionGranularity.DAY
    if p.month is not None:
        return PartitionGranularity.MONTH
    return PartitionGranularity.YEAR
