"""Domain entities for PostgreSQL partitioning."""

from __future__ import annotations

import functools
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .constants import (
    DEFAULT_CREATE_AHEAD_COUNT,
    DEFAULT_RETENTION_COUNT,
    MAX_HOUR,
    MAX_IDENTIFIER_LENGTH,
    MAX_ISO_WEEK,
    MAX_MONTH,
    MAX_QUARTER,
    MIN_HOUR,
    MIN_ISO_WEEK,
    MIN_MONTH,
    MIN_QUARTER,
)
from .types import NonNegativeInt, PositiveInt, StrippedNonEmptyStr


class PartitionType(StrEnum):
    """PostgreSQL partition type.

    Attributes:
        RANGE: Range partitioning (e.g., by date ranges).
        LIST: List partitioning (e.g., by specific values).
        HASH: Hash partitioning (e.g., by hash of key).
    """

    RANGE = "range"
    LIST = "list"
    HASH = "hash"


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


class PartitionStrategy(StrEnum):
    """Partition strategy type.

    Attributes:
        TIME_BASED: Time-based partitioning using granularity.
        VALUE_BASED: Value-based partitioning (LIST).
        HASH_BASED: Hash-based partitioning.
    """

    TIME_BASED = "time_based"
    VALUE_BASED = "value_based"
    HASH_BASED = "hash_based"


@dataclass(frozen=True)
@functools.total_ordering
class Period:
    """Represents a time period for partition boundaries.

    Every period has exactly one granularity kind (``year``, ``month``,
    ``day``, ``week``, ``hour`` or ``quarter``), decided once by
    :meth:`_granularity_key`. All kind-specific behaviour (validation,
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
        return _SPECS[self._granularity_key(self)]

    @staticmethod
    def _granularity_key(p: Period) -> str:
        if p.week is not None:
            return "week"
        if p.quarter is not None:
            return "quarter"
        if p.hour is not None:
            return "hour"
        if p.day is not None:
            return "day"
        if p.month is not None:
            return "month"
        return "year"

    def to_date(self, day: int = 1) -> date:
        """Convert period to date.

        Args:
            day: Day of month (default: 1). Ignored for weekly periods and
                when self.day is already set. For quarterly periods the day
                applies within the quarter's first month.

        Returns:
            Date object representing this period. For weekly periods returns
            the Monday of the ISO week. For hourly periods returns the
            calendar date (the hour component is dropped; use
            :meth:`to_datetime` to preserve it).

        Raises:
            ValueError: If ``day`` is out of range for the resolved month,
                e.g. ``day=31`` for a Q2 period (April has 30 days).
        """
        return self._spec.start_date(self, day)

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

    def __lt__(self, other: Period) -> bool:
        """Compare periods of the same granularity kind."""
        # Required by Python's data model: returning NotImplemented lets Python
        # try the reflected comparison on `other`.
        if not isinstance(other, Period):  # type: ignore[unreachable]
            return NotImplemented  # type: ignore[unreachable]

        kind = self._granularity_key(self)
        if kind != self._granularity_key(other):
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
# Period._granularity_key to recognise it).
#
# The handlers may assume the invariants enforced by their kind's `validate`
# (e.g. an "hour" period always has month, day and hour set).


@dataclass(frozen=True)
class _GranularitySpec:
    """Kind-specific behaviour of :class:`Period`.

    Attributes:
        validate: Check field consistency and ranges; raise ``ValueError``.
        start_date: Return the period's start date (``to_date`` semantics).
        add: Return the period shifted by an offset of whole periods.
        sort_key: Tuple used to order periods of the same kind.
        fmt: Canonical string form (used in partition names).
    """

    validate: Callable[[Period], None]
    start_date: Callable[[Period, int], date]
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


def _year_start(p: Period, day: int) -> date:
    return date(p.year, 1, day)


def _year_add(p: Period, offset: int) -> Period:
    return Period(year=p.year + offset)


def _year_sort(p: Period) -> tuple[int | None, ...]:
    return (p.year,)


def _year_fmt(p: Period) -> str:
    return f"{p.year:04d}"


# ── month ──


def _month_validate(p: Period) -> None:
    _check_month_range(p)


def _month_start(p: Period, day: int) -> date:
    month = p.month if p.month is not None else 1
    return date(p.year, month, day)


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


def _day_start(p: Period, day: int) -> date:
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


def _week_start(p: Period, day: int) -> date:
    week = p.week if p.week is not None else 1
    return date.fromisocalendar(p.year, week, 1)


def _week_add(p: Period, offset: int) -> Period:
    new_date = _week_start(p, 1) + timedelta(weeks=offset)
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


def _hour_start(p: Period, day: int) -> date:
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


def _quarter_start(p: Period, day: int) -> date:
    quarter = p.quarter if p.quarter is not None else 1
    return date(p.year, (quarter - 1) * 3 + 1, day)


def _quarter_add(p: Period, offset: int) -> Period:
    quarter = p.quarter if p.quarter is not None else 1
    total_quarters = p.year * 4 + quarter - 1 + offset
    return Period(year=total_quarters // 4, quarter=(total_quarters % 4) + 1)


def _quarter_sort(p: Period) -> tuple[int | None, ...]:
    return (p.year, p.quarter)


def _quarter_fmt(p: Period) -> str:
    return f"{p.year:04d}_q{p.quarter}"


_SPECS: dict[str, _GranularitySpec] = {
    "year": _GranularitySpec(_year_validate, _year_start, _year_add, _year_sort, _year_fmt),
    "month": _GranularitySpec(_month_validate, _month_start, _month_add, _month_sort, _month_fmt),
    "day": _GranularitySpec(_day_validate, _day_start, _day_add, _day_sort, _day_fmt),
    "week": _GranularitySpec(_week_validate, _week_start, _week_add, _week_sort, _week_fmt),
    "hour": _GranularitySpec(_hour_validate, _hour_start, _hour_add, _hour_sort, _hour_fmt),
    "quarter": _GranularitySpec(_quarter_validate, _quarter_start, _quarter_add, _quarter_sort, _quarter_fmt),
}


class PartitionInfo(BaseModel):
    """Metadata about a partition.

    Attributes:
        name: Partition table name.
        partition_type: Type of partition (RANGE, LIST, HASH).
        from_value: Start boundary value (for RANGE).
        to_value: End boundary value (for RANGE).
        boundaries_expr: Raw boundary expression as reported by PostgreSQL
            (``pg_get_expr(relpartbound, oid)``). Useful when parsing boundaries
            fails but the partition is still attached.
        is_attached: Whether partition is currently attached to parent table.
        is_default: Whether this is the DEFAULT partition (no explicit boundaries).
        parent_table: Name of parent partitioned table.
    """

    model_config = ConfigDict(frozen=True)

    name: StrippedNonEmptyStr
    partition_type: PartitionType
    from_value: str | None = None
    to_value: str | None = None
    boundaries_expr: str | None = None
    is_attached: bool = True
    is_default: bool = False
    parent_table: StrippedNonEmptyStr | None = None

    @model_validator(mode="after")
    def validate_range_boundaries(self) -> PartitionInfo:
        """Validate that attached RANGE partitions have boundaries.

        Detached (orphaned) partitions may have lost their boundary metadata
        from the catalog and are allowed to carry ``None`` boundaries.

        For attached partitions we accept either parsed boundaries
        (``from_value`` + ``to_value``) OR a raw boundaries expression so that
        callers can still reason about partitions even when expression parsing
        fails.
        """
        if self._requires_boundaries() and not (self._has_parsed_boundaries() or self._has_raw_boundaries()):
            msg = "Attached RANGE partitions must have from_value/to_value or boundaries_expr"
            raise ValueError(msg)
        return self

    def _requires_boundaries(self) -> bool:
        return self.partition_type == PartitionType.RANGE and self.is_attached and not self.is_default

    def _has_parsed_boundaries(self) -> bool:
        return self.from_value is not None and self.to_value is not None

    def _has_raw_boundaries(self) -> bool:
        return self.boundaries_expr is not None and self.boundaries_expr.strip() != ""


def _validate_pg_identifier(v: str | None) -> str | None:
    """Validate and normalise PostgreSQL identifier to lowercase.

    PostgreSQL folds unquoted identifiers to lower-case; normalising here
    ensures that metadata catalogue queries and quoted DDL identifiers
    always refer to the same object.
    """
    if v is None:
        return None
    v = v.lower()
    if not re.match(r"^[a-z_][a-z0-9_]*$", v):
        msg = f"Invalid SQL identifier: {v!r}"
        raise ValueError(msg)
    if len(v) > MAX_IDENTIFIER_LENGTH:
        msg = f"SQL identifier too long (max {MAX_IDENTIFIER_LENGTH} chars): {v!r}"
        raise ValueError(msg)
    return v


class TablePartitionConfig(BaseModel):
    """Configuration for table partitioning maintenance.

    Only TIME_BASED (RANGE by date/time) partitioning is currently supported.
    VALUE_BASED and HASH_BASED strategies are reserved for future use and will
    raise a ValueError at construction time.

    Attributes:
        schema: Optional schema name for the partitioned table. When set, all
            DDL and catalogue queries are schema-qualified, making behaviour
            deterministic in databases with multiple schemas.
        table_name: Name of the partitioned table (lowercase, max 63 chars minus
            the longest generated partition suffix).
        partition_type: Type of partitioning (RANGE, LIST, HASH).
        partition_strategy: Strategy for partitioning.
        partition_column: Column used for partitioning.
        granularity: Time granularity (for TIME_BASED strategy).
        create_ahead_count: Number of periods to ensure exist, including the current period.
        retention_count: Number of partitions to retain.
        auto_attach_after_create: Whether to attach immediately after creation.
    """

    model_config = ConfigDict(frozen=True)

    # NOTE: We store the value under a different field name to avoid Pydantic's
    # warning about shadowing BaseModel.schema(). Externally, the public API is
    # still `schema=...` and `config.db_schema`.
    schema_name: StrippedNonEmptyStr | None = Field(default=None, alias="schema")
    table_name: StrippedNonEmptyStr
    partition_type: PartitionType
    partition_strategy: PartitionStrategy
    partition_column: StrippedNonEmptyStr
    granularity: PartitionGranularity | None = None
    create_ahead_count: PositiveInt = Field(
        default=DEFAULT_CREATE_AHEAD_COUNT,
        description="Number of periods to ensure exist, including the current period",
    )
    retention_count: PositiveInt = Field(default=DEFAULT_RETENTION_COUNT, description="Number of partitions to retain")
    auto_attach_after_create: bool = True

    @property
    def db_schema(self) -> StrippedNonEmptyStr | None:
        """PostgreSQL schema name."""
        return self.schema_name

    @field_validator("table_name", "partition_column")
    @classmethod
    def validate_identifier(cls, v: str) -> str:
        """Validate and normalise SQL identifiers."""
        result = _validate_pg_identifier(v)
        if result is None:
            msg = "SQL identifier cannot be empty"
            raise ValueError(msg)
        return result

    @field_validator("schema_name")
    @classmethod
    def validate_schema(cls, v: str | None) -> str | None:
        """Validate and normalise schema name."""
        return _validate_pg_identifier(v)

    @model_validator(mode="after")
    def validate_strategy_requirements(self) -> TablePartitionConfig:
        """Validate strategy-specific requirements."""
        if self.partition_strategy in (PartitionStrategy.VALUE_BASED, PartitionStrategy.HASH_BASED):
            msg = (
                f"{self.partition_strategy.value!r} strategy is not yet implemented. "
                "Only TIME_BASED is currently supported."
            )
            raise ValueError(msg)

        if self.partition_strategy == PartitionStrategy.TIME_BASED:
            if self.granularity is None:
                msg = "TIME_BASED strategy requires granularity"
                raise ValueError(msg)
            if self.partition_type != PartitionType.RANGE:
                msg = "TIME_BASED strategy requires RANGE partition type"
                raise ValueError(msg)

            # Validate that generated partition names will not exceed PostgreSQL's
            # 63-byte identifier limit (max_identifier_length default).
            suffix_len = {
                PartitionGranularity.HOUR: len("__0000_00_00_00"),
                PartitionGranularity.DAY: len("__0000_00_00"),
                PartitionGranularity.WEEK: len("__0000_w00"),
                PartitionGranularity.MONTH: len("__0000_00"),
                PartitionGranularity.QUARTER: len("__0000_q0"),
                PartitionGranularity.YEAR: len("__0000"),
            }[self.granularity]
            if len(self.table_name) + suffix_len > MAX_IDENTIFIER_LENGTH:
                msg = (
                    f"table_name {self.table_name!r} is too long for "
                    f"{self.granularity.value} granularity: "
                    f"table_name ({len(self.table_name)}) + suffix ({suffix_len}) = "
                    f"{len(self.table_name) + suffix_len} > {MAX_IDENTIFIER_LENGTH} bytes."
                )
                raise ValueError(msg)

        return self


class MaintenanceIssueStep(StrEnum):
    """Maintenance step identifier for hooks."""

    CREATE = "create"
    ATTACH = "attach"
    DETACH = "detach"
    DROP = "drop"
    HOOK_BEFORE_CREATE = "hook_before_create"
    HOOK_AFTER_CREATE = "hook_after_create"
    HOOK_BEFORE_DETACH = "hook_before_detach"
    HOOK_AFTER_DETACH = "hook_after_detach"
    HOOK_BEFORE_DROP = "hook_before_drop"
    HOOK_AFTER_DROP = "hook_after_drop"


class MaintenanceResult(BaseModel):
    """Result of partition maintenance operation.

    Attributes:
        created_count: Number of partitions created.
        detached_count: Number of partitions detached in this run.
        dropped_count: Number of partitions dropped.
        duration_ms: Duration of maintenance in milliseconds.
        error: Fatal error message (set when the whole maintenance run fails).
    """

    model_config = ConfigDict(frozen=True)

    created_count: NonNegativeInt = 0
    detached_count: NonNegativeInt = 0
    dropped_count: NonNegativeInt = 0
    duration_ms: NonNegativeInt = 0
    error: str | None = None

    @property
    def success(self) -> bool:
        """True only when there is no fatal error."""
        return self.error is None
