"""Domain entities for PostgreSQL partitioning."""

from __future__ import annotations

import functools
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
from .topology import (
    DefaultBounds,
    HashBounds,
    HashSubpartitionSpec,
    ListBounds,
    ListGroup,
    ListSubpartitionSpec,
    PartitionBounds,
    PartitionNode,
    PartitionType,
    RangeBounds,
    SubpartitionBounds,
    SubpartitionSpec,
    SubpartitionSpecBase,
    validate_pg_identifier,
)
from .types import NonNegativeInt, PositiveInt, StrippedNonEmptyStr

# ``PartitionType`` and the partition-tree models live in ``topology`` so that
# module can stay IO-free and importable from anywhere; they are re-exported
# here because ``pg_partsmith.entities`` has always been their public home.
__all__ = [
    "DefaultBounds",
    "HashBounds",
    "HashSubpartitionSpec",
    "ListBounds",
    "ListGroup",
    "ListSubpartitionSpec",
    "MaintenanceIssue",
    "MaintenanceIssueStep",
    "MaintenanceResult",
    "PartitionBounds",
    "PartitionGranularity",
    "PartitionInfo",
    "PartitionNode",
    "PartitionStrategy",
    "PartitionType",
    "Period",
    "RangeBounds",
    "SubpartitionBounds",
    "SubpartitionSpec",
    "SubpartitionSpecBase",
    "TablePartitionConfig",
]


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
        bounds: Structured form of the same boundaries, discriminated on the
            bound kind. Populated from ``from_value``/``to_value`` for RANGE
            partitions when not supplied, so the two views never disagree.
        is_attached: Whether partition is currently attached to parent table.
        is_default: Whether this is the DEFAULT partition (no explicit boundaries).
        subpartition_type: How this partition partitions its own children, when
            it is itself a partitioned table. ``None`` for a leaf — which is
            what distinguishes a legacy leaf from a subpartitioned branch.
        parent_table: Name of parent partitioned table.
    """

    model_config = ConfigDict(frozen=True)

    name: StrippedNonEmptyStr
    partition_type: PartitionType
    from_value: str | None = None
    to_value: str | None = None
    boundaries_expr: str | None = None
    bounds: PartitionBounds | None = None
    is_attached: bool = True
    is_default: bool = False
    subpartition_type: PartitionType | None = None
    parent_table: StrippedNonEmptyStr | None = None

    @model_validator(mode="before")
    @classmethod
    def derive_range_bounds(cls, data: object) -> object:
        """Keep ``bounds`` and ``from_value``/``to_value`` in step.

        Both spellings of a RANGE boundary are part of the public surface:
        callers written before structured bounds existed pass the pair, newer
        ones pass ``bounds``. Deriving the missing side here means neither kind
        of caller can observe a half-populated model.
        """
        if not isinstance(data, dict):
            return data

        bounds = data.get("bounds")
        if bounds is None:
            from_value, to_value = data.get("from_value"), data.get("to_value")
            if not data.get("is_default") and from_value is not None and to_value is not None:
                data["bounds"] = RangeBounds(from_value=from_value, to_value=to_value)
            elif data.get("is_default"):
                data["bounds"] = DefaultBounds()
        elif isinstance(bounds, RangeBounds):
            data.setdefault("from_value", bounds.from_value)
            data.setdefault("to_value", bounds.to_value)

        return data

    @property
    def is_subpartitioned(self) -> bool:
        """True when this partition is itself a partitioned table (a branch)."""
        return self.subpartition_type is not None

    @property
    def hash_bounds(self) -> HashBounds | None:
        """This partition's ``MODULUS``/``REMAINDER`` bounds, when hash-bound."""
        return self.bounds if isinstance(self.bounds, HashBounds) else None

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

    @property
    def schema_name(self) -> str | None:
        """Schema part of :attr:`name`, or None when the name is unqualified."""
        schema, _ = _split_name(self.name)
        return schema

    @property
    def relname(self) -> str:
        """Bare relation name without the schema qualifier.

        ``list_partitions`` always returns schema-qualified names; use this
        when addressing the partition through code that works with bare names
        (period parsing, export layouts, catalogue lookups).
        """
        _, relname = _split_name(self.name)
        return relname


def _split_name(name: str) -> tuple[str | None, str]:
    """Split ``schema.relname`` into parts; unqualified names get a None schema."""
    parts = name.split(".")
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0], parts[1]
    return None, name


class TablePartitionConfig(BaseModel):
    """Configuration for table partitioning maintenance.

    A root is either **time-based** — RANGE over a date/time dimension, with a
    create-ahead window and a retention window — or **static**: HASH_BASED or
    VALUE_BASED, divided into a fixed set of partitions described by
    :attr:`root_layout`, which neither grows with the clock nor ages out.

    Either kind can be subpartitioned further.

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
        retention_count: Number of partitions to retain. Counted in top-level
            time periods, never in subpartitions - the time dimension is the
            lifecycle dimension.
        auto_attach_after_create: Whether to attach immediately after creation.
        root_layout: For a HASH_BASED or VALUE_BASED root, the fixed set of
            partitions the table itself is divided into. Such a table has no
            time dimension, so it has no create-ahead window and nothing ages
            out of it — maintenance only converges the set. Must be ``None``
            for a TIME_BASED root, whose partitions come from its periods.
        subpartition: Optional subpartitioning applied inside each partition,
            making it a partitioned table in its own right (for example
            ``RANGE(created_at)`` weekly -> ``HASH(tenant_id)``). Leave ``None``
            for the classic one-leaf-per-partition layout.
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
    root_layout: SubpartitionSpec | None = None
    subpartition: SubpartitionSpec | None = None

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
        if self.partition_strategy == PartitionStrategy.TIME_BASED:
            self._validate_time_based()
        else:
            self._validate_static_root()

        return self

    @property
    def is_time_based(self) -> bool:
        """True when this table's partitions come from a calendar period.

        A time-based table has a create-ahead window and a retention window; a
        static one — HASH or LIST at the root — has a fixed set of partitions
        that neither grows with the clock nor ages out.
        """
        return self.partition_strategy == PartitionStrategy.TIME_BASED

    def _validate_time_based(self) -> None:
        """Check a TIME_BASED root and the names its periods will generate."""
        if self.granularity is None:
            msg = "TIME_BASED strategy requires granularity"
            raise ValueError(msg)
        if self.partition_type != PartitionType.RANGE:
            msg = "TIME_BASED strategy requires RANGE partition type"
            raise ValueError(msg)
        if self.root_layout is not None:
            msg = (
                "root_layout is only for HASH_BASED / VALUE_BASED roots; a TIME_BASED table's "
                "partitions come from its periods. Use `subpartition` to divide each period."
            )
            raise ValueError(msg)

        # Validate that generated partition names will not exceed PostgreSQL's
        # 63-byte identifier limit (max_identifier_length default). PostgreSQL
        # truncates silently, so two hash buckets could otherwise collapse
        # onto a single name.
        suffix_len = _NAME_SUFFIX_LEN[self.granularity]
        subpartition_len = self.subpartition.name_length_budget() if self.subpartition is not None else 0
        total = len(self.table_name) + suffix_len + subpartition_len
        if total > MAX_IDENTIFIER_LENGTH:
            subpartition_part = f" + subpartition suffix ({subpartition_len})" if subpartition_len else ""
            msg = (
                f"table_name {self.table_name!r} is too long for "
                f"{self.granularity.value} granularity: "
                f"table_name ({len(self.table_name)}) + suffix ({suffix_len})"
                f"{subpartition_part} = {total} > {MAX_IDENTIFIER_LENGTH} bytes."
            )
            raise ValueError(msg)

    def _validate_static_root(self) -> None:
        """Check a HASH_BASED or VALUE_BASED root and its generated names."""
        expected_type = _STATIC_ROOT_TYPES[self.partition_strategy]

        if self.root_layout is None:
            msg = (
                f"{self.partition_strategy.value!r} strategy requires root_layout, describing the "
                f"{expected_type.value.upper()} partitions the table is divided into"
            )
            raise ValueError(msg)
        if self.partition_type != expected_type:
            msg = (
                f"{self.partition_strategy.value!r} strategy requires "
                f"{expected_type.value.upper()} partition type, got {self.partition_type.value.upper()}"
            )
            raise ValueError(msg)
        if self.root_layout.partition_type != expected_type:
            msg = (
                f"root_layout describes {self.root_layout.partition_type.value.upper()} partitions but "
                f"{self.partition_strategy.value!r} needs {expected_type.value.upper()}"
            )
            raise ValueError(msg)
        if self.root_layout.column != self.partition_column:
            msg = (
                f"root_layout column {self.root_layout.column!r} must be the table's own partition "
                f"column {self.partition_column!r}"
            )
            raise ValueError(msg)
        if self.granularity is not None:
            msg = f"{self.partition_strategy.value!r} strategy has no periods, so granularity must be unset"
            raise ValueError(msg)
        if self.subpartition is not None:
            msg = "Nest deeper levels inside root_layout's own `subpartition` rather than alongside it"
            raise ValueError(msg)

        total = len(self.table_name) + self.root_layout.name_length_budget()
        if total > MAX_IDENTIFIER_LENGTH:
            msg = (
                f"table_name {self.table_name!r} is too long for this layout: table_name "
                f"({len(self.table_name)}) + partition suffix ({self.root_layout.name_length_budget()}) "
                f"= {total} > {MAX_IDENTIFIER_LENGTH} bytes."
            )
            raise ValueError(msg)

    @model_validator(mode="after")
    def validate_subpartitioning(self) -> TablePartitionConfig:
        """Reject subpartitioning the library cannot manage on this root."""
        if self.subpartition is not None and self.partition_type != PartitionType.RANGE:
            msg = "Subpartitioning is only supported under a RANGE-partitioned root table"
            raise ValueError(msg)

        # Every level must divide on a fresh dimension: reusing a column would
        # leave the lower level with nothing left to separate. The root counts,
        # and for a static root it is already the first declared spec.
        columns = [self.partition_column]
        if self.root_layout is not None:
            columns.extend(spec.column for spec in self.root_layout.walk()[1:])
        elif self.subpartition is not None:
            columns.extend(spec.column for spec in self.subpartition.walk())

        duplicates = sorted({column for column in columns if columns.count(column) > 1})
        if duplicates:
            msg = f"Partition columns must be distinct across levels; {duplicates!r} appears more than once"
            raise ValueError(msg)

        return self

    @property
    def subpartition_levels(self) -> list[SubpartitionSpec]:
        """Every declared level, outermost first.

        For a static root that starts with :attr:`root_layout` itself; for a
        time-based one the root is the period dimension, which is not a spec, so
        the list starts below it.
        """
        if self.root_layout is not None:
            return self.root_layout.walk()
        return self.subpartition.walk() if self.subpartition is not None else []


# Root partition type each non-time strategy describes.
_STATIC_ROOT_TYPES: dict[PartitionStrategy, PartitionType] = {
    PartitionStrategy.HASH_BASED: PartitionType.HASH,
    PartitionStrategy.VALUE_BASED: PartitionType.LIST,
}

# Partition-name suffix lengths per granularity; must track the _SPECS fmt
# handlers and the calculators' _NAME_PATTERNs.
_NAME_SUFFIX_LEN: dict[PartitionGranularity, int] = {
    PartitionGranularity.HOUR: len("__0000_00_00_00"),
    PartitionGranularity.DAY: len("__0000_00_00"),
    PartitionGranularity.WEEK: len("__0000_w00"),
    PartitionGranularity.MONTH: len("__0000_00"),
    PartitionGranularity.QUARTER: len("__0000_q0"),
    PartitionGranularity.YEAR: len("__0000"),
}


def _validate_pg_identifier(v: str | None) -> str | None:
    """Validate and normalise an optional PostgreSQL identifier to lowercase."""
    return None if v is None else validate_pg_identifier(v)


class MaintenanceIssueStep(StrEnum):
    """Lifecycle step in which a non-fatal maintenance issue occurred."""

    CREATE = "create"
    RECONCILE = "reconcile"
    DETACH = "detach"
    DROP = "drop"


class MaintenanceIssue(BaseModel):
    """A non-fatal problem recorded during a maintenance run.

    Attributes:
        step: Lifecycle step the problem occurred in.
        error: Error message (``TypeName: message``).
        partition_name: Partition the problem concerns, when it is specific to
            one - subpartition reconciliation always sets it.
    """

    model_config = ConfigDict(frozen=True)

    step: MaintenanceIssueStep
    error: StrippedNonEmptyStr
    partition_name: str | None = None


class MaintenanceResult(BaseModel):
    """Result of partition maintenance operation.

    Attributes:
        created_count: Number of top-level partitions created. A subpartitioned
            branch counts once, however many leaves it contains - the branch is
            the lifecycle unit.
        repaired_count: Number of subpartitions created inside *pre-existing*
            branches to close gaps in their child sets.
        detached_count: Number of partitions detached in this run.
        dropped_count: Number of partitions dropped.
        duration_ms: Duration of maintenance in milliseconds.
        error: Fatal error message (set when the whole maintenance run fails).
        issues: Non-fatal problems. Step failures land here when the run was
            started with ``continue_on_error=True``; topology divergences that
            reconciliation deliberately refused to repair are always recorded,
            since leaving them unreported would hide rejected writes.
    """

    model_config = ConfigDict(frozen=True)

    created_count: NonNegativeInt = 0
    repaired_count: NonNegativeInt = 0
    detached_count: NonNegativeInt = 0
    dropped_count: NonNegativeInt = 0
    duration_ms: NonNegativeInt = 0
    error: str | None = None
    issues: tuple[MaintenanceIssue, ...] = ()

    @property
    def success(self) -> bool:
        """True only when there is no fatal error (non-fatal ``issues`` may exist)."""
        return self.error is None
