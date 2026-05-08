"""Domain entities for PostgreSQL partitioning."""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .constants import (
    DEFAULT_CREATE_AHEAD_COUNT,
    DEFAULT_RETENTION_COUNT,
    MAX_IDENTIFIER_LENGTH,
    MAX_ISO_WEEK,
    MAX_MONTH,
    MIN_ISO_WEEK,
    MIN_MONTH,
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
        DAY: Daily partitions.
        WEEK: Weekly partitions.
        MONTH: Monthly partitions.
        YEAR: Yearly partitions.
    """

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
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

    Attributes:
        year: Year component.
        month: Month component (1-12), optional.
        day: Day component (1-31), optional.
        week: ISO week number (1-53), optional.
    """

    year: int
    month: int | None = None
    day: int | None = None
    week: int | None = None

    def __post_init__(self) -> None:
        """Validate period components."""
        self._check_kind_consistency()
        self._check_month_range()
        self._check_day_resolves_to_real_date()
        self._check_week_resolves_to_real_iso_week()

    def _check_kind_consistency(self) -> None:
        if self.week is not None and (self.month is not None or self.day is not None):
            msg = "Period cannot have both week and month/day"
            raise ValueError(msg)

    def _check_month_range(self) -> None:
        if self.month is None:
            return
        if not MIN_MONTH <= self.month <= MAX_MONTH:
            msg = f"Month must be between {MIN_MONTH} and {MAX_MONTH}, got {self.month}"
            raise ValueError(msg)

    def _check_day_resolves_to_real_date(self) -> None:
        if self.day is None:
            return
        if self.month is None:
            msg = "Month is required when day is specified"
            raise ValueError(msg)
        try:
            date(self.year, self.month, self.day)
        except ValueError as exc:
            msg = f"Invalid date: {self.year}-{self.month}-{self.day}"
            raise ValueError(msg) from exc

    def _check_week_resolves_to_real_iso_week(self) -> None:
        if self.week is None:
            return
        if not MIN_ISO_WEEK <= self.week <= MAX_ISO_WEEK:
            msg = f"Week must be between {MIN_ISO_WEEK} and {MAX_ISO_WEEK}, got {self.week}"
            raise ValueError(msg)
        try:
            date.fromisocalendar(self.year, self.week, 1)
        except ValueError as exc:
            msg = f"Invalid ISO week: {self.year:04d}-W{self.week:02d}"
            raise ValueError(msg) from exc

    def to_date(self, day: int = 1) -> date:
        """Convert period to date.

        Args:
            day: Day of month (default: 1). Ignored for weekly periods and
                when self.day is already set.

        Returns:
            Date object representing this period. For weekly periods returns the
            Monday of the ISO week.
        """
        if self.week is not None:
            return date.fromisocalendar(self.year, self.week, 1)

        month = self.month if self.month is not None else 1
        day_value = self.day if self.day is not None else day

        return date(self.year, month, day_value)

    def __add__(self, offset: int) -> Period:
        """Add offset to period (implementation depends on granularity).

        Args:
            offset: Number of periods to add.

        Returns:
            New Period object.
        """
        if self.week is not None:
            monday = date.fromisocalendar(self.year, self.week, 1)
            new_date = monday + timedelta(weeks=offset)
            iso_year, iso_week, _ = new_date.isocalendar()
            return Period(year=iso_year, week=iso_week)

        if self.day is not None and self.month is not None:
            d = date(self.year, self.month, self.day) + timedelta(days=offset)
            return Period(year=d.year, month=d.month, day=d.day)

        if self.month is not None:
            total_months = self.year * 12 + self.month - 1 + offset
            new_year = total_months // 12
            new_month = (total_months % 12) + 1
            return Period(year=new_year, month=new_month)

        return Period(year=self.year + offset)

    def __sub__(self, offset: int) -> Period:
        """Subtract offset from period.

        Args:
            offset: Number of periods to subtract.

        Returns:
            New Period object.
        """
        return self.__add__(-offset)

    def __lt__(self, other: Period) -> bool:
        """Compare periods."""
        # Required by Python's data model: returning NotImplemented lets Python
        # try the reflected comparison on `other`.
        if not isinstance(other, Period):  # type: ignore[unreachable]
            return NotImplemented  # type: ignore[unreachable]

        self_kind = self._granularity_key(self)
        other_kind = self._granularity_key(other)
        if self_kind != other_kind:
            return NotImplemented

        if self_kind == "year":
            return self.year < other.year

        if self_kind == "month":
            return (self.year, self.month) < (other.year, other.month)

        if self_kind == "day":
            return (self.year, self.month, self.day) < (other.year, other.month, other.day)

        # week
        return (self.year, self.week) < (other.year, other.week)

    @staticmethod
    def _granularity_key(p: Period) -> str:
        if p.week is not None:
            return "week"
        if p.day is not None:
            return "day"
        if p.month is not None:
            return "month"
        return "year"

    def __str__(self) -> str:
        """String representation."""
        if self.month is not None and self.day is not None:
            return f"{self.year:04d}_{self.month:02d}_{self.day:02d}"
        if self.month is not None:
            return f"{self.year:04d}_{self.month:02d}"
        if self.week is not None:
            return f"{self.year:04d}_w{self.week:02d}"
        return f"{self.year:04d}"


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
                PartitionGranularity.DAY: len("__0000_00_00"),
                PartitionGranularity.WEEK: len("__0000_w00"),
                PartitionGranularity.MONTH: len("__0000_00"),
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
