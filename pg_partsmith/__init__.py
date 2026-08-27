"""PostgreSQL partition lifecycle management with extensible hooks."""

from .__version__ import __version__
from .entities import (
    MaintenanceIssue,
    MaintenanceIssueStep,
    MaintenanceResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    Period,
    TablePartitionConfig,
)
from .exceptions import (
    DropRetryExhaustedError,
    InvalidPartitionConfigError,
    LockAcquisitionError,
    PartitionAlreadyExistsError,
    PartitionAttachedError,
    PartitionDetachInProgressError,
    PartitionError,
    PartitionNotFoundError,
    UnmanagedPartitionDropError,
)
from .protocols import DdlTimezoneAware, PeriodCalculator, TimezoneAwareCalculator
from .strategies import (
    BasePeriodCalculator,
    DayPeriodCalculator,
    HourPeriodCalculator,
    MonthPeriodCalculator,
    QuarterPeriodCalculator,
    WeekPeriodCalculator,
    YearPeriodCalculator,
    get_period_calculator,
)
from .utils import qualify, split_qualified_name

__all__ = [
    "BasePeriodCalculator",
    "DayPeriodCalculator",
    "DdlTimezoneAware",
    "DropRetryExhaustedError",
    "HourPeriodCalculator",
    "InvalidPartitionConfigError",
    "LockAcquisitionError",
    "MaintenanceIssue",
    "MaintenanceIssueStep",
    "MaintenanceResult",
    "MonthPeriodCalculator",
    "PartitionAlreadyExistsError",
    "PartitionAttachedError",
    "PartitionDetachInProgressError",
    "PartitionError",
    "PartitionGranularity",
    "PartitionInfo",
    "PartitionNotFoundError",
    "PartitionStrategy",
    "PartitionType",
    "Period",
    "PeriodCalculator",
    "QuarterPeriodCalculator",
    "TablePartitionConfig",
    "TimezoneAwareCalculator",
    "UnmanagedPartitionDropError",
    "WeekPeriodCalculator",
    "YearPeriodCalculator",
    "__version__",
    "get_period_calculator",
    "qualify",
    "split_qualified_name",
]
