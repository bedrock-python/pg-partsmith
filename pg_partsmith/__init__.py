"""PostgreSQL partition lifecycle management with extensible hooks."""

from .__version__ import __version__
from .entities import (
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
from .protocols import PeriodCalculator
from .strategies import (
    BasePeriodCalculator,
    DayPeriodCalculator,
    HourPeriodCalculator,
    MonthPeriodCalculator,
    QuarterPeriodCalculator,
    WeekPeriodCalculator,
    YearPeriodCalculator,
)
from .strategies.selector import get_period_calculator

__all__ = [
    "BasePeriodCalculator",
    "DayPeriodCalculator",
    "DropRetryExhaustedError",
    "HourPeriodCalculator",
    "InvalidPartitionConfigError",
    "LockAcquisitionError",
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
    "UnmanagedPartitionDropError",
    "WeekPeriodCalculator",
    "YearPeriodCalculator",
    "__version__",
    "get_period_calculator",
]
