"""Domain exceptions for partition management."""


class PartitionError(Exception):
    """Base exception for partition-related errors."""


class PartitionAlreadyExistsError(PartitionError):
    """Raised when attempting to create a partition that already exists."""

    def __init__(self, partition_name: str) -> None:
        super().__init__(f"Partition already exists: {partition_name}")
        self.partition_name = partition_name


class PartitionNotFoundError(PartitionError):
    """Raised when a partition is not found."""

    def __init__(self, partition_name: str) -> None:
        super().__init__(f"Partition not found: {partition_name}")
        self.partition_name = partition_name


class PartitionAttachedError(PartitionError):
    """Raised when attempting to drop an attached partition."""

    def __init__(self, partition_name: str, table_name: str) -> None:
        super().__init__(f"Partition {partition_name} is still attached to table {table_name}")
        self.partition_name = partition_name
        self.table_name = table_name


class PartitionDetachInProgressError(PartitionError):
    """Raised when detach operation is in progress."""

    def __init__(self, partition_name: str) -> None:
        super().__init__(f"Detach operation in progress for partition: {partition_name}")
        self.partition_name = partition_name


class PartitionTopologyError(PartitionError):
    """Raised when an existing partition tree diverges from the configured one.

    Carries the planner's finding verbatim so callers can branch on
    :attr:`reason` instead of matching on message text. Reconciliation records
    these on ``MaintenanceResult.issues`` rather than raising, because one
    historical branch with an unexpected shape must not abort maintenance for
    every other partition.
    """

    def __init__(self, partition_name: str, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.partition_name = partition_name
        self.reason = reason
        self.detail = detail


class UnsupportedCapabilityError(PartitionError):
    """Raised when a config is wired to components that cannot serve it.

    Custom repositories and metadata providers written against the flat
    protocols keep working for flat configs; they are only refused when a
    config actually asks for something they do not implement. The capability is
    named rather than assumed, because more than one of them is optional.
    """

    def __init__(self, component: str, capability: str, expected: str) -> None:
        msg = (
            f"{component} does not support {capability}: it must implement {expected}. "
            "Use the bundled PostgreSQL implementation, or extend yours with the missing methods."
        )
        super().__init__(msg)
        self.component = component
        self.capability = capability
        self.expected = expected


class InvalidPartitionConfigError(PartitionError):
    """Raised when partition configuration is invalid."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Invalid partition configuration: {message}")


class LockAcquisitionError(PartitionError):
    """Raised when unable to acquire lock for partition operation."""

    def __init__(self, table_name: str, reason: str | None = None) -> None:
        msg = f"Failed to acquire lock for table {table_name}"
        if reason:
            msg = f"{msg}: {reason}"
        super().__init__(msg)
        self.table_name = table_name
        self.reason = reason


class DropRetryExhaustedError(PartitionError):
    """Raised when all drop_partition retry attempts are exhausted.

    This means PostgreSQL returned a retryable error (deadlock, lock timeout,
    or query cancellation) on every attempt.  Inspect ``cause`` for the last
    underlying error.
    """

    def __init__(self, partition_name: str, attempts: int, cause: BaseException | None = None) -> None:
        msg = (
            f"Failed to drop partition {partition_name!r} after {attempts} attempt(s) "
            "due to persistent lock contention or deadlock"
        )
        if cause:
            msg = f"{msg}: {cause}"
        super().__init__(msg)
        self.partition_name = partition_name
        self.attempts = attempts
        self.cause = cause


class UnmanagedPartitionDropError(PartitionError):
    """Raised when attempting to drop a table not managed by this library."""

    def __init__(self, partition_name: str) -> None:
        msg = f"Refusing to drop unmanaged table {partition_name!r}; set drop_allow_unmanaged=True to override."
        super().__init__(msg)
        self.partition_name = partition_name
