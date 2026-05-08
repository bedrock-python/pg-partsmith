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
