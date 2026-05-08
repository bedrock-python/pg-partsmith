import pytest

from pg_partsmith.exceptions import (
    InvalidPartitionConfigError,
    LockAcquisitionError,
    PartitionAlreadyExistsError,
    PartitionAttachedError,
    PartitionDetachInProgressError,
    PartitionError,
    PartitionNotFoundError,
)


@pytest.mark.parametrize(
    "exc_class",
    [
        PartitionAlreadyExistsError,
        PartitionNotFoundError,
        PartitionAttachedError,
        PartitionDetachInProgressError,
        InvalidPartitionConfigError,
        LockAcquisitionError,
    ],
)
def test__exception_hierarchy__all_domain_exceptions__are_subclasses_of_partition_error(
    exc_class: type[PartitionError],
) -> None:
    # Arrange / Act / Assert
    assert issubclass(exc_class, PartitionError)


def test__partition_already_exists_error__with_name__message_contains_name() -> None:
    # Arrange / Act
    e = PartitionAlreadyExistsError("events__2024_01")

    # Assert
    assert "events__2024_01" in str(e)
    assert e.partition_name == "events__2024_01"


def test__partition_not_found_error__with_name__message_contains_name() -> None:
    # Arrange / Act
    e = PartitionNotFoundError("events__2024_01")

    # Assert
    assert "events__2024_01" in str(e)
    assert e.partition_name == "events__2024_01"


def test__partition_attached_error__with_name_and_table__message_contains_both() -> None:
    # Arrange / Act
    e = PartitionAttachedError("events__2024_01", "events")

    # Assert
    assert "events__2024_01" in str(e)
    assert "events" in str(e)
    assert e.partition_name == "events__2024_01"
    assert e.table_name == "events"


def test__partition_detach_in_progress_error__with_name__message_contains_name() -> None:
    # Arrange / Act
    e = PartitionDetachInProgressError("events__2024_01")

    # Assert
    assert "events__2024_01" in str(e)


def test__invalid_partition_config_error__with_message__message_is_preserved() -> None:
    # Arrange / Act
    e = InvalidPartitionConfigError("bad config")

    # Assert
    assert "bad config" in str(e)


def test__lock_acquisition_error__without_reason__reason_is_none() -> None:
    # Arrange / Act
    e = LockAcquisitionError("events")

    # Assert
    assert "events" in str(e)
    assert e.table_name == "events"
    assert e.reason is None


def test__lock_acquisition_error__with_reason__reason_in_message() -> None:
    # Arrange / Act
    e = LockAcquisitionError("events", "timeout")

    # Assert
    assert "timeout" in str(e)
    assert e.reason == "timeout"
