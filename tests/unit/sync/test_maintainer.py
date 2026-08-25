from unittest.mock import MagicMock, patch

import pytest

from pg_partsmith.entities import (
    MaintenanceResult,
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import LockAcquisitionError, PartitionNotFoundError
from pg_partsmith.sync import maintain_partitions
from pg_partsmith.sync.maintainer import PartitionMaintainer


@pytest.fixture
def config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )


def _make_service(result: MaintenanceResult | None = None, side_effect: BaseException | None = None) -> MagicMock:
    mock_service = MagicMock()
    if side_effect is not None:
        mock_service.maintain_lifecycle = MagicMock(side_effect=side_effect)
    else:
        mock_service.maintain_lifecycle = MagicMock(return_value=result or MaintenanceResult())
    return mock_service


def test__run_maintenance__service_succeeds__returns_result_with_counts(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(MaintenanceResult(created_count=2, detached_count=1, dropped_count=1))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success is True
    assert result.created_count == 2
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert result.duration_ms >= 0
    mock_logger.info.assert_called()
    service.maintain_lifecycle.assert_called_once_with(config, skip_create=False, skip_detach=False, skip_drop=False)


def test__run_maintenance__service_returns_failure_result__propagates_result(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(MaintenanceResult(error="DB unreachable"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success is False
    assert result.error == "DB unreachable"
    mock_logger.info.assert_called()


def test__run_maintenance__service_raises_exception__reraises_and_logs(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(side_effect=Exception("unexpected crash"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger):
        with pytest.raises(Exception, match="unexpected crash"):
            maintainer.run_maintenance(config)

        mock_logger.exception.assert_called_once()


def test__run_maintenance__lock_acquisition_error__logs_warning_not_exception(
    config: TablePartitionConfig,
) -> None:
    """Lock contention is a routine operational failure — warning path, not the "unexpected" exception path."""
    # Arrange
    service = _make_service(side_effect=LockAcquisitionError("events", "advisory lock unavailable"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger), pytest.raises(LockAcquisitionError):
        maintainer.run_maintenance(config)

    mock_logger.exception.assert_not_called()
    warnings = [c for c in mock_logger.warning.call_args_list if "operational error" in c.args[0]]
    assert len(warnings) == 1
    extra = warnings[0].kwargs["extra"]
    assert extra["table_name"] == "events"
    assert extra["error_type"] == "LockAcquisitionError"
    assert extra["duration_ms"] >= 0


def test__run_maintenance__partition_error__logs_warning_not_exception(
    config: TablePartitionConfig,
) -> None:
    """Domain ``PartitionError`` subclasses are operational failures — warning path, not the exception path."""
    # Arrange
    service = _make_service(side_effect=PartitionNotFoundError("events_2024_01"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger), pytest.raises(PartitionNotFoundError):
        maintainer.run_maintenance(config)

    mock_logger.exception.assert_not_called()
    warnings = [c for c in mock_logger.warning.call_args_list if "operational error" in c.args[0]]
    assert len(warnings) == 1
    assert warnings[0].kwargs["extra"]["error_type"] == "PartitionNotFoundError"


def test__run_maintenance__keyboard_interrupt__propagates_and_logs_interrupted(
    config: TablePartitionConfig,
) -> None:
    """Sync analog of the async CancelledError test: KeyboardInterrupt is logged as interruption and re-raised."""
    # Arrange
    service = _make_service(side_effect=KeyboardInterrupt("operator interrupted run"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger):
        with pytest.raises(KeyboardInterrupt, match="operator interrupted run"):
            maintainer.run_maintenance(config)

        assert mock_logger.info.call_count >= 1
        messages = [str(c) for c in mock_logger.info.call_args_list]
        assert any("interrupted" in m or "system signal" in m for m in messages)


def test__run_maintenance_safe__service_raises__returns_failure_result(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(side_effect=RuntimeError("unexpected crash"))
    maintainer = PartitionMaintainer(service)

    # Act
    result = maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error is not None
    assert "RuntimeError" in result.error
    assert "unexpected crash" in result.error
    assert result.duration_ms >= 0


def test__run_maintenance_safe__keyboard_interrupt__returns_failure_result(
    config: TablePartitionConfig,
) -> None:
    """Sync analog of the async CancelledError test: KeyboardInterrupt is captured into ``result.error``."""
    # Arrange
    service = _make_service(side_effect=KeyboardInterrupt("operator interrupted run"))
    maintainer = PartitionMaintainer(service)

    # Act
    result = maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error is not None
    assert "KeyboardInterrupt" in result.error
    assert result.duration_ms >= 0


def test__run_maintenance_safe__system_exit__returns_failure_result(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(side_effect=SystemExit(1))
    maintainer = PartitionMaintainer(service)

    # Act
    result = maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error is not None
    assert "SystemExit" in result.error
    assert result.duration_ms >= 0


def test__run_maintenance__skip_flags__passed_through_to_service(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service()
    maintainer = PartitionMaintainer(service)

    # Act
    maintainer.run_maintenance(config, skip_create=True, skip_detach=True, skip_drop=True)

    # Assert
    service.maintain_lifecycle.assert_called_once_with(config, skip_create=True, skip_detach=True, skip_drop=True)


def test__run_maintenance__duration__is_non_negative(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service()
    maintainer = PartitionMaintainer(service)

    # Act
    result = maintainer.run_maintenance(config)

    # Assert
    assert result.duration_ms >= 0


def test__maintain_partitions__delegates_to_run_maintenance_safe_with_kwargs(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    expected = MaintenanceResult(created_count=1)
    maintainer = MagicMock()
    maintainer.run_maintenance_safe = MagicMock(return_value=expected)

    # Act
    result = maintain_partitions(maintainer, config, skip_create=True)

    # Assert
    assert result is expected
    maintainer.run_maintenance_safe.assert_called_once_with(
        config,
        skip_create=True,
        skip_detach=False,
        skip_drop=False,
    )


# NOTE: the async test ``test__run_maintenance_safe__redis_watchdog_cancels_task__returns_failure_result`` is not
# ported: the sync Redis renewal watchdog cannot cancel the holder — on renewal failure it only logs a warning and
# exits (covered in tests/unit/sync/test_locks.py).
