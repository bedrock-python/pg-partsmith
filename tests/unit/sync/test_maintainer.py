"""Unit tests for the sync ``PartitionMaintainer`` orchestrator and the ``maintain_partitions`` helper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pg_partsmith.entities import (
    MaintenanceIssue,
    MaintenanceIssueStep,
    MaintenanceResult,
    PartitionGranularity,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import InvalidPartitionConfigError, LockAcquisitionError, PartitionNotFoundError
from pg_partsmith.sync import maintain_partitions
from pg_partsmith.sync.maintainer import PartitionMaintainer


@pytest.fixture
def config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        schema="public",
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


# ── run_maintenance ─────────────────────────────────────────────────────────────


def test__run_maintenance__service_succeeds__returns_result_with_counts_and_duration(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(
        MaintenanceResult(created_count=2, repaired_count=1, attached_count=1, detached_count=1, dropped_count=1)
    )
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.success is True
    assert (result.created_count, result.repaired_count, result.attached_count) == (2, 1, 1)
    assert (result.detached_count, result.dropped_count) == (1, 1)
    assert result.duration_ms >= 0
    service.maintain_lifecycle.assert_called_once_with(
        config, skip_create=False, skip_detach=False, skip_drop=False, continue_on_error=False
    )
    started, completed = mock_logger.info.call_args_list
    assert started.args[0] == "Starting partition maintenance"
    assert started.kwargs["extra"]["table_name"] == "public.events"
    assert completed.args[0] == "Partition maintenance completed successfully"
    assert completed.kwargs["extra"]["created_count"] == 2
    assert "duration" in completed.kwargs["extra"]


def test__run_maintenance__issues_on_the_result__are_kept(config: TablePartitionConfig) -> None:
    # Arrange
    issue = MaintenanceIssue(step=MaintenanceIssueStep.DROP, error="SQLAlchemyError: boom", partition_name="x")
    service = _make_service(MaintenanceResult(issues=(issue,)))
    maintainer = PartitionMaintainer(service)

    # Act
    result = maintainer.run_maintenance(config)

    # Assert
    assert result.success is True
    assert result.issues == (issue,)


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


def test__run_maintenance__service_raises_unexpected_exception__reraises_and_logs_traceback(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    class _CrashError(Exception):
        pass

    service = _make_service(side_effect=_CrashError("unexpected crash"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with (
        patch("pg_partsmith.sync.maintainer.logger", mock_logger),
        pytest.raises(_CrashError, match="unexpected crash"),
    ):
        maintainer.run_maintenance(config)

    mock_logger.exception.assert_called_once()
    assert mock_logger.exception.call_args.kwargs["extra"]["table_name"] == "public.events"
    mock_logger.warning.assert_not_called()


def test__run_maintenance__lock_acquisition_error__logs_warning_not_exception(
    config: TablePartitionConfig,
) -> None:
    # Arrange -- lock contention is a routine operational failure, not an unexpected one
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
    assert extra["table_name"] == "public.events"
    assert extra["error_type"] == "LockAcquisitionError"
    assert extra["duration_ms"] >= 0


@pytest.mark.parametrize(
    "error",
    [PartitionNotFoundError("events_2024_01"), InvalidPartitionConfigError("column mismatch")],
)
def test__run_maintenance__partition_error__logs_warning_not_exception(
    config: TablePartitionConfig, error: Exception
) -> None:
    # Arrange -- domain errors are operational failures: warning path, not the exception path
    service = _make_service(side_effect=error)
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger), pytest.raises(type(error)):
        maintainer.run_maintenance(config)

    mock_logger.exception.assert_not_called()
    warnings = [c for c in mock_logger.warning.call_args_list if "operational error" in c.args[0]]
    assert len(warnings) == 1
    assert warnings[0].kwargs["extra"]["error_type"] == type(error).__name__


@pytest.mark.parametrize("error", [ValueError("bad value"), TypeError("bad type"), RuntimeError("bad state")])
def test__run_maintenance__programming_error__logs_plain_warning_and_reraises(
    config: TablePartitionConfig, error: Exception
) -> None:
    # Arrange
    service = _make_service(side_effect=error)
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger), pytest.raises(type(error)):
        maintainer.run_maintenance(config)

    mock_logger.exception.assert_not_called()
    mock_logger.warning.assert_called_once()
    assert mock_logger.warning.call_args.args[0] == "Partition maintenance failed"
    assert mock_logger.warning.call_args.kwargs["extra"]["error"] == str(error)


@pytest.mark.parametrize("error", [KeyboardInterrupt("operator interrupted run"), SystemExit(1)])
def test__run_maintenance__system_signal__propagates_and_logs_interrupted(
    config: TablePartitionConfig, error: BaseException
) -> None:
    # Arrange -- the sync analog of a cancelled task: logged as an interruption and re-raised
    service = _make_service(side_effect=error)
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.sync.maintainer.logger", mock_logger):
        with pytest.raises(type(error)):
            maintainer.run_maintenance(config)

        messages = [c.args[0] for c in mock_logger.info.call_args_list]
        assert "Partition maintenance was interrupted by system signal" in messages
        mock_logger.warning.assert_not_called()
        mock_logger.exception.assert_not_called()


def test__run_maintenance__skip_flags__passed_through_to_service(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service()
    maintainer = PartitionMaintainer(service)

    # Act
    maintainer.run_maintenance(config, skip_create=True, skip_detach=True, skip_drop=True)

    # Assert
    service.maintain_lifecycle.assert_called_once_with(
        config, skip_create=True, skip_detach=True, skip_drop=True, continue_on_error=False
    )


def test__run_maintenance__continue_on_error__passed_through_to_service(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service()
    maintainer = PartitionMaintainer(service)

    # Act
    maintainer.run_maintenance(config, continue_on_error=True)

    # Assert
    service.maintain_lifecycle.assert_called_once_with(
        config, skip_create=False, skip_detach=False, skip_drop=False, continue_on_error=True
    )


def test__run_maintenance__duration__measures_the_elapsed_time(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service()
    maintainer = PartitionMaintainer(service)

    # Act
    with patch("pg_partsmith.sync.maintainer.time.perf_counter", side_effect=[10.0, 10.25]):
        result = maintainer.run_maintenance(config)

    # Assert
    assert result.duration_ms == 250


# ── run_maintenance_safe ────────────────────────────────────────────────────────


def test__run_maintenance_safe__service_succeeds__returns_the_result(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service(MaintenanceResult(created_count=1))
    maintainer = PartitionMaintainer(service)

    # Act
    result = maintainer.run_maintenance_safe(config, skip_drop=True)

    # Assert
    assert result.success is True
    assert result.created_count == 1
    service.maintain_lifecycle.assert_called_once_with(
        config, skip_create=False, skip_detach=False, skip_drop=True, continue_on_error=False
    )


def test__run_maintenance_safe__service_raises__returns_failure_result(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service(side_effect=RuntimeError("unexpected crash"))
    maintainer = PartitionMaintainer(service)

    # Act
    result = maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error == "RuntimeError: unexpected crash"
    assert result.duration_ms >= 0


@pytest.mark.parametrize("error", [KeyboardInterrupt("operator interrupted run"), SystemExit(1)])
def test__run_maintenance_safe__system_signal__returns_failure_result(
    config: TablePartitionConfig, error: BaseException
) -> None:
    # Arrange -- the sync analog of a cancelled task: captured into ``result.error``
    service = _make_service(side_effect=error)
    maintainer = PartitionMaintainer(service)

    # Act
    result = maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error is not None
    assert type(error).__name__ in result.error
    assert result.duration_ms >= 0


# ── maintain_partitions ─────────────────────────────────────────────────────────


def test__maintain_partitions__delegates_to_run_maintenance_safe_with_kwargs(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    expected = MaintenanceResult(created_count=1)
    maintainer = MagicMock()
    maintainer.run_maintenance_safe = MagicMock(return_value=expected)

    # Act
    result = maintain_partitions(maintainer, config, skip_create=True, continue_on_error=True)

    # Assert
    assert result is expected
    maintainer.run_maintenance_safe.assert_called_once_with(
        config,
        skip_create=True,
        skip_detach=False,
        skip_drop=False,
        continue_on_error=True,
    )


# NOTE: the aio test ``test__run_maintenance_safe__redis_watchdog_cancels_task__returns_failure_result`` has no sync
# analog: the sync Redis renewal watchdog cannot cancel the holder -- on renewal failure it only logs a warning and
# exits (covered in tests/unit/sync/test_locks.py).
