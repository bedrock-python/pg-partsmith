"""Unit tests for the aio ``PartitionMaintainer`` orchestrator and the ``maintain_partitions`` helper."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_partsmith.aio import maintain_partitions
from pg_partsmith.aio.lock.redis import RedisDistributedLockManager
from pg_partsmith.aio.maintainer import PartitionMaintainer
from pg_partsmith.entities import (
    MaintenanceIssue,
    MaintenanceIssueStep,
    MaintenanceResult,
    PartitionGranularity,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import InvalidPartitionConfigError, LockAcquisitionError, PartitionNotFoundError


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
        mock_service.maintain_lifecycle = AsyncMock(side_effect=side_effect)
    else:
        mock_service.maintain_lifecycle = AsyncMock(return_value=result or MaintenanceResult())
    return mock_service


# ── run_maintenance ─────────────────────────────────────────────────────────────


async def test__run_maintenance__service_succeeds__returns_result_with_counts_and_duration(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(
        MaintenanceResult(created_count=2, repaired_count=1, attached_count=1, detached_count=1, dropped_count=1)
    )
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act
    with patch("pg_partsmith.aio.maintainer.logger", mock_logger):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success is True
    assert (result.created_count, result.repaired_count, result.attached_count) == (2, 1, 1)
    assert (result.detached_count, result.dropped_count) == (1, 1)
    assert result.duration_ms >= 0
    service.maintain_lifecycle.assert_awaited_once_with(
        config, skip_create=False, skip_detach=False, skip_drop=False, continue_on_error=False
    )
    started, completed = mock_logger.info.call_args_list
    assert started.args[0] == "Starting partition maintenance"
    assert started.kwargs["extra"]["table_name"] == "public.events"
    assert completed.args[0] == "Partition maintenance completed successfully"
    assert completed.kwargs["extra"]["created_count"] == 2
    assert "duration" in completed.kwargs["extra"]


async def test__run_maintenance__issues_on_the_result__are_kept(config: TablePartitionConfig) -> None:
    # Arrange
    issue = MaintenanceIssue(step=MaintenanceIssueStep.DROP, error="SQLAlchemyError: boom", partition_name="x")
    service = _make_service(MaintenanceResult(issues=(issue,)))
    maintainer = PartitionMaintainer(service)

    # Act
    result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success is True
    assert result.issues == (issue,)


async def test__run_maintenance__service_returns_failure_result__propagates_result(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(MaintenanceResult(error="DB unreachable"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act
    with patch("pg_partsmith.aio.maintainer.logger", mock_logger):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.success is False
    assert result.error == "DB unreachable"
    mock_logger.info.assert_called()


async def test__run_maintenance__service_raises_unexpected_exception__reraises_and_logs_traceback(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    class _CrashError(Exception):
        pass

    service = _make_service(side_effect=_CrashError("unexpected crash"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.aio.maintainer.logger", mock_logger), pytest.raises(_CrashError, match="unexpected crash"):
        await maintainer.run_maintenance(config)

    mock_logger.exception.assert_called_once()
    assert mock_logger.exception.call_args.kwargs["extra"]["table_name"] == "public.events"
    mock_logger.warning.assert_not_called()


async def test__run_maintenance__lock_acquisition_error__logs_warning_not_exception(
    config: TablePartitionConfig,
) -> None:
    # Arrange -- lock contention is a routine operational failure, not an unexpected one
    service = _make_service(side_effect=LockAcquisitionError("events", "advisory lock unavailable"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.aio.maintainer.logger", mock_logger), pytest.raises(LockAcquisitionError):
        await maintainer.run_maintenance(config)

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
async def test__run_maintenance__partition_error__logs_warning_not_exception(
    config: TablePartitionConfig, error: Exception
) -> None:
    # Arrange -- domain errors are operational failures: warning path, not the exception path
    service = _make_service(side_effect=error)
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.aio.maintainer.logger", mock_logger), pytest.raises(type(error)):
        await maintainer.run_maintenance(config)

    mock_logger.exception.assert_not_called()
    warnings = [c for c in mock_logger.warning.call_args_list if "operational error" in c.args[0]]
    assert len(warnings) == 1
    assert warnings[0].kwargs["extra"]["error_type"] == type(error).__name__


@pytest.mark.parametrize("error", [ValueError("bad value"), TypeError("bad type"), RuntimeError("bad state")])
async def test__run_maintenance__programming_error__logs_plain_warning_and_reraises(
    config: TablePartitionConfig, error: Exception
) -> None:
    # Arrange
    service = _make_service(side_effect=error)
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.aio.maintainer.logger", mock_logger), pytest.raises(type(error)):
        await maintainer.run_maintenance(config)

    mock_logger.exception.assert_not_called()
    mock_logger.warning.assert_called_once()
    assert mock_logger.warning.call_args.args[0] == "Partition maintenance failed"
    assert mock_logger.warning.call_args.kwargs["extra"]["error"] == str(error)


async def test__run_maintenance__cancelled_error__propagates_and_logs_interrupted(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service(side_effect=asyncio.CancelledError("lock watchdog cancelled run"))
    mock_logger = MagicMock()
    maintainer = PartitionMaintainer(service)

    # Act / Assert
    with patch("pg_partsmith.aio.maintainer.logger", mock_logger):
        with pytest.raises(asyncio.CancelledError, match="lock watchdog cancelled run"):
            await maintainer.run_maintenance(config)

        messages = [c.args[0] for c in mock_logger.info.call_args_list]
        assert "Partition maintenance was interrupted by system signal" in messages
        mock_logger.warning.assert_not_called()
        mock_logger.exception.assert_not_called()


async def test__run_maintenance__skip_flags__passed_through_to_service(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service()
    maintainer = PartitionMaintainer(service)

    # Act
    await maintainer.run_maintenance(config, skip_create=True, skip_detach=True, skip_drop=True)

    # Assert
    service.maintain_lifecycle.assert_awaited_once_with(
        config, skip_create=True, skip_detach=True, skip_drop=True, continue_on_error=False
    )


async def test__run_maintenance__continue_on_error__passed_through_to_service(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    service = _make_service()
    maintainer = PartitionMaintainer(service)

    # Act
    await maintainer.run_maintenance(config, continue_on_error=True)

    # Assert
    service.maintain_lifecycle.assert_awaited_once_with(
        config, skip_create=False, skip_detach=False, skip_drop=False, continue_on_error=True
    )


async def test__run_maintenance__duration__measures_the_elapsed_time(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service()
    maintainer = PartitionMaintainer(service)

    # Act
    with patch("pg_partsmith.aio.maintainer.time.perf_counter", side_effect=[10.0, 10.25]):
        result = await maintainer.run_maintenance(config)

    # Assert
    assert result.duration_ms == 250


# ── run_maintenance_safe ────────────────────────────────────────────────────────


async def test__run_maintenance_safe__service_succeeds__returns_the_result(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service(MaintenanceResult(created_count=1))
    maintainer = PartitionMaintainer(service)

    # Act
    result = await maintainer.run_maintenance_safe(config, skip_drop=True)

    # Assert
    assert result.success is True
    assert result.created_count == 1
    service.maintain_lifecycle.assert_awaited_once_with(
        config, skip_create=False, skip_detach=False, skip_drop=True, continue_on_error=False
    )


async def test__run_maintenance_safe__service_raises__returns_failure_result(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service(side_effect=RuntimeError("unexpected crash"))
    maintainer = PartitionMaintainer(service)

    # Act
    result = await maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error == "RuntimeError: unexpected crash"
    assert result.duration_ms >= 0


async def test__run_maintenance_safe__cancelled_error__returns_failure_result(config: TablePartitionConfig) -> None:
    # Arrange
    service = _make_service(side_effect=asyncio.CancelledError("lock watchdog cancelled run"))
    maintainer = PartitionMaintainer(service)

    # Act
    result = await maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error is not None
    assert "CancelledError" in result.error
    assert result.duration_ms >= 0


@pytest.mark.parametrize("error", [KeyboardInterrupt("operator interrupted"), SystemExit(1)])
async def test__run_maintenance_safe__system_signal__returns_failure_result(
    config: TablePartitionConfig, error: BaseException
) -> None:
    # Arrange
    service = _make_service(side_effect=error)
    maintainer = PartitionMaintainer(service)

    # Act
    result = await maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error is not None
    assert type(error).__name__ in result.error


async def test__run_maintenance_safe__redis_watchdog_cancels_task__returns_failure_result(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    redis_client = MagicMock()
    redis_client.set = AsyncMock(return_value=True)
    unlock_script = AsyncMock(return_value=1)
    renew_script = AsyncMock(return_value=0)  # returns 0 -> lock lost
    redis_client.register_script.side_effect = [unlock_script, renew_script]

    with patch("pg_partsmith.aio.lock.redis._redis_available", True):
        lock_manager = RedisDistributedLockManager(redis_client, ttl_seconds=3)

    class _ServiceWithRedisLock:
        async def maintain_lifecycle(self, *_args: object, **_kwargs: object) -> MaintenanceResult:
            async with lock_manager.acquire_lock(config.table_name):
                await asyncio.sleep(1.2)
            return MaintenanceResult()

    maintainer = PartitionMaintainer(_ServiceWithRedisLock())

    # Act
    result = await maintainer.run_maintenance_safe(config)

    # Assert
    assert result.success is False
    assert result.error is not None
    assert "CancelledError" in result.error
    assert unlock_script.called


# ── maintain_partitions ─────────────────────────────────────────────────────────


async def test__maintain_partitions__delegates_to_run_maintenance_safe_with_kwargs(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    expected = MaintenanceResult(created_count=1)
    maintainer = MagicMock()
    maintainer.run_maintenance_safe = AsyncMock(return_value=expected)

    # Act
    result = await maintain_partitions(maintainer, config, skip_create=True, continue_on_error=True)

    # Assert
    assert result is expected
    maintainer.run_maintenance_safe.assert_awaited_once_with(
        config,
        skip_create=True,
        skip_detach=False,
        skip_drop=False,
        continue_on_error=True,
    )
