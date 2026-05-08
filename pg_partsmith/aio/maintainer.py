"""Partition maintenance orchestrator."""

import asyncio
import logging
import time

from pg_partsmith.aio.protocols import PartitionLifecycle
from pg_partsmith.entities import MaintenanceResult, TablePartitionConfig
from pg_partsmith.utils import format_duration_ms, qualify

logger = logging.getLogger(__name__)


class PartitionMaintainer:
    """Orchestrator for partition lifecycle maintenance.

    Wraps a lifecycle service with timing, logging, and error handling.
    Operational failures are logged and re-raised by ``run_maintenance``.
    Use ``run_maintenance_safe`` (or the ``maintain_partitions`` helper) when
    you need a scheduler-friendly API that always returns ``MaintenanceResult``.
    """

    def __init__(
        self,
        lifecycle_service: PartitionLifecycle,
    ) -> None:
        """Initialize maintainer.

        Args:
            lifecycle_service: Partition lifecycle service.
        """
        self._service = lifecycle_service

    async def run_maintenance(
        self,
        config: TablePartitionConfig,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
    ) -> MaintenanceResult:
        """Execute full partition lifecycle maintenance.

        Args:
            config: Table partition configuration.
            skip_create: Skip creating future partitions.
            skip_detach: Skip detaching old partitions.
            skip_drop: Skip dropping detached partitions.

        Returns:
            Maintenance result with counts and duration.

        Raises:
            asyncio.CancelledError: Propagated after being logged.
            Exception: Propagated after being logged.
        """
        start_time = time.perf_counter()
        qualified_table = qualify(config.db_schema, config.table_name)

        logger.info(
            "Starting partition maintenance",
            extra={
                "table_name": qualified_table,
                "skip_create": skip_create,
                "skip_detach": skip_detach,
                "skip_drop": skip_drop,
            },
        )

        try:
            result = await self._service.maintain_lifecycle(
                config, skip_create=skip_create, skip_detach=skip_detach, skip_drop=skip_drop
            )

            duration_ms = int((time.perf_counter() - start_time) * 1000)
            result = result.model_copy(update={"duration_ms": duration_ms})

            logger.info(
                "Partition maintenance completed successfully",
                extra={
                    "table_name": qualified_table,
                    "created_count": result.created_count,
                    "detached_count": result.detached_count,
                    "dropped_count": result.dropped_count,
                    "duration_ms": duration_ms,
                    "duration": format_duration_ms(duration_ms),
                },
            )
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            logger.info(
                "Partition maintenance was interrupted by system signal",
                extra={
                    "table_name": qualified_table,
                    "duration_ms": duration_ms,
                },
            )
            raise
        except (RuntimeError, ValueError) as e:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            logger.warning(
                "Partition maintenance failed (recoverable error)",
                extra={
                    "table_name": qualified_table,
                    "duration_ms": duration_ms,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise
        except Exception:
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            logger.exception(
                "Partition maintenance raised unexpected exception",
                extra={
                    "table_name": qualified_table,
                    "duration_ms": duration_ms,
                },
            )
            raise
        else:
            return result

    async def run_maintenance_safe(
        self,
        config: TablePartitionConfig,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
    ) -> MaintenanceResult:
        """Run maintenance and always return ``MaintenanceResult``, never raise.

        Scheduler-friendly wrapper around :meth:`run_maintenance`. Any exception
        — including ``asyncio.CancelledError`` — is captured and reported via
        ``result.error``; the ``duration_ms`` field always reflects the elapsed
        time even on failure.

        Args:
            config: Table partitioning configuration.
            skip_create: Skip creating future partitions.
            skip_detach: Skip detaching old partitions.
            skip_drop: Skip dropping detached partitions.

        Returns:
            ``MaintenanceResult`` with counts on success or ``error`` set on failure.
        """
        start_time = time.perf_counter()
        try:
            return await self.run_maintenance(
                config,
                skip_create=skip_create,
                skip_detach=skip_detach,
                skip_drop=skip_drop,
            )
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit) as e:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            msg = str(e)
            error = f"{type(e).__name__}: {msg}" if msg else type(e).__name__
            return MaintenanceResult(duration_ms=duration_ms, error=error)
        except Exception as e:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            msg = str(e)
            error = f"{type(e).__name__}: {msg}" if msg else type(e).__name__
            return MaintenanceResult(duration_ms=duration_ms, error=error)


async def maintain_partitions(
    maintainer: PartitionMaintainer,
    config: TablePartitionConfig,
    *,
    skip_create: bool = False,
    skip_detach: bool = False,
    skip_drop: bool = False,
) -> MaintenanceResult:
    """Convenience wrapper for scheduler integrations.

    Delegates to ``PartitionMaintainer.run_maintenance_safe``.
    """
    return await maintainer.run_maintenance_safe(
        config,
        skip_create=skip_create,
        skip_detach=skip_detach,
        skip_drop=skip_drop,
    )
