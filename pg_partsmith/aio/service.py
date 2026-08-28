"""Domain service for partition lifecycle management."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, cast

from pg_partsmith.aio.protocols import (
    LockManager,
    NestedPartitionMetadata,
    PartitionMetadataProvider,
    PartitionRepository,
    SubpartitionRepository,
)
from pg_partsmith.entities import (
    MaintenanceIssue,
    MaintenanceIssueStep,
    MaintenanceResult,
    PartitionInfo,
    Period,
    TablePartitionConfig,
)
from pg_partsmith.subpartition_plan import SubpartitionReconcileResult, to_maintenance_issue
from pg_partsmith.utils import describe_exception, qualify, validate_timezone_alignment

from .services.creation import PartitionCreationService
from .services.deletion import PartitionDeletionService
from .services.detachment import PartitionDetachmentService
from .services.pruning import PartitionPruningService
from .services.subpartitions import PartitionSubpartitionService
from .services.validation import PartitionValidationService

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

    from pg_partsmith.aio.hooks import PartitionLifecycleHooks
    from pg_partsmith.protocols import PeriodCalculator

logger = logging.getLogger(__name__)


class PartitionLifecycleService:
    """Service for managing the full partition lifecycle.

    Orchestrates partition creation, detachment, and deletion by delegating
    to specialized component services.
    """

    def __init__(
        self,
        repo: PartitionRepository,
        metadata: PartitionMetadataProvider,
        locks: LockManager,
        period_calculator: PeriodCalculator[Period],
        hooks: list[PartitionLifecycleHooks] | None = None,
    ) -> None:
        """Initialize the partition lifecycle service.

        Args:
            repo: DDL operations on partitions (create / attach / detach / drop).
            metadata: Read-only access to PostgreSQL catalog data.
            locks: Distributed lock manager preventing concurrent maintenance runs.
            period_calculator: Strategy for determining partition names and boundaries.
            hooks: Optional list of lifecycle hooks called around each step.
        """
        validate_timezone_alignment(repo, period_calculator)

        self._locks = locks
        self._metadata = metadata

        # Component services
        self._validation_service = PartitionValidationService(metadata)
        # Subpartitioning is optional, so the collaborators are typed loosely
        # here and checked against the nested protocols only when a config
        # actually asks for it — a flat setup with a custom repository or
        # metadata provider keeps working unchanged.
        self._subpartition_service = PartitionSubpartitionService(
            cast("SubpartitionRepository", repo),
            cast("NestedPartitionMetadata", metadata),
        )
        self._creation_service = PartitionCreationService(
            repo, metadata, period_calculator, hooks, subpartitions=self._subpartition_service
        )
        self._pruning_service = PartitionPruningService(metadata, period_calculator)
        self._detachment_service = PartitionDetachmentService(repo, hooks)
        self._deletion_service = PartitionDeletionService(repo, hooks)

    async def create_future_partitions(self, config: TablePartitionConfig) -> list[PartitionInfo]:
        """Create partitions for future periods.

        Ensures partitions exist for the next ``config.create_ahead_count`` periods
        starting from the current period (inclusive). Idempotent: existing partitions
        are skipped.

        Args:
            config: Table partitioning configuration.

        Returns:
            List of newly created partitions (empty if all already existed).

        Raises:
            PartitionAlreadyExistsError: If a partition exists with conflicting boundaries.
            InvalidPartitionConfigError: If ``config`` is incompatible with the parent table.
        """
        return await self._creation_service.create_future_partitions(config)

    async def ensure_partition(self, config: TablePartitionConfig, period: Period) -> PartitionInfo | None:
        """Create and attach the partition for one specific period (idempotent).

        Unlike :meth:`create_future_partitions`, targets exactly ``period`` —
        useful for writers that must guarantee a partition exists before an
        insert (e.g. an hourly outbox buffer). Runs the same DEFAULT
        reconciliation and attach-race handling as the create-ahead path.

        Args:
            config: Table partitioning configuration.
            period: The period the partition must cover.

        Returns:
            The created partition, or None when it already existed (an existing
            detached partition is re-attached when ``auto_attach_after_create``).
        """
        return await self._creation_service.ensure_partition(config, period)

    async def ensure_partitions(
        self,
        config: TablePartitionConfig,
        periods: Iterable[Period],
    ) -> list[PartitionInfo]:
        """Create and attach partitions for an explicit set of periods (idempotent).

        The backfill counterpart of :meth:`create_future_partitions`: the caller
        chooses the periods, so data that already sits in the table can be given
        partitions without waiting for create-ahead to reach it.

        Args:
            config: Table partitioning configuration.
            periods: Periods that must have a partition. Duplicates are ignored;
                order is preserved.

        Returns:
            The partitions created by this call; periods that already had one
            are absent from the list.
        """
        return await self._creation_service.ensure_partitions(config, periods)

    async def get_partitions_for_pruning(self, config: TablePartitionConfig) -> list[PartitionInfo]:
        """Return partitions older than ``config.retention_count`` periods.

        Args:
            config: Table partitioning configuration.

        Returns:
            Partitions that are eligible for detach + drop, sorted oldest first.
        """
        return await self._pruning_service.get_partitions_for_pruning(config)

    async def detach_old_partitions(
        self,
        table_name: str,
        partitions: list[PartitionInfo],
    ) -> list[str]:
        """Detach attached partitions from their parent table.

        Args:
            table_name: Qualified parent table name.
            partitions: Attached partitions to detach.

        Returns:
            Names of successfully detached partitions.

        Raises:
            PartitionDetachInProgressError: If a concurrent detach is in progress.
        """
        return await self._detachment_service.detach_old_partitions(table_name, partitions)

    async def drop_detached_partitions(
        self,
        table_name: str,
        partition_names: list[str],
    ) -> int:
        """Drop previously detached, marker-tagged partitions.

        Attached partitions are skipped with a warning (they raise
        ``PartitionAttachedError`` internally). Unmanaged tables are refused
        unless the underlying repository is configured otherwise.

        Args:
            table_name: Qualified parent table name (used for hook context).
            partition_names: Names of partitions to drop.

        Returns:
            Number of partitions actually dropped.
        """
        return await self._deletion_service.drop_detached_partitions(table_name, partition_names)

    async def reconcile_subpartitions(
        self,
        config: TablePartitionConfig,
        *,
        exclude: Collection[str] = (),
    ) -> SubpartitionReconcileResult:
        """Converge the subtree of every attached partition towards the config.

        Idempotent and safe to call on its own: it creates only the buckets a
        branch is genuinely missing, and reports rather than "repairs" any
        branch whose shape it cannot converge without risk.

        Args:
            config: Table partitioning configuration. Without a subpartition
                spec this is a no-op returning an empty result.
            exclude: Schema-qualified partition names to skip.

        Returns:
            The subpartitions created and the divergences left alone.
        """
        return await self._subpartition_service.reconcile(config, exclude=exclude)

    async def maintain_lifecycle(
        self,
        config: TablePartitionConfig,
        *,
        skip_create: bool = False,
        skip_detach: bool = False,
        skip_drop: bool = False,
        continue_on_error: bool = False,
    ) -> MaintenanceResult:
        """Run create + detach + drop in a single locked maintenance window.

        The whole sequence runs under a single distributed lock acquired through
        the configured :class:`LockManager`, so concurrent maintainers do not
        race on the same parent table.

        Args:
            config: Table partitioning configuration.
            skip_create: Skip the create-ahead step.
            skip_detach: Skip detaching old partitions (orphans are still dropped).
            skip_drop: Skip dropping detached partitions.
            continue_on_error: Isolate step failures instead of aborting the run:
                a failed create still prunes (which may free the space create
                needs), a failed detach still drops existing orphans. Failures
                are collected into ``MaintenanceResult.issues``. Validation and
                lock failures are always fatal.

        Subpartitioned configs additionally reconcile each branch's bucket set
        between create and detach; branches whose shape cannot be converged
        safely are reported through ``MaintenanceResult.issues`` regardless of
        ``continue_on_error``, since leaving them silent would hide writes that
        PostgreSQL is rejecting.

        Returns:
            ``MaintenanceResult`` with the per-step counters; ``error`` is unset
            because exceptions propagate from this method (the maintainer is
            responsible for catching them).

        Raises:
            LockAcquisitionError: If the table-level maintenance lock is unavailable.
            InvalidPartitionConfigError: If ``config`` does not match the parent table.
        """
        qualified_parent = qualify(config.db_schema, config.table_name)

        created_count = 0
        repaired_count = 0
        detached_count = 0
        dropped_count = 0
        issues: list[MaintenanceIssue] = []

        def _record_issue(step: MaintenanceIssueStep, exc: Exception) -> None:
            error = describe_exception(exc)
            issues.append(MaintenanceIssue(step=step, error=error))
            logger.warning(
                "Maintenance step failed; continuing with the remaining steps",
                extra={"table_name": qualified_parent, "step": step.value, "error": error},
            )

        async with self._locks.acquire_lock(qualified_parent):
            await self._validation_service.validate_config(config)

            # Optimization: fetch all partitions once
            all_partitions = await self._metadata.list_partitions(qualified_parent)

            if not skip_create:
                try:
                    created = await self._creation_service.create_future_partitions(
                        config, existing_partitions=all_partitions
                    )
                except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    raise
                except Exception as e:
                    if not continue_on_error:
                        raise
                    _record_issue(MaintenanceIssueStep.CREATE, e)
                else:
                    created_count = len(created)
                    if created:
                        all_partitions.extend(created)

            partitions_to_prune = await self._pruning_service.identify_partitions_to_prune(config, all_partitions)

            # Reconcile before pruning so a branch that is on its way out is not
            # repaired just to be dropped moments later.
            if config.subpartition is not None:
                try:
                    reconciled = await self._subpartition_service.reconcile(
                        config, exclude={p.name for p in partitions_to_prune}
                    )
                except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    raise
                except Exception as e:
                    if not continue_on_error:
                        raise
                    _record_issue(MaintenanceIssueStep.RECONCILE, e)
                else:
                    repaired_count = reconciled.created_count
                    issues.extend(to_maintenance_issue(f) for f in reconciled.findings if f.is_actionable)

            if not partitions_to_prune:
                return MaintenanceResult(
                    created_count=created_count,
                    repaired_count=repaired_count,
                    issues=tuple(issues),
                )

            attached_to_detach = [p for p in partitions_to_prune if p.is_attached]
            orphan_names = [p.name for p in partitions_to_prune if not p.is_attached]

            names_to_drop = orphan_names
            if not skip_detach:
                try:
                    detached_names = await self._detachment_service.detach_old_partitions(
                        qualified_parent,
                        attached_to_detach,
                    )
                except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    raise
                except Exception as e:
                    if not continue_on_error:
                        raise
                    # Partially detached partitions carry the orphan marker and
                    # are collected as orphans on the next run.
                    _record_issue(MaintenanceIssueStep.DETACH, e)
                else:
                    detached_count = len(detached_names)
                    names_to_drop = orphan_names + detached_names

            if not skip_drop and names_to_drop:
                try:
                    dropped_count = await self._deletion_service.drop_detached_partitions(
                        qualified_parent,
                        names_to_drop,
                    )
                except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    raise
                except Exception as e:
                    if not continue_on_error:
                        raise
                    _record_issue(MaintenanceIssueStep.DROP, e)

        return MaintenanceResult(
            created_count=created_count,
            repaired_count=repaired_count,
            detached_count=detached_count,
            dropped_count=dropped_count,
            issues=tuple(issues),
        )
