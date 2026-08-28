"""Partition creation domain service."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.protocols import (
    CompositeKeyRepository,
    SubpartitionRepository,
)
from pg_partsmith.constants import ATTACH_CONFLICT_SQLSTATES, DEFAULT_CONFLICT_MAX_RETRIES, MAX_IDENTIFIER_LENGTH
from pg_partsmith.entities import PartitionInfo, Period
from pg_partsmith.exceptions import (
    InvalidPartitionConfigError,
    PartitionAlreadyExistsError,
    UnsupportedCapabilityError,
)
from pg_partsmith.utils import is_default_partition_conflict, pg_sqlstate, qualify, split_qualified_name

from .base import BasePartitionService

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pg_partsmith.aio.hooks import PartitionLifecycleHooks
    from pg_partsmith.aio.protocols import PartitionMetadataProvider, PartitionRepository
    from pg_partsmith.aio.services.subpartitions import PartitionSubpartitionService
    from pg_partsmith.entities import TablePartitionConfig
    from pg_partsmith.protocols import PeriodCalculator

logger = logging.getLogger(__name__)


class PartitionCreationService(BasePartitionService):
    """Service for creating future partitions."""

    def __init__(
        self,
        repo: PartitionRepository,
        metadata: PartitionMetadataProvider,
        calculator: PeriodCalculator[Period],
        hooks: list[PartitionLifecycleHooks] | None = None,
        subpartitions: PartitionSubpartitionService | None = None,
    ) -> None:
        super().__init__(hooks=hooks)
        self._repo = repo
        self._metadata = metadata
        self._calculator = calculator
        self._subpartitions = subpartitions

    async def create_future_partitions(
        self,
        config: TablePartitionConfig,
        *,
        existing_partitions: list[PartitionInfo] | None = None,
    ) -> list[PartitionInfo]:
        """Create partitions for future periods.

        Returns:
            List of newly created and attached partitions.

        Raises:
            Exception: Any error during creation, listing, or from a hook (hook errors always propagate).
        """
        created: list[PartitionInfo] = []
        qualified_parent = qualify(config.db_schema, config.table_name)
        periods = self._calculator.next_periods(config.create_ahead_count)

        if existing_partitions is None:
            existing_partitions = await self._metadata.list_partitions(qualified_parent)

        existing_by_period = self._map_partitions_to_periods(existing_partitions)

        for period in periods:
            p_info = await self._ensure_partition_for_period(config, period, existing_by_period.get(period))
            if p_info:
                created.append(p_info)

        return created

    async def ensure_partition(self, config: TablePartitionConfig, period: Period) -> PartitionInfo | None:
        """Create and attach the partition for one specific period (idempotent).

        Unlike :meth:`create_future_partitions`, targets exactly ``period`` —
        useful for writers that must guarantee a partition exists before an
        insert (e.g. an hourly outbox buffer). Runs the same DEFAULT
        reconciliation and attach-race handling as the create-ahead path.

        For a subpartitioned config this also completes the branch's bucket set,
        so a writer that gets a successful return can rely on every row of that
        period having somewhere to land.

        Returns:
            The created partition, or None when it already existed (an existing
            detached partition is re-attached when ``auto_attach_after_create``).
        """
        created = await self.ensure_partitions(config, (period,))
        return created[0] if created else None

    async def ensure_partitions(
        self,
        config: TablePartitionConfig,
        periods: Iterable[Period],
    ) -> list[PartitionInfo]:
        """Create and attach partitions for an explicit set of periods (idempotent).

        Where :meth:`create_future_partitions` walks forward from the current
        period, this takes the periods from the caller — which is what backfill
        needs. Migrating onto this library usually means covering data that is
        already in the table, and the periods it lives in are not the ones
        create-ahead would produce::

            current = calculator.current_period()
            past = [calculator.period_before(current, n) for n in range(1, 53)]
            await service.ensure_partitions(config, past)

        The catalogue is read **once** for the whole batch rather than once per
        period, so backfilling a year costs one listing instead of fifty-two.

        Args:
            config: Table partitioning configuration.
            periods: Periods that must have a partition. Duplicates are ignored;
                order is preserved.

        Returns:
            The partitions created by this call, in the order the periods were
            given. Periods that already had one are absent from the list.
        """
        qualified_parent = qualify(config.db_schema, config.table_name)
        existing_partitions = await self._metadata.list_partitions(qualified_parent)
        existing_by_period = self._map_partitions_to_periods(existing_partitions)

        created: list[PartitionInfo] = []
        # dict.fromkeys de-duplicates while preserving the caller's order; a
        # repeated period would otherwise be looked up against a stale map.
        for period in dict.fromkeys(periods):
            info = await self._ensure_period_with_subtree(config, period, existing_by_period.get(period))
            if info is not None:
                created.append(info)

        return created

    async def _ensure_period_with_subtree(
        self,
        config: TablePartitionConfig,
        period: Period,
        existing: PartitionInfo | None,
    ) -> PartitionInfo | None:
        """Ensure one period's partition *and* its bucket set.

        Kept separate from :meth:`_ensure_partition_for_period` because the
        create-ahead path deliberately does not converge subtrees one period at
        a time — maintenance reconciles the whole table once per run, from a
        single tree query, which is far cheaper than one query per period.
        """
        created = await self._ensure_partition_for_period(config, period, existing)

        if created is None and existing is not None and config.subpartition is not None:
            # The branch was already there; a caller that targets a period needs
            # its buckets complete, not merely the branch present.
            await self._converge_existing_branch(config, existing.name)

        return created

    def _map_partitions_to_periods(self, partitions: list[PartitionInfo]) -> dict[Period, PartitionInfo]:
        """Map partitions to periods, handling ambiguities."""
        candidates: dict[Period, list[PartitionInfo]] = {}

        for p in partitions:
            if p.is_default:
                continue
            try:
                _, relname = split_qualified_name(p.name)
                period = self._calculator.parse_partition_name(relname)
                if period is not None:
                    candidates.setdefault(period, []).append(p)
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                raise
            except (ValueError, KeyError, TypeError) as exc:
                logger.debug(
                    "Failed to parse partition name; skipping dedupe",
                    extra={
                        "partition_name": p.name,
                        "error": str(exc),
                    },
                )

        existing_by_period: dict[Period, PartitionInfo] = {}
        for period, parts in candidates.items():
            if len(parts) == 1:
                existing_by_period[period] = parts[0]
                continue

            attached = [p for p in parts if p.is_attached]
            pool = attached or parts
            chosen = sorted(pool, key=lambda x: x.name)[0]
            existing_by_period[period] = chosen

            names = ", ".join(sorted(p.name for p in parts))
            logger.warning(
                "Multiple partitions match period; using one of them",
                extra={
                    "period": str(period),
                    "chosen": chosen.name,
                    "all_matches": names,
                },
            )

        return existing_by_period

    async def _ensure_partition_for_period(
        self,
        config: TablePartitionConfig,
        period: Period,
        existing: PartitionInfo | None,
    ) -> PartitionInfo | None:
        """Ensure a partition exists and is attached for a specific period."""
        partition_name, from_value, to_value = self._get_partition_metadata(config, period)

        if existing is not None:
            await self._handle_existing_partition(config, existing, from_value, to_value)
            return None

        # Hooks: before create
        await self._run_hooks(
            lambda h: h.before_create(config, partition_name, from_value, to_value),
            "before_create",
            partition_name=partition_name,
        )

        # Creation and optional attachment
        partition_info = await self._create_and_attach_partition(config, partition_name, from_value, to_value)

        if partition_info:
            # Hooks: after create
            await self._run_hooks(
                lambda h: h.after_create(config, partition_info),
                "after_create",
                partition_name=partition_name,
            )

        return partition_info

    def _get_partition_metadata(self, config: TablePartitionConfig, period: Period) -> tuple[str, str, str]:
        """Prepare partition name and boundaries, validating limits."""
        partition_relname = self._calculator.format_partition_name(config.table_name, period)
        partition_name = qualify(config.db_schema, partition_relname)
        from_value, to_value = self._calculator.get_boundaries(period)

        if len(partition_relname.encode("utf-8")) > MAX_IDENTIFIER_LENGTH:
            msg = (
                f"Generated partition name '{partition_relname}' exceeds PostgreSQL's "
                f"{MAX_IDENTIFIER_LENGTH}-byte identifier limit"
            )
            raise InvalidPartitionConfigError(msg)

        return partition_name, from_value, to_value

    async def _attach_with_reconcile(
        self,
        config: TablePartitionConfig,
        partition_name: str,
        from_value: str,
        to_value: str,
    ) -> None:
        """Attach partition with automatic DEFAULT reconciliation.

        If the attach ultimately fails after rows were reconciled out of the
        DEFAULT partition, the moved rows are returned to DEFAULT (best
        effort) so they do not end up stranded in a table that is invisible
        through the parent.
        """
        qualified_parent = qualify(config.db_schema, config.table_name)
        reconciled_from: str | None = None

        for attempt in range(1, DEFAULT_CONFLICT_MAX_RETRIES + 1):
            try:
                await self._attach(config, qualified_parent, partition_name, from_value, to_value)
            except asyncio.CancelledError:
                # Shielded so the compensating move-back completes even mid-cancellation.
                await asyncio.shield(
                    self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                )
                raise
            except (OSError, TimeoutError):
                await self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                raise
            except SQLAlchemyError as e:
                if not is_default_partition_conflict(e):
                    await self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                    raise

                if attempt == DEFAULT_CONFLICT_MAX_RETRIES:
                    logger.exception(
                        "Failed to attach after reconciliation retries",
                        extra={
                            "partition_name": partition_name,
                            "attempts": attempt,
                        },
                    )
                    await self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                    raise

                # Get DEFAULT partition
                default_partition = await self._metadata.get_default_partition(qualified_parent)
                if not default_partition:
                    logger.warning(
                        "DEFAULT conflict detected but no DEFAULT partition found",
                        extra={"partition_name": partition_name},
                    )
                    await self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                    raise

                # Reconcile conflicting rows
                logger.info(
                    "Reconciling DEFAULT partition before attach",
                    extra={
                        "partition_name": partition_name,
                        "default_partition": default_partition.name,
                        "attempt": attempt,
                    },
                )

                moved_count = await self._move_default_rows(
                    config,
                    source=default_partition.name,
                    target=partition_name,
                    from_value=from_value,
                    to_value=to_value,
                )
                if moved_count:
                    reconciled_from = default_partition.name

                logger.info(
                    "Reconciliation completed",
                    extra={
                        "partition_name": partition_name,
                        "moved_rows": moved_count,
                    },
                )
            else:
                return  # Success

    async def _attach(
        self,
        config: TablePartitionConfig,
        qualified_parent: str,
        partition_name: str,
        from_value: str,
        to_value: str,
    ) -> None:
        """Attach a partition, padding the bound when the key is composite.

        A single-column key takes the long-standing path untouched; only a
        multi-column one needs the trailing MINVALUE columns, and it is only
        then that the repository has to support them.
        """
        if config.key_arity == 1:
            await self._repo.attach_partition(qualified_parent, partition_name, from_value, to_value)
            return

        repo = self._repo
        if not isinstance(repo, CompositeKeyRepository):
            raise UnsupportedCapabilityError(
                f"Repository {type(repo).__name__}", "composite partition keys", "CompositeKeyRepository"
            )
        await repo.attach_composite_partition(
            qualified_parent, partition_name, from_value, to_value, key_arity=config.key_arity
        )

    async def _move_default_rows(
        self,
        config: TablePartitionConfig,
        *,
        source: str,
        target: str,
        from_value: str,
        to_value: str,
    ) -> int:
        """Move rows between a DEFAULT partition and one period's partition.

        The trailing key columns are only passed for a composite key, so a
        repository written against the single-column signature keeps serving
        every config it could already serve.
        """
        if config.key_arity == 1:
            return await self._repo.reconcile_default_rows(
                default_partition_name=source,
                target_partition_name=target,
                partition_column=config.partition_column,
                from_value=from_value,
                to_value=to_value,
            )

        return await self._repo.reconcile_default_rows(
            default_partition_name=source,
            target_partition_name=target,
            partition_column=config.partition_column,
            trailing_columns=config.trailing_partition_columns,
            from_value=from_value,
            to_value=to_value,
        )

    async def _restore_reconciled_rows(
        self,
        default_partition_name: str | None,
        config: TablePartitionConfig,
        partition_name: str,
        from_value: str,
        to_value: str,
    ) -> None:
        """Return previously reconciled rows to the DEFAULT partition (best effort).

        Reconciliation commits independently of ATTACH, so a final attach
        failure would otherwise leave the moved rows in a standalone table
        that no query against the parent can see. Failures here are logged,
        never raised, so the original attach error stays the primary one.
        """
        if default_partition_name is None:
            return
        try:
            restored = await self._move_default_rows(
                config,
                source=partition_name,
                target=default_partition_name,
                from_value=from_value,
                to_value=to_value,
            )
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            logger.exception(
                "Failed to return reconciled rows to DEFAULT partition; rows remain in the detached table",
                extra={"partition_name": partition_name, "default_partition": default_partition_name},
            )
        else:
            logger.warning(
                "Attach failed after reconciliation; returned rows to DEFAULT partition",
                extra={
                    "partition_name": partition_name,
                    "default_partition": default_partition_name,
                    "restored_rows": restored,
                },
            )

    async def _handle_existing_partition(
        self, config: TablePartitionConfig, existing: PartitionInfo, from_value: str, to_value: str
    ) -> None:
        """Handle case where partition already exists in metadata."""
        if not (config.auto_attach_after_create and not existing.is_attached):
            return

        # Converge before it becomes reachable, not after. Attaching first
        # makes the branch live for row routing while its child set may still
        # be short of what the spec asks for -- and if the process stops in
        # that window, it stays live and rejecting. A detached branch is
        # introspectable on its own, so there is no reason to attach first.
        await self._converge_existing_branch(config, existing.name)
        await self._attach_tolerating_lost_race(config, existing.name, from_value, to_value)

    async def _is_attach_conflict_benign(
        self, qualified_parent: str, partition_name: str, exc: SQLAlchemyError
    ) -> bool:
        """A conflict SQLSTATE proves a lost race only if the postcondition holds.

        42809 (wrong_object_type) also fires for typed tables, inheritance
        children, and attachments to a *different* parent — verify the
        partition is actually attached to our parent instead of trusting the
        error code alone.
        """
        if pg_sqlstate(exc) not in ATTACH_CONFLICT_SQLSTATES:
            return False
        return await self._metadata.is_partition_attached(qualified_parent, partition_name)

    async def _create_and_attach_partition(
        self, config: TablePartitionConfig, partition_name: str, from_value: str, to_value: str
    ) -> PartitionInfo | None:
        """Create the partition (or the whole branch) and attach it to the parent.

        With a subpartition spec the partition is a branch: it is created
        detached, filled with its buckets, and attached last, so the root never
        sees a branch that cannot route part of its keyspace.
        """
        try:
            partition_info = await self._create_partition_relation(config, partition_name, from_value, to_value)
        except PartitionAlreadyExistsError:
            # The relation exists but list_partitions did not report it, so it
            # is not attached: almost always a previous run interrupted between
            # creating a branch and attaching it. Finish that work rather than
            # leaving an invisible table behind forever.
            await self._converge_existing_branch(config, partition_name)
            if config.auto_attach_after_create:
                await self._attach_tolerating_lost_race(config, partition_name, from_value, to_value)
            return None

        if config.subpartition is not None:
            await self._subpartition_service().build_new_branch(config, partition_name)

        if config.auto_attach_after_create:
            await self._attach_tolerating_lost_race(config, partition_name, from_value, to_value)
            partition_info = partition_info.model_copy(update={"is_attached": True})

        return partition_info

    async def _create_partition_relation(
        self, config: TablePartitionConfig, partition_name: str, from_value: str, to_value: str
    ) -> PartitionInfo:
        """Create the detached relation backing one period."""
        # Every capability this config needs is checked before the first
        # statement. Discovering a missing one after the relation exists would
        # leave an unmarked detached table behind on every tick, and unmarked
        # tables are never collected by orphan cleanup.
        self._require_capabilities(config)

        if config.subpartition is None:
            return await self._repo.create_partition(config, partition_name, from_value, to_value)

        repo = self._repo
        assert isinstance(repo, SubpartitionRepository)  # guaranteed by _require_capabilities
        return await repo.create_branch(config, partition_name, from_value, to_value, config.subpartition)

    def _require_capabilities(self, config: TablePartitionConfig) -> None:
        """Refuse a wiring that cannot serve this config, before any DDL runs."""
        if config.subpartition is not None:
            self._subpartition_service()
            if not isinstance(self._repo, SubpartitionRepository):
                raise UnsupportedCapabilityError(
                    f"Repository {type(self._repo).__name__}", "subpartitioning", "SubpartitionRepository"
                )

        if config.key_arity > 1 and not isinstance(self._repo, CompositeKeyRepository):
            raise UnsupportedCapabilityError(
                f"Repository {type(self._repo).__name__}", "composite partition keys", "CompositeKeyRepository"
            )

    async def _converge_existing_branch(self, config: TablePartitionConfig, partition_name: str) -> None:
        """Complete the subtree of a branch left half-built by an earlier run."""
        if config.subpartition is None:
            return
        result = await self._subpartition_service().converge_branch(config, partition_name)
        if result.created_count:
            logger.info(
                "Completed the subtree of a partition left behind by an earlier run",
                extra={"partition_name": partition_name, "subpartition_count": result.created_count},
            )

    def _subpartition_service(self) -> PartitionSubpartitionService:
        """Return the wired subpartition service, or explain that there is none."""
        if self._subpartitions is None:
            raise UnsupportedCapabilityError(
                f"Creation service {type(self).__name__}",
                "subpartitioning",
                "a PartitionSubpartitionService collaborator",
            )
        return self._subpartitions

    async def _attach_tolerating_lost_race(
        self,
        config: TablePartitionConfig,
        partition_name: str,
        from_value: str,
        to_value: str,
    ) -> None:
        """Attach with DEFAULT reconciliation, tolerating a benign lost attach race."""
        qualified_parent = qualify(config.db_schema, config.table_name)
        try:
            await self._attach_with_reconcile(config, partition_name, from_value, to_value)
        except SQLAlchemyError as e:
            if not await self._is_attach_conflict_benign(qualified_parent, partition_name, e):
                raise
            logger.debug(
                "Partition already attached (race with another worker)",
                extra={"partition_name": partition_name, "sqlstate": pg_sqlstate(e)},
            )
