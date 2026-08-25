"""Partition creation domain service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.entities import PartitionInfo, Period
from pg_partsmith.exceptions import InvalidPartitionConfigError, PartitionAlreadyExistsError
from pg_partsmith.utils import pg_sqlstate, qualify, split_qualified_name

from .base import BasePartitionService

if TYPE_CHECKING:
    from pg_partsmith.entities import TablePartitionConfig
    from pg_partsmith.protocols import PeriodCalculator
    from pg_partsmith.sync.hooks import PartitionLifecycleHooks
    from pg_partsmith.sync.protocols import PartitionMetadataProvider, PartitionRepository

logger = logging.getLogger(__name__)

_PG_IDENTIFIER_MAX_BYTES = 63
# SQLSTATEs that indicate partition already attached or duplicate (race with another worker).
# 42809 (wrong_object_type) is what PostgreSQL raises for "X is already a partition".
_ATTACH_CONFLICT_SQLSTATES = frozenset({"42P07", "42710", "42809"})
_PG_CHECK_VIOLATION = "23514"
_DEFAULT_CONFLICT_MAX_RETRIES = 2


def _is_default_partition_conflict(exc: SQLAlchemyError) -> bool:
    """Check if error is DEFAULT partition conflict."""
    if pg_sqlstate(exc) != _PG_CHECK_VIOLATION:
        return False

    error_text = str(exc).lower()
    return (
        "updated partition constraint" in error_text
        and "default partition" in error_text
        and "would be violated" in error_text
    )


class PartitionCreationService(BasePartitionService):
    """Service for creating future partitions."""

    def __init__(
        self,
        repo: PartitionRepository,
        metadata: PartitionMetadataProvider,
        calculator: PeriodCalculator[Period],
        hooks: list[PartitionLifecycleHooks] | None = None,
    ) -> None:
        super().__init__(hooks=hooks)
        self._repo = repo
        self._metadata = metadata
        self._calculator = calculator

    def create_future_partitions(
        self,
        config: TablePartitionConfig,
        *,
        existing_partitions: list[PartitionInfo] | None = None,
    ) -> list[PartitionInfo]:
        """Create partitions for future periods.

        Returns:
            List of newly created and attached partitions.

        Raises:
            Exception: Any error during creation, listing or hooks (if fail_on_hook_error is True).
        """
        created: list[PartitionInfo] = []
        qualified_parent = qualify(config.db_schema, config.table_name)
        periods = self._calculator.next_periods(config.create_ahead_count)

        if existing_partitions is None:
            existing_partitions = self._metadata.list_partitions(qualified_parent)

        existing_by_period = self._map_partitions_to_periods(qualified_parent, existing_partitions)

        for period in periods:
            p_info = self._ensure_partition_for_period(config, period, existing_by_period.get(period))
            if p_info:
                created.append(p_info)

        return created

    def _map_partitions_to_periods(
        self, qualified_parent: str, partitions: list[PartitionInfo]
    ) -> dict[Period, PartitionInfo]:
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
            except KeyboardInterrupt:
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

    def _ensure_partition_for_period(
        self,
        config: TablePartitionConfig,
        period: Period,
        existing: PartitionInfo | None,
    ) -> PartitionInfo | None:
        """Ensure a partition exists and is attached for a specific period."""
        partition_name, from_value, to_value = self._get_partition_metadata(config, period)

        if existing is not None:
            self._handle_existing_partition(config, existing, from_value, to_value)
            return None

        # Hooks: before create
        self._run_hooks(
            lambda h: h.before_create(config, partition_name, from_value, to_value),
            "before_create",
            partition_name=partition_name,
        )

        # Creation and optional attachment
        partition_info = self._create_and_attach_partition(config, partition_name, from_value, to_value)

        if partition_info:
            # Hooks: after create
            self._run_hooks(
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

        if len(partition_relname.encode("utf-8")) > _PG_IDENTIFIER_MAX_BYTES:
            msg = (
                f"Generated partition name '{partition_relname}' exceeds PostgreSQL's "
                f"{_PG_IDENTIFIER_MAX_BYTES}-byte identifier limit"
            )
            raise InvalidPartitionConfigError(msg)

        return partition_name, from_value, to_value

    def _attach_with_reconcile(
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

        for attempt in range(1, _DEFAULT_CONFLICT_MAX_RETRIES + 1):
            try:
                self._repo.attach_partition(qualified_parent, partition_name, from_value, to_value)
            except (KeyboardInterrupt, SystemExit):
                # Best-effort compensating move-back before the interrupt propagates.
                self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                raise
            except (OSError, TimeoutError):
                self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                raise
            except SQLAlchemyError as e:
                if not _is_default_partition_conflict(e):
                    self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                    raise

                if attempt == _DEFAULT_CONFLICT_MAX_RETRIES:
                    logger.exception(
                        "Failed to attach after reconciliation retries",
                        extra={
                            "partition_name": partition_name,
                            "attempts": attempt,
                        },
                    )
                    self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
                    raise

                # Get DEFAULT partition
                default_partition = self._metadata.get_default_partition(qualified_parent)
                if not default_partition:
                    logger.warning(
                        "DEFAULT conflict detected but no DEFAULT partition found",
                        extra={"partition_name": partition_name},
                    )
                    self._restore_reconciled_rows(reconciled_from, config, partition_name, from_value, to_value)
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

                moved_count = self._repo.reconcile_default_rows(
                    default_partition_name=default_partition.name,
                    target_partition_name=partition_name,
                    partition_column=config.partition_column,
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

    def _restore_reconciled_rows(
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
            restored = self._repo.reconcile_default_rows(
                default_partition_name=partition_name,
                target_partition_name=default_partition_name,
                partition_column=config.partition_column,
                from_value=from_value,
                to_value=to_value,
            )
        except (KeyboardInterrupt, SystemExit):
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

    def _handle_existing_partition(
        self, config: TablePartitionConfig, existing: PartitionInfo, from_value: str, to_value: str
    ) -> None:
        """Handle case where partition already exists in metadata."""
        if config.auto_attach_after_create and not existing.is_attached:
            qualified_parent = qualify(config.db_schema, config.table_name)
            try:
                self._attach_with_reconcile(config, existing.name, from_value, to_value)
            except (OSError, TimeoutError):
                raise
            except SQLAlchemyError as e:
                if not self._is_attach_conflict_benign(qualified_parent, existing.name, e):
                    raise
                logger.debug(
                    "Partition already attached (race with another worker)",
                    extra={"partition_name": existing.name, "sqlstate": pg_sqlstate(e)},
                )

    def _is_attach_conflict_benign(self, qualified_parent: str, partition_name: str, exc: SQLAlchemyError) -> bool:
        """A conflict SQLSTATE proves a lost race only if the postcondition holds.

        42809 (wrong_object_type) also fires for typed tables, inheritance
        children, and attachments to a *different* parent — verify the
        partition is actually attached to our parent instead of trusting the
        error code alone.
        """
        if pg_sqlstate(exc) not in _ATTACH_CONFLICT_SQLSTATES:
            return False
        return self._metadata.is_partition_attached(qualified_parent, partition_name)

    def _create_and_attach_partition(
        self, config: TablePartitionConfig, partition_name: str, from_value: str, to_value: str
    ) -> PartitionInfo | None:
        """Create partition and optionally attach it to parent."""
        qualified_parent = qualify(config.db_schema, config.table_name)
        try:
            partition_info = self._repo.create_partition(config, partition_name, from_value, to_value)
        except PartitionAlreadyExistsError:
            if config.auto_attach_after_create:
                try:
                    self._attach_with_reconcile(config, partition_name, from_value, to_value)
                except (OSError, TimeoutError):
                    raise
                except SQLAlchemyError as e:
                    if not self._is_attach_conflict_benign(qualified_parent, partition_name, e):
                        raise
                    logger.debug(
                        "Partition already attached (race)",
                        extra={"partition_name": partition_name, "sqlstate": pg_sqlstate(e)},
                    )
            return None

        if config.auto_attach_after_create:
            try:
                self._attach_with_reconcile(config, partition_name, from_value, to_value)
            except (OSError, TimeoutError):
                raise
            except SQLAlchemyError as e:
                if not self._is_attach_conflict_benign(qualified_parent, partition_name, e):
                    raise
                logger.debug(
                    "Partition already attached (race)",
                    extra={"partition_name": partition_name, "sqlstate": pg_sqlstate(e)},
                )
            partition_info = partition_info.model_copy(update={"is_attached": True})

        return partition_info
