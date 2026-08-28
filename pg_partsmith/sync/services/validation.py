"""Partition validation domain service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.sync.protocols import NestedPartitionMetadata
from pg_partsmith.utils import qualify

if TYPE_CHECKING:
    from pg_partsmith.entities import SubpartitionSpec, TablePartitionConfig
    from pg_partsmith.sync.protocols import PartitionMetadataProvider


class PartitionValidationService:
    """Service for validating partition configurations against database catalog."""

    def __init__(self, metadata: PartitionMetadataProvider) -> None:
        self._metadata = metadata

    def validate_config(self, config: TablePartitionConfig) -> None:
        """Fail fast if the provided config does not match the actual table definition."""
        qualified_parent = qualify(config.db_schema, config.table_name)

        actual_type = self._metadata.get_partition_type(qualified_parent)
        if actual_type is None:
            msg = f"Table {qualified_parent!r} is not partitioned"
            raise InvalidPartitionConfigError(msg)

        if actual_type != config.partition_type:
            msg = (
                f"Partition type mismatch for table {qualified_parent!r}: "
                f"config={config.partition_type.value!r} actual={actual_type.value!r}"
            )
            raise InvalidPartitionConfigError(msg)

        try:
            actual_column = self._metadata.get_partition_column(qualified_parent)
        except ValueError as exc:
            raise InvalidPartitionConfigError(str(exc)) from exc

        if actual_column is None:
            msg = f"Could not determine partition column for table {qualified_parent!r}"
            raise InvalidPartitionConfigError(msg)

        # A quoted mixed-case column would pass a lowercased comparison here but
        # later fail in reconcile SQL, which quotes the config's lowercase name.
        if actual_column != actual_column.lower():
            msg = (
                f"Partition column {actual_column!r} of table {qualified_parent!r} is mixed-case; "
                "only lowercase partition columns are supported"
            )
            raise InvalidPartitionConfigError(msg)

        if actual_column != config.partition_column:
            msg = (
                f"Partition column mismatch for table {qualified_parent!r}: "
                f"config={config.partition_column!r} actual={actual_column!r}"
            )
            raise InvalidPartitionConfigError(msg)
        self._validate_subpartitioning(config, qualified_parent)

    def _validate_subpartitioning(self, config: TablePartitionConfig, qualified_parent: str) -> None:
        """Refuse a partitioning layout the database could not accept.

        PostgreSQL requires every UNIQUE / PRIMARY KEY constraint on a
        partitioned table to contain all of its partition-key columns. A hash
        column missing from one of them makes the very first branch fail with
        ``unique constraint on partitioned table must include all partitioning
        columns`` — mid-run, after other tables were already changed. Catching
        it here turns that into a configuration error with a fix in it.
        """
        levels = config.subpartition_levels
        if not levels:
            return

        metadata = self._metadata
        if not isinstance(metadata, NestedPartitionMetadata):
            msg = (
                f"Metadata provider {type(metadata).__name__} cannot introspect partition trees, "
                "which subpartitioning requires; it must implement NestedPartitionMetadata."
            )
            raise InvalidPartitionConfigError(msg)

        constraints = metadata.get_unique_constraint_columns(qualified_parent)
        if not constraints:
            return

        for spec in levels:
            _require_column_in_constraints(spec, constraints, qualified_parent)


def _require_column_in_constraints(
    spec: SubpartitionSpec,
    constraints: tuple[tuple[str, ...], ...],
    qualified_parent: str,
) -> None:
    """Raise when a subpartition column is missing from any unique constraint."""
    offenders = [columns for columns in constraints if spec.column not in columns]
    if not offenders:
        return

    missing = ", ".join("(" + ", ".join(columns) + ")" for columns in offenders)
    msg = (
        f"Subpartition column {spec.column!r} is missing from unique constraint(s) {missing} "
        f"on table {qualified_parent!r}. PostgreSQL requires every UNIQUE/PRIMARY KEY on a "
        f"partitioned table to include all partition key columns, so add {spec.column!r} to "
        "them before enabling this subpartitioning."
    )
    raise InvalidPartitionConfigError(msg)
