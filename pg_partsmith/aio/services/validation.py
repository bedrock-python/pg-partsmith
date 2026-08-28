"""Partition validation domain service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pg_partsmith.aio.protocols import CompositeKeyMetadata, NestedPartitionMetadata
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.utils import qualify

if TYPE_CHECKING:
    from pg_partsmith.aio.protocols import PartitionMetadataProvider
    from pg_partsmith.entities import SubpartitionSpec, TablePartitionConfig


class PartitionValidationService:
    """Service for validating partition configurations against database catalog."""

    def __init__(self, metadata: PartitionMetadataProvider) -> None:
        self._metadata = metadata

    async def validate_config(self, config: TablePartitionConfig) -> None:
        """Fail fast if the provided config does not match the actual table definition."""
        qualified_parent = qualify(config.db_schema, config.table_name)

        actual_type = await self._metadata.get_partition_type(qualified_parent)
        if actual_type is None:
            msg = f"Table {qualified_parent!r} is not partitioned"
            raise InvalidPartitionConfigError(msg)

        if actual_type != config.partition_type:
            msg = (
                f"Partition type mismatch for table {qualified_parent!r}: "
                f"config={config.partition_type.value!r} actual={actual_type.value!r}"
            )
            raise InvalidPartitionConfigError(msg)

        actual_columns = await self._actual_partition_columns(config, qualified_parent)

        if not actual_columns:
            msg = f"Could not determine partition column for table {qualified_parent!r}"
            raise InvalidPartitionConfigError(msg)

        # A quoted mixed-case column would pass a lowercased comparison here but
        # later fail in reconcile SQL, which quotes the config's lowercase name.
        mixed_case = [column for column in actual_columns if column != column.lower()]
        if mixed_case:
            msg = (
                f"Partition column(s) {mixed_case!r} of table {qualified_parent!r} are mixed-case; "
                "only lowercase partition columns are supported"
            )
            raise InvalidPartitionConfigError(msg)

        if actual_columns != config.partition_columns:
            # Single-column tables keep the wording they have always reported.
            if config.key_arity == 1 and len(actual_columns) == 1:
                msg = (
                    f"Partition column mismatch for table {qualified_parent!r}: "
                    f"config={config.partition_column!r} actual={actual_columns[0]!r}"
                )
            else:
                msg = (
                    f"Partition key mismatch for table {qualified_parent!r}: "
                    f"config={config.partition_columns!r} actual={actual_columns!r}"
                )
            raise InvalidPartitionConfigError(msg)

        await self._validate_subpartitioning(config, qualified_parent)

    async def _actual_partition_columns(
        self,
        config: TablePartitionConfig,
        qualified_parent: str,
    ) -> tuple[str, ...]:
        """Read the table's real partition key, in key order.

        A single-column config goes through the original accessor unchanged, so
        a metadata provider that predates composite keys keeps working; the
        newer one is required only when the config declares a multi-column key.
        """
        metadata = self._metadata

        if config.key_arity == 1:
            # The long-standing accessor, untouched: a single-column config must
            # keep working with a metadata provider that predates composite keys.
            try:
                actual_column = await metadata.get_partition_column(qualified_parent)
            except ValueError as exc:
                raise InvalidPartitionConfigError(str(exc)) from exc
            return (actual_column,) if actual_column is not None else ()

        if not isinstance(metadata, CompositeKeyMetadata):
            msg = (
                f"Metadata provider {type(metadata).__name__} cannot introspect composite partition "
                "keys; it must implement CompositeKeyMetadata."
            )
            raise InvalidPartitionConfigError(msg)

        return await metadata.get_partition_columns(qualified_parent)

    async def _validate_subpartitioning(self, config: TablePartitionConfig, qualified_parent: str) -> None:
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

        constraints = await metadata.get_unique_constraint_columns(qualified_parent)
        if not constraints:
            return

        for spec in levels:
            _require_column_in_constraints(spec, constraints, qualified_parent)


def _require_column_in_constraints(
    spec: SubpartitionSpec,
    constraints: tuple[tuple[str, ...], ...],
    qualified_parent: str,
) -> None:
    """Raise when a subpartition key column is missing from any unique constraint.

    Every column the level adds is checked, not only the leading one: a
    composite subpartition key puts all of them into the branch's PARTITION BY,
    and PostgreSQL requires all of them in every uniqueness constraint.
    """
    offenders = [columns for columns in constraints if any(column not in columns for column in spec.columns)]
    if not offenders:
        return

    absent = sorted({column for column in spec.columns for cols in offenders if column not in cols})
    named = ", ".join(repr(column) for column in absent)
    missing = ", ".join("(" + ", ".join(columns) + ")" for columns in offenders)
    msg = (
        f"Subpartition column(s) {named} missing from unique constraint(s) {missing} "
        f"on table {qualified_parent!r}. PostgreSQL requires every UNIQUE/PRIMARY KEY on a "
        f"partitioned table to include all partition key columns, so add {named} to "
        "them before enabling this subpartitioning."
    )
    raise InvalidPartitionConfigError(msg)
