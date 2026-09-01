"""Unit tests for the sync ``PartitionValidationService``: the config is checked against the catalog before any DDL."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pg_partsmith.entities import PartitionGranularity, PartitionType, TablePartitionConfig
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.leaves import ForeignLeaves
from pg_partsmith.scheme import HashPartitioning, ListGroup, ListPartitioning
from pg_partsmith.sync.services.validation import PartitionValidationService, _require_column_in_constraints

# ── fixtures and builders ────────────────────────────────────────────────────────


@pytest.fixture
def metadata() -> MagicMock:
    metadata = MagicMock()
    metadata.get_partition_type = MagicMock(return_value=PartitionType.RANGE)
    metadata.get_partition_columns = MagicMock(return_value=("created_at",))
    metadata.get_unique_constraint_columns = MagicMock(return_value=())
    return metadata


@pytest.fixture
def validation(metadata: MagicMock) -> PartitionValidationService:
    return PartitionValidationService(metadata)


def _config(**overrides: object) -> TablePartitionConfig:
    fields: dict[str, object] = {
        "table_name": "events",
        "partition_column": "created_at",
        "granularity": PartitionGranularity.MONTH,
    }
    fields.update(overrides)
    return TablePartitionConfig(**fields)  # type: ignore[arg-type]


def _composite_config() -> TablePartitionConfig:
    return _config(trailing_partition_columns=("tenant_id",))


def _nested_config(child: HashPartitioning | ListPartitioning) -> TablePartitionConfig:
    return _config(subpartition=child)


# ── the root ────────────────────────────────────────────────────────────────────


def test__validate_config__matching_table__passes(validation: PartitionValidationService, metadata: MagicMock) -> None:
    # Arrange / Act -- must not raise
    validation.validate_config(_config(schema="public"))

    # Assert
    metadata.get_partition_type.assert_called_once_with("public.events")
    metadata.get_partition_columns.assert_called_once_with("public.events")
    metadata.get_unique_constraint_columns.assert_not_called()


def test__validate_config__table_not_partitioned__raises(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_type.return_value = None

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="'events' is not partitioned"):
        validation.validate_config(_config())

    metadata.get_partition_columns.assert_not_called()


def test__validate_config__partition_type_mismatch__raises_with_both_types(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_type.return_value = PartitionType.LIST

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match=r"Partition type mismatch.*config='range' actual='list'"):
        validation.validate_config(_config())


def test__validate_config__no_partition_columns__raises(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_columns.return_value = ()

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="Could not determine partition column"):
        validation.validate_config(_config())


def test__validate_config__mixed_case_column__raises(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange -- a quoted mixed-case column would break the reconcile SQL later; fail fast instead
    metadata.get_partition_columns.return_value = ("createdAt",)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match=r"\['createdAt'\].*mixed-case"):
        validation.validate_config(_config(partition_column="createdat"))


def test__validate_config__single_column_mismatch__keeps_the_historical_wording(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_columns.return_value = ("other_col",)

    # Act / Assert
    with pytest.raises(
        InvalidPartitionConfigError, match=r"Partition column mismatch.*config='created_at' actual='other_col'"
    ):
        validation.validate_config(_config())


def test__validate_config__composite_key_mismatch__reports_the_whole_key(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_columns.return_value = ("created_at", "region")

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match=r"Partition key mismatch.*\('created_at', 'tenant_id'\)"):
        validation.validate_config(_composite_config())


def test__validate_config__composite_config_on_a_single_column_table__reports_the_whole_key(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_columns.return_value = ("created_at",)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="Partition key mismatch"):
        validation.validate_config(_composite_config())


def test__validate_config__composite_key_in_catalog_order__passes(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_columns.return_value = ("created_at", "tenant_id")

    # Act / Assert -- must not raise
    validation.validate_config(_composite_config())


def test__validate_config__expression_key__error_propagates_as_invalid_config(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange -- the metadata provider refuses a key position it cannot address
    metadata.get_partition_columns.side_effect = InvalidPartitionConfigError("partitions on an expression")

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="partitions on an expression"):
        validation.validate_config(_config())


# ── nested levels and unique constraints ────────────────────────────────────────


def test__validate_config__nested_level_missing_from_a_constraint__raises_with_a_fix(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_unique_constraint_columns.return_value = (("id", "created_at"), ("created_at", "tenant_id"))

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError) as excinfo:
        validation.validate_config(_nested_config(HashPartitioning(key="tenant_id", modulus=4)))

    message = str(excinfo.value)
    assert "Subpartition column(s) 'tenant_id' missing from unique constraint(s) (id, created_at)" in message
    assert "add 'tenant_id' to them" in message


def test__validate_config__nested_level_present_in_every_constraint__passes(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_unique_constraint_columns.return_value = (("id", "created_at", "tenant_id"),)

    # Act / Assert -- must not raise
    validation.validate_config(_nested_config(HashPartitioning(key="tenant_id", modulus=4)))
    metadata.get_unique_constraint_columns.assert_called_once_with("events")


def test__validate_config__no_unique_constraints__passes(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_unique_constraint_columns.return_value = ()

    # Act / Assert -- must not raise
    validation.validate_config(_nested_config(HashPartitioning(key="tenant_id", modulus=4)))


def test__validate_config__composite_nested_key__every_column_is_checked(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange -- the leading column is covered, the trailing one is not
    metadata.get_unique_constraint_columns.return_value = (("id", "created_at", "tenant_id"),)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="'shard_id'"):
        validation.validate_config(_nested_config(HashPartitioning(key=("tenant_id", "shard_id"), modulus=4)))


def test__validate_config__list_level_missing_from_a_constraint__raises(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_unique_constraint_columns.return_value = (("id", "created_at"),)
    level = ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("de", "fr")),))

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="'region'"):
        validation.validate_config(_nested_config(level))


def test__validate_config__deeper_levels__are_checked_too(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_unique_constraint_columns.return_value = (("id", "created_at", "tenant_id"),)
    level = HashPartitioning(
        key="tenant_id",
        modulus=2,
        child=ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("de",)),)),
    )

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="'region'"):
        validation.validate_config(_nested_config(level))


def test__require_column_in_constraints__trailing_column_missing__is_refused() -> None:
    # Arrange
    level = HashPartitioning(key=("tenant_id", "shard_id"), modulus=2)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="'shard_id' missing from unique constraint"):
        _require_column_in_constraints(level, (("id", "tenant_id", "created_at"),), "public.events")


def test__require_column_in_constraints__both_columns_missing__names_them_sorted() -> None:
    # Arrange
    level = HashPartitioning(key=("tenant_id", "shard_id"), modulus=2)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="'shard_id', 'tenant_id'"):
        _require_column_in_constraints(level, (("id", "created_at"),), "public.events")


def test__require_column_in_constraints__every_key_column_present__accepted() -> None:
    # Arrange
    level = HashPartitioning(key=("tenant_id", "shard_id"), modulus=2)

    # Act / Assert -- no exception
    _require_column_in_constraints(level, (("id", "tenant_id", "shard_id", "created_at"),), "public.events")


# ── foreign leaves ──────────────────────────────────────────────────────────────


def test__validate_config__foreign_leaves_on_a_table_with_a_unique_index__raises(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_unique_constraint_columns.return_value = (("id", "created_at"),)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="refuses a foreign table as a partition") as excinfo:
        validation.validate_config(_config(leaves=ForeignLeaves(server="archive")))

    assert "(id, created_at)" in str(excinfo.value)


def test__validate_config__foreign_leaves_on_an_index_free_table__passes(
    validation: PartitionValidationService, metadata: MagicMock
) -> None:
    # Arrange / Act -- must not raise
    validation.validate_config(_config(leaves=ForeignLeaves(server="archive")))

    # Assert
    metadata.get_unique_constraint_columns.assert_called_once_with("events")
