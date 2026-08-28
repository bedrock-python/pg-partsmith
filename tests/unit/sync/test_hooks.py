"""Unit tests for the sync lifecycle hooks: the no-op base and the protocol."""

from __future__ import annotations

import pytest

from pg_partsmith.entities import PartitionGranularity, PartitionInfo, PartitionType, RangeBounds, TablePartitionConfig
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks, PartitionLifecycleHooks


@pytest.fixture
def config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )


@pytest.fixture
def partition_info() -> PartitionInfo:
    return PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        bounds=RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
        is_attached=False,
        subpartition_type=PartitionType.HASH,
        parent_table="events",
    )


def test__base_hooks__before_create__is_noop(config: TablePartitionConfig, partition_info: PartitionInfo) -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert -- must not raise
    assert hooks.before_create(config, partition_info) is None


def test__base_hooks__after_create__is_noop(config: TablePartitionConfig, partition_info: PartitionInfo) -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert -- must not raise
    assert hooks.after_create(config, partition_info) is None


def test__base_hooks__before_detach__is_noop(partition_info: PartitionInfo) -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert -- must not raise
    assert hooks.before_detach("events", partition_info) is None


def test__base_hooks__after_detach__is_noop() -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert -- must not raise
    assert hooks.after_detach("events", "events__2024_01") is None


def test__base_hooks__before_drop__is_noop() -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert -- must not raise
    assert hooks.before_drop("events", "events__2024_01") is None


def test__base_hooks__after_drop__is_noop() -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert -- must not raise
    assert hooks.after_drop("events", "events__2024_01") is None


def test__base_hooks__satisfies_the_protocol() -> None:
    # Arrange / Act / Assert
    assert isinstance(BasePartitionLifecycleHooks(), PartitionLifecycleHooks)


def test__protocol__object_missing_a_hook__is_not_an_instance() -> None:
    # Arrange
    class _Partial:
        def before_drop(self, table_name: str, partition_name: str) -> None:
            return None

    # Act / Assert
    assert not isinstance(_Partial(), PartitionLifecycleHooks)


def test__protocol__duck_typed_implementation__is_an_instance() -> None:
    # Arrange -- no inheritance, every method present
    class _Ducky:
        def before_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
            return None

        def after_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
            return None

        def before_detach(self, table_name: str, partition: PartitionInfo) -> None:
            return None

        def after_detach(self, table_name: str, partition_name: str) -> None:
            return None

        def before_drop(self, table_name: str, partition_name: str) -> None:
            return None

        def after_drop(self, table_name: str, partition_name: str) -> None:
            return None

    # Act / Assert
    assert isinstance(_Ducky(), PartitionLifecycleHooks)


def test__base_hooks_subclass__before_create__receives_the_partition_about_to_be_created(
    config: TablePartitionConfig, partition_info: PartitionInfo
) -> None:
    # Arrange
    seen: list[PartitionInfo] = []

    class _Hooks(BasePartitionLifecycleHooks):
        def before_create(self, cfg: TablePartitionConfig, partition: PartitionInfo) -> None:
            seen.append(partition)

    # Act
    _Hooks().before_create(config, partition_info)

    # Assert -- the new signature carries the whole partition, bounds and subpartitioning included
    assert seen == [partition_info]
    assert seen[0].from_value == "2024-01-01"
    assert seen[0].to_value == "2024-02-01"
    assert seen[0].subpartition_type is PartitionType.HASH
    assert seen[0].is_attached is False


def test__base_hooks_subclass__overriding_one_method__only_overridden_method_fires(
    config: TablePartitionConfig, partition_info: PartitionInfo
) -> None:
    # Arrange
    called: list[str] = []

    class _Hooks(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            called.append(f"before_drop:{table_name}:{partition_name}")

    hooks = _Hooks()

    # Act
    hooks.before_create(config, partition_info)
    hooks.before_drop("events", "events__2024_01")

    # Assert
    assert called == ["before_drop:events:events__2024_01"]
