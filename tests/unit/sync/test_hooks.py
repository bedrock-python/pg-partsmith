import pytest

from pg_partsmith.entities import (
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks


@pytest.fixture
def config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )


@pytest.fixture
def partition_info() -> PartitionInfo:
    return PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
    )


def test__base_hooks__before_create__is_noop(config: TablePartitionConfig) -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert — must not raise
    hooks.before_create(config, "events__2024_01", "2024-01-01", "2024-02-01")


def test__base_hooks__after_create__is_noop(config: TablePartitionConfig, partition_info: PartitionInfo) -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert — must not raise
    hooks.after_create(config, partition_info)


def test__base_hooks__before_detach__is_noop(partition_info: PartitionInfo) -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert — must not raise
    hooks.before_detach("events", partition_info)


def test__base_hooks__after_detach__is_noop() -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert — must not raise
    hooks.after_detach("events", "events__2024_01")


def test__base_hooks__before_drop__is_noop() -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert — must not raise
    hooks.before_drop("events", "events__2024_01")


def test__base_hooks__after_drop__is_noop() -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert — must not raise
    hooks.after_drop("events", "events__2024_01")


def test__base_hooks_subclass__overriding_one_method__only_overridden_method_fires(
    config: TablePartitionConfig,
) -> None:
    # Arrange
    called: list[str] = []

    class MyHooks(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            called.append(f"before_drop:{table_name}:{partition_name}")

    hooks = MyHooks()

    # Act
    hooks.before_create(config, "events__2024_01", "2024-01-01", "2024-02-01")
    hooks.before_drop("events", "events__2024_01")

    # Assert
    assert called == ["before_drop:events:events__2024_01"]
