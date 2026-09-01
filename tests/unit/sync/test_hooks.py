"""Unit tests for the sync lifecycle hooks: the no-op base, the protocol and the event."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from pg_partsmith.boundaries import TimeBoundaries
from pg_partsmith.entities import PartitionGranularity, PartitionInfo, PartitionType, RangeBounds, TablePartitionConfig
from pg_partsmith.events import HOOK_METHODS, HookPhase, PartitionEvent, validate_hook_signatures
from pg_partsmith.plan import CreatePartition, DropPartition, Reason
from pg_partsmith.scheme import HashPartitioning
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks, PartitionLifecycleHooks

APRIL = RangeBounds(from_value="2024-04-01", to_value="2024-05-01")


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
        name="events__2024_04",
        partition_type=PartitionType.RANGE,
        bounds=APRIL,
        is_attached=False,
        subpartition_type=PartitionType.HASH,
        parent_table="events",
    )


@pytest.fixture
def event(config: TablePartitionConfig, partition_info: PartitionInfo) -> PartitionEvent:
    return PartitionEvent.build(
        HookPhase.BEFORE_CREATE,
        config,
        partition_info,
        CreatePartition(
            target="events__2024_04",
            parent_name="public.events",
            bounds=APRIL,
            key_columns=("created_at",),
            reason=Reason.CREATE_AHEAD,
            detail="2024_04 under 'create 3 ahead'",
        ),
    )


# ── the event ───────────────────────────────────────────────────────────────────


def test__event__table_name__is_the_configs_qualified_name(event: PartitionEvent) -> None:
    # Arrange / Act / Assert -- derived, so the two cannot drift
    assert event.table_name == event.config.qualified_name


def test__event__range_root__carries_the_window_the_bounds_stand_for(event: PartitionEvent) -> None:
    # Arrange
    months = TimeBoundaries(granularity=PartitionGranularity.MONTH)

    # Act / Assert -- the period the hook actually wants, not a pair of literals
    assert event.window == months.window_at(datetime(2024, 4, 15, tzinfo=UTC))


def test__event__hash_root__has_no_window(config: TablePartitionConfig, partition_info: PartitionInfo) -> None:
    # Arrange -- a set divides a keyspace, not an axis: its members cover no period
    hashed = config.model_copy(update={"scheme": HashPartitioning(key="tenant_id", modulus=4)})

    # Act
    event = PartitionEvent.build(HookPhase.BEFORE_CREATE, hashed, partition_info, _drop_op())

    # Assert
    assert event.window is None


def test__event__partition_the_plan_knows_only_by_name__is_not_refused(config: TablePartitionConfig) -> None:
    # Arrange -- an attached RANGE partition without bounds breaks the listing invariant,
    # but a hook must still get its event: the operation never needed the bound
    bare = PartitionInfo.model_construct(name="events__2024_04", partition_type=PartitionType.RANGE, is_attached=True)

    # Act
    event = PartitionEvent.build(HookPhase.BEFORE_DETACH, config, bare, _drop_op())

    # Assert
    assert event.partition.name == "events__2024_04"
    assert event.window is None


def test__event__is_frozen(event: PartitionEvent) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="frozen"):
        event.phase = HookPhase.AFTER_DROP  # type: ignore[misc]


def test__hook_phase__values_are_the_method_names() -> None:
    # Arrange / Act / Assert -- what lets the executor dispatch by phase
    assert HOOK_METHODS == (
        "before_create",
        "after_create",
        "before_detach",
        "after_detach",
        "before_drop",
        "after_drop",
        "on_event",
    )
    assert all(hasattr(BasePartitionLifecycleHooks(), name) for name in HOOK_METHODS)


# ── the no-op base ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("method", HOOK_METHODS)
def test__base_hooks__every_method__is_a_noop(method: str, event: PartitionEvent) -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act / Assert -- must not raise
    assert getattr(hooks, method)(event) is None


def test__protocol__duck_typed_implementation__is_an_instance() -> None:
    # Arrange
    class Custom:
        def before_create(self, event: PartitionEvent) -> None: ...
        def after_create(self, event: PartitionEvent) -> None: ...
        def before_detach(self, event: PartitionEvent) -> None: ...
        def after_detach(self, event: PartitionEvent) -> None: ...
        def before_drop(self, event: PartitionEvent) -> None: ...
        def after_drop(self, event: PartitionEvent) -> None: ...
        def on_event(self, event: PartitionEvent) -> None: ...

    # Act / Assert
    assert isinstance(Custom(), PartitionLifecycleHooks)


def test__base_hooks_subclass__overriding_one_method__the_others_stay_quiet(event: PartitionEvent) -> None:
    # Arrange
    seen: list[str] = []

    class Partial(BasePartitionLifecycleHooks):
        def before_drop(self, event: PartitionEvent) -> None:
            seen.append(event.partition.name)

    hooks = Partial()

    # Act
    hooks.before_create(event)
    hooks.before_drop(event)
    hooks.on_event(event)

    # Assert
    assert seen == ["events__2024_04"]


# ── refusing a hook from before 1.1 ─────────────────────────────────────────────


def test__validate_hook_signatures__event_shaped_hooks__pass() -> None:
    # Arrange
    class Ported(BasePartitionLifecycleHooks):
        def before_drop(self, event: PartitionEvent) -> None: ...

    # Act / Assert
    validate_hook_signatures([Ported(), BasePartitionLifecycleHooks()])


@pytest.mark.parametrize(
    "hook",
    [
        pytest.param(type("Old", (), {"before_create": lambda self, config, partition: None})(), id="create"),
        pytest.param(type("Old", (), {"before_drop": lambda self, table, name: None})(), id="drop"),
    ],
)
def test__validate_hook_signatures__pre_1_1_shape__is_refused_at_wiring_time(hook: object) -> None:
    # Arrange / Act / Assert -- otherwise it is accepted here and fails mid-run
    with pytest.raises(ValueError, match="takes one PartitionEvent"):
        validate_hook_signatures([hook])


def test__validate_hook_signatures__signature_that_cannot_be_read__is_left_alone() -> None:
    # Arrange -- a C callable or a decorator that hides its parameters is not ours to judge
    class Opaque(BasePartitionLifecycleHooks):
        def before_drop(self, event: PartitionEvent) -> None: ...

    # Act / Assert
    with patch("pg_partsmith.events.inspect.signature", side_effect=ValueError("no signature")):
        validate_hook_signatures([Opaque()])


def test__validate_hook_signatures__unreadable_or_variadic__is_left_alone() -> None:
    # Arrange -- a decorated method, a mock, anything taking *args: not ours to judge
    class Variadic:
        def before_drop(self, *args: object, **kwargs: object) -> None: ...

    # Act / Assert
    validate_hook_signatures([Variadic(), object()])


def _drop_op() -> DropPartition:
    return DropPartition(target="events__2024_04", reason=Reason.GRACE_ELAPSED, detail="grace elapsed")
