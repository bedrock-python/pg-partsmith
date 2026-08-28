"""Unit tests for the aio ``PartitionLifecycleService``: plan / apply / maintain and the conveniences over them."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.boundaries import NumericBoundaries, Window
from pg_partsmith.entities import (
    MaintenanceResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionNode,
    PartitionType,
    Period,
    RangeBounds,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import InvalidPartitionConfigError, PartitionAlreadyExistsError, PartitionAttachedError
from pg_partsmith.lifecycle import (
    CreateAhead,
    DetachMode,
    ExpireIf,
    KeepBehind,
    LifecyclePolicy,
    SizeAbove,
    SqlPredicate,
)
from pg_partsmith.plan import (
    AttachPartition,
    CreatePartition,
    DetachPartition,
    DropPartition,
    MaintenancePlan,
    Reason,
)
from pg_partsmith.planner import PlanMode
from pg_partsmith.scheme import HashPartitioning, ListGroup, ListPartitioning, RangePartitioning
from pg_partsmith.topology import ActualTree, DetachedPartition, FactKind, HashBounds

NOW = datetime(2024, 3, 15, 12, 0, tzinfo=UTC)
MARCH = RangeBounds(from_value="2024-03-01", to_value="2024-04-01")

# ── fixtures and builders ────────────────────────────────────────────────────────


class _FakeLock:
    """An async context manager that records when it is held."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def __aenter__(self) -> None:
        self._events.append("acquire")

    async def __aexit__(self, *exc: object) -> bool:
        self._events.append("release")
        return False


@pytest.fixture
def events() -> list[str]:
    return []


@pytest.fixture
def repo() -> MagicMock:
    repo = MagicMock()
    repo.ddl_timezone = "UTC"
    repo.create_table_like = AsyncMock(return_value=None)
    repo.attach_partition = AsyncMock(return_value=None)
    repo.detach_partition = AsyncMock(return_value=None)
    repo.drop_partition = AsyncMock(return_value=None)
    repo.reconcile_default_rows = AsyncMock(return_value=0)
    return repo


@pytest.fixture
def metadata() -> MagicMock:
    metadata = MagicMock()
    metadata.get_partition_type = AsyncMock(return_value=PartitionType.RANGE)
    metadata.get_partition_columns = AsyncMock(return_value=("created_at",))
    metadata.get_actual_tree = AsyncMock(return_value=_tree())
    metadata.measure = AsyncMock(side_effect=lambda tree, **kwargs: tree)
    metadata.get_partition_tree = AsyncMock(return_value=None)
    metadata.get_default_partition = AsyncMock(return_value=None)
    metadata.is_partition_attached = AsyncMock(return_value=False)
    metadata.get_relation_oid = AsyncMock(return_value=None)
    metadata.get_unique_constraint_columns = AsyncMock(return_value=())
    metadata.get_key_high_water_mark = AsyncMock(return_value=None)
    return metadata


@pytest.fixture
def locks(events: list[str]) -> MagicMock:
    locks = MagicMock()
    locks.acquire_lock.return_value = _FakeLock(events)
    return locks


@pytest.fixture
def service(repo: MagicMock, metadata: MagicMock, locks: MagicMock) -> PartitionLifecycleService:
    return PartitionLifecycleService(repo, metadata, locks)


@pytest.fixture
def config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=1,
        retention_count=2,
    )


def _nested_config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=1,
        retention_count=2,
        subpartition=HashPartitioning(key="tenant_id", modulus=2),
    )


def _numeric_config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="queue",
        scheme=RangePartitioning(key="id", boundaries=NumericBoundaries(step=100_000)),
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=2), retention=KeepBehind(distance=1_000_000)),
    )


def _range_child(name: str, start: str, end: str, *, oid: int | None = None, **extra: Any) -> PartitionNode:
    return PartitionNode(
        name=name, parent_name="events", level=1, oid=oid, bounds=RangeBounds(from_value=start, to_value=end), **extra
    )


def _tree(*children: PartitionNode, orphans: tuple[DetachedPartition, ...] = (), name: str = "events") -> ActualTree:
    root = PartitionNode(
        name=name, partition_type=PartitionType.RANGE, partition_columns=("created_at",), children=children
    )
    return ActualTree(root=root, orphans=orphans)


def _plan_with_every_kind() -> MaintenancePlan:
    return MaintenancePlan(
        table_name="events",
        generated_at=NOW,
        operations=(
            CreatePartition(target="events__2024_04", parent_name="events", bounds=MARCH, reason=Reason.CREATE_AHEAD),
            AttachPartition(target="events__2024_02", parent_name="events", bounds=MARCH, reason=Reason.REATTACH),
            DetachPartition(target="events__2024_01", parent_name="events", reason=Reason.RETENTION_EXPIRED),
            DropPartition(target="events__2024_01", reason=Reason.FOLLOWS_DETACH, follows_detach=True),
            DropPartition(target="events__2023_12", reason=Reason.GRACE_ELAPSED),
        ),
    )


# ── plan ────────────────────────────────────────────────────────────────────────


async def test__plan__empty_tree__creates_the_current_window_at_the_given_instant(
    service: PartitionLifecycleService, config: TablePartitionConfig
) -> None:
    # Arrange / Act
    plan = await service.plan(config, now=NOW)

    # Assert
    assert plan.generated_at == NOW
    assert [op.target for op in plan.creates] == ["events__2024_03"]
    assert plan.creates[0].bounds == MARCH
    assert plan.creates[0].reason is Reason.CREATE_AHEAD


async def test__plan__naive_now__is_read_as_utc(
    service: PartitionLifecycleService, config: TablePartitionConfig
) -> None:
    # Arrange / Act
    plan = await service.plan(config, now=NOW.replace(tzinfo=None))

    # Assert
    assert plan.generated_at == NOW


async def test__plan__type_mismatch__raises_before_reading_the_tree(
    service: PartitionLifecycleService, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_partition_type.return_value = PartitionType.LIST

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="Partition type mismatch"):
        await service.plan(config)

    metadata.get_actual_tree.assert_not_awaited()


async def test__plan__column_mismatch__raises_invalid_config(
    service: PartitionLifecycleService, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_partition_columns.return_value = ("other_col",)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="Partition column mismatch"):
        await service.plan(config)


async def test__plan__calendar_and_ddl_timezones_disagree__refused_before_touching_the_catalog(
    repo: MagicMock, metadata: MagicMock, locks: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange -- the calendar runs in UTC while the repository writes literals in Moscow time
    repo.ddl_timezone = "Europe/Moscow"
    service = PartitionLifecycleService(repo, metadata, locks)

    # Act / Assert
    with pytest.raises(ValueError, match="Timezone mismatch"):
        await service.plan(config)

    metadata.get_partition_type.assert_not_awaited()


async def test__plan__ddl_timezone_none_with_utc_calendar__is_accepted(
    repo: MagicMock, metadata: MagicMock, locks: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    repo.ddl_timezone = None
    service = PartitionLifecycleService(repo, metadata, locks)

    # Act
    plan = await service.plan(config, now=NOW)

    # Assert
    assert not plan.is_noop


async def test__plan__repository_without_ddl_timezone__skips_the_alignment_check(
    metadata: MagicMock, locks: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange -- a custom repository written against the protocol alone
    repo = MagicMock(spec=["create_table_like", "attach_partition", "detach_partition", "drop_partition"])
    service = PartitionLifecycleService(repo, metadata, locks)

    # Act
    plan = await service.plan(config, now=NOW)

    # Assert
    assert [op.target for op in plan.creates] == ["events__2024_03"]


async def test__plan__numeric_root__ignores_the_ddl_timezone_and_reads_the_cursor(
    repo: MagicMock, metadata: MagicMock, locks: MagicMock
) -> None:
    # Arrange -- no calendar anywhere, so there is nothing to align
    repo.ddl_timezone = "Europe/Moscow"
    metadata.get_partition_columns.return_value = ("id",)
    metadata.get_actual_tree.return_value = ActualTree(
        root=PartitionNode(name="queue", partition_type=PartitionType.RANGE, partition_columns=("id",))
    )
    metadata.get_key_high_water_mark.return_value = 250_000
    service = PartitionLifecycleService(repo, metadata, locks)

    # Act
    plan = await service.plan(_numeric_config(), now=NOW)

    # Assert
    metadata.get_key_high_water_mark.assert_awaited_once_with("queue", "id", sequence=False)
    assert plan.cursors == {"id": 250_000}
    assert [op.target for op in plan.creates] == ["queue__200000", "queue__300000"]


async def test__plan__table_not_partitioned__raises_invalid_config(
    service: PartitionLifecycleService, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = None

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="not partitioned"):
        await service.plan(config)


async def test__plan__explicit_mode__creates_only_the_named_windows_and_expires_nothing(
    service: PartitionLifecycleService, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange -- January is past retention, but an explicit plan never expires
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_01", "2024-01-01", "2024-02-01", oid=1))
    windows = {"created_at": (Window(start=datetime(2023, 11, 1, tzinfo=UTC), end=datetime(2023, 12, 1, tzinfo=UTC)),)}

    # Act
    plan = await service.plan(config, mode=PlanMode.EXPLICIT, now=NOW, windows=windows)

    # Assert
    assert [op.target for op in plan.creates] == ["events__2023_11"]
    assert plan.creates[0].reason is Reason.EXPLICIT
    assert plan.detaches == ()
    assert plan.drops == ()


async def test__plan__reconcile_mode__creates_nothing_ahead_and_repairs_existing_branches(
    service: PartitionLifecycleService, metadata: MagicMock
) -> None:
    # Arrange -- the March branch exists with one of its two buckets
    branch = _range_child(
        "events__2024_03",
        "2024-03-01",
        "2024-04-01",
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=(PartitionNode(name="events__2024_03__h0", bounds=HashBounds(modulus=2, remainder=0)),),
    )
    metadata.get_actual_tree.return_value = _tree(branch)

    # Act
    plan = await service.plan(_nested_config(), mode=PlanMode.RECONCILE, now=NOW)

    # Assert
    assert [(op.target, op.counts_as) for op in plan.creates] == [("events__2024_03__h1", "repaired")]


async def test__plan__maintain_mode_with_a_fact_hungry_policy__measures_the_progression_members(
    service: PartitionLifecycleService, metadata: MagicMock
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        lifecycle=LifecyclePolicy(creation=CreateAhead(count=1), retention=ExpireIf(when=SizeAbove(bytes=10))),
    )
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_02", "2024-02-01", "2024-03-01", oid=2))

    # Act
    await service.plan(config, now=NOW)

    # Assert
    metadata.measure.assert_awaited_once()
    kwargs = metadata.measure.call_args.kwargs
    assert kwargs["targets"] == ("events__2024_02",)
    assert kwargs["facts"] == frozenset({FactKind.SIZE})
    assert kwargs["sql_predicates"] == ()


@pytest.mark.parametrize("mode", [PlanMode.RECONCILE, PlanMode.EXPLICIT])
async def test__plan__non_maintain_modes__never_measure(
    service: PartitionLifecycleService, metadata: MagicMock, mode: PlanMode
) -> None:
    # Arrange
    predicate = SqlPredicate(sql="SELECT count(*) = 0 FROM {partition}")
    config = TablePartitionConfig(
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        lifecycle=LifecyclePolicy(retention=ExpireIf(when=predicate)),
    )
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_02", "2024-02-01", "2024-03-01", oid=2))

    # Act
    await service.plan(config, mode=mode, now=NOW)

    # Assert
    metadata.measure.assert_not_awaited()


async def test__plan__policy_that_needs_no_facts__never_measures(
    service: PartitionLifecycleService, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_02", "2024-02-01", "2024-03-01", oid=2))

    # Act
    await service.plan(config, now=NOW)

    # Assert
    metadata.measure.assert_not_awaited()


async def test__plan__takes_no_lock_and_issues_no_ddl(
    service: PartitionLifecycleService, repo: MagicMock, locks: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange / Act
    await service.plan(config, now=NOW)

    # Assert
    locks.acquire_lock.assert_not_called()
    repo.create_table_like.assert_not_awaited()
    repo.attach_partition.assert_not_awaited()


# ── inspect ─────────────────────────────────────────────────────────────────────


async def test__inspect__returns_the_tree_without_measuring(
    service: PartitionLifecycleService, metadata: MagicMock
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        lifecycle=LifecyclePolicy(retention=ExpireIf(when=SizeAbove(bytes=10))),
    )
    tree = _tree(_range_child("events__2024_02", "2024-02-01", "2024-03-01", oid=2))
    metadata.get_actual_tree.return_value = tree

    # Act
    result = await service.inspect(config)

    # Assert
    assert result is tree
    metadata.measure.assert_not_awaited()


async def test__inspect__table_not_partitioned__returns_none(
    service: PartitionLifecycleService, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = None

    # Act / Assert
    assert await service.inspect(config) is None


# ── apply ───────────────────────────────────────────────────────────────────────


async def test__apply__holds_the_table_lock_while_the_executor_runs(
    service: PartitionLifecycleService, locks: MagicMock, events: list[str], config: TablePartitionConfig
) -> None:
    # Arrange
    plan = _plan_with_every_kind()
    expected = MaintenanceResult(created_count=1)

    async def _apply(*args: object, **kwargs: object) -> MaintenanceResult:
        events.append("apply")
        return expected

    service._executor.apply = AsyncMock(side_effect=_apply)  # type: ignore[method-assign]

    # Act
    result = await service.apply(config, plan, continue_on_error=True)

    # Assert
    assert result is expected
    locks.acquire_lock.assert_called_once_with("events")
    assert events == ["acquire", "apply", "release"]
    service._executor.apply.assert_awaited_once_with(config, plan, continue_on_error=True)


async def test__apply__executor_fails__lock_is_still_released(
    service: PartitionLifecycleService, events: list[str], config: TablePartitionConfig
) -> None:
    # Arrange
    service._executor.apply = AsyncMock(side_effect=SQLAlchemyError("boom"))  # type: ignore[method-assign]

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="boom"):
        await service.apply(config, _plan_with_every_kind())

    assert events == ["acquire", "release"]


# ── maintain ────────────────────────────────────────────────────────────────────


async def test__maintain__plans_and_applies_under_one_lock(
    service: PartitionLifecycleService, locks: MagicMock, events: list[str], config: TablePartitionConfig
) -> None:
    # Arrange
    plan = _plan_with_every_kind()

    async def _plan(*args: object, **kwargs: object) -> MaintenancePlan:
        events.append("plan")
        return plan

    async def _apply(*args: object, **kwargs: object) -> MaintenanceResult:
        events.append("apply")
        return MaintenanceResult()

    service.plan = AsyncMock(side_effect=_plan)  # type: ignore[method-assign]
    service._executor.apply = AsyncMock(side_effect=_apply)  # type: ignore[method-assign]

    # Act
    await service.maintain(config, continue_on_error=True)

    # Assert
    assert locks.acquire_lock.call_count == 1
    assert events == ["acquire", "plan", "apply", "release"]
    service.plan.assert_awaited_once_with(config)
    applied = service._executor.apply.call_args
    assert applied.args[1].operations == plan.operations
    assert applied.kwargs == {"continue_on_error": True}


@pytest.mark.parametrize(
    "flags,expected_kinds",
    [
        ({}, ["create", "attach", "detach", "drop", "drop"]),
        ({"skip_create": True}, ["detach", "drop", "drop"]),
        ({"skip_detach": True}, ["create", "attach", "drop"]),
        ({"skip_drop": True}, ["create", "attach", "detach"]),
        ({"skip_create": True, "skip_detach": True, "skip_drop": True}, []),
    ],
)
async def test__maintain__skip_flags__filter_the_plan_before_it_is_applied(
    service: PartitionLifecycleService,
    config: TablePartitionConfig,
    flags: dict[str, bool],
    expected_kinds: list[str],
) -> None:
    # Arrange
    service.plan = AsyncMock(return_value=_plan_with_every_kind())  # type: ignore[method-assign]
    service._executor.apply = AsyncMock(return_value=MaintenanceResult())  # type: ignore[method-assign]

    # Act
    await service.maintain(config, **flags)

    # Assert -- skip_detach also drops the drop that would have followed the detach
    applied = service._executor.apply.call_args.args[1]
    assert [op.kind.value for op in applied.operations] == expected_kinds


async def test__maintain__skip_detach__keeps_the_orphan_drop(
    service: PartitionLifecycleService, config: TablePartitionConfig
) -> None:
    # Arrange
    service.plan = AsyncMock(return_value=_plan_with_every_kind())  # type: ignore[method-assign]
    service._executor.apply = AsyncMock(return_value=MaintenanceResult())  # type: ignore[method-assign]

    # Act
    await service.maintain(config, skip_detach=True)

    # Assert
    applied = service._executor.apply.call_args.args[1]
    assert [op.target for op in applied.drops] == ["events__2023_12"]


async def test__maintain__end_to_end__returns_counters_and_the_plan_it_executed(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange -- January is expired, February current-ish, March missing; an orphan is past its grace
    metadata.get_actual_tree.return_value = _tree(
        _range_child("events__2024_01", "2024-01-01", "2024-02-01", oid=1),
        _range_child("events__2024_02", "2024-02-01", "2024-03-01", oid=2),
        orphans=(DetachedPartition(name="events__2023_12", oid=55, parent_name="events"),),
    )
    metadata.get_relation_oid.return_value = 1
    metadata.is_partition_attached.return_value = True

    # Act
    with patch("pg_partsmith.aio.services.inspection.datetime") as clock:
        clock.now.return_value = NOW
        result = await service.maintain(config)

    # Assert
    assert result.success
    assert (result.created_count, result.detached_count, result.dropped_count) == (1, 1, 2)
    assert result.issues == ()
    assert isinstance(result.plan, MaintenancePlan)
    assert result.maintenance_plan is result.plan
    assert [op.target for op in result.plan.creates] == ["events__2024_03"]
    repo.create_table_like.assert_awaited_once_with("events", "events__2024_03", None)
    repo.attach_partition.assert_awaited_once_with("events", "events__2024_03", MARCH, key_arity=1)
    repo.detach_partition.assert_awaited_once_with("events", "events__2024_01", mode=DetachMode.AUTO)
    assert [call.args[0] for call in repo.drop_partition.call_args_list] == ["events__2024_01", "events__2023_12"]


async def test__maintain__validation_failure__is_fatal_even_when_continuing_on_error(
    service: PartitionLifecycleService, metadata: MagicMock, repo: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_partition_type.return_value = None

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="not partitioned"):
        await service.maintain(config, continue_on_error=True)

    repo.create_table_like.assert_not_awaited()


async def test__maintain__continue_on_error__isolates_a_failed_create_and_still_prunes(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_01", "2024-01-01", "2024-02-01", oid=1))
    metadata.get_relation_oid.return_value = 1
    metadata.is_partition_attached.return_value = True
    repo.create_table_like.side_effect = SQLAlchemyError("create failed")

    # Act
    with patch("pg_partsmith.aio.services.inspection.datetime") as clock:
        clock.now.return_value = NOW
        result = await service.maintain(config, continue_on_error=True)

    # Assert
    assert result.success
    assert result.created_count == 0
    assert result.detached_count == 1
    assert result.dropped_count == 1
    assert [issue.error for issue in result.issues] == ["SQLAlchemyError: create failed"]


async def test__maintain_lifecycle__is_an_alias_of_maintain(
    service: PartitionLifecycleService, config: TablePartitionConfig
) -> None:
    # Arrange
    expected = MaintenanceResult(created_count=3)
    service.maintain = AsyncMock(return_value=expected)  # type: ignore[method-assign]

    # Act
    result = await service.maintain_lifecycle(config, skip_create=True, skip_drop=True, continue_on_error=True)

    # Assert
    assert result is expected
    service.maintain.assert_awaited_once_with(
        config, skip_create=True, skip_detach=False, skip_drop=True, continue_on_error=True
    )


async def test__maintain__hooks_given_to_the_service__reach_the_executor(
    repo: MagicMock, metadata: MagicMock, locks: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    seen: list[str] = []

    class _Hooks(BasePartitionLifecycleHooks):
        async def after_create(self, cfg: TablePartitionConfig, partition: PartitionInfo) -> None:
            seen.append(partition.name)

    service = PartitionLifecycleService(repo, metadata, locks, hooks=[_Hooks()])

    # Act
    with patch("pg_partsmith.aio.services.inspection.datetime") as clock:
        clock.now.return_value = NOW
        await service.maintain(config)

    # Assert
    assert seen == ["events__2024_03"]


# ── reconcile ───────────────────────────────────────────────────────────────────


async def test__reconcile__plans_in_reconcile_mode_without_a_lock(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, locks: MagicMock
) -> None:
    # Arrange
    branch = _range_child(
        "events__2024_03",
        "2024-03-01",
        "2024-04-01",
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=(PartitionNode(name="events__2024_03__h0", bounds=HashBounds(modulus=2, remainder=0)),),
    )
    metadata.get_actual_tree.return_value = _tree(branch)
    service.plan = AsyncMock(wraps=service.plan)  # type: ignore[method-assign]

    # Act
    result = await service.reconcile(_nested_config())

    # Assert
    service.plan.assert_awaited_once_with(_nested_config(), mode=PlanMode.RECONCILE)
    locks.acquire_lock.assert_not_called()
    assert result.repaired_count == 1
    assert result.created_count == 0
    repo.create_table_like.assert_awaited_once_with("events__2024_03", "events__2024_03__h1", None)
    repo.detach_partition.assert_not_awaited()


# ── ensure_partition / ensure_partitions ────────────────────────────────────────


async def test__ensure_partitions__periods_on_a_time_root__creates_each_window_and_returns_them(
    service: PartitionLifecycleService, repo: MagicMock, locks: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    periods = [Period(year=2024, month=1), Period(year=2024, month=2)]

    # Act
    created = await service.ensure_partitions(config, periods)

    # Assert
    assert [p.name for p in created] == ["events__2024_01", "events__2024_02"]
    assert created[0] == PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        bounds=RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
        boundaries_expr="range",
        is_attached=True,
        parent_table="events",
    )
    assert [call.args[1] for call in repo.create_table_like.call_args_list] == ["events__2024_01", "events__2024_02"]
    locks.acquire_lock.assert_not_called()


async def test__ensure_partitions__windows__are_accepted_as_they_are(
    service: PartitionLifecycleService, repo: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    window = Window(start=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 2, 1, tzinfo=UTC))

    # Act
    created = await service.ensure_partitions(config, [window])

    # Assert
    assert [p.name for p in created] == ["events__2024_01"]
    repo.attach_partition.assert_awaited_once_with(
        "events", "events__2024_01", RangeBounds(from_value="2024-01-01", to_value="2024-02-01"), key_arity=1
    )


async def test__ensure_partitions__windows_on_a_numeric_root__are_created(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_columns.return_value = ("id",)
    metadata.get_actual_tree.return_value = ActualTree(
        root=PartitionNode(name="queue", partition_type=PartitionType.RANGE, partition_columns=("id",))
    )

    # Act
    created = await service.ensure_partitions(_numeric_config(), [Window(start=100_000, end=200_000)])

    # Assert
    assert [p.name for p in created] == ["queue__100000"]
    assert created[0].bounds == RangeBounds(from_value="100000", to_value="200000")


async def test__ensure_partitions__period_on_a_numeric_root__is_refused(
    service: PartitionLifecycleService, metadata: MagicMock
) -> None:
    # Arrange / Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="not partitioned by time"):
        await service.ensure_partitions(_numeric_config(), [Period(year=2024, month=1)])

    metadata.get_partition_type.assert_not_awaited()


@pytest.mark.parametrize(
    "scheme",
    [
        HashPartitioning(key="tenant_id", modulus=4),
        ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("de",)),)),
    ],
)
async def test__ensure_partitions__non_range_root__is_refused(
    service: PartitionLifecycleService, metadata: MagicMock, scheme: HashPartitioning | ListPartitioning
) -> None:
    # Arrange
    config = TablePartitionConfig(table_name="tasks", scheme=scheme)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="RANGE root"):
        await service.ensure_partitions(config, [Period(year=2024, month=1)])

    metadata.get_partition_type.assert_not_awaited()


async def test__ensure_partitions__existing_window__is_absent_from_the_result(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_01", "2024-01-01", "2024-02-01", oid=1))

    # Act
    created = await service.ensure_partitions(config, [Period(year=2024, month=1), Period(year=2024, month=2)])

    # Assert
    assert [p.name for p in created] == ["events__2024_02"]
    repo.create_table_like.assert_awaited_once()


async def test__ensure_partitions__window_that_could_not_be_created__is_absent_from_the_result(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange -- the January name is held by a relation with other bounds; a topology conflict never aborts
    async def _create(template: str, name: str, partition_by: object) -> None:
        if name == "events__2024_01":
            raise PartitionAlreadyExistsError(name)

    repo.create_table_like.side_effect = _create
    metadata.is_partition_attached.return_value = True
    metadata.get_partition_tree.return_value = PartitionNode(
        name="events__2024_01", bounds=RangeBounds(from_value="2024-01-01", to_value="2024-01-15")
    )

    # Act
    created = await service.ensure_partitions(config, [Period(year=2024, month=1), Period(year=2024, month=2)])

    # Assert
    assert [p.name for p in created] == ["events__2024_02"]


async def test__ensure_partitions__repeated_period__is_created_once(
    service: PartitionLifecycleService, repo: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    period = Period(year=2024, month=5)

    # Act
    created = await service.ensure_partitions(config, [period, period, period])

    # Assert
    assert [p.name for p in created] == ["events__2024_05"]
    assert repo.create_table_like.await_count == 1


async def test__ensure_partitions__windows_come_back_in_chronological_order(
    service: PartitionLifecycleService, config: TablePartitionConfig
) -> None:
    # Arrange
    periods = [Period(year=2024, month=6), Period(year=2024, month=2), Period(year=2024, month=4)]

    # Act
    created = await service.ensure_partitions(config, periods)

    # Assert
    assert [p.name for p in created] == ["events__2024_02", "events__2024_04", "events__2024_06"]


async def test__ensure_partitions__no_periods__does_nothing(
    service: PartitionLifecycleService, repo: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange / Act
    created = await service.ensure_partitions(config, [])

    # Assert
    assert created == []
    repo.create_table_like.assert_not_awaited()


async def test__ensure_partitions__expired_partition_in_the_tree__is_left_alone(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2023_01", "2023-01-01", "2023-02-01", oid=1))

    # Act
    await service.ensure_partitions(config, [Period(year=2024, month=6)])

    # Assert
    repo.detach_partition.assert_not_awaited()
    repo.drop_partition.assert_not_awaited()


async def test__ensure_partitions__nested_scheme__builds_the_subtree_and_reports_the_branch(
    service: PartitionLifecycleService, repo: MagicMock
) -> None:
    # Arrange / Act
    created = await service.ensure_partitions(_nested_config(), [Period(year=2024, month=1)])

    # Assert
    assert [p.name for p in created] == ["events__2024_01"]
    assert created[0].subpartition_type is PartitionType.HASH
    assert [call.args[1] for call in repo.create_table_like.call_args_list] == [
        "events__2024_01",
        "events__2024_01__h0",
        "events__2024_01__h1",
    ]


async def test__ensure_partition__new_period__returns_the_created_partition(
    service: PartitionLifecycleService, config: TablePartitionConfig
) -> None:
    # Arrange / Act
    result = await service.ensure_partition(config, Period(year=2024, month=7))

    # Assert
    assert result is not None
    assert result.name == "events__2024_07"
    assert result.is_attached is True


async def test__ensure_partition__existing_period__returns_none(
    service: PartitionLifecycleService, metadata: MagicMock, repo: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_07", "2024-07-01", "2024-08-01", oid=7))

    # Act
    result = await service.ensure_partition(config, Period(year=2024, month=7))

    # Assert
    assert result is None
    repo.create_table_like.assert_not_awaited()


# ── granular steps ──────────────────────────────────────────────────────────────


async def test__create_future_partitions__runs_only_the_creation_half_of_the_plan(
    service: PartitionLifecycleService,
    repo: MagicMock,
    metadata: MagicMock,
    locks: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange -- January would be detached and an orphan dropped by a full run
    metadata.get_actual_tree.return_value = _tree(
        _range_child("events__2024_01", "2024-01-01", "2024-02-01", oid=1),
        orphans=(DetachedPartition(name="events__2023_12", oid=55, parent_name="events"),),
    )

    # Act
    with patch("pg_partsmith.aio.services.inspection.datetime") as clock:
        clock.now.return_value = NOW
        created = await service.create_future_partitions(config)

    # Assert
    assert [p.name for p in created] == ["events__2024_03"]
    repo.detach_partition.assert_not_awaited()
    repo.drop_partition.assert_not_awaited()
    locks.acquire_lock.assert_not_called()


async def test__create_future_partitions__orphan_for_a_wanted_window__is_reattached_not_recreated(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(
        orphans=(DetachedPartition(name="events__2024_03", oid=66, parent_name="events"),)
    )
    metadata.get_relation_oid.return_value = 66

    # Act
    with patch("pg_partsmith.aio.services.inspection.datetime") as clock:
        clock.now.return_value = NOW
        created = await service.create_future_partitions(config)

    # Assert
    assert created == []
    repo.create_table_like.assert_not_awaited()
    repo.attach_partition.assert_awaited_once_with("events", "events__2024_03", MARCH, key_arity=1)


async def test__create_future_partitions__everything_exists__returns_nothing(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_03", "2024-03-01", "2024-04-01", oid=3))

    # Act
    with patch("pg_partsmith.aio.services.inspection.datetime") as clock:
        clock.now.return_value = NOW
        created = await service.create_future_partitions(config)

    # Assert
    assert created == []
    repo.create_table_like.assert_not_awaited()


async def test__create_future_partitions__db_error__propagates(
    service: PartitionLifecycleService, repo: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    repo.create_table_like.side_effect = SQLAlchemyError("create failed")

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="create failed"):
        await service.create_future_partitions(config)


async def test__get_partitions_for_pruning__expired_members_first_then_orphans_past_grace(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(
        _range_child("events__2024_01", "2024-01-01", "2024-02-01", oid=1),
        _range_child("events__2024_02", "2024-02-01", "2024-03-01", oid=2),
        _range_child("events__2024_03", "2024-03-01", "2024-04-01", oid=3),
        orphans=(DetachedPartition(name="events__2023_12", oid=55, parent_name="events"),),
    )

    # Act
    with patch("pg_partsmith.aio.services.inspection.datetime") as clock:
        clock.now.return_value = NOW
        to_prune = await service.get_partitions_for_pruning(config)

    # Assert -- the drop that follows January's detach is not listed twice
    assert [p.name for p in to_prune] == ["events__2024_01", "events__2023_12"]
    expired, orphan = to_prune
    assert expired == PartitionInfo(
        name="events__2024_01",
        oid=1,
        partition_type=PartitionType.RANGE,
        bounds=RangeBounds(from_value="2024-01-01", to_value="2024-02-01"),
        boundaries_expr="range",
        is_attached=True,
        parent_table="events",
    )
    assert orphan == PartitionInfo(
        name="events__2023_12", oid=55, partition_type=PartitionType.RANGE, is_attached=False, parent_table="events"
    )
    repo.detach_partition.assert_not_awaited()
    repo.drop_partition.assert_not_awaited()


async def test__get_partitions_for_pruning__nothing_expired__returns_empty(
    service: PartitionLifecycleService, metadata: MagicMock, config: TablePartitionConfig
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_range_child("events__2024_02", "2024-02-01", "2024-03-01", oid=2))

    # Act
    with patch("pg_partsmith.aio.services.inspection.datetime") as clock:
        clock.now.return_value = NOW
        to_prune = await service.get_partitions_for_pruning(config)

    # Assert
    assert to_prune == []


async def test__detach_old_partitions__attached_partition__is_detached_via_its_own_parent(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.is_partition_attached.return_value = True
    partition = PartitionInfo(
        name="public.events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
        is_attached=True,
        parent_table="public.events",
    )

    # Act
    detached = await service.detach_old_partitions("events", [partition])

    # Assert
    assert detached == ["public.events__2024_01"]
    repo.detach_partition.assert_awaited_once_with("public.events", "public.events__2024_01", mode=DetachMode.AUTO)
    metadata.is_partition_attached.assert_awaited_once_with("public.events", "public.events__2024_01")


async def test__detach_old_partitions__no_parent_on_the_partition__falls_back_to_the_table(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.is_partition_attached.return_value = True
    partition = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
        is_attached=True,
    )

    # Act
    await service.detach_old_partitions("events", [partition])

    # Assert
    repo.detach_partition.assert_awaited_once_with("events", "events__2024_01", mode=DetachMode.AUTO)


async def test__detach_old_partitions__already_detached_input__counted_without_ddl(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    partition = PartitionInfo(name="events__2024_01", partition_type=PartitionType.RANGE, is_attached=False)

    # Act
    detached = await service.detach_old_partitions("events", [partition])

    # Assert
    assert detached == ["events__2024_01"]
    repo.detach_partition.assert_not_awaited()
    metadata.is_partition_attached.assert_not_awaited()


async def test__detach_old_partitions__db_error__propagates(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.is_partition_attached.return_value = True
    repo.detach_partition.side_effect = SQLAlchemyError("db error")
    partition = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
        is_attached=True,
    )

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="db error"):
        await service.detach_old_partitions("events", [partition])


async def test__detach_old_partitions__hooks_fire_around_the_detach(
    repo: MagicMock, metadata: MagicMock, locks: MagicMock
) -> None:
    # Arrange
    calls: list[str] = []

    class _Hooks(BasePartitionLifecycleHooks):
        async def before_detach(self, table_name: str, partition: PartitionInfo) -> None:
            calls.append(f"before_detach:{table_name}:{partition.name}")

        async def after_detach(self, table_name: str, partition_name: str) -> None:
            calls.append(f"after_detach:{table_name}:{partition_name}")

    metadata.is_partition_attached.return_value = True
    service = PartitionLifecycleService(repo, metadata, locks, hooks=[_Hooks()])
    partition = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
        is_attached=True,
    )

    # Act
    await service.detach_old_partitions("events", [partition])

    # Assert
    assert calls == ["before_detach:events:events__2024_01", "after_detach:events:events__2024_01"]


async def test__detach_old_partitions__attached_partition_with_only_a_raw_bound__is_still_detached(
    service: PartitionLifecycleService, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the catalog bound could not be parsed, so only the raw expression is known
    metadata.is_partition_attached.return_value = True
    partition = PartitionInfo(
        name="events__weird",
        partition_type=PartitionType.RANGE,
        boundaries_expr="FOR VALUES FROM (weird) ???",
        is_attached=True,
        parent_table="events",
    )

    # Act
    detached = await service.detach_old_partitions("events", [partition])

    # Assert
    assert detached == ["events__weird"]
    repo.detach_partition.assert_awaited_once_with("events", "events__weird", mode=DetachMode.AUTO)


async def test__drop_detached_partitions__drops_each_and_counts(
    service: PartitionLifecycleService, repo: MagicMock
) -> None:
    # Arrange / Act
    count = await service.drop_detached_partitions("events", ["events__2023_11", "events__2023_12"])

    # Assert
    assert count == 2
    assert [call.args[0] for call in repo.drop_partition.call_args_list] == ["events__2023_11", "events__2023_12"]
    assert repo.drop_partition.call_args.kwargs == {"expected_oid": None}


async def test__drop_detached_partitions__still_attached__is_skipped(
    service: PartitionLifecycleService, repo: MagicMock
) -> None:
    # Arrange
    repo.drop_partition.side_effect = [PartitionAttachedError("events__2024_04", "events"), None]

    # Act
    count = await service.drop_detached_partitions("events", ["events__2024_04", "events__2023_12"])

    # Assert
    assert count == 1


async def test__drop_detached_partitions__db_error__propagates(
    service: PartitionLifecycleService, repo: MagicMock
) -> None:
    # Arrange
    repo.drop_partition.side_effect = SQLAlchemyError("drop error")

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="drop error"):
        await service.drop_detached_partitions("events", ["events__2024_04"])


async def test__drop_detached_partitions__hooks_fire_in_registration_order(
    repo: MagicMock, metadata: MagicMock, locks: MagicMock
) -> None:
    # Arrange
    calls: list[str] = []

    class _HookA(BasePartitionLifecycleHooks):
        async def before_drop(self, table_name: str, partition_name: str) -> None:
            calls.append("A:before")

        async def after_drop(self, table_name: str, partition_name: str) -> None:
            calls.append("A:after")

    class _HookB(BasePartitionLifecycleHooks):
        async def before_drop(self, table_name: str, partition_name: str) -> None:
            calls.append("B:before")

    service = PartitionLifecycleService(repo, metadata, locks, hooks=[_HookA(), _HookB()])

    # Act
    await service.drop_detached_partitions("events", ["events__2024_04"])

    # Assert
    assert calls == ["A:before", "B:before", "A:after"]
