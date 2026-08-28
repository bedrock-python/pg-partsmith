"""Unit tests for the sync ``PlanExecutor``: how a plan becomes DDL, hooks, counters and issues."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.boundaries import NumericBoundaries, TimeBoundaries
from pg_partsmith.entities import (
    DefaultBounds,
    HashBounds,
    ListBounds,
    MaintenanceIssueStep,
    PartitionGranularity,
    PartitionInfo,
    PartitionNode,
    PartitionType,
    RangeBounds,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import (
    PartitionAlreadyExistsError,
    PartitionAttachedError,
    PartitionReferencedError,
    PartitionTopologyError,
    PlanStaleError,
)
from pg_partsmith.leaves import ForeignLeaves, LocalLeaves
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.plan import (
    AttachPartition,
    CreatePartition,
    DetachPartition,
    DropPartition,
    Finding,
    FindingReason,
    MaintenancePlan,
    Operation,
    PartitionBy,
    Reason,
)
from pg_partsmith.scheme import HashPartitioning, RangePartitioning
from pg_partsmith.sync.hooks import BasePartitionLifecycleHooks
from pg_partsmith.sync.services.base import BasePartitionService
from pg_partsmith.sync.services.execution import PlanExecutor
from pg_partsmith.topology import PartitionBounds

NOW = datetime(2024, 3, 15, 12, 0, tzinfo=UTC)
APRIL = RangeBounds(from_value="2024-04-01", to_value="2024-05-01")
MARCH = RangeBounds(from_value="2024-03-01", to_value="2024-04-01")

# ── fixtures and builders ────────────────────────────────────────────────────────


class _RecordingHooks(BasePartitionLifecycleHooks):
    """Hooks that remember every call with the payload they were handed."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def before_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
        self.calls.append(("before_create", partition))

    def after_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
        self.calls.append(("after_create", partition))

    def before_detach(self, table_name: str, partition: PartitionInfo) -> None:
        self.calls.append(("before_detach", (table_name, partition)))

    def after_detach(self, table_name: str, partition_name: str) -> None:
        self.calls.append(("after_detach", (table_name, partition_name)))

    def before_drop(self, table_name: str, partition_name: str) -> None:
        self.calls.append(("before_drop", (table_name, partition_name)))

    def after_drop(self, table_name: str, partition_name: str) -> None:
        self.calls.append(("after_drop", (table_name, partition_name)))

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


class _BoomError(Exception):
    """An exception outside the (ValueError, TypeError, RuntimeError) family."""


@pytest.fixture
def repo() -> MagicMock:
    repo = MagicMock()
    repo.create_table_like = MagicMock(return_value=None)
    repo.create_foreign_table_like = MagicMock(return_value=None)
    repo.attach_partition = MagicMock(return_value=None)
    repo.detach_partition = MagicMock(return_value=None)
    repo.drop_partition = MagicMock(return_value=None)
    repo.reconcile_default_rows = MagicMock(return_value=0)
    return repo


@pytest.fixture
def metadata() -> MagicMock:
    metadata = MagicMock()
    metadata.is_partition_attached = MagicMock(return_value=False)
    metadata.get_partition_tree = MagicMock(return_value=None)
    metadata.get_default_partition = MagicMock(return_value=None)
    metadata.get_relation_oid = MagicMock(return_value=None)
    return metadata


@pytest.fixture
def hooks() -> _RecordingHooks:
    return _RecordingHooks()


@pytest.fixture
def executor(repo: MagicMock, metadata: MagicMock, hooks: _RecordingHooks) -> PlanExecutor:
    return PlanExecutor(repo, metadata, hooks=[hooks])


def _config() -> TablePartitionConfig:
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
        subpartition=HashPartitioning(key="tenant_id", modulus=2),
    )


def _hash_root_config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        scheme=HashPartitioning(
            key="tenant_id",
            modulus=2,
            child=RangePartitioning(
                key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)
            ),
        ),
    )


def _plan(*operations: Operation, findings: tuple[Finding, ...] = ()) -> MaintenancePlan:
    return MaintenancePlan(table_name="events", generated_at=NOW, operations=tuple(operations), findings=findings)


def _create_op(
    target: str = "events__2024_04",
    *,
    parent: str = "events",
    bounds: PartitionBounds | None = None,
    partition_by: PartitionBy | None = None,
    key_columns: tuple[str, ...] = ("created_at",),
    children: tuple[CreatePartition, ...] = (),
    counts_as: str = "created",
) -> CreatePartition:
    return CreatePartition(
        target=target,
        parent_name=parent,
        bounds=APRIL if bounds is None else bounds,
        partition_by=partition_by,
        key_columns=key_columns,
        children=children,
        counts_as=counts_as,  # type: ignore[arg-type]
        reason=Reason.CREATE_AHEAD,
    )


def _branch_op() -> CreatePartition:
    """An April branch subpartitioned into two hash buckets."""
    return _create_op(
        partition_by=PartitionBy(method=PartitionType.HASH, columns=("tenant_id",)),
        children=(
            _create_op(
                "events__2024_04__h0",
                parent="events__2024_04",
                bounds=HashBounds(modulus=2, remainder=0),
                key_columns=("tenant_id",),
                counts_as="subtree",
            ),
            _create_op(
                "events__2024_04__h1",
                parent="events__2024_04",
                bounds=HashBounds(modulus=2, remainder=1),
                key_columns=("tenant_id",),
                counts_as="subtree",
            ),
        ),
    )


def _attach_op(oid: int | None = 77, parent: str = "events", target: str = "events__2024_04") -> AttachPartition:
    return AttachPartition(
        target=target, oid=oid, parent_name=parent, bounds=APRIL, key_columns=("created_at",), reason=Reason.REATTACH
    )


def _detach_op(
    oid: int | None = 77, bounds: PartitionBounds | None = MARCH, mode: DetachMode = DetachMode.AUTO
) -> DetachPartition:
    return DetachPartition(
        target="events__2024_03",
        oid=oid,
        parent_name="events",
        mode=mode,
        bounds=bounds,
        reason=Reason.RETENTION_EXPIRED,
    )


def _drop_op(target: str = "events__2023_12", *, oid: int | None = 55, follows_detach: bool = False) -> DropPartition:
    return DropPartition(target=target, oid=oid, reason=Reason.GRACE_ELAPSED, follows_detach=follows_detach)


def _sqlstate_error(sqlstate: str, message: str = "pg error") -> SQLAlchemyError:
    exc = SQLAlchemyError(message)
    orig = MagicMock()
    orig.sqlstate = sqlstate
    exc.orig = orig  # type: ignore[attr-defined]
    return exc


def _default_conflict() -> SQLAlchemyError:
    return _sqlstate_error("23514", "updated partition constraint for default partition would be violated by some row")


def _default_partition() -> PartitionInfo:
    return PartitionInfo(name="events_default", partition_type=PartitionType.RANGE, is_default=True, is_attached=True)


def _record_ddl(repo: MagicMock) -> list[str]:
    """Make the repository log creates and attaches in the order they happen."""
    order: list[str] = []

    def _create(template: str, name: str, partition_by: PartitionBy | None) -> None:
        order.append(f"create {name}")

    def _attach(parent: str, name: str, bounds: PartitionBounds, *, key_arity: int) -> None:
        order.append(f"attach {name}")

    repo.create_table_like.side_effect = _create
    repo.attach_partition.side_effect = _attach
    return order


# ── create: attach-last, key arity, hooks, counters ─────────────────────────────


def test__apply__create_op__creates_table_then_attaches_with_key_arity(executor: PlanExecutor, repo: MagicMock) -> None:
    # Arrange
    op = _create_op(key_columns=("created_at", "tenant_id"))

    # Act
    result = executor.apply(_config(), _plan(op))

    # Assert
    repo.create_table_like.assert_called_once_with("events", "events__2024_04", None)
    repo.attach_partition.assert_called_once_with("events", "events__2024_04", APRIL, key_arity=2)
    assert result.created_count == 1
    assert result.issues == ()
    assert result.plan is not None


def test__apply__create_op_without_key_columns__attaches_with_arity_one(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange
    op = _create_op(key_columns=())

    # Act
    executor.apply(_config(), _plan(op))

    # Assert
    assert repo.attach_partition.call_args.kwargs["key_arity"] == 1


def test__apply__branch_with_children__children_are_attached_before_the_parent(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange
    order = _record_ddl(repo)

    # Act
    result = executor.apply(_nested_config(), _plan(_branch_op()))

    # Assert -- attach-last: the branch goes live only once every bucket is in place
    assert order == [
        "create events__2024_04",
        "create events__2024_04__h0",
        "attach events__2024_04__h0",
        "create events__2024_04__h1",
        "attach events__2024_04__h1",
        "attach events__2024_04",
    ]
    assert repo.create_table_like.call_args_list[0].args == ("events", "events__2024_04", _branch_op().partition_by)
    assert result.created_count == 1
    assert result.repaired_count == 0


def test__apply__create_op__hooks_receive_the_partition_before_and_after(
    executor: PlanExecutor, hooks: _RecordingHooks
) -> None:
    # Arrange
    config = _nested_config()

    # Act
    executor.apply(config, _plan(_branch_op()))

    # Assert -- one pair of calls for the lifecycle unit, none for its buckets
    assert hooks.names() == ["before_create", "after_create"]
    before = hooks.calls[0][1]
    after = hooks.calls[1][1]
    assert before == PartitionInfo(
        name="events__2024_04",
        partition_type=PartitionType.RANGE,
        bounds=APRIL,
        boundaries_expr="range",
        is_attached=False,
        subpartition_type=PartitionType.HASH,
        parent_table="events",
    )
    assert before.from_value == "2024-04-01"
    assert before.to_value == "2024-05-01"
    assert after == before.model_copy(update={"is_attached": True})


@pytest.mark.parametrize("counts_as", ["repaired", "subtree"])
def test__apply__create_op_not_counting_as_created__fires_no_hooks(
    executor: PlanExecutor, hooks: _RecordingHooks, counts_as: str
) -> None:
    # Arrange
    op = _create_op(counts_as=counts_as)

    # Act
    result = executor.apply(_config(), _plan(op))

    # Assert
    assert hooks.calls == []
    assert result.created_count == 0
    assert result.repaired_count == (1 if counts_as == "repaired" else 0)


def test__apply__hash_and_list_and_default_bounds__partition_info_reports_the_parent_method(
    executor: PlanExecutor, hooks: _RecordingHooks
) -> None:
    # Arrange
    ops = (
        _create_op("events__h1", bounds=HashBounds(modulus=2, remainder=1), key_columns=("tenant_id",)),
        _create_op("events__eu", bounds=ListBounds(values=("de", "fr")), key_columns=("region",)),
        _create_op("events__other", bounds=DefaultBounds(), key_columns=("region",)),
    )

    # Act
    executor.apply(_config(), _plan(*ops))

    # Assert
    infos = [partition for name, partition in hooks.calls if name == "before_create"]
    assert [p.partition_type for p in infos] == [PartitionType.HASH, PartitionType.LIST, PartitionType.RANGE]
    assert [p.is_default for p in infos] == [False, False, True]
    assert [p.boundaries_expr for p in infos] == ["hash", "list", "default"]


def test__apply__before_create_hook_raises__create_never_happens(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    class _Refusing(BasePartitionLifecycleHooks):
        def before_create(self, config: TablePartitionConfig, partition: PartitionInfo) -> None:
            raise RuntimeError("hook failed")

    executor = PlanExecutor(repo, metadata, hooks=[_Refusing()])

    # Act / Assert
    with pytest.raises(RuntimeError, match="hook failed"):
        executor.apply(_config(), _plan(_create_op()))

    repo.create_table_like.assert_not_called()


def test__apply__multiple_hooks__fire_in_registration_order(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    first, second = _RecordingHooks(), _RecordingHooks()
    seen: list[str] = []
    first.calls = _Appending(seen, "A")  # type: ignore[assignment]
    second.calls = _Appending(seen, "B")  # type: ignore[assignment]
    executor = PlanExecutor(repo, metadata, hooks=[first, second])

    # Act
    executor.apply(_config(), _plan(_drop_op()))

    # Assert
    assert seen == ["A", "B", "A", "B"]


class _Appending(list):  # type: ignore[type-arg]
    """A list stand-in that reports which hook appended, in order."""

    def __init__(self, sink: list[str], label: str) -> None:
        super().__init__()
        self._sink = sink
        self._label = label

    def append(self, item: object) -> None:
        self._sink.append(self._label)
        super().append(item)


# ── create: the relation already exists ─────────────────────────────────────────


def test__apply__already_exists_and_attached_with_planned_bounds__is_a_benign_no_op(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange -- another worker won the race and its relation owns exactly our bounds
    repo.create_table_like.side_effect = PartitionAlreadyExistsError("events__2024_04")
    metadata.is_partition_attached.return_value = True
    metadata.get_partition_tree.return_value = PartitionNode(name="events__2024_04", bounds=APRIL)

    # Act
    result = executor.apply(_config(), _plan(_create_op()))

    # Assert
    assert result.created_count == 0
    assert result.issues == ()
    repo.attach_partition.assert_not_called()
    assert hooks.names() == ["before_create"]


def test__apply__already_exists_and_attached_with_other_bounds__records_topology_issue_and_continues(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the name is taken by a relation that is not the partition we planned
    other = RangeBounds(from_value="2024-04-01", to_value="2024-04-15")
    repo.create_table_like.side_effect = [PartitionAlreadyExistsError("events__2024_04"), None]
    metadata.is_partition_attached.return_value = True
    metadata.get_partition_tree.return_value = PartitionNode(name="events__2024_04", bounds=other)
    second = _create_op("events__2024_05", bounds=RangeBounds(from_value="2024-05-01", to_value="2024-06-01"))

    # Act
    result = executor.apply(_config(), _plan(_create_op(), second))

    # Assert -- recorded, never raised, and the next operation still ran
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.CREATE]
    assert result.issues[0].partition_name == "events__2024_04"
    assert "PartitionTopologyError" in result.issues[0].error
    assert "not the planned ones" in result.issues[0].error
    assert result.created_count == 1
    repo.attach_partition.assert_called_once()
    assert repo.attach_partition.call_args.args[1] == "events__2024_05"


def test__apply__already_exists_attached_but_unreadable__records_topology_issue(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the relation cannot even be inspected, so its bounds cannot be trusted
    repo.create_table_like.side_effect = PartitionAlreadyExistsError("events__2024_04")
    metadata.is_partition_attached.return_value = True
    metadata.get_partition_tree.return_value = None

    # Act
    result = executor.apply(_config(), _plan(_create_op()))

    # Assert
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.CREATE]
    assert result.created_count == 0


def test__apply__already_exists_not_attached_single_level__attaches_without_inspecting_a_subtree(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- a leaf left behind by an interrupted run has no subtree to complete
    repo.create_table_like.side_effect = PartitionAlreadyExistsError("events__2024_04")

    # Act
    result = executor.apply(_config(), _plan(_create_op()))

    # Assert
    metadata.get_partition_tree.assert_not_called()
    repo.attach_partition.assert_called_once_with("events", "events__2024_04", APRIL, key_arity=1)
    assert result.created_count == 1
    assert result.repaired_count == 0


def test__apply__already_exists_not_attached_branch__missing_buckets_are_repaired_then_attached(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the branch exists detached with one of its two buckets
    order = _record_ddl(repo)

    def _create(template: str, name: str, partition_by: PartitionBy | None) -> None:
        if name == "events__2024_04":
            raise PartitionAlreadyExistsError(name)
        order.append(f"create {name}")

    repo.create_table_like.side_effect = _create
    metadata.get_partition_tree.return_value = PartitionNode(
        name="events__2024_04",
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        is_attached=False,
        children=(PartitionNode(name="events__2024_04__h0", bounds=HashBounds(modulus=2, remainder=0)),),
    )

    # Act
    result = executor.apply(_nested_config(), _plan(_branch_op()))

    # Assert -- only the gap is filled, and the branch goes live last
    assert order == ["create events__2024_04__h1", "attach events__2024_04__h1", "attach events__2024_04"]
    metadata.get_partition_tree.assert_called_once_with("events__2024_04")
    h1_attach = repo.attach_partition.call_args_list[0]
    assert h1_attach.args == ("events__2024_04", "events__2024_04__h1", HashBounds(modulus=2, remainder=1))
    assert h1_attach.kwargs == {"key_arity": 1}
    assert result.created_count == 1
    assert result.repaired_count == 1


def test__apply__buckets_repaired_inside_a_detached_branch__fire_no_create_hooks(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange
    def _create(template: str, name: str, partition_by: PartitionBy | None) -> None:
        if name == "events__2024_04":
            raise PartitionAlreadyExistsError(name)

    repo.create_table_like.side_effect = _create
    metadata.get_partition_tree.return_value = PartitionNode(
        name="events__2024_04",
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        is_attached=False,
        children=(PartitionNode(name="events__2024_04__h0", bounds=HashBounds(modulus=2, remainder=0)),),
    )

    # Act
    executor.apply(_nested_config(), _plan(_branch_op()))

    # Assert -- the week is the lifecycle unit; its buckets are not
    assert [partition.name for _, partition in hooks.calls] == ["events__2024_04", "events__2024_04"]


def test__apply__already_exists_not_attached_branch_with_range_children__windows_are_replanned(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- a HASH root whose buckets hold monthly partitions; bucket 0 exists detached and empty
    order = _record_ddl(repo)

    def _create(template: str, name: str, partition_by: PartitionBy | None) -> None:
        if name == "events__h0":
            raise PartitionAlreadyExistsError(name)
        order.append(f"create {name}")

    repo.create_table_like.side_effect = _create
    metadata.get_partition_tree.return_value = PartitionNode(
        name="events__h0", partition_type=PartitionType.RANGE, partition_columns=("created_at",), is_attached=False
    )
    bucket = _create_op(
        "events__h0",
        bounds=HashBounds(modulus=2, remainder=0),
        partition_by=PartitionBy(method=PartitionType.RANGE, columns=("created_at",)),
        key_columns=("tenant_id",),
        children=(
            _create_op("events__h0__2024_03", parent="events__h0", bounds=MARCH, counts_as="subtree"),
            _create_op(
                "events__h0__2024_04",
                parent="events__h0",
                bounds=RangeBounds(from_value="MINVALUE", to_value="2024-05-01"),
                counts_as="subtree",
            ),
            _create_op("events__h0__other", parent="events__h0", bounds=DefaultBounds(), counts_as="subtree"),
        ),
    )

    # Act
    result = executor.apply(_hash_root_config(), _plan(bucket))

    # Assert -- the readable window is rebuilt from the planned children; the unreadable one is not guessed at
    assert order == ["create events__h0__2024_03", "attach events__h0__2024_03", "attach events__h0"]
    assert result.repaired_count == 1
    assert result.created_count == 1


def test__apply__already_exists_not_attached_but_tree_unreadable__attaches_without_repairs(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.create_table_like.side_effect = PartitionAlreadyExistsError("events__2024_04")
    metadata.get_partition_tree.return_value = None

    # Act
    result = executor.apply(_nested_config(), _plan(_branch_op()))

    # Assert
    repo.attach_partition.assert_called_once()
    assert result.repaired_count == 0
    assert result.created_count == 1


def test__apply__detached_branch_with_a_different_method__finding_becomes_an_issue(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the detached relation is LIST-partitioned where the scheme asks for HASH
    repo.create_table_like.side_effect = PartitionAlreadyExistsError("events__2024_04")
    metadata.get_partition_tree.return_value = PartitionNode(
        name="events__2024_04", partition_type=PartitionType.LIST, partition_columns=("tenant_id",), is_attached=False
    )

    # Act
    result = executor.apply(_nested_config(), _plan(_branch_op()))

    # Assert
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.RECONCILE]
    assert result.issues[0].partition_name == "events__2024_04"
    assert "asks for HASH" in result.issues[0].error
    assert result.repaired_count == 0
    repo.attach_partition.assert_called_once()


# ── attach: DEFAULT reconciliation ──────────────────────────────────────────────


def test__apply__default_conflict_on_range_attach__moves_rows_and_retries(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.attach_partition.side_effect = [_default_conflict(), None]
    repo.reconcile_default_rows.return_value = 5
    metadata.get_default_partition.return_value = _default_partition()

    # Act
    result = executor.apply(_config(), _plan(_create_op()))

    # Assert
    metadata.get_default_partition.assert_called_once_with("events")
    repo.reconcile_default_rows.assert_called_once_with(
        default_partition_name="events_default",
        target_partition_name="events__2024_04",
        key_columns=("created_at",),
        from_value="2024-04-01",
        to_value="2024-05-01",
    )
    assert repo.attach_partition.call_count == 2
    assert result.created_count == 1
    assert result.issues == ()


def test__apply__default_conflict_retries_exhausted__restores_rows_and_raises(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.attach_partition.side_effect = _default_conflict()
    repo.reconcile_default_rows.return_value = 5
    metadata.get_default_partition.return_value = _default_partition()
    logger = MagicMock()

    # Act / Assert
    with patch("pg_partsmith.sync.services.execution.logger", logger), pytest.raises(SQLAlchemyError):
        executor.apply(_config(), _plan(_create_op()))

    assert repo.attach_partition.call_count == 2
    assert repo.reconcile_default_rows.call_count == 2
    restore = repo.reconcile_default_rows.call_args_list[-1].kwargs
    assert restore["default_partition_name"] == "events__2024_04"
    assert restore["target_partition_name"] == "events_default"
    logger.exception.assert_called_once()


def test__apply__default_conflict_with_nothing_moved__nothing_is_restored(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.attach_partition.side_effect = _default_conflict()
    repo.reconcile_default_rows.return_value = 0
    metadata.get_default_partition.return_value = _default_partition()

    # Act / Assert
    with pytest.raises(SQLAlchemyError):
        executor.apply(_config(), _plan(_create_op()))

    assert repo.reconcile_default_rows.call_count == 1


def test__apply__default_conflict_without_a_default_partition__raises(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.attach_partition.side_effect = _default_conflict()
    metadata.get_default_partition.return_value = None
    logger = MagicMock()

    # Act / Assert
    with patch("pg_partsmith.sync.services.execution.logger", logger), pytest.raises(SQLAlchemyError):
        executor.apply(_config(), _plan(_create_op()))

    repo.reconcile_default_rows.assert_not_called()
    assert repo.attach_partition.call_count == 1
    logger.warning.assert_called_once()


def test__apply__default_conflict_on_non_range_bounds__records_default_holds_rows(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- only a RANGE window can be moved by its key; a hash bucket is reported for a human
    repo.attach_partition.side_effect = [_default_conflict(), None]
    bucket = _create_op("events__h1", bounds=HashBounds(modulus=2, remainder=1), key_columns=("tenant_id",))
    second = _create_op("events__h0", bounds=HashBounds(modulus=2, remainder=0), key_columns=("tenant_id",))

    # Act
    result = executor.apply(_config(), _plan(bucket, second))

    # Assert
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.CREATE]
    assert "DEFAULT partition holds rows" in result.issues[0].error
    assert result.issues[0].partition_name == "events__h1"
    metadata.get_default_partition.assert_not_called()
    assert result.created_count == 1


def test__apply__default_conflict_error_with_wrong_text__is_not_a_default_conflict(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- 23514 alone is any check violation; only the DEFAULT wording triggers reconciliation
    repo.attach_partition.side_effect = _sqlstate_error("23514", "check constraint violated")

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="check constraint violated"):
        executor.apply(_config(), _plan(_create_op()))

    metadata.get_default_partition.assert_not_called()


# ── attach: races and conflicts ─────────────────────────────────────────────────


@pytest.mark.parametrize("sqlstate", ["42P07", "42710", "42809"])
def test__apply__attach_conflict_and_attached_with_planned_bounds__is_benign(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, sqlstate: str
) -> None:
    # Arrange
    repo.attach_partition.side_effect = _sqlstate_error(sqlstate, "already a partition")
    metadata.is_partition_attached.return_value = True
    metadata.get_partition_tree.return_value = PartitionNode(name="events__2024_04", bounds=APRIL)

    # Act
    result = executor.apply(_config(), _plan(_create_op()))

    # Assert
    assert result.issues == ()
    metadata.is_partition_attached.assert_called_once_with("events", "events__2024_04")


@pytest.mark.parametrize("sqlstate", ["42P07", "42710", "42809"])
def test__apply__attach_conflict_and_attached_with_other_bounds__records_topology_issue(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, sqlstate: str
) -> None:
    # Arrange
    repo.attach_partition.side_effect = _sqlstate_error(sqlstate, "already a partition")
    metadata.is_partition_attached.return_value = True
    metadata.get_partition_tree.return_value = PartitionNode(
        name="events__2024_04", bounds=RangeBounds(from_value="2024-04-01", to_value="2024-04-02")
    )

    # Act
    result = executor.apply(_config(), _plan(_create_op()))

    # Assert
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.CREATE]
    assert "not the planned ones" in result.issues[0].error
    assert result.created_count == 0


@pytest.mark.parametrize("sqlstate", ["42P07", "42710", "42809"])
def test__apply__attach_conflict_but_not_attached__propagates(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, sqlstate: str
) -> None:
    # Arrange -- the SQLSTATE alone is not proof of a lost race
    repo.attach_partition.side_effect = _sqlstate_error(sqlstate, "already a partition")
    metadata.is_partition_attached.return_value = False

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="already a partition"):
        executor.apply(_config(), _plan(_create_op()))


def test__apply__attach_conflict_after_reconcile__rows_are_restored_before_the_race_is_judged(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.attach_partition.side_effect = [_default_conflict(), _sqlstate_error("42P07", "duplicate")]
    repo.reconcile_default_rows.return_value = 3
    metadata.get_default_partition.return_value = _default_partition()
    metadata.is_partition_attached.return_value = True
    metadata.get_partition_tree.return_value = PartitionNode(name="events__2024_04", bounds=APRIL)

    # Act
    result = executor.apply(_config(), _plan(_create_op()))

    # Assert
    assert result.issues == ()
    move, restore = repo.reconcile_default_rows.call_args_list
    assert move.kwargs["default_partition_name"] == "events_default"
    assert restore.kwargs["default_partition_name"] == "events__2024_04"
    assert restore.kwargs["target_partition_name"] == "events_default"


def test__apply__unrelated_database_error_on_attach__propagates_after_restoring_rows(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.attach_partition.side_effect = [_default_conflict(), _sqlstate_error("53100", "disk full")]
    repo.reconcile_default_rows.return_value = 3
    metadata.get_default_partition.return_value = _default_partition()

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="disk full"):
        executor.apply(_config(), _plan(_create_op()))

    assert repo.reconcile_default_rows.call_count == 2


@pytest.mark.parametrize("error", [OSError("connection reset"), TimeoutError()])
def test__apply__transport_error_on_attach__propagates_after_restoring_rows(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, error: BaseException
) -> None:
    # Arrange
    repo.attach_partition.side_effect = [_default_conflict(), error]
    repo.reconcile_default_rows.return_value = 3
    metadata.get_default_partition.return_value = _default_partition()

    # Act / Assert
    with pytest.raises(type(error)):
        executor.apply(_config(), _plan(_create_op()))

    assert repo.reconcile_default_rows.call_count == 2


def test__apply__restore_fails__original_attach_error_still_propagates(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.attach_partition.side_effect = [_default_conflict(), _sqlstate_error("53100", "disk full")]
    repo.reconcile_default_rows.side_effect = [3, SQLAlchemyError("restore failed")]
    metadata.get_default_partition.return_value = _default_partition()
    logger = MagicMock()

    # Act / Assert
    with (
        patch("pg_partsmith.sync.services.execution.logger", logger),
        pytest.raises(SQLAlchemyError, match="disk full"),
    ):
        executor.apply(_config(), _plan(_create_op()))

    logger.exception.assert_called_once()


def test__apply__interrupted_while_restoring_rows__propagates_the_interruption(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the compensating move-back itself is interrupted
    repo.attach_partition.side_effect = [_default_conflict(), _sqlstate_error("53100", "disk full")]
    repo.reconcile_default_rows.side_effect = [5, KeyboardInterrupt()]
    metadata.get_default_partition.return_value = _default_partition()

    # Act / Assert
    with pytest.raises(KeyboardInterrupt):
        executor.apply(_config(), _plan(_create_op()))


def test__apply__interrupted_during_attach_after_reconcile__restores_rows_and_reraises(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.attach_partition.side_effect = [_default_conflict(), KeyboardInterrupt()]
    repo.reconcile_default_rows.return_value = 5
    metadata.get_default_partition.return_value = _default_partition()

    # Act / Assert -- interruption wins, but only after the compensating move-back
    with pytest.raises(KeyboardInterrupt):
        executor.apply(_config(), _plan(_create_op()), continue_on_error=True)

    restore = repo.reconcile_default_rows.call_args_list[-1].kwargs
    assert restore["default_partition_name"] == "events__2024_04"
    assert restore["target_partition_name"] == "events_default"


# ── re-attach ───────────────────────────────────────────────────────────────────


def test__apply__attach_op__revalidates_oid_then_attaches(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = 77

    # Act
    result = executor.apply(_config(), _plan(_attach_op(oid=77)))

    # Assert
    metadata.get_relation_oid.assert_called_once_with("events__2024_04")
    repo.attach_partition.assert_called_once_with("events", "events__2024_04", APRIL, key_arity=1)
    repo.create_table_like.assert_not_called()
    assert result.attached_count == 1
    assert result.created_count == 0
    assert hooks.calls == []


def test__apply__attach_op_without_oid__skips_revalidation(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange / Act
    result = executor.apply(_config(), _plan(_attach_op(oid=None)))

    # Assert
    metadata.get_relation_oid.assert_not_called()
    assert result.attached_count == 1


def test__apply__attach_op_with_other_oid__is_stale_and_raises(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the name now belongs to a relation the plan never saw
    metadata.get_relation_oid.return_value = 78

    # Act / Assert
    with pytest.raises(PlanStaleError, match="OID 78"):
        executor.apply(_config(), _plan(_attach_op(oid=77)))

    repo.attach_partition.assert_not_called()


def test__apply__attach_op_relation_gone__is_stale_and_recorded_when_continuing(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = None

    # Act
    result = executor.apply(_config(), _plan(_attach_op(oid=77)), continue_on_error=True)

    # Assert
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.ATTACH]
    assert "no longer exists" in result.issues[0].error
    assert result.attached_count == 0


def test__apply__attach_op_on_a_branch__missing_buckets_are_repaired_first(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    order = _record_ddl(repo)
    metadata.get_relation_oid.return_value = 77
    metadata.get_partition_tree.return_value = PartitionNode(
        name="events__2024_04",
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        is_attached=False,
        children=(PartitionNode(name="events__2024_04__h1", bounds=HashBounds(modulus=2, remainder=1)),),
    )

    # Act
    result = executor.apply(_nested_config(), _plan(_attach_op(oid=77)))

    # Assert
    assert order == ["create events__2024_04__h0", "attach events__2024_04__h0", "attach events__2024_04"]
    assert result.repaired_count == 1
    assert result.attached_count == 1


def test__apply__attach_op_under_a_bucket_of_a_hash_root__depth_comes_from_the_range_level(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the monthly partitions live one level down, under each bucket
    op = _attach_op(oid=None, parent="events__h0", target="events__h0__2024_04")

    # Act
    result = executor.apply(_hash_root_config(), _plan(op))

    # Assert -- a leaf level has nothing below it to converge
    metadata.get_partition_tree.assert_not_called()
    repo.attach_partition.assert_called_once_with("events__h0", "events__h0__2024_04", APRIL, key_arity=1)
    assert result.attached_count == 1


def test__apply__attach_op_under_a_non_root_parent_without_a_deeper_range_level__falls_back_to_the_root(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    op = AttachPartition(
        target="events__2024_04__h1",
        parent_name="events__2024_04",
        bounds=HashBounds(modulus=2, remainder=1),
        key_columns=("tenant_id",),
        reason=Reason.REATTACH,
    )

    # Act
    result = executor.apply(_nested_config(), _plan(op))

    # Assert
    metadata.get_partition_tree.assert_called_once_with("events__2024_04__h1")
    assert result.attached_count == 1


# ── detach ──────────────────────────────────────────────────────────────────────


def test__apply__detach_op__revalidates_then_detaches_with_hooks_around_it(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = 77
    metadata.is_partition_attached.return_value = True

    # Act
    result = executor.apply(_config(), _plan(_detach_op(mode=DetachMode.BLOCKING)))

    # Assert
    repo.detach_partition.assert_called_once_with("events", "events__2024_03", mode=DetachMode.BLOCKING)
    assert hooks.names() == ["before_detach", "after_detach"]
    table_name, info = hooks.calls[0][1]
    assert table_name == "events"
    assert info == PartitionInfo(
        name="events__2024_03",
        oid=77,
        partition_type=PartitionType.RANGE,
        bounds=MARCH,
        boundaries_expr="range",
        is_attached=True,
        parent_table="events",
    )
    assert hooks.calls[1][1] == ("events", "events__2024_03")
    assert result.detached_count == 1


@pytest.mark.parametrize(
    "bounds,expected",
    [
        (HashBounds(modulus=4, remainder=1), PartitionType.HASH),
        (ListBounds(values=("eu",)), PartitionType.LIST),
        (DefaultBounds(), PartitionType.RANGE),
    ],
)
def test__detach_single_partition__non_range_bounds__hook_sees_the_parent_method(
    executor: PlanExecutor,
    metadata: MagicMock,
    hooks: _RecordingHooks,
    bounds: PartitionBounds,
    expected: PartitionType,
) -> None:
    # Arrange
    metadata.is_partition_attached.return_value = True

    # Act
    executor.detach_single_partition("events", _detach_op(oid=None, bounds=bounds))

    # Assert
    _, info = hooks.calls[0][1]
    assert info.partition_type is expected
    assert info.bounds == bounds


def test__apply__detach_op_oid_mismatch__is_stale_and_nothing_is_detached(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = 999

    # Act / Assert
    with pytest.raises(PlanStaleError):
        executor.apply(_config(), _plan(_detach_op(oid=77)))

    repo.detach_partition.assert_not_called()
    assert hooks.calls == []


def test__apply__detach_op_relation_gone__is_stale(executor: PlanExecutor, metadata: MagicMock) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = None

    # Act / Assert
    with pytest.raises(PlanStaleError, match="no longer exists"):
        executor.apply(_config(), _plan(_detach_op(oid=77)))


def test__apply__detach_op_no_longer_attached__is_stale(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = 77
    metadata.is_partition_attached.return_value = False

    # Act / Assert
    with pytest.raises(PlanStaleError, match="no longer attached"):
        executor.apply(_config(), _plan(_detach_op(oid=77)))

    repo.detach_partition.assert_not_called()


def test__apply__detach_op_stale_with_continue_on_error__records_a_detach_issue(
    executor: PlanExecutor, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = 999

    # Act
    result = executor.apply(_config(), _plan(_detach_op(oid=77)), continue_on_error=True)

    # Assert
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.DETACH]
    assert "PlanStaleError" in result.issues[0].error
    assert result.detached_count == 0


# ── drop ────────────────────────────────────────────────────────────────────────


def test__apply__drop_op__drops_with_the_expected_oid_and_hooks_around_it(
    executor: PlanExecutor, repo: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange / Act
    result = executor.apply(_config(), _plan(_drop_op(oid=55)))

    # Assert
    repo.drop_partition.assert_called_once_with("events__2023_12", expected_oid=55)
    assert hooks.calls == [
        ("before_drop", ("events", "events__2023_12")),
        ("after_drop", ("events", "events__2023_12")),
    ]
    assert result.dropped_count == 1


def test__apply__drop_op_still_attached__is_skipped_without_after_hook(
    executor: PlanExecutor, repo: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange
    repo.drop_partition.side_effect = PartitionAttachedError("events__2023_12", "events")
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.services.execution.logger", logger):
        result = executor.apply(_config(), _plan(_drop_op()))

    # Assert
    assert result.dropped_count == 0
    assert result.issues == ()
    assert hooks.names() == ["before_drop"]
    logger.warning.assert_called_once()


def test__drop_single_partition__attached__returns_false(executor: PlanExecutor, repo: MagicMock) -> None:
    # Arrange
    repo.drop_partition.side_effect = PartitionAttachedError("events__2023_12", "events")

    # Act / Assert
    assert executor.drop_single_partition("events", _drop_op()) is False


def test__apply__drop_following_a_failed_detach__is_skipped(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = 77
    metadata.is_partition_attached.return_value = True
    repo.detach_partition.side_effect = SQLAlchemyError("detach failed")
    plan = _plan(_detach_op(oid=77), _drop_op("events__2024_03", oid=77, follows_detach=True), _drop_op())
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.services.execution.logger", logger):
        result = executor.apply(_config(), plan, continue_on_error=True)

    # Assert -- the pre-existing orphan is still dropped; the would-be-detached partition is not
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.DETACH]
    repo.drop_partition.assert_called_once_with("events__2023_12", expected_oid=55)
    assert result.dropped_count == 1
    assert any("did not happen" in call.args[0] for call in logger.info.call_args_list)


def test__apply__drop_following_a_detach_that_is_not_in_the_plan__is_skipped(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange -- the detach was filtered out; the drop must not run on a still-attached partition
    plan = _plan(_drop_op("events__2024_03", oid=77, follows_detach=True))

    # Act
    result = executor.apply(_config(), plan)

    # Assert
    repo.drop_partition.assert_not_called()
    assert result.dropped_count == 0


def test__apply__drop_following_a_successful_detach__is_dropped(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_relation_oid.return_value = 77
    metadata.is_partition_attached.return_value = True
    plan = _plan(_detach_op(oid=77), _drop_op("events__2024_03", oid=77, follows_detach=True))

    # Act
    result = executor.apply(_config(), plan)

    # Assert
    repo.drop_partition.assert_called_once_with("events__2024_03", expected_oid=77)
    assert result.detached_count == 1
    assert result.dropped_count == 1


# ── errors, issues, interruption ────────────────────────────────────────────────


def test__apply__error_without_continue_on_error__propagates_and_stops(executor: PlanExecutor, repo: MagicMock) -> None:
    # Arrange
    repo.create_table_like.side_effect = SQLAlchemyError("create failed")

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="create failed"):
        executor.apply(_config(), _plan(_create_op(), _drop_op()))

    repo.drop_partition.assert_not_called()


@pytest.mark.parametrize(
    "op,method,step",
    [
        (_create_op(), "create_table_like", MaintenanceIssueStep.CREATE),
        (_create_op(counts_as="repaired"), "create_table_like", MaintenanceIssueStep.RECONCILE),
        (_attach_op(oid=None), "attach_partition", MaintenanceIssueStep.ATTACH),
        (_detach_op(oid=None), "detach_partition", MaintenanceIssueStep.DETACH),
        (_drop_op(), "drop_partition", MaintenanceIssueStep.DROP),
    ],
)
def test__apply__error_with_continue_on_error__records_the_step_and_carries_on(
    executor: PlanExecutor,
    repo: MagicMock,
    metadata: MagicMock,
    op: Operation,
    method: str,
    step: MaintenanceIssueStep,
) -> None:
    # Arrange -- only the operation under test fails; the trailing drop must still run
    def _fail_for_target(*args: object, **kwargs: object) -> None:
        if op.target in args:
            raise SQLAlchemyError("boom")

    metadata.is_partition_attached.return_value = True
    getattr(repo, method).side_effect = _fail_for_target
    trailing = _drop_op("events__2020_01", oid=1)
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.services.execution.logger", logger):
        result = executor.apply(_config(), _plan(op, trailing), continue_on_error=True)

    # Assert
    assert [issue.step for issue in result.issues] == [step]
    assert result.issues[0].error == "SQLAlchemyError: boom"
    assert result.issues[0].partition_name == op.target
    assert result.created_count + result.repaired_count + result.attached_count + result.detached_count == 0
    assert repo.drop_partition.call_args.args[0] == "events__2020_01"
    assert logger.warning.call_args.kwargs["extra"]["step"] == step.value


def test__apply__interruption__propagates_even_when_continuing_on_error(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange
    repo.create_table_like.side_effect = KeyboardInterrupt()

    # Act / Assert
    with pytest.raises(KeyboardInterrupt):
        executor.apply(_config(), _plan(_create_op(), _drop_op()), continue_on_error=True)

    repo.drop_partition.assert_not_called()


def test__apply__actionable_findings__become_reconcile_issues(executor: PlanExecutor) -> None:
    # Arrange
    warning = Finding(
        partition_name="events__2024_02_15", reason=FindingReason.RANGE_OVERLAP, detail="overlaps the March window"
    )
    info = Finding(partition_name="events__2023_01", reason=FindingReason.LEGACY_LEAF, detail="a plain leaf")
    plan = _plan(findings=(warning, info))

    # Act
    result = executor.apply(_config(), plan)

    # Assert
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.RECONCILE]
    assert result.issues[0].partition_name == "events__2024_02_15"
    assert "overlaps the March window" in result.issues[0].error
    assert result.plan is plan


def test__apply__empty_plan__zero_counters_and_the_plan_echoed_back(executor: PlanExecutor) -> None:
    # Arrange
    plan = _plan()

    # Act
    result = executor.apply(_config(), plan)

    # Assert
    assert (result.created_count, result.repaired_count, result.attached_count) == (0, 0, 0)
    assert (result.detached_count, result.dropped_count) == (0, 0)
    assert result.issues == ()
    assert result.plan is plan


def test__apply__topology_error_raised_while_executing__is_logged_as_a_warning(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange
    repo.create_table_like.side_effect = PartitionTopologyError("events", "custom_reason", "the detail")
    logger = MagicMock()

    # Act
    with patch("pg_partsmith.sync.services.execution.logger", logger):
        result = executor.apply(_config(), _plan(_create_op()))

    # Assert
    logger.warning.assert_called_once_with(
        "the detail", extra={"partition_name": "events__2024_04", "reason": "custom_reason"}
    )
    assert result.issues[0].partition_name == "events__2024_04"


# ── hook failure logging ────────────────────────────────────────────────────────


def test__run_hooks__runtime_error__logged_as_warning_and_reraised(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    class _Hooks(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            raise RuntimeError("no export today")

    executor = PlanExecutor(repo, metadata, hooks=[_Hooks()])
    logger = MagicMock()

    # Act / Assert
    with patch("pg_partsmith.sync.services.execution.logger", logger), pytest.raises(RuntimeError, match="no export"):
        executor.drop_single_partition("events", _drop_op())

    logger.warning.assert_called_once()
    assert logger.warning.call_args.args[0] == "before_drop hook failed"
    assert logger.warning.call_args.kwargs["extra"]["hook_type"] == "_Hooks"
    assert logger.warning.call_args.kwargs["extra"]["error"] == "no export today"
    logger.exception.assert_not_called()
    repo.drop_partition.assert_not_called()


def test__run_hooks__unexpected_error__logged_with_traceback_and_reraised(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    class _Hooks(BasePartitionLifecycleHooks):
        def after_drop(self, table_name: str, partition_name: str) -> None:
            raise _BoomError("kafka down")

    executor = PlanExecutor(repo, metadata, hooks=[_Hooks()])
    logger = MagicMock()

    # Act / Assert
    with patch("pg_partsmith.sync.services.execution.logger", logger), pytest.raises(_BoomError):
        executor.drop_single_partition("events", _drop_op())

    logger.exception.assert_called_once()
    assert logger.exception.call_args.args[0] == "after_drop hook failed with unexpected error"
    logger.warning.assert_not_called()


def test__run_hooks__interrupted__propagates_without_logging(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    class _Hooks(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            raise KeyboardInterrupt()

    executor = PlanExecutor(repo, metadata, hooks=[_Hooks()])
    logger = MagicMock()

    # Act / Assert
    with patch("pg_partsmith.sync.services.execution.logger", logger), pytest.raises(KeyboardInterrupt):
        executor.drop_single_partition("events", _drop_op())

    logger.warning.assert_not_called()
    logger.exception.assert_not_called()


def test__executor__without_hooks__runs_operations_silently(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    executor = PlanExecutor(repo, metadata)

    # Act
    result = executor.apply(_config(), _plan(_create_op(), _drop_op()))

    # Assert
    assert (result.created_count, result.dropped_count) == (1, 1)


# ── numeric axis in a converged subtree ─────────────────────────────────────────


def test__apply__detached_branch_with_numeric_windows__replans_from_the_planned_bounds(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- a HASH root over integer windows; the bucket exists detached and empty
    config = TablePartitionConfig(
        table_name="queue",
        scheme=HashPartitioning(
            key="worker",
            modulus=2,
            child=RangePartitioning(key="id", boundaries=NumericBoundaries(step=1000)),
        ),
    )
    order = _record_ddl(repo)

    def _create(template: str, name: str, partition_by: PartitionBy | None) -> None:
        if name == "queue__h0":
            raise PartitionAlreadyExistsError(name)
        order.append(f"create {name}")

    repo.create_table_like.side_effect = _create
    metadata.get_partition_tree.return_value = PartitionNode(
        name="queue__h0", partition_type=PartitionType.RANGE, partition_columns=("id",), is_attached=False
    )
    bucket = CreatePartition(
        target="queue__h0",
        parent_name="queue",
        bounds=HashBounds(modulus=2, remainder=0),
        partition_by=PartitionBy(method=PartitionType.RANGE, columns=("id",)),
        key_columns=("worker",),
        children=(
            CreatePartition(
                target="queue__h0__2000",
                parent_name="queue__h0",
                bounds=RangeBounds(from_value="2000", to_value="3000"),
                key_columns=("id",),
                counts_as="subtree",
                reason=Reason.SUBTREE,
            ),
        ),
        reason=Reason.HASH_GAP,
    )
    plan = MaintenancePlan(table_name="queue", generated_at=NOW, cursors={"id": 2500}, operations=(bucket,))

    # Act
    result = executor.apply(config, plan)

    # Assert
    assert order == ["create queue__h0__2000", "attach queue__h0__2000", "attach queue__h0"]
    assert result.repaired_count == 1


# ── the legacy BasePartitionService hook runner ─────────────────────────────────


def test__base_partition_service__hooks_run_in_order_and_failures_are_logged_by_kind() -> None:
    # Arrange
    calls: list[str] = []

    class _Fine(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            calls.append(partition_name)

    class _Loud(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            raise _BoomError("kafka down")

    class _Typed(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            raise ValueError("bad value")

    logger = MagicMock()

    # Act / Assert
    with patch("pg_partsmith.sync.services.base.logger", logger):
        BasePartitionService([_Fine()])._run_hooks(
            lambda h: h.before_drop("events", "events__x"), "before_drop", partition_name="events__x"
        )
        with pytest.raises(_BoomError):
            BasePartitionService([_Loud()])._run_hooks(
                lambda h: h.before_drop("events", "events__x"), "before_drop", partition_name="events__x"
            )
        with pytest.raises(ValueError, match="bad value"):
            BasePartitionService([_Typed()])._run_hooks(
                lambda h: h.before_drop("events", "events__x"), "before_drop", partition_name="events__x"
            )

    assert calls == ["events__x"]
    logger.exception.assert_called_once()
    logger.warning.assert_called_once()
    assert logger.warning.call_args.kwargs["extra"]["hook_type"] == "_Typed"


def test__base_partition_service__no_hooks__runs_nothing() -> None:
    # Arrange
    service = BasePartitionService()
    caller = MagicMock()

    # Act
    service._run_hooks(caller, "before_drop", partition_name="events__x")

    # Assert
    caller.assert_not_called()


def test__base_partition_service__interrupted_hook__propagates_without_logging() -> None:
    # Arrange
    class _Interrupted(BasePartitionLifecycleHooks):
        def before_drop(self, table_name: str, partition_name: str) -> None:
            raise KeyboardInterrupt()

    logger = MagicMock()

    # Act / Assert
    with patch("pg_partsmith.sync.services.base.logger", logger), pytest.raises(KeyboardInterrupt):
        BasePartitionService([_Interrupted()])._run_hooks(
            lambda h: h.before_drop("events", "events__x"), "before_drop", partition_name="events__x"
        )

    logger.warning.assert_not_called()
    logger.exception.assert_not_called()


# ── leaf backends ───────────────────────────────────────────────────────────────


def _leaves_config(leaves: LocalLeaves | ForeignLeaves, *, nested: bool = False) -> TablePartitionConfig:
    return TablePartitionConfig(
        schema="public",
        table_name="events",
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        subpartition=HashPartitioning(key="tenant_id", modulus=2) if nested else None,
        leaves=leaves,
    )


def test__apply__plain_local_leaves__create_is_spelled_as_it_always_was(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange / Act
    op = _create_op("public.events__2024_04", parent="public.events")
    executor.apply(_leaves_config(LocalLeaves()), _plan(op))

    # Assert
    repo.create_table_like.assert_called_once_with("public.events", "public.events__2024_04", None)
    repo.create_foreign_table_like.assert_not_called()


def test__apply__customised_local_leaves__physical_spec_reaches_the_repository(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange
    leaves = LocalLeaves(tablespace="fast", storage_parameters={"fillfactor": 70})

    # Act
    executor.apply(_leaves_config(leaves), _plan(_create_op()))

    # Assert
    repo.create_table_like.assert_called_once_with("events", "events__2024_04", None, physical=leaves)


def test__apply__foreign_leaves__leaf_is_a_foreign_table_with_rendered_options(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange
    leaves = ForeignLeaves(server="archive", options={"table_name": "{relname}", "schema_name": "{schema}"})
    op = _create_op("public.events__2024_04", parent="public.events")

    # Act
    result = executor.apply(_leaves_config(leaves), _plan(op))

    # Assert
    repo.create_foreign_table_like.assert_called_once_with(
        "public.events",
        "public.events__2024_04",
        server="archive",
        options={"table_name": "events__2024_04", "schema_name": "public"},
    )
    repo.create_table_like.assert_not_called()
    repo.attach_partition.assert_called_once_with("public.events", "public.events__2024_04", APRIL, key_arity=1)
    assert result.created_count == 1


def test__apply__foreign_leaves__branch_stays_local_and_its_buckets_are_foreign(
    executor: PlanExecutor, repo: MagicMock
) -> None:
    # Arrange
    leaves = ForeignLeaves(server="archive", options={"table_name": "{parent}_{relname}"})

    # Act
    executor.apply(_leaves_config(leaves, nested=True), _plan(_branch_op()))

    # Assert
    repo.create_table_like.assert_called_once_with("events", "events__2024_04", _branch_op().partition_by)
    assert [call.args[1] for call in repo.create_foreign_table_like.call_args_list] == [
        "events__2024_04__h0",
        "events__2024_04__h1",
    ]
    assert repo.create_foreign_table_like.call_args_list[0].kwargs == {
        "server": "archive",
        "options": {"table_name": "events__2024_04_events__2024_04__h0"},
    }


# ── create_partition with a fill step ───────────────────────────────────────────


def test__create_partition__fill_runs_between_the_build_and_the_attach(
    executor: PlanExecutor, repo: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange
    order = _record_ddl(repo)

    def fill(target: str) -> bool:
        order.append(f"fill {target}")
        return True

    # Act
    attached = executor.create_partition(_config(), _plan(), _create_op(), issues=[], fill=fill)

    # Assert
    assert attached
    assert order == ["create events__2024_04", "fill events__2024_04", "attach events__2024_04"]
    assert hooks.names() == ["before_create", "after_create"]


def test__create_partition__fill_declines__relation_stays_detached_and_no_after_hook(
    executor: PlanExecutor, repo: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange
    def fill(target: str) -> bool:
        return False

    # Act
    attached = executor.create_partition(_config(), _plan(), _create_op(), issues=[], fill=fill)

    # Assert
    assert not attached
    repo.attach_partition.assert_not_called()
    assert hooks.names() == ["before_create"]


def test__create_partition__relation_already_attached__fill_is_skipped(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- a lost race: another worker attached it with the planned bounds
    repo.create_table_like.side_effect = PartitionAlreadyExistsError("events__2024_04")
    metadata.is_partition_attached.return_value = True
    metadata.get_partition_tree.return_value = PartitionNode(name="events__2024_04", bounds=APRIL)
    filled: list[str] = []

    def fill(target: str) -> bool:
        filled.append(target)
        return True

    # Act
    attached = executor.create_partition(_config(), _plan(), _create_op(), issues=[], fill=fill)

    # Assert
    assert attached
    assert filled == []
    repo.attach_partition.assert_not_called()


def test__create_partition__relation_exists_detached__fill_runs_before_it_is_attached(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- an earlier call built it and stopped short of the attach
    repo.create_table_like.side_effect = PartitionAlreadyExistsError("events__2024_04")
    metadata.is_partition_attached.return_value = False
    metadata.get_partition_tree.return_value = PartitionNode(name="events__2024_04", is_attached=False)
    filled: list[str] = []

    def fill(target: str) -> bool:
        filled.append(target)
        return True

    # Act
    attached = executor.create_partition(_config(), _plan(), _create_op(), issues=[], fill=fill)

    # Assert
    assert attached
    assert filled == ["events__2024_04"]
    repo.attach_partition.assert_called_once()


# ── detach refused by a foreign key ─────────────────────────────────────────────


def test__apply__detach_refused_by_a_foreign_key__recorded_and_the_run_goes_on(
    executor: PlanExecutor, repo: MagicMock, metadata: MagicMock, hooks: _RecordingHooks
) -> None:
    # Arrange -- June is still referenced; July is not
    metadata.is_partition_attached.return_value = True
    repo.detach_partition.side_effect = [
        PartitionReferencedError("events__2024_06", "violates foreign key constraint refs_fk"),
        None,
    ]
    june = DetachPartition(target="events__2024_06", parent_name="events", reason=Reason.RETENTION_EXPIRED)
    july = DetachPartition(target="events__2024_07", parent_name="events", reason=Reason.RETENTION_EXPIRED)
    june_drop = DropPartition(target="events__2024_06", reason=Reason.FOLLOWS_DETACH, follows_detach=True)

    # Act
    result = executor.apply(_config(), _plan(june, july, june_drop))

    # Assert -- June's drop is skipped with its detach; nothing else is affected
    assert result.detached_count == 1
    assert result.dropped_count == 0
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.DETACH]
    assert result.issues[0].partition_name == "events__2024_06"
    assert "still referenced by rows of another table" in result.issues[0].error
    assert hooks.names().count("after_detach") == 1
