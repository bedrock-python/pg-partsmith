"""Unit tests for the aio ``DataMover``: draining a DEFAULT partition and unpartitioning, in batches."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from pg_partsmith.boundaries import Window
from pg_partsmith.entities import (
    DefaultBounds,
    MaintenanceIssueStep,
    MigrationResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionNode,
    PartitionType,
    RangeBounds,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import InvalidPartitionConfigError
from pg_partsmith.lifecycle import DetachMode
from pg_partsmith.plan import (
    CreatePartition,
    DetachPartition,
    DropPartition,
    Finding,
    FindingReason,
    MaintenancePlan,
    Reason,
)
from pg_partsmith.scheme import HashPartitioning, ListGroup, ListPartitioning
from pg_partsmith.sync.services.migration import DataMover
from pg_partsmith.topology import ActualTree, RelationKind

NOW = datetime(2026, 8, 28, tzinfo=UTC)
ROOT = "public.events"
DEFAULT = f"{ROOT}_default"

# ── fixtures and builders ────────────────────────────────────────────────────────


@pytest.fixture
def repo() -> MagicMock:
    repo = MagicMock()
    repo.reconcile_default_rows = MagicMock(return_value=0)
    repo.move_rows = MagicMock(return_value=0)
    repo.create_table_like = MagicMock(return_value=None)
    return repo


@pytest.fixture
def metadata() -> MagicMock:
    metadata = MagicMock()
    metadata.get_default_partition = MagicMock(return_value=_default_info())
    metadata.get_leading_key_minimum = MagicMock(return_value=None)
    metadata.get_actual_tree = MagicMock(return_value=None)
    metadata.partition_exists = MagicMock(return_value=True)
    return metadata


@pytest.fixture
def executor() -> MagicMock:
    executor = MagicMock()

    def create_partition(config: Any, plan: Any, op: Any, *, issues: Any, fill: Any = None) -> bool:
        return True if fill is None else fill(op.target)

    executor.create_partition = MagicMock(side_effect=create_partition)
    executor.detach_single_partition = MagicMock(return_value=None)
    executor.drop_single_partition = MagicMock(return_value=True)
    return executor


@pytest.fixture
def mover(repo: MagicMock, metadata: MagicMock, executor: MagicMock) -> DataMover:
    return DataMover(repo, metadata, executor)


def _config(**overrides: Any) -> TablePartitionConfig:
    fields: dict[str, Any] = {
        "schema": "public",
        "table_name": "events",
        "partition_column": "created_at",
        "granularity": PartitionGranularity.MONTH,
    }
    fields.update(overrides)
    return TablePartitionConfig(**fields)


def _default_info() -> PartitionInfo:
    return PartitionInfo(name=DEFAULT, partition_type=PartitionType.RANGE, is_default=True, parent_table=ROOT)


def _window(month: int) -> Window:
    return Window(start=datetime(2026, month, 1, tzinfo=UTC), end=datetime(2026, month + 1, 1, tzinfo=UTC))


def _create_op(month: int) -> CreatePartition:
    return CreatePartition(
        target=f"{ROOT}__2026_{month:02d}",
        parent_name=ROOT,
        bounds=RangeBounds(from_value=f"2026-{month:02d}-01", to_value=f"2026-{month + 1:02d}-01"),
        key_columns=("created_at",),
        lifecycle_unit=True,
        counts_as="created",
        reason=Reason.EXPLICIT,
    )


def _plan(*operations: Any, findings: tuple[Finding, ...] = ()) -> MaintenancePlan:
    return MaintenancePlan(table_name=ROOT, generated_at=NOW, operations=tuple(operations), findings=findings)


def _plans(*plans: MaintenancePlan) -> MagicMock:
    return MagicMock(side_effect=list(plans))


def _month(month: int, **overrides: Any) -> PartitionNode:
    return PartitionNode(
        name=f"{ROOT}__2026_{month:02d}",
        parent_name=ROOT,
        level=1,
        oid=overrides.pop("oid", month),
        bounds=RangeBounds(from_value=f"2026-{month:02d}-01", to_value=f"2026-{month + 1:02d}-01"),
        **overrides,
    )


def _tree(*children: PartitionNode) -> ActualTree:
    root = PartitionNode(
        name=ROOT, partition_type=PartitionType.RANGE, partition_columns=("created_at",), children=children
    )
    return ActualTree(root=root)


# ── partition_data ──────────────────────────────────────────────────────────────


def test__partition_data__no_default_partition__nothing_to_do(mover: DataMover, metadata: MagicMock) -> None:
    # Arrange
    metadata.get_default_partition.return_value = None

    # Act
    result = mover.partition_data(_config(), _plans())

    # Assert
    assert result == MigrationResult(complete=True)
    metadata.get_leading_key_minimum.assert_not_called()


def test__partition_data__empty_default__complete_without_a_plan(mover: DataMover, metadata: MagicMock) -> None:
    # Arrange
    plan_for = _plans()

    # Act
    result = mover.partition_data(_config(), plan_for)

    # Assert
    assert result.complete
    assert result.batches == 0
    plan_for.assert_not_called()
    metadata.get_leading_key_minimum.assert_called_once_with(DEFAULT, ("created_at",))


def test__partition_data__one_window__filled_in_batches_then_attached(
    mover: DataMover, repo: MagicMock, metadata: MagicMock, executor: MagicMock
) -> None:
    # Arrange -- 25 rows of May in DEFAULT, moved 10 at a time
    metadata.get_leading_key_minimum.side_effect = [datetime(2026, 5, 3, tzinfo=UTC), None]
    repo.reconcile_default_rows.side_effect = [10, 10, 5]
    plan_for = _plans(_plan(_create_op(5)))

    # Act
    result = mover.partition_data(_config(), plan_for, batch_rows=10)

    # Assert
    assert result == MigrationResult(rows_moved=25, batches=3, partitions=(f"{ROOT}__2026_05",), complete=True)
    plan_for.assert_called_once_with(_window(5))
    executor.create_partition.assert_called_once()
    assert repo.reconcile_default_rows.call_args_list[0].kwargs == {
        "default_partition_name": DEFAULT,
        "target_partition_name": f"{ROOT}__2026_05",
        "key_columns": ("created_at",),
        "from_value": "2026-05-01",
        "to_value": "2026-06-01",
        "limit": 10,
    }


def test__partition_data__several_windows__oldest_first_until_default_is_empty(
    mover: DataMover, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_leading_key_minimum.side_effect = [
        datetime(2026, 3, 9, tzinfo=UTC),
        datetime(2026, 5, 1, tzinfo=UTC),
        None,
    ]
    repo.reconcile_default_rows.side_effect = [4, 7]
    plan_for = _plans(_plan(_create_op(3)), _plan(_create_op(5)))

    # Act
    result = mover.partition_data(_config(), plan_for, batch_rows=10)

    # Assert
    assert result.partitions == (f"{ROOT}__2026_03", f"{ROOT}__2026_05")
    assert (result.rows_moved, result.batches, result.complete) == (11, 2, True)
    assert [call.args[0] for call in plan_for.call_args_list] == [_window(3), _window(5)]


def test__partition_data__probe_is_a_literal__decoded_on_the_axis(mover: DataMover, metadata: MagicMock) -> None:
    # Arrange -- a driver handing back the key as text still lands in the right window
    metadata.get_leading_key_minimum.side_effect = ["2026-05-20 10:00:00+00", None]
    plan_for = _plans(_plan(_create_op(5)))

    # Act
    mover.partition_data(_config(), plan_for)

    # Assert
    plan_for.assert_called_once_with(_window(5))


def test__partition_data__batch_budget_runs_out__partition_stays_detached_and_result_is_incomplete(
    mover: DataMover, repo: MagicMock, metadata: MagicMock, executor: MagicMock
) -> None:
    # Arrange
    metadata.get_leading_key_minimum.side_effect = [datetime(2026, 5, 3, tzinfo=UTC)]
    repo.reconcile_default_rows.side_effect = [10, 10, 10]
    plan_for = _plans(_plan(_create_op(5)))

    # Act
    result = mover.partition_data(_config(), plan_for, batch_rows=10, max_batches=2)

    # Assert
    assert (result.rows_moved, result.batches, result.complete) == (20, 2, False)
    assert result.partitions == ()
    assert executor.create_partition.call_args.kwargs["fill"] is not None


def test__partition_data__window_that_cannot_be_created__stops_with_an_issue(
    mover: DataMover, metadata: MagicMock, executor: MagicMock
) -> None:
    # Arrange -- the planner refused the window (an overlapping partition, say)
    metadata.get_leading_key_minimum.side_effect = [datetime(2026, 5, 3, tzinfo=UTC)]
    finding = Finding(partition_name=ROOT, reason=FindingReason.RANGE_OVERLAP, detail="archive covers May")
    plan_for = _plans(_plan(findings=(finding,)))

    # Act
    result = mover.partition_data(_config(), plan_for)

    # Assert
    assert not result.complete
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.RECONCILE, MaintenanceIssueStep.MOVE]
    assert "no partition can be created" in result.issues[1].error
    executor.create_partition.assert_not_called()


def test__partition_data__window_selects_none_of_the_probed_rows__stops_instead_of_looping(
    mover: DataMover, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the probe keeps finding May, the bounds keep selecting nothing
    metadata.get_leading_key_minimum.side_effect = [datetime(2026, 5, 3, tzinfo=UTC)] * 5
    repo.reconcile_default_rows.return_value = 0
    plan_for = _plans(*[_plan(_create_op(5))] * 5)

    # Act
    result = mover.partition_data(_config(), plan_for)

    # Assert
    assert not result.complete
    assert plan_for.call_count == 1
    assert "was not selected by its bounds" in result.issues[0].error


def test__partition_data__non_range_root__refused(mover: DataMover) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="events", scheme=ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("eu",)),))
    )

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="not a RANGE level"):
        mover.partition_data(config, _plans())


@pytest.mark.parametrize("kwargs", [{"batch_rows": 0}, {"max_batches": 0}])
def test__partition_data__non_positive_budget__refused(mover: DataMover, kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        mover.partition_data(_config(), _plans(), **kwargs)


# ── unpartition ─────────────────────────────────────────────────────────────────


def test__unpartition__table_not_partitioned__refused(mover: DataMover) -> None:
    with pytest.raises(InvalidPartitionConfigError, match="not partitioned"):
        mover.unpartition(_config(), "public.events_flat")


def test__unpartition__moves_every_partition_oldest_first_then_default(
    mover: DataMover, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    default = PartitionNode(name=DEFAULT, parent_name=ROOT, level=1, oid=99, bounds=DefaultBounds())
    metadata.get_actual_tree.return_value = _tree(_month(8), default, _month(6))
    repo.move_rows.side_effect = [10, 3, 10, 10, 0, 2]

    # Act
    result = mover.unpartition(_config(), "public.events_flat", batch_rows=10)

    # Assert
    assert result.partitions == (f"{ROOT}__2026_06", f"{ROOT}__2026_08", DEFAULT)
    assert (result.rows_moved, result.batches, result.complete) == (35, 6, True)
    assert [call.args for call in repo.move_rows.call_args_list] == [
        (f"{ROOT}__2026_06", "public.events_flat"),
        (f"{ROOT}__2026_06", "public.events_flat"),
        (f"{ROOT}__2026_08", "public.events_flat"),
        (f"{ROOT}__2026_08", "public.events_flat"),
        (f"{ROOT}__2026_08", "public.events_flat"),
        (DEFAULT, "public.events_flat"),
    ]
    repo.create_table_like.assert_not_called()


def test__unpartition__target_missing__created_like_the_root(
    mover: DataMover, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree()
    metadata.partition_exists.return_value = False

    # Act
    mover.unpartition(_config(), "public.events_flat")

    # Assert
    repo.create_table_like.assert_called_once_with(ROOT, "public.events_flat", None)


def test__unpartition__drop_emptied__detaches_and_drops_through_the_executor(
    mover: DataMover, repo: MagicMock, metadata: MagicMock, executor: MagicMock
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_month(6, oid=66))
    repo.move_rows.return_value = 0

    # Act
    result = mover.unpartition(_config(), "public.events_flat", drop_emptied=True)

    # Assert
    assert result.partitions == (f"{ROOT}__2026_06",)
    detach = executor.detach_single_partition.call_args.args[1]
    assert detach == DetachPartition(
        target=f"{ROOT}__2026_06",
        oid=66,
        parent_name=ROOT,
        mode=DetachMode.AUTO,
        bounds=RangeBounds(from_value="2026-06-01", to_value="2026-07-01"),
        reason=Reason.EXPLICIT,
        detail="emptied by unpartition",
    )
    drop = executor.drop_single_partition.call_args.args[1]
    assert drop == DropPartition(
        target=f"{ROOT}__2026_06", oid=66, reason=Reason.EXPLICIT, detail="emptied by unpartition"
    )


def test__unpartition__batch_budget_runs_out__stops_before_the_next_partition(
    mover: DataMover, repo: MagicMock, metadata: MagicMock, executor: MagicMock
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_month(6), _month(7))
    repo.move_rows.side_effect = [10, 10, 10]

    # Act
    result = mover.unpartition(_config(), "public.events_flat", batch_rows=10, max_batches=2, drop_emptied=True)

    # Assert
    assert (result.rows_moved, result.batches, result.complete) == (20, 2, False)
    assert result.partitions == ()
    executor.detach_single_partition.assert_not_called()


def test__unpartition__foreign_partition__skipped_and_reported(
    mover: DataMover, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_month(6, relkind=RelationKind.FOREIGN), _month(7))
    repo.move_rows.return_value = 0

    # Act
    result = mover.unpartition(_config(), "public.events_flat")

    # Assert
    assert result.partitions == (f"{ROOT}__2026_07",)
    assert [issue.partition_name for issue in result.issues] == [f"{ROOT}__2026_06"]
    assert result.issues[0].step is MaintenanceIssueStep.MOVE
    assert result.complete


def test__unpartition__nested_scheme__branches_are_drained_as_a_whole(
    mover: DataMover, repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- the branch is addressed, not its buckets
    config = _config(subpartition=HashPartitioning(key="tenant_id", modulus=2))
    branch = _month(6, partition_type=PartitionType.HASH, partition_columns=("tenant_id",))
    metadata.get_actual_tree.return_value = _tree(branch)
    repo.move_rows.return_value = 0

    # Act
    result = mover.unpartition(config, "public.events_flat")

    # Assert
    assert result.partitions == (f"{ROOT}__2026_06",)
    repo.move_rows.assert_called_once_with(f"{ROOT}__2026_06", "public.events_flat", limit=10_000)
