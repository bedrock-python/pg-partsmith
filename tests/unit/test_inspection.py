"""Unit tests for the aio ``PartitionInspector``: what is measured, and how the planning context is built."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from pg_partsmith.aio.services.inspection import PartitionInspector
from pg_partsmith.boundaries import CursorSource, NumericBoundaries, TimeBoundaries, Window
from pg_partsmith.entities import PartitionGranularity, PartitionNode, PartitionType, RangeBounds, TablePartitionConfig
from pg_partsmith.lifecycle import (
    AllOf,
    CreateAhead,
    ExpireIf,
    KeepNewest,
    LifecyclePolicy,
    RowsAbove,
    SizeAbove,
    SqlPredicate,
)
from pg_partsmith.planner import PlanMode
from pg_partsmith.scheme import HashPartitioning, RangePartitioning
from pg_partsmith.topology import ActualTree, DetachedPartition, FactKind

NOW = datetime(2024, 3, 15, 12, 0, tzinfo=UTC)

# ── fixtures and builders ────────────────────────────────────────────────────────


@pytest.fixture
def metadata() -> MagicMock:
    metadata = MagicMock()
    metadata.get_actual_tree = AsyncMock(return_value=_tree())
    metadata.measure = AsyncMock(side_effect=lambda tree, **kwargs: tree)
    metadata.get_key_high_water_mark = AsyncMock(return_value=None)
    return metadata


@pytest.fixture
def inspector(metadata: MagicMock) -> PartitionInspector:
    return PartitionInspector(metadata)


def _tree(*children: PartitionNode, orphans: tuple[DetachedPartition, ...] = ()) -> ActualTree:
    root = PartitionNode(
        name="events", partition_type=PartitionType.RANGE, partition_columns=("created_at",), children=children
    )
    return ActualTree(root=root, orphans=orphans)


def _child(name: str, start: str, end: str, **extra: object) -> PartitionNode:
    return PartitionNode(
        name=name, parent_name="events", level=1, bounds=RangeBounds(from_value=start, to_value=end), **extra
    )  # type: ignore[arg-type]


def _config(lifecycle: LifecyclePolicy | None = None, **extra: object) -> TablePartitionConfig:
    kwargs: dict[str, object] = {
        "table_name": "events",
        "partition_column": "created_at",
        "granularity": PartitionGranularity.MONTH,
    }
    if lifecycle is not None:
        kwargs["lifecycle"] = lifecycle
    kwargs.update(extra)
    return TablePartitionConfig(**kwargs)  # type: ignore[arg-type]


def _size_policy() -> LifecyclePolicy:
    return LifecyclePolicy(creation=CreateAhead(count=1), retention=ExpireIf(when=SizeAbove(bytes=10)))


# ── inspect ─────────────────────────────────────────────────────────────────────


async def test__inspect__measure_false__returns_the_tree_untouched(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange
    tree = _tree(_child("events__2024_02", "2024-02-01", "2024-03-01"))
    metadata.get_actual_tree.return_value = tree

    # Act
    result = await inspector.inspect(_config(_size_policy()), measure=False)

    # Assert
    assert result is tree
    metadata.get_actual_tree.assert_awaited_once_with("events")
    metadata.measure.assert_not_awaited()


async def test__inspect__table_not_partitioned__returns_none_without_measuring(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = None

    # Act / Assert
    assert await inspector.inspect(_config(_size_policy())) is None
    metadata.measure.assert_not_awaited()


async def test__inspect__scheme_without_a_progression_level__never_measures(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange -- a hash root has a fixed member set; nothing is ever expired, so nothing needs facts
    config = TablePartitionConfig(
        table_name="tasks", scheme=HashPartitioning(key="task_id", modulus=4), lifecycle=_size_policy()
    )
    metadata.get_actual_tree.return_value = ActualTree(
        root=PartitionNode(name="tasks", partition_type=PartitionType.HASH, partition_columns=("task_id",))
    )

    # Act
    await inspector.inspect(config)

    # Assert
    metadata.measure.assert_not_awaited()


async def test__inspect__policy_that_needs_no_facts__never_measures(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_actual_tree.return_value = _tree(_child("events__2024_02", "2024-02-01", "2024-03-01"))

    # Act
    await inspector.inspect(_config(LifecyclePolicy(retention=KeepNewest(count=3))))

    # Assert
    metadata.measure.assert_not_awaited()


async def test__inspect__policy_needs_facts__measures_the_progression_members_and_orphans(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange
    predicate = SqlPredicate(sql="SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')")
    policy = LifecyclePolicy(
        creation=CreateAhead(count=1),
        retention=ExpireIf(when=AllOf(members=(SizeAbove(bytes=10), RowsAbove(rows=5), predicate))),
    )
    tree = _tree(
        _child("events__2024_02", "2024-02-01", "2024-03-01"),
        _child("events__2024_03", "2024-03-01", "2024-04-01"),
        orphans=(DetachedPartition(name="events__2023_12", parent_name="events"),),
    )
    measured = tree.model_copy()
    metadata.get_actual_tree.return_value = tree
    metadata.measure.side_effect = None
    metadata.measure.return_value = measured

    # Act
    result = await inspector.inspect(_config(policy))

    # Assert
    assert result is measured
    metadata.measure.assert_awaited_once_with(
        tree,
        targets=("events__2024_02", "events__2024_03", "events__2023_12"),
        facts=frozenset({FactKind.SIZE, FactKind.ROWS}),
        sql_predicates=(predicate,),
    )


async def test__inspect__default_partition__is_never_a_fact_target(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange
    default = PartitionNode(name="events_default", parent_name="events", level=1, bounds={"kind": "default"})  # type: ignore[arg-type]
    metadata.get_actual_tree.return_value = _tree(_child("events__2024_02", "2024-02-01", "2024-03-01"), default)

    # Act
    await inspector.inspect(_config(_size_policy()))

    # Assert
    assert metadata.measure.call_args.kwargs["targets"] == ("events__2024_02",)


async def test__inspect__nothing_to_measure__returns_the_tree_without_a_query(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange -- an empty root has no candidates for the policy to decide over
    tree = _tree()
    metadata.get_actual_tree.return_value = tree

    # Act
    result = await inspector.inspect(_config(_size_policy()))

    # Assert
    assert result is tree
    metadata.measure.assert_not_awaited()


async def test__inspect__nested_progression_level__measures_members_below_the_root(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange -- monthly windows live under each hash bucket
    config = TablePartitionConfig(
        table_name="events",
        scheme=HashPartitioning(
            key="tenant_id",
            modulus=2,
            child=RangePartitioning(
                key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)
            ),
        ),
        lifecycle=_size_policy(),
    )
    bucket = PartitionNode(
        name="events__h0",
        parent_name="events",
        level=1,
        partition_type=PartitionType.RANGE,
        partition_columns=("created_at",),
        children=(
            PartitionNode(
                name="events__h0__2024_02",
                parent_name="events__h0",
                level=2,
                bounds=RangeBounds(from_value="2024-02-01", to_value="2024-03-01"),
            ),
        ),
    )
    metadata.get_actual_tree.return_value = ActualTree(
        root=PartitionNode(
            name="events", partition_type=PartitionType.HASH, partition_columns=("tenant_id",), children=(bucket,)
        )
    )

    # Act
    await inspector.inspect(config)

    # Assert
    assert metadata.measure.call_args.kwargs["targets"] == ("events__h0__2024_02",)


# ── context ─────────────────────────────────────────────────────────────────────


async def test__context__no_instant_given__defaults_to_the_current_utc_time(inspector: PartitionInspector) -> None:
    # Arrange
    before = datetime.now(UTC)

    # Act
    context = await inspector.context(_config())

    # Assert
    assert context.now.tzinfo is UTC
    assert before <= context.now <= datetime.now(UTC) + timedelta(seconds=1)
    assert context.mode is PlanMode.MAINTAIN
    assert context.cursors == {}
    assert context.explicit_windows == {}


async def test__context__naive_instant__is_read_as_utc(inspector: PartitionInspector) -> None:
    # Arrange / Act
    context = await inspector.context(_config(), now=NOW.replace(tzinfo=None))

    # Assert
    assert context.now == NOW


async def test__context__aware_instant__is_kept_as_is(inspector: PartitionInspector) -> None:
    # Arrange
    moscow = NOW.astimezone(ZoneInfo("Europe/Moscow"))

    # Act
    context = await inspector.context(_config(), now=moscow)

    # Assert
    assert context.now is moscow


async def test__context__mode_and_windows__are_passed_through(inspector: PartitionInspector) -> None:
    # Arrange
    windows = {"created_at": (Window(start=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 2, 1, tzinfo=UTC)),)}

    # Act
    context = await inspector.context(_config(), now=NOW, mode=PlanMode.EXPLICIT, explicit_windows=windows)

    # Assert
    assert context.mode is PlanMode.EXPLICIT
    assert context.explicit_windows == windows
    assert context.explicit_windows is not windows


async def test__context__time_axis__reads_no_cursor(inspector: PartitionInspector, metadata: MagicMock) -> None:
    # Arrange / Act
    await inspector.context(_config(), now=NOW)

    # Assert
    metadata.get_key_high_water_mark.assert_not_awaited()


async def test__context__integer_axis__reads_the_high_water_mark(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="queue",
        schema="public",
        scheme=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=1000)),
    )
    metadata.get_key_high_water_mark.return_value = 4321

    # Act
    context = await inspector.context(config, now=NOW)

    # Assert
    metadata.get_key_high_water_mark.assert_awaited_once_with("public.queue", "msg_id", sequence=False)
    assert context.cursors == {"msg_id": 4321}


async def test__context__sequence_cursor_source__asks_for_the_sequence(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="queue",
        scheme=RangePartitioning(
            key="msg_id", boundaries=NumericBoundaries(step=1000, cursor_source=CursorSource.SEQUENCE)
        ),
    )
    metadata.get_key_high_water_mark.return_value = 10

    # Act
    context = await inspector.context(config, now=NOW)

    # Assert
    metadata.get_key_high_water_mark.assert_awaited_once_with("queue", "msg_id", sequence=True)
    assert context.cursors == {"msg_id": 10}


async def test__context__empty_table__records_no_cursor(inspector: PartitionInspector, metadata: MagicMock) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="queue", scheme=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=1000))
    )
    metadata.get_key_high_water_mark.return_value = None

    # Act
    context = await inspector.context(config, now=NOW)

    # Assert
    assert context.cursors == {}


async def test__context__integer_axis_below_a_hash_root__is_still_read(
    inspector: PartitionInspector, metadata: MagicMock
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="queue",
        scheme=HashPartitioning(
            key="worker",
            modulus=2,
            child=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=1000)),
        ),
    )
    metadata.get_key_high_water_mark.return_value = 7

    # Act
    context = await inspector.context(config, now=NOW)

    # Assert
    assert context.cursors == {"msg_id": 7}


async def test__context__hash_root_alone__reads_nothing(inspector: PartitionInspector, metadata: MagicMock) -> None:
    # Arrange
    config = TablePartitionConfig(table_name="tasks", scheme=HashPartitioning(key="task_id", modulus=4))

    # Act
    context = await inspector.context(config, now=NOW)

    # Assert
    metadata.get_key_high_water_mark.assert_not_awaited()
    assert context.cursors == {}
