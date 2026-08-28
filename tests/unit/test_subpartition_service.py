"""Behaviour of the reconciliation service around races and unsupported wirings."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.services.subpartitions import PartitionSubpartitionService
from pg_partsmith.entities import (
    DefaultBounds,
    HashBounds,
    HashSubpartitionSpec,
    PartitionGranularity,
    PartitionNode,
    PartitionStrategy,
    PartitionType,
    RangeBounds,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import PartitionAlreadyExistsError, UnsupportedCapabilityError
from pg_partsmith.subpartition_plan import SubpartitionAction, TopologyFinding, TopologyReason

BRANCH = "public.events__2026_w35"


def _config(*, subpartition: HashSubpartitionSpec | None = None) -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        schema="public",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.WEEK,
        subpartition=subpartition,
    )


def _spec(modulus: int = 2) -> HashSubpartitionSpec:
    return HashSubpartitionSpec(column="tenant_id", modulus=modulus)


@pytest.fixture
def repo() -> MagicMock:
    repo = MagicMock()
    repo.create_branch = AsyncMock()
    repo.create_subpartition_table = AsyncMock(return_value=None)
    repo.attach_subpartition = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def metadata() -> MagicMock:
    metadata = MagicMock()
    metadata.get_partition_tree = AsyncMock(return_value=None)
    metadata.get_unique_constraint_columns = AsyncMock(return_value=())
    metadata.is_partition_attached = AsyncMock(return_value=False)
    return metadata


def _branch_node(*remainders: int, modulus: int = 2) -> PartitionNode:
    return PartitionNode(
        name=BRANCH,
        parent_name="public.events",
        level=1,
        bounds=RangeBounds(from_value="2026-08-24", to_value="2026-08-31"),
        partition_type=PartitionType.HASH,
        partition_columns=("tenant_id",),
        children=tuple(
            PartitionNode(name=f"{BRANCH}__h{r}", bounds=HashBounds(modulus=modulus, remainder=r)) for r in remainders
        ),
    )


def _root_with(branch: PartitionNode) -> PartitionNode:
    return PartitionNode(
        name="public.events",
        partition_type=PartitionType.RANGE,
        partition_columns=("created_at",),
        children=(branch,),
    )


# ── Wiring ──────────────────────────────────────────────────────────────────────


async def test__reconcile__config_without_a_spec__is_a_no_op(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    service = PartitionSubpartitionService(repo, metadata)

    # Act
    result = await service.reconcile(_config())

    # Assert
    assert result.created_count == 0
    assert result.findings == ()
    metadata.get_partition_tree.assert_not_awaited()


async def test__reconcile__repository_without_subpartition_ddl__refused_with_an_explanation(
    metadata: MagicMock,
) -> None:
    # Arrange: a repository written against the flat protocol only.
    flat_repo = MagicMock(spec=["create_partition", "attach_partition"])
    service = PartitionSubpartitionService(flat_repo, metadata)

    # Act / Assert
    with pytest.raises(UnsupportedCapabilityError, match="SubpartitionRepository"):
        await service.reconcile(_config(subpartition=_spec()))


async def test__reconcile__metadata_without_tree_introspection__refused_with_an_explanation(
    repo: MagicMock,
) -> None:
    # Arrange
    flat_metadata = MagicMock(spec=["list_partitions", "get_partition_type"])
    service = PartitionSubpartitionService(repo, flat_metadata)

    # Act / Assert
    with pytest.raises(UnsupportedCapabilityError, match="NestedPartitionMetadata"):
        await service.reconcile(_config(subpartition=_spec()))


async def test__build_new_branch__config_without_a_spec__rejected(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    service = PartitionSubpartitionService(repo, metadata)

    # Act / Assert
    with pytest.raises(ValueError, match="requires a config with a subpartition spec"):
        await service.build_new_branch(_config(), BRANCH)


# ── Convergence ─────────────────────────────────────────────────────────────────


async def test__reconcile__root_not_partitioned__returns_an_empty_result(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    metadata.get_partition_tree = AsyncMock(return_value=None)
    service = PartitionSubpartitionService(repo, metadata)

    # Act
    result = await service.reconcile(_config(subpartition=_spec()))

    # Assert
    assert result.created_count == 0
    repo.create_subpartition_table.assert_not_awaited()


async def test__reconcile__branch_missing_a_bucket__creates_and_attaches_only_that_one(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_tree = AsyncMock(return_value=_root_with(_branch_node(0)))
    service = PartitionSubpartitionService(repo, metadata)

    # Act
    result = await service.reconcile(_config(subpartition=_spec(modulus=2)))

    # Assert
    assert result.created_count == 1
    repo.create_subpartition_table.assert_awaited_once_with(BRANCH, f"{BRANCH}__h1", None)
    repo.attach_subpartition.assert_awaited_once_with(BRANCH, f"{BRANCH}__h1", HashBounds(modulus=2, remainder=1))


async def test__reconcile__excluded_branch__is_skipped(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    metadata.get_partition_tree = AsyncMock(return_value=_root_with(_branch_node(0)))
    service = PartitionSubpartitionService(repo, metadata)

    # Act: a branch about to be pruned is not worth repairing.
    result = await service.reconcile(_config(subpartition=_spec()), exclude={BRANCH})

    # Assert
    assert result.created_count == 0
    repo.create_subpartition_table.assert_not_awaited()


async def test__converge_branch__branch_does_not_exist__returns_an_empty_result(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    metadata.get_partition_tree = AsyncMock(return_value=None)
    service = PartitionSubpartitionService(repo, metadata)

    # Act
    result = await service.converge_branch(_config(subpartition=_spec()), BRANCH)

    # Assert
    assert result.created_count == 0


# ── Interrupted runs and races ──────────────────────────────────────────────────


async def test__materialize__table_left_by_an_interrupted_run__attached_rather_than_recreated(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    repo.create_subpartition_table = AsyncMock(side_effect=PartitionAlreadyExistsError(f"{BRANCH}__h0"))
    service = PartitionSubpartitionService(repo, metadata)
    action = SubpartitionAction(
        parent_name=BRANCH, child_name=f"{BRANCH}__h0", bounds=HashBounds(modulus=2, remainder=0)
    )

    # Act
    created = await service.materialize((action,))

    # Assert: the half-finished node is completed, not abandoned.
    assert created == 1
    repo.attach_subpartition.assert_awaited_once()


async def test__materialize__concurrent_worker_won_the_attach__tolerated_without_double_counting(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    conflict = SQLAlchemyError("duplicate")
    conflict.orig = MagicMock(sqlstate="42P07")  # type: ignore[attr-defined]
    repo.attach_subpartition = AsyncMock(side_effect=conflict)
    metadata.is_partition_attached = AsyncMock(return_value=True)
    service = PartitionSubpartitionService(repo, metadata)
    action = SubpartitionAction(
        parent_name=BRANCH, child_name=f"{BRANCH}__h0", bounds=HashBounds(modulus=2, remainder=0)
    )

    # Act
    created = await service.materialize((action,))

    # Assert
    assert created == 0


async def test__materialize__conflict_on_a_relation_with_other_bounds__reported_not_swallowed(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange: 42809 fires whenever the name is taken, whether another worker
    # just created this partition or an unrelated relation holds the name.
    conflict = SQLAlchemyError("wrong object type")
    conflict.orig = MagicMock(sqlstate="42809")  # type: ignore[attr-defined]
    repo.attach_subpartition = AsyncMock(side_effect=conflict)
    metadata.is_partition_attached = AsyncMock(return_value=True)
    metadata.get_partition_tree = AsyncMock(
        return_value=PartitionNode(name=f"{BRANCH}__h0", bounds=HashBounds(modulus=4, remainder=0))
    )
    service = PartitionSubpartitionService(repo, metadata)
    action = SubpartitionAction(
        parent_name=BRANCH, child_name=f"{BRANCH}__h0", bounds=HashBounds(modulus=2, remainder=0)
    )
    findings: list[TopologyFinding] = []

    # Act
    created = await service.materialize((action,), findings)

    # Assert: reporting it beats treating a real conflict as a won race, which
    # left that slice of the keyspace rejecting rows and reported success.
    assert created == 0
    assert [f.reason for f in findings] == [TopologyReason.NAME_UNUSABLE]


async def test__materialize__conflict_with_the_planned_bounds__treated_as_a_lost_race(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    conflict = SQLAlchemyError("already a partition")
    conflict.orig = MagicMock(sqlstate="42809")  # type: ignore[attr-defined]
    repo.attach_subpartition = AsyncMock(side_effect=conflict)
    metadata.is_partition_attached = AsyncMock(return_value=True)
    metadata.get_partition_tree = AsyncMock(
        return_value=PartitionNode(name=f"{BRANCH}__h0", bounds=HashBounds(modulus=2, remainder=0))
    )
    service = PartitionSubpartitionService(repo, metadata)
    action = SubpartitionAction(
        parent_name=BRANCH, child_name=f"{BRANCH}__h0", bounds=HashBounds(modulus=2, remainder=0)
    )
    findings: list[TopologyFinding] = []

    # Act
    created = await service.materialize((action,), findings)

    # Assert: same bounds means another worker genuinely got there first.
    assert created == 0
    assert findings == []


async def test__materialize__default_partition_holding_the_rows__reported_not_raised(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange: PostgreSQL refuses the attach while a DEFAULT sibling holds rows
    # belonging to the new partition.
    conflict = SQLAlchemyError("updated partition constraint for default partition would be violated")
    conflict.orig = MagicMock(sqlstate="23514")  # type: ignore[attr-defined]
    repo.attach_subpartition = AsyncMock(side_effect=conflict)
    service = PartitionSubpartitionService(repo, metadata)
    action = SubpartitionAction(
        parent_name=BRANCH, child_name=f"{BRANCH}__eu", bounds=HashBounds(modulus=2, remainder=0)
    )
    findings: list[TopologyFinding] = []

    # Act
    created = await service.materialize((action,), findings)

    # Assert: raising here aborted reconcile for the whole table, which also
    # blocked pruning because reconcile runs first.
    assert created == 0
    assert [f.reason for f in findings] == [TopologyReason.DEFAULT_HOLDS_ROWS]


async def test__materialize__unrelated_database_error__propagates(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    failure = SQLAlchemyError("disk full")
    failure.orig = MagicMock(sqlstate="53100")  # type: ignore[attr-defined]
    repo.attach_subpartition = AsyncMock(side_effect=failure)
    service = PartitionSubpartitionService(repo, metadata)
    action = SubpartitionAction(
        parent_name=BRANCH, child_name=f"{BRANCH}__h0", bounds=HashBounds(modulus=2, remainder=0)
    )

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="disk full"):
        await service.materialize((action,))


async def test__materialize__nested_action__creates_children_before_attaching_the_parent(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange
    order: list[str] = []
    repo.create_subpartition_table = AsyncMock(side_effect=lambda _p, c, _s: order.append(f"create {c}"))
    repo.attach_subpartition = AsyncMock(side_effect=lambda _p, c, _b: order.append(f"attach {c}"))
    service = PartitionSubpartitionService(repo, metadata)
    action = SubpartitionAction(
        parent_name=BRANCH,
        child_name=f"{BRANCH}__h0",
        bounds=HashBounds(modulus=1, remainder=0),
        subpartition=_spec(modulus=1),
        children=(
            SubpartitionAction(
                parent_name=f"{BRANCH}__h0",
                child_name=f"{BRANCH}__h0__h0",
                bounds=HashBounds(modulus=1, remainder=0),
            ),
        ),
    )

    # Act
    created = await service.materialize((action,))

    # Assert: the inner node is in place before the outer one becomes reachable.
    assert created == 2
    assert order == [
        f"create {BRANCH}__h0",
        f"create {BRANCH}__h0__h0",
        f"attach {BRANCH}__h0__h0",
        f"attach {BRANCH}__h0",
    ]


# ── Which branches a run touches, and what one failure costs ────────────────────


async def test__reconcile__branch_named_in_exclude__is_left_alone(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange -- the branch this run is about to prune.
    metadata.get_partition_tree = AsyncMock(return_value=_root_with(_branch_node(0)))
    service = PartitionSubpartitionService(repo, metadata)

    # Act
    result = await service.reconcile(_config(subpartition=_spec()), exclude={BRANCH})

    # Assert -- repairing a branch about to be dropped is pure waste, and takes
    # locks on it while it happens.
    assert result.created_count == 0
    repo.create_subpartition_table.assert_not_called()


async def test__reconcile__detached_branch__is_left_alone(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    branch = _branch_node(0).model_copy(update={"is_attached": False})
    metadata.get_partition_tree = AsyncMock(return_value=_root_with(branch))
    service = PartitionSubpartitionService(repo, metadata)

    # Act
    result = await service.reconcile(_config(subpartition=_spec()))

    # Assert -- a detached branch routes no rows, so a gap in it rejects nothing.
    assert result.created_count == 0
    repo.create_subpartition_table.assert_not_called()


async def test__reconcile__default_partition__is_left_alone(repo: MagicMock, metadata: MagicMock) -> None:
    # Arrange
    default = PartitionNode(name="public.events_default", parent_name="public.events", level=1, bounds=DefaultBounds())
    metadata.get_partition_tree = AsyncMock(return_value=_root_with(default))
    service = PartitionSubpartitionService(repo, metadata)

    # Act
    result = await service.reconcile(_config(subpartition=_spec()))

    # Assert -- it is a catch-all leaf by design; subpartitioning it would move
    # where overflow rows land.
    assert result.created_count == 0
    repo.create_subpartition_table.assert_not_called()


async def test__reconcile__one_branch_fails__the_others_are_still_converged(
    repo: MagicMock, metadata: MagicMock
) -> None:
    # Arrange -- two branches, each missing remainder 1; the first one throws.
    first = _branch_node(0)
    second = _branch_node(0).model_copy(update={"name": "public.events__2026_w36"})
    root = _root_with(first).model_copy(update={"children": (first, second)})
    metadata.get_partition_tree = AsyncMock(return_value=root)
    repo.create_subpartition_table = AsyncMock(side_effect=[SQLAlchemyError("disk full"), None])
    service = PartitionSubpartitionService(repo, metadata)

    # Act
    result = await service.reconcile(_config(subpartition=_spec()))

    # Assert -- reconciliation runs before pruning, so letting one branch abort
    # the run would also stop the table reclaiming disk, on every run forever.
    assert result.created_count == 1
    assert [f.reason for f in result.findings] == [TopologyReason.UNCONVERGEABLE]
    assert result.findings[0].partition_name == BRANCH
