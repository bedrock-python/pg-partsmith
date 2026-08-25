from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks, PartitionLifecycleHooks
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.entities import (
    MaintenanceResult,
    PartitionGranularity,
    PartitionInfo,
    PartitionStrategy,
    PartitionType,
    Period,
    TablePartitionConfig,
)
from pg_partsmith.exceptions import (
    InvalidPartitionConfigError,
    PartitionAlreadyExistsError,
    PartitionAttachedError,
)

# ── fixtures ─────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_repo() -> MagicMock:
    repo = MagicMock()
    repo.create_partition = AsyncMock(return_value=MagicMock())
    repo.attach_partition = AsyncMock(return_value=None)
    repo.detach_partition = AsyncMock(return_value=None)
    repo.drop_partition = AsyncMock(return_value=None)
    repo.reconcile_default_rows = AsyncMock(return_value=0)
    return repo


@pytest.fixture
def mock_metadata() -> MagicMock:
    metadata = MagicMock()
    metadata.partition_exists = AsyncMock(return_value=False)
    metadata.is_partition_attached = AsyncMock(return_value=False)
    metadata.list_partitions = AsyncMock(return_value=[])
    metadata.get_partition_type = AsyncMock(return_value=PartitionType.RANGE)
    metadata.get_partition_column = AsyncMock(return_value="created_at")
    metadata.get_partition_boundaries = AsyncMock(return_value=("2024-01-01", "2024-02-01"))
    metadata.get_default_partition = AsyncMock(return_value=None)
    return metadata


@pytest.fixture
def mock_locks() -> MagicMock:
    locks = MagicMock()
    lock_cm = MagicMock()
    lock_cm.__aenter__ = AsyncMock(return_value=None)
    lock_cm.__aexit__ = AsyncMock(return_value=False)
    locks.acquire_lock.return_value = lock_cm
    return locks


@pytest.fixture
def mock_calculator() -> MagicMock:
    calc = MagicMock()
    calc.current_period.return_value = Period(year=2024, month=3)
    calc.period_before.return_value = Period(year=2024, month=2)
    calc.format_partition_name.return_value = "events__2024_04"
    calc.get_boundaries.return_value = ("2024-04-01", "2024-05-01")
    calc.parse_partition_name.return_value = Period(year=2024, month=4)
    calc.next_periods.return_value = [Period(year=2024, month=4)]
    return calc


@pytest.fixture
def config() -> TablePartitionConfig:
    return TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=1,
        retention_count=2,
    )


@pytest.fixture
def partition_info() -> PartitionInfo:
    return PartitionInfo(
        name="events__2024_04",
        partition_type=PartitionType.RANGE,
        from_value="2024-04-01",
        to_value="2024-05-01",
        is_attached=False,
        parent_table="events",
    )


def _make_service(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    hooks: list[PartitionLifecycleHooks] | None = None,
) -> PartitionLifecycleService:
    return PartitionLifecycleService(mock_repo, mock_metadata, mock_locks, mock_calculator, hooks=hooks)


def _make_23514_exc() -> SQLAlchemyError:
    exc = SQLAlchemyError("updated partition constraint for default partition would be violated by some row")
    orig = MagicMock()
    orig.sqlstate = "23514"
    exc.orig = orig  # type: ignore[attr-defined]
    return exc


# ── create_future_partitions ─────────────────────────────────────────────────────


async def test__create_future_partitions__new_partition__creates_and_returns_it(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    mock_repo.create_partition.return_value = partition_info
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    created = await service.create_future_partitions(config)

    # Assert
    assert len(created) == 1
    assert created[0].name == "events__2024_04"
    mock_repo.create_partition.assert_called_once_with(config, "events__2024_04", "2024-04-01", "2024-05-01")


async def test__create_future_partitions__partition_already_attached__skips_create_and_attach(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_metadata.list_partitions.return_value = [
        PartitionInfo(
            name="events__2024_04",
            partition_type=PartitionType.RANGE,
            from_value="2024-04-01",
            to_value="2024-05-01",
            is_attached=True,
            parent_table="events",
        )
    ]
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    created = await service.create_future_partitions(config)

    # Assert
    assert len(created) == 0
    mock_repo.create_partition.assert_not_called()
    mock_repo.attach_partition.assert_not_called()


async def test__create_future_partitions__partition_exists_unattached__attaches_without_create(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_metadata.list_partitions.return_value = [
        PartitionInfo(
            name="events__2024_04",
            partition_type=PartitionType.RANGE,
            from_value=None,
            to_value=None,
            is_attached=False,
            parent_table="events",
        )
    ]
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    created = await service.create_future_partitions(config)

    # Assert
    assert created == []
    mock_repo.create_partition.assert_not_called()
    mock_repo.attach_partition.assert_called_once_with("events", "events__2024_04", "2024-04-01", "2024-05-01")


async def test__create_future_partitions__auto_attach_enabled__attaches_after_create(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    config = config.model_copy(update={"auto_attach_after_create": True})
    mock_repo.create_partition.return_value = partition_info
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    created = await service.create_future_partitions(config)

    # Assert
    assert created[0].is_attached is True
    mock_repo.attach_partition.assert_called_once()


async def test__create_future_partitions__partition_name_exceeds_identifier_limit__raises_invalid_config(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_calculator.format_partition_name.return_value = "e" * 64
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="identifier limit"):
        await service.create_future_partitions(config)


async def test__create_future_partitions__concurrent_create_race__attaches_existing_unattached(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — create raises AlreadyExists because concurrent worker won the race
    mock_repo.create_partition.side_effect = PartitionAlreadyExistsError("events__2024_04")
    mock_metadata.is_partition_attached.return_value = False
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    created = await service.create_future_partitions(config)

    # Assert
    assert created == []
    mock_repo.attach_partition.assert_called_once_with("events", "events__2024_04", "2024-04-01", "2024-05-01")


async def test__create_future_partitions__create_race_already_attached__still_attaches(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_repo.create_partition.side_effect = PartitionAlreadyExistsError("events__2024_04")
    mock_metadata.is_partition_attached.return_value = True
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    created = await service.create_future_partitions(config)

    # Assert
    assert created == []
    mock_repo.attach_partition.assert_called_once_with("events", "events__2024_04", "2024-04-01", "2024-05-01")


async def test__create_future_partitions__db_error__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_repo.create_partition.side_effect = SQLAlchemyError("create failed")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="create failed"):
        await service.create_future_partitions(config)


# ── detach_old_partitions ────────────────────────────────────────────────────────


async def test__detach_old_partitions__attached_partition__detaches_and_returns_name(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    attached = partition_info.model_copy(update={"is_attached": True})
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    detached = await service.detach_old_partitions("events", [attached])

    # Assert
    assert detached == ["events__2024_04"]
    mock_repo.detach_partition.assert_called_once_with("events", "events__2024_04", concurrent=True)


async def test__detach_old_partitions__already_detached_partition__counts_without_calling_detach(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — partition_info is already detached (is_attached=False)
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    detached = await service.detach_old_partitions("events", [partition_info])

    # Assert
    assert detached == ["events__2024_04"]
    mock_repo.detach_partition.assert_not_called()


async def test__detach_old_partitions__db_error__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    mock_repo.detach_partition.side_effect = SQLAlchemyError("db error")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)
    attached = partition_info.model_copy(update={"is_attached": True})

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="db error"):
        await service.detach_old_partitions("events", [attached])


# ── drop_detached_partitions ─────────────────────────────────────────────────────


async def test__drop_detached_partitions__detached_partition__drops_and_returns_count(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    count = await service.drop_detached_partitions("events", ["events__2024_04"])

    # Assert
    assert count == 1
    mock_repo.drop_partition.assert_called_once_with("events__2024_04")


async def test__drop_detached_partitions__still_attached__skips_and_returns_zero(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    mock_repo.drop_partition.side_effect = PartitionAttachedError("events__2024_04", "events")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    count = await service.drop_detached_partitions("events", ["events__2024_04"])

    # Assert
    assert count == 0
    mock_repo.drop_partition.assert_called_once_with("events__2024_04")


async def test__drop_detached_partitions__db_error__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    mock_repo.drop_partition.side_effect = SQLAlchemyError("drop error")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="drop error"):
        await service.drop_detached_partitions("events", ["events__2024_04"])


# ── get_partitions_for_pruning ───────────────────────────────────────────────────


async def test__get_partitions_for_pruning__old_partition__returns_it(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    old_partition = PartitionInfo(
        name="events__2024_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="2024-02-01",
    )
    mock_metadata.list_partitions.return_value = [old_partition]
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    to_prune = await service.get_partitions_for_pruning(config)

    # Assert
    assert len(to_prune) == 1
    assert to_prune[0].name == "events__2024_01"


async def test__get_partitions_for_pruning__recent_partition__returns_empty(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    recent = PartitionInfo(
        name="events__2024_02",
        partition_type=PartitionType.RANGE,
        from_value="2024-02-01",
        to_value="2024-03-01",
    )
    mock_metadata.list_partitions.return_value = [recent]
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    assert await service.get_partitions_for_pruning(config) == []


async def test__get_partitions_for_pruning__default_partition__skips_without_parsing(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    default_partition = PartitionInfo(
        name="events__2023_12",
        partition_type=PartitionType.RANGE,
        from_value=None,
        to_value=None,
        is_attached=True,
        is_default=True,
    )
    mock_metadata.list_partitions.return_value = [default_partition]
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    to_prune = await service.get_partitions_for_pruning(config)

    # Assert
    assert to_prune == []
    mock_calculator.parse_partition_name.assert_not_called()


async def test__get_partitions_for_pruning__parse_error__skips_partition(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    partition = PartitionInfo(
        name="events__broken",
        partition_type=PartitionType.RANGE,
        from_value="2024-04-01",
        to_value="1",
        is_attached=True,
    )
    mock_metadata.list_partitions.return_value = [partition]
    mock_calculator.parse_partition_name.side_effect = ValueError("bad name")
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    assert await service.get_partitions_for_pruning(config) == []


async def test__get_partitions_for_pruning__invalid_qualified_name__skips_and_logs_warning(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    partition = PartitionInfo(
        name="public.events.bad_extra",
        partition_type=PartitionType.RANGE,
        from_value="2024-04-01",
        to_value="1",
        is_attached=True,
    )
    mock_metadata.list_partitions.return_value = [partition]
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    mock_logger = MagicMock()
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    with patch("pg_partsmith.aio.services.pruning.logger", mock_logger):
        to_prune = await service.get_partitions_for_pruning(config)

    # Assert
    assert to_prune == []
    mock_logger.warning.assert_called_once()


async def test__get_partitions_for_pruning__non_comparable_period__skips_and_logs_warning(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    partition = PartitionInfo(
        name="events__2024_12",
        partition_type=PartitionType.RANGE,
        from_value="2024-12-01",
        to_value="1",
        is_attached=True,
    )
    mock_metadata.list_partitions.return_value = [partition]
    mock_calculator.parse_partition_name.return_value = Period(year=2024, week=12)
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    mock_logger = MagicMock()
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    with patch("pg_partsmith.aio.services.pruning.logger", mock_logger):
        to_prune = await service.get_partitions_for_pruning(config)

    # Assert
    assert to_prune == []
    mock_logger.warning.assert_called_once()


async def test__get_partitions_for_pruning__retention_cutoff__includes_current_period(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — retention_count=2: keeps current + 1 before; anything older is pruned
    partitions = [
        PartitionInfo(
            name="events__2024_01", partition_type=PartitionType.RANGE, from_value="2024-01-01", to_value="2024-02-01"
        ),
        PartitionInfo(
            name="events__2024_02", partition_type=PartitionType.RANGE, from_value="2024-02-01", to_value="2024-03-01"
        ),
        PartitionInfo(
            name="events__2024_03", partition_type=PartitionType.RANGE, from_value="2024-03-01", to_value="2024-04-01"
        ),
    ]
    mock_metadata.list_partitions.return_value = partitions
    mock_calculator.current_period.return_value = Period(year=2024, month=3)
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    to_prune = await service.get_partitions_for_pruning(config)

    # Assert
    assert [p.name for p in to_prune] == ["events__2024_01"]
    mock_calculator.period_before.assert_called_once_with(Period(year=2024, month=3), 1)
    mock_calculator.parse_partition_name.assert_not_called()


async def test__get_partitions_for_pruning__hourly_name_fallback__sorts_same_day_hours_chronologically(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — no boundary values forces the name-based fallback; hour suffixes are deliberately
    # not zero-padded so lexical name order (10 < 7 < 8) differs from chronological order.
    partitions = [
        PartitionInfo(
            name=f"events__2024_03_15_{hour}",
            partition_type=PartitionType.RANGE,
            boundaries_expr="FOR VALUES FROM (...) TO (...)",
            is_attached=True,
        )
        for hour in (10, 7, 11, 8)
    ]
    mock_metadata.list_partitions.return_value = partitions
    mock_calculator.current_period.return_value = Period(year=2024, month=3, day=15, hour=12)
    mock_calculator.period_before.return_value = Period(year=2024, month=3, day=15, hour=11)
    mock_calculator.get_boundaries.return_value = ("2024-03-15 11:00:00+00", "2024-03-15 12:00:00+00")
    mock_calculator.parse_partition_name.side_effect = lambda name: Period(
        year=2024, month=3, day=15, hour=int(name.rsplit("_", 1)[-1])
    )
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    to_prune = await service.get_partitions_for_pruning(config)

    # Assert — hours 7, 8 and 10 are older than the cutoff (hour 11) and come back chronologically
    assert [p.name for p in to_prune] == ["events__2024_03_15_7", "events__2024_03_15_8", "events__2024_03_15_10"]


# ── maintain_lifecycle ───────────────────────────────────────────────────────────


async def test__maintain_lifecycle__full_flow__returns_successful_result(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    mock_repo.create_partition.return_value = partition_info
    mock_metadata.list_partitions.return_value = []
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    result = await service.maintain_lifecycle(config)

    # Assert
    assert isinstance(result, MaintenanceResult)
    assert result.created_count == 1
    assert result.error is None


async def test__maintain_lifecycle__db_error_on_create__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_repo.create_partition.side_effect = SQLAlchemyError("db gone")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="db gone"):
        await service.maintain_lifecycle(config)


async def test__maintain_lifecycle__partition_column_mismatch__raises_invalid_config(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_metadata.get_partition_column.return_value = "other_col"
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="Partition column mismatch"):
        await service.maintain_lifecycle(config)


async def test__maintain_lifecycle__table_not_partitioned__raises_invalid_config(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_metadata.get_partition_type.return_value = None
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="not partitioned"):
        await service.maintain_lifecycle(config)


async def test__maintain_lifecycle__skip_create__skips_partition_creation(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_metadata.list_partitions.return_value = []
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    result = await service.maintain_lifecycle(config, skip_create=True)

    # Assert
    assert result.created_count == 0
    mock_repo.create_partition.assert_not_called()


async def test__maintain_lifecycle__orphan_drop__does_not_increment_detached_count(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    orphan = PartitionInfo(
        name="events__orphan",
        partition_type=PartitionType.RANGE,
        from_value=None,
        to_value=None,
        is_attached=False,
        parent_table="events",
    )
    mock_metadata.list_partitions.return_value = [orphan]
    mock_calculator.parse_partition_name.return_value = None
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    result = await service.maintain_lifecycle(config, skip_create=True)

    # Assert
    assert result.detached_count == 0
    assert result.dropped_count == 1
    mock_repo.detach_partition.assert_not_called()
    mock_repo.drop_partition.assert_called_once_with("events__orphan")


# ── validation service ───────────────────────────────────────────────────────────


async def test__validation_service__partition_type_mismatch__raises_invalid_config(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )
    service = PartitionLifecycleService(mock_repo, mock_metadata, mock_locks, mock_calculator)
    mock_metadata.get_partition_type.return_value = PartitionType.LIST

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="Partition type mismatch"):
        await service._validation_service.validate_config(config)


async def test__validation_service__metadata_error_on_column__raises_invalid_config(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )
    service = PartitionLifecycleService(mock_repo, mock_metadata, mock_locks, mock_calculator)
    mock_metadata.get_partition_type.return_value = PartitionType.RANGE
    mock_metadata.get_partition_column.side_effect = ValueError("metadata error")

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="metadata error"):
        await service._validation_service.validate_config(config)


async def test__validation_service__column_none__raises_invalid_config(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
    )
    service = PartitionLifecycleService(mock_repo, mock_metadata, mock_locks, mock_calculator)
    mock_metadata.get_partition_type.return_value = PartitionType.RANGE
    mock_metadata.get_partition_column.side_effect = None
    mock_metadata.get_partition_column.return_value = None

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="Could not determine partition column"):
        await service._validation_service.validate_config(config)


async def test__creation_service__list_partitions_error__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    config = TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="created_at",
        granularity=PartitionGranularity.MONTH,
        create_ahead_count=1,
    )
    mock_calculator.next_periods.return_value = [Period(year=2024, month=4)]
    mock_calculator.get_boundaries.return_value = ("2024-04-01", "2024-05-01")
    mock_metadata.list_partitions.side_effect = SQLAlchemyError("db down")
    service = PartitionLifecycleService(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="db down"):
        await service._creation_service.create_future_partitions(config)


# ── hooks integration ────────────────────────────────────────────────────────────


async def test__hooks__before_and_after_create__called_in_order(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    calls: list[str] = []

    class TrackingHooks(BasePartitionLifecycleHooks):
        async def before_create(self, cfg: TablePartitionConfig, name: str, fv: str, tv: str) -> None:
            calls.append(f"before_create:{name}")

        async def after_create(self, cfg: TablePartitionConfig, p: PartitionInfo) -> None:
            calls.append(f"after_create:{p.name}")

    mock_repo.create_partition.return_value = partition_info
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator, hooks=[TrackingHooks()])

    # Act
    await service.create_future_partitions(config)

    # Assert
    assert "before_create:events__2024_04" in calls
    assert "after_create:events__2024_04" in calls


async def test__hooks__before_create_raises__aborts_create(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    class ErrorHook(BasePartitionLifecycleHooks):
        async def before_create(self, cfg: TablePartitionConfig, name: str, fv: str, tv: str) -> None:
            raise RuntimeError("hook failed")

    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator, hooks=[ErrorHook()])

    # Act / Assert
    with pytest.raises(RuntimeError, match="hook failed"):
        await service.create_future_partitions(config)

    mock_repo.create_partition.assert_not_called()


async def test__hooks__before_and_after_detach__called_in_order(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    calls: list[str] = []

    class TrackingHooks(BasePartitionLifecycleHooks):
        async def before_detach(self, table_name: str, p: PartitionInfo) -> None:
            calls.append(f"before_detach:{p.name}")

        async def after_detach(self, table_name: str, partition_name: str) -> None:
            calls.append(f"after_detach:{partition_name}")

    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator, hooks=[TrackingHooks()])
    attached = partition_info.model_copy(update={"is_attached": True})

    # Act
    await service.detach_old_partitions("events", [attached])

    # Assert
    assert "before_detach:events__2024_04" in calls
    assert "after_detach:events__2024_04" in calls


async def test__hooks__before_and_after_drop__called_in_order(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    calls: list[str] = []

    class TrackingHooks(BasePartitionLifecycleHooks):
        async def before_drop(self, table_name: str, partition_name: str) -> None:
            calls.append(f"before_drop:{partition_name}")

        async def after_drop(self, table_name: str, partition_name: str) -> None:
            calls.append(f"after_drop:{partition_name}")

    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator, hooks=[TrackingHooks()])

    # Act
    await service.drop_detached_partitions("events", ["events__2024_04"])

    # Assert
    assert "before_drop:events__2024_04" in calls
    assert "after_drop:events__2024_04" in calls


async def test__hooks__multiple_hooks__all_called_in_registration_order(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange
    calls: list[str] = []

    class HookA(BasePartitionLifecycleHooks):
        async def before_drop(self, table_name: str, partition_name: str) -> None:
            calls.append("A")

    class HookB(BasePartitionLifecycleHooks):
        async def before_drop(self, table_name: str, partition_name: str) -> None:
            calls.append("B")

    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator, hooks=[HookA(), HookB()])

    # Act
    await service.drop_detached_partitions("events", ["events__2024_04"])

    # Assert
    assert calls == ["A", "B"]


async def test__hooks__before_create_not_fired_when_partition_already_exists(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange
    mock_metadata.list_partitions.return_value = [
        PartitionInfo(
            name="events__2024_04",
            partition_type=PartitionType.RANGE,
            from_value="2024-04-01",
            to_value="2024-05-01",
            is_attached=True,
            parent_table="events",
        )
    ]
    calls: list[str] = []

    class TrackingHooks(BasePartitionLifecycleHooks):
        async def before_create(self, cfg: TablePartitionConfig, name: str, fv: str, tv: str) -> None:
            calls.append("before_create")

    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator, hooks=[TrackingHooks()])

    # Act
    await service.create_future_partitions(config)

    # Assert
    assert calls == []


# ── DEFAULT partition reconciliation ─────────────────────────────────────────────


async def test__create_future_partitions__23514_error__reconciles_default_and_retries(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = [_make_23514_exc(), None]
    mock_repo.reconcile_default_rows.return_value = 5
    mock_metadata.get_default_partition.return_value = PartitionInfo(
        name="events_default", partition_type=PartitionType.RANGE, is_default=True, is_attached=True
    )
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    created = await service.create_future_partitions(config)

    # Assert
    mock_repo.reconcile_default_rows.assert_called_once()
    assert mock_repo.attach_partition.call_count == 2
    assert len(created) == 1


async def test__create_future_partitions__23514_error_no_default_partition__raises(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = _make_23514_exc()
    mock_metadata.get_default_partition.return_value = None
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="default partition"):
        await service.create_future_partitions(config)


async def test__create_future_partitions__23514_retries_exhausted__raises(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = _make_23514_exc()  # always fails
    mock_repo.reconcile_default_rows.return_value = 5
    mock_metadata.get_default_partition.return_value = PartitionInfo(
        name="events_default", partition_type=PartitionType.RANGE, is_default=True, is_attached=True
    )
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="default partition"):
        await service.create_future_partitions(config)

    assert mock_repo.attach_partition.call_count == 2
    assert mock_repo.reconcile_default_rows.call_count == 1
