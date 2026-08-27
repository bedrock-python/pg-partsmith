import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from pg_partsmith.aio.hooks import BasePartitionLifecycleHooks, PartitionLifecycleHooks
from pg_partsmith.aio.service import PartitionLifecycleService
from pg_partsmith.entities import (
    MaintenanceIssueStep,
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


def _make_sqlstate_exc(sqlstate: str, message: str = "pg error") -> SQLAlchemyError:
    exc = SQLAlchemyError(message)
    orig = MagicMock()
    orig.sqlstate = sqlstate
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


# ── ensure_partition ─────────────────────────────────────────────────────────────


async def test__ensure_partition__new_period__creates_attaches_and_returns_partition(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — no partition exists yet for the requested period
    mock_repo.create_partition.return_value = partition_info
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    result = await service.ensure_partition(config, Period(year=2024, month=4))

    # Assert
    assert result is not None
    assert result.name == "events__2024_04"
    assert result.is_attached is True
    mock_metadata.list_partitions.assert_called_once_with("events")
    mock_repo.create_partition.assert_called_once_with(config, "events__2024_04", "2024-04-01", "2024-05-01")
    mock_repo.attach_partition.assert_called_once_with("events", "events__2024_04", "2024-04-01", "2024-05-01")


async def test__ensure_partition__existing_attached_partition__returns_none_without_create(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — the partition for the period is already attached (idempotent no-op)
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
    result = await service.ensure_partition(config, Period(year=2024, month=4))

    # Assert
    assert result is None
    mock_repo.create_partition.assert_not_called()
    mock_repo.attach_partition.assert_not_called()


async def test__ensure_partition__existing_detached_partition__reattaches_and_returns_none(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — the partition table exists but is detached; the reconcile path re-attaches it
    mock_metadata.list_partitions.return_value = [
        PartitionInfo(
            name="events__2024_04",
            partition_type=PartitionType.RANGE,
            is_attached=False,
            parent_table="events",
        )
    ]
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    result = await service.ensure_partition(config, Period(year=2024, month=4))

    # Assert
    assert result is None
    mock_repo.create_partition.assert_not_called()
    mock_repo.attach_partition.assert_called_once_with("events", "events__2024_04", "2024-04-01", "2024-05-01")


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
    # Arrange — detached orphans carry no boundary values, forcing the name-based fallback; hour
    # suffixes are deliberately not zero-padded so lexical name order (10 < 7 < 8) differs from
    # chronological order. (Attached partitions with unparseable boundaries are now skipped fail-closed.)
    partitions = [
        PartitionInfo(
            name=f"events__2024_03_15_{hour}",
            partition_type=PartitionType.RANGE,
            is_attached=False,
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


async def test__get_partitions_for_pruning__maxvalue_upper_bound__never_pruned_despite_ancient_name(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — an unbounded partition whose name would parse as far older than the cutoff
    partition = PartitionInfo(
        name="events__1970_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="MAXVALUE",
        is_attached=True,
    )
    mock_metadata.list_partitions.return_value = [partition]
    mock_calculator.parse_partition_name.return_value = Period(year=1970, month=1)
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    to_prune = await service.get_partitions_for_pruning(config)

    # Assert — skipped entirely: no pruning, not even the name-based fallback
    assert to_prune == []
    mock_calculator.parse_partition_name.assert_not_called()


async def test__get_partitions_for_pruning__minvalue_lower_bound__still_pruned_by_upper_boundary(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — only the upper bound matters: a MINVALUE lower bound does not protect the partition
    partition = PartitionInfo(
        name="events__historic",
        partition_type=PartitionType.RANGE,
        from_value="MINVALUE",
        to_value="2024-02-01",
        is_attached=True,
    )
    mock_metadata.list_partitions.return_value = [partition]
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    to_prune = await service.get_partitions_for_pruning(config)

    # Assert
    assert [p.name for p in to_prune] == ["events__historic"]


async def test__get_partitions_for_pruning__infinity_upper_bound__never_pruned_despite_ancient_name(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — 'infinity' is an unbounded upper bound just like MAXVALUE and must never be pruned
    partition = PartitionInfo(
        name="events__1970_01",
        partition_type=PartitionType.RANGE,
        from_value="2024-01-01",
        to_value="infinity",
        is_attached=True,
    )
    mock_metadata.list_partitions.return_value = [partition]
    mock_calculator.parse_partition_name.return_value = Period(year=1970, month=1)
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    to_prune = await service.get_partitions_for_pruning(config)

    # Assert — skipped entirely: no pruning, not even the name-based fallback
    assert to_prune == []
    mock_calculator.parse_partition_name.assert_not_called()


async def test__get_partitions_for_pruning__attached_unparseable_boundary__skipped_while_orphan_pruned_by_name(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — an ATTACHED partition whose catalog boundary cannot be interpreted must fail closed
    # (skip + warning), while a detached orphan without boundaries still prunes via the name fallback
    attached = PartitionInfo(
        name="events__2023_01",
        partition_type=PartitionType.RANGE,
        from_value="garbage",
        to_value="garbage",
        boundaries_expr="FOR VALUES FROM (garbage) TO (garbage)",
        is_attached=True,
    )
    orphan = PartitionInfo(
        name="events__2023_02",
        partition_type=PartitionType.RANGE,
        is_attached=False,
    )
    mock_metadata.list_partitions.return_value = [attached, orphan]
    mock_calculator.parse_partition_name.side_effect = lambda name: Period(
        year=2023, month=int(name.rsplit("_", 1)[-1])
    )
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    mock_logger = MagicMock()
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    with patch("pg_partsmith.aio.services.pruning.logger", mock_logger):
        to_prune = await service.get_partitions_for_pruning(config)

    # Assert — only the detached orphan is pruned; the attached one is skipped with a warning
    assert [p.name for p in to_prune] == ["events__2023_02"]
    mock_logger.warning.assert_called_once()
    assert mock_logger.warning.call_args.kwargs["extra"]["partition_name"] == "events__2023_01"


async def test__get_partitions_for_pruning__unparseable_cutoff__attached_name_fallback_still_works(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — a custom calculator with non-datetime boundaries: the cutoff itself is unparseable,
    # so the attached name fallback must keep working (backward compatibility)
    partition = PartitionInfo(
        name="events__old",
        partition_type=PartitionType.RANGE,
        from_value="A",
        to_value="B",
        boundaries_expr="FOR VALUES FROM ('A') TO ('B')",
        is_attached=True,
    )
    mock_metadata.list_partitions.return_value = [partition]
    mock_calculator.parse_partition_name.return_value = Period(year=2023, month=1)
    mock_calculator.period_before.return_value = Period(year=2024, month=2)
    mock_calculator.get_boundaries.return_value = ("A", "B")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    to_prune = await service.get_partitions_for_pruning(config)

    # Assert
    assert [p.name for p in to_prune] == ["events__old"]


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


# ── maintain_lifecycle — continue_on_error ───────────────────────────────────────


def _make_prunable_partitions() -> list[PartitionInfo]:
    """One old attached partition plus one detached orphan, both due for pruning."""
    return [
        PartitionInfo(
            name="events__2024_01",
            partition_type=PartitionType.RANGE,
            from_value="2024-01-01",
            to_value="2024-02-01",
            is_attached=True,
        ),
        PartitionInfo(
            name="events__orphan",
            partition_type=PartitionType.RANGE,
            is_attached=False,
        ),
    ]


async def test__maintain_lifecycle__continue_on_error_create_fails__records_issue_and_still_prunes(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — the create step blows up while old partitions are waiting for pruning
    mock_metadata.list_partitions.return_value = _make_prunable_partitions()
    mock_repo.create_partition.side_effect = SQLAlchemyError("create failed")
    mock_calculator.parse_partition_name.return_value = None
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    result = await service.maintain_lifecycle(config, continue_on_error=True)

    # Assert — the create failure is isolated as an issue; detach and drop still ran
    assert result.success is True
    assert result.created_count == 0
    assert result.detached_count == 1
    assert result.dropped_count == 2
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.CREATE]
    assert "SQLAlchemyError" in result.issues[0].error
    assert "create failed" in result.issues[0].error
    mock_repo.detach_partition.assert_called_once_with("events", "events__2024_01", concurrent=True)
    assert [call.args[0] for call in mock_repo.drop_partition.call_args_list] == ["events__orphan", "events__2024_01"]


async def test__maintain_lifecycle__continue_on_error_detach_fails__still_drops_orphans_only(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — detach blows up; pre-existing orphans must still be dropped
    mock_metadata.list_partitions.return_value = _make_prunable_partitions()
    mock_repo.create_partition.return_value = partition_info
    mock_repo.detach_partition.side_effect = SQLAlchemyError("detach failed")
    mock_calculator.parse_partition_name.return_value = None
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    result = await service.maintain_lifecycle(config, continue_on_error=True)

    # Assert — only the orphan is dropped, never the would-be-detached partition
    assert result.success is True
    assert result.created_count == 1
    assert result.detached_count == 0
    assert result.dropped_count == 1
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.DETACH]
    mock_repo.drop_partition.assert_called_once_with("events__orphan")


async def test__maintain_lifecycle__continue_on_error_drop_fails__returns_counts_with_drop_issue(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — create and detach succeed, the drop step blows up
    mock_metadata.list_partitions.return_value = _make_prunable_partitions()
    mock_repo.create_partition.return_value = partition_info
    mock_repo.drop_partition.side_effect = SQLAlchemyError("drop failed")
    mock_calculator.parse_partition_name.return_value = None
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    result = await service.maintain_lifecycle(config, continue_on_error=True)

    # Assert — earlier counts are preserved and the drop failure is the only issue
    assert result.success is True
    assert result.created_count == 1
    assert result.detached_count == 1
    assert result.dropped_count == 0
    assert [issue.step for issue in result.issues] == [MaintenanceIssueStep.DROP]


async def test__maintain_lifecycle__continue_on_error_false__create_failure_propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — pins the default fail-fast behaviour when the flag is explicitly off
    mock_metadata.list_partitions.return_value = _make_prunable_partitions()
    mock_repo.create_partition.side_effect = SQLAlchemyError("create failed")
    mock_calculator.parse_partition_name.return_value = None
    mock_calculator.get_boundaries.return_value = ("2024-02-01", "2024-03-01")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="create failed"):
        await service.maintain_lifecycle(config, continue_on_error=False)

    mock_repo.detach_partition.assert_not_called()
    mock_repo.drop_partition.assert_not_called()


async def test__maintain_lifecycle__continue_on_error_validation_failure__still_fatal(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — validation failures are never isolated, even with the flag on
    mock_metadata.get_partition_type.return_value = None
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="not partitioned"):
        await service.maintain_lifecycle(config, continue_on_error=True)

    mock_repo.create_partition.assert_not_called()


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


async def test__validation_service__mixed_case_actual_column__raises_invalid_config(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
) -> None:
    # Arrange — a quoted mixed-case column would break the reconcile SQL later; fail fast instead
    config = TablePartitionConfig(
        table_name="events",
        partition_type=PartitionType.RANGE,
        partition_strategy=PartitionStrategy.TIME_BASED,
        partition_column="createdat",
        granularity=PartitionGranularity.MONTH,
    )
    service = PartitionLifecycleService(mock_repo, mock_metadata, mock_locks, mock_calculator)
    mock_metadata.get_partition_type.return_value = PartitionType.RANGE
    mock_metadata.get_partition_column.return_value = "createdAt"

    # Act / Assert
    with pytest.raises(InvalidPartitionConfigError, match="mixed-case"):
        await service._validation_service.validate_config(config)


async def test__validation_service__exact_lowercase_column_match__passes(
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
    mock_metadata.get_partition_column.return_value = "created_at"

    # Act / Assert — must not raise
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
    # Reconciliation ran once, then the final failure restored the moved rows back to DEFAULT
    assert mock_repo.reconcile_default_rows.call_count == 2
    restore_call = mock_repo.reconcile_default_rows.call_args_list[-1]
    assert restore_call.kwargs["default_partition_name"] == "events__2024_04"
    assert restore_call.kwargs["target_partition_name"] == "events_default"


async def test__create_future_partitions__attach_fails_after_reconcile__restores_rows_with_swapped_partitions(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — first attach hits a DEFAULT conflict, reconcile moves rows, second attach fails hard
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = [_make_23514_exc(), SQLAlchemyError("attach failed")]
    mock_repo.reconcile_default_rows.return_value = 5
    mock_metadata.get_default_partition.return_value = PartitionInfo(
        name="events_default", partition_type=PartitionType.RANGE, is_default=True, is_attached=True
    )
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="attach failed"):
        await service.create_future_partitions(config)

    # First call moved rows DEFAULT → target; second call restored them target → DEFAULT
    assert mock_repo.reconcile_default_rows.call_count == 2
    first_call, restore_call = mock_repo.reconcile_default_rows.call_args_list
    assert first_call.kwargs["default_partition_name"] == "events_default"
    assert first_call.kwargs["target_partition_name"] == "events__2024_04"
    assert restore_call.kwargs["default_partition_name"] == "events__2024_04"
    assert restore_call.kwargs["target_partition_name"] == "events_default"


async def test__create_future_partitions__restore_after_failed_attach_fails__original_error_propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — reconcile succeeds, final attach fails, and the best-effort restore also fails
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = [_make_23514_exc(), SQLAlchemyError("attach failed")]
    mock_repo.reconcile_default_rows.side_effect = [5, SQLAlchemyError("restore failed")]
    mock_metadata.get_default_partition.return_value = PartitionInfo(
        name="events_default", partition_type=PartitionType.RANGE, is_default=True, is_attached=True
    )
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert — the restore failure is swallowed; the original attach error propagates
    with pytest.raises(SQLAlchemyError, match="attach failed"):
        await service.create_future_partitions(config)

    assert mock_repo.reconcile_default_rows.call_count == 2


async def test__create_future_partitions__42809_attach_race__treated_as_already_attached(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — another worker attached first: PostgreSQL raises 42809 ("is already a partition")
    # and the post-condition check confirms the partition really is attached to our parent
    config = config.model_copy(update={"auto_attach_after_create": True})
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = _make_sqlstate_exc("42809", "is already a partition")
    mock_metadata.is_partition_attached.return_value = True
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act
    created = await service.create_future_partitions(config)

    # Assert — race is swallowed and the partition is treated as attached
    assert len(created) == 1
    assert created[0].is_attached is True
    mock_metadata.is_partition_attached.assert_called_once_with("events", "events__2024_04")


async def test__create_future_partitions__55006_attach_error__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — 55006 (object_in_use) is no longer treated as an attach-race conflict
    config = config.model_copy(update={"auto_attach_after_create": True})
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = _make_sqlstate_exc("55006", "object is in use")
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert
    with pytest.raises(SQLAlchemyError, match="object is in use"):
        await service.create_future_partitions(config)


async def test__create_future_partitions__conflict_sqlstate_but_not_attached__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — a conflict SQLSTATE alone is not proof of a lost race: 42809 also fires for typed
    # tables or attachments to a different parent; the post-condition check comes back False
    config = config.model_copy(update={"auto_attach_after_create": True})
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = _make_sqlstate_exc("42809", "is already a partition")
    mock_metadata.is_partition_attached.return_value = False
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert — the error must propagate instead of being swallowed
    with pytest.raises(SQLAlchemyError, match="is already a partition"):
        await service.create_future_partitions(config)

    mock_metadata.is_partition_attached.assert_called_once_with("events", "events__2024_04")


async def test__create_future_partitions__existing_detached_attach_conflict_not_attached__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — an existing detached partition is being re-attached; the attach hits a conflict
    # SQLSTATE but the post-condition check says it is NOT attached to our parent
    config = config.model_copy(update={"auto_attach_after_create": True})
    mock_metadata.list_partitions.return_value = [
        PartitionInfo(
            name="events__2024_04",
            partition_type=PartitionType.RANGE,
            is_attached=False,
            parent_table="events",
        )
    ]
    mock_repo.attach_partition.side_effect = _make_sqlstate_exc("42809", "is already a partition")
    mock_metadata.is_partition_attached.return_value = False
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert — the error must propagate instead of being swallowed
    with pytest.raises(SQLAlchemyError, match="is already a partition"):
        await service.create_future_partitions(config)

    mock_metadata.is_partition_attached.assert_called_once_with("events", "events__2024_04")


async def test__create_future_partitions__existing_detached_attach_conflict_verified_attached__swallowed(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — re-attach of an existing detached partition loses the race, but the post-condition
    # check confirms it really is attached to our parent: the conflict is benign
    config = config.model_copy(update={"auto_attach_after_create": True})
    mock_metadata.list_partitions.return_value = [
        PartitionInfo(
            name="events__2024_04",
            partition_type=PartitionType.RANGE,
            is_attached=False,
            parent_table="events",
        )
    ]
    mock_repo.attach_partition.side_effect = _make_sqlstate_exc("42809", "is already a partition")
    mock_metadata.is_partition_attached.return_value = True
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act — must not raise
    created = await service.create_future_partitions(config)

    # Assert
    assert created == []
    mock_metadata.is_partition_attached.assert_called_once_with("events", "events__2024_04")


async def test__create_future_partitions__create_race_attach_conflict_not_attached__propagates(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — another worker won the create race, then our attach hits a conflict SQLSTATE
    # but the post-condition check says the partition is NOT attached to our parent
    config = config.model_copy(update={"auto_attach_after_create": True})
    mock_repo.create_partition.side_effect = PartitionAlreadyExistsError("events__2024_04")
    mock_repo.attach_partition.side_effect = _make_sqlstate_exc("42710", "duplicate object")
    mock_metadata.is_partition_attached.return_value = False
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert — the error must propagate instead of being swallowed
    with pytest.raises(SQLAlchemyError, match="duplicate object"):
        await service.create_future_partitions(config)

    mock_metadata.is_partition_attached.assert_called_once_with("events", "events__2024_04")


async def test__create_future_partitions__create_race_attach_conflict_verified_attached__swallowed(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
) -> None:
    # Arrange — another worker won the create race AND the attach race, and the post-condition
    # check confirms the partition is attached to our parent: the conflict is benign
    config = config.model_copy(update={"auto_attach_after_create": True})
    mock_repo.create_partition.side_effect = PartitionAlreadyExistsError("events__2024_04")
    mock_repo.attach_partition.side_effect = _make_sqlstate_exc("42710", "duplicate object")
    mock_metadata.is_partition_attached.return_value = True
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act — must not raise
    created = await service.create_future_partitions(config)

    # Assert
    assert created == []
    mock_metadata.is_partition_attached.assert_called_once_with("events", "events__2024_04")


async def test__create_future_partitions__cancelled_during_attach_after_reconcile__restores_rows_and_reraises(
    mock_repo: MagicMock,
    mock_metadata: MagicMock,
    mock_locks: MagicMock,
    mock_calculator: MagicMock,
    config: TablePartitionConfig,
    partition_info: PartitionInfo,
) -> None:
    # Arrange — first attach hits a DEFAULT conflict, reconcile moves rows out of DEFAULT, then the
    # retried attach is cancelled: the moved rows must be restored (shielded) before re-raising
    mock_repo.create_partition.return_value = partition_info
    mock_repo.attach_partition.side_effect = [_make_23514_exc(), asyncio.CancelledError()]
    mock_repo.reconcile_default_rows.return_value = 5
    mock_metadata.get_default_partition.return_value = PartitionInfo(
        name="events_default", partition_type=PartitionType.RANGE, is_default=True, is_attached=True
    )
    service = _make_service(mock_repo, mock_metadata, mock_locks, mock_calculator)

    # Act / Assert — the cancellation propagates after the compensating move-back
    with pytest.raises(asyncio.CancelledError):
        await service.create_future_partitions(config)

    assert mock_repo.reconcile_default_rows.call_count == 2
    restore_call = mock_repo.reconcile_default_rows.call_args_list[-1]
    assert restore_call.kwargs["default_partition_name"] == "events__2024_04"
    assert restore_call.kwargs["target_partition_name"] == "events_default"
