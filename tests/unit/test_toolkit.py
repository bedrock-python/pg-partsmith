"""``PartitionToolkit``: one engine in, a set of collaborators that agree out."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from pg_partsmith.aio import (
    BasePartitionLifecycleHooks,
    PartitionLifecycleService,
    PartitionMaintainer,
    PartitionToolkit,
    PostgresAdvisoryLockManager,
    PostgresMetadataProvider,
    PostgresPartitionRepository,
)


@pytest.fixture
def engine() -> MagicMock:
    return MagicMock()


def test__from_engine__builds_every_part_around_the_one_engine(engine: MagicMock) -> None:
    # Arrange / Act
    kit = PartitionToolkit.from_engine(engine)

    # Assert
    assert isinstance(kit.repo, PostgresPartitionRepository)
    assert isinstance(kit.metadata, PostgresMetadataProvider)
    assert isinstance(kit.locks, PostgresAdvisoryLockManager)
    assert isinstance(kit.service, PartitionLifecycleService)
    assert isinstance(kit.maintainer, PartitionMaintainer)


def test__from_engine__marker_prefix__reaches_the_writer_and_the_reader(engine: MagicMock) -> None:
    # Arrange / Act: the prefix given to one of the two only is the failure this
    # exists to prevent -- partitions detached under it are invisible to the
    # other, so they are never dropped.
    kit = PartitionToolkit.from_engine(engine, marker_prefix="acme")

    # Assert
    assert kit.repo.marker_prefix == kit.metadata.marker_prefix
    assert kit.repo.marker_prefix != PartitionToolkit.from_engine(engine).repo.marker_prefix


def test__from_engine__ddl_timezone__is_given_to_both_sides_of_the_boundary(engine: MagicMock) -> None:
    # Arrange / Act
    kit = PartitionToolkit.from_engine(engine, ddl_timezone="Europe/Berlin")

    # Assert: one writes naive bounds in it, the other reads them back in it
    assert kit.repo.ddl_timezone == "Europe/Berlin"
    assert kit.metadata._ddl_timezone == "Europe/Berlin"


def test__from_engine__nothing_passed__leaves_no_disagreement_to_default_into(engine: MagicMock) -> None:
    # The two constructors default differently -- "UTC" on the repository, None
    # on the provider -- so a caller who passed neither used to get a reader on
    # a timezone the writer never used.
    kit = PartitionToolkit.from_engine(engine)

    assert kit.repo.ddl_timezone == "UTC"
    assert kit.metadata._ddl_timezone == "UTC"


def test__from_engine__a_lock_manager_of_your_own__is_used_as_given(engine: MagicMock) -> None:
    # Arrange
    locks = MagicMock()

    # Act
    kit = PartitionToolkit.from_engine(engine, locks=locks)

    # Assert
    assert kit.locks is locks
    assert kit.service._locks is locks


def test__from_engine__the_service_holds_the_very_parts_that_are_returned(engine: MagicMock) -> None:
    # Arrange / Act: the point of returning parts rather than a maintainer is
    # that calling metadata or locks directly needs no second wiring.
    kit = PartitionToolkit.from_engine(engine)

    # Assert
    assert kit.service._repo is kit.repo
    assert kit.service._metadata is kit.metadata
    assert kit.maintainer._service is kit.service


def test__from_engine__hooks__reach_the_executor_that_fires_them(engine: MagicMock) -> None:
    # Arrange
    hooks = BasePartitionLifecycleHooks()

    # Act
    kit = PartitionToolkit.from_engine(engine, hooks=[hooks])

    # Assert
    assert kit.service._executor._hooks == [hooks]


def test__toolkit__is_frozen(engine: MagicMock) -> None:
    # Arrange
    kit = PartitionToolkit.from_engine(engine)

    # Act / Assert
    with pytest.raises(FrozenInstanceError):
        kit.repo = MagicMock()  # type: ignore[misc]
