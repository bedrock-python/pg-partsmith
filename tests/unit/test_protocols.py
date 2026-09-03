"""What a protocol promises is what the Postgres implementation provides, in both mirrors.

A toolkit hands its parts back under the protocols' types, so a method the
implementation has and the protocol lacks is one a caller cannot reach
without narrowing first -- which is what happened to ``is_partition_closed``.
Both directions are held here: every protocol method exists on the
implementation with the same signature, and every public method of the
implementation is on the protocol unless it is listed as the implementation's
own.
"""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest

from pg_partsmith.aio import metadata as aio_metadata
from pg_partsmith.aio import protocols as aio_protocols
from pg_partsmith.aio import repositories as aio_repositories
from pg_partsmith.sync import metadata as sync_metadata
from pg_partsmith.sync import protocols as sync_protocols
from pg_partsmith.sync import repositories as sync_repositories

# Reads only the Postgres provider offers; a caller that wants them narrows to it on purpose.
POSTGRES_ONLY = frozenset({"evaluate_sql_predicate", "get_partition_boundaries", "get_partition_column"})

PAIRS = [
    pytest.param(
        aio_protocols.PartitionMetadataProvider,
        aio_metadata.PostgresMetadataProvider,
        POSTGRES_ONLY,
        id="aio-metadata",
    ),
    pytest.param(
        sync_protocols.PartitionMetadataProvider,
        sync_metadata.PostgresMetadataProvider,
        POSTGRES_ONLY,
        id="sync-metadata",
    ),
    pytest.param(
        aio_protocols.PartitionRepository,
        aio_repositories.PostgresPartitionRepository,
        frozenset(),
        id="aio-repository",
    ),
    pytest.param(
        sync_protocols.PartitionRepository,
        sync_repositories.PostgresPartitionRepository,
        frozenset(),
        id="sync-repository",
    ),
]

METADATA_PROTOCOLS = [
    pytest.param(aio_protocols.PartitionMetadataProvider, id="aio"),
    pytest.param(sync_protocols.PartitionMetadataProvider, id="sync"),
]


def _methods(cls: type[Any]) -> set[str]:
    """The public functions declared on the class itself; a property is not a protocol member here."""
    return {name for name, member in vars(cls).items() if inspect.isfunction(member) and not name.startswith("_")}


@pytest.mark.parametrize(("protocol", "implementation", "implementation_only"), PAIRS)
def test__protocol__every_method__is_implemented_with_the_same_signature(
    protocol: type[Any], implementation: type[Any], implementation_only: frozenset[str]
) -> None:
    # Arrange
    names = sorted(_methods(protocol))

    # Act -- both files start with ``from __future__ import annotations``, so the signatures compare
    # as written: the sync mirror is generated textually from aio, and a spelling that drifts is a
    # mirror that drifted.
    drift = {
        name: (str(inspect.signature(getattr(protocol, name))), str(inspect.signature(getattr(implementation, name))))
        for name in names
        if inspect.signature(getattr(protocol, name)) != inspect.signature(getattr(implementation, name))
    }

    # Assert
    assert names, "an empty protocol promises nothing"
    assert drift == {}
    assert isinstance(implementation(MagicMock()), protocol)
    assert not implementation_only & set(names), "on the protocol and the implementation's own at once"


@pytest.mark.parametrize(("protocol", "implementation", "implementation_only"), PAIRS)
def test__implementation__every_public_method__is_on_the_protocol_or_listed_as_its_own(
    protocol: type[Any], implementation: type[Any], implementation_only: frozenset[str]
) -> None:
    # Arrange / Act
    unreachable = _methods(implementation) - _methods(protocol) - implementation_only

    # Assert
    assert unreachable == set(), "reachable only after narrowing to the implementation"
    assert implementation_only <= _methods(implementation), "the list names a method that is gone"


@pytest.mark.parametrize("protocol", METADATA_PROTOCOLS)
def test__metadata_protocol__the_closed_check__is_askable_through_it(protocol: type[Any]) -> None:
    # Arrange / Act
    member = vars(protocol).get("is_partition_closed")

    # Assert
    assert member is not None, "a toolkit's provider is typed as the protocol; the export question must be on it"
    signature = inspect.signature(member)
    assert list(signature.parameters) == ["self", "partition_name", "settle_seconds", "boundaries"]
    assert signature.parameters["settle_seconds"].default == 0
    assert signature.parameters["boundaries"].default is None
