"""Fixtures for running the image end to end.

``PG_PARTSMITH_E2E_IMAGE`` names the image under test -- ``docker build -t
pg-partsmith:local .`` -- and without it every test here skips. CI builds the
image and points this suite at it, on both architectures. The database is a
container on a network of its own, reachable from the image by alias, and the
commands the tests run are the ones the guides show.
"""

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING

import docker
import pytest
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer

from tests.e2e.support import DB_ALIAS, DB_NAME, DB_PASSWORD, DB_USER, Image

if TYPE_CHECKING:
    from collections.abc import Iterator

IMAGE_ENV_VAR = "PG_PARTSMITH_E2E_IMAGE"


@pytest.fixture(scope="session")
def docker_client() -> Iterator[docker.DockerClient]:
    try:
        client = docker.from_env()
    except Exception:  # whatever the SDK raises, there is no daemon to talk to
        pytest.skip("Docker is required to run the image")
    with contextlib.closing(client):
        try:
            client.ping()
        except Exception:
            pytest.skip("Docker is required to run the image")
        yield client


@pytest.fixture(scope="session")
def image_name(docker_client: docker.DockerClient) -> str:
    """The image under test. Not set: skip. Set to something that is not here: an error."""
    name = os.environ.get(IMAGE_ENV_VAR)
    if not name:
        pytest.skip(f"set {IMAGE_ENV_VAR} to the image under test (docker build -t pg-partsmith:local .)")
    docker_client.images.get(name)
    return name


@pytest.fixture(scope="session")
def postgres_image() -> str:
    return os.environ.get("PG_PARTSMITH_TEST_PG_IMAGE") or "postgres:17-alpine"


@pytest.fixture(scope="session")
def network(image_name: str) -> Iterator[Network]:
    """A network of the session's own: the image dials the database by alias on it."""
    with Network() as created:
        yield created


@pytest.fixture(scope="session")
def postgres(network: Network, postgres_image: str) -> Iterator[PostgresContainer]:
    container = (
        PostgresContainer(postgres_image, username=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
        .with_network(network)
        .with_network_aliases(DB_ALIAS)
    )
    with container as running:
        yield running


@pytest.fixture(scope="session")
def image(docker_client: docker.DockerClient, image_name: str, network: Network) -> Image:
    return Image(docker_client, image_name, network.name)
