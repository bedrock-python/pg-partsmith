"""What the image is, before it reaches a database."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

import pg_partsmith

if TYPE_CHECKING:
    import docker

    from tests.e2e.support import Image

pytestmark = pytest.mark.e2e

REPOSITORY = "https://github.com/bedrock-python/pg-partsmith"

# The shell step CI used to run, as a test: every extension module the image
# keeps loads, and the base holds what they need at runtime.
CLOSURE = """
import bz2, hashlib, lzma, os, ssl, uuid, zlib, zoneinfo
import asyncpg, greenlet, pydantic_core, sqlalchemy, typer, yaml
assert os.getuid() == 65532, os.getuid()
assert not os.path.exists("/bin/sh"), "a shell got in"
assert ssl.create_default_context().cert_store_stats()["x509_ca"] > 0, "no CA certificates"
zoneinfo.ZoneInfo("Pacific/Kiritimati")
print("closure ok")
"""


def test__the_image__is_the_command_as_a_fixed_non_root_user(
    docker_client: docker.DockerClient, image_name: str
) -> None:
    config = docker_client.images.get(image_name).attrs["Config"]

    # A number, not a name: runAsNonRoot is checked by number, and a name fails it.
    assert config["User"] == "65532:65532"
    # Exec form: the command is PID 1 and gets the signals; a shell in between would keep them.
    assert config["Entrypoint"] == ["/opt/venv/bin/python", "-m", "pg_partsmith.cli"]
    assert config["Cmd"] == ["--help"]
    assert config["Labels"]["org.opencontainers.image.source"] == REPOSITORY
    assert config["Labels"]["org.opencontainers.image.version"]


def test__version__is_the_library_s_own_number(image: Image) -> None:
    outcome = image.run("--version")

    assert outcome.code == 0, outcome.stderr
    assert outcome.stdout.strip() == f"pg-partsmith {pg_partsmith.__version__}"


def test__schema__is_json_on_stdout_and_nothing_on_stderr(image: Image) -> None:
    outcome = image.run("schema")

    assert outcome.code == 0, outcome.stderr
    assert outcome.stderr == ""
    assert "properties" in json.loads(outcome.stdout)


def test__a_usage_error__is_64_and_not_the_2_that_means_drift(image: Image) -> None:
    outcome = image.run("plan", "--no-such-flag")

    assert outcome.code == 64
    assert "--no-such-flag" in outcome.stderr


def test__the_runtime__has_the_closure_and_neither_a_shell_nor_root(image: Image) -> None:
    outcome = image.run("-c", CLOSURE, entrypoint="/opt/venv/bin/python")

    assert outcome.code == 0, outcome.stderr
    assert outcome.stdout.strip() == "closure ok"
