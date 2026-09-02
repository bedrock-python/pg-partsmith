"""Running the image the way the guides say to, and reading back what it did."""

from __future__ import annotations

import json
import shlex
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from pathlib import Path

    import docker
    from docker.models.containers import Container
    from testcontainers.postgres import PostgresContainer

DB_USER = "partsmith"
DB_PASSWORD = "partsmith"
DB_NAME = "partsmith"
DB_ALIAS = "db"
# What a container on the same network dials: the alias, not a mapped port.
INTERNAL_DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_ALIAS}:5432/{DB_NAME}"
DOCUMENT_PATH = "/etc/partitions.json"

# A hook that holds the run -- and the table's lock -- for as long as a test
# needs it held. The image has no `sleep`; it has the interpreter.
SLEEPING_HOOK = ["/opt/venv/bin/python", "-c", "import time; time.sleep(120)"]


@dataclass(frozen=True)
class Outcome:
    """What one run of the image came back with, the two streams kept apart."""

    code: int
    stdout: str
    stderr: str


class Image:
    """The image under test, run as the container guide says to run it.

    Every run gets a read-only root filesystem, no capabilities and no
    privilege escalation -- the ``securityContext`` the Kubernetes page shows
    -- and the document is mounted read-only. A test that needs the root
    filesystem writable says so.
    """

    def __init__(self, client: docker.DockerClient, name: str, network: str) -> None:
        self.client = client
        self.name = name
        self.network = network

    def start(
        self,
        *args: str,
        env: Mapping[str, str] | None = None,
        mounts: Mapping[str, str] | None = None,
        read_only: bool = True,
        entrypoint: str | None = None,
        image: str | None = None,
    ) -> Container:
        """Start a container and hand it back running; ``finish`` collects it.

        ``mounts`` maps a host path or a volume name to ``"/target"`` or
        ``"/target:ro"``.
        """
        volumes: dict[str, dict[str, str]] = {}
        for source, target in (mounts or {}).items():
            bind, _, mode = target.partition(":")
            volumes[str(source)] = {"bind": bind, "mode": mode or "rw"}
        return self.client.containers.run(
            image or self.name,
            command=list(args),
            entrypoint=entrypoint,
            environment=dict(env or {}),
            network=self.network,
            volumes=volumes,
            read_only=read_only,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
            detach=True,
        )

    def run(
        self,
        *args: str,
        env: Mapping[str, str] | None = None,
        mounts: Mapping[str, str] | None = None,
        read_only: bool = True,
        entrypoint: str | None = None,
        image: str | None = None,
        timeout: float = 120,
    ) -> Outcome:
        """Run to completion, and remove the container whatever happened."""
        container = self.start(*args, env=env, mounts=mounts, read_only=read_only, entrypoint=entrypoint, image=image)
        try:
            return finish(container, timeout=timeout)
        finally:
            container.remove(force=True)


def finish(container: Container, *, timeout: float = 120) -> Outcome:
    """Wait for the container to exit, then read its exit code and its two streams."""
    try:
        status = container.wait(timeout=timeout)
    except Exception:  # the SDK raises its transport's timeout; any of them means "still running"
        container.kill()
        pytest.fail(f"the container was still running after {timeout}s")
    return Outcome(
        code=int(status["StatusCode"]),
        stdout=container.logs(stdout=True, stderr=False).decode("utf-8", errors="replace"),
        stderr=container.logs(stdout=False, stderr=True).decode("utf-8", errors="replace"),
    )


def sql(postgres: PostgresContainer, statement: str) -> str:
    """One statement through ``psql`` inside the database container; the rows, one per line."""
    code, output = postgres.exec(["psql", "-U", DB_USER, "-d", DB_NAME, "-v", "ON_ERROR_STOP=1", "-Atc", statement])
    text = output.decode("utf-8", errors="replace")
    assert code == 0, text
    return text.strip()


def partitioned_table(postgres: PostgresContainer) -> str:
    """A fresh range-partitioned table with no partitions, under a name of its own."""
    name = f"e2e_{uuid.uuid4().hex[:8]}"
    sql(
        postgres,
        f'CREATE TABLE "{name}" (id bigserial, created_at timestamptz NOT NULL) PARTITION BY RANGE (created_at)',
    )
    return name


def expired_partition(postgres: PostgresContainer, table: str) -> str:
    """A partition long past any retention, attached by hand the way a legacy one would be."""
    name = f"{table}__2020_01"
    sql(
        postgres,
        f'CREATE TABLE "{name}" PARTITION OF "{table}" '
        "FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-02-01 00:00:00+00')",
    )
    return name


def holds_advisory_lock(postgres: PostgresContainer) -> bool:
    """Whether some session holds the advisory lock a maintenance run takes."""
    return sql(postgres, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'") != "0"


def wait_until(condition: Callable[[], bool], *, what: str, timeout: float = 30.0) -> None:
    """Poll until ``condition`` holds, or fail the test naming what did not happen."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.2)
    pytest.fail(f"{what} did not happen within {timeout:.0f}s")


def write_document(
    path: Path,
    *,
    table: str,
    retention: int = 12,
    dsn: str | None = None,
    hooks: Mapping[str, Any] | None = None,
) -> Path:
    """One table, monthly, two partitions ahead: the document the guides start from, as JSON."""
    payload: dict[str, Any] = {
        "tables": [
            {
                "table_name": table,
                "partition_column": "created_at",
                "granularity": "month",
                "create_ahead_count": 2,
                "retention_count": retention,
            }
        ]
    }
    if dsn is not None:
        payload["dsn"] = dsn
    if hooks is not None:
        payload["hooks"] = dict(hooks)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def writable_dir(path: Path) -> Path:
    """A directory UID 65532 can write when it is bind-mounted from a Linux host."""
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o777)  # a test's scratch directory, shared with a container running as another user
    return path


@contextmanager
def root_owned_volume(
    client: docker.DockerClient, helper_image: str, *, secrets: Mapping[str, str] | None = None
) -> Iterator[str]:
    """A named volume whose contents belong to root, as a hostPath or a secret mount would.

    Filled through an image that has a shell, since the image under test has
    none; each secret is a file only root can read, the way a ``defaultMode``
    of ``0400`` arrives.
    """
    volume = client.volumes.create(f"pg-partsmith-e2e-{uuid.uuid4().hex[:8]}")
    try:
        if secrets:
            script = " && ".join(
                f"printf %s {shlex.quote(text)} > /v/{name} && chmod 600 /v/{name}" for name, text in secrets.items()
            )
            client.containers.run(
                helper_image,
                entrypoint="sh",
                command=["-c", script],
                volumes={volume.name: {"bind": "/v", "mode": "rw"}},
                remove=True,
            )
        yield volume.name
    finally:
        volume.remove(force=True)
