"""The Compose shape from the guide -- partitions before the application -- run as written."""

from __future__ import annotations

import shutil
import subprocess
import uuid
from typing import TYPE_CHECKING

import pytest

from tests.e2e.support import DB_ALIAS, DB_NAME, DB_PASSWORD, DB_USER, DOCUMENT_PATH, write_document

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.e2e

# The guide's "Before the application starts", with the securityContext the
# Kubernetes page gives the same container, and the image standing in for the
# application: `--version` is the shortest program that proves it was started.
COMPOSE = """\
services:
  db:
    image: @POSTGRES_IMAGE@
    environment: { POSTGRES_USER: @USER@, POSTGRES_PASSWORD: @PASSWORD@, POSTGRES_DB: @DB@ }
    volumes: ["./init.sql:/docker-entrypoint-initdb.d/init.sql:ro"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -h localhost -U @USER@ -d @DB@"]
      interval: 1s
      timeout: 3s
      retries: 30

  partitions:
    image: @IMAGE@
    command: ["apply", "-c", "@DOCUMENT@"]      # creations only
    environment: { PG_PARTSMITH_DSN_FILE: /run/secrets/pg_dsn }
    secrets: [pg_dsn]
    volumes: ["./partitions.json:@DOCUMENT@:ro"]
    read_only: true
    cap_drop: [ALL]
    security_opt: ["no-new-privileges:true"]
    depends_on:
      db: { condition: service_healthy }

  app:
    image: @IMAGE@
    command: ["--version"]
    depends_on:
      partitions: { condition: service_completed_successfully }

secrets:
  pg_dsn:
    file: ./pg_dsn
"""


@pytest.fixture(scope="module")
def compose() -> list[str]:
    """The Compose plugin, or a skip: the suite is about the image, not about Compose being installed."""
    command = ["docker", "compose"]
    if shutil.which("docker") is None or _run([*command, "version"]).returncode != 0:
        pytest.skip("docker compose is not available here")
    return command


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False)  # noqa: S603 - our own arguments, no shell


def test__before_the_application_starts__the_app_runs_only_once_partitions_exist(
    image_name: str, postgres_image: str, compose: list[str], tmp_path: Path
) -> None:
    (tmp_path / "init.sql").write_text(
        "CREATE TABLE events (id bigserial, created_at timestamptz NOT NULL) PARTITION BY RANGE (created_at);\n",
        encoding="utf-8",
    )
    write_document(tmp_path / "partitions.json", table="events")
    dsn = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_ALIAS}:5432/{DB_NAME}"
    (tmp_path / "pg_dsn").write_text(dsn, encoding="utf-8")
    text = COMPOSE
    for placeholder, value in {
        "@POSTGRES_IMAGE@": postgres_image,
        "@IMAGE@": image_name,
        "@DOCUMENT@": DOCUMENT_PATH,
        "@USER@": DB_USER,
        "@PASSWORD@": DB_PASSWORD,
        "@DB@": DB_NAME,
    }.items():
        text = text.replace(placeholder, value)
    (tmp_path / "compose.yaml").write_text(text, encoding="utf-8")
    project = [*compose, "-p", f"pg-partsmith-e2e-{uuid.uuid4().hex[:8]}", "-f", str(tmp_path / "compose.yaml")]

    try:
        # `run app` starts what app depends on first: the database until healthy,
        # then partitions until it has exited 0. Only then does app run.
        app = _run([*project, "run", "--rm", "app"])
        assert app.returncode == 0, app.stdout + app.stderr
        assert app.stdout.startswith("pg-partsmith ")

        # "A service you run, not one that runs": the same file, one command by hand.
        check = _run([*project, "run", "--rm", "partitions", "plan", "-c", DOCUMENT_PATH, "--check"])
        assert check.returncode == 0, check.stdout + check.stderr
        assert "nothing to do" in check.stdout
    finally:
        _run([*project, "down", "--volumes", "--remove-orphans", "--timeout", "5"])
