"""The image against a database: the commands the guides show, with the codes they promise."""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from typing import TYPE_CHECKING

import pytest

from tests.e2e.support import (
    DB_PASSWORD,
    DOCUMENT_PATH,
    INTERNAL_DSN,
    SLEEPING_HOOK,
    expired_partition,
    finish,
    holds_advisory_lock,
    partitioned_table,
    root_owned_volume,
    wait_until,
    writable_dir,
    write_document,
)

if TYPE_CHECKING:
    from pathlib import Path

    import docker
    from testcontainers.postgres import PostgresContainer

    from tests.e2e.support import Image

pytestmark = pytest.mark.e2e

ENV = {"PG_PARTSMITH_DSN": INTERNAL_DSN}


@pytest.fixture
def table(postgres: PostgresContainer) -> str:
    return partitioned_table(postgres)


@pytest.fixture
def document(tmp_path: Path, table: str) -> Path:
    return write_document(tmp_path / "partitions.json", table=table)


def _mounted(document: Path) -> dict[str, str]:
    """The document where the guides mount it, read-only."""
    return {str(document): f"{DOCUMENT_PATH}:ro"}


# ── The ordinary run ──────────────────────────────────────────────────────────


def test__a_fresh_table__drifts_and_apply_converges_it(image: Image, document: Path, table: str) -> None:
    drift = image.run("plan", "-c", DOCUMENT_PATH, "--check", env=ENV, mounts=_mounted(document))
    assert drift.code == 2, drift.stderr
    assert f"CREATE public.{table}__" in drift.stdout

    applied = image.run("apply", "-c", DOCUMENT_PATH, env=ENV, mounts=_mounted(document))
    assert applied.code == 0, applied.stderr
    assert "created 2" in applied.stdout
    # Human output carries an em dash; PID 1's stdout is a pipe, and it survives.
    assert "—" in applied.stdout

    converged = image.run("plan", "-c", DOCUMENT_PATH, "--check", env=ENV, mounts=_mounted(document))
    assert converged.code == 0, converged.stderr
    assert "nothing to do" in converged.stdout


def test__output_json__is_the_envelope_alone_on_stdout(image: Image, document: Path) -> None:
    outcome = image.run("plan", "-c", DOCUMENT_PATH, "--output", "json", env=ENV, mounts=_mounted(document))

    assert outcome.code == 0, outcome.stderr
    assert outcome.stderr == ""
    envelope = json.loads(outcome.stdout)
    assert envelope["command"] == "plan"
    assert envelope["tables"][0]["plan"]["config_fingerprint"]


def test__write__lands_the_textfile_in_a_volume_and_keeps_stdout_empty(
    image: Image, document: Path, tmp_path: Path
) -> None:
    textfile = writable_dir(tmp_path / "textfile")

    outcome = image.run(
        "plan", "-c", DOCUMENT_PATH, "--check", "--output", "metrics", "--write", "/textfile/partsmith.prom",
        env=ENV,
        mounts={**_mounted(document), str(textfile): "/textfile"},
    )  # fmt: skip

    # Drift is still the answer; the file is only where the output went.
    assert outcome.code == 2, outcome.stderr
    assert outcome.stdout == ""
    written = (textfile / "partsmith.prom").read_text(encoding="utf-8")
    assert written.startswith("# HELP pg_partsmith_run_timestamp_seconds")
    assert "pg_partsmith_pending_operations" in written
    # Written beside the target and renamed: nothing half-done is left for a collector to find.
    assert [p.name for p in textfile.iterdir()] == ["partsmith.prom"]


def test__write__a_directory_the_user_cannot_write__is_exit_4_with_the_path(
    image: Image, document: Path, docker_client: docker.DockerClient, postgres_image: str
) -> None:
    # A hostPath directory on a node is root's, and this container is not root.
    with root_owned_volume(docker_client, postgres_image) as volume:
        outcome = image.run(
            "plan", "-c", DOCUMENT_PATH, "--output", "metrics", "--write", "/textfile/partsmith.prom",
            env=ENV,
            mounts={**_mounted(document), volume: "/textfile"},
        )  # fmt: skip

    assert outcome.code == 4
    assert outcome.stderr.startswith("pg-partsmith: Cannot write /textfile/partsmith.prom")
    assert "Traceback" not in outcome.stderr


def test__save__a_directory_the_user_cannot_write__is_exit_4_and_not_a_database_error(
    image: Image, document: Path, docker_client: docker.DockerClient, postgres_image: str
) -> None:
    with root_owned_volume(docker_client, postgres_image) as volume:
        outcome = image.run(
            "plan", "-c", DOCUMENT_PATH, "--save", "/work/plan.json",
            env=ENV,
            mounts={**_mounted(document), volume: "/work"},
        )  # fmt: skip

    assert outcome.code == 4
    assert outcome.stderr.startswith("pg-partsmith: Cannot write the plan to /work/plan.json")


# ── Where the connection string comes from ────────────────────────────────────


def test__dsn_file__reads_a_mounted_secret(image: Image, document: Path, tmp_path: Path, table: str) -> None:
    secret = tmp_path / "pg_dsn"
    secret.write_text(INTERNAL_DSN, encoding="utf-8")

    outcome = image.run(
        "validate", "-c", DOCUMENT_PATH,
        env={"PG_PARTSMITH_DSN_FILE": "/run/secrets/pg_dsn"},
        mounts={**_mounted(document), str(secret): "/run/secrets/pg_dsn:ro"},
    )  # fmt: skip

    assert outcome.code == 0, outcome.stderr
    assert f"{table} — ok" in outcome.stdout


def test__dsn_file__a_secret_only_root_can_read__is_exit_4_and_a_sentence(
    image: Image, document: Path, docker_client: docker.DockerClient, postgres_image: str
) -> None:
    # A Secret mounted with defaultMode 0400 and no fsGroup arrives exactly like this.
    with root_owned_volume(docker_client, postgres_image, secrets={"pg_dsn": INTERNAL_DSN}) as volume:
        outcome = image.run(
            "validate", "-c", DOCUMENT_PATH,
            env={"PG_PARTSMITH_DSN_FILE": "/run/secrets/pg_dsn"},
            mounts={**_mounted(document), volume: "/run/secrets:ro"},
        )  # fmt: skip

    assert outcome.code == 4
    assert outcome.stderr.startswith("pg-partsmith: Cannot read the DSN from /run/secrets/pg_dsn")


def test__a_password_the_server_rejects__is_exit_5_and_not_a_traceback(image: Image, document: Path) -> None:
    rejected = {"PG_PARTSMITH_DSN": INTERNAL_DSN.replace(f":{DB_PASSWORD}@", ":wrong@")}

    outcome = image.run("validate", "-c", DOCUMENT_PATH, env=rejected, mounts=_mounted(document))

    assert outcome.code == 5
    assert "password authentication failed" in outcome.stderr
    assert "Traceback" not in outcome.stderr


# ── Hooks, stops and overlapping runs ─────────────────────────────────────────


def test__hooks_declared__without_allow_hooks__is_refused_before_connecting(
    image: Image, tmp_path: Path, table: str
) -> None:
    document = write_document(tmp_path / "partitions.json", table=table, hooks={"before_create": SLEEPING_HOOK})
    nowhere = {"PG_PARTSMITH_DSN": "postgresql://nobody:nobody@nowhere.invalid/none"}

    outcome = image.run("apply", "-c", DOCUMENT_PATH, env=nowhere, mounts=_mounted(document))

    assert outcome.code == 4
    assert "--allow-hooks" in outcome.stderr


def test__sigterm__cancels_the_run_releases_the_lock_and_exits_143(
    image: Image, postgres: PostgresContainer, tmp_path: Path, table: str
) -> None:
    # A command hook that holds the run: what an archiver mid-export looks like
    # when a pod's deadline arrives.
    document = write_document(tmp_path / "partitions.json", table=table, hooks={"before_create": SLEEPING_HOOK})
    container = image.start("apply", "-c", DOCUMENT_PATH, "--allow-hooks", env=ENV, mounts=_mounted(document))
    try:
        wait_until(lambda: holds_advisory_lock(postgres), what="the run taking its lock")
        started = time.monotonic()
        container.stop(timeout=20)  # what `docker stop` and a kubelet do: SIGTERM, then SIGKILL after the grace
        elapsed = time.monotonic() - started
        outcome = finish(container)
    finally:
        container.remove(force=True)

    assert outcome.code == 143, outcome.stderr
    assert outcome.stderr.rstrip().endswith("pg-partsmith: terminated")
    assert elapsed < 15, f"stopping took {elapsed:.1f}s: the grace period was used up"
    # The lock went with the run, and nothing was created under a hook that never finished.
    after = image.run("plan", "-c", DOCUMENT_PATH, "--check", env=ENV, mounts=_mounted(document))
    assert after.code == 2, after.stderr


def test__sigterm__a_python_block_still_running__does_not_hold_the_stop(
    image: Image, postgres: PostgresContainer, tmp_path: Path, table: str
) -> None:
    # A block that sleeps runs on a thread of its own, so the stop does not wait for it.
    block = {"python": "import time\ntime.sleep(120)\n"}
    document = write_document(tmp_path / "partitions.json", table=table, hooks={"before_create": block})
    container = image.start("apply", "-c", DOCUMENT_PATH, "--allow-hooks", env=ENV, mounts=_mounted(document))
    try:
        wait_until(lambda: holds_advisory_lock(postgres), what="the run taking its lock")
        started = time.monotonic()
        container.stop(timeout=20)
        elapsed = time.monotonic() - started
        outcome = finish(container)
    finally:
        container.remove(force=True)

    assert outcome.code == 143, outcome.stderr
    assert outcome.stderr.rstrip().endswith("pg-partsmith: terminated")
    assert elapsed < 15, f"stopping took {elapsed:.1f}s: the block held the stop"


def test__an_overlapping_run__stands_aside_with_6_or_with_0_when_told_to(
    image: Image, postgres: PostgresContainer, tmp_path: Path, table: str
) -> None:
    document = write_document(tmp_path / "partitions.json", table=table, hooks={"before_create": SLEEPING_HOOK})
    holder = image.start("apply", "-c", DOCUMENT_PATH, "--allow-hooks", env=ENV, mounts=_mounted(document))
    try:
        wait_until(lambda: holds_advisory_lock(postgres), what="the first run taking its lock")
        second = image.run("apply", "-c", DOCUMENT_PATH, "--allow-hooks", env=ENV, mounts=_mounted(document))
        # An init container restarts on anything non-zero, and a held lock is not a failure there.
        init = image.run(
            "apply", "-c", DOCUMENT_PATH, "--allow-hooks", "--ok-if-locked", env=ENV, mounts=_mounted(document)
        )
    finally:
        holder.stop(timeout=20)
        holder.remove(force=True)

    assert second.code == 6, second.stderr
    assert "lock" in second.stderr
    assert init.code == 0, init.stderr


# ── The artifact between plan and apply, and a hook inside the image ──────────


def test__plan_save_then_apply_plan__is_the_artifact_between_two_containers(
    image: Image, document: Path, tmp_path: Path, table: str
) -> None:
    work = writable_dir(tmp_path / "work")
    mounts = {**_mounted(document), str(work): "/work"}

    planned = image.run("plan", "-c", DOCUMENT_PATH, "--save", "/work/plan.json", "--locks", env=ENV, mounts=mounts)
    assert planned.code == 0, planned.stderr
    saved = json.loads((work / "plan.json").read_text(encoding="utf-8"))
    assert saved["tables"][0]["plan"]["config_fingerprint"]

    applied = image.run("apply", "-c", DOCUMENT_PATH, "--plan", "/work/plan.json", env=ENV, mounts=mounts)
    assert applied.code == 0, applied.stderr
    assert "created 2" in applied.stdout

    # The ConfigMap edited between the two: the plan no longer answers for it.
    edited = write_document(tmp_path / "edited" / "partitions.json", table=table, retention=3)
    refused = image.run(
        "apply", "-c", DOCUMENT_PATH, "--plan", "/work/plan.json",
        env=ENV,
        mounts={**_mounted(edited), str(work): "/work"},
    )  # fmt: skip
    assert refused.code == 4, refused.stderr
    assert "Traceback" not in refused.stderr


def test__a_derived_image__runs_a_python_file_hook_from_inside(
    image: Image, docker_client: docker.DockerClient, postgres: PostgresContainer, tmp_path: Path, table: str
) -> None:
    # The guide's recipe for a hook in a cluster: one FROM, one COPY.
    context = tmp_path / "derived"
    context.mkdir()
    (context / "archive.py").write_text(
        "import pathlib, sys\npathlib.Path('/work/archived.json').write_text(sys.stdin.read(), encoding='utf-8')\n",
        encoding="utf-8",
    )
    (context / "Dockerfile").write_text(
        f"FROM {image.name}\nCOPY --chmod=755 archive.py /opt/hooks/archive.py\n", encoding="utf-8"
    )
    derived = f"pg-partsmith-e2e-derived:{uuid.uuid4().hex[:8]}"
    build = subprocess.run(  # noqa: S603 - our own arguments, no shell
        ["docker", "build", "--quiet", "-t", derived, str(context)], capture_output=True, text=True, check=False
    )
    assert build.returncode == 0, build.stderr

    try:
        # A table maintained once, then a partition long past any retention.
        plain = write_document(tmp_path / "plain.json", table=table)
        assert image.run("apply", "-c", DOCUMENT_PATH, env=ENV, mounts=_mounted(plain), image=derived).code == 0
        expired = expired_partition(postgres, table)
        hooked = write_document(
            tmp_path / "hooked.json",
            table=table,
            retention=1,
            hooks={"before_drop": ["/opt/venv/bin/python", "/opt/hooks/archive.py"]},
        )
        work = writable_dir(tmp_path / "work")

        outcome = image.run(
            "apply", "-c", DOCUMENT_PATH, "--allow-destructive", "--allow-hooks",
            env=ENV,
            mounts={**_mounted(hooked), str(work): "/work"},
            image=derived,
        )  # fmt: skip
    finally:
        docker_client.images.remove(derived, force=True)

    # It ran, inside the image, and was told which partition and why.
    assert outcome.code == 0, outcome.stderr
    event = json.loads((work / "archived.json").read_text(encoding="utf-8"))
    assert event["phase"] == "before_drop"
    assert event["partition"]["name"].endswith(expired)
    assert event["operation"]["reason"] in {"grace_elapsed", "follows_detach"}
