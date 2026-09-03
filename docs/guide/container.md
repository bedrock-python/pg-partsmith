# The container image

```
ghcr.io/bedrock-python/pg-partsmith:latest
```

The image is [the CLI](cli.md) and nothing else: the entrypoint is `pg-partsmith`, so a
Compose service, a Swarm task or a Kubernetes Job names only what it wants done. Every
way of running it — plain Docker, Compose, Swarm, a Pod, a Job, a CronJob, an init
container, CI — is on [Ways to run it](running.md); this page is about the image itself.

```bash
docker run --rm \
  -v "$PWD/partitions.yaml:/etc/partitions.yaml:ro" \
  -e PG_PARTSMITH_DSN=postgresql://app:secret@db.internal/app \
  ghcr.io/bedrock-python/pg-partsmith:latest \
  plan -c /etc/partitions.yaml --check
```

## What is inside

Two stages, and only the second one ships.

The runtime base is [distroless](https://github.com/GoogleContainerTools/distroless)
`cc-debian12`: glibc, OpenSSL, CA certificates, a timezone database, and nothing else.
**No shell, no package manager, no pip.** Onto that go a Python 3.14 interpreter with a
pruned standard library (no IDE, no Tk, no test suite, no REPL modules), one virtualenv
installed from `uv.lock` — the versions the test suite ran against, never whatever the
index served on build day — and exactly the five shared libraries the extension modules
still link against. Compiled extensions are stripped of debug symbols; bytecode is
precompiled, so a start skips it. typer's `rich` is left out: `--help` is plain text,
which is what a CronJob log shows anyway.

It runs as UID 65532, distroless' `nonroot` — a fixed high UID so a `runAsUser` can
name the same one and a mounted document can be made readable to it without guessing. It
writes nothing at runtime, so `readOnlyRootFilesystem: true` works; give `plan --save` and
`--write` a volume that UID can write. A hostPath directory is root's, and one the
container cannot write is exit `4` with the path in the message.

Because there is no shell, the two things a wrapper used to do are flags:
`--write FILE` puts any command's output in a file atomically (a node_exporter
textfile), and `apply --ok-if-locked` makes a held lock exit `0` (an init container). A
command hook inside this image is a binary, or a Python file run as
`["/opt/venv/bin/python", "/opt/hooks/export.py"]` — not a shell script. A team that
needs bash in the image builds one line of its own: `FROM python:3.14-slim` and
`pip install "pg-partsmith[cli]"`.

## Inputs

| Input | How |
|---|---|
| the document | mounted at any path, given with `-c`; read-only is fine |
| the connection string | `--dsn`, or `PG_PARTSMITH_DSN`, or `PG_PARTSMITH_DSN_FILE` pointing at a mounted secret, or the document's `dsn` — in that order |
| which tables | `--table`, repeatable; every table in the document otherwise |
| hook commands | must be *in* the image or mounted into it; see [hooks in a cluster](running.md#hooks-in-a-cluster) |

`PG_PARTSMITH_DSN_FILE` exists for Docker and Swarm secrets, which arrive as files under
`/run/secrets`: naming the file is how the DSN gets in without a shell wrapper and
without appearing in `docker inspect`.

## What this container can do to your database

It issues DDL. Say that plainly to whoever approves the deployment, and give it a role of
its own rather than the application's:

```sql
CREATE ROLE partsmith LOGIN PASSWORD '…';

-- Enough to create partitions in the schema and attach them
GRANT USAGE, CREATE ON SCHEMA public TO partsmith;

-- Enough to detach and drop them: PostgreSQL requires ownership of both the
-- partition and the parent for ATTACH/DETACH, so the role owns the tables it
-- maintains, or is a member of the role that does.
ALTER TABLE public.events OWNER TO partsmith;
-- or: GRANT app_owner TO partsmith;
```

The minimum is narrower for the read-only commands: `inspect`, `plan` and `validate` need
only `USAGE` on the schema, `SELECT` on the catalog (which `PUBLIC` already has), and
`SELECT` on a partition when a retention rule asks a `SqlPredicate` about its rows. A
monitoring-only deployment can run under a role with no write grants at all.

A deployment that never wants this role to destroy anything can leave
`--allow-destructive` off entirely and let a human run the retention half.

Two things worth knowing before you point it at production:

- **`plan` runs SQL you configured.** A `SqlPredicate` in a retention rule is executed
  while planning. It is read-only, but it is your statement running with these
  credentials.
- **`apply` takes locks.** Every operation in a plan reports the heaviest lock it takes;
  `plan --locks` prints it and `plan --output json` carries it per operation under
  `capabilities`, which is the thing to read before scheduling a window.

## When the container is killed

`activeDeadlineSeconds`, an eviction, a rollout, `docker stop`: the runtime sends
`SIGTERM`, waits a grace period, then `SIGKILL`.

On `SIGTERM` the run cancels itself cleanly — the statement in flight is cancelled and
rolled back, the advisory lock released, a hook's child process terminated — and exits
`143` with `pg-partsmith: terminated` in the log. That takes a few seconds; Docker's
default grace of ten and Kubernetes' thirty are plenty.

A Python block hook is the exception. It runs on the loop that handles the signal, so a
block still running at the deadline holds the stop until it returns, and past the grace
period the runtime kills the process — the `SIGKILL` case below. Anything that can take
longer than a grace period belongs in a command hook, which is a child the stop terminates.

On `SIGKILL` none of that runs, and the database is still fine: a dropped connection makes
PostgreSQL cancel the statement and roll back its transaction, a session-level advisory
lock goes with the session, and an interrupted `DETACH … CONCURRENTLY` leaves the pending
state the next run finalizes first. The difference is only that nothing says so in the log,
and a hook's child process is on its own.

## Tags

`1.4.2` is exact and immutable. `1.4` moves within the minor version, which is what a
schedule should follow — base-image security rebuilds reach it without a config change.
`latest` is the newest release, whatever its number: what the examples in these guides
name, so they never go stale, and what a laptop pulls to try it out.

Put a schedule on the minor tag rather than on `latest`: following `latest`, a CronJob
would cross a major version on its own, at 02:15, with `DROP` in its hands.

The library, the CLI and the image carry the same number, always: `pg-partsmith` 1.4.2 on
PyPI, `pg-partsmith --version` → 1.4.2, and `ghcr.io/bedrock-python/pg-partsmith:1.4.2`
containing exactly that. The `org.opencontainers.image.version` label says so too, and CI
refuses to publish an image whose `--version` disagrees with its tag.

## Size

The image is checked against a budget in CI and a regression fails the build, because for
anyone not writing Python the size is the first thing they read about this project. There
is no Debian userland to carry: the budget is 160 MB and the image sits well under it (the
CI log prints the exact number for every build). A start is an interpreter with nothing to
byte-compile and little else to load.

## Supply chain

- **Locked install.** The builder runs `uv sync --locked` from `uv.lock`; a lock file out
  of step with `pyproject.toml` fails the build rather than resolving something else.
- **SBOM and provenance.** Every published image carries an SBOM and a SLSA build
  provenance attestation, so "what is in it" and "what built it" are answers the registry
  gives.
- **Signed.** Every published image is signed keylessly with the release workflow's own
  identity. To check that what you are about to run was built by this repository's
  release workflow and not by someone holding a token:

  ```bash
  cosign verify ghcr.io/bedrock-python/pg-partsmith:latest \
    --certificate-identity https://github.com/bedrock-python/pg-partsmith/.github/workflows/publish.yml@refs/heads/master \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ```

  The identity is the release workflow on `master`, exactly: not any workflow in the
  repository, and not a branch.

- **Both architectures.** The release builds amd64 and arm64 each on a machine of its own
  kind, scans each, and only then joins them under one tag; CI builds and runs both the
  same way, with the same size budget and the same scan.
- **The image before the index.** The release builds, proves and scans the image before
  anything is uploaded to PyPI, so a base image that fails the scan on release day stops
  the release whole rather than leaving a library with no image to go with it.
- **Verified after publishing.** The release workflow pulls the image it just pushed, on
  both architectures, and checks it the way this page tells you to: the signature, the
  SBOM and the provenance, `--version` and the label against the tag, the minor tag
  against the exact one — and then runs the end-to-end suite against it.
- **Scanned**, on every pull request and every release, with `HIGH` and `CRITICAL` failing
  the build. The runtime image holds nothing that is not needed to run one command, which
  is most of what keeps that list empty.
- **Kept current.** Dependabot opens a pull request for the base images, the actions and
  the lock file, so a base-image security rebuild is a merge and a release.

## Building it yourself

```bash
docker build --build-arg VERSION=$(python -c 'import pg_partsmith; print(pg_partsmith.__version__)') -t pg-partsmith:local .
```

The [Dockerfile](https://github.com/bedrock-python/pg-partsmith/blob/master/Dockerfile) is
two stages and takes one argument, the version to stamp into the OCI label. A derived
image that adds a hook binary is one `FROM` and one `COPY` — see
[hooks in a cluster](running.md#hooks-in-a-cluster).

## Exit codes

The same ones [the CLI](cli.md#exit-codes) documents. `0` nothing pending, `2` drift under
`plan --check`, `3` findings or run issues, `4` configuration, `5` the database, `6` the lock
is held, `64` usage, `130` / `143` stopped by a signal, `1` unexpected. Treat `6` as
success in whatever runs this: two runs overlapping is ordinary, and the second standing
aside is the lock doing its job.
