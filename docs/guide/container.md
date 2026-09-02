# The container image

```
ghcr.io/bedrock-python/pg-partsmith:1.1
```

The image is [the CLI](cli.md) and nothing else: the entrypoint is `pg-partsmith`, so a
Compose service, a Swarm task or a Kubernetes Job names only what it wants done. Every
way of running it — plain Docker, Compose, Swarm, a Pod, a Job, a CronJob, an init
container, CI — is on [Ways to run it](running.md); this page is about the image itself.

```bash
docker run --rm \
  -v "$PWD/partitions.yaml:/etc/partitions.yaml:ro" \
  -e PG_PARTSMITH_DSN=postgresql://app:secret@db.internal/app \
  ghcr.io/bedrock-python/pg-partsmith:1.1 \
  plan -c /etc/partitions.yaml --check
```

## What is inside

A `python:3.13-slim` base and one virtualenv: the library, pydantic, SQLAlchemy,
asyncpg, PyYAML and typer. The build stage — pip, the build backend, every
`__pycache__` — is thrown away. There is a `/bin/sh`, used by nothing in the image; it
exists for the one case that needs a redirect (writing metrics to a file) and for
wrappers like the init-container one on the deployment page.

It runs as UID 65532 (`partsmith`), a fixed high UID so a `runAsUser` can name the same
one and a mounted document can be made readable to it without guessing. It writes nothing
at runtime, so `readOnlyRootFilesystem: true` works; give `plan --save` a volume.

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

On `SIGKILL` none of that runs, and the database is still fine: a dropped connection makes
PostgreSQL cancel the statement and roll back its transaction, a session-level advisory
lock goes with the session, and an interrupted `DETACH … CONCURRENTLY` leaves the pending
state the next run finalizes first. The difference is only that nothing says so in the log,
and a hook's child process is on its own.

## Tags

`1.4.2` is exact and immutable. `1.4` moves within the minor version, which is what a
schedule should follow — base-image security rebuilds reach it without a config change.

There is deliberately **no `latest`**: a scheduled job following it would cross a major
version on its own, at 02:15, with `DROP` in its hands.

The library, the CLI and the image carry the same number, always: `pg-partsmith` 1.4.2 on
PyPI, `pg-partsmith --version` → 1.4.2, and `ghcr.io/bedrock-python/pg-partsmith:1.4.2`
containing exactly that. The `org.opencontainers.image.version` label says so too, and CI
refuses to publish an image whose `--version` disagrees with its tag.

## Size

The image is checked against a budget in CI and a regression fails the build, because for
anyone not writing Python the size is the first thing they read about this project. It is
a `python:3.13-slim` base plus the venv: about 165 MB as CI measures it on Linux (Docker
Desktop reports more for the same layers); the budget is 280.

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
`plan --check`, `3` findings or run issues, `4` configuration, `5` connection, `6` the lock
is held, `64` usage, `130` / `143` stopped by a signal, `1` unexpected. Treat `6` as
success in whatever runs this: two runs overlapping is ordinary, and the second standing
aside is the lock doing its job.
