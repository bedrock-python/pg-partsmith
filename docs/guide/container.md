# The container image

```
ghcr.io/bedrock-python/pg-partsmith:1.1
```

The image is [the CLI](cli.md) and nothing else: the entrypoint is `pg-partsmith`, so a
Compose service or a CronJob names only what it wants done.

## Docker Compose

```yaml
services:
  partition-maintenance:
    image: ghcr.io/bedrock-python/pg-partsmith:1.1
    command: ["apply", "-c", "/etc/partitions.yaml"]
    environment:
      PG_PARTSMITH_DSN: postgresql://partsmith:secret@db/app
    volumes:
      - ./partitions.yaml:/etc/partitions.yaml:ro
```

## Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: partition-maintenance
spec:
  schedule: "15 2 * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          securityContext:
            runAsNonRoot: true
            runAsUser: 65532
          containers:
            - name: pg-partsmith
              image: ghcr.io/bedrock-python/pg-partsmith:1.1
              args: ["apply", "-c", "/etc/partitions.yaml", "--allow-destructive"]
              env:
                - name: PG_PARTSMITH_DSN
                  valueFrom:
                    secretKeyRef: { name: partsmith-dsn, key: dsn }
              volumeMounts:
                - { name: config, mountPath: /etc/partitions.yaml, subPath: partitions.yaml, readOnly: true }
          volumes:
            - name: config
              configMap: { name: partition-config }
```

`concurrencyPolicy: Forbid` and the library's advisory lock answer the same question from
two directions; keep both. A run that finds the lock held exits `6`, which is ordinary
operation and not something to alert on.

## As an init container

```yaml
initContainers:
  - name: partitions
    image: ghcr.io/bedrock-python/pg-partsmith:1.1
    args: ["apply", "-c", "/etc/partitions.yaml"]
```

No extra flag: without `--allow-destructive`, `apply` creates the partitions the
application is about to write into and retires nothing. Running retention at application
startup is a bad idea, and the default is what stops it happening by accident.

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
`SELECT` on a partition when a retention rule asks a `SqlPredicate` about its rows.

A deployment that never wants this role to destroy anything can leave
`--allow-destructive` off entirely and let a human run the retention half.

Two things worth knowing before you point it at production:

- **`plan` runs SQL you configured.** A `SqlPredicate` in a retention rule is executed
  while planning. It is read-only, but it is your statement running with these
  credentials.
- **`apply` takes locks.** Every operation in a plan reports the heaviest lock it takes;
  `plan --locks` prints it and `plan --output json` carries it per operation under
  `capabilities`, which is the thing to read before scheduling a window.

## Tags

`1.4.2` is exact and immutable. `1.4` moves within the minor version, which is what a
CronJob should follow — base-image security rebuilds reach it without a config change.

There is deliberately **no `latest`**: a scheduled job following it would cross a major
version on its own, at 02:15, with `DROP` in its hands.

The library, the CLI and the image carry the same number, always: `pg-partsmith` 1.4.2 on
PyPI, `pg-partsmith --version` → 1.4.2, and `ghcr.io/bedrock-python/pg-partsmith:1.4.2`
containing exactly that. The `org.opencontainers.image.version` label says so too, and CI
refuses to publish an image whose `--version` disagrees with its tag.

## Size

The image is checked against a budget in CI and a regression fails the build, because for
anyone not writing Python the size is the first thing they read about this project. It is
a `python:3.13-slim` base plus the venv: pydantic, SQLAlchemy, python-dateutil, asyncpg
and PyYAML, with pip, the build backend and every `__pycache__` left behind in the
discarded build stage.

## Monitoring

`--output metrics` writes Prometheus text exposition, which in a CronJob means mounting a
node_exporter textfile directory and redirecting into it:

```yaml
args: ["plan", "-c", "/etc/partitions.yaml", "--check", "--output", "metrics"]
```

See [the CLI page](cli.md#monitoring-for-free) for the series and the alerts worth
building on them.

## Exit codes

The same ones [the CLI](cli.md#exit-codes) documents. `0` nothing pending, `2` drift under
`plan --check`, `3` findings or run issues, `4` configuration, `5` connection, `6` the lock
is held, `64` usage, `1` unexpected.
