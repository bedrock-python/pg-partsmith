# Ways to run it

`pg-partsmith` is a process that runs one command and exits with a code. It has no
scheduler of its own, on purpose: cron, a CronJob, a CI schedule or a systemd timer
already exist wherever it will run, and a long-lived daemon holding DDL credentials would
need its own liveness story for no gain. Every scenario below is the same four commands
in a different harness.

The commands, and which ones change anything:

| Command | DDL | Lock | Hooks | Typical place |
|---|---|---|---|---|
| `validate` | no | no | no | CI, a pre-deploy check |
| `inspect` | no | no | no | a terminal, a dashboard |
| `plan` | no | no | no | review, monitoring (`--check`, `--output metrics`) |
| `apply` | yes | yes | with `--allow-hooks` | a schedule, an init step |

`apply` creates and re-attaches by default and retires nothing; `--allow-destructive`
adds detaches and drops. That is the whole difference between "safe at startup" and
"nightly maintenance", and it is one flag.

Everything reads the same three inputs: a [document](../reference/document.md) (`-c`),
a connection string (`--dsn`, then `$PG_PARTSMITH_DSN`, then `$PG_PARTSMITH_DSN_FILE`,
then the document's `dsn`), and an optional `--table` to narrow the run. The exit codes
are in [the CLI page](cli.md#exit-codes); the ones a harness cares about are `0`
(nothing pending), `2` (drift, under `plan --check`), `3` (a person is needed) and `6`
(another run holds the lock — ordinary, not a failure).

## On a laptop

```bash
pip install "pg-partsmith[cli]"
export PG_PARTSMITH_DSN=postgresql://app:secret@localhost/app

pg-partsmith validate -c partitions.yaml
pg-partsmith plan     -c partitions.yaml --locks
pg-partsmith apply    -c partitions.yaml --allow-destructive
```

The review loop, when the drop is one you want a second pair of eyes on:

```bash
pg-partsmith plan  -c partitions.yaml --save plan.json     # zero DDL; read it, diff it
pg-partsmith apply -c partitions.yaml --plan plan.json --allow-destructive
```

`apply --plan` is refused if the document was edited in between, so what runs is what
was reviewed.

## Plain Docker

```bash
docker run --rm \
  -v "$PWD/partitions.yaml:/etc/partitions.yaml:ro" \
  -e PG_PARTSMITH_DSN=postgresql://app:secret@db.internal/app \
  ghcr.io/bedrock-python/pg-partsmith:1.1 \
  plan -c /etc/partitions.yaml --check
```

The image's entrypoint is the command, so everything after the image name is the
command line. The document is mounted read-only; the DSN comes from the environment so
the file can be committed.

A database on the same host, reachable at `localhost` from the host but not from a
container: `--network host`, or point the DSN at `host.docker.internal`.

### Host cron

```cron
15 2 * * *  docker run --rm -v /etc/pg-partsmith/partitions.yaml:/etc/partitions.yaml:ro \
            --env-file /etc/pg-partsmith/env ghcr.io/bedrock-python/pg-partsmith:1.1 \
            apply -c /etc/partitions.yaml --allow-destructive >> /var/log/pg-partsmith.log 2>&1
```

Two runs overlapping — a slow night, a shortened interval — are not a problem: the second
finds the table's advisory lock held and exits `6`.

### systemd timer

```ini
# /etc/systemd/system/pg-partsmith.service
[Unit]
Description=Partition maintenance
After=docker.service

[Service]
Type=oneshot
EnvironmentFile=/etc/pg-partsmith/env
ExecStart=/usr/bin/docker run --rm \
  -v /etc/pg-partsmith/partitions.yaml:/etc/partitions.yaml:ro \
  -e PG_PARTSMITH_DSN ghcr.io/bedrock-python/pg-partsmith:1.1 \
  apply -c /etc/partitions.yaml --allow-destructive
SuccessExitStatus=6
```

```ini
# /etc/systemd/system/pg-partsmith.timer
[Timer]
OnCalendar=*-*-* 02:15:00
Persistent=true

[Install]
WantedBy=timers.target
```

`SuccessExitStatus=6` is the lock-held code; without it a run that correctly stood
aside shows up as a failed unit. `Persistent=true` runs a missed tick at the next boot.

## Docker Compose

Three shapes, depending on what should own the timing.

### A service you run, not one that runs

```yaml
services:
  partition-maintenance:
    image: ghcr.io/bedrock-python/pg-partsmith:1.1
    profiles: ["maintenance"]        # not started by `docker compose up`
    environment:
      PG_PARTSMITH_DSN_FILE: /run/secrets/pg_dsn
    secrets: [pg_dsn]
    volumes:
      - ./partitions.yaml:/etc/partitions.yaml:ro
    depends_on:
      db: { condition: service_healthy }

secrets:
  pg_dsn:
    file: ./secrets/pg_dsn
```

```bash
docker compose run --rm partition-maintenance plan  -c /etc/partitions.yaml
docker compose run --rm partition-maintenance apply -c /etc/partitions.yaml --allow-destructive
```

The profile keeps it out of `up`; the secret keeps the DSN out of the file and out of
`docker inspect`. `PG_PARTSMITH_DSN_FILE` is read exactly for this: a secret arrives as a
file under `/run/secrets`, and naming it is how it gets in without a shell wrapper.

### Before the application starts

Compose has the equivalent of an init container:

```yaml
services:
  partitions:
    image: ghcr.io/bedrock-python/pg-partsmith:1.1
    command: ["apply", "-c", "/etc/partitions.yaml"]      # creations only
    environment: { PG_PARTSMITH_DSN_FILE: /run/secrets/pg_dsn }
    secrets: [pg_dsn]
    volumes: ["./partitions.yaml:/etc/partitions.yaml:ro"]
    depends_on:
      db: { condition: service_healthy }

  app:
    image: your/app
    depends_on:
      partitions: { condition: service_completed_successfully }
```

`app` does not start until `partitions` exits `0`. Without `--allow-destructive` that
run creates the partitions the application is about to write into and retires nothing,
which is what running at startup should do.

### On a schedule, inside Compose

Compose has no clock. Either the host's cron runs `docker compose run` (above), or a
cron sidecar does — [ofelia](https://github.com/mcuadros/ofelia) reads labels:

```yaml
services:
  scheduler:
    image: mcuadros/ofelia:latest
    command: daemon --docker
    volumes: ["/var/run/docker.sock:/var/run/docker.sock:ro"]

  partition-maintenance:
    image: ghcr.io/bedrock-python/pg-partsmith:1.1
    command: ["apply", "-c", "/etc/partitions.yaml", "--allow-destructive"]
    environment: { PG_PARTSMITH_DSN_FILE: /run/secrets/pg_dsn }
    secrets: [pg_dsn]
    volumes: ["./partitions.yaml:/etc/partitions.yaml:ro"]
    labels:
      ofelia.enabled: "true"
      ofelia.job-run.maintenance.schedule: "0 15 2 * * *"
```

## Docker Swarm

A Swarm service is expected to keep running; a one-shot task needs
`--restart-condition none`, and a schedule needs something to create the task —
[swarm-cronjob](https://github.com/crazy-max/swarm-cronjob) does that from labels:

```yaml
services:
  cronjob:
    image: crazymax/swarm-cronjob:latest
    volumes: ["/var/run/docker.sock:/var/run/docker.sock:ro"]
    deploy:
      placement:
        constraints: [node.role == manager]

  partition-maintenance:
    image: ghcr.io/bedrock-python/pg-partsmith:1.1
    command: ["apply", "-c", "/etc/partitions.yaml", "--allow-destructive"]
    environment: { PG_PARTSMITH_DSN_FILE: /run/secrets/pg_dsn }
    secrets: [pg_dsn]
    configs:
      - source: partitions
        target: /etc/partitions.yaml
    deploy:
      replicas: 0
      restart_policy: { condition: none }
      labels:
        swarm.cronjob.enable: "true"
        swarm.cronjob.schedule: "0 15 2 * * *"
        swarm.cronjob.skip-running: "true"

configs:
  partitions:
    file: ./partitions.yaml

secrets:
  pg_dsn:
    external: true
```

`replicas: 0` until the schedule fires; `skip-running` is the outer half of "two runs
must not overlap", the advisory lock is the inner half. A one-off run by hand:

```bash
docker service create --name partition-plan --restart-condition none --detach=false \
  --config source=partitions,target=/etc/partitions.yaml --secret pg_dsn \
  -e PG_PARTSMITH_DSN_FILE=/run/secrets/pg_dsn \
  ghcr.io/bedrock-python/pg-partsmith:1.1 plan -c /etc/partitions.yaml --locks
docker service logs partition-plan && docker service rm partition-plan
```

## Kubernetes

The document is a ConfigMap, the DSN a Secret, and the pod runs as the image's own
non-root user. What differs between the shapes is who creates the pod and when.

```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: partition-config }
data:
  partitions.yaml: |
    defaults: { schema: public, granularity: month, create_ahead_count: 3, retention_count: 12 }
    tables:
      - { table_name: events, partition_column: created_at }
---
apiVersion: v1
kind: Secret
metadata: { name: partsmith-dsn }
stringData:
  dsn: postgresql://partsmith:secret@db.internal/app
```

The container spec every shape shares:

```yaml
# anchors are illustrative; paste the block where it is needed
- name: pg-partsmith
  image: ghcr.io/bedrock-python/pg-partsmith:1.1
  args: ["plan", "-c", "/etc/partitions.yaml"]
  env:
    - name: PG_PARTSMITH_DSN
      valueFrom: { secretKeyRef: { name: partsmith-dsn, key: dsn } }
  volumeMounts:
    - { name: config, mountPath: /etc/partitions.yaml, subPath: partitions.yaml, readOnly: true }
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    readOnlyRootFilesystem: true
    allowPrivilegeEscalation: false
  resources:
    requests: { cpu: 50m, memory: 128Mi }
    limits: { memory: 256Mi }
```

`readOnlyRootFilesystem: true` works: the image writes nothing at runtime. Give
`plan --save` a mounted volume to write to.

### A Pod, once, by hand

```bash
kubectl run partition-plan --rm -it --restart=Never \
  --image=ghcr.io/bedrock-python/pg-partsmith:1.1 \
  --env="PG_PARTSMITH_DSN=$(kubectl get secret partsmith-dsn -o jsonpath='{.data.dsn}' | base64 -d)" \
  --overrides='{"spec":{"containers":[{"name":"pg-partsmith","image":"ghcr.io/bedrock-python/pg-partsmith:1.1","args":["inspect","-c","/etc/partitions.yaml"],"volumeMounts":[{"name":"config","mountPath":"/etc/partitions.yaml","subPath":"partitions.yaml"}]}],"volumes":[{"name":"config","configMap":{"name":"partition-config"}}]}}'
```

For a look, not a change: `inspect`, `plan --locks`, `validate`. The exit code comes back
as the pod's.

### A Job: once, reliably

```yaml
apiVersion: batch/v1
kind: Job
metadata: { name: partition-converge }
spec:
  backoffLimit: 0                 # a failed apply is read by a person, not retried blind
  ttlSecondsAfterFinished: 86400  # the log stays a day
  activeDeadlineSeconds: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: pg-partsmith
          image: ghcr.io/bedrock-python/pg-partsmith:1.1
          args: ["apply", "-c", "/etc/partitions.yaml", "--allow-destructive", "--continue-on-error"]
          env:
            - { name: PG_PARTSMITH_DSN, valueFrom: { secretKeyRef: { name: partsmith-dsn, key: dsn } } }
          volumeMounts:
            - { name: config, mountPath: /etc/partitions.yaml, subPath: partitions.yaml, readOnly: true }
      volumes:
        - { name: config, configMap: { name: partition-config } }
```

The first run against a table with years of history, a one-off after a policy change, a
migration step in a pipeline. `backoffLimit: 0` because a retry of a failed `apply` with
no one looking is the wrong reflex — the run is idempotent, so re-running it *after
reading the log* is safe and costs nothing. `activeDeadlineSeconds` sends `SIGTERM`, which
the run [handles](cli.md#being-stopped).

### A CronJob: the nightly tick

```yaml
apiVersion: batch/v1
kind: CronJob
metadata: { name: partition-maintenance }
spec:
  schedule: "15 2 * * *"
  concurrencyPolicy: Forbid
  startingDeadlineSeconds: 3600
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 5
  jobTemplate:
    spec:
      backoffLimit: 0
      activeDeadlineSeconds: 3600
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: pg-partsmith
              image: ghcr.io/bedrock-python/pg-partsmith:1.1
              args: ["apply", "-c", "/etc/partitions.yaml", "--allow-destructive"]
              env:
                - { name: PG_PARTSMITH_DSN, valueFrom: { secretKeyRef: { name: partsmith-dsn, key: dsn } } }
              volumeMounts:
                - { name: config, mountPath: /etc/partitions.yaml, subPath: partitions.yaml, readOnly: true }
          volumes:
            - { name: config, configMap: { name: partition-config } }
```

`concurrencyPolicy: Forbid` and the advisory lock answer the same question from two
directions; keep both. `startingDeadlineSeconds` lets a tick that missed its slot (the
controller was down) still run within the hour rather than being skipped.

How often: often enough that the partitions created ahead outlast the longest gap between
ticks. A converged table costs one catalog round-trip and no DDL, so there is no reason
to be stingy — hourly for daily partitions is normal.

### An init container: create before the app writes

```yaml
spec:
  initContainers:
    - name: partitions
      image: ghcr.io/bedrock-python/pg-partsmith:1.1
      args: ["apply", "-c", "/etc/partitions.yaml"]          # no --allow-destructive
      env:
        - { name: PG_PARTSMITH_DSN, valueFrom: { secretKeyRef: { name: partsmith-dsn, key: dsn } } }
      volumeMounts:
        - { name: config, mountPath: /etc/partitions.yaml, subPath: partitions.yaml, readOnly: true }
  containers:
    - name: app
      image: your/app
```

Creations only. Retention at application startup is a bad idea — every replica restart
would be a chance to drop something — and the default is what makes it impossible by
accident. Ten replicas starting at once are fine: nine find the lock held, exit `6`, and
`6` is not a failure for the pod to restart on... except that an init container *does*
restart on non-zero. Give it the standard wrapper for that one case:

```yaml
      command: ["sh", "-c", "pg-partsmith apply -c /etc/partitions.yaml; rc=$?; [ $rc -eq 6 ] && exit 0; exit $rc"]
```

### Plan on one day, apply on another

The artifact between the two halves, in a cluster: a small PVC, a Job that writes, a
person, a Job that reads.

```yaml
# Job 1
args: ["plan", "-c", "/etc/partitions.yaml", "--save", "/work/plan.json", "--locks"]
volumeMounts: [{ name: work, mountPath: /work }]
# read it:  kubectl cp <pod>:/work/plan.json ./plan.json   (or run `plan --output json` and read the log)
# Job 2
args: ["apply", "-c", "/etc/partitions.yaml", "--plan", "/work/plan.json", "--allow-destructive"]
```

`apply --plan` refuses the file if the ConfigMap changed in between, so a document edited
after review does not get the old plan applied under it.

### Metrics, into a node_exporter textfile

The textfile collector reads a directory on the node; the run writes into it. That needs
a redirect, which is the one place a shell is used:

```yaml
command: ["sh", "-c", "pg-partsmith plan -c /etc/partitions.yaml --check --output metrics > /textfile/partsmith.prom.$$ && mv /textfile/partsmith.prom.$$ /textfile/partsmith.prom"]
volumeMounts:
  - { name: textfile, mountPath: /textfile }
volumes:
  - { name: textfile, hostPath: { path: /var/lib/node_exporter/textfile, type: Directory } }
```

The rename makes the write atomic, so the collector never reads half a file. A
`plan --check` on a schedule of its own is the cheapest monitoring there is: exit `2`
means partitions that should exist do not yet, before an insert finds out.

### Hooks in a cluster

A command hook runs *inside* this container, so the binary has to be in it. Two ways:

```dockerfile
FROM ghcr.io/bedrock-python/pg-partsmith:1.1
COPY --chmod=755 archive-partition /usr/local/bin/archive-partition
```

or an init container that copies it into a shared `emptyDir` mounted at `/opt/hooks`,
with the document naming `/opt/hooks/archive-partition`. Either way, `apply` needs
`--allow-hooks`, and the document declaring hooks is refused without it — see
[Commands around the lifecycle](hooks-in-config.md).

## CI

`validate` is the check that belongs in a pipeline: the document parses, the connection
works, every table it describes is partitioned the way it claims.

```yaml
# GitHub Actions
- name: Validate partition config
  run: pg-partsmith validate -c partitions.yaml
  env:
    PG_PARTSMITH_DSN: ${{ secrets.STAGING_DATABASE_URL }}
```

```yaml
# GitLab CI
validate-partitions:
  image: { name: "ghcr.io/bedrock-python/pg-partsmith:1.1", entrypoint: [""] }
  script:
    - pg-partsmith validate -c partitions.yaml
  variables:
    PG_PARTSMITH_DSN: $STAGING_DATABASE_URL
```

GitLab runs `script` through a shell, so the image's entrypoint is cleared. A scheduled
pipeline running `plan --check` against production is the same alert as the CronJob
version, from the other side.

## Any other scheduler

Airflow's `KubernetesPodOperator` or `DockerOperator`, Argo Workflows, Nomad's periodic
jobs, Dagster, Rundeck: anything that runs a container and reads an exit code runs this.
The contract is the four commands, the three inputs, and the codes — nothing about the
harness is assumed. Treat `6` as success and `2` (under `--check`) as a signal, not a
failure, and the rest maps onto whatever the scheduler calls "failed".

## Several databases

One document per database, or one document and a different `--dsn` per run — the
document does not have to carry a DSN at all. Narrow a big document with `--table` when
one table needs a run of its own.

## What to grant

See [the image page](container.md#what-this-container-can-do-to-your-database): a role
of its own, `USAGE, CREATE` on the schema for creations, ownership of the parent for
detaches and drops. A monitoring-only deployment (`plan`, `inspect`, `validate`) needs
none of the write grants.
