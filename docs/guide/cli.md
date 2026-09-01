# The command line

```bash
pip install "pg-partsmith[cli]"
```

`pg-partsmith` runs partition maintenance over a
[configuration document](../reference/document.md) and a DSN — no Python in your
application, no import, no dependency on this library at all.

```bash
pg-partsmith validate -c partitions.yaml     # does the document match the database?
pg-partsmith inspect  -c partitions.yaml     # what tree actually exists?
pg-partsmith plan     -c partitions.yaml     # what would maintenance do, and why?
pg-partsmith apply    -c partitions.yaml     # do it — creations only, by default
```

The first three issue no DDL, take no lock and fire no hook. `apply` is the one that
acts.

## Where the connection comes from

`--dsn`, then `$PG_PARTSMITH_DSN`, then the document's `dsn` — in that order, so a
mounted ConfigMap can carry the tables while the password stays in a secret.

A DSN naming no driver (`postgresql://…`) is driven with asyncpg, which the `cli` extra
installs. One that names its own (`postgresql+psycopg://…`) is left exactly as written.

## Exit codes

A CronJob and a CI step read the exit code and nothing else, so the codes are worth
distinguishing:

| Code | Meaning |
|---|---|
| 0 | did what it was asked; nothing pending |
| 1 | something unexpected; the message is on stderr |
| 2 | `plan --check` found operations waiting to be applied |
| 3 | the planner reported something a human has to act on |
| 4 | the document does not parse, or does not match the database |
| 5 | the database could not be reached |
| 6 | another maintainer holds the table's lock |

Two of those deserve their reasoning spelled out.

**3 outranks 2.** Drift is what a maintenance run fixes; a finding is what it cannot —
a range overlap, a hash set with a gap no repair is safe for. If both are true, the one
needing a person wins.

**6 is not a failure.** Two runs overlapping is ordinary operation, and the second one
correctly declining to proceed is the lock doing its job. Alerting on it is the first
false page every deployment gets, so it is its own code rather than a generic error.

## Watching for maintenance that stopped running

```bash
pg-partsmith plan -c partitions.yaml --check
```

Exits `2` while anything is pending. That is the check to put on a schedule: it says
"partitions that should exist do not yet", without creating anything to find out.

## In CI

```yaml
- run: pg-partsmith validate -c partitions.yaml
  env:
    PG_PARTSMITH_DSN: ${{ secrets.DATABASE_URL }}
```

`validate` answers the question a review cannot: the document parses, the connection
works, and every table it describes is partitioned the way it claims — the right method,
on the right key.

## JSON

`--output json` on any command. The payload is the library's own model dump under a
versioned envelope, in the vocabulary a configuration file is written in:

```json
{
  "version": 1,
  "command": "plan",
  "generated_at": "2026-09-01T12:00:00+00:00",
  "tables": [
    {
      "table": "public.events",
      "plan": {
        "table_name": "public.events",
        "generated_at": "2026-09-01T12:00:00+00:00",
        "config_fingerprint": "8c1d9f0a3b2e4d57",
        "operations": [
          {"kind": "create", "target": "public.events__2026_10", "reason": "create_ahead", "…": "…"}
        ],
        "findings": []
      }
    }
  ]
}
```

It is the dump, never a shape assembled by the CLI — a hand-rolled one drifts from the
library the first time a field is added. Logs go to stderr, so stdout stays parseable.

## One table at a time

`--table events` (or `--table public.events`), repeatable. A name that no table in the
document answers to is an error naming the ones that do, rather than a silent run over
nothing.

## Applying

```bash
pg-partsmith apply -c partitions.yaml                        # create what is missing
pg-partsmith apply -c partitions.yaml --allow-destructive    # …and retire what expired
```

**Destructive operations are withheld unless you ask for them.** Without
`--allow-destructive`, `apply` creates and re-attaches, and detaches and drops nothing.
That is deliberate: the safe behaviour is the default one rather than a second mode
somebody has to remember to select, and it is exactly what an init container wants —
create the partitions the application is about to write into, retire nothing at startup.

With no `--plan`, planning and applying happen under one lock, which is also what
completes an interrupted `DETACH … CONCURRENTLY` before the rest of the run is decided.

`--continue-on-error` isolates a failed operation into the run's issues instead of
aborting: a failed create still lets pruning run, a failed detach still lets existing
orphans be dropped. Any issue makes the command exit `3`.

## The plan as an artifact

The reason to trust an external tool with `DROP` is that you can read what it will do
first:

```bash
pg-partsmith plan  -c partitions.yaml --save plan.json     # zero DDL
# read it, diff it, gate it on a human or a CI approval
pg-partsmith apply -c partitions.yaml --plan plan.json --allow-destructive
```

`--save` writes the same JSON envelope `--output json` prints, whatever format the
terminal was given. `apply --plan` reads it back and applies exactly that.

Two things are checked before anything runs, by the library rather than by the CLI:

- **The plan must be for this table.** A plan for `public.events` applied under the
  configuration of `public.audit` is refused.
- **The configuration must not have moved.** The plan records a fingerprint of the
  configuration it was made under; if the document has been edited since, applying it is
  refused with exit `4`. `--allow-config-drift` applies it as it stands.

That second one is not the same as the OID revalidation every destructive operation
already does. Revalidation asks whether the relation is still the same relation. The
fingerprint asks whether the plan is still the same intent: a plan made under
`retention_count: 12` names exactly the right partitions to expire, for a reason that
stopped being true the moment someone wrote `120`.

## In a container

The same commands, with nothing installed:
`ghcr.io/bedrock-python/pg-partsmith:1.1` — see [the container image](container.md).

## What is not here yet

`partition_data` and `unpartition` — the batched row-movement verbs — are library-only for
now, because both want a progress story a one-shot command does not have yet.
