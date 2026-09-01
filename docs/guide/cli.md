# The command line

```bash
pip install "pg-partsmith[cli]"
```

`pg-partsmith` runs the library's read-only half over a
[configuration document](../reference/document.md) and a DSN — no Python in your
application, no import, no dependency on this library at all.

```bash
pg-partsmith validate -c partitions.yaml     # does the document match the database?
pg-partsmith inspect  -c partitions.yaml     # what tree actually exists?
pg-partsmith plan     -c partitions.yaml     # what would maintenance do, and why?
```

None of the three issues DDL, takes a lock, or fires a hook.

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

## What is not here yet

`apply`. `plan` and `apply` stay separable — the plan is the artifact between them, and
the library already refuses a plan the configuration did not produce — but the applying
half is not in this release. Until it is, run maintenance from Python and use these three
to see it.
