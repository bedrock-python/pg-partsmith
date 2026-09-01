# Commands around the lifecycle

Hooks are where the destructive half of this becomes usable: an operator who cannot run
anything before a `DROP` will not point a scheduled job at production. From
[a document](../reference/document.md), a hook is a command:

```yaml
hooks:
  timeout_seconds: 600
  before_drop: ["/usr/local/bin/archive-partition"]
  after_create: ["/usr/local/bin/notify", "--channel", "#dba"]
```

```bash
pg-partsmith apply -c partitions.yaml --allow-destructive --allow-hooks
```

This is deliberately language-neutral. A Go or Ruby team already owns an archiver; what
they need is for it to be invoked at the right moment with the right metadata, not a
Python snippet.

## What the command is handed

The whole event, as JSON, on stdin — the same object a Python hook is given:

```json
{
  "phase": "before_drop",
  "config": { "schema": "public", "table_name": "events" },
  "partition": { "name": "public.events__2025_08", "oid": 40321, "bounds": { "kind": "range" } },
  "window": { "start": "2025-08-01T00:00:00+00:00", "end": "2025-09-01T00:00:00+00:00" },
  "operation": { "kind": "drop", "reason": "grace_elapsed", "size_bytes": 1073741824 }
}
```

Plus, for shell scripts that would rather not parse it: `PG_PARTSMITH_PHASE`,
`PG_PARTSMITH_TABLE`, `PG_PARTSMITH_PARTITION`, and `PG_PARTSMITH_WINDOW_START` /
`PG_PARTSMITH_WINDOW_END` when the partition covers a period. The rest of the environment
is inherited, because a hook needs its own credentials and its `PATH`.

`before_drop` is told the window and the size even though `DETACH` has already cleared the
catalog's record of the bounds: what it gets is the window the planner decided the drop on.

## The phases

`before_create`, `after_create`, `before_attach`, `after_attach`, `before_detach`,
`after_detach`, `before_drop`, `after_drop`. Each takes one command.

Hooks fire once per **lifecycle unit** — the partition directly under the root — never
once per leaf. For a `RANGE → HASH` table, `before_drop` runs once for the week, not once
per bucket.

## A non-zero exit is a refusal

Exactly as a raised exception is for a Python hook: the operation is abandoned and planned
again on the next run. A `before_drop` that exits non-zero leaves the partition detached
and undropped, which is what an archiver saying "not yet" should mean.

By default one refusal aborts the whole run. `--continue-on-error` isolates it to its own
partition and lets the rest of the table be maintained; the failure comes back in the
run's issues, and the command exits `3`.

A command that outlives `timeout_seconds` (default 300) is killed, and that counts as a
refusal. There is no "wait forever": a hook that hangs is holding the table's maintenance
lock.

## Hooks never fire during `plan`

`plan` issues no DDL and runs no hook. That is what makes a plan safe to compute anywhere,
and it is why the plan/apply split is worth having.

One honest caveat in the other direction: **`plan` does run SQL you configured.** A
`SqlPredicate` in a retention rule is executed while planning. It is read-only, but it is
your statement, running with the credentials the run holds.

## Why `--allow-hooks` exists

Running a document's commands is arbitrary code execution in a process holding DDL
credentials. The honest framing:

- The document **already** authorises dropping tables. A command in the same file is not a
  new privilege boundary, as long as the document comes from the same trust domain as the
  DSN.
- That stops being true the moment the document is assembled from somewhere less trusted —
  a ConfigMap another team can edit, a templated file, anything user-supplied.

So commands run only when the run is told to run them, and a document declaring hooks is
**refused** rather than quietly stripped when it is not: silently skipping a configured
`before_drop` would let an operator read the file, believe their archiver ran, and be
wrong. A hardened deployment can leave `--allow-hooks` off and prove nothing was executed.

There is **no sandbox**, and none is claimed. If you need real isolation, the command is
the answer rather than a restricted namespace: put it in a sidecar or a mounted binary
with its own credentials, and let the container boundary be the boundary.

## From Python

The same class, without a document:

```python
from pg_partsmith import HookPhase
from pg_partsmith.aio import CommandHooks, PartitionToolkit

kit = PartitionToolkit.from_engine(
    engine,
    hooks=[CommandHooks({HookPhase.BEFORE_DROP: ["/usr/local/bin/archive-partition"]})],
)
```

`pg_partsmith.sync.CommandHooks` is the same thing on a sync engine. Both run the command
the same way, which does not depend on the event loop being able to spawn processes — so a
hook that works in production works on a developer's machine too.

For anything that wants to stay inside Python, write an ordinary hook class instead — see
[Extend the library](extending.md).
