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

## Writing the script

The shipped [`examples/hooks/`](https://github.com/bedrock-python/pg-partsmith/tree/master/examples/hooks)
are the templates: a `pg_dump` before a drop, a webhook after a create, a `COPY` out
before a detach. The pattern is the same in any language: read stdin if you want the whole
event, read the environment if the name is enough, do the thing, exit non-zero to refuse.

**Bash**, with the environment and nothing else:

```bash
#!/usr/bin/env bash
set -euo pipefail
cat >/dev/null                                  # the event; not needed here
partition="${PG_PARTSMITH_PARTITION:?}"
pg_dump --format=custom --table="$partition" --file="/archive/${partition//./_}.dump.part"
mv "/archive/${partition//./_}.dump.part" "/archive/${partition//./_}.dump"
```

**Bash with `jq`**, when the size or the window matters:

```bash
event=$(cat)
size=$(jq -r '.operation.size_bytes // 0' <<<"$event")
window_end=$(jq -r '.window.end' <<<"$event")
[ "$size" -gt 10737418240 ] && echo "too big to archive tonight" >&2 && exit 1
```

**Go**, for a team that already has one:

```go
var event struct {
    Phase     string `json:"phase"`
    Partition struct{ Name string `json:"name"` } `json:"partition"`
    Operation struct {
        Reason    string `json:"reason"`
        SizeBytes *int64 `json:"size_bytes"`
    } `json:"operation"`
}
if err := json.NewDecoder(os.Stdin).Decode(&event); err != nil { log.Fatal(err) }
if err := archive(event.Partition.Name); err != nil { os.Exit(1) }   // refuses the drop
```

**Python, as a file** (`python_file: hooks/export_partition.py`): no stdin, no JSON —
`event` is the object itself, `log` is a logger, and `raise` refuses:

```python
if shutil.which("psql") is None:
    raise RuntimeError("psql is not on PATH; refusing to detach without an export")
log.info("exporting %s", event.partition.name)
subprocess.run(["psql", "-c", f"COPY {event.partition.name} TO STDOUT WITH (FORMAT csv)"], stdout=out, check=True)
```

Three things every script should get right:

- **Write to a temporary name and rename.** A run can be stopped mid-hook; the child is
  terminated, and a half-written dump under the final name looks finished.
- **Refuse loudly.** Whatever went wrong belongs on stderr: it is carried in the error the
  run reports, and it is the only place anyone will look at 03:00.
- **Do not talk to the table being dropped through the same connection pool the run
  uses.** The hook is a separate process with its own credentials; `PGHOST`, `PGUSER`
  and friends are inherited from the run's environment, so `pg_dump` and `psql` just work.

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
lock. A command is also stopped when the run is — Ctrl+C, a pod's `SIGTERM` — with
`terminate` first and `kill` a few seconds later, so nothing it started keeps running
unwatched.

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

## A block of Python instead of a command

For the Python-adjacent audience, a phase can take a block instead of a binary — the
authentik model: a snippet in the document, evaluated with a prepared namespace when the
moment comes.

```yaml
hooks:
  before_drop:
    python: |
      if event.operation.size_bytes and event.operation.size_bytes > 10 * 2**30:
          log.warning("exporting %s before drop", event.partition.name)
          export_to_s3(event.partition.name, event.window)
  after_create:
    python_file: hooks/notify.py
```

The block sees two names: `event`, the same `PartitionEvent` a hook class is handed, and
`log`, a logger named for the phase. Nothing else is injected — a block that needs
`datetime` imports it, the way any Python does. Raising is how a block refuses the
operation, exactly as a non-zero exit is for a command.

Inline blocks get unreadable past a few lines and lose editor support, so `python_file`
points at a file instead, resolved relative to the document. A file can be tested on its
own.

**Every block is compiled by `validate`**, and inline ones the moment the document is
read: a `SyntaxError` is a validation error with a line number in it, not something a
CronJob discovers at 03:00 after some of the run's DDL has committed. A missing or broken
`python_file` fails `validate` by name.

The block runs in the process, on a thread of its own, so the loop that fires it stays free
to take a stop: `SIGTERM` cancels the run, which cleans up and exits, and a block still
running at that point is abandoned with the process. The responsibility is the one a hook
written as a class carries: a block that blocks, blocks maintenance until it returns. Work
that can outlast a grace period is better as a command hook, a child the stop terminates.

### What this is not

A sandbox. `exec` with a filtered `__builtins__` is not a security boundary and is
trivially escaped; documenting one would be worse than not having one. The block runs as
the process runs, with every credential the process holds — which is why it is behind
the same `--allow-hooks` as a command, and why the trust argument above applies to it word
for word. If isolation is what you need, a command hook in a container of its own is the
answer, not a restricted namespace.

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

`PythonHooks` is the block form, and takes the source per phase:

```python
from pg_partsmith.aio import PythonHooks

hooks = PythonHooks({HookPhase.BEFORE_DROP: "log.warning('dropping %s', event.partition.name)"})
```

Every block is compiled when the object is built, so a typo is a `SyntaxError` at wiring
time rather than at the first drop.

For anything that wants to stay inside Python, write an ordinary hook class instead — see
[Extend the library](extending.md).
