# The configuration document

`pg_partsmith.PartitionsDocument` is every table a deployment maintains, plus the wiring
it maintains them through, as one validated model. It is what a file — a ConfigMap, a
mounted YAML, a JSON blob — parses into.

The library reads no files and opens no connections. Parse with whatever owns the format,
validate here:

```python
import yaml

from pg_partsmith import PartitionsDocument
from pg_partsmith.aio import PartitionToolkit

document = PartitionsDocument.model_validate(yaml.safe_load(Path("partitions.yaml").read_text()))
kit = PartitionToolkit.from_options(engine, document.runtime)

for config in document.configs():
    result = await kit.maintainer.run_maintenance_safe(config)
```

```yaml
version: 1

defaults:
  schema: public
  granularity: month
  create_ahead_count: 3
  retention_count: 12

tables:
  - table_name: events
    partition_column: created_at

  - table_name: audit
    partition_column: logged_at
    granularity: day
    retention_count: 400

  - table_name: telemetry
    scheme:
      method: range
      key: created_at
      boundaries: { kind: time, granularity: week }
      child: { method: hash, key: tenant_id, modulus: 4 }
    lifecycle:
      creation: { kind: create_ahead, count: 4 }
      retention: { kind: keep_for, age: P90D }
      drop: { kind: drop_after, grace: P7D }

runtime:
  ddl_timezone: UTC
  marker_prefix: acme
  drop_lock_timeout_ms: 3000
```

## Fields

| Field | Type | Default | Meaning |
|---|---|---|---|
| `version` | `1` | `1` | the document format this file is written in |
| `dsn` | text | — | connection string, for whoever connects; the library never does |
| `defaults` | mapping | `{}` | fields every table starts from |
| `tables` | list | required, at least one | the tables to maintain |
| `runtime` | mapping | library defaults | how the collaborators are wired |
| `hooks` | mapping | — | commands to run around the lifecycle; `apply --allow-hooks` only |

A table entry takes the same fields as
[`PartitionTableSettings`](settings.md) — the environment and the file are one field list,
not two. `schema` is accepted as a spelling of `schema_name`, because that is what
`TablePartitionConfig` dumps.

`runtime` takes one key per keyword of `PartitionToolkit.from_engine`:
`marker_prefix`, `ddl_timezone`, `ddl_timeout_seconds`, `boundary_codec` (a name —
`uuidv7`, `epoch_seconds`, `epoch_milliseconds`), `lock_prefix`,
`lock_min_interval_seconds`, `drop_allow_unmanaged`, `drop_lock_timeout_ms`,
`drop_max_retries`, `drop_retry_delay`, `drop_max_backoff`.

## What is refused

- **An unknown key**, in a table entry, in `runtime`, or in `defaults`. A misspelled field
  in a file nobody reads until 03:00 is a silent policy, not a typo — `granuality: month`
  is reported where it is written rather than defaulting the whole document to no calendar.
- **A document with no tables.** Maintaining nothing is a configuration error, not a
  successful run that did nothing.
- **One relation described twice.** Two entries for `public.events` would maintain it under
  two policies, in an order the file does not make visible.

## How `defaults` merges

Key by key, and only at the top level: a table naming a key owns it entirely. A table with
its own `lifecycle` replaces the default `lifecycle` rather than being merged into it —
a half-inherited policy is one nobody can read off the file.

```yaml
defaults:
  retention_count: 12
tables:
  - { table_name: events, partition_column: created_at, granularity: month }   # keeps 12
  - { table_name: audit, partition_column: at, granularity: day, retention_count: 400 }
```

## Hooks

`hooks` names a command to run at each lifecycle moment — see
[Commands around the lifecycle](../guide/hooks-in-config.md). They fire during `apply`
only, and the CLI refuses a document declaring them unless `--allow-hooks` is passed.

## Building the wiring from it

`PartitionToolkit.from_options(engine, document.runtime, hooks=[...])` is
`from_engine` with the document's `runtime` section as its keywords, the codec name
resolved to a codec. `document.runtime.to_kwargs()` is those keywords on their own, for
code that builds the parts itself.

## One table at a time

`document.config_for("public.events")` returns one table's configuration by the name
PostgreSQL knows it as, and raises `KeyError` naming the tables the document does describe
— which is what a `--table` flag needs.
