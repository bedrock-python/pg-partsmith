# Configuration examples

Every file under [`examples/`](https://github.com/bedrock-python/pg-partsmith/tree/master/examples)
in the repository is validated by the test suite, so what is there is what the current
release reads. Copy one and edit it; `pg-partsmith validate` tells you the moment it
stops being right.

| File | What it shows |
|---|---|
| [`partitions.minimal.yaml`](https://github.com/bedrock-python/pg-partsmith/blob/master/examples/partitions.minimal.yaml) | one monthly table, everything else default |
| [`partitions.yaml`](https://github.com/bedrock-python/pg-partsmith/blob/master/examples/partitions.yaml) | the one to copy from: `defaults`, a flat table, a nested `RANGE → HASH` scheme with a composed lifecycle, a `LIST` root, leaves, `runtime`, and hooks of every kind |
| [`partitions.queue.yaml`](https://github.com/bedrock-python/pg-partsmith/blob/master/examples/partitions.queue.yaml) | an id-partitioned queue: integer windows, the cursor off the sequence, `keep_behind` |
| [`partitions.cold-tiering.yaml`](https://github.com/bedrock-python/pg-partsmith/blob/master/examples/partitions.cold-tiering.yaml) | foreign leaves on an archive server |
| [`partitions.schema.json`](https://github.com/bedrock-python/pg-partsmith/blob/master/examples/partitions.schema.json) | the document's JSON Schema, for an editor |
| [`hooks/archive-partition.sh`](https://github.com/bedrock-python/pg-partsmith/blob/master/examples/hooks/archive-partition.sh) | a `before_drop` command: `pg_dump` the partition, refuse the drop if that fails |
| [`hooks/notify.sh`](https://github.com/bedrock-python/pg-partsmith/blob/master/examples/hooks/notify.sh) | an `after_create` command: one line to a webhook |
| [`hooks/export_partition.py`](https://github.com/bedrock-python/pg-partsmith/blob/master/examples/hooks/export_partition.py) | a `before_detach` block of Python from a file: `COPY` the rows out, write a manifest |

## The shape of a document

```yaml
version: 1

defaults:            # every table starts from these; a table naming a key owns it
  schema: public
  granularity: month
  create_ahead_count: 3
  retention_count: 12

tables:
  - table_name: events                 # the flat spelling, defaults filled in
    partition_column: created_at

  - table_name: telemetry              # any other topology is a scheme
    scheme:
      method: range
      key: id
      boundaries: { kind: time, granularity: week, codec: uuidv7 }
      child: { method: hash, key: tenant_id, modulus: 8 }
    lifecycle:
      creation: { kind: create_ahead, count: 4 }
      retention:
        kind: all_of
        members:
          - { kind: keep_for, age: P90D }
          - { kind: sql, sql: "SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')" }
      detach: concurrent
      drop: { kind: drop_after, grace: P7D, when: { kind: unreferenced } }
    leaves: { kind: local, tablespace: fast_ssd, storage_parameters: { fillfactor: 70 } }

runtime:             # one key per keyword of PartitionToolkit.from_engine
  ddl_timezone: UTC
  marker_prefix: acme

hooks:               # honoured only under `apply --allow-hooks`
  timeout_seconds: 900
  before_drop: ["/opt/hooks/archive-partition.sh"]
  before_detach: { python_file: hooks/export_partition.py }
  after_drop:
    python: |
      log.info("dropped %s (%s)", event.partition.name, event.operation.reason)
```

The vocabulary is the library's own: every `kind` and `method` is the discriminator the
pydantic models dump, so `TablePartitionConfig.model_dump(mode="json", by_alias=True)` is
also a valid table entry. The full field list is on
[the configuration document](../reference/document.md) and
[configuration fields](../reference/configuration.md).

## Editor support

```bash
pg-partsmith schema > partitions.schema.json
```

Then, at the top of the document:

```yaml
# yaml-language-server: $schema=partitions.schema.json
```

VS Code's YAML extension, and anything else speaking the YAML language server, validates
keys and offers completion from it. The committed copy under `examples/` is checked
against the generated one by the test suite, so it is never stale.

## What `validate` catches, and what it does not

`validate` parses the document, compiles every hook block (file-backed ones included),
connects, and checks each table against the catalog: partitioned at all, by the method
and on the key the scheme claims. It does not know whether a `SqlPredicate` is sensible or
whether an archive script works — `plan --output json` shows what the policy decided, and
a hook's own tests are the hook's own business.

## Hooks as scripts

The two shell scripts and the Python file are meant to be copied and edited; each says at
the top what it needs on `PATH` and in the environment. How a script reads the event, and
what it is allowed to do with it, is on
[Commands around the lifecycle](hooks-in-config.md#writing-the-script).
