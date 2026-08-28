# Leaf backends

A **leaf** is the relation that stores rows: the deepest member of every branch. The scheme
decides how many leaves exist and what each owns; `config.leaves` decides what kind of
relation each one is.

```python
from pg_partsmith import ForeignLeaves, LocalLeaves

TablePartitionConfig(..., leaves=LocalLeaves())                              # the default
TablePartitionConfig(..., leaves=LocalLeaves(tablespace="fast_ssd", storage_parameters={"fillfactor": 70}))
TablePartitionConfig(..., leaves=ForeignLeaves(server="archive", options={"table_name": "{relname}"}))
```

Branches — partitions that partition further — are always local tables: PostgreSQL has no
foreign partitioned tables.

## Local leaves

`LocalLeaves()` is what every configuration gets: ordinary tables created
`LIKE parent INCLUDING ALL EXCLUDING IDENTITY`. Three things `LIKE` does not carry can be
declared:

| Field | Effect |
|---|---|
| `tablespace` | `TABLESPACE` on every created relation, leaves and branches alike. PostgreSQL refuses `pg_default` here; name a real tablespace. |
| `storage_parameters` | `WITH (fillfactor = 70, autovacuum_enabled = false, toast.autovacuum_enabled = true)` on every created **leaf**. Branches take none — PostgreSQL refuses storage parameters on a partitioned table. Values of any type are rendered as string literals, which PostgreSQL accepts for every parameter. |
| `inherit_privileges` | The parent's owner and grants are replayed onto every created relation, in the transaction that created it. `LIKE` copies neither; a role that reads *through the parent* needs no grant on a leaf, but one that addresses leaves directly (an export job, `pg_dump` of one partition) does. A grant the maintenance role may not make rolls the creation back — nothing half-configured is left behind. |

```python
leaves=LocalLeaves(tablespace="fast_ssd", storage_parameters={"fillfactor": 70}, inherit_privileges=True)
```

The classic `pg_partman` template table carried exactly these: `LocalLeaves` is the
declarative form.

## Foreign leaves

`ForeignLeaves(server, options)` creates every leaf as
`CREATE FOREIGN TABLE … (columns of the parent) SERVER server OPTIONS (…)` and attaches it
like any other partition. The rows of a window then live wherever the foreign server keeps
them — a column store, an archive database — while the table is still queried through one
parent. This is the `pg_clickhouse` shape.

```python
leaves=ForeignLeaves(server="clickhouse", options={"table_name": "{relname}", "engine": "MergeTree"})
```

Option values are templates. What they may refer to:

| Placeholder | Value |
|---|---|
| `{relname}` | the leaf's own relation name (`events__2026_08`) |
| `{schema}` | the leaf's schema |
| `{parent}` | the relation it is attached to |
| `{root}` | the table the configuration is for |

A template with any other placeholder is refused at construction. Literal values pass
through.

The server and its user mapping exist before the first plan; pg-partsmith never creates
them. Whether the remote table exists is the foreign data wrapper's business — `postgres_fdw`
checks at query time, not at creation.

### PostgreSQL's rule

A foreign table can be a partition only of a parent **without a unique index or primary
key**: PostgreSQL cannot enforce uniqueness across a foreign relation, and refuses both
`CREATE FOREIGN TABLE … PARTITION OF` and `ATTACH PARTITION` (`42809`) when one exists.
Non-unique indexes are fine (they are skipped for the foreign member). The service checks
this against the catalog and refuses the configuration before any DDL, naming the
constraints in the way.

Measured on PostgreSQL 15 and 17 — see [PostgreSQL semantics](../design/postgresql-semantics.md#foreign-tables).

### Ownership

Under a `ForeignLeaves` configuration a foreign partition is a lifecycle partition like any
other: it is created ahead, expires, is detached with its `COMMENT ON FOREIGN TABLE` marker
and dropped with `DROP FOREIGN TABLE` — which removes the mapping and leaves the remote data
alone. Grace periods and `DropNever` apply unchanged.

Under a `LocalLeaves` configuration the very same foreign partition is *not* ours: it is
inspected, reported as `foreign_partition` (INFO) and never created, detached or dropped.
An archive someone attached behind `postgres_fdw` stays where it is.

## In a nested scheme

Only the deepest level's members are leaves. In `RANGE(month) → HASH(tenant)` the month is
a local partitioned branch and its buckets are the leaves — foreign tables under
`ForeignLeaves`, tables `WITH (…)` under `LocalLeaves`. The branch itself takes the
tablespace but no storage parameters.

## Serialization

`leaves` is discriminated on `kind` (`"local"` / `"foreign"`) and round-trips through
`config.model_dump(mode="json")` like the rest of the configuration.
