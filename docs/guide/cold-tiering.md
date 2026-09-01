# Tier cold data to a foreign server

Old windows rarely need fast local storage. Two ways to move them off it without losing
the single table the application queries: put new leaves on a cheaper tablespace, or make
them foreign tables on a server that stores them elsewhere — a column store, an archive
database. Both are settings of [`config.leaves`](../concepts/leaves.md).

## Tablespaces

```python
config = TablePartitionConfig(
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.DAY,
    create_ahead_count=3,
    retention_count=30,
    leaves=LocalLeaves(tablespace="nvme", storage_parameters={"fillfactor": 90}, inherit_privileges=True),
)
```

Every new day is created on `nvme` with the parent's owner and grants. Moving a partition
to another tablespace later is `ALTER TABLE … SET TABLESPACE` — a hook (`after_create`,
or a `before_detach` that instead moves it) is where that goes; the library does not
rewrite storage.

## Foreign leaves

```python
config = TablePartitionConfig(
    table_name="metrics",
    scheme=RangePartitioning(key="ts", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)),
    lifecycle=LifecyclePolicy(creation=CreateAhead(count=2), retention=KeepNewest(count=24)),
    leaves=ForeignLeaves(server="clickhouse", options={"table_name": "{relname}"}),
)
```

Every month is created as

```sql
CREATE FOREIGN TABLE metrics__2026_09 (ts timestamp with time zone NOT NULL, v double precision)
    SERVER clickhouse OPTIONS (table_name 'metrics__2026_09');
ALTER TABLE metrics ATTACH PARTITION metrics__2026_09 FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
```

and queried through `metrics` like any other partition. An expired month is detached
(with its `COMMENT ON FOREIGN TABLE` marker) and dropped with `DROP FOREIGN TABLE`, which
removes the mapping and leaves the remote data where it is. This is the `pg_clickhouse`
shape; `postgres_fdw` to an archive database works the same way.

### Before the first plan

- The foreign server and a user mapping for the maintenance role exist (`CREATE SERVER`,
  `CREATE USER MAPPING`). The library never creates them.
- The remote tables exist, or the wrapper creates them — `postgres_fdw` checks at query
  time, not at `CREATE FOREIGN TABLE`. A hook on `before_create` is a good place to create
  the remote table from `partition.name`.
- The parent has **no unique index or primary key**. PostgreSQL refuses a foreign partition
  otherwise, and so does the service, before any DDL:

```text
InvalidPartitionConfigError: Invalid partition configuration: Table 'public.events' has unique
constraint(s) (id, created_at), and PostgreSQL refuses a foreign table as a partition of a
table with a unique index or primary key. Use local leaves, or drop the constraint before
enabling foreign leaves.
```

### Option templates

| Placeholder | Value |
|---|---|
| `{relname}` | the leaf's relation name |
| `{schema}` | the leaf's schema |
| `{parent}` | the relation it is attached to |
| `{root}` | the configured table |

## Hot local, cold foreign

A table where recent months are local and older ones foreign is two configurations over
time, not one: keep `LocalLeaves` for creation, and move a month to the foreign server in
a hook — export the rows, create the foreign table for the same window, detach the local
partition, attach the foreign one. The library manages what it finds: once the foreign
partition is on the grid, it is owned only under a `ForeignLeaves` configuration
(`foreign_partition` finding under a local one), and its detach and drop follow the
policy from there. `pg_clickhouse`'s `offload-partition.sql` is exactly this dance.

## Ownership, restated

- Under `ForeignLeaves`, a foreign partition on the grid is a lifecycle partition: created,
  expired, detached, dropped (mapping only).
- Under `LocalLeaves`, the same foreign partition is inspected and left alone. An archive
  someone attached behind `postgres_fdw` is never touched.
- A foreign partition never holds rows PostgreSQL moves: DEFAULT reconciliation into a
  foreign leaf inserts through the wrapper like any `INSERT` — `partition_data` drains a
  DEFAULT partition into foreign leaves this way — while `unpartition` skips foreign
  partitions and reports them, because their rows are not this database's to move.
