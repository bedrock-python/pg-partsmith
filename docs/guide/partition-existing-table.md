# Partition an existing table

You have `events`, a plain table with a lot of rows, and you want it partitioned by month
without stopping the application. PostgreSQL has no `ALTER TABLE … PARTITION BY`; the
path is the one `pg_partman` popularised: make the old table the DEFAULT partition of a
new parent, then drain it window by window. pg-partsmith does the draining in bounded
batches.

## 1. Swap the tables

```sql
BEGIN;
ALTER TABLE events RENAME TO events_legacy;
CREATE TABLE events (LIKE events_legacy INCLUDING ALL) PARTITION BY RANGE (created_at);
ALTER TABLE events ATTACH PARTITION events_legacy DEFAULT;
COMMIT;
```

Every row is visible through `events` again as soon as this commits. Writes route to the
DEFAULT partition until monthly partitions exist.

!!! note "Constraints"
    The new parent's primary key must contain the partition key: `PRIMARY KEY (id,
    created_at)`. If the old table's key was `(id)` alone, change it on `events_legacy`
    before the swap, or `ATTACH` fails with `unique constraint … must include all
    partitioning columns`. Sequences, indexes and constraints come across with `LIKE …
    INCLUDING ALL`; foreign keys *to* the old table are best recreated against the new
    parent **after the drain**: rows that are already referenced cannot be moved at all
    (the movers refuse them row-safe), and a `CASCADE`, `SET NULL` or `SET DEFAULT`
    action is refused up front because it would fire on every batch. See
    [Row moves and ON DELETE actions](foreign-keys.md#row-moves-and-on-delete-actions).

## 2. Configure, plan, run the tick

```python
config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,
    retention_count=24,
)

print((await service.plan(config)).describe())
await maintainer.run_maintenance_safe(config)
```

The tick creates the current month and the two after it. As each is attached, the rows
of that month move out of `events_legacy` into it — ordinary DEFAULT reconciliation.
From now on new rows land in real partitions; the old ones are still in the DEFAULT.

## 3. Drain the DEFAULT partition

```python
while not (result := await service.partition_data(config, batch_rows=50_000, max_batches=200)).complete:
    log.info("moved %d rows in %d batches; created %s", result.rows_moved, result.batches, result.partitions)
```

Each call:

1. finds the **oldest window** with rows still in DEFAULT;
2. creates its partition **detached**, subtree included, through the same path a scheduled
   creation takes;
3. moves the window's rows into it in batches of `batch_rows` — one
   `DELETE … RETURNING` / `INSERT` per batch, each committing on its own, so a row is in
   exactly one place at every commit point;
4. attaches the partition once nothing of that window is left in DEFAULT;
5. moves on to the next window, until DEFAULT holds only rows no window can take (rows
   with a NULL key), or `max_batches` is spent.

`result.complete` says whether DEFAULT is drained; `result.partitions` lists what was
created; `result.issues` explains anything that could not be handled. A call that runs out
of budget mid-window leaves that partition detached and filled so far; the next call
finds it, finishes it and attaches it.

!!! warning "What a batch cannot hide"
    While a window's rows are being moved, they sit in a partition that is not yet
    attached and are **invisible through the parent**. PostgreSQL leaves no other order —
    a partition cannot be attached while DEFAULT still holds rows for it. Run the drain in
    a maintenance window, or with small batches during a quiet hour and readers that can
    tolerate a month's rows appearing a little later. Rows already in real partitions, and
    rows still in DEFAULT for other windows, stay visible throughout.

`partition_data` takes the table's lock, so it does not race the scheduled tick. It
refuses a window it cannot create (an unmanaged partition overlaps it), and any move an
incoming foreign key's `ON DELETE` action would corrupt, with a `move` issue and
`complete=False` rather than loop. A window whose partition already exists *detached*
with this library's marker — retention retired it, and late rows for it landed in
DEFAULT — is filled and re-attached rather than given up on.

## 4. Afterwards

Once `events_legacy` is empty it is still the DEFAULT partition and still catches rows
with a NULL key. Keep it — an empty DEFAULT costs nothing and PostgreSQL will scan it on
every attach — or detach and drop it by hand if the key is `NOT NULL`:

```sql
ALTER TABLE events DETACH PARTITION events_legacy;
DROP TABLE events_legacy;
```

Without a DEFAULT partition, `DETACH … CONCURRENTLY` becomes available, which is what
`DetachMode.AUTO` prefers.

## The way back

`unpartition` empties every partition into one plain table, oldest first, in the same
batches, and optionally drops each emptied partition through the ordinary path — marker,
hooks, revalidation:

```python
result = await service.unpartition(config, "public.events_flat", batch_rows=50_000, drop_emptied=True)
```

`events_flat` is created `LIKE` the root when it does not exist; it must be a plain
table that is not a partition of *anything* — the root itself, one of its partitions or
detached partitions, and a partition of any other table are all refused (rows moved
"into" the root would route straight back to where they came from). Rows the drop's own
drain moves are counted in `rows_moved`, and a destination with identity columns has its
sequences advanced past the moved ids, so its next ordinary insert just works. With `drop_emptied` a partition is detached once its last batch comes up
short, the rows that arrived in the meantime are moved, and the drop moves whatever is
left **in the same transaction, under the drop's own lock** — a row committed between the
last batch and the drop ends in `events_flat`, never in the dropped table. Detached
partitions this library owns (orphans waiting out a grace) are emptied too; under
`DropNever` they belong to another process and are reported instead. Foreign partitions
are skipped and reported: their rows are not this database's to move. Rows already moved
are in `events_flat` at every commit point, never in two places.

## Nested schemes

Both movers work unchanged for a `RANGE → HASH` tree: `partition_data` builds each month
with its buckets before filling it, and the rows route into the buckets as they are
inserted; `unpartition` empties a month through its branch.
