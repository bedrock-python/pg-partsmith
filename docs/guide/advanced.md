# Advanced

## Multi-schema databases

Set `schema` in `TablePartitionConfig` to make all operations schema-qualified. This
prevents cross-schema ambiguity and makes behaviour independent of `search_path`.

```python
config = TablePartitionConfig(schema="analytics", table_name="events", ...)
```

Set `marker_prefix` consistently across `PostgresPartitionRepository` and
`PostgresMetadataProvider` when working with multiple deployments in one database:

```python
repo = PostgresPartitionRepository(engine, marker_prefix="myapp")
metadata = PostgresMetadataProvider(engine, marker_prefix="myapp")
```

## Orphan partitions and the ownership marker

When a partition is detached, the repository writes a `COMMENT` on the detached table
*before* the `DETACH`:

```text
pg-partsmith:orphan-parent=public.events
pg-partsmith:detached-at=2026-08-28T10:00:00+00:00
<any comment the table already carried>
```

Only marker-tagged tables are ever dropped, which makes cleanup safe even if the database
contains similarly named tables not managed by pg-partsmith. The instant on the second line
is what a grace period is measured from; a marker written by an older version, or by
`repo.adopt_partition(...)`, has none and counts as past its grace.

The marker survives `pg_dump`/restore (comments are dumped by default), so a restored copy
of a marked table is again eligible for dropping. When repurposing such a table, clear its
comment (`COMMENT ON TABLE ... IS NULL`) or restore with `--no-comments`.

## DEFAULT partition reconciliation

When attaching a RANGE partition, if the parent's DEFAULT partition contains rows belonging
to the new window, pg-partsmith:

1. detects the conflict (`23514`),
2. moves the conflicting rows from DEFAULT into the new partition (naming columns on both
   sides, honouring `IS NOT NULL` on trailing key columns),
3. retries the `ATTACH PARTITION`.

If the attach still fails, the rows are returned to DEFAULT (best effort) rather than left in
a table no query can see. For a nested branch the moved rows are routed onward into its
leaves by PostgreSQL; a DEFAULT sibling holding rows for a hash or list member is reported
(`default_holds_rows`) rather than moved, because only a RANGE window can be selected by
its key.

## Timezone semantics

Three layers stay aligned through one knob:

1. **Period computation** — `TimeBoundaries(tz=...)`: "now" for the current period and the
   meaning of naive boundary literals.
2. **DDL** — for `TIMESTAMP WITH TIME ZONE` keys PostgreSQL interprets naive literals in
   the session `TimeZone`, so `PostgresPartitionRepository` runs `SET LOCAL TIME ZONE
   '<ddl_timezone>'` in the same transaction as `ATTACH PARTITION` and DEFAULT reconciliation
   (`ddl_timezone="UTC"` by default).
3. **Planning** — naive boundaries read back from the catalog are interpreted in the
   calendar's timezone before being compared as UTC instants.

```python
from zoneinfo import ZoneInfo

config = TablePartitionConfig(..., granularity=PartitionGranularity.MONTH, tz="Europe/Moscow")
repo = PostgresPartitionRepository(engine, ddl_timezone="Europe/Moscow")
```

`events__2024_01` then means January in Moscow time and its real bounds are Moscow
midnights. The service **refuses a mismatched pair** at plan time. `ddl_timezone=None`
trusts the session timezone and logs a warning with a non-UTC calendar.

Caveats: hourly granularity is UTC-only; zones whose midnight can be skipped by DST are
resolved by PostgreSQL under `SET LOCAL TIME ZONE`; existing partitions are never
reinterpreted.

## Safe drops

`drop_partition()` refuses any table that is not a marker-tagged orphan
(`UnmanagedPartitionDropError`), any table that is still attached (`PartitionAttachedError`,
skipped with a warning by the executor), and any table whose OID differs from the one the
plan decided about (`PlanStaleError`). The checks run under `ACCESS EXCLUSIVE` in the same
transaction as `DROP TABLE`, closing the window where a concurrently reattached or replaced
relation could be dropped. Foreign keys on the partition are dropped first.

To override the marker check (not recommended):

```python
repo = PostgresPartitionRepository(engine, drop_allow_unmanaged=True)
```

Legacy tables detached by a previous partitioner are better handled with
`repo.adopt_partition(table_name, partition_name)`.

## Scheduler integration

`maintain_partitions` is a plain async function ready for any scheduler:

```python
from pg_partsmith.aio import maintain_partitions

scheduler.add_job(maintain_partitions, "cron", hour=2, kwargs={"maintainer": maintainer, "config": config})
```

It always returns `MaintenanceResult` — it never raises, including on
`asyncio.CancelledError`. Run it as often as you like: a converged table costs one catalog
read and zero DDL. pg-partsmith is not a scheduler; cron, Celery, APScheduler, a Kubernetes
CronJob or an application start-up hook are all fine, and replicas that lose the lock skip
the tick (`LockAcquisitionError`).

## Cancellation semantics

| Method | On exception |
|--------|-------------|
| `PartitionMaintainer.run_maintenance()` | logs and re-raises (including `CancelledError`) |
| `PartitionMaintainer.run_maintenance_safe()` | always returns `MaintenanceResult`; cancellation reported via `result.error` |
| `maintain_partitions()` | same as `run_maintenance_safe()` |

A cancellation that lands mid-run leaves at most a detached, unattached table (attach is
last) or a marked, half-detached partition; the next run converges both.

## Query pruning

A good partition lifecycle does not make queries fast by itself. PostgreSQL prunes
partitions only when the query constrains the **partition key** of every level it should
skip:

```sql
WHERE created_at >= '2026-08-01' AND created_at < '2026-09-01'   -- prunes RANGE(created_at)
  AND tenant_id = 42                                             -- prunes HASH(tenant_id)
```

For an encoded key (UUIDv7), translate the time filter with the codec:
`WHERE id >= :lower AND id < :upper` with `UUIDv7BoundaryCodec().min_uuid_for(...)`.
That is an application concern; pg-partsmith never rewrites queries.

## Foreign partitions

A `pg_partition_tree` may contain foreign tables (`pg_clickhouse`, `postgres_fdw`). They are
introspected with `relkind == RelationKind.FOREIGN`, reported as `foreign_partition`, and
never created, detached or dropped by the library — `DROP TABLE` cannot even remove one.
Creating foreign leaves is a planned extension point, not a 1.0 feature.
