# Backfill partitions for existing data

Create-ahead walks *forward* from the cursor. Rows that already exist in older windows —
imported history, a table that was partitioned around its data — need partitions that
create-ahead will never reach. `ensure_partitions` takes the windows from you.

## Name the windows

```python
calculator = config.scheme.time_boundaries.period_calculator
current = calculator.current_period()
past = [calculator.period_before(current, n) for n in reversed(range(1, 13))]   # the twelve months before this one

created = await service.ensure_partitions(config, past)
```

`ensure_partitions` accepts periods on a time axis, `Window` objects on any axis, or plain
positions on the root's axis — an instant, an integer key value, a sliding-list value:

```python
from datetime import UTC, datetime

await service.ensure_partition(config, datetime(2025, 3, 10, tzinfo=UTC))   # the window holding that instant
await service.ensure_partition(queue_config, 1_250_000)                       # the window holding that id
await service.ensure_partition(builds_config, 250)                            # value 250 of a sliding list
```

It is idempotent: one catalog read for the whole batch, every window built with its
complete subtree before it is attached, windows that already have a partition skipped.
The return value lists the partitions this call created, in order; `ensure_partition`
returns one or `None`.

## Rows already in a DEFAULT partition

If the history sits in the parent's DEFAULT partition, `ensure_partitions` moves each
window's rows into the new partition as it is attached — that is the ordinary DEFAULT
reconciliation, in one statement per window. For a large DEFAULT partition prefer
[`partition_data`](partition-existing-table.md), which moves rows in bounded batches and
creates the partitions as it goes.

## Only what retention keeps

Backfill the windows the policy would keep. A partition outside the retention window is
detached and dropped by the next tick — creating it first is wasted DDL.

## Takes no lock

`ensure_partitions` does not take the table's lock, so it can run from a migration script
while the scheduled tick is off. To run it next to a live maintainer, hold the lock
yourself:

```python
async with locks.acquire_lock(config.qualified_name):
    await service.ensure_partitions(config, past)
```
