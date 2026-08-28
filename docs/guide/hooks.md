# Lifecycle hooks

Hooks let you inject logic at every step of the partition lifecycle — export data before a
partition is dropped, publish events after creation, verify an archive before the drop.

Hooks fire once per **lifecycle unit** — the partition directly under the root — never per
leaf of its subtree: a cold-storage export wants the whole week, not one call per hash
bucket. For a root `HASH` or `LIST` table they fire per member.

## Hook policy

| Hook type | On exception |
|-----------|--------------|
| `before_*` | the operation is aborted; with `continue_on_error` the error lands in `MaintenanceResult.issues`, otherwise it propagates. A detach or drop refused this way comes back on the next run. |
| `after_*`  | logged; the operation already happened |

## Hook points

| Method | When it fires |
|--------|---------------|
| `before_create(config, partition: PartitionInfo)` | before the partition (name, bounds, `subpartition_type`) is created |
| `after_create(config, partition)` | after it is created, its subtree built, and attached |
| `before_detach(table_name, partition: PartitionInfo)` | before detaching — the data is still reachable through the parent |
| `after_detach(table_name, partition_name)` | after a successful detach |
| `before_drop(table_name, partition_name)` | before the table is dropped — the last chance to read the data |
| `after_drop(table_name, partition_name)` | after the table is gone |

`partition.from_value` / `partition.to_value` carry a RANGE window; `partition.bounds` the
structured form for every method.

## Example: archive, verify, then drop

```python
from pg_partsmith.aio import BasePartitionLifecycleHooks, PartitionLifecycleService


class ArchiveHooks(BasePartitionLifecycleHooks):
    def __init__(self, archive: Archive) -> None:
        self._archive = archive

    async def after_detach(self, table_name: str, partition_name: str) -> None:
        await self._archive.export(partition_name)

    async def before_drop(self, table_name: str, partition_name: str) -> None:
        if not await self._archive.verified(partition_name):
            raise RuntimeError(f"{partition_name} is not archived yet")   # dropped on a later run


service = PartitionLifecycleService(repo, metadata, locks, hooks=[ArchiveHooks(archive)])
```

Combine with `DropAfter(grace=...)` so the export has a window, or with `DropNever` when
the archive pipeline owns the drop entirely.

## Multiple hooks

Pass a list — hooks are called in order:

```python
hooks=[KafkaNotifyHooks(producer), MetricsHooks(statsd), AuditLogHooks(session)]
```

## What hooks are not for

A hook decides nothing about *which* partitions are created or expire — that is the
[lifecycle policy](lifecycle-policies.md), evaluated by the planner, with the plan
inspectable before anything runs. Keeping DDL out of hooks is what preserves the ownership,
safety and locking guarantees. For lower-level control subclass the repository:

```python
class AuditedPartitionRepository(PostgresPartitionRepository):
    async def drop_partition(self, partition_name: str, *, expected_oid: int | None = None) -> None:
        await self._audit_log.record("drop", partition_name)
        await super().drop_partition(partition_name, expected_oid=expected_oid)
```
