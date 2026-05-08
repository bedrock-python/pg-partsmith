# Lifecycle hooks

Hooks let you inject logic at every step of the partition lifecycle — export data before a
partition is dropped, publish events to Kafka after creation, archive rows before detachment.

## Hook policy

| Hook type | On exception |
|-----------|--------------|
| `before_*` | **Failure** — the maintenance operation is aborted and the error appears in `MaintenanceResult.error` |
| `after_*`  | **Warning** — logged, but `result.success` remains `True` |

## Available hook points

| Method | When it fires |
|--------|---------------|
| `before_create(config, partition_name, from_value, to_value)` | Before the partition table is created |
| `after_create(config, partition)` | After creation and optional auto-attach |
| `before_detach(table_name, partition)` | Before detaching from the parent table |
| `after_detach(table_name, partition_name)` | After successful detach |
| `before_drop(table_name, partition_name)` | Before the table is dropped — last chance to read data |
| `after_drop(table_name, partition_name)` | After the table has been permanently removed |

## Example: Kafka notifications

Subclass `BasePartitionLifecycleHooks` and override only the methods you need:

```python
from pg_partsmith.aio import BasePartitionLifecycleHooks, PartitionLifecycleService
from pg_partsmith.entities import PartitionInfo, TablePartitionConfig


class KafkaNotifyHooks(BasePartitionLifecycleHooks):
    def __init__(self, producer: KafkaProducer) -> None:
        self._producer = producer

    async def after_create(
        self, config: TablePartitionConfig, partition: PartitionInfo
    ) -> None:
        await self._producer.send("partition.created", {"name": partition.name})

    async def before_drop(self, table_name: str, partition_name: str) -> None:
        await export_to_cold_storage(table_name, partition_name)
        await self._producer.send("partition.expiring", {"name": partition_name})


service = PartitionLifecycleService(
    repo=repo,
    metadata=metadata,
    locks=locks,
    period_calculator=calculator,
    hooks=[KafkaNotifyHooks(producer)],  # multiple hooks supported
)
```

## Multiple hooks

Pass a list — hooks are called in order:

```python
service = PartitionLifecycleService(
    ...,
    hooks=[
        KafkaNotifyHooks(producer),
        MetricsHooks(statsd_client),
        AuditLogHooks(db_session),
    ],
)
```

## Custom repository

Hooks are the idiomatic way to extend behaviour. For lower-level control, you can also
subclass `PostgresPartitionRepository` directly:

```python
from pg_partsmith.aio import PostgresPartitionRepository


class AuditedPartitionRepository(PostgresPartitionRepository):
    async def drop_partition(self, partition_name: str) -> None:
        await self._audit_log.record("drop", partition_name)
        await super().drop_partition(partition_name)
```
