# Extend the library

The service is assembled from four injectable parts, each behind a `@runtime_checkable`
protocol: a **repository** (the DDL), a **metadata provider** (the catalog), a **lock
manager** and **hooks**. Swap any of them, subclass the PostgreSQL implementations, or
drive the lower-level services yourself.

```python
service = PartitionLifecycleService(repo=..., metadata=..., locks=..., hooks=[...])
```

## Subclass the repository

The most common extension: audit, wrap or veto a DDL step.

```python
from pg_partsmith.aio import PostgresPartitionRepository


class AuditedRepository(PostgresPartitionRepository):
    async def drop_partition(self, partition_name: str, *, expected_oid: int | None = None) -> None:
        await self._audit.record("drop", partition_name)
        await super().drop_partition(partition_name, expected_oid=expected_oid)
```

Tuning knobs on the constructor: `ddl_timezone` (the session timezone `ATTACH` runs
under, `"UTC"` by default), `ddl_timeout_seconds`, `marker_prefix`, the drop retry
settings (`drop_lock_timeout_ms`, `drop_max_retries`, `drop_retry_delay`,
`drop_max_backoff`), and `drop_allow_unmanaged` (leave it off).

The repository protocol, for an implementation from scratch:

```python
class PartitionRepository(Protocol):
    async def create_table_like(self, template_name, table_name, partition_by, *, physical=None) -> None: ...
    async def create_foreign_table_like(self, template_name, table_name, *, server, options) -> None: ...
    async def attach_partition(self, parent_name, partition_name, bounds, *, key_arity=1) -> None: ...
    async def detach_partition(self, parent_name, partition_name, *, mode=DetachMode.AUTO) -> None: ...
    async def drop_partition(self, partition_name, *, expected_oid=None) -> None: ...
    async def adopt_partition(self, table_name, partition_name) -> bool: ...
    async def reconcile_default_rows(self, *, default_partition_name, target_partition_name, key_columns, from_value, to_value, limit=None) -> int: ...
    async def move_rows(self, source_name, target_name, *, limit=None) -> int: ...
```

Every method takes and returns plain domain objects (`PartitionBounds`, `PartitionBy`,
`DetachMode`, `LocalLeaves`), so an implementation never needs to know how the planner
works.

## Subclass the metadata provider

Override a catalog query for an unusual setup — a read replica for the reads, a cache, a
different way of listing orphans:

```python
from pg_partsmith.aio import PostgresMetadataProvider


class ReplicaMetadata(PostgresMetadataProvider):
    def __init__(self, replica_engine, **kwargs) -> None:
        super().__init__(replica_engine, **kwargs)
```

Constructor knobs: `marker_prefix` (pass the repository's), `boundary_codec` (only for
`is_partition_closed`), `ddl_timezone` (for reading naive bounds).

The protocol's reads: `get_partition_type`, `get_partition_columns`, `get_actual_tree`,
`measure`, `get_partition_tree`, `get_default_partition`, `partition_exists`,
`is_partition_attached`, `get_relation_oid`, `get_unique_constraint_columns`,
`get_key_high_water_mark`, `get_leading_key_minimum`, `list_partitions`.

## A lock manager

```python
from contextlib import asynccontextmanager

from pg_partsmith.aio.protocols import LockManager


class ZookeeperLockManager:
    def __init__(self, zk) -> None:
        self._zk = zk

    @asynccontextmanager
    async def acquire_lock(self, table_name: str):
        async with self._zk.lock(f"/partsmith/{table_name}"):
            yield

    async def is_locked(self, table_name: str) -> bool:
        return await self._zk.exists(f"/partsmith/{table_name}") is not None


assert isinstance(ZookeeperLockManager(zk), LockManager)
```

`acquire_lock` should be non-blocking and raise `LockAcquisitionError` when the lock is
taken — a tick that collides with another replica skips rather than queues. The lock
must not depend on a transaction of the caller's: the built-in PostgreSQL manager holds
its advisory lock on a dedicated autocommit connection for that reason.

## Hooks

Subclass `BasePartitionLifecycleHooks` and override what you need; the base does nothing.
See [Archive before dropping](archiving.md) for the six hook points and their semantics.

## The pieces under the service

The service is a thin façade over three components you can use directly:

| Component | Role |
|---|---|
| `PartitionInspector(metadata)` | `inspect(config, measure=…)` reads the `ActualTree` and gathers facts; `context(config, now=…, mode=…)` resolves the cursors into a `PlanningContext` |
| `plan_maintenance(config, tree, context)` | the pure planner — no I/O, testable with hand-built trees |
| `PlanExecutor(repo, metadata, hooks)` | `apply(config, plan)`; `create_partition(config, plan, op, issues=…, fill=…)` to load rows before a partition goes live; `detach_single_partition`, `drop_single_partition` for one-at-a-time control |
| `DataMover(repo, metadata, executor)` | the batched movers behind `partition_data` / `unpartition` |

A custom orchestration — plan on one connection, review, apply elsewhere — is a few
lines:

```python
from pg_partsmith import MaintenancePlan

plan = await service.plan(config)
payload = plan.model_dump(mode="json")          # ship it, store it, show it
...
result = await service.apply(config, MaintenancePlan.model_validate(payload))
```

## Testing your extension

The planner is pure: build a `PartitionNode` tree by hand and assert on the plan without a
database. For anything that talks to PostgreSQL, the library's own integration suite runs
against `testcontainers`; a `postgres:17-alpine` container starts in a few seconds and is
the cheapest way to be sure about DDL.

```python
from pg_partsmith import ActualTree, PartitionNode, PartitionType, PlanningContext, RangeBounds, plan_maintenance

root = PartitionNode(name="public.events", partition_type=PartitionType.RANGE, partition_columns=("created_at",),
                     children=(PartitionNode(name="public.events__2026_08", parent_name="public.events", level=1,
                                             bounds=RangeBounds(from_value="2026-08-01", to_value="2026-09-01")),))
plan = plan_maintenance(config, ActualTree(root=root), PlanningContext(now=datetime(2026, 8, 28, tzinfo=UTC)))
```

## Both mirrors

`pg_partsmith.sync` is generated from `pg_partsmith.aio`; a subclass of a sync class
looks exactly like its async twin without the `async` / `await`. Protocols are per
mirror (`pg_partsmith.sync.protocols`).
