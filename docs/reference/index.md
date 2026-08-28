# API Reference

Auto-generated from source using [mkdocstrings](https://mkdocstrings.github.io/).

## pg_partsmith

Top-level public API: entities, enums, exceptions, and period calculators.

### Entities

::: pg_partsmith.entities.Period
    options:
      heading_level: 4

::: pg_partsmith.entities.PartitionInfo
    options:
      heading_level: 4

::: pg_partsmith.entities.TablePartitionConfig
    options:
      heading_level: 4

::: pg_partsmith.entities.MaintenanceResult
    options:
      heading_level: 4

::: pg_partsmith.entities.MaintenanceIssue
    options:
      heading_level: 4

### Partition topology

Bounds, subpartition specs, and the introspected tree.

`PartitionBounds` is the discriminated union of every bound below
(`RangeBounds | ListBounds | HashBounds | DefaultBounds`); `SubpartitionBounds`
is the narrower one a subpartition can be attached with
(`HashBounds | ListBounds | DefaultBounds`), which excludes RANGE because that
belongs to the time dimension at the root.

::: pg_partsmith.topology.HashSubpartitionSpec
    options:
      heading_level: 4

::: pg_partsmith.topology.ListSubpartitionSpec
    options:
      heading_level: 4

::: pg_partsmith.topology.ListGroup
    options:
      heading_level: 4

::: pg_partsmith.topology.SubpartitionSpecBase
    options:
      heading_level: 4

::: pg_partsmith.topology.PartitionNode
    options:
      heading_level: 4

::: pg_partsmith.topology.RangeBounds
    options:
      heading_level: 4

::: pg_partsmith.topology.HashBounds
    options:
      heading_level: 4

::: pg_partsmith.topology.ListBounds
    options:
      heading_level: 4

::: pg_partsmith.topology.DefaultBounds
    options:
      heading_level: 4

::: pg_partsmith.topology
    options:
      heading_level: 4
      members:
        - uniform_modulus
        - hash_keyspace_covered
        - missing_remainders

### Subpartition reconciliation

::: pg_partsmith.subpartition_plan.SubpartitionPlan
    options:
      heading_level: 4

::: pg_partsmith.subpartition_plan.SubpartitionAction
    options:
      heading_level: 4

::: pg_partsmith.subpartition_plan.SubpartitionReconcileResult
    options:
      heading_level: 4

::: pg_partsmith.subpartition_plan.TopologyFinding
    options:
      heading_level: 4

::: pg_partsmith.subpartition_plan.TopologyReason
    options:
      heading_level: 4

::: pg_partsmith.subpartition_plan
    options:
      heading_level: 4
      members:
        - plan_subpartitions
        - plan_new_subtree

### Boundary codecs

::: pg_partsmith.boundaries.RangeBoundaryCodec
    options:
      heading_level: 4

::: pg_partsmith.boundaries.UUIDv7BoundaryCodec
    options:
      heading_level: 4

::: pg_partsmith.protocols.BoundaryDecoder
    options:
      heading_level: 4

### Exceptions

::: pg_partsmith.exceptions
    options:
      heading_level: 4
      members:
        - PartitionError
        - PartitionAlreadyExistsError
        - PartitionNotFoundError
        - PartitionAttachedError
        - PartitionDetachInProgressError
        - InvalidPartitionConfigError
        - LockAcquisitionError
        - DropRetryExhaustedError
        - UnmanagedPartitionDropError
        - PartitionTopologyError
        - UnsupportedCapabilityError

### Period strategies

::: pg_partsmith.strategies.base.BasePeriodCalculator
    options:
      heading_level: 4

::: pg_partsmith.strategies.hour.HourPeriodCalculator
    options:
      heading_level: 4

::: pg_partsmith.strategies.day.DayPeriodCalculator
    options:
      heading_level: 4

::: pg_partsmith.strategies.week.WeekPeriodCalculator
    options:
      heading_level: 4

::: pg_partsmith.strategies.month.MonthPeriodCalculator
    options:
      heading_level: 4

::: pg_partsmith.strategies.quarter.QuarterPeriodCalculator
    options:
      heading_level: 4

::: pg_partsmith.strategies.year.YearPeriodCalculator
    options:
      heading_level: 4

::: pg_partsmith.strategies.selector.get_period_calculator
    options:
      heading_level: 4

---

## pg_partsmith.aio

Async implementations: service, maintainer, repositories, lock managers, and hooks.

### Service and maintainer

::: pg_partsmith.aio.service.PartitionLifecycleService
    options:
      heading_level: 4

::: pg_partsmith.aio.maintainer.PartitionMaintainer
    options:
      heading_level: 4
      members:
        - run_maintenance
        - run_maintenance_safe

### Protocols

Implement these to swap in your own storage or locking. The flat pair is all a
single-column, unnested config needs; the rest are opt-in and only required when a
config actually asks for what they add.

::: pg_partsmith.aio.protocols.PartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.aio.protocols.PartitionMetadataProvider
    options:
      heading_level: 4

::: pg_partsmith.aio.protocols.SubpartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.aio.protocols.NestedPartitionMetadata
    options:
      heading_level: 4

::: pg_partsmith.aio.protocols.CompositeKeyRepository
    options:
      heading_level: 4

::: pg_partsmith.aio.protocols.CompositeKeyMetadata
    options:
      heading_level: 4

::: pg_partsmith.aio.protocols.LockManager
    options:
      heading_level: 4
### PostgreSQL implementations

::: pg_partsmith.aio.repositories.repository.PostgresPartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.aio.metadata.PostgresMetadataProvider
    options:
      heading_level: 4

### Lock managers

::: pg_partsmith.aio.lock.postgres.PostgresAdvisoryLockManager
    options:
      heading_level: 4

::: pg_partsmith.aio.lock.redis.RedisDistributedLockManager
    options:
      heading_level: 4

### Hooks

::: pg_partsmith.aio.hooks.BasePartitionLifecycleHooks
    options:
      heading_level: 4

---

## pg_partsmith.sync

Synchronous mirror of `pg_partsmith.aio`: same class names and API, plain methods
built on the sync SQLAlchemy `Engine`.

### Service and maintainer

::: pg_partsmith.sync.service.PartitionLifecycleService
    options:
      heading_level: 4

::: pg_partsmith.sync.maintainer.PartitionMaintainer
    options:
      heading_level: 4
      members:
        - run_maintenance
        - run_maintenance_safe

### Protocols

Implement these to swap in your own storage or locking. The flat pair is all a
single-column, unnested config needs; the rest are opt-in and only required when a
config actually asks for what they add.

::: pg_partsmith.sync.protocols.PartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.sync.protocols.PartitionMetadataProvider
    options:
      heading_level: 4

::: pg_partsmith.sync.protocols.SubpartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.sync.protocols.NestedPartitionMetadata
    options:
      heading_level: 4

::: pg_partsmith.sync.protocols.CompositeKeyRepository
    options:
      heading_level: 4

::: pg_partsmith.sync.protocols.CompositeKeyMetadata
    options:
      heading_level: 4

::: pg_partsmith.sync.protocols.LockManager
    options:
      heading_level: 4
### PostgreSQL implementations

::: pg_partsmith.sync.repositories.repository.PostgresPartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.sync.metadata.PostgresMetadataProvider
    options:
      heading_level: 4

### Lock managers

::: pg_partsmith.sync.lock.postgres.PostgresAdvisoryLockManager
    options:
      heading_level: 4

::: pg_partsmith.sync.lock.redis.RedisDistributedLockManager
    options:
      heading_level: 4

### Hooks

::: pg_partsmith.sync.hooks.BasePartitionLifecycleHooks
    options:
      heading_level: 4

---

## pg_partsmith.settings

Env-driven configuration via [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).
Requires the `pydantic-settings` extra: `pip install pg-partsmith[pydantic-settings]`.

### Settings

::: pg_partsmith.settings.PartitionTableSettings
    options:
      heading_level: 4
