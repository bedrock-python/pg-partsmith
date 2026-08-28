# API Reference

Auto-generated from source using [mkdocstrings](https://mkdocstrings.github.io/).

## pg_partsmith

Top-level public API: configuration, schemes, boundaries, lifecycle policies, the plan,
entities, exceptions and period calculators.

### Configuration and entities

::: pg_partsmith.entities.TablePartitionConfig
    options:
      heading_level: 4

::: pg_partsmith.entities.PartitionInfo
    options:
      heading_level: 4

::: pg_partsmith.entities.MaintenanceResult
    options:
      heading_level: 4

::: pg_partsmith.entities.MaintenanceIssue
    options:
      heading_level: 4

::: pg_partsmith.periods.Period
    options:
      heading_level: 4

### Partition schemes

::: pg_partsmith.scheme.RangePartitioning
    options:
      heading_level: 4

::: pg_partsmith.scheme.HashPartitioning
    options:
      heading_level: 4

::: pg_partsmith.scheme.ListPartitioning
    options:
      heading_level: 4

::: pg_partsmith.scheme.ListGroup
    options:
      heading_level: 4

::: pg_partsmith.scheme.SchemeBase
    options:
      heading_level: 4

### Boundaries and codecs

::: pg_partsmith.boundaries.TimeBoundaries
    options:
      heading_level: 4

::: pg_partsmith.boundaries.NumericBoundaries
    options:
      heading_level: 4

::: pg_partsmith.boundaries.RangeBoundaries
    options:
      heading_level: 4

::: pg_partsmith.boundaries.Window
    options:
      heading_level: 4

::: pg_partsmith.boundaries.RangeBoundaryCodec
    options:
      heading_level: 4

::: pg_partsmith.boundaries.UUIDv7BoundaryCodec
    options:
      heading_level: 4

::: pg_partsmith.boundaries.EpochBoundaryCodec
    options:
      heading_level: 4

### Lifecycle policies

::: pg_partsmith.lifecycle.LifecyclePolicy
    options:
      heading_level: 4

::: pg_partsmith.lifecycle
    options:
      heading_level: 4
      members:
        - CreateAhead
        - CreateUntil
        - CreateNextIf
        - KeepNewest
        - KeepFor
        - KeepBehind
        - ExpireIf
        - DetachMode
        - DropAfter
        - DropNever
        - SizeAbove
        - RowsAbove
        - WindowAgeAbove
        - SqlPredicate
        - Callback
        - AllOf
        - AnyOf
        - Not
        - Candidate

### The plan

::: pg_partsmith.plan.MaintenancePlan
    options:
      heading_level: 4

::: pg_partsmith.plan
    options:
      heading_level: 4
      members:
        - CreatePartition
        - AttachPartition
        - DetachPartition
        - DropPartition
        - Finding
        - FindingReason
        - Reason
        - Severity
        - OperationKind
        - OperationCapabilities
        - PartitionBy

::: pg_partsmith.planner
    options:
      heading_level: 4
      members:
        - plan_maintenance
        - PlanningContext
        - PlanMode
        - fact_targets

### The actual tree

::: pg_partsmith.topology
    options:
      heading_level: 4
      members:
        - ActualTree
        - PartitionNode
        - DetachedPartition
        - PartitionFacts
        - FactKind
        - RelationKind
        - PartitionType
        - RangeBounds
        - HashBounds
        - ListBounds
        - DefaultBounds
        - uniform_modulus
        - hash_keyspace_covered
        - missing_remainders

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
        - PlanStaleError
        - UnsupportedCapabilityError

### Period strategies

::: pg_partsmith.strategies.base.BasePeriodCalculator
    options:
      heading_level: 4

::: pg_partsmith.strategies
    options:
      heading_level: 4
      members:
        - HourPeriodCalculator
        - DayPeriodCalculator
        - WeekPeriodCalculator
        - MonthPeriodCalculator
        - QuarterPeriodCalculator
        - YearPeriodCalculator
        - get_period_calculator

---

## pg_partsmith.aio

Async implementations: service, executor, inspector, repositories, lock managers, and hooks.

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

::: pg_partsmith.aio.services.execution.PlanExecutor
    options:
      heading_level: 4

::: pg_partsmith.aio.services.inspection.PartitionInspector
    options:
      heading_level: 4

### Protocols

::: pg_partsmith.aio.protocols.PartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.aio.protocols.PartitionMetadataProvider
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

::: pg_partsmith.sync.service.PartitionLifecycleService
    options:
      heading_level: 4

::: pg_partsmith.sync.maintainer.PartitionMaintainer
    options:
      heading_level: 4
      members:
        - run_maintenance
        - run_maintenance_safe

::: pg_partsmith.sync.protocols.PartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.sync.protocols.PartitionMetadataProvider
    options:
      heading_level: 4

::: pg_partsmith.sync.protocols.LockManager
    options:
      heading_level: 4

::: pg_partsmith.sync.repositories.repository.PostgresPartitionRepository
    options:
      heading_level: 4

::: pg_partsmith.sync.metadata.PostgresMetadataProvider
    options:
      heading_level: 4

::: pg_partsmith.sync.lock.postgres.PostgresAdvisoryLockManager
    options:
      heading_level: 4

::: pg_partsmith.sync.lock.redis.RedisDistributedLockManager
    options:
      heading_level: 4

::: pg_partsmith.sync.hooks.BasePartitionLifecycleHooks
    options:
      heading_level: 4

---

## pg_partsmith.settings

Env-driven configuration via [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).
Requires the `pydantic-settings` extra: `pip install pg-partsmith[pydantic-settings]`.

::: pg_partsmith.settings.PartitionTableSettings
    options:
      heading_level: 4
