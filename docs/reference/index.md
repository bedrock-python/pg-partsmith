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

### Period strategies

::: pg_partsmith.strategies.base.BasePeriodCalculator
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

::: pg_partsmith.strategies.year.YearPeriodCalculator
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
