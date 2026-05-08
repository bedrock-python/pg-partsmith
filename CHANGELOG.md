# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-05-08

First public release of `pg-partsmith`.

- Time-based partition lifecycle management: create ahead, detach expired, drop orphans.
- Period calculators: `DayPeriodCalculator`, `WeekPeriodCalculator`,
  `MonthPeriodCalculator`, `YearPeriodCalculator` and `BasePeriodCalculator` for custom strategies.
- `get_period_calculator()` — factory function that returns the right calculator for a given granularity.
- `PartitionLifecycleService` — orchestrates the full create → detach → drop sequence.
- `PartitionMaintainer` — scheduler-friendly wrapper; `run_maintenance_safe()` never raises.
- `maintain_partitions()` — plain async function for APScheduler, Celery Beat, etc.
- Lifecycle hooks: `before_create`, `after_create`, `before_detach`, `after_detach`,
  `before_drop`, `after_drop`. `before_*` failures abort the operation; `after_*` failures are logged as warnings.
- `PostgresPartitionRepository` — PostgreSQL DDL implementation.
- `PostgresMetadataProvider` — PostgreSQL system catalog queries.
- `PostgresAdvisoryLockManager` — session-level advisory locks (no extra dependencies).
- `RedisDistributedLockManager` — Redis distributed locks (`pg-partsmith[redis-locks]`).
- `PartitionTableSettings` — pydantic-settings base class for env-driven configuration (`pg-partsmith[pydantic-settings]`).
- Multi-schema support via `schema` field in `TablePartitionConfig`.
- Orphan partition tracking via `COMMENT` markers on detached tables.
- DEFAULT partition reconciliation: moves conflicting rows and retries `ATTACH PARTITION`.
- TIMESTAMPTZ UTC boundary enforcement (`ddl_timezone="UTC"` default).
- Safe-drop protection: `UnmanagedPartitionDropError` guards against dropping unmanaged tables.
- All `Protocol` classes are `@runtime_checkable` — custom implementations can be validated via `isinstance()`.
- Python 3.11, 3.12, and 3.13 support.

[0.1.0]: https://github.com/bedrock-python/pg-partsmith/releases/tag/v0.1.0
