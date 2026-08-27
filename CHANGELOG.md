# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Migration-ergonomics APIs (all mirrored in `aio` and `sync`), extracted from a real
  migration of a hand-rolled partitioner:
  - `service.ensure_partition(config, period)` — create and attach the partition for one
    specific period (idempotent, with DEFAULT reconciliation and attach-race handling);
    for writers that must guarantee a partition exists before an insert.
  - `repository.adopt_partition(table_name, partition_name)` — stamp the orphan marker on
    a legacy detached table so safe-drop accepts it, instead of disabling the guard with
    `drop_allow_unmanaged`.
  - `maintain_lifecycle(..., continue_on_error=True)` (also on the maintainer and
    `maintain_partitions`) — isolate create/detach/drop failures into the new
    `MaintenanceResult.issues` (`MaintenanceIssue`) instead of aborting the run.
  - `metadata.is_partition_closed(partition_name, *, settle_seconds=0)` — server-side
    "the partition's upper bound has passed (+ settle buffer)" check for export pipelines.
  - `PartitionInfo.schema_name` / `PartitionInfo.relname` accessors; `qualify`,
    `split_qualified_name`, and `MaintenanceIssue` are exported from the package root.
- "Migrating an existing partitioner" documentation guide: retention count-vs-distance,
  adopting legacy partitions, schema-qualified names, lock ownership of granular calls,
  per-step error isolation, and export finalization.

## [0.2.0](https://github.com/bedrock-python/pg-partsmith/compare/pg-partsmith-v0.1.0...pg-partsmith-v0.2.0) (2026-08-26)

### Added

- `pg_partsmith.sync` — synchronous mirror of `pg_partsmith.aio` with the same class
  names and API, built on the sync SQLAlchemy `Engine`: `PartitionLifecycleService`,
  `PartitionMaintainer`, `maintain_partitions`, `PostgresPartitionRepository`,
  `PostgresMetadataProvider`, `PostgresAdvisoryLockManager`, `RedisDistributedLockManager`,
  sync `PartitionLifecycleHooks` / `BasePartitionLifecycleHooks`, and sync protocols.
  Differences from the async package: `ddl_timeout_seconds` is enforced server-side via
  PostgreSQL `statement_timeout` (per statement), and the Redis lock renews its TTL from a
  background thread that logs (but cannot cancel maintenance) on renewal failure.
  ([#13](https://github.com/bedrock-python/pg-partsmith/pull/13))
- Hour and quarter partition granularities: `PartitionGranularity.HOUR` / `.QUARTER`,
  `HourPeriodCalculator` (`table__YYYY_MM_DD_HH`, UTC boundaries with hour precision) and
  `QuarterPeriodCalculator` (`table__YYYY_qN`), plus `hour` / `quarter` fields on `Period`
  with validation, arithmetic, and ordering.
  ([#15](https://github.com/bedrock-python/pg-partsmith/pull/15))
- `Period.to_datetime()` — period start as a timezone-aware UTC datetime preserving the
  hour component; the pruning fallback sort now uses it, so hourly partitions within one
  day order chronologically.

### Changed

- `Period` internals were restructured around a single per-granularity dispatch;
  behaviour is unchanged. Built-in calculators now derive names and boundaries
  from `Period` itself instead of duplicating the formatting and arithmetic.
  ([#16](https://github.com/bedrock-python/pg-partsmith/pull/16))

### Fixed

Hardening from a full-library audit
([#16](https://github.com/bedrock-python/pg-partsmith/pull/16)) and an external review
([#17](https://github.com/bedrock-python/pg-partsmith/pull/17)):

- Pruning fails closed: `infinity` upper bounds are treated as unbounded (like
  `MAXVALUE`), and an attached partition whose catalog boundary cannot be
  interpreted is skipped with a warning instead of being pruned by its name.
- `list_partitions` always returns schema-qualified partition names taken from
  the catalog — a partition living in a different schema than its parent can no
  longer be re-resolved via `search_path` to an unrelated same-named table.
- `drop_partition` revalidates attachment and the orphan marker under an
  `ACCESS EXCLUSIVE` lock in the same transaction as `DROP TABLE`, closing the
  window where a concurrently reattached or replaced relation could be dropped.
- Subpartitioned partitions (`relkind='p'`) are now recognised by existence
  checks and orphan discovery, so a detached partitioned child is dropped
  instead of being silently leaked.
- Attach conflict SQLSTATEs (incl. `42809`) are only treated as a lost race
  after verifying the partition is actually attached to the requested parent.
- The compensating "return rows to DEFAULT" step now also runs when the attach
  is interrupted by cancellation (async, shielded) or KeyboardInterrupt (sync).
- A cancellation that lands while awaiting the Redis `SET NX` response performs
  a token-checked release, so a server-side-applied SET no longer leaks the
  lock until TTL.
- Detach: a partition left in `inhdetachpending` state by a cancelled
  `DETACH CONCURRENTLY` (e.g. a DDL timeout) is now completed with
  `DETACH PARTITION ... FINALIZE` instead of failing on every subsequent run.
- Pruning: partitions with a `MAXVALUE` upper bound are never pruned any more —
  previously the unparseable boundary fell back to name-based ageing, which could
  drop a catch-all partition holding current data.
- DEFAULT reconciliation now runs under the same `SET LOCAL TIME ZONE` as
  `ATTACH PARTITION`, so a non-UTC server timezone no longer moves the wrong row
  range; if the attach still fails after rows were reconciled, they are moved
  back to the DEFAULT partition (best effort) instead of being stranded in a
  detached table.
- Attach race handling: SQLSTATE `42809` ("already a partition", the code
  PostgreSQL actually raises when a concurrent worker wins the attach) is now
  tolerated, while `55006` (partition mid-detach) correctly propagates instead of
  being mislabelled as "already attached".
- DDL statements no longer break when an identifier or literal contains `:`
  (e.g. a pre-existing table comment) — colons are escaped before SQLAlchemy
  `text()` parses them as bind parameters.
- `list_partitions` skips (with a warning) partitions whose schema or name
  contains a dot: such names cannot be addressed safely as `schema.relname`
  strings and previously produced DDL against the wrong relation.
- Boundary parsing only applies to RANGE bound expressions; LIST/HASH bounds no
  longer yield fabricated from/to values.
- Config validation rejects quoted mixed-case partition columns up front instead
  of failing later inside reconciliation SQL.
- Lock managers: the per-table acquire rate limit no longer serializes unrelated
  tables (the delay is now slept outside the shared mutex) and no longer sleeps
  spuriously on the first acquire after host boot; the Redis lock is released
  even when the renewal watchdog fails to start; the async Redis lock no longer
  swallows an external task cancellation during watchdog teardown.
- Maintainer logging: operational `PartitionError`s (e.g. lock contention) are
  logged as warnings instead of "unexpected exception" errors with tracebacks.

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
