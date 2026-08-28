# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0](https://github.com/bedrock-python/pg-partsmith/compare/pg-partsmith-v0.4.0...pg-partsmith-v0.5.0) (2026-08-28)


### Features

* add boundary codecs for time-sortable partition keys ([c10ffdb](https://github.com/bedrock-python/pg-partsmith/commit/c10ffdbf505d206076c22d35978fe3fc5fbc3afe))
* add ensure_partitions for backfilling explicit periods ([2ce6d10](https://github.com/bedrock-python/pg-partsmith/commit/2ce6d10eda0b6015bc6ba1f6afafb8f5f6bd2274))
* add LIST subpartitioning alongside HASH ([1267b5b](https://github.com/bedrock-python/pg-partsmith/commit/1267b5b387d4ed79dc38c6baa248a40bd615d9c0))
* manage HASH and LIST roots that have no time dimension ([59321d4](https://github.com/bedrock-python/pg-partsmith/commit/59321d42647440dce02f9f4e3e80b3984d9d7674))
* manage nested RANGE -&gt; HASH partition trees ([0d98360](https://github.com/bedrock-python/pg-partsmith/commit/0d9836028e4d3956ce7285bc023c3c561813a8ae))
* partition trees, boundary codecs, static roots and composite keys ([#23](https://github.com/bedrock-python/pg-partsmith/issues/23)) ([f150233](https://github.com/bedrock-python/pg-partsmith/commit/f150233beb516289ec86640846fde1ffdf319732))
* support composite partition keys ([184c55c](https://github.com/bedrock-python/pg-partsmith/commit/184c55c083d0abdb66b4e17e762d5840b6eb7b56))


### Bug Fixes

* answer "is it closed?" instead of raising, and refuse a key we cannot address ([188285a](https://github.com/bedrock-python/pg-partsmith/commit/188285afb888c950405fbc1f24f84f0026556f0e))
* count buckets built while finishing a half-built branch ([a9ed268](https://github.com/bedrock-python/pg-partsmith/commit/a9ed268fe076caffa6ff220daaa57cb4f7017ae4))
* exclude identity columns when creating partitions ([8f2b985](https://github.com/bedrock-python/pg-partsmith/commit/8f2b9859b5e41901e287f9e3ce77538db897697d))
* explain a partition that can never report as closed ([1acf74a](https://github.com/bedrock-python/pg-partsmith/commit/1acf74a41828b33909cc1a7ad4d23ca3151dae6a))
* keep doubled quotes from splitting a LIST bound ([3f07273](https://github.com/bedrock-python/pg-partsmith/commit/3f07273d2b79193a8fc3a6ac80419f1405014c4a))
* keep partition_column in serialized configuration ([39c7093](https://github.com/bedrock-python/pg-partsmith/commit/39c7093f3de17f14bfd9e335eb96b372aa29a279))
* leave NULL-keyed rows where PostgreSQL puts them ([55c9b75](https://github.com/bedrock-python/pg-partsmith/commit/55c9b75af47a66736c8cce7807ee29b615890afc))
* never publish a branch that cannot route its whole keyspace ([8b8b1b3](https://github.com/bedrock-python/pg-partsmith/commit/8b8b1b376783afe0b6973de0323799b45bbac2be))
* quoting hazards that only a hand-built statement can reach ([f73dff8](https://github.com/bedrock-python/pg-partsmith/commit/f73dff8ee8139acdde441d72e2d75d762cce4ec5))
* read the whole key, every constraint, and the timezone that wrote the bound ([5e4ea60](https://github.com/bedrock-python/pg-partsmith/commit/5e4ea60c280d3e14f61df10157194e3cc40f371b))
* spell a partition key as one leading column plus a trailing tuple ([a511b44](https://github.com/bedrock-python/pg-partsmith/commit/a511b44f753b4a30a3a8cdfc24788f27341a5e64))
* stop the planner abandoning subtrees and planning unusable names ([b2bcb96](https://github.com/bedrock-python/pg-partsmith/commit/b2bcb96950a2573ea8cf3a9733dcb74ce286f4b4))
* tell a lost race apart from a real conflict, and isolate each branch ([cf77fb9](https://github.com/bedrock-python/pg-partsmith/commit/cf77fb9a3b5d40c3a6aeb96ffe52c4b4e21eb52a))
* tell NULL from 'NULL', and a hidden child from a missing one ([46af2e7](https://github.com/bedrock-python/pg-partsmith/commit/46af2e78cbf24a2c963e5145f465b8be13450bd3))


### Documentation

* add the new boundary and bounds names to the API reference ([cfd92b1](https://github.com/bedrock-python/pg-partsmith/commit/cfd92b15a2f8647f3ad18b1ad8a758b273cbb11a))
* correct three more claims, and test the one that was only written down ([eb6388c](https://github.com/bedrock-python/pg-partsmith/commit/eb6388c3c3c3ad65c6eb691308ea52af0cd56fff))
* correct what the fact-checker falsified, and add what it found missing ([5175daf](https://github.com/bedrock-python/pg-partsmith/commit/5175daffdaf058697867c6ba0ad6b445e5776065))
* document subpartitioning, boundary codecs and the migration path ([fdc4f28](https://github.com/bedrock-python/pg-partsmith/commit/fdc4f282b29aa55bcc0305206c0887d28dfdd759))
* name the difference between two fields called partition_type ([5ed8863](https://github.com/bedrock-python/pg-partsmith/commit/5ed886395348e452a55678f93fb397f7e9b166e4))
* say that issues now fills up on a successful run ([d4f37f8](https://github.com/bedrock-python/pg-partsmith/commit/d4f37f8373b1e3303cc3140f4eb018d9a758cfa7))
* say what the database reported, not what was assumed ([058801e](https://github.com/bedrock-python/pg-partsmith/commit/058801e03e3a8cdec4b0c78b94863f96f77ac42f))

## [0.4.0](https://github.com/bedrock-python/pg-partsmith/compare/pg-partsmith-v0.3.0...pg-partsmith-v0.4.0) (2026-08-27)

### ⚠ BREAKING CHANGES

- `PostgresPartitionRepository.partition_exists` / `.is_partition_attached` were removed —
  use the identical methods on `PostgresMetadataProvider` (the repository protocol is
  write-only by design; the metadata provider is the read API).
  ([#22](https://github.com/bedrock-python/pg-partsmith/pull/22))
- `MaintenanceIssueStep` now contains only the members that are actually produced:
  `CREATE`, `DETACH`, `DROP` (the `ATTACH` and `HOOK_*` members were never emitted).
- `Period.to_date()` no longer accepts a `day` argument.

### Added

- End-to-end configurable timezone: every calculator accepts
  `tz` (``datetime.UTC`` default, or a keyed ``ZoneInfo``) — the current period, partition
  names, and naive boundary literals all follow it; pruning interprets naive catalog
  boundaries in the calculator's timezone; `PartitionLifecycleService` refuses a
  calculator/`ddl_timezone` mismatch so names and real bounds cannot silently drift
  apart; `ddl_timezone=None` with a non-UTC calculator logs a warning.
  `HourPeriodCalculator` is UTC-only (local hour names are ambiguous under DST).
  Defaults are bit-identical to the previous behavior.
  ([#20](https://github.com/bedrock-python/pg-partsmith/pull/20))
- Runtime-checkable `TimezoneAwareCalculator` / `DdlTimezoneAware` protocols; new shared
  pure modules `pg_partsmith.partition_bounds`, `pg_partsmith.pruning_rules`,
  `pg_partsmith.catalog_queries`; `PartitionType.from_partstrat`; utils helpers
  `coerce_str`, `elapsed_ms`, `describe_exception`, `is_default_partition_conflict`,
  `validate_timezone_alignment`; `get_period_calculator` is exported from
  `pg_partsmith.strategies`; `PartitionTableSettings.get_period_calculator(tz=...)`
  forwards the timezone. ([#22](https://github.com/bedrock-python/pg-partsmith/pull/22))

### Changed

- Library-wide quality pass (behavior-preserving beyond the breaking items above): the
  aio/sync mirrors share the pure parsing/pruning/SQL logic instead of hand-maintaining
  two copies; repository defaults and SQLSTATE sets live in `pg_partsmith.constants`;
  timezone metadata is discovered via protocols instead of `getattr` sniffing; error/log
  wording no longer claims recovery where errors propagate;
  `detach_single_partition` / `drop_single_partition` are documented extension points.
  ([#22](https://github.com/bedrock-python/pg-partsmith/pull/22))

## [0.3.0](https://github.com/bedrock-python/pg-partsmith/compare/pg-partsmith-v0.2.0...pg-partsmith-v0.3.0) (2026-08-27)

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
