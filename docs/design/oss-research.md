# OSS research: how production systems manage PostgreSQL partitions

Companion to [RFC 0001](rfc-0001-partition-schemes.md). Ten open-source systems were read
at source level on 2026-08-28 (branch `master`/`main` of each, commits noted where they
matter). For each: what is partitioned, who creates and removes partitions, how failures are
handled, what is application-specific, and which generic primitive replaces the custom code.
Every claim below was read in the cited file; things that could not be verified are marked.

## 1. GlitchTip (`glitchtip/glitchtip-backend`, Python/Django)

**Sources:** `glitchtip/partition_manager.py` (`UUID7Helper`, `PartitionManager`),
`glitchtip/management/commands/maintain_partitions.py`, `glitchtip/tasks.py`
(`perform_maintenance`), `glitchtip/cold_storage.py`, `glitchtip/settings.py`,
`apps/issue_events/migrations/sql/create_events_v2.sql`, `apps/logs/migrations/sql/create_logs_v1.sql`,
`apps/uptime/migrations/sql/create_uptime_v2.sql`, `glitchtip/tests.py`; blog post
"GlitchTip 6 released" (2026-02-03). Manager history: 18 commits 2026-01-15 → 2026-08-04.

- **What is partitioned.** `issue_events_issueevent`, `logs_logevent` and the span staging
  table are `PARTITION BY RANGE (id)` on a UUIDv7 `id`, **daily**, each day
  `PARTITION BY HASH (organization_id)`; `uptime_monitorcheck` is `RANGE (id)` weekly → HASH;
  six aggregate tables are `RANGE (date)` weekly → HASH; `issue_events_issueindex` and
  `performance_transactiongroup` are plain `HASH (organization_id)` roots created once by a
  migration. Every table has a composite primary key `(id|…, organization_id)`.
- **Bounds.** `UUID7Helper._uuid7_for_timestamp(dt, min_random=True)`: 48-bit ms timestamp,
  version 7, variant 2, all random bits zero; both bounds use the *minimum* UUID of their
  instant so adjacent days are contiguous; the timestamp is clamped to `[0, 2^48-1]`.
- **Creation.** Per period: `CREATE TABLE IF NOT EXISTS p_YYYYMMDD PARTITION OF parent FOR
  VALUES FROM (...) TO (...) PARTITION BY HASH (organization_id)` then one
  `… FOR VALUES WITH (MODULUS n, REMAINDER i)` per bucket, `n` from
  `settings.PARTITION_HASH_BUCKETS` (default 2 since 2026-08-04; was 4, originally 16;
  `0` yields a plain leaf). Lookahead: 7 daily, 3–4 weekly. Triggered by the
  `perform-maintenance` scheduled task at 05:00, at startup in all-in-one mode, and inside
  migrations — never on insert (a missing partition surfaces as an `IntegrityError`).
- **Reconciliation.** `execute_partition_creation` reads `pg_class`/`pg_inherits`/
  `pg_get_expr(relpartbound)`, regexes `modulus N`, and: same modulus → create the missing
  `_h{i}` *by name*; complete set at another modulus → leave (info); incomplete set at another
  modulus → fill at the historical modulus (warning); plain leaf, mixed moduli or non-hash
  children → skip with a warning. Commit 7beeca02 explains why the catalog check comes first:
  `CREATE TABLE IF NOT EXISTS … PARTITION OF` takes `AccessExclusiveLock` on the root and
  siblings *before* checking existence and deadlocked with inserts.
- **Retention.** Two paths. `drop_old_partitions` parses `YYYYMMDD` from the *name*
  (catalog bounds are fetched but unused) and issues `DROP TABLE IF EXISTS … CASCADE`
  directly. `cold_storage.py` (events/logs) exports per-org Parquet, then
  `ALTER TABLE … DETACH PARTITION … CONCURRENTLY` in autocommit with three deadlock retries
  and a plain-detach fallback, then `DROP TABLE IF EXISTS`. `cleanup_old_issues` waits
  retention + 7 days. No advisory lock: single-runner semantics come from the task scheduler.
- **Lock budget is a first-class concern:** `max_locks_per_transaction=512` in compose,
  the default bucket count halved, uptime moved from daily to weekly ("1,946 → 118 locks per
  page load"), no DEFAULT partitions, no DB-level foreign keys to `Issue`.
- **App-specific:** the `organization_id` hash column, the hard-coded table list and
  cadences, the `GLITCHTIP_*_RETENTION_DAYS` settings, the DuckDB export coupling,
  `import_legacy_events` creating plain leaves.
- **Unverified:** which `psql_partition` package is imported at runtime; no tests for
  `drop_old_partitions` were found; catalog queries filter by `relname` only (no schema).

**pg-partsmith 1.0 verdict** (the question the RFC asks): can GlitchTip replace
`PartitionManager`?

| Concern | Verdict | With |
|---|---|---|
| Daily/weekly creation ahead | YES | `RangePartitioning(key="id", boundaries=TimeBoundaries(granularity=DAY, codec="uuidv7"))`, `CreateAhead(7)` |
| Nested HASH per period | YES | `child=HashPartitioning(key="organization_id", modulus=2, name_suffix="_h{remainder}")` |
| UUIDv7 bounds | YES | `UUIDv7BoundaryCodec` — bit-identical layout (min UUID both ends, 48-bit clamp) |
| Missing-bucket repair, historical modulus, legacy leaves, mixed moduli | YES | planner rules `HASH_GAP`, `HASH_GAP_HISTORICAL_MODULUS`, `MODULUS_PRESERVED`, `LEGACY_LEAF`, `NON_UNIFORM_*` |
| Retention by bounds instead of names; detach-concurrently-then-drop | YES | `KeepNewest`/`KeepFor`, `DetachMode.AUTO`, `DropAfter` |
| Custom names (`p_YYYYMMDD`, `_h{i}`) | YES | a `PeriodCalculator` subclass in `TimeBoundaries(calculator=…)` + `name_suffix` |
| Cold storage export before drop | YES | `before_detach`/`before_drop` hooks; raising aborts and retries next tick |
| Query-layer routing (UUIDv7 range predicates, `organization_id` filters) | NO (by design) | stays in the application; `UUIDv7BoundaryCodec.min_uuid_for` is exposed for it |
| Lock-budget tuning, `max_locks_per_transaction` | NO (documentation) | operator concern; the planner's zero-DDL steady state helps |

## 2. GitLab (`gitlab-org/gitlab`, Ruby)

**Sources:** `lib/gitlab/database/partitioning/partition_manager.rb`,
`time/{base,daily,weekly,monthly}_strategy.rb`, `time_partition.rb`, `int_range_strategy.rb`,
`sliding_list_strategy.rb`, `ci_sliding_list_strategy.rb`, `single_numeric_list_partition.rb`,
`detach_eligibility.rb`, `detached_partition_dropper.rb`, `with_partitioning_lock_retries.rb`,
`partition_monitoring.rb`, `list/convert_table.rb`, `postgres_partition.rb`,
`app/models/postgresql/detached_partition.rb`, `app/models/concerns/partitioned_table.rb`,
`doc/development/database/partitioning/*.md`; MRs !65093, !67056, !69850, !160342, !162590,
!184045, !184290, !184596, !190634, !231242, !244534, !251847.

- **Strategies:** `daily`/`weekly`/`monthly` (date range), `sliding_list`, `ci_sliding_list`,
  `int_range`. Hash and static list exist only as migration helpers.
- **Planner shape:** `current` from `pg_get_expr(relpartbound)` (views
  `postgres_partitions`), `desired` computed purely, `missing = desired − current`,
  `extra = current − desired`; identity is `(table, name, bounds)`, never a parsed name.
- **Time strategies:** desired = a `MINVALUE` catch-all `<table>_000000` (until retention
  starts) plus one partition per period up to a headroom (monthly 6 months, daily 28 days,
  weekly 4 weeks); `retain_for:` is required (a Duration or `:ever`);
  `retain_non_empty_partitions` skips partitions that hold data.
- **Sliding list:** callbacks `next_partition_if(active)` / `detach_partition_if(partition)`
  receive a `SingleNumericListPartition` (`.value`, `.partition_name`, `.data_size`); the
  active partition is the highest LIST value; extras are all-but-newest taken oldest-first
  while `detach_partition_if` holds, never the partition equal to the column DEFAULT; routing
  is via the column DEFAULT (set in `after_adding_partitions`, repaired in `validate_and_fix`
  under `LOCK TABLE … ACCESS EXCLUSIVE`). Typical callbacks: rotate when the oldest row in
  the active partition is older than a day; detach when no `status_pending` rows remain.
- **`sync_partitions`:** skip if not partitioned → Redis lease per table (1 h,
  non-blocking) → `validate_and_fix` → plan both lists → create → detach → `ANALYZE
  (SKIP_LOCKED)` when `analyze_interval` elapsed; `rescue StandardError` per table.
- **Create:** `CREATE TABLE IF NOT EXISTS gitlab_partitions_dynamic.x (LIKE parent
  INCLUDING ALL)` then `ATTACH PARTITION` (SHARE UPDATE EXCLUSIVE — MR !190634), inside a
  lock_timeout ladder (0.1–1 s, 20 attempts, raise on exhaustion).
- **Detach:** `DetachEligibility` defers a partition while any foreign key references the
  parent (MR !251847, 2026-08-27); the `detached_partitions(table_name, drop_after)` row is
  written *before* the plain `DETACH PARTITION`.
- **Drop:** `RETAIN_DETACHED_PARTITIONS_FOR = 1.week`, per-model
  `retain_detached_partitions_for:` (!244534); `MAX_PARTITION_SIZE = 150.gigabytes` — a
  larger partition gets `drop_after` moved to the next Saturday (!184045/!184596);
  `DetachedPartitionDropper` (cron 03:20) refuses partitions still in `pg_inherits`, locks
  the row, drops FKs one per short transaction, `DROP TABLE IF EXISTS`, sleeps a minute
  between items.
- **Int range:** `partition_size` from the sequence's `min_value`, "current max" is the
  last partition's upper bound (never `max(id)`), six empty trailing partitions kept,
  no retention; deprecated ("incompatible with cells").
- **Monitoring:** Prometheus gauges `db_partitions_present/missing/extra{table}` by
  re-running the planner; no dry-run in the manager.
- **App-specific:** fixed schemas `gitlab_partitions_dynamic`/`_static`, `ci_partitions` and
  the application-written `partition_id`, multi-database plumbing, loose-foreign-key
  triggers on new partitions, ops feature flags.

**pg-partsmith 1.0 verdict, feature by feature:**

| GitLab feature | Verdict | Notes |
|---|---|---|
| date RANGE with headroom and `retain_for` | YES | `CreateAhead`/`CreateUntil`, `KeepFor` |
| `MINVALUE` catch-all partition | PARTIAL | inspected and preserved (`UNBOUNDED_PARTITION`), never created by the planner |
| `retain_non_empty_partitions` | YES | `ExpireIf(AllOf((KeepFor(...), Not(RowsAbove(0)))))` (estimate-based) or an `SqlPredicate` |
| int RANGE | YES | `NumericBoundaries(step)` with `max(key)`/sequence cursor; GitLab's "six empty trailing partitions" rule is `CreateAhead(6)` |
| HASH / static LIST | YES | `HashPartitioning` / `ListPartitioning` roots and levels |
| sliding LIST (`next_partition_if` / `detach_partition_if`) | YES | `ListPartitioning(sequence=IntegerSequence(...))` with `CreateNextIf` / `ExpireIf`; the cursor is the newest partition; the column-DEFAULT routing invariant stays application-side |
| state-dependent detach | YES | `ExpireIf(SqlPredicate(...))`, `Callback` |
| detached grace period, per-table | YES | `DropAfter(grace=…)` per config; detach instant recorded in the marker |
| large-partition drop scheduling | YES | `DropAfter(when=Callback(...))` with `SizeAbove` facts (the Saturday rule is a two-line callback) |
| FK-based detach eligibility | YES | `Unreferenced()` — the condition PostgreSQL enforces on `DETACH` (`23503`); a refused detach is recorded as an issue |
| lock_timeout ladder | PARTIAL | `drop_lock_timeout_ms` + retries on drop; detach has a DDL timeout, no ladder |
| Redis lease | YES | `RedisDistributedLockManager` / advisory locks |
| ANALYZE after create | NO (hook) | `after_create` hook |
| gauges | YES (data) | `plan()` is the gauge source: `len(plan.creates)`, `len(plan.detaches)`, findings |

## 3. Centrifugo (`centrifugal/centrifugo`, Go, `internal/pgoutbox/partitioner.go` @ ec700c1d)

- `Partitioner{Pool, ParentTable, CleanupInterval, LookaheadDays, RetentionDays, ErrorFn}`
  maintains an existing `PARTITION BY RANGE (created_at)` parent with `PRIMARY KEY (id,
  created_at)` and no DEFAULT partition, for `cf_stream_history`, `cf_map_stream`,
  `cf_controller_messages`.
- Create: for each of `LookaheadDays` days, `CREATE TABLE IF NOT EXISTS {parent}_{YYYY}_{MM}_{DD}
  PARTITION OF {parent} FOR VALUES FROM ('YYYY-MM-DD 00:00:00+00') TO (…)` — UTC-explicit
  bounds to dodge session-timezone drift.
- Drop: `RetentionDays <= 0` = never; list children via `pg_inherits` filtered to
  `current_schema()`; parse the date from the last three `_` components of the *name*;
  `DROP TABLE IF EXISTS` directly (no DETACH); per-partition errors go to `ErrorFn`.
  **Names that do not parse are "user-managed" and skipped** — the naming convention is the
  only ownership marker.
- `Run` = ticker doing ensure + drop; callers also ensure at startup; no cross-node lock
  (every node runs the worker; idempotent DDL only).
- Unverified: docs say PostgreSQL 16+, a code comment says 13+.

**Verdict:** YES — daily `RangePartitioning` + `CreateAhead(lookahead + 1)` + `KeepFor`,
and the hand-attached partitions Centrifugo protects by name are protected in 1.0 by
*bounds*: an attached partition whose bounds are not a window of the daily grid is
`UNMANAGED_PARTITION` and never touched (verified by an integration test).

## 4. PGMQ (`pgmq/pgmq`, SQL extension, `pgmq.sql` v1.12.0 lines 1313–1509)

- `create_partitioned(queue_name, partition_interval='10000', retention_interval='100000')`:
  an integer interval text → `PARTITION BY RANGE (msg_id)`, otherwise → `RANGE (enqueued_at)`;
  requires `pg_partman` (`_ensure_pg_partman_installed`).
- pg_partman wiring: `create_parent(p_control := msg_id|enqueued_at, p_interval := <text>)`,
  default `premake` 4, then `UPDATE part_config SET retention = <retention_interval>,
  retention_keep_table = false, retention_keep_index = true, automatic_maintenance = 'on'`.
  Semantics: interval = per-partition width; retention = age (time) or `max(msg_id) − N`
  (numeric). PGMQ never calls `run_maintenance`; it relies on the `pg_partman_bgw`
  background worker.

**Verdict:** YES — `RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=10_000))`
with `CreateAhead(4)` and `KeepBehind(100_000)` is the same policy without the extension,
the BGW, or `shared_preload_libraries`; the queue code itself is untouched.

## 5. Hatchet (`hatchet-dev/hatchet`, Go + SQL)

**Sources:** `sql/schema/v1-core.sql` (9 daily RANGE tables), `sql/schema/v1-olap.sql`
(11 daily/weekly RANGE tables plus `v1_task_events_olap_tmp HASH(task_id)` and
`v1_task_status_updates_tmp HASH(dag_id)`), SQL helpers `create_v1_range_partition`,
`create_v1_weekly_range_partition`, `get_v1_partitions_before_date`,
`create_v1_hash_partitions`, `swap_v1_payload(s)_olap_partition_with_temp`; Go
`UpdateTablePartitions` (task.go / olap.go), `partition_lease.go`, `loader.go`,
`pkg/config/limits/limits.go`; issue #3424.

- Create: `CREATE TABLE (LIKE parent INCLUDING INDEXES …)` + autovacuum/fillfactor
  reloptions + `ATTACH PARTITION`, named `<table>_<YYYYMMDD>`; horizon today + tomorrow,
  every 15 minutes and at startup; replicas excluded by a 15-minute lease row.
- Retention: `SERVER_LIMITS_DEFAULT_TENANT_RETENTION_PERIOD` (720h) with core/OLAP
  overrides; partitions older than `now − retention` are listed by the date in their
  *name*, then on a dedicated non-PgBouncer connection: `SET lock_timeout='1min'` →
  `DETACH … CONCURRENTLY` → `DETACH … FINALIZE` on "already pending detach" → `DROP TABLE`;
  55P03 becomes `ErrPartitionLockConflict`.
- HASH: per-child readers with `FOR UPDATE SKIP LOCKED` on `v1_task_events_olap_tmp_<n>` —
  "process batches of events in parallel without needing to place conflicting locks on
  tasks"; `create_v1_hash_partitions` refuses to shrink the modulus.
- Issue #3424 (closed stale, no maintainer reply): no visibility into sizes or the next
  cleanup, no purge endpoint, manual `pg_total_relation_size` + `DETACH … CONCURRENTLY;
  DROP TABLE` workaround that bypasses application callbacks; asks for a dry run with row
  and disk estimates.

**Verdict:** YES for the lifecycle (daily `RangePartitioning`, `CreateAhead(2)`, `KeepFor(720h)`,
`DetachMode.AUTO` with pending-detach finalization, advisory lock instead of the lease
row), YES for the root `HashPartitioning(key="task_id", modulus=4)`, and the operator
gap is exactly `plan()` with `SizeAbove`/`RowsAbove` facts and `plan.describe()`.
Reloptions/fillfactor per partition stay an `after_create` hook.

## 6. pg-trx-outbox (`darky/pg-trx-outbox`, TypeScript, @ f5f30c1e)

- README-only DDL: `PARTITION BY HASH (key)`, `PRIMARY KEY (id, key)`, children
  `pg_trx_outbox_{0,1,2} … WITH (MODULUS 3, REMAINDER n)` created by the user; the library
  creates nothing and has no maintenance. `outboxOptions.partition?: number` makes
  `transfer.ts` read the child `pg_trx_outbox_<n>` directly with `FOR UPDATE SKIP LOCKED`,
  so the suffix must equal the remainder by convention.

**Verdict:** YES — `HashPartitioning(key="key", modulus=3, name_suffix="_{remainder}")` creates
and repairs the set idempotently; `get_actual_tree` gives consumers the
`(modulus, remainder) → name` mapping from the catalog.

## 7. Hookdeck Outpost (`hookdeck/outpost`, Go, issue #249)

- Migrations declare `events` and `attempts` as `PARTITION BY RANGE (time)` with
  `PRIMARY KEY (time, id)` and *only* `*_default` DEFAULT partitions; `pglogstore.go` has no
  partition logic; the README recommends monthly partitions.
- Issue #249 (open since 2025-02-25) proposes: at `api` startup ensure partitions through
  the end of next year, then drop stale partitions per configured retention; pg_partman vs
  in-app undecided. Nothing implemented since; issue #1027 (2026-08-10) re-raised it and the
  maintainer answered "not on the roadmap", recommending pg_partman/pg_cron or batched
  `DELETE`s; docs PR #1032 (merged 2026-08-19) states "Outpost has no built-in retention for
  PostgreSQL".

**Verdict:** YES — the config that replaces the planned custom workflow:

```python
TablePartitionConfig(
    table_name="events",
    scheme=RangePartitioning(key="time", boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH)),
    lifecycle=LifecyclePolicy(
        creation=CreateUntil(datetime(next_year + 1, 1, 1, tzinfo=UTC)),
        retention=KeepFor(timedelta(days=retention_days)),
    ),
)
```

run at startup under the advisory lock (replicas that lose the lock skip the tick). Rows
already in `events_default` are moved into each new monthly partition by DEFAULT
reconciliation as it is attached.

## 8. ColdFront (`pgEdge/coldfront`, Go)

**Sources:** `docs/architecture.md`, `internal/partition/{reconcile,boundary,catalog}.go`
and the archiver/cutover code (names as in the report).

- `Spec{Parent, Schema, Column, Period, Premake, RetentionInterval, Boundary, LeafPrefix,
  Strategy}` + `RunReconcile` = premake → ensure-current → expire. `Boundary` interface:
  `Literal(t)` / `Parse(lit)` for timestamp, UUIDv7 and snowflake keys; bounds parsed from
  `pg_get_expr(relpartbound)` with `MINVALUE`/`MAXVALUE`/`infinity` sentinels; cutoff
  computed in the database (`$1::timestamptz - $2::interval`).
- Expiry: `DETACH … CONCURRENTLY` on an autocommit `*pgx.Conn` (`RunReconcile` requires a
  non-transactional handle), then `DROP TABLE IF EXISTS`, or `expiration_strategy='detach'`
  keeps the table; the tiered archiver exports to Iceberg and then detaches inside a
  transaction under `lock_timeout=100ms`, retrying only 55P03.
- Multi-node: Spock replicates `CREATE … PARTITION OF` and `DROP TABLE` but skips
  `CONCURRENTLY`, so `detachOnPeers` re-runs the concurrent detach on every peer.
- Registration refuses DEFAULT partitions, UNLOGGED relations, case-only name clashes,
  `_` prefixes and over-long names; 2-level LIST→RANGE provisioning from a `values_source`
  query.

**Verdict:** YES for the lifecycle design — `DetachMode`, `DropNever`
(`expiration_strategy='detach'`), codecs (`UUIDv7BoundaryCodec`; a snowflake codec is a
ten-line `RangeBoundaryCodec`), `before_drop` hooks for "export, verify, then drop", and
`LIST → RANGE` nesting; the peer fan-out of non-transactional DDL stays outside the library.

## 9. pg_partman (`pgpartman/pg_partman` @ development c737cc1d, extension 5.5.0)

Used as an edge-case catalogue. What it needed to configure (`sql/tables/tables.sql`
`part_config`): control column, interval, type, `premake`, `retention`,
`retention_keep_table`, `retention_keep_index`, `retention_schema`, `infinite_time_partitions`,
`datetime_string`, `automatic_maintenance`, `sub_partition_set_full`, `inherit_privileges`,
`constraint_cols`/`optimize_constraint`, `epoch`, `template_table`, `date_trunc_interval`,
`ignore_default_data`, `time_encoder`/`time_decoder`, `detach_before_drop`, `maintenance_role`,
`maintenance_last_run`.

Lessons carried into 1.0:

- **Encoder/decoder** are SQL function names (`timestamptz → key type`, `text → timestamptz`);
  bounds are attached as encoded literals, names stay calendar-based — the same split as
  `RangeBoundaryCodec`. 5.5.0 had to quote those names (CVE-2026-61781/61817/61818); Python
  codecs avoid the class entirely.
- **Integer sets are data-driven:** start = `max(control)` of the parent, maintenance
  reads the highest non-empty child, retention drops a child when
  `retention <= max − upper_bound` — `NumericBoundaries` with `CursorSource.MAX_KEY` and
  `KeepBehind(distance)`.
- **DEFAULT handling:** pg_partman never pre-checks; since 5.5.0 a failing set is skipped
  with a warning (before, the whole run aborted). pg-partsmith keeps its stronger
  reconciliation (move rows, retry, restore on failure).
- **Template tables** carry what PostgreSQL does not propagate: non-key unique indexes and
  their tablespaces, UNLOGGED, reloptions, toast reloptions; pg_partman also copies
  per-column statistics targets and REPLICA IDENTITY. `LocalLeaves(tablespace,
  storage_parameters, inherit_privileges)` is the declarative form of the first three;
  statistics targets and replica identity stay hook material.
- **Retention flow:** ascending scan, expire when `upper_bound < reference − retention`
  (from the catalog, never the name), never the last child, plain DETACH → optional index /
  publication drop → DROP or `SET SCHEMA`.
- **Maintenance:** global transaction advisory lock, no-op on replicas
  (`pg_is_in_recovery()`), per-set exception isolation, `infinite_time_partitions`
  substitutes `now` when data stops (moot for a clock-driven planner).
- **Caveats worth repeating in docs:** session-timezone-dependent names and bounds (run
  in UTC; DST breaks hourly), identity columns only via parent inserts, NULL keys go to
  DEFAULT, FK-referenced tables need detach-before-drop.

Capability comparison (core / extension / out of scope / future):

| pg_partman | pg-partsmith 1.0 |
|---|---|
| premake, retention (time and id), detach-only, DEFAULT reconciliation, encoded keys, subpartitioning, advisory lock, per-set error isolation, timezone discipline, adoption of an existing set | core |
| privileges, tablespace, reloptions (template properties) | core (`LocalLeaves`) |
| `retention_schema`, index/publication drop on detach, `p_analyze`, statistics/replica identity propagation | extension (hooks) |
| `partition_data_proc` / `partition_data_async` batch movers, `undo_partition`, conversion of a plain table | core (`partition_data`, `unpartition`) |
| BGW scheduler and GUCs, `maintenance_order`, `automatic_maintenance`, jobmon | out of scope (the caller's scheduler) |
| `pg_is_in_recovery()` guard, `check_default()` monitoring helper | future |

## 10. pg_clickhouse (`ClickHouse/pg_clickhouse`, `doc/pg_clickhouse.md`, `doc/offload-partition.sql`)

- `CREATE FOREIGN TABLE events_2023 PARTITION OF events FOR VALUES FROM … TO … SERVER ch_svr
  OPTIONS (table_name 'events')` beside a local `CREATE TABLE events_2024 PARTITION OF events …`;
  the offload helper merges contiguous single-column RANGE locals into one foreign table
  (inline `CHECK` so `ATTACH` skips the scan), copies, drops the locals, attaches.
- PostgreSQL restrictions: a foreign partition is allowed only on an index-free parent
  (verified: `42809 cannot create foreign partition of partitioned table` when a PK
  exists); `DROP TABLE` on `relkind='f'` fails with `is not a table`; DETACH (also
  CONCURRENTLY) and ATTACH work; a DEFAULT partition blocks concurrent detach.

**Verdict:** introspection is exact (`RelationKind.FOREIGN` on the node,
`FOREIGN_PARTITION` finding under a local-leaves configuration, never dropped, never
detached) and the tree with a foreign leaf plans and maintains normally around it;
`ForeignLeaves(server, options)` creates every leaf as a foreign table and manages it
through the whole lifecycle (`COMMENT ON FOREIGN TABLE` marker, `DROP FOREIGN TABLE`) —
both verified by integration tests with a `postgres_fdw` loopback. The offload of an
existing local partition (copy, drop, attach foreign) stays a hook-driven workflow.

## Cross-cutting patterns

1. **Nobody trusts names for truth except when they have nothing else.** GitLab, ColdFront,
   pg_partman and GlitchTip's reconciliation read `pg_get_expr(relpartbound)`; Centrifugo,
   Hatchet and GlitchTip's `drop_old_partitions` parse names for retention. 1.0 reads bounds
   everywhere and parses names only to recognise a detached orphan.
2. **Every serious system separates "what should exist" from "what to do now"**: a
   catalog snapshot, a pure desired set, a diff, then DDL (GitLab explicitly; GlitchTip and
   ColdFront implicitly). That is the `plan()`/`apply()` split.
3. **`DETACH CONCURRENTLY` forces a non-transactional executor** (Hatchet, ColdFront,
   GlitchTip cold storage) and a story for the pending-detach state (Hatchet `FINALIZE`).
4. **Detach and drop are different events** (GitLab's `detached_partitions` with
   `drop_after`, ColdFront's `detach` strategy, GlitchTip's export-then-drop).
5. **Ownership is the unsolved problem everywhere**: naming conventions (Centrifugo,
   GlitchTip), a registry (pg_partman), or nothing (Outpost). Alignment with the scheme's
   grid, derived from the catalog, needs no table and protects the cases the conventions
   were written for.
6. **Operators want to see the plan** (Hatchet #3424; GitLab's gauges; ColdFront's
   `--dry-run`/`--print-sql`).
