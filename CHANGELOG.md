# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.1.0 — one event for every hook

**This breaks every hook written against 1.0.** It is shipped in a minor version on
purpose: the library is days old and has no dependants to protect, and carrying the old
shape until a major would mean carrying it for years. Ported hooks are two lines of change
each; unported ones are refused when the service is constructed, with the fix in the
message, rather than failing at their first call in the middle of a maintenance run.

### What changed

Three of the six phases were handed a rich object and three a bare string, so a
`before_drop` that archived a partition could not know which period it was archiving —
and could not look it up either, because `DETACH` clears `relpartbound`. Every hook method
now takes one `PartitionEvent`:

| Field | What it is |
|---|---|
| `phase` | which moment this is (`HookPhase`) |
| `config` | the table's configuration — no longer something a hook has to be given separately |
| `partition` | `PartitionInfo`: name, bounds, OID, `subpartition_type` |
| `window` | the period covered; `None` for a member of a root `HASH` or `LIST` |
| `operation` | the planned operation: `reason`, `detail`, `oid`, `size_bytes`, `row_estimate`, `detached_at` |
| `table_name` | the root table, derived from `config` |

| Before | After |
|---|---|
| `before_create(config, partition)` | `before_create(event)` — `event.config`, `event.partition` |
| `after_create(config, partition)` | `after_create(event)` |
| `before_detach(table_name, partition)` | `before_detach(event)` — `event.table_name`, `event.partition` |
| `after_detach(table_name, partition_name)` | `after_detach(event)` — `event.partition.name` |
| `before_drop(table_name, partition_name)` | `before_drop(event)` — and now `event.window`, `event.operation.reason` |
| `after_drop(table_name, partition_name)` | `after_drop(event)` |
| — | `before_attach(event)` / `after_attach(event)`, new: a detached partition going back into the tree |
| — | `on_event(event)`, new: fires for every phase in addition to the method named for it |

### Also breaking, in the same area

- `PlanExecutor.detach_single_partition` and `drop_single_partition` take the
  `TablePartitionConfig` instead of the parent's name — it is what the event is built
  from, and every other entry point already took the config.
- `PartitionLifecycleService.detach_old_partitions` and `drop_detached_partitions` take
  the config for the same reason: `detach_old_partitions(config, partitions)`,
  `drop_detached_partitions(config, names)`.

### Added

- `PartitionEvent`, `HookPhase` (exported from `pg_partsmith`), and `on_event` — one
  method for an audit trail or a metrics counter across every phase, instead of one
  identical delegating method per phase, plus another with every phase added.
- `before_attach` / `after_attach`: a partition retention released is re-attached when its
  window is wanted again — retention grew, or create-ahead reached back to it — and that
  transition used to happen silently. An export taken while it was detached goes stale the
  moment it comes back, and this is where a hook is told.
- Hook signatures are read when the service is constructed; one still taking the 1.0
  arguments is refused there, naming the class, the method and the shape to move to.

## [1.0.0](https://github.com/bedrock-python/pg-partsmith/compare/pg-partsmith-v0.5.0...pg-partsmith-v1.0.0) (2026-09-01)


### ⚠ BREAKING CHANGES

* partition schemes, lifecycle policies and the maintenance plan, in the core and in both the aio and sync packages

### Features

* batched data movement -- partition_data drains DEFAULT, unpartition moves everything back ([9644df3](https://github.com/bedrock-python/pg-partsmith/commit/9644df3ddf3576e27dce52fd331043d35c24a044))
* foreign-key aware retention -- Unreferenced() and detach refusals as issues ([4257502](https://github.com/bedrock-python/pg-partsmith/commit/4257502d23121ce6d463e6e5a8b43a217a1ec14f))
* leaf backends -- local tablespace/storage/privileges and foreign leaves ([1cecc57](https://github.com/bedrock-python/pg-partsmith/commit/1cecc57208c2dc2e58a99d29d48a421f3e00332d))
* partition schemes, lifecycle policies and the maintenance plan (core + aio/sync, tests pending) ([90af1bb](https://github.com/bedrock-python/pg-partsmith/commit/90af1bb78a186bc25e87996590ad0016189baced))
* sliding LIST progression and Redis lock integration tests ([7cf7a4c](https://github.com/bedrock-python/pg-partsmith/commit/7cf7a4cae229ef55b5fe932089272c5a20410308))


### Bug Fixes

* close every finding of the external review (F-001..F-016) ([927c8e0](https://github.com/bedrock-python/pg-partsmith/commit/927c8e0b7f1a08d860f2ce9826a4bf660206e2ce))
* close the verification round -- referenced rows, pinned detaches, identity sequences ([1fed77f](https://github.com/bedrock-python/pg-partsmith/commit/1fed77f3a3a3cb63c0e1b1e1ea0e877a402655bb))
* count every allocated identity value as possibly cached, not just the newest block ([4571ad1](https://github.com/bedrock-python/pg-partsmith/commit/4571ad1787a2b6e5f23a31c898cc1e7229ecd44b))
* decide cycling identity sequences on their whole range, bound the cached region at START ([343034e](https://github.com/bedrock-python/pg-partsmith/commit/343034e11afeab835eaac2281ef6780033fecb8d))
* decide every identity column before any sequence moves, and park under a name of our own ([9a618c2](https://github.com/bedrock-python/pg-partsmith/commit/9a618c2f24eeef1dd5f309530aca5a9ef1d44df5))
* decide identity sequences on the ids a move carries, not on the destination's own rows ([07b9d9a](https://github.com/bedrock-python/pg-partsmith/commit/07b9d9add44e0687e3c92d875c9d1835800f8518))
* finish interrupted concurrent detaches and accept positional rule arguments ([b93adc1](https://github.com/bedrock-python/pg-partsmith/commit/b93adc192e0b60b8b1b647001f6c2625cfe94f80))
* hold created relations by OID; transactional FINALIZE; deferred FKs; descending identities ([b541b3f](https://github.com/bedrock-python/pg-partsmith/commit/b541b3f70f6f79ba73bcb9cfa827b9c75e5cf1d7))
* hold identities through subtrees, restores and pins; model identity sequences ([5fd0bc6](https://github.com/bedrock-python/pg-partsmith/commit/5fd0bc608582d5675fa108a0d1aba24c6fd5f799))
* measure plain detached tables, re-attach orphans retention wants back, report lifecycle units only ([ce14093](https://github.com/bedrock-python/pg-partsmith/commit/ce14093dd3ea377ab8adff7f0ecc55e4cc09e0cd))
* refuse cached identity blocks, read each identity once, carry it into re-attached subtrees ([cc3d72e](https://github.com/bedrock-python/pg-partsmith/commit/cc3d72eef01d94a2cdb9f4398a5063d2d58e67e3))
* retention rules combine as predicates, no childless branches, foreign leaves satisfy windows, CreateUntil survives JSON ([4e4d955](https://github.com/bedrock-python/pg-partsmith/commit/4e4d955d133e0f7307242d11eabbd97f4d781ce6))


### Documentation

* 1.0 guides, RFC companions, changelog, and the sync mirror scripts ([14252f9](https://github.com/bedrock-python/pg-partsmith/commit/14252f9132632fc5ae0170228fb8e3f73b7bcc10))
* hand any page over as Markdown, from the page itself ([994a06b](https://github.com/bedrock-python/pg-partsmith/commit/994a06bfa464485e46b5984381603db0daaf2c22))
* keep the Markdown emitter inside the linter's rules ([4715336](https://github.com/bedrock-python/pg-partsmith/commit/47153362bbadf3d81b6c14a956cec19dbd686154))
* leaf backends, sliding LIST, data movement and FK semantics; CI matrix on PostgreSQL 15/16/17 ([1235a3e](https://github.com/bedrock-python/pg-partsmith/commit/1235a3eaa00ecaa08d18f99d59fd3485f1c6f652))
* let a page decline the Copy page control ([1a76685](https://github.com/bedrock-python/pg-partsmith/commit/1a76685502e3794b64559e347d2e922fe5bdb713))
* name every way a sequence outlives the move that set it ([ae58bbb](https://github.com/bedrock-python/pg-partsmith/commit/ae58bbb728f2fb56288f8b3707f9e9fff3f23567))
* one page of context for AI agents, with a map of the rest ([3c1474a](https://github.com/bedrock-python/pg-partsmith/commit/3c1474ac2b12d93a6813b30255de2fa6a1c53ac2))
* rebuild the documentation around tutorials, concepts, how-to guides and reference ([0baa5b0](https://github.com/bedrock-python/pg-partsmith/commit/0baa5b006f1a4d6c3360576f650ad7f0216008ed))
* say on the front page that a page for agents exists ([0fbf947](https://github.com/bedrock-python/pg-partsmith/commit/0fbf94758d4cadade0bc9681d4d513c44d287e00))


### Miscellaneous Chores

* cut 1.0.0 ([7a2c6d0](https://github.com/bedrock-python/pg-partsmith/commit/7a2c6d0c9a57d753267f6f2ccb4040b98250ab52))

## 1.0.0 — the long version

Version 1.0 rebuilds the core around four separated concerns — partition scheme, range
boundaries, lifecycle policy, and a maintenance plan — after reading how ten production
systems (GlitchTip, GitLab, Centrifugo, PGMQ, Hatchet, pg-trx-outbox, Hookdeck Outpost,
ColdFront, pg_partman, pg_clickhouse) manage PostgreSQL partitions. See
[RFC 0001](https://bedrock-python.github.io/pg-partsmith/design/rfc-0001-partition-schemes/), the
[OSS research](https://bedrock-python.github.io/pg-partsmith/design/oss-research/) and the
[verified PostgreSQL semantics](https://bedrock-python.github.io/pg-partsmith/design/postgresql-semantics/).

### ⚠ BREAKING CHANGES

- `PartitionLifecycleService(repo, metadata, locks, hooks=...)` no longer takes a
  `period_calculator`; the calendar, timezone and codec live in the config's
  `TimeBoundaries`. `PostgresMetadataProvider(boundary_codec=...)` is only needed for
  `is_partition_closed`.
- Nested and static topologies are spelled as a scheme: `subpartition=HashSubpartitionSpec(...)`
  → `subpartition=HashPartitioning(key=..., modulus=...)` or `scheme=RangePartitioning(..., child=...)`;
  `root_layout=...` + `HASH_BASED`/`VALUE_BASED` → `scheme=HashPartitioning(...)` /
  `ListPartitioning(...)`. `HashSubpartitionSpec`, `ListSubpartitionSpec`, `SubpartitionSpec`,
  `root_layout` and `auto_attach_after_create` are removed (a partition is always attached
  last, after its subtree).
- Hooks: `before_create(config, partition: PartitionInfo)` replaces
  `before_create(config, partition_name, from_value, to_value)`. Hooks fire once per partition
  directly under the root (also for root `HASH`/`LIST` members).
- Repository protocol: `create_table_like(template, name, partition_by)`,
  `attach_partition(parent, name, bounds, *, key_arity=1)`, `detach_partition(parent, name,
  *, mode=DetachMode.AUTO)`, `drop_partition(name, *, expected_oid=None)`,
  `reconcile_default_rows(..., key_columns=...)` replace `create_partition`, `create_branch`,
  `create_subpartition_table`, `attach_subpartition`, `attach_composite_partition` and the
  `concurrent=` flag. Metadata protocol: `get_actual_tree`, `measure`, `get_partition_tree`,
  `get_relation_oid`, `get_key_high_water_mark` replace the `NestedPartitionMetadata` /
  `CompositeKeyMetadata` capability protocols.
- `pg_partsmith.subpartition_plan` and `pg_partsmith.pruning_rules` are removed;
  `SubpartitionPlan` / `TopologyFinding` / `TopologyReason` / `plan_subpartitions` become
  `MaintenancePlan` / `Finding` / `FindingReason` / `plan_maintenance`.
  `service.reconcile_subpartitions()` is `service.reconcile()`.
- Removed with their feature: `SubpartitionAction`, `SubpartitionBounds` and
  `SubpartitionReconcileResult` (root imports), `SubpartitionSpecBase`
  (`pg_partsmith.entities`); `ListGroup` moved to `pg_partsmith.scheme` and stays
  exported from `pg_partsmith`.
- `Period` and `PartitionGranularity` moved to `pg_partsmith.periods` (still re-exported from
  `pg_partsmith` and `pg_partsmith.entities`). Custom calculators implement `period_at(instant)`
  (`current_period()` is derived from it; a calculator without it still works, slower).
- `ensure_partitions` returns the created `PartitionInfo`s built with their whole subtree;
  `get_partitions_for_pruning` also lists detached orphans past their grace.
- Ownership: an attached partition whose bounds are not a window of the scheme's grid (nor
  inside one) is reported as `unmanaged_partition` and never detached or dropped. In 0.x any
  attached partition with an old upper bound was pruned.
- `PartitionStrategy` gains `NUMERIC_BASED`; `MaintenanceIssueStep` gains `ATTACH` and `MOVE`;
  `UnsupportedCapabilityError` is removed with the capability protocols it served.
- Repository protocol additions a custom implementation has to provide:
  `create_table_like(..., physical=) -> int` and `create_foreign_table_like -> int`
  (both return the created relation's OID, read in the creating transaction),
  `move_rows`,
  `reconcile_default_rows(..., limit=, expected_target_oid=)`,
  `attach_partition(..., expected_oid=, expected_parent_oid=)`,
  `detach_partition(..., expected_oid=)`,
  `drop_partition(..., drain_into=) -> int`; metadata protocol: `get_leading_key_minimum`,
  `get_relation_kind`. `partition_exists` now reports foreign tables too.

### Added

- `scheme=` composition: `RangePartitioning`, `HashPartitioning`, `ListPartitioning`
  nest to any depth (`RANGE → LIST → HASH`, `LIST → RANGE`, root `HASH`/`LIST`), with composite
  keys, per-level naming and 63-byte name budgets.
- `NumericBoundaries(step, origin)`: integer `RANGE` windows with the cursor read from
  `max(key)` or the serial/identity sequence (`CursorSource`).
- `LifecyclePolicy`: creation `CreateAhead`, `CreateUntil(position)`, `CreateNextIf(predicate)`;
  retention `KeepNewest`, `KeepFor(age)`, `KeepBehind(distance)`, `ExpireIf(predicate)` with
  `AllOf`/`AnyOf`/`Not`; predicates `SizeAbove`, `RowsAbove`, `WindowAgeAbove`, `SqlPredicate`,
  `Callback`; `DetachMode.AUTO/CONCURRENT/BLOCKING`; `DropAfter(grace, when)`, `DropNever`.
  Facts (sizes, row estimates, SQL answers) are gathered only when a policy asks.
- `service.plan()` (read-only, lock-free), `service.apply(plan)`, `service.maintain()`;
  `MaintenancePlan` with typed operations (`CreatePartition`, `AttachPartition`,
  `DetachPartition`, `DropPartition`), reasons, sizes, findings with severities,
  `without()`/`only()` filters, `describe()` and JSON round-trip; `MaintenanceResult.plan`,
  `attached_count`.
- Destructive revalidation: detach and drop check the relation's OID (and attachment /
  marker) at apply time; a recreated table raises `PlanStaleError`.
- The orphan marker records the detach instant (`pg-partsmith:detached-at=`), which grace
  periods are measured from; orphans whose window is wanted again are re-attached.
- `ActualTree` / `PartitionNode` carry `oid`, `relkind` (foreign tables are inspected and
  never touched), `detach_pending`, `facts`; `DetachedPartition` carries `detached_at`.
- `EpochBoundaryCodec`; codecs and boundaries addressable by name in serialized configs;
  `PartitionTableSettings` accepts `scheme` / `lifecycle` JSON, `tz`, `boundary_codec`.
- Every lifecycle rule with one defining value takes it positionally: `KeepNewest(12)`,
  `CreateAhead(3)`, `DropAfter(timedelta(days=7))`, `ExpireIf(SqlPredicate(...))`.
- An interrupted `DETACH CONCURRENTLY` is completed by the next `maintain()` call, which
  re-plans under the same lock and re-attaches the finalized table when its window is still
  wanted — or retires it under the drop policy (`detach_finalize`). The tree is read with a
  recursive walk over `pg_inherits` instead of `pg_partition_tree`, which omits such a
  partition and takes `ACCESS SHARE` on every member; inspection now takes no relation lock.
- Row moves (DEFAULT reconciliation, `partition_data`, `unpartition`) fail closed under
  incoming foreign keys (`RowMoveRefusedError`, recorded as a `move` issue): a destructive
  `ON DELETE` action (`CASCADE`, `SET NULL`, `SET DEFAULT`) is refused up front, and a
  *referenced* row is refused atomically by PostgreSQL itself — mid-move it is outside the
  referenced tree, so even `NO ACTION` cannot pass, and `SET CONSTRAINTS ALL IMMEDIATE`
  keeps a `DEFERRABLE` check from escaping to commit unhandled. Generated columns are
  recomputed rather than copied; identity values survive a move (`OVERRIDING SYSTEM
  VALUE`) and the target's identity sequences are advanced past the ids they could still
  reissue — along the sequence's own path and direction, inside its declared range. A
  destination whose sequence cycles (any id in its range: a wraparound restarts it at the
  far end and can move it onto values its increments skipped), would have nothing left to
  issue, or has handed cached blocks out to sessions — which keep them where no `setval`
  reaches, while PostgreSQL publishes only the newest allocation, so everything issued
  since `START` counts — is refused instead of quietly broken. Every one of those
  questions is about the ids the move carries, which the move statement hands over as it
  places them: a row the destination already held came from that sequence itself and
  never refuses a move. A destination with several identity columns is decided in full
  before any of its sequences moves, since PostgreSQL does not roll a sequence back with
  the transaction.
- `unpartition` refuses a destination that is the root, a member or an orphan of its tree,
  or a partition of any other table; empties this library's detached partitions too; and —
  under `drop_emptied` — detaches, drains the tail, and lets the drop move whatever
  arrived late in the same transaction under the drop's lock, counted in `rows_moved`, so
  a row committed between the last batch and the drop is moved, never dropped.
  `partition_data` fills and re-attaches a matching detached partition instead of giving
  up on its window, and drains DEFAULT into foreign leaves. Every fill target is held by
  OID — read in the creating transaction for a fresh partition, the planned orphan's
  otherwise — verified on every batch (before and, for an unlockable foreign target,
  after the statement, rolling it back) and re-checked by the `ATTACH` itself, which also
  verifies the parent it attaches into, so a branch replaced while its subtree is being
  built never gains the children. A compensating move back into a DEFAULT partition
  checks both ends. A name that resolves to nothing where an identity was expected is a
  stale plan, never name-only execution.
- An OID-guarded detach fails closed (`detach_partition(expected_oid=)`): the blocking
  form checks identity and attachment in its own transaction and re-checks after the
  statement, rolling a swap back marker and all; the concurrent form pins the relation
  (`pg_partsmith.aio.repositories.pin`) while it is verified and marked, releasing only
  once the statement's own backend is queued for the partition's lock (the sync mirror
  pins through a worker thread); a statement that has not got that far when the DDL
  timeout expires is cancelled and waited for before the pin is released. A foreign
  relation cannot be pinned and uses the transactional blocking form. Finalizing an interrupted detach is transactional too:
  lock, checks, marker and `FINALIZE` commit or roll back together. Attaches carry the same identity
  (`attach_partition(expected_oid=)`, re-checked after `ATTACH` in its transaction), and
  `reconcile_default_rows(expected_target_oid=)` verifies the fill target under the
  move's lock. Re-attaching removes the orphan marker in the attach's transaction, so a
  later detach starts a fresh grace period instead of inheriting the old instant.
- Under `DropNever` a matching detached table is never re-attached; a wanted window whose
  name it holds is reported (`name_unusable`) instead of recreated over it.
- User-authored models — `TablePartitionConfig`, schemes, boundaries, leaves, lifecycle
  rules — refuse unknown fields (`extra="forbid"`): a misspelled `garce` or `retentoin` no
  longer validates into a destructive default. `KeepFor` / `WindowAgeAbove` refuse
  negative ages.
- `CreatePartition.capabilities` and `AttachPartition.capabilities` name the
  `SHARE ROW EXCLUSIVE` locks PostgreSQL takes on tables referencing the parent.
- Sliding `LIST`: `ListPartitioning(key, sequence=IntegerSequence(start))` is a progression
  level — one integer value per partition, the cursor read off the newest member
  (`CursorSource.NEWEST_MEMBER`), rotated with `CreateNextIf` or bounded with `CreateUntil`;
  `ensure_partition(s)` accept positions on the root's axis (a value, an instant).
- Leaf backends: `config.leaves = LocalLeaves(tablespace, storage_parameters,
  inherit_privileges)` (the default, plain) or `ForeignLeaves(server, options)` — leaves as
  foreign tables with templated options; refused before any DDL on a parent with a unique
  index; foreign relations commented with `COMMENT ON FOREIGN TABLE` and dropped with
  `DROP FOREIGN TABLE`; a foreign partition is managed only under a `ForeignLeaves` config.
- Batched data movement: `service.partition_data(config, batch_rows, max_batches)` drains a
  DEFAULT partition into lifecycle partitions window by window (created detached, filled,
  attached); `service.unpartition(config, into, drop_emptied)` moves everything back into
  one table. `MigrationResult`; `PlanExecutor.create_partition(fill=...)`.
- Foreign-key aware retention: `Unreferenced()` (the condition PostgreSQL enforces on
  `DETACH`, `23503`), `FactKind.REFERENCES` / `PartitionFacts.referenced`; a refused detach
  is `PartitionReferencedError`, recorded as an issue while the run goes on; measured lock
  levels on referencing tables on `DetachPartition.capabilities`.
- `scripts/sync_mirror.py` regenerates `pg_partsmith.sync` from `pg_partsmith.aio`;
  `scripts/sync_tests_mirror.py` regenerates the sync integration suite.
- Tests: the Redis lock managers against a real Redis; the integration suites run on
  PostgreSQL 15, 16 and 17 in CI (`PG_PARTSMITH_TEST_PG_IMAGE`).
- Design docs: RFC 0001, OSS research with migration verdicts per project, PostgreSQL
  semantics verified on 15 and 17.

### Changed

- Documentation rebuilt around tutorials, concepts, how-to guides and reference pages,
  with plans and findings captured from a real PostgreSQL 17.
- Existing partitions are matched to windows by their catalog bounds, never by name; names
  are parsed only to recognise a detached orphan.
- Topology conflicts discovered while applying (`PartitionTopologyError`) are recorded as
  issues and never abort the run; other errors abort unless `continue_on_error`.
- `is_partition_closed`, DEFAULT reconciliation, attach-last subtree creation, hash
  convergence rules, fail-closed pruning of unreadable bounds, timezone alignment and the
  orphan marker keep their 0.5 semantics.

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
[Unreleased]: https://github.com/bedrock-python/pg-partsmith/compare/pg-partsmith-v0.5.0...HEAD
