# RFC 0001 — Partition schemes, lifecycle policies and the maintenance plan

**Status:** accepted for 1.0.0 · **Scope:** core architecture of `pg-partsmith`
· **Companion:** [PostgreSQL semantics verified on 15 and 17](postgresql-semantics.md),
[OSS research](oss-research.md)

## 0. Why this RFC exists

`pg-partsmith` 0.5 manages one shape well: a `RANGE` root over a calendar dimension, with
create-ahead, count-based retention and an optional `HASH`/`LIST` subtree inside each
period. Everything else that real applications do with PostgreSQL declarative
partitioning — integer ranges, hash roots that never age, list partitions rotated by
application state, detach-now-drop-later, dry runs, size-aware deletion — either needs
a special case or cannot be expressed at all.

Ten production systems were read for this RFC (GlitchTip, GitLab, Centrifugo, PGMQ,
Hatchet, pg-trx-outbox, Hookdeck Outpost, ColdFront, pg_partman, pg_clickhouse). None of
them needs an application-specific concept from the library. What they need, between
them, is that four independent concerns stop being coupled through one
`TablePartitionConfig`:

| Concern | Question it answers | 0.5 |
|---|---|---|
| **Topology** | Which levels exist, by which method, on which key? | `partition_type` + `partition_strategy` + `root_layout` + `subpartition` |
| **Boundaries** | What are the bounds of the partitions at a level? | `granularity` + a `PeriodCalculator` wired into the service |
| **Lifecycle policy** | When is a partition created, detached, dropped? | `create_ahead_count`, `retention_count` |
| **Execution** | How is the difference between desired and actual applied? | one service method per lifecycle step |

This RFC separates them. The result is version **1.0.0**: a small number of composable
abstractions (`PartitionScheme`, `RangeBoundaries`, `LifecyclePolicy`,
`MaintenancePlan`) rather than fifty options, with the simple monthly table still
configured in five lines.

## 1. Current architecture (0.5.0)

### 1.1 Modules

```text
entities.py            TablePartitionConfig, Period, PartitionInfo, MaintenanceResult
topology.py            PartitionType, *Bounds, HashSubpartitionSpec, ListSubpartitionSpec, PartitionNode
subpartition_plan.py   plan_subpartitions(spec, node) → SubpartitionPlan(actions, findings)
pruning_rules.py       select_partitions_to_prune(calculator, tz, config, partitions)
boundaries.py          RangeBoundaryCodec, UUIDv7BoundaryCodec
partition_bounds.py    parse_partition_bounds / parse_range_boundaries / parse_boundary_literal
strategies/            BasePeriodCalculator + Hour/Day/Week/Month/Quarter/Year
aio/, sync/            service.py (façade), services/{creation,subpartitions,pruning,detachment,
                       deletion,validation}.py, repositories/{creator,remover,resolver,fk_manager}.py,
                       metadata.py, maintainer.py, hooks.py, lock/{postgres,redis}.py, protocols.py
```

### 1.2 Sequence of `maintain_lifecycle` (0.5)

```text
maintainer.run_maintenance(config)
  └─ service.maintain_lifecycle(config)
       ├─ locks.acquire_lock(parent)                      advisory lock, dedicated AUTOCOMMIT connection
       ├─ validation.validate_config(config)              partition type / key / unique-constraint checks
       ├─ [static root] subpartitions.reconcile(config)   plan_subpartitions(root_layout, tree) → materialize
       ├─ metadata.list_partitions(parent)                attached children + marker-tagged orphans
       ├─ creation.create_future_partitions(config)       periods = calculator.next_periods(create_ahead)
       │     for each period: name = calculator.format_partition_name(...)
       │       existing?  → (re)attach if detached, converge branch
       │       missing?   → before_create → CREATE (LIKE parent) [+ subtree, attach last] → ATTACH
       │                    (DEFAULT reconciliation, lost-race tolerance) → after_create
       ├─ pruning.identify_partitions_to_prune(config)    cutoff = period_before(current, retention-1)
       │     boundary-based (decode via codec/tz), name fallback for orphans, fail closed otherwise
       ├─ subpartitions.reconcile(config, exclude=prune)  gap filling per branch (hash/list rules)
       ├─ detachment.detach_old_partitions(...)           before_detach → COMMENT marker → DETACH [CONCURRENTLY] → after_detach
       └─ deletion.drop_detached_partitions(...)          before_drop → LOCK + revalidate + DROP → after_drop
```

### 1.3 What is strong and must survive

- Per-statement transactions with **attach-last** ordering for subtrees (no partially
  covering branch is ever reachable from the root).
- DEFAULT reconciliation with row restore on a failed attach; NULL trailing keys left in
  DEFAULT; lost attach races told apart from real conflicts by re-reading bounds.
- Drop revalidation under `ACCESS EXCLUSIVE`; orphan `COMMENT` marker as ownership evidence.
- Hash convergence rules: fill gaps at the branch's *own* modulus, preserve a complete set
  at another modulus, compute mixed-modulus coverage over the LCM, refuse to guess.
- Fail-closed pruning: undecodable bound → skip with a warning; `MAXVALUE`/`infinity` never
  pruned; identifier length checked before DDL because PostgreSQL truncates silently.
- Timezone alignment between calculator and DDL session; boundary codecs for UUIDv7.
- Full aio/sync parity built on shared pure modules.

### 1.4 What is limiting

1. `Period` is calendar-only, so **numeric ranges** (PGMQ, GitLab `int_range`) cannot be
   expressed, and `PeriodCalculator` is both the boundary rule *and* wiring passed to the
   service — the config's `granularity` and the service's calculator are two sources of
   truth that are never checked against each other.
2. `TablePartitionConfig` carries `partition_type`, `partition_strategy`, `granularity`,
   `root_layout` and `subpartition` with cross-field rules; the "giant config object"
   the target architecture warns against.
3. Lifecycle is two integers. No ensure-until, no age-based retention, no grace period
   between detach and drop, no state-dependent creation or detachment (GitLab
   `next_partition_if` / `detach_partition_if`), no size awareness.
4. There is a planner only for the nested levels (`plan_subpartitions`); create-ahead and
   retention are decided *while executing*. There is no `MaintenancePlan`, no dry run, and
   nothing serializable an operator could read before DDL runs.
5. Ownership is decided by the orphan marker only. Every attached child of the parent is
   treated as a lifecycle partition: a DBA's hand-attached archive whose bounds are old is
   detached and dropped by retention.
6. Existing partitions are matched to periods by **parsing names**, not by catalog bounds.
7. Destructive operations are revalidated by name, not by OID.
8. Detach mode is an implicit `concurrent=True` with a silent fallback.

## 2. Findings from production systems

See [oss-research.md](oss-research.md) for the per-project reports with source references.
The generic requirements they converge on:

| Requirement | Seen in | Generic primitive |
|---|---|---|
| Time partitioning over a non-timestamp key (UUIDv7, epoch, text) | GlitchTip, pg_partman `time_encoder` | `RangeBoundaryCodec` (exists) |
| `RANGE → HASH` nested tree; hash gap repair; modulus evolution; legacy leaves | GlitchTip | scheme composition + convergence rules (exist) |
| Root `HASH` that never ages | Hatchet, pg-trx-outbox | `HashPartitioning` as a root scheme (exists as `root_layout`) |
| Integer `RANGE` stepping by a fixed width | PGMQ, GitLab, pg_partman `id` sets | `NumericBoundaries` (new) |
| Sequential `LIST` rotated by application state | GitLab sliding list | `ListPartitioning(sequence=…)` + predicate-driven creation (model now, planner P2.5) |
| Detach now, drop after a grace period | GitLab `RETAIN_DETACHED_PARTITIONS_FOR` | `DropAfter(grace)` + detached-at stamp in the orphan marker (new) |
| Create until a horizon, not a count | Hookdeck Outpost | `CreateUntil(position)` (new) |
| Only touch what you recognise; leave user-attached partitions alone | Centrifugo | catalog-derived ownership: aligned = managed, else unmanaged (new) |
| Know what will happen, how big it is, and why, before DDL | Hatchet users | `MaintenancePlan` with reasons, sizes and safety (new) |
| Operations that cannot run in a transaction | ColdFront, `DETACH CONCURRENTLY` | operation capabilities on the plan (new) |
| Foreign tables in the tree | pg_clickhouse | `relkind` on nodes, never planned for, never dropped (new) |
| Batch data movement, template properties | pg_partman | out of core; extension points kept open |

## 3. Gap matrix

| Capability | 0.5 | Needed by | 1.0 primitive | Phase |
|---|---|---|---|---|
| UUIDv7 / encoded bounds | yes | GlitchTip, pg_partman | `RangeBoundaryCodec` | done |
| `RANGE → HASH`, `→ LIST`, deeper | yes | GlitchTip, SaaS logs | `PartitionScheme.child` | P0 (reshaped) |
| Root `HASH` / `LIST` | yes | Hatchet, outbox | `HashPartitioning` / `ListPartitioning` at root | P0 (reshaped) |
| Composite keys | yes | — | `PartitionKey` with several columns | P0 |
| Numeric `RANGE` | **no** | PGMQ, GitLab | `NumericBoundaries` + cursor from `max(key)` / sequence | P1 |
| Match partitions by catalog bounds, not names | partial | Centrifugo, adoption | window decoding in the planner | P0 |
| Plan / dry-run / serializable output | partial (nested only) | Hatchet, DBAs | `MaintenancePlan` + `plan()` / `apply()` | P0 |
| Ownership classification | marker only | Centrifugo | `Ownership` on every node | P0 |
| OID revalidation before DETACH/DROP | name only | safety | `oid` on operations, checked at apply | P0 |
| Ensure-until creation | no | Hookdeck | `CreateUntil` | P2 |
| Age-based retention | no | GitLab, Hatchet | `KeepFor` | P2 |
| Detach grace period | no | GitLab | `DropAfter(grace)` | P2 |
| Detach mode explicit | implicit | ColdFront | `DetachMode` | P2 |
| Size / row facts on demand | no | Hatchet, GitLab | `PartitionFacts`, `SizeAbove`, `RowsAbove` | P2 |
| State-dependent create/detach | no | GitLab | `CreateNextIf`, `DetachIf`, `SqlPredicate` | P2 |
| Sliding `LIST` | no | GitLab | `ListPartitioning(sequence=…)` | P2.5 |
| Foreign leaves introspection | crashes on `relkind='f'` in existence checks | pg_clickhouse | `RelationKind` on nodes | P0 |
| Adopt existing trees | partial (`adopt_partition` for detached) | migrations | alignment-based ownership + `adopt_partition` | P0/P3 |
| Batch migration of monolithic tables | no | pg_partman | future module | P3 |
| Template properties / tablespaces | no | pg_partman | hooks now; initializer later | P4 |

## 4. Domain model

### 4.1 Four layers

```text
config (what)        PartitionScheme ─ RangeBoundaries ─ LifecyclePolicy       pure, serializable
introspection (is)   ActualTree ← PostgresMetadataProvider                     one catalog round-trip
planning (diff)      plan_maintenance(config, actual, context) → MaintenancePlan  pure, no IO
execution (do)       apply(plan) → MaintenanceResult                            per-statement DDL
```

Everything above the execution layer is IO-free and shared verbatim by `pg_partsmith.aio`
and `pg_partsmith.sync`. The mirrors contain only what talks to a database: the
introspector, the executor, the locks.

### 4.2 Topology — `PartitionScheme`

```python
RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=MONTH), child=None)
HashPartitioning(key="tenant_id", modulus=16, name_suffix="__h{remainder}", child=None)
ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("de", "fr")),), include_default=True)
```

A scheme is one level of the tree plus, optionally, the level below it. `key` is the
PostgreSQL partition key of that level (one or several columns). The same class describes
a root and a nested level: `HashPartitioning` at the root is Hatchet's task table,
`HashPartitioning` as `child` of a `RangePartitioning` is GlitchTip's event table.

Rules enforced by the model, all of them PostgreSQL's: a `LIST` key has one column; every
level partitions on a fresh column; depth is bounded; generated names fit 63 bytes at
every level (PostgreSQL truncates silently, which would collapse two siblings onto one
name).

Two kinds of level fall out of this:

- a **progression level** produces an ordered, open-ended sequence of slots — `RANGE`
  windows, or a sliding `LIST` value sequence. It is the *lifecycle dimension*: partitions
  are created ahead of a cursor and expire behind it. Its partition is the lifecycle unit,
  subtree included.
- a **set level** produces a fixed, complete set — `HASH` buckets, static `LIST` groups.
  It is reconciled (missing members created) and never expires.

A progression level may appear below a set level (`LIST(tier) → RANGE(created_at)`): each
branch then runs its own lifecycle. The model allows it and the planner is recursive over
it; the tests in 1.0 cover one such tree.

### 4.3 Boundaries — `RangeBoundaries`

A `RANGE` level needs a rule that turns a *position* on its axis into a half-open
`Window(start, end)`, steps between adjacent windows, renders a window as the two literals
PostgreSQL compares against, reads a catalog literal back into a position, and names the
window. Three implementations ship:

| Strategy | Axis | Windows | Physical literals |
|---|---|---|---|
| `TimeBoundaries(granularity | calculator, tz, codec)` | instants | calendar periods via any `PeriodCalculator` | timestamps, or the codec's encoding (UUIDv7, epoch…) |
| `NumericBoundaries(step, origin)` | integers | `[origin + k·step, origin + (k+1)·step)` | integer literals |
| custom `RangeBoundaries` | anything comparable | yours | yours |

The existing period calculators are the time implementation; a custom calculator plugs in
through `TimeBoundaries(calculator=...)` and keeps working with every topology and codec.

A progression level's **cursor** is where "now" is on its axis: the clock for time, the
key's high-water mark (`max(key)` or the serial/identity sequence) for integers. The
introspector resolves it; the planner receives it in `PlanningContext`.

### 4.4 Lifecycle policy

```python
LifecyclePolicy(
    creation=CreateAhead(3),                    # or CreateUntil(datetime(2028, 1, 1)), CreateNextIf(pred)
    retention=KeepNewest(12),                   # or KeepFor(timedelta(days=90)), ExpireIf(pred), AllOf(...)
    detach=DetachMode.AUTO,                     # CONCURRENT | BLOCKING | AUTO (concurrent, blocking when a DEFAULT exists)
    drop=DropAfter(timedelta(days=7)),          # grace after detach; DropAfter(0) = same run; DropNever
)
```

Policies are **predicates over candidates**, evaluated by the pure planner. What a
predicate needs to know is declared up front (`required_facts`): the introspector
gathers exactly those facts — size, row estimate, the result of an `SqlPredicate` — and
nothing else. A monthly table with `KeepNewest` never pays for `pg_total_relation_size`.

A policy answers *eligible or not*; it never executes DDL. Ownership, safety and locking
stay with the core, which is what keeps a user predicate from turning into an accidental
`DROP TABLE`.

### 4.5 Actual tree and ownership

`PostgresMetadataProvider.get_actual_tree(table)` returns the tree from `pg_partition_tree`
plus the marker-tagged detached orphans, in one round-trip. Every node carries its
`oid`, `relkind` (table / partitioned / foreign), bounds parsed into the discriminated
union, its own partition key, and — on demand — `PartitionFacts`.

Ownership is derived from the catalog against the scheme, no metadata table:

| State | Meaning | Lifecycle may |
|---|---|---|
| `MANAGED` | attached and aligned with the scheme: a window on the boundaries' grid, a hash bucket, a list group | create below it, detach, drop |
| `UNMANAGED` | attached but not describable by the scheme: unaligned bounds, a foreign table, an expression key | inspect and report only |
| `DEFAULT` | the DEFAULT partition | reconcile rows out of it; never prune |
| `ORPHAN` | detached and marker-tagged (by us, or adopted) | drop when policy allows |

Alignment is the safe generalisation of "did we create it": a partition whose bounds are
exactly the window the scheme would have produced is indistinguishable from ours and is
treated as ours; a DBA's `events_archive_2000_2019` never aligns with a daily grid and is
never touched. `repo.adopt_partition` remains for detached legacy tables.

### 4.6 Maintenance plan

```python
plan = await service.plan(config)          # read-only, lock-free
for op in plan.operations:
    print(op.kind, op.target, op.reason, op.size_bytes)
result = await service.apply(plan)         # takes the lock, revalidates, executes
```

Operations are typed, ordered and nested where ordering matters
(`CreatePartition.children` are built before the parent is attached):

```text
CreatePartition   parent, name, bounds, partition_by (for a branch), children, reason
AttachPartition   re-attach a marker-tagged orphan whose window is wanted again
DetachPartition   name, oid, mode, reason (retention rule that fired)
DropPartition     name, oid, reason, detached_at, size_bytes
```

Every operation records its **reason** (`CREATE_AHEAD`, `RETENTION_EXPIRED`,
`GRACE_ELAPSED`, `HASH_GAP`, `LIST_GROUP_MISSING`, …) and its transactional
**capabilities** (`DETACH … CONCURRENTLY` cannot run inside a transaction block — verified).
What the planner refuses to do is not an operation but a **finding** with a reason and a
safety classification (`INFO` for expected steady states such as a preserved older modulus,
`WARNING` for what needs a human: mixed moduli leaving a gap, an unaligned partition
overlapping a wanted window, an expression key). Findings surface through
`MaintenanceResult.issues` exactly as in 0.5.

`plan.model_dump(mode="json")` is the wire format for CLIs, dashboards and audit logs.

### 4.7 Execution

`apply` keeps the transaction semantics of 0.5 — every statement commits on its own, a
branch is attached only once its subtree is complete — and adds **revalidation** before
anything destructive: the relation must still have the OID the plan saw, still be attached
(detach) or still be a marker-tagged orphan (drop). A table dropped and recreated under
the same name between plan and apply is left alone.

`maintain()` is `plan` + `apply` under one lock, and is what `PartitionMaintainer` and
`maintain_partitions` call. The 0.5 `skip_*` flags become plan filters.

## 5. API examples

### Ordinary monthly table (unchanged)

```python
config = TablePartitionConfig(
    schema="public", table_name="events",
    partition_column="created_at", granularity=PartitionGranularity.MONTH,
    create_ahead_count=3, retention_count=12,
)
```

`partition_type` / `partition_strategy` are still accepted and validated; they are no
longer required. The flat fields are sugar for:

```python
TablePartitionConfig(
    schema="public", table_name="events",
    scheme=RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=MONTH)),
    lifecycle=LifecyclePolicy(creation=CreateAhead(3), retention=KeepNewest(12)),
)
```

### Numeric queue (PGMQ-like)

```python
TablePartitionConfig(
    table_name="queue",
    scheme=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=100_000)),
    lifecycle=LifecyclePolicy(creation=CreateAhead(3), retention=KeepNewest(100)),
)
```

### GlitchTip-like event store

```python
TablePartitionConfig(
    table_name="issue_events",
    scheme=RangePartitioning(
        key="id",
        boundaries=TimeBoundaries(granularity=WEEK, codec=UUIDv7BoundaryCodec()),
        child=HashPartitioning(key="organization_id", modulus=2, name_suffix="_h{remainder}"),
    ),
    lifecycle=LifecyclePolicy(creation=CreateAhead(3), retention=KeepNewest(12)),
)
```

### Hatchet / outbox root hash

```python
TablePartitionConfig(table_name="tasks", scheme=HashPartitioning(key="task_id", modulus=8))
```

### SaaS audit logs

```python
scheme=RangePartitioning(
    key="created_at", boundaries=TimeBoundaries(granularity=MONTH, tz=ZoneInfo("Europe/Helsinki")),
    child=HashPartitioning(key="account_id", modulus=16),
)
```

### GitLab-like sliding list (P2.5)

```python
scheme=ListPartitioning(key="partition_id", sequence=IntegerSequence(start=100)),
lifecycle=LifecyclePolicy(
    creation=CreateNextIf(AnyOf(SizeAbove(10 * 2**30), OldestRowAgeAbove(timedelta(days=1)))),
    retention=ExpireIf(SqlPredicate("NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')")),
    drop=DropAfter(timedelta(days=7)),
)
```

### Cold tiering

```python
lifecycle=LifecyclePolicy(
    creation=CreateAhead(2), retention=KeepFor(timedelta(days=90)),
    drop=DropAfter(timedelta(days=7)),      # the before_drop hook verifies the archive and may abort
)
```

## 6. Backward compatibility

1.0.0 is a major release. What changes for existing code:

| 0.5 | 1.0 | Migration |
|---|---|---|
| `TablePartitionConfig(partition_type=, partition_strategy=, partition_column=, granularity=, create_ahead_count=, retention_count=)` | unchanged; `partition_type` / `partition_strategy` optional | none |
| `subpartition=HashSubpartitionSpec(column=, modulus=, subpartition=)` | `scheme=RangePartitioning(..., child=HashPartitioning(key=, modulus=, child=))` | rename (released 0.5.0 the same day; no known users) |
| `root_layout=HashSubpartitionSpec(...)` + `HASH_BASED` | `scheme=HashPartitioning(...)` | rename |
| `PartitionLifecycleService(period_calculator=...)` | calculator lives in `TimeBoundaries(calculator=...)`; the service no longer takes one | move one argument |
| `PostgresMetadataProvider(boundary_codec=...)` | codec lives in `TimeBoundaries(codec=...)`; the provider reads it from the config | remove one argument |
| hooks `before_create(config, partition_name, from_value, to_value)` | `before_create(config, partition: PartitionInfo)` | `partition.name`, `.from_value`, `.to_value` |
| `plan_subpartitions`, `pruning_rules`, `SubpartitionPlan`, `TopologyReason` | `plan_maintenance`, `MaintenancePlan`, `FindingReason` | new names |
| `MaintenanceResult` | same counters; gains `plan` | additive |
| orphan `COMMENT` marker | first line unchanged; a `detached-at` line is added | old orphans are read as "grace unknown → eligible" |

`PartitionTableSettings` keeps its flat fields and still produces a valid config.

## 7. PostgreSQL semantics (verified)

Measured on PostgreSQL 15.19 and 17.11 with the script in
[postgresql-semantics.md](postgresql-semantics.md). The findings the design relies on:

- `DETACH PARTITION … CONCURRENTLY` inside a transaction block → `25001`; with a DEFAULT
  partition present → `55000 cannot detach partitions concurrently when a default partition exists`.
  A statement timeout mid-way leaves `inhdetachpending = true`: the partition stays
  `relispartition`, is invisible through the parent, rejects its own rows (`23514`), a second
  `CONCURRENTLY` fails with `55000 already pending detach`, and only `… FINALIZE` completes it.
- Detaching a subpartitioned branch keeps its subtree intact and readable; `DROP TABLE` of a
  branch (attached or detached) drops its children without `CASCADE`.
- Lock levels: `CREATE TABLE … PARTITION OF` → `ACCESS EXCLUSIVE` on the parent;
  `ATTACH` → `SHARE UPDATE EXCLUSIVE` on the parent, `ACCESS EXCLUSIVE` on the child and on
  the DEFAULT partition; plain `DETACH` → `ACCESS EXCLUSIVE` on parent and child;
  `DROP` of a detached table → `ACCESS EXCLUSIVE` on that table only, of an attached
  partition → on the parent too; `CREATE TABLE … (LIKE parent)` → `ACCESS SHARE`;
  `COMMENT ON` → `SHARE UPDATE EXCLUSIVE`; `pg_partition_tree()` → `ACCESS SHARE` on every
  member; plain catalog reads, `pg_total_relation_size`, `reltuples` → no relation lock.
- A foreign table can be a partition only of an index-free parent; it appears in
  `pg_partition_tree` with `relkind = 'f'`; `DROP TABLE` / `COMMENT ON TABLE` on it fail with
  `42809 is not a table`; `ATTACH`/`DETACH` (also `CONCURRENTLY`) work; `pg_total_relation_size`
  is 0.
- Overlapping `RANGE`/`LIST`/`HASH` siblings → `42P17`; a hash modulus that is not a factor of
  the next larger one → `42P17`; a row in a range gap → `23514 no partition of relation … found for row`.
- Bound rendering: integer keys render quoted (`FROM ('0') TO ('100000')`, `('-100')`),
  `numeric` may render unquoted (`TO (100000.5)`), `MINVALUE`/`MAXVALUE` bare, `timestamptz`
  with offset, `timestamp`/`date` naive.
- `pg_total_relation_size` of a partitioned relation is 0: sizes are summed over the leaves of
  `pg_partition_tree`. `reltuples` is `-1` before the first `ANALYZE`; `n_live_tup` is 0 until
  the stats collector flushes.
- An OID changes when a table is dropped and recreated under the same name;
  `to_regclass` of a missing name is NULL.
- `LIST → RANGE` nesting is accepted; `CREATE TABLE IF NOT EXISTS … PARTITION OF` succeeds
  silently against a same-named relation with *different* bounds (which is why the library
  never uses it).

## 8. Implementation phases

Each phase is one mergeable PR keeping the whole suite green.

- **P0 — foundation (this PR):** `scheme`, `boundaries` strategies, `lifecycle` policies,
  `ActualTree` with ownership/relkind/oid, unified recursive planner, `MaintenancePlan`,
  executor with revalidation, `plan()`/`apply()`/`maintain()` on both mirrors; the flat
  config as sugar; docs and CHANGELOG; the full 0.5 behaviour re-verified by the ported
  tests.
- **P1 — numeric ranges:** `NumericBoundaries`, cursor from `max(key)`/sequence,
  integration tests for contiguity and retention. (Shipped with P0 where the planner is
  already generic.)
- **P2 — lifecycle policies:** `CreateUntil`, `KeepFor`, `DropAfter` grace with the
  detached-at stamp, `DetachMode`, `PartitionFacts` and `SizeAbove`/`RowsAbove`,
  `SqlPredicate`, `CreateNextIf` / `ExpireIf`.
- **P2.5 — sliding LIST:** `ListPartitioning(sequence=IntegerSequence(...))` as a
  progression level.
- **P3 — adoption and migration:** inspection report for an existing tree, batch data
  movement out of a monolithic table, `undo`.
- **P4 — physical realisation:** partition initializer (tablespace, storage parameters,
  grants), foreign leaves as a leaf backend.

## 9. Risks

| Risk | Mitigation |
|---|---|
| API complexity for the simple case | the flat fields stay; the composed form is opt-in |
| Catalog parsing of bounds across types and versions | one parser, exercised by integration tests on 15 and 17 with every key type this RFC mentions |
| Lock escalation | measured lock levels documented; converged tree issues zero DDL; `ATTACH` rather than `CREATE … PARTITION OF` on live parents |
| `DETACH CONCURRENTLY` semantics | not transactional by construction; pending state finalised; DEFAULT present → blocking fallback |
| Partial nested trees | attach-last ordering; next run converges |
| Identifier length | per-level name budget validated at config time and re-checked at plan time |
| Ownership ambiguity | alignment rule is conservative: not aligned → never destructive; findings explain |
| Historical topology drift | preserved by rule, reported as `INFO` |
| PostgreSQL version differences | 15 and 17 in the semantics script; capability checks explicit |
| Two sources of truth for boundaries | the calculator now lives in the config; the service has no `period_calculator` |
