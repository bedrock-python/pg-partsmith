# Final report: what the 1.0 research changed

The 1.0 work started as a question rather than a feature request: *if ten production
systems each wrote their own PostgreSQL partition manager, what did they all need, and
which of it belongs in a library?* This page is the answer in one place — findings,
abstractions, what changed, what is verified, what each of those projects would still have
to write themselves, and what is left.

The long forms live in [RFC 0001](rfc-0001-partition-schemes.md), the
[OSS research](oss-research.md), the [verified semantics](postgresql-semantics.md) and the
[1.0 API cheat sheet](api-cheatsheet-1.0.md).

## A. What the OSS systems actually do

Ten systems were read at a pinned commit each: GlitchTip, GitLab, Centrifugo, PGMQ,
Hatchet, pg-trx-outbox, Hookdeck Outpost, ColdFront, pg_partman and pg_clickhouse. Six
patterns recur across projects that never spoke to each other:

1. **Time partitioning over a key that is not a timestamp.** GlitchTip range-partitions
   UUIDv7 event ids; pg_partman ships `time_encoder`/`time_decoder` for exactly this. The
   calendar and the physical bound are two different things.
2. **Nested topology as a normal state, not an exotic one.** `RANGE → HASH` in GlitchTip
   and Hatchet, `LIST → RANGE` in ColdFront; root `HASH` with no time axis at all in
   Hatchet and pg-trx-outbox.
3. **History is heterogeneous and must stay that way.** A hash set built at modulus 4
   cannot be re-tiled to 2, and a leaf that predates a subpartitioning change cannot become
   a branch. Every mature manager leaves history alone and applies the new shape forward.
4. **Detach and drop are separate events.** GitLab keeps detached partitions for a week
   before dropping them; ColdFront's `detach` strategy never drops at all; GlitchTip
   exports before dropping.
5. **Lifecycle conditions are not only calendar age.** GitLab's `next_partition_if` /
   `detach_partition_if` decide on the data — the oldest row, whether anything is still
   pending — and PostgreSQL itself refuses to detach a partition whose rows are still
   referenced.
6. **Ownership is the unsolved problem everywhere.** Naming conventions (Centrifugo,
   GlitchTip), a registry table (pg_partman), or nothing at all (Outpost). Everyone is
   afraid of dropping a table someone else attached.

And one thing nobody had: operators asking, in Hatchet's issue #3424, simply to *see* what
maintenance is about to do and how big it is.

## B. The abstractions those patterns forced

Four concerns, separated, each replaceable without touching the others:

| Concern | Answers | Primitive |
|---|---|---|
| Topology | what levels exist and how each divides | `RangePartitioning` / `ListPartitioning` / `HashPartitioning`, composed through `child=` |
| Boundaries | where one partition ends and the next begins | `TimeBoundaries` / `NumericBoundaries`, plus a `RangeBoundaryCodec` for encoded keys |
| Lifecycle | when to create, expire, detach and drop | `LifecyclePolicy` of small composable rules |
| Reconciliation | what to do about the difference | `ActualTree` → `plan_maintenance` → `MaintenancePlan` → executor |

Three decisions did more work than the rest:

- **Bounds, not names, are the truth.** The catalog's `pg_get_expr(relpartbound, …)` decides
  what a partition is; names are generated and parsed only to recognise a detached orphan.
  This is also what makes adoption free: a tree another tool built is recognised by its
  bounds landing on the scheme's grid.
- **Ownership is derived, not recorded.** A partition aligned with the grid is managed; one
  that is not is `unmanaged_partition` — inspected, reported, never touched. No metadata
  table, no naming convention, and Centrifugo's hand-attached neighbours survive.
- **The plan is a value.** Typed operations with a reason, a safety class, optional facts
  and OIDs, serializable to JSON. Dry-run, audit, metrics and approval flows are then the
  same object, and `apply()` revalidates OIDs before anything destructive.

## C. What changed in the codebase

New pure modules — `scheme`, `boundaries`, `lifecycle`, `topology`, `plan`, `planner`,
`leaves`, `partition_bounds`, `catalog_queries` — hold the whole domain; the `aio` and
`sync` packages carry only I/O, with `pg_partsmith/sync` generated from `pg_partsmith/aio`
by `scripts/sync_mirror.py`. The old `subpartition_plan` and `pruning_rules` modules are
gone, replaced by one recursive planner over the whole tree.

Service surface, both mirrors: `inspect`, `plan`, `apply`, `maintain`, `reconcile`,
`ensure_partitions`, `partition_data`, `unpartition`, `get_partitions_for_pruning`.

The full inventory — every removed name, every protocol change, every migration step — is
the "1.0.0 — the long version" section of the [changelog](../changelog.md).

## D. The public API, before and after

The simple case is unchanged; that was a hard requirement:

```python
TablePartitionConfig(
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,
    retention_count=12,
)
```

The composed form is what the flat fields expand into, and the only way to say anything the
flat fields cannot:

```python
TablePartitionConfig(
    table_name="issue_events",
    scheme=RangePartitioning(
        key="id",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="uuidv7"),
        child=HashPartitioning(key="organization_id", modulus=2),
    ),
    lifecycle=LifecyclePolicy(
        creation=CreateAhead(count=4),
        retention=KeepFor(timedelta(days=90)),
        detach=DetachMode.CONCURRENTLY,
        drop=DropAfter(grace=timedelta(days=7)),
    ),
)
```

Side-by-side translations of every 0.x form are in the
[API cheat sheet](api-cheatsheet-1.0.md); the runnable shapes are in the
[recipes](../guide/recipes.md).

## E. PostgreSQL behaviour that was measured, not assumed

On `postgres:15-alpine`, `16` and `17` via testcontainers, with the load-bearing facts
re-asserted by the integration suite on every CI run:

- transaction rules for each DDL form, including the two ways `DETACH … CONCURRENTLY` is
  refused, and the pending-detach state it leaves behind (`FINALIZE` is the only way out);
- lock levels for every statement the executor issues — which is why leaves are built
  standalone with `LIKE` and attached last, and why a converged tree issues no DDL;
- subtree semantics: a detached branch keeps its children; `DROP TABLE branch` takes the
  whole subtree without `CASCADE`;
- foreign tables as partitions: what is refused, what works, what `DROP` does;
- foreign keys across 98 scenarios on 15, 16 and 17 — identical behaviour, and the exact
  error a referenced partition's detach raises;
- overlap, gap and modulus-compatibility errors, and how every key type renders its bounds;
- clock changes: a Berlin day that lasts 23 or 25 hours, and where its rows land;
- query pruning: what prunes at planning time, what prunes at executor start, and what does
  not prune at all.

All of it, with the error codes and the plans: [verified semantics](postgresql-semantics.md).

## F. Migration matrix

Could each researched system drop its custom partition code for `pg-partsmith`?

| Project | What it wrote itself | Replaced by | Still application code |
|---|---|---|---|
| **GlitchTip** | `PartitionManager`: weekly UUIDv7 `RANGE` → `HASH(organization_id)`, gap repair, modulus history, legacy leaves, export-then-drop | the whole manager: `codec="uuidv7"`, `child=HashPartitioning(...)`, the convergence rules, `DropAfter`, `before_drop` | query-layer routing (UUIDv7 predicates, org filters); `min_uuid_for` is exposed for it |
| **GitLab** | `PartitionManager`, sliding LIST, `next_partition_if`/`detach_partition_if`, detached-partition table with `drop_after` | date and int `RANGE`, `ListPartitioning(sequence=…)`, `CreateNextIf`/`ExpireIf`/`SqlPredicate`, `DropAfter(grace=…)`, `Unreferenced()` | the column-DEFAULT routing invariant; a lock_timeout *ladder* on detach; `ANALYZE` after create (a hook) |
| **Centrifugo** | `Partitioner`: daily `RANGE`, lookahead, retention, name-based ownership | `CreateAhead(lookahead + 1)` + `KeepFor`; ownership from bounds protects the same hand-attached partitions | nothing partition-related |
| **PGMQ** | `pg_partman` dependency: numeric `RANGE`, premake, retention, BGW | `NumericBoundaries(step=…)` + `CreateAhead` + `KeepBehind` — no extension, no `shared_preload_libraries` | the queue logic itself, untouched |
| **Hatchet** | 9 daily `RANGE` tables + root `HASH(task_id)`, lease row, manual size checks | the lifecycle, the root hash set, the advisory lock, and `plan()` with `SizeAbove`/`RowsAbove` for the visibility issue #3424 asked for | per-partition reloptions (an `after_create` hook) |
| **pg-trx-outbox** | hand-written `HASH(key)` DDL in the README; no maintenance at all | `HashPartitioning(key="key", modulus=3, name_suffix="_{remainder}")`, created and repaired idempotently; `get_actual_tree` gives consumers the `(modulus, remainder) → name` map | choosing which remainder a worker consumes |
| **Hookdeck Outpost** | nothing yet — issue #249 open since 2025, "not on the roadmap" | `CreateUntil(...)` + `KeepFor(...)` at start-up under the advisory lock; DEFAULT rows move into each new month as it is attached | nothing partition-related |
| **ColdFront** | `LIST → RANGE` provisioning, `detach` expiration strategy, concurrent detach fanned out to peers | `DetachMode`, `DropNever`, nested `LIST → RANGE`, `before_drop` for export-verify-drop, codecs | the peer fan-out of non-transactional DDL across a replicated cluster |
| **pg_partman** | the reference catalogue, not a migration target | premake, retention, encoded keys, subpartitioning, DEFAULT reconciliation, adoption, `partition_data`/`unpartition`, template properties via `LocalLeaves` | BGW scheduling and GUCs stay out of core by design |
| **pg_clickhouse** | foreign partitions beside local ones, offload helper | `ForeignLeaves(server, options)` manages foreign leaves through the whole lifecycle; a foreign leaf under a local config is reported and never touched | offloading an *existing* local partition to a foreign one (a hook-driven workflow) |

## G. What is not covered

Deliberate omissions, from the RFC and unchanged since:

- **`MINVALUE`/`MAXVALUE` catch-all partitions** are inspected and preserved, never created.
- **A lock_timeout ladder** exists for drop (`drop_lock_timeout_ms` plus retries); detach has
  a single DDL timeout.
- **`ANALYZE` after create**, statistics and replica-identity propagation, index or
  publication drops on detach: hooks, not core.
- **A standby guard** (`pg_is_in_recovery()`) and a **DEFAULT-partition monitor**
  (pg_partman's `check_default()`) are not implemented.
- **Rewriting history** — re-tiling a hash set, converting a legacy leaf into a branch — is
  refused by design. Changing the modulus changes future partitions only.
- **Offloading an existing local partition** to a foreign one is a hook workflow, not an
  operation.
- **Out of scope on purpose:** a scheduler, query rewriting, an ORM integration, an S3 or
  ClickHouse client, and any requirement for a PostgreSQL extension or superuser.

## H. Roadmap

Proposals, in the order they look worth doing — none of them requires a redesign, which was
the point of the architecture:

- **Next.** A standby guard that refuses maintenance on a replica; a lock_timeout ladder for
  detach to match the one drop has; a reported count of rows sitting in a DEFAULT partition,
  which is the one operational question the plan cannot currently answer.
- **After that.** More shipped codecs (ULID, Snowflake) — each is a ten-line
  `RangeBoundaryCodec`, and shipping them makes the abstraction visible; an operation for
  converting a local leaf into a foreign one, closing the pg_clickhouse offload path.
- **Later, if asked for.** Explicit migration tooling for a topology change that *does* need
  a rewrite (hash 4 → 8 across history), kept separate from maintenance so that the safety
  rule — maintenance never rewrites history — stays absolute.
