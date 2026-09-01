# pg-partsmith for AI agents

> One page holding everything a coding assistant needs to configure and drive
> pg-partsmith correctly, plus a map of where the rest of the documentation keeps the
> details it leaves out. Give an agent this page rather than the whole site.

| | |
|---|---|
| Package | `pg-partsmith` on PyPI, import root `pg_partsmith` |
| Requires | Python 3.11+, PostgreSQL 15+ (tested on 15, 16 and 17), SQLAlchemy 2 |
| Install | `pip install pg-partsmith` · extras: `redis-locks`, `pydantic-settings` |
| Async | `pg_partsmith.aio`, on an `AsyncEngine` |
| Sync | `pg_partsmith.sync` — same class names, same arguments, no `await` |
| Source | <https://github.com/bedrock-python/pg-partsmith> |

## How to read this page

Every page of this site is also served as raw Markdown at its own URL with `.md` in
place of the trailing slash — this page is `/agents.md`, the foreign-key guide is
`/guide/foreign-keys.md` — so anything the map below points at can be fetched as plain
text rather than scraped out of HTML. The **Copy page** control at the top of a page does
the same thing for a human with a chat window open. The one exception is the API
reference: its Markdown is a list of instructions to a docstring renderer rather than the
API, so it carries neither the control nor a `.md` twin — read it as HTML, or read the
docstrings in the source.

Top to bottom before writing code. [Rules that hold or break the code](#rules-that-hold-or-break-the-code)
is the section correctness lives in — those are the things the library will not save you
from. Every name used below is in the public API; if you need something not listed here,
fetch the page the [documentation map](#documentation-map) points at rather than guessing
a method that sounds plausible.

## Scope

**It does** keep the partitions of a partitioned table in the shape you declare: create
ahead of the cursor, fill gaps in a set, detach what expired, drop after a grace period.
It reads the tree from the catalog, produces a plan you can inspect before applying it,
and moves rows in batches into partitions (`partition_data`) or back out into one table
(`unpartition`).

**It does not** create the parent table, its columns, its indexes or its constraints —
that DDL is yours; it runs no scheduler of its own; it needs no extension and no
superuser; it never issues `CASCADE`; it never touches a relation it does not own.

## Mental model

A configuration is a **scheme** — the shape of the tree, level by level — and a
**lifecycle policy** — when the partitions of the progression level appear and go.

* `plan()` reads the catalog, compares it with the scheme and returns typed operations,
  each with the reason it is there, plus **findings**: what the planner saw and
  deliberately did not touch.
* `apply()` runs a plan under the table's lock, revalidating every destructive operation
  against the catalog first, and refusing a plan this configuration did not produce.
* `PartitionToolkit.from_engine(engine, ...)` builds the repository, the metadata provider,
  the locks, the service and the maintainer around one engine, giving each setting that
  belongs to two of them (`marker_prefix`, `ddl_timezone`, `boundary_codec`) exactly once.
* `pg-partsmith` (extra `cli`) runs `inspect` / `plan` / `validate` / `apply` over a
  document and a DSN. The first three issue no DDL; `apply` withholds detaches and drops
  unless `--allow-destructive`. `plan --save FILE` writes the artifact `apply --plan FILE`
  reads back, and applying it is refused if it was made for another table or under a
  configuration that has since changed (`--allow-config-drift` overrides). Exit codes: 0
  nothing pending, 2 drift under `plan --check`, 3 findings or run issues, 4 configuration,
  5 connection, 6 lock held, 1 unexpected. `--output json` is the model dump under a
  versioned envelope; `--output metrics` is Prometheus text exposition for a node_exporter
  textfile, all gauges, prefixed `pg_partsmith_`; `plan --locks` prints the heaviest lock
  each operation takes. There is deliberately no `--sql`. See `guide/cli.md`.
* `CommandHooks` (both mirrors) runs a configured command per phase with the
  `PartitionEvent` as JSON on stdin; a non-zero exit refuses the operation. In a document
  it is the `hooks` section, honoured only under `apply --allow-hooks`. Hooks never fire
  during `plan`. See `guide/hooks-in-config.md`.
* The same CLI ships as `ghcr.io/bedrock-python/pg-partsmith:<version>` with the command
  as its entrypoint; tags are the exact version and a moving minor, never `latest`. See
  `guide/container.md`.
* `PartitionsDocument` is several tables and their wiring as one validated model — what a
  YAML or JSON file parses into. `document.configs()` gives every `TablePartitionConfig`,
  `document.config_for(name)` one of them, and
  `PartitionToolkit.from_options(engine, document.runtime)` the wiring. The library parses
  no files: hand it `yaml.safe_load(...)` / `json.loads(...)` output.
* `maintain()` is both under one lock, and is what a scheduled tick calls.

Two kinds of level:

* **progression level** — RANGE windows, or a LIST over an `IntegerSequence`. Open-ended,
  has a cursor, obeys the lifecycle policy.
* **set level** — HASH buckets, LIST groups. Fixed membership: reconciled when incomplete,
  never expired.

The **lifecycle unit** is the partition directly under the root's progression level. It is
what gets created, counted, hooked and expired as one — its whole subtree included.

A converged tree costs zero DDL. Running maintenance twice in a row is a no-op the second
time; that is an integration test, not a promise.

## Wiring

```python
from sqlalchemy.ext.asyncio import create_async_engine

from pg_partsmith import PartitionGranularity, TablePartitionConfig
from pg_partsmith.aio import (
    PartitionLifecycleService,
    PartitionMaintainer,
    PostgresAdvisoryLockManager,
    PostgresMetadataProvider,
    PostgresPartitionRepository,
)

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")

service = PartitionLifecycleService(
    repo=PostgresPartitionRepository(engine),
    metadata=PostgresMetadataProvider(engine),
    locks=PostgresAdvisoryLockManager(engine),
    hooks=None,                      # optional list of BasePartitionLifecycleHooks
)

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,
    retention_count=12,
)

plan = await service.plan(config)                # no lock, no DDL
print(plan.describe())
result = await PartitionMaintainer(service).run_maintenance_safe(config)
```

The sync twin is the same text with `sqlalchemy.create_engine`, `pg_partsmith.sync` and no
`await`. Both modules export the same names.

Constructor options worth knowing:

* `PostgresPartitionRepository(engine, *, ddl_timezone="UTC", ddl_timeout_seconds=30.0,
  marker_prefix=None, drop_allow_unmanaged=False, drop_lock_timeout_ms=3000,
  drop_max_retries=3, drop_retry_delay=0.5, drop_max_backoff=300.0)`
* `PostgresMetadataProvider(engine, *, marker_prefix=None, boundary_codec=None, ddl_timezone=None)`
* `PostgresAdvisoryLockManager(engine, prefix="partitioner", acquire_min_interval_seconds=0.0)`
* `RedisDistributedLockManager(redis_client, prefix=…, ttl_seconds=300, acquire_min_interval_seconds=0.0)`
  — needs the `redis-locks` extra; renews itself while long DDL runs.

`marker_prefix` must be the same on the repository and the provider — the first writes the
ownership marker, the second finds it — and `PartitionLifecycleService` refuses a pair that
disagrees. The provider's `boundary_codec` and `ddl_timezone` are used by
`is_partition_closed` alone, which also takes `boundaries=config.time_boundaries` and reads
both from there instead.

## Configuration

`TablePartitionConfig` is a frozen Pydantic model with `extra="forbid"`: an unknown keyword
is an error, not a hint. Its real fields are `schema` (alias — read it back as
`config.db_schema`), `table_name`, `scheme`, `lifecycle` and `leaves`.

### Flat form

Sugar, accepted **only** for a RANGE root over a time axis:

```python
TablePartitionConfig(
    schema="public",                      # optional; qualifies every statement
    table_name="events",
    partition_column="created_at",
    trailing_partition_columns=["tenant_id"],   # composite key, optional
    granularity=PartitionGranularity.MONTH,     # HOUR DAY WEEK MONTH QUARTER YEAR
    tz="UTC",
    boundary_codec=UUIDv7BoundaryCodec(),       # when the key encodes the instant
    subpartition=HashPartitioning(key="tenant_id", modulus=4),
    create_ahead_count=3,                       # default 6, current period included
    retention_count=12,                         # default 12, current period included
)
```

Passing a flat field **and** `scheme` (or `lifecycle`) raises `ValueError`. Any other
topology must be spelled out.

### Composed form

```python
TablePartitionConfig(
    table_name="issue_events",
    scheme=RangePartitioning(
        key="id",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=UUIDv7BoundaryCodec()),
        child=HashPartitioning(key="organization_id", modulus=4),
    ),
    lifecycle=LifecyclePolicy(
        creation=CreateAhead(count=3),
        retention=KeepNewest(count=12),
        detach=DetachMode.AUTO,
        drop=DropAfter(grace=timedelta(days=7)),
    ),
    leaves=LocalLeaves(tablespace="fast", storage_parameters={"fillfactor": 90}),
)
```

`LifecyclePolicy()` with no arguments is `CreateAhead(6)`, `KeepNewest(12)`,
`DetachMode.AUTO`, `DropAfter(grace=timedelta(0))` — **it detaches and drops in the same
run.** Pass `drop=DropNever()` if detached tables should be kept.

### Levels

| Level | Constructor | Kind |
|---|---|---|
| RANGE | `RangePartitioning(key, boundaries, child=None)` | progression |
| HASH | `HashPartitioning(key, modulus, name_suffix="__h{remainder}", child=None)` | set |
| LIST, groups | `ListPartitioning(key, groups=(ListGroup(name, values), …), include_default=False, default_name="other", name_suffix="__{name}", child=None)` | set |
| LIST, sliding | `ListPartitioning(key, sequence=IntegerSequence(start=1), …)` | progression |

Nesting is `child=`, up to five levels including the root.

### Boundaries

| Axis | Constructor |
|---|---|
| Calendar over a timestamp | `TimeBoundaries(granularity=… \| calculator=…, tz="UTC", codec=None)` |
| Calendar over an encoded key | the same, with `codec=UUIDv7BoundaryCodec()` or `EpochBoundaryCodec(unit)` |
| Fixed-width integer windows | `NumericBoundaries(step, origin=0, name_suffix="__{start}", cursor_source=…)` |
| One value per partition | `IntegerSequence(start=1, name_suffix="__{value}", cursor_source=…)` |

### Lifecycle vocabulary

| Slot | Options |
|---|---|
| `creation` | `CreateAhead(count)`, `CreateUntil(position)`, `CreateNextIf(when)` |
| `retention` | `KeepNewest(count)`, `KeepFor(age)`, `KeepBehind(distance)`, `ExpireIf(when)` |
| `detach` | `DetachMode.AUTO` (concurrent where PostgreSQL allows), `.CONCURRENT`, `.BLOCKING` |
| `drop` | `DropAfter(grace, when=None)`, `DropNever()` |
| predicates | `SizeAbove(bytes)`, `RowsAbove(rows)`, `WindowAgeAbove(age)`, `Unreferenced()`, `SqlPredicate(sql)`, `Callback(fn, facts, label)` |
| combinators | `AllOf(...)`, `AnyOf(...)`, `Not(...)` |

Rules take positional arguments too: `CreateAhead(3)`, `KeepNewest(12)`. A `SqlPredicate`
gets `{partition}` substituted with the qualified partition name. `Unreferenced()` expires
a partition only when no foreign key still points into it.

### Generated names

The parent's name plus a suffix, and the whole thing must fit 63 bytes.

| Level | Suffix | Example |
|---|---|---|
| year / quarter / month | `__2026`, `__2026_q3`, `__2026_08` | `events__2026_08` |
| week / day / hour | `__2026_w35`, `__2026_08_28`, `__2026_08_28_14` | `events__2026_w35` |
| numeric window | `__{start}` | `queue__400000` |
| hash bucket | `__h{remainder}` | `events__2026_08__h3` |
| list group | `__{name}` | `regions__eu` |

## Service API

Every method takes the `TablePartitionConfig` as its first argument.

| Method | Lock | DDL | Returns |
|---|---|---|---|
| `inspect(config)` | no | no | `ActualTree \| None` |
| `plan(config, *, mode=PlanMode.MAINTAIN, now=None, windows=None)` | no | no | `MaintenancePlan` |
| `apply(config, plan, *, continue_on_error=False, allow_config_drift=False)` | **yes** | yes | `MaintenanceResult` |
| `maintain(config, *, skip_create=False, skip_detach=False, skip_drop=False, continue_on_error=False)` | **yes** | yes | `MaintenanceResult` |
| `reconcile(config)` | no | yes | `MaintenanceResult` |
| `ensure_partition(config, period_or_window_or_position)` | no | yes | `PartitionInfo \| None` |
| `ensure_partitions(config, periods)` | no | yes | `list[PartitionInfo]` |
| `partition_data(config, *, batch_rows=10_000, max_batches=None)` | **yes** | yes | `MigrationResult` |
| `unpartition(config, into, *, batch_rows=10_000, max_batches=None, drop_emptied=False)` | **yes** | yes | `MigrationResult` |
| `create_future_partitions(config)` | no | yes | `list[PartitionInfo]` |
| `get_partitions_for_pruning(config)` | no | no | `list[PartitionInfo]` |
| `detach_old_partitions(config, partitions)` | no | yes | `list[str]` |
| `drop_detached_partitions(config, partition_names)` | no | yes | `int` |

`maintain_lifecycle` is the same method as `maintain`. `PartitionMaintainer(service)` adds
`run_maintenance(...)` and `run_maintenance_safe(...)` — same keywords; the *safe* one never
raises and reports the failure on `result.error` instead, which is what a scheduler wants.

`PlanMode` is `MAINTAIN` (the tick), `RECONCILE` (converge only, create nothing ahead) or
`EXPLICIT` (only the windows named in `windows=`).

On a `MaintenancePlan`: `.operations`, `.findings`, `.cursors`, `.generated_at`,
`.config_fingerprint`, `.creates` / `.attaches` / `.detaches` / `.drops`, `.is_noop`,
`.only(*kinds)`, `.without(*kinds)`, `.describe()`. Filtering a plan and applying the rest
is supported and is how you split creation from pruning across schedules.

Every operation dumps its `capabilities` (the heaviest lock it takes, and whether it can
run in a transaction block) and `is_destructive` beside its fields; both are computed on the
way out and ignored on the way in, so the round trip holds.

Serialize a plan with `model_dump_json(by_alias=True)` — the same vocabulary a
configuration uses (`kind`, `method`, `schema`) — and read it back with
`MaintenancePlan.model_validate_json`. `apply()` refuses a plan made for another table, or
one whose `config_fingerprint` no longer matches `config.fingerprint`, with
`PlanConfigMismatchError`; pass `allow_config_drift=True` to apply it anyway. OID
revalidation asks whether this is still the same relation, the fingerprint whether it is
still the same intent.

Results:

* `MaintenanceResult` — `created_count`, `repaired_count`, `attached_count`,
  `detached_count`, `dropped_count`, `duration_ms`, `error`, `issues`, `.success`,
  `.maintenance_plan`.
* `MigrationResult` — `rows_moved`, `batches`, `partitions`, `complete`, `issues`. Call
  again while `complete` is `False`.
* `MaintenanceIssue` — `step` (`create` `reconcile` `attach` `detach` `drop` `move`),
  `error` (`"TypeName: message"`), `partition_name`.

## Hooks

```python
class ColdStorageHooks(BasePartitionLifecycleHooks):
    async def before_drop(self, event: PartitionEvent) -> None:
        await export(event.partition.name)   # raising aborts this drop; it is retried next tick
```

Nine methods, each taking one `PartitionEvent(phase, config, partition, window, operation)`:
`before_create` / `after_create`, `before_attach` / `after_attach` (a detached partition
coming back), `before_detach` / `after_detach`, `before_drop` / `after_drop`, and
`on_event`, which fires for every phase in addition to the named method.
`event.table_name` is the root; `event.operation.reason` says why the operation is in the
plan; `event.window` is the period, or None for a member of a root HASH or LIST.
`before_*` exceptions abort that operation; `after_*` exceptions are logged. Hooks fire for
lifecycle units — partitions directly under the root — never once per leaf of a subtree.

## Rules that hold or break the code

1. **Give it an `Engine` / `AsyncEngine`, never a `Session` / `AsyncSession`.** DDL runs on
   its own connection and commits immediately; a session's transaction is the wrong shape
   for it.
2. **There is no scheduler inside.** Call `maintain()` from cron, APScheduler, Celery beat,
   a Kubernetes CronJob — whatever you already run. Once per period is not enough; run it
   often enough that a missed tick is harmless.
3. **The parent table is yours to create.** pg-partsmith attaches partitions to a table
   that already exists and is already `PARTITION BY …`; it does not `CREATE TABLE` the root
   and does not manage its indexes or constraints.
4. **Ownership is decided by bounds, not by name.** An attached partition whose bounds sit
   on the scheme's grid is a lifecycle partition; one whose bounds do not is reported as
   `unmanaged_partition` and is never detached, dropped or counted — however old it looks.
5. **Only marker-tagged tables are dropped.** The marker is a `COMMENT` written before the
   `DETACH`, and it records when the detach happened, which is what a grace period counts
   from. A table detached by someone else is adopted deliberately, with
   `repo.adopt_partition(...)`.
6. **A plan goes stale.** Destructive operations revalidate the relation by OID at the
   moment they run; a table recreated between plan and apply raises `PlanStaleError`
   (recorded as an issue under `continue_on_error=True`).
7. **`create_ahead_count` and `retention_count` include the current period.**
   `create_ahead_count=3` means this month and the next two.
8. **The default lifecycle drops.** `DropAfter()` has a zero grace, so an expired partition
   is detached and dropped in the same run. Choose `DropAfter(grace=…)` or `DropNever()`
   consciously.
9. **`lifecycle` is meaningless for a scheme with no progression level** — a root
   `HashPartitioning` or a static `ListPartitioning` has a fixed set of partitions, and the
   policy is ignored rather than obeyed.
10. **During `partition_data`, a window's rows are invisible through the parent** between
    the first batch and the attach: PostgreSQL will not attach a partition while DEFAULT
    still holds rows for it, so no ordering keeps them visible throughout. Rows are never
    in two places, and never lost.
11. **Flat and composed spellings do not mix**, and `extra="forbid"` means a misspelled
    keyword raises rather than being ignored.
12. **`schema=` goes in, `config.db_schema` comes out** — the field is aliased to avoid
    shadowing Pydantic's own `schema()`.
13. **Names must fit 63 bytes with the suffix**, and the scheme may be five levels deep at
    most, root included.
14. **Two maintainers are safe.** `apply`, `maintain`, `partition_data` and `unpartition`
    take the table's lock; the granular methods do not, and lost races are recognised and
    reported rather than retried into a failure.

## Common mistakes

```python
# WRONG — a session, and a config field that does not exist
service = PartitionLifecycleService(repo=PostgresPartitionRepository(async_session), ...)
config = TablePartitionConfig(table_name="events", retention_days=90)

# RIGHT
service = PartitionLifecycleService(repo=PostgresPartitionRepository(engine), ...)
config = TablePartitionConfig(
    table_name="events",
    scheme=RangePartitioning(key="created_at", boundaries=TimeBoundaries(granularity=PartitionGranularity.DAY)),
    lifecycle=LifecyclePolicy(retention=KeepFor(timedelta(days=90))),
)
```

```python
# WRONG — flat sugar for a topology it does not cover
TablePartitionConfig(table_name="tasks", partition_column="task_id", granularity=..., partition_type="hash")

# RIGHT
TablePartitionConfig(table_name="tasks", scheme=HashPartitioning(key="task_id", modulus=8))
```

```python
# WRONG — one call and done
await service.partition_data(config)

# RIGHT — batched on purpose; loop until it says it is finished
while not (await service.partition_data(config, batch_rows=10_000)).complete:
    ...
```

```python
# WRONG — assuming a plan applies itself, or that a result with issues is a failure
plan = await service.plan(config)          # this issued no DDL at all

# RIGHT
result = await service.apply(config, plan)
if not result.success:                      # result.error is the fatal case
    alert(result.error)
for issue in result.issues:                 # non-fatal, but they are why nothing happened
    log.warning("%s %s: %s", issue.step, issue.partition_name, issue.error)
```

## Errors

`InvalidPartitionConfigError` (the config does not match the table),
`LockAcquisitionError` (another maintainer holds the lock), `PlanStaleError`,
`PlanConfigMismatchError` (the plan was not made from this configuration),
`PartitionTopologyError`, `PartitionReferencedError` (a detach PostgreSQL refused because
rows still reference the partition), `RowMoveRefusedError` (a row move an incoming foreign
key's `ON DELETE` action would corrupt), `PartitionAlreadyExistsError`,
`PartitionNotFoundError`, `PartitionAttachedError`, `PartitionDetachInProgressError`,
`UnmanagedPartitionDropError`, `DropRetryExhaustedError` — all deriving from
`PartitionError`. The middle group is normally recorded on `result.issues` rather than
raised; see [findings and issues](reference/findings.md) for the full table of what each
one means and what to do about it.

## Documentation map

Fetch a page when the task is the one named beside it.

| Page | Read it when |
|---|---|
| [Installation](getting-started/installation.md) | choosing extras, drivers, a PostgreSQL version |
| [Your first partitioned table](getting-started/first-table.md) | writing the very first integration end to end |
| [Running it in production](getting-started/production.md) | scheduling, locks, alerting, rollout |
| [A multi-tenant event store](getting-started/event-store.md) | a worked nested scheme with a real shape |
| [How it works](concepts/overview.md) | the flow from catalog to plan to DDL |
| [Partition schemes](concepts/schemes.md) | nesting, composite keys, sliding lists, introspection |
| [Boundaries, cursors, calendars](concepts/boundaries.md) | the grid, cursor sources, codecs, custom calendars |
| [Lifecycle policies](concepts/lifecycle.md) | predicates, facts, combinators, what a policy can see |
| [The maintenance plan](concepts/plan.md) | operation and finding types, filtering, serializing |
| [Ownership and safety](concepts/ownership.md) | the marker, adoption, what is never touched |
| [Executing DDL](concepts/execution.md) | transactions, timeouts, attach-last, concurrency |
| [Leaf backends](concepts/leaves.md) | tablespaces, storage parameters, foreign tables |
| [Configure a table](guide/configuration.md) | picking between the flat and composed forms |
| [Schedule maintenance](guide/scheduling.md) | cron, APScheduler, Celery, Kubernetes |
| [Monitor and alert](guide/monitoring.md) | turning results and issues into metrics |
| [Query a partitioned table](guide/querying.md) | what prunes, what does not, encoded keys |
| [Backfill partitions](guide/backfill.md) | giving existing data its windows |
| [Partition an existing table](guide/partition-existing-table.md) | the DEFAULT-attach migration, `partition_data` |
| [Migrate from pg_partman](guide/migration.md) | adopting a tree another tool built |
| [Change a scheme safely](guide/changing-the-scheme.md) | granularity, modulus or key changes |
| [Archive before dropping](guide/archiving.md) | `before_drop`, exports, cold copies |
| [Handle foreign keys](guide/foreign-keys.md) | `Unreferenced()`, refused detaches, refused row moves |
| [Tier cold data](guide/cold-tiering.md) | `ForeignLeaves`, `postgres_fdw`, ClickHouse |
| [Custom calendars and codecs](guide/calendars-and-codecs.md) | your own period calculator, names or key encoding |
| [Extend the library](guide/extending.md) | replacing a repository, provider, lock or executor |
| [Troubleshoot](guide/troubleshooting.md) | a specific error message or a run that did nothing |
| [Recipes](guide/recipes.md) | the shape of a real system close to yours |
| [API reference](reference/index.md) | an exact signature, field or docstring — HTML only, see above |
| [Configuration fields](reference/configuration.md) | every field, type and default |
| [Findings and issues](reference/findings.md) | what a reason code means and what to do |
| [Environment settings](reference/settings.md) | configuring a table from env vars or JSON |
| [Glossary](reference/glossary.md) | a term used here without explanation |
| [PostgreSQL semantics](design/postgresql-semantics.md) | why a refusal exists — verified server behaviour |
| [RFC 0001](design/rfc-0001-partition-schemes.md) | the design of schemes and lifecycle policies |
| [Final report](design/final-report.md) | what 1.0 changed, the migration matrix per project, what is not covered |
| [Changelog](changelog.md) | what changed between versions |
