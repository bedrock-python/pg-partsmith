# Planning and dry runs

Maintenance is a desired-state loop with the decision separated from the execution:

```text
catalog ──► ActualTree ──┐
                         ├──► plan_maintenance() ──► MaintenancePlan ──► apply()
config ──► scheme + policy ┘         (pure)            (inspect, filter, serialize)
```

## `plan()`

```python
plan = await service.plan(config)
print(plan.describe())
```

```text
plan for public.events at 2026-11-15T00:00:00+00:00
  CREATE public.events__2026_11 (create_ahead)
    CREATE public.events__2026_11__h0 (subtree)
    CREATE public.events__2026_11__h1 (subtree)
  DETACH public.events__2026_08 (retention_expired) size=128000000000 rows~41000000
  DROP public.events__2026_08 (follows_detach) size=128000000000 rows~41000000
  [info] unmanaged_partition: public.events_archive covers 2000-01-01 … 2020-01-01, which is not a window of the configured scheme; it is not a lifecycle partition and is left alone.
```

`plan()` reads the catalog, gathers whatever the policy needs, and issues **no DDL** and
takes **no lock**. The result is a plain Pydantic model:

- `plan.operations` — typed, ordered: creations (each with its subtree nested in
  `children`), re-attachments, detaches, drops. Every operation carries a `reason`
  (`create_ahead`, `hash_gap`, `retention_expired`, `grace_elapsed`, …), a human `detail`,
  the target's `oid`, and `size_bytes` / `row_estimate` when they were measured.
- `plan.findings` — what the planner deliberately left alone, each with a `reason` and a
  `severity`: `INFO` for expected steady states, `WARNING` for what needs a human.
- `plan.model_dump(mode="json")` — the wire format for CLIs, dashboards and audit logs;
  `MaintenancePlan.model_validate` reads it back.
- `plan.without(OperationKind.DROP)` / `plan.only(OperationKind.CREATE)` — filters. Removing
  the detaches also removes the drops that follow them.
- `op.capabilities` — whether the statement may run inside a transaction block and the
  heaviest lock it takes (measured).

## `apply()`

```python
result = await service.apply(config, plan)             # takes the table's lock
result = await service.maintain(config)                 # plan + apply under one lock
```

`apply` executes the operations in order. Before anything destructive it **revalidates**:
the relation must still have the OID the plan saw, still be attached (detach), still be a
marker-tagged orphan (drop). A table dropped and recreated under the same name between plan
and apply is left alone and reported as `PlanStaleError`.

Failures: a topology conflict discovered while executing — a DEFAULT sibling holding rows
for a hash bucket, a name held by a relation with other bounds — is recorded in
`result.issues` and never aborts the run. Any other error aborts unless
`continue_on_error=True`, in which case it is recorded and the next operation runs.
`result.plan` is the plan that was executed.

`PartitionMaintainer.run_maintenance_safe()` wraps `maintain()` with logging and never
raises — it is what a scheduler calls.

## Modes

| Mode | Used by | Creates ahead | Expires | Fills set-level gaps |
|---|---|---|---|---|
| `PlanMode.MAINTAIN` | `maintain()`, `plan()` default | yes | yes | yes |
| `PlanMode.RECONCILE` | `reconcile()` | no | no | yes |
| `PlanMode.EXPLICIT` | `ensure_partition(s)` | the named windows only | no | inside them |

## Ownership

Ownership is derived from the catalog against the scheme; there is no metadata table.

| An attached partition whose bounds… | is | Lifecycle may |
|---|---|---|
| are a window of the boundaries' grid, or lie inside one (a day inside a monthly grid, left by an earlier finer config) | managed | create below it, detach, drop |
| are not on the grid — a hand-attached `events_archive` spanning years, a week straddling two months | `unmanaged_partition` (INFO) | inspect and report only; a wanted window overlapping it is `range_overlap` (WARNING) and not created |
| are open on one side (`MINVALUE` / `MAXVALUE` / `infinity`) | `unbounded_partition` (INFO) | never pruned |
| cannot be read on the level's axis | `unreadable_bound` (WARNING) | never pruned — guessing risks dropping live data |
| belong to a foreign table | `foreign_partition` (INFO) | nothing — `DROP TABLE` cannot even remove it |
| are pending an interrupted `DETACH CONCURRENTLY` | `detach_pending` (WARNING) | nothing until `FINALIZE` |

Detached tables are considered only when they carry the library's `COMMENT` marker; the
marker is written at detach (with the instant) or by `repo.adopt_partition(...)` for legacy
tables. An orphan whose window is wanted again — it is in the create-ahead set, or
retention grew and no longer expires it — is **re-attached**, not recreated. Under
`DropNever` detached tables belong to whatever process the policy hands them to and are
never brought back.

## Convergence rules

| Actual state | Plan |
|---|---|
| RANGE window missing ahead of the cursor | create it, subtree included |
| RANGE window expired | detach; drop in the same run or after the grace |
| hash set missing buckets at the configured modulus | create exactly the missing ones (`hash_gap`) |
| hash set complete at the configured modulus | nothing — zero DDL |
| hash set complete at another modulus | leave it (`modulus_preserved`, INFO) |
| hash set incomplete at another modulus | fill the gaps **at its own modulus** (`hash_gap_historical_modulus`) |
| hash siblings at mixed moduli that still tile the keyspace | leave it (`non_uniform_complete`, INFO) |
| hash siblings at mixed moduli leaving a gap | leave it, report (`non_uniform_incomplete`, WARNING) |
| a plain leaf where the scheme expects a branch | leave it (`legacy_leaf`, INFO); new partitions use the new topology |
| a branch partitioned by another method or column | leave it, report (`strategy_mismatch` / `column_mismatch`) |
| LIST group missing | create it |
| LIST group present under another name with the same values | leave it |
| LIST value owned by another partition | report (`list_values_conflict`) |
| a wanted name held by a relation with other bounds, or over 63 bytes | report (`name_unusable`) |

An incomplete hash set is not cosmetic: PostgreSQL rejects every row whose key hashes into
a missing remainder. Repair is what restores ingestion for that slice of tenants — and a
repair never mixes moduli, because PostgreSQL would refuse the overlap.

## Reported issues

Actionable findings and execution problems land in `MaintenanceResult.issues`:

```python
result = await maintainer.run_maintenance_safe(config)
for issue in result.issues:
    log.warning("%s %s: %s", issue.step.value, issue.partition_name, issue.error)
```

They are reported **regardless of `continue_on_error`** — a branch rejecting writes must
not stay silent — and never abort the run. `result.success` reports a fatal error and
nothing else. Expected steady states (`legacy_leaf`, `modulus_preserved`,
`unmanaged_partition`, `grace_pending`) are logged at INFO/DEBUG and kept out of `issues`;
read them from `result.plan.findings`.

## Sizes and rows

`SizeAbove` / `RowsAbove` and any `SqlPredicate` make the introspector measure the
progression-level members: one query summing `pg_total_relation_size` over each subtree's
leaves and reading `pg_stat_user_tables.n_live_tup`, plus one query per SQL predicate and
candidate. The numbers appear on the operations (`size_bytes`, `row_estimate`). Nothing is
measured for a policy that does not ask.

## Locks

`plan()` takes no lock. `apply()` and `maintain()` take the table's distributed lock; two
maintainers on the same table serialize or the loser raises `LockAcquisitionError`
(non-blocking acquisition). `reconcile()`, `ensure_partition(s)` and the granular methods
take no lock of their own — wrap them in `locks.acquire_lock(table)` when orchestrating by
hand.
