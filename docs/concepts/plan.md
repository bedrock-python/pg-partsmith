# The maintenance plan

Every run is a plan first. `plan_maintenance` compares the configured scheme with the
tree that exists and returns a `MaintenancePlan`: what to do, in order, with a reason on
every operation — and what it deliberately refused to touch, with a reason on every
finding. It is pure Python over data already read from the catalog: no SQL, no I/O.

```python
plan = await service.plan(config)
print(plan.describe())
```

```text
plan for public.events at 2027-09-15T03:00:00+00:00
  CREATE public.events__2027_09 (create_ahead)
  CREATE public.events__2027_10 (create_ahead)
  CREATE public.events__2027_11 (create_ahead)
  DETACH public.events__2026_08 (retention_expired)
  DETACH public.events__2026_09 (retention_expired)
  DROP public.events__2026_08 (follows_detach)
  DROP public.events__2026_09 (follows_detach)
```

## Operations

Operations are typed, ordered as they must execute, and nested where order matters:

| Operation | What it does | Carries |
|---|---|---|
| `CreatePartition` | create a partition, build its subtree, attach it | `parent_name`, `bounds`, `partition_by` (for a branch), `children`, `counts_as` |
| `AttachPartition` | re-attach a detached orphan whose window is wanted again | `parent_name`, `bounds` |
| `DetachPartition` | detach an expired partition, subtree included | `parent_name`, `mode`, `oid` |
| `DropPartition` | drop a detached, marker-tagged orphan | `oid`, `detached_at`, `follows_detach` |

Creations come first (each with its subtree inside it), then re-attachments, then
detaches, then drops. Every operation records:

- its **reason** — why it is in the plan: `create_ahead`, `create_until`, `create_next`,
  `explicit`, `subtree`, `hash_gap`, `hash_gap_historical_modulus`, `list_group_missing`,
  `list_default_missing`, `reattach`, `retention_expired`, `detach_finalize`,
  `follows_detach`, `grace_elapsed`;
- a human **detail** (`"2026_08 under 'create 3 ahead'"`);
- the relation's **OID** when it exists — what a destructive operation is revalidated
  against at execution time;
- `size_bytes` and `row_estimate` when the policy had them measured;
- its **capabilities**: whether the statement may run inside a transaction block
  (`DETACH … CONCURRENTLY` may not) and the heaviest lock it takes, as measured.

`plan.creates`, `plan.attaches`, `plan.detaches`, `plan.drops` are the typed views;
`plan.relation_count` counts every relation the plan would create, subtrees included.

## Findings

A finding is something the planner saw and chose not to change. Each has a reason and a
severity:

- **`INFO`** — an expected steady state the planner recognised and chose correctly for: an
  orphan still in its grace period, a legacy leaf, a partition someone else owns, a hash
  set at an older modulus that still tiles the keyspace.
- **`WARNING`** — something a human has to act on: a wanted window that overlaps a partition
  the library does not own, a hash set with a gap no repair is safe for, a bound that
  cannot be read, a DEFAULT partition holding rows in the way.

```text
  [info] grace_pending: public.events__2026_10 was detached at 2027-10-02T03:00:00+00:00 and is kept until 2027-10-09T03:00:00+00:00 ('drop after 7 days, 0:00:00').
  [warning] range_overlap: public.events needs a partition for 2028_03 but public.events_oddweeks already covers part of it with bounds the scheme did not produce; creating it would fail, and detaching the other is not this library's decision.
```

`plan.actionable_findings` are the warnings. When the plan is applied they are copied
into `MaintenanceResult.issues` — regardless of `continue_on_error`, because a branch that
is rejecting writes must not stay silent — while the informational ones stay on
`result.plan.findings`. Every reason is listed with its cause and remedy in
[Findings and issues](../reference/findings.md).

## Modes

| Mode | Used by | Creates ahead | Expires | Fills set-level gaps |
|---|---|---|---|---|
| `MAINTAIN` | `maintain()`, `plan()` by default | yes | yes | yes |
| `RECONCILE` | `reconcile()` | no | no | yes |
| `EXPLICIT` | `ensure_partition(s)` | the named windows only | no | inside them |

## Filtering and serializing

```python
plan.without(OperationKind.DROP)                     # everything but the drops
plan.only(OperationKind.CREATE, OperationKind.ATTACH) # creations only
plan.is_noop                                          # no operation at all
plan.model_dump(mode="json")                          # the wire format
MaintenancePlan.model_validate(payload)               # and back
```

Removing the detaches also removes the drops that follow them — a partition that is not
detached this run cannot be dropped this run. The `skip_create` / `skip_detach` /
`skip_drop` flags of `maintain()` are these filters.

The JSON form is what a CLI, a dashboard or an audit log would store. A plan carries the
instant it was made (`generated_at`), the cursors it was made against, every operation
with its bounds and reason, and every finding.

## Convergence rules

What the planner does with each state it can meet, in one table:

| Actual state | Plan |
|---|---|
| a window missing ahead of the cursor | create it, subtree included |
| a window expired | detach; drop in the same run or after the grace |
| a detached orphan whose window is wanted (create-ahead, or retention grew) | re-attach |
| a detached orphan past its grace | drop (`grace_elapsed`) |
| a hash set missing buckets at the configured modulus | create exactly the missing ones (`hash_gap`) |
| a hash set complete at the configured modulus | nothing — zero DDL |
| a hash set complete at another modulus | leave it (`modulus_preserved`, INFO) |
| a hash set incomplete at another modulus | fill the gaps **at its own modulus** (`hash_gap_historical_modulus`) |
| hash siblings at mixed moduli that still tile the keyspace | leave it (`non_uniform_complete`, INFO) |
| hash siblings at mixed moduli leaving a gap | leave it, report (`non_uniform_incomplete`, WARNING) |
| a plain leaf where the scheme expects a branch | leave it (`legacy_leaf`, INFO); new partitions use the new topology |
| a branch partitioned by another method or column | leave it, report (`strategy_mismatch` / `column_mismatch`) |
| a LIST group missing | create it |
| a LIST group present under another name with the same values | leave it |
| a LIST value owned by another partition | report (`list_values_conflict`) |
| a partition whose bounds are not on the grid | leave it (`unmanaged_partition`, INFO); a wanted window it overlaps is `range_overlap` (WARNING) and not created |
| a partition with an unreadable or open-ended bound | leave it, never prune it |
| a foreign partition under a local-leaves configuration | leave it (`foreign_partition`, INFO) |
| a partition pending an interrupted `DETACH CONCURRENTLY` | complete the detach with `FINALIZE` (`detach_finalize`); report it (`detach_pending`, INFO) |
| a wanted name held by a relation with other bounds, or over 63 bytes | report (`name_unusable`) |

A converged tree produces no operation at all, so a tick against it costs the catalog
reads below and no DDL.

## What a plan costs

Planning itself is a pure function over the configuration and the tree: it never queries
the database, and it is linear in the number of nodes. Reading the tree is a fixed number
of statements, whatever the table's size:

| Step | Statements | Grows with the tree? |
|---|---|---|
| the whole tree — every level, bounds, `relkind`, oids | 1 | no |
| detached orphans and their markers | 1 | no |
| sizes and row estimates | 1, and only when a policy asks for them | no |

There is no query per partition anywhere in the path. Facts are measured for lifecycle
units only — the partitions retention decides over — so a nested tree does not multiply
them: 2 000 monthly branches with eight buckets each are 2 000 targets, not 16 000.

For scale, a table of 18 001 nodes (2 000 monthly branches × 8 hash buckets, fifty years
of history) plans in roughly 0.15 s on a developer machine, and a converged one plans to a
no-op. Both the statement counts and a loose time budget are pinned by
`tests/unit/test_scale.py`, so a planner that turned quadratic would fail the suite rather
than a maintenance window.
