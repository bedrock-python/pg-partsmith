# Findings and issues

A **finding** is something the planner saw and deliberately left alone. Each carries a
`reason`, a `severity` and a human-readable `detail`. `INFO` findings are expected steady
states and stay on `plan.findings`; `WARNING` findings need a human and are also copied
into `MaintenanceResult.issues` when the plan is applied. Execution-time problems are
issues too, with the exception's name in front of the message.

## Finding reasons

| Reason | Severity | Meaning | What to do |
|---|---|---|---|
| `legacy_leaf` | INFO | A plain table where the scheme expects a branch; it cannot gain partitions and holds valid data. | Nothing. New partitions follow the scheme. |
| `strategy_mismatch` | WARNING | A branch partitioned by another method than the scheme's. | Fix the scheme, or repartition by hand. |
| `column_mismatch` | WARNING | A branch partitioned by the right method on another key, or on an expression. | Same. |
| `modulus_preserved` | INFO | A complete hash set at a modulus the scheme no longer uses. | Nothing; it tiles the keyspace. |
| `modulus_repaired` | INFO | An incomplete hash set at another modulus is being filled at its own modulus. | Nothing. |
| `non_uniform_complete` | INFO | Hash siblings at mixed moduli that still tile the keyspace. | Nothing. |
| `non_uniform_incomplete` | WARNING | Hash siblings at mixed moduli leaving a gap; rows hashing into it are rejected. | Add the missing buckets by hand. |
| `coverage_unknown` | WARNING | The branch cannot be reasoned about: a child's name contains a dot and cannot be addressed by qualified-name DDL, or the hash moduli's least common multiple is too large to check coverage. | Rename the child; simplify the moduli. |
| `list_values_conflict` | WARNING | A configured LIST group claims a value another partition owns. | Detach the other, or change the group. |
| `name_unusable` | WARNING | The name the scheme produces is taken by a relation with other bounds, or over 63 bytes. | Rename the stray relation; shorten names. |
| `default_holds_rows` | WARNING | A DEFAULT sibling holds rows belonging to a hash or list member being attached. | Move the rows out by hand. |
| `unconvergeable` | WARNING | A partition could not be created because part of its subtree could not be planned. | Read the findings on the subtree. |
| `range_overlap` | WARNING | A wanted window overlaps a partition the library does not own. | Decide about that partition by hand. |
| `unmanaged_partition` | INFO | An attached partition whose bounds are not on the grid; never detached, dropped or counted. | Nothing, unless it should be managed — then align its bounds. |
| `unreadable_bound` | WARNING | A bound the level's axis cannot read; never pruned. | Check the codec and the key. |
| `unbounded_partition` | INFO | A partition open on one side (`MINVALUE`/`MAXVALUE`); never pruned. | Nothing. |
| `foreign_partition` | INFO | A foreign table under a local-leaves configuration; never touched. | Configure `ForeignLeaves` if it should be managed. |
| `detach_pending` | INFO | An interrupted `DETACH CONCURRENTLY`; the partition rejects its rows until finalized. `maintain()` completes it with `DETACH … FINALIZE` and re-plans in the same call. | Nothing; check that the run went through. |
| `grace_pending` | INFO | A detached orphan still within its grace period. | Nothing. |
| `drop_deferred` | INFO | An orphan past its grace whose drop condition does not hold yet. | Nothing. |

## Operation reasons

| Reason | On | Meaning |
|---|---|---|
| `create_ahead` | create | a window the creation policy wants ahead of the cursor |
| `create_until` | create | a window before the configured horizon |
| `create_next` | create | the window after the newest, because its predicate held |
| `explicit` | create, detach, drop | a window the caller named (`ensure_partitions`); a partition emptied by `unpartition(drop_emptied=True)` |
| `subtree` | create | a member of the subtree of a partition being created |
| `hash_gap` | create | a missing bucket at the configured modulus |
| `hash_gap_historical_modulus` | create | a missing bucket at the modulus the set was built with |
| `list_group_missing` | create | a configured LIST group with no partition |
| `list_default_missing` | create | the configured LIST catch-all with no partition |
| `reattach` | attach | a detached orphan whose window is wanted again |
| `retention_expired` | detach | the retention policy declared the window expired (also `detach_old_partitions`) |
| `detach_finalize` | detach | an interrupted `DETACH CONCURRENTLY` is completed with `FINALIZE` |
| `follows_detach` | drop | dropped in the same run as its detach |
| `grace_elapsed` | drop | an orphan past its grace period (also `drop_detached_partitions`) |

## Issue steps

`MaintenanceIssue.step` says where a problem occurred: `create`, `reconcile` (a planner
finding, or a gap filled inside an existing branch), `attach`, `detach`, `drop`, `move`
(the batch movers).

## Exceptions

| Exception | Raised by | Meaning |
|---|---|---|
| `InvalidPartitionConfigError` | `plan()` and everything over it | the configuration does not match the table, or PostgreSQL would refuse it |
| `LockAcquisitionError` | `apply()`, `maintain()`, the movers | another maintainer holds the table's lock |
| `PartitionTopologyError` | recorded as an issue | a topology conflict met while executing |
| `PartitionReferencedError` | recorded as an issue | a detach PostgreSQL refused because rows are still referenced |
| `RowMoveRefusedError` | recorded as an issue | a row move an incoming foreign key's `ON DELETE` action would corrupt |
| `PlanStaleError` | detach and drop | the relation is not the one the plan decided about |
| `PartitionAlreadyExistsError` | `create_table_like` | the name is taken (handled by the executor as a race or a conflict) |
| `PartitionNotFoundError` | detach, moves | the relation does not exist |
| `PartitionAttachedError` | `drop_partition`, `adopt_partition` | the table is still attached |
| `PartitionDetachInProgressError` | detach | another detach of the partition is pending |
| `UnmanagedPartitionDropError` | `drop_partition` | the table carries no marker |
| `DropRetryExhaustedError` | `drop_partition` | lock contention outlasted the retries |

All derive from `PartitionError`. `PartitionMaintainer.run_maintenance_safe()` reports any
of them on `result.error` as `"<Name>: <message>"`.
