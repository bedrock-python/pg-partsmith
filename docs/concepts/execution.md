# Executing DDL

How the plan becomes statements: what runs in which transaction, what locks are taken,
what an interruption leaves behind. All of it was measured on PostgreSQL 15 and 17
(details in [PostgreSQL semantics](../design/postgresql-semantics.md)).

## One statement, one transaction

Every statement the executor runs commits on its own. A partition is created in one
transaction and attached in another; a detach and the drop that follows it are separate
statements. There is no long transaction wrapping a run, because `DETACH … CONCURRENTLY`
cannot run inside a transaction block at all, and because a run that creates three
partitions and fails on the fourth should keep the three.

The consequence for callers: pass the service an **engine**, never a session you are
using elsewhere.

## Create standalone, attach last

A partition is never created with `CREATE TABLE … PARTITION OF`. That statement takes
`ACCESS EXCLUSIVE` on the parent and stalls every writer routing through it. Instead:

1. `CREATE TABLE child (LIKE parent INCLUDING ALL EXCLUDING IDENTITY) [PARTITION BY …]` —
   a standalone table, `ACCESS SHARE` on the live parent only. Identity columns are
   excluded because a partition may not carry one; the parent's identity propagates on
   attach.
2. Its own subtree is built the same way and attached *inside* it, deepest first.
3. `ALTER TABLE parent ATTACH PARTITION child FOR VALUES …` — `SHARE UPDATE EXCLUSIVE` on
   the parent, `ACCESS EXCLUSIVE` on the child (and on a DEFAULT sibling, which is scanned
   for rows the new partition would claim).

Until step 3 commits the child is invisible to row routing. An interruption anywhere
before it leaves a detached table no writer can reach — never a live branch that rejects
part of its keyspace. The next run finds the table, completes its subtree, and attaches it.

```text
  step 1            step 2                     step 3
  CREATE branch     CREATE + ATTACH buckets    ATTACH branch to parent
  (standalone)      (inside the branch)        (goes live, whole)
```

A lost race with another worker — the name already exists *and* it is attached with the
planned bounds — is benign. The same name attached with other bounds is a conflict,
reported as `name_unusable`.

## DEFAULT reconciliation

When a RANGE partition is attached and the parent's DEFAULT partition holds rows that
belong to the new window, PostgreSQL refuses the attach (`23514`). The executor:

1. moves those rows from DEFAULT into the new partition, naming columns on both sides
   (`ATTACH` matches by name, so physical column order may differ) and leaving rows with a
   NULL trailing key where PostgreSQL routes them;
2. retries the attach.

If the attach still fails, the rows are returned to DEFAULT rather than left in a table
no query can see. For a nested branch the moved rows are routed onward into its leaves.
A DEFAULT sibling holding rows for a hash or list member is reported
(`default_holds_rows`) rather than moved: only a RANGE window can be selected by its key.

## Detach

`DetachMode.AUTO` runs `DETACH … CONCURRENTLY` on an autocommit connection —
`SHARE UPDATE EXCLUSIVE` on the parent, readers and writers untouched — and falls back to
the blocking form when PostgreSQL refuses the concurrent one, which it does when a DEFAULT
partition exists. The marker is written *before* the detach, and with the plan's OID the
detach fails closed, after the `before_detach` hooks ran: the blocking form checks
identity and attachment inside its own transaction and re-checks after the statement, so
a swapped-in relation rolls everything back — marker included; the concurrent form
**pins** the relation (a holder connection takes `ACCESS SHARE` and verifies the OID
under it) while it is checked and marked, and releases the pin only once *that
statement's own backend* is queued for the partition's lock — from there a swap can only
make the statement fail, never redirect it. A statement that has still not got that far
when the DDL timeout expires is cancelled and waited for **before** the pin goes, so a
late `DETACH` can never fire at a name that changed hands in the meantime. A foreign relation cannot be pinned and uses the transactional
blocking form instead. Either way a swap gets a `PlanStaleError`, not a detached
replacement.

A `DETACH CONCURRENTLY` interrupted mid-way (a statement timeout, a killed connection)
leaves the partition in a **pending** state: still attached in the catalog, invisible
through the parent, rejecting its own rows. The next `maintain()` call reports it as
`detach_pending`, completes it with `DETACH … FINALIZE` first (reason `detach_finalize`),
and re-plans under the same lock: the finalized table comes back as an orphan that the
same call re-attaches — its window still wanted, its data intact — or retires under the
drop policy.

Detaching a branch keeps its subtree intact and readable through the branch.

## Drop

A drop runs in one transaction: `SET lock_timeout`, `LOCK TABLE … IN ACCESS EXCLUSIVE
MODE`, revalidate OID, attachment and marker, drain the remaining rows when the caller
asked for it (`drain_into` — `unpartition`'s guarantee, with the moved count reported),
drop the partition's own foreign keys, then `DROP TABLE` — `DROP FOREIGN TABLE` for a foreign leaf, which cannot be locked and carries
no constraints. Dropping a partitioned branch takes its children with it; `CASCADE` is
never used. Lock contention is retried with exponential backoff
(`drop_lock_timeout_ms`, `drop_max_retries`, `drop_retry_delay`, `drop_max_backoff` on the
repository).

## Lock levels, measured

| Statement | Locks held |
|---|---|
| `CREATE TABLE … (LIKE parent)` | `ACCESS SHARE` on the parent |
| `ATTACH PARTITION` | `SHARE UPDATE EXCLUSIVE` on the parent, `ACCESS EXCLUSIVE` on the child and on a DEFAULT sibling; `SHARE ROW EXCLUSIVE` on tables referencing the parent through a foreign key |
| `DETACH PARTITION` (plain) | `ACCESS EXCLUSIVE` on parent, partition, and every table referencing the parent |
| `DETACH PARTITION … CONCURRENTLY` | `SHARE UPDATE EXCLUSIVE` on the parent; `ACCESS EXCLUSIVE` on the partition and, in its second transaction, on referencing tables |
| `DROP TABLE` of a detached table | `ACCESS EXCLUSIVE` on that table only |
| `COMMENT ON` | `SHARE UPDATE EXCLUSIVE` on the table |
| the catalog reads (tree, orphans, sizes, estimates) | no relation lock: the tree is walked over `pg_inherits`, not with `pg_partition_tree()`, which would take `ACCESS SHARE` on every member — and omit a partition whose `DETACH CONCURRENTLY` was interrupted |

`CREATE TABLE … PARTITION OF` — `ACCESS EXCLUSIVE` on the parent — is the one statement
the library never issues against a live parent. Each operation reports the heaviest lock
it takes on `op.capabilities`.

## Failures

| What happened | Effect on the run |
|---|---|
| a topology conflict at execution time — a DEFAULT sibling holding rows for a hash bucket, a name taken by a relation with other bounds, a detach PostgreSQL refuses because rows are still referenced | recorded in `result.issues`; the run goes on |
| a `PlanStaleError` — the relation is not the one the plan saw | recorded as an issue with `continue_on_error`, otherwise raised |
| any other error — a connection drop, a permission denied, a `before_*` hook raising | aborts the run, unless `continue_on_error`, in which case it is recorded and the next operation runs |
| validation or lock failure | fatal, always |

`PartitionMaintainer.run_maintenance_safe()` catches everything, cancellation included,
and reports it on `result.error`.

## What an interruption leaves behind

Because attach is last and the marker is written first, a run cut off at any point leaves
one of a small number of states, each of which the next run converges:

| Cut off during | Left behind | Next run |
|---|---|---|
| creating a partition or its subtree | a detached, unmarked table, unreachable by writers | completes the subtree, attaches it |
| a detach | a marked table, attached or pending | finishes the detach (`FINALIZE` if pending), then proceeds |
| a drop | either the table or nothing | drops it, or finds nothing to do |

## Hooks

Eight hooks fire around create, attach, detach and drop, once per **lifecycle unit** — the partition
under the root, never per leaf of its subtree — and once per member of a root `HASH` or
`LIST`. A `before_*` hook that raises aborts that operation (the partition comes back on
the next run); an `after_*` hook that raises is logged and re-raised — the operation has
already happened, but the run aborts unless `continue_on_error`. Hooks decide nothing about *which*
partitions come and go; they react. See [Archive before dropping](../guide/archiving.md).
