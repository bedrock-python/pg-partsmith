# PostgreSQL partition semantics, verified

Everything the 1.0 design relies on was measured against real servers rather than assumed:
`postgres:17-alpine` (17.11) and `postgres:15-alpine` (15.19) via testcontainers, on
2026-08-28. Both versions behaved identically in every case below. The measurement scripts
live with the development scratch files; the integration suite re-asserts the load-bearing
facts on every run, on PostgreSQL 15, 16 and 17 in CI.

## Transactions

| Statement | Inside a transaction block |
|---|---|
| `ALTER TABLE … DETACH PARTITION … CONCURRENTLY` | **refused**: `25001 ALTER TABLE ... DETACH CONCURRENTLY cannot run inside a transaction block` |
| `… DETACH PARTITION … CONCURRENTLY` with a DEFAULT partition present | **refused** even in autocommit: `55000 cannot detach partitions concurrently when a default partition exists` |
| plain `DETACH`, `ATTACH`, `CREATE TABLE … PARTITION OF`, `DROP TABLE`, `COMMENT ON` | fine |

The executor therefore runs the concurrent form on an AUTOCOMMIT connection and,
in `DetachMode.AUTO`, falls back to the blocking form on `55000`/`0A000`/`42601`.

## The pending-detach state

A `DETACH … CONCURRENTLY` interrupted by `statement_timeout` (measured with 1.5 s against an
open transaction holding the partition) leaves `pg_inherits.inhdetachpending = true`:

- the partition still has `relispartition = true`;
- it is invisible through the parent (`SELECT count(*) FROM parent` → 0 rows from it);
- rows that belong to it are **rejected** with `23514 no partition of relation … found for row`;
- a second `DETACH … CONCURRENTLY` fails with `55000 partition … already pending detach`;
- a plain `DETACH` blocks on the lock;
- only `ALTER TABLE … DETACH PARTITION … FINALIZE` completes it (`relispartition` → false);
- `pg_partition_tree(parent)` and queries through the parent **omit** the pending partition
  once the first transaction has committed (verified on 15–17); `pg_inherits` still lists it
  with `inhdetachpending = true`, which is why the tree is read from `pg_inherits`.

`PartitionRemover.detach` checks `inhdetachpending` first and finalizes; the planner reports
such a partition as `DETACH_PENDING` (INFO), plans its detach (`DETACH_FINALIZE`) and
otherwise treats its window as absent.

While the concurrent detach waits for an open transaction it holds no relation lock of its
own (`wait_event = Lock/virtualxid`), but `inhdetachpending` is already set — so a long
wait is itself a period of rejected writes for that partition.

## Subtrees

- Detaching a subpartitioned branch keeps its subtree intact: the branch loses
  `relispartition`, its children keep theirs and their `relpartbound`; the rows stay
  readable through the branch (100 rows before and after) and disappear from the root.
- `DROP TABLE branch` drops the whole subtree **without `CASCADE`**, whether the branch is
  attached or detached. Nothing is left in `pg_class`.
- `pg_partition_tree(detached_branch)` works: the branch is level 0 of its own tree, which
  is how a half-built branch is inspected before it is attached. `pg_partition_tree` of a
  plain table or of `to_regclass(NULL)` returns no rows.
- `LIST (tier) → RANGE (created_at)` nesting is accepted.
- A partition may live in another schema than its parent; `pg_partition_tree` reports it as
  `arch.cc_old`.

## Locks (measured from a second session via `pg_locks`)

| Statement | Locks held |
|---|---|
| `CREATE TABLE … PARTITION OF parent …` | `ACCESS EXCLUSIVE` on **parent** |
| `CREATE TABLE … (LIKE parent INCLUDING ALL)` | `ACCESS SHARE` on parent |
| `ALTER TABLE parent ATTACH PARTITION child …` | `SHARE UPDATE EXCLUSIVE` on parent, `ACCESS EXCLUSIVE` on child |
| … with a DEFAULT partition present | additionally `ACCESS EXCLUSIVE` on the DEFAULT partition |
| `ALTER TABLE parent DETACH PARTITION child` (plain) | `ACCESS EXCLUSIVE` on parent **and** child |
| `DROP TABLE detached_table` | `ACCESS EXCLUSIVE` on that table only |
| `DROP TABLE attached_partition` | `ACCESS EXCLUSIVE` on parent and partition |
| `COMMENT ON TABLE x` | `SHARE UPDATE EXCLUSIVE` on x |
| `ANALYZE x` | `SHARE UPDATE EXCLUSIVE` on x |
| `SELECT * FROM pg_partition_tree('parent')` | `ACCESS SHARE` on every member |
| `pg_get_expr(relpartbound, oid)`, `pg_inherits`, `obj_description`, `pg_total_relation_size`, `reltuples` | no relation lock |
| `SELECT count(*) FROM x` | `ACCESS SHARE` on x |

Hence: create standalone with `LIKE`, attach last; never `CREATE TABLE … PARTITION OF`
against a live parent; a converged tree must issue no DDL at all.

## Foreign tables

- `CREATE FOREIGN TABLE … PARTITION OF parent` and `ATTACH PARTITION` of a foreign table are
  refused with `42809 cannot create foreign partition of partitioned table` / `cannot attach
  foreign table … as partition` when the parent has a **unique index or primary key**; both
  succeed on an index-free parent and on one with only non-unique indexes.
- A foreign table attached as a partition must carry the parent's `NOT NULL` constraints
  (`42804 column … in child table must be marked NOT NULL` otherwise), which is why
  `create_foreign_table_like` copies `attnotnull` along with the types.
- In `pg_partition_tree` the foreign leaf has `relkind = 'f'`, `isleaf = true`, a normal
  `relpartbound`; `SELECT … FROM parent` reads through it.
- `DROP TABLE`, `COMMENT ON TABLE` and `LOCK TABLE` on it fail with `42809 "x" is not a table`
  / `not supported for foreign tables`; `COMMENT ON FOREIGN TABLE` and `DROP FOREIGN TABLE`
  work; `pg_total_relation_size` is 0.
- `ATTACH`, `DETACH` and `DETACH … CONCURRENTLY` all work with a foreign partition; a foreign
  DEFAULT partition is accepted.
- `CREATE INDEX` on a parent that has a foreign partition succeeds (non-unique indexes skip
  foreign children).

Under a `LocalLeaves` configuration the library never plans DDL for a foreign leaf and
reports it as `FOREIGN_PARTITION`; under `ForeignLeaves` it creates, comments, detaches and
drops foreign leaves with the statements above.

## Storage parameters, tablespaces, privileges

- `WITH (...)` on a partitioned table → `42809 cannot specify storage parameters for a
  partitioned table`; on a leaf it works. `LocalLeaves` applies storage parameters to leaves
  only.
- `TABLESPACE pg_default` on a partitioned table → `0A000 cannot specify default tablespace
  for partitioned relations`; a real tablespace is accepted on branches and leaves alike.
- `CREATE TABLE … (LIKE parent INCLUDING ALL)` copies no grants and keeps the creator as
  owner: the new relation's `relacl` is NULL while the parent's carries its grants.
  `aclexplode(relacl)` lists them (PUBLIC is grantee 0); `LocalLeaves(inherit_privileges=True)`
  replays owner and grants in the creating transaction.

## Foreign keys (measured on 15.19 and 17.11, 98 scenarios, no difference)

- An FK on another table pointing at the partitioned parent is cloned onto every partition
  (`conparentid ≠ 0`, `conislocal = false`). `DETACH PARTITION` — plain and `CONCURRENTLY` —
  is refused with `23503 removing partition "x" violates foreign key constraint "…"` while
  a row of the referencing table points at a row of the partition; `ON DELETE CASCADE`
  changes nothing and nothing is cascade-deleted. A failed `CONCURRENTLY` leaves no
  `inhdetachpending` state behind. Once the referencing rows are gone the detach succeeds,
  the detached table carries no FK-related constraint or trigger, and its rows are invisible
  to the FK (an insert into the referencing table pointing at them fails `23503`).
  Re-`ATTACH` recreates the clone.
- `DROP TABLE` of an **attached** partition referenced by such an FK always fails (`2BP01`),
  with or without referencing rows: the DETAIL names the parent constraint. `DROP … CASCADE`
  silently removes the whole FK from the referencing table. Detach first, then drop.
- An FK from the partitioned table to another table is cloned onto every partition under the
  **same constraint name**; it survives DETACH as a standalone constraint (still enforced),
  cannot be dropped on an attached partition (`42P16`), and can be dropped on the detached
  one — which is what the safe-drop path does before `DROP TABLE`.
- An FK pointing at a partition **directly** does not block DETACH; `DROP` of that partition
  needs `CASCADE`, which drops that FK.
- Locks: plain `DETACH` takes `ACCESS EXCLUSIVE` on the referencing table as well as on
  parent and partition (the documented `SHARE` is what the referencing side needs; the
  measured lock is `ACCESS EXCLUSIVE`, taken to drop the clone constraint); `DETACH …
  CONCURRENTLY` takes it in its second transaction, while `inhdetachpending = true`; `ATTACH`
  and `CREATE … PARTITION OF` take `SHARE ROW EXCLUSIVE` on the referencing table; a plain
  `DETACH` that has to wait for the referencing table does so while already holding
  `ACCESS EXCLUSIVE` on the parent.

The library translates `23503` on detach into `PartitionReferencedError`, records it as an
issue and goes on; `Unreferenced()` keeps such partitions out of the plan.

### Referential actions on single-statement moves (verified on 15, 16, 17)

Moving a row with `WITH moved AS (DELETE … RETURNING …) INSERT …` in one statement:

- `NO ACTION`'s trigger runs at the end of the statement and passes only when the key is
  still reachable *through the referenced tree*. A move into a detached table (a
  `partition_data` fill, DEFAULT reconciliation before an attach) or out of the tree
  (`unpartition`) leaves it unreachable: `23503`, atomic — so a *referenced* row cannot
  be moved at all, whatever the action.
- `RESTRICT` fires immediately and fails the statement — safe, everything rolls back.
- `CASCADE`, `SET NULL` and `SET DEFAULT` act on the DELETE alone: the referencing rows
  are deleted or rewritten even though the parent row is re-inserted in the same
  statement. The movers refuse to run when an incoming key declares one of these.

`GENERATED ALWAYS AS … STORED` columns refuse explicit values (`428C9`), so the movers
list only writable columns (`attgenerated = ''`) and let the target recompute; a
`GENERATED ALWAYS AS IDENTITY` column on the target needs `OVERRIDING SYSTEM VALUE` for
the moved values to survive, which the movers add when one is present. `OVERRIDING`
leaves the backing sequence where it was, so after a move the movers advance every
identity sequence on the target past the moved ids (`setval`, in the move's transaction,
keeping a higher pre-existing position) — otherwise the target's next ordinary INSERT
would draw an id a moved row already owns (`23505`).

The first transaction of `DETACH PARTITION … CONCURRENTLY` requests `ACCESS EXCLUSIVE`
on the partition being detached (measured on 17: `pg_locks` shows the waiting AEL), so
*any* lock held on the partition blocks it — which is what the detach pin exploits, and
why the pin must be released while the statement is already queued behind it; the release
is scoped to the statement backend's own pid, so a bystander's queued lock cannot lift the
pin early. Unlike the `CONCURRENTLY` form, `DETACH … FINALIZE` may run inside a
transaction block (verified on 15–17), which is what makes a fully transactional
finalize — lock, identity checks, marker, statement — possible.

`SET CONSTRAINTS ALL IMMEDIATE` inside the move's transaction forces `DEFERRABLE
INITIALLY DEFERRED` foreign-key checks to fire at the statement, where the `23503`
translation can catch them, instead of at commit. Identity sequences are read whole (`pg_sequence` plus
`pg_sequence_last_value`, which is NULL until the sequence has been called): the next
value it would issue is `seqstart`, or `last_value + seqincrement`. Only ids on that
arithmetic path, ahead of it, and inside `[seqmin, seqmax]` can ever be reissued, so only
those are chased — with `MAX` for an ascending sequence and `MIN` for a descending one.
`setval` past `seqmax` raises `22003`, and a bounded sequence whose remaining path is
entirely taken raises `2200H` on its next insert, which is why both are refused up front
instead.

`seqcache > 1` is refused too, and for the whole allocated region — `seqstart` through
`pg_sequence_last_value` — rather than the newest block: the catalog publishes only the
latest allocation, so a session that drew an earlier block (before another session moved
the catalog on) holds values no query can see and no `setval` can take back. Values from
before `seqstart` were never issued and are left alone. Measured on 17: session A's
`nextval` returns `1` and keeps `2..5`; session B's returns `6` and moves `last_value` to
`10`; `2` is still A's to issue.

A sequence is not transactional: `setval` stands whatever becomes of the transaction
around it. So every identity column of a destination is decided before any of its
sequences is moved — otherwise a refusal on the second column would leave the first one
spent on rows that rolled back, and a bounded one could be exhausted by a move that never
happened. What no amount of ordering can undo is a sequence that moves once the decision
is already made: a rollback *after* the move was accepted, or a failure partway through
issuing the `setval`s — a role holding `UPDATE` on one of a destination's sequences and
not the next fails the second with `42501`, and the first stays where it was put. Either
way the rows go back and the sequence does not, leaving it past ids that are no longer
there. That direction is safe — a sequence too far ahead skips values, it does not repeat
them — and it is why the ordering is worth having even though it cannot cover this: what
it prevents is a *semantic* refusal spending a sequence, which is the case that recurs.

Which rows those questions are about is settled by the move statement itself. Its
`INSERT` returns the identity values it placed, and one enclosing `INSERT` parks them in a
temporary relation for the length of the transaction — one statement, so what it reports
is exactly what it moved, with no second look at the destination to confuse a moved id
with one the destination already held. An identity column the source does not carry is
skipped: the sequence fills it as it would in any ordinary insert, so the move takes
nothing from it. The relation is named per move (`pg_partsmith_moved_` and a fresh
suffix): the temporary schema belongs to the session, and a caller may be holding names in
it of its own.

A cycling sequence is decided before any of that, and on its whole range: `CYCLE` restarts
it at `seqmin` (ascending) or `seqmax` (descending), which need not lie on the residue
class its increments were walking, so after a wrap it can issue values it previously
skipped. Measured on 17 with `INCREMENT 3 MINVALUE 1 MAXVALUE 10 START 2 CYCLE CACHE 2`:
`2, 5, 8` then the wrap to `1, 4, …` — a different class, and `last_value` moves *behind*
a block another session still holds.

## Overlaps and gaps

- Overlapping RANGE siblings (`ATTACH` or `CREATE … PARTITION OF`), overlapping LIST values,
  a hash bucket whose residue class overlaps an existing one → `42P17`
  (`partition "x" would overlap partition "y"`).
- A hash modulus that is not a factor of the next larger modulus → `42P17 every hash
  partition modulus must be a factor of the next larger modulus`. Mixed moduli are legal
  only when factor-chained (2 and 4; not 3 and 4) and their residue classes do not overlap.
- A row falling into a range gap (or an uncovered hash residue) → `23514 no partition of
  relation "x" found for row`.
- `ATTACH` of a partition when the DEFAULT partition holds rows for its range →
  `23514 updated partition constraint for default partition "x_default" would be violated by some row`.
- `ATTACH` of an already attached partition → `42809 "x" is already a partition`;
  `CREATE TABLE … PARTITION OF` with an existing name → `42P07`;
  `CREATE TABLE IF NOT EXISTS … PARTITION OF` **succeeds silently** against a same-named
  relation with different bounds — one reason the library never uses it.

## Bound rendering (`pg_get_expr(relpartbound, oid)`)

| Key type | Rendering |
|---|---|
| `timestamptz` | `FOR VALUES FROM ('2026-08-31 00:00:00+00') TO ('2026-09-07 00:00:00+00')` |
| `timestamp` / `date` | `FROM ('2026-01-01 00:00:00') TO (…)` / `FROM ('2026-01-01') TO (…)` (naive) |
| `bigint` | `FROM ('0') TO ('100000')`, `FROM ('-100') TO ('0')` — **quoted** |
| `int` | `FROM (MINVALUE) TO (0)` — bare keyword, unquoted literal |
| `numeric` | `FROM ('0') TO (100000.5)` — mixed |
| `uuid` | `FROM ('019a0000-0000-7000-8000-000000000000') TO (…)` |
| `text` | `FROM ('a') TO ('m')` |
| unbounded | `MINVALUE` / `MAXVALUE` bare |

`parse_partition_bounds` strips quotes and casts, so both spellings decode alike.

## Sizes, rows, cursors, identity

- `pg_total_relation_size(partitioned_relation)` is **0**; sizes are summed over the leaves
  of `pg_partition_tree` (`188416` for a 1 000-row branch with two buckets).
- `pg_class.reltuples` is `-1` before the first `ANALYZE`; `pg_stat_user_tables.n_live_tup`
  reads `0` until the statistics collector flushes and `535` shortly after. Estimates only.
- `pg_get_serial_sequence` works for `serial` and identity columns;
  `pg_sequence_last_value` is `NULL` for an unused sequence and equals `max(id)` after
  inserts.
- Dropping and recreating a table under the same name yields a **different OID**;
  `to_regclass('missing')` is `NULL`. The executor revalidates OIDs before detach and drop.
- `max_identifier_length` is 63.
