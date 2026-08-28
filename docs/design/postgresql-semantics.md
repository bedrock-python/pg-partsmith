# PostgreSQL partition semantics, verified

Everything the 1.0 design relies on was measured against real servers rather than assumed:
`postgres:17-alpine` (17.11) and `postgres:15-alpine` (15.19) via testcontainers, on
2026-08-28. Both versions behaved identically in every case below. The measurement script
lives with the development scratch files; the integration suite re-asserts the load-bearing
facts on every run.

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
- only `ALTER TABLE … DETACH PARTITION … FINALIZE` completes it (`relispartition` → false).

`PartitionRemover.detach` checks `inhdetachpending` first and finalizes; the planner reports
such a partition as `DETACH_PENDING` (WARNING) and excludes it from every decision.

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

- `CREATE FOREIGN TABLE … PARTITION OF parent` is refused with
  `42809 cannot create foreign partition of partitioned table` when the parent has any
  index (a primary key counts); it succeeds on an index-free parent.
- In `pg_partition_tree` the foreign leaf has `relkind = 'f'`, `isleaf = true`, a normal
  `relpartbound`; `SELECT … FROM parent` reads through it.
- `DROP TABLE` and `COMMENT ON TABLE` on it fail with `42809 "x" is not a table`;
  `COMMENT ON FOREIGN TABLE` and `DROP FOREIGN TABLE` work; `pg_total_relation_size` is 0.
- `ATTACH`, `DETACH` and `DETACH … CONCURRENTLY` all work with a foreign partition; a foreign
  DEFAULT partition is accepted.
- `CREATE INDEX` on a parent that has a foreign partition succeeds (non-unique indexes skip
  foreign children).

The library never plans DDL for a foreign leaf and reports it as `FOREIGN_PARTITION`.

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
