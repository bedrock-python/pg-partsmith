# Lifecycle policies

A scheme says *what* partitions exist; a `LifecyclePolicy` says *when* the partitions of
a progression level are created, detached and dropped. It has five parts:

```python
from datetime import timedelta

from pg_partsmith import CreateAhead, DetachMode, DropAfter, ExpireAction, KeepNewest, LifecyclePolicy

LifecyclePolicy(
    creation=CreateAhead(count=3),               # which windows must exist ahead of the cursor
    retention=KeepNewest(count=12),              # when a window behind the cursor has expired
    expire=ExpireAction.DETACH,                  # detach an expired partition, or empty it in place
    detach=DetachMode.AUTO,                      # how an expired partition is detached
    drop=DropAfter(grace=timedelta(days=7)),     # what happens to it afterwards
)
```

The flat fields `create_ahead_count` / `retention_count` are this policy with
`CreateAhead` and `KeepNewest`; spell `lifecycle=` out for anything else.

A rule with one defining value takes it positionally as well: `CreateAhead(3)`,
`KeepNewest(12)`, `KeepFor(timedelta(days=90))`, `DropAfter(timedelta(days=7))`.

## The timeline of one partition

```text
                cursor (now)
                    │
   ──┬─────────┬────┼────┬─────────┬─────────┬──►  time
     │ expired │kept│ current   │ ahead   │ ahead │
     │         │    │           │         │       │
     ▼         ▼    ▼           ▼         ▼       ▼
  detach     keep  never      create   create  (not yet)
  → grace          expired    ahead    ahead
  → drop
```

Ahead of the cursor, the **creation** rule decides which windows must exist. Behind it,
the **retention** rule decides which have expired. The cursor's own window and everything
ahead of it receive rows and are never expired, whatever the rule says. An expired
partition is **detached** — it leaves the parent but keeps its data — and then, once the
**drop** rule allows, dropped. A policy can [empty it in place](#empty-in-place-instead-of-detaching)
instead, and leave it attached.

## Creation

| Rule | Windows that must exist |
|---|---|
| `CreateAhead(count)` | the cursor's window and the `count − 1` after it. `CreateAhead(count=3)` in June: June, July, August |
| `CreateUntil(position)` | every window from the cursor's up to the one holding `position` — a `datetime` on a time axis, an `int` on an integer one. "Partitions through the end of next year" is `CreateUntil(position=datetime(2028, 1, 1, tzinfo=UTC))` |
| `CreateNextIf(when)` | the cursor's window always; the window after the newest existing partition only once `when` holds for that partition — rotation by application state, not by the calendar |

`CreateNextIf` is what a [sliding list](schemes.md#list-with-a-sequence-the-sliding-list)
uses ("open the next value once the newest holds a day of data"), and what an
id-partitioned queue can use to size windows by volume. A sliding list refuses
`CreateAhead`: its cursor *is* the newest partition, so "ahead" would never converge.

## Retention

| Rule | A window is expired when |
|---|---|
| `KeepNewest(count)` | it ends at or before the start of the window `count − 1` steps behind the cursor's. A *count* of windows, current one included — the `retention_count` semantics |
| `KeepFor(age)` | it ended at least `age` ago (time axis only) |
| `KeepBehind(distance)` | the cursor is `distance` or more past its end (integer axis only; pg_partman's rule for id sets) |
| `ExpireIf(predicate)` | the predicate holds |
| `AllOf(…)`, `AnyOf(…)`, `Not(…)` | combinations |

The retention rules are predicates too, so they combine with everything else. "Older than
twelve months *and* nothing pending in it":

```python
from pg_partsmith import AllOf, ExpireIf, KeepNewest, SqlPredicate

retention=ExpireIf(when=AllOf(members=(
    KeepNewest(count=12),
    SqlPredicate(sql="SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')"),
)))
```

!!! warning "Count, not distance"
    Hand-rolled pruners usually say "drop everything older than N months", which keeps
    `N + 1` partitions on disk. `KeepNewest(count=N)` keeps exactly `N`. Coming from a distance,
    pass `N + 1`, or express the age directly with `KeepFor`.

## Predicates and facts

Every rule is a pure function of a **candidate**: the partition's window, its node in the
tree, the cursor, and its **facts**. Facts are what the introspector measured because a
rule asked for it — nothing is measured for a policy that does not ask, so a monthly table
with `KeepNewest` never pays for `pg_total_relation_size`.

| Predicate | Needs | True when |
|---|---|---|
| `SizeAbove(bytes)` | size | the partition and its subtree exceed `bytes` on disk |
| `RowsAbove(rows)` | rows | the planner's row estimate exceeds `rows` — never `COUNT(*)`; a fresh partition reads as empty until statistics catch up |
| `WindowAgeAbove(age)` | — | the window ended at least `age` ago |
| `Unreferenced()` | references | no row of another table references a row of the partition through a foreign key — the condition PostgreSQL itself imposes on `DETACH`. An unmeasured partition reads as referenced, so it is kept |
| `SqlPredicate(sql)` | one query per candidate | the statement yields true. `{partition}` is replaced with the quoted name; nothing else is interpolated. A partition that does not exist yet reads as false |
| `Callback(fn, facts=…, label=…)` | what it declares | `fn(candidate)` returns true — plain Python over the gathered facts, usable from both mirrors |
| `AllOf`, `AnyOf`, `Not` | the union of their members' needs | — |

Facts are gathered only for the partitions a policy can decide over — the members of
progression levels and their detached orphans — in one query for sizes and rows, one
`EXISTS` per incoming foreign key for references, one query per `SqlPredicate` and
candidate. The numbers appear on the plan (`size_bytes`, `row_estimate`). A policy that
empties partitions in place also asks whether each one is empty already (`FactKind.EMPTY`,
in the same query as sizes) and which tables reference it (`FactKind.REFERENCED_BY`, one
catalog query).

A policy decides *eligibility*; it never executes DDL. Ownership, safety and locking stay
with the core, which is what keeps a user predicate from turning into an accidental
`DROP TABLE`.

## Detach

| `DetachMode` | Statement |
|---|---|
| `AUTO` (default) | `DETACH … CONCURRENTLY`, falling back to the blocking form when PostgreSQL refuses it |
| `CONCURRENT` | the concurrent form only; the refusal propagates |
| `BLOCKING` | plain `DETACH`: `ACCESS EXCLUSIVE` on the parent for the duration |

The concurrent form takes `SHARE UPDATE EXCLUSIVE` on the parent and lets readers and
writers through; it cannot run inside a transaction block, so it goes out on an
autocommit connection. A detach interrupted mid-way is finished with `DETACH … FINALIZE`
on the next attempt.

PostgreSQL refuses the concurrent form outright while the parent holds a DEFAULT
partition. The planner reads that from the catalog, so an `AUTO` detach of a parent with
a DEFAULT partition is planned as the blocking form and says so — `plan --locks` names
the `ACCESS EXCLUSIVE` it will take, and no statement is issued that could only fail.
`CONCURRENT` is left as written: asking for the concurrent form and being refused is the
answer that mode exists to give.

Either form takes `ACCESS EXCLUSIVE` on every table that references the parent through a
foreign key, and neither can detach a partition whose rows such a table still references:
PostgreSQL refuses with `23503`. The executor records that as an issue and goes on;
`Unreferenced()` in the retention rule keeps those partitions out of the plan until the
referencing rows are gone. See [Handle foreign keys](../guide/foreign-keys.md).

## Drop

| Rule | Effect |
|---|---|
| `DropAfter()` (no grace) | dropped in the same run as the detach |
| `DropAfter(grace=timedelta(days=7))` (the default when the policy names no drop) | kept detached for a week, then dropped. The detach instant is recorded on the table's marker; an orphan marked by an older version, or adopted, has no instant and is treated as past its grace |
| `DropAfter(grace=…, when=predicate)` | dropped only once the grace has passed *and* the predicate holds — "not while bigger than 150 GB on a weekday" is a `Callback` over the size fact |
| `DropNever()` | detached partitions are left alone; something else owns the drop — an archive pipeline, a DBA. They are never re-attached, and a wanted window whose name such a table holds is reported (`name_unusable`), not recreated over it |

While a detached partition waits it is an **orphan**: still in the database, no longer
reachable through the parent, carrying the marker that makes it the library's to drop. If
retention grows again before the grace runs out and the orphan's window is wanted, it is
**re-attached** — the data comes back rather than being recreated empty — and the marker
comes off in the attach's transaction, so a later detach starts a fresh grace period. Under
`DropNever` orphans belong to whatever process the policy hands them to and are never
brought back.

## Empty in place instead of detaching

`expire=ExpireAction.TRUNCATE` keeps an expired partition attached and empties it with
`TRUNCATE`, subtree included:

```python
from datetime import timedelta

from pg_partsmith import ExpireAction, KeepFor, LifecyclePolicy

LifecyclePolicy(retention=KeepFor(timedelta(days=365)), expire=ExpireAction.TRUNCATE)
```

In a document it is `expire: truncate` beside `retention` in the table's `lifecycle`.

Choose it when the set of partitions should not change: the table keeps a partition for
every window it ever had, and whatever reads the tree sees the same one before and after a
run. It is also lighter on the parent. `TRUNCATE` takes `ACCESS EXCLUSIVE` on the
partition and the relations below it and nothing on the parent, so inserts and pruned
reads that go to other partitions do not wait for it. A blocking detach, the only kind
PostgreSQL allows once the parent has a DEFAULT partition, holds `ACCESS EXCLUSIVE` on the
parent and on the DEFAULT partition while it runs. Both were measured on PostgreSQL 15 to
18 (see [PostgreSQL semantics](../design/postgresql-semantics.md)). A query that has to
read the partition being emptied, an unpruned scan of the parent included, still waits for
it, and the truncate waits at most `ddl_lock_timeout_ms` for its lock before it gives up
until the next run.

| The expired partition | What the plan does |
|---|---|
| holds rows | `TruncatePartition` (kind `truncate`, reason `retention_expired`) |
| is empty already: no leaf has a heap page | nothing; one `expired_empty` INFO finding per parent counts them |
| holds rows, and another table references it through a foreign key | nothing; a `truncate_blocked` WARNING names the referencing tables |
| holds rows, and its subtree holds a foreign table | nothing; a `truncate_blocked` WARNING |

Emptiness is read from the catalog, never by scanning, so a second run issues no DDL. A
row written into the window later makes the partition non-empty, and the next run empties
it again. PostgreSQL refuses `TRUNCATE` of a partition any foreign key reaches, whether or
not a row is actually referenced, so one foreign key on the parent blocks every partition:
expire such a table by detaching, with `Unreferenced()` in the retention rule. `TRUNCATE` of
a foreign table empties the remote table behind it, so `ForeignLeaves` with
`expire=TRUNCATE` is refused when the configuration is built.

An emptied partition is never taken out of the tree, and the `expired_empty` finding is how
you see the set grow. To retire them, switch `expire` back to `detach`: retention still says
they expired, so the next run detaches them and the drop rule takes over. `detach` and
`drop` keep governing the detached tables that already exist, such as the ones an earlier
detaching policy left in their grace period.

`before_truncate` and `after_truncate` fire around the statement, and `on_event` sees both
(see [Archive before dropping](../guide/archiving.md#the-hooks)). `apply` without
`--allow-destructive` withholds truncates like detaches and drops, and
`maintain(skip_detach=True)` leaves them out.

## One policy, every progression level

A configuration has one `LifecyclePolicy`, applied at every progression level of its
scheme. A scheme with no progression level — a root `HASH`, a `LIST` of fixed groups —
has a fixed partition set; its policy is ignored.

Every rule is a Pydantic model discriminated on `kind`, so `config.model_dump(mode="json")`
round-trips — except `Callback`, whose function cannot be serialized.
