# Lifecycle policies

A scheme says *what* partitions exist; a `LifecyclePolicy` says *when* the partitions of
a progression level are created, detached and dropped.

```python
from datetime import timedelta

from pg_partsmith import CreateAhead, DetachMode, DropAfter, KeepNewest, LifecyclePolicy

LifecyclePolicy(
    creation=CreateAhead(count=3),
    retention=KeepNewest(count=12),
    detach=DetachMode.AUTO,
    drop=DropAfter(grace=timedelta(days=7)),
)
```

Every rule is evaluated by the pure planner against a `Candidate` it already knows
everything about. What a rule needs beyond the catalog — a size, a row estimate, the answer
to a SQL question — it declares up front, and the introspector gathers exactly that. A
monthly table with `KeepNewest` never pays for `pg_total_relation_size`.

A policy decides *eligibility*; it never executes DDL. Ownership, safety and locking stay
with the core, so a user predicate cannot turn into an accidental `DROP TABLE`.

## Creation

| Policy | Windows that must exist |
|---|---|
| `CreateAhead(count)` | the cursor's window and the `count − 1` after it |
| `CreateUntil(position)` | every window from the cursor's up to the one holding `position` — a `datetime` on a time axis, an `int` on an integer axis. "Partitions through the end of next year" is `CreateUntil(datetime(2028, 1, 1, tzinfo=UTC))` |
| `CreateNextIf(when)` | the cursor's window always; the window after the newest existing partition only once `when` holds for that partition — rotation by application state rather than by the calendar |

The **cursor** is "now" on the axis: the clock for time (in the calendar's timezone),
`max(key)` — or the serial/identity sequence, with `NumericBoundaries(cursor_source=CursorSource.SEQUENCE)`
— for integers. An empty integer-keyed table starts at `origin`.

## Retention

A window behind the cursor is *expired* when the retention rule says so. The cursor's own
window and everything ahead of it receive rows and are never expired, whatever the rule.

| Policy | Expired when |
|---|---|
| `KeepNewest(count)` | the window ends at or before the start of the window `count − 1` steps behind the cursor's (the 0.x `retention_count` semantics: a *count* of periods, current one included) |
| `KeepFor(age)` | the window ended more than `age` ago (time axis) |
| `KeepBehind(distance)` | the cursor is `distance` or more past the window's end (integer axis; `pg_partman`'s id retention) |
| `ExpireIf(predicate)` | the predicate holds |
| `AllOf(...)`, `AnyOf(...)`, `Not(...)` | combinations of the above |

"Older than twelve periods *and* nothing pending in it":

```python
retention=ExpireIf(AllOf((
    KeepNewest(count=12),
    SqlPredicate("SELECT NOT EXISTS (SELECT 1 FROM {partition} WHERE status = 'pending')"),
)))
```

## Predicates

| Predicate | Needs | True when |
|---|---|---|
| `SizeAbove(bytes)` | size | the partition and its subtree exceed `bytes` on disk |
| `RowsAbove(rows)` | rows | the planner's row estimate exceeds `rows` (never `COUNT(*)`; a fresh partition reads as empty until statistics catch up) |
| `WindowAgeAbove(age)` | — | the window ended more than `age` ago |
| `SqlPredicate(sql)` | one query per candidate | the statement yields true; `{partition}` is replaced with the quoted name, nothing else is interpolated; a partition that does not exist yet reads as false |
| `Callback(fn, facts=..., label=...)` | what it declares | `fn(candidate)` returns true — pure Python over the gathered facts, usable from both mirrors |
| `AllOf`, `AnyOf`, `Not` | union of members | — |

Facts are gathered only for the partitions a policy can decide over (progression-level
members and their orphans), in one query for every target, and only when some rule asks.

## Detach

| `DetachMode` | Statement |
|---|---|
| `AUTO` (default) | `DETACH … CONCURRENTLY`, falling back to the blocking form when PostgreSQL refuses it — it does when a DEFAULT partition exists |
| `CONCURRENT` | the concurrent form only; the refusal propagates |
| `BLOCKING` | plain `DETACH`, `ACCESS EXCLUSIVE` on the parent |

The concurrent form cannot run inside a transaction block, so it goes out on an autocommit
connection; a detach interrupted mid-way (`inhdetachpending`) is finished with
`DETACH … FINALIZE` on the next attempt. The ownership marker is written *before* the
detach, so an interrupted run leaves a marked table rather than one orphan discovery would
never see.

## Drop

| Policy | Effect |
|---|---|
| `DropAfter(grace=timedelta(0))` (default) | dropped in the same run as its detach |
| `DropAfter(grace=timedelta(days=7))` | kept detached for a week, then dropped — the marker records the detach instant; an orphan marked by an older version or adopted with `adopt_partition` has no instant and is treated as past its grace |
| `DropAfter(grace=..., when=predicate)` | dropped only once the grace has passed *and* the predicate holds — "not while bigger than 150 GB on a weekday" is a `Callback` over `SizeAbove` facts |
| `DropNever` | detached partitions are left alone; something else owns the drop (a cold-storage pipeline, a DBA) |

A drop is executed only against the relation the plan saw: the table's OID is revalidated
under `ACCESS EXCLUSIVE` together with its attachment state and its marker.

## Where the policy lives

One `LifecyclePolicy` per config, applied at every progression level of the scheme. A
scheme with no progression level (a root `HASH` or a static `LIST`) has a fixed partition
set; its policy is ignored.

Serialization: every policy is a Pydantic model discriminated on `kind`, so
`config.model_dump(mode="json")` round-trips — except `Callback`, whose function is
excluded.
