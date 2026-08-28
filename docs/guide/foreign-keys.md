# Handle foreign keys

Foreign keys and partition retirement interact in one way that matters: **PostgreSQL will
not detach a partition whose rows another table still references.** This guide shows what
happens, how to keep such partitions out of the plan, and what the library does when one
gets in anyway. Everything here was measured on PostgreSQL 15 and 17
([details](../design/postgresql-semantics.md#foreign-keys-measured-on-1519-and-1711-98-scenarios-no-difference)).

## What PostgreSQL does

Take `ci_artifacts` with a foreign key to the partitioned `ci_builds`:

```sql
CREATE TABLE ci_artifacts (
    build_id   BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (build_id, created_at) REFERENCES ci_builds (id, created_at)
);
```

PostgreSQL clones the constraint onto every partition of `ci_builds`. Detaching a
partition that still has referenced rows — plain or `CONCURRENTLY` — fails:

```text
ERROR:  removing partition "ci_builds__2026_06" violates foreign key constraint "ci_artifacts_build_id_created_at_fkey1"
DETAIL:  Key (id, created_at)=(1, 2026-06-10 00:00:00+00) is still referenced from table "ci_artifacts".
```

`ON DELETE CASCADE` does not help, and nothing is cascade-deleted. Once the referencing
rows are gone the detach succeeds, and the detached table carries no trace of the
constraint. (Dropping an *attached* partition would fail too, and `DROP … CASCADE` would
silently remove the whole foreign key from `ci_artifacts` — which is one reason the
library never drops an attached partition and never uses `CASCADE`.)

A foreign key *from* the partitioned table to another table is different: it survives on
the detached partition and is dropped by the library right before the partition is
dropped.

## What happens without any configuration

The plan expires the partition like any other; the detach is refused by PostgreSQL; the
executor records it and goes on:

```text
detach: public.ci_builds__2026_06
  PartitionReferencedError: Partition public.ci_builds__2026_06 is still referenced by rows of another table: removing partition "ci_builds__2026_06" violates foreign key constraint "ci_artifacts_build_id_created_at_fkey1"
```

The partition stays attached and in service, the other operations of the run happen, and
the issue repeats every tick until the referencing rows are gone. Safe, but noisy.

## Keep referenced partitions out of the plan

`Unreferenced()` is a retention predicate that asks the same question PostgreSQL asks —
*does any row of another table reference a row of this partition?* — and combines with
the calendar rules:

```python
from pg_partsmith import AllOf, ExpireIf, KeepNewest, LifecyclePolicy, Unreferenced

lifecycle = LifecyclePolicy(
    creation=CreateAhead(count=1),
    retention=ExpireIf(AllOf((KeepNewest(count=12), Unreferenced()))),
)
```

"Older than the twelve newest months *and* no longer referenced." A partition that is old
but still referenced is simply not expired — no detach is planned, no issue is raised —
and it expires on the tick after the last referencing row disappears:

```text
plan for public.ci_builds at 2026-08-28T10:00:00+00:00
  nothing to do
```

This is GitLab's rule for `ci_builds`: partitions are retired only once no pipeline points
into them.

## What it costs

`Unreferenced()` declares the `references` fact. When a rule declares it, the introspector
reads the incoming foreign keys of the parent (and of each candidate partition, for keys
pointing at a partition directly) and runs one `EXISTS` per foreign key and candidate,
joining the referencing table to the partition on the key columns:

```sql
SELECT EXISTS (SELECT 1 FROM "public"."ci_artifacts" r JOIN "public"."ci_builds__2026_06" p
               ON r."build_id" = p."id" AND r."created_at" = p."created_at")
```

It stops at the first match. Index the referencing side on its foreign-key columns —
you want that index anyway for `ON DELETE` checks. Nothing is measured for a policy that
does not ask.

A partition that could not be measured reads as referenced, so it is kept.

## Locks

Either form of `DETACH` takes `ACCESS EXCLUSIVE` on every table that references the parent
through a foreign key — the plain form for its duration, the concurrent form in its second
transaction — and waits for open transactions on them. `ATTACH` takes
`SHARE ROW EXCLUSIVE` on them. On a busy referencing table, prefer the concurrent detach
(`DetachMode.AUTO`, the default) and a maintenance window for the tick.
