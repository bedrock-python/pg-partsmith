# Change a scheme safely

Configurations change: a bucket count grows with the tenant base, monthly partitions
become weekly, a tenant dimension is added under the time dimension. This guide says what
each change does to the existing tree and what it leaves alone. The rule behind all of
it: **history is preserved, new partitions follow the new shape.**

Before any change, plan against a copy and read the findings.

## Changing the hash bucket count

```python
HashPartitioning(key="tenant_id", modulus=4)     # was 2
```

A hash set cannot change modulus without a rewrite, and PostgreSQL refuses a bucket that
would overlap an existing set. So:

- **existing periods keep their buckets** — a complete two-bucket week is reported as
  `modulus_preserved` (INFO) and left alone;
- **an incomplete old set is repaired at its own modulus** (`hash_gap_historical_modulus`),
  never at the new one;
- **new periods** get four buckets.

To rebucket history, migrate the data yourself: create the new period tree, move the
rows, detach the old branch. `unpartition` into a flat table followed by `partition_data`
under the new scheme is the blunt but safe version.

## Changing the granularity

```python
granularity=PartitionGranularity.WEEK      # was MONTH
```

A monthly partition is **not on a weekly grid** — it spans several weeks — so it becomes
`unmanaged_partition`: kept, never expired, and a wanted week that overlaps it is not
created (`range_overlap`, WARNING) until the month ends. The weekly lifecycle starts with
the first week that lies entirely after the last monthly partition.

The other direction works better: a week that lies wholly *inside* a month is inside a
cell of the monthly grid, so under a monthly configuration it stays managed and retention
retires it by its own upper bound (a week straddling two months is unmanaged, like any
partition that crosses a cell boundary). The first tick under the new configuration creates monthly partitions from the
current month onward; weeks already created ahead overlap the current month and are
reported until they are behind the cursor.

Either way: plan first, expect findings for the transition period, and do not try to
"fix" the old partitions — they hold data.

## Changing the timezone

Existing partitions are never reinterpreted. A month created under `UTC` and read under
`Europe/Helsinki` has bounds that are no longer midnights of the new calendar; it lies
*inside* the new grid's cell only if it fits, otherwise it straddles two cells and becomes
unmanaged. Change the timezone of a table you have not started, or accept a transition
period. The service refuses a calendar whose timezone disagrees with the repository's
`ddl_timezone`, so change both together.

## Adding a level under the root

```python
subpartition=HashPartitioning(key="tenant_id", modulus=4)
```

Existing months are plain tables and cannot gain partitions; they are reported as
`legacy_leaf` (INFO) and stay valid. New months are created as branches with their
buckets. Nothing else happens — which means the table has mixed shapes for as long as the
retention window, and queries keep working throughout.

Before enabling, check the constraints: every `UNIQUE` / `PRIMARY KEY` must contain
`tenant_id` too, or the configuration is refused at plan time.

## Changing the retention

Growing it re-attaches orphans still in their grace period whose windows are now wanted
(`reattach`) and leaves everything else. Shrinking it expires more on the next tick —
plan first, and consider a grace period for the first run under the new value.

## Changing the creation rule

Purely forward-looking. Switching from `CreateAhead` to `CreateUntil` creates up to the
horizon on the next tick; switching to `CreateNextIf` stops creating until the newest
partition satisfies the predicate. Nothing existing is touched.

## Renaming conventions

Names are never parsed for truth, so a new `name_suffix` or calculator only changes new
partitions. Old names stay; the tree has two conventions until the old partitions age
out. Detached orphans *are* recognised by name (it is all a detached table has), so an
orphan named by the old convention is not re-attached — but it is still dropped when its
grace ends, because the marker, not the name, makes it ours.

## Switching leaf backends

`LocalLeaves` settings apply to partitions created after the change; existing ones keep
their tablespace, storage parameters and grants. Switching to `ForeignLeaves` makes new
leaves foreign and — importantly — makes *existing* foreign partitions on the grid
managed, so check the plan for detaches you did not expect. Switching back leaves the
foreign partitions in place, reported as `foreign_partition`.

## A checklist

1. Plan against a staging copy with the new configuration.
2. Read every `WARNING` finding; INFO findings are the transition period explaining itself.
3. Decide what to do with unmanaged partitions by hand, if anything.
4. Deploy the configuration to every replica at once — two configurations on one table
   would each undo the other's creations.
5. Watch `result.issues` for a few ticks.
