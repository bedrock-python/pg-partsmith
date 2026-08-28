# Glossary

**Actual tree** — the partition tree as the catalog reports it: every node with its
bounds, OID and kind, plus the marker-tagged orphans below the root.
[Partition schemes](../concepts/schemes.md#introspection)

**Adopt** — write the marker on a detached table the library did not detach, making it an
orphan the lifecycle may drop. [Ownership](../concepts/ownership.md#adopting-legacy-orphans)

**Boundaries** — the rule dividing a progression axis into windows and naming them:
`TimeBoundaries`, `NumericBoundaries`, `IntegerSequence`. [Boundaries](../concepts/boundaries.md)

**Branch** — a partition that partitions further; always a local partitioned table.

**Candidate** — a partition as a policy rule sees it: its window, its node, the cursor and
its facts. [Lifecycle policies](../concepts/lifecycle.md#predicates-and-facts)

**Codec** — the translation between period instants and the literals of an encoded key
(UUIDv7, epoch). [Boundaries](../concepts/boundaries.md#codecs-time-partitions-over-an-encoded-key)

**Cursor** — "now" on a progression axis: the clock, the key's high-water mark, or the
newest partition. [Boundaries](../concepts/boundaries.md#cursors-in-one-place)

**Facts** — what the introspector measured about a partition because a rule asked: size,
row estimate, references, SQL answers. [Lifecycle policies](../concepts/lifecycle.md#predicates-and-facts)

**Finding** — something the planner saw and chose not to change, with a reason and a
severity. [The maintenance plan](../concepts/plan.md#findings) · [reference](findings.md)

**Grace period** — the time a detached partition is kept before it is dropped
(`DropAfter(grace=…)`). [Lifecycle policies](../concepts/lifecycle.md#drop)

**Grid** — every window a boundaries rule can produce; the test of ownership for an
attached partition. [Boundaries](../concepts/boundaries.md#windows-and-the-grid)

**Issue** — a per-partition problem on a `MaintenanceResult`: a warning finding, or a
failure met while executing. [Monitor and alert](../guide/monitoring.md)

**Leaf** — a relation that stores rows: the deepest member of every branch. Local or
foreign. [Leaf backends](../concepts/leaves.md)

**Level** — one `PARTITION BY` in the tree; a root or a nested one.
[Partition schemes](../concepts/schemes.md)

**Lifecycle policy** — when the partitions of a progression level are created, detached
and dropped. [Lifecycle policies](../concepts/lifecycle.md)

**Lifecycle unit** — the partition directly under a progression level: what is created,
counted, hooked and expired as one, subtree included.

**Marker** — the `COMMENT` written on a table at detach that makes it the library's to
drop. [Ownership](../concepts/ownership.md#detached-tables-the-marker)

**Orphan** — a detached table carrying the marker. [Ownership](../concepts/ownership.md)

**Plan** — the operations and findings for one run of one table.
[The maintenance plan](../concepts/plan.md)

**Progression level** — a level whose members form an open-ended sequence with a cursor:
RANGE windows, or a sliding LIST. The lifecycle dimension.

**Reconcile** — create the members a set level is missing, without creating ahead or
expiring. `service.reconcile()`.

**Scheme** — the shape of the tree, level by level. [Partition schemes](../concepts/schemes.md)

**Set level** — a level whose members form a complete set: HASH buckets, LIST groups.
Reconciled, never expired.

**Sliding list** — a LIST level over an `IntegerSequence`: one value per partition, rotated
by application state. [Partition schemes](../concepts/schemes.md#list-with-a-sequence-the-sliding-list)

**Tick** — one maintenance run for one table: plan and apply under the table's lock.
[Schedule maintenance](../guide/scheduling.md)

**Window** — one slot of a progression axis, `[start, end)`: a month, a 100 000-id step,
one list value. [Boundaries](../concepts/boundaries.md#windows-and-the-grid)
