# pg-partsmith 1.0 — API cheat sheet (what changed from 0.5, for porting tests and docs)

Source of truth is the code; this page is the map.

## Pure core (shared by aio and sync)

| Module | What lives there |
|---|---|
| `pg_partsmith/periods.py` | `PartitionGranularity`, `Period` (moved out of `entities`, still re-exported there) |
| `pg_partsmith/boundaries.py` | `Window(start, end, token)`, `RangeBoundaries` protocol, `TimeBoundaries(granularity=… \| calculator=…, tz=…, codec=…)`, `NumericBoundaries(step, origin=0, name_suffix="__{start}", cursor_source=MAX_KEY)`, `Axis`, `CursorSource`, codecs `RangeBoundaryCodec`, `UUIDv7BoundaryCodec`, `EpochBoundaryCodec(unit)` |
| `pg_partsmith/scheme.py` | `RangePartitioning(key, boundaries, child)`, `HashPartitioning(key, modulus, name_suffix="__h{remainder}", child)`, `ListPartitioning(key, groups, include_default, default_name, name_suffix="__{name}", child)`, `ListGroup(name, values)`, `PartitionScheme` (discriminated on `method`), `LevelKind` |
| `pg_partsmith/lifecycle.py` | `LifecyclePolicy(creation, retention, detach, drop)`; creation: `CreateAhead(count)`, `CreateUntil(position)`, `CreateNextIf(when)`; retention: `KeepNewest(count)`, `KeepFor(age)`, `KeepBehind(distance)`, `ExpireIf(when)`, `AllOf/AnyOf/Not`; predicates: `SizeAbove(bytes)`, `RowsAbove(rows)`, `WindowAgeAbove(age)`, `SqlPredicate(sql with {partition})`, `Callback(fn, facts, label)`; `DetachMode.AUTO/CONCURRENT/BLOCKING`; drop: `DropAfter(grace, when)`, `DropNever`; `Candidate` |
| `pg_partsmith/topology.py` | `PartitionType`, `RelationKind`, bounds (`RangeBounds`, `ListBounds`, `HashBounds`, `DefaultBounds`, `PartitionBounds`), `PartitionNode` (+`oid`, `relkind`, `detach_pending`, `facts`), `DetachedPartition(name, oid, relkind, parent_name, detached_at, facts)`, `ActualTree(root, orphans)`, `PartitionFacts`, `FactKind`, `PartitionTreeRow`, `build_partition_tree`, hash helpers |
| `pg_partsmith/plan.py` | `MaintenancePlan(table_name, generated_at, cursors, operations, findings)` with `.creates/.attaches/.detaches/.drops/.is_noop/.without(*kinds)/.only(*kinds)/.describe()`; operations `CreatePartition(target, parent_name, bounds, partition_by, key_columns, children, lifecycle_unit, counts_as, reason, detail)`, `AttachPartition`, `DetachPartition(mode, bounds)`, `DropPartition(detached_at, follows_detach)`; `Reason`, `Finding(partition_name, reason, severity, detail)`, `FindingReason`, `Severity`, `OperationKind`, `PartitionBy(method, columns)` |
| `pg_partsmith/planner.py` | `plan_maintenance(config, actual_tree, PlanningContext(now, cursors, mode, explicit_windows))`, `PlanMode.MAINTAIN/RECONCILE/EXPLICIT`, `fact_targets(config, tree)`, `to_maintenance_issue(finding)` |
| `pg_partsmith/entities.py` | `TablePartitionConfig(schema, table_name, scheme, lifecycle)` + flat sugar (`partition_column`, `trailing_partition_columns`, `granularity`, `tz`, `boundary_codec`, `subpartition`, `create_ahead_count`, `retention_count`; `partition_type`/`partition_strategy` optional and checked); derived props `partition_type`, `partition_strategy` (`TIME_BASED/NUMERIC_BASED/VALUE_BASED/HASH_BASED`), `partition_column`, `partition_columns`, `key_arity`, `granularity`, `time_boundaries`, `subpartition`, `create_ahead_count`, `retention_count`, `qualified_name`, `levels`, `is_time_based`, `has_progression_level`; `PartitionInfo` (+`oid`, `relkind`); `MaintenanceResult` (+`attached_count`, `plan`); `MaintenanceIssueStep` (+`PLAN`, `ATTACH`) |
| `pg_partsmith/utils.py` | + `orphan_comment(parent, detached_at=, existing_comment=, marker_prefix=)`, `parse_orphan_comment(comment, marker_prefix=)`, `DETACHED_AT_MARKER` |
| removed | `subpartition_plan.py`, `pruning_rules.py`, `HashSubpartitionSpec`, `ListSubpartitionSpec`, `SubpartitionSpec`, `root_layout`, `auto_attach_after_create`, `TopologyReason` (→ `FindingReason`), `TopologyFinding` (→ `Finding`), `SubpartitionPlan` (→ `MaintenancePlan`) |

## Config examples

```python
# flat (unchanged from 0.x, partition_type/partition_strategy now optional)
TablePartitionConfig(table_name="events", partition_column="created_at",
                     granularity=PartitionGranularity.MONTH, create_ahead_count=3, retention_count=12)
# flat + nested level
TablePartitionConfig(table_name="events", partition_column="created_at", granularity=PartitionGranularity.WEEK,
                     subpartition=HashPartitioning(key="tenant_id", modulus=4))
# composed
TablePartitionConfig(table_name="issue_events",
    scheme=RangePartitioning(key="id", boundaries=TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=UUIDv7BoundaryCodec()),
                             child=HashPartitioning(key="organization_id", modulus=2, name_suffix="_h{remainder}")),
    lifecycle=LifecyclePolicy(creation=CreateAhead(count=3), retention=KeepNewest(count=12), drop=DropAfter(grace=timedelta(days=7))))
# numeric
TablePartitionConfig(table_name="queue", scheme=RangePartitioning(key="msg_id", boundaries=NumericBoundaries(step=100_000)),
                     lifecycle=LifecyclePolicy(creation=CreateAhead(count=3), retention=KeepBehind(distance=1_000_000)))
# static roots
TablePartitionConfig(table_name="tasks", scheme=HashPartitioning(key="task_id", modulus=8))
TablePartitionConfig(table_name="regions", scheme=ListPartitioning(key="region", groups=(ListGroup(name="eu", values=("de","fr")),), include_default=True))
```

## aio / sync

Service wiring: `PartitionLifecycleService(repo, metadata, locks, hooks=None)` — **no `period_calculator` argument**; the calculator/tz/codec live in `TimeBoundaries`. `PostgresMetadataProvider(engine, marker_prefix=, boundary_codec=, ddl_timezone=)` (codec only for `is_partition_closed`).

Service methods: `inspect(config) -> ActualTree | None`, `plan(config, *, mode=, now=, windows=) -> MaintenancePlan` (no lock, no DDL), `apply(config, plan, *, continue_on_error=) -> MaintenanceResult` (lock), `maintain(config, *, skip_create, skip_detach, skip_drop, continue_on_error)` (= `maintain_lifecycle`, lock; plan + apply), `reconcile(config)`, `ensure_partition(config, period|Window) -> PartitionInfo | None`, `ensure_partitions(config, periods) -> list[PartitionInfo]`, granular `create_future_partitions(config)`, `get_partitions_for_pruning(config)`, `detach_old_partitions(table, partitions)`, `drop_detached_partitions(table, names)`.

Executor (`aio/services/execution.py` `PlanExecutor(repo, metadata, hooks)`): `apply(config, plan, continue_on_error)`, extension points `detach_single_partition(table_name, DetachPartition)`, `drop_single_partition(table_name, DropPartition)`. Inspector (`services/inspection.py` `PartitionInspector(metadata)`): `inspect(config, measure=)`, `context(config, now=, mode=, explicit_windows=)`. Validation unchanged in spirit (`PartitionValidationService(metadata).validate_config(config)`).

Repository protocol (`PartitionRepository`): `create_table_like(template_name, table_name, partition_by: PartitionBy | None)`, `attach_partition(parent, partition, bounds: PartitionBounds, *, key_arity=1)`, `detach_partition(parent, partition, *, mode=DetachMode.AUTO)`, `drop_partition(partition, *, expected_oid=None)`, `adopt_partition(table, partition) -> bool`, `reconcile_default_rows(*, default_partition_name, target_partition_name, key_columns, from_value, to_value) -> int`. Old `create_partition/create_branch/create_subpartition_table/attach_subpartition/attach_composite_partition` are gone.

Metadata protocol (`PartitionMetadataProvider`): `get_partition_type`, `get_partition_columns`, `get_actual_tree(table) -> ActualTree | None`, `measure(tree, *, targets, facts, sql_predicates) -> ActualTree`, `get_partition_tree(name) -> PartitionNode | None`, `get_default_partition`, `partition_exists`, `is_partition_attached`, `get_relation_oid(name)`, `get_unique_constraint_columns`, `get_key_high_water_mark(table, column, *, sequence=False)`, `list_partitions`. Plus on the Postgres implementation: `get_partition_column` (single-column only), `get_partition_boundaries`, `is_partition_closed`, `evaluate_sql_predicate`.

Hooks: `before_create(config, partition: PartitionInfo)` (was `(config, name, from_value, to_value)`); the other five unchanged. Hooks fire for partitions directly under the root only (lifecycle units / root set members), never per leaf of a subtree.

Semantics to test: attach-last subtree creation; a converged tree costs zero DDL; ownership by alignment (an attached partition whose bounds are not on the scheme's grid is `UNMANAGED_PARTITION` INFO and never touched; a finer-than-grid one is managed); orphan `COMMENT` marker line 1 unchanged, line 2 `pg-partsmith:detached-at=<iso>`; `DropAfter(grace)` keeps orphans until `detached_at + grace`, unknown instant = eligible; `DropNever` leaves detached tables alone; drops revalidate OID (`PlanStaleError` on a recreated table, recorded as an issue with `continue_on_error`, otherwise raised); detach revalidates OID and attachment; topology conflicts at apply time (`PartitionTopologyError`, e.g. DEFAULT holding rows for a hash bucket) are recorded as issues and never abort; other errors abort unless `continue_on_error`.

Exceptions: + `PlanStaleError(partition_name, detail)`.
