# Configuration fields

Every field of `TablePartitionConfig` and of the models it is built from, in one place.
Types are the accepted inputs; defaults are what you get when the field is omitted.

## `TablePartitionConfig`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `schema` | `str` | `None` | Schema of the parent table; read back as `db_schema`. Recommended. |
| `table_name` | `str` | required | The parent table, lowercase, at most 63 bytes minus the scheme's longest suffix. |
| `scheme` | `RangePartitioning \| ListPartitioning \| HashPartitioning` | from the flat fields | The root level and everything below it. |
| `lifecycle` | `LifecyclePolicy` | `CreateAhead(count=6)`, `KeepNewest(count=12)`, `DetachMode.AUTO`, `DropAfter()` | When partitions of the progression level are created, detached and dropped. |
| `leaves` | `LocalLeaves \| ForeignLeaves` | `LocalLeaves()` | What kind of relation the leaves are. |

Flat fields, accepted instead of `scheme` / `lifecycle` for a time-partitioned RANGE root:

| Field | Type | Default | Becomes |
|---|---|---|---|
| `partition_column` | `str` | required | `RangePartitioning.key[0]` |
| `trailing_partition_columns` | `tuple[str, ...]` | `()` | the rest of `key` |
| `granularity` | `PartitionGranularity` | required | `TimeBoundaries.granularity` |
| `tz` | `str \| tzinfo` | `UTC` | `TimeBoundaries.tz` |
| `boundary_codec` | `str \| RangeBoundaryCodec` | `None` | `TimeBoundaries.codec` |
| `subpartition` | `HashPartitioning \| ListPartitioning` | `None` | `RangePartitioning.child` |
| `create_ahead_count` | `int ≥ 1` | `6` | `CreateAhead(count)` |
| `retention_count` | `int ≥ 1` | `12` | `KeepNewest(count)` |
| `partition_type` | `PartitionType` | `None` | checked against the scheme's root method |
| `partition_strategy` | `PartitionStrategy` | `None` | checked against the scheme (`TIME_BASED`, `NUMERIC_BASED`, `VALUE_BASED`, `HASH_BASED`) |

Derived, read-only: `qualified_name`, `partition_type`, `partition_strategy`,
`partition_column`, `partition_columns`, `key_arity`, `granularity`, `time_boundaries`,
`subpartition`, `create_ahead_count`, `retention_count`, `levels`, `is_time_based`,
`is_progression_root`, `has_progression_level`, `manages_foreign_leaves`.

## Scheme levels

Common to every level:

| Field | Type | Meaning |
|---|---|---|
| `key` | `str \| tuple[str, ...]` | the level's partition key, in key order |
| `child` | a level | the level below, if any |

### `RangePartitioning`

| Field | Type | Meaning |
|---|---|---|
| `boundaries` | `TimeBoundaries \| NumericBoundaries \| RangeBoundaries` | the rule dividing the axis into windows; a dict is read by its `kind` |

### `HashPartitioning`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `modulus` | `int ≥ 1` | required | bucket count for newly created sets |
| `name_suffix` | `str` | `"__h{remainder}"` | appended to the parent's name; must contain `{remainder}` |

### `ListPartitioning`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `groups` | `tuple[ListGroup, ...]` | `()` | the partitions to maintain (a set level); mutually exclusive with `sequence` |
| `sequence` | `IntegerSequence` | `None` | one value per partition (a progression level); mutually exclusive with `groups` |
| `include_default` | `bool` | `False` | maintain a DEFAULT catch-all next to the groups; not with `sequence` |
| `default_name` | `str` | `"other"` | name fragment of that DEFAULT partition |
| `name_suffix` | `str` | `"__{name}"` | appended to the parent's name for a group; must contain `{name}` |

`ListGroup(name, values)`: a name fragment and the values it owns, written as string
literals.

## Boundaries

### `TimeBoundaries`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `granularity` | `PartitionGranularity` | — | `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`; exclusive with `calculator` |
| `tz` | `str \| tzinfo` | `UTC` | the calendar's timezone; IANA name or keyed `ZoneInfo` |
| `codec` | `str \| RangeBoundaryCodec` | `None` | `"uuidv7"`, `"epoch_seconds"`, `"epoch_milliseconds"` or an instance |
| `calculator` | `PeriodCalculator` | — | a custom calendar; carries its own `tz` and codec |

### `NumericBoundaries`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `step` | `int ≥ 1` | required | window width |
| `origin` | `int` | `0` | a window boundary the grid is anchored on |
| `name_suffix` | `str` | `"__{start}"` | must contain `{start}` |
| `cursor_source` | `CursorSource` | `MAX_KEY` | `MAX_KEY` or `SEQUENCE` |

### `IntegerSequence`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `start` | `int` | `1` | the first value, used when the level has no partition yet |
| `name_suffix` | `str` | `"__{value}"` | must contain `{value}` |
| `cursor_source` | `CursorSource` | `NEWEST_MEMBER` | `NEWEST_MEMBER`, `MAX_KEY` or `SEQUENCE` |

## `LifecyclePolicy`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `creation` | `CreateAhead \| CreateUntil \| CreateNextIf` | `CreateAhead(count=6)` | which windows must exist ahead of the cursor |
| `retention` | any predicate | `KeepNewest(count=12)` | when a window behind the cursor has expired |
| `detach` | `DetachMode` | `AUTO` | `AUTO`, `CONCURRENT`, `BLOCKING` |
| `drop` | `DropAfter \| DropNever` | `DropAfter()` | what happens after the detach |

Creation rules: `CreateAhead(count ≥ 1)`; `CreateUntil(position)` — a `datetime`, an
`int`, or their string forms; `CreateNextIf(when)` — a predicate over the newest
partition.

Retention rules and predicates: `KeepNewest(count ≥ 1)`, `KeepFor(age: timedelta)`,
`KeepBehind(distance ≥ 1)`, `ExpireIf(when)`, `SizeAbove(bytes ≥ 1)`, `RowsAbove(rows ≥ 0)`,
`WindowAgeAbove(age)`, `Unreferenced()`, `SqlPredicate(sql)` (must contain `{partition}`),
`Callback(fn, facts=frozenset(), label="callback")`, `AllOf(members)`, `AnyOf(members)`,
`Not(member)`.

Drop rules: `DropAfter(grace: timedelta = 0, when: predicate | None = None)`;
`DropNever()`.

## Leaves

### `LocalLeaves`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `tablespace` | `str` | `None` | tablespace for every created relation |
| `storage_parameters` | `dict[str, str \| int \| float \| bool]` | `{}` | `WITH (…)` on every created leaf; names `name` or `toast.name` |
| `inherit_privileges` | `bool` | `False` | replay the parent's owner and grants |

### `ForeignLeaves`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `server` | `str` | required | the foreign server |
| `options` | `dict[str, str]` | `{}` | foreign table options; values may use `{relname}`, `{schema}`, `{parent}`, `{root}` |

## Repository and metadata provider

`PostgresPartitionRepository(engine, *, ddl_timezone="UTC", ddl_timeout_seconds=30,
marker_prefix=None, drop_allow_unmanaged=False, drop_lock_timeout_ms=3000,
drop_max_retries=3, drop_retry_delay=0.5, drop_max_backoff=300)`.

`PostgresMetadataProvider(engine, *, marker_prefix=None, boundary_codec=None,
ddl_timezone=None)`.

`marker_prefix` must be the same on the repository and the provider — the first writes the
ownership marker, the second finds it — and `PartitionLifecycleService` refuses a pair that
disagrees. The provider's `boundary_codec` and `ddl_timezone` are used by
`is_partition_closed` alone, which also takes `boundaries=config.time_boundaries` and reads
both from there instead.

`PostgresAdvisoryLockManager(engine, prefix="partitioner", acquire_min_interval_seconds=0)`;
`RedisDistributedLockManager(redis_client, prefix="partitioner:lock", ttl_seconds=300,
acquire_min_interval_seconds=0)`.

## Serialization

`config.model_dump(mode="json", by_alias=True)` round-trips through
`TablePartitionConfig.model_validate`. `config.fingerprint` is a digest of that form,
which is what a plan records in `config_fingerprint` and what `apply()` compares against;
anything excluded from serialization is invisible to it, so two configurations differing
only in a `Callback`'s function share a fingerprint. Boundaries dump with a `kind` (`time`, `integer`,
`sequence`), levels with `method` (`range`, `list`, `hash`), policies and predicates with
`kind`, leaves with `kind` (`local`, `foreign`). Custom calculators, custom codecs and
`Callback` functions are left out.
