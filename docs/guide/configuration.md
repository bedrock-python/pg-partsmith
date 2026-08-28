# Configuration

`TablePartitionConfig` describes one partitioned table: its **scheme** (which levels exist,
by which method, on which key, with which boundaries) and its **lifecycle policy** (when
partitions of the progression level are created, detached and dropped).

## The flat spelling

```python
from pg_partsmith import PartitionGranularity, TablePartitionConfig

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,
    retention_count=12,
)
```

| Field | Meaning |
|---|---|
| `schema` | Schema of the parent table. Optional, strongly recommended: everything is then schema-qualified and independent of `search_path`. Read back as `config.db_schema`. |
| `table_name` | The parent (partitioned) table, lowercase. |
| `partition_column` | Leading column of the partition key — the time dimension. |
| `trailing_partition_columns` | The rest of a composite key, in key order (bounded with `MINVALUE`). |
| `granularity` | `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`. |
| `tz` | Timezone the calendar is computed in (IANA name or `ZoneInfo`); `UTC` by default. |
| `boundary_codec` | Physical encoding of the key: `"uuidv7"`, `"epoch_seconds"`, `"epoch_milliseconds"`, or a codec instance. |
| `subpartition` | A level below the root (`HashPartitioning(...)` / `ListPartitioning(...)`). |
| `create_ahead_count` | Periods that must exist, the current one included. `3` in June means June, July, August. |
| `retention_count` | Newest periods kept, the current one included; older ones are detached and dropped. A *count*, not a distance. |
| `partition_type`, `partition_strategy` | Optional; checked against the scheme (`RANGE` / `TIME_BASED` for this spelling). |

The flat spelling covers a time-partitioned `RANGE` root. Everything else uses `scheme`.

## The composed spelling

```python
config = TablePartitionConfig(
    schema="public",
    table_name="events",
    scheme=RangePartitioning(
        key="created_at",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH, tz="Europe/Helsinki"),
        child=HashPartitioning(key="tenant_id", modulus=4),
    ),
    lifecycle=LifecyclePolicy(creation=CreateAhead(count=3), retention=KeepNewest(count=12)),
)
```

| Field | See |
|---|---|
| `scheme` | [Partition schemes](partition-schemes.md) — `RangePartitioning`, `ListPartitioning`, `HashPartitioning`, nesting, composite keys |
| `lifecycle` | [Lifecycle policies](lifecycle-policies.md) — creation, retention, detach mode, drop policy |

Passing `scheme` together with the flat scheme fields, or `lifecycle` together with
`create_ahead_count` / `retention_count`, is an error: one spelling per config.

## Derived views

Whichever spelling you use, the config exposes both:

```python
config.scheme                 # the root level
config.lifecycle              # the policy
config.partition_type         # PartitionType.RANGE / LIST / HASH — the root's method
config.partition_strategy     # TIME_BASED / NUMERIC_BASED / VALUE_BASED / HASH_BASED
config.partition_columns      # the root's whole key
config.granularity            # for a time-based root using a built-in granularity
config.create_ahead_count     # when the creation policy is CreateAhead, else None
config.retention_count        # when the retention policy is KeepNewest, else None
config.levels                 # every level, root first
config.qualified_name         # "public.events"
```

## Serialization

Configs are frozen Pydantic models. `config.model_dump(mode="json")` produces a JSON
document (`tz` as an IANA name, built-in codecs by name, policies discriminated on `kind`)
and `TablePartitionConfig.model_validate(...)` reads it back — including nested schemes,
numeric boundaries and combinator policies. Custom calculators, custom codecs and
`Callback` predicates are objects and are excluded.

`PartitionTableSettings` (`pip install pg-partsmith[pydantic-settings]`) loads the flat
fields from the environment and accepts `SCHEME` / `LIFECYCLE` as JSON for the composed
form:

```python
class OutboxSettings(PartitionTableSettings):
    model_config = SettingsConfigDict(env_prefix="OUTBOX_")

config = OutboxSettings().to_config()   # OUTBOX_TABLE_NAME, OUTBOX_PARTITION_COLUMN, OUTBOX_GRANULARITY, …
```

## Validation

At construction: identifiers are lowercased and checked, LIST keys are single-column, every
level partitions on a fresh column, generated names fit 63 bytes at every level.

At plan time, against the catalog: the parent is partitioned by the root's method on the
root's key (composite keys compared in key order; mixed-case and expression keys refused),
and every nested level's key columns appear in every `UNIQUE` / `PRIMARY KEY` constraint —
PostgreSQL would otherwise reject the first branch mid-run.
