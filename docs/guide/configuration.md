# Configure a table

`TablePartitionConfig` describes one partitioned table. This guide shows the two ways to
write one, how to load it from the environment or JSON, and what is checked when.

## The flat spelling

For the ordinary time-partitioned table — a `RANGE` root over a timestamp, one partition
per period:

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
| `schema` | Schema of the parent table. Optional but recommended: everything is then schema-qualified and independent of `search_path`. |
| `table_name` | The parent table, lowercase. |
| `partition_column` | Leading column of the partition key — the time dimension. |
| `trailing_partition_columns` | The rest of a composite key, in key order. |
| `granularity` | `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`. |
| `tz` | Timezone the calendar is computed in (IANA name or `ZoneInfo`); `UTC` by default. |
| `boundary_codec` | Physical encoding of the key: `"uuidv7"`, `"epoch_seconds"`, `"epoch_milliseconds"`, or a codec instance. |
| `subpartition` | A level below the root: `HashPartitioning(...)` or `ListPartitioning(...)`. |
| `create_ahead_count` | Periods that must exist, the current one included. |
| `retention_count` | Newest periods kept, the current one included — a count, not a distance. |
| `partition_type`, `partition_strategy` | Optional; checked against the scheme. |

## The composed spelling

Everything else — an integer axis, a hash or list root, a sliding list, a policy beyond
"N ahead, M kept", leaves that are not plain tables — is spelled out:

```python
from datetime import timedelta

from pg_partsmith import (
    CreateAhead,
    DropAfter,
    HashPartitioning,
    KeepNewest,
    LifecyclePolicy,
    LocalLeaves,
    PartitionGranularity,
    RangePartitioning,
    TablePartitionConfig,
    TimeBoundaries,
)

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    scheme=RangePartitioning(
        key="created_at",
        boundaries=TimeBoundaries(granularity=PartitionGranularity.MONTH, tz="Europe/Helsinki"),
        child=HashPartitioning(key="tenant_id", modulus=4),
    ),
    lifecycle=LifecyclePolicy(
        creation=CreateAhead(count=3),
        retention=KeepNewest(count=12),
        drop=DropAfter(grace=timedelta(days=7)),
    ),
    leaves=LocalLeaves(storage_parameters={"fillfactor": 90}),
)
```

| Field | See |
|---|---|
| `scheme` | [Partition schemes](../concepts/schemes.md) |
| `lifecycle` | [Lifecycle policies](../concepts/lifecycle.md) |
| `leaves` | [Leaf backends](../concepts/leaves.md) |

Passing `scheme` together with the flat scheme fields, or `lifecycle` together with
`create_ahead_count` / `retention_count`, is an error: one spelling per config.

Whichever spelling you used, both views are there:

```python
config.scheme                 # the root level
config.lifecycle              # the policy
config.leaves                 # what kind of relation the leaves are
config.partition_type         # PartitionType.RANGE / LIST / HASH — the root's method
config.partition_strategy     # TIME_BASED / NUMERIC_BASED / VALUE_BASED / HASH_BASED
config.partition_columns      # the root's whole key
config.levels                 # every level, root first
config.qualified_name         # "public.events"
```

## From the environment

`PartitionTableSettings` (`pip install "pg-partsmith[pydantic-settings]"`) reads the flat
fields from environment variables, and `SCHEME` / `LIFECYCLE` / `LEAVES` as JSON for the
composed form:

```python
from pydantic_settings import SettingsConfigDict

from pg_partsmith.settings import PartitionTableSettings


class EventsSettings(PartitionTableSettings):
    model_config = SettingsConfigDict(env_prefix="EVENTS_")


config = EventsSettings().to_config()
```

```bash
EVENTS_TABLE_NAME=events
EVENTS_SCHEMA_NAME=public
EVENTS_PARTITION_COLUMN=created_at
EVENTS_GRANULARITY=month
EVENTS_CREATE_AHEAD_COUNT=3
EVENTS_RETENTION_COUNT=12
```

Every variable is listed in [Environment settings](../reference/settings.md).

## From JSON

Configs are frozen Pydantic models. `model_dump(mode="json")` produces a document —
timezones as IANA names, built-in codecs by name, policies discriminated on `kind` — and
`model_validate` reads it back, nested schemes, numeric boundaries and combinator
policies included:

```python
payload = config.model_dump(mode="json", by_alias=True)
same = TablePartitionConfig.model_validate(payload)
```

Custom calculators, custom codecs and `Callback` predicates are Python objects and are
left out of the dump; a config using them is built in code.

## Several tables

One config per table; the service is stateless across them:

```python
CONFIGS = [events_config, audit_config, outbox_config]

for table_config in CONFIGS:
    result = await maintainer.run_maintenance_safe(table_config)
```

Tables are locked and maintained independently — a problem with one never blocks
another.

## What is checked, and when

**At construction** — no database involved: identifiers are lowercased and checked, a
LIST key is one column, every level partitions on a fresh column, a sliding list has no
`CreateAhead`, every generated name fits 63 bytes at every level:

```text
table_name 'a_very_long_table_name_for_an_events_store_that_goes_on_and_on' is too long for
this scheme: table_name (58) + the longest generated suffix (15) = 73 > 63 bytes. PostgreSQL
truncates identifiers silently, which would collapse two partitions onto one name.
```

**At plan time** — against the catalog: the table exists and is partitioned by the root's
method on the root's key (composite keys compared in key order; mixed-case and expression
keys refused); the calendar's timezone and the repository's `ddl_timezone` agree; every
nested level's key columns appear in every `UNIQUE` / `PRIMARY KEY` constraint; foreign
leaves are not asked of a parent with a unique index. Each failure is an
`InvalidPartitionConfigError` naming the fix, raised before any DDL.
