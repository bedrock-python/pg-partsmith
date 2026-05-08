# Configuration

`TablePartitionConfig` controls how pg-partsmith manages a single table's partitions.

## Full example

```python
from pg_partsmith import (
    MonthPeriodCalculator,
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    TablePartitionConfig,
)

config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_type=PartitionType.RANGE,
    partition_strategy=PartitionStrategy.TIME_BASED,
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,
    retention_count=12,
)
```

## Fields

### `table_name`

The name of the parent (partitioned) table. Must match the PostgreSQL table name exactly.

### `schema`

The PostgreSQL schema that contains the table. Optional, but **strongly recommended** in
multi-schema databases. When set, all catalog queries and DDL are schema-qualified and
independent of `search_path`.

```python
schema="public"      # default schema
schema="analytics"   # custom schema
schema=None          # relies on search_path (not recommended)
```

### `partition_type`

```python
class PartitionType(str, Enum):
    RANGE = "RANGE"
    LIST  = "LIST"
    HASH  = "HASH"
```

Use `RANGE` for time-based partitioning.

### `partition_strategy`

```python
class PartitionStrategy(str, Enum):
    TIME_BASED  = "TIME_BASED"
    VALUE_BASED = "VALUE_BASED"
    HASH_BASED  = "HASH_BASED"
```

Use `TIME_BASED` for automatic period calculation via a `PeriodCalculator`.

### `partition_column`

Column used as the partition key. For `TIME_BASED` this is typically a `TIMESTAMP` or
`TIMESTAMPTZ` column.

```python
partition_column="created_at"
partition_column="event_date"
```

### `granularity`

Controls which built-in period calculator to use when one is not passed explicitly.

```python
class PartitionGranularity(str, Enum):
    DAY   = "DAY"
    WEEK  = "WEEK"
    MONTH = "MONTH"
    YEAR  = "YEAR"
```

See [Period strategies](strategies.md) for details.

### `create_ahead_count`

Number of partitions to ensure exist, counting from the current period inclusive.

| Value | Effect (MONTH granularity, current = 2024-06) |
|-------|-----------------------------------------------|
| `1`   | Only `events__2024_06` |
| `3`   | `events__2024_06`, `events__2024_07`, `events__2024_08` |

### `retention_count`

Number of **most recent** periods to keep attached. Partitions older than this are detached.

```python
retention_count=12   # keep 12 months; detach everything older
retention_count=90   # keep 90 days (with DAY granularity)
```

Detached partitions are tagged with a marker comment and become orphans eligible for dropping.

## Enums reference

```python
from pg_partsmith import PartitionType, PartitionGranularity, PartitionStrategy
```

All three enums are `str` subclasses and serialise cleanly to/from JSON.
