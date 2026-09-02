# Environment settings

`pg_partsmith.settings.PartitionTableSettings` (requires
`pip install "pg-partsmith[pydantic-settings]"`) loads one table's configuration from
environment variables. Subclass it with an `env_prefix`, then call `to_config()`.

```python
from pydantic_settings import SettingsConfigDict

from pg_partsmith.settings import PartitionTableSettings


class EventsSettings(PartitionTableSettings):
    model_config = SettingsConfigDict(env_prefix="EVENTS_")


config = EventsSettings().to_config()
```

## Variables

With prefix `EVENTS_`:

| Variable | Type | Default | Meaning |
|---|---|---|---|
| `EVENTS_TABLE_NAME` | text | required | the parent table |
| `EVENTS_SCHEMA_NAME` | text | — | its schema |
| `EVENTS_PARTITION_COLUMN` | text | — | leading key column (flat form) |
| `EVENTS_TRAILING_PARTITION_COLUMNS` | JSON list | `[]` | the rest of a composite key: `["tenant_id"]` |
| `EVENTS_GRANULARITY` | `hour` / `day` / `week` / `month` / `quarter` / `year` | — | period size (flat form) |
| `EVENTS_TZ` | IANA name | `UTC` | the calendar's timezone |
| `EVENTS_BOUNDARY_CODEC` | `uuidv7` / `epoch_seconds` / `epoch_milliseconds` | — | key encoding |
| `EVENTS_CREATE_AHEAD_COUNT` | integer ≥ 1 | `6` | periods that must exist, current one included |
| `EVENTS_RETENTION_COUNT` | integer ≥ 1 | `12` | newest periods kept, current one included |
| `EVENTS_PARTITION_TYPE` | `range` / `list` / `hash` | — | checked against the scheme |
| `EVENTS_PARTITION_STRATEGY` | `time_based` / `numeric_based` / `value_based` / `hash_based` | — | checked against the scheme |
| `EVENTS_SCHEME` | JSON | — | any topology; takes precedence over the flat fields |
| `EVENTS_LIFECYCLE` | JSON | — | a lifecycle policy; takes precedence over the counts |
| `EVENTS_LEAVES` | JSON | — | a leaf backend |

The JSON forms are exactly what the models dump: `config.scheme.model_dump(mode="json",
by_alias=True)` is a valid `SCHEME`.

```bash
EVENTS_TABLE_NAME=issue_events
EVENTS_SCHEMA_NAME=public
EVENTS_SCHEME='{"method": "range", "key": "id",
                "boundaries": {"kind": "time", "granularity": "week", "codec": "uuidv7"},
                "child": {"method": "hash", "key": "organization_id", "modulus": 2}}'
EVENTS_LIFECYCLE='{"creation": {"kind": "create_ahead", "count": 3},
                   "retention": {"kind": "keep_newest", "count": 12},
                   "drop": {"kind": "drop_after", "grace": "P7D"}}'
EVENTS_LEAVES='{"kind": "local", "storage_parameters": {"fillfactor": 90}}'
```

Durations are ISO 8601 (`P7D`, `PT12H`) or seconds. `Callback` predicates, custom
calculators and custom codecs cannot be expressed in JSON; build those configs in code.

## Several tables

A deployment maintaining several tables from a file wants
[the configuration document](document.md): one list of tables, shared `defaults`, and the
same field list as below. From the environment, it is one subclass per table, each with its
own prefix:

```python
class EventsSettings(PartitionTableSettings):
    model_config = SettingsConfigDict(env_prefix="EVENTS_")


class AuditSettings(PartitionTableSettings):
    model_config = SettingsConfigDict(env_prefix="AUDIT_")


CONFIGS = [EventsSettings().to_config(), AuditSettings().to_config()]
```

`settings.get_period_calculator(tz=…)` returns the calculator for the configured
granularity, for code that needs the calendar outside the library.
