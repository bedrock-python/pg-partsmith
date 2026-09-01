"""One document describing every partitioned table of a deployment.

A real deployment maintains several tables in one run, and describes them in a
file -- a ConfigMap, a mounted YAML, a JSON blob -- rather than in Python. This
module is that document, as models: :class:`PartitionTableSpec` is one table's
entry, :class:`ToolkitOptions` the wiring every table is maintained through,
and :class:`PartitionsDocument` the whole file.

Nothing here reads a file or opens a connection. The document is parsed by
whoever owns the format (``json.loads``, ``tomllib.load``, a YAML loader) and
validated here, so one field list serves a file, an environment and a Python
caller alike -- :class:`~pg_partsmith.settings.PartitionTableSettings` is the
same fields read from the environment.

Usage::

    document = PartitionsDocument.model_validate(yaml.safe_load(text))
    kit = PartitionToolkit.from_options(engine, document.runtime)
    for config in document.configs():
        await kit.service.maintain(config)
"""

from __future__ import annotations

from datetime import UTC, tzinfo
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .boundaries import resolve_codec
from .constants import (
    DEFAULT_CREATE_AHEAD_COUNT,
    DEFAULT_DDL_TIMEOUT_SECONDS,
    DEFAULT_DDL_TIMEZONE,
    DEFAULT_DROP_LOCK_TIMEOUT_MS,
    DEFAULT_DROP_MAX_BACKOFF,
    DEFAULT_DROP_MAX_RETRIES,
    DEFAULT_DROP_RETRY_DELAY,
    DEFAULT_HOOK_TIMEOUT_SECONDS,
    DEFAULT_LOCK_PREFIX,
    DEFAULT_RETENTION_COUNT,
)
from .entities import PartitionGranularity, PartitionStrategy, PartitionType, TablePartitionConfig
from .events import HookPhase
from .strategies import BasePeriodCalculator, get_period_calculator

__all__ = ["HookOptions", "PartitionTableSpec", "PartitionsDocument", "ToolkitOptions"]


class PartitionTableSpec(BaseModel):
    """One table's entry: a configuration written down rather than constructed.

    The flat fields describe the ordinary time-partitioned table; any other
    topology is given as ``scheme``, which takes precedence over them;
    ``lifecycle`` and ``leaves`` take the same JSON their models dump.
    ``PartitionType``, ``PartitionStrategy`` and ``PartitionGranularity`` are
    ``StrEnum`` values, so their lowercase spellings are what a file writes.

    ``schema`` is accepted as a spelling of ``schema_name``, because that is
    what :class:`~pg_partsmith.TablePartitionConfig` dumps and what an operator
    writes.

    Unknown keys are refused rather than ignored: a misspelled field in a file
    nobody reads until 03:00 is a silent policy, not a typo.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_name: str | None = Field(default=None, description="PostgreSQL schema (omit for public)")
    table_name: str = Field(..., description="Partitioned table name")
    partition_type: PartitionType | None = Field(
        default=None, description="Checked against the scheme: range, list, hash"
    )
    partition_strategy: PartitionStrategy | None = Field(
        default=None,
        description="Checked against the scheme: time_based, numeric_based, value_based, hash_based",
    )
    partition_column: str | None = Field(default=None, description="Leading column of a time-partitioned root")
    trailing_partition_columns: tuple[str, ...] = Field(
        default=(),
        description='Rest of a composite partition key, as JSON: ["tenant_id"]',
    )
    granularity: PartitionGranularity | None = Field(
        default=None,
        description="Time granularity: hour, day, week, month, quarter, year",
    )
    tz: str = Field(default="UTC", description="IANA timezone the calendar is computed in")
    boundary_codec: str | None = Field(
        default=None,
        description="Physical key encoding by name: uuidv7, epoch_seconds, epoch_milliseconds",
    )
    create_ahead_count: int = Field(
        default=DEFAULT_CREATE_AHEAD_COUNT,
        ge=1,
        description="Number of periods to ensure exist, including the current period",
    )
    retention_count: int = Field(
        default=DEFAULT_RETENTION_COUNT,
        ge=1,
        description="Number of newest periods to keep, current one included",
    )
    scheme: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Any topology, as JSON: "
            '{"method": "range", "key": "created_at", "boundaries": {"kind": "time", "granularity": "week"}, '
            '"child": {"method": "hash", "key": "tenant_id", "modulus": 4}}'
        ),
    )
    lifecycle: dict[str, Any] | None = Field(
        default=None,
        description='Lifecycle policy, as JSON: {"creation": {"kind": "create_ahead", "count": 3}, ...}',
    )
    leaves: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Leaf backend, as JSON: "
            '{"kind": "local", "tablespace": "fast", "storage_parameters": {"fillfactor": 70}} or '
            '{"kind": "foreign", "server": "archive", "options": {"table_name": "{relname}"}}'
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _accept_schema_as_schema_name(cls, data: Any) -> Any:
        """Take ``schema``, the spelling a configuration dumps, as ``schema_name``."""
        return _with_schema_name(data) if isinstance(data, dict) else data

    def to_config(self) -> TablePartitionConfig:
        """Build a :class:`~pg_partsmith.TablePartitionConfig` from these fields."""
        fields: dict[str, Any] = {"schema": self.schema_name, "table_name": self.table_name}
        if self.partition_type is not None:
            fields["partition_type"] = self.partition_type
        if self.partition_strategy is not None:
            fields["partition_strategy"] = self.partition_strategy

        if self.scheme is not None:
            fields["scheme"] = self.scheme
        else:
            fields.update(
                partition_column=self.partition_column,
                trailing_partition_columns=self.trailing_partition_columns,
                granularity=self.granularity,
                tz=self.tz,
                boundary_codec=self.boundary_codec,
            )

        if self.lifecycle is not None:
            fields["lifecycle"] = self.lifecycle
        else:
            fields.update(create_ahead_count=self.create_ahead_count, retention_count=self.retention_count)
        if self.leaves is not None:
            fields["leaves"] = self.leaves

        return TablePartitionConfig(**fields)

    def get_period_calculator(self, tz: tzinfo = UTC) -> BasePeriodCalculator:
        """Return the period calculator matching :attr:`granularity`.

        Args:
            tz: Timezone the calculator works in (``datetime.UTC`` or a keyed
                :class:`zoneinfo.ZoneInfo`). HOUR accepts only UTC.

        Raises:
            ValueError: If ``granularity`` is ``None`` or has no registered
                calculator, or ``tz`` is unsupported for it.
        """
        if self.granularity is None:
            msg = "Cannot resolve a period calculator: granularity is not set"
            raise ValueError(msg)
        return get_period_calculator(self.granularity, tz=tz)


class ToolkitOptions(BaseModel):
    """The wiring every table in the document is maintained through.

    One field per keyword of ``PartitionToolkit.from_engine``, so a document
    configures the wiring the way code does. ``boundary_codec`` is a name here
    (``uuidv7``, ``epoch_seconds``, ``epoch_milliseconds``); :meth:`to_kwargs`
    resolves it to the codec itself.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    marker_prefix: str | None = Field(default=None, description="COMMENT prefix marking detached partitions as ours")
    ddl_timezone: str | None = Field(
        default=DEFAULT_DDL_TIMEZONE,
        description="Session timezone naive boundary literals are written and read in",
    )
    ddl_timeout_seconds: float = Field(default=DEFAULT_DDL_TIMEOUT_SECONDS, gt=0, description="Statement timeout")
    boundary_codec: str | None = Field(default=None, description="Codec name encoded bounds are read with")
    lock_prefix: str = Field(default=DEFAULT_LOCK_PREFIX, description="Prefix of the advisory lock keys")
    lock_min_interval_seconds: float = Field(
        default=0.0, ge=0, description="Minimum seconds between acquire attempts per table; 0 disables"
    )
    drop_allow_unmanaged: bool = Field(default=False, description="Allow dropping a relation with no ownership marker")
    drop_lock_timeout_ms: int = Field(default=DEFAULT_DROP_LOCK_TIMEOUT_MS, ge=0, description="lock_timeout for a drop")
    drop_max_retries: int = Field(default=DEFAULT_DROP_MAX_RETRIES, ge=1, description="Attempts a drop makes")
    drop_retry_delay: float = Field(default=DEFAULT_DROP_RETRY_DELAY, ge=0, description="Delay before the retry")
    drop_max_backoff: float = Field(default=DEFAULT_DROP_MAX_BACKOFF, ge=0, description="Ceiling on that backoff")

    def to_kwargs(self) -> dict[str, Any]:
        """These options as the keywords ``PartitionToolkit.from_engine`` takes.

        Raises:
            ValueError: If ``boundary_codec`` names no known codec.
        """
        return {**self.model_dump(), "boundary_codec": resolve_codec(self.boundary_codec)}


class HookOptions(BaseModel):
    """A command to run at each lifecycle moment, as a file writes them.

    One field per phase, spelled out rather than a free mapping, so a
    misspelled ``befor_drop`` is refused where it is written instead of
    silently never running -- which, for the phase that exports data before a
    ``DROP``, is the difference between an archive and no archive.

    Each command is an argument vector. Nothing is passed through a shell, so a
    partition name can never be read as syntax.

    Hooks fire during ``apply`` only. ``plan`` issues no DDL and runs no hook,
    which is what makes a plan safe to compute anywhere.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    timeout_seconds: float = Field(
        default=DEFAULT_HOOK_TIMEOUT_SECONDS,
        gt=0,
        description="How long a command may run before it is killed; it holds the table's lock",
    )
    before_create: tuple[str, ...] | None = Field(default=None, description="Before a partition exists")
    after_create: tuple[str, ...] | None = Field(default=None, description="After it is created and attached")
    before_attach: tuple[str, ...] | None = Field(default=None, description="Before a detached partition returns")
    after_attach: tuple[str, ...] | None = Field(default=None, description="After it is taking rows again")
    before_detach: tuple[str, ...] | None = Field(default=None, description="While its rows are still reachable")
    after_detach: tuple[str, ...] | None = Field(default=None, description="After it stands alone")
    before_drop: tuple[str, ...] | None = Field(default=None, description="The last moment its rows exist")
    after_drop: tuple[str, ...] | None = Field(default=None, description="After the table is gone")

    def commands(self) -> dict[HookPhase, tuple[str, ...]]:
        """The phases a command was given for, in lifecycle order."""
        return {phase: command for phase in HookPhase if (command := getattr(self, phase.value)) is not None}

    @property
    def is_empty(self) -> bool:
        """True when the section names no command at all."""
        return not self.commands()


class PartitionsDocument(BaseModel):
    """Every table a deployment maintains, and the wiring it maintains them through.

    ``defaults`` is merged into each entry of ``tables`` before validation, key
    by key: a table naming a key owns it entirely. The merge is deliberately
    shallow -- a table with its own ``lifecycle`` replaces the default one
    rather than being merged into it, because a half-inherited policy is one
    nobody can read off the file.

    ``dsn`` is for whoever connects; the library opens no connection of its own,
    and a deployment is free to keep the DSN out of the file entirely.

    ``hooks`` names commands to run around the lifecycle. They are code this
    file causes to run in a process holding DDL credentials -- which is not a
    new privilege boundary, since the same file already authorises dropping
    tables, but it stops being true the moment the document is assembled from
    somewhere less trusted than the DSN. Whoever runs the document decides
    whether they are honoured; the CLI refuses to apply a document declaring
    hooks unless it is told to.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal[1] = Field(default=1, description="Document format version")
    dsn: str | None = Field(default=None, description="Connection string, when the file carries one")
    defaults: dict[str, Any] = Field(default_factory=dict, description="Fields every table starts from")
    tables: tuple[PartitionTableSpec, ...] = Field(..., min_length=1, description="The tables to maintain")
    runtime: ToolkitOptions = Field(default_factory=ToolkitOptions, description="How the collaborators are wired")
    hooks: HookOptions | None = Field(
        default=None, description="Commands to run around the lifecycle; they fire during apply only"
    )

    @model_validator(mode="before")
    @classmethod
    def _apply_defaults(cls, data: Any) -> Any:
        """Start every table from ``defaults``, and refuse a default no table field takes."""
        if not isinstance(data, dict):
            return data
        defaults = data.get("defaults")
        if not isinstance(defaults, dict) or not defaults:
            # Nothing to merge, or a ``defaults`` the field's own validation refuses.
            return data
        unknown = sorted(set(defaults) - set(PartitionTableSpec.model_fields) - {"schema"})
        if unknown:
            msg = f"defaults names fields no table has: {', '.join(unknown)}"
            raise ValueError(msg)
        tables = data.get("tables")
        if not isinstance(tables, (list, tuple)):
            return data
        # Both sides are normalised before they meet: a default written as
        # ``schema`` and a table written as ``schema_name`` are one key, not two
        # that survive the merge and read as an unknown field.
        shared = _with_schema_name(defaults)
        merged = [{**shared, **_with_schema_name(table)} if isinstance(table, dict) else table for table in tables]
        return {**data, "tables": merged}

    @model_validator(mode="after")
    def _refuse_a_table_described_twice(self) -> PartitionsDocument:
        """Two entries for one relation would maintain it under two policies."""
        seen: set[str] = set()
        for spec in self.tables:
            name = spec.to_config().qualified_name
            if name in seen:
                msg = f"Table {name!r} is described twice; one relation is maintained under one policy"
                raise ValueError(msg)
            seen.add(name)
        return self

    def configs(self) -> tuple[TablePartitionConfig, ...]:
        """Every table as a :class:`~pg_partsmith.TablePartitionConfig`, in document order."""
        return tuple(spec.to_config() for spec in self.tables)

    def config_for(self, qualified_name: str) -> TablePartitionConfig:
        """The configuration of one table, by the name PostgreSQL knows it as.

        Raises:
            KeyError: If the document describes no such table.
        """
        configs = self.configs()
        for config in configs:
            if config.qualified_name == qualified_name:
                return config
        known = ", ".join(config.qualified_name for config in configs)
        msg = f"{qualified_name!r} is not in this document; it describes {known}"
        raise KeyError(msg)


def _with_schema_name(mapping: dict[str, Any]) -> dict[str, Any]:
    """``schema`` renamed to the field it fills, leaving everything else alone."""
    if "schema" not in mapping:
        return mapping
    renamed = {key: value for key, value in mapping.items() if key != "schema"}
    renamed.setdefault("schema_name", mapping["schema"])
    return renamed
