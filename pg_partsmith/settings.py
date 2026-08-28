"""Pydantic-settings base class for env-driven partition configuration.

Requires the ``pydantic-settings`` extra::

    pip install pg-partsmith[pydantic-settings]

Usage::

    from pydantic_settings import SettingsConfigDict
    from pg_partsmith.settings import PartitionTableSettings

    class OutboxSettings(PartitionTableSettings):
        model_config = SettingsConfigDict(env_prefix="OUTBOX_")

    cfg = OutboxSettings()          # reads OUTBOX_TABLE_NAME, OUTBOX_GRANULARITY, …
    config = cfg.to_config()        # → TablePartitionConfig
"""

from __future__ import annotations

from datetime import UTC, tzinfo
from typing import Any

try:
    from pydantic_settings import BaseSettings
except ImportError as _err:
    _msg = (
        "pydantic-settings is required for PartitionTableSettings. "
        "Install it with: pip install pg-partsmith[pydantic-settings]"
    )
    raise ImportError(_msg) from _err

from pydantic import Field

from .constants import DEFAULT_CREATE_AHEAD_COUNT, DEFAULT_RETENTION_COUNT
from .entities import PartitionGranularity, PartitionStrategy, PartitionType, TablePartitionConfig
from .strategies import BasePeriodCalculator, get_period_calculator


class PartitionTableSettings(BaseSettings):
    """Env-loadable base class that maps onto :class:`~pg_partsmith.TablePartitionConfig`.

    Subclass it, set ``model_config`` with your env prefix, then call
    :meth:`to_config` to get a ready-to-use ``TablePartitionConfig``.

    The flat fields describe the ordinary time-partitioned table; any other
    topology is given as ``scheme`` (JSON), which takes precedence over them.
    ``PartitionType``, ``PartitionStrategy``, and ``PartitionGranularity`` are
    ``StrEnum`` values — env vars accept their lowercase string forms
    (e.g. ``GRANULARITY=month``).

    Example::

        class OutboxSettings(PartitionTableSettings):
            model_config = SettingsConfigDict(env_prefix="OUTBOX_")

        # Reads: OUTBOX_TABLE_NAME, OUTBOX_PARTITION_COLUMN, OUTBOX_GRANULARITY, …
        settings = OutboxSettings()
        config = settings.to_config()
    """

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

    def to_config(self) -> TablePartitionConfig:
        """Build a :class:`~pg_partsmith.TablePartitionConfig` from these settings."""
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


__all__ = ["PartitionTableSettings"]
