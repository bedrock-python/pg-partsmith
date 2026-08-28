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
    calculator = cfg.get_period_calculator()  # → MonthPeriodCalculator() etc.
"""

from __future__ import annotations

from datetime import UTC, tzinfo

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
from .entities import (
    PartitionGranularity,
    PartitionStrategy,
    PartitionType,
    SubpartitionSpec,
    TablePartitionConfig,
)
from .strategies import BasePeriodCalculator, get_period_calculator


class PartitionTableSettings(BaseSettings):
    """Env-loadable base class that maps 1-to-1 with :class:`~pg_partsmith.TablePartitionConfig`.

    Subclass it, set ``model_config`` with your env prefix, then call
    :meth:`to_config` to get a ready-to-use ``TablePartitionConfig``.

    All fields correspond directly to ``TablePartitionConfig`` arguments.
    ``PartitionType``, ``PartitionStrategy``, and ``PartitionGranularity``
    are ``StrEnum`` values — env vars accept their lowercase string forms
    (e.g. ``GRANULARITY=month``).

    Example::

        class OutboxSettings(PartitionTableSettings):
            model_config = SettingsConfigDict(env_prefix="OUTBOX_")

        # Reads: OUTBOX_TABLE_NAME, OUTBOX_PARTITION_TYPE, OUTBOX_GRANULARITY, …
        settings = OutboxSettings()
        config = settings.to_config()
        calculator = settings.get_period_calculator()
    """

    schema_name: str | None = Field(default=None, description="PostgreSQL schema (omit for public)")
    table_name: str = Field(..., description="Partitioned table name")
    partition_type: PartitionType = Field(..., description="Partition type: range, list, hash")
    partition_strategy: PartitionStrategy = Field(
        ...,
        description="Strategy: time_based, value_based, hash_based",
    )
    partition_column: str | None = Field(
        default=None,
        description="Column used for partitioning; use PARTITION_COLUMNS for a composite key",
    )
    partition_columns: tuple[str, ...] | None = Field(
        default=None,
        description='Composite partition key in key order, as JSON: ["created_at", "tenant_id"]',
    )
    granularity: PartitionGranularity | None = Field(
        default=None,
        description="Time granularity: hour, day, week, month, quarter, year",
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
    auto_attach_after_create: bool = Field(
        default=True,
        description="Attach new partitions immediately after creation",
    )
    root_layout: SubpartitionSpec | None = Field(
        default=None,
        description=(
            "For a HASH_BASED / VALUE_BASED table, the partitions it is divided into, as JSON: "
            '{"strategy": "hash", "column": "tenant_id", "modulus": 16}'
        ),
    )
    subpartition: SubpartitionSpec | None = Field(
        default=None,
        description=(
            "Subpartitioning inside each time partition, as JSON: "
            '{"strategy": "hash", "column": "tenant_id", "modulus": 4}'
        ),
    )

    @property
    def resolved_partition_columns(self) -> tuple[str, ...]:
        """The partition key, however it was spelled in the environment."""
        if self.partition_columns:
            return self.partition_columns
        if self.partition_column:
            return (self.partition_column,)
        msg = "Set either partition_column or partition_columns"
        raise ValueError(msg)

    def to_config(self) -> TablePartitionConfig:
        """Build a :class:`~pg_partsmith.TablePartitionConfig` from these settings."""
        return TablePartitionConfig(
            schema=self.schema_name,
            table_name=self.table_name,
            partition_type=self.partition_type,
            partition_strategy=self.partition_strategy,
            partition_columns=self.resolved_partition_columns,
            granularity=self.granularity,
            create_ahead_count=self.create_ahead_count,
            retention_count=self.retention_count,
            auto_attach_after_create=self.auto_attach_after_create,
            root_layout=self.root_layout,
            subpartition=self.subpartition,
        )

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
