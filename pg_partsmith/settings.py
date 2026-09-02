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

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError as _err:
    _msg = (
        "pydantic-settings is required for PartitionTableSettings. "
        "Install it with: pip install pg-partsmith[pydantic-settings]"
    )
    raise ImportError(_msg) from _err

from .document import PartitionTableSpec


class PartitionTableSettings(PartitionTableSpec, BaseSettings):
    """One table's fields, read from the environment.

    The same fields as :class:`~pg_partsmith.document.PartitionTableSpec` — the
    entry a configuration file gives per table — with an environment as their
    source instead of a document. Subclass it, set ``model_config`` with your
    env prefix, then call :meth:`~pg_partsmith.document.PartitionTableSpec.to_config`
    to get a ready-to-use ``TablePartitionConfig``.

    The flat fields describe the ordinary time-partitioned table; any other
    topology is given as ``scheme`` (JSON), which takes precedence over them;
    ``lifecycle`` and ``leaves`` take the same JSON their models dump.
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

    # Restated so the two bases agree on one config type; the values are the
    # spec's, and a subclass adding ``env_prefix`` still merges into them.
    model_config = SettingsConfigDict(extra="forbid", populate_by_name=True)


__all__ = ["PartitionTableSettings"]
