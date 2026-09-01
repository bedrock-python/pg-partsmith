"""One set of collaborators for a table's maintenance, wired so they agree.

Three settings live on two objects and have to match. ``marker_prefix``
stamps a detached partition and decides what counts as ours when dropping,
so a prefix given to the repository alone leaves the metadata provider
unable to see the orphans it made -- they are never dropped.
``ddl_timezone`` writes naive boundary literals and reads them back in
:meth:`is_partition_closed`, and the two objects do not even default to the
same value. ``boundary_codec`` must be the one the bounds were encoded with.

:meth:`PartitionToolkit.from_engine` takes each of those once and hands them
to everything that needs them, and returns the parts by name rather than
just a maintainer -- code that calls ``metadata.is_partition_closed`` or
``locks.acquire_lock`` directly should not have to build a second set and
keep it consistent by hand.

Usage::

    kit = PartitionToolkit.from_engine(engine, hooks=[ExportHooks()])
    plan = kit.service.plan(config)
    result = kit.maintainer.run_maintenance_safe(config)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pg_partsmith.constants import (
    DEFAULT_DDL_TIMEOUT_SECONDS,
    DEFAULT_DDL_TIMEZONE,
    DEFAULT_DROP_LOCK_TIMEOUT_MS,
    DEFAULT_DROP_MAX_BACKOFF,
    DEFAULT_DROP_MAX_RETRIES,
    DEFAULT_DROP_RETRY_DELAY,
    DEFAULT_LOCK_PREFIX,
)

from .lock import PostgresAdvisoryLockManager
from .maintainer import PartitionMaintainer
from .metadata import PostgresMetadataProvider
from .repositories import PostgresPartitionRepository
from .service import PartitionLifecycleService

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from pg_partsmith.boundaries import RangeBoundaryCodec

    from .hooks import PartitionLifecycleHooks
    from .protocols import LockManager, PartitionMetadataProvider, PartitionRepository


@dataclass(frozen=True, slots=True)
class PartitionToolkit:
    """The repository, provider, locks, service and maintainer of one wiring.

    Build it with :meth:`from_engine`, or construct it directly around parts
    of your own -- a repository against another database, a Redis lock
    manager -- when the defaults do not fit.

    Attributes:
        repo: DDL operations on partitions.
        metadata: Read-only catalog access.
        locks: The lock one maintenance run at a time is taken under.
        service: The three verbs: plan, apply, maintain.
        maintainer: The service with timing, logging and error handling, which
            is what a scheduled tick calls.
    """

    repo: PartitionRepository
    metadata: PartitionMetadataProvider
    locks: LockManager
    service: PartitionLifecycleService
    maintainer: PartitionMaintainer

    @classmethod
    def from_engine(
        cls,
        engine: Engine,
        *,
        hooks: list[PartitionLifecycleHooks] | None = None,
        locks: LockManager | None = None,
        marker_prefix: str | None = None,
        ddl_timezone: str | None = DEFAULT_DDL_TIMEZONE,
        ddl_timeout_seconds: float = DEFAULT_DDL_TIMEOUT_SECONDS,
        boundary_codec: RangeBoundaryCodec | None = None,
        lock_prefix: str = DEFAULT_LOCK_PREFIX,
        lock_min_interval_seconds: float = 0.0,
        drop_allow_unmanaged: bool = False,
        drop_lock_timeout_ms: int = DEFAULT_DROP_LOCK_TIMEOUT_MS,
        drop_max_retries: int = DEFAULT_DROP_MAX_RETRIES,
        drop_retry_delay: float = DEFAULT_DROP_RETRY_DELAY,
        drop_max_backoff: float = DEFAULT_DROP_MAX_BACKOFF,
    ) -> PartitionToolkit:
        """Wire every part around one engine, with the shared settings given once.

        Args:
            engine: The engine every part works through.
            hooks: Lifecycle hooks, handed to the service.
            locks: A lock manager of your own -- Redis, or an advisory manager
                with a different ID derivation. A PostgreSQL advisory manager
                on ``engine`` by default.
            marker_prefix: Orphan marker prefix, given to the repository that
                writes it and the provider that looks for it.
            ddl_timezone: Session timezone naive boundary literals are written
                and read in, given to both for the same reason.
            ddl_timeout_seconds: Statement timeout for DDL.
            boundary_codec: Codec ``is_partition_closed`` reads encoded bounds
                with; only needed when the key is an encoded identifier.
            lock_prefix: Prefix of the default advisory lock manager's keys.
            lock_min_interval_seconds: Minimum seconds between acquire attempts
                per table; 0 disables the rate limit.
            drop_allow_unmanaged: Let the repository drop a relation carrying no
                ownership marker.
            drop_lock_timeout_ms: ``lock_timeout`` a drop attempt runs under.
            drop_max_retries: Attempts a drop makes before giving up.
            drop_retry_delay: Delay before the second attempt; it backs off.
            drop_max_backoff: Ceiling on that backoff.

        Returns:
            The parts, wired consistently.
        """
        repo = PostgresPartitionRepository(
            engine,
            ddl_timezone=ddl_timezone,
            ddl_timeout_seconds=ddl_timeout_seconds,
            marker_prefix=marker_prefix,
            drop_allow_unmanaged=drop_allow_unmanaged,
            drop_lock_timeout_ms=drop_lock_timeout_ms,
            drop_max_retries=drop_max_retries,
            drop_retry_delay=drop_retry_delay,
            drop_max_backoff=drop_max_backoff,
        )
        metadata = PostgresMetadataProvider(
            engine,
            marker_prefix=marker_prefix,
            boundary_codec=boundary_codec,
            ddl_timezone=ddl_timezone,
        )
        lock_manager = (
            locks
            if locks is not None
            else PostgresAdvisoryLockManager(
                engine, prefix=lock_prefix, acquire_min_interval_seconds=lock_min_interval_seconds
            )
        )
        service = PartitionLifecycleService(repo=repo, metadata=metadata, locks=lock_manager, hooks=hooks)
        return cls(
            repo=repo,
            metadata=metadata,
            locks=lock_manager,
            service=service,
            maintainer=PartitionMaintainer(service),
        )
