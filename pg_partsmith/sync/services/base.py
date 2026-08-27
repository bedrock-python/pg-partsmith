"""Base class for partition services."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pg_partsmith.sync.hooks import PartitionLifecycleHooks

logger = logging.getLogger(__name__)


class BasePartitionService:
    """Base class for partition maintenance services."""

    def __init__(
        self,
        hooks: list[PartitionLifecycleHooks] | None = None,
    ) -> None:
        self._hooks = hooks or []

    def _run_hooks(
        self,
        hook_caller: Callable[[PartitionLifecycleHooks], None],
        hook_name: str,
        *,
        partition_name: str,
    ) -> None:
        """Execute a specific hook across all registered lifecycle hooks.

        Args:
            hook_caller: A callable that takes a hook instance and invokes one hook method.
            hook_name: Name of the hook for logging.
            partition_name: Name of the partition for logging context.

        Raises:
            Exception: Any exception raised by a hook.
        """
        for hook in self._hooks:
            try:
                hook_caller(hook)
            except (KeyboardInterrupt, SystemExit):
                raise
            except (ValueError, TypeError, RuntimeError) as e:
                logger.warning(
                    f"{hook_name} hook failed",
                    extra={
                        "partition_name": partition_name,
                        "hook_type": type(hook).__name__,
                        "error": str(e),
                    },
                )
                raise
            except Exception:
                logger.exception(
                    f"{hook_name} hook failed with unexpected error",
                    extra={
                        "partition_name": partition_name,
                        "hook_type": type(hook).__name__,
                    },
                )
                raise
