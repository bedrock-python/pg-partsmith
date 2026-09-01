"""What a lifecycle hook is handed: one event per phase, per partition.

Every hook takes the same object, so a hook written for one phase reads like a
hook written for another, and a hook that wants to watch all of them can be one
method. The event carries what is *known* at that moment and nothing invented:
the configuration, the partition, the window it covers when its level has one,
and the planned operation itself — with the reason it was planned, its OID, and
whatever the policy had measured.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterable
from enum import StrEnum
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, SkipValidation

from .boundaries import Window
from .entities import PartitionInfo, TablePartitionConfig
from .plan import Operation

if TYPE_CHECKING:
    from .topology import PartitionBounds


class HookPhase(StrEnum):
    """The moment a hook fires. Values are the method names they arrive at.

    Attributes:
        BEFORE_CREATE: Before the partition exists.
        AFTER_CREATE: After it is created, its subtree built, and attached.
        BEFORE_ATTACH: Before a detached partition goes back into the tree —
            its subtree complete, not yet receiving rows.
        AFTER_ATTACH: After it is attached and taking rows again.
        BEFORE_DETACH: Before it leaves its parent — the rows are still
            reachable through the root.
        AFTER_DETACH: After the detach — the table stands alone.
        BEFORE_DROP: The last moment its rows exist.
        AFTER_DROP: After the table is gone.
    """

    BEFORE_CREATE = "before_create"
    AFTER_CREATE = "after_create"
    BEFORE_ATTACH = "before_attach"
    AFTER_ATTACH = "after_attach"
    BEFORE_DETACH = "before_detach"
    AFTER_DETACH = "after_detach"
    BEFORE_DROP = "before_drop"
    AFTER_DROP = "after_drop"


class PartitionEvent(BaseModel):
    """One thing happening to one partition, as a hook sees it.

    Hooks fire once per **lifecycle unit** — the partition directly under the
    root — never once per leaf of its subtree, and once per member of a root
    ``HASH`` or ``LIST``.

    Attributes:
        phase: Which moment this is.
        config: The table's configuration, so a hook needs nothing injected to
            know the calendar, the codec or the policy it is running under.
        partition: The partition itself: name, bounds, OID when known, and how
            it partitions its own children if it does.
        window: The period the partition covers, when its level has windows at
            all. ``None`` for a member of a root ``HASH`` or ``LIST``, and for a
            partition whose bounds no longer say (a detached orphan whose name
            does not decode).
        operation: The planned operation being carried out — its ``reason``,
            ``detail``, ``oid``, and the ``size_bytes`` / ``row_estimate`` the
            policy measured, when it asked for them.
    """

    model_config = ConfigDict(frozen=True)

    phase: HookPhase
    config: TablePartitionConfig
    # Built by the executor from the plan, not taken from a caller: a plan may
    # know a partition only by name and OID, which the listing invariant that
    # every attached RANGE partition carries bounds would refuse. A hook must
    # not be denied its event over a bound the operation never needed.
    partition: SkipValidation[PartitionInfo]
    window: Window | None = None
    operation: Operation

    @property
    def table_name(self) -> str:
        """Schema-qualified root table. Derived, so it cannot drift from the config."""
        return self.config.qualified_name

    @classmethod
    def build(
        cls,
        phase: HookPhase,
        config: TablePartitionConfig,
        partition: PartitionInfo,
        operation: Operation,
    ) -> PartitionEvent:
        """Assemble the event, deriving the window from the partition's bounds."""
        return cls(
            phase=phase,
            config=config,
            partition=partition,
            window=_window_of(config, partition.bounds),
            operation=operation,
        )


def _window_of(config: TablePartitionConfig, bounds: PartitionBounds | None) -> Window | None:
    """The period bounds stand for at the root level, when that level has periods.

    A hash or list set divides its keyspace, not an axis, so its members cover
    no window; asking is meaningless rather than an error.
    """
    if bounds is None:
        return None
    resolve = getattr(config.scheme, "window_of", None)
    return None if resolve is None else resolve(bounds)


HOOK_METHODS: tuple[str, ...] = (*(phase.value for phase in HookPhase), "on_event")
"""Every method a hook may implement: one per phase, plus the catch-all."""


def validate_hook_signatures(hooks: Iterable[object]) -> None:
    """Refuse a hook whose methods still take the arguments they took before 1.1.

    Every hook method now takes one :class:`PartitionEvent`. A hook written
    against the older ``(config, partition)`` / ``(table_name, partition_name)``
    shapes is accepted by the runtime-checkable protocol -- it only asks whether
    the attribute exists -- and would fail at the first call instead, in the
    middle of a maintenance run and after some of its DDL had committed.
    Reading the signature at wiring time turns that into a refusal to start.

    Methods whose signature cannot be read (built-ins, mocks, anything taking
    ``*args``) are left alone rather than guessed at.

    Raises:
        ValueError: If a hook method requires more than the event.
    """
    for hook in hooks:
        for name in HOOK_METHODS:
            method = getattr(hook, name, None)
            if method is None or not callable(method):
                continue
            try:
                parameters = inspect.signature(method).parameters.values()
            except (TypeError, ValueError):
                continue
            required = [
                parameter
                for parameter in parameters
                if parameter.kind in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
                and parameter.default is parameter.empty
            ]
            if len(required) > 1:
                taken = ", ".join(parameter.name for parameter in required)
                msg = (
                    f"{type(hook).__name__}.{name} takes ({taken}); since 1.1 every hook method takes one "
                    f"PartitionEvent. The event carries the config, the partition, the window it covers and "
                    f"the operation: {name}(self, event) -> None, then event.partition.name, event.table_name, "
                    f"event.window, event.operation.reason."
                )
                raise ValueError(msg)
