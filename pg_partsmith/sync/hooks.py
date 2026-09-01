"""Lifecycle hooks (middleware) and protocol for partition operations.

Hooks let you inject custom logic at each step of the partition lifecycle -
for example, exporting data before a partition is dropped, publishing events
to Kafka after a partition is created, or archiving rows before detachment.

Every hook takes one :class:`~pg_partsmith.events.PartitionEvent`: the phase,
the configuration, the partition, the window it covers, and the planned
operation with the reason it was planned. A hook that wants every phase - an
audit trail, metrics - implements :meth:`on_event` once instead of one
delegating method per phase.

Hooks fire once per **lifecycle unit** -- the partition directly under the
root -- never per leaf of its subtree: a cold-storage export wants the whole
week, not one call per hash bucket.

Usage example::

    class KafkaExportHooks(BasePartitionLifecycleHooks):
        def __init__(self, producer: KafkaProducer) -> None:
            self._producer = producer

        def before_drop(self, event: PartitionEvent) -> None:
            self._producer.send("partition.before_drop", {
                "table": event.table_name,
                "partition": event.partition.name,
                "covering": None if event.window is None else event.window.start.isoformat(),
                "why": event.operation.reason,
            })

    service = PartitionLifecycleService(
        repo=repo,
        metadata=metadata,
        locks=locks,
        hooks=[KafkaExportHooks(producer)],
    )
"""

from typing import Protocol, runtime_checkable

from pg_partsmith.events import PartitionEvent


@runtime_checkable
class PartitionLifecycleHooks(Protocol):
    """Protocol for partition lifecycle hooks.

    Implement this protocol to inject custom logic at each step of the partition
    lifecycle without inheriting from a specific base class.
    """

    def before_create(self, event: PartitionEvent) -> None: ...

    def after_create(self, event: PartitionEvent) -> None: ...

    def before_attach(self, event: PartitionEvent) -> None: ...

    def after_attach(self, event: PartitionEvent) -> None: ...

    def before_detach(self, event: PartitionEvent) -> None: ...

    def after_detach(self, event: PartitionEvent) -> None: ...

    def before_drop(self, event: PartitionEvent) -> None: ...

    def after_drop(self, event: PartitionEvent) -> None: ...

    def on_event(self, event: PartitionEvent) -> None: ...


class BasePartitionLifecycleHooks:
    """No-op base implementation of partition lifecycle hooks.

    Subclass and override only the methods you need.
    All methods are no-ops by default so you can selectively add behaviour
    without implementing every step.
    """

    def before_create(self, event: PartitionEvent) -> None:
        """Called before a partition is created.

        Args:
            event: The partition about to be created -- its name, its bounds,
                the window it covers, and how it will partition its own
                children, if it does. Raising aborts the creation.
        """

    def after_create(self, event: PartitionEvent) -> None:
        """Called after a partition has been created, built and attached.

        Args:
            event: The partition, now attached.
        """

    def before_attach(self, event: PartitionEvent) -> None:
        """Called before a detached partition is brought back into the tree.

        A partition retention released is re-attached when its window is
        wanted again -- retention grew, or create-ahead reached back to it.
        Anything derived from it while it was out (an export, a cold copy)
        is about to go stale: this is where to say so. Raising aborts the
        attach; the relation stays detached and the next run plans it again.

        The guarantee is that *this run* has not attached it yet, not that
        nothing else has: a partition someone else attached under the planned
        name in the meantime is recognised as a benign race after this fires.

        Args:
            event: The partition about to go live, its subtree already
                complete.
        """

    def after_attach(self, event: PartitionEvent) -> None:
        """Called after a detached partition is attached and taking rows again.

        Args:
            event: The partition, back in the tree.
        """

    def before_detach(self, event: PartitionEvent) -> None:
        """Called before a partition is detached from its parent table.

        This is a good place to export or archive data while the partition
        is still accessible via the parent table's indexes and constraints.

        Args:
            event: The partition about to be detached, with the reason
                retention gave for it. Raising aborts the detach.
        """

    def after_detach(self, event: PartitionEvent) -> None:
        """Called after a partition has been detached.

        Args:
            event: The partition, now a standalone table.
        """

    def before_drop(self, event: PartitionEvent) -> None:
        """Called before a partition table is dropped.

        This is the last chance to read or export data from the partition
        before it is permanently destroyed. Raising aborts the drop; the
        orphan marker brings the partition back on the next run.

        Args:
            event: The partition about to be dropped. ``DETACH`` clears the
                catalog's record of its bounds, so what ``partition.bounds``
                and ``window`` report is the window the planner decided the
                drop on -- ``None`` when its name does not decode.
        """

    def after_drop(self, event: PartitionEvent) -> None:
        """Called after a partition table has been dropped.

        Args:
            event: The partition that is now gone.
        """

    def on_event(self, event: PartitionEvent) -> None:
        """Called for **every** phase, in addition to the method named above it.

        One place for the cross-cutting things -- an audit trail, metrics, a
        log line per operation -- that would otherwise be one identical method
        per phase, and another with every phase added later.

        Args:
            event: The event, whose ``phase`` says which moment this is.
        """
