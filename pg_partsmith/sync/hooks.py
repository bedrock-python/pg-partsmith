"""Lifecycle hooks (middleware) and protocol for partition operations.

Hooks let you inject custom logic at each step of the partition lifecycle -
for example, exporting data before a partition is dropped, publishing events
to Kafka after a partition is created, or archiving rows before detachment.

Usage example::

    class KafkaExportHooks(BasePartitionLifecycleHooks):
        def __init__(self, producer: KafkaProducer) -> None:
            self._producer = producer

        def before_drop(self, table_name: str, partition_name: str) -> None:
            self._producer.send("partition.before_drop", {
                "table": table_name,
                "partition": partition_name,
            })

    service = PartitionLifecycleService(
        repo=repo,
        metadata=metadata,
        locks=locks,
        period_calculator=calculator,
        hooks=[KafkaExportHooks(producer)],
    )
"""

from typing import Protocol, runtime_checkable

from pg_partsmith.entities import PartitionInfo, TablePartitionConfig


@runtime_checkable
class PartitionLifecycleHooks(Protocol):
    """Protocol for partition lifecycle hooks.

    Implement this protocol to inject custom logic at each step of the partition
    lifecycle without inheriting from a specific base class.
    """

    def before_create(
        self,
        config: TablePartitionConfig,
        partition_name: str,
        from_value: str,
        to_value: str,
    ) -> None: ...

    def after_create(
        self,
        config: TablePartitionConfig,
        partition: PartitionInfo,
    ) -> None: ...

    def before_detach(
        self,
        table_name: str,
        partition: PartitionInfo,
    ) -> None: ...

    def after_detach(
        self,
        table_name: str,
        partition_name: str,
    ) -> None: ...

    def before_drop(
        self,
        table_name: str,
        partition_name: str,
    ) -> None: ...

    def after_drop(
        self,
        table_name: str,
        partition_name: str,
    ) -> None: ...


class BasePartitionLifecycleHooks:
    """No-op base implementation of partition lifecycle hooks.

    Subclass and override only the methods you need.
    All methods are no-ops by default so you can selectively add behaviour
    without implementing every step.
    """

    def before_create(
        self,
        config: TablePartitionConfig,
        partition_name: str,
        from_value: str,
        to_value: str,
    ) -> None:
        """Called before a partition is created.

        Args:
            config: Table partition configuration.
            partition_name: Name the new partition will be given.
            from_value: Start boundary value.
            to_value: End boundary value.
        """

    def after_create(
        self,
        config: TablePartitionConfig,
        partition: PartitionInfo,
    ) -> None:
        """Called after a partition has been created (and optionally attached).

        Args:
            config: Table partition configuration.
            partition: Info about the newly created partition.
        """

    def before_detach(
        self,
        table_name: str,
        partition: PartitionInfo,
    ) -> None:
        """Called before a partition is detached from its parent table.

        This is a good place to export or archive data while the partition
        is still accessible via the parent table's indexes and constraints.

        Args:
            table_name: Parent table name.
            partition: Info about the partition being detached.
        """

    def after_detach(
        self,
        table_name: str,
        partition_name: str,
    ) -> None:
        """Called after a partition has been detached.

        Args:
            table_name: Parent table name.
            partition_name: Name of the detached partition.
        """

    def before_drop(
        self,
        table_name: str,
        partition_name: str,
    ) -> None:
        """Called before a partition table is dropped.

        This is the last chance to read or export data from the partition
        before it is permanently destroyed.

        Args:
            table_name: Parent table name.
            partition_name: Name of the partition about to be dropped.
        """

    def after_drop(
        self,
        table_name: str,
        partition_name: str,
    ) -> None:
        """Called after a partition table has been dropped.

        Args:
            table_name: Parent table name.
            partition_name: Name of the dropped partition.
        """
