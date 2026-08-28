"""Pure retention-selection rules shared by the aio and sync pruning services.

Selecting which partitions fall outside the retention window involves no IO —
the calculator protocol is synchronous and the partition list is already
fetched — so one implementation serves both mirrors.
"""

from __future__ import annotations

import logging
from datetime import datetime, tzinfo
from typing import TYPE_CHECKING

from pg_partsmith.partition_bounds import parse_boundary_literal
from pg_partsmith.protocols import BoundaryDecoder
from pg_partsmith.utils import split_qualified_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from pg_partsmith.entities import PartitionInfo, Period, TablePartitionConfig
    from pg_partsmith.protocols import PeriodCalculator

logger = logging.getLogger(__name__)

# Upper-bound spellings that mean "no upper limit" — such partitions hold
# current data and must never be pruned.
_UNBOUNDED_UPPER = frozenset({"MAXVALUE", "INFINITY", "+INFINITY"})


def select_partitions_to_prune(
    calculator: PeriodCalculator[Period],
    boundary_tz: tzinfo,
    config: TablePartitionConfig,
    all_partitions: list[PartitionInfo],
) -> list[PartitionInfo]:
    """Select partitions outside the retention window, oldest first.

    Boundary-based selection is preferred; an attached partition whose catalog
    boundary cannot be interpreted (while the cutoff is a real instant) is
    skipped — guessing by name risks dropping live data. Name-based fallback
    applies to detached orphans, and to everything when the cutoff itself is
    non-temporal (custom calculators).

    Boundaries are read back through the calculator when it implements
    :class:`~pg_partsmith.protocols.BoundaryDecoder`, so a table keyed by an
    encoded identifier (a UUIDv7, a sortable id) is pruned by the same rules as
    a timestamp-keyed one.
    """
    decode = _boundary_decoder(calculator, boundary_tz)

    current_period = calculator.current_period()
    cutoff_period = calculator.period_before(current_period, config.retention_count - 1)
    cutoff_start_raw = calculator.get_boundaries(cutoff_period)[0]
    cutoff_start_dt = decode(cutoff_start_raw)

    partitions_to_prune: list[PartitionInfo] = []
    parsed_period_by_name: dict[str, Period] = {}
    parsed_end_dt_by_name: dict[str, datetime] = {}

    for partition in all_partitions:
        if partition.is_default:
            continue

        # An unbounded upper bound (MAXVALUE / infinity) means the partition
        # holds current data no matter what its name suggests — never prune it.
        if partition.to_value is not None and partition.to_value.strip().upper() in _UNBOUNDED_UPPER:
            logger.info(
                "Skipping partition with unbounded upper boundary",
                extra={"partition_name": partition.name, "to_value": partition.to_value},
            )
            continue

        end_dt = decode(partition.to_value)
        if cutoff_start_dt is not None and end_dt is not None:
            if end_dt <= cutoff_start_dt:
                partitions_to_prune.append(partition)
                parsed_end_dt_by_name[partition.name] = end_dt
            continue

        # An attached partition always carries a catalog boundary; if we
        # cannot interpret it while the cutoff is a real instant, guessing
        # by name risks dropping live data — fail closed instead.
        if partition.is_attached and cutoff_start_dt is not None:
            logger.warning(
                "Cannot interpret attached partition boundary; skipping to avoid unsafe pruning",
                extra={"partition_name": partition.name, "to_value": partition.to_value},
            )
            continue

        period: Period | None = None
        try:
            _, relname = split_qualified_name(partition.name)
            period = calculator.parse_partition_name(relname)
            if period is not None:
                if period < cutoff_period:
                    partitions_to_prune.append(partition)
                    parsed_period_by_name[partition.name] = period
                    continue
                parsed_period_by_name[partition.name] = period
        except (KeyboardInterrupt, SystemExit):
            raise
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning(
                "Failed to parse or compare partition period; treating as unknown",
                extra={
                    "partition_name": partition.name,
                    "error": str(exc),
                },
            )

        # Also prune orphan partitions that don't match current naming pattern
        if not partition.is_attached and period is None:
            partitions_to_prune.append(partition)

    def _sort_key(p: PartitionInfo) -> tuple[int, datetime | None, str]:
        end_dt_key = parsed_end_dt_by_name.get(p.name)
        if end_dt_key is not None:
            return (0, end_dt_key, p.name)
        period_key = parsed_period_by_name.get(p.name)
        if period_key is not None:
            return (1, period_key.to_datetime(), p.name)
        return (2, None, p.name)

    # Precompute keys once (O(n)) to avoid O(n*log(n)) key evaluations during sort.
    key_by_name = {p.name: _sort_key(p) for p in partitions_to_prune}
    partitions_to_prune.sort(key=lambda p: key_by_name[p.name])
    return partitions_to_prune


def _boundary_decoder(
    calculator: PeriodCalculator[Period],
    boundary_tz: tzinfo,
) -> Callable[[str | None], datetime | None]:
    """Return the function that turns a boundary literal into a UTC instant.

    A calculator that encodes its own boundaries must also decode them, so it
    is asked first. Runtime-checkable protocols only verify that an attribute
    exists, so what comes back is checked too: a loose implementation returning
    something that is not an instant falls back to timestamp parsing rather
    than poisoning the retention comparison with a non-comparable value.
    """
    if not isinstance(calculator, BoundaryDecoder):
        return lambda value: parse_boundary_literal(value, boundary_tz)

    decode_boundary = calculator.decode_boundary

    def decode(value: str | None) -> datetime | None:
        if value is None:
            return None
        # Annotated as object so the type checker keeps the fallback branch:
        # the guard exists precisely for implementations that do not honour the
        # declared return type.
        decoded: object = decode_boundary(value)
        if decoded is None:
            return None
        if isinstance(decoded, datetime):
            if decoded.tzinfo is not None:
                return decoded
            # A naive datetime is exactly the non-comparable value this guard
            # exists to keep out: comparing it with the aware cutoff raises from
            # the middle of retention, where the codec that produced it is no
            # longer in view. Fall back rather than propagate.
            logger.debug(
                "Calculator returned a naive datetime boundary; falling back to timestamp parsing",
                extra={"boundary": value, "calculator": type(calculator).__name__},
            )
            return parse_boundary_literal(value, boundary_tz)
        # Quiet by design, matching validate_timezone_alignment: a boundary that
        # ends up uninterpretable is already reported where it affects a
        # decision, and this path fires per partition.
        logger.debug(
            "Calculator returned a non-datetime boundary; falling back to timestamp parsing",
            extra={"boundary": value, "calculator": type(calculator).__name__},
        )
        return parse_boundary_literal(value, boundary_tz)

    return decode


# Retained under its original name: ``pruning_rules.parse_boundary_to_utc_dt``
# was importable before the parser moved next to the rest of the bound parsing.
parse_boundary_to_utc_dt = parse_boundary_literal
