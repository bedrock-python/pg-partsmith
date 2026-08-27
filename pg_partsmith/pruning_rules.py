"""Pure retention-selection rules shared by the aio and sync pruning services.

Selecting which partitions fall outside the retention window involves no IO —
the calculator protocol is synchronous and the partition list is already
fetched — so one implementation serves both mirrors.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, tzinfo
from typing import TYPE_CHECKING

from dateutil.parser import isoparse

from pg_partsmith.utils import split_qualified_name

if TYPE_CHECKING:
    from pg_partsmith.entities import PartitionInfo, Period, TablePartitionConfig
    from pg_partsmith.protocols import PeriodCalculator

logger = logging.getLogger(__name__)

# Upper-bound spellings that mean "no upper limit" — such partitions hold
# current data and must never be pruned.
_UNBOUNDED_UPPER = frozenset({"MAXVALUE", "INFINITY", "+INFINITY"})

_DATE_ONLY_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


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
    """
    current_period = calculator.current_period()
    cutoff_period = calculator.period_before(current_period, config.retention_count - 1)
    cutoff_start_raw = calculator.get_boundaries(cutoff_period)[0]
    cutoff_start_dt = parse_boundary_to_utc_dt(cutoff_start_raw, boundary_tz)

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

        end_dt = parse_boundary_to_utc_dt(partition.to_value, boundary_tz)
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


def parse_boundary_to_utc_dt(value: str | None, boundary_tz: tzinfo) -> datetime | None:
    """Parse a PostgreSQL partition boundary string to a UTC instant.

    Naive values (bare dates, timestamps without an offset) are interpreted in
    ``boundary_tz``; values carrying an offset are converted as-is.
    Comparisons downstream always happen between UTC instants.
    """
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None

    if v.upper() in ("MINVALUE", "MAXVALUE"):
        return None

    if "-" not in v and ":" not in v:
        return None

    if _DATE_ONLY_PATTERN.fullmatch(v):
        try:
            return datetime.fromisoformat(v).replace(tzinfo=boundary_tz).astimezone(UTC)
        except ValueError:
            return None

    try:
        parsed = isoparse(v)
    except (KeyboardInterrupt, SystemExit):
        raise
    except (ValueError, TypeError):
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=boundary_tz).astimezone(UTC)
    return parsed.astimezone(UTC)
