"""Physical encoding of period boundaries into partition-key literals.

A time-based partition has two independent notions of "when":

* the **semantic period** — a week, a month — which decides the partition's
  name, its place in the create-ahead window, and when retention drops it, and
* the **physical boundary** — the literal PostgreSQL compares the partition key
  against.

For a ``timestamptz`` key the two coincide and no codec is needed. They come
apart whenever the partition key is a *time-sortable identifier* rather than a
timestamp: a UUIDv7, a ULID, a Snowflake id, an epoch bigint. Such a table is
still partitioned by time — the ordering of the key is the ordering of time —
but its ``FOR VALUES FROM … TO …`` literals are identifiers, not dates.

A :class:`RangeBoundaryCodec` bridges the two, so the lifecycle keeps reasoning
in periods while the DDL speaks the key's own language::

    WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())

Codecs are bidirectional on purpose: retention compares a partition's *catalog*
upper bound against the cutoff, so a codec that could only encode would create
partitions the library could never prune.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

__all__ = ["RangeBoundaryCodec", "UUIDv7BoundaryCodec"]


@runtime_checkable
class RangeBoundaryCodec(Protocol):
    """Translates between instants and the literals a RANGE partition is bound by.

    Implement this to partition by any time-sortable key. The only contract is
    that :meth:`encode` is monotonic in its argument and :meth:`decode` inverts
    it closely enough for retention comparisons — adjacent periods must produce
    contiguous ``[lower, upper)`` literals with no gap and no overlap, or rows
    fall through into the DEFAULT partition.
    """

    def encode(self, start: datetime, end: datetime) -> tuple[str, str]:
        """Encode a half-open period into ``(from_value, to_value)`` literals.

        Args:
            start: Period start, inclusive; timezone-aware.
            end: Period end, exclusive; timezone-aware.

        Returns:
            The literals to use in ``FOR VALUES FROM (…) TO (…)``.
        """
        ...

    def decode(self, literal: str) -> datetime | None:
        """Decode a catalog boundary literal back to a UTC instant.

        Args:
            literal: A boundary as read from ``pg_get_expr(relpartbound, oid)``
                and unwrapped of quoting and casts.

        Returns:
            The instant the literal stands for, or None when it carries no
            instant (``MINVALUE``, ``MAXVALUE``, an unparseable value).
        """
        ...


class UUIDv7BoundaryCodec:
    """Encodes periods as the smallest UUIDv7 of each boundary instant.

    UUIDv7 (RFC 9562) puts a 48-bit big-endian Unix-milliseconds timestamp in
    its leading bits, so UUIDv7 values sort chronologically and a table keyed by
    one can be RANGE-partitioned by time.

    Both boundaries use the *minimum* UUID for their instant — every random bit
    zero. Using the minimum on both ends is what makes adjacent periods exactly
    contiguous: one period's upper bound is the next period's lower bound, so no
    identifier can fall between two partitions.

    Timestamps are truncated to milliseconds, matching UUIDv7's own resolution.
    Period boundaries are whole hours or larger, so this never loses a boundary.
    """

    _VERSION = 0x7
    _VARIANT = 0x2
    _TIMESTAMP_BITS = 48
    _MAX_TIMESTAMP_MS = (1 << _TIMESTAMP_BITS) - 1

    def encode(self, start: datetime, end: datetime) -> tuple[str, str]:
        """Return the minimum UUIDv7 for each boundary instant.

        Args:
            start: Period start, inclusive.
            end: Period end, exclusive.

        Returns:
            Canonical UUID strings for the two boundaries.
        """
        return str(self.min_uuid_for(start)), str(self.min_uuid_for(end))

    def decode(self, literal: str) -> datetime | None:
        """Return the instant encoded in a UUIDv7 literal, or None.

        Non-UUID literals (``MINVALUE``, ``MAXVALUE``, anything the catalog
        renders for a differently-typed key) and UUIDs of another version
        decode to None rather than raising, so a mixed-history table can still
        be introspected.
        """
        stripped = literal.strip()
        if not _UUID_PATTERN.match(stripped):
            return None

        try:
            value = UUID(stripped)
        except ValueError:
            return None

        if value.version != self._VERSION:
            return None

        timestamp_ms = int.from_bytes(value.bytes[:6], byteorder="big")
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)

    def min_uuid_for(self, instant: datetime) -> UUID:
        """Return the smallest valid UUIDv7 whose timestamp is ``instant``.

        Deterministic: every bit outside the timestamp, version, and variant
        fields is zero, so the same instant always yields the same boundary.

        Args:
            instant: A timezone-aware datetime; naive values are read as UTC.

        Returns:
            The minimum UUIDv7 for that millisecond.
        """
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)

        # UUIDv7's timestamp field is unsigned and 48 bits wide; clamping keeps
        # far-past and far-future periods encodable instead of raising.
        timestamp_ms = max(0, min(int(instant.timestamp() * 1000), self._MAX_TIMESTAMP_MS))

        # [48-bit timestamp][ver=7][12 bits rand_a][variant=0b10][62 bits rand_b]
        as_int = (timestamp_ms << 80) | (self._VERSION << 76) | (self._VARIANT << 62)
        return UUID(int=as_int)


_UUID_PATTERN = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
