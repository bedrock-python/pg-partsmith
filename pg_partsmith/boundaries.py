"""Where a RANGE level's partitions begin and end.

A ``RANGE`` level needs one rule — a :class:`RangeBoundaries` — that turns a
*position* on its axis into the half-open :class:`Window` holding it, steps
between adjacent windows, renders a window as the two literals PostgreSQL
compares the key against, reads such a literal back into a position, and
names the partition for a window. Two axes ship:

* :class:`TimeBoundaries` — instants, divided into calendar periods by any
  :class:`~pg_partsmith.protocols.PeriodCalculator`; and
* :class:`NumericBoundaries` — integers, divided into fixed-width steps.

Both are IO-free. Whatever "now" means on an axis — the clock, the key's
high-water mark — is resolved outside, by the introspector, and handed to the
planner as the level's *cursor*.

The physical literal and the semantic window come apart whenever the key is a
*time-sortable identifier* rather than a timestamp — a UUIDv7, a ULID, an
epoch bigint. A :class:`RangeBoundaryCodec` bridges the two so the lifecycle
keeps reasoning in periods while the DDL speaks the key's own language::

    TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=UUIDv7BoundaryCodec())

Codecs are bidirectional on purpose: retention compares a partition's
*catalog* upper bound against the cutoff, so a codec that could only encode
would create partitions the library could never prune.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, tzinfo
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

from .constants import DEFAULT_NUMERIC_NAME_SUFFIX
from .partition_bounds import parse_boundary_literal
from .periods import PartitionGranularity, Period
from .protocols import PeriodCalculator
from .types import PositiveInt, StrippedNonEmptyStr
from .utils import timezone_name

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "Axis",
    "CursorSource",
    "NumericBoundaries",
    "RangeBoundaries",
    "RangeBoundaryCodec",
    "TimeBoundaries",
    "UUIDv7BoundaryCodec",
    "Window",
]


class Axis(StrEnum):
    """What kind of value a RANGE level's key is ordered by.

    Attributes:
        TIME: Instants; the cursor is the clock.
        INTEGER: Integers; the cursor is the key's high-water mark.
    """

    TIME = "time"
    INTEGER = "integer"


class CursorSource(StrEnum):
    """Where the planner reads a progression level's "now" from.

    Attributes:
        CLOCK: The wall clock, in the level's timezone.
        MAX_KEY: ``max(key)`` over the table — always right, one index probe
            per leaf.
        SEQUENCE: The last value of the key's serial/identity sequence — one
            catalog read, right only for a key fed by that sequence.
    """

    CLOCK = "clock"
    MAX_KEY = "max_key"
    SEQUENCE = "sequence"


@dataclass(frozen=True)
class Window:
    """A half-open interval ``[start, end)`` on a RANGE level's axis.

    Attributes:
        start: Inclusive lower position.
        end: Exclusive upper position.
        token: Opaque handle the boundaries that produced this window may need
            to name or render it again (a :class:`~pg_partsmith.Period`, say).
            Never compared; a window read back from the catalog has none.
    """

    start: Any
    end: Any
    token: Any = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Window):
            return NotImplemented
        return bool(self.start == other.start and self.end == other.end)

    def __hash__(self) -> int:
        return hash((self.start, self.end))

    def __lt__(self, other: Window) -> bool:
        return bool(self.start < other.start)

    def overlaps(self, other: Window) -> bool:
        """True when the two windows share at least one position."""
        return bool(self.start < other.end and other.start < self.end)


@runtime_checkable
class RangeBoundaries(Protocol):
    """The rule dividing a RANGE level's axis into partitions.

    Implement this to partition over an axis the library does not know. The
    contract: windows tile the axis with no gap and no overlap, ``shift`` walks
    them in order, ``decode`` inverts ``literals`` closely enough for equality
    comparisons, and ``child_name`` is deterministic.
    """

    @property
    def axis(self) -> Axis:
        """What the key is ordered by."""
        ...

    @property
    def cursor_source(self) -> CursorSource:
        """Where the level's "now" comes from."""
        ...

    def window_at(self, position: Any) -> Window:
        """Return the window holding ``position``."""
        ...

    def shift(self, window: Window, offset: int) -> Window:
        """Return the window ``offset`` steps after (negative: before) ``window``."""
        ...

    def literals(self, window: Window) -> tuple[str, str]:
        """Return the ``FROM`` and ``TO`` literals for ``window``."""
        ...

    def decode(self, literal: str) -> Any | None:
        """Return the position a catalog literal stands for, or None.

        ``MINVALUE``, ``MAXVALUE`` and anything of another type decode to None
        rather than raising, so a table with a mixed history can still be
        introspected.
        """
        ...

    def child_name(self, parent_relname: str, window: Window) -> str:
        """Return the bare relation name of ``window``'s partition under ``parent_relname``."""
        ...

    def parse_child_name(self, relname: str) -> Window | None:
        """Return the window a relation name encodes, or None when it encodes none."""
        ...

    def describe(self, window: Window) -> str:
        """Render ``window`` for a human (reasons, logs)."""
        ...


# ── Codecs ──────────────────────────────────────────────────────────────────────


@runtime_checkable
class RangeBoundaryCodec(Protocol):
    """Translates between instants and the literals a time-keyed RANGE partition is bound by.

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

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self)

    def __hash__(self) -> int:
        return hash(type(self))


class EpochBoundaryCodec:
    """Encodes periods as integer seconds (or milliseconds) since the Unix epoch.

    For a ``bigint`` key that stores a timestamp as a number — the shape
    ``pg_partman`` calls an *epoch* control column.

    Attributes:
        unit: ``"seconds"`` or ``"milliseconds"``.
    """

    _SCALE: ClassVar[dict[str, int]] = {"seconds": 1, "milliseconds": 1000}

    def __init__(self, unit: str = "seconds") -> None:
        """Initialize the codec.

        Args:
            unit: ``"seconds"`` (default) or ``"milliseconds"``.

        Raises:
            ValueError: If ``unit`` is neither.
        """
        if unit not in self._SCALE:
            msg = f"unit must be one of {sorted(self._SCALE)}, got {unit!r}"
            raise ValueError(msg)
        self._unit = unit
        self._scale = self._SCALE[unit]

    @property
    def unit(self) -> str:
        """The unit the key counts in."""
        return self._unit

    def encode(self, start: datetime, end: datetime) -> tuple[str, str]:
        """Return the epoch value of each boundary instant."""
        return str(self._to_epoch(start)), str(self._to_epoch(end))

    def decode(self, literal: str) -> datetime | None:
        """Return the instant an epoch literal stands for, or None."""
        stripped = literal.strip()
        if not _INTEGER_PATTERN.match(stripped):
            return None
        try:
            return datetime.fromtimestamp(int(stripped) / self._scale, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None

    def _to_epoch(self, instant: datetime) -> int:
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)
        return int(instant.timestamp() * self._scale)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, EpochBoundaryCodec) and other._unit == self._unit

    def __hash__(self) -> int:
        return hash((type(self), self._unit))


# Codecs addressable by name from serialized configuration.
_CODECS_BY_NAME: dict[str, Callable[[], RangeBoundaryCodec]] = {
    "uuidv7": UUIDv7BoundaryCodec,
    "epoch_seconds": lambda: EpochBoundaryCodec("seconds"),
    "epoch_milliseconds": lambda: EpochBoundaryCodec("milliseconds"),
}


def resolve_codec(value: object) -> RangeBoundaryCodec | None:
    """Turn a codec name or instance into an instance.

    Args:
        value: None, a :class:`RangeBoundaryCodec`, or one of the names in
            :data:`codec_names`.

    Raises:
        ValueError: If the name is unknown or the object is not a codec.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return _CODECS_BY_NAME[value]()
        except KeyError:
            msg = f"Unknown boundary codec {value!r}; known names: {sorted(_CODECS_BY_NAME)}"
            raise ValueError(msg) from None
    if isinstance(value, RangeBoundaryCodec):
        return value
    msg = f"boundary codec must implement encode()/decode(), got {type(value).__name__}"
    raise TypeError(msg)


def codec_names() -> tuple[str, ...]:
    """Names accepted wherever a codec can be given as a string."""
    return tuple(sorted(_CODECS_BY_NAME))


# ── Time axis ───────────────────────────────────────────────────────────────────


class TimeBoundaries(BaseModel):
    """Calendar periods over a time-ordered key.

    The period arithmetic is a :class:`~pg_partsmith.protocols.PeriodCalculator`:
    either one of the built-in granularities, or a custom calculator passed in
    directly. The physical literals are timestamps unless a codec says
    otherwise.

    Attributes:
        kind: Discriminator; always ``"time"``.
        granularity: Built-in period size. Mutually exclusive with ``calculator``.
        tz: Timezone the calendar is computed in. Only ``datetime.UTC`` and keyed
            :class:`zoneinfo.ZoneInfo` instances are accepted; may also be given
            as an IANA name. Ignored when ``calculator`` is given.
        codec: Encoder for the physical key, by name (``"uuidv7"``,
            ``"epoch_seconds"``, ``"epoch_milliseconds"``) or instance. Ignored
            when ``calculator`` is given — a custom calculator carries its own.
        calculator: A ready-made calculator, for custom calendars.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: str = Field(default="time", frozen=True)
    granularity: PartitionGranularity | None = None
    tz: Any = UTC
    codec: Any = None
    calculator: Any = Field(default=None, exclude=True)

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, v: str) -> str:
        """Refuse a foreign discriminator smuggled in from serialized data."""
        if v != "time":
            msg = f"TimeBoundaries.kind must be 'time', got {v!r}"
            raise ValueError(msg)
        return v

    @field_validator("tz", mode="before")
    @classmethod
    def validate_tz(cls, v: object) -> tzinfo:
        """Accept an IANA name as well as a tzinfo."""
        if isinstance(v, str):
            if v.upper() == "UTC":
                return UTC
            return ZoneInfo(v)
        if isinstance(v, tzinfo):
            return v
        msg = f"tz must be a tzinfo or an IANA name, got {type(v).__name__}"
        raise TypeError(msg)

    @field_validator("codec", mode="before")
    @classmethod
    def validate_codec(cls, v: object) -> RangeBoundaryCodec | None:
        """Resolve a codec name to an instance."""
        return resolve_codec(v)

    @field_serializer("tz")
    def serialize_tz(self, v: tzinfo) -> str:
        """Render the zone by its IANA name."""
        return timezone_name(v)

    @field_serializer("codec")
    def serialize_codec(self, v: object) -> str | None:
        """Render a built-in codec by name; anything else by its class."""
        if v is None:
            return None
        for name, factory in _CODECS_BY_NAME.items():
            if factory() == v:
                return name
        return type(v).__name__

    @model_validator(mode="after")
    def validate_source(self) -> TimeBoundaries:
        """Exactly one of ``granularity`` / ``calculator`` decides the calendar."""
        if self.calculator is None and self.granularity is None:
            msg = "TimeBoundaries needs either a granularity or a calculator"
            raise ValueError(msg)
        if self.calculator is not None:
            if self.granularity is not None:
                msg = "TimeBoundaries takes either a granularity or a calculator, not both"
                raise ValueError(msg)
            if not isinstance(self.calculator, PeriodCalculator):
                msg = f"calculator must implement PeriodCalculator, got {type(self.calculator).__name__}"
                raise TypeError(msg)
        return self

    @property
    def axis(self) -> Axis:
        """Instants."""
        return Axis.TIME

    @property
    def cursor_source(self) -> CursorSource:
        """The clock."""
        return CursorSource.CLOCK

    @property
    def period_calculator(self) -> PeriodCalculator[Period]:
        """The calculator doing the period arithmetic."""
        if self.calculator is not None:
            return self.calculator  # type: ignore[no-any-return]
        return _calculator_for(self)

    def window_for(self, period: Period) -> Window:
        """Return the window one period spans."""
        calculator = self.period_calculator
        return Window(start=_period_start(calculator, period), end=_period_start(calculator, period + 1), token=period)

    def window_at(self, position: Any) -> Window:
        """Return the period holding ``position``.

        Args:
            position: A timezone-aware instant, or None for the current period.
        """
        calculator = self.period_calculator
        if position is None:
            return self.window_for(calculator.current_period())
        return self.window_for(_period_at(calculator, position))

    def shift(self, window: Window, offset: int) -> Window:
        """Return the window ``offset`` periods away."""
        period = self._period_of(window)
        return self.window_for(period + offset)

    def literals(self, window: Window) -> tuple[str, str]:
        """Render the period's bounds the way its calculator always has."""
        return self.period_calculator.get_boundaries(self._period_of(window))

    def decode(self, literal: str) -> datetime | None:
        """Read a catalog literal back as a UTC instant."""
        calculator = self.period_calculator
        decode_boundary = getattr(calculator, "decode_boundary", None)
        if decode_boundary is not None:
            decoded = decode_boundary(literal)
            if isinstance(decoded, datetime):
                return decoded if decoded.tzinfo is not None else decoded.replace(tzinfo=UTC)
            return None
        return parse_boundary_literal(literal, self.timezone)

    def child_name(self, parent_relname: str, window: Window) -> str:
        """Name the period's partition the way its calculator always has."""
        return self.period_calculator.format_partition_name(parent_relname, self._period_of(window))

    def parse_child_name(self, relname: str) -> Window | None:
        """Read a period back out of a relation name."""
        period = self.period_calculator.parse_partition_name(relname)
        return None if period is None else self.window_for(period)

    def describe(self, window: Window) -> str:
        """Render the period compactly."""
        return str(self._period_of(window))

    @property
    def timezone(self) -> tzinfo:
        """Timezone the calendar is computed in."""
        calculator = self.period_calculator
        tz = getattr(calculator, "tz", None)
        return tz if isinstance(tz, tzinfo) else UTC

    @property
    def timezone_name(self) -> str | None:
        """IANA name of :attr:`timezone`, when the calculator declares one."""
        name = getattr(self.period_calculator, "timezone_name", None)
        return name if isinstance(name, str) else None

    def _period_of(self, window: Window) -> Period:
        if isinstance(window.token, Period):
            return window.token
        # A window read back from the catalog carries no period; the one that
        # starts where it starts is the same window.
        return _period_at(self.period_calculator, window.start)


def _calculator_for(boundaries: TimeBoundaries) -> PeriodCalculator[Period]:
    """Build (and cache on the model) the calculator for a built-in granularity."""
    cached = boundaries.__dict__.get("_cached_calculator")
    if cached is not None:
        return cached  # type: ignore[no-any-return]
    from .strategies.selector import get_period_calculator  # noqa: PLC0415 -- strategies import this module

    assert boundaries.granularity is not None  # guaranteed by validate_source
    calculator = get_period_calculator(boundaries.granularity, tz=boundaries.tz, boundary_codec=boundaries.codec)
    # ``object.__setattr__`` because the model is frozen; the cache is derived
    # state, not a field, and equality/hash never look at it.
    object.__setattr__(boundaries, "_cached_calculator", calculator)
    return calculator


def _period_start(calculator: PeriodCalculator[Period], period: Period) -> datetime:
    """The instant a period begins, in the calculator's timezone, as an aware datetime."""
    period_start = getattr(calculator, "period_start", None)
    if period_start is not None:
        start = period_start(period)
        if isinstance(start, datetime):
            return start if start.tzinfo is not None else start.replace(tzinfo=UTC)
    return period.to_datetime()


def _period_at(calculator: PeriodCalculator[Period], instant: datetime) -> Period:
    """The period holding ``instant``.

    Calculators that implement ``period_at`` answer directly; any other is
    walked from its current period, one step at a time, which is slow only for
    a position very far from now.
    """
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=UTC)
    period_at = getattr(calculator, "period_at", None)
    if period_at is not None:
        result = period_at(instant)
        if isinstance(result, Period):
            return result

    period = calculator.current_period()
    if instant < _period_start(calculator, period):
        while instant < _period_start(calculator, period):
            period = period - 1
        return period
    while instant >= _period_start(calculator, period + 1):
        period = period + 1
    return period


# ── Integer axis ────────────────────────────────────────────────────────────────


class NumericBoundaries(BaseModel):
    """Fixed-width steps over an integer key.

    Windows are ``[origin + k·step, origin + (k+1)·step)``; a queue partitioned
    every 100 000 message ids is ``NumericBoundaries(step=100_000)``.

    Attributes:
        kind: Discriminator; always ``"integer"``.
        step: Width of every window.
        origin: A window boundary the grid is anchored on; 0 by default.
        name_suffix: Template appended to the parent's name; must contain
            ``{start}`` and otherwise only lowercase identifier characters.
        cursor_source: Where the level's high-water mark is read from.
    """

    model_config = ConfigDict(frozen=True)

    _NAME_SUFFIX_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^[a-z0-9_]*\{start\}[a-z0-9_]*$")

    kind: str = Field(default="integer", frozen=True)
    step: PositiveInt
    origin: int = 0
    name_suffix: StrippedNonEmptyStr = DEFAULT_NUMERIC_NAME_SUFFIX
    cursor_source: CursorSource = CursorSource.MAX_KEY

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, v: str) -> str:
        """Refuse a foreign discriminator smuggled in from serialized data."""
        if v != "integer":
            msg = f"NumericBoundaries.kind must be 'integer', got {v!r}"
            raise ValueError(msg)
        return v

    @field_validator("name_suffix")
    @classmethod
    def validate_name_suffix(cls, v: str) -> str:
        """Reject templates that could not produce a safe, unique identifier."""
        if not cls._NAME_SUFFIX_PATTERN.match(v):
            msg = (
                f"name_suffix {v!r} must contain '{{start}}' and otherwise only lowercase letters, digits, "
                "and underscores"
            )
            raise ValueError(msg)
        return v

    @field_validator("cursor_source")
    @classmethod
    def validate_cursor_source(cls, v: CursorSource) -> CursorSource:
        """An integer axis has no clock to read."""
        if v is CursorSource.CLOCK:
            msg = "NumericBoundaries cannot read its cursor from the clock; use MAX_KEY or SEQUENCE"
            raise ValueError(msg)
        return v

    @property
    def axis(self) -> Axis:
        """Integers."""
        return Axis.INTEGER

    def window_at(self, position: Any) -> Window:
        """Return the window holding ``position`` (``origin`` when None)."""
        value = self.origin if position is None else int(position)
        start = self.origin + ((value - self.origin) // self.step) * self.step
        return Window(start=start, end=start + self.step)

    def shift(self, window: Window, offset: int) -> Window:
        """Return the window ``offset`` steps away."""
        start = int(window.start) + offset * self.step
        return Window(start=start, end=start + self.step)

    def literals(self, window: Window) -> tuple[str, str]:
        """Render both bounds as plain integers."""
        return str(int(window.start)), str(int(window.end))

    def decode(self, literal: str) -> int | None:
        """Read an integer literal back; anything else is None."""
        stripped = literal.strip()
        return int(stripped) if _INTEGER_PATTERN.match(stripped) else None

    def child_name(self, parent_relname: str, window: Window) -> str:
        """Name the window's partition after its start value."""
        return f"{parent_relname}{self.name_suffix.format(start=_spell_int(int(window.start)))}"

    def parse_child_name(self, relname: str) -> Window | None:
        """Read the start value back out of a relation name."""
        match = self._name_pattern().search(relname)
        if not match:
            return None
        start = _unspell_int(match.group("start"))
        if start is None or (start - self.origin) % self.step:
            return None
        return Window(start=start, end=start + self.step)

    def describe(self, window: Window) -> str:
        """Render the window as ``[start, end)``."""
        return f"[{window.start}, {window.end})"

    def own_name_budget(self) -> int:
        """Bytes this level adds to a partition name, sized for a 19-digit value."""
        return len(self.name_suffix) - len("{start}") + len("m") + 19

    def _name_pattern(self) -> re.Pattern[str]:
        return re.compile(re.escape(self.name_suffix).replace(re.escape("{start}"), r"(?P<start>m?\d+)") + "$")


def _spell_int(value: int) -> str:
    """Spell an integer so it can live inside an identifier (``m`` for minus)."""
    return f"m{-value}" if value < 0 else str(value)


def _unspell_int(text: str) -> int | None:
    if text.startswith("m"):
        return -int(text[1:]) if text[1:].isdigit() else None
    return int(text) if text.isdigit() else None


def parse_boundaries(value: object) -> RangeBoundaries:
    """Turn serialized boundaries into a strategy, passing instances through.

    Dicts are dispatched on their ``kind`` (``"time"`` / ``"integer"``);
    anything already implementing :class:`RangeBoundaries` is returned as-is.

    Raises:
        ValueError: If the dict names an unknown kind.
        TypeError: If the object is neither.
    """
    if isinstance(value, dict):
        kind = value.get("kind", "time")
        if kind == "time":
            return TimeBoundaries.model_validate(value)
        if kind == "integer":
            return NumericBoundaries.model_validate(value)
        msg = f"Unknown boundaries kind {kind!r}; expected 'time' or 'integer'"
        raise ValueError(msg)
    if isinstance(value, RangeBoundaries):
        return value
    msg = f"boundaries must implement RangeBoundaries, got {type(value).__name__}"
    raise TypeError(msg)


_UUID_PATTERN = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_INTEGER_PATTERN = re.compile(r"^-?\d+$")
