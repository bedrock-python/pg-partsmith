"""Where a RANGE level's partitions begin and end: windows, boundaries and codecs."""

from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pytest
from freezegun import freeze_time
from pydantic import ValidationError

from pg_partsmith.boundaries import (
    Axis,
    CursorSource,
    EpochBoundaryCodec,
    NumericBoundaries,
    RangeBoundaries,
    RangeBoundaryCodec,
    TimeBoundaries,
    UUIDv7BoundaryCodec,
    Window,
    codec_names,
    parse_boundaries,
    resolve_codec,
)
from pg_partsmith.periods import PartitionGranularity, Period
from pg_partsmith.strategies import DayPeriodCalculator, MonthPeriodCalculator, WeekPeriodCalculator

_MOSCOW = ZoneInfo("Europe/Moscow")
_BERLIN = ZoneInfo("Europe/Berlin")  # a zone that changes its clocks twice a year


class _LegacyCalculator:
    """A pre-1.0 monthly calculator: every protocol method except ``period_at``."""

    def __init__(self, current: Period) -> None:
        self._current = current

    def current_period(self) -> Period:
        return self._current

    def next_periods(self, count: int) -> list[Period]:
        return [self._current + offset for offset in range(count)]

    def period_before(self, reference: Period, offset: int) -> Period:
        return reference - offset

    def format_partition_name(self, table_name: str, period: Period) -> str:
        return f"{table_name}__legacy_{period}"

    def parse_partition_name(self, partition_name: str) -> Period | None:
        _, sep, rest = partition_name.partition("__legacy_")
        if not sep:
            return None
        year, _, month = rest.partition("_")
        return Period(year=int(year), month=int(month))

    def get_boundaries(self, period: Period) -> tuple[str, str]:
        return (f"start of {period}", f"start of {period + 1}")


class _DecliningCalculator(_LegacyCalculator):
    """Carries ``period_at`` but never answers it, so the boundaries have to walk."""

    def period_at(self, instant: datetime) -> Period | None:
        return None


class _StartlessCalculator(_DecliningCalculator):
    """Declares ``period_start`` but never answers it either."""

    def period_start(self, period: Period) -> datetime | None:
        return None


class _NaiveDecodingCalculator(MonthPeriodCalculator):
    """Decodes every literal to the same naive instant."""

    def decode_boundary(self, literal: str) -> datetime | None:
        return datetime(2024, 6, 1)


class _ConfusedDecodingCalculator(MonthPeriodCalculator):
    """Decodes every literal to something that is not an instant at all."""

    def decode_boundary(self, literal: str) -> datetime | None:
        return "2024-06-01"  # type: ignore[return-value]


class _HexCodec:
    """A user-written codec: seconds since the epoch, in hex."""

    def encode(self, start: datetime, end: datetime) -> tuple[str, str]:
        return (format(int(start.timestamp()), "x"), format(int(end.timestamp()), "x"))

    def decode(self, literal: str) -> datetime | None:
        try:
            return datetime.fromtimestamp(int(literal, 16), tz=UTC)
        except ValueError:
            return None


def _millis_of(value: UUID) -> int:
    """Read the 48-bit Unix-millisecond prefix back out of a UUIDv7."""
    return value.int >> 80


def _instant_for_millis(millis: int) -> datetime:
    """A stand-in instant carrying ``millis``, past what datetime can express."""

    class _Beyond(datetime):
        def timestamp(self) -> float:
            return millis / 1000

    return _Beyond(2026, 8, 24, tzinfo=UTC)


# -- Window ------------------------------------------------------------------------------


def test__window__equality__ignores_the_token() -> None:
    # Arrange
    tagged = Window(start=1, end=2, token=Period(year=2024))
    bare = Window(start=1, end=2)

    # Act / Assert
    assert tagged == bare
    assert hash(tagged) == hash(bare)
    assert tagged.token == Period(year=2024)


def test__window__different_positions__not_equal() -> None:
    # Arrange / Act / Assert
    assert Window(start=1, end=2) != Window(start=1, end=3)
    assert Window(start=1, end=2) != Window(start=0, end=2)


def test__window__compared_with_something_else__not_equal() -> None:
    # Arrange
    window = Window(start=1, end=2)

    # Act / Assert
    assert window.__eq__("1..2") is NotImplemented
    assert window != "1..2"


def test__window__ordering__follows_the_start() -> None:
    # Arrange
    early = Window(start=1, end=10)
    late = Window(start=5, end=6)

    # Act / Assert
    assert early < late
    assert sorted([late, early]) == [early, late]


def test__window__usable_as_a_set_member_and_dict_key() -> None:
    # Arrange / Act
    members = {Window(start=1, end=2, token="a"), Window(start=1, end=2, token="b"), Window(start=2, end=3)}

    # Assert
    assert len(members) == 2


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        (Window(1, 3), Window(2, 4), True),
        (Window(1, 10), Window(3, 4), True),
        (Window(1, 2), Window(2, 3), False),
        (Window(2, 3), Window(1, 2), False),
        (Window(1, 2), Window(5, 6), False),
    ],
)
def test__window__overlaps__true_only_when_a_position_is_shared(left: Window, right: Window, expected: bool) -> None:
    # Arrange / Act / Assert -- half-open, so touching windows do not overlap
    assert left.overlaps(right) is expected
    assert right.overlaps(left) is expected


# -- UUIDv7 codec ----------------------------------------------------------------------------


def test__uuidv7_codec__encodes_an_instant__produces_a_valid_version_7_uuid() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()

    # Act
    value = codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))

    # Assert
    assert value.version == 7
    assert value.variant == "specified in RFC 4122"


def test__uuidv7_codec__encode__returns_the_min_uuid_strings_of_both_instants() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    start, end = datetime(2026, 8, 24, tzinfo=UTC), datetime(2026, 8, 31, tzinfo=UTC)

    # Act
    lower, upper = codec.encode(start, end)

    # Assert
    assert lower == str(codec.min_uuid_for(start))
    assert upper == str(codec.min_uuid_for(end))
    assert lower < upper


def test__uuidv7_codec__round_trip__recovers_the_instant() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    instant = datetime(2026, 8, 24, 13, 45, 12, tzinfo=UTC)

    # Act
    decoded = codec.decode(str(codec.min_uuid_for(instant)))

    # Assert
    assert decoded == instant


def test__uuidv7_codec__decode__tolerates_surrounding_whitespace() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    instant = datetime(2026, 8, 24, tzinfo=UTC)

    # Act / Assert
    assert codec.decode(f"  {codec.min_uuid_for(instant)}  ") == instant


def test__uuidv7_codec__same_instant_twice__is_deterministic() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    instant = datetime(2026, 8, 24, tzinfo=UTC)

    # Act / Assert
    assert codec.min_uuid_for(instant) == codec.min_uuid_for(instant)


def test__uuidv7_codec__ordering__matches_chronological_order() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    instants = [datetime(2026, m, 1, tzinfo=UTC) for m in (1, 5, 9, 12)]

    # Act
    encoded = [codec.min_uuid_for(i) for i in instants]

    # Assert
    assert encoded == sorted(encoded)


def test__uuidv7_codec__naive_datetime__read_as_utc() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()

    # Act / Assert
    assert codec.min_uuid_for(datetime(2026, 8, 24)) == codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))


def test__uuidv7_codec__sub_millisecond_instants__truncated_to_the_millisecond() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()

    # Act / Assert
    assert codec.min_uuid_for(datetime(2026, 8, 24, 0, 0, 0, 999, tzinfo=UTC)) == codec.min_uuid_for(
        datetime(2026, 8, 24, tzinfo=UTC)
    )


@pytest.mark.parametrize("literal", ["MAXVALUE", "MINVALUE", "2026-08-24", "not-a-uuid", "", "not-a-uuid-at-all"])
def test__uuidv7_codec__non_uuid_literal__decodes_to_none(literal: str) -> None:
    # Arrange / Act / Assert -- declining is what lets a mixed-history table still be introspected
    assert UUIDv7BoundaryCodec().decode(literal) is None


def test__uuidv7_codec__uuid_of_another_version__decodes_to_none() -> None:
    # Arrange -- a v4 UUID carries no timestamp
    # Act / Assert
    assert UUIDv7BoundaryCodec().decode("f81d4fae-7dec-41d0-a765-00a0c91e6bf6") is None


def test__uuidv7_codec__far_future_instant__clamped_instead_of_raising() -> None:
    # Arrange -- the 48-bit millisecond field runs out in the year 10889, well past datetime.max, so the
    # clamp needs an instant no datetime can express
    codec = UUIDv7BoundaryCodec()
    beyond = _instant_for_millis(UUIDv7BoundaryCodec._MAX_TIMESTAMP_MS + 86_400_000)

    # Act
    value = codec.min_uuid_for(beyond)

    # Assert -- clamped to the largest representable millisecond; wrapping would place the bound below
    # most real rows instead of above them
    assert value.version == 7
    assert _millis_of(value) == UUIDv7BoundaryCodec._MAX_TIMESTAMP_MS


def test__uuidv7_codec__instant_before_the_epoch__clamps_to_the_lowest_uuid() -> None:
    # Arrange -- a UUIDv7 timestamp field is unsigned, so a pre-1970 instant has no representation at all
    codec = UUIDv7BoundaryCodec()

    # Act
    encoded = codec.min_uuid_for(datetime(1969, 7, 20, tzinfo=UTC))

    # Assert
    assert encoded == codec.min_uuid_for(datetime(1970, 1, 1, tzinfo=UTC))


def test__uuidv7_codec__instant_inside_the_range__is_not_clamped() -> None:
    # Arrange -- year 9999 is only 253e12 ms, comfortably inside the 281e12 the field holds
    codec = UUIDv7BoundaryCodec()
    instant = datetime(9999, 12, 31, tzinfo=UTC)

    # Act
    encoded = codec.min_uuid_for(instant)

    # Assert
    assert _millis_of(encoded) == int(instant.timestamp() * 1000)
    assert str(encoded) > str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC)))


def test__uuidv7_codec__min_uuid_for__fills_every_bit_below_the_timestamp_with_zero() -> None:
    # Arrange -- "min" is what makes the bound safe: every real row in that millisecond has random bits at
    # or above these, so none sorts below it and falls into the previous partition
    codec = UUIDv7BoundaryCodec()

    # Act
    value = codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))

    # Assert
    assert value.int & ((1 << 76) - 1) == (0b10 << 62)
    assert value.version == 7
    assert value.variant == "specified in RFC 4122"


def test__uuidv7_codec__equality__every_instance_is_the_same_codec() -> None:
    # Arrange / Act / Assert
    assert UUIDv7BoundaryCodec() == UUIDv7BoundaryCodec()
    assert hash(UUIDv7BoundaryCodec()) == hash(UUIDv7BoundaryCodec())
    assert UUIDv7BoundaryCodec() != EpochBoundaryCodec()
    assert isinstance(UUIDv7BoundaryCodec(), RangeBoundaryCodec)


# -- Epoch codec -------------------------------------------------------------------------------


def test__epoch_codec__seconds__encodes_and_decodes_whole_seconds() -> None:
    # Arrange
    codec = EpochBoundaryCodec()
    start, end = datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC)

    # Act
    lower, upper = codec.encode(start, end)

    # Assert
    assert (lower, upper) == ("1704067200", "1706745600")
    assert codec.decode(lower) == start
    assert codec.decode(upper) == end
    assert codec.unit == "seconds"


def test__epoch_codec__milliseconds__scales_by_a_thousand() -> None:
    # Arrange
    codec = EpochBoundaryCodec("milliseconds")
    start, end = datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 0, 1, 500_000, tzinfo=UTC)

    # Act
    lower, upper = codec.encode(start, end)

    # Assert
    assert (lower, upper) == ("1704067200000", "1704067201500")
    assert codec.decode(upper) == end
    assert codec.unit == "milliseconds"


def test__epoch_codec__naive_instant__read_as_utc() -> None:
    # Arrange
    codec = EpochBoundaryCodec()

    # Act / Assert
    assert codec.encode(datetime(2024, 1, 1), datetime(2024, 1, 2)) == codec.encode(
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)
    )


def test__epoch_codec__invalid_unit__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="unit must be one of"):
        EpochBoundaryCodec("minutes")


@pytest.mark.parametrize("literal", ["MAXVALUE", "MINVALUE", "1.5", "2024-01-01", "", "1e3"])
def test__epoch_codec__non_integer_literal__decodes_to_none(literal: str) -> None:
    # Arrange / Act / Assert
    assert EpochBoundaryCodec().decode(literal) is None


def test__epoch_codec__integer_too_large_for_an_instant__decodes_to_none() -> None:
    # Arrange / Act / Assert
    assert EpochBoundaryCodec().decode("99999999999999999999") is None


def test__epoch_codec__decode__tolerates_surrounding_whitespace() -> None:
    # Arrange / Act / Assert
    assert EpochBoundaryCodec().decode(" 1704067200 ") == datetime(2024, 1, 1, tzinfo=UTC)


def test__epoch_codec__equality__depends_on_the_unit() -> None:
    # Arrange / Act / Assert
    assert EpochBoundaryCodec("seconds") == EpochBoundaryCodec("seconds")
    assert hash(EpochBoundaryCodec("seconds")) == hash(EpochBoundaryCodec("seconds"))
    assert EpochBoundaryCodec("seconds") != EpochBoundaryCodec("milliseconds")
    assert EpochBoundaryCodec() != UUIDv7BoundaryCodec()


# -- codec registry -------------------------------------------------------------------------


def test__codec_names__lists_the_built_in_codecs_in_order() -> None:
    # Arrange / Act / Assert
    assert codec_names() == ("epoch_milliseconds", "epoch_seconds", "uuidv7")


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("uuidv7", UUIDv7BoundaryCodec()),
        ("epoch_seconds", EpochBoundaryCodec("seconds")),
        ("epoch_milliseconds", EpochBoundaryCodec("milliseconds")),
    ],
)
def test__resolve_codec__known_name__returns_that_codec(name: str, expected: object) -> None:
    # Arrange / Act / Assert
    assert resolve_codec(name) == expected


def test__resolve_codec__instance__passed_through_untouched() -> None:
    # Arrange
    codec = _HexCodec()

    # Act / Assert
    assert resolve_codec(codec) is codec
    assert resolve_codec(None) is None


def test__resolve_codec__unknown_name__lists_the_known_ones() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match=r"Unknown boundary codec 'ulid'; known names: \['epoch_milliseconds'"):
        resolve_codec("ulid")


def test__resolve_codec__object_without_encode_and_decode__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="boundary codec must implement encode\\(\\)/decode\\(\\), got object"):
        resolve_codec(object())


# -- TimeBoundaries: construction ---------------------------------------------------------------


def test__time_boundaries__granularity__builds_the_matching_calculator() -> None:
    # Arrange / Act
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH)

    # Assert
    assert isinstance(boundaries.period_calculator, MonthPeriodCalculator)
    assert boundaries.kind == "time"
    assert boundaries.axis is Axis.TIME
    assert boundaries.cursor_source is CursorSource.CLOCK
    assert boundaries.tz is UTC
    assert boundaries.codec is None
    assert boundaries.calculator is None


def test__time_boundaries__granularity_as_a_string__accepted() -> None:
    # Arrange / Act / Assert
    assert TimeBoundaries(granularity="week").granularity is PartitionGranularity.WEEK


def test__time_boundaries__calculator__used_as_given() -> None:
    # Arrange
    calculator = WeekPeriodCalculator(tz=_MOSCOW)

    # Act
    boundaries = TimeBoundaries(calculator=calculator)

    # Assert
    assert boundaries.period_calculator is calculator
    assert boundaries.granularity is None


def test__time_boundaries__granularity_and_calculator__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="either a granularity or a calculator, not both"):
        TimeBoundaries(granularity=PartitionGranularity.MONTH, calculator=MonthPeriodCalculator())


def test__time_boundaries__neither_granularity_nor_calculator__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="needs either a granularity or a calculator"):
        TimeBoundaries()


def test__time_boundaries__calculator_that_is_not_one__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="calculator must implement PeriodCalculator, got object"):
        TimeBoundaries(calculator=object())


def test__time_boundaries__foreign_kind__rejected() -> None:
    # Arrange / Act / Assert -- a discriminator smuggled in from serialized data
    with pytest.raises(ValidationError, match=r"TimeBoundaries.kind must be 'time', got 'integer'"):
        TimeBoundaries(kind="integer", granularity=PartitionGranularity.MONTH)


@pytest.mark.parametrize(
    ("tz", "expected"),
    [("Europe/Moscow", _MOSCOW), (_MOSCOW, _MOSCOW), ("UTC", UTC), ("utc", UTC), (UTC, UTC)],
    ids=["iana-name", "zoneinfo", "UTC-name", "utc-lowercase", "datetime.UTC"],
)
def test__time_boundaries__tz__accepts_a_name_or_a_tzinfo(tz: object, expected: object) -> None:
    # Arrange / Act
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY, tz=tz)

    # Assert
    assert boundaries.tz is expected
    assert boundaries.timezone is expected


def test__time_boundaries__tz_that_is_neither__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="tz must be a tzinfo or an IANA name, got int"):
        TimeBoundaries(granularity=PartitionGranularity.DAY, tz=3)


def test__time_boundaries__unknown_iana_name__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ZoneInfoNotFoundError):
        TimeBoundaries(granularity=PartitionGranularity.DAY, tz="Mars/Olympus_Mons")


def test__time_boundaries__fixed_offset_tz__rejected_by_the_calculator() -> None:
    # Arrange / Act / Assert -- no IANA name, nothing usable in SET LOCAL TIME ZONE
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY, tz=timezone(timedelta(hours=3)))
    with pytest.raises(ValueError, match="Unsupported timezone"):
        _ = boundaries.period_calculator


@pytest.mark.parametrize(
    ("codec", "expected"),
    [
        ("uuidv7", UUIDv7BoundaryCodec()),
        (UUIDv7BoundaryCodec(), UUIDv7BoundaryCodec()),
        ("epoch_milliseconds", EpochBoundaryCodec("milliseconds")),
        (EpochBoundaryCodec("seconds"), EpochBoundaryCodec("seconds")),
    ],
)
def test__time_boundaries__codec__by_name_or_instance(codec: object, expected: object) -> None:
    # Arrange / Act
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=codec)

    # Assert
    assert boundaries.codec == expected
    assert boundaries.period_calculator.boundary_codec == expected  # type: ignore[attr-defined]


def test__time_boundaries__unknown_codec_name__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="Unknown boundary codec 'ulid'"):
        TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="ulid")


def test__time_boundaries__codec_that_is_not_one__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="boundary codec must implement"):
        TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=object())


def test__time_boundaries__is_frozen__assignment_rejected() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK)

    # Act / Assert
    with pytest.raises(ValidationError, match="frozen"):
        boundaries.granularity = PartitionGranularity.DAY  # type: ignore[misc]


def test__time_boundaries__satisfies_the_range_boundaries_protocol() -> None:
    # Arrange / Act / Assert
    assert isinstance(TimeBoundaries(granularity=PartitionGranularity.WEEK), RangeBoundaries)


# -- TimeBoundaries: windows -----------------------------------------------------------------


def test__time_boundaries__window_at_instant__is_the_period_holding_it() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH)

    # Act
    window = boundaries.window_at(datetime(2024, 3, 15, 12, tzinfo=UTC))

    # Assert
    assert window.start == datetime(2024, 3, 1, tzinfo=UTC)
    assert window.end == datetime(2024, 4, 1, tzinfo=UTC)
    assert window.token == Period(year=2024, month=3)


def test__time_boundaries__window_at_in_a_business_timezone__periods_start_at_local_midnight() -> None:
    # Arrange -- 23:30 UTC on 31 March is already 02:30 on 1 April in Moscow
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH, tz="Europe/Moscow")

    # Act
    window = boundaries.window_at(datetime(2024, 3, 31, 23, 30, tzinfo=UTC))

    # Assert
    assert window.token == Period(year=2024, month=4)
    assert window.start == datetime(2024, 4, 1, tzinfo=_MOSCOW)
    assert window.start == datetime(2024, 3, 31, 21, tzinfo=UTC)
    assert window.end == datetime(2024, 5, 1, tzinfo=_MOSCOW)


def test__time_boundaries__window_at_naive_instant__read_as_utc() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH, tz="Europe/Moscow")

    # Act / Assert
    assert boundaries.window_at(datetime(2024, 3, 31, 23, 30)) == boundaries.window_at(
        datetime(2024, 3, 31, 23, 30, tzinfo=UTC)
    )


@freeze_time("2024-03-15 12:00:00")
def test__time_boundaries__window_at_none__is_the_current_period() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY)

    # Act
    window = boundaries.window_at(None)

    # Assert
    assert window.token == Period(year=2024, month=3, day=15)
    assert window == boundaries.window_at(datetime(2024, 3, 15, 23, 59, tzinfo=UTC))


def test__time_boundaries__window_for__spans_exactly_one_period() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK)

    # Act
    window = boundaries.window_for(Period(year=2026, week=35))

    # Assert
    assert window == Window(datetime(2026, 8, 24, tzinfo=UTC), datetime(2026, 8, 31, tzinfo=UTC))
    assert window.token == Period(year=2026, week=35)


@pytest.mark.parametrize(("offset", "expected"), [(1, Period(year=2024, month=4)), (-3, Period(year=2023, month=12))])
def test__time_boundaries__shift__moves_by_whole_periods(offset: int, expected: Period) -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH)
    window = boundaries.window_at(datetime(2024, 3, 15, tzinfo=UTC))

    # Act
    shifted = boundaries.shift(window, offset)

    # Assert
    assert shifted.token == expected
    assert shifted == boundaries.window_for(expected)


def test__time_boundaries__catalog_window_without_token__resolved_by_its_start() -> None:
    # Arrange -- a window read back from the catalog carries no period
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH)
    raw = Window(datetime(2024, 3, 1, tzinfo=UTC), datetime(2024, 4, 1, tzinfo=UTC))

    # Act / Assert
    assert boundaries.shift(raw, 1).token == Period(year=2024, month=4)
    assert boundaries.describe(raw) == "2024_03"
    assert boundaries.child_name("events", raw) == "events__2024_03"
    assert boundaries.literals(raw) == ("2024-03-01", "2024-04-01")


def test__time_boundaries__describe__renders_the_period() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.QUARTER)

    # Act / Assert
    assert boundaries.describe(boundaries.window_at(datetime(2024, 8, 1, tzinfo=UTC))) == "2024_q3"


def test__time_boundaries__child_name__round_trips_through_parse_child_name() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK)
    window = boundaries.window_at(datetime(2026, 8, 26, tzinfo=UTC))

    # Act
    name = boundaries.child_name("events", window)

    # Assert
    assert name == "events__2026_w35"
    assert boundaries.parse_child_name(name) == window
    assert boundaries.parse_child_name("events__2026_08") is None
    assert boundaries.parse_child_name("events") is None


def test__time_boundaries__literals_without_codec__are_the_calendar_literals() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY)

    # Act / Assert
    assert boundaries.literals(boundaries.window_at(datetime(2024, 2, 29, tzinfo=UTC))) == ("2024-02-29", "2024-03-01")


def test__time_boundaries__literals_with_uuidv7_codec__are_min_uuids_of_the_period_bounds() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=codec)
    window = boundaries.window_at(datetime(2026, 8, 24, tzinfo=UTC))

    # Act
    lower, upper = boundaries.literals(window)

    # Assert
    assert lower == str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC)))
    assert upper == str(codec.min_uuid_for(datetime(2026, 8, 31, tzinfo=UTC)))


def test__time_boundaries__literals_with_epoch_codec__are_epoch_integers() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY, codec="epoch_seconds")

    # Act / Assert
    assert boundaries.literals(boundaries.window_at(datetime(2024, 1, 1, tzinfo=UTC))) == ("1704067200", "1704153600")


def test__time_boundaries__literals_with_codec_in_a_business_timezone__encode_the_local_period_start() -> None:
    # Arrange -- Monday 00:00 in Berlin is 22:00 Sunday UTC; encoding the UTC midnight instead would
    # misroute every row in those two hours
    berlin = ZoneInfo("Europe/Berlin")
    codec = UUIDv7BoundaryCodec()
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK, tz=berlin, codec=codec)

    # Act
    lower, _ = boundaries.literals(boundaries.window_for(Period(year=2026, week=35)))

    # Assert
    assert codec.decode(lower) == datetime(2026, 8, 24, tzinfo=berlin).astimezone(UTC)


@pytest.mark.parametrize(
    ("period", "start", "end", "real_hours"),
    [
        # Berlin skips 02:00 on 29 March 2026: that local day lasts 23 hours.
        (
            Period(year=2026, month=3, day=29),
            datetime(2026, 3, 28, 23, tzinfo=UTC),
            datetime(2026, 3, 29, 22, tzinfo=UTC),
            23,
        ),
        # It repeats 02:00 on 25 October 2026: that one lasts 25.
        (
            Period(year=2026, month=10, day=25),
            datetime(2026, 10, 24, 22, tzinfo=UTC),
            datetime(2026, 10, 25, 23, tzinfo=UTC),
            25,
        ),
    ],
)
def test__time_boundaries__window_over_a_clock_change__keeps_local_midnights(
    period: Period, start: datetime, end: datetime, real_hours: int
) -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY, tz=_BERLIN)

    # Act
    window = boundaries.window_for(period)

    # Assert -- both ends are local midnight; only their instants show the day is not 24 hours.
    # Subtracting two datetimes that share one tzinfo compares wall clocks, so convert first.
    assert window.start.astimezone(UTC) == start
    assert window.end.astimezone(UTC) == end
    assert window.end.astimezone(UTC) - window.start.astimezone(UTC) == timedelta(hours=real_hours)


@pytest.mark.parametrize(
    ("instant", "expected"),
    [
        (datetime(2026, 3, 29, 21, 30, tzinfo=UTC), Period(year=2026, month=3, day=29)),  # 23:30, summer time
        (datetime(2026, 3, 29, 22, 30, tzinfo=UTC), Period(year=2026, month=3, day=30)),  # 00:30 the next day
        (datetime(2026, 10, 25, 22, 30, tzinfo=UTC), Period(year=2026, month=10, day=25)),  # 23:30, winter time
        (datetime(2026, 10, 25, 23, 30, tzinfo=UTC), Period(year=2026, month=10, day=26)),
    ],
)
def test__time_boundaries__window_at_around_a_clock_change__is_the_local_day(
    instant: datetime, expected: Period
) -> None:
    # Arrange -- the last hour of a Berlin day is a different UTC hour in summer and in winter
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY, tz=_BERLIN)

    # Act / Assert
    assert boundaries.window_at(instant).token == expected


def test__time_boundaries__codec_over_a_clock_change__encodes_the_shifted_instants_contiguously() -> None:
    # Arrange -- an encoded key must follow the same local calendar the bounds do, or the rows
    # written in the hour the clocks moved land outside the partition meant for them
    codec = UUIDv7BoundaryCodec()
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY, tz=_BERLIN, codec=codec)

    # Act
    before = boundaries.literals(boundaries.window_for(Period(year=2026, month=3, day=28)))
    across = boundaries.literals(boundaries.window_for(Period(year=2026, month=3, day=29)))

    # Assert -- no gap over the transition, and both ends are the local midnights in UTC
    assert before[1] == across[0]
    assert codec.decode(across[0]) == datetime(2026, 3, 28, 23, tzinfo=UTC)
    assert codec.decode(across[1]) == datetime(2026, 3, 29, 22, tzinfo=UTC)


def test__time_boundaries__adjacent_windows__literals_are_contiguous() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK, codec="uuidv7")
    window = boundaries.window_for(Period(year=2026, week=35))

    # Act
    _, upper = boundaries.literals(window)
    next_lower, _ = boundaries.literals(boundaries.shift(window, 1))

    # Assert -- no gap, no overlap
    assert upper == next_lower


def test__time_boundaries__decode_without_codec__reads_a_calendar_literal_in_the_zone() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH, tz="Europe/Moscow")

    # Act / Assert
    assert boundaries.decode("2024-04-01") == datetime(2024, 3, 31, 21, tzinfo=UTC)
    assert boundaries.decode("2024-04-01 00:00:00+00") == datetime(2024, 4, 1, tzinfo=UTC)
    assert boundaries.decode("MAXVALUE") is None
    assert boundaries.decode("MINVALUE") is None


def test__time_boundaries__decode_with_codec__reads_the_codecs_literal_and_nothing_else() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK, codec=codec)

    # Act / Assert
    assert boundaries.decode(str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC)))) == datetime(
        2026, 8, 24, tzinfo=UTC
    )
    assert boundaries.decode("2026-08-24") is None
    assert boundaries.decode("MAXVALUE") is None


def test__time_boundaries__decode__round_trips_its_own_literals() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY, codec="epoch_milliseconds")
    window = boundaries.window_at(datetime(2024, 6, 15, tzinfo=UTC))

    # Act
    lower, upper = boundaries.literals(window)

    # Assert
    assert boundaries.decode(lower) == window.start
    assert boundaries.decode(upper) == window.end


def test__time_boundaries__calculator_decoding_to_a_naive_instant__read_as_utc() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=_NaiveDecodingCalculator())

    # Act / Assert
    assert boundaries.decode("anything") == datetime(2024, 6, 1, tzinfo=UTC)


def test__time_boundaries__calculator_decoding_to_a_non_instant__is_none() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=_ConfusedDecodingCalculator())

    # Act / Assert
    assert boundaries.decode("2024-06-01") is None


def test__time_boundaries__custom_codec_object__used_for_literals_and_decoding() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.DAY, codec=_HexCodec())
    window = boundaries.window_at(datetime(2024, 1, 1, tzinfo=UTC))

    # Act
    lower, upper = boundaries.literals(window)

    # Assert
    assert (lower, upper) == (format(int(window.start.timestamp()), "x"), format(int(window.end.timestamp()), "x"))
    assert lower == "65920080"
    assert boundaries.decode(lower) == window.start
    assert boundaries.decode(upper) == window.end


# -- TimeBoundaries: calculator plumbing ---------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "timezone_", "name"),
    [
        ({"granularity": PartitionGranularity.DAY}, UTC, "UTC"),
        ({"granularity": PartitionGranularity.DAY, "tz": "Europe/Moscow"}, _MOSCOW, "Europe/Moscow"),
        ({"calculator": DayPeriodCalculator(tz=_MOSCOW)}, _MOSCOW, "Europe/Moscow"),
        ({"calculator": _DecliningCalculator(Period(year=2024, month=6))}, UTC, None),
    ],
    ids=["utc", "moscow", "calculator-zone", "calculator-without-zone"],
)
def test__time_boundaries__timezone_and_name__come_from_the_calculator(
    kwargs: dict[str, object], timezone_: object, name: str | None
) -> None:
    # Arrange / Act
    boundaries = TimeBoundaries(**kwargs)

    # Assert
    assert boundaries.timezone is timezone_
    assert boundaries.timezone_name == name


def test__time_boundaries__calculator_given__tz_and_codec_fields_do_not_override_it() -> None:
    # Arrange -- a ready-made calculator carries its own zone and codec
    calculator = WeekPeriodCalculator(tz=_MOSCOW, boundary_codec=UUIDv7BoundaryCodec())

    # Act
    boundaries = TimeBoundaries(calculator=calculator, tz=UTC)

    # Assert
    assert boundaries.timezone is _MOSCOW
    lower, _ = boundaries.literals(boundaries.window_for(Period(year=2026, week=35)))
    assert UUIDv7BoundaryCodec().decode(lower) == datetime(2026, 8, 24, tzinfo=_MOSCOW).astimezone(UTC)


def test__time_boundaries__period_calculator__built_once_and_cached() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.MONTH, tz="Europe/Moscow", codec="uuidv7")

    # Act
    first = boundaries.period_calculator
    second = boundaries.period_calculator

    # Assert
    assert first is second
    assert isinstance(first, MonthPeriodCalculator)
    assert first.tz is _MOSCOW
    assert first.boundary_codec == UUIDv7BoundaryCodec()


def test__time_boundaries__cached_calculator__does_not_affect_equality_or_hash() -> None:
    # Arrange
    warmed = TimeBoundaries(granularity=PartitionGranularity.MONTH, tz="Europe/Moscow")
    _ = warmed.period_calculator
    fresh = TimeBoundaries(granularity=PartitionGranularity.MONTH, tz=_MOSCOW)

    # Act / Assert
    assert warmed == fresh
    assert hash(warmed) == hash(fresh)
    assert warmed.model_dump() == fresh.model_dump()


def test__time_boundaries__calculator_without_period_at__window_at_still_walks_to_the_period() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=_LegacyCalculator(Period(year=2024, month=6)))

    # Act / Assert
    assert boundaries.window_at(datetime(2024, 9, 10, tzinfo=UTC)).token == Period(year=2024, month=9)
    assert boundaries.window_at(datetime(2024, 2, 10, tzinfo=UTC)).token == Period(year=2024, month=2)


def test__time_boundaries__calculator_declining_period_at__walks_forward_from_the_current_period() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=_DecliningCalculator(Period(year=2024, month=6)))

    # Act
    window = boundaries.window_at(datetime(2024, 9, 10, tzinfo=UTC))

    # Assert -- and the period's start comes from Period.to_datetime, the calculator having no period_start
    assert window.token == Period(year=2024, month=9)
    assert window == Window(datetime(2024, 9, 1, tzinfo=UTC), datetime(2024, 10, 1, tzinfo=UTC))


def test__time_boundaries__calculator_declining_period_at__walks_backward_from_the_current_period() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=_DecliningCalculator(Period(year=2024, month=6)))

    # Act
    window = boundaries.window_at(datetime(2024, 2, 10, tzinfo=UTC))

    # Assert
    assert window.token == Period(year=2024, month=2)
    assert window == Window(datetime(2024, 2, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC))


def test__time_boundaries__calculator_declining_period_at__instant_inside_the_current_period_needs_no_walk() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=_DecliningCalculator(Period(year=2024, month=6)))

    # Act / Assert
    assert boundaries.window_at(datetime(2024, 6, 30, 23, 59, tzinfo=UTC)).token == Period(year=2024, month=6)
    assert boundaries.window_at(datetime(2024, 6, 1, tzinfo=UTC)).token == Period(year=2024, month=6)
    assert boundaries.window_at(None).token == Period(year=2024, month=6)


def test__time_boundaries__calculator_declining_period_start__windows_start_at_the_utc_period_start() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=_StartlessCalculator(Period(year=2024, month=6)))

    # Act
    window = boundaries.window_at(datetime(2024, 8, 10, tzinfo=UTC))

    # Assert
    assert window == Window(Period(year=2024, month=8).to_datetime(), Period(year=2024, month=9).to_datetime())
    assert window.token == Period(year=2024, month=8)


def test__time_boundaries__custom_calculator__names_literals_and_parses_through_it() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=_DecliningCalculator(Period(year=2024, month=6)))
    window = boundaries.window_for(Period(year=2024, month=6))

    # Act / Assert
    assert boundaries.child_name("events", window) == "events__legacy_2024_06"
    assert boundaries.parse_child_name("events__legacy_2024_06") == window
    assert boundaries.parse_child_name("events__2024_06") is None
    assert boundaries.literals(window) == ("start of 2024_06", "start of 2024_07")
    assert boundaries.decode("2024-06-01") == datetime(2024, 6, 1, tzinfo=UTC)


# -- TimeBoundaries: serialization ---------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (
            {"granularity": PartitionGranularity.MONTH},
            {"kind": "time", "granularity": "month", "tz": "UTC", "codec": None},
        ),
        (
            {"granularity": PartitionGranularity.WEEK, "tz": "Europe/Moscow", "codec": "uuidv7"},
            {"kind": "time", "granularity": "week", "tz": "Europe/Moscow", "codec": "uuidv7"},
        ),
        (
            {"granularity": PartitionGranularity.DAY, "codec": EpochBoundaryCodec("milliseconds")},
            {"kind": "time", "granularity": "day", "tz": "UTC", "codec": "epoch_milliseconds"},
        ),
        (
            {"granularity": PartitionGranularity.DAY, "codec": _HexCodec()},
            {"kind": "time", "granularity": "day", "tz": "UTC", "codec": "_HexCodec"},
        ),
    ],
    ids=["defaults", "zone-and-codec-by-name", "codec-instance", "custom-codec"],
)
def test__time_boundaries__dump__renders_zone_and_codec_by_name(kwargs: dict[str, object], expected: dict) -> None:
    # Arrange / Act / Assert
    assert TimeBoundaries(**kwargs).model_dump(mode="json") == expected


def test__time_boundaries__dump__leaves_the_calculator_out() -> None:
    # Arrange
    boundaries = TimeBoundaries(calculator=MonthPeriodCalculator())

    # Act
    dumped = boundaries.model_dump()

    # Assert
    assert "calculator" not in dumped
    assert dumped["granularity"] is None


def test__time_boundaries__json_round_trip__reloads_zone_and_codec() -> None:
    # Arrange
    boundaries = TimeBoundaries(granularity=PartitionGranularity.WEEK, tz="Europe/Moscow", codec="epoch_seconds")

    # Act
    reloaded = TimeBoundaries.model_validate_json(boundaries.model_dump_json())

    # Assert
    assert reloaded == boundaries
    assert reloaded.tz is _MOSCOW
    assert reloaded.codec == EpochBoundaryCodec("seconds")


# -- NumericBoundaries ------------------------------------------------------------------------


def test__numeric_boundaries__defaults__anchor_at_zero_and_read_the_max_key() -> None:
    # Arrange / Act
    boundaries = NumericBoundaries(step=100)

    # Assert
    assert boundaries.kind == "integer"
    assert boundaries.origin == 0
    assert boundaries.name_suffix == "__{start}"
    assert boundaries.cursor_source is CursorSource.MAX_KEY
    assert boundaries.axis is Axis.INTEGER
    assert isinstance(boundaries, RangeBoundaries)


@pytest.mark.parametrize(
    ("position", "expected"),
    [
        (250, Window(200, 300)),
        (200, Window(200, 300)),
        (299, Window(200, 300)),
        (0, Window(0, 100)),
        (-1, Window(-100, 0)),
        (-100, Window(-100, 0)),
        (-101, Window(-200, -100)),
        (None, Window(0, 100)),
        ("250", Window(200, 300)),
    ],
)
def test__numeric_boundaries__window_at__is_the_step_holding_the_position(position: object, expected: Window) -> None:
    # Arrange / Act
    window = NumericBoundaries(step=100).window_at(position)

    # Assert
    assert window == expected
    assert window.token is None


@pytest.mark.parametrize(
    ("position", "expected"),
    [(120, Window(50, 150)), (50, Window(50, 150)), (49, Window(-50, 50)), (None, Window(50, 150))],
)
def test__numeric_boundaries__window_at_with_origin__grid_is_anchored_on_it(position: object, expected: Window) -> None:
    # Arrange / Act / Assert
    assert NumericBoundaries(step=100, origin=50).window_at(position) == expected


@pytest.mark.parametrize(("offset", "expected"), [(1, Window(300, 400)), (-3, Window(-100, 0)), (0, Window(200, 300))])
def test__numeric_boundaries__shift__moves_by_whole_steps(offset: int, expected: Window) -> None:
    # Arrange
    boundaries = NumericBoundaries(step=100)

    # Act / Assert
    assert boundaries.shift(Window(200, 300), offset) == expected


def test__numeric_boundaries__literals__are_plain_integers() -> None:
    # Arrange / Act / Assert
    assert NumericBoundaries(step=100).literals(Window(200, 300)) == ("200", "300")
    assert NumericBoundaries(step=100).literals(Window(-100, 0)) == ("-100", "0")


@pytest.mark.parametrize(
    ("literal", "expected"),
    [("300", 300), ("-300", -300), (" 12 ", 12), ("'300'", None), ("abc", None), ("1.5", None), ("MAXVALUE", None)],
)
def test__numeric_boundaries__decode__reads_integers_and_declines_everything_else(
    literal: str, expected: int | None
) -> None:
    # Arrange / Act / Assert
    assert NumericBoundaries(step=100).decode(literal) == expected


@pytest.mark.parametrize(
    ("window", "expected"),
    [(Window(200, 300), "queue__200"), (Window(0, 100), "queue__0"), (Window(-100, 0), "queue__m100")],
)
def test__numeric_boundaries__child_name__spells_the_start_with_m_for_minus(window: Window, expected: str) -> None:
    # Arrange / Act / Assert
    assert NumericBoundaries(step=100).child_name("queue", window) == expected


def test__numeric_boundaries__custom_name_suffix__used_for_children() -> None:
    # Arrange
    boundaries = NumericBoundaries(step=100, name_suffix="_p{start}")

    # Act / Assert
    assert boundaries.child_name("queue", Window(200, 300)) == "queue_p200"
    assert boundaries.parse_child_name("queue_p200") == Window(200, 300)
    assert boundaries.parse_child_name("queue__200") is None


@pytest.mark.parametrize(
    ("relname", "expected"),
    [
        ("queue__200", Window(200, 300)),
        ("queue__0", Window(0, 100)),
        ("queue__m100", Window(-100, 0)),
        ("queue__250", None),
        ("queue__m50", None),
        ("queue__x", None),
        ("queue", None),
        ("queue__200_extra", None),
    ],
)
def test__numeric_boundaries__parse_child_name__reads_on_grid_starts_only(
    relname: str, expected: Window | None
) -> None:
    # Arrange / Act / Assert
    assert NumericBoundaries(step=100).parse_child_name(relname) == expected


def test__numeric_boundaries__parse_child_name_with_origin__grid_is_anchored_on_it() -> None:
    # Arrange
    boundaries = NumericBoundaries(step=100, origin=50)

    # Act / Assert
    assert boundaries.parse_child_name("queue__150") == Window(150, 250)
    assert boundaries.parse_child_name("queue__100") is None


def test__numeric_boundaries__child_name__round_trips_through_parse_child_name() -> None:
    # Arrange
    boundaries = NumericBoundaries(step=7, origin=-3)
    window = boundaries.window_at(-40)

    # Act / Assert
    assert boundaries.parse_child_name(boundaries.child_name("q", window)) == window


def test__numeric_boundaries__describe__renders_the_half_open_interval() -> None:
    # Arrange / Act / Assert
    assert NumericBoundaries(step=100).describe(Window(200, 300)) == "[200, 300)"


def test__numeric_boundaries__own_name_budget__sized_for_a_signed_19_digit_start() -> None:
    # Arrange / Act / Assert
    assert NumericBoundaries(step=100).own_name_budget() == len("__") + 1 + 19
    assert NumericBoundaries(step=100, name_suffix="_p{start}").own_name_budget() == len("_p") + 1 + 19


@pytest.mark.parametrize("suffix", ["_bucket", "{start}X", "{start}-", "__{remainder}", "{start}{start}"])
def test__numeric_boundaries__unsafe_name_suffix__rejected(suffix: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="start"):
        NumericBoundaries(step=100, name_suffix=suffix)


def test__numeric_boundaries__empty_name_suffix__rejected() -> None:
    # Arrange / Act / Assert -- an empty template could not tell two windows apart
    with pytest.raises(ValidationError, match="name_suffix"):
        NumericBoundaries(step=100, name_suffix="")


def test__numeric_boundaries__cursor_from_the_clock__rejected() -> None:
    # Arrange / Act / Assert -- an integer axis has no clock to read
    with pytest.raises(ValidationError, match="cannot read its cursor from the clock"):
        NumericBoundaries(step=100, cursor_source=CursorSource.CLOCK)


def test__numeric_boundaries__cursor_from_the_sequence__accepted() -> None:
    # Arrange / Act / Assert
    assert NumericBoundaries(step=100, cursor_source="sequence").cursor_source is CursorSource.SEQUENCE


@pytest.mark.parametrize("step", [0, -5])
def test__numeric_boundaries__non_positive_step__rejected(step: int) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="step"):
        NumericBoundaries(step=step)


def test__numeric_boundaries__foreign_kind__rejected() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match=r"NumericBoundaries.kind must be 'integer', got 'time'"):
        NumericBoundaries(step=100, kind="time")


def test__numeric_boundaries__json_round_trip__reloads_an_equal_rule() -> None:
    # Arrange
    boundaries = NumericBoundaries(step=100, origin=7, name_suffix="_p{start}", cursor_source=CursorSource.SEQUENCE)

    # Act
    dumped = boundaries.model_dump(mode="json")

    # Assert
    assert dumped == {
        "kind": "integer",
        "step": 100,
        "origin": 7,
        "name_suffix": "_p{start}",
        "cursor_source": "sequence",
    }
    assert NumericBoundaries.model_validate(dumped) == boundaries


# -- parse_boundaries ------------------------------------------------------------------------


def test__parse_boundaries__time_dict__builds_time_boundaries() -> None:
    # Arrange / Act
    parsed = parse_boundaries({"kind": "time", "granularity": "month", "tz": "Europe/Moscow"})

    # Assert
    assert parsed == TimeBoundaries(granularity=PartitionGranularity.MONTH, tz=_MOSCOW)


def test__parse_boundaries__dict_without_kind__defaults_to_time() -> None:
    # Arrange / Act / Assert
    assert parse_boundaries({"granularity": "day"}) == TimeBoundaries(granularity=PartitionGranularity.DAY)


def test__parse_boundaries__integer_dict__builds_numeric_boundaries() -> None:
    # Arrange / Act / Assert
    assert parse_boundaries({"kind": "integer", "step": 100}) == NumericBoundaries(step=100)


def test__parse_boundaries__unknown_kind__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="Unknown boundaries kind 'geo'; expected 'time', 'integer' or 'sequence'"):
        parse_boundaries({"kind": "geo"})


@pytest.mark.parametrize(
    "boundaries", [TimeBoundaries(granularity=PartitionGranularity.DAY), NumericBoundaries(step=10)]
)
def test__parse_boundaries__instance__passed_through_untouched(boundaries: object) -> None:
    # Arrange / Act / Assert
    assert parse_boundaries(boundaries) is boundaries


def test__parse_boundaries__object_that_is_not_boundaries__raises_type_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError, match="boundaries must implement RangeBoundaries, got str"):
        parse_boundaries("month")
