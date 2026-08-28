"""Encoding periods into a partition key that is not a timestamp."""

from datetime import UTC, datetime
from uuid import UUID

import pytest

from pg_partsmith.boundaries import UUIDv7BoundaryCodec
from pg_partsmith.entities import Period
from pg_partsmith.strategies import DayPeriodCalculator, WeekPeriodCalculator

# ── UUIDv7 boundary codec ───────────────────────────────────────────────────────


def _millis_of(value: UUID) -> int:
    """Read the 48-bit Unix-millisecond prefix back out of a UUIDv7."""
    return value.int >> 80


def _instant_for_millis(millis: int) -> datetime:
    """A stand-in instant carrying ``millis``, past what datetime can express."""

    class _Beyond(datetime):
        def timestamp(self) -> float:
            return millis / 1000

    return _Beyond(2026, 8, 24, tzinfo=UTC)


def test__uuidv7_codec__encodes_an_instant__produces_a_valid_version_7_uuid() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()

    # Act
    value = codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))

    # Assert
    assert value.version == 7
    assert value.variant == "specified in RFC 4122"


def test__uuidv7_codec__round_trip__recovers_the_instant() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    instant = datetime(2026, 8, 24, 13, 45, 12, tzinfo=UTC)

    # Act
    decoded = codec.decode(str(codec.min_uuid_for(instant)))

    # Assert
    assert decoded == instant


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


@pytest.mark.parametrize("literal", ["MAXVALUE", "MINVALUE", "2026-08-24", "not-a-uuid", ""])
def test__uuidv7_codec__non_uuid_literal__decodes_to_none(literal: str) -> None:
    # Arrange / Act / Assert
    assert UUIDv7BoundaryCodec().decode(literal) is None


def test__uuidv7_codec__uuid_of_another_version__decodes_to_none() -> None:
    # Arrange: a v4 UUID carries no timestamp.
    # Act / Assert
    assert UUIDv7BoundaryCodec().decode("f81d4fae-7dec-41d0-a765-00a0c91e6bf6") is None


def test__uuidv7_codec__far_future_instant__clamped_instead_of_raising() -> None:
    # Arrange -- the 48-bit millisecond field runs out in the year 10889, well
    # past datetime.max, so the clamp needs an instant no datetime can express.
    codec = UUIDv7BoundaryCodec()
    beyond = _instant_for_millis(UUIDv7BoundaryCodec._MAX_TIMESTAMP_MS + 86_400_000)

    # Act
    value = codec.min_uuid_for(beyond)

    # Assert -- clamped to the largest representable millisecond. Letting it
    # wrap would place the bound below most real rows instead of above them.
    assert value.version == 7
    assert _millis_of(value) == UUIDv7BoundaryCodec._MAX_TIMESTAMP_MS


# ── Calculators with a boundary codec ───────────────────────────────────────────


def test__week_calculator__without_codec__keeps_calendar_boundaries() -> None:
    # Arrange
    calculator = WeekPeriodCalculator()

    # Act / Assert
    assert calculator.get_boundaries(Period(year=2026, week=35)) == ("2026-08-24", "2026-08-31")


def test__week_calculator__with_uuidv7_codec__emits_uuid_boundaries() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    calculator = WeekPeriodCalculator(boundary_codec=codec)

    # Act
    lower, upper = calculator.get_boundaries(Period(year=2026, week=35))

    # Assert
    assert lower == str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC)))
    assert upper == str(codec.min_uuid_for(datetime(2026, 8, 31, tzinfo=UTC)))


def test__week_calculator__with_codec__adjacent_periods_are_contiguous() -> None:
    # Arrange
    calculator = WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())
    period = Period(year=2026, week=35)

    # Act
    _, upper = calculator.get_boundaries(period)
    next_lower, _ = calculator.get_boundaries(period + 1)

    # Assert: no gap, no overlap.
    assert upper == next_lower


def test__week_calculator__with_codec__partition_names_are_unchanged() -> None:
    # Arrange
    calculator = WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())

    # Act / Assert: the semantic period still decides the name.
    assert calculator.format_partition_name("events", Period(year=2026, week=35)) == "events__2026_w35"


def test__week_calculator__with_codec__decodes_its_own_boundaries() -> None:
    # Arrange
    calculator = WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())
    lower, _ = calculator.get_boundaries(Period(year=2026, week=35))

    # Act / Assert
    assert calculator.decode_boundary(lower) == datetime(2026, 8, 24, tzinfo=UTC)


def test__day_calculator__without_codec__decodes_timestamp_boundaries() -> None:
    # Arrange
    calculator = DayPeriodCalculator()

    # Act / Assert
    assert calculator.decode_boundary("2026-08-24") == datetime(2026, 8, 24, tzinfo=UTC)


def test__day_calculator__with_codec__boundaries_round_trip_through_periods() -> None:
    # Arrange
    calculator = DayPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())
    period = Period(year=2026, month=8, day=24)

    # Act
    lower, upper = calculator.get_boundaries(period)

    # Assert
    assert calculator.decode_boundary(lower) == datetime(2026, 8, 24, tzinfo=UTC)
    assert calculator.decode_boundary(upper) == datetime(2026, 8, 25, tzinfo=UTC)


def test__uuid7_codec__instant_before_the_epoch__clamps_to_the_lowest_uuid() -> None:
    # Arrange -- a UUIDv7 timestamp field is unsigned, so a pre-1970 instant has
    # no representation at all.
    codec = UUIDv7BoundaryCodec()

    # Act
    encoded = codec.min_uuid_for(datetime(1969, 7, 20, tzinfo=UTC))

    # Assert -- clamping keeps the bound sortable and below every real row,
    # where wrapping would place it above most of them.
    assert encoded == codec.min_uuid_for(datetime(1970, 1, 1, tzinfo=UTC))


def test__uuid7_codec__instant_inside_the_range__is_not_clamped() -> None:
    # Arrange -- year 9999 looks extreme but is only 253e12 ms, comfortably
    # inside the 281e12 the field holds. Nothing here should be clamped.
    codec = UUIDv7BoundaryCodec()
    instant = datetime(9999, 12, 31, tzinfo=UTC)

    # Act
    encoded = codec.min_uuid_for(instant)

    # Assert -- the millisecond survives intact, and the bound still sorts
    # above a present-day one.
    assert _millis_of(encoded) == int(instant.timestamp() * 1000)
    assert str(encoded) > str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC)))


def test__uuid7_codec__min_uuid_for__fills_every_bit_below_the_timestamp_with_zero() -> None:
    # Arrange -- "min" is what makes the bound safe: every real row in that
    # millisecond has random bits at or above these, so none sorts below it and
    # falls into the previous partition.
    codec = UUIDv7BoundaryCodec()

    # Act
    value = codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC))

    # Assert
    assert value.int & ((1 << 76) - 1) == (0b10 << 62)
    assert value.version == 7
    assert value.variant == "specified in RFC 4122"
