"""Encoding periods into a partition key that is not a timestamp."""

from datetime import UTC, datetime

import pytest

from pg_partsmith.boundaries import UUIDv7BoundaryCodec
from pg_partsmith.entities import Period
from pg_partsmith.strategies import DayPeriodCalculator, WeekPeriodCalculator

# ── UUIDv7 boundary codec ───────────────────────────────────────────────────────


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
    # Arrange
    codec = UUIDv7BoundaryCodec()

    # Act
    value = codec.min_uuid_for(datetime(9999, 12, 31, tzinfo=UTC))

    # Assert
    assert value.version == 7


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
