"""Period calculators: naming, boundaries, parsing and the calendar in a timezone."""

from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from freezegun import freeze_time

from pg_partsmith.boundaries import EpochBoundaryCodec, UUIDv7BoundaryCodec
from pg_partsmith.periods import PartitionGranularity, Period
from pg_partsmith.protocols import BoundaryDecoder, PeriodCalculator, TimezoneAwareCalculator
from pg_partsmith.strategies import (
    BasePeriodCalculator,
    DayPeriodCalculator,
    HourPeriodCalculator,
    MonthPeriodCalculator,
    QuarterPeriodCalculator,
    WeekPeriodCalculator,
    YearPeriodCalculator,
    get_period_calculator,
)

_MOSCOW = ZoneInfo("Europe/Moscow")
_BERLIN = ZoneInfo("Europe/Berlin")  # a zone that changes its clocks twice a year
_ALL_CALCULATORS = [
    HourPeriodCalculator,
    DayPeriodCalculator,
    WeekPeriodCalculator,
    MonthPeriodCalculator,
    QuarterPeriodCalculator,
    YearPeriodCalculator,
]
_LOCAL_CALCULATORS = [
    DayPeriodCalculator,
    WeekPeriodCalculator,
    MonthPeriodCalculator,
    QuarterPeriodCalculator,
    YearPeriodCalculator,
]

# -- MonthPeriodCalculator ---------------------------------------------------------------


def test__month_calculator__format__produces_table_double_underscore_year_month() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act
    name = calc.format_partition_name("events", Period(year=2024, month=3))

    # Assert
    assert name == "events__2024_03"


def test__month_calculator__format_without_month__raises_value_error() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Month is required"):
        calc.format_partition_name("events", Period(year=2024))


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("events__2024_03", Period(year=2024, month=3)),
        ("events__2024_12", Period(year=2024, month=12)),
        ("public_events__2024_01", Period(year=2024, month=1)),
    ],
)
def test__month_calculator__parse_valid_name__returns_period(name: str, expected: Period) -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) == expected


@pytest.mark.parametrize("name", ["invalid", "events__2024", "events__2024_13", "events__2024_00", "events_2024_03"])
def test__month_calculator__parse_invalid_name__returns_none(name: str) -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) is None


def test__month_calculator__get_boundaries__returns_first_day_of_month_and_next() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, month=3))

    # Assert
    assert from_val == "2024-03-01"
    assert to_val == "2024-04-01"


def test__month_calculator__get_boundaries_december__wraps_to_january_next_year() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, month=12))

    # Assert
    assert from_val == "2024-12-01"
    assert to_val == "2025-01-01"


def test__month_calculator__get_boundaries_without_month__raises_value_error() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Month is required"):
        calc.get_boundaries(Period(year=2024))


@freeze_time("2024-03-15")
def test__month_calculator__current_period__returns_march_2024() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act
    p = calc.current_period()

    # Assert
    assert p == Period(year=2024, month=3)


@freeze_time("2024-03-15")
def test__month_calculator__next_periods_3__returns_march_april_may() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act
    periods = calc.next_periods(3)

    # Assert
    assert periods == [Period(year=2024, month=3), Period(year=2024, month=4), Period(year=2024, month=5)]


def test__month_calculator__next_periods_zero__raises_value_error() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="positive"):
        calc.next_periods(0)


def test__month_calculator__period_before__subtracts_offset() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    assert calc.period_before(Period(year=2024, month=3), 2) == Period(year=2024, month=1)


def test__month_calculator__period_before_crosses_year__wraps_to_previous_year() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    assert calc.period_before(Period(year=2025, month=1), 2) == Period(year=2024, month=11)


def test__month_calculator__period_after__adds_offset() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    assert calc.period_after(Period(year=2024, month=11), 2) == Period(year=2025, month=1)
    assert calc.period_after(Period(year=2024, month=11), 0) == Period(year=2024, month=11)


@pytest.mark.parametrize("method", ["period_before", "period_after"])
def test__calculator__negative_offset__raises_value_error(method: str) -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Offset must be non-negative"):
        getattr(calc, method)(Period(year=2024, month=3), -1)


# -- DayPeriodCalculator -------------------------------------------------------------------


def test__day_calculator__format__produces_table_double_underscore_year_month_day() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act
    name = calc.format_partition_name("events", Period(year=2024, month=3, day=5))

    # Assert
    assert name == "events__2024_03_05"


def test__day_calculator__format_without_day__raises_value_error() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Month and day are required"):
        calc.format_partition_name("events", Period(year=2024, month=3))


def test__day_calculator__parse_valid_name__returns_period() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name("events__2024_03_15") == Period(year=2024, month=3, day=15)


@pytest.mark.parametrize("name", ["events__2024_03", "events__2024_02_30", "events__2024_03_15_07"])
def test__day_calculator__parse_invalid_name__returns_none(name: str) -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) is None


def test__day_calculator__get_boundaries__returns_day_and_next_day() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, month=3, day=15))

    # Assert
    assert from_val == "2024-03-15"
    assert to_val == "2024-03-16"


def test__day_calculator__get_boundaries_month_end__wraps_to_next_month() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, month=1, day=31))

    # Assert
    assert from_val == "2024-01-31"
    assert to_val == "2024-02-01"


def test__day_calculator__get_boundaries_without_day__raises_value_error() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Month and day are required"):
        calc.get_boundaries(Period(year=2024, month=3))


@freeze_time("2024-03-15")
def test__day_calculator__current_period__returns_march_15_2024() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act / Assert
    assert calc.current_period() == Period(year=2024, month=3, day=15)


@freeze_time("2024-03-15")
def test__day_calculator__next_periods_3__returns_consecutive_days() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act
    periods = calc.next_periods(3)

    # Assert
    assert periods == [
        Period(year=2024, month=3, day=15),
        Period(year=2024, month=3, day=16),
        Period(year=2024, month=3, day=17),
    ]


# -- WeekPeriodCalculator ------------------------------------------------------------------


def test__week_calculator__format__produces_table_double_underscore_year_lowercase_w_week() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act
    name = calc.format_partition_name("events", Period(year=2024, week=12))

    # Assert
    assert name == "events__2024_w12"


def test__week_calculator__format_without_week__raises_value_error() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Week is required"):
        calc.format_partition_name("events", Period(year=2024))


def test__week_calculator__parse_lowercase_w__returns_period() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name("events__2024_w12") == Period(year=2024, week=12)


def test__week_calculator__parse_uppercase_w__returns_none() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert -- uppercase W is not accepted
    assert calc.parse_partition_name("events__2024_W12") is None


@pytest.mark.parametrize("name", ["events__2024_03", "events__2024_w00", "events__2021_w53", "events__2024_w54"])
def test__week_calculator__parse_invalid_name__returns_none(name: str) -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) is None


def test__week_calculator__get_boundaries_week_1_2024__returns_correct_range() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, week=1))

    # Assert
    assert from_val == "2024-01-01"
    assert to_val == "2024-01-08"


def test__week_calculator__get_boundaries_last_week_of_a_long_year__crosses_into_the_next_year() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2020, week=53))

    # Assert
    assert from_val == "2020-12-28"
    assert to_val == "2021-01-04"


def test__week_calculator__get_boundaries_without_week__raises_value_error() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Week is required"):
        calc.get_boundaries(Period(year=2024))


@freeze_time("2024-03-18")  # Monday of ISO week 12
def test__week_calculator__current_period__returns_week_12_2024() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    assert calc.current_period() == Period(year=2024, week=12)


@freeze_time("2024-12-30")  # Monday of ISO week 1 of 2025
def test__week_calculator__current_period_in_the_first_iso_week__uses_the_iso_year() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    assert calc.current_period() == Period(year=2025, week=1)


@freeze_time("2024-01-01")
def test__week_calculator__next_periods_2__returns_consecutive_weeks() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act
    periods = calc.next_periods(2)

    # Assert
    assert periods == [Period(year=2024, week=1), Period(year=2024, week=2)]


# -- YearPeriodCalculator ------------------------------------------------------------------


def test__year_calculator__format__produces_table_double_underscore_year() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    assert calc.format_partition_name("events", Period(year=2024)) == "events__2024"


def test__year_calculator__parse_valid_name__returns_period() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name("events__2024") == Period(year=2024)


@pytest.mark.parametrize("name", ["events__2024_03", "invalid", "events__24"])
def test__year_calculator__parse_invalid_name__returns_none(name: str) -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) is None


def test__year_calculator__get_boundaries__returns_january_first_to_next_year() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    assert calc.get_boundaries(Period(year=2024)) == ("2024-01-01", "2025-01-01")


@freeze_time("2024-06-15")
def test__year_calculator__current_period__returns_2024() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    assert calc.current_period() == Period(year=2024)


@freeze_time("2024-01-01")
def test__year_calculator__next_periods_3__returns_2024_2025_2026() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act
    periods = calc.next_periods(3)

    # Assert
    assert [p.year for p in periods] == [2024, 2025, 2026]


@pytest.mark.parametrize("count", [0, -1])
def test__year_calculator__next_periods_non_positive__raises_value_error(count: int) -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="positive"):
        calc.next_periods(count)


def test__year_calculator__period_before__subtracts_offset_from_year() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    assert calc.period_before(Period(year=2024), 3) == Period(year=2021)


# -- HourPeriodCalculator ------------------------------------------------------------------


def test__hour_calculator__format__produces_table_double_underscore_year_month_day_hour() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act
    name = calc.format_partition_name("events", Period(year=2024, month=3, day=5, hour=7))

    # Assert
    assert name == "events__2024_03_05_07"


def test__hour_calculator__format_without_hour__raises_value_error() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Month, day and hour are required"):
        calc.format_partition_name("events", Period(year=2024, month=3, day=5))


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("events__2024_03_15_07", Period(year=2024, month=3, day=15, hour=7)),
        ("events__2024_12_31_23", Period(year=2024, month=12, day=31, hour=23)),
    ],
)
def test__hour_calculator__parse_valid_name__returns_period(name: str, expected: Period) -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) == expected


def test__hour_calculator__parse_day_style_name__returns_none() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act / Assert -- a day-granularity name must not parse as an hour period
    assert calc.parse_partition_name("t__2026_08_25") is None


@pytest.mark.parametrize("name", ["invalid", "events__2024_03_15_24", "events__2024_02_30_05", "events__2024_13_01_00"])
def test__hour_calculator__parse_invalid_name__returns_none(name: str) -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) is None


def test__hour_calculator__get_boundaries__returns_hour_and_next_hour() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, month=3, day=15, hour=7))

    # Assert
    assert from_val == "2024-03-15 07:00:00+00"
    assert to_val == "2024-03-15 08:00:00+00"


def test__hour_calculator__get_boundaries_last_hour_of_day__wraps_to_next_day_midnight() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, month=3, day=31, hour=23))

    # Assert
    assert from_val == "2024-03-31 23:00:00+00"
    assert to_val == "2024-04-01 00:00:00+00"


def test__hour_calculator__get_boundaries_without_hour__raises_value_error() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Month, day and hour are required"):
        calc.get_boundaries(Period(year=2024, month=3, day=15))


@freeze_time("2024-03-15 14:30:00")
def test__hour_calculator__current_period__returns_march_15_hour_14() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act / Assert
    assert calc.current_period() == Period(year=2024, month=3, day=15, hour=14)


@freeze_time("2024-03-15 22:30:00")
def test__hour_calculator__next_periods_3__crosses_midnight_into_next_day() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act
    periods = calc.next_periods(3)

    # Assert
    assert periods == [
        Period(year=2024, month=3, day=15, hour=22),
        Period(year=2024, month=3, day=15, hour=23),
        Period(year=2024, month=3, day=16, hour=0),
    ]


# -- QuarterPeriodCalculator ---------------------------------------------------------------


def test__quarter_calculator__format__produces_table_double_underscore_year_lowercase_q_quarter() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    assert calc.format_partition_name("events", Period(year=2024, quarter=2)) == "events__2024_q2"


def test__quarter_calculator__format_without_quarter__raises_value_error() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Quarter is required"):
        calc.format_partition_name("events", Period(year=2024))


@pytest.mark.parametrize(
    ("name", "expected"),
    [("events__2024_q1", Period(year=2024, quarter=1)), ("events__2024_q4", Period(year=2024, quarter=4))],
)
def test__quarter_calculator__parse_valid_name__returns_period(name: str, expected: Period) -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) == expected


@pytest.mark.parametrize(
    "name", ["invalid", "events__2024_03", "events__2024_q0", "events__2024_q5", "events__2024_Q1"]
)
def test__quarter_calculator__parse_invalid_name__returns_none(name: str) -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) is None


def test__quarter_calculator__get_boundaries__returns_first_day_of_quarter_and_next() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    assert calc.get_boundaries(Period(year=2024, quarter=2)) == ("2024-04-01", "2024-07-01")


def test__quarter_calculator__get_boundaries_q4__wraps_to_january_next_year() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    assert calc.get_boundaries(Period(year=2024, quarter=4)) == ("2024-10-01", "2025-01-01")


def test__quarter_calculator__get_boundaries_without_quarter__raises_value_error() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="Quarter is required"):
        calc.get_boundaries(Period(year=2024))


@freeze_time("2024-08-15")
def test__quarter_calculator__current_period__returns_q3_2024() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    assert calc.current_period() == Period(year=2024, quarter=3)


@freeze_time("2024-08-15")
def test__quarter_calculator__next_periods_3__returns_q3_q4_and_next_year_q1() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act
    periods = calc.next_periods(3)

    # Assert
    assert periods == [Period(year=2024, quarter=3), Period(year=2024, quarter=4), Period(year=2025, quarter=1)]


# -- get_period_calculator -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("granularity", "expected_cls"),
    [
        (PartitionGranularity.HOUR, HourPeriodCalculator),
        (PartitionGranularity.DAY, DayPeriodCalculator),
        (PartitionGranularity.WEEK, WeekPeriodCalculator),
        (PartitionGranularity.MONTH, MonthPeriodCalculator),
        (PartitionGranularity.QUARTER, QuarterPeriodCalculator),
        (PartitionGranularity.YEAR, YearPeriodCalculator),
    ],
)
def test__get_period_calculator__known_granularity__returns_correct_instance(
    granularity: PartitionGranularity, expected_cls: type[BasePeriodCalculator]
) -> None:
    # Arrange / Act
    calc = get_period_calculator(granularity)

    # Assert
    assert isinstance(calc, expected_cls)
    assert isinstance(calc, PeriodCalculator)
    assert isinstance(calc, TimezoneAwareCalculator)
    assert isinstance(calc, BoundaryDecoder)


def test__get_period_calculator__each_call__returns_a_fresh_instance() -> None:
    # Arrange / Act / Assert
    assert get_period_calculator(PartitionGranularity.DAY) is not get_period_calculator(PartitionGranularity.DAY)


def test__get_period_calculator__unknown_granularity__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="No calculator for granularity"):
        get_period_calculator("invalid")  # type: ignore[arg-type]


def test__get_period_calculator__default_tz__is_utc() -> None:
    # Arrange / Act
    calc = get_period_calculator(PartitionGranularity.DAY)

    # Assert
    assert calc.tz is UTC
    assert calc.timezone_name == "UTC"
    assert calc.boundary_codec is None


def test__get_period_calculator__moscow_tz__returns_calculator_in_that_zone() -> None:
    # Arrange / Act
    calc = get_period_calculator(PartitionGranularity.MONTH, tz=_MOSCOW)

    # Assert
    assert calc.tz is _MOSCOW
    assert calc.timezone_name == "Europe/Moscow"


def test__get_period_calculator__hour_with_moscow_tz__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="only UTC"):
        get_period_calculator(PartitionGranularity.HOUR, tz=_MOSCOW)


def test__get_period_calculator__boundary_codec__forwarded_to_the_calculator() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()

    # Act
    calc = get_period_calculator(PartitionGranularity.WEEK, boundary_codec=codec)

    # Assert
    assert calc.boundary_codec is codec


# -- timezone configuration ----------------------------------------------------------------


def test__calculator__default_tz__is_utc_named_utc() -> None:
    # Arrange / Act
    calc = MonthPeriodCalculator()

    # Assert
    assert calc.tz is UTC
    assert calc.timezone_name == "UTC"


def test__calculator__zoneinfo_tz__exposes_zone_and_iana_key() -> None:
    # Arrange / Act
    calc = MonthPeriodCalculator(tz=_MOSCOW)

    # Assert
    assert calc.tz is _MOSCOW
    assert calc.timezone_name == "Europe/Moscow"


@pytest.mark.parametrize("calculator_cls", _ALL_CALCULATORS)
def test__calculator__fixed_offset_tz__raises_value_error(calculator_cls: type[BasePeriodCalculator]) -> None:
    # Arrange / Act / Assert -- timezone(timedelta(...)) carries no IANA name usable in SET LOCAL TIME ZONE
    with pytest.raises(ValueError, match="Unsupported timezone"):
        calculator_cls(tz=timezone(timedelta(hours=3)))


def test__hour_calculator__moscow_tz__raises_value_error() -> None:
    # Arrange / Act / Assert -- local-time hour names are ambiguous under DST
    with pytest.raises(ValueError, match="only UTC"):
        HourPeriodCalculator(tz=_MOSCOW)


def test__hour_calculator__explicit_utc_tz__constructs() -> None:
    # Arrange / Act
    calc = HourPeriodCalculator(tz=UTC)

    # Assert
    assert calc.tz is UTC
    assert calc.timezone_name == "UTC"


# -- period_at -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("calculator_cls", "in_utc", "in_moscow"),
    [
        (DayPeriodCalculator, Period(year=2024, month=3, day=17), Period(year=2024, month=3, day=18)),
        (WeekPeriodCalculator, Period(year=2024, week=11), Period(year=2024, week=12)),
        (MonthPeriodCalculator, Period(year=2024, month=3), Period(year=2024, month=3)),
        (QuarterPeriodCalculator, Period(year=2024, quarter=1), Period(year=2024, quarter=1)),
        (YearPeriodCalculator, Period(year=2024), Period(year=2024)),
    ],
)
def test__calculator__period_at_sunday_night__is_monday_in_moscow(
    calculator_cls: type[BasePeriodCalculator], in_utc: Period, in_moscow: Period
) -> None:
    # Arrange -- Sunday 17 March 23:30 UTC is Monday 18 March 02:30 in Moscow (UTC+3)
    instant = datetime(2024, 3, 17, 23, 30, tzinfo=UTC)

    # Act / Assert
    assert calculator_cls().period_at(instant) == in_utc
    assert calculator_cls(tz=_MOSCOW).period_at(instant) == in_moscow


@pytest.mark.parametrize(
    ("calculator_cls", "instant", "in_utc", "in_moscow"),
    [
        (
            MonthPeriodCalculator,
            datetime(2024, 3, 31, 23, 30, tzinfo=UTC),
            Period(year=2024, month=3),
            Period(year=2024, month=4),
        ),
        (
            QuarterPeriodCalculator,
            datetime(2024, 3, 31, 23, 30, tzinfo=UTC),
            Period(year=2024, quarter=1),
            Period(year=2024, quarter=2),
        ),
        (YearPeriodCalculator, datetime(2024, 12, 31, 23, 30, tzinfo=UTC), Period(year=2024), Period(year=2025)),
        (
            DayPeriodCalculator,
            datetime(2024, 2, 29, 21, 0, tzinfo=UTC),
            Period(year=2024, month=2, day=29),
            Period(year=2024, month=3, day=1),
        ),
        (
            WeekPeriodCalculator,
            datetime(2024, 12, 29, 22, 0, tzinfo=UTC),
            Period(year=2024, week=52),
            Period(year=2025, week=1),
        ),
    ],
)
def test__calculator__period_at_late_evening__crosses_the_boundary_in_moscow_only(
    calculator_cls: type[BasePeriodCalculator], instant: datetime, in_utc: Period, in_moscow: Period
) -> None:
    # Arrange / Act / Assert
    assert calculator_cls().period_at(instant) == in_utc
    assert calculator_cls(tz=_MOSCOW).period_at(instant) == in_moscow


def test__hour_calculator__period_at__is_the_utc_hour() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act / Assert
    assert calc.period_at(datetime(2024, 3, 17, 23, 30, tzinfo=UTC)) == Period(year=2024, month=3, day=17, hour=23)
    assert calc.period_at(datetime(2024, 3, 18, 0, 0, tzinfo=UTC)) == Period(year=2024, month=3, day=18, hour=0)


@pytest.mark.parametrize("calculator_cls", _ALL_CALCULATORS)
def test__calculator__period_at_naive_instant__read_as_utc(calculator_cls: type[BasePeriodCalculator]) -> None:
    # Arrange
    calc = calculator_cls()
    aware = datetime(2024, 3, 17, 23, 30, tzinfo=UTC)

    # Act / Assert
    assert calc.period_at(aware.replace(tzinfo=None)) == calc.period_at(aware)


@pytest.mark.parametrize("calculator_cls", _LOCAL_CALCULATORS)
def test__calculator__period_at_offset_aware_instant__converted_into_the_calculators_zone(
    calculator_cls: type[BasePeriodCalculator],
) -> None:
    # Arrange -- 02:30 at UTC+3 on 18 March is 23:30 UTC on 17 March
    calc = calculator_cls()
    at_plus_three = datetime(2024, 3, 18, 2, 30, tzinfo=timezone(timedelta(hours=3)))

    # Act / Assert
    assert calc.period_at(at_plus_three) == calc.period_at(datetime(2024, 3, 17, 23, 30, tzinfo=UTC))


@pytest.mark.parametrize("calculator_cls", _LOCAL_CALCULATORS)
@pytest.mark.parametrize("tz", [UTC, _MOSCOW], ids=["utc", "moscow"])
@freeze_time("2024-03-17 23:30:00")
def test__calculator__current_period__is_period_at_now(
    calculator_cls: type[BasePeriodCalculator], tz: ZoneInfo
) -> None:
    # Arrange
    calc = calculator_cls(tz=tz)

    # Act / Assert
    assert calc.current_period() == calc.period_at(datetime.now(UTC))


@freeze_time("2024-03-17 23:30:00")
def test__hour_calculator__current_period__is_period_at_now() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act / Assert
    assert calc.current_period() == calc.period_at(datetime.now(UTC))
    assert calc.current_period() == Period(year=2024, month=3, day=17, hour=23)


# -- timezone-aware current periods ---------------------------------------------------------


@freeze_time("2024-03-31 23:30:00")
def test__month_calculator__current_period_utc_vs_moscow__crosses_month_boundary() -> None:
    # Arrange -- 23:30 UTC on Mar 31 is already 02:30 Apr 1 in Moscow (UTC+3)
    utc_calc = MonthPeriodCalculator()
    moscow_calc = MonthPeriodCalculator(tz=_MOSCOW)

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024, month=3)
    assert moscow_calc.current_period() == Period(year=2024, month=4)


@freeze_time("2024-03-15 23:30:00")
def test__day_calculator__current_period_utc_vs_moscow__crosses_day_boundary() -> None:
    # Arrange
    utc_calc = DayPeriodCalculator()
    moscow_calc = DayPeriodCalculator(tz=_MOSCOW)

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024, month=3, day=15)
    assert moscow_calc.current_period() == Period(year=2024, month=3, day=16)


@freeze_time("2024-03-17 23:30:00")  # Sunday of ISO week 11 in UTC
def test__week_calculator__current_period_utc_vs_moscow__crosses_iso_week_boundary() -> None:
    # Arrange -- Moscow (UTC+3) is already on Monday of ISO week 12
    utc_calc = WeekPeriodCalculator()
    moscow_calc = WeekPeriodCalculator(tz=_MOSCOW)

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024, week=11)
    assert moscow_calc.current_period() == Period(year=2024, week=12)


@freeze_time("2024-03-31 23:30:00")
def test__quarter_calculator__current_period_utc_vs_moscow__crosses_quarter_boundary() -> None:
    # Arrange
    utc_calc = QuarterPeriodCalculator()
    moscow_calc = QuarterPeriodCalculator(tz=_MOSCOW)

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024, quarter=1)
    assert moscow_calc.current_period() == Period(year=2024, quarter=2)


@freeze_time("2024-12-31 23:30:00")
def test__year_calculator__current_period_utc_vs_moscow__crosses_year_boundary() -> None:
    # Arrange
    utc_calc = YearPeriodCalculator()
    moscow_calc = YearPeriodCalculator(tz=_MOSCOW)

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024)
    assert moscow_calc.current_period() == Period(year=2025)


# -- period_start and boundary decoding -----------------------------------------------------


def test__calculator__period_start__is_local_midnight_in_the_calculators_zone() -> None:
    # Arrange
    calc = MonthPeriodCalculator(tz=_MOSCOW)

    # Act
    start = calc.period_start(Period(year=2024, month=4))

    # Assert
    assert start == datetime(2024, 4, 1, tzinfo=_MOSCOW)
    assert start.tzinfo is _MOSCOW
    assert start == datetime(2024, 3, 31, 21, tzinfo=UTC)


@pytest.mark.parametrize(
    ("period", "start", "next_start"),
    [
        # Berlin skips 02:00 on 29 March 2026 and repeats it on 25 October.
        (
            Period(year=2026, month=3, day=29),
            datetime(2026, 3, 28, 23, tzinfo=UTC),
            datetime(2026, 3, 29, 22, tzinfo=UTC),
        ),
        (
            Period(year=2026, month=10, day=25),
            datetime(2026, 10, 24, 22, tzinfo=UTC),
            datetime(2026, 10, 25, 23, tzinfo=UTC),
        ),
    ],
)
def test__day_calculator__period_start_across_a_clock_change__takes_the_offset_of_that_day(
    period: Period, start: datetime, next_start: datetime
) -> None:
    # Arrange
    calc = DayPeriodCalculator(tz=_BERLIN)

    # Act / Assert -- the two ends of one day carry different UTC offsets
    assert calc.period_start(period) == start
    assert calc.period_start(period + 1) == next_start

    # and the literals PostgreSQL will read under SET LOCAL TIME ZONE decode back to them
    lower, upper = calc.get_boundaries(period)
    assert calc.decode_boundary(lower) == start
    assert calc.decode_boundary(upper) == next_start


@pytest.mark.parametrize(
    ("instant", "expected"),
    [
        (datetime(2026, 3, 29, 21, 30, tzinfo=UTC), Period(year=2026, month=3, day=29)),  # 23:30, summer time
        (datetime(2026, 3, 29, 22, 30, tzinfo=UTC), Period(year=2026, month=3, day=30)),  # 00:30 the next day
        (datetime(2026, 10, 25, 22, 30, tzinfo=UTC), Period(year=2026, month=10, day=25)),  # 23:30, winter time
        (datetime(2026, 10, 25, 23, 30, tzinfo=UTC), Period(year=2026, month=10, day=26)),
    ],
)
def test__day_calculator__period_at_around_a_clock_change__follows_the_local_clock(
    instant: datetime, expected: Period
) -> None:
    # Arrange
    calc = DayPeriodCalculator(tz=_BERLIN)

    # Act / Assert
    assert calc.period_at(instant) == expected


def test__calculator__period_start_in_utc__matches_period_to_datetime() -> None:
    # Arrange
    calc = HourPeriodCalculator()
    period = Period(year=2024, month=3, day=15, hour=7)

    # Act / Assert
    assert calc.period_start(period) == period.to_datetime()


def test__calculator__decode_boundary_without_codec__reads_a_naive_literal_in_the_calculators_zone() -> None:
    # Arrange
    calc = MonthPeriodCalculator(tz=_MOSCOW)

    # Act / Assert
    assert calc.decode_boundary("2024-04-01") == datetime(2024, 3, 31, 21, tzinfo=UTC)
    assert calc.decode_boundary("2024-04-01 00:00:00+00") == datetime(2024, 4, 1, tzinfo=UTC)
    assert calc.decode_boundary("MAXVALUE") is None


def test__day_calculator__without_codec__decodes_timestamp_boundaries() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act / Assert
    assert calc.decode_boundary("2026-08-24") == datetime(2026, 8, 24, tzinfo=UTC)


# -- boundary codec integration -------------------------------------------------------------


def test__week_calculator__without_codec__keeps_calendar_boundaries() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    assert calc.get_boundaries(Period(year=2026, week=35)) == ("2026-08-24", "2026-08-31")


def test__week_calculator__with_uuidv7_codec__emits_uuid_boundaries() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()
    calc = WeekPeriodCalculator(boundary_codec=codec)

    # Act
    lower, upper = calc.get_boundaries(Period(year=2026, week=35))

    # Assert
    assert lower == str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC)))
    assert upper == str(codec.min_uuid_for(datetime(2026, 8, 31, tzinfo=UTC)))


def test__week_calculator__with_codec__adjacent_periods_are_contiguous() -> None:
    # Arrange
    calc = WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())
    period = Period(year=2026, week=35)

    # Act
    _, upper = calc.get_boundaries(period)
    next_lower, _ = calc.get_boundaries(period + 1)

    # Assert -- no gap, no overlap
    assert upper == next_lower


def test__week_calculator__with_codec__partition_names_are_unchanged() -> None:
    # Arrange
    calc = WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())

    # Act / Assert -- the semantic period still decides the name
    assert calc.format_partition_name("events", Period(year=2026, week=35)) == "events__2026_w35"


def test__week_calculator__with_codec__decodes_its_own_boundaries() -> None:
    # Arrange
    calc = WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())
    lower, _ = calc.get_boundaries(Period(year=2026, week=35))

    # Act / Assert
    assert calc.decode_boundary(lower) == datetime(2026, 8, 24, tzinfo=UTC)


def test__week_calculator__with_codec__declines_a_timestamp_literal() -> None:
    # Arrange -- with a codec, timestamp parsing is not a fallback: the literal is the codec's or nothing
    calc = WeekPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())

    # Act / Assert
    assert calc.decode_boundary("2026-08-24") is None


def test__day_calculator__with_codec__boundaries_round_trip_through_periods() -> None:
    # Arrange
    calc = DayPeriodCalculator(boundary_codec=UUIDv7BoundaryCodec())
    period = Period(year=2026, month=8, day=24)

    # Act
    lower, upper = calc.get_boundaries(period)

    # Assert
    assert calc.decode_boundary(lower) == datetime(2026, 8, 24, tzinfo=UTC)
    assert calc.decode_boundary(upper) == datetime(2026, 8, 25, tzinfo=UTC)


def test__hour_calculator__with_epoch_codec__emits_millisecond_boundaries() -> None:
    # Arrange
    calc = HourPeriodCalculator(boundary_codec=EpochBoundaryCodec("milliseconds"))

    # Act / Assert
    assert calc.get_boundaries(Period(year=2024, month=4, day=1, hour=3)) == ("1711940400000", "1711944000000")


def test__week_calculator__non_utc_timezone_with_a_codec__encodes_the_local_period_start() -> None:
    # Arrange -- Monday 00:00 in Berlin is 22:00 Sunday UTC; encoding the UTC instant instead would
    # misroute every row in those two hours
    berlin = ZoneInfo("Europe/Berlin")
    codec = UUIDv7BoundaryCodec()
    calc = WeekPeriodCalculator(tz=berlin, boundary_codec=codec)

    # Act
    lower, _upper = calc.get_boundaries(Period(year=2026, week=35))

    # Assert
    assert codec.decode(lower) == datetime(2026, 8, 24, tzinfo=berlin).astimezone(UTC)


def test__base_period_calculator__boundary_codec__is_readable_back() -> None:
    # Arrange
    codec = UUIDv7BoundaryCodec()

    # Act
    calc = WeekPeriodCalculator(boundary_codec=codec)

    # Assert -- pruning reads it off the calculator to decide how to compare
    assert calc.boundary_codec is codec
    assert WeekPeriodCalculator().boundary_codec is None
