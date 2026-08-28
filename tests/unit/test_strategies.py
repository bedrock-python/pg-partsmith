from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from freezegun import freeze_time

from pg_partsmith.boundaries import UUIDv7BoundaryCodec
from pg_partsmith.entities import PartitionGranularity, Period
from pg_partsmith.pruning_rules import _boundary_decoder
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

# ── MonthPeriodCalculator ───────────────────────────────────────────────────────


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
    with pytest.raises(ValueError):
        calc.format_partition_name("events", Period(year=2024))


@pytest.mark.parametrize(
    "name,expected",
    [
        ("events__2024_03", Period(year=2024, month=3)),
        ("events__2024_12", Period(year=2024, month=12)),
    ],
)
def test__month_calculator__parse_valid_name__returns_period(name: str, expected: Period) -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) == expected


@pytest.mark.parametrize(
    "name",
    ["invalid", "events__2024", "events__2024_13"],
)
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
    with pytest.raises(ValueError):
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
    assert len(periods) == 3
    assert periods[0] == Period(year=2024, month=3)
    assert periods[1] == Period(year=2024, month=4)
    assert periods[2] == Period(year=2024, month=5)


def test__month_calculator__next_periods_zero__raises_value_error() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError, match="positive"):
        calc.next_periods(0)


def test__month_calculator__period_before__subtracts_offset() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act
    result = calc.period_before(Period(year=2024, month=3), 2)

    # Assert
    assert result == Period(year=2024, month=1)


def test__month_calculator__period_before_crosses_year__wraps_to_previous_year() -> None:
    # Arrange
    calc = MonthPeriodCalculator()

    # Act
    result = calc.period_before(Period(year=2025, month=1), 2)

    # Assert
    assert result == Period(year=2024, month=11)


# ── DayPeriodCalculator ─────────────────────────────────────────────────────────


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
    with pytest.raises(ValueError):
        calc.format_partition_name("events", Period(year=2024, month=3))


def test__day_calculator__parse_valid_name__returns_period() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name("events__2024_03_15") == Period(year=2024, month=3, day=15)


@pytest.mark.parametrize(
    "name",
    ["events__2024_03", "events__2024_02_30"],
)
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


@freeze_time("2024-03-15")
def test__day_calculator__current_period__returns_march_15_2024() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act
    p = calc.current_period()

    # Assert
    assert p == Period(year=2024, month=3, day=15)


@freeze_time("2024-03-15")
def test__day_calculator__next_periods_3__returns_consecutive_days() -> None:
    # Arrange
    calc = DayPeriodCalculator()

    # Act
    periods = calc.next_periods(3)

    # Assert
    assert periods[0] == Period(year=2024, month=3, day=15)
    assert periods[2] == Period(year=2024, month=3, day=17)


# ── WeekPeriodCalculator ────────────────────────────────────────────────────────


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
    with pytest.raises(ValueError):
        calc.format_partition_name("events", Period(year=2024))


def test__week_calculator__parse_lowercase_w__returns_period() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name("events__2024_w12") == Period(year=2024, week=12)


def test__week_calculator__parse_uppercase_w__returns_none() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert — uppercase W is not accepted
    assert calc.parse_partition_name("events__2024_W12") is None


@pytest.mark.parametrize(
    "name",
    ["events__2024_03", "events__2024_w00", "events__2021_W53"],
)
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


def test__week_calculator__get_boundaries_without_week__raises_value_error() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError):
        calc.get_boundaries(Period(year=2024))


@freeze_time("2024-03-18")  # Monday of ISO week 12
def test__week_calculator__current_period__returns_week_12_2024() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act
    p = calc.current_period()

    # Assert
    assert p.year == 2024
    assert p.week == 12


@freeze_time("2024-01-01")
def test__week_calculator__next_periods_2__returns_consecutive_weeks() -> None:
    # Arrange
    calc = WeekPeriodCalculator()

    # Act
    periods = calc.next_periods(2)

    # Assert
    assert len(periods) == 2
    assert periods[0].week is not None and periods[1].week is not None
    assert periods[1].week == periods[0].week + 1 or periods[1].year > periods[0].year


# ── YearPeriodCalculator ────────────────────────────────────────────────────────


def test__year_calculator__format__produces_table_double_underscore_year() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act
    name = calc.format_partition_name("events", Period(year=2024))

    # Assert
    assert name == "events__2024"


def test__year_calculator__parse_valid_name__returns_period() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name("events__2024") == Period(year=2024)


@pytest.mark.parametrize(
    "name",
    ["events__2024_03", "invalid"],
)
def test__year_calculator__parse_invalid_name__returns_none(name: str) -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) is None


def test__year_calculator__get_boundaries__returns_january_first_to_next_year() -> None:
    # Arrange
    calc = YearPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024))

    # Assert
    assert from_val.startswith("2024-01-01")
    assert to_val.startswith("2025-01-01")


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


# ── HourPeriodCalculator ────────────────────────────────────────────────────────


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
    with pytest.raises(ValueError):
        calc.format_partition_name("events", Period(year=2024, month=3, day=5))


@pytest.mark.parametrize(
    "name,expected",
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

    # Act / Assert — a day-granularity name must not parse as an hour period
    assert calc.parse_partition_name("t__2026_08_25") is None


@pytest.mark.parametrize(
    "name",
    ["invalid", "events__2024_03_15_24", "events__2024_02_30_05", "events__2024_13_01_00"],
)
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
    with pytest.raises(ValueError):
        calc.get_boundaries(Period(year=2024, month=3, day=15))


@freeze_time("2024-03-15 14:30:00")
def test__hour_calculator__current_period__returns_march_15_hour_14() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act
    p = calc.current_period()

    # Assert
    assert p == Period(year=2024, month=3, day=15, hour=14)


@freeze_time("2024-03-15 22:30:00")
def test__hour_calculator__next_periods_3__crosses_midnight_into_next_day() -> None:
    # Arrange
    calc = HourPeriodCalculator()

    # Act
    periods = calc.next_periods(3)

    # Assert
    assert periods[0] == Period(year=2024, month=3, day=15, hour=22)
    assert periods[1] == Period(year=2024, month=3, day=15, hour=23)
    assert periods[2] == Period(year=2024, month=3, day=16, hour=0)


# ── QuarterPeriodCalculator ─────────────────────────────────────────────────────


def test__quarter_calculator__format__produces_table_double_underscore_year_lowercase_q_quarter() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act
    name = calc.format_partition_name("events", Period(year=2024, quarter=2))

    # Assert
    assert name == "events__2024_q2"


def test__quarter_calculator__format_without_quarter__raises_value_error() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError):
        calc.format_partition_name("events", Period(year=2024))


@pytest.mark.parametrize(
    "name,expected",
    [
        ("events__2024_q1", Period(year=2024, quarter=1)),
        ("events__2024_q4", Period(year=2024, quarter=4)),
    ],
)
def test__quarter_calculator__parse_valid_name__returns_period(name: str, expected: Period) -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) == expected


@pytest.mark.parametrize(
    "name",
    ["invalid", "events__2024_03", "events__2024_q0", "events__2024_q5", "events__2024_Q1"],
)
def test__quarter_calculator__parse_invalid_name__returns_none(name: str) -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    assert calc.parse_partition_name(name) is None


def test__quarter_calculator__get_boundaries__returns_first_day_of_quarter_and_next() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, quarter=2))

    # Assert
    assert from_val == "2024-04-01"
    assert to_val == "2024-07-01"


def test__quarter_calculator__get_boundaries_q4__wraps_to_january_next_year() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act
    from_val, to_val = calc.get_boundaries(Period(year=2024, quarter=4))

    # Assert
    assert from_val == "2024-10-01"
    assert to_val == "2025-01-01"


def test__quarter_calculator__get_boundaries_without_quarter__raises_value_error() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act / Assert
    with pytest.raises(ValueError):
        calc.get_boundaries(Period(year=2024))


@freeze_time("2024-08-15")
def test__quarter_calculator__current_period__returns_q3_2024() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act
    p = calc.current_period()

    # Assert
    assert p == Period(year=2024, quarter=3)


@freeze_time("2024-08-15")
def test__quarter_calculator__next_periods_3__returns_q3_q4_and_next_year_q1() -> None:
    # Arrange
    calc = QuarterPeriodCalculator()

    # Act
    periods = calc.next_periods(3)

    # Assert
    assert periods[0] == Period(year=2024, quarter=3)
    assert periods[1] == Period(year=2024, quarter=4)
    assert periods[2] == Period(year=2025, quarter=1)


# ── get_period_calculator ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "granularity,expected_cls",
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
    granularity: PartitionGranularity,
    expected_cls: type,
) -> None:
    # Arrange / Act
    calc = get_period_calculator(granularity)

    # Assert
    assert isinstance(calc, expected_cls)


def test__get_period_calculator__unknown_granularity__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="granularity"):
        get_period_calculator("invalid")  # type: ignore[arg-type]


def test__get_period_calculator__default_tz__is_utc() -> None:
    # Arrange / Act
    calc = get_period_calculator(PartitionGranularity.DAY)

    # Assert
    assert calc.tz is UTC
    assert calc.timezone_name == "UTC"


def test__get_period_calculator__moscow_tz__returns_calculator_in_that_zone() -> None:
    # Arrange
    tz = ZoneInfo("Europe/Moscow")

    # Act
    calc = get_period_calculator(PartitionGranularity.MONTH, tz=tz)

    # Assert
    assert calc.tz is tz
    assert calc.timezone_name == "Europe/Moscow"


def test__get_period_calculator__hour_with_moscow_tz__raises_value_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="only UTC"):
        get_period_calculator(PartitionGranularity.HOUR, tz=ZoneInfo("Europe/Moscow"))


# ── timezone configuration ──────────────────────────────────────────────────────


def test__calculator__default_tz__is_utc_named_utc() -> None:
    # Arrange / Act
    calc = MonthPeriodCalculator()

    # Assert
    assert calc.tz is UTC
    assert calc.timezone_name == "UTC"


def test__calculator__zoneinfo_tz__exposes_zone_and_iana_key() -> None:
    # Arrange
    tz = ZoneInfo("Europe/Moscow")

    # Act
    calc = MonthPeriodCalculator(tz=tz)

    # Assert
    assert calc.tz is tz
    assert calc.timezone_name == "Europe/Moscow"


@pytest.mark.parametrize(
    "calculator_cls",
    [
        HourPeriodCalculator,
        DayPeriodCalculator,
        WeekPeriodCalculator,
        MonthPeriodCalculator,
        QuarterPeriodCalculator,
        YearPeriodCalculator,
    ],
)
def test__calculator__fixed_offset_tz__raises_value_error(calculator_cls: type[BasePeriodCalculator]) -> None:
    # Arrange / Act / Assert — timezone(timedelta(...)) carries no IANA name usable in SET LOCAL TIME ZONE
    with pytest.raises(ValueError, match="Unsupported timezone"):
        calculator_cls(tz=timezone(timedelta(hours=3)))


def test__hour_calculator__moscow_tz__raises_value_error() -> None:
    # Arrange / Act / Assert — local-time hour names are ambiguous under DST
    with pytest.raises(ValueError, match="only UTC"):
        HourPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))


def test__hour_calculator__explicit_utc_tz__constructs() -> None:
    # Arrange / Act
    calc = HourPeriodCalculator(tz=UTC)

    # Assert
    assert calc.tz is UTC
    assert calc.timezone_name == "UTC"


# ── timezone-aware current periods ──────────────────────────────────────────────


@freeze_time("2024-03-31 23:30:00")
def test__month_calculator__current_period_utc_vs_moscow__crosses_month_boundary() -> None:
    # Arrange — 23:30 UTC on Mar 31 is already 02:30 Apr 1 in Moscow (UTC+3)
    utc_calc = MonthPeriodCalculator()
    moscow_calc = MonthPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024, month=3)
    assert moscow_calc.current_period() == Period(year=2024, month=4)


@freeze_time("2024-03-15 23:30:00")
def test__day_calculator__current_period_utc_vs_moscow__crosses_day_boundary() -> None:
    # Arrange — 23:30 UTC on Mar 15 is already 02:30 Mar 16 in Moscow (UTC+3)
    utc_calc = DayPeriodCalculator()
    moscow_calc = DayPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024, month=3, day=15)
    assert moscow_calc.current_period() == Period(year=2024, month=3, day=16)


@freeze_time("2024-03-17 23:30:00")  # Sunday of ISO week 11 in UTC
def test__week_calculator__current_period_utc_vs_moscow__crosses_iso_week_boundary() -> None:
    # Arrange — Moscow (UTC+3) is already on Monday of ISO week 12
    utc_calc = WeekPeriodCalculator()
    moscow_calc = WeekPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024, week=11)
    assert moscow_calc.current_period() == Period(year=2024, week=12)


@freeze_time("2024-03-31 23:30:00")
def test__quarter_calculator__current_period_utc_vs_moscow__crosses_quarter_boundary() -> None:
    # Arrange — 23:30 UTC on Mar 31 is already 02:30 Apr 1 in Moscow (UTC+3)
    utc_calc = QuarterPeriodCalculator()
    moscow_calc = QuarterPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024, quarter=1)
    assert moscow_calc.current_period() == Period(year=2024, quarter=2)


@freeze_time("2024-12-31 23:30:00")
def test__year_calculator__current_period_utc_vs_moscow__crosses_year_boundary() -> None:
    # Arrange — 23:30 UTC on Dec 31 is already 02:30 Jan 1 in Moscow (UTC+3)
    utc_calc = YearPeriodCalculator()
    moscow_calc = YearPeriodCalculator(tz=ZoneInfo("Europe/Moscow"))

    # Act / Assert
    assert utc_calc.current_period() == Period(year=2024)
    assert moscow_calc.current_period() == Period(year=2025)


def test__boundary_decoder__calculator_with_a_codec__is_used_instead_of_timestamp_parsing() -> None:
    # Arrange -- without this branch a UUIDv7-keyed table never prunes anything,
    # silently, because every bound reads as unparseable.
    codec = UUIDv7BoundaryCodec()
    calculator = WeekPeriodCalculator(boundary_codec=codec)
    bound = str(codec.min_uuid_for(datetime(2026, 8, 24, tzinfo=UTC)))

    # Act
    decode = _boundary_decoder(calculator, UTC)

    # Assert
    assert decode(bound) == datetime(2026, 8, 24, tzinfo=UTC)


def test__boundary_decoder__calculator_without_one__falls_back_to_timestamp_parsing() -> None:
    # Arrange
    calculator = WeekPeriodCalculator()

    # Act
    decode = _boundary_decoder(calculator, UTC)

    # Assert
    assert decode("2026-08-24 00:00:00+00") == datetime(2026, 8, 24, tzinfo=UTC)


def test__boundary_decoder__naive_datetime_from_a_codec__falls_back_rather_than_raising() -> None:
    # Arrange -- comparing a naive bound with the aware cutoff raises from the
    # middle of retention, nowhere near the codec that produced it.
    class NaiveCodec:
        def encode(self, instant: datetime) -> str:
            return instant.isoformat()

        def decode(self, literal: str) -> datetime | None:
            return datetime(2026, 8, 24)

    calculator = WeekPeriodCalculator(boundary_codec=NaiveCodec())  # type: ignore[arg-type]

    # Act
    decode = _boundary_decoder(calculator, UTC)

    # Assert -- the timestamp fallback answers instead.
    assert decode("2026-08-24 00:00:00+00") == datetime(2026, 8, 24, tzinfo=UTC)


def test__week_calculator__non_utc_timezone_with_a_codec__encodes_the_local_period_start() -> None:
    # Arrange -- every existing codec test uses a default-UTC calculator, so a
    # business-timezone calculator combined with a codec was unpinned.
    berlin = ZoneInfo("Europe/Berlin")
    codec = UUIDv7BoundaryCodec()
    calculator = WeekPeriodCalculator(tz=berlin, boundary_codec=codec)

    # Act
    lower, _upper = calculator.get_boundaries(Period(year=2026, week=35))

    # Assert -- Monday 00:00 in Berlin is 22:00 Sunday UTC; encoding the UTC
    # instant instead would misroute every row in those two hours.
    assert codec.decode(lower) == datetime(2026, 8, 24, tzinfo=berlin).astimezone(UTC)
