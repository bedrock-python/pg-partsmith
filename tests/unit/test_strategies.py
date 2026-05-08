import pytest
from freezegun import freeze_time

from pg_partsmith.entities import PartitionGranularity, Period
from pg_partsmith.strategies import (
    DayPeriodCalculator,
    MonthPeriodCalculator,
    WeekPeriodCalculator,
    YearPeriodCalculator,
)
from pg_partsmith.strategies.selector import get_period_calculator

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


# ── get_period_calculator ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "granularity,expected_cls",
    [
        (PartitionGranularity.DAY, DayPeriodCalculator),
        (PartitionGranularity.WEEK, WeekPeriodCalculator),
        (PartitionGranularity.MONTH, MonthPeriodCalculator),
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
