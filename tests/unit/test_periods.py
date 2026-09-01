"""Calendar periods: validation, arithmetic, ordering and formatting."""

from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime

import pytest

from pg_partsmith import entities
from pg_partsmith.periods import PartitionGranularity, Period

# -- construction ----------------------------------------------------------------


def test__period__year_only__other_components_are_none() -> None:
    # Arrange / Act
    p = Period(year=2024)

    # Assert
    assert p.year == 2024
    assert (p.month, p.day, p.week, p.hour, p.quarter) == (None, None, None, None, None)


@pytest.mark.parametrize(
    ("kwargs", "attribute", "expected"),
    [
        ({"year": 2024, "month": 3}, "month", 3),
        ({"year": 2024, "month": 3, "day": 15}, "day", 15),
        ({"year": 2024, "week": 12}, "week", 12),
        ({"year": 2024, "month": 3, "day": 15, "hour": 7}, "hour", 7),
        ({"year": 2024, "quarter": 2}, "quarter", 2),
    ],
)
def test__period__each_granularity__stores_its_component(kwargs: dict[str, int], attribute: str, expected: int) -> None:
    # Arrange / Act
    p = Period(**kwargs)

    # Assert
    assert getattr(p, attribute) == expected


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"year": 2024, "month": 13}, "(?i)month"),
        ({"year": 2024, "month": 0}, "(?i)month"),
        ({"year": 2024, "month": 1, "day": 32}, "(?i)(day|date)"),
        ({"year": 2024, "month": 1, "day": 0}, "(?i)(day|date)"),
        ({"year": 2023, "month": 2, "day": 29}, "(?i)date"),
        ({"year": 2024, "week": 54}, "(?i)week"),
        ({"year": 2024, "week": 0}, "(?i)week"),
        ({"year": 2024, "month": 1, "day": 1, "hour": 24}, "(?i)hour"),
        ({"year": 2024, "month": 1, "day": 1, "hour": -1}, "(?i)hour"),
        ({"year": 2024, "quarter": 5}, "(?i)quarter"),
        ({"year": 2024, "quarter": 0}, "(?i)quarter"),
    ],
)
def test__period__out_of_range_component__raises_value_error(kwargs: dict[str, int], match: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match=match):
        Period(**kwargs)


def test__period__week_53_in_a_year_without_one__raises_value_error() -> None:
    # Arrange / Act / Assert -- 2021 has 52 ISO weeks
    with pytest.raises(ValueError, match="Invalid ISO week"):
        Period(year=2021, week=53)


def test__period__week_53_in_a_long_year__accepted() -> None:
    # Arrange / Act -- 2020 has 53 ISO weeks
    p = Period(year=2020, week=53)

    # Assert
    assert p.to_date() == date(2020, 12, 28)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"year": 2024, "month": 3, "hour": 5}, "(?i)day is required"),
        ({"year": 2024, "hour": 5}, "(?i)day is required"),
        ({"year": 2024, "day": 5}, "(?i)month is required"),
        ({"year": 2024, "month": 3, "quarter": 1}, "(?i)quarter"),
        ({"year": 2024, "month": 3, "day": 1, "quarter": 1}, "(?i)quarter"),
        ({"year": 2024, "week": 12, "hour": 5}, "(?i)week"),
        ({"year": 2024, "week": 12, "month": 3}, "(?i)week"),
        ({"year": 2024, "week": 12, "quarter": 1}, "(?i)quarter"),
    ],
)
def test__period__incompatible_components__raises_value_error(kwargs: dict[str, int], match: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match=match):
        Period(**kwargs)


def test__period__is_frozen__assignment_raises() -> None:
    # Arrange
    p = Period(year=2024)

    # Act / Assert
    with pytest.raises(FrozenInstanceError):
        p.year = 2025  # type: ignore[misc]


# -- arithmetic ------------------------------------------------------------------


def test__period__add_months_across_year__wraps_the_year() -> None:
    # Arrange
    p = Period(year=2024, month=11)

    # Act
    result = p + 2

    # Assert
    assert result == Period(year=2025, month=1)


def test__period__subtract_months_across_year__wraps_the_year() -> None:
    # Arrange
    p = Period(year=2024, month=1)

    # Act
    result = p - 2

    # Assert
    assert result == Period(year=2023, month=11)


def test__period__add_years__increments_year() -> None:
    # Arrange
    p = Period(year=2024)

    # Act
    result = p + 1

    # Assert
    assert result == Period(year=2025)


def test__period__add_days_across_leap_day__counts_february_29th() -> None:
    # Arrange
    p = Period(year=2024, month=2, day=28)

    # Act
    next_day = p + 1
    day_after = next_day + 1

    # Assert
    assert next_day == Period(year=2024, month=2, day=29)
    assert day_after == Period(year=2024, month=3, day=1)


def test__period__add_days_across_year__wraps_the_year() -> None:
    # Arrange
    p = Period(year=2024, month=12, day=31)

    # Act / Assert
    assert p + 1 == Period(year=2025, month=1, day=1)


def test__period__add_hours_across_midnight_and_month__wraps_day_and_month() -> None:
    # Arrange
    p = Period(year=2024, month=3, day=31, hour=23)

    # Act
    result = p + 1

    # Assert
    assert result == Period(year=2024, month=4, day=1, hour=0)


def test__period__subtract_hours_across_midnight__lands_on_leap_day() -> None:
    # Arrange
    p = Period(year=2024, month=3, day=1, hour=0)

    # Act
    result = p - 1

    # Assert -- 2024 is a leap year, so 1 March minus one hour lands on 29 February
    assert result == Period(year=2024, month=2, day=29, hour=23)


def test__period__add_weeks_across_iso_year__uses_the_next_iso_year() -> None:
    # Arrange
    p = Period(year=2020, week=53)

    # Act / Assert
    assert p + 1 == Period(year=2021, week=1)


def test__period__subtract_weeks_across_iso_year__lands_on_week_53() -> None:
    # Arrange
    p = Period(year=2021, week=1)

    # Act / Assert
    assert p - 1 == Period(year=2020, week=53)


def test__period__add_quarters_across_year__wraps_the_year() -> None:
    # Arrange
    p = Period(year=2024, quarter=4)

    # Act / Assert
    assert p + 2 == Period(year=2025, quarter=2)


def test__period__subtract_quarters_across_year__wraps_to_previous_year() -> None:
    # Arrange
    p = Period(year=2024, quarter=1)

    # Act / Assert
    assert p - 1 == Period(year=2023, quarter=4)


@pytest.mark.parametrize(
    "period",
    [
        Period(year=2024),
        Period(year=2024, month=3),
        Period(year=2024, month=3, day=15),
        Period(year=2024, month=3, day=15, hour=7),
        Period(year=2024, week=12),
        Period(year=2024, quarter=2),
    ],
)
def test__period__add_zero__returns_an_equal_period(period: Period) -> None:
    # Arrange / Act / Assert
    assert period + 0 == period
    assert period - 0 == period


@pytest.mark.parametrize(
    "period",
    [
        Period(year=2024),
        Period(year=2024, month=3),
        Period(year=2024, month=3, day=15),
        Period(year=2024, month=3, day=15, hour=7),
        Period(year=2024, week=12),
        Period(year=2024, quarter=2),
    ],
)
def test__period__subtraction__is_addition_of_the_negated_offset(period: Period) -> None:
    # Arrange / Act / Assert
    assert period - 3 == period + (-3)
    assert (period + 5) - 5 == period


# -- ordering and equality --------------------------------------------------------


@pytest.mark.parametrize(
    ("smaller", "larger"),
    [
        (Period(year=2023), Period(year=2024)),
        (Period(year=2024, month=1), Period(year=2024, month=3)),
        (Period(year=2023, month=12), Period(year=2024, month=1)),
        (Period(year=2024, month=3, day=14), Period(year=2024, month=3, day=15)),
        (Period(year=2024, month=3, day=15, hour=7), Period(year=2024, month=3, day=15, hour=9)),
        (Period(year=2024, week=11), Period(year=2024, week=12)),
        (Period(year=2024, quarter=1), Period(year=2024, quarter=3)),
    ],
)
def test__period__same_granularity__orders_chronologically(smaller: Period, larger: Period) -> None:
    # Arrange / Act / Assert
    assert smaller < larger
    assert larger > smaller
    assert smaller <= larger
    assert larger >= smaller
    assert smaller <= smaller
    assert not smaller > larger


def test__period__sorted__orders_by_position_on_the_calendar() -> None:
    # Arrange
    periods = [Period(year=2024, month=3), Period(year=2023, month=12), Period(year=2024, month=1)]

    # Act
    ordered = sorted(periods)

    # Assert
    assert ordered == [Period(year=2023, month=12), Period(year=2024, month=1), Period(year=2024, month=3)]


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (Period(year=2024, month=1), Period(year=2024, week=1)),
        (Period(year=2024, month=3, day=15, hour=7), Period(year=2024, month=3, day=15)),
        (Period(year=2024, quarter=1), Period(year=2024, month=1)),
        (Period(year=2024), Period(year=2024, month=1)),
    ],
)
def test__period__different_granularities__comparison_raises_type_error(left: Period, right: Period) -> None:
    # Arrange / Act / Assert
    with pytest.raises(TypeError):
        _ = left < right
    with pytest.raises(TypeError):
        _ = right > left


def test__period__lt_with_something_else__returns_not_implemented() -> None:
    # Arrange
    p = Period(year=2024, month=1)

    # Act / Assert
    assert p.__lt__(1) is NotImplemented
    assert p.__lt__(Period(year=2024, week=1)) is NotImplemented
    assert p.__lt__(Period(year=2024, quarter=1)) is NotImplemented
    assert p.__lt__(Period(year=2024, month=1, day=1, hour=0)) is NotImplemented


def test__period__equal_components__equal_and_hash_alike() -> None:
    # Arrange
    a = Period(year=2024, month=1)
    b = Period(year=2024, month=1)

    # Act / Assert
    assert a == b
    assert hash(a) == hash(b)
    assert len({a, b}) == 1


def test__period__missing_component__not_equal_to_the_finer_period() -> None:
    # Arrange / Act / Assert
    assert Period(year=2024) != Period(year=2024, month=1)
    assert Period(year=2024, month=1) != Period(year=2024, month=1, day=1)


# -- formatting and conversion ----------------------------------------------------


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (Period(year=2024), "2024"),
        (Period(year=2024, month=3), "2024_03"),
        (Period(year=2024, month=3, day=5), "2024_03_05"),
        (Period(year=2024, month=3, day=5, hour=7), "2024_03_05_07"),
        (Period(year=2024, week=7), "2024_w07"),
        (Period(year=2024, quarter=2), "2024_q2"),
        (Period(year=999), "0999"),
    ],
)
def test__period__str__is_the_partition_name_fragment(period: Period, expected: str) -> None:
    # Arrange / Act / Assert
    assert str(period) == expected


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (Period(year=2024), date(2024, 1, 1)),
        (Period(year=2024, month=3), date(2024, 3, 1)),
        (Period(year=2024, month=3, day=15), date(2024, 3, 15)),
        (Period(year=2024, month=3, day=15, hour=7), date(2024, 3, 15)),
        (Period(year=2024, week=12), date(2024, 3, 18)),
        (Period(year=2024, quarter=3), date(2024, 7, 1)),
    ],
)
def test__period__to_date__returns_the_start_date(period: Period, expected: date) -> None:
    # Arrange / Act / Assert
    assert period.to_date() == expected


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (Period(year=2024), datetime(2024, 1, 1, tzinfo=UTC)),
        (Period(year=2024, month=3), datetime(2024, 3, 1, tzinfo=UTC)),
        (Period(year=2024, month=3, day=15), datetime(2024, 3, 15, tzinfo=UTC)),
        (Period(year=2024, month=3, day=15, hour=7), datetime(2024, 3, 15, 7, tzinfo=UTC)),
        (Period(year=2024, week=12), datetime(2024, 3, 18, tzinfo=UTC)),
        (Period(year=2024, quarter=4), datetime(2024, 10, 1, tzinfo=UTC)),
    ],
)
def test__period__to_datetime__returns_the_utc_start_instant(period: Period, expected: datetime) -> None:
    # Arrange / Act
    start = period.to_datetime()

    # Assert
    assert start == expected
    assert start.tzinfo is UTC


def test__period__to_datetime_of_two_hours__distinct_instants_on_the_same_date() -> None:
    # Arrange
    seven = Period(year=2024, month=3, day=15, hour=7)
    nine = Period(year=2024, month=3, day=15, hour=9)

    # Act / Assert -- to_date collapses them, to_datetime keeps them apart
    assert seven.to_date() == nine.to_date()
    assert nine.to_datetime() - seven.to_datetime() == datetime(2024, 1, 1, 2, tzinfo=UTC) - datetime(
        2024, 1, 1, tzinfo=UTC
    )


# -- PartitionGranularity --------------------------------------------------------


@pytest.mark.parametrize(
    ("member", "value"),
    [
        (PartitionGranularity.HOUR, "hour"),
        (PartitionGranularity.DAY, "day"),
        (PartitionGranularity.WEEK, "week"),
        (PartitionGranularity.MONTH, "month"),
        (PartitionGranularity.QUARTER, "quarter"),
        (PartitionGranularity.YEAR, "year"),
    ],
)
def test__partition_granularity__member__is_its_lowercase_name(member: PartitionGranularity, value: str) -> None:
    # Arrange / Act / Assert
    assert member.value == value
    assert str(member) == value
    assert PartitionGranularity(value) is member


def test__partition_granularity__has_exactly_six_sizes() -> None:
    # Arrange / Act / Assert
    assert len(PartitionGranularity) == 6


def test__entities__still_re_exports_the_calendar_types() -> None:
    # Arrange / Act / Assert -- ``pg_partsmith.entities`` has always been their public home
    assert entities.Period is Period
    assert entities.PartitionGranularity is PartitionGranularity
