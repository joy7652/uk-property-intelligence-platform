"""Tests for the Gold daily calendar.

Every test needs a session: the calendar is built by expanding rate intervals inside
Spark, so there is no pure-Python half to check separately.

The synthetic rate series is deliberately tiny. What matters is the shape of the
interval chain, not its length, and a three-interval chain exercises a closed
interval, a second closed interval abutting it, and the open one.

The calendar attributes are checked against hand-computed values for specific days
rather than against Spark rebuilding them a second way, which would only prove the two
expressions agree.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from databricks_src.gold.transforms.dim_date import (
    GOLD_COLUMNS,
    transform_dim_date,
)

RATE_SCHEMA = StructType(
    [
        StructField("effective_date", DateType(), nullable=False),
        StructField("expiry_date", DateType(), nullable=True),
        StructField("rate_pct", DecimalType(6, 4), nullable=False),
        StructField("rate_type", StringType(), nullable=False),
    ]
)

# Three intervals abutting exactly, the last open. Spans a leap day, a quarter
# boundary and a month end.
INTERVALS: list[tuple] = [
    (date(2024, 1, 1), date(2024, 2, 29), Decimal("5.2500"), "Official Bank Rate"),
    (date(2024, 3, 1), date(2024, 3, 31), Decimal("5.0000"), "Official Bank Rate"),
    (date(2024, 4, 1), None, Decimal("4.7500"), "Official Bank Rate"),
]

END_DATE = date(2024, 4, 30)

# 31 + 29 + 31 + 30 in a leap year.
EXPECTED_DAYS = 121


def rate_frame(spark, intervals=None):
    return spark.createDataFrame(intervals or INTERVALS, RATE_SCHEMA)


def calendar(spark, intervals=None, end_date: date = END_DATE):
    return transform_dim_date(rate_frame(spark, intervals), end_date)


def by_day(spark, **kwargs) -> dict[date, object]:
    return {row["date_key"]: row for row in calendar(spark, **kwargs).collect()}


# --------------------------------------------------------------------------- #
# Shape
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    assert tuple(calendar(spark).columns) == GOLD_COLUMNS


def test_one_row_per_day_across_the_whole_range(spark):
    rows = by_day(spark)
    assert len(rows) == EXPECTED_DAYS
    assert min(rows) == date(2024, 1, 1)
    assert max(rows) == END_DATE


def test_calendar_starts_where_the_rate_series_starts(spark):
    """Not at a fixed year. base_rate_pct is NOT NULL, and a calendar reaching back
    before the series would have no rate to carry."""
    intervals = [(date(2019, 6, 3), None, Decimal("0.7500"), "Official Bank Rate")]
    rows = by_day(spark, intervals=intervals, end_date=date(2019, 6, 30))
    assert min(rows) == date(2019, 6, 3)
    assert len(rows) == 28


def test_leap_day_is_present(spark):
    assert date(2024, 2, 29) in by_day(spark)


def test_target_types_are_set_by_the_transform(spark):
    """The insert would widen these silently. Setting them here is what lets a value
    that no longer fits fail rather than be promoted."""
    types = dict(calendar(spark).dtypes)
    assert types["calendar_year"] == "int"
    assert types["year_month"] == "int"
    assert types["calendar_quarter"] == "tinyint"
    assert types["calendar_month"] == "tinyint"
    assert types["day_of_month"] == "tinyint"
    assert types["day_of_week"] == "tinyint"
    assert types["base_rate_pct"] == "decimal(6,4)"


# --------------------------------------------------------------------------- #
# The rate carried on each day
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "day, rate",
    [
        (date(2024, 1, 1), Decimal("5.2500")),
        (date(2024, 2, 29), Decimal("5.2500")),
        (date(2024, 3, 1), Decimal("5.0000")),
        (date(2024, 3, 31), Decimal("5.0000")),
        (date(2024, 4, 1), Decimal("4.7500")),
        (END_DATE, Decimal("4.7500")),
    ],
)
def test_rate_resolves_to_the_interval_covering_the_day(spark, day, rate):
    assert by_day(spark)[day]["base_rate_pct"] == rate


def test_every_day_carries_a_rate(spark):
    rows = calendar(spark).collect()
    assert not [row for row in rows if row["base_rate_pct"] is None]
    assert not [row for row in rows if row["base_rate_type"] is None]


def test_open_interval_runs_to_the_end_date(spark):
    rows = by_day(spark)
    open_days = [
        day for day, row in rows.items() if row["base_rate_pct"] == Decimal("4.7500")
    ]
    assert max(open_days) == END_DATE
    assert min(open_days) == date(2024, 4, 1)


# --------------------------------------------------------------------------- #
# Calendar attributes
# --------------------------------------------------------------------------- #


def test_attributes_for_a_known_day(spark):
    # 2024-02-29 was a Thursday, and the last day of February.
    row = by_day(spark)[date(2024, 2, 29)]
    assert row["calendar_year"] == 2024
    assert row["calendar_quarter"] == 1
    assert row["calendar_month"] == 2
    assert row["day_of_month"] == 29
    assert row["day_of_week"] == 4
    assert row["month_name"] == "February"
    assert row["day_name"] == "Thursday"
    assert row["year_month"] == 202402
    assert row["month_start_date"] == date(2024, 2, 1)
    assert row["quarter_start_date"] == date(2024, 1, 1)
    assert row["is_month_end"] is True
    assert row["is_weekend"] is False


@pytest.mark.parametrize(
    "day, iso_weekday, name, weekend",
    [
        (date(2024, 4, 1), 1, "Monday", False),
        (date(2024, 4, 5), 5, "Friday", False),
        (date(2024, 4, 6), 6, "Saturday", True),
        (date(2024, 4, 7), 7, "Sunday", True),
        (date(2024, 4, 8), 1, "Monday", False),
    ],
)
def test_weekday_numbering_is_iso(spark, day, iso_weekday, name, weekend):
    """One Monday to seven Sunday, which is not what Spark's dayofweek returns."""
    row = by_day(spark)[day]
    assert row["day_of_week"] == iso_weekday
    assert row["day_name"] == name
    assert row["is_weekend"] is weekend


@pytest.mark.parametrize(
    "day, quarter_start",
    [
        (date(2024, 1, 1), date(2024, 1, 1)),
        (date(2024, 3, 31), date(2024, 1, 1)),
        (date(2024, 4, 1), date(2024, 4, 1)),
    ],
)
def test_quarter_start_matches_the_table_constraint(spark, day, quarter_start):
    """dim_date_starts_align checks this expression on write, so a mismatch here would
    fail the insert rather than land a wrong value."""
    assert by_day(spark)[day]["quarter_start_date"] == quarter_start


def test_month_end_is_marked_once_per_month(spark):
    ends = {day for day, row in by_day(spark).items() if row["is_month_end"]}
    assert ends == {
        date(2024, 1, 31),
        date(2024, 2, 29),
        date(2024, 3, 31),
        date(2024, 4, 30),
    }


def test_year_month_is_derived_the_way_the_constraint_reads_it(spark):
    for row in calendar(spark).collect():
        assert row["year_month"] == row["calendar_year"] * 100 + row["calendar_month"]


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_missing_source_column_aborts(spark):
    source = rate_frame(spark).drop("rate_type")
    with pytest.raises(ValueError, match="missing columns it reads"):
        transform_dim_date(source, END_DATE)


def test_empty_rate_series_aborts(spark):
    source = spark.createDataFrame([], RATE_SCHEMA)
    with pytest.raises(ValueError, match="no rate intervals"):
        transform_dim_date(source, END_DATE)


def test_end_date_inside_the_series_aborts(spark):
    """sequence() reverses rather than failing when its start is past its stop, so
    this would otherwise produce a descending run of dates."""
    with pytest.raises(ValueError, match="falls before the last rate change"):
        calendar(spark, end_date=date(2024, 2, 1))


def test_overlapping_intervals_abort(spark):
    intervals = [
        (date(2024, 1, 1), date(2024, 3, 15), Decimal("5.2500"), "Official Bank Rate"),
        (date(2024, 3, 1), None, Decimal("4.7500"), "Official Bank Rate"),
    ]
    with pytest.raises(ValueError, match="intervals overlap"):
        calendar(spark, intervals=intervals)


def test_gap_in_the_chain_aborts(spark):
    intervals = [
        (date(2024, 1, 1), date(2024, 2, 15), Decimal("5.2500"), "Official Bank Rate"),
        (date(2024, 3, 1), None, Decimal("4.7500"), "Official Bank Rate"),
    ]
    with pytest.raises(ValueError, match="leave a gap"):
        calendar(spark, intervals=intervals)


def test_series_with_no_open_interval_aborts(spark):
    """Every interval closed means the last one expires before end_date, so the
    calendar would stop short without the rate having ended."""
    intervals = [
        (date(2024, 1, 1), date(2024, 2, 29), Decimal("5.2500"), "Official Bank Rate"),
        (date(2024, 3, 1), date(2024, 3, 31), Decimal("5.0000"), "Official Bank Rate"),
    ]
    with pytest.raises(ValueError, match="no rate interval is open"):
        calendar(spark, intervals=intervals)


def test_two_open_intervals_abort(spark):
    """Caught as an overlap: both expand to end_date, so the days they share appear
    twice. No separate check for a second current row is needed."""
    intervals = [
        (date(2024, 1, 1), date(2024, 2, 29), Decimal("5.2500"), "Official Bank Rate"),
        (date(2024, 3, 1), None, Decimal("5.0000"), "Official Bank Rate"),
        (date(2024, 4, 1), None, Decimal("4.7500"), "Official Bank Rate"),
    ]
    with pytest.raises(ValueError, match="intervals overlap"):
        calendar(spark, intervals=intervals)
