"""Tests for the shared Silver column expressions.

Every test needs a session: the function returns a Column, and what it returns is
only observable once something evaluates it.

The formats parametrised here are the five the sources actually publish, so a change
to the parse is checked against real shapes rather than invented ones. The values
that must not parse matter as much as the ones that must: a transform relies on a
malformed cell arriving as null, because its own cast guard turns that null back
into a failure naming the column.
"""

from __future__ import annotations

from datetime import date

import pytest

from databricks_src.silver.transforms.expressions import parsed_date

# Format, a value that parses, and what it parses to. One row per source.
PUBLISHED_FORMATS = [
    ("yyyy-MM-dd", "2020-01-31", date(2020, 1, 31)),
    ("dd/MM/yyyy", "31/01/2020", date(2020, 1, 31)),
    ("yyyy-MM", "2020-01", date(2020, 1, 1)),
    ("yyyy-MM-dd HH:mm", "2020-01-31 00:00", date(2020, 1, 31)),
]

IDS = ["doogal_ons", "hpi", "police", "ppd"]


def evaluate(spark, expression, values, column="raw"):
    frame = spark.createDataFrame([(value,) for value in values], f"`{column}` string")
    return [row[0] for row in frame.select(expression.alias("v")).collect()]


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("date_format, value, expected", PUBLISHED_FORMATS, ids=IDS)
def test_each_published_format_parses(spark, date_format, value, expected):
    assert evaluate(spark, parsed_date("raw", date_format), [value]) == [expected]


def test_a_month_format_lands_on_the_first(spark):
    """police publishes months. The day the parse supplies is the one the crime
    month column carries for the whole month."""
    assert evaluate(spark, parsed_date("raw", "yyyy-MM"), ["2020-06"]) == [
        date(2020, 6, 1)
    ]


def test_a_time_component_is_dropped_not_rejected(spark):
    """ppd publishes a zero time alongside the date."""
    assert evaluate(
        spark, parsed_date("raw", "yyyy-MM-dd HH:mm"), ["2020-01-31 00:00"]
    ) == [date(2020, 1, 31)]


def test_the_result_is_a_date_not_a_timestamp(spark):
    """The parse routes through a timestamp. A column left as one would widen every
    Silver schema that declares DATE."""
    frame = spark.createDataFrame([("2020-01-31",)], "`raw` string")
    typed = frame.select(parsed_date("raw", "yyyy-MM-dd").alias("v"))
    assert dict(typed.dtypes)["v"] == "date"


# --------------------------------------------------------------------------- #
# Values that must not parse
# --------------------------------------------------------------------------- #


def test_an_unparseable_value_is_null(spark):
    assert evaluate(spark, parsed_date("raw", "yyyy-MM-dd"), ["rubbish"]) == [None]


def test_an_empty_string_is_null(spark):
    assert evaluate(spark, parsed_date("raw", "yyyy-MM-dd"), [""]) == [None]


def test_null_stays_null(spark):
    assert evaluate(spark, parsed_date("raw", "yyyy-MM-dd"), [None]) == [None]


def test_an_impossible_date_is_null(spark):
    assert evaluate(spark, parsed_date("raw", "yyyy-MM-dd"), ["2020-13-45"]) == [None]


def test_the_wrong_format_is_null_rather_than_reordered(spark):
    """hpi publishes dd/MM/yyyy. An ISO value arriving there is a source change, and
    a silent reinterpretation would put the wrong date in the table."""
    assert evaluate(spark, parsed_date("raw", "dd/MM/yyyy"), ["2020-01-31"]) == [None]


def test_a_malformed_value_does_not_raise(spark):
    """ANSI mode is on, so a plain cast would abort the load. The whole point of the
    try form is that the transform's own guard reports the column instead."""
    assert evaluate(
        spark, parsed_date("raw", "yyyy-MM-dd"), ["2020-01-31", "rubbish", None]
    ) == [date(2020, 1, 31), None, None]


# --------------------------------------------------------------------------- #
# Raw column names
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "column",
    ["Month", "Crime ID", "1m%Change", "date_of_transfer"],
    ids=["plain", "spaced", "digit_and_percent", "underscored"],
)
def test_published_header_shapes_resolve(spark, column):
    """Backticks are what make this work. Police headers carry spaces and HPI headers
    open with a digit and carry a percent sign."""
    assert evaluate(
        spark, parsed_date(column, "yyyy-MM-dd"), ["2020-01-31"], column=column
    ) == [date(2020, 1, 31)]


# --------------------------------------------------------------------------- #
# Session
# --------------------------------------------------------------------------- #


def test_the_result_is_unaliased(spark):
    """Callers writing a named column add their own alias, and callers wrapping the
    result in year() do not want one."""
    frame = spark.createDataFrame([("2020-01-31",)], "`raw` string")
    assert frame.select(parsed_date("raw", "yyyy-MM-dd")).columns != ["raw"]


@pytest.mark.parametrize(
    "timezone", ["UTC", "Europe/London", "America/Los_Angeles", "Pacific/Kiritimati"]
)
def test_the_date_survives_the_session_timezone(spark, timezone):
    """The parse produces a timestamp and the cast reads it back. Both happen in the
    session timezone, so the two shifts cancel wherever the session is set."""
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", timezone)
    try:
        assert evaluate(spark, parsed_date("raw", "yyyy-MM-dd"), ["2020-06-30"]) == [
            date(2020, 6, 30)
        ]
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)
