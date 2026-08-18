"""Tests for the Gold house price index fact.

Every test needs a session. The transform is a filter and a projection, so there is no
pure-Python half to check separately.

The source fixture carries `region_name` alongside the columns the fact reads. Silver
publishes 56 columns and the fact takes 13, so a test frame holding exactly the 13 would
never show that the extras are dropped rather than carried.

Rows are sparse by default with only the two headline measures populated, which is the
shape of a real row above district level: nine of the eleven measures are null on most
of the panel, and the two seasonally adjusted ones are null on 96 percent of it.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from databricks_src.gold.transforms.fact_area_month_hpi import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    SOURCE_DATE_COLUMN,
    transform_fact_area_month_hpi,
)

ENGLAND = "E92000001"
ABERDEENSHIRE = "S12000034"

SOURCE_FIELDS: tuple[str, ...] = ("area_code", "region_name", "date") + MEASURE_COLUMNS

SOURCE_SCHEMA = (
    "area_code string, region_name string, date date, "
    "avg_price decimal(18,6), avg_price_seasonally_adjusted decimal(18,6), "
    "price_index decimal(18,6), price_index_seasonally_adjusted decimal(18,6), "
    "pct_change_1m decimal(18,6), pct_change_12m decimal(18,6), "
    "sales_volume int, detached_price decimal(18,6), "
    "semi_detached_price decimal(18,6), terraced_price decimal(18,6), "
    "flat_price decimal(18,6)"
)

DEFAULTS = {
    "area_code": ENGLAND,
    "region_name": "England",
    "date": date(2010, 1, 1),
    "avg_price": Decimal("200000.000000"),
    "price_index": Decimal("100.000000"),
}


def source_row(**overrides):
    """One Silver row: sparse, with the two headline measures populated."""
    row = {name: None for name in SOURCE_FIELDS}
    row.update(DEFAULTS)
    row.update(overrides)
    return row


def only_measure(**populated):
    """Every measure null except the ones named.

    Built as one dict rather than passed as keyword arguments beside a dynamic one,
    which is what a parametrised measure collides with when it is also in DEFAULTS. It
    also stops a test depending on which measures DEFAULTS happens to populate.
    """
    overrides = dict.fromkeys(MEASURE_COLUMNS)
    overrides.update(populated)
    return overrides


def source(spark, rows):
    return spark.createDataFrame(
        [[row[name] for name in SOURCE_FIELDS] for row in rows], SOURCE_SCHEMA
    )


def fact(spark, rows=None):
    return transform_fact_area_month_hpi(source(spark, rows or [source_row()]))


def one(spark, **overrides):
    return fact(spark, [source_row(**overrides)]).collect()[0]


# --------------------------------------------------------------------------- #
# Column contract
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    """INSERT OVERWRITE matches on position, so a projection out of order would load
    values into the wrong columns without failing."""
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measures():
    """GOLD_COLUMNS is built from the other two tuples, which is what stops the
    projection and the measure list drifting apart. Asserted so an edit that writes it
    out longhand fails here."""
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_every_measure_is_read_from_the_source():
    assert set(MEASURE_COLUMNS) <= set(SOURCE_COLUMNS)


def test_a_silver_column_the_fact_does_not_carry_is_dropped(spark):
    """region_name belongs to the area dimension. Carried here it would be a second
    copy of a name that can be renamed in one place and not the other."""
    assert "region_name" not in fact(spark).columns


def test_types_come_through_unchanged(spark):
    """Nothing is cast, because Silver already declares what the table declares. This
    is the assertion that makes that safe rather than assumed."""
    types = dict(fact(spark).dtypes)
    assert types["month_start_date"] == "date"
    assert types["sales_volume"] == "int"
    for name in set(MEASURE_COLUMNS) - {"sales_volume"}:
        assert types[name] == "decimal(18,6)", name


# --------------------------------------------------------------------------- #
# Projection
# --------------------------------------------------------------------------- #


def test_the_month_key_is_renamed_from_the_silver_date(spark):
    row = one(spark, date=date(2015, 9, 1))
    assert row["month_start_date"] == date(2015, 9, 1)
    assert SOURCE_DATE_COLUMN not in fact(spark).columns


def test_a_populated_measure_survives_at_full_precision(spark):
    """Six decimal places on both sides. A silent widening or narrowing here would move
    a published price."""
    row = one(spark, avg_price=Decimal("81693.669640"))
    assert row["avg_price"] == Decimal("81693.669640")


def test_a_negative_change_survives(spark):
    row = one(spark, pct_change_1m=Decimal("-0.018248"))
    assert row["pct_change_1m"] == Decimal("-0.018248")


def test_a_null_measure_stays_null(spark):
    row = one(spark)
    assert row["price_index_seasonally_adjusted"] is None
    assert row["flat_price"] is None


def test_no_geographic_filter_is_applied(spark):
    """Every level the publisher issues belongs here, and Silver has already removed
    what is below a nation's coverage floor. A Scottish row from 2004 is measured data
    and passes through."""
    rows = [
        source_row(),
        source_row(area_code=ABERDEENSHIRE, region_name="Aberdeenshire", date=date(2004, 1, 1)),
    ]
    assert fact(spark, rows).count() == 2


def test_rows_are_not_aggregated(spark):
    """The index is mix-adjusted and published at every level, so a region is read from
    its own row. One source row is one fact row."""
    rows = [source_row(date=date(2010, month, 1)) for month in (1, 2, 3)]
    assert fact(spark, rows).count() == 3


# --------------------------------------------------------------------------- #
# Rows carrying no measure
# --------------------------------------------------------------------------- #


def test_a_row_with_no_measure_is_dropped(spark):
    rows = [source_row(), source_row(date=date(2010, 2, 1), **only_measure())]
    kept = fact(spark, rows).collect()
    assert [row["month_start_date"] for row in kept] == [date(2010, 1, 1)]


@pytest.mark.parametrize("measure", MEASURE_COLUMNS)
def test_one_populated_measure_is_enough_to_keep_a_row(spark, measure):
    """Any single measure makes the row a fact. Parametrised over all eleven so a
    measure added to the tuple is covered without the test being edited."""
    value = 1 if measure == "sales_volume" else Decimal("1.000000")
    rows = [source_row(**only_measure(**{measure: value}))]
    assert fact(spark, rows).count() == 1


def test_a_zero_sales_volume_is_a_measurement_and_stays(spark):
    """Null, not falsy. A month with no transactions recorded zero of them, which is a
    different statement from having published nothing."""
    rows = [source_row(**only_measure(sales_volume=0))]
    assert fact(spark, rows).count() == 1


def test_a_frame_of_nothing_but_measureless_rows_produces_no_rows(spark):
    rows = [source_row(**only_measure())]
    assert fact(spark, rows).count() == 0


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = source(spark, [source_row()]).drop("sales_volume")
    with pytest.raises(ValueError, match="sales_volume"):
        transform_fact_area_month_hpi(frame)


def test_a_missing_date_column_aborts(spark):
    frame = source(spark, [source_row()]).drop("date")
    with pytest.raises(ValueError, match="missing columns it reads"):
        transform_fact_area_month_hpi(frame)


def test_an_extra_source_column_is_accepted(spark):
    """One direction only. Silver adding a column is not this table's problem."""
    frame = source(spark, [source_row()]).withColumn("new_breakdown", F.lit(1))
    assert transform_fact_area_month_hpi(frame).count() == 1


def test_a_repeated_key_aborts(spark):
    """Silver asserts its own grain on (area_code, date). This asserts the table's,
    which is a different key on a different frame."""
    rows = [source_row(), source_row()]
    with pytest.raises(ValueError, match="grain broken"):
        fact(spark, rows)


def test_the_same_month_in_two_areas_is_not_a_repeat(spark):
    rows = [source_row(), source_row(area_code=ABERDEENSHIRE, region_name="Aberdeenshire")]
    assert fact(spark, rows).count() == 2


def test_a_repeat_among_dropped_rows_does_not_abort(spark):
    """Grain is asserted after the drop. Two measureless rows for one month are two rows
    the table never carries."""
    rows = [
        source_row(),
        source_row(date=date(2010, 2, 1), **only_measure()),
        source_row(date=date(2010, 2, 1), **only_measure()),
    ]
    assert fact(spark, rows).count() == 1


def test_the_transform_is_deterministic(spark):
    rows = [source_row(), source_row(date=date(2010, 2, 1))]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
