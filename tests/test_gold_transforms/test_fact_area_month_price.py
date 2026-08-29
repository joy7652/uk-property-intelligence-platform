"""Tests for the Gold monthly area price fact.

Every test needs a session. The transform is a filter, an explode and an aggregate, so
there is no pure-Python half to check separately.

Fixtures are rows of the resolved transaction frame rather than raw Silver, because that
is what the transform takes. `test_transactions.py` covers how a transaction becomes one
of these; this file covers what happens to it afterwards. The two are tied together by
`test_the_columns_read_exist_on_the_resolved_frame`, which fails if the resolution stops
producing something this module reads.

Prices are chosen so the median and the mean land on a half wherever the rounding rule is
what a test is about, since whole pounds are what the table declares and a half is the
only place the rule shows.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality

from databricks_src.gold.transforms.fact_area_month_price import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    transform_fact_area_month_price,
)
from databricks_src.gold.transforms.transactions import (
    COMPOSITE_AREA_CODE,
    RESOLVED,
    RESOLVED_COLUMNS,
)

ENGLAND = "E92000001"
WALES = "W92000004"
NORTH_EAST = "E12000001"
HARTLEPOOL = "E06000001"
CARDIFF = "W06000015"

RESOLVED_SCHEMA = (
    "population string, price int, month_start_date date, year_start_date date, "
    "district_code string, region_code string, nation_code string, lsoa_code string, "
    "property_type string, old_new string, duration string, ppd_category_type string"
)


def resolved_row(**overrides):
    """One resolved transaction: Hartlepool, category A, June 2015."""
    row = {
        "population": RESOLVED,
        "price": 250_000,
        "month_start_date": date(2015, 6, 1),
        "year_start_date": date(2015, 1, 1),
        "district_code": HARTLEPOOL,
        "region_code": NORTH_EAST,
        "nation_code": ENGLAND,
        "lsoa_code": "E01012000",
        "property_type": "D",
        "old_new": "N",
        "duration": "F",
        "ppd_category_type": "A",
    }
    row.update(overrides)
    return row


def welsh_row(**overrides):
    """A Welsh transaction, which carries no region and counts at three levels."""
    row = {
        "district_code": CARDIFF,
        "region_code": None,
        "nation_code": WALES,
        "lsoa_code": "W01001000",
    }
    row.update(overrides)
    return resolved_row(**row)


def resolved(spark, rows):
    return spark.createDataFrame(
        [[row[name] for name in RESOLVED_COLUMNS] for row in rows], RESOLVED_SCHEMA
    )


def fact(spark, rows=None):
    return transform_fact_area_month_price(resolved(spark, rows or [resolved_row()]))


def by_area(spark, rows=None):
    return {row["area_code"]: row for row in fact(spark, rows).collect()}


# --------------------------------------------------------------------------- #
# Column contract
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    """INSERT OVERWRITE matches on position, so a projection out of order would load
    values into the wrong columns without failing."""
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measures():
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_the_columns_read_exist_on_the_resolved_frame():
    """The resolution owns the frame this reads. A column dropped there would otherwise
    surface as a resolution failure inside a load."""
    assert set(SOURCE_COLUMNS) <= set(RESOLVED_COLUMNS)


def test_the_measures_are_whole_pounds_and_counts(spark):
    types = dict(fact(spark).dtypes)
    assert types["month_start_date"] == "date"
    for name in MEASURE_COLUMNS:
        assert types[name] == "int", name


def test_the_resolution_scaffolding_does_not_reach_the_output(spark):
    columns = set(fact(spark).columns)
    assert not columns & {"population", "district_code", "lsoa_code", "price"}


# --------------------------------------------------------------------------- #
# Levels
# --------------------------------------------------------------------------- #


def test_an_english_transaction_lands_at_four_levels(spark):
    assert set(by_area(spark)) == {HARTLEPOOL, NORTH_EAST, ENGLAND, COMPOSITE_AREA_CODE}


def test_a_welsh_transaction_lands_at_three_levels(spark):
    assert set(by_area(spark, [welsh_row()])) == {CARDIFF, WALES, COMPOSITE_AREA_CODE}


def test_each_level_aggregates_from_the_transactions(spark):
    """A median cannot be recovered from the medians below it, so the composite is
    computed over both districts rather than from their two answers."""
    rows = [resolved_row(price=100_000), welsh_row(price=300_000)]
    loaded = by_area(spark, rows)
    assert loaded[HARTLEPOOL]["median_price"] == 100_000
    assert loaded[CARDIFF]["median_price"] == 300_000
    assert loaded[COMPOSITE_AREA_CODE]["median_price"] == 200_000
    assert loaded[COMPOSITE_AREA_CODE]["transaction_count"] == 2


def test_two_months_are_two_cells(spark):
    rows = [resolved_row(), resolved_row(month_start_date=date(2015, 7, 1))]
    months = [
        row["month_start_date"]
        for row in fact(spark, rows).collect()
        if row["area_code"] == HARTLEPOOL
    ]
    assert sorted(months) == [date(2015, 6, 1), date(2015, 7, 1)]


# --------------------------------------------------------------------------- #
# Sale category
# --------------------------------------------------------------------------- #


def test_category_b_is_excluded(spark):
    """Its share steps at 2013 from near nothing to a sixth of the table, so a series
    including it compares two different populations either side of that date."""
    rows = [resolved_row(price=100_000), resolved_row(price=900_000_000, ppd_category_type="B")]
    loaded = by_area(spark, rows)
    assert loaded[HARTLEPOOL]["transaction_count"] == 1
    assert loaded[HARTLEPOOL]["mean_price"] == 100_000


def test_a_cell_of_nothing_but_category_b_produces_no_row(spark):
    rows = [resolved_row(ppd_category_type="B")]
    assert fact(spark, rows).count() == 0


# --------------------------------------------------------------------------- #
# Measures
# --------------------------------------------------------------------------- #


def test_the_median_of_an_odd_cell_is_the_middle_value(spark):
    rows = [resolved_row(price=price) for price in (100_000, 200_000, 300_000)]
    assert by_area(spark, rows)[HARTLEPOOL]["median_price"] == 200_000


def test_the_median_of_an_even_cell_takes_the_midpoint_and_rounds_up(spark):
    """The exact median lands on a half here, and the table declares whole pounds."""
    rows = [resolved_row(price=100_000), resolved_row(price=200_001)]
    assert by_area(spark, rows)[HARTLEPOOL]["median_price"] == 150_001


def test_the_mean_rounds_to_whole_pounds(spark):
    rows = [resolved_row(price=100_000), resolved_row(price=200_001)]
    assert by_area(spark, rows)[HARTLEPOOL]["mean_price"] == 150_001


def test_the_count_is_transactions_not_areas(spark):
    rows = [resolved_row(price=price) for price in (100_000, 200_000, 300_000)]
    assert by_area(spark, rows)[HARTLEPOOL]["transaction_count"] == 3


def test_a_one_pound_sale_is_carried(spark):
    """22 of these in the July 2026 release, all category A. No floor is applied,
    because a threshold nobody measured is a rule invented here."""
    rows = [resolved_row(price=1), resolved_row(price=300_001)]
    loaded = by_area(spark, rows)
    assert loaded[HARTLEPOOL]["transaction_count"] == 2
    assert loaded[HARTLEPOOL]["mean_price"] == 150_001


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = resolved(spark, [resolved_row()]).drop("region_code")
    with pytest.raises(ValueError, match="region_code"):
        transform_fact_area_month_price(frame)


def test_the_transform_is_deterministic(spark):
    rows = [resolved_row(), welsh_row(price=310_000)]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
