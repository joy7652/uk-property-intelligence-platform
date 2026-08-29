"""Tests for the Gold annual small-area price fact.

Every test needs a session. The transform is two filters and an aggregate, so there is no
pure-Python half to check separately.

Fixtures are rows of the resolved transaction frame, as in the two area suites. The
difference this file exists to cover is that no level explode applies: a small area keys
on itself and conforms to `dim_area` through `district_code`, so a transaction lands in
exactly one row here where it lands in three or four in the area facts.

The dropped population is a transaction whose district resolved and whose small area did
not. The July 2026 release held none, and `no_small_area` is exposed anyway so a load
records a zero it measured rather than one nobody asked about.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality

from databricks_src.gold.transforms.fact_lsoa_year_price import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    no_small_area,
    transform_fact_lsoa_year_price,
)
from databricks_src.gold.transforms.transactions import RESOLVED, RESOLVED_COLUMNS

ENGLAND = "E92000001"
NORTH_EAST = "E12000001"
HARTLEPOOL = "E06000001"
HARTLEPOOL_LSOA = "E01012000"
NEIGHBOURING_LSOA = "E01012001"

RESOLVED_SCHEMA = (
    "population string, price int, month_start_date date, year_start_date date, "
    "district_code string, region_code string, nation_code string, lsoa_code string, "
    "property_type string, old_new string, duration string, ppd_category_type string"
)


def resolved_row(**overrides):
    """One resolved transaction: a Hartlepool small area, category A, 2015."""
    row = {
        "population": RESOLVED,
        "price": 250_000,
        "month_start_date": date(2015, 6, 1),
        "year_start_date": date(2015, 1, 1),
        "district_code": HARTLEPOOL,
        "region_code": NORTH_EAST,
        "nation_code": ENGLAND,
        "lsoa_code": HARTLEPOOL_LSOA,
        "property_type": "D",
        "old_new": "N",
        "duration": "F",
        "ppd_category_type": "A",
    }
    row.update(overrides)
    return row


def resolved(spark, rows):
    return spark.createDataFrame(
        [[row[name] for name in RESOLVED_COLUMNS] for row in rows], RESOLVED_SCHEMA
    )


def fact(spark, rows=None):
    return transform_fact_lsoa_year_price(resolved(spark, rows or [resolved_row()]))


def by_area(spark, rows=None):
    return {row["lsoa_code"]: row for row in fact(spark, rows).collect()}


# --------------------------------------------------------------------------- #
# Column contract
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measures():
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_the_columns_read_exist_on_the_resolved_frame():
    assert set(SOURCE_COLUMNS) <= set(RESOLVED_COLUMNS)


def test_the_measures_are_whole_pounds_and_counts(spark):
    types = dict(fact(spark).dtypes)
    assert types["year_start_date"] == "date"
    for name in MEASURE_COLUMNS:
        assert types[name] == "int", name


def test_no_area_code_reaches_the_output(spark):
    """A small area conforms through district_code on the dimension, so the fact carries
    neither a district nor an area code of its own."""
    columns = set(fact(spark).columns)
    assert not columns & {"area_code", "district_code", "region_code", "nation_code"}


# --------------------------------------------------------------------------- #
# Grain
# --------------------------------------------------------------------------- #


def test_a_transaction_lands_in_exactly_one_row(spark):
    """No level explode. The rollup to district and above is fact_area_month_price,
    aggregated from the same transactions rather than from these medians."""
    assert fact(spark).count() == 1


def test_months_in_one_year_are_one_cell(spark):
    """Annual because monthly is too thin to carry a median: 55 percent of small-area
    months hold two transactions or fewer."""
    rows = [
        resolved_row(month_start_date=date(2015, 6, 1), price=100_000),
        resolved_row(month_start_date=date(2015, 9, 1), price=300_000),
    ]
    loaded = by_area(spark, rows)[HARTLEPOOL_LSOA]
    assert loaded["transaction_count"] == 2
    assert loaded["year_start_date"] == date(2015, 1, 1)


def test_two_years_are_two_cells(spark):
    rows = [
        resolved_row(),
        resolved_row(year_start_date=date(2016, 1, 1), month_start_date=date(2016, 3, 1)),
    ]
    years = sorted(row["year_start_date"] for row in fact(spark, rows).collect())
    assert years == [date(2015, 1, 1), date(2016, 1, 1)]


def test_two_small_areas_are_two_cells(spark):
    rows = [resolved_row(), resolved_row(lsoa_code=NEIGHBOURING_LSOA)]
    assert set(by_area(spark, rows)) == {HARTLEPOOL_LSOA, NEIGHBOURING_LSOA}


# --------------------------------------------------------------------------- #
# Dropped rows
# --------------------------------------------------------------------------- #


def test_a_transaction_with_no_small_area_is_dropped(spark):
    """The district resolved and the small area did not, which is a postcode the
    directory places in a district without giving it an output area."""
    rows = [resolved_row(), resolved_row(lsoa_code=None)]
    assert by_area(spark, rows)[HARTLEPOOL_LSOA]["transaction_count"] == 1


def test_no_small_area_selects_exactly_those_rows(spark):
    """Exposed so the load records how many it dropped rather than reporting a zero
    nobody measured."""
    rows = [resolved_row(), resolved_row(lsoa_code=None)]
    assert resolved(spark, rows).filter(no_small_area()).count() == 1


def test_a_frame_of_nothing_but_unlocated_rows_produces_no_rows(spark):
    assert fact(spark, [resolved_row(lsoa_code=None)]).count() == 0


def test_category_b_is_excluded(spark):
    """As in fact_area_month_price and for the same reason. It costs 814 of the
    1,135,047 small-area years and no small area its series."""
    rows = [resolved_row(price=100_000), resolved_row(price=900_000, ppd_category_type="B")]
    loaded = by_area(spark, rows)[HARTLEPOOL_LSOA]
    assert loaded["transaction_count"] == 1
    assert loaded["mean_price"] == 100_000


def test_a_cell_of_nothing_but_category_b_produces_no_row(spark):
    assert fact(spark, [resolved_row(ppd_category_type="B")]).count() == 0


# --------------------------------------------------------------------------- #
# Measures
# --------------------------------------------------------------------------- #


def test_the_median_of_an_odd_cell_is_the_middle_value(spark):
    rows = [resolved_row(price=price) for price in (100_000, 200_000, 300_000)]
    assert by_area(spark, rows)[HARTLEPOOL_LSOA]["median_price"] == 200_000


def test_the_median_of_an_even_cell_takes_the_midpoint_and_rounds_up(spark):
    rows = [resolved_row(price=100_000), resolved_row(price=200_001)]
    assert by_area(spark, rows)[HARTLEPOOL_LSOA]["median_price"] == 150_001


def test_the_mean_rounds_to_whole_pounds(spark):
    rows = [resolved_row(price=100_000), resolved_row(price=200_001)]
    assert by_area(spark, rows)[HARTLEPOOL_LSOA]["mean_price"] == 150_001


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = resolved(spark, [resolved_row()]).drop("lsoa_code")
    with pytest.raises(ValueError, match="lsoa_code"):
        transform_fact_lsoa_year_price(frame)


def test_the_transform_is_deterministic(spark):
    rows = [resolved_row(), resolved_row(lsoa_code=NEIGHBOURING_LSOA, price=310_000)]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
