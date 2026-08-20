"""Tests for the Gold monthly transaction mix fact.

Every test needs a session. The transform is an explode and a count, so there is no
pure-Python half to check separately.

Fixtures are rows of the resolved transaction frame, as in the price fact's suite.

The reconciliation against `fact_area_month_price` is asserted here rather than left to
prose. The two tables deliberately disagree on a bare count, because this one keeps both
sale categories and that one keeps category A, and the only statement that holds is that
the price fact's count equals this table's count filtered to category A. Asserted so the
relationship is a property rather than a note somebody has to find.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from databricks_src.gold.transforms.fact_area_month_price import (
    transform_fact_area_month_price,
)
from databricks_src.gold.transforms.fact_area_month_transaction_mix import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    transform_fact_area_month_transaction_mix,
)
from databricks_src.gold.transforms.transactions import (
    COMPOSITE_AREA_CODE,
    RESOLVED,
    RESOLVED_COLUMNS,
    SALE_ATTRIBUTE_COLUMNS,
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
    """One resolved transaction: Hartlepool, detached, established, freehold, category A."""
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
    return transform_fact_area_month_transaction_mix(
        resolved(spark, rows or [resolved_row()])
    )


def district_rows(spark, rows=None):
    return fact(spark, rows).filter(F.col("area_code") == HARTLEPOOL).collect()


# --------------------------------------------------------------------------- #
# Column contract
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measure():
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_the_four_sale_attributes_are_in_the_key():
    """They sit in the key rather than in a dimension, because each has a handful of
    values and no attributes of its own."""
    assert set(SALE_ATTRIBUTE_COLUMNS) <= set(KEY_COLUMNS)


def test_the_columns_read_exist_on_the_resolved_frame():
    assert set(SOURCE_COLUMNS) <= set(RESOLVED_COLUMNS)


def test_the_count_is_an_int(spark):
    assert dict(fact(spark).dtypes)["transaction_count"] == "int"


def test_no_price_reaches_the_output(spark):
    """Counts only. A median at this grain would sit on cells too thin to support one."""
    assert "price" not in fact(spark).columns


# --------------------------------------------------------------------------- #
# Grain
# --------------------------------------------------------------------------- #


def test_two_attribute_combinations_are_two_rows(spark):
    rows = [resolved_row(), resolved_row(property_type="F", duration="L")]
    assert len(district_rows(spark, rows)) == 2


def test_the_same_combination_is_counted_once(spark):
    rows = [resolved_row(), resolved_row(price=310_000)]
    counted = district_rows(spark, rows)
    assert len(counted) == 1
    assert counted[0]["transaction_count"] == 2


def test_two_months_are_two_rows(spark):
    rows = [resolved_row(), resolved_row(month_start_date=date(2015, 7, 1))]
    assert sorted(row["month_start_date"] for row in district_rows(spark, rows)) == [
        date(2015, 6, 1),
        date(2015, 7, 1),
    ]


def test_an_english_transaction_lands_at_four_levels(spark):
    areas = {row["area_code"] for row in fact(spark).collect()}
    assert areas == {HARTLEPOOL, NORTH_EAST, ENGLAND, COMPOSITE_AREA_CODE}


def test_a_welsh_transaction_lands_at_three_levels(spark):
    areas = {row["area_code"] for row in fact(spark, [welsh_row()]).collect()}
    assert areas == {CARDIFF, WALES, COMPOSITE_AREA_CODE}


# --------------------------------------------------------------------------- #
# Sale category
# --------------------------------------------------------------------------- #


def test_both_sale_categories_are_carried(spark):
    """Composition is what this table is for, so the category that steps at 2013 is part
    of the composition rather than a contaminant of it."""
    rows = [resolved_row(), resolved_row(ppd_category_type="B")]
    categories = {row["ppd_category_type"] for row in district_rows(spark, rows)}
    assert categories == {"A", "B"}


def test_the_price_fact_counts_the_category_a_subset(spark):
    """The only count relationship that holds between the two tables. Stated in the mix
    module's docstring; asserted here so it is a property rather than a note."""
    rows = [
        resolved_row(),
        resolved_row(property_type="F"),
        resolved_row(ppd_category_type="B"),
        welsh_row(),
    ]
    frame = resolved(spark, rows)

    mix = transform_fact_area_month_transaction_mix(frame)
    price = transform_fact_area_month_price(frame)

    mix_a = (
        mix.filter(F.col("ppd_category_type") == "A")
        .groupBy("area_code", "month_start_date")
        .agg(F.sum("transaction_count").alias("mix_count"))
    )
    joined = price.join(mix_a, ["area_code", "month_start_date"], "full_outer").collect()

    assert joined
    for row in joined:
        assert row["transaction_count"] == row["mix_count"], row["area_code"]


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = resolved(spark, [resolved_row()]).drop("old_new")
    with pytest.raises(ValueError, match="old_new"):
        transform_fact_area_month_transaction_mix(frame)


def test_the_transform_is_deterministic(spark):
    rows = [resolved_row(), resolved_row(ppd_category_type="B"), welsh_row()]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
