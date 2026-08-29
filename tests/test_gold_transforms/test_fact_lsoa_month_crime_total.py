"""Tests for the Gold small-area crime total fact.

Every test needs a session. The transform is a projection over the shared totals, so
there is no pure-Python half to check separately.

Input frames come from `crime.small_area_totals` rather than a schema declared here, so
a change to the aggregate's shape reaches these tests instead of passing them.

The zero cases are the point of this file. A cell holding anti-social behaviour alone
reports zero other crime, and a cell holding no anti-social behaviour reports zero of it.
102,735 and 1,272,887 cells respectively on the July 2026 release, which is why both
measures are counted in one aggregate rather than counted apart and joined.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality

from databricks_src.gold.transforms.crime import (
    ANTI_SOCIAL_BEHAVIOUR,
    RESOLVED,
    RESOLVED_COLUMNS,
    small_area_totals,
)
from databricks_src.gold.transforms.fact_lsoa_month_crime_total import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    transform_fact_lsoa_month_crime_total,
)

ENGLAND = "E92000001"
NORTH_EAST = "E12000001"
HARTLEPOOL = "E06000001"
HARTLEPOOL_LSOA = "E01012000"
NEIGHBOUR_LSOA = "E01012001"
BURGLARY = "Burglary"
JUNE = date(2015, 6, 1)

RESOLVED_SCHEMA = (
    "population string, lsoa_code string, month_start_date date, crime_type string, "
    "district_code string, region_code string, nation_code string"
)


def resolved_row(**overrides):
    row = {
        "population": RESOLVED,
        "lsoa_code": HARTLEPOOL_LSOA,
        "month_start_date": JUNE,
        "crime_type": BURGLARY,
        "district_code": HARTLEPOOL,
        "region_code": NORTH_EAST,
        "nation_code": ENGLAND,
    }
    row.update(overrides)
    return row


def totals(spark, rows=None):
    frame = spark.createDataFrame(
        [[row[name] for name in RESOLVED_COLUMNS] for row in rows or [resolved_row()]],
        RESOLVED_SCHEMA,
    )
    return small_area_totals(frame)


def fact(spark, rows=None):
    return transform_fact_lsoa_month_crime_total(totals(spark, rows))


def one(spark, rows=None):
    return fact(spark, rows).collect()[0]


def test_column_order_matches_the_target(spark):
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measures():
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_the_columns_read_exist_on_the_shared_aggregate(spark):
    assert set(SOURCE_COLUMNS) <= set(totals(spark).columns)


def test_the_ancestry_is_not_carried(spark):
    columns = set(fact(spark).columns)
    assert not columns & {"district_code", "region_code", "nation_code", "crime_type"}


def test_both_measures_are_ints(spark):
    types = dict(fact(spark).dtypes)
    for name in MEASURE_COLUMNS:
        assert types[name] == "int", name


def test_a_cell_splits_into_the_two_measures(spark):
    rows = [
        resolved_row(),
        resolved_row(),
        resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR),
    ]
    row = one(spark, rows)
    assert row["crime_count_excl_asb"] == 2
    assert row["anti_social_behaviour"] == 1


def test_a_cell_of_nothing_but_anti_social_behaviour_keeps_its_row(spark):
    """102,735 cells are this. The check constraint admits zero here rather than more
    than zero, unlike the type table, because no other crime is a measurement."""
    row = one(spark, [resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR)])
    assert row["crime_count_excl_asb"] == 0
    assert row["anti_social_behaviour"] == 1


def test_a_cell_with_no_anti_social_behaviour_keeps_its_row(spark):
    """1,272,887 cells are this, which is the side a left join in the other direction
    would have lost."""
    row = one(spark)
    assert row["crime_count_excl_asb"] == 1
    assert row["anti_social_behaviour"] == 0


def test_every_crime_type_falls_into_one_measure_or_the_other(spark):
    rows = [
        resolved_row(),
        resolved_row(crime_type="Shoplifting"),
        resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR),
    ]
    row = one(spark, rows)
    assert row["crime_count_excl_asb"] + row["anti_social_behaviour"] == len(rows)


def test_two_small_areas_are_two_rows(spark):
    rows = [resolved_row(), resolved_row(lsoa_code=NEIGHBOUR_LSOA)]
    assert {row["lsoa_code"] for row in fact(spark, rows).collect()} == {
        HARTLEPOOL_LSOA,
        NEIGHBOUR_LSOA,
    }


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = totals(spark).drop("anti_social_behaviour")
    with pytest.raises(ValueError, match="anti_social_behaviour"):
        transform_fact_lsoa_month_crime_total(frame)


def test_a_repeated_key_aborts(spark):
    doubled = totals(spark).unionAll(totals(spark))
    with pytest.raises(ValueError, match="grain broken"):
        transform_fact_lsoa_month_crime_total(doubled)


def test_the_transform_is_deterministic(spark):
    rows = [resolved_row(), resolved_row(lsoa_code=NEIGHBOUR_LSOA)]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
