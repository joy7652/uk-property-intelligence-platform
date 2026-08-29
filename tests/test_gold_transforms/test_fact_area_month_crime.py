"""Tests for the Gold area crime fact.

Every test needs a session. The transform is an explode and a sum over the shared
aggregate, so there is no pure-Python half to check separately.

Input frames come from `crime.small_area_type_counts`, not a schema declared here.

The rollup is what this file covers. A count is additive, so summing the small-area
aggregate at each level gives what counting the records at that level would, and the
tests assert the identity at unit scale that the load checks at 67,886,868.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality

from databricks_src.gold.transforms.crime import (
    ANTI_SOCIAL_BEHAVIOUR,
    COMPOSITE_AREA_CODE,
    RESOLVED,
    RESOLVED_COLUMNS,
    small_area_type_counts,
)
from databricks_src.gold.transforms.fact_area_month_crime import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    transform_fact_area_month_crime,
)

ENGLAND = "E92000001"
WALES = "W92000004"
NORTH_EAST = "E12000001"
HARTLEPOOL = "E06000001"
CARDIFF = "W06000015"
HARTLEPOOL_LSOA = "E01012000"
NEIGHBOUR_LSOA = "E01012001"
CARDIFF_LSOA = "W01001000"
BURGLARY = "Burglary"
SHOPLIFTING = "Shoplifting"
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


def welsh_row(**overrides):
    """A Welsh record, which carries no region and rolls up to three levels."""
    row = {
        "lsoa_code": CARDIFF_LSOA,
        "district_code": CARDIFF,
        "region_code": None,
        "nation_code": WALES,
    }
    row.update(overrides)
    return resolved_row(**row)


def counts(spark, rows=None):
    frame = spark.createDataFrame(
        [[row[name] for name in RESOLVED_COLUMNS] for row in rows or [resolved_row()]],
        RESOLVED_SCHEMA,
    )
    return small_area_type_counts(frame)


def fact(spark, rows=None):
    return transform_fact_area_month_crime(counts(spark, rows))


def by_area(spark, rows=None):
    return {row["area_code"]: row for row in fact(spark, rows).collect()}


def test_column_order_matches_the_target(spark):
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measure():
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_the_columns_read_exist_on_the_shared_aggregate(spark):
    assert set(SOURCE_COLUMNS) <= set(counts(spark).columns)


def test_the_small_area_does_not_reach_the_output(spark):
    """This table keys on the published area. The small-area detail is
    fact_lsoa_month_crime."""
    assert "lsoa_code" not in fact(spark).columns


def test_the_count_is_an_int(spark):
    types = dict(fact(spark).dtypes)
    assert types["crime_count"] == "int"
    assert types["month_start_date"] == "date"


def test_an_english_record_rolls_up_to_four_levels(spark):
    assert set(by_area(spark)) == {HARTLEPOOL, NORTH_EAST, ENGLAND, COMPOSITE_AREA_CODE}


def test_a_welsh_record_rolls_up_to_three_levels(spark):
    """Wales is not divided into regions, so the null level is dropped rather than
    counted."""
    assert set(by_area(spark, [welsh_row()])) == {CARDIFF, WALES, COMPOSITE_AREA_CODE}


def test_two_small_areas_in_one_district_sum(spark):
    rows = [
        resolved_row(),
        resolved_row(),
        resolved_row(lsoa_code=NEIGHBOUR_LSOA),
    ]
    assert by_area(spark, rows)[HARTLEPOOL]["crime_count"] == 3


def test_the_composite_sums_both_nations(spark):
    """The identity the load checks at 67,886,868: summing the small-area aggregate
    gives what counting the records would."""
    rows = [resolved_row(), resolved_row(), welsh_row()]
    loaded = by_area(spark, rows)
    assert loaded[HARTLEPOOL]["crime_count"] == 2
    assert loaded[CARDIFF]["crime_count"] == 1
    assert loaded[COMPOSITE_AREA_CODE]["crime_count"] == 3


def test_two_types_stay_apart_at_every_level(spark):
    rows = [resolved_row(), resolved_row(crime_type=SHOPLIFTING)]
    composite = {
        row["crime_type"]: row["crime_count"]
        for row in fact(spark, rows).collect()
        if row["area_code"] == COMPOSITE_AREA_CODE
    }
    assert composite == {BURGLARY: 1, SHOPLIFTING: 1}


def test_two_months_are_separate_cells(spark):
    rows = [resolved_row(), resolved_row(month_start_date=date(2015, 7, 1))]
    months = [
        row["month_start_date"]
        for row in fact(spark, rows).collect()
        if row["area_code"] == HARTLEPOOL
    ]
    assert sorted(months) == [date(2015, 6, 1), date(2015, 7, 1)]


def test_anti_social_behaviour_has_no_row(spark):
    rows = [resolved_row(), resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR)]
    assert {row["crime_type"] for row in fact(spark, rows).collect()} == {BURGLARY}


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = counts(spark).drop("region_code")
    with pytest.raises(ValueError, match="region_code"):
        transform_fact_area_month_crime(frame)


def test_the_transform_is_deterministic(spark):
    rows = [resolved_row(), welsh_row()]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
