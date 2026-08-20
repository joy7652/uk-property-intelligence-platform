"""Tests for the Gold area crime total fact.

Every test needs a session. The transform is an explode and two sums over the shared
totals, so there is no pure-Python half to check separately.

Input frames come from `crime.small_area_totals`, not a schema declared here.

The reconciliation against `fact_area_month_crime` is asserted rather than left to prose.
The table's own column comment states that `crime_count_excl_asb` is the sum of every
type in that table for the same area and month, and this is what holds it to that.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from databricks_src.gold.transforms.crime import (
    ANTI_SOCIAL_BEHAVIOUR,
    COMPOSITE_AREA_CODE,
    RESOLVED,
    RESOLVED_COLUMNS,
    small_area_totals,
    small_area_type_counts,
)
from databricks_src.gold.transforms.fact_area_month_crime import (
    transform_fact_area_month_crime,
)
from databricks_src.gold.transforms.fact_area_month_crime_total import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    transform_fact_area_month_crime_total,
)

ENGLAND = "E92000001"
WALES = "W92000004"
NORTH_EAST = "E12000001"
HARTLEPOOL = "E06000001"
CARDIFF = "W06000015"
HARTLEPOOL_LSOA = "E01012000"
CARDIFF_LSOA = "W01001000"
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


def welsh_row(**overrides):
    row = {
        "lsoa_code": CARDIFF_LSOA,
        "district_code": CARDIFF,
        "region_code": None,
        "nation_code": WALES,
    }
    row.update(overrides)
    return resolved_row(**row)


def resolved(spark, rows=None):
    return spark.createDataFrame(
        [[row[name] for name in RESOLVED_COLUMNS] for row in rows or [resolved_row()]],
        RESOLVED_SCHEMA,
    )


def fact(spark, rows=None):
    return transform_fact_area_month_crime_total(small_area_totals(resolved(spark, rows)))


def by_area(spark, rows=None):
    return {row["area_code"]: row for row in fact(spark, rows).collect()}


def test_column_order_matches_the_target(spark):
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measures():
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_the_columns_read_exist_on_the_shared_aggregate(spark):
    assert set(SOURCE_COLUMNS) <= set(small_area_totals(resolved(spark)).columns)


def test_both_measures_are_ints(spark):
    types = dict(fact(spark).dtypes)
    for name in MEASURE_COLUMNS:
        assert types[name] == "int", name


def test_an_english_record_rolls_up_to_four_levels(spark):
    assert set(by_area(spark)) == {HARTLEPOOL, NORTH_EAST, ENGLAND, COMPOSITE_AREA_CODE}


def test_a_welsh_record_rolls_up_to_three_levels(spark):
    assert set(by_area(spark, [welsh_row()])) == {CARDIFF, WALES, COMPOSITE_AREA_CODE}


def test_both_measures_sum_across_small_areas(spark):
    rows = [
        resolved_row(),
        resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR),
        welsh_row(),
        welsh_row(crime_type=ANTI_SOCIAL_BEHAVIOUR),
    ]
    composite = by_area(spark, rows)[COMPOSITE_AREA_CODE]
    assert composite["crime_count_excl_asb"] == 2
    assert composite["anti_social_behaviour"] == 2


def test_an_area_month_of_nothing_but_anti_social_behaviour_keeps_its_row(spark):
    """Nine area-months are this on the July 2026 release. The check constraint admits
    zero here rather than more than zero for exactly that reason."""
    row = by_area(spark, [resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR)])[HARTLEPOOL]
    assert row["crime_count_excl_asb"] == 0
    assert row["anti_social_behaviour"] == 1


def test_an_area_month_with_no_anti_social_behaviour_keeps_its_row(spark):
    """747 area-months are this."""
    row = by_area(spark)[HARTLEPOOL]
    assert row["crime_count_excl_asb"] == 1
    assert row["anti_social_behaviour"] == 0


def test_the_total_is_the_sum_of_the_type_fact(spark):
    """The relationship the column comment states. Asserted so it is a property rather
    than a note somebody has to find."""
    rows = [
        resolved_row(),
        resolved_row(crime_type="Shoplifting"),
        resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR),
        welsh_row(),
    ]
    frame = resolved(spark, rows)

    totals = transform_fact_area_month_crime_total(small_area_totals(frame))
    types = transform_fact_area_month_crime(small_area_type_counts(frame))

    summed = types.groupBy(*KEY_COLUMNS).agg(F.sum("crime_count").alias("type_sum"))
    joined = totals.join(summed, list(KEY_COLUMNS), "full_outer").collect()

    assert joined
    for row in joined:
        assert row["crime_count_excl_asb"] == row["type_sum"], row["area_code"]


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = small_area_totals(resolved(spark)).drop("nation_code")
    with pytest.raises(ValueError, match="nation_code"):
        transform_fact_area_month_crime_total(frame)


def test_the_transform_is_deterministic(spark):
    rows = [resolved_row(), welsh_row()]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
