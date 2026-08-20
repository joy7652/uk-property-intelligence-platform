"""Tests for the Gold small-area crime fact.

Every test needs a session. The transform is a projection over the shared aggregate, so
there is no pure-Python half to check separately.

The input frames are built by calling `crime.small_area_type_counts` on a resolved-shaped
frame rather than by declaring the aggregate's schema here. A hand-written schema would
pass after the aggregate changed shape, which is the one failure these suites exist to
catch.

Every row builder takes its defaults as a dict and updates it from `**overrides`.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality

from databricks_src.gold.transforms.crime import (
    ANTI_SOCIAL_BEHAVIOUR,
    RESOLVED,
    RESOLVED_COLUMNS,
    small_area_type_counts,
)
from databricks_src.gold.transforms.fact_lsoa_month_crime import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    transform_fact_lsoa_month_crime,
)

ENGLAND = "E92000001"
NORTH_EAST = "E12000001"
HARTLEPOOL = "E06000001"
HARTLEPOOL_LSOA = "E01012000"
NEIGHBOUR_LSOA = "E01012001"
BURGLARY = "Burglary"
SHOPLIFTING = "Shoplifting"
JUNE = date(2015, 6, 1)

RESOLVED_SCHEMA = (
    "population string, lsoa_code string, month_start_date date, crime_type string, "
    "district_code string, region_code string, nation_code string"
)


def resolved_row(**overrides):
    """One resolved crime record: a Hartlepool small area, burglary, June 2015."""
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


def counts(spark, rows=None):
    frame = spark.createDataFrame(
        [[row[name] for name in RESOLVED_COLUMNS] for row in rows or [resolved_row()]],
        RESOLVED_SCHEMA,
    )
    return small_area_type_counts(frame)


def fact(spark, rows=None):
    return transform_fact_lsoa_month_crime(counts(spark, rows))


def test_column_order_matches_the_target(spark):
    """INSERT OVERWRITE matches on position, so a projection out of order would load
    values into the wrong columns without failing."""
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measure():
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_the_columns_read_exist_on_the_shared_aggregate(spark):
    """The aggregate owns the frame this reads. A column dropped there would otherwise
    surface as a resolution failure inside a load."""
    assert set(SOURCE_COLUMNS) <= set(counts(spark).columns)


def test_the_ancestry_is_not_carried(spark):
    """A small area's district belongs to dim_lsoa. Carried here it would be a second
    copy on 26 million rows, free to disagree with the dimension."""
    columns = set(fact(spark).columns)
    assert not columns & {"district_code", "region_code", "nation_code", "population"}


def test_the_count_is_an_int(spark):
    types = dict(fact(spark).dtypes)
    assert types["crime_count"] == "int"
    assert types["month_start_date"] == "date"


def test_records_of_one_type_are_counted_together(spark):
    rows = [resolved_row(), resolved_row(), resolved_row()]
    row = fact(spark, rows).collect()[0]
    assert row["crime_count"] == 3


def test_two_types_are_two_rows(spark):
    rows = [resolved_row(), resolved_row(crime_type=SHOPLIFTING)]
    loaded = {row["crime_type"]: row["crime_count"] for row in fact(spark, rows).collect()}
    assert loaded == {BURGLARY: 1, SHOPLIFTING: 1}


def test_two_small_areas_are_two_rows(spark):
    rows = [resolved_row(), resolved_row(lsoa_code=NEIGHBOUR_LSOA)]
    assert {row["lsoa_code"] for row in fact(spark, rows).collect()} == {
        HARTLEPOOL_LSOA,
        NEIGHBOUR_LSOA,
    }


def test_two_months_are_two_rows(spark):
    rows = [resolved_row(), resolved_row(month_start_date=date(2015, 7, 1))]
    assert sorted(row["month_start_date"] for row in fact(spark, rows).collect()) == [
        date(2015, 6, 1),
        date(2015, 7, 1),
    ]


def test_anti_social_behaviour_has_no_row(spark):
    """Dropped in the shared aggregate and refused by check constraint as well, so a
    row reaching Delta is a failed write rather than a number nobody questions."""
    rows = [resolved_row(), resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR)]
    assert {row["crime_type"] for row in fact(spark, rows).collect()} == {BURGLARY}


def test_a_frame_of_nothing_but_anti_social_behaviour_produces_no_rows(spark):
    assert fact(spark, [resolved_row(crime_type=ANTI_SOCIAL_BEHAVIOUR)]).count() == 0


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = counts(spark).drop("crime_count")
    with pytest.raises(ValueError, match="crime_count"):
        transform_fact_lsoa_month_crime(frame)


def test_a_repeated_key_aborts(spark):
    """The aggregate groups on a superset of this key, so a repeat here means the
    aggregate itself broke."""
    doubled = counts(spark).unionAll(counts(spark))
    with pytest.raises(ValueError, match="grain broken"):
        transform_fact_lsoa_month_crime(doubled)


def test_the_transform_is_deterministic(spark):
    rows = [resolved_row(), resolved_row(lsoa_code=NEIGHBOUR_LSOA)]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
