"""Tests for the shared Gold crime resolution.

Every test needs a session. The module is joins, a labelled case expression and an
explode, so there is no pure-Python half to check separately.

The fixtures are a miniature country: one English small area carrying a region, one
Welsh one carrying none, one Scottish data zone, one England and Wales code the
dimension does not hold, and one small area whose district has no row in `dim_area`.
Those cover every population the module labels, and the last three are the ones the July
2026 release measured at zero, which is why they are fixtures rather than left to a
future release to discover.

The crime frame carries `crime_id`. It is here so the suite shows it is dropped: it is
blank for anti-social behaviour, recurs meaninglessly across Northern Irish rows, and no
fact keys on an incident.

Every row builder takes its defaults as a dict and updates it from `**overrides`, rather
than setting fields as named arguments beside `**overrides`, which raises on the exact
callers that override one of them.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from databricks_src.gold.transforms.crime import (
    ANTI_SOCIAL_BEHAVIOUR,
    ANTI_SOCIAL_COLUMN,
    AREA_COLUMN,
    COMPOSITE_AREA_CODE,
    CRIME_COUNT_COLUMN,
    DISTRICT_NOT_IN_DIM_AREA,
    EXCLUDING_ASB_COLUMN,
    LSOA_COLUMN,
    LSOA_NOT_IN_DIM_LSOA,
    LSOA_OUTSIDE_ENGLAND_WALES,
    MONTH_COLUMN,
    NO_LSOA_CODE,
    POPULATION_COLUMN,
    POPULATIONS,
    RESOLVED,
    RESOLVED_COLUMNS,
    SMALL_AREA_MATCHED_COLUMN,
    SOURCE_MONTH_COLUMN,
    TOTAL_MEASURE_COLUMNS,
    area_levels,
    crime_count,
    is_anti_social_behaviour,
    is_resolved,
    resolve_crime,
    total_measures,
)

ENGLAND = "E92000001"
WALES = "W92000004"
NORTH_EAST = "E12000001"

HARTLEPOOL = "E06000001"
CARDIFF = "W06000015"
UNPUBLISHED_DISTRICT = "E06009999"

HARTLEPOOL_LSOA = "E01012000"
CARDIFF_LSOA = "W01001000"
SCOTTISH_ZONE = "S01019652"
ABSENT_LSOA = "E01099999"
ORPHAN_LSOA = "E01099998"

BURGLARY = "Burglary"
JUNE = date(2015, 6, 1)

POLICE_FIELDS: tuple[str, ...] = ("lsoa_code", "crime_month", "crime_type", "crime_id")
POLICE_SCHEMA = (
    "lsoa_code string, crime_month date, crime_type string, crime_id string"
)

LSOA_SCHEMA = "lsoa_code string, district_code string, has_crime boolean"

SMALL_AREAS = [
    (HARTLEPOOL_LSOA, HARTLEPOOL, True),
    (CARDIFF_LSOA, CARDIFF, True),
    # Held by the dimension, its district absent from dim_area.
    (ORPHAN_LSOA, UNPUBLISHED_DISTRICT, True),
]

AREA_SCHEMA = (
    "area_code string, area_level string, region_code string, nation_code string"
)

AREAS = [
    (HARTLEPOOL, "district", NORTH_EAST, ENGLAND),
    (CARDIFF, "district", None, WALES),
    (NORTH_EAST, "region", NORTH_EAST, ENGLAND),
    (ENGLAND, "nation", None, ENGLAND),
    (COMPOSITE_AREA_CODE, "composite", None, None),
]

# An aggregate at small-area grain, which is what area_levels takes.
AGGREGATE_SCHEMA = (
    "lsoa_code string, month_start_date date, district_code string, "
    "region_code string, nation_code string, crime_count int"
)


def crime_row(**overrides):
    """One Silver crime record: a Hartlepool small area, burglary, June 2015."""
    row = {
        "lsoa_code": HARTLEPOOL_LSOA,
        "crime_month": JUNE,
        "crime_type": BURGLARY,
        "crime_id": "a1b2c3",
    }
    row.update(overrides)
    return row


def crimes(spark, rows):
    return spark.createDataFrame(
        [[row[name] for name in POLICE_FIELDS] for row in rows], POLICE_SCHEMA
    )


def small_areas(spark, rows=None):
    return spark.createDataFrame(SMALL_AREAS if rows is None else rows, LSOA_SCHEMA)


def areas(spark, rows=None):
    return spark.createDataFrame(AREAS if rows is None else rows, AREA_SCHEMA)


def resolve(spark, rows=None, lsoa_rows=None, area_rows=None):
    return resolve_crime(
        crimes(spark, rows or [crime_row()]),
        small_areas(spark, lsoa_rows),
        areas(spark, area_rows),
    )


def one(spark, **overrides):
    return resolve(spark, [crime_row(**overrides)]).collect()[0]


def label(spark, **overrides):
    return one(spark, **overrides)[POPULATION_COLUMN]


def aggregate(spark, rows, *measures):
    """Resolved records aggregated at small-area grain, as the facts do it."""
    return (
        resolve(spark, rows)
        .filter(is_resolved())
        .groupBy(LSOA_COLUMN, MONTH_COLUMN)
        .agg(*measures)
    )


# --------------------------------------------------------------------------- #
# Column contract
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_declared_tuple(spark):
    """RESOLVED_COLUMNS is what the four fact modules read against."""
    assert tuple(resolve(spark).columns) == RESOLVED_COLUMNS


def test_the_incident_identifier_is_not_carried(spark):
    """Blank for anti-social behaviour, repeated across Northern Irish rows, and no
    fact here is keyed on an incident."""
    assert "crime_id" not in resolve(spark).columns


def test_the_join_scaffolding_does_not_reach_the_output(spark):
    assert SMALL_AREA_MATCHED_COLUMN not in resolve(spark).columns


def test_every_label_produced_is_declared(spark):
    rows = [
        crime_row(),
        crime_row(lsoa_code=None),
        crime_row(lsoa_code=SCOTTISH_ZONE),
        crime_row(lsoa_code=ABSENT_LSOA),
        crime_row(lsoa_code=ORPHAN_LSOA),
    ]
    produced = {
        row[POPULATION_COLUMN]
        for row in resolve(spark, rows).select(POPULATION_COLUMN).distinct().collect()
    }
    assert produced == set(POPULATIONS)


def test_nothing_is_dropped(spark):
    """Labelled, then filtered. A record the facts discard still has to be counted."""
    rows = [crime_row(), crime_row(lsoa_code=None), crime_row(lsoa_code=SCOTTISH_ZONE)]
    assert resolve(spark, rows).count() == 3


# --------------------------------------------------------------------------- #
# Populations
# --------------------------------------------------------------------------- #


def test_a_record_in_england_resolves(spark):
    row = one(spark)
    assert row[POPULATION_COLUMN] == RESOLVED
    assert row["district_code"] == HARTLEPOOL
    assert row["region_code"] == NORTH_EAST
    assert row["nation_code"] == ENGLAND


def test_a_record_in_wales_resolves_with_no_region(spark):
    row = one(spark, lsoa_code=CARDIFF_LSOA)
    assert row[POPULATION_COLUMN] == RESOLVED
    assert row["nation_code"] == WALES
    assert row["region_code"] is None


def test_a_record_with_no_small_area_code_is_labelled(spark):
    """Northern Ireland files 2,311,848 of these and places none of them. England and
    Wales forces contribute another 1,428,441 by failing to place records they could."""
    assert label(spark, lsoa_code=None) == NO_LSOA_CODE


def test_a_code_outside_the_england_and_wales_series_is_labelled(spark):
    """Scotland publishes data zones and Northern Ireland super output areas, neither
    of which shares this code series."""
    assert label(spark, lsoa_code=SCOTTISH_ZONE) == LSOA_OUTSIDE_ENGLAND_WALES


def test_a_code_the_dimension_does_not_hold_is_labelled(spark):
    assert label(spark, lsoa_code=ABSENT_LSOA) == LSOA_NOT_IN_DIM_LSOA


def test_a_district_absent_from_the_area_dimension_is_labelled(spark):
    """Told apart from a code the small-area dimension never held, because the fix is
    different: one rebuilds dim_area, the other rebuilds dim_lsoa."""
    assert label(spark, lsoa_code=ORPHAN_LSOA) == DISTRICT_NOT_IN_DIM_AREA


# --------------------------------------------------------------------------- #
# The month key
# --------------------------------------------------------------------------- #


def test_the_month_is_renamed_and_carried_unchanged(spark):
    """Silver parses it from yyyy-MM, so it is already a month start and nothing
    truncates it. 187 months carried zero exceptions on the July 2026 release."""
    row = one(spark, crime_month=date(2011, 3, 1))
    assert row[MONTH_COLUMN] == date(2011, 3, 1)
    assert SOURCE_MONTH_COLUMN not in resolve(spark).columns


# --------------------------------------------------------------------------- #
# Predicates and measures
# --------------------------------------------------------------------------- #


def test_is_resolved_keeps_only_the_resolved_population(spark):
    rows = [crime_row(), crime_row(lsoa_code=None), crime_row(lsoa_code=SCOTTISH_ZONE)]
    kept = resolve(spark, rows).filter(is_resolved()).collect()
    assert [row[LSOA_COLUMN] for row in kept] == [HARTLEPOOL_LSOA]


def test_is_anti_social_behaviour_selects_that_type_alone(spark):
    rows = [crime_row(), crime_row(crime_type=ANTI_SOCIAL_BEHAVIOUR)]
    kept = resolve(spark, rows).filter(is_anti_social_behaviour()).collect()
    assert [row["crime_type"] for row in kept] == [ANTI_SOCIAL_BEHAVIOUR]


def test_the_crime_count_is_an_int_named_for_the_column(spark):
    counted = aggregate(spark, [crime_row(), crime_row()], crime_count())
    assert dict(counted.dtypes)[CRIME_COUNT_COLUMN] == "int"
    assert counted.collect()[0][CRIME_COUNT_COLUMN] == 2


def test_the_total_measures_are_named_and_ordered_as_declared(spark):
    totals = aggregate(spark, [crime_row()], *total_measures())
    assert tuple(totals.columns)[2:] == TOTAL_MEASURE_COLUMNS
    types = dict(totals.dtypes)
    for name in TOTAL_MEASURE_COLUMNS:
        assert types[name] == "int", name


def test_the_two_totals_split_a_cell(spark):
    rows = [crime_row(), crime_row(), crime_row(crime_type=ANTI_SOCIAL_BEHAVIOUR)]
    row = aggregate(spark, rows, *total_measures()).collect()[0]
    assert row[EXCLUDING_ASB_COLUMN] == 2
    assert row[ANTI_SOCIAL_COLUMN] == 1


def test_a_cell_of_nothing_but_anti_social_behaviour_counts_a_real_zero(spark):
    """102,735 small-area cells are this. Counted in one aggregate rather than joined,
    so the zero is a count and not an absent join partner."""
    rows = [crime_row(crime_type=ANTI_SOCIAL_BEHAVIOUR)]
    row = aggregate(spark, rows, *total_measures()).collect()[0]
    assert row[EXCLUDING_ASB_COLUMN] == 0
    assert row[ANTI_SOCIAL_COLUMN] == 1


def test_a_cell_with_no_anti_social_behaviour_counts_a_real_zero(spark):
    """1,272,887 small-area cells are this, which is the other side a join would lose."""
    row = aggregate(spark, [crime_row()], *total_measures()).collect()[0]
    assert row[EXCLUDING_ASB_COLUMN] == 1
    assert row[ANTI_SOCIAL_COLUMN] == 0


# --------------------------------------------------------------------------- #
# Levels
# --------------------------------------------------------------------------- #


def aggregated_frame(spark, rows):
    return spark.createDataFrame(rows, AGGREGATE_SCHEMA)


def test_an_english_small_area_rolls_up_to_four_levels(spark):
    frame = aggregated_frame(
        spark, [(HARTLEPOOL_LSOA, JUNE, HARTLEPOOL, NORTH_EAST, ENGLAND, 7)]
    )
    exploded = area_levels(frame).collect()
    assert sorted(row[AREA_COLUMN] for row in exploded) == sorted(
        [HARTLEPOOL, NORTH_EAST, ENGLAND, COMPOSITE_AREA_CODE]
    )


def test_a_welsh_small_area_rolls_up_to_three_levels(spark):
    frame = aggregated_frame(spark, [(CARDIFF_LSOA, JUNE, CARDIFF, None, WALES, 4)])
    exploded = area_levels(frame).collect()
    assert sorted(row[AREA_COLUMN] for row in exploded) == sorted(
        [CARDIFF, WALES, COMPOSITE_AREA_CODE]
    )


def test_no_exploded_row_carries_a_null_area(spark):
    frame = aggregated_frame(
        spark,
        [
            (HARTLEPOOL_LSOA, JUNE, HARTLEPOOL, NORTH_EAST, ENGLAND, 7),
            (CARDIFF_LSOA, JUNE, CARDIFF, None, WALES, 4),
        ],
    )
    assert area_levels(frame).filter(F.col(AREA_COLUMN).isNull()).count() == 0


def test_the_measure_survives_the_explode(spark):
    """area_levels takes an aggregate, so the count has to reach every level intact for
    the sum above it to be the sum of the records."""
    frame = aggregated_frame(
        spark, [(HARTLEPOOL_LSOA, JUNE, HARTLEPOOL, NORTH_EAST, ENGLAND, 7)]
    )
    assert {row[CRIME_COUNT_COLUMN] for row in area_levels(frame).collect()} == {7}


def test_summing_the_explode_gives_the_same_total_at_every_level(spark):
    """A count is additive, which is what makes the rollup legitimate. Two small areas
    in one district sum to that district and to the composite alike."""
    frame = aggregated_frame(
        spark,
        [
            (HARTLEPOOL_LSOA, JUNE, HARTLEPOOL, NORTH_EAST, ENGLAND, 7),
            ("E01012001", JUNE, HARTLEPOOL, NORTH_EAST, ENGLAND, 5),
        ],
    )
    summed = {
        row[AREA_COLUMN]: row["total"]
        for row in area_levels(frame)
        .groupBy(AREA_COLUMN)
        .agg(F.sum(CRIME_COUNT_COLUMN).alias("total"))
        .collect()
    }
    assert summed[HARTLEPOOL] == 12
    assert summed[COMPOSITE_AREA_CODE] == 12


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = crimes(spark, [crime_row()]).drop("crime_type")
    with pytest.raises(ValueError, match="crime_type"):
        resolve_crime(frame, small_areas(spark), areas(spark))


def test_a_missing_small_area_column_aborts(spark):
    """The mistake this catches is passing the frame that produced dim_lsoa rather than
    the loaded table."""
    frame = small_areas(spark).drop("district_code")
    with pytest.raises(ValueError, match="dim_lsoa"):
        resolve_crime(crimes(spark, [crime_row()]), frame, areas(spark))


def test_a_missing_dimension_column_aborts(spark):
    frame = areas(spark).drop("nation_code")
    with pytest.raises(ValueError, match="dim_area"):
        resolve_crime(crimes(spark, [crime_row()]), small_areas(spark), frame)


def test_an_extra_column_on_any_input_is_accepted(spark):
    """One direction only. Silver or a dimension adding a column is not this module's
    problem."""
    frame = crimes(spark, [crime_row()]).withColumn("context", F.lit("note"))
    assert resolve_crime(frame, small_areas(spark), areas(spark)).count() == 1


def test_a_repeated_small_area_in_the_dimension_aborts(spark):
    """A repeat fans one record into several and multiplies every count in all four
    facts, and nothing downstream carries an incident identity that would show it."""
    lsoa_rows = SMALL_AREAS + [(HARTLEPOOL_LSOA, CARDIFF, True)]
    with pytest.raises(ValueError, match="grain broken"):
        resolve(spark, lsoa_rows=lsoa_rows).count()


def test_a_repeated_district_in_the_area_dimension_aborts(spark):
    area_rows = AREAS + [(HARTLEPOOL, "district", NORTH_EAST, ENGLAND)]
    with pytest.raises(ValueError, match="grain broken"):
        resolve(spark, area_rows=area_rows).count()


def test_a_composite_code_published_at_another_level_aborts(spark):
    """Two levels sharing a code count one record twice inside one group, which
    produces no duplicate key for anything downstream to catch."""
    area_rows = AREAS + [(COMPOSITE_AREA_CODE, "district", None, ENGLAND)]
    with pytest.raises(ValueError, match=COMPOSITE_AREA_CODE):
        resolve(spark, area_rows=area_rows).count()


def test_the_resolution_is_deterministic(spark):
    rows = [crime_row(), crime_row(lsoa_code=CARDIFF_LSOA), crime_row(lsoa_code=None)]
    assert_df_equality(
        resolve(spark, rows).orderBy(POPULATION_COLUMN, LSOA_COLUMN),
        resolve(spark, rows).orderBy(POPULATION_COLUMN, LSOA_COLUMN),
        ignore_nullable=True,
    )
