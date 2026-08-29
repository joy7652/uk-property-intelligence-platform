"""Tests for the shared Gold transaction resolution.

Every test needs a session. The module is joins and a labelled case expression, so there
is no pure-Python half to check separately.

The fixtures are a miniature country: one English district carrying a region, one Welsh
district carrying none, one Scottish district a border postcode reaches, one postcode the
directory holds without a district, and one postcode whose district has no row in the
dimension. Those five cover every population the module labels, and the last two are the
ones the July 2026 release measured at zero, which is exactly why they are fixtures
rather than left to a future release to discover.

The transaction frame carries `district`, the district recorded on the sale. It is here
so the suite shows it is dropped: the whole point of the resolution is that geography
comes from the postcode, and a frame holding only the columns the module reads would
never demonstrate that.

Every row builder takes its defaults as a dict and updates it from `**overrides`, rather
than setting fields as named arguments beside `**overrides`, which raises on the exact
callers that override one of them.
"""

from __future__ import annotations

from datetime import date

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from databricks_src.gold.transforms.transactions import (
    COMPOSITE_AREA_CODE,
    DISTRICT_NOT_IN_DIM_AREA,
    DISTRICT_OUTSIDE_ENGLAND_WALES,
    FULL_MARKET_VALUE,
    NO_DISTRICT_ON_POSTCODE,
    NO_POSTCODE,
    DISTRICT_COLUMN,
    POPULATION_COLUMN,
    POPULATIONS,
    POSTCODE_MATCHED_COLUMN,
    POSTCODE_NOT_IN_DIRECTORY,
    PRICE_MEASURE_COLUMNS,
    RESOLVED,
    RESOLVED_COLUMNS,
    SALE_ATTRIBUTE_COLUMNS,
    TRANSACTION_COUNT_COLUMN,
    area_levels,
    is_full_market_value,
    is_resolved,
    price_measures,
    resolve_transactions,
    transaction_count,
)

ENGLAND = "E92000001"
WALES = "W92000004"
SCOTLAND = "S92000003"

NORTH_EAST = "E12000001"
HARTLEPOOL = "E06000001"
CARDIFF = "W06000015"
SCOTTISH_BORDERS = "S12000026"
UNPUBLISHED_DISTRICT = "E06009999"

HARTLEPOOL_POSTCODE = "TS24 8AA"
CARDIFF_POSTCODE = "CF10 1AA"
BORDERS_POSTCODE = "TD15 1SZ"
FORCES_POSTCODE = "BF1 1AA"
UNPUBLISHED_POSTCODE = "ZZ1 1ZZ"
ABSENT_POSTCODE = "XX9 9XX"

HARTLEPOOL_LSOA = "E01012000"
CARDIFF_LSOA = "W01001000"
BORDERS_LSOA = "S01019652"

PPD_FIELDS: tuple[str, ...] = (
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "ppd_category_type",
    "district",
)

PPD_SCHEMA = (
    "price int, date_of_transfer date, postcode string, property_type string, "
    "old_new string, duration string, ppd_category_type string, district string"
)

DIRECTORY_SCHEMA = "postcode string, district_code string, lsoa_code_2021 string"

DIRECTORY = [
    (HARTLEPOOL_POSTCODE, HARTLEPOOL, HARTLEPOOL_LSOA),
    (CARDIFF_POSTCODE, CARDIFF, CARDIFF_LSOA),
    (BORDERS_POSTCODE, SCOTTISH_BORDERS, BORDERS_LSOA),
    # British Forces Post Office: held by the directory, non-geographic by design.
    (FORCES_POSTCODE, None, None),
    (UNPUBLISHED_POSTCODE, UNPUBLISHED_DISTRICT, "E01099999"),
]

AREA_SCHEMA = "area_code string, area_level string, region_code string, nation_code string"

AREAS = [
    (HARTLEPOOL, "district", NORTH_EAST, ENGLAND),
    (CARDIFF, "district", None, WALES),
    (SCOTTISH_BORDERS, "district", None, SCOTLAND),
    (NORTH_EAST, "region", NORTH_EAST, ENGLAND),
    (ENGLAND, "nation", None, ENGLAND),
    (COMPOSITE_AREA_CODE, "composite", None, None),
]


def ppd_row(**overrides):
    """One Silver transaction: Hartlepool, category A, June 2015."""
    row = {
        "price": 250_000,
        "date_of_transfer": date(2015, 6, 15),
        "postcode": HARTLEPOOL_POSTCODE,
        "property_type": "D",
        "old_new": "N",
        "duration": "F",
        "ppd_category_type": FULL_MARKET_VALUE,
        "district": "HARTLEPOOL",
    }
    row.update(overrides)
    return row


def transactions(spark, rows):
    return spark.createDataFrame(
        [[row[name] for name in PPD_FIELDS] for row in rows], PPD_SCHEMA
    )


def directory(spark, rows=None):
    return spark.createDataFrame(DIRECTORY if rows is None else rows, DIRECTORY_SCHEMA)


def areas(spark, rows=None):
    return spark.createDataFrame(AREAS if rows is None else rows, AREA_SCHEMA)


def resolve(spark, rows=None, directory_rows=None, area_rows=None):
    return resolve_transactions(
        transactions(spark, rows or [ppd_row()]),
        directory(spark, directory_rows),
        areas(spark, area_rows),
    )


def one(spark, **overrides):
    return resolve(spark, [ppd_row(**overrides)]).collect()[0]


def label(spark, **overrides):
    return one(spark, **overrides)[POPULATION_COLUMN]


# --------------------------------------------------------------------------- #
# Column contract
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_declared_tuple(spark):
    """RESOLVED_COLUMNS is what the three fact modules read against. The select is
    written out longhand, so nothing but this stops the two drifting apart."""
    assert tuple(resolve(spark).columns) == RESOLVED_COLUMNS


def test_the_recorded_district_is_not_carried(spark):
    """Geography comes from the postcode. Carrying the recorded district would leave two
    answers in the frame and no rule saying which one a fact reads."""
    assert "district" not in resolve(spark).columns


def test_the_join_scaffolding_does_not_reach_the_output(spark):
    assert POSTCODE_MATCHED_COLUMN not in resolve(spark).columns


def test_every_label_produced_is_declared(spark):
    """POPULATIONS is what a load iterates to record a zero for a population that did
    not occur, so a label outside it would never be counted."""
    rows = [
        ppd_row(),
        ppd_row(postcode=None),
        ppd_row(postcode=ABSENT_POSTCODE),
        ppd_row(postcode=FORCES_POSTCODE),
        ppd_row(postcode=UNPUBLISHED_POSTCODE),
        ppd_row(postcode=BORDERS_POSTCODE),
    ]
    produced = {
        row[POPULATION_COLUMN]
        for row in resolve(spark, rows).select(POPULATION_COLUMN).distinct().collect()
    }
    assert produced == set(POPULATIONS)


def test_nothing_is_dropped(spark):
    """Labelled, not filtered. A row the facts discard still has to be counted."""
    rows = [ppd_row(), ppd_row(postcode=None), ppd_row(postcode=BORDERS_POSTCODE)]
    assert resolve(spark, rows).count() == 3


# --------------------------------------------------------------------------- #
# Populations
# --------------------------------------------------------------------------- #


def test_a_transaction_in_england_resolves(spark):
    row = one(spark)
    assert row[POPULATION_COLUMN] == RESOLVED
    assert row["district_code"] == HARTLEPOOL
    assert row["region_code"] == NORTH_EAST
    assert row["nation_code"] == ENGLAND
    assert row["lsoa_code"] == HARTLEPOOL_LSOA


def test_a_transaction_in_wales_resolves_with_no_region(spark):
    """Only England is divided into regions, so the null is the dimension saying so."""
    row = one(spark, postcode=CARDIFF_POSTCODE)
    assert row[POPULATION_COLUMN] == RESOLVED
    assert row["nation_code"] == WALES
    assert row["region_code"] is None


def test_a_missing_postcode_is_labelled(spark):
    assert label(spark, postcode=None) == NO_POSTCODE


def test_a_blank_postcode_is_labelled_as_missing(spark):
    """Whitespace is absence written differently. Left to the directory branch it would
    be reported as a postcode the directory does not hold, which is a different fault."""
    assert label(spark, postcode="   ") == NO_POSTCODE


def test_a_postcode_the_directory_does_not_hold_is_labelled(spark):
    assert label(spark, postcode=ABSENT_POSTCODE) == POSTCODE_NOT_IN_DIRECTORY


def test_a_postcode_with_no_district_is_labelled(spark):
    """The directory holds it and gives it no geography, which is what a British Forces
    Post Office postcode looks like. Told apart from one the directory never held."""
    assert label(spark, postcode=FORCES_POSTCODE) == NO_DISTRICT_ON_POSTCODE


def test_a_district_absent_from_the_dimension_is_labelled(spark):
    assert label(spark, postcode=UNPUBLISHED_POSTCODE) == DISTRICT_NOT_IN_DIM_AREA


def test_a_district_outside_england_and_wales_is_labelled(spark):
    """A postcode unit lying across the Anglo-Scottish border is assigned whole to one
    district, so a Berwick-upon-Tweed sale resolves into Scottish Borders."""
    assert label(spark, postcode=BORDERS_POSTCODE) == DISTRICT_OUTSIDE_ENGLAND_WALES


# --------------------------------------------------------------------------- #
# Period keys
# --------------------------------------------------------------------------- #


def test_the_month_and_the_year_are_truncated_from_the_transfer_date(spark):
    row = one(spark, date_of_transfer=date(2015, 6, 15))
    assert row["month_start_date"] == date(2015, 6, 1)
    assert row["year_start_date"] == date(2015, 1, 1)


def test_the_first_of_a_month_is_its_own_month_start(spark):
    row = one(spark, date_of_transfer=date(2015, 6, 1))
    assert row["month_start_date"] == date(2015, 6, 1)


def test_the_sale_attributes_survive(spark):
    row = one(spark, property_type="F", old_new="Y", duration="L", ppd_category_type="B")
    assert [row[name] for name in SALE_ATTRIBUTE_COLUMNS] == ["F", "Y", "L", "B"]


# --------------------------------------------------------------------------- #
# Predicates
# --------------------------------------------------------------------------- #


def test_is_resolved_keeps_only_the_resolved_population(spark):
    rows = [ppd_row(), ppd_row(postcode=BORDERS_POSTCODE), ppd_row(postcode=None)]
    kept = resolve(spark, rows).filter(is_resolved()).collect()
    assert [row["district_code"] for row in kept] == [HARTLEPOOL]


def test_is_full_market_value_keeps_category_a(spark):
    rows = [ppd_row(), ppd_row(ppd_category_type="B")]
    kept = resolve(spark, rows).filter(is_full_market_value()).collect()
    assert [row["ppd_category_type"] for row in kept] == [FULL_MARKET_VALUE]


# --------------------------------------------------------------------------- #
# Shared measures
# --------------------------------------------------------------------------- #


def aggregated(spark, measures):
    return resolve(spark).filter(is_resolved()).groupBy(DISTRICT_COLUMN).agg(*measures)


def test_the_price_measures_match_the_declared_tuple(spark):
    """Both price facts build their aggregate from price_measures and their projection
    from PRICE_MEASURE_COLUMNS. A rename in one of the two alone would load values into
    the wrong columns without failing anywhere."""
    assert tuple(aggregated(spark, price_measures()).columns)[1:] == PRICE_MEASURE_COLUMNS


def test_the_price_measures_are_whole_pounds_and_a_count(spark):
    types = dict(aggregated(spark, price_measures()).dtypes)
    for name in PRICE_MEASURE_COLUMNS:
        assert types[name] == "int", name


def test_the_count_carries_one_name_for_all_three_facts(spark):
    """The reconciliation between the price fact and the mix fact holds only if both
    count the same way, so both read this definition rather than their own."""
    counted = aggregated(spark, [transaction_count()])
    assert counted.columns[1] == TRANSACTION_COUNT_COLUMN
    assert counted.collect()[0][TRANSACTION_COUNT_COLUMN] == 1


# --------------------------------------------------------------------------- #
# Levels
# --------------------------------------------------------------------------- #


def test_an_english_transaction_counts_at_four_levels(spark):
    exploded = area_levels(resolve(spark).filter(is_resolved()))
    assert sorted(row["area_code"] for row in exploded.collect()) == sorted(
        [HARTLEPOOL, NORTH_EAST, ENGLAND, COMPOSITE_AREA_CODE]
    )


def test_a_welsh_transaction_counts_at_three_levels(spark):
    """The null region is dropped rather than counted, so Wales contributes three rows
    and no area_code is ever null."""
    resolved = resolve(spark, [ppd_row(postcode=CARDIFF_POSTCODE)]).filter(is_resolved())
    exploded = area_levels(resolved)
    assert sorted(row["area_code"] for row in exploded.collect()) == sorted(
        [CARDIFF, WALES, COMPOSITE_AREA_CODE]
    )


def test_no_exploded_row_carries_a_null_area(spark):
    rows = [ppd_row(), ppd_row(postcode=CARDIFF_POSTCODE)]
    exploded = area_levels(resolve(spark, rows).filter(is_resolved()))
    assert exploded.filter(F.col("area_code").isNull()).count() == 0


def test_the_explode_carries_the_measures_and_attributes(spark):
    """Both area facts read price and the four attributes off the exploded frame, so a
    level that lost them would aggregate nothing."""
    exploded = area_levels(resolve(spark).filter(is_resolved()))
    row = exploded.filter(F.col("area_code") == ENGLAND).collect()[0]
    assert row["price"] == 250_000
    assert row["month_start_date"] == date(2015, 6, 1)
    assert [row[name] for name in SALE_ATTRIBUTE_COLUMNS] == ["D", "N", "F", "A"]


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_transaction_column_aborts_naming_it(spark):
    frame = transactions(spark, [ppd_row()]).drop("price")
    with pytest.raises(ValueError, match="price"):
        resolve_transactions(frame, directory(spark), areas(spark))


def test_a_missing_directory_column_aborts_naming_it(spark):
    frame = directory(spark).drop("lsoa_code_2021")
    with pytest.raises(ValueError, match="lsoa_code_2021"):
        resolve_transactions(transactions(spark, [ppd_row()]), frame, areas(spark))


def test_a_missing_dimension_column_aborts(spark):
    """The mistake this catches is passing the frame that produced dim_area rather than
    the loaded table."""
    frame = areas(spark).drop("nation_code")
    with pytest.raises(ValueError, match="dim_area"):
        resolve_transactions(transactions(spark, [ppd_row()]), directory(spark), frame)


def test_an_extra_column_on_any_input_is_accepted(spark):
    """One direction only. Silver or the dimension adding a column is not this module's
    problem."""
    frame = transactions(spark, [ppd_row()]).withColumn("tuid", F.lit("{X}"))
    assert resolve_transactions(frame, directory(spark), areas(spark)).count() == 1


def test_a_repeated_postcode_in_the_directory_aborts(spark):
    """A repeat fans one transaction into several and multiplies every count and every
    median weight in all three facts, and nothing downstream carries the transaction
    identity that would show it."""
    directory_rows = DIRECTORY + [(HARTLEPOOL_POSTCODE, CARDIFF, CARDIFF_LSOA)]
    with pytest.raises(ValueError, match="grain broken"):
        resolve(spark, directory_rows=directory_rows).count()


def test_a_repeated_district_in_the_dimension_aborts(spark):
    area_rows = AREAS + [(HARTLEPOOL, "district", NORTH_EAST, ENGLAND)]
    with pytest.raises(ValueError, match="grain broken"):
        resolve(spark, area_rows=area_rows).count()


def test_a_district_with_no_nation_aborts(spark):
    """The label reads a null nation as a district the dimension does not hold, so this
    would drop real rows under a name for the wrong cause."""
    area_rows = [
        (HARTLEPOOL, "district", NORTH_EAST, None),
        (NORTH_EAST, "region", NORTH_EAST, ENGLAND),
    ]
    with pytest.raises(ValueError, match="carrying no nation"):
        resolve(spark, area_rows=area_rows).count()


def test_a_composite_code_colliding_with_a_district_aborts(spark):
    """Two levels sharing a code count one transaction twice inside one group, which
    produces no duplicate key for anything downstream to catch."""
    area_rows = AREAS + [(COMPOSITE_AREA_CODE, "district", None, ENGLAND)]
    with pytest.raises(ValueError, match=COMPOSITE_AREA_CODE):
        resolve(spark, area_rows=area_rows).count()


def test_a_dimension_with_no_districts_still_resolves_the_populations(spark):
    """An empty district lookup is a legitimate frame. Every transaction then lands in
    the population naming the dimension rather than failing the join."""
    area_rows = [(ENGLAND, "nation", None, ENGLAND)]
    assert (
        resolve(spark, area_rows=area_rows).collect()[0][POPULATION_COLUMN]
        == DISTRICT_NOT_IN_DIM_AREA
    )


def test_the_resolution_is_deterministic(spark):
    rows = [ppd_row(), ppd_row(postcode=CARDIFF_POSTCODE), ppd_row(postcode=None)]
    assert_df_equality(
        resolve(spark, rows).orderBy(POPULATION_COLUMN, "district_code"),
        resolve(spark, rows).orderBy(POPULATION_COLUMN, "district_code"),
        ignore_nullable=True,
    )
