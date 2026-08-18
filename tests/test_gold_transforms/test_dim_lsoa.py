"""Tests for the Gold small-area dimension.

The fixtures are a miniature England and Wales. One area sits wholly in one district,
one straddles two at a decisive four-to-one, one is exclusive to the 2011 boundaries
and carries crime only, one is exclusive to 2021 and carries a price only, and one is
Welsh. Small enough to count by hand, wide enough that every assignment, vintage and
membership branch appears.

Postcode counts are what settle a district, so the directory fixture is built per
postcode rather than per area, and a test that changes an assignment does it by moving
postcodes.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from chispa import assert_df_equality

from databricks_src.gold.transforms.dim_lsoa import (
    ASSIGNMENTS,
    BOTH,
    EXACT,
    GOLD_COLUMNS,
    MAJORITY,
    NATION_BY_INITIAL,
    ONLY_2011,
    ONLY_2021,
    VINTAGES,
    assert_districts_conform,
    assert_maps_consistent,
    measure_small_areas,
    transform_dim_lsoa,
)

ENGLAND = "E92000001"
WALES = "W92000004"

BIRMINGHAM = "E08000025"
DUDLEY = "E08000027"
CARDIFF = "W06000015"

WHOLLY_IN_ONE = "E01000001"  # every postcode in Birmingham
STRADDLES = "E01000002"  # four postcodes in Birmingham, one in Dudley
GONE_IN_2021 = "E01000003"  # 2011 only, crime and no price
NEW_IN_2021 = "E01000004"  # 2021 only, price and no crime
WELSH = "W01000005"

# (postcode, district, lsoa_2011, name_2011, lsoa_2021, name_2021)
DIRECTORY = [
    ("B1 1AA", BIRMINGHAM, WHOLLY_IN_ONE, "Birmingham 001A",
     WHOLLY_IN_ONE, "Birmingham 001A"),
    ("B1 1AB", BIRMINGHAM, WHOLLY_IN_ONE, "Birmingham 001A",
     WHOLLY_IN_ONE, "Birmingham 001A"),
    ("B2 2AA", BIRMINGHAM, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
    ("B2 2AB", BIRMINGHAM, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
    ("B2 2AC", BIRMINGHAM, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
    ("B2 2AD", BIRMINGHAM, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
    ("DY1 1AA", DUDLEY, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
    ("B3 3AA", BIRMINGHAM, GONE_IN_2021, "Birmingham 003A", None, None),
    ("B4 4AA", BIRMINGHAM, None, None, NEW_IN_2021, "Birmingham 004A"),
    ("CF10 1AA", CARDIFF, WELSH, "Caerdydd 005A", WELSH, "Cardiff 005A"),
]

CRIME = [WHOLLY_IN_ONE, STRADDLES, GONE_IN_2021, WELSH]

# STRADDLES divided two and two. No measured area splits this evenly, since all 82 of
# them hold at least 92 percent in one district, so this only ever exercises the
# tie-break and the guard that refuses to record one as a majority.
EVEN_SPLIT = [
    ("B2 2AA", BIRMINGHAM, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
    ("B2 2AB", BIRMINGHAM, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
    ("DY1 1AA", DUDLEY, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
    ("DY1 1AB", DUDLEY, STRADDLES, "Birmingham 002A", STRADDLES, "Birmingham 002A"),
]
TRANSACTIONS = ["B1 1AA", "B2 2AA", "B4 4AA", "CF10 1AA"]

DOOGAL_SCHEMA = (
    "postcode string, district_code string, lsoa_code_2011 string, "
    "lsoa_name_2011 string, lsoa_code_2021 string, lsoa_name_2021 string"
)
POLICE_SCHEMA = "lsoa_code string"
PPD_SCHEMA = "postcode string"

AREA_SCHEMA = "area_code string"
DISTRICTS = [(BIRMINGHAM,), (DUDLEY,), (CARDIFF,)]


def sources(spark, directory=None, crime=None, transactions=None):
    """The three Silver frames, each overridable one at a time."""
    return (
        spark.createDataFrame(DIRECTORY if directory is None else directory, DOOGAL_SCHEMA),
        spark.createDataFrame(
            [(code,) for code in (CRIME if crime is None else crime)], POLICE_SCHEMA
        ),
        spark.createDataFrame(
            [(pc,) for pc in (TRANSACTIONS if transactions is None else transactions)],
            PPD_SCHEMA,
        ),
    )


def dimension(spark, **kwargs):
    return transform_dim_lsoa(measure_small_areas(*sources(spark, **kwargs)))


def loaded(spark, **kwargs):
    return {row["lsoa_code"]: row.asDict() for row in dimension(spark, **kwargs).collect()}


# --------------------------------------------------------------------------- #
# Authored values
# --------------------------------------------------------------------------- #


def test_maps_are_consistent():
    assert_maps_consistent()


def test_vocabularies_match_the_table():
    """Both are constrained in the DDL, so a value here the table rejects would abort
    the write rather than this module."""
    assert set(ASSIGNMENTS) == {"exact", "majority"}
    assert set(VINTAGES) == {"both", "only_2011", "only_2021"}


def test_only_england_and_wales_have_a_nation():
    """Scotland publishes data zones and Northern Ireland super output areas, neither
    on this code series."""
    assert set(NATION_BY_INITIAL) == {"E", "W"}


# --------------------------------------------------------------------------- #
# Shape and membership
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    assert tuple(dimension(spark).columns) == GOLD_COLUMNS


def test_one_row_per_area(spark):
    rows = dimension(spark)
    assert rows.count() == rows.select("lsoa_code").distinct().count()


def test_membership_is_crime_or_price(spark):
    """Every area either source reaches, and nothing else."""
    assert set(loaded(spark)) == {
        WHOLLY_IN_ONE,
        STRADDLES,
        GONE_IN_2021,
        NEW_IN_2021,
        WELSH,
    }


def test_an_area_the_directory_knows_but_neither_source_reaches_is_left_out(spark):
    """A dimension row with no fact behind it is a row nothing joins to."""
    directory = DIRECTORY + [
        ("B9 9AA", BIRMINGHAM, "E01000099", "Birmingham 099A",
         "E01000099", "Birmingham 099A")
    ]
    assert "E01000099" not in loaded(spark, directory=directory)


def test_membership_flags_follow_the_two_sources(spark):
    rows = loaded(spark)
    assert rows[WHOLLY_IN_ONE]["has_crime"] and rows[WHOLLY_IN_ONE]["has_price"]
    assert rows[GONE_IN_2021]["has_crime"] and not rows[GONE_IN_2021]["has_price"]
    assert rows[NEW_IN_2021]["has_price"] and not rows[NEW_IN_2021]["has_crime"]


def test_a_transaction_resolves_through_the_2021_code(spark):
    """The transaction fixture carries postcodes, not area codes, so this is the
    directory lookup working."""
    assert loaded(spark)[NEW_IN_2021]["has_price"]


# --------------------------------------------------------------------------- #
# District assignment
# --------------------------------------------------------------------------- #


def test_an_area_wholly_in_one_district_is_exact(spark):
    row = loaded(spark)[WHOLLY_IN_ONE]
    assert row["district_code"] == BIRMINGHAM
    assert row["district_assignment"] == EXACT
    assert row["majority_share"] == Decimal("1.0000")


def test_a_straddling_area_takes_the_district_holding_most_of_it(spark):
    """Four postcodes in Birmingham against one in Dudley."""
    row = loaded(spark)[STRADDLES]
    assert row["district_code"] == BIRMINGHAM
    assert row["district_assignment"] == MAJORITY
    assert row["majority_share"] == Decimal("0.8000")


def test_moving_the_majority_moves_the_district(spark):
    """The assignment is measured, not fixed. Reassigning three Birmingham postcodes to
    Dudley flips it."""
    directory = [
        (pc, DUDLEY if pc in ("B2 2AA", "B2 2AB", "B2 2AC") else district, *rest)
        for pc, district, *rest in DIRECTORY
    ]
    assert loaded(spark, directory=directory)[STRADDLES]["district_code"] == DUDLEY


def test_a_tie_is_broken_on_district_code(spark):
    """Asserted on the measure rather than the dimension, because the transform refuses
    to record a tie at all. What matters here is that the ranking settles it the same
    way every time: one resolved by shuffle order would move between releases and take
    a whole area's crime with it."""
    measured = measure_small_areas(
        *sources(spark, directory=EVEN_SPLIT, crime=[STRADDLES], transactions=[])
    ).collect()
    assert [row["district_code"] for row in measured] == [min(BIRMINGHAM, DUDLEY)]


def test_exact_share_is_written_rather_than_divided(spark):
    """The table requires exactly 1.0 for an exact assignment, and a ratio rounding up
    to 1.0000 from just below would pass while claiming the area does not straddle."""
    assert loaded(spark)[WHOLLY_IN_ONE]["majority_share"] == Decimal("1.0000")


def test_share_carries_the_scale_the_table_declares(spark):
    """Cast here rather than left to the insert, so the stored value is deterministic
    instead of whatever rounding the write chose."""
    assert loaded(spark)[STRADDLES]["majority_share"].as_tuple().exponent == -4


@pytest.mark.parametrize("code", [WHOLLY_IN_ONE, STRADDLES, GONE_IN_2021, WELSH])
def test_share_and_assignment_agree(spark, code):
    """The table's own share_matches_assignment constraint, asserted on the frame."""
    row = loaded(spark)[code]
    if row["district_assignment"] == EXACT:
        assert row["majority_share"] == Decimal("1.0000")
    else:
        assert Decimal("0.5") < row["majority_share"] < Decimal("1.0")


# --------------------------------------------------------------------------- #
# Vintage and nation
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("code, vintage", [
    (WHOLLY_IN_ONE, BOTH),
    (STRADDLES, BOTH),
    (GONE_IN_2021, ONLY_2011),
    (NEW_IN_2021, ONLY_2021),
    (WELSH, BOTH),
])
def test_vintage_follows_which_columns_carry_the_code(spark, code, vintage):
    assert loaded(spark)[code]["boundary_vintage"] == vintage


def test_a_2011_exclusive_code_carries_crime_and_no_price(spark):
    """Transactions are attributed to 2021 codes only, which is why the pairing holds."""
    row = loaded(spark)[GONE_IN_2021]
    assert row["boundary_vintage"] == ONLY_2011
    assert row["has_crime"] and not row["has_price"]


def test_the_2021_name_wins_where_a_code_has_both(spark):
    """The 2021 revision was largely a renaming exercise in Wales, so the newer name is
    the substantive one."""
    assert loaded(spark)[WELSH]["lsoa_name"] == "Cardiff 005A"


def test_the_2011_name_stands_in_where_there_is_no_2021_one(spark):
    assert loaded(spark)[GONE_IN_2021]["lsoa_name"] == "Birmingham 003A"


@pytest.mark.parametrize("code, nation", [
    (WHOLLY_IN_ONE, ENGLAND),
    (WELSH, WALES),
])
def test_nation_comes_from_the_code_initial(spark, code, nation):
    assert loaded(spark)[code]["nation_code"] == nation


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_source_column_aborts(spark):
    directory, crime, ppd = sources(spark)
    with pytest.raises(ValueError, match="missing columns it reads"):
        measure_small_areas(directory.drop("lsoa_code_2021"), crime, ppd)


def test_transform_rejects_a_frame_that_is_not_the_measured_one(spark):
    _, crime, _ = sources(spark)
    with pytest.raises(ValueError, match="missing columns it reads"):
        transform_dim_lsoa(crime)


def test_a_scottish_code_reaching_membership_by_price_is_excluded(spark):
    """Transactions are the route this actually happens by. A postcode unit lying
    across the Anglo-Scottish border is assigned whole to one district, so an England
    and Wales sale can resolve to a Scottish data zone. The filter sits on the
    assembled membership for that reason, not on either source."""
    directory = DIRECTORY + [
        ("TD15 1SZ", "S12000026", "S01019652", "Scottish Borders 001",
         "S01019652", "Scottish Borders 001"),
    ]
    rows = loaded(
        spark, directory=directory, transactions=TRANSACTIONS + ["TD15 1SZ"]
    )
    assert "S01019652" not in rows


def test_a_scottish_code_in_the_crime_source_is_excluded(spark):
    """The measured crime source publishes England and Wales codes only, so this covers
    the filter rather than a population that exists. Kept because the guard behind it
    is the reason a code from another nation would otherwise reach a NOT NULL column."""
    rows = loaded(spark, crime=CRIME + ["S01000006"])
    assert "S01000006" not in rows
    assert set(rows) == {WHOLLY_IN_ONE, STRADDLES, GONE_IN_2021, NEW_IN_2021, WELSH}


def test_a_code_reaching_the_measured_frame_by_another_route_aborts(spark):
    """The guard is a backstop behind that filter. Injected directly, because no
    supported input can produce this any more, which is the point of keeping it."""
    measured = measure_small_areas(*sources(spark))
    intruder = spark.createDataFrame(
        [("S01000006", "Scottish Borders 001", "S12000026", 4, 4, True, True, True, False)],
        measured.schema,
    )
    with pytest.raises(ValueError, match="outside the England and Wales series"):
        transform_dim_lsoa(measured.unionByName(intruder))


def test_a_published_code_the_directory_misses_aborts(spark):
    """No postcodes means no district and no share, and both are NOT NULL."""
    with pytest.raises(ValueError, match="reach no postcode"):
        dimension(spark, crime=CRIME + ["E01099999"])


def test_an_even_split_aborts(spark):
    """A coin toss recorded as a measurement. The tie-break picks a district and the
    share lands on exactly one half, which the table refuses and so does this."""
    with pytest.raises(ValueError, match="half their postcodes or fewer"):
        dimension(spark, directory=EVEN_SPLIT, crime=[STRADDLES], transactions=[])


def test_conformance_passes_when_every_district_has_a_row(spark):
    areas = spark.createDataFrame(DISTRICTS, AREA_SCHEMA)
    assert_districts_conform(dimension(spark), areas)


def test_a_district_missing_from_dim_area_aborts(spark):
    """The foreign key is informational, so the area would reach Delta and drop out of
    every rollup silently.

    Cardiff is the one held back, not Dudley. Dudley holds a single postcode of the
    straddling area and loses it, so no small area is ever assigned there and removing
    it would leave nothing dangling.
    """
    areas = spark.createDataFrame([(BIRMINGHAM,), (DUDLEY,)], AREA_SCHEMA)
    with pytest.raises(ValueError, match="no row in dim_area"):
        assert_districts_conform(dimension(spark), areas)


def test_no_area_is_assigned_to_a_district_that_only_holds_a_minority(spark):
    """What the test above depends on, stated rather than assumed."""
    assigned = {row["district_code"] for row in dimension(spark).collect()}
    assert assigned == {BIRMINGHAM, CARDIFF}


def test_conformance_needs_dim_areas_own_column(spark):
    areas = spark.createDataFrame(DISTRICTS, "district_code string")
    with pytest.raises(ValueError, match="area_code"):
        assert_districts_conform(dimension(spark), areas)


def test_the_result_is_stable_across_runs(spark):
    """The tie-break and the majority ranking both depend on it, and a rerun that moves
    rows moves whole areas of crime between districts."""
    assert_df_equality(
        dimension(spark).orderBy("lsoa_code"),
        dimension(spark).orderBy("lsoa_code"),
        ignore_nullable=True,
    )
