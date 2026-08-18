"""Tests for the Gold published-area dimension.

Two halves. The authored maps and the derived code are pure Python and need no
session; everything else builds synthetic Silver frames through the `spark` fixture.

The fixtures are a miniature United Kingdom: two English districts in one region, a
Welsh district, the region, three nations, one composite, a county, a Scottish rental
market area and an uncoded Northern Irish one. Small enough to reason about, wide
enough that every level and every name rule appears at least once.
"""

from __future__ import annotations

import re

import pytest
from chispa import assert_df_equality

from databricks_src.gold.transforms.dim_area import (
    COMPOSITE,
    COMPOSITE_NATIONS,
    COUNTRY_BY_INITIAL,
    COUNTY,
    DERIVED,
    DISTRICT,
    FROM_POSTCODES,
    FROM_PRICE_INDEX,
    FROM_RENT_SERIES,
    GOLD_COLUMNS,
    LEVEL_BY_PREFIX,
    LEVELS,
    NATION,
    NATION_CODE,
    PUBLISHED,
    REGION,
    REGION_NAME_ALIAS,
    RENTAL_MARKET_AREA,
    assert_maps_consistent,
    derived_area_code,
    derived_codes,
    measure_published_areas,
    transform_dim_area,
)

# Shape the table's own code_shape constraint admits.
PUBLISHED_CODE = re.compile(r"^[A-Z][0-9]{8}$")
DERIVED_CODE = re.compile(r"^BRMA_NI_[A-Z_]+$")

ENGLAND = "E92000001"
WALES = "W92000004"
SCOTLAND = "S92000003"
NORTHERN_IRELAND = "N92000002"

WEST_MIDLANDS_REGION = "E12000005"
BIRMINGHAM = "E08000025"
DUDLEY = "E08000027"
CARDIFF = "W06000015"
WEST_MIDLANDS_COUNTY = "E11000005"
ENGLAND_AND_WALES = "K04000001"
EDINBURGH_BRMA = "S33000012"

# (area_code, name the price index publishes)
HPI_ROWS = [
    (WEST_MIDLANDS_REGION, "West Midlands Region"),
    (BIRMINGHAM, "Birmingham"),
    (DUDLEY, "Dudley"),
    (CARDIFF, "Cardiff"),
    (WEST_MIDLANDS_COUNTY, "West Midlands"),
    (ENGLAND, "England"),
    (WALES, "Wales"),
    (SCOTLAND, "Scotland"),
    (NORTHERN_IRELAND, "Northern Ireland"),
    (ENGLAND_AND_WALES, "England and Wales"),
]

# (area_code, area_name, region_or_country_name)
ONS_ROWS = [
    (BIRMINGHAM, "Birmingham", "West Midlands"),
    (CARDIFF, "Cardiff", "Wales"),
    (WEST_MIDLANDS_REGION, "West Midlands", "England"),
    (ENGLAND, "England", "England"),
    (EDINBURGH_BRMA, "Lothian", "Scotland"),
    (None, "Belfast", "Northern Ireland"),
    (None, "North West", "Northern Ireland"),
]

# (postcode, district_code, district, region, country)
# The directory does not leave region empty outside England: it restates the country,
# so Cardiff arrives naming "Wales" as its region. Getting this wrong in the fixture is
# what hid the region guard firing on all 65 non-English districts.
DOOGAL_ROWS = [
    ("B1 1AA", BIRMINGHAM, "Birmingham", "West Midlands", "England"),
    ("B2 2BB", BIRMINGHAM, "Birmingham", "West Midlands", "England"),
    ("DY1 1AA", DUDLEY, "Dudley", "West Midlands", "England"),
    ("CF10 1AA", CARDIFF, "Cardiff", "Wales", "Wales"),
]

HPI_SCHEMA = "area_code string, region_name string"
ONS_SCHEMA = "area_code string, area_name string, region_or_country_name string"
DOOGAL_SCHEMA = (
    "postcode string, district_code string, district string, "
    "region string, country string"
)


def sources(spark, hpi=None, ons=None, doogal=None):
    """The three Silver frames, each overridable one at a time."""
    return (
        spark.createDataFrame(HPI_ROWS if hpi is None else hpi, HPI_SCHEMA),
        spark.createDataFrame(ONS_ROWS if ons is None else ons, ONS_SCHEMA),
        spark.createDataFrame(DOOGAL_ROWS if doogal is None else doogal, DOOGAL_SCHEMA),
    )


def dimension(spark, **kwargs):
    """The dimension built from the synthetic sources."""
    return transform_dim_area(measure_published_areas(*sources(spark, **kwargs)))


def loaded(spark, **kwargs):
    """The dimension keyed on area_code, for asserting a row at a time."""
    return {row["area_code"]: row.asDict() for row in dimension(spark, **kwargs).collect()}


# --------------------------------------------------------------------------- #
# Authored maps
# --------------------------------------------------------------------------- #


def test_maps_are_consistent():
    """Runs at import too. Kept as a test so a bad edit fails in CI rather than only on
    the cluster, where it would abort a load that had already shuffled the directory."""
    assert_maps_consistent()


def test_every_prefix_maps_to_a_known_level():
    assert set(LEVEL_BY_PREFIX.values()) <= set(LEVELS)


def test_every_level_is_reachable_from_some_prefix():
    """A level no prefix produces is a branch in the parent and name rules that nothing
    can ever take."""
    assert set(LEVEL_BY_PREFIX.values()) == set(LEVELS)


@pytest.mark.parametrize("code, level", [
    (BIRMINGHAM, DISTRICT),
    (CARDIFF, DISTRICT),
    (WEST_MIDLANDS_COUNTY, COUNTY),
    (WEST_MIDLANDS_REGION, REGION),
    (ENGLAND, NATION),
    (ENGLAND_AND_WALES, COMPOSITE),
    (EDINBURGH_BRMA, RENTAL_MARKET_AREA),
])
def test_prefix_places_a_known_code(code, level):
    assert LEVEL_BY_PREFIX[code[:3]] == level


def test_nation_codes_agree_with_their_own_initials():
    """The initial is how a code's country is read, so a nation code whose letter names
    a different country would contradict every area under it."""
    for name, code in NATION_CODE.items():
        assert COUNTRY_BY_INITIAL[code[0]] == name


def test_composites_span_only_named_nations():
    for nations in COMPOSITE_NATIONS.values():
        assert set(nations) <= set(NATION_CODE)


def test_the_alias_covers_the_region_the_index_renames():
    """Without it a district in the West Midlands matches the metropolitan county
    instead of the region, which is a wrong parent rather than a missing one."""
    assert REGION_NAME_ALIAS["West Midlands Region"] == "West Midlands"


# --------------------------------------------------------------------------- #
# Derived codes
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("name, code", [
    ("Belfast", "BRMA_NI_BELFAST"),
    ("North West", "BRMA_NI_NORTH_WEST"),
    ("Lough Neagh Upper", "BRMA_NI_LOUGH_NEAGH_UPPER"),
    ("  Belfast  ", "BRMA_NI_BELFAST"),
])
def test_derived_code_is_the_name_upcased(name, code):
    assert derived_area_code(name) == code


def test_derived_code_matches_the_shape_the_table_admits():
    for _, name, _ in [row for row in ONS_ROWS if row[0] is None]:
        assert DERIVED_CODE.fullmatch(derived_area_code(name))


def test_derived_code_is_stable():
    """A fact keys on this, so the same name has to give the same code every release."""
    assert derived_area_code("North West") == derived_area_code("North West")


@pytest.mark.parametrize("name", ["", "   ", "---"])
def test_nameless_input_is_rejected(name):
    with pytest.raises(ValueError):
        derived_area_code(name)


def test_a_digit_is_rejected_rather_than_dropped():
    """The code shape admits letters and underscores only. Dropping the digit would let
    two areas collapse into one code."""
    with pytest.raises(ValueError, match="digit"):
        derived_area_code("Area 51")


def test_colliding_names_are_rejected():
    with pytest.raises(ValueError, match="collide"):
        derived_codes(["North West", "North-West"])


def test_distinct_names_pass():
    assert derived_codes(["Belfast", "North West"]) == {
        "Belfast": "BRMA_NI_BELFAST",
        "North West": "BRMA_NI_NORTH_WEST",
    }


# --------------------------------------------------------------------------- #
# Shape
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    """INSERT OVERWRITE matches on position, so a projection that drifts from the
    declared order would load values into the wrong columns."""
    assert tuple(dimension(spark).columns) == GOLD_COLUMNS


def test_one_row_per_area(spark):
    rows = dimension(spark)
    assert rows.count() == rows.select("area_code").distinct().count()


def test_the_union_of_three_publishers_is_the_membership(spark):
    """A code any one publisher carries earns a row. Dudley is in the index and the
    directory but not the rent series; Lothian is in the rent series alone."""
    codes = set(loaded(spark))
    assert {BIRMINGHAM, DUDLEY, CARDIFF, EDINBURGH_BRMA} <= codes
    assert "BRMA_NI_BELFAST" in codes


def test_every_code_matches_a_shape_the_table_admits(spark):
    for code, row in loaded(spark).items():
        pattern = PUBLISHED_CODE if row["code_source"] == PUBLISHED else DERIVED_CODE
        assert pattern.fullmatch(code), (code, row["code_source"])


# --------------------------------------------------------------------------- #
# Levels
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("code, level", [
    (BIRMINGHAM, DISTRICT),
    (CARDIFF, DISTRICT),
    (WEST_MIDLANDS_COUNTY, COUNTY),
    (WEST_MIDLANDS_REGION, REGION),
    (ENGLAND, NATION),
    (ENGLAND_AND_WALES, COMPOSITE),
    (EDINBURGH_BRMA, RENTAL_MARKET_AREA),
])
def test_level_comes_from_the_prefix(spark, code, level):
    assert loaded(spark)[code]["area_level"] == level


def test_a_derived_code_is_a_rental_market_area(spark):
    """It carries no GSS prefix, so the level cannot come from one."""
    row = loaded(spark)["BRMA_NI_BELFAST"]
    assert (row["area_level"], row["code_source"]) == (RENTAL_MARKET_AREA, DERIVED)


def test_an_unmapped_prefix_aborts(spark):
    """A geography with no level has no parent, no name rule and no flags, so it fails
    rather than loading against a null."""
    hpi = HPI_ROWS + [("X99000001", "Somewhere new")]
    with pytest.raises(ValueError, match="no level in LEVEL_BY_PREFIX"):
        dimension(spark, hpi=hpi)


# --------------------------------------------------------------------------- #
# Names
# --------------------------------------------------------------------------- #


def test_the_directory_names_a_district(spark):
    row = loaded(spark)[BIRMINGHAM]
    assert (row["area_name"], row["name_source"]) == ("Birmingham", FROM_POSTCODES)


def test_the_index_names_everything_above_a_district(spark):
    rows = loaded(spark)
    for code in (WEST_MIDLANDS_REGION, ENGLAND, ENGLAND_AND_WALES, WEST_MIDLANDS_COUNTY):
        assert rows[code]["name_source"] == FROM_PRICE_INDEX


def test_the_rent_series_names_a_rental_market_area(spark):
    row = loaded(spark)[EDINBURGH_BRMA]
    assert (row["area_name"], row["name_source"]) == ("Lothian", FROM_RENT_SERIES)


def test_the_region_keeps_the_index_name_not_the_directory_one(spark):
    """The alias exists to match the directory's string, not to rename the region."""
    assert loaded(spark)[WEST_MIDLANDS_REGION]["area_name"] == "West Midlands Region"


def test_a_district_the_directory_misses_falls_to_the_index(spark):
    """Precedence is an order, not a requirement. name_source records which rule
    actually applied rather than which one ruled."""
    doogal = [row for row in DOOGAL_ROWS if row[1] != DUDLEY]
    row = loaded(spark, doogal=doogal)[DUDLEY]
    assert (row["area_name"], row["name_source"]) == ("Dudley", FROM_PRICE_INDEX)


def test_names_are_trimmed(spark):
    hpi = [(code, f"  {name}  ") for code, name in HPI_ROWS]
    assert loaded(spark, hpi=hpi)[ENGLAND]["area_name"] == "England"


# --------------------------------------------------------------------------- #
# Ancestry
# --------------------------------------------------------------------------- #


def test_an_english_district_sits_under_its_region(spark):
    row = loaded(spark)[BIRMINGHAM]
    assert row["parent_area_code"] == WEST_MIDLANDS_REGION
    assert row["region_code"] == WEST_MIDLANDS_REGION


def test_a_welsh_district_sits_under_its_nation(spark):
    """Only England is divided into regions, so the region is null and the parent skips
    a level rather than pointing at nothing.

    The directory files this district under the region name "Wales". The lookup is
    gated on the country rather than left to miss, so that name never reaches it."""
    row = loaded(spark)[CARDIFF]
    assert (row["parent_area_code"], row["region_code"]) == (WALES, None)


def test_a_district_outside_england_does_not_abort_on_its_region(spark):
    """The country restated in the region column is not an unresolved region. Treating
    it as one fires the guard on every district in three of the four nations."""
    assert loaded(spark)[CARDIFF]["region_code"] is None


def test_an_english_district_the_directory_misses_does_not_abort(spark):
    """No region is available from anywhere for it, which is a coverage gap rather than
    a broken join. Demanding one of every English district fires on this too, and
    has_postcodes already records the absence."""
    doogal = [row for row in DOOGAL_ROWS if row[1] != DUDLEY]
    row = loaded(spark, doogal=doogal)[DUDLEY]
    assert (row["region_code"], row["parent_area_code"]) == (None, ENGLAND)
    assert not row["has_postcodes"]


def test_a_region_is_its_own_region(spark):
    """Matching how nation_code behaves on a nation row, so a filter on the column
    reaches the published region series as well as the districts under it."""
    row = loaded(spark)[WEST_MIDLANDS_REGION]
    assert row["region_code"] == WEST_MIDLANDS_REGION
    assert row["parent_area_code"] == ENGLAND


def test_a_nation_is_its_own_nation(spark):
    row = loaded(spark)[ENGLAND]
    assert (row["nation_code"], row["parent_area_code"]) == (ENGLAND, None)


def test_a_composite_spanning_two_nations_claims_neither(spark):
    row = loaded(spark)[ENGLAND_AND_WALES]
    assert (row["nation_code"], row["country_name"]) == (None, None)
    assert row["parent_area_code"] is None


def test_a_county_has_no_parent(spark):
    """Its children cannot be identified, so naming it a parent would imply a rollup
    that does not exist."""
    row = loaded(spark)[WEST_MIDLANDS_COUNTY]
    assert (row["parent_area_code"], row["region_code"]) == (None, None)


def test_a_rental_market_area_sits_under_its_nation(spark):
    assert loaded(spark)[EDINBURGH_BRMA]["parent_area_code"] == SCOTLAND


def test_a_derived_area_takes_the_country_the_rent_series_filed_it_under(spark):
    """Its code carries no initial to read a country from."""
    row = loaded(spark)["BRMA_NI_BELFAST"]
    assert (row["nation_code"], row["country_name"]) == (
        NORTHERN_IRELAND,
        "Northern Ireland",
    )


def test_every_pointer_lands_on_a_row(spark):
    rows = loaded(spark)
    for code, row in rows.items():
        for column in ("parent_area_code", "region_code", "nation_code"):
            if row[column] is not None:
                assert row[column] in rows, (code, column, row[column])


def test_a_dangling_parent_aborts(spark):
    """Dropping England leaves the region and both nations-level pointers hanging."""
    hpi = [row for row in HPI_ROWS if row[0] != ENGLAND]
    ons = [row for row in ONS_ROWS if row[0] != ENGLAND]
    with pytest.raises(ValueError, match="no row"):
        dimension(spark, hpi=hpi, ons=ons)


def test_an_unresolvable_region_aborts(spark):
    """Silent otherwise: the district falls through to its nation parent and looks
    correct while vanishing from every region figure."""
    hpi = [row for row in HPI_ROWS if row[0] != WEST_MIDLANDS_REGION]
    ons = [row for row in ONS_ROWS if row[0] != WEST_MIDLANDS_REGION]
    with pytest.raises(ValueError, match="no code for"):
        dimension(spark, hpi=hpi, ons=ons)


# --------------------------------------------------------------------------- #
# Flags
# --------------------------------------------------------------------------- #


def test_price_and_rent_flags_follow_the_publishers(spark):
    rows = loaded(spark)
    assert rows[BIRMINGHAM]["has_price_index"] and rows[BIRMINGHAM]["has_rent_index"]
    assert rows[DUDLEY]["has_price_index"] and not rows[DUDLEY]["has_rent_index"]
    assert not rows[EDINBURGH_BRMA]["has_price_index"]


def test_a_district_has_postcodes_when_the_directory_carries_it(spark):
    rows = loaded(spark)
    assert rows[BIRMINGHAM]["has_postcodes"]


def test_a_district_the_directory_misses_has_none(spark):
    doogal = [row for row in DOOGAL_ROWS if row[1] != DUDLEY]
    assert not loaded(spark, doogal=doogal)[DUDLEY]["has_postcodes"]


def test_regions_and_nations_resolve_true(spark):
    """The rent series publishes both, and the table admits only a rental market area
    as carrying rent without postcodes."""
    rows = loaded(spark)
    assert rows[WEST_MIDLANDS_REGION]["has_postcodes"]
    assert rows[ENGLAND]["has_postcodes"] and rows[WALES]["has_postcodes"]


def test_a_region_the_directory_files_nothing_under_has_no_postcodes(spark):
    """Measured against the region names the directory actually uses, not against the
    region set itself, which would compare it with its own members and always agree."""
    hpi = HPI_ROWS + [("E12000003", "Yorkshire and The Humber")]
    assert not loaded(spark, hpi=hpi)["E12000003"]["has_postcodes"]


def test_a_composite_resolves_true_where_every_nation_it_spans_is_covered(spark):
    assert loaded(spark)[ENGLAND_AND_WALES]["has_postcodes"]


def test_a_composite_reaching_beyond_the_directory_resolves_false(spark):
    """England and Wales spans Wales, so dropping every Welsh postcode leaves the
    composite partly uncovered."""
    doogal = [row for row in DOOGAL_ROWS if row[4] != "Wales"]
    assert not loaded(spark, doogal=doogal)[ENGLAND_AND_WALES]["has_postcodes"]


def test_a_county_never_has_postcodes(spark):
    """Its only available membership is the ceremonial code collision this model
    discarded."""
    assert not loaded(spark)[WEST_MIDLANDS_COUNTY]["has_postcodes"]


def test_a_rental_market_area_never_has_postcodes(spark):
    assert not loaded(spark)[EDINBURGH_BRMA]["has_postcodes"]


def test_rent_without_postcodes_is_only_a_rental_market_area(spark):
    """The table's own constraint, asserted on the frame before it reaches Delta."""
    for code, row in loaded(spark).items():
        if row["has_rent_index"] and not row["has_postcodes"]:
            assert row["area_level"] == RENTAL_MARKET_AREA, code


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_source_column_aborts(spark):
    hpi, ons, doogal = sources(spark)
    with pytest.raises(ValueError, match="missing columns it reads"):
        measure_published_areas(hpi.drop("region_name"), ons, doogal)


def test_transform_rejects_a_frame_that_is_not_the_measured_one(spark):
    hpi, _, _ = sources(spark)
    with pytest.raises(ValueError, match="missing columns it reads"):
        transform_dim_area(hpi)


def test_an_uncoded_area_outside_northern_ireland_aborts(spark):
    """The derived prefix names Northern Ireland, so a code from anywhere else would
    claim a country it is not in."""
    ons = ONS_ROWS + [(None, "Somewhere", "Scotland")]
    with pytest.raises(ValueError, match="outside Northern Ireland"):
        dimension(spark, ons=ons)


def test_a_district_spanning_two_regions_aborts(spark):
    """Its parent would be whichever value the aggregate happened to return."""
    doogal = DOOGAL_ROWS + [
        ("B9 9ZZ", BIRMINGHAM, "Birmingham", "East Midlands", "England")
    ]
    with pytest.raises(ValueError, match="more than one region or country"):
        dimension(spark, doogal=doogal)


def test_an_area_no_publisher_names_aborts(spark):
    hpi = [(code, None) for code, _ in HPI_ROWS]
    with pytest.raises(ValueError, match="no name from any publisher"):
        dimension(spark, hpi=hpi)


def test_the_result_is_stable_across_runs(spark):
    """Two builds of the same sources must agree, or a rerun moves rows for no reason.
    The tie-break in the name precedence and the region lookup both depend on it."""
    assert_df_equality(
        dimension(spark).orderBy("area_code"),
        dimension(spark).orderBy("area_code"),
        ignore_nullable=True,
    )
