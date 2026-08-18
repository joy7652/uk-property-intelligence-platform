"""Tests for the Gold private rent fact.

Every test needs a session. The transform is a filter, a join and a projection, so there
is no pure-Python half to check separately.

The fixtures are a miniature rent series: one country published with a code, one district
published with a code, and one Northern Irish rental market area published with none. The
last is the whole reason this fact takes a dimension as a parameter, so it appears in
every resolution test.

The source frame carries `region_or_country_name` alongside the columns the fact reads.
Silver publishes 42 columns and the fact takes 14, so a frame holding exactly the 14
would never show that the extras are dropped rather than carried.

Rows are sparse by default with only the headline rent and index populated. Every row
builder here takes its defaults as a dict and updates it from `**overrides`, rather than
setting some fields as named arguments beside `**overrides`: a caller overriding one of
those named fields passes the same keyword twice and the helper raises before any
assertion runs.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from databricks_src.gold.transforms.fact_area_month_rent import (
    GOLD_COLUMNS,
    KEY_COLUMNS,
    MEASURE_COLUMNS,
    SOURCE_COLUMNS,
    SOURCE_DATE_COLUMN,
    transform_fact_area_month_rent,
)

ENGLAND = "E92000001"
HARTLEPOOL = "E06000001"
BELFAST_CODE = "BRMA_NI_BELFAST_BRMA"
BELFAST_NAME = "Belfast BRMA"

SOURCE_FIELDS: tuple[str, ...] = (
    "area_code",
    "area_name",
    "region_or_country_name",
    "date",
) + MEASURE_COLUMNS

SOURCE_SCHEMA = (
    "area_code string, area_name string, region_or_country_name string, date date, "
    "rental_price int, price_index decimal(18,6), "
    "pct_change_1m decimal(18,6), pct_change_12m decimal(18,6), "
    "one_bed_rental_price int, two_bed_rental_price int, "
    "three_bed_rental_price int, four_or_more_bed_rental_price int, "
    "detached_rental_price int, semi_detached_rental_price int, "
    "terraced_rental_price int, flat_maisonette_rental_price int"
)

DIM_SCHEMA = "area_code string, area_name string, area_level string, code_source string"

DIM_AREA = [
    (ENGLAND, "England", "nation", "published"),
    (HARTLEPOOL, "Hartlepool", "district", "published"),
    (BELFAST_CODE, BELFAST_NAME, "rental_market_area", "derived"),
]

DEFAULTS = {
    "area_code": ENGLAND,
    "area_name": "England",
    "region_or_country_name": "England",
    "date": date(2015, 1, 1),
    "rental_price": 1388,
    "price_index": Decimal("100.000000"),
}


def only_measure(**populated):
    """Every measure null except the ones named.

    Built as one dict rather than passed as keyword arguments beside a dynamic one,
    which is what a parametrised measure collides with when it is also in DEFAULTS.
    """
    overrides = dict.fromkeys(MEASURE_COLUMNS)
    overrides.update(populated)
    return overrides


def source_row(**overrides):
    """One Silver row: sparse, with the headline rent and index populated."""
    row = {name: None for name in SOURCE_FIELDS}
    row.update(DEFAULTS)
    row.update(overrides)
    return row


def uncoded_row(**overrides):
    """A Northern Irish rental market area row, which carries no published code."""
    row = {
        "area_code": None,
        "area_name": BELFAST_NAME,
        "region_or_country_name": "Northern Ireland",
    }
    row.update(overrides)
    return source_row(**row)


def source(spark, rows):
    return spark.createDataFrame(
        [[row[name] for name in SOURCE_FIELDS] for row in rows], SOURCE_SCHEMA
    )


def areas(spark, rows=None):
    return spark.createDataFrame(DIM_AREA if rows is None else rows, DIM_SCHEMA)


def fact(spark, rows=None, dim_rows=None):
    return transform_fact_area_month_rent(
        source(spark, rows or [source_row()]), areas(spark, dim_rows)
    )


def loaded(spark, rows=None, dim_rows=None):
    return {row["area_code"]: row for row in fact(spark, rows, dim_rows).collect()}


# --------------------------------------------------------------------------- #
# Column contract
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    """INSERT OVERWRITE matches on position, so a projection out of order would load
    values into the wrong columns without failing."""
    assert tuple(fact(spark).columns) == GOLD_COLUMNS


def test_the_gold_columns_are_the_keys_then_the_measures():
    assert GOLD_COLUMNS == KEY_COLUMNS + MEASURE_COLUMNS


def test_every_measure_is_read_from_the_source():
    assert set(MEASURE_COLUMNS) <= set(SOURCE_COLUMNS)


def test_the_source_name_is_not_carried(spark):
    """area_name belongs to the dimension. Carried here it would be a second copy of a
    name that can be renamed in one place and not the other, and for eight areas it is
    also the thing the key was derived from."""
    assert "area_name" not in fact(spark).columns
    assert "region_or_country_name" not in fact(spark).columns


def test_the_join_scaffolding_does_not_reach_the_output(spark):
    columns = set(fact(spark, [uncoded_row()]).columns)
    assert not [name for name in columns if name.startswith("derived_")]


def test_types_come_through_unchanged(spark):
    """Nothing is cast, because Silver already declares what the table declares."""
    types = dict(fact(spark).dtypes)
    assert types["month_start_date"] == "date"
    assert types["price_index"] == "decimal(18,6)"
    assert types["pct_change_1m"] == "decimal(18,6)"
    assert types["pct_change_12m"] == "decimal(18,6)"
    for name in MEASURE_COLUMNS:
        if name.endswith("rental_price"):
            assert types[name] == "int", name


# --------------------------------------------------------------------------- #
# Projection
# --------------------------------------------------------------------------- #


def test_the_month_key_is_renamed_from_the_silver_date(spark):
    rows = [source_row(date=date(2026, 4, 1))]
    assert loaded(spark, rows)[ENGLAND]["month_start_date"] == date(2026, 4, 1)
    assert SOURCE_DATE_COLUMN not in fact(spark).columns


def test_a_rental_price_survives_as_a_whole_pound(spark):
    assert loaded(spark)[ENGLAND]["rental_price"] == 1388


def test_an_index_survives_at_full_precision(spark):
    rows = [source_row(price_index=Decimal("117.432100"))]
    assert loaded(spark, rows)[ENGLAND]["price_index"] == Decimal("117.432100")


def test_a_negative_change_survives(spark):
    rows = [source_row(pct_change_12m=Decimal("-0.021500"))]
    assert loaded(spark, rows)[ENGLAND]["pct_change_12m"] == Decimal("-0.021500")


def test_a_null_measure_stays_null(spark):
    assert loaded(spark)[ENGLAND]["detached_rental_price"] is None


def test_rows_are_not_aggregated(spark):
    rows = [source_row(date=date(2015, month, 1)) for month in (1, 2, 3)]
    assert fact(spark, rows).count() == 3


# --------------------------------------------------------------------------- #
# Key resolution
# --------------------------------------------------------------------------- #


def test_a_published_code_is_carried_through(spark):
    assert set(loaded(spark)) == {ENGLAND}


def test_an_uncoded_area_takes_the_derived_code_matching_its_name(spark):
    """The eight Northern Irish rental market areas. This is the whole reason the
    transform takes a dimension."""
    assert set(loaded(spark, [uncoded_row()])) == {BELFAST_CODE}


def test_the_name_is_trimmed_on_both_sides(spark):
    """dim_area trims the name it stores, so a source that stops trimming would
    otherwise resolve to nothing."""
    rows = [uncoded_row(area_name=f"  {BELFAST_NAME} ")]
    dim_rows = [
        (ENGLAND, "England", "nation", "published"),
        (BELFAST_CODE, f" {BELFAST_NAME}  ", "rental_market_area", "derived"),
    ]
    assert set(loaded(spark, rows, dim_rows)) == {BELFAST_CODE}


def test_a_published_row_keeps_its_code_even_when_its_name_matches_a_derived_one(spark):
    """The join key is null wherever a code is present, and null matches nothing."""
    rows = [source_row(area_code=HARTLEPOOL, area_name=BELFAST_NAME)]
    assert set(loaded(spark, rows)) == {HARTLEPOOL}


def test_a_published_row_matching_a_derived_name_is_not_duplicated(spark):
    rows = [source_row(area_code=HARTLEPOOL, area_name=BELFAST_NAME)]
    assert fact(spark, rows).count() == 1


def test_an_uncoded_name_matching_no_derived_code_aborts(spark):
    rows = [uncoded_row(area_name="Mid Ulster BRMA")]
    with pytest.raises(ValueError, match="Mid Ulster BRMA"):
        fact(spark, rows)


def test_an_uncoded_name_matching_only_a_published_area_aborts(spark):
    """The lookup is restricted to derived codes. A published area sharing a name with
    a rental market area is a different place, and lending its code would file the
    rents under it."""
    rows = [uncoded_row(area_name="Hartlepool")]
    with pytest.raises(ValueError, match="Hartlepool"):
        fact(spark, rows)


def test_a_derived_name_under_two_codes_aborts(spark):
    """The join would fan one row into two carrying different keys, and the primary key
    is informational, so the grain check downstream passes on both."""
    dim_rows = DIM_AREA + [
        ("BRMA_NI_BELFAST_BRMA_2", BELFAST_NAME, "rental_market_area", "derived")
    ]
    with pytest.raises(ValueError, match="derived-code lookup grain broken"):
        fact(spark, [uncoded_row()], dim_rows)


def test_a_dimension_carrying_no_derived_codes_still_serves_published_rows(spark):
    """An empty lookup is a legitimate state for a release with nothing uncoded in it."""
    dim_rows = [(ENGLAND, "England", "nation", "published")]
    assert set(loaded(spark, dim_rows=dim_rows)) == {ENGLAND}


# --------------------------------------------------------------------------- #
# Rows carrying no measure
# --------------------------------------------------------------------------- #


def test_an_unpublished_month_is_dropped(spark):
    """Northern Ireland lags, ONS marks every measure unavailable, Silver keeps the row.
    Eighteen of these in the May 2026 release."""
    rows = [source_row(), source_row(date=date(2026, 6, 1), **only_measure())]
    kept = fact(spark, rows).collect()
    assert [row["month_start_date"] for row in kept] == [date(2015, 1, 1)]


@pytest.mark.parametrize("measure", MEASURE_COLUMNS)
def test_one_populated_measure_is_enough_to_keep_a_row(spark, measure):
    value = Decimal("1.000000") if measure.startswith("p") else 1
    rows = [source_row(**only_measure(**{measure: value}))]
    assert fact(spark, rows).count() == 1


def test_a_frame_of_nothing_but_unpublished_months_produces_no_rows(spark):
    assert fact(spark, [source_row(**only_measure())]).count() == 0


def test_an_uncoded_row_with_no_measure_is_dropped_before_it_needs_a_key(spark):
    """The dropped rows belong to the areas resolved by name, so a release that stopped
    naming one would otherwise abort on rows the table never carries."""
    dim_rows = [(ENGLAND, "England", "nation", "published")]
    rows = [source_row(), uncoded_row(date=date(2026, 6, 1), **only_measure())]
    assert set(loaded(spark, rows, dim_rows)) == {ENGLAND}


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_a_missing_source_column_aborts_naming_it(spark):
    frame = source(spark, [source_row()]).drop("two_bed_rental_price")
    with pytest.raises(ValueError, match="two_bed_rental_price"):
        transform_fact_area_month_rent(frame, areas(spark))


def test_a_missing_source_name_column_aborts(spark):
    """The name is the key for eight areas, so losing it is not a cosmetic change."""
    frame = source(spark, [source_row()]).drop("area_name")
    with pytest.raises(ValueError, match="missing columns it reads"):
        transform_fact_area_month_rent(frame, areas(spark))


def test_a_dimension_missing_its_code_source_aborts(spark):
    """The mistake this catches is passing the frame that produced dim_area rather than
    the loaded table."""
    with pytest.raises(ValueError, match="code_source"):
        transform_fact_area_month_rent(
            source(spark, [source_row()]), areas(spark).drop("code_source")
        )


def test_an_extra_source_column_is_accepted(spark):
    frame = source(spark, [source_row()]).withColumn("new_breakdown", F.lit(1))
    assert transform_fact_area_month_rent(frame, areas(spark)).count() == 1


def test_a_repeated_key_aborts(spark):
    rows = [source_row(), source_row()]
    with pytest.raises(ValueError, match="grain broken"):
        fact(spark, rows)


def test_two_names_under_one_published_code_break_the_grain(spark):
    """Silver keys on name and month, the fact keys on code and month. A code carried
    under two names merges two series, and this is what refuses it."""
    rows = [source_row(), source_row(area_name="England and Wales")]
    with pytest.raises(ValueError, match="grain broken"):
        fact(spark, rows)


def test_the_same_month_in_two_areas_is_not_a_repeat(spark):
    rows = [source_row(), uncoded_row()]
    assert fact(spark, rows).count() == 2


def test_a_repeat_among_dropped_rows_does_not_abort(spark):
    rows = [
        source_row(),
        source_row(date=date(2026, 6, 1), **only_measure()),
        source_row(date=date(2026, 6, 1), **only_measure()),
    ]
    assert fact(spark, rows).count() == 1


def test_the_transform_is_deterministic(spark):
    rows = [source_row(), uncoded_row()]
    assert_df_equality(
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        fact(spark, rows).orderBy(*KEY_COLUMNS),
        ignore_nullable=True,
    )
