"""Tests for the Doogal Bronze to Silver transform.

The transform is pure, so every test builds a small synthetic frame in the published
column order and asserts on the output. No ZIP, no CSV, no Delta, no I/O.

Two row shapes recur. `raw_row` is an ordinary geographic postcode. `bfpo_row` is a
British Forces Post Office postcode, which carries coordinates and nothing else: no
country, no quality, no grid reference. Null geography is legitimate only in that
shape, and several guards turn on the distinction.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_src.silver.transforms.doogal import (
    CAST_CHECKED_COLUMNS,
    CODE_COLUMNS,
    COLUMN_MAP,
    COORD_COLUMNS,
    DATE_COLUMNS,
    DOMAINS,
    DROPPED_COLUMNS,
    GRID_COLUMNS,
    LINEAGE_COLUMNS,
    SILVER_COLUMNS,
    SOURCE_COLUMNS,
    TYPED_COLUMNS,
    silver_table_ddl,
    transform_doogal,
)

SOURCE_FILE = "/Volumes/uk_property_intel/bronze/doogal/postcodes.zip"
INGESTION_TS = datetime(2026, 8, 3, 9, 30, 0)

RAW_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in SOURCE_COLUMNS]
)

DEFAULTS = {
    "Postcode": "CT16 1AA",
    "In Use?": "Yes",
    "Latitude": "51.127762",
    "Longitude": "1.313403",
    "Easting": "631896",
    "Northing": "141830",
    "Country": "England",
    "Quality": "1",
    "User Type": "0",
    "Introduced": "1980-01-01",
    "Last updated": "2026-05-29",
}

# A postcode with no UK geography at all. Every column the non-geographic guard
# inspects is null, and the coordinates point at an overseas base.
BFPO_DEFAULTS = {
    "Postcode": "BF1 0AB",
    "Latitude": "51.776812",
    "Longitude": "8.713910",
    "Easting": None,
    "Northing": None,
    "Country": None,
    "Quality": None,
    "User Type": None,
    "Introduced": None,
    "Last updated": "2018-10-14",
}


def raw_row(**overrides: str | None) -> dict[str, str | None]:
    """One source row: sparse by default, since most geography columns are optional."""
    row: dict[str, str | None] = {name: None for name in SOURCE_COLUMNS}
    row.update(DEFAULTS)
    row.update(overrides)
    return row


def bfpo_row(**overrides: str | None) -> dict[str, str | None]:
    return raw_row(**{**BFPO_DEFAULTS, **overrides})


def raw(spark, rows: list[dict[str, str | None]]):
    return spark.createDataFrame(
        [[row[name] for name in SOURCE_COLUMNS] for row in rows], RAW_SCHEMA
    )


def transform(spark, rows: list[dict[str, str | None]]):
    return transform_doogal(
        raw_df=raw(spark, rows),
        source_file=SOURCE_FILE,
        ingestion_ts=INGESTION_TS,
    )


# --------------------------------------------------------------------------- #
# Read and write contract
# --------------------------------------------------------------------------- #


def test_every_source_column_is_kept_or_dropped():
    """A column in neither set disappears in rename_columns without anything failing,
    so a column added to a future release would be discarded silently."""
    assert set(COLUMN_MAP) | DROPPED_COLUMNS == set(SOURCE_COLUMNS)


def test_no_column_is_both_kept_and_dropped():
    assert not set(COLUMN_MAP) & DROPPED_COLUMNS


def test_column_map_targets_are_unique():
    """A repeated Silver name would put two columns under one label, and every
    downstream reference to it would be ambiguous rather than wrong."""
    assert len(set(COLUMN_MAP.values())) == len(COLUMN_MAP)


def test_cast_checked_columns_cover_every_retyped_column():
    """A column that changes type but is missing from CAST_CHECKED_COLUMNS loses
    values to try_cast with nothing to compare counts against."""
    retyped = DATE_COLUMNS | COORD_COLUMNS | GRID_COLUMNS | CODE_COLUMNS
    assert set(CAST_CHECKED_COLUMNS) == retyped


def test_domain_columns_are_silver_columns():
    """DOMAINS is applied after renaming, so its keys are Silver names."""
    assert set(DOMAINS) <= set(TYPED_COLUMNS)


def test_table_ddl_matches_the_transform_output(spark):
    """The DDL and the cast expressions each declare a type. Drift between them would
    load values into a column that truncates or nulls them on write."""
    declared = spark.createDataFrame(
        [], schema=silver_table_ddl().replace(" NOT NULL", "")
    ).schema
    produced = transform(spark, [raw_row()]).schema
    assert [(f.name, f.dataType) for f in declared.fields] == [
        (f.name, f.dataType) for f in produced.fields
    ]


# --------------------------------------------------------------------------- #
# Source column guard
# --------------------------------------------------------------------------- #


def test_expected_column_set_passes(spark):
    assert transform(spark, [raw_row()]).count() == 1


def test_missing_source_column_raises(spark):
    df = raw(spark, [raw_row()]).drop("Rural/urban 2021")
    with pytest.raises(ValueError, match="Rural/urban 2021"):
        transform_doogal(df, SOURCE_FILE, INGESTION_TS)


def test_unexpected_source_column_raises(spark):
    df = raw(spark, [raw_row()]).withColumn("Broadband speed", F.lit("fast"))
    with pytest.raises(ValueError, match="Broadband speed"):
        transform_doogal(df, SOURCE_FILE, INGESTION_TS)


def test_byte_order_mark_on_the_header_raises(spark):
    """The file carries a BOM and the reader strips it. A reader option change that
    let it through would rename the key column to something no guard looks for."""
    df = raw(spark, [raw_row()]).withColumnRenamed("Postcode", "\ufeffPostcode")
    with pytest.raises(ValueError, match="Postcode"):
        transform_doogal(df, SOURCE_FILE, INGESTION_TS)


# --------------------------------------------------------------------------- #
# In Use? restates Terminated
# --------------------------------------------------------------------------- #


def test_live_postcode_passes(spark):
    assert transform(spark, [raw_row()]).count() == 1


def test_terminated_postcode_passes(spark):
    rows = [raw_row(**{"In Use?": "No", "Terminated": "1996-06-01"})]
    assert transform(spark, rows).count() == 1


def test_in_use_with_a_termination_date_raises(spark):
    rows = [raw_row(**{"In Use?": "Yes", "Terminated": "1996-06-01"})]
    with pytest.raises(ValueError, match="In Use\\?"):
        transform(spark, rows)


def test_not_in_use_without_a_termination_date_raises(spark):
    with pytest.raises(ValueError, match="In Use\\?"):
        transform(spark, [raw_row(**{"In Use?": "No"})])


# --------------------------------------------------------------------------- #
# Code sets
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "source_column, bad_value, silver_column",
    [
        ("Country", "Atlantis", "country"),
        ("Quality", "12", "positional_quality"),
        ("User Type", "2", "user_type"),
        ("London zone", "0", "london_travel_zone"),
    ],
)
def test_unrecognised_code_raises_naming_the_column(
    spark, source_column, bad_value, silver_column
):
    with pytest.raises(ValueError, match=silver_column):
        transform(spark, [raw_row(**{source_column: bad_value})])


def test_published_quality_seven_is_accepted(spark):
    """Quality 7 means deliberately left blank. It is absent from the current release,
    which is a fact about the release rather than about the contract."""
    rows = [raw_row(**{"Quality": "7"})]
    assert transform(spark, rows).collect()[0]["positional_quality"] == 7


def test_null_london_zone_outside_london_passes(spark):
    rows = [raw_row(**{"London zone": None})]
    assert transform(spark, rows).collect()[0]["london_travel_zone"] is None


# --------------------------------------------------------------------------- #
# Non-geographic postcodes
# --------------------------------------------------------------------------- #


def test_bfpo_row_passes_with_no_geography(spark):
    row = transform(spark, [bfpo_row()]).collect()[0]
    assert row["country"] is None
    assert row["positional_quality"] is None
    assert row["introduced_date"] is None
    assert row["latitude"] == pytest.approx(51.776812)


@pytest.mark.parametrize("column", ["Country", "Quality", "User Type"])
def test_null_geography_outside_the_bf_area_raises(spark, column):
    """Null here is legitimate only for BFPO. Without this guard a geography the
    publisher has not yet mapped would be indistinguishable from one that has none."""
    with pytest.raises(ValueError, match="BF postcode area"):
        transform(spark, [raw_row(**{column: None})])


def test_bfpo_and_geographic_rows_coexist(spark):
    rows = [raw_row(), bfpo_row()]
    assert transform(spark, rows).count() == 2


# --------------------------------------------------------------------------- #
# Fabricated coordinates
# --------------------------------------------------------------------------- #


def quality_nine_row(**overrides: str | None) -> dict[str, str | None]:
    """The published shape for a postcode with no grid reference: blank easting and
    northing, and a zero coordinate pair the publisher writes in their place."""
    return raw_row(
        **{
            "Quality": "9",
            "Easting": None,
            "Northing": None,
            "Latitude": "0",
            "Longitude": "0",
            **overrides,
        }
    )


def test_quality_nine_coordinates_become_null(spark):
    row = transform(spark, [quality_nine_row()]).collect()[0]
    assert row["latitude"] is None
    assert row["longitude"] is None


def test_quality_nine_row_is_kept(spark):
    """Only the fabricated position is dropped. The postcode still resolves to a
    district, which is what the PPD join needs. It carries no LSOA: without a grid
    reference the source assigns no statistical geography either."""
    row = transform(spark, [quality_nine_row(**{"District Code": "E07000108"})]).collect()[0]
    assert row["postcode"] == "CT16 1AA"
    assert row["district_code"] == "E07000108"


def test_nulled_coordinates_keep_their_type(spark):
    """The null is substituted into a typed column, so the result must stay double
    rather than collapsing to the type of the literal."""
    schema = {f.name: f.dataType for f in transform(spark, [quality_nine_row()]).schema}
    assert schema["latitude"] == DoubleType()
    assert schema["longitude"] == DoubleType()


def test_quality_nine_with_a_grid_reference_raises(spark):
    with pytest.raises(ValueError, match="grid"):
        transform(spark, [quality_nine_row(**{"Easting": "631896"})])


def test_missing_grid_reference_below_quality_nine_raises(spark):
    with pytest.raises(ValueError, match="grid"):
        transform(spark, [raw_row(**{"Easting": None})])


def test_bfpo_row_does_not_trip_the_grid_reference_guard(spark):
    """BFPO carries no quality and no grid reference, which the guard must read as
    absent rather than as a contradiction."""
    assert transform(spark, [bfpo_row()]).count() == 1


def test_zero_coordinates_outside_quality_nine_raise(spark):
    """The nulling keys on quality alone, so a zero pair anywhere else survives it and
    would reach the table as a position in the Atlantic."""
    rows = [raw_row(**{"Latitude": "0", "Longitude": "0"})]
    with pytest.raises(ValueError, match="zero coordinate"):
        transform(spark, rows)


# --------------------------------------------------------------------------- #
# Key and grain guards
# --------------------------------------------------------------------------- #


def test_null_postcode_raises(spark):
    with pytest.raises(ValueError, match="missing postcode"):
        transform(spark, [raw_row(**{"Postcode": None})])


def test_duplicate_postcode_raises(spark):
    with pytest.raises(ValueError, match="not unique"):
        transform(spark, [raw_row(), raw_row()])


def test_distinct_postcodes_pass(spark):
    rows = [raw_row(**{"Postcode": "CT16 1AA"}), raw_row(**{"Postcode": "CT16 1AB"})]
    assert transform(spark, rows).count() == 2


# --------------------------------------------------------------------------- #
# Typing
# --------------------------------------------------------------------------- #


def test_output_schema_types(spark):
    schema = {f.name: f.dataType for f in transform(spark, [raw_row()]).schema}

    assert schema["postcode"] == StringType()
    assert schema["_source_file"] == StringType()
    assert schema["_ingestion_ts"] == TimestampType()
    for name in DATE_COLUMNS:
        assert schema[name] == DateType(), name
    for name in COORD_COLUMNS:
        assert schema[name] == DoubleType(), name
    for name in GRID_COLUMNS:
        assert schema[name] == IntegerType(), name
    for name in CODE_COLUMNS:
        assert schema[name] == ByteType(), name

    retyped = DATE_COLUMNS | COORD_COLUMNS | GRID_COLUMNS | CODE_COLUMNS
    for name in set(TYPED_COLUMNS) - retyped:
        assert schema[name] == StringType(), name


def test_output_column_order_is_canonical(spark):
    """INSERT OVERWRITE matches on position, so a reordered projection would load
    values into the wrong table columns without failing."""
    assert tuple(transform(spark, [raw_row()]).columns) == SILVER_COLUMNS


def test_no_source_column_names_survive(spark):
    result = transform(spark, [raw_row()])
    assert not set(result.columns) & set(SOURCE_COLUMNS)


def test_dropped_columns_do_not_reach_silver(spark):
    result = transform(spark, [raw_row()])
    assert not set(result.columns) & DROPPED_COLUMNS


def test_dates_parsed_from_iso_format(spark):
    rows = [raw_row(**{"In Use?": "No", "Terminated": "1996-06-01"})]
    row = transform(spark, rows).collect()[0]
    assert row["introduced_date"] == dt.date(1980, 1, 1)
    assert row["terminated_date"] == dt.date(1996, 6, 1)
    assert row["source_last_updated"] == dt.date(2026, 5, 29)


def test_live_postcode_has_no_termination_date(spark):
    assert transform(spark, [raw_row()]).collect()[0]["terminated_date"] is None


def test_coordinates_survive_typing(spark):
    row = transform(spark, [raw_row()]).collect()[0]
    assert row["latitude"] == pytest.approx(51.127762)
    assert row["longitude"] == pytest.approx(1.313403)


def test_negative_longitude_survives_typing(spark):
    rows = [raw_row(**{"Longitude": "-6.058011", "Latitude": "54.516246"})]
    assert transform(spark, rows).collect()[0]["longitude"] == pytest.approx(-6.058011)


def test_grid_reference_survives_typing(spark):
    row = transform(spark, [raw_row()]).collect()[0]
    assert row["easting"] == 631896
    assert row["northing"] == 141830


def test_optional_geography_stays_null(spark):
    row = transform(spark, [raw_row()]).collect()[0]
    assert row["parish"] is None
    assert row["national_park"] is None
    assert row["lsoa_code_2021"] is None


@pytest.mark.parametrize(
    "source_column, bad_value, silver_column",
    [
        ("Introduced", "01/01/1980", "introduced_date"),
        ("Last updated", "29 May 2026", "source_last_updated"),
        ("Latitude", "not a coordinate", "latitude"),
        ("Easting", "631896.5", "easting"),
    ],
)
def test_malformed_value_raises_naming_the_column(
    spark, source_column, bad_value, silver_column
):
    """try_cast yields null rather than raising, so the count comparison is what turns
    a lost value into a failure that names where it went."""
    with pytest.raises(ValueError, match=silver_column):
        transform(spark, [raw_row(**{source_column: bad_value})])


# --------------------------------------------------------------------------- #
# Lineage
# --------------------------------------------------------------------------- #


def test_lineage_stamped_on_every_row(spark):
    rows = [raw_row(**{"Postcode": "CT16 1AA"}), raw_row(**{"Postcode": "CT16 1AB"})]
    out = transform(spark, rows).collect()
    assert all(row["_source_file"] == SOURCE_FILE for row in out)
    assert all(row["_ingestion_ts"] == INGESTION_TS for row in out)


def test_lineage_columns_are_last(spark):
    assert tuple(transform(spark, [raw_row()]).columns[-2:]) == LINEAGE_COLUMNS


def test_transform_is_deterministic(spark):
    """Lineage is a parameter, not generated inside, so the same inputs must give the
    same frame. A current_timestamp() call in the transform would break this and make
    the end-to-end assertion untestable."""
    assert_df_equality(
        transform(spark, [raw_row()]),
        transform(spark, [raw_row()]),
        ignore_nullable=True,
    )


# --------------------------------------------------------------------------- #
# End to end
# --------------------------------------------------------------------------- #


def test_end_to_end_projection(spark):
    rows = [
        raw_row(
            **{
                "Postcode": "CT16 1AA",
                "District Code": "E07000108",
                "LSOA21 Code": "E01015344",
                "Region": "South East",
            }
        ),
        raw_row(
            **{
                "Postcode": "AB1 0AE",
                "In Use?": "No",
                "Terminated": "1996-06-01",
                "Introduced": "1980-01-01",
                "Latitude": "57.084429",
                "Longitude": "-2.255714",
                "Easting": "384600",
                "Northing": "799298",
                "Country": "Scotland",
                "Quality": "8",
                "District Code": "S12000034",
                "LSOA21 Code": None,
                "Region": None,
            }
        ),
        quality_nine_row(**{"Postcode": "CT16 1AB", "District Code": "E07000108"}),
    ]

    actual = transform(spark, rows).select(
        "postcode",
        "introduced_date",
        "terminated_date",
        "latitude",
        "longitude",
        "easting",
        "positional_quality",
        "country",
        "region",
        "district_code",
        "lsoa_code_2021",
        "_source_file",
        "_ingestion_ts",
    )

    expected_schema = StructType(
        [
            StructField("postcode", StringType(), True),
            StructField("introduced_date", DateType(), True),
            StructField("terminated_date", DateType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("easting", IntegerType(), True),
            StructField("positional_quality", ByteType(), True),
            StructField("country", StringType(), True),
            StructField("region", StringType(), True),
            StructField("district_code", StringType(), True),
            StructField("lsoa_code_2021", StringType(), True),
            StructField("_source_file", StringType(), False),
            StructField("_ingestion_ts", TimestampType(), False),
        ]
    )
    expected = spark.createDataFrame(
        [
            (
                "CT16 1AA",
                dt.date(1980, 1, 1),
                None,
                51.127762,
                1.313403,
                631896,
                1,
                "England",
                "South East",
                "E07000108",
                "E01015344",
                SOURCE_FILE,
                INGESTION_TS,
            ),
            (
                "AB1 0AE",
                dt.date(1980, 1, 1),
                dt.date(1996, 6, 1),
                57.084429,
                -2.255714,
                384600,
                8,
                "Scotland",
                None,
                "S12000034",
                None,
                SOURCE_FILE,
                INGESTION_TS,
            ),
            (
                "CT16 1AB",
                dt.date(1980, 1, 1),
                None,
                None,
                None,
                None,
                9,
                "England",
                None,
                "E07000108",
                None,
                SOURCE_FILE,
                INGESTION_TS,
            ),
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_row_order=True)
