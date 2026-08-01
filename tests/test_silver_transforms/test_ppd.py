"""Tests for the PPD Bronze to Silver transform."""

from __future__ import annotations

import datetime as dt
from datetime import datetime

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_src.silver.transforms.ppd import (
    DOMAINS,
    FRAME_COLUMNS,
    LINEAGE_COLUMNS,
    SILVER_COLUMNS,
    SOURCE_COLUMNS,
    TYPED_COLUMNS,
    silver_table_ddl,
    string_schema,
    transform_ppd,
)

FILE_2019 = "abfss://bronze@acct.dfs.core.windows.net/ppd/yearly/2019/pp-2019.csv"
FILE_2020 = "abfss://bronze@acct.dfs.core.windows.net/ppd/yearly/2020/pp-2020.csv"

INGESTION_TS = datetime(2026, 7, 31, 9, 30, 0)

RAW_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in FRAME_COLUMNS]
)

DEFAULTS = {
    "tuid": "{8CAC1318-5AA6-0253-E053-6B04A8C08E51}",
    "price": "165000",
    "date_of_transfer": "2019-05-24 00:00",
    "postcode": "M32 0JL",
    "property_type": "S",
    "old_new": "N",
    "duration": "F",
    "paon": "35",
    "street": "STANWAY STREET",
    "locality": "STRETFORD",
    "town_city": "MANCHESTER",
    "district": "TRAFFORD",
    "county": "GREATER MANCHESTER",
    "ppd_category_type": "A",
    "record_status": "A",
    "_source_file": FILE_2019,
}


def tuid(n: int) -> str:
    """A distinct identifier, since most tests need uniqueness to hold."""
    return "{00000000-0000-0000-0000-%012d}" % n


def raw_row(**overrides: str | None) -> dict[str, str | None]:
    """One source row: saon is null by default, as it is on most transactions."""
    row: dict[str, str | None] = {name: None for name in FRAME_COLUMNS}
    row.update(DEFAULTS)
    row.update(overrides)
    return row


def raw(spark, rows: list[dict[str, str | None]]):
    return spark.createDataFrame(
        [[row[name] for name in FRAME_COLUMNS] for row in rows], RAW_SCHEMA
    )


def transform(spark, rows: list[dict[str, str | None]]):
    return transform_ppd(raw_df=raw(spark, rows), ingestion_ts=INGESTION_TS)


# --------------------------------------------------------------------------- #
# Read and write contract
# --------------------------------------------------------------------------- #


def test_read_schema_matches_the_published_column_order():
    """The files are headerless, so the read is positional. A reordered schema loads
    values into the wrong columns and every other test agrees with the mistake."""
    assert [field.name for field in string_schema().fields] == list(SOURCE_COLUMNS)
    assert SOURCE_COLUMNS[0] == "tuid"
    assert SOURCE_COLUMNS[1] == "price"
    assert SOURCE_COLUMNS[2] == "date_of_transfer"
    assert SOURCE_COLUMNS[-1] == "record_status"


def test_read_schema_is_all_string():
    """Types are asserted in the transform, never inferred at the reader."""
    assert {field.dataType for field in string_schema().fields} == {StringType()}


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


def test_output_column_order_is_canonical(spark):
    """INSERT OVERWRITE matches on position, so a reordered projection would load
    values into the wrong table columns without failing."""
    assert tuple(transform(spark, [raw_row()]).columns) == SILVER_COLUMNS


def test_record_status_is_dropped(spark):
    """It is constant across the yearly files and names an operation, not an
    attribute of the transaction."""
    assert "record_status" not in transform(spark, [raw_row()]).columns


# --------------------------------------------------------------------------- #
# Source column guard
# --------------------------------------------------------------------------- #


def test_expected_column_set_passes(spark):
    assert transform(spark, [raw_row()]).count() == 1


def test_missing_source_column_raises(spark):
    df = raw(spark, [raw_row()]).drop("county")
    with pytest.raises(ValueError, match="county"):
        transform_ppd(df, INGESTION_TS)


def test_missing_source_file_column_raises(spark):
    """The per-file guards group on it, so its absence would silence them."""
    df = raw(spark, [raw_row()]).drop("_source_file")
    with pytest.raises(ValueError, match="_source_file"):
        transform_ppd(df, INGESTION_TS)


def test_unexpected_source_column_raises(spark):
    df = raw(spark, [raw_row()]).withColumn("energy_rating", F.lit("C"))
    with pytest.raises(ValueError, match="energy_rating"):
        transform_ppd(df, INGESTION_TS)


# --------------------------------------------------------------------------- #
# Date format guard
# --------------------------------------------------------------------------- #


def test_unparseable_date_raises(spark):
    with pytest.raises(ValueError, match="published format"):
        transform(spark, [raw_row(date_of_transfer="24/05/2019")])


def test_nonzero_time_component_raises(spark):
    """A real time parses cleanly and is then discarded, so only the string check
    sees it."""
    with pytest.raises(ValueError, match="published format"):
        transform(spark, [raw_row(date_of_transfer="2019-05-24 09:30")])


def test_missing_date_raises(spark):
    with pytest.raises(ValueError, match="unparseable date"):
        transform(spark, [raw_row(date_of_transfer=None)])


# --------------------------------------------------------------------------- #
# Key guard
# --------------------------------------------------------------------------- #


def test_null_tuid_raises(spark):
    with pytest.raises(ValueError, match="missing TUID"):
        transform(spark, [raw_row(tuid=None)])


def test_key_failure_names_the_source_file(spark):
    with pytest.raises(ValueError, match="pp-2019.csv"):
        transform(spark, [raw_row(tuid=None)])


# --------------------------------------------------------------------------- #
# Typing
# --------------------------------------------------------------------------- #


def test_non_numeric_price_raises_naming_the_column(spark):
    with pytest.raises(ValueError, match="price"):
        transform(spark, [raw_row(price="165,000")])


def test_price_beyond_int_range_raises(spark):
    with pytest.raises(ValueError, match="price"):
        transform(spark, [raw_row(price="9999999999")])


def test_null_price_at_source_is_not_a_cast_failure(spark):
    assert transform(spark, [raw_row(price=None)]).count() == 1


def test_output_schema_types(spark):
    schema = {f.name: f.dataType for f in transform(spark, [raw_row()]).schema}

    assert schema["price"] == IntegerType()
    assert schema["date_of_transfer"] == DateType()
    assert schema["transfer_year"] == IntegerType()
    assert schema["_source_file"] == StringType()
    assert schema["_ingestion_ts"] == TimestampType()
    for name in set(TYPED_COLUMNS) - {"price", "date_of_transfer", "transfer_year"}:
        assert schema[name] == StringType(), name


def test_price_parsed_as_whole_pounds(spark):
    assert transform(spark, [raw_row(price="165000")]).collect()[0]["price"] == 165000


def test_zero_price_survives_typing(spark):
    assert transform(spark, [raw_row(price="0")]).collect()[0]["price"] == 0


def test_date_parsed_from_published_format(spark):
    result = transform(spark, [raw_row(date_of_transfer="2019-09-01 00:00")])
    assert result.collect()[0]["date_of_transfer"] == dt.date(2019, 9, 1)


def test_transfer_year_derives_from_the_date(spark):
    result = transform(spark, [raw_row(date_of_transfer="2019-11-02 00:00")])
    assert result.collect()[0]["transfer_year"] == 2019


def test_null_address_fields_stay_null(spark):
    row = transform(
        spark, [raw_row(saon=None, locality=None, postcode=None)]
    ).collect()[0]
    assert row["saon"] is None
    assert row["locality"] is None
    assert row["postcode"] is None


# --------------------------------------------------------------------------- #
# Code set guard
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("column", sorted(DOMAINS))
def test_unrecognised_code_raises_naming_the_column(spark, column):
    with pytest.raises(ValueError, match=column):
        transform(spark, [raw_row(**{column: "Z"})])


@pytest.mark.parametrize("column", sorted(DOMAINS))
def test_missing_code_raises(spark, column):
    with pytest.raises(ValueError, match=column):
        transform(spark, [raw_row(**{column: None})])


def test_every_published_code_passes(spark):
    rows = []
    n = 0
    for column, values in DOMAINS.items():
        for value in values:
            n += 1
            rows.append(raw_row(tuid=tuid(n), **{column: value}))
    assert transform(spark, rows).count() == n


def test_category_b_rows_are_kept(spark):
    rows = [
        raw_row(tuid=tuid(1), ppd_category_type="A"),
        raw_row(tuid=tuid(2), ppd_category_type="B"),
    ]
    result = {row["ppd_category_type"] for row in transform(spark, rows).collect()}
    assert result == {"A", "B"}


def test_record_status_is_still_validated_though_it_is_dropped(spark):
    """It leaves the projection, so a guard running on the typed frame would stop
    checking it silently."""
    with pytest.raises(ValueError, match="record_status"):
        transform(spark, [raw_row(record_status="X")])


# --------------------------------------------------------------------------- #
# Partition key guard
# --------------------------------------------------------------------------- #


def test_two_transfer_years_in_one_file_raises(spark):
    rows = [
        raw_row(tuid=tuid(1), date_of_transfer="2019-12-31 00:00"),
        raw_row(tuid=tuid(2), date_of_transfer="2020-01-01 00:00"),
    ]
    with pytest.raises(ValueError, match="one-year-per-file"):
        transform(spark, rows)


def test_filename_disagreeing_with_content_raises(spark):
    with pytest.raises(ValueError, match="one-year-per-file"):
        transform(spark, [raw_row(date_of_transfer="2018-05-24 00:00")])


def test_year_boundaries_stay_in_one_partition(spark):
    rows = [
        raw_row(tuid=tuid(1), date_of_transfer="2019-01-01 00:00"),
        raw_row(tuid=tuid(2), date_of_transfer="2019-12-31 00:00"),
    ]
    assert {row["transfer_year"] for row in transform(spark, rows).collect()} == {2019}


def test_files_are_checked_independently_in_one_frame(spark):
    rows = [
        raw_row(tuid=tuid(1), date_of_transfer="2019-05-24 00:00"),
        raw_row(
            tuid=tuid(2), date_of_transfer="2020-05-24 00:00", _source_file=FILE_2020
        ),
    ]
    result = {
        (row["transfer_year"], row["_source_file"])
        for row in transform(spark, rows).collect()
    }
    assert result == {(2019, FILE_2019), (2020, FILE_2020)}


# --------------------------------------------------------------------------- #
# Key uniqueness
# --------------------------------------------------------------------------- #


def test_duplicate_tuid_raises(spark):
    with pytest.raises(ValueError, match="not unique"):
        transform(spark, [raw_row(tuid=tuid(7)), raw_row(tuid=tuid(7))])


def test_duplicate_tuid_across_files_raises(spark):
    rows = [
        raw_row(tuid=tuid(7), date_of_transfer="2019-05-24 00:00"),
        raw_row(
            tuid=tuid(7), date_of_transfer="2020-05-24 00:00", _source_file=FILE_2020
        ),
    ]
    with pytest.raises(ValueError, match="not unique"):
        transform(spark, rows)


def test_identical_transactions_under_distinct_tuids_are_kept(spark):
    """Two flats sold the same day at the same price differ only by identifier, so a
    dedup on the business columns would discard a real transaction."""
    assert transform(spark, [raw_row(tuid=tuid(1)), raw_row(tuid=tuid(2))]).count() == 2


# --------------------------------------------------------------------------- #
# Lineage
# --------------------------------------------------------------------------- #


def test_lineage_stamped_on_every_row(spark):
    rows = [
        raw_row(tuid=tuid(1), _source_file=FILE_2019),
        raw_row(
            tuid=tuid(2), date_of_transfer="2020-05-24 00:00", _source_file=FILE_2020
        ),
    ]
    out = transform(spark, rows).collect()
    assert {row["_source_file"] for row in out} == {FILE_2019, FILE_2020}
    assert all(row["_ingestion_ts"] == INGESTION_TS for row in out)


def test_lineage_columns_are_last(spark):
    assert tuple(transform(spark, [raw_row()]).columns[-2:]) == LINEAGE_COLUMNS


def test_transform_is_deterministic(spark):
    """Lineage is a parameter, not generated inside, so the same inputs must give the
    same frame."""
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
            tuid=tuid(1),
            price="165000",
            date_of_transfer="2019-05-24 00:00",
            ppd_category_type="A",
        ),
        raw_row(
            tuid=tuid(2),
            price="116000",
            date_of_transfer="2019-06-07 00:00",
            postcode="L9 1DZ",
            property_type="T",
            duration="L",
            paon="91",
            street="BREEZE HILL",
            locality=None,
            town_city="LIVERPOOL",
            district="LIVERPOOL",
            county="MERSEYSIDE",
            ppd_category_type="B",
        ),
    ]

    actual = transform(spark, rows).select(
        "tuid",
        "price",
        "date_of_transfer",
        "transfer_year",
        "postcode",
        "property_type",
        "duration",
        "locality",
        "town_city",
        "ppd_category_type",
        "_source_file",
        "_ingestion_ts",
    )

    expected_schema = StructType(
        [
            StructField("tuid", StringType(), True),
            StructField("price", IntegerType(), True),
            StructField("date_of_transfer", DateType(), True),
            StructField("transfer_year", IntegerType(), True),
            StructField("postcode", StringType(), True),
            StructField("property_type", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("locality", StringType(), True),
            StructField("town_city", StringType(), True),
            StructField("ppd_category_type", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_ingestion_ts", TimestampType(), False),
        ]
    )
    expected = spark.createDataFrame(
        [
            (
                tuid(1),
                165000,
                dt.date(2019, 5, 24),
                2019,
                "M32 0JL",
                "S",
                "F",
                "STRETFORD",
                "MANCHESTER",
                "A",
                FILE_2019,
                INGESTION_TS,
            ),
            (
                tuid(2),
                116000,
                dt.date(2019, 6, 7),
                2019,
                "L9 1DZ",
                "T",
                "L",
                None,
                "LIVERPOOL",
                "B",
                FILE_2019,
                INGESTION_TS,
            ),
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_row_order=True)
