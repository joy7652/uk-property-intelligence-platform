"""Tests for the HPI Bronze to Silver transform."""

from __future__ import annotations

import datetime as dt
from datetime import datetime
from decimal import Decimal

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_src.silver.transforms.hpi import (
    COLUMN_MAP,
    LINEAGE_COLUMNS,
    SILVER_COLUMNS,
    SOURCE_COLUMNS,
    TYPED_COLUMNS,
    VOLUME_COLUMNS,
    silver_table_ddl,
    transform_hpi,
)

SOURCE_FILE = "/Volumes/uk_property_intel/bronze/hpi/uk-hpi-full-file-2026-05.csv"
INGESTION_TS = datetime(2026, 6, 12, 9, 30, 0)

RAW_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in SOURCE_COLUMNS]
)

DEFAULTS = {
    "Date": "01/01/2010",
    "RegionName": "England",
    "AreaCode": "E92000001",
    "AveragePrice": "200000",
    "Index": "100.0",
}


def raw_row(**overrides: str | None) -> dict[str, str | None]:
    """One source row: sparse by default, since most breakdowns are nullable."""
    row: dict[str, str | None] = {name: None for name in SOURCE_COLUMNS}
    row.update(DEFAULTS)
    row.update(overrides)
    return row


def raw(spark, rows: list[dict[str, str | None]]):
    return spark.createDataFrame(
        [[row[name] for name in SOURCE_COLUMNS] for row in rows], RAW_SCHEMA
    )


def transform(spark, rows: list[dict[str, str | None]]):
    return transform_hpi(
        raw_df=raw(spark, rows),
        source_file=SOURCE_FILE,
        ingestion_ts=INGESTION_TS,
    )


# --------------------------------------------------------------------------- #
# Read and write contract
# --------------------------------------------------------------------------- #


def test_column_map_targets_are_unique():
    """A repeated Silver name would put two columns under one label, and every
    downstream reference to it would be ambiguous rather than wrong."""
    assert len(set(COLUMN_MAP.values())) == len(COLUMN_MAP)


def test_volume_columns_cover_every_volume_in_the_map():
    """A volume missing from VOLUME_COLUMNS casts straight to int, so a release that
    writes counts with a decimal point blanks that column and nothing raises."""
    assert {name for name in TYPED_COLUMNS if name.endswith("sales_volume")} == set(
        VOLUME_COLUMNS
    )


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
    df = raw(spark, [raw_row()]).drop("FlatPrice")
    with pytest.raises(ValueError, match="FlatPrice"):
        transform_hpi(df, SOURCE_FILE, INGESTION_TS)


def test_unexpected_source_column_raises(spark):
    df = raw(spark, [raw_row()]).withColumn("RegionalTrendIndex", F.lit("1.0"))
    with pytest.raises(ValueError, match="RegionalTrendIndex"):
        transform_hpi(df, SOURCE_FILE, INGESTION_TS)


# --------------------------------------------------------------------------- #
# Key and grain guards
# --------------------------------------------------------------------------- #


def test_null_area_code_raises(spark):
    with pytest.raises(ValueError, match="missing area code"):
        transform(spark, [raw_row(AreaCode=None)])


def test_unparseable_date_raises(spark):
    # ISO order, which the dd/MM/yyyy parser rejects.
    with pytest.raises(ValueError, match="unparseable date"):
        transform(spark, [raw_row(Date="2010-01-01")])


def test_duplicate_grain_raises(spark):
    rows = [
        raw_row(Date="01/03/2010", AreaCode="E92000001"),
        raw_row(Date="01/03/2010", AreaCode="E92000001"),
    ]
    with pytest.raises(ValueError, match="grain broken"):
        transform(spark, rows)


def test_same_month_different_geography_is_not_a_duplicate(spark):
    rows = [
        raw_row(Date="01/03/2010", AreaCode="E92000001"),
        raw_row(Date="01/03/2010", AreaCode="W92000004"),
    ]
    assert transform(spark, rows).count() == 2


def test_duplicate_in_the_discarded_era_does_not_raise(spark):
    # Grain is a contract on the output, not on rows the floor removes.
    rows = [
        raw_row(Date="01/03/1990", AreaCode="E92000001"),
        raw_row(Date="01/03/1990", AreaCode="E92000001"),
        raw_row(Date="01/03/2010", AreaCode="E92000001"),
    ]
    assert transform(spark, rows).count() == 1


# --------------------------------------------------------------------------- #
# Geography guard
# --------------------------------------------------------------------------- #


def test_unmapped_area_code_prefix_raises(spark):
    with pytest.raises(ValueError, match="no coverage floor"):
        transform(spark, [raw_row(AreaCode="Z12000001", RegionName="Atlantis")])


def test_unmapped_composite_code_raises(spark):
    with pytest.raises(ValueError, match="no coverage floor"):
        transform(spark, [raw_row(AreaCode="K05000001", RegionName="New composite")])


# --------------------------------------------------------------------------- #
# Coverage floor
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "area_code, region_name, dropped, kept",
    [
        ("E92000001", "England", "01/12/1994", "01/01/1995"),
        ("W92000004", "Wales", "01/12/1994", "01/01/1995"),
        ("S12000034", "Aberdeenshire", "01/12/2003", "01/01/2004"),
        ("N92000002", "Northern Ireland", "01/12/2004", "01/01/2005"),
        ("K04000001", "England and Wales", "01/12/1994", "01/01/1995"),
        ("K03000001", "Great Britain", "01/12/2003", "01/01/2004"),
        ("K02000001", "United Kingdom", "01/12/2004", "01/01/2005"),
    ],
)
def test_coverage_floor_boundary(spark, area_code, region_name, dropped, kept):
    rows = [
        raw_row(Date=dropped, AreaCode=area_code, RegionName=region_name),
        raw_row(Date=kept, AreaCode=area_code, RegionName=region_name),
    ]
    result = transform(spark, rows).select("date").collect()
    assert [row["date"] for row in result] == [
        dt.datetime.strptime(kept, "%d/%m/%Y").date()
    ]


def test_nations_floor_independently_in_one_frame(spark):
    rows = [
        raw_row(Date="01/06/1996", AreaCode="E92000001", RegionName="England"),
        raw_row(Date="01/06/1996", AreaCode="S12000034", RegionName="Aberdeenshire"),
        raw_row(Date="01/06/2006", AreaCode="S12000034", RegionName="Aberdeenshire"),
        raw_row(Date="01/06/1996", AreaCode="N92000002", RegionName="Northern Ireland"),
    ]
    result = {
        (row["area_code"], row["date"].year) for row in transform(spark, rows).collect()
    }
    assert result == {("E92000001", 1996), ("S12000034", 2006)}


# --------------------------------------------------------------------------- #
# Series continuity
# --------------------------------------------------------------------------- #


def series(area_code: str, region_name: str, months: list[tuple[int, int]]):
    """One geography's rows, one per (year, month) given."""
    return [
        raw_row(
            Date=f"01/{month:02d}/{year}", AreaCode=area_code, RegionName=region_name
        )
        for year, month in months
    ]


def test_a_contiguous_series_passes(spark):
    rows = series("E92000001", "England", [(2010, 1), (2010, 2), (2010, 3)])
    assert transform(spark, rows).count() == 3


def test_a_missing_month_inside_a_series_raises(spark):
    rows = series("E92000001", "England", [(2010, 1), (2010, 3)])
    with pytest.raises(ValueError, match="missing a month inside"):
        transform(spark, rows)


def test_a_gap_across_a_year_boundary_raises(spark):
    """The span counts year-months, so December to March is two missing rather than
    one, and a naive month subtraction would read it as nine."""
    rows = series("E92000001", "England", [(2010, 12), (2011, 3)])
    with pytest.raises(ValueError, match="missing a month inside"):
        transform(spark, rows)


def test_a_series_starting_after_its_floor_passes(spark):
    """An authority created partway through the series. Reorganisation is a fact about
    the country, so the notebook records it and this does not raise."""
    rows = series("E06000060", "Buckinghamshire", [(2020, 5), (2020, 6)])
    assert transform(spark, rows).count() == 2


def test_a_series_stopping_early_passes(spark):
    """The same in the other direction: an abolished authority ends where it ends."""
    rows = series("E92000001", "England", [(2010, 1), (2010, 2)])
    rows += series("W92000004", "Wales", [(2010, 1), (2010, 2), (2010, 3)])
    assert transform(spark, rows).count() == 5


def test_a_single_month_series_passes(spark):
    rows = series("E92000001", "England", [(2010, 1)])
    assert transform(spark, rows).count() == 1


def test_the_offending_geography_is_named(spark):
    rows = series("E92000001", "England", [(2010, 1), (2010, 2), (2010, 3)])
    rows += series("W92000004", "Wales", [(2010, 1), (2010, 3)])
    with pytest.raises(ValueError, match="W92000004"):
        transform(spark, rows)


def test_the_gap_is_measured_after_the_coverage_floor(spark):
    """Rows the floor removes are not a hole. Scotland floors at 2004, so the 2003 row
    is gone before this counts anything, and the two months left are contiguous."""
    rows = series("S12000034", "Aberdeenshire", [(2003, 1), (2004, 1), (2004, 2)])
    assert transform(spark, rows).count() == 2


# --------------------------------------------------------------------------- #
# Typing
# --------------------------------------------------------------------------- #


def test_output_schema_types(spark):
    schema = {f.name: f.dataType for f in transform(spark, [raw_row()]).schema}

    assert schema["date"] == DateType()
    assert schema["region_name"] == StringType()
    assert schema["area_code"] == StringType()
    assert schema["_source_file"] == StringType()
    assert schema["_ingestion_ts"] == TimestampType()
    for name in VOLUME_COLUMNS:
        assert schema[name] == IntegerType(), name
    measures = set(TYPED_COLUMNS) - {"date", "region_name", "area_code"} - VOLUME_COLUMNS
    for name in measures:
        assert schema[name] == DecimalType(18, 6), name


def test_output_column_order_is_canonical(spark):
    assert tuple(transform(spark, [raw_row()]).columns) == SILVER_COLUMNS


def test_no_source_column_names_survive(spark):
    result = transform(spark, [raw_row()])
    assert not set(result.columns) & set(SOURCE_COLUMNS)


def test_date_parsed_from_uk_format(spark):
    assert transform(spark, [raw_row(Date="01/09/2015")]).collect()[0]["date"] == (
        dt.date(2015, 9, 1)
    )


def test_whole_pound_price_survives_typing(spark):
    result = transform(spark, [raw_row(AveragePrice="84638")])
    assert result.collect()[0]["avg_price"] == Decimal("84638.000000")


def test_high_precision_price_survives_typing(spark):
    # Earlier vintages published prices to five decimal places.
    result = transform(spark, [raw_row(AveragePrice="81693.66964")])
    assert result.collect()[0]["avg_price"] == Decimal("81693.669640")


def test_volume_written_with_a_decimal_point_survives(spark):
    result = transform(spark, [raw_row(SalesVolume="388.0")])
    assert result.collect()[0]["sales_volume"] == 388


def test_negative_change_survives_typing(spark):
    result = transform(spark, [raw_row(**{"1m%Change": "-0.018248"})])
    assert result.collect()[0]["pct_change_1m"] == Decimal("-0.018248")


def test_null_measures_stay_null(spark):
    row = transform(spark, [raw_row()]).collect()[0]
    assert row["price_index_seasonally_adjusted"] is None
    assert row["sales_volume"] is None
    assert row["flat_price"] is None


def test_malformed_measure_raises_naming_the_column(spark):
    with pytest.raises(ValueError, match="flat_price"):
        transform(spark, [raw_row(FlatPrice="not a price")])


# --------------------------------------------------------------------------- #
# Lineage
# --------------------------------------------------------------------------- #


def test_lineage_stamped_on_every_row(spark):
    rows = [
        raw_row(Date="01/01/2010", AreaCode="E92000001"),
        raw_row(Date="01/02/2010", AreaCode="E92000001"),
    ]
    out = transform(spark, rows).collect()
    assert all(row["_source_file"] == SOURCE_FILE for row in out)
    assert all(row["_ingestion_ts"] == INGESTION_TS for row in out)


def test_lineage_columns_are_last(spark):
    assert tuple(transform(spark, [raw_row()]).columns[-2:]) == LINEAGE_COLUMNS


def test_transform_is_deterministic(spark):
    """Lineage is a parameter, not generated inside, so the same inputs must give
    the same frame. A current_timestamp() call in the transform would break this and
    make the end-to-end assertion untestable."""
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
        # Derived era for Scotland, dropped.
        raw_row(
            Date="01/01/2003",
            RegionName="Aberdeenshire",
            AreaCode="S12000034",
            AveragePrice="70000",
            Index="35.0",
        ),
        raw_row(
            Date="01/01/2004",
            RegionName="Aberdeenshire",
            AreaCode="S12000034",
            AveragePrice="84638",
            Index="41.1",
            SalesVolume="388",
            FlatPrice="49322",
            NewSalesVolume="103",
        ),
        raw_row(
            Date="01/02/2004",
            RegionName="Aberdeenshire",
            AreaCode="S12000034",
            AveragePrice="84620",
            Index="41.0",
            SalesVolume="326",
            FlatPrice="49180",
            NewSalesVolume="98",
        ),
    ]

    actual = transform(spark, rows).select(
        "date",
        "region_name",
        "area_code",
        "avg_price",
        "price_index",
        "sales_volume",
        "flat_price",
        "new_build_sales_volume",
        "_source_file",
        "_ingestion_ts",
    )

    expected_schema = StructType(
        [
            StructField("date", DateType(), True),
            StructField("region_name", StringType(), True),
            StructField("area_code", StringType(), True),
            StructField("avg_price", DecimalType(18, 6), True),
            StructField("price_index", DecimalType(18, 6), True),
            StructField("sales_volume", IntegerType(), True),
            StructField("flat_price", DecimalType(18, 6), True),
            StructField("new_build_sales_volume", IntegerType(), True),
            StructField("_source_file", StringType(), False),
            StructField("_ingestion_ts", TimestampType(), False),
        ]
    )
    expected = spark.createDataFrame(
        [
            (
                dt.date(2004, 1, 1),
                "Aberdeenshire",
                "S12000034",
                Decimal("84638.000000"),
                Decimal("41.100000"),
                388,
                Decimal("49322.000000"),
                103,
                SOURCE_FILE,
                INGESTION_TS,
            ),
            (
                dt.date(2004, 2, 1),
                "Aberdeenshire",
                "S12000034",
                Decimal("84620.000000"),
                Decimal("41.000000"),
                326,
                Decimal("49180.000000"),
                98,
                SOURCE_FILE,
                INGESTION_TS,
            ),
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_row_order=True)
