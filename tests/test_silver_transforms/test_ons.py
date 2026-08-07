"""Tests for the ONS private rents Bronze to Silver transform.

The transform is pure, so every test builds a small synthetic frame in the converted
sheet's shape and asserts on the output. No Excel, no CSV, no Delta, no I/O.

Multi-row invariants the Delta CHECK constraints cannot express (unique grain, markers
confined to the positions ONS publishes them in, unpublished months being the trailing
ones) are asserted here, per the notebook's note that they belong in the test suite.
"""

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

from databricks_src.silver.transforms.ons import (
    COLUMN_MAP,
    LINEAGE_COLUMNS,
    MARKER_NOT_APPLICABLE,
    MARKER_UNAVAILABLE,
    MEASURE_COLUMNS,
    PCT_CHANGE_1M_COLUMNS,
    PCT_CHANGE_12M_COLUMNS,
    RENTAL_PRICE_COLUMNS,
    SILVER_COLUMNS,
    SOURCE_COLUMNS,
    TYPED_COLUMNS,
    parse_published_date,
    silver_table_ddl,
    transform_ons,
)

SOURCE_FILE = (
    "/Volumes/uk_property_intel/bronze/ons/private_rent_index/"
    "priceindexofprivaterents-2026-07.xlsx"
)
INGESTION_TS = datetime(2026, 8, 5, 9, 30, 0)

# The header as it appears on row 3 of Table 1, in sheet order. COLUMN_MAP is generated
# from a breakdown list and a metric list rather than written out, so this is what pins
# the generation to the file. Every other test builds its frame from the map and would
# agree with a typo in it.
SHEET_HEADER: tuple[str, ...] = (
    "Time period",
    "Area code",
    "Area name",
    "Region or country name",
    "Index",
    "Monthly change",
    "Annual change",
    "Rental price",
    "Index one bed",
    "Monthly change one bed",
    "Annual change one bed",
    "Rental price one bed",
    "Index two bed",
    "Monthly change two bed",
    "Annual change two bed",
    "Rental price two bed",
    "Index three bed",
    "Monthly change three bed",
    "Annual change three bed",
    "Rental price three bed",
    "Index four or more bed",
    "Monthly change four or more bed",
    "Annual change four or more bed",
    "Rental price four or more bed",
    "Index detached",
    "Monthly change detached",
    "Annual change detached",
    "Rental price detached",
    "Index semidetached",
    "Monthly change semidetached",
    "Annual change semidetached",
    "Rental price semidetached",
    "Index terraced",
    "Monthly change terraced",
    "Annual change terraced",
    "Rental price terraced",
    "Index flat maisonette",
    "Monthly change flat maisonette",
    "Annual change flat maisonette",
    "Rental price flat maisonette",
)

RAW_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in SOURCE_COLUMNS]
)

MEASURE_HEADERS: tuple[str, ...] = tuple(
    source for source, target in COLUMN_MAP.items() if target in set(MEASURE_COLUMNS)
)

RENTAL_PRICE_HEADERS: frozenset[str] = frozenset(
    source for source, target in COLUMN_MAP.items() if target in RENTAL_PRICE_COLUMNS
)

# A clean row, dated well past the first year so no marker is structurally expected.
DEFAULTS: dict[str, str | None] = {
    "Time period": "2020-01-01",
    "Area code": "K02000001",
    "Area name": "United Kingdom",
    "Region or country name": MARKER_NOT_APPLICABLE,
    **{
        header: "1000" if header in RENTAL_PRICE_HEADERS else "100.0"
        for header in MEASURE_HEADERS
    },
}


def raw_row(**overrides: str | None) -> dict[str, str | None]:
    """One source row, fully populated unless overridden.

    Populated by default rather than sparse, because the marker guards read the whole
    row: a row of nulls is not the same as a row of [x]. Every source header carries a
    space, so overrides are passed as an unpacked dict rather than as keywords.
    """
    row = dict(DEFAULTS)
    row.update(overrides)
    return row


def unavailable_row(**overrides: str | None) -> dict[str, str | None]:
    """A row whose every measure is [x], which is how an unpublished month arrives."""
    row = raw_row(**{header: MARKER_UNAVAILABLE for header in MEASURE_HEADERS})
    row.update(overrides)
    return row


def northern_ireland_row(**overrides: str | None) -> dict[str, str | None]:
    return raw_row(
        **{
            "Area code": "N92000002",
            "Area name": "Northern Ireland",
            **overrides,
        }
    )


def rental_market_area_row(**overrides: str | None) -> dict[str, str | None]:
    """One of the eight Northern Irish areas: no code, parent names the nation."""
    return raw_row(
        **{
            "Area code": MARKER_NOT_APPLICABLE,
            "Area name": "Belfast BRMA",
            "Region or country name": "Northern Ireland",
            **overrides,
        }
    )


def raw(spark, rows: list[dict[str, str | None]]):
    return spark.createDataFrame(
        [[row[name] for name in SOURCE_COLUMNS] for row in rows], RAW_SCHEMA
    )


def transform(spark, rows: list[dict[str, str | None]]):
    return transform_ons(
        raw_df=raw(spark, rows),
        source_file=SOURCE_FILE,
        ingestion_ts=INGESTION_TS,
    )


# --------------------------------------------------------------------------- #
# Read and write contract
# --------------------------------------------------------------------------- #


def test_column_map_reproduces_the_sheet_header():
    assert SOURCE_COLUMNS == SHEET_HEADER


def test_column_map_targets_are_unique():
    """A repeated Silver name would put two columns under one label, and every
    downstream reference to it would be ambiguous rather than wrong."""
    assert len(set(COLUMN_MAP.values())) == len(COLUMN_MAP)


def test_measure_groups_partition_the_measures():
    """The marker guard allows [x] by group and the cast picks a type by group, so a
    measure in no group, or in two, is typed or checked by accident."""
    index_columns = {name for name in MEASURE_COLUMNS if name.endswith("price_index")}
    groups = [
        index_columns,
        set(PCT_CHANGE_1M_COLUMNS),
        set(PCT_CHANGE_12M_COLUMNS),
        set(RENTAL_PRICE_COLUMNS),
    ]
    assert set().union(*groups) == set(MEASURE_COLUMNS)
    assert sum(len(group) for group in groups) == len(MEASURE_COLUMNS)


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
    df = raw(spark, [raw_row()]).drop("Rental price terraced")
    with pytest.raises(ValueError, match="Rental price terraced"):
        transform_ons(df, SOURCE_FILE, INGESTION_TS)


def test_unexpected_source_column_raises(spark):
    df = raw(spark, [raw_row()]).withColumn("Index five bed", F.lit("100.0"))
    with pytest.raises(ValueError, match="Index five bed"):
        transform_ons(df, SOURCE_FILE, INGESTION_TS)


# --------------------------------------------------------------------------- #
# Marker: [z], not applicable
# --------------------------------------------------------------------------- #


def test_not_applicable_in_label_columns_passes(spark):
    assert transform(spark, [raw_row(), rental_market_area_row()]).count() == 2


def test_not_applicable_in_a_measure_raises(spark):
    with pytest.raises(ValueError, match="price_index"):
        transform(spark, [raw_row(**{"Index": MARKER_NOT_APPLICABLE})])


def test_not_applicable_report_names_every_offending_measure(spark):
    """The check aggregates rather than sampling rows: which columns are affected is
    the diagnosis, and five example rows would not give it."""
    rows = [
        raw_row(**{"Index": MARKER_NOT_APPLICABLE}),
        raw_row(
            **{"Time period": "2020-02-01", "Rental price one bed": MARKER_NOT_APPLICABLE}
        ),
    ]
    with pytest.raises(ValueError, match="one_bed_rental_price"):
        transform(spark, rows)


# --------------------------------------------------------------------------- #
# Marker: [x], structurally unavailable
# --------------------------------------------------------------------------- #


def test_monthly_change_unavailable_in_first_month_passes(spark):
    rows = [
        raw_row(**{"Time period": "2015-01-01", "Monthly change": MARKER_UNAVAILABLE})
    ]
    assert transform(spark, rows).collect()[0]["pct_change_1m"] is None


def test_monthly_change_unavailable_after_first_month_raises(spark):
    rows = [
        raw_row(**{"Time period": "2015-02-01", "Monthly change": MARKER_UNAVAILABLE})
    ]
    with pytest.raises(ValueError, match="pct_change_1m"):
        transform(spark, rows)


@pytest.mark.parametrize("period", ["2015-01-01", "2015-12-01"])
def test_annual_change_unavailable_inside_first_year_passes(spark, period):
    rows = [raw_row(**{"Time period": period, "Annual change": MARKER_UNAVAILABLE})]
    assert transform(spark, rows).collect()[0]["pct_change_12m"] is None


def test_annual_change_unavailable_at_twelve_months_raises(spark):
    """The allowance runs to the twelfth month and stops. January 2016 has a January
    2015 to compare against, so an [x] there is a gap rather than an origin."""
    rows = [
        raw_row(**{"Time period": "2016-01-01", "Annual change": MARKER_UNAVAILABLE})
    ]
    with pytest.raises(ValueError, match="pct_change_12m"):
        transform(spark, rows)


def test_unavailable_index_alone_raises(spark):
    """Index carries no structural allowance at any date: either it is published or
    the whole row is not."""
    rows = [raw_row(**{"Time period": "2015-01-01", "Index": MARKER_UNAVAILABLE})]
    with pytest.raises(ValueError, match="price_index"):
        transform(spark, rows)


def test_whole_row_unavailable_for_northern_ireland_passes(spark):
    rows = [
        northern_ireland_row(**{"Time period": "2020-01-01"}),
        unavailable_row(
            **{
                "Time period": "2020-02-01",
                "Area code": "N92000002",
                "Area name": "Northern Ireland",
            }
        ),
    ]
    result = {
        row["date"]: row["price_index"] for row in transform(spark, rows).collect()
    }
    assert result[dt.date(2020, 2, 1)] is None
    assert result[dt.date(2020, 1, 1)] is not None


def test_whole_row_unavailable_for_an_uncoded_rental_area_passes(spark):
    """The eight rental areas carry no code, so the nation is read off the parent
    column instead. Identifying them by code alone would reject this row."""
    rows = [
        rental_market_area_row(**{"Time period": "2020-01-01"}),
        rental_market_area_row(
            **{
                "Time period": "2020-02-01",
                **{header: MARKER_UNAVAILABLE for header in MEASURE_HEADERS},
            }
        ),
    ]
    assert transform(spark, rows).count() == 2


def test_whole_row_unavailable_outside_northern_ireland_raises(spark):
    rows = [unavailable_row(**{"Area code": "E92000001", "Area name": "England"})]
    with pytest.raises(ValueError, match="outside Northern Ireland"):
        transform(spark, rows)


def test_interior_unpublished_month_raises(spark):
    """A lagging nation is missing its latest months. A gap in the middle passes the
    structural guard, which tests position by column rather than by date."""
    rows = [
        unavailable_row(
            **{
                "Time period": "2020-01-01",
                "Area code": "N92000002",
                "Area name": "Northern Ireland",
            }
        ),
        northern_ireland_row(**{"Time period": "2020-02-01"}),
    ]
    with pytest.raises(ValueError, match="interior gap"):
        transform(spark, rows)


def test_trailing_unpublished_months_do_not_raise(spark):
    rows = [
        northern_ireland_row(**{"Time period": "2020-01-01"}),
        unavailable_row(
            **{
                "Time period": "2020-02-01",
                "Area code": "N92000002",
                "Area name": "Northern Ireland",
            }
        ),
        unavailable_row(
            **{
                "Time period": "2020-03-01",
                "Area code": "N92000002",
                "Area name": "Northern Ireland",
            }
        ),
    ]
    assert transform(spark, rows).count() == 3


# --------------------------------------------------------------------------- #
# Key and grain guards
# --------------------------------------------------------------------------- #


def test_null_area_name_raises(spark):
    with pytest.raises(ValueError, match="missing area name"):
        transform(spark, [raw_row(**{"Area name": None})])


def test_unparseable_date_raises(spark):
    # UK order, which the ISO parser rejects.
    with pytest.raises(ValueError, match="unparseable date"):
        transform(spark, [raw_row(**{"Time period": "01/01/2020"})])


def test_duplicate_grain_raises(spark):
    with pytest.raises(ValueError, match="grain broken"):
        transform(spark, [raw_row(), raw_row()])


def test_same_month_different_area_is_not_a_duplicate(spark):
    assert transform(spark, [raw_row(), northern_ireland_row()]).count() == 2


def test_two_uncoded_areas_are_not_a_duplicate(spark):
    """Both key on name, so the absent code is not a collision. Keying on area_code
    would collapse all eight rental areas into one."""
    rows = [
        rental_market_area_row(),
        rental_market_area_row(**{"Area name": "Lough Neagh Upper BRMA"}),
    ]
    assert transform(spark, rows).count() == 2


def test_uncoded_area_outside_northern_ireland_raises(spark):
    rows = [
        raw_row(
            **{
                "Area code": MARKER_NOT_APPLICABLE,
                "Area name": "Somewhere",
                "Region or country name": "South West",
            }
        )
    ]
    with pytest.raises(ValueError, match="no area code"):
        transform(spark, rows)


# --------------------------------------------------------------------------- #
# Typing
# --------------------------------------------------------------------------- #


def test_output_schema_types(spark):
    schema = {f.name: f.dataType for f in transform(spark, [raw_row()]).schema}

    assert schema["date"] == DateType()
    assert schema["area_code"] == StringType()
    assert schema["area_name"] == StringType()
    assert schema["region_or_country_name"] == StringType()
    assert schema["_source_file"] == StringType()
    assert schema["_ingestion_ts"] == TimestampType()
    for name in RENTAL_PRICE_COLUMNS:
        assert schema[name] == IntegerType(), name
    for name in set(MEASURE_COLUMNS) - RENTAL_PRICE_COLUMNS:
        assert schema[name] == DecimalType(18, 6), name


def test_output_column_order_is_canonical(spark):
    assert tuple(transform(spark, [raw_row()]).columns) == SILVER_COLUMNS


def test_every_typed_column_reaches_the_output(spark):
    """Four labels and 36 measures. A measure dropped from the projection would be
    invisible to the end-to-end test, which checks eight columns."""
    assert len(TYPED_COLUMNS) == 40
    assert set(TYPED_COLUMNS) <= set(transform(spark, [raw_row()]).columns)


def test_no_source_column_names_survive(spark):
    result = transform(spark, [raw_row()])
    assert not set(result.columns) & set(SOURCE_COLUMNS)


def test_date_parsed_from_iso(spark):
    result = transform(spark, [raw_row(**{"Time period": "2019-09-01"})])
    assert result.collect()[0]["date"] == dt.date(2019, 9, 1)


def test_six_decimal_precision_survives_typing(spark):
    """The sheet holds six decimals and displays one. A reader that rounds on the way
    in loses five of them without failing anything."""
    result = transform(spark, [raw_row(**{"Index": "81.413747"})])
    assert result.collect()[0]["price_index"] == Decimal("81.413747")


def test_small_change_survives_typing(spark):
    result = transform(spark, [raw_row(**{"Monthly change": "0.000034"})])
    assert result.collect()[0]["pct_change_1m"] == Decimal("0.000034")


def test_negative_change_survives_typing(spark):
    result = transform(spark, [raw_row(**{"Annual change": "-0.018248"})])
    assert result.collect()[0]["pct_change_12m"] == Decimal("-0.018248")


def test_rental_price_written_with_a_decimal_point_survives(spark):
    """try_cast('912.0' as int) is null, so a release that writes prices with a
    decimal point would blank every price column."""
    result = transform(spark, [raw_row(**{"Rental price": "912.0"})])
    assert result.collect()[0]["rental_price"] == 912


def test_not_applicable_in_the_parent_column_becomes_null(spark):
    assert transform(spark, [raw_row()]).collect()[0]["region_or_country_name"] is None


def test_uncoded_area_becomes_null_rather_than_a_marker(spark):
    assert transform(spark, [rental_market_area_row()]).collect()[0]["area_code"] is None


def test_null_measures_stay_null(spark):
    row = transform(spark, [raw_row(**{"Index detached": None})]).collect()[0]
    assert row["detached_price_index"] is None


def test_malformed_measure_raises_naming_the_column(spark):
    with pytest.raises(ValueError, match="flat_maisonette_rental_price"):
        transform(spark, [raw_row(**{"Rental price flat maisonette": "not a price"})])


# --------------------------------------------------------------------------- #
# Lineage
# --------------------------------------------------------------------------- #


def test_lineage_stamped_on_every_row(spark):
    rows = [raw_row(), raw_row(**{"Time period": "2020-02-01"})]
    out = transform(spark, rows).collect()
    assert all(row["_source_file"] == SOURCE_FILE for row in out)
    assert all(row["_ingestion_ts"] == INGESTION_TS for row in out)


def test_lineage_records_the_workbook_not_the_staged_csv(spark):
    """The CSV is an intermediate written to local disk and deleted at the end of the
    run. Recording it would name a path that no longer exists."""
    assert transform(spark, [raw_row()]).collect()[0]["_source_file"].endswith(".xlsx")


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
        # Series start: no prior month and no prior year.
        raw_row(
            **{
                "Time period": "2015-01-01",
                "Index": "81.258259",
                "Monthly change": MARKER_UNAVAILABLE,
                "Annual change": MARKER_UNAVAILABLE,
                "Rental price": "910",
            }
        ),
        # Second month: a monthly change exists, an annual one still does not.
        raw_row(
            **{
                "Time period": "2015-02-01",
                "Index": "81.413747",
                "Monthly change": "0.191351",
                "Annual change": MARKER_UNAVAILABLE,
                "Rental price": "912",
            }
        ),
        # A nation ONS has not published for this month.
        unavailable_row(
            **{
                "Time period": "2026-06-01",
                "Area code": "N92000002",
                "Area name": "Northern Ireland",
            }
        ),
    ]

    actual = transform(spark, rows).select(
        "date",
        "area_code",
        "area_name",
        "region_or_country_name",
        "price_index",
        "pct_change_1m",
        "pct_change_12m",
        "rental_price",
        "_source_file",
        "_ingestion_ts",
    )

    expected_schema = StructType(
        [
            StructField("date", DateType(), True),
            StructField("area_code", StringType(), True),
            StructField("area_name", StringType(), True),
            StructField("region_or_country_name", StringType(), True),
            StructField("price_index", DecimalType(18, 6), True),
            StructField("pct_change_1m", DecimalType(18, 6), True),
            StructField("pct_change_12m", DecimalType(18, 6), True),
            StructField("rental_price", IntegerType(), True),
            StructField("_source_file", StringType(), False),
            StructField("_ingestion_ts", TimestampType(), False),
        ]
    )
    expected = spark.createDataFrame(
        [
            (
                dt.date(2015, 1, 1),
                "K02000001",
                "United Kingdom",
                None,
                Decimal("81.258259"),
                None,
                None,
                910,
                SOURCE_FILE,
                INGESTION_TS,
            ),
            (
                dt.date(2015, 2, 1),
                "K02000001",
                "United Kingdom",
                None,
                Decimal("81.413747"),
                Decimal("0.191351"),
                None,
                912,
                SOURCE_FILE,
                INGESTION_TS,
            ),
            (
                dt.date(2026, 6, 1),
                "N92000002",
                "Northern Ireland",
                None,
                None,
                None,
                None,
                None,
                SOURCE_FILE,
                INGESTION_TS,
            ),
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_row_order=True)


# --------------------------------------------------------------------------- #
# Cover sheet publication date
# --------------------------------------------------------------------------- #

# The statement as ONS writes it, taken from the 2026-07 release.
PUBLISHED_LINE = (
    "The data tables in this spreadsheet were originally published at 9:30am on "
    "22 July 2026. Crown copyright \u00a9 2026."
)


def test_publication_date_parsed_from_the_real_statement():
    """This source's URL cannot be pattern-matched, so the landed filename records
    which release was asked for rather than which one ONS served. The publication date
    is the only value in the file that can contradict it."""
    assert parse_published_date(PUBLISHED_LINE) == dt.date(2026, 7, 22)


@pytest.mark.parametrize(
    "day, expected",
    [("1 March 2025", dt.date(2025, 3, 1)), ("01 March 2025", dt.date(2025, 3, 1))],
    ids=["unpadded", "zero padded"],
)
def test_day_is_parsed_with_or_without_a_leading_zero(day, expected):
    assert parse_published_date(f"originally published at 9:30am on {day}.") == expected


def test_december_is_parsed_as_written():
    """The month name is read rather than computed, so the year-end case that breaks
    ordinal arithmetic elsewhere does not arise here. Pinned so it stays that way."""
    assert parse_published_date("published on 31 December 2026") == dt.date(2026, 12, 31)


@pytest.mark.parametrize(
    "line",
    [
        None,
        "",
        "none found on 'Cover sheet'",
        "no date at all here",
        "published on 32 July 2026",
        "published on 22 Julyy 2026",
    ],
    ids=["none", "empty", "fallback text", "no date", "impossible day", "misspelt month"],
)
def test_unparseable_statement_yields_nothing_rather_than_raising(line):
    """A wording change should cost this signal and not the load. The line itself is
    recorded verbatim either way, so the change stays visible."""
    assert parse_published_date(line) is None


def test_the_fallback_string_does_not_parse():
    """The notebook records a missing line as text containing the word 'on', which a
    looser pattern would match against the sheet name rather than a date."""
    assert parse_published_date("none found on 'Cover sheet'") is None
