"""UK House Price Index: Bronze CSV to Silver.

Grain: one row per (area_code, date), monthly.

The source is a wide monthly panel of 54 columns: a headline price and index block,
then nine breakdowns (property type, funding, buyer type, build age). Every value
arrives as a string and is cast here. The reader must not infer types, because
inference varies with the values a given release happens to carry: prices were
published to five decimal places in earlier vintages and as whole pounds in current
ones.

Coverage floor. The published file carries a derived back-series constructed from the
older ONS index, extending before each nation's native Land Registry coverage. Only
measured rows are kept: England and Wales from 1995, Scotland from 2004, Northern
Ireland from 2005. A composite geography floors at the latest native start among the
nations it spans, so no part-derived United Kingdom or Great Britain row survives.

Geographic scoping for downstream joins is a Gold concern. This module removes only
what is not measured.

Casts use try_cast rather than cast. ANSI mode is on from DBR 17.0, so a plain cast
raises on the first malformed cell with an error that does not name the column.
try_cast yields null instead, and assert_casts_preserved turns that null back into a
failure that names the column.

No I/O here. The read and the Delta write live in
databricks_src/silver/notebooks/02_hpi.py.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MEASURE_DDL = "decimal(18, 6)"
VOLUME_DDL = "int"
DATE_FORMAT = "dd/MM/yyyy"

# Source header to Silver column. Output order follows this mapping.
COLUMN_MAP: dict[str, str] = {
    "Date": "date",
    "RegionName": "region_name",
    "AreaCode": "area_code",
    "AveragePrice": "avg_price",
    "Index": "price_index",
    "IndexSA": "price_index_seasonally_adjusted",
    "1m%Change": "pct_change_1m",
    "12m%Change": "pct_change_12m",
    "AveragePriceSA": "avg_price_seasonally_adjusted",
    "SalesVolume": "sales_volume",
    "DetachedPrice": "detached_price",
    "DetachedIndex": "detached_price_index",
    "Detached1m%Change": "detached_pct_change_1m",
    "Detached12m%Change": "detached_pct_change_12m",
    "SemiDetachedPrice": "semi_detached_price",
    "SemiDetachedIndex": "semi_detached_price_index",
    "SemiDetached1m%Change": "semi_detached_pct_change_1m",
    "SemiDetached12m%Change": "semi_detached_pct_change_12m",
    "TerracedPrice": "terraced_price",
    "TerracedIndex": "terraced_price_index",
    "Terraced1m%Change": "terraced_pct_change_1m",
    "Terraced12m%Change": "terraced_pct_change_12m",
    "FlatPrice": "flat_price",
    "FlatIndex": "flat_price_index",
    "Flat1m%Change": "flat_pct_change_1m",
    "Flat12m%Change": "flat_pct_change_12m",
    "CashPrice": "cash_price",
    "CashIndex": "cash_price_index",
    "Cash1m%Change": "cash_pct_change_1m",
    "Cash12m%Change": "cash_pct_change_12m",
    "CashSalesVolume": "cash_sales_volume",
    "MortgagePrice": "mortgage_price",
    "MortgageIndex": "mortgage_price_index",
    "Mortgage1m%Change": "mortgage_pct_change_1m",
    "Mortgage12m%Change": "mortgage_pct_change_12m",
    "MortgageSalesVolume": "mortgage_sales_volume",
    "FTBPrice": "first_time_buyer_price",
    "FTBIndex": "first_time_buyer_price_index",
    "FTB1m%Change": "first_time_buyer_pct_change_1m",
    "FTB12m%Change": "first_time_buyer_pct_change_12m",
    "FOOPrice": "former_owner_occupier_price",
    "FOOIndex": "former_owner_occupier_price_index",
    "FOO1m%Change": "former_owner_occupier_pct_change_1m",
    "FOO12m%Change": "former_owner_occupier_pct_change_12m",
    "NewPrice": "new_build_price",
    "NewIndex": "new_build_price_index",
    "New1m%Change": "new_build_pct_change_1m",
    "New12m%Change": "new_build_pct_change_12m",
    "NewSalesVolume": "new_build_sales_volume",
    "OldPrice": "existing_resold_price",
    "OldIndex": "existing_resold_price_index",
    "Old1m%Change": "existing_resold_pct_change_1m",
    "Old12m%Change": "existing_resold_pct_change_12m",
    "OldSalesVolume": "existing_resold_sales_volume",
}

SOURCE_COLUMNS: tuple[str, ...] = tuple(COLUMN_MAP)

# Columns produced by cast_columns, before lineage is stamped.
TYPED_COLUMNS: tuple[str, ...] = tuple(COLUMN_MAP.values())

LINEAGE_COLUMNS: tuple[str, ...] = ("_source_file", "_ingestion_ts")

SILVER_COLUMNS: tuple[str, ...] = TYPED_COLUMNS + LINEAGE_COLUMNS

KEY_COLUMNS: tuple[str, ...] = ("date", "region_name", "area_code")

STRING_COLUMNS: frozenset[str] = frozenset({"region_name", "area_code"})

VOLUME_COLUMNS: frozenset[str] = frozenset(
    {
        "sales_volume",
        "cash_sales_volume",
        "mortgage_sales_volume",
        "new_build_sales_volume",
        "existing_resold_sales_volume",
    }
)

# First year of measured Land Registry coverage, by ONS area-code prefix.
NATION_START_YEAR: dict[str, int] = {"E": 1995, "W": 1995, "S": 2004, "N": 2005}

# Composite geographies floor at the latest native start they span.
COMPOSITE_START_YEAR: dict[str, int] = {
    "K04000001": 1995,  # England and Wales
    "K03000001": 2004,  # Great Britain
    "K02000001": 2005,  # United Kingdom
}


def native_start_year() -> Column:
    """First measured year for a row's geography, null where unmapped.

    Composites are matched on the full code before any prefix rule, so a K-code can
    never pick up a nation floor.
    """
    code = F.col("area_code")
    expr: Column | None = None
    for area_code, year in COMPOSITE_START_YEAR.items():
        condition = code == area_code
        expr = (
            F.when(condition, F.lit(year))
            if expr is None
            else expr.when(condition, F.lit(year))
        )
    for prefix, year in NATION_START_YEAR.items():
        expr = expr.when(code.startswith(prefix), F.lit(year))
    return expr


def _column_ddl(name: str) -> str:
    if name == "date":
        data_type = "DATE"
    elif name == "_ingestion_ts":
        data_type = "TIMESTAMP"
    elif name in STRING_COLUMNS or name == "_source_file":
        data_type = "STRING"
    elif name in VOLUME_COLUMNS:
        data_type = VOLUME_DDL.upper()
    else:
        data_type = MEASURE_DDL.upper()
    nullable = name not in set(KEY_COLUMNS) | set(LINEAGE_COLUMNS)
    return f"{name} {data_type}" + ("" if nullable else " NOT NULL")


def silver_table_ddl() -> str:
    """Column definitions for the Silver table, derived from the cast types.

    Generated rather than hand-written. The types are declared once above, and a
    second copy in SQL across 56 columns would be free to drift from the cast without
    anything failing.
    """
    return ",\n    ".join(_column_ddl(name) for name in SILVER_COLUMNS)


def assert_source_columns(raw_df: DataFrame) -> DataFrame:
    """Fail if the release does not carry exactly the expected column set."""
    actual = set(raw_df.columns)
    expected = set(SOURCE_COLUMNS)
    if actual != expected:
        raise ValueError(
            "HPI source columns do not match the expected set. "
            f"missing={sorted(expected - actual)} "
            f"unexpected={sorted(actual - expected)}"
        )
    return raw_df


def rename_columns(raw_df: DataFrame) -> DataFrame:
    """Apply the Silver names and fix column order.

    Backticks are required: several source headers open with a digit and carry a
    percent sign.
    """
    return raw_df.select(
        [F.col(f"`{source}`").alias(target) for source, target in COLUMN_MAP.items()]
    )


def _cast_expr(name: str) -> Column:
    if name == "date":
        return F.expr(f"try_to_date(`{name}`, '{DATE_FORMAT}')").alias(name)
    if name in STRING_COLUMNS:
        return F.col(name)
    if name in VOLUME_COLUMNS:
        # Volumes route through decimal: try_cast('388.0' as int) is null, so a
        # release that writes counts with a decimal point would blank the column.
        return F.expr(
            f"try_cast(try_cast(`{name}` as {MEASURE_DDL}) as {VOLUME_DDL})"
        ).alias(name)
    return F.expr(f"try_cast(`{name}` as {MEASURE_DDL})").alias(name)


def cast_columns(df: DataFrame) -> DataFrame:
    """Type every column. Measures share one decimal type, volumes are counts."""
    return df.select([_cast_expr(name) for name in TYPED_COLUMNS])


def assert_casts_preserved(renamed: DataFrame, typed: DataFrame) -> DataFrame:
    """Fail if a populated measure did not survive its cast.

    try_cast returns null on a malformed value. Comparing non-null counts per column
    turns that into a failure that names the column, which the ANSI cast exception
    does not.

    Keys are excluded. A null date is caught by assert_keys_present whatever its
    cause, which covers more than this check and reports the offending row.
    """
    checked = [name for name in TYPED_COLUMNS if name not in KEY_COLUMNS]
    before = (
        renamed.agg(*[F.count(F.col(name)).alias(name) for name in checked])
        .collect()[0]
        .asDict()
    )
    after = (
        typed.agg(*[F.count(F.col(name)).alias(name) for name in checked])
        .collect()[0]
        .asDict()
    )

    lost = {name: (before[name], after[name]) for name in checked if before[name] != after[name]}
    if lost:
        raise ValueError(
            "HPI values did not survive typing, so the source holds malformed "
            f"entries. column: (populated_before, populated_after) = {lost}"
        )
    return typed


def assert_keys_present(df: DataFrame) -> DataFrame:
    """Fail on a missing area code or a date that did not parse."""
    offenders = (
        df.filter(F.col("date").isNull() | F.col("area_code").isNull())
        .select("region_name", "area_code", "date")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "HPI rows carry a missing area code or an unparseable date "
            f"(expected {DATE_FORMAT}): {[row.asDict() for row in offenders]}"
        )
    return df


def assert_geographies_mapped(df: DataFrame) -> DataFrame:
    """Fail on any geography with no coverage floor.

    Runs before the floor filter: an unmapped code yields a null start year, and the
    filter would drop those rows silently.
    """
    unmapped = (
        df.filter(native_start_year().isNull())
        .select("region_name", "area_code")
        .distinct()
        .limit(10)
        .collect()
    )
    if unmapped:
        raise ValueError(
            "HPI geographies have no coverage floor mapped, so their measured "
            f"start year is unknown: {[row.asDict() for row in unmapped]}"
        )
    return df


def drop_derived_back_series(df: DataFrame) -> DataFrame:
    """Keep only rows from each geography's measured era."""
    return df.filter(F.year(F.col("date")) >= native_start_year())


def assert_grain_unique(df: DataFrame) -> DataFrame:
    """Fail on a repeated (area_code, date), which is the table's grain."""
    duplicates = (
        df.groupBy("area_code", "date")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if duplicates:
        raise ValueError(
            "HPI grain broken, (area_code, date) is not unique: "
            f"{[row.asDict() for row in duplicates]}"
        )
    return df


def transform_hpi(
    raw_df: DataFrame,
    source_file: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """Bronze HPI CSV, read as all-string, to the Silver frame.

    Args:
        raw_df: the release CSV read with header on and inferSchema off.
        source_file: bronze path of the vintage read, recorded as lineage.
        ingestion_ts: load timestamp, recorded as lineage. Passed in rather than
            generated here so the transform stays deterministic under test.

    Returns:
        One row per (area_code, date) in the measured era, with the columns named in
        SILVER_COLUMNS.
    """
    assert_source_columns(raw_df)
    renamed = rename_columns(raw_df)
    typed = cast_columns(renamed)
    assert_casts_preserved(renamed, typed)
    assert_keys_present(typed)
    assert_geographies_mapped(typed)
    kept = drop_derived_back_series(typed)
    # Grain is asserted on the output, not the source: a duplicate among rows this
    # transform discards is not a contract the table makes.
    assert_grain_unique(kept)
    return (
        kept.withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_ts", F.lit(ingestion_ts).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )
