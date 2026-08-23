"""ONS Price Index of Private Rents: Bronze XLSX to Silver.

Grain: one row per (area_name, date), monthly.

The source is one sheet, Table 1, in a monthly workbook: four label columns then a
headline block and eight breakdowns, four metrics each. The sheet is converted to CSV
before it reaches Spark, so every value arrives as a string and is cast here. The
converter and the reasoning for it live in the notebook.

Key. ONS publishes no area code for the eight Northern Irish broad rental market
areas, writing [z] there instead, so area_code cannot be the key. Area name is unique
across the release and is the key alongside date. area_code stays nullable and is
what joins to HPI and Doogal where it exists.

Geography is not uniform. England and Wales report by local authority district,
Scotland and Northern Ireland by broad rental market area. The Scottish S33 codes and
the uncoded Northern Irish areas join to nothing else in Silver. That is a property of
the source, not a reason to drop rows here.

Markers. [x] is data that cannot exist and [z] is data that does not apply. Both
become null, but only after their positions are asserted: every [x] in the release
falls into one of three structural cases, and [z] reaches only the two label columns.
A marker anywhere else is a source change rather than a row to null out.

The latest months of the UK series are part imputed. Northern Ireland lags the other
nations, and ONS estimates it forward to publish a UK figure. Those rows are kept:
they are the published headline, the method is documented, and Great Britain covers
the same months fully measured. Which months are affected moves every release, so it
is derivable from the Northern Ireland rows rather than fixed.

Casts use try_cast rather than cast. ANSI mode is on from DBR 17.0, so a plain cast
raises on the first malformed cell with an error that does not name the column.
try_cast yields null instead, and assert_casts_preserved turns that null back into a
failure that names the column.

No I/O here. The conversion, the read, and the Delta write live in
databricks_src/silver/notebooks/05_ons.py. parse_published_date is the one function
here that reads the cover sheet rather than the data, because it is the only part of
that sheet with a meaning worth testing.
"""

from __future__ import annotations

import re
from datetime import date, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from databricks_src.silver.transforms.expressions import parsed_date

MEASURE_DDL = "decimal(18, 6)"
PRICE_DDL = "int"
DATE_FORMAT = "yyyy-MM-dd"

# First month of PIPR. Published rather than inferred, so a release that moves the
# series start fails the marker guard instead of redefining what counts as structural.
SERIES_START = "2015-01-01"

MARKER_UNAVAILABLE = "[x]"
MARKER_NOT_APPLICABLE = "[z]"
MARKERS: tuple[str, ...] = (MARKER_UNAVAILABLE, MARKER_NOT_APPLICABLE)

NORTHERN_IRELAND_CODE = "N92000002"
NORTHERN_IRELAND_NAME = "Northern Ireland"

# Label columns, source header to Silver name.
LABEL_MAP: dict[str, str] = {
    "Time period": "date",
    "Area code": "area_code",
    "Area name": "area_name",
    "Region or country name": "region_or_country_name",
}

# Metric as it appears in the header, and its Silver suffix.
METRICS: tuple[tuple[str, str], ...] = (
    ("Index", "price_index"),
    ("Monthly change", "pct_change_1m"),
    ("Annual change", "pct_change_12m"),
    ("Rental price", "rental_price"),
)

# Breakdown as it appears in the header, and its Silver prefix. The headline block
# carries no breakdown and so no prefix; it comes first in the sheet.
BREAKDOWNS: tuple[tuple[str, str], ...] = (
    ("", ""),
    ("one bed", "one_bed"),
    ("two bed", "two_bed"),
    ("three bed", "three_bed"),
    ("four or more bed", "four_or_more_bed"),
    ("detached", "detached"),
    ("semidetached", "semi_detached"),
    ("terraced", "terraced"),
    ("flat maisonette", "flat_maisonette"),
)


# The cover sheet's publication statement, which reads "originally published at
# 9:30am on 22 July 2026". This source is the one whose URL cannot be pattern-matched,
# so the landed filename records which release was asked for rather than which one ONS
# served. The publication date is the only thing in the file that can contradict the
# filename, and a mismatch between them is a fetch of the wrong release.
PUBLISHED_DATE = re.compile(r"\bon (\d{1,2} [A-Za-z]+ \d{4})")

PUBLISHED_DATE_FORMAT = "%d %B %Y"


def parse_published_date(line: str | None) -> date | None:
    """The publication date from the cover sheet statement, or None.

    Returns None rather than raising. The statement is a cross-check on the filename,
    not a contract: ONS rewording it should cost the signal and not the load. The
    caller records the line verbatim either way, so a wording change stays visible.
    """
    if not line:
        return None
    match = PUBLISHED_DATE.search(line)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), PUBLISHED_DATE_FORMAT).date()
    except ValueError:
        return None


def _measure_map() -> dict[str, str]:
    """Header to Silver name for the 36 measures, in sheet order.

    Generated from the two lists above. The header is perfectly regular, and 36
    hand-written pairs would be free to drift from them; assert_source_columns fails
    on the first run if either list is wrong.
    """
    return {
        f"{metric} {breakdown}".strip(): f"{prefix}_{suffix}".lstrip("_")
        for breakdown, prefix in BREAKDOWNS
        for metric, suffix in METRICS
    }


# Source header to Silver column. Output order follows this mapping.
COLUMN_MAP: dict[str, str] = {**LABEL_MAP, **_measure_map()}

SOURCE_COLUMNS: tuple[str, ...] = tuple(COLUMN_MAP)

# Columns produced by cast_columns, before lineage is stamped.
TYPED_COLUMNS: tuple[str, ...] = tuple(COLUMN_MAP.values())

LINEAGE_COLUMNS: tuple[str, ...] = ("_source_file", "_ingestion_ts")

SILVER_COLUMNS: tuple[str, ...] = TYPED_COLUMNS + LINEAGE_COLUMNS

KEY_COLUMNS: tuple[str, ...] = ("date", "area_name")

STRING_COLUMNS: frozenset[str] = frozenset(
    {"area_code", "area_name", "region_or_country_name"}
)

MEASURE_COLUMNS: tuple[str, ...] = tuple(_measure_map().values())

PCT_CHANGE_1M_COLUMNS: frozenset[str] = frozenset(
    name for name in MEASURE_COLUMNS if name.endswith("pct_change_1m")
)

PCT_CHANGE_12M_COLUMNS: frozenset[str] = frozenset(
    name for name in MEASURE_COLUMNS if name.endswith("pct_change_12m")
)

# Whole pounds. Routed through decimal on the way to int, since a release that writes
# a price with a decimal point would otherwise blank the column.
RENTAL_PRICE_COLUMNS: frozenset[str] = frozenset(
    name for name in MEASURE_COLUMNS if name.endswith("rental_price")
)


def _column_ddl(name: str) -> str:
    if name == "date":
        data_type = "DATE"
    elif name == "_ingestion_ts":
        data_type = "TIMESTAMP"
    elif name in STRING_COLUMNS or name == "_source_file":
        data_type = "STRING"
    elif name in RENTAL_PRICE_COLUMNS:
        data_type = PRICE_DDL.upper()
    else:
        data_type = MEASURE_DDL.upper()
    nullable = name not in set(KEY_COLUMNS) | set(LINEAGE_COLUMNS)
    return f"{name} {data_type}" + ("" if nullable else " NOT NULL")


def silver_table_ddl() -> str:
    """Column definitions for the Silver table, derived from the cast types.

    Generated rather than hand-written. The types are declared once above, and a
    second copy in SQL across 42 columns would be free to drift from the cast without
    anything failing.
    """
    return ",\n    ".join(_column_ddl(name) for name in SILVER_COLUMNS)


def assert_source_columns(raw_df: DataFrame) -> DataFrame:
    """Fail if the release does not carry exactly the expected column set."""
    actual = set(raw_df.columns)
    expected = set(SOURCE_COLUMNS)
    if actual != expected:
        raise ValueError(
            "ONS source columns do not match the expected set. "
            f"missing={sorted(expected - actual)} "
            f"unexpected={sorted(actual - expected)}"
        )
    return raw_df


def rename_columns(raw_df: DataFrame) -> DataFrame:
    """Apply the Silver names and fix column order.

    Backticks are required: every source header carries a space.
    """
    return raw_df.select(
        [F.col(f"`{source}`").alias(target) for source, target in COLUMN_MAP.items()]
    )


def _parsed_date() -> Column:
    """The date column parsed, for guards that run before casting."""
    return parsed_date("date", DATE_FORMAT)


def is_northern_ireland() -> Column:
    """True for the Northern Ireland country row and its eight rental market areas.

    Tested two ways because the eight areas carry no code and the country row's
    region_or_country_name is not relied on.
    """
    return F.col("area_code").eqNullSafe(F.lit(NORTHERN_IRELAND_CODE)) | F.col(
        "region_or_country_name"
    ).eqNullSafe(F.lit(NORTHERN_IRELAND_NAME))


def row_fully_unavailable() -> Column:
    """True when every measure on the row is [x], which is how a month ONS has not
    published yet arrives."""
    condition: Column | None = None
    for name in MEASURE_COLUMNS:
        test = F.col(name).eqNullSafe(F.lit(MARKER_UNAVAILABLE))
        condition = test if condition is None else condition & test
    return condition


def unexpected_unavailable() -> Column:
    """Comma-joined names of the measures carrying [x] outside its three structural
    positions.

    Monthly change has no prior month in the first month of the series. Annual change
    has no prior year in the first twelve. A nation that ONS has not published yet
    carries [x] across the whole row. Every [x] in the release falls into one of the
    three; anything else means the source changed.

    concat_ws drops nulls, so a clean row yields an empty string and one pass covers
    all 36 columns while still naming which one failed.
    """
    series_start = F.lit(SERIES_START).cast("date")
    first_month = _parsed_date() == series_start
    first_year = _parsed_date() < F.add_months(series_start, 12)
    unpublished = row_fully_unavailable()

    def expected(name: str) -> Column:
        if name in PCT_CHANGE_1M_COLUMNS:
            allowed = first_month | unpublished
        elif name in PCT_CHANGE_12M_COLUMNS:
            allowed = first_year | unpublished
        else:
            allowed = unpublished
        # An unparseable date makes the position tests null, which would drop the
        # column from the offender list rather than report it.
        return F.coalesce(allowed, F.lit(False))

    return F.concat_ws(
        ",",
        *[
            F.when(
                F.col(name).eqNullSafe(F.lit(MARKER_UNAVAILABLE)) & ~expected(name),
                F.lit(name),
            )
            for name in MEASURE_COLUMNS
        ],
    )


def assert_unavailable_is_structural(df: DataFrame) -> DataFrame:
    """Fail on any [x] that is not one of the three published gaps."""
    offenders = (
        df.select(
            "date",
            "area_code",
            "area_name",
            unexpected_unavailable().alias("unexpected"),
        )
        .filter(F.col("unexpected") != "")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            f"ONS rows carry {MARKER_UNAVAILABLE} outside the first month, the first "
            "twelve months, and an unpublished nation, so the source has a gap this "
            f"transform does not model: {[row.asDict() for row in offenders]}"
        )
    return df


def assert_unpublished_rows_are_northern_ireland(df: DataFrame) -> DataFrame:
    """Fail if a whole-row gap appears outside Northern Ireland.

    Northern Ireland is the nation that lags. The same pattern under another
    geography would be a different fault and would pass the structural guard.
    """
    offenders = (
        df.filter(row_fully_unavailable() & ~is_northern_ireland())
        .select("date", "area_code", "area_name", "region_or_country_name")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "ONS rows carry no measures at all outside Northern Ireland: "
            f"{[row.asDict() for row in offenders]}"
        )
    return df


def assert_unpublished_months_are_trailing(df: DataFrame) -> DataFrame:
    """Fail if an unpublished month sits inside the published series.

    A lagging nation is missing its latest months. A gap in the middle would pass the
    structural guard, since that guard tests position by column rather than by date.
    """
    offenders = (
        df.groupBy("area_name")
        .agg(
            F.max(
                F.when(~row_fully_unavailable(), _parsed_date())
            ).alias("last_published"),
            F.min(
                F.when(row_fully_unavailable(), _parsed_date())
            ).alias("first_unpublished"),
        )
        .filter(F.col("first_unpublished") < F.col("last_published"))
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "ONS unpublished months are not the latest ones, so the series has an "
            f"interior gap: {[row.asDict() for row in offenders]}"
        )
    return df


def assert_not_applicable_confined(df: DataFrame) -> DataFrame:
    """Fail if [z] reaches a measure column.

    [z] marks a value that does not apply to the row's geography, and it occurs only
    in area_code and region_or_country_name. In a measure it would mean something new
    about the source rather than a value to null.
    """
    present = [
        F.count_if(F.col(name).eqNullSafe(F.lit(MARKER_NOT_APPLICABLE))).alias(name)
        for name in MEASURE_COLUMNS
    ]
    counts = df.agg(*present).collect()[0].asDict()
    offenders = {name: count for name, count in counts.items() if count}
    if offenders:
        raise ValueError(
            f"ONS measures carry {MARKER_NOT_APPLICABLE}, which belongs only to the "
            f"label columns. column: rows = {offenders}"
        )
    return df


def null_markers(df: DataFrame) -> DataFrame:
    """Replace both markers with null.

    Runs after the position guards, which is what makes this safe: by here the
    absence each marker records is already known to be one the transform models.
    """
    return df.select(
        [
            F.when(~F.col(name).isin(*MARKERS), F.col(name)).alias(name)
            for name in TYPED_COLUMNS
        ]
    )


def _cast_expr(name: str) -> Column:
    if name == "date":
        return parsed_date(name, DATE_FORMAT).alias(name)
    if name in STRING_COLUMNS:
        return F.col(name)
    if name in RENTAL_PRICE_COLUMNS:
        return F.expr(
            f"try_cast(try_cast(`{name}` as {MEASURE_DDL}) as {PRICE_DDL})"
        ).alias(name)
    return F.expr(f"try_cast(`{name}` as {MEASURE_DDL})").alias(name)


def cast_columns(df: DataFrame) -> DataFrame:
    """Type every column. Indices and changes share one decimal type, rental prices
    are whole pounds."""
    return df.select([_cast_expr(name) for name in TYPED_COLUMNS])


def assert_casts_preserved(cleared: DataFrame, typed: DataFrame) -> DataFrame:
    """Fail if a populated measure did not survive its cast.

    try_cast returns null on a malformed value. Comparing non-null counts per column
    turns that into a failure that names the column, which the ANSI cast exception
    does not. Compares against the marker-cleared frame, so a nulled marker is not
    read as a lost value.

    Keys are excluded. A null date is caught by assert_keys_present whatever its
    cause, which covers more than this check and reports the offending row.
    """
    checked = [name for name in TYPED_COLUMNS if name not in KEY_COLUMNS]
    before = (
        cleared.agg(*[F.count(F.col(name)).alias(name) for name in checked])
        .collect()[0]
        .asDict()
    )
    after = (
        typed.agg(*[F.count(F.col(name)).alias(name) for name in checked])
        .collect()[0]
        .asDict()
    )

    lost = {
        name: (before[name], after[name])
        for name in checked
        if before[name] != after[name]
    }
    if lost:
        raise ValueError(
            "ONS values did not survive typing, so the source holds malformed "
            f"entries. column: (populated_before, populated_after) = {lost}"
        )
    return typed


def assert_keys_present(df: DataFrame) -> DataFrame:
    """Fail on a missing area name or a date that did not parse."""
    offenders = (
        df.filter(F.col("date").isNull() | F.col("area_name").isNull())
        .select("area_code", "area_name", "date")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "ONS rows carry a missing area name or an unparseable date "
            f"(expected {DATE_FORMAT}): {[row.asDict() for row in offenders]}"
        )
    return df


def assert_missing_code_is_northern_ireland(df: DataFrame) -> DataFrame:
    """Fail if an uncoded geography is anything but a Northern Irish rental area.

    A null area_code is the [z] the source publishes for those eight areas. A release
    that starts coding them, or that drops a code elsewhere, changes what joins to the
    rest of Silver and should not pass quietly.
    """
    offenders = (
        df.filter(
            F.col("area_code").isNull()
            & ~F.col("region_or_country_name").eqNullSafe(
                F.lit(NORTHERN_IRELAND_NAME)
            )
        )
        .select("area_name", "region_or_country_name", "date")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "ONS rows have no area code outside the Northern Irish rental areas: "
            f"{[row.asDict() for row in offenders]}"
        )
    return df


def assert_grain_unique(df: DataFrame) -> DataFrame:
    """Fail on a repeated (area_name, date), which is the table's grain."""
    duplicates = (
        df.groupBy("area_name", "date")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if duplicates:
        raise ValueError(
            "ONS grain broken, (area_name, date) is not unique: "
            f"{[row.asDict() for row in duplicates]}"
        )
    return df


def transform_ons(
    raw_df: DataFrame,
    source_file: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """Converted ONS Table 1 CSV, read as all-string, to the Silver frame.

    Args:
        raw_df: the converted sheet read with header on and inferSchema off.
        source_file: bronze path of the workbook read, recorded as lineage. The
            staged CSV is an intermediate and is not what Silver records.
        ingestion_ts: load timestamp, recorded as lineage. Passed in rather than
            generated here so the transform stays deterministic under test.

    Returns:
        One row per (area_name, date), with the columns named in SILVER_COLUMNS.
    """
    assert_source_columns(raw_df)
    renamed = rename_columns(raw_df)
    assert_not_applicable_confined(renamed)
    assert_unavailable_is_structural(renamed)
    assert_unpublished_rows_are_northern_ireland(renamed)
    assert_unpublished_months_are_trailing(renamed)
    cleared = null_markers(renamed)
    typed = cast_columns(cleared)
    assert_casts_preserved(cleared, typed)
    assert_keys_present(typed)
    assert_missing_code_is_northern_ireland(typed)
    assert_grain_unique(typed)
    return (
        typed.withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_ts", F.lit(ingestion_ts).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )
