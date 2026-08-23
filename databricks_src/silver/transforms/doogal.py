"""Doogal UK postcode lookup (ONSPD mirror): Bronze CSV to Silver.

Grain: one row per postcode, live and terminated.

The source is a single CSV inside a quarterly ZIP, 60 columns with a header. Every
value arrives as a string and is cast here.

Column selection. The file carries an ONS Postcode Directory spine alongside the
publisher's own enrichment. The spine is kept. Eighteen columns are dropped: five
that restate a retained column, nine derived by the publisher with no stated method
or vintage, two comma-separated lists that are multi-valued at this grain, and the
two deprivation columns, which hold four national indices at four vintages and four
scales under one name. The publisher's field documentation lists a column the file
does not carry, so the header is the contract and the documentation is reference.

Terminated postcodes are kept. They are a third of the file, and PPD transfers run
back to 1995 against postcodes since withdrawn.

The BF postcode area is British Forces Post Office and is non-geographic: every
administrative and statistical column is null there, and the coordinates are
overseas. Those rows are measured data and are kept, so null geography is
legitimate. assert_non_geographic_are_bfpo confines it to that area, since an
unmapped geography arriving later would otherwise be indistinguishable from them.

Positional quality 9 means ONS publishes no grid reference. The publisher leaves
easting and northing blank on those rows and writes 0 into latitude and longitude. A
zero pair is a valid coordinate in the Atlantic and survives every range check, so
the grid columns are treated as the honest signal and the coordinates are nulled.

Casts use try_cast rather than cast. ANSI mode is on from DBR 17.0, so a plain cast
raises on the first malformed cell with an error that does not name the column.
try_cast yields null instead, and assert_casts_preserved turns that null back into a
failure that names the column.

No I/O here. The unzip, the read, and the Delta write live in
databricks_src/silver/notebooks/04_doogal.py.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

COORD_DDL = "double"
GRID_DDL = "int"
CODE_DDL = "tinyint"
DATE_FORMAT = "yyyy-MM-dd"

# Postcode area for British Forces Post Office addresses, introduced 2012. The only
# area where null administrative geography is expected.
BFPO_POSTCODE_AREA = "BF"

# Positional quality flag for a postcode with no published grid reference.
NO_GRID_REFERENCE = 9

# The published header, in file order. The reader binds by name, so the guard
# compares sets; this tuple is also what the test suite checks the map against.
SOURCE_COLUMNS: tuple[str, ...] = (
    "Postcode",
    "In Use?",
    "Latitude",
    "Longitude",
    "Easting",
    "Northing",
    "Grid Ref",
    "County",
    "District",
    "Ward",
    "District Code",
    "Ward Code",
    "Country",
    "County Code",
    "Introduced",
    "Terminated",
    "Parish",
    "National Park",
    "Population",
    "Households",
    "Built up area",
    "Lower layer super output area",
    "Rural/urban",
    "Region",
    "Altitude",
    "London zone",
    "LSOA Code",
    "Local authority",
    "MSOA Code",
    "Middle layer super output area",
    "Parish Code",
    "Census output area",
    "Index of Multiple Deprivation",
    "Quality",
    "User Type",
    "Last updated",
    "Nearest station",
    "Distance to station",
    "Postcode area",
    "Postcode district",
    "Police force",
    "Plus Code",
    "Average Income",
    "Travel To Work Area",
    "ITL level 2",
    "ITL level 3",
    "UPRNs",
    "Distance to sea",
    "LSOA21 Code",
    "Lower layer super output area 2021",
    "MSOA21 Code",
    "Middle layer super output area 2021",
    "Census output area 2021",
    "IMD decile",
    "Constituency Code 2024",
    "Constituency Name 2024",
    "Property Type",
    "Roads",
    "FixPhrase",
    "Rural/urban 2021",
)

# Source header to Silver column. Output order follows this mapping, and the
# columns absent from it are the ones rename_columns drops.
COLUMN_MAP: dict[str, str] = {
    "Postcode": "postcode",
    "Introduced": "introduced_date",
    "Terminated": "terminated_date",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Easting": "easting",
    "Northing": "northing",
    "Quality": "positional_quality",
    "Country": "country",
    "Region": "region",
    "County": "county",
    "County Code": "county_code",
    # Source calls this "Local authority", but it holds the county council area and
    # is unpopulated for unitary authorities and London. "District" is the local
    # authority district.
    "Local authority": "county_council",
    "District": "district",
    "District Code": "district_code",
    "Ward": "ward",
    "Ward Code": "ward_code",
    "Parish": "parish",
    "Parish Code": "parish_code",
    "LSOA Code": "lsoa_code_2011",
    "Lower layer super output area": "lsoa_name_2011",
    "MSOA Code": "msoa_code_2011",
    "Middle layer super output area": "msoa_name_2011",
    "Census output area": "output_area_code_2011",
    "Rural/urban": "rural_urban_2011",
    "LSOA21 Code": "lsoa_code_2021",
    "Lower layer super output area 2021": "lsoa_name_2021",
    "MSOA21 Code": "msoa_code_2021",
    "Middle layer super output area 2021": "msoa_name_2021",
    "Census output area 2021": "output_area_code_2021",
    "Rural/urban 2021": "rural_urban_2021",
    "Built up area": "built_up_area",
    "National Park": "national_park",
    "Travel To Work Area": "travel_to_work_area",
    "ITL level 2": "itl_level_2",
    "ITL level 3": "itl_level_3",
    "Constituency Code 2024": "constituency_code_2024",
    "Constituency Name 2024": "constituency_name_2024",
    "Police force": "police_force",
    "London zone": "london_travel_zone",
    "User Type": "user_type",
    "Last updated": "source_last_updated",
}

# Dropped deliberately, listed so the test suite can prove every source column is
# accounted for. A column that is neither mapped nor named here would be dropped by
# rename_columns without anything failing.
DROPPED_COLUMNS: frozenset[str] = frozenset(
    {
        # Restate a retained column.
        "In Use?",
        "Grid Ref",
        "Postcode area",
        "Postcode district",
        "Plus Code",
        # Publisher-derived with no stated method or vintage. Population, Households
        # and Average Income are also fixed-vintage estimates at a coarser grain;
        # the output area and MSOA codes are retained so Gold can make the join.
        "Altitude",
        "Nearest station",
        "Distance to station",
        "Distance to sea",
        "Property Type",
        "Population",
        "Households",
        "Average Income",
        "FixPhrase",
        # Comma-separated lists. Multi-valued at postcode grain.
        "UPRNs",
        "Roads",
        # Four national indices at four vintages and four scales under one name,
        # plus a 0 that appears in none of the published ranges.
        "Index of Multiple Deprivation",
        "IMD decile",
    }
)

# Columns produced by cast_columns, before lineage is stamped.
TYPED_COLUMNS: tuple[str, ...] = tuple(COLUMN_MAP.values())

LINEAGE_COLUMNS: tuple[str, ...] = ("_source_file", "_ingestion_ts")

SILVER_COLUMNS: tuple[str, ...] = TYPED_COLUMNS + LINEAGE_COLUMNS

KEY_COLUMNS: tuple[str, ...] = ("postcode",)

DATE_COLUMNS: frozenset[str] = frozenset(
    {"introduced_date", "terminated_date", "source_last_updated"}
)

COORD_COLUMNS: frozenset[str] = frozenset({"latitude", "longitude"})

GRID_COLUMNS: frozenset[str] = frozenset({"easting", "northing"})

CODE_COLUMNS: frozenset[str] = frozenset(
    {"positional_quality", "user_type", "london_travel_zone"}
)

# Every column that changes type. The remaining 32 stay strings, so a count
# comparison over them would compare a frame with itself.
CAST_CHECKED_COLUMNS: tuple[str, ...] = tuple(
    name
    for name in TYPED_COLUMNS
    if name in DATE_COLUMNS | COORD_COLUMNS | GRID_COLUMNS | CODE_COLUMNS
)

# Published code sets, checked before casting so an unrecognised value is named
# rather than turned into a null by try_cast. Null is admissible: it is the BFPO
# case, which assert_non_geographic_are_bfpo confines. Quality 7 is published
# ("deliberately left blank") and absent from current data, which is a fact about
# the release rather than the contract.
DOMAINS: dict[str, tuple[str, ...]] = {
    "country": ("England", "Scotland", "Wales", "Northern Ireland"),
    "positional_quality": ("1", "2", "3", "4", "5", "6", "7", "8", "9"),
    "user_type": ("0", "1"),
    "london_travel_zone": ("1", "2", "3", "4", "5", "6", "7", "8", "9"),
}


def _column_ddl(name: str) -> str:
    if name in DATE_COLUMNS:
        data_type = "DATE"
    elif name == "_ingestion_ts":
        data_type = "TIMESTAMP"
    elif name in COORD_COLUMNS:
        data_type = COORD_DDL.upper()
    elif name in GRID_COLUMNS:
        data_type = GRID_DDL.upper()
    elif name in CODE_COLUMNS:
        data_type = CODE_DDL.upper()
    else:
        data_type = "STRING"
    nullable = name not in set(KEY_COLUMNS) | set(LINEAGE_COLUMNS)
    return f"{name} {data_type}" + ("" if nullable else " NOT NULL")


def silver_table_ddl() -> str:
    """Column definitions for the Silver table, derived from the cast types.

    Generated rather than hand-written, so the DDL cannot drift from the cast.
    """
    return ",\n    ".join(_column_ddl(name) for name in SILVER_COLUMNS)


def assert_source_columns(raw_df: DataFrame) -> DataFrame:
    """Fail if the release does not carry exactly the expected column set.

    Also catches a byte order mark surviving into the first column name. It is
    present in the file and stripped by the reader, so a reader option change would
    surface here as a missing Postcode and an unexpected one.
    """
    actual = set(raw_df.columns)
    expected = set(SOURCE_COLUMNS)
    if actual != expected:
        raise ValueError(
            "Doogal source columns do not match the expected set. "
            f"missing={sorted(expected - actual)} "
            f"unexpected={sorted(actual - expected)}"
        )
    return raw_df


def assert_in_use_restates_terminated(raw_df: DataFrame) -> DataFrame:
    """Fail if In Use? carries information beyond Terminated.

    In Use? is dropped, because Yes holds exactly when Terminated is null across the
    whole file. Runs on the source frame, since the column does not reach the
    renamed one. A break here would mean the publisher changed what the column
    means, not that a row is wrong.
    """
    offenders = (
        raw_df.filter(
            (F.col("`In Use?`") == "Yes") != F.col("`Terminated`").isNull()
        )
        .select(F.col("`Postcode`"), F.col("`In Use?`"), F.col("`Terminated`"))
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "Doogal In Use? no longer restates Terminated, so dropping it would "
            f"lose information: {[row.asDict() for row in offenders]}"
        )
    return raw_df


def rename_columns(raw_df: DataFrame) -> DataFrame:
    """Apply the Silver names, fix column order, and drop the unmapped columns.

    Backticks are required: several source headers carry spaces, a question mark, or
    a slash. Any guard needing a dropped column has to run before this.
    """
    return raw_df.select(
        [F.col(f"`{source}`").alias(target) for source, target in COLUMN_MAP.items()]
    )


def unrecognised_domains() -> Column:
    """Comma-joined names of the code columns whose value is outside its set.

    concat_ws drops nulls, so a clean row yields an empty string and one pass covers
    every code column while still naming which one failed. A null value is not an
    offender here: it is the BFPO case.
    """
    return F.concat_ws(
        ",",
        *[
            F.when(
                F.col(column).isNotNull() & ~F.col(column).isin(*values),
                F.lit(column),
            )
            for column, values in DOMAINS.items()
        ],
    )


def assert_domains_known(df: DataFrame) -> DataFrame:
    """Fail on any code outside its published set.

    A new code is a source change, not a row to skip: keeping it would put a value in
    Silver that no downstream model knows how to read. Runs before casting, since
    try_cast would turn an unrecognised code into a null and hand the diagnosis to
    assert_casts_preserved, which cannot name the value.
    """
    offenders = (
        df.select(
            "postcode",
            *DOMAINS,
            unrecognised_domains().alias("unrecognised"),
        )
        .filter(F.col("unrecognised") != "")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "Doogal rows carry a code outside its published set "
            f"({ {column: list(values) for column, values in DOMAINS.items()} }): "
            f"{[row.asDict() for row in offenders]}"
        )
    return df


def _cast_expr(name: str) -> Column:
    if name in DATE_COLUMNS:
        return F.expr(f"CAST(try_to_timestamp(\{name}`, '{DATE_FORMAT}') AS DATE)").alias(name)
    if name in COORD_COLUMNS:
        return F.expr(f"try_cast(`{name}` as {COORD_DDL})").alias(name)
    if name in GRID_COLUMNS:
        return F.expr(f"try_cast(`{name}` as {GRID_DDL})").alias(name)
    if name in CODE_COLUMNS:
        return F.expr(f"try_cast(`{name}` as {CODE_DDL})").alias(name)
    return F.col(name)


def cast_columns(df: DataFrame) -> DataFrame:
    """Type the dates, coordinates, grid references, and code columns.

    Coordinates are double rather than decimal. Five decimal places is metre
    resolution, and nothing here depends on exact equality the way BoE change
    detection does.
    """
    return df.select([_cast_expr(name) for name in TYPED_COLUMNS])


def assert_casts_preserved(renamed: DataFrame, typed: DataFrame) -> DataFrame:
    """Fail if a populated value did not survive its cast.

    try_cast returns null on a malformed value. Comparing non-null counts per column
    turns that into a failure that names the column, which the ANSI cast exception
    does not.

    Runs before the quality 9 coordinates are nulled, so a deliberate null is never
    counted as a failed cast.
    """
    before = (
        renamed.agg(
            *[F.count(F.col(name)).alias(name) for name in CAST_CHECKED_COLUMNS]
        )
        .collect()[0]
        .asDict()
    )
    after = (
        typed.agg(*[F.count(F.col(name)).alias(name) for name in CAST_CHECKED_COLUMNS])
        .collect()[0]
        .asDict()
    )

    lost = {
        name: (before[name], after[name])
        for name in CAST_CHECKED_COLUMNS
        if before[name] != after[name]
    }
    if lost:
        raise ValueError(
            "Doogal values did not survive typing, so the source holds malformed "
            f"entries. column: (populated_before, populated_after) = {lost}"
        )
    return typed


def assert_keys_present(df: DataFrame) -> DataFrame:
    """Fail on a missing postcode, which is the table's key.

    The dates are not keys and are legitimately null: terminated_date on a live
    postcode, introduced_date on a BFPO one. A date that failed to parse is caught by
    assert_casts_preserved instead.
    """
    offenders = (
        df.filter(F.col("postcode").isNull())
        .select("postcode", "country", "introduced_date")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            f"Doogal rows carry a missing postcode: {[row.asDict() for row in offenders]}"
        )
    return df


def assert_non_geographic_are_bfpo(df: DataFrame) -> DataFrame:
    """Fail on null administrative geography outside the BF postcode area.

    BFPO postcodes are non-geographic by design and carry no country, quality, or
    user type. Because that null is legitimate, a geography the publisher has not yet
    mapped would blend into them silently. This is the inverse of the HPI coverage
    floor guard, where the null is always a fault.
    """
    offenders = (
        df.filter(
            (
                F.col("country").isNull()
                | F.col("positional_quality").isNull()
                | F.col("user_type").isNull()
            )
            & ~F.col("postcode").startswith(BFPO_POSTCODE_AREA)
        )
        .select("postcode", "country", "positional_quality", "user_type")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "Doogal rows have null geography outside the "
            f"{BFPO_POSTCODE_AREA} postcode area, so their geography is unmapped "
            f"rather than absent: {[row.asDict() for row in offenders]}"
        )
    return df


def assert_quality_nine_lacks_grid_ref(df: DataFrame) -> DataFrame:
    """Fail unless positional quality 9 and a blank grid reference coincide.

    null_fabricated_coordinates reads quality alone, so it is only safe while the two
    agree. Rows with no quality at all are BFPO and are excluded.
    """
    no_grid_reference = F.col("positional_quality") == NO_GRID_REFERENCE
    offenders = (
        df.filter(
            F.col("positional_quality").isNotNull()
            & (no_grid_reference != F.col("easting").isNull())
        )
        .select("postcode", "positional_quality", "easting", "latitude", "longitude")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            f"Doogal positional quality {NO_GRID_REFERENCE} and a blank grid "
            "reference no longer coincide, so the fabricated coordinates cannot be "
            f"identified by quality: {[row.asDict() for row in offenders]}"
        )
    return df


def assert_postcode_unique(df: DataFrame) -> DataFrame:
    """Fail on a repeated postcode, which is the table's key."""
    duplicates = (
        df.groupBy("postcode").count().filter(F.col("count") > 1).limit(5).collect()
    )
    if duplicates:
        raise ValueError(
            "Doogal key broken, postcode is not unique: "
            f"{[row.asDict() for row in duplicates]}"
        )
    return df


def null_fabricated_coordinates(df: DataFrame) -> DataFrame:
    """Replace the fabricated zero coordinates with null.

    Where ONS publishes no grid reference the publisher writes 0 into latitude and
    longitude while leaving easting and northing blank. Zero is a valid coordinate,
    so it passes every range check and plots in the Atlantic.
    """
    fabricated = F.col("positional_quality") == NO_GRID_REFERENCE
    return df.withColumn(
        "latitude", F.when(fabricated, None).otherwise(F.col("latitude"))
    ).withColumn(
        "longitude", F.when(fabricated, None).otherwise(F.col("longitude"))
    )


def assert_no_zero_coordinates(df: DataFrame) -> DataFrame:
    """Fail on a zero coordinate pair surviving into the output.

    Asserted on the output rather than the source: the source is expected to carry
    them, and this is the contract the table makes.
    """
    offenders = (
        df.filter((F.col("latitude") == 0) & (F.col("longitude") == 0))
        .select("postcode", "positional_quality", "latitude", "longitude")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "Doogal rows carry a zero coordinate pair, which is a fabricated "
            f"position rather than a location: {[row.asDict() for row in offenders]}"
        )
    return df


def transform_doogal(
    raw_df: DataFrame,
    source_file: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """Bronze Doogal CSV, read as all-string, to the Silver frame.

    Args:
        raw_df: the unzipped postcode CSV read with header on and inferSchema off.
        source_file: bronze path of the archive read, recorded as lineage.
        ingestion_ts: load timestamp, recorded as lineage. Passed in rather than
            generated here so the transform stays deterministic under test.

    Returns:
        One row per postcode, live and terminated, with the columns named in
        SILVER_COLUMNS.
    """
    assert_source_columns(raw_df)
    assert_in_use_restates_terminated(raw_df)
    renamed = rename_columns(raw_df)
    assert_domains_known(renamed)
    typed = cast_columns(renamed)
    assert_casts_preserved(renamed, typed)
    assert_keys_present(typed)
    assert_non_geographic_are_bfpo(typed)
    assert_quality_nine_lacks_grid_ref(typed)
    assert_postcode_unique(typed)
    located = null_fabricated_coordinates(typed)
    assert_no_zero_coordinates(located)
    return (
        located.withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_ts", F.lit(ingestion_ts).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )
