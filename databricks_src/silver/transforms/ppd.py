"""HM Land Registry Price Paid Data: Bronze CSV to Silver.

Grain: one row per transaction, keyed on TUID.

The source is one headerless CSV per transfer year, 16 columns in the published
order. Position is the contract, so the reader supplies an all-string schema and
types are asserted here rather than inferred.

Land Registry regenerates every yearly file on each monthly release, so Silver is
rebuilt from the current vintage rather than accumulated. The monthly change-only
file is a separate feed carrying additions, changes, and deletions; it is applied by
a merge, not by this module.

Each yearly file holds exactly one transfer year, confirmed across all 32 files on
two vintages. That is what makes transfer_year a safe partition key for
replaceWhere, so it is asserted rather than assumed.

record_status is not carried into Silver. It is constant "A" across the yearly
files, and in the monthly file it names an operation to apply rather than an
attribute of the transaction.

TUID is unique across the whole dataset, so a repeat is asserted rather than
deduplicated. Both cost the same shuffle; only the assertion names what collided.

Casts use try_cast rather than cast. ANSI mode is on from DBR 17.0, so a plain cast
raises on the first malformed cell with an error that does not name the column.
try_cast yields null instead, and assert_casts_preserved turns that null back into a
failure that names the column.

No I/O here. The read and the Delta write live in
databricks_src/silver/notebooks/03_ppd.py.
"""

from __future__ import annotations

import re
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from databricks_src.silver.transforms.expressions import parsed_date
from pyspark.sql.types import StringType, StructField, StructType

PRICE_DDL = "int"
DATE_FORMAT = "yyyy-MM-dd HH:mm"

# The published shape, including the constant zero time. A real time parses without
# complaint and is then discarded, so the check has to run on the source string.
DATE_PATTERN = r"^\d{4}-\d{2}-\d{2} 00:00$"

# Published column order. The files carry no header, so this is positional and a
# reordering here would load values into the wrong columns without failing.
SOURCE_COLUMNS: tuple[str, ...] = (
    "tuid",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
)

# _source_file is added by the reader from _metadata, because the load spans 32
# files and the per-file guards group on it.
FRAME_COLUMNS: tuple[str, ...] = SOURCE_COLUMNS + ("_source_file",)

# Columns produced by cast_columns, before lineage is stamped. transfer_year is
# derived; record_status is dropped.
TYPED_COLUMNS: tuple[str, ...] = (
    "tuid",
    "price",
    "date_of_transfer",
    "transfer_year",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
)

LINEAGE_COLUMNS: tuple[str, ...] = ("_source_file", "_ingestion_ts")

SILVER_COLUMNS: tuple[str, ...] = TYPED_COLUMNS + LINEAGE_COLUMNS

KEY_COLUMNS: tuple[str, ...] = ("tuid", "date_of_transfer", "transfer_year")

# Only price. date_of_transfer is a key, and assert_keys_present covers it more
# completely than a count comparison can; every other column stays a string.
CAST_CHECKED_COLUMNS: tuple[str, ...] = ("price",)

INT_COLUMNS: frozenset[str] = frozenset({"price", "transfer_year"})

# Published code sets. Confirm against the discovery cell in the notebook before the
# first production run: a value absent here aborts rather than passing through.
DOMAINS: dict[str, tuple[str, ...]] = {
    "property_type": ("D", "S", "T", "F", "O"),
    "old_new": ("Y", "N"),
    "duration": ("F", "L", "U"),
    "ppd_category_type": ("A", "B"),
    "record_status": ("A", "C", "D"),
}

_FILE_YEAR = re.compile(r"pp-(\d{4})\.csv$", re.IGNORECASE)


def string_schema() -> StructType:
    """All-string read schema in the published column order."""
    return StructType([StructField(name, StringType(), True) for name in SOURCE_COLUMNS])


def _column_ddl(name: str) -> str:
    if name == "date_of_transfer":
        data_type = "DATE"
    elif name == "_ingestion_ts":
        data_type = "TIMESTAMP"
    elif name in INT_COLUMNS:
        data_type = PRICE_DDL.upper()
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
    """Fail unless the frame carries the published columns and the source path."""
    actual = set(raw_df.columns)
    expected = set(FRAME_COLUMNS)
    if actual != expected:
        raise ValueError(
            "PPD source columns do not match the expected set. "
            f"missing={sorted(expected - actual)} "
            f"unexpected={sorted(actual - expected)}"
        )
    return raw_df


def assert_date_format(raw_df: DataFrame) -> DataFrame:
    """Fail on a transfer date that does not match the published shape.

    Runs on the source string, before casting, because a time other than 00:00
    parses cleanly and the value is then lost without trace.

    Nulls are not caught here: rlike yields null on a null input, and a missing date
    belongs to assert_keys_present, which reports the offending row.
    """
    offenders = (
        raw_df.filter(~F.col("date_of_transfer").rlike(DATE_PATTERN))
        .select("tuid", "date_of_transfer", "_source_file")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            f"PPD transfer dates do not match the published format ({DATE_FORMAT}, "
            f"zero time): {[row.asDict() for row in offenders]}"
        )
    return raw_df


def _cast_expr(name: str) -> Column:
    if name == "date_of_transfer":
        return parsed_date(name, DATE_FORMAT).alias(name)
    if name == "transfer_year":
        return F.year(parsed_date("date_of_transfer", DATE_FORMAT)).alias(name)
    if name == "price":
        return F.expr(f"try_cast(`{name}` as {PRICE_DDL})").alias(name)
    return F.col(name)


def cast_columns(df: DataFrame) -> DataFrame:
    """Type the two non-string columns and derive the partition key.

    _source_file rides along: the per-file guards run after typing, because the year
    they check is derived from the parsed date.
    """
    return df.select([_cast_expr(name) for name in TYPED_COLUMNS] + [F.col("_source_file")])


def assert_casts_preserved(source: DataFrame, typed: DataFrame) -> DataFrame:
    """Fail if a populated value did not survive its cast.

    try_cast returns null on a malformed value. Comparing non-null counts per column
    turns that into a failure that names the column, which the ANSI cast exception
    does not.

    Keys are excluded. A null date is caught by assert_keys_present whatever its
    cause, which covers more than this check and reports the offending row.
    """
    before = (
        source.agg(*[F.count(F.col(name)).alias(name) for name in CAST_CHECKED_COLUMNS])
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
            "PPD values did not survive typing, so the source holds malformed "
            f"entries. column: (populated_before, populated_after) = {lost}"
        )
    return typed


def assert_keys_present(df: DataFrame) -> DataFrame:
    """Fail on a missing TUID or a date that did not parse."""
    offenders = (
        df.filter(F.col("tuid").isNull() | F.col("date_of_transfer").isNull())
        .select("tuid", "date_of_transfer", "_source_file")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "PPD rows carry a missing TUID or an unparseable date "
            f"(expected {DATE_FORMAT}): {[row.asDict() for row in offenders]}"
        )
    return df


def unrecognised_domains() -> Column:
    """Comma-joined names of the code columns whose value is outside its set.

    concat_ws drops nulls, so a clean row yields an empty string and one pass covers
    every code column while still naming which one failed.
    """
    return F.concat_ws(
        ",",
        *[
            F.when(F.col(column).isNull() | ~F.col(column).isin(*values), F.lit(column))
            for column, values in DOMAINS.items()
        ],
    )


def assert_domains_known(raw_df: DataFrame) -> DataFrame:
    """Fail on any code outside its published set.

    A new code is a source change, not a row to skip: keeping it would put a value in
    Silver that no downstream model knows how to read. Runs on the source frame,
    since record_status is checked and does not reach the typed frame.
    """
    offenders = (
        raw_df.select(
            "tuid",
            "_source_file",
            *DOMAINS,
            unrecognised_domains().alias("unrecognised"),
        )
        .filter(F.col("unrecognised") != "")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "PPD rows carry a code outside its published set "
            f"({ {column: list(values) for column, values in DOMAINS.items()} }): "
            f"{[row.asDict() for row in offenders]}"
        )
    return raw_df


def assert_one_year_per_file(df: DataFrame) -> DataFrame:
    """Fail if a file spans more than one transfer year or disagrees with its name.

    replaceWhere rewrites a whole year partition from one file, so a file spanning
    two years would delete rows belonging to another file. The filename check is the
    second half: a mislabelled file passes the span test and still writes the wrong
    partition.
    """
    per_file = (
        df.groupBy("_source_file")
        .agg(
            F.countDistinct("transfer_year").alias("years"),
            F.min("transfer_year").alias("min_year"),
            F.max("transfer_year").alias("max_year"),
        )
        .collect()
    )

    offenders = []
    for row in per_file:
        name = row["_source_file"].rsplit("/", 1)[-1]
        if row["years"] != 1:
            offenders.append(
                {
                    "file": name,
                    "years": row["years"],
                    "span": f"{row['min_year']} to {row['max_year']}",
                }
            )
            continue
        match = _FILE_YEAR.search(name)
        if match and int(match.group(1)) != row["min_year"]:
            offenders.append(
                {"file": name, "claimed": int(match.group(1)), "content": row["min_year"]}
            )

    if offenders:
        raise ValueError(
            "PPD files break the one-year-per-file rule the transfer_year partition "
            f"depends on: {offenders[:5]}"
        )
    return df


def assert_tuid_unique(df: DataFrame) -> DataFrame:
    """Fail on a repeated TUID, which is the table's key."""
    duplicates = (
        df.groupBy("tuid").count().filter(F.col("count") > 1).limit(5).collect()
    )
    if duplicates:
        raise ValueError(
            f"PPD key broken, tuid is not unique: {[row.asDict() for row in duplicates]}"
        )
    return df


def transform_ppd(
    raw_df: DataFrame,
    ingestion_ts: datetime,
) -> DataFrame:
    """Bronze PPD yearly CSVs, read as all-string, to the Silver frame.

    Args:
        raw_df: the yearly files read with string_schema, carrying _source_file.
        ingestion_ts: load timestamp, recorded as lineage. Passed in rather than
            generated here so the transform stays deterministic under test.

    Returns:
        One row per transaction, with the columns named in SILVER_COLUMNS.

    Note:
        source_file is not a parameter, unlike the BoE and HPI transforms. The load
        spans 32 files, so lineage is per row and comes from the reader.
    """
    assert_source_columns(raw_df)
    assert_date_format(raw_df)
    assert_domains_known(raw_df)
    typed = cast_columns(raw_df)
    assert_casts_preserved(raw_df, typed)
    assert_keys_present(typed)
    assert_one_year_per_file(typed)
    assert_tuid_unique(typed)
    return typed.withColumn(
        "_ingestion_ts", F.lit(ingestion_ts).cast("timestamp")
    ).select(*SILVER_COLUMNS)
