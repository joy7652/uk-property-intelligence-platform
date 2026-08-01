"""Bank of England base rate: Bronze XLS to Silver.

Grain: one row per rate level, with a validity interval.

Source: bronze/boe/base_rate/baserate.xls, sheet 'Raw Data'.

The 'Raw Data' sheet is a daily series the BoE maintains to feed an embedded chart.
The official UK policy rate has been renamed several times since 1973 (Minimum
Lending Rate, Minimum Band 1 Dealing Rate, Repo Rate, Official Bank Rate), so each
daily row populates exactly one of five rate columns. This module coalesces those
into a single rate, collapses the daily series to its change events, and models the
result as a Type 2 slowly-changing dimension.

A rename that leaves the rate unchanged is not an event. The table tracks the rate,
and rate_type records the regime in effect at effective_date.

Neither function performs I/O. The spark-excel read and the Delta write live in
databricks_src/silver/notebooks/01_boe_base_rate.py.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StructField,
    StructType,
)

# Explicit schema for the spark-excel read of the 'Raw Data' sheet. Columns are
# positional and must match the sheet layout left to right; the header sits on row 2,
# so the reader is anchored at 'Raw Data'!A2.
#
# Rate columns are DecimalType(6, 4). The Repo Rate era quoted rates in sixteenths of
# a percent (5.9375), so a scale of 2 would silently round them. Decimal rather than
# double also gives exact equality, which change detection depends on.
RAW_DATA_SCHEMA: StructType = StructType(
    [
        StructField("date", DateType(), nullable=False),
        StructField("bank_rate", DecimalType(6, 4), nullable=True),
        StructField("zero_line", IntegerType(), nullable=True),
        StructField("min_lending_rate", DecimalType(6, 4), nullable=True),
        StructField("min_band_1_dealing_rate", DecimalType(6, 4), nullable=True),
        StructField("repo_rate", DecimalType(6, 4), nullable=True),
        StructField("official_bank_rate", DecimalType(6, 4), nullable=True),
    ]
)

# Rate columns in newest-regime-first precedence. coalesce walks this order, so on a
# changeover day, where two columns hold the same value, the newer regime supplies
# rate_type. zero_line is chart scaffolding and is deliberately absent.
RATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("official_bank_rate", "Official Bank Rate"),
    ("repo_rate", "Repo Rate"),
    ("min_band_1_dealing_rate", "Minimum Band 1 Dealing Rate"),
    ("min_lending_rate", "Minimum Lending Rate"),
    ("bank_rate", "Bank Rate"),
)

SILVER_COLUMNS: tuple[str, ...] = (
    "effective_date",
    "expiry_date",
    "is_current",
    "rate_pct",
    "rate_type",
    "_source_file",
    "_ingestion_ts",
)


def assert_rate_columns_consistent(raw_df: DataFrame) -> DataFrame:
    """Fail if any daily row carries conflicting values across the rate columns.

    Two rate columns are populated on a regime-changeover day, which is expected only
    when they agree. A disagreement means the coalesce in transform_boe_base_rate
    would silently pick one value over the other. greatest and least skip nulls, so
    this fires only when two or more distinct non-null rates appear on one row.

    The full count is reported rather than a sample alone: the daily series is small
    enough that the extra pass is free, and how many rows are affected changes the
    diagnosis.
    """
    rate_columns = [F.col(name) for name, _ in RATE_COLUMNS]
    conflicts = raw_df.filter(F.greatest(*rate_columns) != F.least(*rate_columns))
    bad = conflicts.count()
    if bad:
        sample = (
            conflicts.select("date", *[name for name, _ in RATE_COLUMNS])
            .limit(5)
            .collect()
        )
        raise ValueError(
            f"BoE: {bad} row(s) carry conflicting rate values across columns. "
            f"Sample: {[row.asDict() for row in sample]}"
        )
    return raw_df


def transform_boe_base_rate(
    raw_df: DataFrame,
    source_file: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """Daily BoE rate series to a Type 2 SCD of rate-change events.

    Args:
        raw_df: the 'Raw Data' sheet read with RAW_DATA_SCHEMA.
        source_file: bronze path of the workbook, recorded as lineage.
        ingestion_ts: load timestamp, recorded as lineage. Passed in rather than
            generated here so the transform stays deterministic under test.

    Returns:
        One row per rate level, ordered by validity, with the columns named in
        SILVER_COLUMNS.

    Note:
        The earliest row's effective_date is the start of the 'Raw Data' series
        (1973-01-01), not necessarily the date that rate level began. The series is
        left-censored. Every later effective_date is a genuine change date.
    """
    # Coalesce the five era-specific columns into one rate plus its regime.
    rate_pct = F.coalesce(*[F.col(name) for name, _ in RATE_COLUMNS])
    rate_type = F.coalesce(
        *[
            F.when(F.col(name).isNotNull(), F.lit(label))
            for name, label in RATE_COLUMNS
        ]
    )
    daily = raw_df.select(
        F.col("date"),
        rate_pct.alias("rate_pct"),
        rate_type.alias("rate_type"),
    ).filter(F.col("rate_pct").isNotNull())

    # Collapse the daily series to change events: keep the first day, and every day
    # whose rate differs from the day before. The window is unpartitioned by
    # necessity, since a rate series has to be lagged over one contiguous order.
    by_day = Window.orderBy("date")
    events = (
        daily.withColumn("_prev_rate", F.lag("rate_pct").over(by_day))
        .filter(
            F.col("_prev_rate").isNull() | (F.col("rate_pct") != F.col("_prev_rate"))
        )
        .drop("_prev_rate")
    )

    # Validity interval. expiry_date is the day before the next change; the open
    # interval carries a null expiry.
    by_effective = Window.orderBy("effective_date")
    return (
        events.withColumnRenamed("date", "effective_date")
        .withColumn("_next", F.lead("effective_date").over(by_effective))
        .withColumn("expiry_date", F.date_sub(F.col("_next"), 1))
        .withColumn("is_current", F.col("_next").isNull())
        .drop("_next")
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_ts", F.lit(ingestion_ts).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )
