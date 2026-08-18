"""Gold dim_date: the daily calendar carrying the Bank of England rate.

Grain: one row per calendar day.

The calendar starts where the rate series starts rather than at a fixed year, and it
is built by expanding each Silver rate interval into its own days. There is no join.
The intervals are contiguous and non-overlapping, so the union of their expansions is
the calendar, and every row carries a rate by construction. base_rate_pct NOT NULL
then holds without a fallback value standing in for a gap.

That construction turns two source faults into failures rather than silence. Two
intervals covering one day produce two rows for it, and a gap produces none. Both are
caught by assert_calendar_complete, which is why no separate overlap check exists.

The rate is a daily attribute and no monthly rate is stored. A report that needs one
can average the days, take either end, or read the series as steps, and those answers
diverge in a month like March 2020 where the rate moved twice.

Monthly facts key on the first of their month and annual facts on the first of
January. Both are rows here, so every fact joins on date_key without a second date
dimension.

The first day is 1973-01-01, where the published sheet begins. That is the start of
the series rather than the start of the rate level it carries, so the earliest
interval is left-censored.

No lineage columns, unlike Silver. Which run produced a Gold table is recorded in
uk_property_intel.quality.pipeline_run rather than on every row.

No table DDL here either. The Gold contract is declared once in
databricks_src/gold/notebooks/00_create_gold_tables.py, and a generator in this module
would be a second copy of it.

No I/O here. The read and the Delta write live in
databricks_src/gold/notebooks/01_load_dimensions.py.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

YEAR_DDL = "int"
PART_DDL = "tinyint"
RATE_DDL = "decimal(6, 4)"

# ISO weekday numbers for Saturday and Sunday. Spark's weekday() counts from zero at
# Monday, so day_of_week is that plus one.
WEEKEND_DAYS: tuple[int, ...] = (6, 7)

GOLD_COLUMNS: tuple[str, ...] = (
    "date_key",
    "calendar_year",
    "calendar_quarter",
    "calendar_month",
    "day_of_month",
    "day_of_week",
    "month_name",
    "day_name",
    "year_month",
    "month_start_date",
    "quarter_start_date",
    "is_month_end",
    "is_weekend",
    "base_rate_pct",
    "base_rate_type",
)

KEY_COLUMNS: tuple[str, ...] = ("date_key",)

# Columns read from uk_property_intel.silver.boe_base_rate.
SOURCE_COLUMNS: tuple[str, ...] = (
    "effective_date",
    "expiry_date",
    "rate_pct",
    "rate_type",
)


def assert_source_columns(base_rate_df: DataFrame) -> DataFrame:
    """Fail unless the rate frame carries the columns this module reads.

    One direction only. Gold reads a projection of a table it does not own, so a
    missing column is a fault and an extra one is not.
    """
    missing = sorted(set(SOURCE_COLUMNS) - set(base_rate_df.columns))
    if missing:
        raise ValueError(f"dim_date source is missing columns it reads: {missing}")
    return base_rate_df


def assert_end_covers_series(base_rate_df: DataFrame, end_date: date) -> DataFrame:
    """Fail if the calendar would end before the last rate change.

    sequence() runs backwards when its start is later than its stop, so an end_date
    inside the series produces a descending run of dates rather than an error.
    """
    bounds = base_rate_df.agg(
        F.count(F.lit(1)).alias("intervals"),
        F.max("effective_date").alias("latest"),
    ).first()

    if not bounds["intervals"]:
        raise ValueError("dim_date source holds no rate intervals.")
    if bounds["latest"] > end_date:
        raise ValueError(
            f"dim_date end_date {end_date} falls before the last rate change "
            f"{bounds['latest']}, so the calendar would not cover the series."
        )
    return base_rate_df


def expand_intervals(base_rate_df: DataFrame, end_date: date) -> DataFrame:
    """One row per day per rate interval, carrying the rate in force that day.

    The open interval carries a null expiry and runs to end_date.
    """
    return base_rate_df.select(
        F.explode(
            F.sequence(
                F.col("effective_date"),
                F.coalesce(F.col("expiry_date"), F.lit(end_date).cast("date")),
            )
        ).alias("date_key"),
        F.col("rate_pct").cast(RATE_DDL).alias("base_rate_pct"),
        F.col("rate_type").alias("base_rate_type"),
    )


def assert_calendar_complete(days: DataFrame, end_date: date) -> DataFrame:
    """Fail unless the expanded days form one unbroken run ending at end_date.

    One pass covers three faults. More rows than distinct dates means two intervals
    claim the same day. Fewer distinct dates than the span means the chain has a gap,
    which would leave a day with no rate. A last day short of end_date means no
    interval is open.
    """
    measured = days.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("date_key").alias("days"),
        F.min("date_key").alias("first"),
        F.max("date_key").alias("last"),
    ).first()

    if not measured["rows"]:
        raise ValueError("dim_date expanded to no days.")

    span = (measured["last"] - measured["first"]).days + 1
    if measured["rows"] != measured["days"]:
        raise ValueError(
            "dim_date rate intervals overlap, so a day would carry two rates: "
            f"{measured['rows']:,} rows over {measured['days']:,} distinct days."
        )
    if measured["days"] != span:
        raise ValueError(
            "dim_date rate intervals leave a gap, so a day would carry no rate: "
            f"{measured['days']:,} days across a span of {span:,} from "
            f"{measured['first']} to {measured['last']}."
        )
    if measured["last"] != end_date:
        raise ValueError(
            f"dim_date ends at {measured['last']} rather than the requested "
            f"{end_date}, so no rate interval is open."
        )
    return days


def add_calendar_attributes(days: DataFrame) -> DataFrame:
    """Derive every calendar column from date_key.

    Types are set here rather than left to the insert. The target columns are TINYINT
    and INT while the Spark builtins return INT and BIGINT, and an implicit widening
    on write would hide a value that no longer fits.
    """
    calendar_year = F.year("date_key").cast(YEAR_DDL)
    calendar_month = F.month("date_key").cast(PART_DDL)
    day_of_week = F.expr("weekday(date_key) + 1").cast(PART_DDL)

    return (
        days.withColumn("calendar_year", calendar_year)
        .withColumn("calendar_quarter", F.quarter("date_key").cast(PART_DDL))
        .withColumn("calendar_month", calendar_month)
        .withColumn("day_of_month", F.dayofmonth("date_key").cast(PART_DDL))
        .withColumn("day_of_week", day_of_week)
        # Spark formats dates with Locale.US whatever the session locale is set to,
        # so these names are English on any cluster.
        .withColumn("month_name", F.date_format("date_key", "MMMM"))
        .withColumn("day_name", F.date_format("date_key", "EEEE"))
        .withColumn("year_month", (calendar_year * 100 + calendar_month).cast(YEAR_DDL))
        .withColumn("month_start_date", F.trunc("date_key", "MM"))
        .withColumn("quarter_start_date", F.trunc("date_key", "QUARTER"))
        .withColumn("is_month_end", F.col("date_key") == F.last_day("date_key"))
        .withColumn("is_weekend", day_of_week.isin(*WEEKEND_DAYS))
    )


def transform_dim_date(base_rate_df: DataFrame, end_date: date) -> DataFrame:
    """Silver Bank of England rate intervals to the Gold daily calendar.

    Args:
        base_rate_df: uk_property_intel.silver.boe_base_rate, the Type 2 rate table.
        end_date: last day of the calendar. Passed in rather than read off the clock
            so the transform stays deterministic under test.

    Returns:
        One row per day from the earliest effective date to end_date, with the columns
        named in GOLD_COLUMNS.
    """
    assert_source_columns(base_rate_df)
    assert_end_covers_series(base_rate_df, end_date)
    days = expand_intervals(base_rate_df, end_date)
    assert_calendar_complete(days, end_date)
    return add_calendar_attributes(days).select(*GOLD_COLUMNS)
