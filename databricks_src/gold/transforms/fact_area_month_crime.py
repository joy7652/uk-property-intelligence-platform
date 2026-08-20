"""Gold fact_area_month_crime: monthly crime counts by published area and crime type.

Grain: one row per (area_code, month_start_date, crime_type).

Summed up from small areas rather than counted from the records. A count is additive, so
rolling up the small-area aggregate gives what counting the records at each level would,
and the load checks that it does against the composite. It puts 25,984,439 rows through
the level explode instead of 67,886,868. The price facts cannot do this, because a
median cannot be recovered from the medians below it.

Four levels: the district a small area is assigned to, its region, its nation, and the
England and Wales composite. Wales has no region, so a Welsh small area counts three
times. Summation stops at the England and Wales composite, because the source publishes
no Scottish force and files nothing against Northern Irish rows.

The district comes from dim_lsoa, which assigns a small area straddling two districts to
the one holding most of its postcodes. That assignment is exact for almost every area
and an estimate for the handful that straddle, and it reaches this table unchanged.

Anti-social behaviour has no row here, dropped in the shared aggregate and refused by
check constraint as well.

736,822 rows on the July 2026 release, over 330 areas and 187 months. The declared
estimate was roughly 940,000, which assumed a flat 15 types in every area-month. Three
published vocabularies were in use across the period, so the reachable ceiling is nearer
758,670 and this table fills 97.1 percent of it.

Grain is asserted here, after the aggregate. Conformance against dim_area, dim_date and
dim_crime_type runs in the notebook against the loaded dimensions.

No lineage columns, no table DDL, and no I/O here. The Gold contract is declared in
databricks_src/gold/notebooks/00_create_gold_tables.py, and the reads and the Delta
write live in databricks_src/gold/notebooks/04_load_crime_facts.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from databricks_src.gold.transforms.conformance import assert_columns_present
from databricks_src.gold.transforms.crime import (
    AREA_COLUMN,
    CRIME_COUNT_COLUMN,
    CRIME_TYPE_COLUMN,
    DISTRICT_COLUMN,
    MONTH_COLUMN,
    NATION_COLUMN,
    REGION_COLUMN,
    area_levels,
)

TABLE = "fact_area_month_crime"

KEY_COLUMNS: tuple[str, ...] = (AREA_COLUMN, MONTH_COLUMN, CRIME_TYPE_COLUMN)

MEASURE_COLUMNS: tuple[str, ...] = (CRIME_COUNT_COLUMN,)

GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

# Columns read from crime.small_area_type_counts. The ancestry is what the explode
# reads; the small-area code itself is not needed once the levels are assigned.
SOURCE_COLUMNS: tuple[str, ...] = (
    MONTH_COLUMN,
    CRIME_TYPE_COLUMN,
    CRIME_COUNT_COLUMN,
    DISTRICT_COLUMN,
    REGION_COLUMN,
    NATION_COLUMN,
)


def assert_source_columns(counts_df: DataFrame) -> DataFrame:
    return assert_columns_present(counts_df, SOURCE_COLUMNS, TABLE)


def transform_fact_area_month_crime(counts_df: DataFrame) -> DataFrame:
    """The shared small-area aggregate to the Gold area crime fact.

    Args:
        counts_df: crime.small_area_type_counts over the resolved records, one row per
            small area, month, type and its district ancestry.

    Returns:
        One row per (area_code, month_start_date, crime_type) with the columns named in
        GOLD_COLUMNS. The grain follows from the aggregate and is not asserted again.
    """
    assert_source_columns(counts_df)
    return (
        area_levels(counts_df)
        .groupBy(*KEY_COLUMNS)
        .agg(F.sum(CRIME_COUNT_COLUMN).cast("int").alias(CRIME_COUNT_COLUMN))
        .select(*GOLD_COLUMNS)
    )
