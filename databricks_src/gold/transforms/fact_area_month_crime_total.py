"""Gold fact_area_month_crime_total: monthly crime totals by published area.

Grain: one row per (area_code, month_start_date).

Summed up from the shared small-area totals, at the same four levels as
fact_area_month_crime and for the same reason: a count is additive, so the rollup is
exact and it runs over an aggregate rather than over 92 million records.

Both measures are summed together, because they were counted together. A cell holding
only anti-social behaviour reports zero for the other measure and keeps its row, which
is why the check constraint here admits zero or more rather than more than zero. Nine
area-months are that cell; 747 carry no anti-social behaviour at all.

61,681 rows on the July 2026 release, against 61,710 possible across 330 areas and 187
months. The 29 absent are area-months in which nothing at all was recorded.

Anti-social behaviour sits in its own column beside a total that excludes it, so a sum
over crime types cannot pick it up by accident. Forces double count it and its share of
records drifts from 0.4239 in 2010 to 0.1574 in 2026.

Grain follows from the aggregate. Conformance against dim_area and dim_date runs in the
notebook against the loaded dimensions.

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
    DISTRICT_COLUMN,
    MONTH_COLUMN,
    NATION_COLUMN,
    REGION_COLUMN,
    TOTAL_MEASURE_COLUMNS,
    area_levels,
)

TABLE = "fact_area_month_crime_total"

KEY_COLUMNS: tuple[str, ...] = (AREA_COLUMN, MONTH_COLUMN)

# The same pair fact_lsoa_month_crime_total declares, in the same order.
MEASURE_COLUMNS: tuple[str, ...] = TOTAL_MEASURE_COLUMNS

GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

# Columns read from crime.small_area_totals.
SOURCE_COLUMNS: tuple[str, ...] = (
    (MONTH_COLUMN, DISTRICT_COLUMN, REGION_COLUMN, NATION_COLUMN)
    + TOTAL_MEASURE_COLUMNS
)


def assert_source_columns(totals_df: DataFrame) -> DataFrame:
    return assert_columns_present(totals_df, SOURCE_COLUMNS, TABLE)


def transform_fact_area_month_crime_total(totals_df: DataFrame) -> DataFrame:
    """The shared small-area totals to the Gold area total fact.

    Args:
        totals_df: crime.small_area_totals over the resolved records, one row per small
            area, month and its district ancestry.

    Returns:
        One row per (area_code, month_start_date) with the columns named in
        GOLD_COLUMNS. The grain follows from the aggregate and is not asserted again.
    """
    assert_source_columns(totals_df)
    return (
        area_levels(totals_df)
        .groupBy(*KEY_COLUMNS)
        .agg(*[F.sum(name).cast("int").alias(name) for name in MEASURE_COLUMNS])
        .select(*GOLD_COLUMNS)
    )
