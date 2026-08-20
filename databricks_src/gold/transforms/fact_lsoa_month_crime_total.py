"""Gold fact_lsoa_month_crime_total: monthly crime totals by small area.

Grain: one row per (lsoa_code, month_start_date).

A projection of the shared small-area totals. Both measures are counted in one aggregate
in databricks_src/gold/transforms/crime.py rather than counted apart and joined: 102,735
cells hold anti-social behaviour and nothing else, and 1,272,887 hold no anti-social
behaviour at all, so a join in either direction loses one of those populations and an
outer join has to invent the missing side. Counted together, a zero is a zero because it
was counted.

That is what the table's check constraint admits at zero or more rather than more than
zero, unlike the type tables. A small area whose only records in a month were
anti-social behaviour reports no other crime, and no other crime is a measurement there.

Anti-social behaviour sits in its own column beside a total that excludes it. Its share
of records falls from 0.4239 in 2010 to 0.1574 in 2026, drifting the whole way with a
rise to 0.2813 in 2020, so a series including it moves with reporting practice. Forces
also double count it. Reported separately, a sum over the other column cannot pick it up
by accident.

6,328,185 rows on the July 2026 release, 92.1 percent of the 6,872,437 code-months the
36,751 small areas could hold across 187 months.

Grain is asserted here, after the projection. Conformance against dim_lsoa and dim_date
runs in the notebook against the loaded dimensions.

No lineage columns, no table DDL, and no I/O here. The Gold contract is declared in
databricks_src/gold/notebooks/00_create_gold_tables.py, and the reads and the Delta
write live in databricks_src/gold/notebooks/04_load_crime_facts.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from databricks_src.gold.transforms.conformance import (
    assert_columns_present,
    assert_grain_unique,
)
from databricks_src.gold.transforms.crime import (
    LSOA_COLUMN,
    MONTH_COLUMN,
    TOTAL_MEASURE_COLUMNS,
)

TABLE = "fact_lsoa_month_crime_total"

KEY_COLUMNS: tuple[str, ...] = (LSOA_COLUMN, MONTH_COLUMN)

# The same pair fact_area_month_crime_total declares, in the same order. Shared so the
# aggregates that fill them cannot drift from the names.
MEASURE_COLUMNS: tuple[str, ...] = TOTAL_MEASURE_COLUMNS

GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

# Columns read from crime.small_area_totals.
SOURCE_COLUMNS: tuple[str, ...] = GOLD_COLUMNS


def assert_source_columns(totals_df: DataFrame) -> DataFrame:
    return assert_columns_present(totals_df, SOURCE_COLUMNS, TABLE)


def transform_fact_lsoa_month_crime_total(totals_df: DataFrame) -> DataFrame:
    """The shared small-area totals to the Gold small-area total fact.

    Args:
        totals_df: crime.small_area_totals over the resolved records, one row per small
            area, month and its district ancestry.

    Returns:
        One row per (lsoa_code, month_start_date) with the columns named in
        GOLD_COLUMNS.
    """
    assert_source_columns(totals_df)
    projected = totals_df.select(*[F.col(name) for name in GOLD_COLUMNS])
    assert_grain_unique(projected, KEY_COLUMNS, TABLE)
    return projected
