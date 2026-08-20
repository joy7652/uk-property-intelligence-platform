"""Gold fact_lsoa_month_crime: monthly crime counts by small area and crime type.

Grain: one row per (lsoa_code, month_start_date, crime_type).

A projection of the shared small-area aggregate. The counting happens once in
databricks_src/gold/transforms/crime.py and fact_area_month_crime rolls up the same
frame, so the two tables cannot disagree about what a small area holds in a month.

Anti-social behaviour has no row here. It is dropped in the shared aggregate, and the
table refuses it by check constraint as well, so a row reaching Delta is a failed write
rather than a number nobody questions. Its counts live in fact_lsoa_month_crime_total,
in a column of their own beside a total that excludes them.

The ancestry columns are dropped. They exist on the aggregate to let the area fact roll
up without reading dim_lsoa twice, and a small area's district belongs to the dimension
rather than to a second copy on 26 million fact rows.

25,984,439 rows on the July 2026 release, over 36,751 small areas and 187 months.

Grain is asserted here, after the projection. Conformance against dim_lsoa, dim_date and
dim_crime_type is not: it runs in the notebook against the loaded dimensions, since a
fact checked against the frame that produced a dimension has not been checked against
the table it will join to.

No lineage columns, unlike Silver. Which run produced a Gold table is recorded in
uk_property_intel.quality.pipeline_run rather than on every row.

No table DDL here either. The Gold contract is declared once in
databricks_src/gold/notebooks/00_create_gold_tables.py.

No I/O here. The reads and the Delta write live in
databricks_src/gold/notebooks/04_load_crime_facts.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from databricks_src.gold.transforms.conformance import (
    assert_columns_present,
    assert_grain_unique,
)
from databricks_src.gold.transforms.crime import (
    CRIME_COUNT_COLUMN,
    CRIME_TYPE_COLUMN,
    LSOA_COLUMN,
    MONTH_COLUMN,
)

TABLE = "fact_lsoa_month_crime"

KEY_COLUMNS: tuple[str, ...] = (LSOA_COLUMN, MONTH_COLUMN, CRIME_TYPE_COLUMN)

MEASURE_COLUMNS: tuple[str, ...] = (CRIME_COUNT_COLUMN,)

# Built from the two tuples above rather than written out, so the projection and the
# measure list cannot drift from each other.
GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

# Columns read from crime.small_area_type_counts.
SOURCE_COLUMNS: tuple[str, ...] = GOLD_COLUMNS


def assert_source_columns(counts_df: DataFrame) -> DataFrame:
    return assert_columns_present(counts_df, SOURCE_COLUMNS, TABLE)


def transform_fact_lsoa_month_crime(counts_df: DataFrame) -> DataFrame:
    """The shared small-area aggregate to the Gold small-area crime fact.

    Args:
        counts_df: crime.small_area_type_counts over the resolved records, one row per
            small area, month, type and its district ancestry.

    Returns:
        One row per (lsoa_code, month_start_date, crime_type) with the columns named in
        GOLD_COLUMNS.
    """
    assert_source_columns(counts_df)
    projected = counts_df.select(*[F.col(name) for name in GOLD_COLUMNS])
    assert_grain_unique(projected, KEY_COLUMNS, TABLE)
    return projected
