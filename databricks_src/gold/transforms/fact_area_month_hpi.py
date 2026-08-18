"""Gold fact_area_month_hpi: the house price index as a monthly area panel.

Grain: one row per (area_code, month_start_date).

A projection and a rename. Every measure the fact carries exists in Silver under the
same name at the same type, so there is nothing to compute here and the work is in what
the table refuses to carry.

Nothing is cast. Silver declares decimal(18, 6) for the measures and int for the volume,
which is what the Gold table declares, and a cast here would be a second declaration of
types Silver owns. Columns that are derived rather than read do get cast, which is why
dim_date does and this does not.

Rows carrying no measure are dropped. The index panel held none of these in the May 2026
release, unlike the rent series, but the rule belongs to the table rather than to a
release: a row with a key and no value is not a fact, and it counts once in every total
taken over the table. no_measure is exposed so the load records how many it dropped
rather than reporting a zero nobody measured.

No geographic filter. The index is mix-adjusted and published at every level, so a region
is read from its own row and never summed from its districts, and every level the
publisher issues belongs here. Silver has already removed the derived back-series below
each nation's coverage floor, which is a question about reliability rather than about
what this table is for.

The two seasonally adjusted columns are null for every district and county. Seasonal
adjustment is published at region, nation and composite only, and Northern Ireland is
absent from it at nation level although the United Kingdom composite is not. That is the
source's shape, not sparseness, and it is why those columns can serve a benchmark line
and never the area a screen is about.

Grain is asserted here. Conformance against dim_area and dim_date is not: it runs in the
notebook against the loaded dimensions, since a dimension checked against the frame that
produced it has not been checked against the table the fact will actually join to.

No lineage columns, unlike Silver. Which run produced a Gold table is recorded in
uk_property_intel.quality.pipeline_run rather than on every row.

No table DDL here either. The Gold contract is declared once in
databricks_src/gold/notebooks/00_create_gold_tables.py, and a generator in this module
would be a second copy of it.

No I/O here. The read and the Delta write live in
databricks_src/gold/notebooks/02_load_panel_facts.py.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from databricks_src.gold.transforms.conformance import assert_grain_unique

TABLE = "fact_area_month_hpi"

# Measures in the order the table declares them.
MEASURE_COLUMNS: tuple[str, ...] = (
    "avg_price",
    "avg_price_seasonally_adjusted",
    "price_index",
    "price_index_seasonally_adjusted",
    "pct_change_1m",
    "pct_change_12m",
    "sales_volume",
    "detached_price",
    "semi_detached_price",
    "terraced_price",
    "flat_price",
)

KEY_COLUMNS: tuple[str, ...] = ("area_code", "month_start_date")

# Built from the two tuples above rather than written out, so the projection and the
# measure list cannot drift from each other.
GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

# The Silver column the month key is read from. Renamed rather than reused: date_key on
# the calendar is a day and this is the first of a month, and the fact's own column name
# is what says so.
SOURCE_DATE_COLUMN = "date"

# Columns read from uk_property_intel.silver.hpi.
SOURCE_COLUMNS: tuple[str, ...] = ("area_code", SOURCE_DATE_COLUMN) + MEASURE_COLUMNS


def assert_source_columns(hpi_df: DataFrame) -> DataFrame:
    """Fail unless the Silver frame carries the columns this module reads.

    One direction only. Gold reads a projection of a table it does not own, so a missing
    column is a fault and an extra one is not.
    """
    missing = sorted(set(SOURCE_COLUMNS) - set(hpi_df.columns))
    if missing:
        raise ValueError(f"{TABLE} source is missing columns it reads: {missing}")
    return hpi_df


def no_measure() -> Column:
    """True where every measure the fact carries is null on the row.

    Null, not falsy. A month with a sales volume of zero and no price is a measurement
    and stays.
    """
    condition: Column | None = None
    for name in MEASURE_COLUMNS:
        test = F.col(name).isNull()
        condition = test if condition is None else condition & test
    return condition


def transform_fact_area_month_hpi(hpi_df: DataFrame) -> DataFrame:
    """Silver house price index to the Gold monthly area panel.

    Args:
        hpi_df: uk_property_intel.silver.hpi, one row per (area_code, date).

    Returns:
        One row per (area_code, month_start_date) with the columns named in
        GOLD_COLUMNS, carrying only rows that hold at least one measure.
    """
    assert_source_columns(hpi_df)
    measured = hpi_df.filter(~no_measure())
    projected = measured.select(
        F.col("area_code"),
        F.col(SOURCE_DATE_COLUMN).alias("month_start_date"),
        *[F.col(name) for name in MEASURE_COLUMNS],
    )
    # After the drop, not before. A repeat among rows the fact discards is not a
    # contract this table makes.
    assert_grain_unique(projected, KEY_COLUMNS, TABLE)
    return projected
