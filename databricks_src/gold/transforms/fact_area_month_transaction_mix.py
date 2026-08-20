"""Gold fact_area_month_transaction_mix: transaction counts by what was sold.

Grain: one row per (area_code, month_start_date, property_type, old_new, duration,
ppd_category_type).

The only fact carrying the four attributes the source records about a sale. They sit in
the key rather than in a dimension because each has a handful of values and no attributes
of its own.

Counts only. A count sums cleanly to any coarser cut, and holding the median out keeps it
from appearing at a grain too thin to support one.

Both sale categories are carried, unlike the two price facts, which take category A
alone. Composition is what this table is for, and a category whose share moves from
nothing to a sixth of the table is part of the composition rather than a contaminant of
it. The consequence is that this table and fact_area_month_price do not reconcile on a
bare count: the price fact's transaction_count equals the sum of this table's count
filtered to ppd_category_type = 'A'.

The four levels match fact_area_month_price rather than stopping at district, so an area
profile filtering on a region code finds a mix beside the price it already found. A count
is additive, so district rows alone would carry the same information; 102,087 rows is
what the model pays to keep the two facts keyed alike.

Silver asserts each of the four attributes against its published code set and aborts on a
value outside it or on a null, so nothing is defaulted or coalesced here.

Grain is not asserted. The aggregation groups on exactly the columns the table declares
as its key, so a repeat is not reachable.

Conformance against dim_area and dim_date runs in the notebook against the loaded
dimensions.

No lineage columns, no table DDL, no I/O here. The reads and the Delta write live in
databricks_src/gold/notebooks/03_load_transaction_facts.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from databricks_src.gold.transforms.transactions import (
    AREA_COLUMN,
    MONTH_COLUMN,
    SALE_ATTRIBUTE_COLUMNS,
    TRANSACTION_COUNT_COLUMN,
    area_levels,
    assert_columns_present,
    transaction_count,
)

TABLE = "fact_area_month_transaction_mix"

# The same count the two price facts declare. Named once, because the reconciliation
# between this table and fact_area_month_price rests on both counting the same way.
MEASURE_COLUMNS: tuple[str, ...] = (TRANSACTION_COUNT_COLUMN,)

KEY_COLUMNS: tuple[str, ...] = (AREA_COLUMN, MONTH_COLUMN) + SALE_ATTRIBUTE_COLUMNS

# Built from the two tuples above rather than written out, so the projection and the
# measure list cannot drift from each other.
GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

# Columns read from the resolved transaction frame.
SOURCE_COLUMNS: tuple[str, ...] = (
    MONTH_COLUMN,
    "district_code",
    "region_code",
    "nation_code",
) + SALE_ATTRIBUTE_COLUMNS


def assert_source_columns(resolved_df: DataFrame) -> DataFrame:
    """Fail unless the resolved frame carries the columns this module reads."""
    return assert_columns_present(resolved_df, SOURCE_COLUMNS, f"{TABLE} source")


def transform_fact_area_month_transaction_mix(resolved_df: DataFrame) -> DataFrame:
    """Resolved transactions to the Gold monthly composition breakdown.

    Args:
        resolved_df: the output of resolve_transactions, filtered to the rows that
            resolved.

    Returns:
        One row per (area, month, property type, build status, tenure, sale category)
        with the columns named in GOLD_COLUMNS.
    """
    assert_source_columns(resolved_df)
    return (
        area_levels(resolved_df)
        .groupBy(*KEY_COLUMNS)
        .agg(transaction_count())
        .select(*GOLD_COLUMNS)
    )
