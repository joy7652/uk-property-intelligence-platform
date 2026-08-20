"""Gold fact_area_month_price: transaction prices as a monthly area panel.

Grain: one row per (area_code, month_start_date).

An aggregation, unlike the two published panels, because the source is one row per
transaction and the table is one row per area and month. Rows exist for districts, their
regions, the two nations and the England and Wales composite, and every level aggregates
from the transactions themselves: a median cannot be recovered from the medians below it,
so a region summed from its districts would be a different number wearing the same name.

Category B is excluded. Its share steps from 0.06 percent of transactions in 2012 to 2.4
percent in 2013 and settles between 15 and 18 percent from 2017, so a series including it
compares two different populations either side of that step. Excluding it costs 14 of the
120,109 district-months and no district its row: every one of the 318 districts a
transaction reaches carries at least one category A sale. It also removes the 346
transactions above 100 million pounds, all of which are category B, from every mean.

The three measures come from `price_measures` in the resolution module, shared with
`fact_lsoa_year_price`, which reports the same three at a different grain.

Grain is not asserted. The aggregation groups on exactly the columns the table declares
as its key, so a repeat is not reachable, unlike the two panel facts where the key is
carried through a projection. The risk the check would cover at this grain is one
transaction counted twice inside a group, which needs two levels sharing a code, and
that is asserted in the resolution instead.

Conformance against dim_area and dim_date runs in the notebook against the loaded
dimensions, since a fact checked against the frame that produced a dimension has not been
checked against the table it will join to.

No lineage columns, unlike Silver. Which run produced a Gold table is recorded in
uk_property_intel.quality.pipeline_run rather than on every row.

No table DDL here. The Gold contract is declared once in
databricks_src/gold/notebooks/00_create_gold_tables.py.

No I/O here. The reads and the Delta write live in
databricks_src/gold/notebooks/03_load_transaction_facts.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from databricks_src.gold.transforms.transactions import (
    AREA_COLUMN,
    MONTH_COLUMN,
    PRICE_MEASURE_COLUMNS,
    area_levels,
    assert_columns_present,
    is_full_market_value,
    price_measures,
)

TABLE = "fact_area_month_price"

# Aliased rather than written out. fact_lsoa_year_price declares the same three, and
# price_measures builds the aggregate that has to match them.
MEASURE_COLUMNS: tuple[str, ...] = PRICE_MEASURE_COLUMNS

KEY_COLUMNS: tuple[str, ...] = (AREA_COLUMN, MONTH_COLUMN)

# Built from the two tuples above rather than written out, so the projection and the
# measure list cannot drift from each other.
GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

# Columns read from the resolved transaction frame.
SOURCE_COLUMNS: tuple[str, ...] = (
    "price",
    MONTH_COLUMN,
    "ppd_category_type",
    "district_code",
    "region_code",
    "nation_code",
)


def assert_source_columns(resolved_df: DataFrame) -> DataFrame:
    """Fail unless the resolved frame carries the columns this module reads."""
    return assert_columns_present(resolved_df, SOURCE_COLUMNS, f"{TABLE} source")


def transform_fact_area_month_price(resolved_df: DataFrame) -> DataFrame:
    """Resolved transactions to the Gold monthly area panel.

    Args:
        resolved_df: the output of resolve_transactions, filtered to the rows that
            resolved. Filtering is the notebook's, so the counts it drops are recorded
            before they disappear.

    Returns:
        One row per (area_code, month_start_date) with the columns named in
        GOLD_COLUMNS, covering category A sales at four levels.
    """
    assert_source_columns(resolved_df)
    full_market = resolved_df.filter(is_full_market_value())
    return (
        area_levels(full_market)
        .groupBy(*KEY_COLUMNS)
        .agg(*price_measures())
        .select(*GOLD_COLUMNS)
    )
