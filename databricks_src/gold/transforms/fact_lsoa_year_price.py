"""Gold fact_lsoa_year_price: transaction prices as an annual small-area series.

Grain: one row per (lsoa_code, year_start_date).

Annual rather than monthly. Across 10,867,452 small-area months, 3,246,648 hold one
transaction and 2,740,723 hold two, so for 55 percent of cells a monthly median would
describe an individual property. At annual grain 998,445 of 1,135,051 cells hold eleven
transactions or more.

Small areas conform to dim_area through district_code rather than carrying an area code,
so this fact keys on the small area alone and no level explode applies. The rollup to
district and above is fact_area_month_price, aggregated from the same transactions.

Transactions are attributed to 2021 boundary codes only. No crosswalk is applied, so an
area whose code changed carries its price under the new code and its earlier crime under
the old one, and the 1,106 codes exclusive to the 2011 boundaries carry crime and no
price. dim_lsoa constrains that: a code marked only_2011 cannot carry has_price.

Category B is excluded, as in fact_area_month_price and for the same reason. It costs 814
of the 1,135,047 small-area years and no small area its series: all 35,672 codes a
transaction reaches carry at least one category A sale, so dim_lsoa.has_price stays
consistent with which codes appear here.

A transaction with no small-area code is dropped rather than aborting the load. The
resolved England and Wales population held none in the July 2026 release, and the postcode
directory publishing a postcode without one is a coverage gap of the kind the panel facts
already drop and count. no_small_area is exposed so the load records how many rather than
reporting a zero nobody measured.

The measures come from `price_measures` in the resolution module, shared with
`fact_area_month_price`. Cells are thinner here, around 28 transactions on average, which
is where an approximate median would do most of its damage.

Grain is not asserted. The aggregation groups on exactly the columns the table declares
as its key.

Conformance against dim_lsoa and dim_date runs in the notebook against the loaded
dimensions.

No lineage columns, no table DDL, no I/O here. The reads and the Delta write live in
databricks_src/gold/notebooks/03_load_transaction_facts.py.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from databricks_src.gold.transforms.transactions import (
    LSOA_COLUMN,
    PRICE_MEASURE_COLUMNS,
    YEAR_COLUMN,
    assert_columns_present,
    is_full_market_value,
    price_measures,
)

TABLE = "fact_lsoa_year_price"

# Aliased rather than written out, as in fact_area_month_price.
MEASURE_COLUMNS: tuple[str, ...] = PRICE_MEASURE_COLUMNS

KEY_COLUMNS: tuple[str, ...] = (LSOA_COLUMN, YEAR_COLUMN)

# Built from the two tuples above rather than written out, so the projection and the
# measure list cannot drift from each other.
GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

# Columns read from the resolved transaction frame.
SOURCE_COLUMNS: tuple[str, ...] = (
    "price",
    LSOA_COLUMN,
    YEAR_COLUMN,
    "ppd_category_type",
)


def assert_source_columns(resolved_df: DataFrame) -> DataFrame:
    """Fail unless the resolved frame carries the columns this module reads."""
    return assert_columns_present(resolved_df, SOURCE_COLUMNS, f"{TABLE} source")


def no_small_area() -> Column:
    """True where a resolved transaction carries no small-area code.

    The district resolves and the small area does not, which is a postcode the directory
    places in a district without giving it an output area.
    """
    return F.col(LSOA_COLUMN).isNull()


def transform_fact_lsoa_year_price(resolved_df: DataFrame) -> DataFrame:
    """Resolved transactions to the Gold annual small-area series.

    Args:
        resolved_df: the output of resolve_transactions, filtered to the rows that
            resolved.

    Returns:
        One row per (lsoa_code, year_start_date) with the columns named in GOLD_COLUMNS,
        covering category A sales.
    """
    assert_source_columns(resolved_df)
    located = resolved_df.filter(~no_small_area() & is_full_market_value())
    return (
        located.groupBy(*KEY_COLUMNS).agg(*price_measures()).select(*GOLD_COLUMNS)
    )
