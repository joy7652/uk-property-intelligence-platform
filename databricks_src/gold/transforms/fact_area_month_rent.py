"""Gold fact_area_month_rent: the private rent series as a monthly area panel.

Grain: one row per (area_code, month_start_date).

A projection and a rename, like the index fact, plus one thing that fact does not need:
eight of the 357 geographies have no key available from the table they come from.

ONS publishes no area code for the Northern Irish broad rental market areas, so Silver
keys the panel on area name and leaves the code null on those rows. dim_area already
assigns each one a code derived from its name, deterministic across releases and marked
as project-assigned rather than published. This module reads that code off the dimension
instead of recomputing the derivation, which would put the same rule on both sides of a
foreign key with nothing forcing the two copies to agree.

The lookup is checked for uniqueness before it is joined. A name carried under two
derived codes would fan a row into two rows with different keys, and the primary key is
informational, so nothing downstream would notice. The join key is null for every row
that already has a code, so a published area whose name happens to match a derived one
cannot be duplicated or overwritten.

Rows carrying no measure are dropped. This is the population the rule exists for:
Northern Ireland lags the other nations, ONS writes its unpublished months as a marker
across every measure, and Silver turns those into nulls and keeps the rows. Eighteen rows
in the May 2026 release, nine areas across two months, and the count moves with each
release. Kept, they would make the latest published rent for Belfast read as a blank in a
month where England reads a figure. no_measure is exposed so the load records how many it
dropped.

Nothing is cast. Silver declares int for the rental prices and decimal(18, 6) for the
index and the changes, which is what the Gold table declares, and a cast here would be a
second declaration of types Silver owns.

Scotland and Northern Ireland are published on broad rental market areas, which conform
to nothing below nation. Those rows carry a rental market area key and cannot be paired
with a price for the same geography, which is a property of the source rather than
something this module resolves.

Grain is asserted here, after the drop. Conformance against dim_area and dim_date is not:
it runs in the notebook against the loaded dimensions, since a fact checked against the
frame that produced a dimension has not been checked against the table it will join to.

No lineage columns, unlike Silver. Which run produced a Gold table is recorded in
uk_property_intel.quality.pipeline_run rather than on every row.

No table DDL here either. The Gold contract is declared once in
databricks_src/gold/notebooks/00_create_gold_tables.py, and a generator in this module
would be a second copy of it.

No I/O here. The reads and the Delta write live in
databricks_src/gold/notebooks/02_load_panel_facts.py.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from databricks_src.gold.transforms.conformance import assert_grain_unique
from databricks_src.gold.transforms.dim_area import DERIVED

TABLE = "fact_area_month_rent"

# Measures in the order the table declares them.
MEASURE_COLUMNS: tuple[str, ...] = (
    "rental_price",
    "price_index",
    "pct_change_1m",
    "pct_change_12m",
    "one_bed_rental_price",
    "two_bed_rental_price",
    "three_bed_rental_price",
    "four_or_more_bed_rental_price",
    "detached_rental_price",
    "semi_detached_rental_price",
    "terraced_rental_price",
    "flat_maisonette_rental_price",
)

KEY_COLUMNS: tuple[str, ...] = ("area_code", "month_start_date")

# Built from the two tuples above rather than written out, so the projection and the
# measure list cannot drift from each other.
GOLD_COLUMNS: tuple[str, ...] = KEY_COLUMNS + MEASURE_COLUMNS

SOURCE_DATE_COLUMN = "date"
SOURCE_NAME_COLUMN = "area_name"

# Columns read from uk_property_intel.silver.ons_private_rents.
SOURCE_COLUMNS: tuple[str, ...] = (
    "area_code",
    SOURCE_NAME_COLUMN,
    SOURCE_DATE_COLUMN,
) + MEASURE_COLUMNS

# Columns read from the loaded uk_property_intel.gold.dim_area.
DIMENSION_COLUMNS: tuple[str, ...] = ("area_code", "area_name", "code_source")

# Join scaffolding, dropped before the projection. Named rather than aliased inline so
# the uniqueness failure names a column a reader can find.
LOOKUP_NAME_COLUMN = "derived_area_name"
LOOKUP_CODE_COLUMN = "derived_area_code"


def assert_source_columns(ons_df: DataFrame) -> DataFrame:
    """Fail unless the Silver frame carries the columns this module reads.

    One direction only. Gold reads a projection of a table it does not own, so a missing
    column is a fault and an extra one is not.
    """
    missing = sorted(set(SOURCE_COLUMNS) - set(ons_df.columns))
    if missing:
        raise ValueError(f"{TABLE} source is missing columns it reads: {missing}")
    return ons_df


def assert_dimension_columns(area_df: DataFrame) -> DataFrame:
    """Fail unless dim_area carries what the name resolution reads.

    Separate from the source guard because the mistake it catches is different: passing
    the frame that produced the dimension, or the wrong dimension entirely.
    """
    missing = sorted(set(DIMENSION_COLUMNS) - set(area_df.columns))
    if missing:
        raise ValueError(f"{TABLE} needs dim_area columns it does not carry: {missing}")
    return area_df


def no_measure() -> Column:
    """True where every measure the fact carries is null on the row."""
    condition: Column | None = None
    for name in MEASURE_COLUMNS:
        test = F.col(name).isNull()
        condition = test if condition is None else condition & test
    return condition


def derived_code_lookup(area_df: DataFrame) -> DataFrame:
    """Trimmed name to code, for the codes this project assigned.

    Restricted to derived codes. A published area sharing a name with a rental market
    area would otherwise lend it a code, and the two are different places.
    """
    lookup = area_df.filter(F.col("code_source") == F.lit(DERIVED)).select(
        F.trim(F.col("area_name")).alias(LOOKUP_NAME_COLUMN),
        F.col("area_code").alias(LOOKUP_CODE_COLUMN),
    )
    # A name under two codes fans one row into two carrying different keys. The primary
    # key is informational, so the grain check downstream would pass on both.
    assert_grain_unique(lookup, (LOOKUP_NAME_COLUMN,), f"{TABLE} derived-code lookup")
    return lookup


def resolve_area_codes(ons_df: DataFrame, area_df: DataFrame) -> DataFrame:
    """Fill the missing area codes from dim_area, matching on name.

    The join key is null wherever a row already carries a code, and null matches nothing,
    so a published row keeps its own key whatever its name is.
    """
    lookup = derived_code_lookup(area_df)
    return (
        ons_df.withColumn(
            LOOKUP_NAME_COLUMN,
            F.when(
                F.col("area_code").isNull(), F.trim(F.col(SOURCE_NAME_COLUMN))
            ),
        )
        .join(lookup, LOOKUP_NAME_COLUMN, "left")
        .withColumn(
            "area_code", F.coalesce(F.col("area_code"), F.col(LOOKUP_CODE_COLUMN))
        )
        .drop(LOOKUP_NAME_COLUMN, LOOKUP_CODE_COLUMN)
    )


def assert_keys_resolved(resolved: DataFrame) -> DataFrame:
    """Fail on a row still carrying no area code.

    area_code is NOT NULL in the target, so this would abort the insert after the whole
    transform had run. It names the area rather than the count, because the fix is
    always a name that stopped matching.
    """
    unresolved = (
        resolved.filter(F.col("area_code").isNull())
        .select(SOURCE_NAME_COLUMN)
        .distinct()
        .limit(10)
        .collect()
    )
    if unresolved:
        raise ValueError(
            f"{TABLE} rows carry no published area code and no derived code in dim_area "
            "matches their name, so the key would be null: "
            f"{sorted(row[SOURCE_NAME_COLUMN] for row in unresolved)}"
        )
    return resolved


def transform_fact_area_month_rent(
    ons_df: DataFrame, area_df: DataFrame
) -> DataFrame:
    """Silver private rents to the Gold monthly area panel.

    Args:
        ons_df: uk_property_intel.silver.ons_private_rents, one row per
            (area_name, date).
        area_df: the loaded uk_property_intel.gold.dim_area, which owns the codes the
            eight uncoded rental market areas resolve to.

    Returns:
        One row per (area_code, month_start_date) with the columns named in
        GOLD_COLUMNS, carrying only rows that hold at least one measure.
    """
    assert_source_columns(ons_df)
    assert_dimension_columns(area_df)
    # Before the resolution. A row the fact discards needs no key, and the unpublished
    # months this drops belong to the areas whose keys are resolved by name.
    measured = ons_df.filter(~no_measure())
    resolved = resolve_area_codes(measured, area_df)
    assert_keys_resolved(resolved)
    projected = resolved.select(
        F.col("area_code"),
        F.col(SOURCE_DATE_COLUMN).alias("month_start_date"),
        *[F.col(name) for name in MEASURE_COLUMNS],
    )
    assert_grain_unique(projected, KEY_COLUMNS, TABLE)
    return projected
