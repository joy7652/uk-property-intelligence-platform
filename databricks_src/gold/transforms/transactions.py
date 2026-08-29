"""What the three Gold facts built from Price Paid Data share.

Mostly the resolution, at a grain of one row per transaction, carrying the geography it
resolves to and a label saying whether it resolves at all. Also the level explode, the
column guard all three run against their own source tuple, and the aggregates the two
price facts report identically.

Price Paid Data records a postcode and no area code, so every geography these facts key
on comes from the postcode directory. All three need the same join over the same 31.4
million rows, and a fact resolving district differently from its siblings would disagree
with them about which transactions exist. One resolution, read once, is what prevents
that.

District is taken from the postcode, not from the district recorded on the transaction.
The two disagree on 4,704,276 rows at a rate falling from 24.7 percent in 1995 to 5.0
percent in 2026, which is local government reorganisation showing in an old record.
Resolving through the postcode restates history onto current boundaries, matching both
index publishers.

Rows are labelled rather than filtered here. A drop leaving no trace is a drop nobody can
size, so the load reads a count per population off one pass and filters afterwards.
`NO_DISTRICT_ON_POSTCODE` and `DISTRICT_NOT_IN_DIM_AREA` were empty in the July 2026
release and are still declared: a bucket that stops existing is how a new fault gets
absorbed into a healthy one.

No threshold on the drops. A bound belongs to a measured release rather than a guess,
which is the rule the Silver freshness bounds already follow.

Postcodes join as published on both sides. 2,013 of 31,430,611 transactions carry a
postcode the directory does not hold, and the unmatched values are well-formed, so
normalising would be an unmeasured transformation over 2.7 million directory rows to
reach 0.006 percent of the table.

`area_levels` explodes a transaction to every published area it counts under. A median
cannot be recovered from the medians below it, so each level aggregates from the
transactions themselves rather than from the level under it. Two levels sharing a code
would double a transaction inside one group, which `dim_area`'s primary key makes
impossible for the three read off the dimension; `COMPOSITE_AREA_CODE` is a constant
here and is the one that could collide, so it is checked against them.

The England and Wales composite is the only one these facts write. `dim_area` also
carries United Kingdom and Great Britain, and Price Paid Data covers neither Scotland nor
Northern Ireland, so a row under either would be a partial count under a whole one's
label.

`price_measures` and `transaction_count` live here rather than in the fact that happens
to use them first. `fact_area_month_price`'s count is the sum of
`fact_area_month_transaction_mix`'s count over category A, and a relationship asserted
between two tables needs one definition rather than two that agree today. The same
argument covers the median and the rounding, which are a reporting rule for this project
rather than a property of either grain.

No I/O here. The reads and the Delta writes live in
databricks_src/gold/notebooks/03_load_transaction_facts.py.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from databricks_src.gold.transforms.dim_area import ENGLAND_AND_WALES
from databricks_src.gold.transforms.conformance import (
    assert_columns_present,
    assert_grain_unique,
)

SUBJECT = "transaction resolution"

ENGLAND = "E92000001"
WALES = "W92000004"

# England and Wales. Not the United Kingdom or Great Britain composites, which dim_area
# also carries and this source cannot fill. Read off the module that owns area codes, so
# the two fact families cannot come to differ about which composite they write.
COMPOSITE_AREA_CODE = ENGLAND_AND_WALES

DISTRICT_LEVEL = "district"

# Sale recorded at full market value. Category B covers repossessions, portfolio and
# buy-to-let transfers, and other sales not at market value.
FULL_MARKET_VALUE = "A"

# Output column names, referenced by all three fact modules.
POPULATION_COLUMN = "population"
DISTRICT_COLUMN = "district_code"
REGION_COLUMN = "region_code"
NATION_COLUMN = "nation_code"
LSOA_COLUMN = "lsoa_code"
MONTH_COLUMN = "month_start_date"
YEAR_COLUMN = "year_start_date"
AREA_COLUMN = "area_code"

# Declared by all three facts. The reconciliation between the price fact and the mix fact
# rests on both counting the same way, so the name is written once.
TRANSACTION_COUNT_COLUMN = "transaction_count"

# The two price facts declare these three at the same types on different grains, and both
# alias this tuple rather than writing it out. price_measures aliases its aggregates from
# the same three names, so the projection and the aggregate cannot disagree.
MEDIAN_PRICE_COLUMN = "median_price"
MEAN_PRICE_COLUMN = "mean_price"

PRICE_MEASURE_COLUMNS: tuple[str, ...] = (
    MEDIAN_PRICE_COLUMN,
    MEAN_PRICE_COLUMN,
    TRANSACTION_COUNT_COLUMN,
)

# Resolution outcomes. Every transaction carries exactly one.
RESOLVED = "resolved"
NO_POSTCODE = "no_postcode"
POSTCODE_NOT_IN_DIRECTORY = "postcode_not_in_directory"
NO_DISTRICT_ON_POSTCODE = "no_district_on_postcode"
DISTRICT_NOT_IN_DIM_AREA = "district_not_in_dim_area"
DISTRICT_OUTSIDE_ENGLAND_WALES = "district_outside_england_wales"

POPULATIONS: tuple[str, ...] = (
    RESOLVED,
    NO_POSTCODE,
    POSTCODE_NOT_IN_DIRECTORY,
    NO_DISTRICT_ON_POSTCODE,
    DISTRICT_NOT_IN_DIM_AREA,
    DISTRICT_OUTSIDE_ENGLAND_WALES,
)

# The four attributes of a sale, carried through to the mix fact. Silver asserts each
# against its published code set, so none is null by the time it reaches here.
SALE_ATTRIBUTE_COLUMNS: tuple[str, ...] = (
    "property_type",
    "old_new",
    "duration",
    "ppd_category_type",
)

SOURCE_DATE_COLUMN = "date_of_transfer"
SOURCE_LSOA_COLUMN = "lsoa_code_2021"

# Columns read from uk_property_intel.silver.ppd.
PPD_COLUMNS: tuple[str, ...] = (
    "price",
    SOURCE_DATE_COLUMN,
    "postcode",
) + SALE_ATTRIBUTE_COLUMNS

# Columns read from uk_property_intel.silver.doogal.
DOOGAL_COLUMNS: tuple[str, ...] = ("postcode", DISTRICT_COLUMN, SOURCE_LSOA_COLUMN)

# Columns read from the loaded uk_property_intel.gold.dim_area.
DIMENSION_COLUMNS: tuple[str, ...] = (
    "area_code",
    "area_level",
    REGION_COLUMN,
    NATION_COLUMN,
)

# Produced by resolve_transactions, in this order.
RESOLVED_COLUMNS: tuple[str, ...] = (
    POPULATION_COLUMN,
    "price",
    MONTH_COLUMN,
    YEAR_COLUMN,
    DISTRICT_COLUMN,
    REGION_COLUMN,
    NATION_COLUMN,
    LSOA_COLUMN,
) + SALE_ATTRIBUTE_COLUMNS

# Join scaffolding, dropped before the output. A row matching no directory entry has to
# be told apart from one matching an entry that carries no district.
POSTCODE_MATCHED_COLUMN = "postcode_in_directory"


def assert_ppd_columns(ppd_df: DataFrame) -> DataFrame:
    return assert_columns_present(ppd_df, PPD_COLUMNS, f"{SUBJECT} source silver.ppd")


def assert_doogal_columns(doogal_df: DataFrame) -> DataFrame:
    return assert_columns_present(
        doogal_df, DOOGAL_COLUMNS, f"{SUBJECT} source silver.doogal"
    )


def assert_dimension_columns(area_df: DataFrame) -> DataFrame:
    """Separate from the two source guards because the mistake it catches is different:
    passing the frame that produced dim_area rather than the loaded table."""
    return assert_columns_present(
        area_df, DIMENSION_COLUMNS, f"{SUBJECT} source dim_area"
    )


def assert_districts_carry_a_nation(districts: DataFrame) -> DataFrame:
    """Fail on a district in dim_area with no nation.

    The population label reads a null nation as a district absent from the dimension, so
    a district present without one would be counted as missing and its rows dropped
    under a label naming the wrong cause. Runs on the district rows alone, a few hundred.
    """
    offenders = (
        districts.filter(F.col(NATION_COLUMN).isNull())
        .select(DISTRICT_COLUMN)
        .limit(10)
        .collect()
    )
    if offenders:
        raise ValueError(
            f"{SUBJECT} found districts in dim_area carrying no nation, which the "
            "population label reads as a district the dimension does not hold: "
            f"{sorted(row[DISTRICT_COLUMN] for row in offenders)}"
        )
    return districts


def assert_composite_is_not_a_district_level(districts: DataFrame) -> DataFrame:
    """Fail if the composite code also names a district, region or nation.

    dim_area's primary key keeps the three levels read off the dimension distinct from
    each other. The composite is a constant here, so it is the one that could collide,
    and a collision would count every affected transaction twice inside one group rather
    than producing a duplicate key anything downstream could see.
    """
    matched = F.lit(COMPOSITE_AREA_CODE)
    offenders = (
        districts.filter(
            (F.col(DISTRICT_COLUMN) == matched)
            | (F.col(REGION_COLUMN) == matched)
            | (F.col(NATION_COLUMN) == matched)
        )
        .select(DISTRICT_COLUMN, REGION_COLUMN, NATION_COLUMN)
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            f"{SUBJECT} composite code {COMPOSITE_AREA_CODE} also names a district, "
            "region or nation, so every transaction under it would be counted twice in "
            f"one group: {[row.asDict() for row in offenders]}"
        )
    return districts


def district_lookup(area_df: DataFrame) -> DataFrame:
    """District code to its region, nation and the levels above it.

    Restricted to districts. Every other level reaches these facts through a district's
    ancestry rather than through a postcode.
    """
    districts = area_df.filter(F.col("area_level") == F.lit(DISTRICT_LEVEL)).select(
        F.col("area_code").alias(DISTRICT_COLUMN),
        F.col(REGION_COLUMN),
        F.col(NATION_COLUMN),
    )
    assert_grain_unique(districts, (DISTRICT_COLUMN,), f"{SUBJECT} district lookup")
    assert_districts_carry_a_nation(districts)
    assert_composite_is_not_a_district_level(districts)
    return districts


def postcode_lookup(doogal_df: DataFrame) -> DataFrame:
    """Postcode to the district and small area it sits in.

    Grain is asserted rather than inherited. Silver asserts the same key on the same
    table, but a repeat here fans one transaction into several and multiplies every
    count and every median weight in all three facts, and nothing downstream carries the
    transaction identity that would show it. One shuffle over 2.7 million rows.
    """
    lookup = doogal_df.select(
        F.col("postcode"),
        F.col(DISTRICT_COLUMN),
        F.col(SOURCE_LSOA_COLUMN).alias(LSOA_COLUMN),
        F.lit(True).alias(POSTCODE_MATCHED_COLUMN),
    )
    assert_grain_unique(lookup, ("postcode",), f"{SUBJECT} postcode lookup")
    return lookup


def population() -> Column:
    """The resolution outcome for a row, evaluated in order.

    Order carries the diagnosis. A row failing at the postcode is not also reported as
    failing at the district, and each clause runs only where the one before it left the
    row unlabelled, which is what makes the nation test safe against a null.
    """
    return (
        F.when(
            F.col("postcode").isNull() | (F.trim(F.col("postcode")) == F.lit("")),
            F.lit(NO_POSTCODE),
        )
        .when(F.col(POSTCODE_MATCHED_COLUMN).isNull(), F.lit(POSTCODE_NOT_IN_DIRECTORY))
        .when(F.col(DISTRICT_COLUMN).isNull(), F.lit(NO_DISTRICT_ON_POSTCODE))
        .when(F.col(NATION_COLUMN).isNull(), F.lit(DISTRICT_NOT_IN_DIM_AREA))
        .when(
            ~F.col(NATION_COLUMN).isin(ENGLAND, WALES),
            F.lit(DISTRICT_OUTSIDE_ENGLAND_WALES),
        )
        .otherwise(F.lit(RESOLVED))
    )


def is_resolved() -> Column:
    """True where a transaction reached an England or Wales district."""
    return F.col(POPULATION_COLUMN) == F.lit(RESOLVED)


def is_full_market_value() -> Column:
    """True for a category A sale.

    Category B's share of the table steps from 0.06 percent in 2012 to 2.4 percent in
    2013 and settles between 15 and 18 percent from 2017, which is a change in what the
    source publishes rather than in the market. A price series spanning that step and
    including B compares two different populations either side of it. The mix fact keys
    on the category instead, so B stays reachable there.
    """
    return F.col("ppd_category_type") == F.lit(FULL_MARKET_VALUE)


def transaction_count() -> Column:
    """Rows in the group, as all three facts declare the measure."""
    return F.count(F.lit(1)).cast("int").alias(TRANSACTION_COUNT_COLUMN)


def price_measures() -> list[Column]:
    """The three aggregates the two price facts report, in the order both declare them.

    The median is exact rather than approximate. Cells hold a few hundred transactions at
    monthly area grain and around 28 at annual small-area grain, small enough that the
    exact aggregate costs little and an approximate one would put an estimate where the
    column name promises a measurement.

    Both prices round to whole pounds, because both tables declare int and the source
    records whole pounds. An even-count cell takes the midpoint of the two middle values,
    so the median can land on a half, and it rounds up.
    """
    return [
        F.round(F.median("price")).cast("int").alias(MEDIAN_PRICE_COLUMN),
        F.round(F.avg("price")).cast("int").alias(MEAN_PRICE_COLUMN),
        transaction_count(),
    ]


def resolve_transactions(
    ppd_df: DataFrame, doogal_df: DataFrame, area_df: DataFrame
) -> DataFrame:
    """Silver transactions with their resolved geography and a resolution label.

    Args:
        ppd_df: uk_property_intel.silver.ppd, one row per transaction.
        doogal_df: uk_property_intel.silver.doogal, one row per postcode, live and
            terminated. Transfers run back to 1995 against postcodes since withdrawn,
            so the terminated ones are load-bearing.
        area_df: the loaded uk_property_intel.gold.dim_area, which owns the ancestry
            every level above district is read from.

    Returns:
        One row per transaction with the columns named in RESOLVED_COLUMNS. Every row is
        kept, whether or not it resolved, so the load can size what it drops.
    """
    assert_ppd_columns(ppd_df)
    assert_doogal_columns(doogal_df)
    assert_dimension_columns(area_df)

    joined = ppd_df.select(*PPD_COLUMNS).join(
        postcode_lookup(doogal_df), "postcode", "left"
    ).join(district_lookup(area_df), DISTRICT_COLUMN, "left")

    # Both keys are truncated from the transfer date rather than read off Silver's
    # transfer_year, so the month and the year cannot disagree about which period a
    # transaction falls in.
    return joined.select(
        population().alias(POPULATION_COLUMN),
        F.col("price"),
        F.trunc(F.col(SOURCE_DATE_COLUMN), "MM").alias(MONTH_COLUMN),
        F.trunc(F.col(SOURCE_DATE_COLUMN), "YEAR").alias(YEAR_COLUMN),
        F.col(DISTRICT_COLUMN),
        F.col(REGION_COLUMN),
        F.col(NATION_COLUMN),
        F.col(LSOA_COLUMN),
        *[F.col(name) for name in SALE_ATTRIBUTE_COLUMNS],
    )


def area_levels(resolved_df: DataFrame) -> DataFrame:
    """One row per transaction and published area it counts under.

    Four levels for an English transaction, three for a Welsh one: only England is
    divided into regions, and a null element is dropped rather than counted. Adding
    `area_code` here rather than in each fact keeps the two area facts covering the same
    levels by construction.
    """
    levels = F.array(
        F.col(DISTRICT_COLUMN),
        F.col(REGION_COLUMN),
        F.col(NATION_COLUMN),
        F.lit(COMPOSITE_AREA_CODE),
    )
    return resolved_df.withColumn(AREA_COLUMN, F.explode(levels)).filter(
        F.col(AREA_COLUMN).isNotNull()
    )
