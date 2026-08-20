"""What the four Gold facts built from street-level crime share.

The resolution, the level explode, and the two measures every crime table reports with.
Shared for the reason `transactions.py` is: four tables built from one source in one
notebook, where a difference between them is a difference nobody asked for.

Grain of the resolution: one row per published crime record, carrying the geography it
resolves to and a label saying whether it resolves at all.

Crime arrives already located. Police.uk publishes a small-area code on the row, so
there is no postcode join here and no coordinate is read: the published code is the
geography, and the coordinates are snapped to shared map points anyway. The district
comes off `dim_lsoa`, which is where the majority assignment for a small area straddling
two districts lives, and the levels above it come off `dim_area`.

That assignment propagates. A small area lying across two districts puts all of its
crime in the one holding most of its postcodes, which is exact for almost every area and
an estimate for the handful that straddle. `dim_lsoa.district_assignment` marks which.

Rows are labelled with an outcome rather than filtered silently. On the July 2026
release, 92,352,547 of 96,092,836 records resolve and 3,740,289 carry no small-area
code. That population is two things. Northern Ireland files 2,311,848 records and places
none of them, because the publisher issues no small-area geography there at all. The
other 1,428,441 come from 44 England and Wales forces that place most of their records
and fail on some, at rates from 8.14 percent for Avon and Somerset down to four
thousandths of a percent for Greater Manchester. A crime count is therefore understated
by a force-specific amount that has nothing to do with crime, which no measure here
corrects and every measure here inherits.

Three further outcomes are declared and measured zero: a code outside the England and
Wales series, a code `dim_lsoa` does not hold, and a district with no row in `dim_area`.
Undeclared, all three would fall into the resolved population and key on null.

Area facts are summed from the small-area aggregate rather than from the records. A
count is additive, so the two routes agree exactly, and the load checks that they do
against the composite. It puts 25,984,439 rows through the level explode instead of
67,886,868. The price facts cannot do this, because a median cannot be recovered from
the medians below it.

Anti-social behaviour is counted and held apart. Its share of records falls from 0.4239
in 2010 to 0.1574 in 2026, drifting the whole way with a rise to 0.2813 in 2020 rather
than stepping on a date, so a series including it moves with reporting practice. Both
type tables refuse it by check constraint, and both total tables carry it in its own
column beside a total that excludes it. It costs no small area its place in the type
tables: all 36,751 codes carrying any crime carry a non-anti-social one.

No I/O here, as with every module in this folder. The reads and the Delta writes live in
databricks_src/gold/notebooks/04_load_crime_facts.py.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from databricks_src.gold.transforms.conformance import (
    assert_columns_present,
    assert_grain_unique,
)
from databricks_src.gold.transforms.dim_area import ENGLAND_AND_WALES
from databricks_src.gold.transforms.dim_lsoa import is_england_or_wales

SUBJECT = "crime resolution"

# The only composite the crime source can fill. It publishes no Scottish force and files
# nothing against Northern Irish rows, so a United Kingdom or Great Britain row built
# from it would report two nations under a name covering four. Read off the module that
# owns area codes, so the two fact families cannot come to differ about which composite
# they write.
COMPOSITE_AREA_CODE = ENGLAND_AND_WALES

DISTRICT_LEVEL = "district"

ANTI_SOCIAL_BEHAVIOUR = "Anti-social behaviour"

LSOA_COLUMN = "lsoa_code"
MONTH_COLUMN = "month_start_date"
CRIME_TYPE_COLUMN = "crime_type"
DISTRICT_COLUMN = "district_code"
REGION_COLUMN = "region_code"
NATION_COLUMN = "nation_code"
AREA_COLUMN = "area_code"

POPULATION_COLUMN = "population"

# Measure names. The two type tables declare the count; the two total tables declare the
# split pair. Named here so the aggregates that fill them cannot drift from the columns
# that carry them.
CRIME_COUNT_COLUMN = "crime_count"
EXCLUDING_ASB_COLUMN = "crime_count_excl_asb"
ANTI_SOCIAL_COLUMN = "anti_social_behaviour"

TOTAL_MEASURE_COLUMNS: tuple[str, ...] = (EXCLUDING_ASB_COLUMN, ANTI_SOCIAL_COLUMN)

# The Silver column the month key is read from. Silver parses it from a yyyy-MM string,
# so it is already the first of a month and needs no truncation; 187 months carry zero
# exceptions. Renamed rather than reused, because the fact's own column name is what
# says the value is a month start.
SOURCE_MONTH_COLUMN = "crime_month"

# Columns read from uk_property_intel.silver.police_street_crime.
POLICE_COLUMNS: tuple[str, ...] = (
    LSOA_COLUMN,
    SOURCE_MONTH_COLUMN,
    CRIME_TYPE_COLUMN,
)

# Columns read from the loaded uk_property_intel.gold.dim_lsoa.
SMALL_AREA_COLUMNS: tuple[str, ...] = (LSOA_COLUMN, DISTRICT_COLUMN)

# Columns read from the loaded uk_property_intel.gold.dim_area.
DIMENSION_COLUMNS: tuple[str, ...] = (
    AREA_COLUMN,
    "area_level",
    REGION_COLUMN,
    NATION_COLUMN,
)

# Join scaffolding, dropped before the projection. Named rather than aliased inline so a
# failure names a column a reader can find.
SMALL_AREA_MATCHED_COLUMN = "_lsoa_in_dim_lsoa"

RESOLVED = "resolved"
NO_LSOA_CODE = "no_lsoa_code"
LSOA_OUTSIDE_ENGLAND_WALES = "lsoa_outside_england_wales"
LSOA_NOT_IN_DIM_LSOA = "lsoa_not_in_dim_lsoa"
DISTRICT_NOT_IN_DIM_AREA = "district_not_in_dim_area"

# Every record carries exactly one of these. Declared so a load can record a zero for an
# outcome that did not occur, which a group-by alone cannot report.
POPULATIONS: tuple[str, ...] = (
    RESOLVED,
    NO_LSOA_CODE,
    LSOA_OUTSIDE_ENGLAND_WALES,
    LSOA_NOT_IN_DIM_LSOA,
    DISTRICT_NOT_IN_DIM_AREA,
)

# The resolved frame, carrying what the four facts need and nothing else. crime_id is
# not among them: it is blank for anti-social behaviour, recurs meaninglessly across
# Northern Irish rows, and no fact here is keyed on an incident.
RESOLVED_COLUMNS: tuple[str, ...] = (
    POPULATION_COLUMN,
    LSOA_COLUMN,
    MONTH_COLUMN,
    CRIME_TYPE_COLUMN,
    DISTRICT_COLUMN,
    REGION_COLUMN,
    NATION_COLUMN,
)


def assert_police_columns(police_df: DataFrame) -> DataFrame:
    return assert_columns_present(
        police_df, POLICE_COLUMNS, f"{SUBJECT} source silver.police_street_crime"
    )


def assert_small_area_columns(lsoa_df: DataFrame) -> DataFrame:
    """Separate from the source guard because the mistake it catches is different:
    passing the frame that produced dim_lsoa rather than the loaded table."""
    return assert_columns_present(lsoa_df, SMALL_AREA_COLUMNS, f"{SUBJECT} dim_lsoa")


def assert_dimension_columns(area_df: DataFrame) -> DataFrame:
    return assert_columns_present(area_df, DIMENSION_COLUMNS, f"{SUBJECT} dim_area")


def assert_composite_is_not_a_published_area(area_df: DataFrame) -> DataFrame:
    """Fail if the composite constant also names a district, region or nation.

    Two levels sharing a code would count one record twice inside a single group, and
    that produces no duplicate key for the grain check or the primary key to catch.
    """
    collisions = (
        area_df.filter(
            (F.col(AREA_COLUMN) == F.lit(COMPOSITE_AREA_CODE))
            & (F.col("area_level") != F.lit("composite"))
        )
        .select(AREA_COLUMN, "area_level")
        .limit(5)
        .collect()
    )
    if collisions:
        raise ValueError(
            f"{SUBJECT} sums to {COMPOSITE_AREA_CODE}, which dim_area also carries at "
            f"another level: {[row.asDict() for row in collisions]}. One record would "
            "be counted twice in one group."
        )
    return area_df


def small_area_lookup(lsoa_df: DataFrame) -> DataFrame:
    """Small-area code to its district, with a marker for a dimension match.

    The marker separates a code the dimension does not hold from one it holds without a
    district, which a left join alone folds into a single null. dim_lsoa requires a
    district on every row, so the second is measured zero and stays declared.
    """
    lookup = lsoa_df.select(
        F.col(LSOA_COLUMN),
        F.col(DISTRICT_COLUMN),
        F.lit(True).alias(SMALL_AREA_MATCHED_COLUMN),
    )
    # A repeat would fan one record into several and multiply every count in all four
    # facts, and nothing downstream carries an incident identity that would show it.
    assert_grain_unique(lookup, (LSOA_COLUMN,), f"{SUBJECT} small-area lookup")
    return lookup


def district_lookup(area_df: DataFrame) -> DataFrame:
    """District code to its region and nation, from the loaded area dimension.

    Ancestry is read off the dimension rather than walked, so a Welsh district returning
    a null region is the dimension stating that only England is divided into regions.
    """
    lookup = area_df.filter(F.col("area_level") == F.lit(DISTRICT_LEVEL)).select(
        F.col(AREA_COLUMN).alias(DISTRICT_COLUMN),
        F.col(REGION_COLUMN),
        F.col(NATION_COLUMN),
    )
    assert_grain_unique(lookup, (DISTRICT_COLUMN,), f"{SUBJECT} district lookup")
    return lookup


def population() -> Column:
    """The population a record belongs to, one value per row.

    Ordered so each branch tests one thing and the branch below it can assume the ones
    above did not fire.
    """
    return (
        F.when(F.col(LSOA_COLUMN).isNull(), F.lit(NO_LSOA_CODE))
        .when(~is_england_or_wales(), F.lit(LSOA_OUTSIDE_ENGLAND_WALES))
        .when(F.col(SMALL_AREA_MATCHED_COLUMN).isNull(), F.lit(LSOA_NOT_IN_DIM_LSOA))
        .when(F.col(NATION_COLUMN).isNull(), F.lit(DISTRICT_NOT_IN_DIM_AREA))
        .otherwise(F.lit(RESOLVED))
    )


def is_resolved() -> Column:
    """True on the records the four facts are built from."""
    return F.col(POPULATION_COLUMN) == F.lit(RESOLVED)


def is_anti_social_behaviour() -> Column:
    """True on the type both total tables report separately and both type tables refuse."""
    return F.col(CRIME_TYPE_COLUMN) == F.lit(ANTI_SOCIAL_BEHAVIOUR)


def crime_count() -> Column:
    """Records in a group, as both type facts count them.

    Cast to int because the tables declare int and count returns bigint. Under ANSI a
    group too large to fit raises rather than wrapping; the largest is the composite in
    one month, three orders of magnitude inside the range.
    """
    return F.count(F.lit(1)).cast("int").alias(CRIME_COUNT_COLUMN)


def total_measures() -> list[Column]:
    """The split pair both total tables report, in the order both declare them.

    One aggregate over one frame rather than two aggregates joined together. Counted
    apart and joined, a cell holding only anti-social behaviour appears on one side and
    not the other, and the join has to be an outer one with both sides filled in: 102,735
    small-area cells hold anti-social behaviour alone and 1,272,887 hold no anti-social
    behaviour at all. Counted together, every cell carries both measures and a zero is a
    zero because it was counted, not because a join produced nothing.
    """
    return [
        F.count_if(~is_anti_social_behaviour()).cast("int").alias(EXCLUDING_ASB_COLUMN),
        F.count_if(is_anti_social_behaviour()).cast("int").alias(ANTI_SOCIAL_COLUMN),
    ]


def small_area_type_counts(resolved_df: DataFrame) -> DataFrame:
    """Records counted at small area, month and type, carrying district ancestry.

    Anti-social behaviour is dropped here. Both type tables refuse it by check
    constraint, so a row reaching either is a failed write rather than a bad number.

    Ancestry sits in the group key although it adds nothing to the grain, because it is
    functionally determined by the small area and carrying it forward is what lets the
    area fact roll up without joining `dim_lsoa` a second time.

    Projected by fact_lsoa_month_crime and rolled up by fact_area_month_crime, so the
    two cannot disagree about what a small area holds.
    """
    return (
        resolved_df.filter(~is_anti_social_behaviour())
        .groupBy(
            LSOA_COLUMN,
            MONTH_COLUMN,
            CRIME_TYPE_COLUMN,
            DISTRICT_COLUMN,
            REGION_COLUMN,
            NATION_COLUMN,
        )
        .agg(crime_count())
    )


def small_area_totals(resolved_df: DataFrame) -> DataFrame:
    """Both measures counted at small area and month, carrying district ancestry.

    Every record is counted, anti-social behaviour into its own column. A cell holding
    only anti-social behaviour reports zero for the other measure, and that zero is a
    count rather than a missing join partner.
    """
    return resolved_df.groupBy(
        LSOA_COLUMN,
        MONTH_COLUMN,
        DISTRICT_COLUMN,
        REGION_COLUMN,
        NATION_COLUMN,
    ).agg(*total_measures())


def resolve_crime(
    police_df: DataFrame, lsoa_df: DataFrame, area_df: DataFrame
) -> DataFrame:
    """Label every crime record with its geography and the population it belongs to.

    Args:
        police_df: uk_property_intel.silver.police_street_crime, one row per published
            record.
        lsoa_df: the loaded uk_property_intel.gold.dim_lsoa, which owns the district a
            small area is assigned to.
        area_df: the loaded uk_property_intel.gold.dim_area, which owns the district to
            region and nation ancestry.

    Returns:
        Every input row, carrying RESOLVED_COLUMNS. Nothing is dropped, so the caller
        counts the populations in one pass and then filters.
    """
    assert_police_columns(police_df)
    assert_small_area_columns(lsoa_df)
    assert_dimension_columns(area_df)
    assert_composite_is_not_a_published_area(area_df)

    joined = police_df.select(*POLICE_COLUMNS).join(
        small_area_lookup(lsoa_df), LSOA_COLUMN, "left"
    ).join(district_lookup(area_df), DISTRICT_COLUMN, "left")

    return joined.select(
        population().alias(POPULATION_COLUMN),
        F.col(LSOA_COLUMN),
        F.col(SOURCE_MONTH_COLUMN).alias(MONTH_COLUMN),
        F.col(CRIME_TYPE_COLUMN),
        F.col(DISTRICT_COLUMN),
        F.col(REGION_COLUMN),
        F.col(NATION_COLUMN),
    )


def area_levels(small_area_df: DataFrame) -> DataFrame:
    """One row per small-area aggregate and published area it rolls up into.

    An English small area counts four times, at its district, its region, England, and
    the England and Wales composite. A Welsh one counts three times, because Wales is
    not divided into regions and the array entry is null there.

    Takes an aggregate rather than the records. Counts are additive, so summing the
    exploded aggregate gives what exploding the records would, over 25,984,439 rows
    instead of 67,886,868.
    """
    levels = F.array(
        F.col(DISTRICT_COLUMN),
        F.col(REGION_COLUMN),
        F.col(NATION_COLUMN),
        F.lit(COMPOSITE_AREA_CODE),
    )
    return small_area_df.withColumn(AREA_COLUMN, F.explode(levels)).filter(
        F.col(AREA_COLUMN).isNotNull()
    )
