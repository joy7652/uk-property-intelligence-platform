"""Gold dim_lsoa: lower layer super output areas in England and Wales.

Grain: one row per small-area code.

Scotland and Northern Ireland are out of scope, and transactions are the only route by
which another nation's codes arrive. The crime source publishes England and Wales codes
alone and files nothing at all against Northern Irish rows. Transactions carry a
postcode, and a postcode unit lying across the Anglo-Scottish border is assigned whole
to one district, so a small number of England and Wales sales resolve to a Scottish data
zone. That is the national-boundary form of the straddling problem majority_share
handles one level down, except that here the answer is exclusion rather than an
assignment.

Membership is therefore filtered where it is assembled rather than on either source. A
filter on the source that looked likely would have left the one that was not.

Membership is what the platform can say something about: every code the crime source
publishes, plus every code a transaction resolves to. has_crime and has_price record
which, and a code carried by both is a small area where crime and price sit side by
side. Codes the postcode directory knows but neither source reaches are left out, since
a dimension row with no fact behind it is a row nothing joins to.

District comes from postcodes, not from the code. Small areas nest inside districts by
design and almost all of them do in practice, but a handful straddle two, and the
published code carries no district of its own. Counting the directory's postcodes per
district settles it: the district holding most of them takes the area, and
district_assignment marks the estimate so it stays separable from the exact ones.

Ties do not arise in the measured data, where every straddling area holds at least 92
percent of its postcodes in one district, but the ranking still breaks one
deterministically on district code.
A tie resolved by whatever order a shuffle returned would move between releases and
take a whole area's crime with it.

Boundary vintage records which census boundary set a code belongs to. The 2011 to 2021
transition was a 30-month band of per-force gazetteer updates rather than a clean
cutover, so both sets are live in the data at once and a code can belong to either or
to both. A code exclusive to 2011 carries crime and no price, because transactions are
attributed to 2021 codes only, and the table constrains that rather than trusting it.

Postcodes are counted whether or not they are still in use. A terminated postcode still
carried transactions while it existed, and the price series reaches back to 1995, so
excluding them would attribute an area by its present shape and then count history
against it.

The name is the 2021 one where a code has both, falling back to 2011. The 2021 revision
was largely a renaming exercise in Wales, so the newer name is the substantive one.

No lineage columns, unlike Silver. Which run produced a Gold table is recorded in
uk_property_intel.quality.pipeline_run rather than on every row.

No table DDL here either. The Gold contract is declared once in
databricks_src/gold/notebooks/00_create_gold_tables.py.

No I/O here. The reads and the Delta write live in
databricks_src/gold/notebooks/01_load_dimensions.py.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from databricks_src.gold.transforms.conformance import assert_keys_conform

EXACT = "exact"
MAJORITY = "majority"
ASSIGNMENTS: tuple[str, ...] = (EXACT, MAJORITY)

BOTH = "both"
ONLY_2011 = "only_2011"
ONLY_2021 = "only_2021"
VINTAGES: tuple[str, ...] = (BOTH, ONLY_2011, ONLY_2021)

# Nation by the code's first letter. Small areas are an England and Wales geography;
# Scotland publishes data zones and Northern Ireland super output areas, neither of
# which shares this code series. The table constrains the same two values.
NATION_BY_INITIAL: dict[str, str] = {"E": "E92000001", "W": "W92000004"}

# Scale of majority_share in the target. The computed share is a double, and letting
# the insert cast it would leave the stored value to whatever rounding the write chose.
SHARE_SCALE = 4
SHARE_DDL = f"decimal(5, {SHARE_SCALE})"

GOLD_COLUMNS: tuple[str, ...] = (
    "lsoa_code",
    "lsoa_name",
    "district_code",
    "district_assignment",
    "majority_share",
    "boundary_vintage",
    "nation_code",
    "has_crime",
    "has_price",
)

KEY_COLUMNS: tuple[str, ...] = ("lsoa_code",)

# Columns read from each Silver table.
DOOGAL_COLUMNS: tuple[str, ...] = (
    "postcode",
    "district_code",
    "lsoa_code_2011",
    "lsoa_name_2011",
    "lsoa_code_2021",
    "lsoa_name_2021",
)
POLICE_COLUMNS: tuple[str, ...] = ("lsoa_code",)
PPD_COLUMNS: tuple[str, ...] = ("postcode",)

# What measure_small_areas returns and the transform reads.
MEASURED_COLUMNS: tuple[str, ...] = (
    "lsoa_code",
    "lsoa_name",
    "district_code",
    "postcodes_in_district",
    "postcodes_total",
    "in_2011",
    "in_2021",
    "has_crime",
    "has_price",
)


def assert_maps_consistent() -> None:
    """Fail on authored values that contradict the table's own vocabulary.

    Runs at import, so a bad edit stops the module loading rather than aborting a load
    that has already scanned the crime and transaction tables.
    """
    if set(NATION_BY_INITIAL) != {"E", "W"}:
        raise ValueError(
            "dim_lsoa NATION_BY_INITIAL must cover exactly the England and Wales code "
            f"initials: {sorted(NATION_BY_INITIAL)}"
        )
    misshapen = sorted(
        code
        for code in NATION_BY_INITIAL.values()
        if len(code) != 9 or not code[1:].isdigit()
    )
    if misshapen:
        raise ValueError(f"dim_lsoa NATION_BY_INITIAL holds malformed codes: {misshapen}")


assert_maps_consistent()


def is_england_or_wales() -> Column:
    """Whether a code belongs to the England and Wales small-area series.

    Shares NATION_BY_INITIAL with nation_code, so the codes kept and the codes given a
    nation cannot come apart.
    """
    return F.substring("lsoa_code", 1, 1).isin(*sorted(NATION_BY_INITIAL))


def nation_code() -> Column:
    """Nation a code sits in, from its first letter. Null outside England and Wales."""
    initial = F.substring("lsoa_code", 1, 1)
    expr: Column | None = None
    for candidate, code in NATION_BY_INITIAL.items():
        condition = initial == F.lit(candidate)
        expr = (
            F.when(condition, F.lit(code))
            if expr is None
            else expr.when(condition, F.lit(code))
        )
    return expr.cast("string")


def boundary_vintage() -> Column:
    """Which census boundary set a code belongs to."""
    return (
        F.when(F.col("in_2011") & F.col("in_2021"), F.lit(BOTH))
        .when(F.col("in_2011"), F.lit(ONLY_2011))
        .otherwise(F.lit(ONLY_2021))
    )


def district_assignment() -> Column:
    """Whether the district is the area's only one or the one holding most of it."""
    return F.when(
        F.col("postcodes_in_district") == F.col("postcodes_total"), F.lit(EXACT)
    ).otherwise(F.lit(MAJORITY))


def majority_share() -> Column:
    """Share of the area's postcodes falling in the assigned district.

    Cast explicitly to the target scale. An exact assignment is written as a literal
    rather than divided, because the table requires it to equal 1.0 and a ratio that
    rounds up to 1.0000 from just below would pass the check while claiming the area
    does not straddle.
    """
    ratio = F.col("postcodes_in_district") / F.col("postcodes_total")
    return F.when(
        F.col("postcodes_in_district") == F.col("postcodes_total"), F.lit(1.0)
    ).otherwise(ratio).cast(SHARE_DDL)


def assert_source_columns(
    doogal_df: DataFrame, police_df: DataFrame, ppd_df: DataFrame
) -> None:
    """Fail unless each Silver frame carries the columns this module reads.

    One direction only. Gold reads a projection of tables it does not own, so a missing
    column is a fault and an extra one is not.
    """
    for label, frame, expected in (
        ("postcode directory", doogal_df, DOOGAL_COLUMNS),
        ("street crime", police_df, POLICE_COLUMNS),
        ("price paid", ppd_df, PPD_COLUMNS),
    ):
        missing = sorted(set(expected) - set(frame.columns))
        if missing:
            raise ValueError(
                f"dim_lsoa {label} source is missing columns it reads: {missing}"
            )


def directory_codes(doogal_df: DataFrame) -> DataFrame:
    """One row per postcode and small-area code, across both boundary vintages.

    A postcode carries a code in each vintage, so it contributes a row to each. Stacking
    them rather than joining lets one pass count postcodes per district and settle
    vintage membership at the same time.
    """
    def vintage(code: str, name: str, flag_2011: bool) -> DataFrame:
        return doogal_df.where(F.col(code).isNotNull()).select(
            F.col("postcode"),
            F.col("district_code"),
            F.col(code).alias("lsoa_code"),
            F.trim(F.col(name)).alias("lsoa_name"),
            F.lit(flag_2011).alias("is_2011"),
        )

    return vintage("lsoa_code_2011", "lsoa_name_2011", True).unionByName(
        vintage("lsoa_code_2021", "lsoa_name_2021", False)
    )


def crime_codes(police_df: DataFrame) -> DataFrame:
    """Distinct England and Wales small-area codes the crime source publishes.

    The null filter is what does the work: Northern Irish rows carry no code, because
    the publisher files nothing there. The geography filter removes nothing measured,
    since the source publishes England and Wales codes only, and is kept as the cheap
    half of the pair with the one in measure_small_areas. Were another nation to appear
    here, it would be excluded before the distinct rather than after, which matters at
    96 million rows.
    """
    return (
        police_df.where(F.col("lsoa_code").isNotNull() & is_england_or_wales())
        .select(F.col("lsoa_code"))
        .distinct()
    )


def priced_codes(doogal_df: DataFrame, ppd_df: DataFrame) -> DataFrame:
    """Distinct 2021 codes at least one transaction resolves to.

    Transactions carry a postcode, not a small-area code, so the directory resolves
    them. The projection is two columns, which is what keeps the lookup small enough to
    broadcast against the transaction table.

    Not filtered to England and Wales here. The transaction series is an England and
    Wales one, so in principle nothing else can come through, and measure_small_areas
    filters the assembled membership anyway. Adding a second filter here would state a
    guarantee this function does not make.
    """
    lookup = doogal_df.where(F.col("lsoa_code_2021").isNotNull()).select(
        F.col("postcode"), F.col("lsoa_code_2021").alias("lsoa_code")
    )
    return (
        ppd_df.where(F.col("postcode").isNotNull())
        .select("postcode")
        .join(lookup, "postcode", "inner")
        .select("lsoa_code")
        .distinct()
    )


def measure_small_areas(
    doogal_df: DataFrame, police_df: DataFrame, ppd_df: DataFrame
) -> DataFrame:
    """One row per small area, carrying its district candidates and membership.

    Args:
        doogal_df: uk_property_intel.silver.doogal, or a projection carrying
            DOOGAL_COLUMNS.
        police_df: uk_property_intel.silver.police_street_crime, or a projection
            carrying POLICE_COLUMNS.
        ppd_df: uk_property_intel.silver.ppd, or a projection carrying PPD_COLUMNS.

    Returns:
        MEASURED_COLUMNS, one row per code with the district holding most of its
        postcodes already chosen.

    Note:
        This is the expensive half and the reason 3.2 touches both large tables. The
        crime scan is shared with dim_crime_type, and the transaction join exists only
        to answer has_price: the directory says which areas hold postcodes, never which
        hold transactions. The caller persists this frame.
    """
    assert_source_columns(doogal_df, police_df, ppd_df)

    stacked = directory_codes(doogal_df)

    # Postcodes per district, plus vintage membership and each vintage's name, in one
    # pass. A separate aggregate per question would restack the directory each time.
    per_district = stacked.groupBy("lsoa_code", "district_code").agg(
        F.count(F.lit(1)).alias("postcodes_in_district"),
        F.max(F.when(F.col("is_2011"), F.col("lsoa_name"))).alias("name_2011"),
        F.max(F.when(~F.col("is_2011"), F.col("lsoa_name"))).alias("name_2021"),
        F.max(F.col("is_2011").cast("int")).alias("seen_2011"),
        F.max((~F.col("is_2011")).cast("int")).alias("seen_2021"),
    )

    per_area = Window.partitionBy("lsoa_code")
    # Most postcodes wins. district_code breaks a tie so the winner is the same on
    # every release rather than whichever row the shuffle returned first.
    by_weight = per_area.orderBy(
        F.col("postcodes_in_district").desc(), F.col("district_code").asc()
    )

    chosen = (
        per_district.withColumn("_rank", F.row_number().over(by_weight))
        .withColumn("postcodes_total", F.sum("postcodes_in_district").over(per_area))
        .withColumn(
            "lsoa_name",
            F.coalesce(
                F.max("name_2021").over(per_area), F.max("name_2011").over(per_area)
            ),
        )
        .withColumn("in_2011", F.max("seen_2011").over(per_area) == F.lit(1))
        .withColumn("in_2021", F.max("seen_2021").over(per_area) == F.lit(1))
        .where(F.col("_rank") == 1)
        .select(
            "lsoa_code",
            "lsoa_name",
            "district_code",
            "postcodes_in_district",
            "postcodes_total",
            "in_2011",
            "in_2021",
        )
    )

    # Membership is driven by the two fact sources, not by the directory. Starting from
    # the directory instead would drop a published crime code it does not carry, which
    # is the case assert_every_area_has_postcodes exists to report.
    #
    # The geography filter sits here, on the assembled set, rather than on each input.
    # Every route in passes through this point, so a source that turns out to carry a
    # code from another nation cannot arrive by a path nobody filtered. crime_codes
    # also filters, which is not redundancy but cost: it keeps the other nations out of
    # a distinct over 96 million rows. Both use is_england_or_wales, so they cannot
    # disagree about what belongs.
    members = (
        crime_codes(police_df)
        .withColumn("_has_crime", F.lit(True))
        .join(
            priced_codes(doogal_df, ppd_df).withColumn("_has_price", F.lit(True)),
            "lsoa_code",
            "full_outer",
        )
        .where(is_england_or_wales())
    )

    return members.join(chosen, "lsoa_code", "left").select(
        "lsoa_code",
        "lsoa_name",
        "district_code",
        "postcodes_in_district",
        "postcodes_total",
        F.coalesce(F.col("in_2011"), F.lit(False)).alias("in_2011"),
        F.coalesce(F.col("in_2021"), F.lit(False)).alias("in_2021"),
        F.coalesce(F.col("_has_crime"), F.lit(False)).alias("has_crime"),
        F.coalesce(F.col("_has_price"), F.lit(False)).alias("has_price"),
    )


def assert_measured_columns(measured: DataFrame) -> DataFrame:
    """Fail unless the frame is the one measure_small_areas returns."""
    missing = sorted(set(MEASURED_COLUMNS) - set(measured.columns))
    if missing:
        raise ValueError(f"dim_lsoa input is missing columns it reads: {missing}")
    return measured


def assert_codes_are_england_or_wales(measured: DataFrame) -> DataFrame:
    """Fail on a code outside the England and Wales series.

    A backstop rather than the filter. Membership is drawn from crime codes already
    restricted to the two nations and from transactions, which are an England and Wales
    series throughout, so nothing should reach here. If something does, it entered
    membership by a route this module does not know about, and it would take a null
    nation into a NOT NULL column.
    """
    offenders = (
        measured.where(nation_code().isNull())
        .select("lsoa_code", "district_code")
        .limit(10)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_lsoa codes sit outside the England and Wales series, which is the "
            f"only one this geography uses: {[row.asDict() for row in offenders]}"
        )
    return measured


def assert_every_area_has_postcodes(measured: DataFrame) -> DataFrame:
    """Fail where an area reaches no postcode, since the district comes from them.

    A code the crime source publishes that the directory does not carry has no district
    and no share, and both are NOT NULL.
    """
    offenders = (
        measured.where(
            F.col("district_code").isNull()
            | F.col("postcodes_total").isNull()
            | (F.col("postcodes_total") <= 0)
        )
        .select("lsoa_code", "has_crime", "has_price")
        .limit(10)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_lsoa areas reach no postcode in the directory, so no district can be "
            f"assigned to them: {[row.asDict() for row in offenders]}"
        )
    return measured


def assert_shares_are_decisive(shaped: DataFrame) -> DataFrame:
    """Fail where the assigned district holds no more than half the area's postcodes.

    Two shapes reach here and they call for different answers, which is why the message
    carries the counts rather than the share alone. An even split between two districts
    has no winner: the ranking settles it on district code, and the result would be a
    coin toss recorded as a measurement. A plurality across three or more has an
    unambiguous winner and still falls below one half, so what fails is the share the
    table admits rather than the choice of district.

    Neither occurs in the measured data, where all 82 straddling areas hold at least 92
    percent of their postcodes in the district they take.
    """
    offenders = (
        shaped.where(
            (F.col("district_assignment") == F.lit(MAJORITY))
            & (F.col("majority_share") <= F.lit(0.5))
        )
        .select(
            "lsoa_code",
            "district_code",
            "postcodes_in_district",
            "postcodes_total",
            "majority_share",
        )
        .limit(10)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_lsoa areas take a district holding half their postcodes or fewer, so "
            "the share falls below what the table admits: "
            f"{[row.asDict() for row in offenders]}. Group the postcode directory on "
            "the same code for the full per-district split."
        )
    return shaped


def assert_prices_are_2021_coded(shaped: DataFrame) -> DataFrame:
    """Fail where a code exclusive to the 2011 boundaries carries a price.

    Transactions are resolved through the 2021 column alone, so a 2011-exclusive code
    reaching one means the two vintages have been crossed somewhere upstream.
    """
    offenders = (
        shaped.where(
            (F.col("boundary_vintage") == F.lit(ONLY_2011)) & F.col("has_price")
        )
        .select("lsoa_code", "district_code")
        .limit(10)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_lsoa codes exclusive to the 2011 boundaries carry a price, but "
            f"transactions resolve through 2021 codes only: "
            f"{[row.asDict() for row in offenders]}"
        )
    return shaped


def assert_codes_unique(shaped: DataFrame) -> DataFrame:
    """Fail on a repeated code, which is the table's key.

    The primary key is informational and unenforced, so two rows for one area would
    reach Delta and double every measure keyed on it.
    """
    duplicates = (
        shaped.groupBy("lsoa_code").count().where(F.col("count") > 1).limit(5).collect()
    )
    if duplicates:
        raise ValueError(
            "dim_lsoa key broken, lsoa_code is not unique: "
            f"{[row.asDict() for row in duplicates]}"
        )
    return shaped


def assert_districts_conform(shaped: DataFrame, dim_area_df: DataFrame) -> DataFrame:
    """Fail where a district has no row in dim_area.

    The foreign key is informational, so a district the area dimension does not carry
    would reach Delta and drop out of every rollup silently. Runs against the loaded
    dimension rather than the frame that produced it, which is what forces dim_area to
    be written first.

    Kept as a named function rather than called inline from the notebook. The order it
    imposes on the load is a property of this table, and a bare call in a notebook cell
    states nothing about why it has to sit after the write.
    """
    return assert_keys_conform(
        shaped,
        dim_area_df,
        child_column="district_code",
        parent_column="area_code",
        child_name="dim_lsoa",
        parent_name="dim_area",
        example_column="lsoa_code",
    )


def transform_dim_lsoa(measured: DataFrame) -> DataFrame:
    """The measured small-area population to the Gold small-area dimension.

    Args:
        measured: output of measure_small_areas.

    Returns:
        One row per small area, with the columns named in GOLD_COLUMNS.

    Note:
        Conformance to dim_area is not checked here. It runs against the loaded
        dimension through assert_districts_conform, which the notebook calls once
        dim_area is written.
    """
    assert_measured_columns(measured)
    assert_codes_are_england_or_wales(measured)
    assert_every_area_has_postcodes(measured)

    shaped = (
        measured.withColumn("district_assignment", district_assignment())
        .withColumn("majority_share", majority_share())
        .withColumn("boundary_vintage", boundary_vintage())
        .withColumn("nation_code", nation_code())
    )
    assert_shares_are_decisive(shaped)
    assert_prices_are_2021_coded(shaped)
    assert_codes_unique(shaped)
    return shaped.select(*GOLD_COLUMNS)
