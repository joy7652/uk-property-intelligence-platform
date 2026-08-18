"""Gold dim_area: every published area the platform reports on, at any level.

Grain: one row per area.

Three publishers name three different area sets and the dimension is their union: the
house price index above district level, the rent series including geographies that
exist nowhere else, and the postcode directory at district level. A code carried by
all three is a district where a rental yield can be computed.

Levels are not one hierarchy. Districts roll up to region and then to nation. County
areas carry a published index and no children, because the only district-to-county
membership available uses ceremonial counties on a code series whose values collide
with the metropolitan county codes the index uses. Rental market areas nest inside a
nation and match nothing below them.

Level comes from the code prefix and is authored. An unmapped prefix aborts rather
than defaulting, because the level decides the parent, the name precedence and every
flag below it, and getting one of those silently wrong is worse than failing.

has_postcodes asks whether the postcode directory can attribute postcodes to the area,
not whether the code appears in its district column. The rent series publishes regions
and nations, and the table's own rent_needs_postcodes constraint admits only a rental
market area as published with rent and no postcodes, so those levels have to resolve
true. Counties are the other false: their membership is the collision above.

Names follow the level. The postcode directory names districts, the price index names
everything above them, and the rent series names rental market areas. Where the ruling
publisher does not carry an area the next one does, and name_source records which rule
applied, so the seventeen codes named differently by different publishers stay
traceable to a rule rather than to a coincidence of join order.

Only England is divided into regions, and the postcode directory does not leave its
region column empty for the other three: it restates the country there. A Scottish
district therefore arrives naming "Scotland" as its region, which no region code
matches, so the lookup is gated on the country rather than allowed to miss.

Ancestry is flattened, not left to a walk. A row states every level it belongs to:
region_code and nation_code sit beside parent_area_code, so a region figure counts each
area once whether or not it has a district below it, and an area that exists only at
nation level still contributes there. region_code is null for the 65 districts in
Wales, Scotland and Northern Ireland, since only England is divided into regions.

Every pointer lands on a row. The self-reference carries no foreign key, so parent,
region and nation are each checked against the codes in the same frame before the
write, and a district naming a region the price index has no code for aborts rather
than falling through to its nation and disappearing from every region figure.

Eight Northern Irish rental market areas are published with no code at all. Each is
given one derived from its name, deterministic so it holds across releases, and
code_source separates them from every published code. The derivation runs in Python
over the handful of collected names rather than as a UDF, so one implementation
produces the code and the test exercises that same one.

No lineage columns, unlike Silver. Which run produced a Gold table is recorded in
uk_property_intel.quality.pipeline_run rather than on every row.

No table DDL here either. The Gold contract is declared once in
databricks_src/gold/notebooks/00_create_gold_tables.py, and a generator in this module
would be a second copy of it.

No I/O here. The reads and the Delta write live in
databricks_src/gold/notebooks/01_load_dimensions.py.
"""

from __future__ import annotations

import re

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DISTRICT = "district"
COUNTY = "county"
REGION = "region"
NATION = "nation"
COMPOSITE = "composite"
RENTAL_MARKET_AREA = "rental_market_area"

LEVELS: tuple[str, ...] = (
    DISTRICT,
    COUNTY,
    REGION,
    NATION,
    COMPOSITE,
    RENTAL_MARKET_AREA,
)

PUBLISHED = "published"
DERIVED = "derived"

FROM_POSTCODES = "postcode_directory"
FROM_PRICE_INDEX = "house_price_index"
FROM_RENT_SERIES = "rent_series"

# Prefix of the code this project assigns where the publisher issues none. The table's
# code_shape constraint admits letters and underscores only after it, so a name
# carrying a digit cannot yield a valid code and is rejected rather than mangled.
DERIVED_CODE_PREFIX = "BRMA_NI_"

DERIVED_CODE_COUNTRY = "Northern Ireland"

_NOT_A_LETTER = re.compile(r"[^A-Z]+")

# Level by the first three characters of the GSS code. Authored from the code series
# each publisher uses, and asserted rather than defaulted: an unmapped prefix is a
# geography this model has no rule for.
LEVEL_BY_PREFIX: dict[str, str] = {
    "E06": DISTRICT,  # unitary authority, England
    "E07": DISTRICT,  # non-metropolitan district
    "E08": DISTRICT,  # metropolitan borough
    "E09": DISTRICT,  # London borough
    "W06": DISTRICT,  # unitary authority, Wales
    "S12": DISTRICT,  # council area, Scotland
    "N09": DISTRICT,  # district, Northern Ireland
    "E10": COUNTY,  # county
    "E11": COUNTY,  # metropolitan county
    "E13": COUNTY,  # inner and outer London
    "E12": REGION,
    "E92": NATION,
    "W92": NATION,
    "S92": NATION,
    "N92": NATION,
    "K02": COMPOSITE,  # United Kingdom
    "K03": COMPOSITE,  # Great Britain
    "K04": COMPOSITE,  # England and Wales
    "S33": RENTAL_MARKET_AREA,  # broad rental market area, Scotland
}

NATION_CODE: dict[str, str] = {
    "England": "E92000001",
    "Wales": "W92000004",
    "Scotland": "S92000003",
    "Northern Ireland": "N92000002",
}

# Country by the first letter of the code. K codes are composites and carry no single
# country, so they are absent here by design.
COUNTRY_BY_INITIAL: dict[str, str] = {
    "E": "England",
    "W": "Wales",
    "S": "Scotland",
    "N": "Northern Ireland",
}

# Nations each composite spans. The same three codes the Silver price index module
# floors at the latest native start among.
COMPOSITE_NATIONS: dict[str, tuple[str, ...]] = {
    "K04000001": ("England", "Wales"),
    "K03000001": ("England", "Wales", "Scotland"),
    "K02000001": ("England", "Wales", "Scotland", "Northern Ireland"),
}

# The only country divided into regions. The postcode directory restates the country
# in its region column elsewhere, so "region" outside England names a nation.
COUNTRY_WITH_REGIONS = "England"

# The price index publishes E12000005 as "West Midlands Region" to keep it apart from
# the metropolitan county of the same name. The postcode directory writes the plain
# name, so matching regions on name needs this to reach nine of nine rather than eight.
REGION_NAME_ALIAS: dict[str, str] = {"West Midlands Region": "West Midlands"}

# Levels a parent is assigned for. Nations and composites sit at the top; a county's
# children cannot be identified, so naming it a parent would imply a rollup that does
# not exist.
LEVELS_WITH_A_PARENT: frozenset[str] = frozenset(
    {DISTRICT, REGION, RENTAL_MARKET_AREA}
)

GOLD_COLUMNS: tuple[str, ...] = (
    "area_code",
    "area_name",
    "area_level",
    "parent_area_code",
    "region_code",
    "nation_code",
    "country_name",
    "code_source",
    "name_source",
    "has_price_index",
    "has_rent_index",
    "has_postcodes",
)

# Pointers to another row of this same table. Every one of them has to land on a row
# that exists, or a drill-down loses whatever hangs off the dangling end.
ANCESTRY_COLUMNS: tuple[str, ...] = (
    "parent_area_code",
    "region_code",
    "nation_code",
)

KEY_COLUMNS: tuple[str, ...] = ("area_code",)

# Columns read from each Silver table.
HPI_COLUMNS: tuple[str, ...] = ("area_code", "region_name")
ONS_COLUMNS: tuple[str, ...] = ("area_code", "area_name", "region_or_country_name")
DOOGAL_COLUMNS: tuple[str, ...] = ("district_code", "district", "region", "country")

# What measure_published_areas returns and the transform reads. source_country is
# named apart from the output's country_name so neither overwrites the other midway
# through the projection.
MEASURED_COLUMNS: tuple[str, ...] = (
    "area_code",
    "price_index_name",
    "rent_series_name",
    "postcode_directory_name",
    "source_region",
    "source_country",
    "has_price_index",
    "has_rent_index",
    "in_postcode_districts",
)


# --------------------------------------------------------------------------- #
# Authored maps
# --------------------------------------------------------------------------- #


def assert_maps_consistent() -> None:
    """Fail on authored maps that contradict each other.

    Runs at import, so a bad edit stops the module loading rather than aborting a load
    that has already shuffled the postcode directory.
    """
    unknown = sorted(set(LEVEL_BY_PREFIX.values()) - set(LEVELS))
    if unknown:
        raise ValueError(
            f"dim_area LEVEL_BY_PREFIX names levels outside {list(LEVELS)}: {unknown}"
        )

    misshapen = sorted(
        prefix
        for prefix in LEVEL_BY_PREFIX
        if not re.fullmatch(r"[A-Z][0-9]{2}", prefix)
    )
    if misshapen:
        raise ValueError(
            "dim_area LEVEL_BY_PREFIX keys must be the first three characters of a "
            f"GSS code, one letter then two digits: {misshapen}"
        )

    for code, nations in COMPOSITE_NATIONS.items():
        if LEVEL_BY_PREFIX.get(code[:3]) != COMPOSITE:
            raise ValueError(
                f"dim_area COMPOSITE_NATIONS names {code}, which LEVEL_BY_PREFIX does "
                f"not treat as a {COMPOSITE}."
            )
        unnamed = sorted(set(nations) - set(NATION_CODE))
        if unnamed:
            raise ValueError(
                f"dim_area COMPOSITE_NATIONS gives {code} nations with no code in "
                f"NATION_CODE: {unnamed}"
            )

    if set(COUNTRY_BY_INITIAL.values()) != set(NATION_CODE):
        raise ValueError(
            "dim_area COUNTRY_BY_INITIAL and NATION_CODE name different countries: "
            f"{sorted(set(COUNTRY_BY_INITIAL.values()) ^ set(NATION_CODE))}"
        )

    for name, code in NATION_CODE.items():
        if COUNTRY_BY_INITIAL.get(code[0]) != name:
            raise ValueError(
                f"dim_area NATION_CODE gives {name} the code {code}, whose initial "
                f"names {COUNTRY_BY_INITIAL.get(code[0])!r} instead."
            )

    if DERIVED_CODE_COUNTRY not in NATION_CODE:
        raise ValueError(
            f"dim_area DERIVED_CODE_COUNTRY {DERIVED_CODE_COUNTRY!r} has no code in "
            "NATION_CODE, so a derived area could not be given a nation."
        )

    stray = sorted(LEVELS_WITH_A_PARENT - set(LEVELS))
    if stray:
        raise ValueError(f"dim_area LEVELS_WITH_A_PARENT names unknown levels: {stray}")


assert_maps_consistent()


def derived_area_code(area_name: str) -> str:
    """A stable code for an area the publisher issues none for.

    Pure Python and deterministic: the same name yields the same code on every release,
    which is what lets a fact key on it. Letters only, because the code_shape
    constraint admits nothing else after the prefix, so a name carrying a digit is
    rejected here rather than quietly losing it and colliding with another name.
    """
    if not area_name or not area_name.strip():
        raise ValueError("dim_area cannot derive a code from an empty area name.")
    if any(character.isdigit() for character in area_name):
        raise ValueError(
            f"dim_area cannot derive a code from {area_name!r}: the code shape admits "
            "letters and underscores only, so a digit would be dropped and two names "
            "could collapse into one code."
        )
    slug = _NOT_A_LETTER.sub("_", area_name.strip().upper()).strip("_")
    if not slug:
        raise ValueError(
            f"dim_area cannot derive a code from {area_name!r}: it carries no letters."
        )
    return DERIVED_CODE_PREFIX + slug


def derived_codes(area_names: list[str]) -> dict[str, str]:
    """Name to derived code, failing where two names would collapse into one.

    The self-reference a code carries is its identity, and nothing downstream can tell
    two areas sharing a code apart, so the collision is caught here.
    """
    codes = {name: derived_area_code(name) for name in area_names}
    seen: dict[str, list[str]] = {}
    for name, code in codes.items():
        seen.setdefault(code, []).append(name)
    collisions = {code: sorted(names) for code, names in seen.items() if len(names) > 1}
    if collisions:
        raise ValueError(
            f"dim_area derived codes collide, so two areas would share one: "
            f"{collisions}"
        )
    return codes


def _mapped_to(values: dict[str, object], key: Column, data_type: str) -> Column:
    """Chained when over an authored map, cast to the target type.

    The cast is not optional. A branch holding None is untyped, so a column whose every
    branch is null resolves to void and fails the insert. An empty map yields a typed
    null, which is what a lookup against nothing should give.
    """
    expr: Column | None = None
    for candidate, value in values.items():
        condition = key == F.lit(candidate)
        expr = (
            F.when(condition, F.lit(value))
            if expr is None
            else expr.when(condition, F.lit(value))
        )
    if expr is None:
        return F.lit(None).cast(data_type)
    return expr.cast(data_type)


def area_level() -> Column:
    """Level for a row's code, null for a prefix outside the authored map.

    A derived code carries no GSS prefix and is a rental market area by construction,
    which is settled before the prefix lookup runs.
    """
    return F.when(
        F.col("code_source") == F.lit(DERIVED), F.lit(RENTAL_MARKET_AREA)
    ).otherwise(
        _mapped_to(dict(LEVEL_BY_PREFIX), F.substring("area_code", 1, 3), "string")
    )


def country_of_code() -> Column:
    """Country a code sits in, from its first letter. Null for a composite."""
    return _mapped_to(
        dict(COUNTRY_BY_INITIAL), F.substring("area_code", 1, 1), "string"
    )


def directory_region_name() -> Column:
    """A region's name as the postcode directory writes it."""
    index_name = F.col("price_index_name")
    return F.coalesce(
        _mapped_to(dict(REGION_NAME_ALIAS), index_name, "string"), index_name
    )


# --------------------------------------------------------------------------- #
# Measurement
# --------------------------------------------------------------------------- #


def assert_source_columns(
    hpi_df: DataFrame, ons_df: DataFrame, doogal_df: DataFrame
) -> None:
    """Fail unless each Silver frame carries the columns this module reads.

    One direction only. Gold reads a projection of tables it does not own, so a missing
    column is a fault and an extra one is not.
    """
    for label, frame, expected in (
        ("house price index", hpi_df, HPI_COLUMNS),
        ("rent series", ons_df, ONS_COLUMNS),
        ("postcode directory", doogal_df, DOOGAL_COLUMNS),
    ):
        missing = sorted(set(expected) - set(frame.columns))
        if missing:
            raise ValueError(
                f"dim_area {label} source is missing columns it reads: {missing}"
            )


def price_index_areas(hpi_df: DataFrame) -> DataFrame:
    """One row per area the price index publishes, with the name it uses."""
    return (
        hpi_df.where(F.col("area_code").isNotNull())
        .select(
            F.col("area_code"),
            F.trim(F.col("region_name")).alias("price_index_name"),
        )
        .distinct()
    )


def rent_series_areas(ons_df: DataFrame) -> DataFrame:
    """One row per area the rent series publishes, coded or not.

    area_code is the key everywhere except the eight Northern Irish rental market
    areas, which the publisher writes without one. Those are carried here with a null
    code and given a derived one downstream.
    """
    return (
        ons_df.where(F.col("area_name").isNotNull())
        .select(
            F.col("area_code"),
            F.trim(F.col("area_name")).alias("rent_series_name"),
            F.trim(F.col("region_or_country_name")).alias("rent_series_country"),
        )
        .distinct()
    )


def postcode_districts(doogal_df: DataFrame) -> DataFrame:
    """One row per district the postcode directory assigns postcodes to.

    Region and country come from the same pass. Both are properties of the district
    rather than of a postcode, so a district resolving to two of either is a source
    change and is asserted rather than settled by picking one.
    """
    return (
        doogal_df.where(F.col("district_code").isNotNull())
        .groupBy("district_code")
        .agg(
            F.max(F.trim(F.col("district"))).alias("postcode_directory_name"),
            F.countDistinct("region").alias("region_count"),
            # max ignores nulls, so a district outside England keeps a null region.
            F.max(F.trim(F.col("region"))).alias("source_region"),
            F.countDistinct("country").alias("country_count"),
            F.max(F.trim(F.col("country"))).alias("source_country"),
        )
        .withColumnRenamed("district_code", "area_code")
    )


def assert_districts_resolve_once(districts: DataFrame) -> DataFrame:
    """Fail where a district spans two regions or two countries.

    Districts nest inside a region and a country, and the parent this dimension assigns
    depends on that. A district spanning either would take whichever value max
    happened to return.
    """
    offenders = (
        districts.where((F.col("region_count") > 1) | (F.col("country_count") > 1))
        .select("area_code", "postcode_directory_name", "region_count", "country_count")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_area districts span more than one region or country in the postcode "
            "directory, so their parent is ambiguous: "
            f"{[row.asDict() for row in offenders]}"
        )
    return districts


def measure_published_areas(
    hpi_df: DataFrame, ons_df: DataFrame, doogal_df: DataFrame
) -> DataFrame:
    """The union of the three published area sets, one row per code.

    Args:
        hpi_df: uk_property_intel.silver.hpi, or a projection carrying HPI_COLUMNS.
        ons_df: uk_property_intel.silver.ons_private_rents, or a projection carrying
            ONS_COLUMNS.
        doogal_df: uk_property_intel.silver.doogal, or a projection carrying
            DOOGAL_COLUMNS.

    Returns:
        MEASURED_COLUMNS, with a row per coded area and a null-coded row for each area
        the rent series publishes without a code.

    Note:
        Separate from the transform because this is the expensive half. It shuffles the
        postcode directory to district grain and distincts two panels, while everything
        downstream works on a few hundred rows and runs several actions over them. The
        caller persists this frame.
    """
    assert_source_columns(hpi_df, ons_df, doogal_df)

    prices = price_index_areas(hpi_df)
    rents = rent_series_areas(ons_df)
    districts = assert_districts_resolve_once(postcode_districts(doogal_df)).drop(
        "region_count", "country_count"
    )

    coded_rents = rents.where(F.col("area_code").isNotNull()).drop("rent_series_country")
    uncoded_rents = rents.where(F.col("area_code").isNull())

    coded = (
        prices.join(coded_rents, "area_code", "full_outer")
        .join(districts, "area_code", "full_outer")
        .select(
            "area_code",
            "price_index_name",
            "rent_series_name",
            "postcode_directory_name",
            "source_region",
            "source_country",
            F.col("price_index_name").isNotNull().alias("has_price_index"),
            F.col("rent_series_name").isNotNull().alias("has_rent_index"),
            F.col("postcode_directory_name").isNotNull().alias("in_postcode_districts"),
        )
    )

    uncoded = uncoded_rents.select(
        F.col("area_code"),
        F.lit(None).cast("string").alias("price_index_name"),
        F.col("rent_series_name"),
        F.lit(None).cast("string").alias("postcode_directory_name"),
        F.lit(None).cast("string").alias("source_region"),
        F.col("rent_series_country").alias("source_country"),
        F.lit(False).alias("has_price_index"),
        F.lit(True).alias("has_rent_index"),
        F.lit(False).alias("in_postcode_districts"),
    )

    return coded.unionByName(uncoded).select(*MEASURED_COLUMNS)


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def assert_measured_columns(measured: DataFrame) -> DataFrame:
    """Fail unless the frame is the one measure_published_areas returns."""
    missing = sorted(set(MEASURED_COLUMNS) - set(measured.columns))
    if missing:
        raise ValueError(f"dim_area input is missing columns it reads: {missing}")
    return measured


def assert_uncoded_areas_are_northern_irish(measured: DataFrame) -> DataFrame:
    """Fail where an area with no published code sits outside Northern Ireland.

    The derived code names Northern Ireland in its prefix, so an uncoded area from
    anywhere else would take a code claiming a country it is not in.
    """
    offenders = (
        measured.where(
            F.col("area_code").isNull()
            & ~F.col("source_country").eqNullSafe(F.lit(DERIVED_CODE_COUNTRY))
        )
        .select("rent_series_name", "source_country")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            f"dim_area found areas published with no code outside "
            f"{DERIVED_CODE_COUNTRY}, which the derived code prefix names: "
            f"{[row.asDict() for row in offenders]}"
        )
    return measured


def assert_levels_mapped(levelled: DataFrame) -> DataFrame:
    """Fail on any code whose prefix has no level.

    Runs before anything reads the level. An unmapped prefix yields null, and the
    parent, the name precedence and every flag below would resolve against that null
    without anything failing.
    """
    unmapped = (
        levelled.where(F.col("area_level").isNull())
        .select("area_code", "price_index_name", "rent_series_name")
        .distinct()
        .limit(10)
        .collect()
    )
    if unmapped:
        raise ValueError(
            "dim_area codes carry a prefix with no level in LEVEL_BY_PREFIX, so the "
            "geography is one this model has no rule for: "
            f"{[row.asDict() for row in unmapped]}"
        )
    return levelled


def assert_every_area_is_named(named: DataFrame) -> DataFrame:
    """Fail where no publisher carries a name, since area_name is NOT NULL."""
    offenders = (
        named.where(F.col("area_name").isNull() | (F.trim("area_name") == F.lit("")))
        .select("area_code", "area_level")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            f"dim_area areas carry no name from any publisher: "
            f"{[row.asDict() for row in offenders]}"
        )
    return named


def assert_regions_resolve(named: DataFrame) -> DataFrame:
    """Fail where a district names a region the price index has no code for.

    The two publishers are linked by a string match: the postcode directory carries
    region as a name and the price index as a code plus a name. A failed match is
    silent otherwise, because the district falls through to its nation parent and looks
    correct, while every region-level figure quietly loses it.

    Only English districts the directory carries are checked, and every one of them is.
    Two cases have to stay apart. A district the directory files under a region name
    that no region code matches is a broken join and fails here. A district the
    directory does not carry at all has no region available from anywhere, which is a
    coverage gap rather than a fault, and has_postcodes already records it.

    Testing that a region name was supplied would merge the two, and outside England the
    column is never empty anyway: it restates the country, so a Welsh district arrives
    naming "Wales" as its region.
    """
    offenders = (
        named.where(
            (F.col("area_level") == F.lit(DISTRICT))
            & (F.col("country_name") == F.lit(COUNTRY_WITH_REGIONS))
            & F.col("in_postcode_districts")
            & F.col("region_code").isNull()
        )
        .select("area_code", "area_name", "source_region")
        .limit(10)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_area districts name a region the price index publishes no code for, "
            "so they would roll up to their nation and vanish from every region "
            f"figure: {[row.asDict() for row in offenders]}"
        )
    return named


def assert_ancestry_closed(named: DataFrame) -> DataFrame:
    """Fail where a parent, region or nation names an area with no row of its own.

    Every pointer in ANCESTRY_COLUMNS refers back to this table, and the self-reference
    carries no foreign key, so a dangling one reaches Delta unchallenged. One pass:
    the pointers are stacked and anti-joined against the codes once rather than a join
    per column.
    """
    stacked: DataFrame | None = None
    for column in ANCESTRY_COLUMNS:
        part = named.where(F.col(column).isNotNull()).select(
            F.col("area_code").alias("held_by"),
            F.lit(column).alias("pointer"),
            F.col(column).alias("target"),
        )
        stacked = part if stacked is None else stacked.unionByName(part)

    dangling = (
        stacked.join(
            named.select(F.col("area_code").alias("target")), "target", "left_anti"
        )
        .limit(10)
        .collect()
    )
    if dangling:
        raise ValueError(
            "dim_area rows point at an area that has no row, so a drill-down would "
            f"lose whatever hangs off it: {[row.asDict() for row in dangling]}"
        )
    return named


def assert_codes_unique(named: DataFrame) -> DataFrame:
    """Fail on a repeated code, which is the table's key.

    The primary key is informational and unenforced, so a full outer join producing
    two rows for one code would reach Delta without complaint.
    """
    duplicates = (
        named.groupBy("area_code")
        .count()
        .where(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if duplicates:
        raise ValueError(
            "dim_area key broken, area_code is not unique: "
            f"{[row.asDict() for row in duplicates]}"
        )
    return named


# --------------------------------------------------------------------------- #
# Attribution
# --------------------------------------------------------------------------- #


def assign_codes(measured: DataFrame) -> DataFrame:
    """Give every uncoded area a derived code and record which rule applied.

    The derivation runs in Python over the collected names rather than as a UDF, so the
    code a fact keys on comes from the same function the test exercises, and the
    collision check has the whole set in front of it.
    """
    names = [
        row["rent_series_name"]
        for row in measured.where(F.col("area_code").isNull())
        .select("rent_series_name")
        .distinct()
        .collect()
    ]
    codes = derived_codes(names)

    return measured.withColumn(
        "code_source",
        F.when(F.col("area_code").isNull(), F.lit(DERIVED)).otherwise(F.lit(PUBLISHED)),
    ).withColumn(
        "area_code",
        F.coalesce(
            F.col("area_code"), _mapped_to(codes, F.col("rent_series_name"), "string")
        ),
    )


def area_name_and_source() -> tuple[Column, Column]:
    """The name to use and the publisher it came from.

    Precedence follows the level: the postcode directory names districts, the price
    index names everything above them, and the rent series names rental market areas.
    Where the ruling publisher does not carry an area the next one does, so a name is
    always produced while name_source stays honest about which rule applied.

    Name and source are chosen together from one ordered list rather than by a coalesce
    beside a parallel when-chain, which would be free to disagree about which publisher
    won.
    """

    def candidates(*pairs: tuple[Column, str]) -> Column:
        return F.array(*[F.array(name, F.lit(label)) for name, label in pairs])

    from_directory = (F.col("postcode_directory_name"), FROM_POSTCODES)
    from_index = (F.col("price_index_name"), FROM_PRICE_INDEX)
    from_rents = (F.col("rent_series_name"), FROM_RENT_SERIES)

    ordered = (
        F.when(
            F.col("area_level") == F.lit(DISTRICT),
            candidates(from_directory, from_index, from_rents),
        )
        .when(
            F.col("area_level") == F.lit(RENTAL_MARKET_AREA),
            candidates(from_rents, from_index, from_directory),
        )
        .otherwise(candidates(from_index, from_rents, from_directory))
    )
    # filter preserves order, so precedence is the array order rather than whatever the
    # optimiser settles on.
    chosen = F.filter(ordered, lambda pair: pair[0].isNotNull())
    # try_element_at rather than indexing. ANSI mode is on from DBR 17.0, and an area no
    # publisher names leaves this array empty, where a plain index raises instead of
    # yielding the null that assert_every_area_is_named exists to report. One-based.
    first = F.try_element_at(chosen, F.lit(1))
    return F.try_element_at(first, F.lit(1)), F.try_element_at(first, F.lit(2))


def region_code(regions: dict[str, str]) -> Column:
    """Region an area sits in, null outside England.

    Carried as a column rather than left to a walk up parent_area_code, so a row states
    every level it belongs to outright and a region-level figure counts each area once
    whether or not it has a district below it.

    A region row carries its own code, which is how nation_code already behaves on a
    nation row. Filtering on the column then reaches the published region series as
    well as the districts under it.

    Only England is divided into regions. The lookup is gated on that rather than left
    to miss, because the postcode directory does not leave its region column empty
    outside England: it restates the country there, so a Scottish district arrives
    carrying the region name "Scotland". Ungated, that would be indistinguishable from
    an English district whose region failed to match.

    Args:
        regions: region name as the postcode directory writes it, to the code the price
            index publishes it under.
    """
    level = F.col("area_level")
    return (
        F.when(level == F.lit(REGION), F.col("area_code"))
        .when(
            (level == F.lit(DISTRICT))
            & (country_name() == F.lit(COUNTRY_WITH_REGIONS)),
            _mapped_to(regions, F.col("source_region"), "string"),
        )
        .otherwise(F.lit(None).cast("string"))
    )


def parent_area_code() -> Column:
    """Immediate parent, null where the level has none.

    A district's parent is its region where it has one and its nation otherwise.
    Regions and rental market areas sit under a nation. Counties, nations and
    composites have none: a county's children cannot be identified, so naming it a
    parent would imply a rollup that does not exist.

    Reads region_code rather than repeating its lookup, so the two cannot disagree
    about which region a district belongs to.
    """
    to_nation = _mapped_to(dict(NATION_CODE), country_name(), "string")
    level = F.col("area_level")
    return (
        F.when(level == F.lit(DISTRICT), F.coalesce(F.col("region_code"), to_nation))
        .when(level == F.lit(REGION), F.lit(NATION_CODE["England"]))
        .when(level == F.lit(RENTAL_MARKET_AREA), to_nation)
        .otherwise(F.lit(None).cast("string"))
    )


def country_name() -> Column:
    """Country an area sits in, null for a composite spanning more than one.

    The code's initial rules, since it is the publisher's own statement. A derived code
    carries no such initial, so the country the rent series filed it under stands in.
    """
    return F.when(
        F.col("area_level") == F.lit(COMPOSITE), F.lit(None).cast("string")
    ).otherwise(F.coalesce(country_of_code(), F.col("source_country")))


def nation_code() -> Column:
    """Code of the nation an area sits in, null for a composite."""
    return _mapped_to(dict(NATION_CODE), country_name(), "string")


def region_codes(levelled: DataFrame) -> dict[str, str]:
    """Region name as the directory writes it, to the price index's code for it.

    Collected rather than joined. There are nine regions, and a district's parent is a
    lookup against them that a join would turn into a shuffle.
    """
    rows = (
        levelled.where(F.col("area_level") == F.lit(REGION))
        .select(directory_region_name().alias("name"), F.col("area_code"))
        .where(F.col("name").isNotNull())
        .collect()
    )
    return {row["name"]: row["area_code"] for row in rows}


def has_postcodes(regions: list[str], countries: list[str]) -> Column:
    """Whether the postcode directory can attribute postcodes to the area.

    Not the same question as whether the code appears in its district column. The rent
    series publishes regions and nations, and the table admits only a rental market
    area as carrying rent without postcodes, so those levels resolve through the
    membership measured for them. Counties are false: their only available membership
    is the ceremonial code collision this model discarded.

    A composite resolves true where every nation it spans is one the directory covers,
    which is a subset test no join expresses.
    """
    known_countries = {name for name in countries if name}
    reachable = [
        code
        for code, nations in COMPOSITE_NATIONS.items()
        if set(nations) <= known_countries
    ]
    known_regions = sorted({name for name in regions if name})

    in_regions = (
        directory_region_name().isin(*known_regions) if known_regions else F.lit(False)
    )
    in_countries = (
        country_name().isin(*sorted(known_countries)) if known_countries else F.lit(False)
    )
    in_composites = (
        F.col("area_code").isin(*reachable) if reachable else F.lit(False)
    )

    level = F.col("area_level")
    return (
        F.when(level == F.lit(DISTRICT), F.col("in_postcode_districts"))
        .when(level == F.lit(REGION), in_regions)
        .when(level == F.lit(NATION), in_countries)
        .when(level == F.lit(COMPOSITE), in_composites)
        .otherwise(F.lit(False))
    )


def transform_dim_area(measured: DataFrame) -> DataFrame:
    """The measured area union to the Gold published-area dimension.

    Args:
        measured: output of measure_published_areas.

    Returns:
        One row per published area, with the columns named in GOLD_COLUMNS.
    """
    assert_measured_columns(measured)
    assert_uncoded_areas_are_northern_irish(measured)

    levelled = assert_levels_mapped(
        assign_codes(measured).withColumn("area_level", area_level())
    )

    # Both read off the dimension being built. The regions are the price index's own
    # E12 rows, and the countries are what the directory filed districts under, so
    # neither needs a second pass over Silver.
    regions = region_codes(levelled)

    # What the directory actually files postcodes under, in one collect. Region
    # membership is measured against these rather than against the region set itself,
    # which would compare it with its own members and answer true every time.
    filed_under = levelled.select("source_region", "source_country").distinct().collect()
    directory_regions = [row["source_region"] for row in filed_under]
    countries = [row["source_country"] for row in filed_under]

    name, name_source = area_name_and_source()
    named = (
        levelled.withColumn("area_name", F.trim(name))
        .withColumn("name_source", name_source)
        .withColumn("nation_code", nation_code())
        .withColumn("country_name", country_name())
        .withColumn("region_code", region_code(regions))
        .withColumn("parent_area_code", parent_area_code())
        .withColumn("has_postcodes", has_postcodes(directory_regions, countries))
    )
    assert_every_area_is_named(named)
    assert_regions_resolve(named)
    assert_codes_unique(named)
    assert_ancestry_closed(named)
    return named.select(*GOLD_COLUMNS)
