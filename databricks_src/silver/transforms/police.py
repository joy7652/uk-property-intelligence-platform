"""UK Police street-level crime: Bronze ZIP archives to Silver.

Grain: one row per published incident record.

There is no natural key. Crime ID is a one-way hash of the force's offence reference
and is left blank for anti-social behaviour, which is roughly a third of the rows.
Northern Ireland reuses a small pool of references, so 9,656 of its ids recur monthly
across the whole series and identify nothing. Date is truncated to year and month at
anonymisation and coordinates are snapped to shared map points, so two genuine
incidents on one street in one month are indistinguishable. Nothing here deduplicates
rows: duplicates are counted and reported, because deleting them would remove real
crimes.

Cross-snapshot duplication is resolved before this module runs. Each archive is a
rolling snapshot restating up to 36 months, so the same (month, force) file appears in
several archives. select_newest picks the latest copy from the member names and the
rest are never decompressed, so no shuffle is spent on it and no key is needed to tell
copies apart.

snapshot_month records which archive supplied the row. Outcome state is whatever that
archive held, so a 2011 crime has years of outcome settlement behind it and a crime
from the newest month has none. The lag is derivable from snapshot_month and
crime_month together, and a Gold model comparing outcome rates across years has to
account for it.

Validation is one pass. Every rule is a row predicate, so the whole set is evaluated
in a single aggregate rather than one action each; at ninety-six million rows the
difference is most of the runtime. Rules stay individually named, documented and
testable, and a failure reports every rule that fired rather than only the first.
Detail is collected afterwards, once, and only for rules that actually failed.

Coordinates are nulled in two cases. (0, 0) is the publisher's sentinel for a crime it
could not place within 20 km of a map point. Points outside the UK box are corrupt:
British Transport Police published twenty-four rows in early 2021 giving Scottish
stations positive longitudes that put them in the North Sea. Both are nulled and
counted rather than kept as locations.

force comes from the file path and is the slug the publisher uses. reported_by is the
force's display name from inside the file. They are different vocabularies and both
are carried.

Casts use try_cast rather than cast. ANSI mode is on from DBR 17.0, so a plain cast
raises on the first malformed cell with an error that does not name the column.
try_cast yields null instead, and the cast rules turn that null back into a failure
that names the column and the value that caused it.

No I/O here. Archive selection, extraction, the read, and the Delta write live in
databricks_src/silver/notebooks/06_police.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

COORDINATE_DDL = "decimal(9, 6)"
YEAR_DDL = "int"
DATE_FORMAT = "yyyy-MM"

DATASET = "street"

# First month police.uk published.
SERIES_START = "2010-12"

# The publisher's sentinel for a crime it could not place. Both columns carry it or
# neither does.
UNLOCATED = 0

# Bounding box for a published coordinate. British Transport Police cover the whole of
# Great Britain, so the northern edge has to admit Thurso at 58.59N even though no
# Scottish force files territorial data. The eastern edge sits just past Lowestoft
# Ness at 1.76E, the easternmost point of England.
LATITUDE_RANGE: tuple[float, float] = (49.0, 61.0)
LONGITUDE_RANGE: tuple[float, float] = (-9.0, 2.0)

# Per-row path of the staged member, added by the reader from _metadata. The load
# spans thousands of files and month and force are only in the path.
MEMBER_PATH = "_member_path"

# Path parts, as regexes for regexp_extract. The force segment carries hyphens, so the
# dataset name anchors the match at the end rather than a split on hyphen.
FOLDER_MONTH = r".*/(\d{4}-\d{2})/[^/]+$"
FILE_MONTH = rf".*/(\d{{4}}-\d{{2}})-.+-{DATASET}\.csv$"
FILE_FORCE = rf".*/\d{{4}}-\d{{2}}-(.+)-{DATASET}\.csv$"

SNAPSHOT_LABEL = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# Source header to Silver column.
COLUMN_MAP: dict[str, str] = {
    "Crime ID": "crime_id",
    "Month": "crime_month",
    "Reported by": "reported_by",
    "Falls within": "falls_within",
    "Longitude": "longitude",
    "Latitude": "latitude",
    "Location": "location",
    "LSOA code": "lsoa_code",
    "LSOA name": "lsoa_name",
    "Crime type": "crime_type",
    "Last outcome category": "last_outcome_category",
    "Context": "context",
}

SOURCE_COLUMNS: tuple[str, ...] = tuple(COLUMN_MAP)

SOURCE_OF: dict[str, str] = {target: source for source, target in COLUMN_MAP.items()}

# Not in the file. crime_year is derived from the month, force from the path, and
# snapshot_month from the archive the row was read out of.
DERIVED_COLUMNS: tuple[str, ...] = ("crime_year", "force", "snapshot_month")

# The reader adds the member path so month and force can be derived per row.
FRAME_COLUMNS: tuple[str, ...] = SOURCE_COLUMNS + (MEMBER_PATH,)

# Columns produced by cast_columns, before lineage is stamped.
TYPED_COLUMNS: tuple[str, ...] = (
    "crime_id",
    "crime_month",
    "crime_year",
    "force",
    "reported_by",
    "falls_within",
    "longitude",
    "latitude",
    "location",
    "lsoa_code",
    "lsoa_name",
    "crime_type",
    "last_outcome_category",
    "context",
    "snapshot_month",
)

LINEAGE_COLUMNS: tuple[str, ...] = ("_source_file", "_ingestion_ts")

SILVER_COLUMNS: tuple[str, ...] = TYPED_COLUMNS + LINEAGE_COLUMNS

# Columns that must be populated on every row. Not named KEY_COLUMNS, unlike the other
# sources, because these do not identify a row: they are the grain the file selection
# works at plus the vintage it came from.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "crime_month",
    "crime_year",
    "force",
    "snapshot_month",
)

COORDINATE_COLUMNS: tuple[str, ...] = ("longitude", "latitude")

# The sixteen categories a full load observed, which is the whole published
# vocabulary across the series. Three eras: six categories to 2011-08, eleven to
# 2013-04, then fourteen from 2013-05, when the Home Office split public disorder and
# weapons in two and renamed violent crime. The older names stay in the older months,
# so both vocabularies have to load.
#
# last_outcome_category is deliberately not guarded. It comes from Ministry of Justice
# court-result matching, its vocabulary has changed repeatedly across sixteen years,
# and a fixed set would abort on a category the Home Office renamed rather than on a
# fault. Its observed values are reported by check_rules instead.
DOMAINS: dict[str, tuple[str, ...]] = {
    "crime_type": (
        "Anti-social behaviour",
        "Bicycle theft",
        "Burglary",
        "Criminal damage and arson",
        "Drugs",
        "Other crime",
        "Other theft",
        "Possession of weapons",
        "Public order",
        "Robbery",
        "Shoplifting",
        "Theft from the person",
        "Vehicle crime",
        "Violence and sexual offences",
        "Public disorder and weapons",
        "Violent crime",
    ),
}

# Columns whose distinct values are collected during the validation pass. Only
# bounded-cardinality columns belong here: collect_set materialises every distinct
# value on the driver, so the same call on location or lsoa_code would be a hazard.
VOCABULARY_COLUMNS: tuple[str, ...] = ("crime_type", "last_outcome_category")


# --------------------------------------------------------------------------- #
# Archive selection. Pure Python, no Spark: the notebook supplies the member
# listings it read from the ZIP central directories.
# --------------------------------------------------------------------------- #

# Inner path of a member, in the same shape as the staged path but with the dataset
# still open, since an archive holds all three.
ARCHIVE_MEMBER = re.compile(
    r"^(?:.*/)?(\d{4}-\d{2})/(\d{4}-\d{2})-(.+)-(street|outcomes|stop-and-search)\.csv$"
)


def parse_archive_member(name: str) -> dict[str, str | bool] | None:
    """Month, force, and dataset from an inner archive path, or None for anything that
    is not a data file."""
    match = ARCHIVE_MEMBER.match(name)
    if not match:
        return None
    folder_month, file_month, force, dataset = match.groups()
    return {
        "month": file_month,
        "force": force,
        "dataset": dataset,
        "month_agrees": folder_month == file_month,
    }


def select_newest(
    listings: dict[str, list[str]],
) -> dict[tuple[str, str], tuple[str, str]]:
    """The newest snapshot's copy of each (month, force), for DATASET only.

    Archives are rolling snapshots, so most slots appear in several of them. Resolving
    from the names means the losing copies are never decompressed and the dedup costs
    no shuffle. Snapshot labels are yyyy-MM and sort chronologically as strings.

    Returns the slot mapped to the snapshot that supplies it and the member name
    inside that archive.
    """
    selected: dict[tuple[str, str], tuple[str, str]] = {}
    for snapshot, names in listings.items():
        for name in names:
            parsed = parse_archive_member(name)
            if parsed is None or parsed["dataset"] != DATASET:
                continue
            if not parsed["month_agrees"]:
                continue
            key = (parsed["month"], parsed["force"])
            held = selected.get(key)
            if held is None or snapshot > held[0]:
                selected[key] = (snapshot, name)
    return selected


def unusable_members(listings: dict[str, list[str]]) -> dict[str, list[str]]:
    """Members select_newest leaves out for a reason other than losing a slot.

    Two cases: a name the convention does not fit at all, and a DATASET member whose
    folder month and filename month disagree, whose slot is ambiguous. Both are
    reported rather than dropped quietly, since either means the archive layout has
    changed.
    """
    unusable: dict[str, list[str]] = {}
    for snapshot, names in listings.items():
        found = []
        for name in names:
            parsed = parse_archive_member(name)
            if parsed is None:
                found.append(name)
            elif parsed["dataset"] == DATASET and not parsed["month_agrees"]:
                found.append(name)
        if found:
            unusable[snapshot] = sorted(found)
    return unusable


def assert_snapshot_label(snapshot: str) -> str:
    """Fail on a snapshot label that is not a real yyyy-MM month.

    Checked once per call rather than per row: snapshot_month is a literal, so a bad
    label would put the same null in every row of the archive.
    """
    if not SNAPSHOT_LABEL.match(snapshot or ""):
        raise ValueError(
            f"Police snapshot label {snapshot!r} is not a {DATE_FORMAT} month. It "
            "names the archive vintage and is written to every row."
        )
    return snapshot


# --------------------------------------------------------------------------- #
# Table definition
# --------------------------------------------------------------------------- #


def _column_ddl(name: str) -> str:
    if name in ("crime_month", "snapshot_month"):
        data_type = "DATE"
    elif name == "crime_year":
        data_type = YEAR_DDL.upper()
    elif name == "_ingestion_ts":
        data_type = "TIMESTAMP"
    elif name in COORDINATE_COLUMNS:
        data_type = COORDINATE_DDL.upper()
    else:
        data_type = "STRING"
    nullable = name not in set(REQUIRED_COLUMNS) | set(LINEAGE_COLUMNS)
    return f"{name} {data_type}" + ("" if nullable else " NOT NULL")


def silver_table_ddl() -> str:
    """Column definitions for the Silver table, derived from the cast types.

    Generated rather than hand-written, so the DDL cannot drift from the cast.
    """
    return ",\n    ".join(_column_ddl(name) for name in SILVER_COLUMNS)


def crime_type_check() -> str:
    """CHECK expression for the crime type domain, generated from DOMAINS.

    A second copy of the list in SQL would be free to drift from the guard.
    """
    values = ", ".join(f"'{value}'" for value in DOMAINS["crime_type"])
    return f"crime_type IN ({values})"


def coordinate_box_check() -> str:
    """CHECK expression for the coordinate box, generated from the same constants the
    transform nulls on, so the table cannot disagree with what was written."""
    return (
        "latitude IS NULL OR (latitude BETWEEN "
        f"{LATITUDE_RANGE[0]} AND {LATITUDE_RANGE[1]} AND longitude BETWEEN "
        f"{LONGITUDE_RANGE[0]} AND {LONGITUDE_RANGE[1]})"
    )


# --------------------------------------------------------------------------- #
# Typed expressions
#
# One definition per column, used both by the projection and by the rules that check
# it. A rule that rebuilt its own cast could pass while the projection wrote null.
# --------------------------------------------------------------------------- #


def folder_month() -> Column:
    return F.regexp_extract(F.col(MEMBER_PATH), FOLDER_MONTH, 1)


def file_month() -> Column:
    return F.regexp_extract(F.col(MEMBER_PATH), FILE_MONTH, 1)


def file_force() -> Column:
    return F.regexp_extract(F.col(MEMBER_PATH), FILE_FORCE, 1)


def source(name: str) -> Column:
    """The raw column behind a Silver name. Backticks are required: the published
    headers carry spaces."""
    return F.col(f"`{SOURCE_OF[name]}`")


def typed(name: str, snapshot: str) -> Column:
    """The Silver value for a column, unaliased, from the raw frame."""
    if name == "crime_month":
        return F.expr(f"CAST(try_to_timestamp(`{SOURCE_OF['crime_month']}`, '{DATE_FORMAT}') AS DATE)")
    if name == "crime_year":
        return F.year(typed("crime_month", snapshot))
    if name == "force":
        return file_force()
    if name == "snapshot_month":
        return F.to_date(F.lit(snapshot), DATE_FORMAT)
    if name in COORDINATE_COLUMNS:
        return F.expr(f"try_cast(`{SOURCE_OF[name]}` as {COORDINATE_DDL})")
    return source(name)


def cast_columns(df: DataFrame, snapshot: str) -> DataFrame:
    """Type the coordinates and the month, and add the three derived columns."""
    return df.select([typed(name, snapshot).alias(name) for name in TYPED_COLUMNS])


# --------------------------------------------------------------------------- #
# Validation rules
#
# Each rule is a predicate that is true on a row that breaks it, so the whole set is
# counted in one aggregate. Rules keep their own name, the constraint in words, and
# the columns worth showing when they fire.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, eq=False)
class Rule:
    """One validation rule. eq is off because Column overloads == to build an
    expression rather than to compare."""

    name: str
    constraint: str
    violation: Column
    evidence: tuple[Column, ...]


@dataclass(frozen=True, eq=False)
class Measure:
    """A count reported rather than enforced. Folded into the same pass as the rules,
    so it costs nothing beyond the aggregate."""

    name: str
    note: str
    expression: Column


@dataclass(frozen=True)
class CheckResult:
    """What the validation pass observed. Returned so the caller can report it without
    reading the frame again."""

    rows: int
    violations: dict[str, int]
    measures: dict[str, int]
    vocabularies: dict[str, list[str]]


def member_path_rule(snapshot: str) -> Rule:
    """The staged path must fit the published naming convention.

    regexp_extract yields an empty string rather than null on no match, so an
    unparseable path would give every row of that file a blank force and put it in its
    own group rather than failing.
    """
    return Rule(
        name="member_path_parses",
        constraint=f"staged path fits <month>/<month>-<force>-{DATASET}.csv",
        violation=(folder_month() == "")
        | (file_month() == "")
        | (file_force() == ""),
        evidence=(F.col(MEMBER_PATH).alias("path"),),
    )


def month_consistency_rule(snapshot: str) -> Rule:
    """The folder, the filename, and the Month column must agree.

    The file selection keys on the month in the path and every partition keys on the
    month in the row. A file whose contents belong to another month would pass every
    other rule and land under the wrong label.
    """
    return Rule(
        name="month_consistent",
        constraint="folder month, filename month, and Month column all agree",
        violation=(folder_month() != file_month())
        | ~source("crime_month").eqNullSafe(file_month()),
        evidence=(
            F.col(MEMBER_PATH).alias("path"),
            source("crime_month").alias("month_column"),
            folder_month().alias("folder_month"),
            file_month().alias("file_month"),
        ),
    )


def month_parses_rule(snapshot: str) -> Rule:
    """A populated Month must parse.

    A month number of 13 fits the path pattern and agrees with the filename, so only
    the parse catches it. A null month is caught by month_consistent.
    """
    return Rule(
        name="month_parses",
        constraint=f"populated Month parses as {DATE_FORMAT}",
        violation=source("crime_month").isNotNull()
        & typed("crime_month", snapshot).isNull(),
        evidence=(
            source("crime_month").alias("month_column"),
            F.col(MEMBER_PATH).alias("path"),
        ),
    )


def snapshot_bound_rule(snapshot: str) -> Rule:
    """A crime month must sit between the series start and the archive that published
    it.

    An archive cannot report a month it predates. Either bound failing means the
    staged files came from somewhere other than the archive named in the call.
    """
    month = typed("crime_month", snapshot)
    return Rule(
        name="month_within_snapshot",
        constraint=f"crime month between {SERIES_START} and the snapshot {snapshot}",
        violation=month.isNotNull()
        & (
            (month > typed("snapshot_month", snapshot))
            | (month < F.to_date(F.lit(SERIES_START), DATE_FORMAT))
        ),
        evidence=(
            source("crime_month").alias("month_column"),
            F.col(MEMBER_PATH).alias("path"),
        ),
    )


def crime_type_rule(snapshot: str) -> Rule:
    """Crime type must be a published category.

    A new category is a vocabulary change across the whole load, not a row to skip:
    keeping it would put a value in Silver that no downstream model knows how to read.
    """
    column = source("crime_type")
    return Rule(
        name="crime_type_known",
        constraint="crime type is one of the published categories",
        violation=column.isNull() | ~column.isin(*DOMAINS["crime_type"]),
        evidence=(column.alias("crime_type"), F.col(MEMBER_PATH).alias("path")),
    )


def coordinate_pairing_rule(snapshot: str) -> Rule:
    """Both coordinates are present or neither is.

    The publisher writes them as a pair. One alone means the pair no longer describes
    a point, and nulling the survivor would hide that.
    """
    return Rule(
        name="coordinates_paired",
        constraint="longitude and latitude are both present or both absent",
        violation=source("longitude").isNull() != source("latitude").isNull(),
        evidence=(
            source("longitude").alias("longitude"),
            source("latitude").alias("latitude"),
            F.col(MEMBER_PATH).alias("path"),
        ),
    )


def coordinate_cast_rule(snapshot: str) -> Rule:
    """A populated coordinate must survive its cast.

    try_cast returns null on a malformed value, which would otherwise reach Silver as
    a missing location. Reporting the value that failed is what a count comparison
    between the raw and typed frames cannot do.
    """
    lost = [
        source(name).isNotNull() & typed(name, snapshot).isNull()
        for name in COORDINATE_COLUMNS
    ]
    return Rule(
        name="coordinates_cast",
        constraint=f"populated coordinates cast to {COORDINATE_DDL}",
        violation=lost[0] | lost[1],
        evidence=(
            source("longitude").alias("longitude"),
            source("latitude").alias("latitude"),
            F.col(MEMBER_PATH).alias("path"),
        ),
    )


# Order is the order failures are reported in, running from the path inward.
RULES = (
    member_path_rule,
    month_consistency_rule,
    month_parses_rule,
    snapshot_bound_rule,
    crime_type_rule,
    coordinate_pairing_rule,
    coordinate_cast_rule,
)


def out_of_area(snapshot: str) -> Column:
    """True where a parsed coordinate falls outside the UK box.

    Not a rule. British Transport Police published corrupt longitudes for three
    Scottish stations in early 2021, and those are the publisher's error rather than a
    load fault, so they are nulled and counted.
    """
    latitude = typed("latitude", snapshot)
    longitude = typed("longitude", snapshot)
    return latitude.isNotNull() & ~(
        latitude.between(*LATITUDE_RANGE) & longitude.between(*LONGITUDE_RANGE)
    )


def measures(snapshot: str) -> tuple[Measure, ...]:
    """Counts worth reporting on every load, none of them fatal."""
    return (
        Measure(
            "rows_out_of_area",
            "coordinates outside the UK box, nulled on write",
            out_of_area(snapshot),
        ),
        Measure(
            "rows_at_unlocated_sentinel",
            "coordinates at (0, 0), the publisher's unplaceable marker, nulled on write",
            (typed("longitude", snapshot) == UNLOCATED)
            & (typed("latitude", snapshot) == UNLOCATED),
        ),
        Measure(
            "rows_without_crime_id",
            "anti-social behaviour, which carries no offence reference",
            source("crime_id").isNull(),
        ),
        Measure(
            "rows_without_coordinates",
            "no location published",
            source("latitude").isNull(),
        ),
        Measure(
            "rows_without_lsoa",
            "no LSOA published, so no join to postcode geography",
            source("lsoa_code").isNull(),
        ),
        Measure(
            "rows_without_outcome",
            "no outcome yet, or none published for this record",
            source("last_outcome_category").isNull(),
        ),
        Measure(
            "rows_with_context",
            "context populated, which the publisher describes as unused",
            source("context").isNotNull(),
        ),
        Measure(
            "rows_where_falls_within_differs",
            "falls_within differs from reported_by, which the publisher expects to "
            "start happening",
            ~source("falls_within").eqNullSafe(source("reported_by")),
        ),
    )


def assert_source_columns(raw_df: DataFrame) -> DataFrame:
    """Fail unless the frame carries the published columns and the member path.

    Metadata only, so this runs before anything is read and costs no scan.
    """
    actual = set(raw_df.columns)
    expected = set(FRAME_COLUMNS)
    if actual != expected:
        raise ValueError(
            "Police source columns do not match the expected set. "
            f"missing={sorted(expected - actual)} "
            f"unexpected={sorted(actual - expected)}"
        )
    return raw_df


def check_rules(raw_df: DataFrame, snapshot: str, samples: int = 5) -> CheckResult:
    """Evaluate every rule, measure, and vocabulary in one pass over the frame.

    A clean frame costs one scan. A frame that breaks a rule costs one more short
    read per failing rule, each stopping at `samples` rows, which is affordable
    because the load is about to abort anyway.

    Every failing rule is reported together. Guards that raise one at a time send the
    caller round the extraction loop once per fault.

    Raises:
        ValueError: naming each rule that fired, its constraint, how many rows broke
            it, and a sample of those rows.
    """
    assert_source_columns(raw_df)
    assert_snapshot_label(snapshot)

    rules = [build(snapshot) for build in RULES]
    counters = measures(snapshot)

    row = raw_df.agg(
        F.count(F.lit(1)).alias("_rows"),
        *[F.count_if(rule.violation).alias(f"rule_{rule.name}") for rule in rules],
        *[F.count_if(item.expression).alias(f"measure_{item.name}") for item in counters],
        *[
            F.collect_set(source(column)).alias(f"vocabulary_{column}")
            for column in VOCABULARY_COLUMNS
        ],
    ).collect()[0]

    result = CheckResult(
        rows=row["_rows"],
        violations={rule.name: row[f"rule_{rule.name}"] for rule in rules},
        measures={item.name: row[f"measure_{item.name}"] for item in counters},
        vocabularies={
            column: sorted(row[f"vocabulary_{column}"])
            for column in VOCABULARY_COLUMNS
        },
    )

    failing = [rule for rule in rules if result.violations[rule.name]]
    if not failing:
        return result

    report = []
    for rule in failing:
        offenders = (
            raw_df.filter(rule.violation)
            .select(*rule.evidence)
            .limit(samples)
            .collect()
        )
        report.append(
            f"  {rule.name}: {result.violations[rule.name]:,} row(s) break "
            f"'{rule.constraint}'. Sample: {[item.asDict() for item in offenders]}"
        )
    raise ValueError(
        f"Police archive {snapshot} broke {len(failing)} of {len(rules)} rules over "
        f"{result.rows:,} rows:\n" + "\n".join(report)
    )


# --------------------------------------------------------------------------- #
# Shaping
# --------------------------------------------------------------------------- #


def null_unplaceable_coordinates(df: DataFrame) -> DataFrame:
    """Null coordinates the publisher could not place, and coordinates outside the UK.

    coalesce makes the test null-safe, so a row with no coordinates keeps its nulls
    whatever order this runs in. A longitude of zero alone is the Greenwich meridian
    and a real UK location: only the pair is the sentinel.
    """
    longitude, latitude = F.col("longitude"), F.col("latitude")
    unplaceable = ((longitude == UNLOCATED) & (latitude == UNLOCATED)) | ~(
        latitude.between(*LATITUDE_RANGE) & longitude.between(*LONGITUDE_RANGE)
    )
    drop = F.coalesce(unplaceable, F.lit(False))
    return df.select(
        [
            F.when(~drop, F.col(name)).alias(name)
            if name in COORDINATE_COLUMNS
            else F.col(name)
            for name in df.columns
        ]
    )


def shape_police(
    raw_df: DataFrame,
    source_file: str,
    snapshot: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """Project a validated frame into the Silver shape. Runs no checks and reads
    nothing: call check_rules first, or call transform_police, which does both."""
    typed_df = cast_columns(raw_df, snapshot)
    return (
        null_unplaceable_coordinates(typed_df)
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_ts", F.lit(ingestion_ts).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )


def transform_police(
    raw_df: DataFrame,
    source_file: str,
    snapshot: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """One archive's staged street CSVs, read as all-string, to the Silver frame.

    Args:
        raw_df: the staged members read with header on and inferSchema off, carrying
            the member path.
        source_file: bronze path of the archive read, recorded as lineage. The staged
            copies are intermediates and are not what Silver records; the member a row
            came from is recoverable from crime_month and force.
        snapshot: the archive's month, as yyyy-MM. Recorded as snapshot_month, which
            is the vintage the outcome state was observed at.
        ingestion_ts: load timestamp, recorded as lineage. Passed in rather than
            generated here so the transform stays deterministic under test.

    Returns:
        One row per published incident record, with the columns named in
        SILVER_COLUMNS.

    Note:
        The validation pass returns counts and vocabularies a caller usually wants to
        report. This signature matches the other five sources and discards them; the
        notebook calls check_rules and shape_police separately to keep them without
        reading the frame twice.

        No uniqueness is asserted. The source has no natural key, and duplicates are
        measured by identical_row_duplicates and crime_id_month_spread rather than
        removed.
    """
    check_rules(raw_df, snapshot)
    return shape_police(raw_df, source_file, snapshot, ingestion_ts)


# --------------------------------------------------------------------------- #
# Duplicate measurement
# --------------------------------------------------------------------------- #


def identical_row_duplicates(df: DataFrame) -> DataFrame:
    """Rows that repeat across every published column, with how many times.

    Month truncation and coordinate snapping make two genuine incidents on one street
    in one month identical, and the publisher separately suspects some forces of
    double counting anti-social behaviour. The two are indistinguishable from the row,
    so this counts them rather than removing them.

    snapshot_month is excluded from the comparison. Including it would let the same
    record counted twice pass as two vintages.
    """
    published = [name for name in TYPED_COLUMNS if name != "snapshot_month"]
    return (
        df.groupBy(*published)
        .count()
        .filter(F.col("count") > 1)
        .orderBy(F.col("count").desc())
    )


def crime_id_month_spread(df: DataFrame) -> DataFrame:
    """Crime ids appearing under more than one month.

    A crime sits in the month it was recorded and is restated in place, so a handful
    of months means a force moved a crime's date. A span of years means the id is not
    a per-crime reference at all: Northern Ireland reuses a pool of them monthly
    across the whole series.
    """
    return (
        df.filter(F.col("crime_id").isNotNull())
        .groupBy("crime_id")
        .agg(
            F.countDistinct("crime_month").alias("months"),
            F.min("crime_month").alias("first_month"),
            F.max("crime_month").alias("last_month"),
            F.countDistinct("force").alias("forces"),
            F.count("*").alias("rows"),
        )
        .filter(F.col("months") > 1)
    )
