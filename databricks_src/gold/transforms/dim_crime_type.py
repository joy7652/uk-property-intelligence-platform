"""Gold dim_crime_type: crime types with their era boundaries and split lineage.

Grain: one row per crime type.

Two columns are measured and two are authored. first_published_month,
last_published_month and is_current come from the crime table. vocabulary_era and
predecessor_crime_type come from the literal below, because the split lineage is not
recoverable from the data: nothing in a crime row says shoplifting was carved out of
Other crime.

Era is derivable from the measured first month and predecessor is not, so both are
authored together and the era is then checked against what the source publishes. A
mixed approach lets them disagree without anything failing, and the table's CHECK
constraint only catches an era 1 type carrying a predecessor.

The map is closed in both directions. A published type outside it aborts the load,
since attributing it to an era would be a guess. A mapped type the source has stopped
publishing also aborts: the crime table holds the full series from 2010-12, so a type
cannot legitimately disappear from it.

Both vocabulary changes split an existing type into new ones that sum back to it, so
an all-types total stays comparable across the whole series while an individual type
series does not. A predecessor that itself ceased means its successor set is complete;
one that continued means the successors are a subset. That difference is readable from
the predecessor's own last_published_month and needs no column here, but the ordering
it implies is checked: a predecessor that ceased must close before its successors
open, or the split double counts across the boundary.

Anti-social behaviour is flagged rather than filtered. The flag documents why the type
is absent from fact_lsoa_month_crime; the exclusion happens in the fact load.

No lineage columns, unlike Silver. Which run produced a Gold table is recorded in
uk_property_intel.quality.pipeline_run rather than on every row.

No table DDL here either. The Gold contract is declared once in
databricks_src/gold/notebooks/00_create_gold_tables.py, and a generator in this module
would be a second copy of it.

No I/O here. The read and the Delta write live in
databricks_src/gold/notebooks/01_load_dimensions.py.
"""

from __future__ import annotations

from datetime import date
from typing import NamedTuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

ERA_DDL = "tinyint"

ANTI_SOCIAL_BEHAVIOUR = "Anti-social behaviour"


class CrimeTypeEntry(NamedTuple):
    """Era a type entered the vocabulary, and the type it was split out of."""

    vocabulary_era: int
    predecessor: str | None


# Era, the month it opened, the type its members were split out of, and those members.
Vocabulary = tuple[tuple[int, date, str | None, tuple[str, ...]], ...]

# The authored vocabulary, one entry per era and predecessor group. Measured against
# the crime table in phase 3.1: era 1 is the original publication, era 2 splits Other
# crime, and era 3 splits three separate types on one date.
#
# Kept in this shape rather than flattened to a type-keyed dict, because the grouping
# is what makes the lineage reviewable. The flat maps below are derived from it, so
# they cannot drift.
_VOCABULARY: Vocabulary = (
    (
        1,
        date(2010, 12, 1),
        None,
        (
            "Anti-social behaviour",
            "Burglary",
            "Other crime",
            "Robbery",
            "Vehicle crime",
            "Violent crime",
        ),
    ),
    (
        2,
        date(2011, 9, 1),
        "Other crime",
        (
            "Criminal damage and arson",
            "Drugs",
            "Other theft",
            "Public disorder and weapons",
            "Shoplifting",
        ),
    ),
    (3, date(2013, 5, 1), "Violent crime", ("Violence and sexual offences",)),
    (
        3,
        date(2013, 5, 1),
        "Public disorder and weapons",
        ("Public order", "Possession of weapons"),
    ),
    (
        3,
        date(2013, 5, 1),
        "Other theft",
        ("Bicycle theft", "Theft from the person"),
    ),
)

def era_months(vocabulary: Vocabulary) -> dict[int, date]:
    """Era to the month it opened. A repeated era keeps its last declared month."""
    return {era: first for era, first, _, _ in vocabulary}


def crime_types(vocabulary: Vocabulary) -> dict[str, CrimeTypeEntry]:
    """Crime type to its era and predecessor. A repeated type keeps its last entry."""
    return {
        name: CrimeTypeEntry(era, predecessor)
        for era, _, predecessor, names in vocabulary
        for name in names
    }


ERA_FIRST_MONTH: dict[int, date] = era_months(_VOCABULARY)

CRIME_TYPES: dict[str, CrimeTypeEntry] = crime_types(_VOCABULARY)

GOLD_COLUMNS: tuple[str, ...] = (
    "crime_type",
    "first_published_month",
    "last_published_month",
    "is_current",
    "vocabulary_era",
    "predecessor_crime_type",
    "is_anti_social_behaviour",
)

KEY_COLUMNS: tuple[str, ...] = ("crime_type",)

# Columns read from uk_property_intel.silver.police_street_crime.
SOURCE_COLUMNS: tuple[str, ...] = ("crime_type", "crime_month")

# What measure_publication_window returns and the transform reads.
MEASURED_COLUMNS: tuple[str, ...] = (
    "crime_type",
    "first_published_month",
    "last_published_month",
)


def assert_map_consistent(vocabulary: Vocabulary = _VOCABULARY) -> None:
    """Fail on an internally broken vocabulary map.

    Runs at import, so a bad edit stops the module from loading rather than aborting a
    load that has already scanned 96 million rows. The predecessor check is the one
    that matters most: the self-reference on this column carries no foreign key, so a
    typo in a predecessor name is caught nowhere else.
    """
    types = crime_types(vocabulary)

    declared = sum(len(names) for _, _, _, names in vocabulary)
    if declared != len(types):
        counted: dict[str, int] = {}
        for _, _, _, names in vocabulary:
            for name in names:
                counted[name] = counted.get(name, 0) + 1
        raise ValueError(
            "dim_crime_type map lists a type more than once, so one entry silently "
            f"replaced another: {sorted(name for name, n in counted.items() if n > 1)}"
        )

    eras: dict[int, date] = {}
    for era, first_month, _, _ in vocabulary:
        if eras.setdefault(era, first_month) != first_month:
            raise ValueError(
                f"dim_crime_type era {era} is declared with two different first "
                f"months: {eras[era]} and {first_month}."
            )

    for name, entry in types.items():
        if entry.predecessor is None:
            if entry.vocabulary_era != 1:
                raise ValueError(
                    f"dim_crime_type '{name}' entered at era {entry.vocabulary_era} "
                    "with no predecessor, so what it was split out of is unrecorded."
                )
            continue
        if entry.vocabulary_era == 1:
            raise ValueError(
                f"dim_crime_type '{name}' is era 1 and carries the predecessor "
                f"'{entry.predecessor}', but era 1 predates both splits."
            )
        if entry.predecessor == name:
            raise ValueError(f"dim_crime_type '{name}' is its own predecessor.")
        if entry.predecessor not in types:
            raise ValueError(
                f"dim_crime_type '{name}' names the predecessor "
                f"'{entry.predecessor}', which is not a type in the map."
            )
        parent_era = types[entry.predecessor].vocabulary_era
        if parent_era >= entry.vocabulary_era:
            raise ValueError(
                f"dim_crime_type '{name}' at era {entry.vocabulary_era} is split out "
                f"of '{entry.predecessor}' at era {parent_era}, so the predecessor "
                "does not predate it."
            )


assert_map_consistent()


def _mapped_to(values: dict[str, object], data_type: str) -> Column:
    """Chained when over a crime-type keyed map, cast to the target type.

    The cast is not optional. A branch holding None is untyped, so a column whose
    every branch is null resolves to void and fails the insert.
    """
    expr: Column | None = None
    for crime_type, value in values.items():
        condition = F.col("crime_type") == F.lit(crime_type)
        expr = (
            F.when(condition, F.lit(value))
            if expr is None
            else expr.when(condition, F.lit(value))
        )
    return expr.cast(data_type)


def authored_era() -> Column:
    """Vocabulary era for a crime type, null for a type outside the map."""
    return _mapped_to(
        {name: entry.vocabulary_era for name, entry in CRIME_TYPES.items()}, ERA_DDL
    )


def authored_predecessor() -> Column:
    """Type a crime type was split out of, null where it entered at era 1."""
    return _mapped_to(
        {name: entry.predecessor for name, entry in CRIME_TYPES.items()}, "string"
    )


def era_first_month() -> Column:
    """First month of the era a row is attributed to."""
    expr: Column | None = None
    for era, first_month in ERA_FIRST_MONTH.items():
        condition = F.col("vocabulary_era") == F.lit(era)
        expr = (
            F.when(condition, F.lit(first_month))
            if expr is None
            else expr.when(condition, F.lit(first_month))
        )
    return expr.cast("date")


def assert_source_columns(crime_df: DataFrame) -> DataFrame:
    """Fail unless the crime frame carries the columns this module reads.

    One direction only. Gold reads a projection of a table it does not own, so a
    missing column is a fault and an extra one is not.
    """
    missing = sorted(set(SOURCE_COLUMNS) - set(crime_df.columns))
    if missing:
        raise ValueError(
            f"dim_crime_type source is missing columns it reads: {missing}"
        )
    return crime_df


def assert_measured_columns(measured: DataFrame) -> DataFrame:
    """Fail unless the frame is the one measure_publication_window returns."""
    missing = sorted(set(MEASURED_COLUMNS) - set(measured.columns))
    if missing:
        raise ValueError(
            f"dim_crime_type input is missing columns it reads: {missing}"
        )
    return measured


def assert_types_known(measured: DataFrame) -> DataFrame:
    """Fail unless the published types are exactly the authored ones.

    Both directions. A new type cannot be attributed to an era without guessing, and a
    type that has left the source is equally a change, since the crime table holds the
    whole series and nothing published in 2010 can stop having been published.
    """
    published = {row["crime_type"] for row in measured.select("crime_type").collect()}
    mapped = set(CRIME_TYPES)
    if published != mapped:
        raise ValueError(
            "dim_crime_type published types do not match the authored map. "
            f"missing={sorted(mapped - published)} "
            f"unexpected={sorted(published - mapped)}"
        )
    return measured


def assert_eras_match_first_month(attributed: DataFrame) -> DataFrame:
    """Fail where a type first appears in a month other than its era's.

    The authored era claims when a type entered the vocabulary. The source records
    when it first appears. A disagreement means one of them is wrong, and neither is
    safe to prefer silently.
    """
    offenders = (
        attributed.filter(F.col("first_published_month") != era_first_month())
        .select("crime_type", "vocabulary_era", "first_published_month")
        .withColumn("era_starts", era_first_month())
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_crime_type authored eras disagree with the months the source "
            f"first publishes: {[row.asDict() for row in offenders]}"
        )
    return attributed


def assert_ceased_predecessors_close_first(attributed: DataFrame) -> DataFrame:
    """Fail where a predecessor that stopped publishing overlaps what it split into.

    A predecessor still being published splits into a partial successor set and the
    two run alongside each other, which is how Other crime and Other theft behave. One
    that ceased handed its whole volume over, so its last month must fall before its
    successors' first. An overlap means the split double counts across the boundary,
    and every reconstruction reading predecessor_crime_type inherits the error.

    Joining on the shared column name rather than aliasing the frame twice keeps the
    predicate unambiguous. The join is inner because assert_types_known and
    assert_map_consistent between them have already proved every predecessor is a
    published type, so nothing can be dropped here.
    """
    successors = attributed.select(
        "crime_type", "first_published_month", "predecessor_crime_type"
    ).filter(F.col("predecessor_crime_type").isNotNull())

    predecessors = attributed.select(
        F.col("crime_type").alias("predecessor_crime_type"),
        F.col("last_published_month").alias("predecessor_last_month"),
        F.col("is_current").alias("predecessor_is_current"),
    )

    offenders = (
        successors.join(predecessors, "predecessor_crime_type", "inner")
        .filter(~F.col("predecessor_is_current"))
        .filter(F.col("predecessor_last_month") >= F.col("first_published_month"))
        .select(
            "crime_type",
            "first_published_month",
            "predecessor_crime_type",
            "predecessor_last_month",
        )
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_crime_type ceased predecessors overlap the types split out of them, "
            f"so the split double counts: {[row.asDict() for row in offenders]}"
        )
    return attributed


def measure_publication_window(crime_df: DataFrame) -> DataFrame:
    """One row per published crime type, with the months it first and last appears.

    Args:
        crime_df: uk_property_intel.silver.police_street_crime, or a projection of it
            carrying crime_type and crime_month.

    Returns:
        crime_type, first_published_month and last_published_month.

    Note:
        Separate from the transform because this is the expensive half. It shuffles
        the whole crime table, while everything downstream of it works on sixteen
        rows, and the transform runs three actions over its input before the write
        runs a fourth. The caller persists this frame so that shuffle happens once.
    """
    assert_source_columns(crime_df)
    return crime_df.groupBy("crime_type").agg(
        F.min("crime_month").alias("first_published_month"),
        F.max("crime_month").alias("last_published_month"),
    )


def transform_dim_crime_type(measured: DataFrame) -> DataFrame:
    """The measured publication window to the Gold crime type dimension.

    Args:
        measured: output of measure_publication_window.

    Returns:
        One row per published crime type, with the columns named in GOLD_COLUMNS.
    """
    assert_measured_columns(measured)
    assert_types_known(measured)

    # Unpartitioned by necessity: the newest month is a property of the release rather
    # than of any one type, and there are sixteen rows to scan.
    whole_release = Window.partitionBy()
    attributed = (
        measured.withColumn("vocabulary_era", authored_era())
        .withColumn("predecessor_crime_type", authored_predecessor())
        .withColumn(
            "is_current",
            F.col("last_published_month")
            == F.max("last_published_month").over(whole_release),
        )
        .withColumn(
            "is_anti_social_behaviour",
            F.col("crime_type") == F.lit(ANTI_SOCIAL_BEHAVIOUR),
        )
    )
    assert_eras_match_first_month(attributed)
    assert_ceased_predecessors_close_first(attributed)
    return attributed.select(*GOLD_COLUMNS)
