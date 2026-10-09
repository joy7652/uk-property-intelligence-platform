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

Conformance is derived rather than authored. A predecessor that ceased handed its
whole volume to its successors, so it and they are one category under two or three
names whose combined series runs the length of the table. One that is still
publishing kept its label and lost contents, which no grouping repairs. That
difference is is_current on the predecessor's own row, already measured here, so
conformance_status is a function of the lineage and the measured window rather than a
second copy of either. Only the name a group reports under is authored, because a
split into two leaves no surviving name to resolve to.

The derivation is safe against a release that drops a type from its newest month only
because assert_ceased_predecessors_close_first runs before it. A predecessor that
appears to have ceased while its successors opened years earlier is an overlap, and
the load stops rather than quietly reclassifying the row as conformed.

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

# What happened to a type's label over the series. The table constrains the same three.
# A boolean would be two short: it collapses a category that can be reported across a
# break with one that cannot, which is the distinction these columns exist to carry.
STABLE = "stable"
CONFORMED = "conformed"
COMPOSITION_CHANGED = "composition_changed"
CONFORMANCE_STATUSES: tuple[str, ...] = (STABLE, CONFORMED, COMPOSITION_CHANGED)

# What a conformed group reports under, keyed on the ceased predecessor that roots it.
# Authored because it cannot be derived: a one-into-one rename leaves a surviving name
# and a one-into-two split leaves none, so the second has to be given one. Phase 5.4.1
# measured both groups at the composite and neither loses volume across its break.
GROUP_LABEL: dict[str, str] = {
    "Violent crime": "Violence and sexual offences",
    "Public disorder and weapons": "Public order and weapons",
}

# Join scaffolding, dropped before the projection. Named rather than written inline
# because three functions read them and a fourth creates them.
PREDECESSOR_CEASED_COLUMN = "_predecessor_ceased"
GROUP_ROOT_COLUMN = "_is_group_root"
IN_GROUP_COLUMN = "_in_group"
COMPOSITION_COLUMN = "_changed_composition"
STATUS_FLAG_COLUMNS: tuple[str, ...] = (
    GROUP_ROOT_COLUMN,
    IN_GROUP_COLUMN,
    COMPOSITION_COLUMN,
)


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
            ANTI_SOCIAL_BEHAVIOUR,
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

# Types something else was split out of. Authored, in the sense that it is read off the
# map rather than off the data, so it cannot disagree with predecessor_crime_type.
PREDECESSORS: frozenset[str] = frozenset(
    entry.predecessor for entry in CRIME_TYPES.values() if entry.predecessor is not None
)

GOLD_COLUMNS: tuple[str, ...] = (
    "crime_type",
    "first_published_month",
    "last_published_month",
    "is_current",
    "vocabulary_era",
    "predecessor_crime_type",
    "is_anti_social_behaviour",
    "conformance_status",
    "reporting_crime_type",
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


def assert_groups_labelled(
    vocabulary: Vocabulary = _VOCABULARY,
    labels: dict[str, str] | None = None,
) -> None:
    """Fail on a group label that names something no group can be rooted at.

    Separate from assert_map_consistent, which every broken-vocabulary test calls with
    a two-entry map these labels do not appear in. Only half the pairing is checkable
    here: which types can root a group is authored, and which of them actually ceased
    is measured, so the other half is assert_every_group_is_labelled on the frame.
    """
    labels = GROUP_LABEL if labels is None else labels
    types = crime_types(vocabulary)

    unknown = sorted(set(labels) - set(types))
    if unknown:
        raise ValueError(
            f"dim_crime_type labels a group rooted at {unknown}, which is not a type "
            "in the map."
        )

    parents = {
        entry.predecessor for entry in types.values() if entry.predecessor is not None
    }
    childless = sorted(set(labels) - parents)
    if childless:
        raise ValueError(
            f"dim_crime_type labels a group rooted at {childless}, which nothing was "
            "split out of, so there is no group for the label to name."
        )


assert_groups_labelled()


def _mapped_to(
    values: dict[str, object], data_type: str, key: str = "crime_type"
) -> Column:
    """Chained when over a map keyed on one column, cast to the target type.

    The cast is not optional. A branch holding None is untyped, so a column whose
    every branch is null resolves to void and fails the insert. An empty map is the
    same case with no branches at all, which is why it returns a typed null rather
    than falling off the end.
    """
    expr: Column | None = None
    for mapped_from, value in values.items():
        condition = F.col(key) == F.lit(mapped_from)
        expr = (
            F.when(condition, F.lit(value))
            if expr is None
            else expr.when(condition, F.lit(value))
        )
    if expr is None:
        return F.lit(None).cast(data_type)
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


def is_a_predecessor() -> Column:
    """True for a type something else was split out of, read off the authored map."""
    if not PREDECESSORS:
        return F.lit(False)
    return F.col("crime_type").isin(*sorted(PREDECESSORS))


def group_label(key: str) -> Column:
    """The name the group rooted at the type in `key` reports under."""
    return _mapped_to(dict(GROUP_LABEL), "string", key=key)


def attach_conformance(attributed: DataFrame) -> DataFrame:
    """Add conformance_status and reporting_crime_type, plus the flags they rest on.

    Three conditions, each a column so the guards below read the same values the
    output does rather than recomputing them:

    - the row ceased and something was split out of it, so it roots a group
    - the row's own predecessor ceased, so the row sits inside that group
    - the row is still published and something was split out of it, so its label
      survived and its contents did not

    Whether a predecessor ceased is on that predecessor's row, so this joins the frame
    to itself on predecessor_crime_type, as assert_ceased_predecessors_close_first
    does. Sixteen rows either side.
    """
    parents = attributed.select(
        F.col("crime_type").alias("predecessor_crime_type"),
        (~F.col("is_current")).alias(PREDECESSOR_CEASED_COLUMN),
    )

    flagged = (
        attributed.join(parents, "predecessor_crime_type", "left")
        .withColumn(GROUP_ROOT_COLUMN, (~F.col("is_current")) & is_a_predecessor())
        .withColumn(
            IN_GROUP_COLUMN,
            F.coalesce(F.col(PREDECESSOR_CEASED_COLUMN), F.lit(False)),
        )
        .withColumn(COMPOSITION_COLUMN, F.col("is_current") & is_a_predecessor())
    )

    return flagged.withColumn(
        "conformance_status",
        F.when(
            F.col(GROUP_ROOT_COLUMN) | F.col(IN_GROUP_COLUMN), F.lit(CONFORMED)
        )
        .when(F.col(COMPOSITION_COLUMN), F.lit(COMPOSITION_CHANGED))
        .otherwise(F.lit(STABLE)),
    ).withColumn(
        "reporting_crime_type",
        F.when(F.col(GROUP_ROOT_COLUMN), group_label("crime_type"))
        .when(F.col(IN_GROUP_COLUMN), group_label("predecessor_crime_type"))
        .otherwise(F.col("crime_type")),
    )


def assert_status_is_unambiguous(flagged: DataFrame) -> DataFrame:
    """Fail where a type satisfies more than one of the three conditions.

    They are exclusive on the measured vocabulary: a type that ceased handed its whole
    volume over and so cannot also have changed composition, and nothing carved out of
    a type that ceased is itself the parent of a later split. A map that produced one
    would take its status from the order of the branches above rather than from what
    the row means, and the wrong one would be indistinguishable from the right one.
    """
    satisfied = sum(
        (F.col(name).cast("int") for name in STATUS_FLAG_COLUMNS),
        F.lit(0),
    )
    offenders = (
        flagged.filter(satisfied > F.lit(1))
        .select("crime_type", "is_current", "predecessor_crime_type", *STATUS_FLAG_COLUMNS)
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_crime_type types satisfy more than one conformance condition, so the "
            f"status would come from branch order: {[row.asDict() for row in offenders]}"
        )
    return flagged


def assert_every_group_is_labelled(shaped: DataFrame) -> DataFrame:
    """Fail where a group has no authored label.

    Which types have ceased is measured, so a release that retires one GROUP_LABEL
    does not cover leaves its whole group reporting under null, into a NOT NULL
    column. assert_groups_labelled cannot see this: it knows which types could root a
    group and not which of them have stopped publishing.
    """
    offenders = (
        shaped.filter(F.col("reporting_crime_type").isNull())
        .select("crime_type", "predecessor_crime_type", "last_published_month")
        .limit(5)
        .collect()
    )
    if offenders:
        raise ValueError(
            "dim_crime_type groups have no authored label, so the types in them would "
            f"report under null: {[row.asDict() for row in offenders]}. Add the group "
            "root to GROUP_LABEL."
        )
    return shaped


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
        rows, and the transform runs five actions over its input before the write
        runs a sixth. The caller persists this frame so that shuffle happens once.
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
    # Before the derivation, not merely before the write. A predecessor that looks
    # ceased because one release dropped it from its newest month would otherwise be
    # read as a complete split and its successors grouped under it.
    assert_ceased_predecessors_close_first(attributed)

    shaped = attach_conformance(attributed)
    assert_status_is_unambiguous(shaped)
    assert_every_group_is_labelled(shaped)
    return shaped.select(*GOLD_COLUMNS)
