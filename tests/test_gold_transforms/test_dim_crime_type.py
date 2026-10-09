"""Tests for the Gold crime type dimension.

Split in two. The vocabulary map is pure Python and is checked without a session, so a
bad edit to the literal fails on import in CI. Everything below it needs a session,
because the measured columns come from an aggregate over the crime frame.

The synthetic crime frame is generated from the map itself rather than written out, so
adding a type to the literal extends the happy-path tests with it instead of leaving
them describing an older vocabulary.
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql.types import DateType, StringType, StructField, StructType

from databricks_src.gold.transforms.dim_crime_type import (
    ANTI_SOCIAL_BEHAVIOUR,
    COMPOSITION_CHANGED,
    CONFORMANCE_STATUSES,
    CONFORMED,
    CRIME_TYPES,
    ERA_FIRST_MONTH,
    GOLD_COLUMNS,
    GROUP_LABEL,
    MEASURED_COLUMNS,
    PREDECESSOR_CEASED_COLUMN,
    STABLE,
    STATUS_FLAG_COLUMNS,
    CrimeTypeEntry,
    assert_groups_labelled,
    assert_map_consistent,
    crime_types,
    era_months,
    measure_publication_window,
    transform_dim_crime_type,
)

# Types the source stopped publishing when era 3 split them. Both end the month before
# era 3 opens.
CEASED: dict[str, date] = {
    "Violent crime": date(2013, 4, 1),
    "Public disorder and weapons": date(2013, 4, 1),
}

LATEST_MONTH = date(2026, 6, 1)

# Recorded in the phase 3.1 exploration notebook as crime_distinct_types.
MEASURED_TYPE_COUNT = 16

# The conformance answer for all sixteen rows, written out rather than derived from the
# map. The derivation is what is under test, and this is the figure phase 5.4 is owed.
COMPOSITION_CHANGED_TYPES = {"Other crime", "Other theft"}
CONFORMED_TYPES = {
    "Violent crime",
    "Violence and sexual offences",
    "Public disorder and weapons",
    "Public order",
    "Possession of weapons",
}
STABLE_TYPE_COUNT = 9

# What each conformed group reports under, and which rows sit in it.
REPORTING_GROUPS: dict[str, set[str]] = {
    "Violence and sexual offences": {"Violent crime", "Violence and sexual offences"},
    "Public order and weapons": {
        "Public disorder and weapons",
        "Public order",
        "Possession of weapons",
    },
}

CRIME_SCHEMA = StructType(
    [
        StructField("crime_type", StringType(), nullable=False),
        StructField("crime_month", DateType(), nullable=False),
    ]
)


def crime_rows(
    latest: date = LATEST_MONTH,
    ceased: dict[str, date] | None = None,
    first_month: dict[str, date] | None = None,
    drop: set[str] | None = None,
    extra: list[tuple[str, date]] | None = None,
) -> list[tuple[str, date]]:
    """Two rows per type: the month its era opened, and the month it was last seen."""
    ceased = CEASED if ceased is None else ceased
    first_month = first_month or {}
    drop = drop or set()
    rows = [
        (name, month)
        for name, entry in CRIME_TYPES.items()
        if name not in drop
        for month in (
            first_month.get(name, ERA_FIRST_MONTH[entry.vocabulary_era]),
            ceased.get(name, latest),
        )
    ]
    return rows + (extra or [])


def dimension_from(source):
    """The dimension built from a crime frame the caller has already shaped.

    Both stages, because the guards under test sit either side of the measure.
    """
    return transform_dim_crime_type(measure_publication_window(source))


def dimension(spark, **kwargs):
    """The dimension built from a synthetic crime frame."""
    return dimension_from(spark.createDataFrame(crime_rows(**kwargs), CRIME_SCHEMA))


def loaded(spark, **kwargs):
    """The dimension keyed on crime type."""
    return {row["crime_type"]: row for row in dimension(spark, **kwargs).collect()}


# --------------------------------------------------------------------------- #
# The vocabulary map
# --------------------------------------------------------------------------- #


def test_map_is_consistent():
    """Runs at import too. Kept as a test so a bad edit fails in CI rather than only
    on the cluster, where it would abort a load that had already scanned the source."""
    assert_map_consistent()


def test_type_count_matches_the_measured_vocabulary():
    """The map claims to cover what the source publishes. 3.1 counted 16 distinct
    types across the whole series."""
    assert len(CRIME_TYPES) == MEASURED_TYPE_COUNT


def test_every_predecessor_is_itself_a_mapped_type():
    """The column has no foreign key, so nothing but this catches a typo."""
    named = {
        entry.predecessor
        for entry in CRIME_TYPES.values()
        if entry.predecessor is not None
    }
    assert named <= set(CRIME_TYPES)


def test_only_era_one_types_have_no_predecessor():
    for name, entry in CRIME_TYPES.items():
        assert (entry.predecessor is None) == (entry.vocabulary_era == 1), name


def test_every_predecessor_predates_its_successor():
    for name, entry in CRIME_TYPES.items():
        if entry.predecessor is None:
            continue
        parent = CRIME_TYPES[entry.predecessor]
        assert parent.vocabulary_era < entry.vocabulary_era, name


def test_eras_open_in_order():
    months = [ERA_FIRST_MONTH[era] for era in sorted(ERA_FIRST_MONTH)]
    assert months == sorted(months)


def test_group_labels_are_consistent():
    """Runs at import too, for the same reason assert_map_consistent is tested here."""
    assert_groups_labelled()


def test_conformance_vocabulary_matches_the_table():
    """Constrained in the DDL, so a value here the table rejects aborts the write
    rather than this module."""
    assert set(CONFORMANCE_STATUSES) == {"stable", "conformed", "composition_changed"}


def test_the_fixture_ceases_exactly_the_types_the_labels_root():
    """What every conformance test below rests on. A label rooted at a type that never
    ceases is never exercised, and a ceased type with no label aborts them all."""
    assert set(CEASED) == set(GROUP_LABEL)


def test_derived_maps_cover_the_literal():
    vocabulary = ((1, date(2010, 12, 1), None, ("A", "B")),)
    assert crime_types(vocabulary) == {
        "A": CrimeTypeEntry(1, None),
        "B": CrimeTypeEntry(1, None),
    }
    assert era_months(vocabulary) == {1: date(2010, 12, 1)}


# --------------------------------------------------------------------------- #
# Broken vocabularies
# --------------------------------------------------------------------------- #


def test_repeated_type_is_rejected():
    vocabulary = (
        (1, date(2010, 12, 1), None, ("Burglary",)),
        (2, date(2011, 9, 1), "Burglary", ("Burglary",)),
    )
    with pytest.raises(ValueError, match="more than once"):
        assert_map_consistent(vocabulary)


def test_era_with_two_first_months_is_rejected():
    vocabulary = (
        (1, date(2010, 12, 1), None, ("Burglary",)),
        (1, date(2011, 1, 1), None, ("Robbery",)),
    )
    with pytest.raises(ValueError, match="two different first months"):
        assert_map_consistent(vocabulary)


def test_unmapped_predecessor_is_rejected():
    vocabulary = (
        (1, date(2010, 12, 1), None, ("Burglary",)),
        (2, date(2011, 9, 1), "Other crime", ("Shoplifting",)),
    )
    with pytest.raises(ValueError, match="not a type in the map"):
        assert_map_consistent(vocabulary)


def test_self_predecessor_is_rejected():
    vocabulary = (
        (1, date(2010, 12, 1), None, ("Other crime",)),
        (2, date(2011, 9, 1), "Shoplifting", ("Shoplifting",)),
    )
    with pytest.raises(ValueError, match="its own predecessor"):
        assert_map_consistent(vocabulary)


def test_era_one_with_a_predecessor_is_rejected():
    vocabulary = (
        (1, date(2010, 12, 1), None, ("Other crime",)),
        (1, date(2010, 12, 1), "Other crime", ("Shoplifting",)),
    )
    with pytest.raises(ValueError, match="predates both splits"):
        assert_map_consistent(vocabulary)


def test_later_type_with_no_predecessor_is_rejected():
    vocabulary = (
        (1, date(2010, 12, 1), None, ("Other crime",)),
        (2, date(2011, 9, 1), None, ("Shoplifting",)),
    )
    with pytest.raises(ValueError, match="with no predecessor"):
        assert_map_consistent(vocabulary)


def test_predecessor_newer_than_its_successor_is_rejected():
    vocabulary = (
        (1, date(2010, 12, 1), None, ("Other crime",)),
        (2, date(2011, 9, 1), "Bicycle theft", ("Shoplifting",)),
        (3, date(2013, 5, 1), "Other crime", ("Bicycle theft",)),
    )
    with pytest.raises(ValueError, match="does not predate it"):
        assert_map_consistent(vocabulary)


def test_a_label_for_an_unmapped_type_is_rejected():
    with pytest.raises(ValueError, match="not a type in the map"):
        assert_groups_labelled(labels={"Cyber fraud": "Fraud"})


def test_a_label_for_a_type_nothing_was_split_out_of_is_rejected():
    with pytest.raises(ValueError, match="nothing was split out of"):
        assert_groups_labelled(labels={"Burglary": "Burglary and theft"})


# --------------------------------------------------------------------------- #
# The dimension
# --------------------------------------------------------------------------- #


def test_column_order_matches_the_target(spark):
    assert tuple(dimension(spark).columns) == GOLD_COLUMNS


def test_publication_window_is_measured_before_the_transform(spark):
    """The expensive half is separate so the caller can persist it. Its own contract
    is checked here, since the transform reads it rather than the crime frame."""
    source = spark.createDataFrame(crime_rows(), CRIME_SCHEMA)
    measured = measure_publication_window(source)
    assert tuple(measured.columns) == MEASURED_COLUMNS
    assert measured.count() == len(CRIME_TYPES)


def test_one_row_per_published_type(spark):
    assert len(loaded(spark)) == len(CRIME_TYPES)


def test_publication_window_comes_from_the_source(spark):
    rows = loaded(spark)
    for name, entry in CRIME_TYPES.items():
        assert rows[name]["first_published_month"] == ERA_FIRST_MONTH[
            entry.vocabulary_era
        ], name
        assert rows[name]["last_published_month"] == CEASED.get(name, LATEST_MONTH), name


def test_is_current_marks_only_types_reaching_the_latest_month(spark):
    rows = loaded(spark)
    for name in CRIME_TYPES:
        assert rows[name]["is_current"] == (name not in CEASED), name


def test_era_and_predecessor_come_from_the_map(spark):
    rows = loaded(spark)
    for name, entry in CRIME_TYPES.items():
        assert rows[name]["vocabulary_era"] == entry.vocabulary_era, name
        assert rows[name]["predecessor_crime_type"] == entry.predecessor, name


def test_anti_social_behaviour_is_flagged_and_nothing_else_is(spark):
    rows = loaded(spark)
    flagged = {name for name, row in rows.items() if row["is_anti_social_behaviour"]}
    assert flagged == {ANTI_SOCIAL_BEHAVIOUR}


def test_anti_social_behaviour_is_present_in_the_dimension(spark):
    """The type is excluded from the crime fact, not from the dimension. Dropping it
    here would leave the fact's exclusion undocumented."""
    assert ANTI_SOCIAL_BEHAVIOUR in loaded(spark)


def test_ceased_predecessors_end_before_their_successors_begin(spark):
    """A predecessor that stopped means its successor set is complete. This is the
    property a cross-era reconstruction reads, so it is asserted rather than assumed."""
    rows = loaded(spark)
    for name in CEASED:
        successors = [
            other
            for other, entry in CRIME_TYPES.items()
            if entry.predecessor == name
        ]
        assert successors, name
        for successor in successors:
            assert rows[name]["last_published_month"] < rows[successor][
                "first_published_month"
            ], successor


# --------------------------------------------------------------------------- #
# Conformance
# --------------------------------------------------------------------------- #


def test_every_row_carries_the_status_its_lineage_implies(spark):
    rows = loaded(spark)
    for name, row in rows.items():
        if name in COMPOSITION_CHANGED_TYPES:
            expected = COMPOSITION_CHANGED
        elif name in CONFORMED_TYPES:
            expected = CONFORMED
        else:
            expected = STABLE
        assert row["conformance_status"] == expected, name


def test_a_predecessor_that_still_publishes_changed_composition(spark):
    """Its label survived both breaks and its contents did not, which is the case no
    grouping repairs. Measured at the composite in 5.4.1: the two together lose 58
    percent of their volume across 2011-09, against a total that does not move."""
    rows = loaded(spark)
    changed = {n for n, r in rows.items() if r["conformance_status"] == COMPOSITION_CHANGED}
    assert changed == COMPOSITION_CHANGED_TYPES
    for name in changed:
        assert rows[name]["is_current"], name


def test_a_predecessor_that_ceased_and_its_successors_are_conformed(spark):
    """A predecessor that stopped handed its whole volume over, so it and its
    successors are one category under several names."""
    rows = loaded(spark)
    conformed = {n for n, r in rows.items() if r["conformance_status"] == CONFORMED}
    assert conformed == CONFORMED_TYPES


def test_the_rest_are_stable(spark):
    rows = loaded(spark)
    stable = {n for n, r in rows.items() if r["conformance_status"] == STABLE}
    assert len(stable) == STABLE_TYPE_COUNT
    assert stable == set(CRIME_TYPES) - CONFORMED_TYPES - COMPOSITION_CHANGED_TYPES


def test_a_conformed_group_reports_under_one_name(spark):
    rows = loaded(spark)
    for label, members in REPORTING_GROUPS.items():
        under = {n for n, r in rows.items() if r["reporting_crime_type"] == label}
        assert under == members, label


def test_only_a_conformed_row_reports_under_another_name(spark):
    """The table's own constraint, asserted on the frame."""
    for name, row in loaded(spark).items():
        if row["reporting_crime_type"] != name:
            assert row["conformance_status"] == CONFORMED, name


def test_neither_conformance_column_is_null(spark):
    """Both are NOT NULL, and a null would reach DISTINCTCOUNT as a blank member."""
    for name, row in loaded(spark).items():
        assert row["conformance_status"] is not None, name
        assert row["reporting_crime_type"] is not None, name


def test_the_join_scaffolding_does_not_reach_the_output(spark):
    columns = set(dimension(spark).columns)
    assert not columns & {PREDECESSOR_CEASED_COLUMN, *STATUS_FLAG_COLUMNS}


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #


def test_missing_source_column_aborts(spark):
    source = spark.createDataFrame(crime_rows(), CRIME_SCHEMA).drop("crime_month")
    with pytest.raises(ValueError, match="missing columns it reads"):
        measure_publication_window(source)


def test_transform_rejects_a_frame_that_is_not_the_measured_one(spark):
    source = spark.createDataFrame(crime_rows(), CRIME_SCHEMA)
    with pytest.raises(ValueError, match="missing columns it reads"):
        transform_dim_crime_type(source)


def test_unmapped_published_type_aborts(spark):
    source = spark.createDataFrame(
        crime_rows(extra=[("Cyber fraud", date(2026, 1, 1))]), CRIME_SCHEMA
    )
    with pytest.raises(ValueError, match="unexpected=\\['Cyber fraud'\\]"):
        dimension_from(source)


def test_mapped_type_absent_from_the_source_aborts(spark):
    """The crime table holds the whole series, so a type published in 2010 cannot stop
    having been published. Its absence is a source change, not an empty period."""
    source = spark.createDataFrame(crime_rows(drop={"Shoplifting"}), CRIME_SCHEMA)
    with pytest.raises(ValueError, match="missing=\\['Shoplifting'\\]"):
        dimension_from(source)


def test_era_disagreeing_with_the_first_published_month_aborts(spark):
    source = spark.createDataFrame(
        crime_rows(first_month={"Shoplifting": date(2012, 3, 1)}), CRIME_SCHEMA
    )
    with pytest.raises(ValueError, match="authored eras disagree"):
        dimension_from(source)


def test_ceased_predecessor_overlapping_its_successor_aborts(spark):
    source = spark.createDataFrame(
        crime_rows(ceased={**CEASED, "Violent crime": date(2013, 6, 1)}), CRIME_SCHEMA
    )
    with pytest.raises(ValueError, match="ceased predecessors overlap"):
        dimension_from(source)


def test_continued_predecessor_may_overlap_its_successors(spark):
    """Other crime still publishes and its successors opened in 2011-09, so the two
    run alongside each other. The guard keys on is_current for exactly this reason: a
    partial split is expected to overlap, a complete one is not."""
    rows = loaded(spark)
    assert rows["Other crime"]["is_current"]
    assert (
        rows["Other crime"]["last_published_month"]
        > rows["Shoplifting"]["first_published_month"]
    )


def test_a_ceased_group_with_no_label_aborts(spark):
    """Which types have ceased is measured, so a release retiring one GROUP_LABEL does
    not cover puts its whole group under null, into a NOT NULL column."""
    source = spark.createDataFrame(
        crime_rows(ceased={**CEASED, "Other theft": date(2013, 4, 1)}), CRIME_SCHEMA
    )
    with pytest.raises(ValueError, match="no authored label"):
        dimension_from(source)


def test_a_type_that_both_ceased_and_kept_splitting_aborts(spark):
    """Other crime ceasing in 2011-08 leaves Other theft carved out of something that
    stopped and still the parent of a 2013 split. Two conditions, and the branch order
    would pick one of them silently."""
    source = spark.createDataFrame(
        crime_rows(ceased={**CEASED, "Other crime": date(2011, 8, 1)}), CRIME_SCHEMA
    )
    with pytest.raises(ValueError, match="more than one conformance condition"):
        dimension_from(source)


def test_a_stale_cease_aborts_before_the_derivation_reads_it(spark):
    """The derivation reads is_current, which one thin release could move. The overlap
    guard runs first for exactly that reason: Other crime absent from the newest month
    alone is not a complete split, and its successors opened in 2011."""
    source = spark.createDataFrame(
        crime_rows(ceased={**CEASED, "Other crime": date(2026, 5, 1)}), CRIME_SCHEMA
    )
    with pytest.raises(ValueError, match="ceased predecessors overlap"):
        dimension_from(source)
