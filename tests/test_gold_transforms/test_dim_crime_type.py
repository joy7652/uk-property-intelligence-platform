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
    CRIME_TYPES,
    ERA_FIRST_MONTH,
    GOLD_COLUMNS,
    MEASURED_COLUMNS,
    CrimeTypeEntry,
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
