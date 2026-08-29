"""Tests for the shared Gold conformance guards.

Every test needs a session: all three functions are joins and aggregates, so there is no
pure-Python half to check separately.

The fixtures are deliberately abstract. These functions are called with whichever pair
of tables the caller has, and building them out of real area codes would suggest the
checks know something about geography. They do not, which is the point of extracting
them.

The parent frame is given a second column throughout, because the failure this module
was written against is passing the wrong frame, and a single-column parent would let a
positional mistake pass.
"""

from __future__ import annotations

import pytest

from databricks_src.gold.transforms.conformance import (
    SAMPLE_ROWS,
    SAMPLE_VALUES,
    assert_column_present,
    assert_columns_present,
    assert_grain_unique,
    assert_keys_conform,
    measure_dimension_coverage,
)

PARENT_SCHEMA = "area_code string, area_name string"
CHILD_SCHEMA = "area_code string, month_start_date string, measure int"
LSOA_SCHEMA = "lsoa_code string, district_code string"

PARENT = [
    ("E06000001", "Hartlepool"),
    ("E06000002", "Middlesbrough"),
    ("E06000003", "Redcar and Cleveland"),
]

CHILD = [
    ("E06000001", "2026-01-01", 10),
    ("E06000001", "2026-02-01", 11),
    ("E06000002", "2026-01-01", 12),
]

CHILD_NAME = "fact_area_month_hpi"
PARENT_NAME = "dim_area"


def parent(spark, rows=None):
    return spark.createDataFrame(PARENT if rows is None else rows, PARENT_SCHEMA)


def child(spark, rows=None):
    return spark.createDataFrame(CHILD if rows is None else rows, CHILD_SCHEMA)


def conform(spark, child_rows=None, parent_rows=None):
    return assert_keys_conform(
        child(spark, child_rows),
        parent(spark, parent_rows),
        child_column="area_code",
        parent_column="area_code",
        child_name=CHILD_NAME,
        parent_name=PARENT_NAME,
    )


# --------------------------------------------------------------------------- #
# Column presence
# --------------------------------------------------------------------------- #


def test_present_column_passes(spark):
    frame = parent(spark)
    assert assert_column_present(frame, "area_code", CHILD_NAME, PARENT_NAME) is frame


def test_missing_column_names_the_column_and_the_frame(spark):
    """Caught before the join. An AnalysisException about an unresolved name says
    nothing about which of two frames was passed wrongly."""
    with pytest.raises(ValueError, match="area_code"):
        assert_column_present(
            parent(spark).drop("area_code"), "area_code", CHILD_NAME, PARENT_NAME
        )


def test_missing_column_message_lists_what_the_frame_carries(spark):
    with pytest.raises(ValueError, match="area_name"):
        assert_column_present(
            parent(spark).drop("area_code"), "area_code", CHILD_NAME, PARENT_NAME
        )


# --------------------------------------------------------------------------- #
# Read lists
# --------------------------------------------------------------------------- #


def test_a_frame_carrying_every_column_passes(spark):
    frame = child(spark)
    assert assert_columns_present(frame, ("area_code", "measure"), CHILD_NAME) is frame


def test_a_missing_column_names_it(spark):
    with pytest.raises(ValueError, match="measure"):
        assert_columns_present(
            child(spark).drop("measure"), ("area_code", "measure"), CHILD_NAME
        )


def test_every_missing_column_is_named_and_sorted(spark):
    """A frame passed from the wrong place is usually missing several. Naming one turns
    that into one run per name, and sorting makes two runs report identically."""
    frame = child(spark).drop("measure", "month_start_date")
    with pytest.raises(ValueError, match=r"\['measure', 'month_start_date'\]"):
        assert_columns_present(
            frame, ("area_code", "measure", "month_start_date"), CHILD_NAME
        )


def test_the_reader_is_named_rather_than_the_shared_module(spark):
    """Both fact families call this through their resolutions. A fact losing a column
    has to say which fact, not which module noticed."""
    with pytest.raises(ValueError, match=CHILD_NAME):
        assert_columns_present(child(spark).drop("measure"), ("measure",), CHILD_NAME)


def test_an_extra_column_is_accepted(spark):
    """One direction only. Silver adding a column is not a Gold table's problem."""
    assert assert_columns_present(child(spark), ("area_code",), CHILD_NAME).count() == len(
        CHILD
    )


def test_requiring_nothing_passes(spark):
    assert assert_columns_present(child(spark), (), CHILD_NAME).count() == len(CHILD)


# --------------------------------------------------------------------------- #
# Key conformance
# --------------------------------------------------------------------------- #


def test_every_key_resolving_passes(spark):
    assert conform(spark).count() == len(CHILD)


def test_the_child_frame_is_returned_unchanged(spark):
    """Called as a statement in sequence, matching how the guards in every other
    module are called."""
    assert tuple(conform(spark).columns) == ("area_code", "month_start_date", "measure")


def test_a_key_with_no_parent_row_aborts(spark):
    rows = CHILD + [("E06000099", "2026-01-01", 13)]
    with pytest.raises(ValueError, match="no row in dim_area"):
        conform(spark, child_rows=rows)


def test_the_offending_value_is_named(spark):
    rows = CHILD + [("E06000099", "2026-01-01", 13)]
    with pytest.raises(ValueError, match="E06000099"):
        conform(spark, child_rows=rows)


def test_the_offender_carries_the_row_count(spark):
    """A count says whether one code is missing from one row or from a whole series,
    which a sample of rows does not."""
    rows = CHILD + [
        ("E06000099", "2026-01-01", 13),
        ("E06000099", "2026-02-01", 14),
    ]
    with pytest.raises(ValueError, match="'rows': 2"):
        conform(spark, child_rows=rows)


def test_a_null_key_aborts(spark):
    """Equality is false against null, so an anti-join returns it. The rent fact
    resolves eight keys by name lookup and a miss leaves exactly this."""
    rows = CHILD + [(None, "2026-01-01", 13)]
    with pytest.raises(ValueError, match="no row in dim_area"):
        conform(spark, child_rows=rows)


def test_a_repeated_parent_key_does_not_multiply_the_report(spark):
    """The parent is distincted before the join. A dimension carrying a duplicate is
    its own fault to report, not a reason for this check to count wrongly."""
    rows = PARENT + [("E06000001", "Hartlepool")]
    assert conform(spark, parent_rows=rows).count() == len(CHILD)


def test_an_empty_child_passes(spark):
    """A fact with no rows is a load that produced nothing, which the row count catches.
    Nothing dangles, so this is not where it should fail."""
    assert conform(spark, child_rows=[]).count() == 0


def test_offenders_are_capped(spark):
    """The list goes into an exception message that also reaches the audit row."""
    rows = CHILD + [
        (f"E060009{index:02d}", "2026-01-01", index)
        for index in range(SAMPLE_VALUES + 5)
    ]
    with pytest.raises(ValueError) as raised:
        conform(spark, child_rows=rows)
    assert str(raised.value).count("'area_code'") == SAMPLE_VALUES


def test_the_total_is_reported_beside_the_capped_list(spark):
    """Without it, ten values shown cannot be told from ten of four hundred. Paid for
    in a second pass that only a failing check runs."""
    rows = CHILD + [
        (f"E060009{index:02d}", "2026-01-01", index)
        for index in range(SAMPLE_VALUES + 5)
    ]
    with pytest.raises(ValueError, match=f"{SAMPLE_VALUES + 5} distinct values"):
        conform(spark, child_rows=rows)


def test_the_parent_missing_its_key_column_aborts(spark):
    with pytest.raises(ValueError, match="area_code"):
        assert_keys_conform(
            child(spark),
            parent(spark).drop("area_code"),
            child_column="area_code",
            parent_column="area_code",
            child_name=CHILD_NAME,
            parent_name=PARENT_NAME,
        )


def test_key_columns_may_differ_in_name(spark):
    """dim_lsoa points district_code at dim_area's area_code, so the two names are not
    assumed equal."""
    lsoa = spark.createDataFrame([("E01000001", "E06000001")], LSOA_SCHEMA)
    assert_keys_conform(
        lsoa,
        parent(spark),
        child_column="district_code",
        parent_column="area_code",
        child_name="dim_lsoa",
        parent_name=PARENT_NAME,
    )


# --------------------------------------------------------------------------- #
# The worked example
# --------------------------------------------------------------------------- #


def lsoa_conform(spark, rows, example_column="lsoa_code"):
    return assert_keys_conform(
        spark.createDataFrame(rows, LSOA_SCHEMA),
        parent(spark),
        child_column="district_code",
        parent_column="area_code",
        child_name="dim_lsoa",
        parent_name=PARENT_NAME,
        example_column=example_column,
    )


def test_an_example_child_row_is_carried(spark):
    """A null or malformed district code says nothing about which small areas produced
    it, and the frame is pre-write inside a failed load and cannot be queried after."""
    rows = [("E01000001", "E06000001"), ("E01000009", None), ("E01000004", None)]
    with pytest.raises(ValueError, match="'example_lsoa_code': 'E01000004'"):
        lsoa_conform(spark, rows)


def test_the_example_is_the_same_one_on_a_rerun(spark):
    """Lowest value per offender. One chosen by whatever order a shuffle returned would
    name a different area each time the same failure was investigated."""
    rows = [("E01000009", None), ("E01000004", None), ("E01000007", None)]
    messages = []
    for _ in range(2):
        with pytest.raises(ValueError) as raised:
            lsoa_conform(spark, rows)
        messages.append(str(raised.value))
    assert messages[0] == messages[1]
    assert "E01000004" in messages[0]


def test_no_example_appears_when_none_is_asked_for(spark):
    """The facts leave it unset: there the failing key is the whole diagnosis, and a
    month alongside it would add a column that explains nothing."""
    rows = CHILD + [("E06000099", "2026-01-01", 13)]
    with pytest.raises(ValueError) as raised:
        conform(spark, child_rows=rows)
    assert "example" not in str(raised.value)


def test_an_unknown_example_column_aborts(spark):
    rows = [("E01000001", None)]
    with pytest.raises(ValueError, match="postcode"):
        lsoa_conform(spark, rows, example_column="postcode")


def test_the_example_column_does_not_change_the_offender_count(spark):
    rows = [("E01000001", None), ("E01000002", None)]
    with pytest.raises(ValueError, match="'rows': 2"):
        lsoa_conform(spark, rows)


# --------------------------------------------------------------------------- #
# Grain
# --------------------------------------------------------------------------- #


def test_a_unique_grain_passes(spark):
    assert assert_grain_unique(
        child(spark), ("area_code", "month_start_date"), CHILD_NAME
    ).count() == len(CHILD)


def test_a_repeated_grain_aborts(spark):
    rows = CHILD + [("E06000001", "2026-01-01", 99)]
    with pytest.raises(ValueError, match="grain broken"):
        assert_grain_unique(
            child(spark, rows), ("area_code", "month_start_date"), CHILD_NAME
        )


def test_the_grain_message_names_the_key_columns(spark):
    rows = CHILD + [("E06000001", "2026-01-01", 99)]
    with pytest.raises(ValueError, match=r"\(area_code, month_start_date\)"):
        assert_grain_unique(
            child(spark, rows), ("area_code", "month_start_date"), CHILD_NAME
        )


def test_the_repeated_key_total_is_reported(spark):
    rows = [
        (f"E060000{index:02d}", "2026-01-01", index)
        for index in range(SAMPLE_ROWS + 3)
    ] * 2
    with pytest.raises(ValueError, match=f"{SAMPLE_ROWS + 3} repeated keys"):
        assert_grain_unique(
            child(spark, rows), ("area_code", "month_start_date"), CHILD_NAME
        )


def test_a_repeat_on_one_key_column_only_is_not_a_duplicate(spark):
    """The grain is the pair. Two months for one area is the table working."""
    assert_grain_unique(child(spark), ("area_code", "month_start_date"), CHILD_NAME)


def test_a_single_column_grain_works(spark):
    rows = PARENT + [("E06000001", "Hartlepool again")]
    with pytest.raises(ValueError, match="grain broken"):
        assert_grain_unique(parent(spark, rows), ("area_code",), PARENT_NAME)


def test_a_missing_key_column_aborts(spark):
    with pytest.raises(ValueError, match="month_start_date"):
        assert_grain_unique(
            child(spark).drop("month_start_date"),
            ("area_code", "month_start_date"),
            CHILD_NAME,
        )


# --------------------------------------------------------------------------- #
# Coverage
# --------------------------------------------------------------------------- #


def test_coverage_counts_parent_rows_the_child_reaches(spark):
    """Two of three areas carry fact rows. The third is ordinary, which is why this
    measures rather than asserts."""
    assert measure_dimension_coverage(
        child(spark), parent(spark), "area_code", "area_code"
    ) == (2, 3)


def test_coverage_counts_an_area_once_however_many_rows_it_has(spark):
    reached, _ = measure_dimension_coverage(
        child(spark), parent(spark), "area_code", "area_code"
    )
    assert reached == 2


def test_an_unmatched_child_key_counts_towards_neither(spark):
    """Which is why the docstring says to call this after the conformance check."""
    rows = CHILD + [("E06000099", "2026-01-01", 13)]
    assert measure_dimension_coverage(
        child(spark, rows), parent(spark), "area_code", "area_code"
    ) == (2, 3)


def test_a_duplicated_parent_row_does_not_inflate_the_base(spark):
    rows = PARENT + [("E06000001", "Hartlepool")]
    assert measure_dimension_coverage(
        child(spark), parent(spark, rows), "area_code", "area_code"
    ) == (2, 3)


def test_an_empty_child_reaches_nothing(spark):
    assert measure_dimension_coverage(
        child(spark, []), parent(spark), "area_code", "area_code"
    ) == (0, 3)
