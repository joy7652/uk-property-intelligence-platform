"""Tests for the threshold rule evaluator and the rule_result table.

Most of this is pure Python: the registry, the generated DDL, the verdict, and the
values the evaluator refuses. Every Delta write is excluded and is verified on the
cluster, not here.

The exception is the verdict constraint. It is SQL that has to agree with a verdict
Python computed, and the two are written separately, so a handful of tests evaluate
the constraint through the `spark` fixture against rows the evaluator produced. That
is the only place the two declarations can be shown to match.
"""

from __future__ import annotations

import math

import pytest

from databricks_src.quality.rules.evaluator import (
    BOUND_COLUMNS,
    KINDS,
    RULE_COLUMNS,
    RULE_CONSTRAINTS,
    RULES,
    assert_registry_consistent,
    assert_rules_reported,
    bounds_present_check,
    evaluate,
    failures,
    rule_frame_schema,
    rule_table_ddl,
    verdict_check,
)

SPARK_TO_DDL = {"StringType": "STRING", "DoubleType": "DOUBLE", "BooleanType": "BOOLEAN"}

A_RULE = "ppd_hpi_count_correlation"
A_SCOPED_RULE = "ppd_hpi_count_ratio_by_year"


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #


def test_registry_is_consistent():
    """Runs at import too. Kept as a test so a bad edit fails in CI rather than only
    on the cluster, where it would abort a check that had already read three tables."""
    assert_registry_consistent()


def test_every_rule_carries_a_known_kind():
    assert {rule.kind for rule in RULES.values()} <= set(KINDS)


def test_every_rule_carries_a_note():
    assert all(rule.note.strip() for rule in RULES.values())


def test_registry_keys_match_rule_names():
    assert all(name == rule.name for name, rule in RULES.items())


def test_every_rule_carries_at_least_one_bound():
    """A rule with neither bound asserts nothing, and a check that asserts nothing
    belongs in a transform as a guard."""
    assert all(
        rule.lower is not None or rule.upper is not None for rule in RULES.values()
    )


def test_every_bound_is_a_finite_number():
    assert all(
        math.isfinite(bound)
        for rule in RULES.values()
        for bound in (rule.lower, rule.upper)
        if bound is not None
    )


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #


def test_a_value_above_a_floor_passes():
    assert evaluate(A_RULE, 0.9998).passed


def test_a_value_below_a_floor_fails():
    assert not evaluate(A_RULE, 0.80).passed


def test_a_value_exactly_on_the_bound_passes():
    """The bound is the worst acceptable value, not the best rejected one."""
    floor = RULES[A_RULE].lower
    assert evaluate(A_RULE, floor).passed


def test_the_bounds_are_copied_onto_the_result():
    """Read back with the row rather than looked up, so widening a bound later cannot
    reinterpret a result recorded under the old one."""
    result = evaluate(A_RULE, 0.9998)
    assert (result.lower_bound, result.upper_bound) == (
        RULES[A_RULE].lower,
        RULES[A_RULE].upper,
    )


def test_the_scope_is_carried():
    assert evaluate(A_SCOPED_RULE, 1.0094, scope="2024").scope == "2024"


def test_detail_is_truncated():
    from databricks_src.quality.rules.evaluator import MESSAGE_LIMIT

    result = evaluate(A_RULE, 0.80, detail="x" * 9000)
    assert len(result.detail) == MESSAGE_LIMIT


def test_failures_returns_only_breaches():
    results = (
        evaluate(A_SCOPED_RULE, 1.0094, scope="2024"),
        evaluate(A_SCOPED_RULE, 1.5716, scope="2026"),
    )
    assert [result.scope for result in failures(results)] == ["2026"]


# --------------------------------------------------------------------------- #
# Values the evaluator refuses
# --------------------------------------------------------------------------- #


def test_nan_is_rejected():
    """Spark orders NaN above every number, so a NaN observation would satisfy any
    floor and persist as a pass."""
    with pytest.raises(ValueError, match="not a number"):
        evaluate(A_RULE, float("nan"))


@pytest.mark.parametrize("value", [float("inf"), float("-inf")], ids=["inf", "-inf"])
def test_infinity_is_rejected(value):
    with pytest.raises(ValueError, match="not a number"):
        evaluate(A_RULE, value)


def test_bool_is_rejected():
    """bool subclasses int, so a flag would be compared against a threshold as 0 or 1."""
    with pytest.raises(ValueError, match="bool"):
        evaluate(A_RULE, True)


def test_a_non_number_is_rejected():
    with pytest.raises(ValueError, match="not a number"):
        evaluate(A_RULE, "0.9998")


def test_an_unregistered_rule_name_is_rejected():
    """A free-text name is how a rename silently breaks a series."""
    with pytest.raises(ValueError, match="registry"):
        evaluate("count_correlation", 0.9998)


# --------------------------------------------------------------------------- #
# Scope declaration
# --------------------------------------------------------------------------- #


def test_a_scoped_rule_without_a_scope_is_rejected():
    """Several per-year results under one run would be indistinguishable."""
    with pytest.raises(ValueError, match="no scope"):
        evaluate(A_SCOPED_RULE, 1.0094)


def test_an_unscoped_rule_given_a_scope_is_rejected():
    with pytest.raises(ValueError, match="one result per run"):
        evaluate(A_RULE, 0.9998, scope="2024")


# --------------------------------------------------------------------------- #
# The bands against the history they were set from
# --------------------------------------------------------------------------- #

# Every transfer year as measured on the cluster, after the count ratio was corrected
# to exclude cells the publisher has not reported a volume for.
MEASURED = {
    "ppd_hpi_count_ratio_by_year": (0.9728, 1.0641),
    "ppd_hpi_median_ratio_by_year": (0.9524, 1.0587),
}


@pytest.mark.parametrize("rule", sorted(MEASURED))
def test_the_band_admits_the_measured_extremes(rule):
    """A band rejecting a year already observed would fire on the next ordinary one.
    Both extremes are real years: 2014 and 2023 for the count, 2008 and 2020 for the
    median."""
    for observed in MEASURED[rule]:
        assert evaluate(rule, observed, scope="measured").passed


@pytest.mark.parametrize("rule", sorted(MEASURED))
def test_the_band_leaves_room_beyond_the_measured_extremes(rule):
    """A bound sitting on an observed value is one that has already been reached."""
    low, high = MEASURED[rule]
    registered = RULES[rule]
    assert registered.lower < low and registered.upper > high


def test_the_uncorrected_trailing_year_would_have_failed():
    """1.5716 was what the year-by-year cell reported for 2026 before the ratio was
    restricted to cells carrying both counts."""
    assert not evaluate("ppd_hpi_count_ratio_by_year", 1.5716, scope="2026").passed


# --------------------------------------------------------------------------- #
# Completeness
# --------------------------------------------------------------------------- #


def test_every_expected_rule_reporting_passes():
    results = (evaluate(A_RULE, 0.9998),)
    assert assert_rules_reported(results, (A_RULE,)) == results


def test_a_rule_that_did_not_report_is_named():
    """No constraint fires on a row that was never written, so a check that stops
    running looks exactly like one that keeps passing."""
    with pytest.raises(ValueError, match=A_RULE):
        assert_rules_reported((), (A_RULE,))


def test_expecting_an_unregistered_rule_is_rejected():
    with pytest.raises(ValueError, match="not registered"):
        assert_rules_reported((), ("no_such_rule",))


def test_extra_results_are_not_a_failure():
    """A run evaluating more than it promised is not the failure this guards."""
    results = (evaluate(A_RULE, 0.9998), evaluate(A_SCOPED_RULE, 1.0094, scope="2024"))
    assert assert_rules_reported(results, (A_RULE,)) == results


# --------------------------------------------------------------------------- #
# Table definition
# --------------------------------------------------------------------------- #


def test_ddl_matches_the_write_schema():
    """The DDL and the write schema each declare a type. Drift between them loads
    values into a column that truncates or nulls them, and append matches on name."""
    declared = [
        (line.split()[0], line.split()[1])
        for line in rule_table_ddl().replace(" NOT NULL", "").split(",\n    ")
    ]
    produced = [
        (field.name, SPARK_TO_DDL[type(field.dataType).__name__])
        for field in rule_frame_schema().fields
    ]
    assert declared == produced


def test_ddl_column_order_is_canonical():
    """INSERT matches on position where the column list is generated from the same
    tuple, so a reordering here would load values into the wrong columns."""
    assert tuple(
        line.split()[0] for line in rule_table_ddl().split(",\n    ")
    ) == RULE_COLUMNS


def test_nullability_matches_between_ddl_and_schema():
    not_null_in_ddl = {
        line.split()[0] for line in rule_table_ddl().split(",\n    ") if "NOT NULL" in line
    }
    not_null_in_schema = {
        field.name for field in rule_frame_schema().fields if not field.nullable
    }
    assert not_null_in_ddl == not_null_in_schema


def test_the_bound_check_covers_every_bound_column():
    """A third bound added to the writer without the constraint following it would
    let a row carry a verdict against nothing."""
    assert all(name in bounds_present_check() for name in BOUND_COLUMNS)


def test_constraint_names_are_distinct():
    names = [name for name, _ in RULE_CONSTRAINTS]
    assert len(names) == len(set(names))


# --------------------------------------------------------------------------- #
# The constraint against the evaluator
# --------------------------------------------------------------------------- #

def test_the_frame_carries_the_columns_in_order(spark):
    from databricks_src.quality.rules.evaluator import rule_frame

    results = (evaluate(A_RULE, 0.9998), evaluate(A_SCOPED_RULE, 1.0094, scope="2024"))
    frame = rule_frame(spark, "run-1", results)
    assert tuple(frame.columns) == RULE_COLUMNS
    assert frame.count() == 2


def test_the_frame_declares_its_types_when_empty(spark):
    """An inferred schema on no rows has no types, and append matches on both."""
    from databricks_src.quality.rules.evaluator import rule_frame

    frame = rule_frame(spark, "run-1", ())
    assert frame.count() == 0
    assert dict(frame.dtypes)["observed"] == "double"
    assert dict(frame.dtypes)["passed"] == "boolean"


def test_the_frame_carries_the_bounds_and_verdict(spark):
    from databricks_src.quality.rules.evaluator import rule_frame

    row = rule_frame(spark, "run-1", (evaluate(A_SCOPED_RULE, 1.5716, scope="2026"),)).collect()[0]
    assert (row["rule"], row["scope"], row["passed"]) == (A_SCOPED_RULE, "2026", False)
    assert (row["lower_bound"], row["upper_bound"]) == (0.95, 1.08)


CONSTRAINT_SCHEMA = (
    "observed double, lower_bound double, upper_bound double, passed boolean"
)


def accepted(spark, rows):
    frame = spark.createDataFrame(rows, CONSTRAINT_SCHEMA)
    return [
        row[0]
        for row in frame.selectExpr(f"({verdict_check()}) AS ok").collect()
    ]


@pytest.mark.parametrize("observed", [0.9998, 0.99, 0.80, 0.0])
def test_the_constraint_accepts_what_the_evaluator_produces(spark, observed):
    """The verdict is computed in Python and constrained in SQL, and the two are
    written separately. This is the only place they can be shown to agree."""
    result = evaluate(A_RULE, observed)
    assert accepted(
        spark,
        [(result.observed, result.lower_bound, result.upper_bound, result.passed)],
    ) == [True]


def test_the_constraint_rejects_a_mislabelled_breach(spark):
    """A writer bug recording a pass its own numbers contradict never reaches Delta."""
    assert accepted(spark, [(0.80, 0.99, None, True)]) == [False]


def test_the_constraint_rejects_a_mislabelled_pass(spark):
    assert accepted(spark, [(0.9998, 0.99, None, False)]) == [False]


@pytest.mark.parametrize("passed", [True, False], ids=["as_pass", "as_fail"])
def test_the_constraint_rejects_nan_under_either_verdict(spark, passed):
    """Spark orders NaN above every number, so without the isnan guard a NaN
    observation satisfies the floor and persists as a pass."""
    assert accepted(spark, [(float("nan"), 0.99, None, passed)]) == [False]


def test_a_band_accepts_a_value_inside_it(spark):
    assert accepted(spark, [(1.011, 0.95, 1.05, True)]) == [True]


def test_a_band_rejects_a_verdict_that_ignores_the_ceiling(spark):
    assert accepted(spark, [(1.30, 0.95, 1.05, True)]) == [False]
