"""Threshold rules and the table their results land in.

A rule is a measured number, a bound it has to sit inside, and the verdict. That is
the whole shape. Checks with no bound live in the transforms as guards, because a
column that vanished or a key that repeated is wrong at any tolerance and there is
nothing to configure. The population here is the opposite case: numbers that are
never exactly anything, where the question is whether they have moved too far.

Bounds live on the Rule rather than at the call site. A caller passing its own bound
can pass the wrong one and produce a row that is internally consistent and false, and
nothing downstream could tell. The same reasoning keeps metric names in a registry in
the audit writer.

Bounds are also written into each row rather than looked up when the row is read. A
bound that is widened later would otherwise reinterpret every historical result, and
a run that passed under one bound and would fail under another is exactly what a
reader needs to see.

Results key on pipeline_run.run_id, so a rule evaluated inside a load and a rule
evaluated by a standalone check are recorded the same way and join to the same run
table.

window_step lives beside the rules it feeds because it is how one rule's observed
value is obtained, and a measurement defined away from the bound it is compared
against is free to drift from it. It is pure Python, so it is tested without a
session and runs in CI on any dialect.

No I/O here. The frame is built and written by the notebook that opened the run.
"""

from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from typing import NamedTuple

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from databricks_src.quality.audit.writer import MESSAGE_LIMIT

CATALOG = "uk_property_intel"
SCHEMA = "quality"

RULE_TABLE = f"{CATALOG}.{SCHEMA}.rule_result"

RULE_COLUMNS: tuple[str, ...] = (
    "run_id",
    "rule",
    "scope",
    "observed",
    "lower_bound",
    "upper_bound",
    "passed",
    "detail",
)

BOUND_COLUMNS: tuple[str, ...] = ("lower_bound", "upper_bound")

_DOUBLE_COLUMNS: frozenset[str] = frozenset({"observed", *BOUND_COLUMNS})

_BOOLEAN_COLUMNS: frozenset[str] = frozenset({"passed"})

# Bounds are deliberately absent: a rule with only a floor leaves upper_bound null,
# and the pairing is a constraint rather than a nullability rule.
_RULE_NOT_NULL: frozenset[str] = frozenset({"run_id", "rule", "observed", "passed"})


# What each kind means, carried here rather than in a comment so a reader of the
# recorded data can join to it.
KINDS: dict[str, str] = {
    "reconciliation": (
        "two independent products of one source compared against each other"
    ),
    "coverage": "the share of a population something reaches",
    "distribution": "where a value sits against the spread of its own history",
}


@dataclass(frozen=True)
class Rule:
    """One threshold check.

    A rule with neither bound would assert nothing, so at least one is required. A
    floor sets lower, a ceiling sets upper, and a band sets both.

    scoped declares whether the rule produces one result per run or one per
    subdivision. A per-year rule recorded without its year is ambiguous, and several
    of them under one run cannot be told apart afterwards.
    """

    name: str
    kind: str
    lower: float | None
    upper: float | None
    scoped: bool
    note: str


# The transaction pipeline and the published index count the same registry sales by
# different methods, so their counts move together or one of them changed. This is the
# only evidence the platform holds that does not come from the platform.
#
# The floor is read off a measured run rather than chosen: phase 3 closed at 0.9998
# over 29.6 million transactions. One observation is thin, so the floor sits well below
# it, and it should tighten once several runs have reported.
_RECONCILIATION = (
    Rule(
        "ppd_hpi_count_correlation",
        "reconciliation",
        0.99,
        None,
        False,
        "correlation between transaction_count and the publisher's sales_volume, "
        "over cells carrying both. A coarse check only: the cells span district to "
        "composite, so the two variables cover four orders of magnitude and the "
        "correlation stays above 0.999 even where districts disagree badly. It "
        "catches a structural break and nothing finer. The per-year ratio is the "
        "sharper check.",
    ),
    Rule(
        "ppd_hpi_count_ratio_by_year",
        "reconciliation",
        0.95,
        1.08,
        True,
        "transactions we counted over sales the publisher reported, per transfer "
        "year, over cells carrying both counts. Measured across 1995 to 2026 it runs "
        "0.9728 to 1.0641 with a mean of 1.0039. The band is wider below than above "
        "because a shortfall is the failure this pipeline can cause and a surplus is "
        "usually the publisher still reporting.",
    ),
    Rule(
        "ppd_hpi_median_ratio_by_year",
        "reconciliation",
        0.93,
        1.08,
        True,
        "the transaction median over the publisher's mix-adjusted average, per "
        "transfer year. The dashboard models the published average as tracking the "
        "median rather than the mean, and this is the relationship that claim rests "
        "on. Measured 0.9524 to 1.0587 with a mean of 1.0122.",
    ),
)

# A published vocabulary change splits an existing category into new ones that sum back
# to it, so a total across every category should cross the break unmoved while any one
# category's series does not. Every cross-era crime figure rests on that, including the
# district total three independent paths agree on at 67,886,868, and until phase 5.4.1
# nothing measured it.
#
# Measured at the England and Wales composite on 08-10-2026, excluding anti-social
# behaviour: the three-month mean moves 0.969 across 2011-09 and 1.054 across 2013-05,
# which is a log ratio of 0.0318 and 0.0525. Those sit at the 57th and 36th percentile of
# the moves the same series makes in other Septembers and Mays, whose largest are 0.0599
# and 0.1591. Comparing a September against other Septembers is what makes the figure
# readable: both breaks fall on seasonal turning points, and against all months rather
# than the same month the seasonal component dominates.
#
# The ceiling is a drift bound and not a seasonal one. Both windows are settled history,
# so the value moves only if a Silver regression loses records in one era or the
# publisher restates. 0.10 absorbs a restatement of a few percent and still catches a
# doubling, and it should tighten toward 0.07 once several runs have reported, as the
# correlation floor above should.
#
# Not registered for the conformed groups, whose own steps run 0.1487 to 0.2475 against
# same-month ceilings of 0.1981 to 0.2711. A rule sitting that close to its bound on the
# first run fails for a reason nobody can act on, and the notebook raises on any breach.
_STABILITY = (
    Rule(
        "crime_total_step_at_vocabulary_change",
        "distribution",
        None,
        0.10,
        True,
        "absolute log ratio of the three-month mean crime count from a published "
        "vocabulary change to the three-month mean before it, at the England and Wales "
        "composite, excluding anti-social behaviour. Scope is the month the change took "
        "effect. A split moves offences between labels and leaves the total alone, so a "
        "step here says the eras are not comparable and every series crossing them "
        "inherits it. Measured 0.0318 at 2011-09 and 0.0525 at 2013-05 against "
        "same-month maxima of 0.0599 and 0.1591.",
    ),
)

_ALL: tuple[Rule, ...] = _RECONCILIATION + _STABILITY

RULES: dict[str, Rule] = {rule.name: rule for rule in _ALL}


def assert_registry_consistent() -> None:
    """Fail on a registry that cannot be read back correctly.

    Runs at import, so a bad edit fails in CI rather than part-way through a check
    that has already read three tables.
    """
    if len(RULES) != len(_ALL):
        counted: dict[str, int] = {}
        for rule in _ALL:
            counted[rule.name] = counted.get(rule.name, 0) + 1
        raise ValueError(
            "rules: rule names are defined more than once, so two series would "
            f"collapse into one: {sorted(n for n, c in counted.items() if c > 1)}"
        )

    unknown = sorted({rule.name for rule in _ALL if rule.kind not in KINDS})
    if unknown:
        raise ValueError(f"rules: rules carry a kind outside {list(KINDS)}: {unknown}")

    unbounded = sorted(
        rule.name for rule in _ALL if rule.lower is None and rule.upper is None
    )
    if unbounded:
        raise ValueError(
            f"rules: rules with neither bound assert nothing: {unbounded}. A check "
            "with no threshold belongs in the transform as a guard."
        )

    inverted = sorted(
        rule.name
        for rule in _ALL
        if rule.lower is not None and rule.upper is not None and rule.lower > rule.upper
    )
    if inverted:
        raise ValueError(f"rules: rules whose floor sits above their ceiling: {inverted}")

    unreal = sorted(
        rule.name
        for rule in _ALL
        for bound in (rule.lower, rule.upper)
        if bound is not None and not math.isfinite(bound)
    )
    if unreal:
        raise ValueError(f"rules: bounds that are not finite numbers: {unreal}")

    noteless = sorted(rule.name for rule in _ALL if not rule.note.strip())
    if noteless:
        raise ValueError(f"rules: rules with no note: {noteless}")


assert_registry_consistent()


# --------------------------------------------------------------------------- #
# Measurement
# --------------------------------------------------------------------------- #

# Periods either side of the point being measured. Three months absorbs one unusual
# month without spanning enough of the year to carry the season with it.
STEP_WINDOW = 3


class WindowStep(NamedTuple):
    """The means either side of a point, and the log ratio between them.

    Both means are carried rather than the ratio alone, so a caller can record what the
    figure was built from without summing the series a second time and arriving at
    something slightly different.
    """

    before: float
    after: float
    log_ratio: float


def window_step(
    series: dict[dt.date, float], at: dt.date, window: int = STEP_WINDOW
) -> WindowStep | None:
    """Compare the `window` periods from `at` against the `window` periods before it.

    A log ratio rather than a difference, so a step is comparable across a series whose
    level moves, and symmetric: a halving and a doubling are the same distance from
    unmoved in opposite directions.

    Args:
        series: period start to value, in any order. Keys are sorted here.
        at: the first period on the later side. It belongs to `after`, not `before`.
        window: periods each side.

    Returns:
        None where the comparison cannot be made rather than a figure standing in for
        one: `at` absent, fewer than `window` periods either side, or a non-positive
        value anywhere in either window, which no logarithm describes. A caller counts
        those and reports them, so a window that stops being measurable is visible.

    Raises:
        ValueError: where window is below one, which compares nothing against nothing.
    """
    if window < 1:
        raise ValueError(f"rules: window_step needs a window of at least 1, got {window}")

    periods = sorted(series)
    if at not in series:
        return None

    index = periods.index(at)
    if index < window or index + window > len(periods):
        return None

    before = [series[period] for period in periods[index - window : index]]
    after = [series[period] for period in periods[index : index + window]]
    if min(before) <= 0 or min(after) <= 0:
        return None

    mean_before = sum(before) / window
    mean_after = sum(after) / window
    return WindowStep(
        before=mean_before,
        after=mean_after,
        log_ratio=math.log(mean_after / mean_before),
    )


# --------------------------------------------------------------------------- #
# Evaluation
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class RuleResult:
    """One evaluated rule, before it reaches Delta."""

    rule: str
    scope: str | None
    observed: float
    lower_bound: float | None
    upper_bound: float | None
    passed: bool
    detail: str | None


def evaluate(
    rule: str,
    observed: float,
    scope: str | None = None,
    detail: str | None = None,
) -> RuleResult:
    """Compare a measured value against its registered bounds.

    NaN is rejected rather than compared. Spark orders NaN above every number, so a
    correlation that came back NaN would satisfy any floor and record as a pass. An
    infinity is rejected for the same reason. Both mean the measurement failed, and a
    failed measurement is not a result.
    """
    if rule not in RULES:
        raise ValueError(
            f"rules: {rule!r} is not in the registry. Add it there rather than "
            "passing a free-text name, or a rename breaks the series silently."
        )
    if isinstance(observed, bool):
        raise ValueError(f"rules: {rule!r} was given a bool, which is not a measurement.")
    if not isinstance(observed, (int, float)):
        raise ValueError(
            f"rules: {rule!r} was given {type(observed).__name__}, which is not a number."
        )
    if not math.isfinite(observed):
        raise ValueError(
            f"rules: {rule!r} measured {observed}, which is not a number. The "
            "population was empty or degenerate, and that is a fault in the check "
            "rather than a result to record."
        )

    registered = RULES[rule]
    if registered.scoped and scope is None:
        raise ValueError(
            f"rules: {rule!r} is scoped and was given no scope. Several results "
            "under one run would be indistinguishable afterwards."
        )
    if not registered.scoped and scope is not None:
        raise ValueError(
            f"rules: {rule!r} produces one result per run and was given scope "
            f"{scope!r}. Declare it scoped in the registry if that changed."
        )
    value = float(observed)
    passed = (registered.lower is None or value >= registered.lower) and (
        registered.upper is None or value <= registered.upper
    )
    return RuleResult(
        rule=rule,
        scope=scope,
        observed=value,
        lower_bound=registered.lower,
        upper_bound=registered.upper,
        passed=passed,
        detail=None if detail is None else detail[:MESSAGE_LIMIT],
    )


def assert_rules_reported(
    results: tuple[RuleResult, ...], expected: tuple[str, ...]
) -> tuple[RuleResult, ...]:
    """Fail unless every rule that should have run did.

    No table constraint can fire on a row that was never written, so a check that
    quietly stops running looks exactly like one that keeps passing. The caller names
    what it intended to evaluate and this compares that against what it produced.
    """
    unknown = sorted(set(expected) - set(RULES))
    if unknown:
        raise ValueError(f"rules: expected rules that are not registered: {unknown}")

    missing = sorted(set(expected) - {result.rule for result in results})
    if missing:
        raise ValueError(
            f"rules: {len(missing)} rule(s) did not report: {missing}. A rule that "
            "stops running is indistinguishable from one that keeps passing."
        )
    return results


def failures(results: tuple[RuleResult, ...]) -> tuple[RuleResult, ...]:
    """The results that breached their bounds, for a caller deciding whether to raise."""
    return tuple(result for result in results if not result.passed)


def rule_frame(session, run_id: str, results: tuple[RuleResult, ...]):
    """The evaluated rules as a frame, in RULE_COLUMNS order.

    Built here and written by the notebook, as with every module in this layer. The
    schema is declared rather than inferred: an empty result set would otherwise
    produce a frame with no types, and a run that evaluated nothing is a run whose
    completeness check should have caught it first.
    """
    return session.createDataFrame(
        [
            (
                run_id,
                result.rule,
                result.scope,
                result.observed,
                result.lower_bound,
                result.upper_bound,
                result.passed,
                result.detail,
            )
            for result in results
        ],
        rule_frame_schema(),
    )


# --------------------------------------------------------------------------- #
# Table definition
# --------------------------------------------------------------------------- #


def _column_ddl(name: str) -> str:
    if name in _DOUBLE_COLUMNS:
        data_type = "DOUBLE"
    elif name in _BOOLEAN_COLUMNS:
        data_type = "BOOLEAN"
    else:
        data_type = "STRING"
    return f"{name} {data_type}" + ("" if name not in _RULE_NOT_NULL else " NOT NULL")


def rule_table_ddl() -> str:
    """Column definitions for rule_result, generated from the same constants the
    writer projects."""
    return ",\n    ".join(_column_ddl(name) for name in RULE_COLUMNS)


def bounds_present_check() -> str:
    """A row with neither bound records a verdict against nothing.

    Generated from BOUND_COLUMNS so a third bound cannot be added without the
    constraint following it.
    """
    terms = " OR ".join(f"{name} IS NOT NULL" for name in BOUND_COLUMNS)
    return f"({terms})"


def bounds_ordered_check() -> str:
    return "lower_bound IS NULL OR upper_bound IS NULL OR lower_bound <= upper_bound"


def verdict_check() -> str:
    """The verdict agrees with the numbers beside it.

    The isnan guard is not decoration. Spark orders NaN above every number, so
    without it a NaN observation satisfies any floor and persists as a pass.
    """
    return (
        "NOT isnan(observed) AND passed = ("
        "(lower_bound IS NULL OR observed >= lower_bound) AND "
        "(upper_bound IS NULL OR observed <= upper_bound))"
    )


RULE_CONSTRAINTS: tuple[tuple[str, str], ...] = (
    ("has_a_bound", bounds_present_check()),
    ("bounds_ordered", bounds_ordered_check()),
    ("verdict_matches_bounds", verdict_check()),
)


RULE_COMMENT = (
    "One row per rule evaluated per run, keyed on pipeline_run.run_id. Holds the "
    "measured value, the bounds in force when it was measured, and the verdict. "
    "Bounds are written into the row rather than looked up, so widening a bound "
    "later cannot reinterpret a result that was recorded under the old one. A "
    "constraint ties the verdict to the numbers beside it, so a row claiming a pass "
    "its own values contradict cannot be written. scope names a subdivision within "
    "one run and is null for run-level rules. rule names and their bounds are "
    "defined in databricks_src/quality/rules/evaluator.py, which carries what each "
    "one means."
)


def rule_frame_schema() -> StructType:
    """Write schema for the evaluated rules, in RULE_COLUMNS order."""
    return StructType(
        [
            StructField("run_id", StringType(), nullable=False),
            StructField("rule", StringType(), nullable=False),
            StructField("scope", StringType(), nullable=True),
            StructField("observed", DoubleType(), nullable=False),
            StructField("lower_bound", DoubleType(), nullable=True),
            StructField("upper_bound", DoubleType(), nullable=True),
            StructField("passed", BooleanType(), nullable=False),
            StructField("detail", StringType(), nullable=True),
        ]
    )
