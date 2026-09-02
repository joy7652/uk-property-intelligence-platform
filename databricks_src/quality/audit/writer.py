"""Pipeline audit: what every notebook run recorded, and whether it finished.

Two tables in the `quality` schema. `pipeline_run` takes one row per notebook
execution whatever the outcome; `pipeline_metric` takes one row per measured value,
keyed on that run.

The run row is inserted at start with status 'started' and updated at the end. A run
killed outright leaves its row at 'started', so a detached notebook or a terminated
cluster stays visible rather than raising the observed success rate by disappearing.

A stage the plan skipped closes as 'skipped' and names what it waits on. That is a
third outcome and not a soft failure: nothing ran and the cause is upstream. It is
recorded rather than recomputed later, because the skip set is a function of the
failures and of the dependency chain in force at the time, and the chain changes when
a table is added.

A skipped row cannot say that a stage was never reached at all, since a task the job
never starts runs no code. 02_post_run_record_state therefore records the plan it
computed as planned_stage metrics under its own run, so a stage planned to run and
carrying no run row is distinguishable from one that ran and waited.

Metrics buffer in the run object and flush once, on succeed or fail. A commit per
metric would cost a Delta transaction for every printed count.

Values are stored as a count and its base, never as a ratio. Counts and bases sum
across runs and ratios do not, so a stored percentage cannot be re-aggregated into a
weekly or quarterly figure.

METRICS is the name registry. Free-text names typed into six notebooks would let a
rename break a series silently, and the reader cannot tell a renamed metric from a
discontinued one. `kind` is carried here rather than on the rows so it joins at read
time and applies to everything already recorded.

A run names what identifies it. Silver names the Bronze source it read; Gold names the
table it built, because a Gold load reads several Silver tables and no one of them
identifies the run. Both vocabularies are closed and each name carries the layer it
belongs to, so a run naming one layer's vocabulary under the other's label fails at
construction rather than landing a row nothing will find.

FRESHNESS_BOUND_DAYS is empty of values by design. Each bound is set from what the
first recorded runs report, not from a publisher's release calendar. Until a source
has a bound, its freshness value is recorded and nothing is asserted.

Unlike the Silver transform modules, this one performs I/O: the write is the point of
it. The table definitions live in databricks_src/setup/.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

CATALOG = "uk_property_intel"
SCHEMA = "quality"

RUN_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_run"
METRIC_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_metric"

STARTED = "started"
SUCCEEDED = "succeeded"
FAILED = "failed"
SKIPPED = "skipped"

STATUSES: tuple[str, ...] = (STARTED, SUCCEEDED, FAILED, SKIPPED)

# What a skipped run carries in error_type. The column is the taxonomy of why a run
# produced no data, and an upstream failure is one of the reasons it can hold.
CASCADE = "failure_cascade"

# Source names, matching the Bronze volume namespace rather than the Silver table
# names, so a run row names where the data came from.
SOURCES: tuple[str, ...] = ("boe", "hpi", "ppd", "doogal", "ons", "police")

# Gold runs name the table they build instead. A dimension load reads four or five
# Silver tables at once, so no single source identifies it, while the target always
# does. One run per Gold table, which is also what makes rows_written a real number
# rather than a total across whatever a notebook happened to write.
GOLD_TABLES: tuple[str, ...] = (
    "dim_date",
    "dim_area",
    "dim_lsoa",
    "dim_crime_type",
    "fact_area_month_hpi",
    "fact_area_month_rent",
    "fact_area_month_price",
    "fact_area_month_transaction_mix",
    "fact_area_month_crime",
    "fact_area_month_crime_total",
    "fact_lsoa_month_crime",
    "fact_lsoa_month_crime_total",
    "fact_lsoa_year_price",
)

# Checks read tables and build none, so they name what they verify. A verification
# that reported under a table's name would be indistinguishable from that table's
# load, and both write metrics.
CHECKS: tuple[str, ...] = ("cross_source_verification",)

# The orchestration step itself, which names no source and builds no table. It records
# what the run is about to do, so a stage the job never reached can be told apart from
# one that ran and waited. Without it the plan would live only in a printed cell.
ORCHESTRATION: tuple[str, ...] = ("pipeline_plan",)

LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")

RUN_NAMES: tuple[str, ...] = SOURCES + GOLD_TABLES + CHECKS + ORCHESTRATION

# Which layer each run name belongs to. The pairing is total: a Bronze source name is
# only ever loaded to Silver and a Gold table name is only ever produced by Gold, so a
# run naming one with the other's layer is a typo rather than a case to allow.
# A source is recorded twice per pipeline run: once when ADF lands its file, once when
# Silver reads it. Those are separate runs with separate outcomes, so a name maps to
# the layers it may appear at rather than to one.
LAYERS_OF: dict[str, tuple[str, ...]] = {
    **{name: ("bronze", "silver") for name in SOURCES},
    **{name: ("gold",) for name in GOLD_TABLES},
    # A check names the layer it reads. Nothing here builds a table, so the layer says
    # where the run acted rather than what it produced.
    **{name: ("gold",) for name in CHECKS},
    # The plan is computed once Bronze has closed and before any Silver task opens.
    **{name: ("bronze",) for name in ORCHESTRATION},
}

# Police error text carries row samples and the transform reports every failing rule
# at once, so the message is bounded rather than assumed short.
MESSAGE_LIMIT = 4000

RUN_COLUMNS: tuple[str, ...] = (
    "run_id",
    "source",
    "layer",
    "started_ts",
    "ended_ts",
    "status",
    "rows_written",
    "ingestion_ts",
    "error_type",
    "error_message",
    # The Databricks job run every task of one pipeline execution shares. Null for a
    # notebook run by hand, which is what makes a stage's skip list this run's
    # failures rather than every failure ever recorded.
    "job_run_id",
)

METRIC_COLUMNS: tuple[str, ...] = (
    "run_id",
    "metric",
    "scope",
    "value_numeric",
    "value_text",
    "value_date",
    "denominator",
)

VALUE_COLUMNS: tuple[str, ...] = ("value_numeric", "value_text", "value_date")

_TIMESTAMP_COLUMNS: frozenset[str] = frozenset(
    {"started_ts", "ended_ts", "ingestion_ts"}
)

_BIGINT_COLUMNS: frozenset[str] = frozenset({"rows_written"})

_DOUBLE_COLUMNS: frozenset[str] = frozenset({"value_numeric", "denominator"})

_DATE_COLUMNS: frozenset[str] = frozenset({"value_date"})

# Columns that must be populated. ended_ts is deliberately absent: its pairing with
# status is a constraint rather than a nullability rule.
_RUN_NOT_NULL: frozenset[str] = frozenset(
    {"run_id", "source", "layer", "started_ts", "status"}
)

_METRIC_NOT_NULL: frozenset[str] = frozenset({"run_id", "metric"})


# --------------------------------------------------------------------------- #
# Metric registry
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Metric:
    """One recordable measurement.

    kind is what a reader needs to interpret the number and cannot infer from it.
    A share sitting at a third of all rows can be the source's shape rather than a
    fault, so the alert is a move; a defect count at a millionth is the opposite.

    Direction is deliberately absent. Whether a rise is good depends on the question
    being asked, and getting it wrong in a chart costs a config change rather than an
    edit to six notebooks.
    """

    name: str
    kind: str
    note: str


# What each kind means, carried here rather than in a comment so a reader of the
# recorded data can join to it.
KINDS: dict[str, str] = {
    "volume": "a count of rows or objects",
    "artefact": "identifies the Bronze file read, independent of what it contains",
    "freshness": "how new the content is; the only kind that can be asserted on",
    "share": "a count carrying a denominator",
    "vocabulary": "a bounded set of observed values",
    "date": "a recorded date that is not a freshness signal",
    "disposition": (
        "what a run decided about something, recorded so the decision survives a "
        "change to the rules that produced it"
    ),
}

# Recorded by every source.
_COMMON = (
    Metric("source_files", "artefact", "Bronze objects read this run"),
    Metric("source_bytes", "artefact", "total size of those objects"),
    Metric("source_rows", "volume", "rows read from Bronze before typing"),
    Metric("silver_rows", "volume", "rows the transform produced"),
    Metric(
        "newest_data_date",
        "freshness",
        "newest date the content carries, whatever column expresses it",
    ),
)

# The daily sheet is pre-filled ahead of its save date to feed a chart, and the
# pre-filled rows carry the rate forward rather than blanking it, so the last row and
# the last published rate are the same day. Measured equal on two runs, and it cannot
# reverse: were the pre-fill ever blanked, the last published rate would become the
# better signal. Only newest_data_date is recorded, with every other source.
_BOE = (
    Metric(
        "newest_rate_event_date",
        "date",
        "last rate change, which holds for months at a time and is not a freshness "
        "signal",
    ),
)

# The index publishes every geography over the period it existed, back to its nation's
# coverage floor at the earliest. A series shorter than that span started late or ended
# early, and both are local government reorganisation rather than a fault, which is why
# this is measured. A hole inside a series has no such cause and the transform aborts on
# it instead.
_HPI = (
    Metric(
        "geographies_with_a_full_series",
        "share",
        "geographies carrying every month from their nation's coverage floor to the "
        "newest month in the release",
    ),
)

# Recorded by the two sources published as a dated release file carrying a monthly
# geography panel. HPI keys its panel on area code and ONS on area name, since the
# eight Northern Irish rental areas carry no GSS code, but the count means the same
# thing in both and belongs in one series.
_RELEASE_PANEL = (
    Metric("vintages_present", "volume", "release files under the volume"),
    Metric("vintage_label", "artefact", "release the filename claims"),
    Metric("geographies", "volume", "distinct geographies in the panel"),
)

_PPD = (
    Metric("transfer_years", "volume", "distinct years, one per yearly file"),
)

_DOOGAL = (
    Metric("live_postcodes", "share", "postcodes with no termination date"),
    Metric("terminated_postcodes", "share", "retained so old transactions resolve"),
)

_ONS = (
    Metric(
        "published_line",
        "artefact",
        "publication statement from the cover sheet, which the filename cannot give",
    ),
    Metric(
        "published_date",
        "artefact",
        "publication date parsed from that statement. The filename records which "
        "release was asked for, so the two disagreeing means the wrong one was served",
    ),
)

# The eight CheckResult measures, plus the two vocabularies and the coverage pair.
# Names match the Measure names in the transform so the two cannot drift.
_POLICE = (
    Metric(
        "archives_read",
        "volume",
        "archives this run extracted and validated. Fewer than the archives "
        "present when the load resumes, and zero when staging was already "
        "complete",
    ),
    Metric("winning_slots", "volume", "(month, force) files selected across archives"),
    Metric("rows_out_of_area", "share", "coordinates outside the UK box, nulled"),
    Metric(
        "rows_at_unlocated_sentinel",
        "share",
        "coordinates at (0, 0), the unplaceable marker, nulled",
    ),
    Metric(
        "rows_without_crime_id",
        "share",
        "anti-social behaviour, which carries no offence reference",
    ),
    Metric("rows_without_coordinates", "share", "no location published"),
    Metric("rows_without_lsoa", "share", "no join to postcode geography"),
    Metric("rows_without_outcome", "share", "no outcome yet, or none published"),
    Metric(
        "rows_with_context",
        "share",
        "context populated, which the publisher describes as unused",
    ),
    Metric(
        "rows_where_falls_within_differs",
        "share",
        "falls_within differs from reported_by",
    ),
    Metric("crime_type_values", "vocabulary", "distinct crime types observed"),
    Metric(
        "last_outcome_category_values",
        "vocabulary",
        "distinct outcome categories, unguarded because the vocabulary moves",
    ),
    Metric(
        "rows_identical_to_another",
        "share",
        "rows beyond the first in a group of identical rows. Dates are truncated to "
        "month and coordinates snapped to shared points, so these are usually "
        "distinct incidents rather than double counting",
    ),
)

# Entity coverage, recorded where the source has a bounded entity set and a time
# dimension. Absence from the newest period is the signal; three police forces have
# stopped filing permanently, so this is measured rather than asserted.
_COVERAGE = (
    Metric(
        "entities_in_newest_period",
        "share",
        "entities filing in the newest period, against the set seen in twelve months",
    ),
    Metric(
        "entities_absent_from_newest_period",
        "vocabulary",
        "entities seen in the last twelve months and absent from the newest",
    ),
)

# Recorded by Gold loads. gold_rows is the counterpart to silver_rows and is the one
# metric every Gold run records. Two more are recorded by every fact, and the rest
# belong to a single dimension each. There is no Gold equivalent of source_rows or
# source_bytes, because a Gold run reads Silver tables whose own runs already recorded
# what they hold; where a fact needs a base for a share it carries it as the
# denominator rather than as a metric of its own.
#
# Breakdowns ride on gold_rows through scope rather than earning their own names:
# dim_area records it once per area level and dim_lsoa once per boundary vintage, so
# a level or a vintage appearing or vanishing is visible without a registry edit.
# dimension_rows_with_facts uses scope the same way, naming the dimension it measured,
# so one entry serves nine facts against four dimensions.
_GOLD = (
    Metric("gold_rows", "volume", "rows the transform produced"),
    Metric(
        "rows_without_a_measure",
        "share",
        "rows the source publishes carrying a key and no value, dropped rather than "
        "loaded, against the rows read. The rent series lags in Northern Ireland and "
        "marks those months unavailable across every measure",
    ),
    Metric(
        "dimension_rows_with_facts",
        "share",
        "rows of the dimension named in scope that this fact reaches, against all of "
        "its rows. Coverage rather than integrity: a dimension row no fact reaches is "
        "ordinary, while a fact key with no dimension row aborts the load",
    ),
    Metric(
        "derived_area_codes",
        "volume",
        "areas carrying a code this project assigned, because the publisher issues "
        "none. The eight Northern Irish rental market areas",
    ),
    Metric(
        "majority_assigned_small_areas",
        "share",
        "small areas straddling two districts, counted under the one holding most of "
        "their postcodes, against all small areas",
    ),
    Metric(
        "small_areas_with_crime",
        "share",
        "small areas the crime source publishes, against all small areas",
    ),
    Metric(
        "small_areas_with_price",
        "share",
        "small areas at least one transaction resolves to, against all small areas",
    ),
)

# Recorded once per stage by the orchestration run, naming the stage in scope. The
# plan is computed at Bronze close, so comparing it against what the stages recorded
# also shows where a run degraded after that point: planned run against an actual skip
# is a Silver stage that failed mid-run.
_ORCHESTRATION = (
    Metric(
        "planned_stage",
        "disposition",
        "what the plan computed at Bronze close says about the stage named in scope, "
        "run or skip. A stage planned to run and carrying no run row is one the job "
        "never reached, which no run row of its own could report",
    ),
)

_ALL: tuple[Metric, ...] = (
    _COMMON + _RELEASE_PANEL + _BOE + _HPI + _PPD + _DOOGAL + _ONS + _POLICE
    + _COVERAGE + _GOLD + _ORCHESTRATION
)

METRICS: dict[str, Metric] = {metric.name: metric for metric in _ALL}

# Days between the newest date the content carries and the run, above which the load
# aborts. Every bound starts unset: it is read off what the first runs report, not
# guessed from a release calendar. A source with no bound records its freshness value
# and asserts nothing.
FRESHNESS_BOUND_DAYS: dict[str, int | None] = {source: None for source in SOURCES}


def assert_registry_consistent() -> None:
    """Fail on a registry that cannot be read back correctly.

    Runs at import. A duplicate name would collapse two series into one, and an
    unknown kind would reach the dashboard with no interpretation attached.
    """
    if len(METRICS) != len(_ALL):
        counted: dict[str, int] = {}
        for metric in _ALL:
            counted[metric.name] = counted.get(metric.name, 0) + 1
        raise ValueError(
            "audit: metric names are defined more than once, so two series would "
            f"collapse into one: {sorted(name for name, n in counted.items() if n > 1)}"
        )

    unknown = sorted({metric.name for metric in _ALL if metric.kind not in KINDS})
    if unknown:
        raise ValueError(
            f"audit: metrics carry a kind outside {list(KINDS)}: {unknown}"
        )

    unbounded = sorted(set(FRESHNESS_BOUND_DAYS) - set(SOURCES))
    if unbounded:
        raise ValueError(
            f"audit: freshness bounds named for unknown sources: {unbounded}"
        )

    if len(RUN_NAMES) != len(set(RUN_NAMES)):
        counted = {}
        for name in RUN_NAMES:
            counted[name] = counted.get(name, 0) + 1
        raise ValueError(
            "audit: a run name is defined in more than one of SOURCES, GOLD_TABLES "
            "and CHECKS and ORCHESTRATION, so its layer would be decided by "
            "declaration order: "
            f"{sorted(name for name, n in counted.items() if n > 1)}"
        )

    unpaired = sorted(set(RUN_NAMES) - set(LAYERS_OF))
    if unpaired:
        raise ValueError(f"audit: run names with no layer paired to them: {unpaired}")

    stray = sorted(
        {
            name
            for name, layers in LAYERS_OF.items()
            for layer in layers
            if layer not in LAYERS
        }
    )
    if stray:
        raise ValueError(
            f"audit: run names paired to a layer outside {list(LAYERS)}: {stray}"
        )

    # A source dropped from bronze would stop being recorded there with nothing to
    # show it, and the skip cascade would read every Bronze fetch as having succeeded.
    misplaced = sorted(
        name for name in SOURCES if LAYERS_OF[name] != ("bronze", "silver")
    )
    if misplaced:
        raise ValueError(
            f"audit: every source records at both bronze and silver, and these do "
            f"not: {misplaced}"
        )

    layerless = sorted(name for name, layers in LAYERS_OF.items() if not layers)
    if layerless:
        raise ValueError(
            f"audit: run names paired to no layer, so no run could open under them: "
            f"{layerless}"
        )


assert_registry_consistent()


# --------------------------------------------------------------------------- #
# Table definitions
# --------------------------------------------------------------------------- #


def _column_ddl(name: str, not_null: frozenset[str]) -> str:
    if name in _TIMESTAMP_COLUMNS:
        data_type = "TIMESTAMP"
    elif name in _BIGINT_COLUMNS:
        data_type = "BIGINT"
    elif name in _DOUBLE_COLUMNS:
        data_type = "DOUBLE"
    elif name in _DATE_COLUMNS:
        data_type = "DATE"
    else:
        data_type = "STRING"
    return f"{name} {data_type}" + ("" if name not in not_null else " NOT NULL")


def run_table_ddl() -> str:
    """Column definitions for pipeline_run, generated from the same constants the
    writer projects."""
    return ",\n    ".join(_column_ddl(name, _RUN_NOT_NULL) for name in RUN_COLUMNS)


def metric_table_ddl() -> str:
    """Column definitions for pipeline_metric."""
    return ",\n    ".join(
        _column_ddl(name, _METRIC_NOT_NULL) for name in METRIC_COLUMNS
    )


def status_check() -> str:
    """CHECK expression for the status domain, generated from the same constants the
    writer sets, so the table cannot reject a status the code produces."""
    values = ", ".join(f"'{value}'" for value in STATUSES)
    return f"status IN ({values})"


def open_run_check() -> str:
    """A run is open exactly while it is unfinished.

    Same shape as the BoE table's current_couples_with_open_interval: the pairing is
    enforced rather than left as a convention two writers have to remember.
    """
    return f"(ended_ts IS NULL) = (status = '{STARTED}')"


def one_value_check() -> str:
    """Exactly one value column populated.

    Generated from VALUE_COLUMNS so a fourth value type cannot be added to the writer
    without the constraint following it.
    """
    terms = " + ".join(
        f"CASE WHEN {name} IS NULL THEN 0 ELSE 1 END" for name in VALUE_COLUMNS
    )
    return f"({terms}) = 1"


def denominator_check() -> str:
    """A base only means something against a count."""
    return "denominator IS NULL OR value_numeric IS NOT NULL"


RUN_COMMENT = (
    "One row per notebook execution against the platform, Silver or Gold. Inserted "
    "at start with status 'started' and updated on completion, so a run killed "
    "outright stays visible as an open row rather than disappearing and raising the "
    "observed success rate. rows_written is populated only where the write "
    "completed; ingestion_ts is the value stamped on the rows the run produced, so "
    "an audit row joins to the data it describes."
)

METRIC_COMMENT = (
    "One row per measured value per run, keyed on pipeline_run.run_id. Values are "
    "stored as a count and its base rather than as a ratio, because counts and bases "
    "re-aggregate across runs and percentages do not. Exactly one value column is "
    "populated per row. scope names a subdivision within one run and is null for "
    "run-level metrics; the police load reports per archive, so one metric name "
    "appears several times under one run_id. metric names are defined in "
    "this module, which carries what each one means."
)


def sql_literal(value: str) -> str:
    """A single-quoted SQL string literal.

    Table comments name status values and column contents, so an apostrophe in one
    would close the literal early and fail the CREATE. Doubling is the SQL escape.
    """
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


RUN_CONSTRAINTS: tuple[tuple[str, str], ...] = (
    ("status_known", status_check()),
    ("open_run_has_no_end", open_run_check()),
    ("rows_written_nonneg", "rows_written IS NULL OR rows_written >= 0"),
)

METRIC_CONSTRAINTS: tuple[tuple[str, str], ...] = (
    ("exactly_one_value", one_value_check()),
    ("denominator_needs_a_count", denominator_check()),
)


# --------------------------------------------------------------------------- #
# Recording
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Recorded:
    """One buffered metric, before it reaches Delta."""

    metric: str
    scope: str | None
    value_numeric: float | None
    value_text: str | None
    value_date: dt.date | None
    denominator: float | None


def _as_row_values(
    metric: str,
    value: object,
    scope: str | None,
    denominator: float | None,
) -> Recorded:
    """Route a Python value to its value column.

    bool is rejected rather than routed. It is a subclass of int, so a flag would
    land in value_numeric as 0 or 1 and read as a count.
    """
    if metric not in METRICS:
        raise ValueError(
            f"audit: {metric!r} is not in the registry. Add it there rather than "
            "passing a free-text name, or a rename breaks the series silently."
        )
    if value is None:
        raise ValueError(
            f"audit: {metric!r} has no value. A metric with nothing to report is not "
            "recorded, so the gap stays visible."
        )
    if isinstance(value, bool):
        raise ValueError(f"audit: {metric!r} is a bool, which has no value column.")

    numeric = text = date = None
    if isinstance(value, dt.datetime):
        date = value.date()
    elif isinstance(value, dt.date):
        date = value
    elif isinstance(value, (int, float, Decimal)):
        numeric = float(value)
    elif isinstance(value, (list, tuple, set, frozenset)):
        text = ", ".join(sorted(str(item) for item in value))
    elif isinstance(value, str):
        text = value
    else:
        raise ValueError(
            f"audit: {metric!r} holds {type(value).__name__}, which has no value column."
        )

    if denominator is not None and numeric is None:
        raise ValueError(
            f"audit: {metric!r} carries a denominator but its value is not a count."
        )

    return Recorded(
        metric=metric,
        scope=scope,
        value_numeric=numeric,
        value_text=text,
        value_date=date,
        denominator=None if denominator is None else float(denominator),
    )


def freshness_lag_days(newest: dt.date, run_ts: dt.datetime) -> int:
    """Days between the newest date the content carries and the run."""
    return (run_ts.date() - newest).days


def freshness_verdict(
    source: str,
    newest: dt.date,
    run_ts: dt.datetime,
    skip: bool = False,
) -> tuple[int, str | None]:
    """The lag, and the failure message if the bound is breached.

    Returns no message where the source has no bound set, which is the state every
    source starts in: the value is recorded and the bound is read off it later.

    A rebuild from a Bronze snapshot kept on purpose is a legitimate run that would
    breach the bound, which is what skip is for.
    """
    lag = freshness_lag_days(newest, run_ts)
    bound = FRESHNESS_BOUND_DAYS.get(source)
    if bound is None or skip:
        return lag, None
    if lag <= bound:
        return lag, None
    return lag, (
        f"{source}: newest data is {newest.isoformat()}, {lag} days before this run, "
        f"against a bound of {bound}. Bronze is holding a stale release, or this is a "
        "deliberate rebuild, in which case set SKIP_FRESHNESS in the notebook."
    )


class AuditRun:
    """One notebook execution's audit record.

    Notebook cells cannot be wrapped in a single context manager, so start, fail, and
    succeed are called explicitly. A run that reaches none of them stays 'started',
    which is the correct reading of a killed cluster.
    """

    def __init__(
        self,
        source: str,
        layer: str,
        ingestion_ts: dt.datetime,
        started_ts: dt.datetime | None = None,
        job_run_id: str | None = None,
    ) -> None:
        if source not in RUN_NAMES:
            raise ValueError(
                f"audit: source {source!r} is not a Bronze source {list(SOURCES)}, a "
                f"Gold table {list(GOLD_TABLES)}, a check {list(CHECKS)}, or an "
                f"orchestration step {list(ORCHESTRATION)}."
            )
        if layer not in LAYERS:
            raise ValueError(f"audit: layer {layer!r} is not one of {list(LAYERS)}.")
        if layer not in LAYERS_OF[source]:
            raise ValueError(
                f"audit: {source!r} runs at {list(LAYERS_OF[source])}, not {layer!r}. "
                "A Bronze run names the source ADF landed, a Silver run names the "
                "source it read, and a Gold run names the table it builds."
            )
        self.run_id = str(uuid.uuid4())
        self.source = source
        self.layer = layer
        self.ingestion_ts = ingestion_ts
        # Defaults to the ingestion timestamp so the audit row and the Silver rows it
        # describes agree on when the run happened.
        self.started_ts = started_ts or ingestion_ts
        self.job_run_id = job_run_id
        self.buffered: list[Recorded] = []
        self.closed = False

    def measure(
        self,
        metric: str,
        value: object,
        scope: str | None = None,
        denominator: float | None = None,
    ) -> object:
        """Buffer a metric and return the value, so a print can wrap the call."""
        self.buffered.append(_as_row_values(metric, value, scope, denominator))
        return value

    def freshness(
        self,
        newest: dt.date,
        scope: str | None = None,
        skip: bool = False,
    ) -> int:
        """Record the newest date the content carries and assert the bound.

        Raises:
            ValueError: where the source has a bound and the content is older than it.
        """
        self.measure("newest_data_date", newest, scope=scope)
        lag, message = freshness_verdict(self.source, newest, self.started_ts, skip)
        if message:
            raise ValueError(message)
        return lag

    @contextmanager
    def step(self):
        """Record a failure and re-raise it.

        Wraps whichever notebook cells can raise. Exception rather than
        BaseException: a cancelled cell was killed rather than failed, and leaving it
        at 'started' is the honest reading of that.
        """
        try:
            yield self
        except Exception as error:
            if not self.closed:
                self.fail(error)
            raise

    def start(self) -> str:
        """Insert the open run row. Call before anything that can fail."""
        session = _session()
        columns = ", ".join(RUN_COLUMNS)
        session.sql(
            f"""
            INSERT INTO {RUN_TABLE} ({columns})
            VALUES (
                :run_id,
                :source,
                :layer,
                cast(:started_ts AS TIMESTAMP),
                cast(NULL AS TIMESTAMP),
                '{STARTED}',
                cast(NULL AS BIGINT),
                cast(:ingestion_ts AS TIMESTAMP),
                cast(NULL AS STRING),
                cast(NULL AS STRING),
                cast(:job_run_id AS STRING)
            )
            """,
            args={
                "run_id": self.run_id,
                "source": self.source,
                "layer": self.layer,
                "started_ts": self.started_ts,
                "ingestion_ts": self.ingestion_ts,
                "job_run_id": self.job_run_id,
            },
        )
        return self.run_id

    def succeed(self, rows_written: int | None = None) -> None:
        """Close the run as succeeded and flush the buffered metrics."""
        self._close(SUCCEEDED, rows_written=rows_written)

    def fail(self, error: BaseException) -> None:
        """Close the run as failed and flush what was measured before it broke.

        Called from an except block. The caller re-raises: this records, it does not
        swallow.
        """
        self._close(
            FAILED,
            error_type=type(error).__name__,
            error_message=str(error)[:MESSAGE_LIMIT],
        )

    def skip(self, blocked_by: Iterable[str]) -> None:
        """Close the run as skipped, naming the failures it waits on.

        Not a failure and not a success. Nothing ran and the cause is upstream, so
        rows_written stays null and the reason goes where every other reason for
        producing no data goes.

        Raises:
            ValueError: where nothing is named. A stage skips because something else
                failed, so an empty cause means the plan and the caller disagree
                about why this stage is not running.
        """
        waiting = sorted(blocked_by)
        if not waiting:
            raise ValueError(
                f"audit: {self.source!r} was skipped with nothing named as the cause. "
                "A skip is always downstream of a recorded failure."
            )
        self._close(
            SKIPPED,
            error_type=CASCADE,
            error_message=f"waiting on {', '.join(waiting)}"[:MESSAGE_LIMIT],
        )

    def _close(
        self,
        status: str,
        rows_written: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if self.closed:
            raise ValueError(
                f"audit: run {self.run_id} is already closed, so this notebook has "
                "already recorded its outcome. Re-run it from the first cell rather "
                "than from the cell that failed."
            )
        session = _session()
        self._flush(session)
        session.sql(
            f"""
            UPDATE {RUN_TABLE}
               SET ended_ts      = cast(:ended_ts AS TIMESTAMP),
                   status        = :status,
                   rows_written  = cast(:rows_written AS BIGINT),
                   error_type    = cast(:error_type AS STRING),
                   error_message = cast(:error_message AS STRING)
             WHERE run_id = :run_id
            """,
            args={
                "ended_ts": dt.datetime.now(dt.timezone.utc),
                "status": status,
                "rows_written": rows_written,
                "error_type": error_type,
                "error_message": error_message,
                "run_id": self.run_id,
            },
        )
        self.closed = True

    def _flush(self, session: SparkSession) -> None:
        if not self.buffered:
            return
        rows = [
            (
                self.run_id,
                item.metric,
                item.scope,
                item.value_numeric,
                item.value_text,
                item.value_date,
                item.denominator,
            )
            for item in self.buffered
        ]
        # Explicit schema: the buffer holds a null in two of the three value columns
        # on every row, and inference over those would settle on void.
        session.createDataFrame(rows, schema=metric_frame_schema()).write.mode(
            "append"
        ).saveAsTable(METRIC_TABLE)
        self.buffered = []


def metric_frame_schema() -> StructType:
    """Write schema for the buffered metrics, in METRIC_COLUMNS order."""
    return StructType(
        [
            StructField("run_id", StringType(), nullable=False),
            StructField("metric", StringType(), nullable=False),
            StructField("scope", StringType(), nullable=True),
            StructField("value_numeric", DoubleType(), nullable=True),
            StructField("value_text", StringType(), nullable=True),
            StructField("value_date", DateType(), nullable=True),
            StructField("denominator", DoubleType(), nullable=True),
        ]
    )


def _session() -> SparkSession:
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError(
            "audit: no active SparkSession. This module writes Delta and is called "
            "from a notebook, not from a local test."
        )
    return session

def failed_in_run(job_run_id: str | None) -> set[str]:
    """Names recorded as failed under one job run.

    Every stage calls this before planning what to write. The filter is exactly
    `status = 'failed'`: a skipped stage already sits downstream of a failure, and
    reading it as one would cascade that same failure a second time.

    A null job run id returns nothing rather than every failure ever recorded. A
    notebook run by hand belongs to no pipeline execution, so it plans a full run,
    which is the reading `02_post_run_record_state` already gives an empty widget.
    """
    if not job_run_id:
        return set()
    rows = _session().sql(
        f"""
        SELECT DISTINCT source
          FROM {RUN_TABLE}
         WHERE job_run_id = :job_run_id
           AND status = '{FAILED}'
        """,
        args={"job_run_id": job_run_id},
    ).collect()
    return {row["source"] for row in rows}

def succeeded_in_run(job_run_id: str | None) -> set[tuple[str, str]] | None:
    """Name and layer pairs recorded as succeeded under one job run.

    Evidence, where `failed_in_run` is absence. A stage plans from what failed and
    writes only where everything it reads rebuilt in the same job, because a
    dependency that failed, one that skipped, and one the job never reached are three
    different events that all mean the table in front of it is last month's.

    The layer is returned beside the name rather than inferred from it. A source
    records twice, at bronze when ADF lands the file and at silver when the notebook
    reads it, so a bronze success is not a Silver rebuild and nothing about the name
    says which one this is.

    Returns:
        None where there is no job run, meaning no evidence exists to read. That is
        distinct from an empty set, which means a job run that has recorded no
        success yet. A notebook run by hand is in the first case and gates on
        nothing, since gating on absent evidence would make every notebook
        unrunnable outside the job.
    """
    if not job_run_id:
        return None
    rows = _session().sql(
        f"""
        SELECT DISTINCT source, layer
          FROM {RUN_TABLE}
         WHERE job_run_id = :job_run_id
           AND status = '{SUCCEEDED}'
        """,
        args={"job_run_id": job_run_id},
    ).collect()
    return {(row["source"], row["layer"]) for row in rows}
