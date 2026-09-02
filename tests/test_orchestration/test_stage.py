"""Tests for the gate every Silver and Gold notebook opens with.

Pure Python. `dbutils` is a parameter rather than an import, which is what lets the
widget read be exercised here at all, and the two Delta readers are replaced with
what they would have returned.

The reason this module exists is that eleven notebooks would otherwise carry the same
twenty lines. What is worth testing is therefore the wiring rather than the cascade,
which `test_source_dependency` already covers: that the job run id reaches the run
row, that a skip closes rather than raises, and that nothing here ends a notebook.
"""

from __future__ import annotations

import datetime as dt

import pytest

from databricks_src.orchestration import stage
from databricks_src.orchestration.stage import WIDGET, open_stage, read_plan

INGESTION_TS = dt.datetime(2026, 8, 31, 9, 0, tzinfo=dt.timezone.utc)

SOURCES = ("boe", "hpi", "ppd", "doogal", "ons", "police")
GOLD = (
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

# A run in which everything succeeded. Facts included, or a test asking whether one
# rebuilt would read a gap in this fixture as a gap in the run.
ALL_REBUILT = frozenset(
    {(source, "bronze") for source in SOURCES}
    | {(source, "silver") for source in SOURCES}
    | {(table, "gold") for table in GOLD}
)


class FakeWidgets:
    """The notebook's widget box, which keeps a value already entered."""

    def __init__(self, entered=None):
        self.entered = dict(entered or {})
        self.declared = []

    def text(self, name, default):
        self.declared.append(name)
        self.entered.setdefault(name, default)

    def get(self, name):
        return self.entered[name]


class FakeDbutils:
    def __init__(self, entered=None):
        self.widgets = FakeWidgets(entered)

    class notebook:
        @staticmethod
        def exit(value):
            raise AssertionError("stage ended the notebook; that belongs to the caller")


class FakeRun:
    """Stands in for AuditRun, recording what was asked of it."""

    def __init__(self, **passed):
        self.passed = passed
        self.run_id = "run-1"
        self.started = False
        self.skipped = None

    def start(self):
        self.started = True

    def skip(self, blocked_by):
        blocked_by = sorted(blocked_by)
        if not blocked_by:
            raise ValueError("audit: skipped with nothing named as the cause")
        self.skipped = blocked_by


def stub(monkeypatch, failed=(), rebuilt=ALL_REBUILT):
    """Replace the two Delta readers and the run constructor.

    Returns the list the constructor appends to, so a test can read what the stage
    asked for rather than only what it got back.
    """
    made: list[FakeRun] = []

    def make(**passed):
        made.append(FakeRun(**passed))
        return made[-1]

    monkeypatch.setattr(stage, "failed_in_run", lambda job: set(failed))
    monkeypatch.setattr(stage, "succeeded_in_run", lambda job: rebuilt)
    monkeypatch.setattr(stage, "AuditRun", make)
    return made


# --------------------------------------------------------------------------- #
# Reading the plan
# --------------------------------------------------------------------------- #


def test_the_widget_is_declared_under_the_name_the_job_passes(monkeypatch):
    stub(monkeypatch)
    dbutils = FakeDbutils()
    read_plan(dbutils)
    assert dbutils.widgets.declared == [WIDGET]


def test_a_value_already_entered_survives_a_rerun(monkeypatch):
    """Declaring the widget again must not clear it, or setting a probe id by hand
    would be undone by the cell that reads it."""
    stub(monkeypatch)
    dbutils = FakeDbutils({WIDGET: "probe-hpi-a"})
    assert read_plan(dbutils).job_run_id == "probe-hpi-a"


def test_an_empty_widget_reads_as_no_job_run(monkeypatch):
    stub(monkeypatch, rebuilt=None)
    assert read_plan(FakeDbutils()).job_run_id is None


def test_whitespace_only_is_no_job_run(monkeypatch):
    stub(monkeypatch, rebuilt=None)
    assert read_plan(FakeDbutils({WIDGET: "   "})).job_run_id is None


def test_absent_evidence_is_carried_as_none_not_as_empty(monkeypatch):
    """A hand run has no evidence to read. An empty set would mean a job that has
    recorded no success yet, and would block every stage."""
    stub(monkeypatch, rebuilt=None)
    assert read_plan(FakeDbutils()).rebuilt is None


def test_the_recorded_failures_reach_the_plan(monkeypatch):
    stub(monkeypatch, failed={"ppd", "police"})
    assert read_plan(FakeDbutils({WIDGET: "941"})).failed == frozenset({"ppd", "police"})


def test_evidence_keeps_its_layers(monkeypatch):
    stub(monkeypatch, rebuilt=frozenset({("hpi", "bronze")}))
    assert read_plan(FakeDbutils({WIDGET: "941"})).rebuilt == frozenset({("hpi", "bronze")})


# --------------------------------------------------------------------------- #
# Opening a stage
# --------------------------------------------------------------------------- #


def test_a_stage_with_nothing_waiting_gets_a_started_run(monkeypatch):
    made = stub(monkeypatch)
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    run = open_stage("hpi", "silver", INGESTION_TS, plan)
    assert run is made[0]
    assert run.started and run.skipped is None


def test_the_job_run_id_reaches_the_run_row(monkeypatch):
    """Without it the row records under a null id and no later stage reads it, which
    is a failure that reports success."""
    made = stub(monkeypatch)
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    open_stage("hpi", "silver", INGESTION_TS, plan)
    assert made[0].passed["job_run_id"] == "941"
    assert made[0].passed["ingestion_ts"] is INGESTION_TS
    assert (made[0].passed["source"], made[0].passed["layer"]) == ("hpi", "silver")


def test_a_hand_run_opens_with_a_null_job_run_id(monkeypatch):
    made = stub(monkeypatch, rebuilt=None)
    plan = read_plan(FakeDbutils())
    assert open_stage("hpi", "silver", INGESTION_TS, plan) is not None
    assert made[0].passed["job_run_id"] is None


def test_a_waiting_stage_returns_nothing_and_records_the_skip(monkeypatch):
    made = stub(monkeypatch, failed={"hpi"}, rebuilt=ALL_REBUILT - {("hpi", "bronze")})
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    assert open_stage("hpi", "silver", INGESTION_TS, plan) is None
    assert made[0].started and made[0].skipped == ["hpi (bronze)"]


def test_a_skip_opens_the_run_before_closing_it(monkeypatch):
    """A skip is inserted and closed like any other outcome, so a stage that waited
    is distinguishable from one the job never reached, which leaves no row."""
    made = stub(monkeypatch, failed={"police"}, rebuilt=ALL_REBUILT - {("police", "bronze")})
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    open_stage("police", "silver", INGESTION_TS, plan)
    assert made[0].started is True


def test_opening_a_stage_never_ends_the_notebook(monkeypatch):
    """FakeDbutils.notebook.exit raises if called. Ending the run belongs to the
    caller, because a skipped table in 01_load_dimensions must not stop the three
    beside it."""
    stub(monkeypatch, failed={"police"}, rebuilt=ALL_REBUILT - {("police", "bronze")})
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    assert open_stage("police", "silver", INGESTION_TS, plan) is None


def test_a_gold_table_gates_on_its_dimensions(monkeypatch):
    stub(monkeypatch, failed=set(), rebuilt=ALL_REBUILT - {("dim_lsoa", "gold")})
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    assert open_stage("fact_area_month_hpi", "gold", INGESTION_TS, plan) is not None
    assert open_stage("fact_lsoa_month_crime", "gold", INGESTION_TS, plan) is None


def test_the_plan_answers_for_an_item_without_opening_a_run(monkeypatch):
    """01_load_dimensions asks about four tables and writes whichever survive, so the
    question has to be separable from the run."""
    stub(monkeypatch, failed={"police"})
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    assert plan.waiting_on("dim_date") == []
    assert plan.waiting_on("dim_crime_type") == ["police"]


def test_a_stage_the_plan_kept_cannot_be_skipped(monkeypatch):
    """AuditRun.skip refuses an empty cause, so a gate disagreeing with the plan
    fails at the write rather than recording a skip nobody asked for."""
    stub(monkeypatch)
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    with pytest.raises(ValueError, match="nothing named as the cause"):
        FakeRun().skip(plan.waiting_on("hpi"))


# --------------------------------------------------------------------------- #
# Asking whether a table rebuilt
# --------------------------------------------------------------------------- #


def test_a_table_with_a_successful_run_reads_as_rebuilt(monkeypatch):
    stub(monkeypatch)
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    assert plan.rebuilt_this_run("fact_area_month_rent", "gold") is True


def test_a_table_with_no_successful_run_reads_as_not_rebuilt(monkeypatch):
    """The question a check asks before opening a table it only measures. Nothing in
    `waiting_on` answers it: that asks whether something would run given what its own
    dependencies did, not whether this table actually rebuilt."""
    stub(monkeypatch, rebuilt=ALL_REBUILT - {("fact_area_month_rent", "gold")})
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    assert plan.rebuilt_this_run("fact_area_month_rent", "gold") is False


def test_the_layer_is_part_of_the_question(monkeypatch):
    stub(monkeypatch, rebuilt=frozenset({("hpi", "bronze")}))
    plan = read_plan(FakeDbutils({WIDGET: "941"}))
    assert plan.rebuilt_this_run("hpi", "bronze") is True
    assert plan.rebuilt_this_run("hpi", "silver") is False


def test_a_hand_run_treats_every_table_as_rebuilt(monkeypatch):
    """The evidence does not exist to be absent, and a notebook has to stay runnable
    outside the job."""
    stub(monkeypatch, rebuilt=None)
    plan = read_plan(FakeDbutils())
    assert plan.rebuilt_this_run("dim_date", "gold") is True
