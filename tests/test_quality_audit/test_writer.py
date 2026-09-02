"""Tests for the pipeline audit writer and its metric registry.

Everything here is pure Python: the registry, the generated DDL, the routing of a
value to its column, and the freshness verdict. Every Delta write is excluded and is
verified on the cluster, not here.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from databricks_src.quality.audit.writer import (
    CASCADE,
    CHECKS,
    FAILED,
    FRESHNESS_BOUND_DAYS,
    GOLD_TABLES,
    KINDS,
    MESSAGE_LIMIT,
    METRIC_COLUMNS,
    METRIC_COMMENT,
    METRICS,
    RUN_COLUMNS,
    ORCHESTRATION,
    RUN_COMMENT,
    SKIPPED,
    SOURCES,
    STARTED,
    STATUSES,
    SUCCEEDED,
    AuditRun,
    _as_row_values,
    assert_registry_consistent,
    failed_in_run,
    freshness_lag_days,
    freshness_verdict,
    metric_frame_schema,
    metric_table_ddl,
    one_value_check,
    open_run_check,
    run_table_ddl,
    sql_literal,
    status_check,
    succeeded_in_run,
)

INGESTION_TS = dt.datetime(2026, 8, 7, 9, 30, 0)

SPARK_TO_DDL = {"StringType": "STRING", "DoubleType": "DOUBLE", "DateType": "DATE"}


def run(source: str = "hpi") -> AuditRun:
    return AuditRun(source=source, layer="silver", ingestion_ts=INGESTION_TS)


def run_with_job(job_run_id: str, source: str = "hpi") -> AuditRun:
    return AuditRun(
        source=source,
        layer="silver",
        ingestion_ts=INGESTION_TS,
        job_run_id=job_run_id,
    )


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #


def test_registry_is_consistent():
    """Runs at import too. Kept as a test so a bad edit fails in CI rather than only
    on the cluster, where it would abort a load that had already extracted."""
    assert_registry_consistent()


def test_every_metric_carries_a_known_kind():
    """kind is what the dashboard reads to decide whether the level or the move is
    the signal. An unknown one reaches the chart with no interpretation."""
    assert {metric.kind for metric in METRICS.values()} <= set(KINDS)


def test_every_metric_carries_a_note():
    """The note is the only place a reader learns that a third of Police rows having
    no crime id is the source's shape rather than a fault."""
    assert all(metric.note.strip() for metric in METRICS.values())


def test_registry_keys_match_metric_names():
    assert all(name == metric.name for name, metric in METRICS.items())


def test_freshness_bounds_cover_every_source():
    """A source absent from the map asserts nothing and would look healthy forever."""
    assert set(FRESHNESS_BOUND_DAYS) == set(SOURCES)


def test_freshness_bounds_all_start_unset():
    """Bounds are read off what the first runs report. A value here before that has
    been done is a guess, which is what this phase set out not to do."""
    assert all(bound is None for bound in FRESHNESS_BOUND_DAYS.values())


# --------------------------------------------------------------------------- #
# Table definition
# --------------------------------------------------------------------------- #


def test_metric_ddl_matches_the_write_schema():
    """The DDL and the write schema each declare a type. Drift between them loads
    values into a column that truncates or nulls them, and append matches on name."""
    declared = [
        (line.split()[0], line.split()[1])
        for line in metric_table_ddl().replace(" NOT NULL", "").split(",\n    ")
    ]
    produced = [
        (field.name, SPARK_TO_DDL[type(field.dataType).__name__])
        for field in metric_frame_schema().fields
    ]
    assert declared == produced


@pytest.mark.parametrize(
    "ddl, columns",
    [(run_table_ddl(), RUN_COLUMNS), (metric_table_ddl(), METRIC_COLUMNS)],
    ids=["pipeline_run", "pipeline_metric"],
)
def test_ddl_column_order_is_canonical(ddl, columns):
    """INSERT matches on position where the column list is generated from the same
    tuple, so a reordering here would load values into the wrong columns."""
    assert tuple(line.split()[0] for line in ddl.split(",\n    ")) == columns


def test_status_check_admits_every_status_the_writer_sets():
    """A status the code produces and the constraint rejects would abort the close
    and leave the run open, which reads as a killed cluster."""
    for status in STATUSES:
        assert f"'{status}'" in status_check()


def test_one_value_check_covers_every_value_column():
    """A fourth value type added to the writer without the constraint following it
    would let a row carry two values, and the reader could not tell which is meant."""
    from databricks_src.quality.audit.writer import VALUE_COLUMNS

    assert all(name in one_value_check() for name in VALUE_COLUMNS)


# --------------------------------------------------------------------------- #
# Value routing
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "value, column, expected",
    [
        (dt.date(2026, 6, 1), "value_date", dt.date(2026, 6, 1)),
        (dt.datetime(2026, 6, 1, 14, 3), "value_date", dt.date(2026, 6, 1)),
        (31_430_611, "value_numeric", 31430611.0),
        (0.114, "value_numeric", 0.114),
        (Decimal("3.7500"), "value_numeric", 3.75),
        ("2026-06", "value_text", "2026-06"),
    ],
)
def test_value_routes_to_its_column(value, column, expected):
    recorded = _as_row_values("source_rows", value, None, None)
    assert getattr(recorded, column) == expected


def test_only_one_value_column_is_populated():
    """The table's CHECK enforces this, but the writer failing it would abort a load
    that had already run, so it is caught before the write."""
    recorded = _as_row_values("newest_data_date", dt.date(2026, 6, 1), None, None)
    populated = [
        name
        for name in ("value_numeric", "value_text", "value_date")
        if getattr(recorded, name) is not None
    ]
    assert populated == ["value_date"]


def test_collection_is_stored_sorted_and_joined():
    """Absent forces and observed crime types are sets. Sorting makes two runs with
    the same members compare equal as text."""
    recorded = _as_row_values(
        "entities_absent_from_newest_period",
        {"gloucestershire", "british-transport-police", "greater-manchester"},
        None,
        None,
    )
    assert recorded.value_text == (
        "british-transport-police, gloucestershire, greater-manchester"
    )


def test_bool_is_rejected():
    """bool subclasses int, so a flag would land in value_numeric as 0 or 1 and be
    read as a count."""
    with pytest.raises(ValueError, match="bool"):
        _as_row_values("source_rows", True, None, None)


def test_none_is_rejected():
    with pytest.raises(ValueError, match="no value"):
        _as_row_values("source_rows", None, None, None)


def test_unregistered_metric_name_is_rejected():
    """A free-text name is how a rename silently breaks a series."""
    with pytest.raises(ValueError, match="registry"):
        _as_row_values("rows_without_postcode", 12, None, None)


def test_unsupported_type_is_rejected():
    with pytest.raises(ValueError, match="no value column"):
        _as_row_values("source_rows", {"rows": 1}, None, None)


def test_denominator_survives():
    recorded = _as_row_values("rows_out_of_area", 24, None, 96_092_836)
    assert (recorded.value_numeric, recorded.denominator) == (24.0, 96092836.0)


def test_denominator_without_a_count_is_rejected():
    """A base against a date or a vocabulary means nothing, and the share it implies
    would be computed anyway."""
    with pytest.raises(ValueError, match="denominator"):
        _as_row_values("newest_data_date", dt.date(2026, 6, 1), None, 45)


def test_scope_is_carried():
    """Police loops seven archives in one run, so the same metric name appears seven
    times under one run_id and needs the archive to tell them apart."""
    recorded = _as_row_values("rows_out_of_area", 24, "2026-06", None)
    assert recorded.scope == "2026-06"


# --------------------------------------------------------------------------- #
# Freshness
# --------------------------------------------------------------------------- #


def test_lag_counts_days_to_the_run_date():
    assert freshness_lag_days(dt.date(2026, 6, 1), INGESTION_TS) == 67


def test_unset_bound_records_without_asserting():
    """Every source starts here. The bound is read off what these runs report."""
    lag, message = freshness_verdict("police", dt.date(2026, 2, 1), INGESTION_TS)
    assert lag == 187 and message is None


def test_breached_bound_reports_the_lag_and_the_bound(monkeypatch):
    monkeypatch.setitem(FRESHNESS_BOUND_DAYS, "police", 90)
    lag, message = freshness_verdict("police", dt.date(2026, 2, 1), INGESTION_TS)
    assert lag == 187
    assert "187 days" in message and "bound of 90" in message


def test_bound_met_exactly_passes(monkeypatch):
    monkeypatch.setitem(FRESHNESS_BOUND_DAYS, "hpi", 67)
    _, message = freshness_verdict("hpi", dt.date(2026, 6, 1), INGESTION_TS)
    assert message is None


def test_skip_suppresses_a_breach(monkeypatch):
    """A rebuild from a Bronze snapshot kept on purpose is a legitimate run."""
    monkeypatch.setitem(FRESHNESS_BOUND_DAYS, "police", 90)
    _, message = freshness_verdict("police", dt.date(2026, 2, 1), INGESTION_TS, skip=True)
    assert message is None


def test_freshness_records_the_date_before_it_raises(monkeypatch):
    """The value has to reach the buffer even when the assertion fires, or the run
    that most needs explaining is the one that recorded nothing."""
    monkeypatch.setitem(FRESHNESS_BOUND_DAYS, "police", 90)
    audit_run = run("police")
    with pytest.raises(ValueError, match="stale release"):
        audit_run.freshness(dt.date(2026, 2, 1))
    assert [item.metric for item in audit_run.buffered] == ["newest_data_date"]
    assert audit_run.buffered[0].value_date == dt.date(2026, 2, 1)


# --------------------------------------------------------------------------- #
# Run object
# --------------------------------------------------------------------------- #


def test_unknown_source_is_rejected():
    """A typo would open a series nothing else writes to, which reads as a source
    that stopped rather than one that was never named correctly."""
    with pytest.raises(ValueError, match="source"):
        AuditRun(source="boe_base_rate", layer="silver", ingestion_ts=INGESTION_TS)


def test_unknown_layer_is_rejected():
    with pytest.raises(ValueError, match="layer"):
        AuditRun(source="hpi", layer="raw", ingestion_ts=INGESTION_TS)


def test_a_source_opens_at_bronze_and_at_silver():
    """ADF lands the file and Silver reads it. Two runs, two outcomes, and the skip
    cascade needs to tell them apart."""
    for layer in ("bronze", "silver"):
        assert AuditRun(source="hpi", layer=layer, ingestion_ts=INGESTION_TS).layer == layer


def test_a_gold_table_does_not_open_at_bronze():
    """Nothing lands a Gold table, so a run naming one there is a typo."""
    with pytest.raises(ValueError, match=r"runs at \['gold'\]"):
        AuditRun(source="dim_area", layer="bronze", ingestion_ts=INGESTION_TS)


def test_a_source_does_not_open_at_gold():
    with pytest.raises(ValueError, match=r"runs at \['bronze', 'silver'\]"):
        AuditRun(source="hpi", layer="gold", ingestion_ts=INGESTION_TS)


def test_every_layer_a_name_carries_is_a_declared_layer():
    from databricks_src.quality.audit.writer import LAYERS, LAYERS_OF

    assert all(set(layers) <= set(LAYERS) for layers in LAYERS_OF.values())


def test_every_source_records_at_both_bronze_and_silver():
    """A source dropped from bronze stops being recorded there with nothing to show
    it, and the cascade would read every Bronze fetch as having succeeded."""
    from databricks_src.quality.audit.writer import LAYERS_OF, SOURCES

    assert all(LAYERS_OF[name] == ("bronze", "silver") for name in SOURCES)


def test_a_source_missing_bronze_is_caught(monkeypatch):
    """The registry check runs at import. This drives it directly, since an import
    that already succeeded cannot show what it would have refused."""
    from databricks_src.quality.audit import writer

    monkeypatch.setitem(writer.LAYERS_OF, "hpi", ("silver",))
    with pytest.raises(ValueError, match="both bronze and silver"):
        writer.assert_registry_consistent()


def test_every_run_name_carries_at_least_one_layer():
    from databricks_src.quality.audit.writer import LAYERS_OF, RUN_NAMES

    assert all(LAYERS_OF.get(name) for name in RUN_NAMES)


# --------------------------------------------------------------------------- #
# Job run
# --------------------------------------------------------------------------- #


def test_the_job_run_id_is_carried():
    """Every task of one pipeline execution shares it, which is what makes a stage's
    skip list this run's failures rather than every failure ever recorded."""
    assert run_with_job("j-42").job_run_id == "j-42"


def test_a_hand_run_notebook_carries_no_job_run_id():
    """Null is the honest reading: it belonged to no pipeline execution."""
    assert run().job_run_id is None


def test_the_job_run_id_is_the_last_run_column():
    """Appended, because ALTER TABLE ADD COLUMN appends and the generated DDL has to
    keep matching a table that already holds history."""
    assert RUN_COLUMNS[-1] == "job_run_id"


def test_the_job_run_id_is_nullable():
    from databricks_src.quality.audit.writer import run_table_ddl

    line = [
        row for row in run_table_ddl().split(",\n    ") if row.startswith("job_run_id")
    ]
    assert line and "NOT NULL" not in line[0]


def test_started_ts_defaults_to_the_ingestion_timestamp():
    """The audit row and the Silver rows it describes have to agree on when the run
    happened, or they cannot be joined."""
    assert run().started_ts == INGESTION_TS


def test_run_ids_are_distinct():
    assert run().run_id != run().run_id


def test_measure_buffers_and_returns_the_value():
    """The return lets a notebook wrap an existing print without computing twice."""
    audit_run = run()
    assert audit_run.measure("silver_rows", 147_453) == 147_453
    assert len(audit_run.buffered) == 1


def test_closing_twice_is_rejected():
    """A second close would append the buffered metrics again and double every count
    on the dashboard."""
    audit_run = run()
    audit_run.closed = True
    with pytest.raises(ValueError, match="already closed"):
        audit_run.succeed(rows_written=1)


# --------------------------------------------------------------------------- #
# SQL literals
# --------------------------------------------------------------------------- #


def test_apostrophe_is_escaped():
    """A table comment naming a status value closes its own literal otherwise, and
    the CREATE fails on the cluster rather than in review."""
    assert sql_literal("status 'started'") == "'status ''started'''"


def test_plain_text_is_quoted_unchanged():
    assert sql_literal("one row per run") == "'one row per run'"


def test_table_comments_survive_escaping():
    """Both comments run through this before reaching DDL. Escaping them twice, or
    not at all, is the failure this pins."""
    for comment in (RUN_COMMENT, METRIC_COMMENT):
        quoted = sql_literal(comment)
        assert quoted.startswith("'") and quoted.endswith("'")
        assert quoted[1:-1].replace("''", "'") == comment


# --------------------------------------------------------------------------- #
# step
# --------------------------------------------------------------------------- #


def test_step_records_a_failure_and_re_raises(monkeypatch):
    """The notebook stops either way. What step adds is the row saying why."""
    audit_run = run()
    closed = {}
    monkeypatch.setattr(
        audit_run, "_close", lambda status, **kw: closed.update(status=status, **kw)
    )
    with pytest.raises(ValueError, match="HPI grain broken"):
        with audit_run.step():
            raise ValueError("HPI grain broken, (area_code, date) is not unique")
    assert closed["status"] == "failed"
    assert closed["error_type"] == "ValueError"
    assert "grain broken" in closed["error_message"]


def test_step_leaves_a_clean_cell_open(monkeypatch):
    """Cells run one at a time, so the run stays open until the notebook says
    otherwise."""
    audit_run = run()
    monkeypatch.setattr(audit_run, "_close", lambda *a, **kw: pytest.fail("closed"))
    with audit_run.step():
        audit_run.measure("source_rows", 19_000)
    assert not audit_run.closed


def test_step_does_not_close_an_already_closed_run(monkeypatch):
    """A second failure after one was recorded would append the metrics twice."""
    audit_run = run()
    audit_run.closed = True
    monkeypatch.setattr(audit_run, "_close", lambda *a, **kw: pytest.fail("closed"))
    with pytest.raises(ValueError):
        with audit_run.step():
            raise ValueError("second failure")


def test_cancelling_a_cell_is_not_a_failure(monkeypatch):
    """KeyboardInterrupt is a kill, not a fault, and an open row reads as one."""
    audit_run = run()
    monkeypatch.setattr(audit_run, "_close", lambda *a, **kw: pytest.fail("closed"))
    with pytest.raises(KeyboardInterrupt):
        with audit_run.step():
            raise KeyboardInterrupt
    assert not audit_run.closed


def test_long_error_message_is_truncated():
    """Police reports every failing rule with row samples, so the message is not
    assumed short."""
    audit_run = run()
    audit_run.buffered = []
    error = ValueError("x" * 9000)
    truncated = str(error)[:MESSAGE_LIMIT]
    assert len(truncated) == MESSAGE_LIMIT

# --------------------------------------------------------------------------- #
# Skipping
# --------------------------------------------------------------------------- #


def test_a_skip_is_neither_a_success_nor_a_failure():
    """Three end states became four. A skip recorded as either of the other two
    would move the success rate in a direction nothing went wrong in."""
    assert SKIPPED in STATUSES
    assert SKIPPED not in (SUCCEEDED, FAILED)


def test_a_skipped_run_is_not_an_open_one():
    """open_run_check pairs an absent end timestamp with 'started' alone, so a skip
    carries both timestamps and closes like any other outcome."""
    assert STARTED in open_run_check()
    assert SKIPPED not in open_run_check()


def test_skip_records_what_it_waits_on(monkeypatch):
    audit_run = run()
    closed = {}
    monkeypatch.setattr(
        audit_run, "_close", lambda status, **kw: closed.update(status=status, **kw)
    )
    audit_run.skip(["ppd"])
    assert closed["status"] == SKIPPED
    assert closed["error_type"] == CASCADE
    assert "ppd" in closed["error_message"]


def test_skip_names_every_cause_in_a_stable_order(monkeypatch):
    """Two runs blocked by the same pair should read identically, or a dashboard
    grouping on the message splits one cause into two."""
    audit_run = run()
    closed = {}
    monkeypatch.setattr(
        audit_run, "_close", lambda status, **kw: closed.update(status=status, **kw)
    )
    audit_run.skip({"ppd", "doogal"})
    assert closed["error_message"] == "waiting on doogal, ppd"


def test_skip_writes_no_row_count(monkeypatch):
    """Nothing ran, so a zero here would be a measurement rather than an absence."""
    audit_run = run()
    closed = {}
    monkeypatch.setattr(
        audit_run, "_close", lambda status, **kw: closed.update(status=status, **kw)
    )
    audit_run.skip(["hpi"])
    assert closed.get("rows_written") is None


def test_a_skip_with_no_cause_is_rejected():
    """A stage skips because something else failed. Nothing named means the gate and
    the plan disagree about why this one is not running."""
    with pytest.raises(ValueError, match="nothing named as the cause"):
        run().skip([])


def test_a_long_cause_list_is_truncated(monkeypatch):
    """The message column is bounded, and a cascade naming every stage is the run
    where the reason matters most."""
    audit_run = run()
    closed = {}
    monkeypatch.setattr(
        audit_run, "_close", lambda status, **kw: closed.update(status=status, **kw)
    )
    audit_run.skip([f"source_{index:04d}" for index in range(2000)])
    assert len(closed["error_message"]) == MESSAGE_LIMIT


def test_skipping_a_closed_run_is_rejected():
    audit_run = run()
    audit_run.closed = True
    with pytest.raises(ValueError, match="already closed"):
        audit_run.skip(["ppd"])


# --------------------------------------------------------------------------- #
# The orchestration run
# --------------------------------------------------------------------------- #


def test_the_plan_run_opens_at_bronze():
    """02 records the plan under a run of its own. None of its six source runs owns
    those metrics, and without it nothing records that 02 ran at all."""
    audit_run = AuditRun(
        source="pipeline_plan", layer="bronze", ingestion_ts=INGESTION_TS
    )
    assert audit_run.layer == "bronze"


def test_the_plan_run_does_not_open_at_gold():
    with pytest.raises(ValueError, match="pipeline_plan"):
        AuditRun(source="pipeline_plan", layer="gold", ingestion_ts=INGESTION_TS)


def test_the_orchestration_name_is_not_a_source_or_a_table():
    """It names no source and builds nothing, so a name shared with either would
    have its layer decided by declaration order."""
    assert not set(ORCHESTRATION) & (set(SOURCES) | set(GOLD_TABLES) | set(CHECKS))


def test_a_planned_stage_carries_its_disposition_as_text():
    recorded = _as_row_values("planned_stage", "skip", "dim_lsoa", None)
    assert (recorded.value_text, recorded.scope) == ("skip", "dim_lsoa")
    assert recorded.value_numeric is None


def test_the_plan_is_recorded_once_per_stage_through_scope():
    """One registry entry serves nineteen stages, the same way gold_rows serves
    every area level."""
    assert METRICS["planned_stage"].kind in KINDS


# --------------------------------------------------------------------------- #
# Reading a run back
# --------------------------------------------------------------------------- #


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _Session:
    """Records the statement and its bindings, and returns rows the test supplies."""

    def __init__(self, rows):
        self.rows = list(rows)
        self.statements = []

    def sql(self, statement, args=None):
        self.statements.append((statement, args))
        return _Rows(self.rows)


def stub_session(monkeypatch, rows=()):
    """Replace the module's session, since these two are the only readers here.

    What can be checked without a cluster is the statement they build and how they
    read the answer, and both failure modes live there: a filter admitting a status
    it should not, and a name read back without the layer beside it.
    """
    from databricks_src.quality.audit import writer as module

    session = _Session(rows)
    monkeypatch.setattr(module, "_session", lambda: session)
    return session


def test_a_hand_run_reads_no_failures():
    """A notebook run outside a job belongs to no pipeline execution, so it plans a
    full run rather than inheriting every failure ever recorded."""
    assert failed_in_run(None) == set()
    assert failed_in_run("") == set()


def test_a_hand_run_has_no_evidence_rather_than_empty_evidence():
    """The two absences mean different things and are different values. An empty set
    would block every gated stage and make the notebooks unrunnable by hand."""
    assert succeeded_in_run(None) is None
    assert failed_in_run(None) == set()


def test_neither_reader_touches_a_session_without_a_job(monkeypatch):
    from databricks_src.quality.audit import writer as module

    monkeypatch.setattr(
        module, "_session", lambda: pytest.fail("opened a session with no job run")
    )
    assert failed_in_run(None) == set()
    assert succeeded_in_run(None) is None


def test_failures_are_read_by_name(monkeypatch):
    session = stub_session(monkeypatch, [{"source": "ppd"}, {"source": "police"}])
    assert failed_in_run("941") == {"ppd", "police"}
    assert session.statements[0][1] == {"job_run_id": "941"}


def test_the_failure_filter_is_exactly_failed(monkeypatch):
    """A skipped stage already sits downstream of a failure. Reading it as one would
    cascade the same failure a second time."""
    session = stub_session(monkeypatch, [])
    failed_in_run("941")
    statement = session.statements[0][0]
    assert f"status = '{FAILED}'" in statement
    assert SKIPPED not in statement


def test_a_failure_is_read_the_same_from_either_layer(monkeypatch):
    """A source records at bronze and at silver, and it cascades identically from
    both, so the layer is not part of this answer."""
    stub_session(monkeypatch, [{"source": "ons"}, {"source": "ons"}])
    assert failed_in_run("941") == {"ons"}


def test_successes_are_read_as_name_and_layer(monkeypatch):
    session = stub_session(
        monkeypatch,
        [
            {"source": "hpi", "layer": "bronze"},
            {"source": "hpi", "layer": "silver"},
            {"source": "dim_area", "layer": "gold"},
        ],
    )
    assert succeeded_in_run("941") == {
        ("hpi", "bronze"),
        ("hpi", "silver"),
        ("dim_area", "gold"),
    }
    assert f"status = '{SUCCEEDED}'" in session.statements[0][0]


def test_a_bronze_success_is_not_a_silver_rebuild(monkeypatch):
    """The layer is what carries this. Read back as a bare name, the file landing
    would stand in for the table being rebuilt."""
    stub_session(monkeypatch, [{"source": "hpi", "layer": "bronze"}])
    rebuilt = succeeded_in_run("941")
    assert ("hpi", "bronze") in rebuilt
    assert ("hpi", "silver") not in rebuilt
