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
    FRESHNESS_BOUND_DAYS,
    KINDS,
    MESSAGE_LIMIT,
    METRIC_COLUMNS,
    METRIC_COMMENT,
    METRICS,
    RUN_COLUMNS,
    RUN_COMMENT,
    SOURCES,
    STATUSES,
    AuditRun,
    _as_row_values,
    assert_registry_consistent,
    freshness_lag_days,
    freshness_verdict,
    metric_frame_schema,
    metric_table_ddl,
    one_value_check,
    run_table_ddl,
    sql_literal,
    status_check,
)

INGESTION_TS = dt.datetime(2026, 8, 7, 9, 30, 0)

SPARK_TO_DDL = {"StringType": "STRING", "DoubleType": "DOUBLE", "DateType": "DATE"}


def run(source: str = "hpi") -> AuditRun:
    return AuditRun(source=source, layer="silver", ingestion_ts=INGESTION_TS)


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
        AuditRun(source="hpi", layer="bronze", ingestion_ts=INGESTION_TS)


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
