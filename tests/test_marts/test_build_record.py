"""Tests for reading the marts build back from the job run and from dbt's results.

Pure Python. The three run_results.json fixtures were captured from dbt-core 1.12.5, the
version the job pins, building a throwaway project on DuckDB, and trimmed to the fields
the recorder reads: one build with a warning, one with a view erroring and a test failing,
and one `dbt run`. The adapter differs from production and the file does not, since
run_results.json is written by dbt-core.

The job run entries are built from the fields the Jobs API documents for a task run,
because no captured response exists outside the workspace. Their timings are chosen to
straddle the fixtures' own timestamps, so the check that a file belongs to its build sees
a real pair.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import tarfile
import zipfile
from pathlib import Path

import pytest

from databricks_src.marts.build_record import (
    CLOCK_SLACK,
    FAILURES_NAMED,
    NEVER_RAN,
    TASK_KEY,
    BuildSummary,
    MartsResultsUnreadable,
    Node,
    TaskOutcome,
    artifacts_request,
    assert_belongs_to,
    countable_views,
    decide,
    find_task,
    from_epoch_ms,
    node_name,
    node_scopes,
    parse_run_results,
    run_results_from_archive,
    summary_metrics,
    task_outcome,
)

FIXTURES = Path(__file__).parent / "fixtures"

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def ms(text: str) -> int:
    return (dt.datetime.fromisoformat(text) - EPOCH) // dt.timedelta(milliseconds=1)


def utc(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text)


# The warning fixture's invocation ran 22:32:27.4 to 22:32:30.0, after `dbt deps`, so the
# task's commands are placed from 22:32:20 to 22:32:31.
EXECUTION_STARTED = "2026-10-16T22:32:20+00:00"


def task(
    result: str | None = "SUCCESS",
    life: str = "TERMINATED",
    started: str = EXECUTION_STARTED,
    seconds: int = 11,
    setup_ms: int = 50_000,
    cleanup_ms: int = 400,
    message: str = "",
    key: str = TASK_KEY,
    attempt: int = 0,
    run_id: int = 7001,
) -> dict:
    """A task run as runs/get lists it, executing from `started` for `seconds`."""
    began = ms(started)
    executed = seconds * 1000
    state = {"life_cycle_state": life, "state_message": message}
    if result is not None:
        state["result_state"] = result
    return {
        "task_key": key,
        "run_id": run_id,
        "attempt_number": attempt,
        "start_time": began - setup_ms,
        "setup_duration": setup_ms,
        "execution_duration": executed,
        "cleanup_duration": cleanup_ms,
        "end_time": began + executed + cleanup_ms,
        "state": state,
    }


def never_ran(result: str = "EXCLUDED", life: str = "TERMINATED") -> dict:
    return {
        "task_key": TASK_KEY,
        "run_id": 7002,
        "start_time": 0,
        "end_time": 0,
        "setup_duration": 0,
        "execution_duration": 0,
        "cleanup_duration": 0,
        "state": {"life_cycle_state": life, "result_state": result, "state_message": ""},
    }


def outcome(**overrides) -> TaskOutcome:
    return task_outcome(task(**overrides))


def tarball(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, body in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
    return buffer.getvalue()


def zipped(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return buffer.getvalue()


def as_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def summary(name: str = "build_with_warning.json") -> BuildSummary:
    return parse_run_results(fixture(name))


def node(unique_id: str, status: str, **fields) -> Node:
    return Node(
        unique_id=unique_id,
        resource_type=unique_id.split(".", 1)[0],
        name=node_name(unique_id),
        status=status,
        execution_time=fields.get("execution_time", 0.1),
        failures=fields.get("failures"),
        message=fields.get("message"),
        relation_name=fields.get("relation_name"),
    )


def built(*nodes: Node) -> BuildSummary:
    stamp = utc("2026-10-16T22:32:30+00:00")
    return BuildSummary("build", stamp, stamp, 1.0, tuple(nodes))


# --------------------------------------------------------------------------- #
# The task
# --------------------------------------------------------------------------- #


def test_epoch_milliseconds_read_as_utc():
    assert from_epoch_ms(0) == EPOCH


def test_a_millisecond_survives_the_conversion_exactly():
    """Going through a float would land some of these a microsecond short, and a window
    edge that moves is a window that can refuse its own results."""
    assert from_epoch_ms(1_760_000_000_123).microsecond == 123_000


def test_the_task_is_found_by_its_key():
    job_run = {"run_id": 941, "tasks": [task(key="verify_cross_source"), task()]}
    assert find_task(job_run)["task_key"] == TASK_KEY


def test_a_missing_task_names_what_the_run_does_hold():
    """A key renamed in job_definition.json and not here fails with both names in view."""
    job_run = {"run_id": 941, "tasks": [task(key="verify_cross_source")]}
    with pytest.raises(ValueError, match="verify_cross_source"):
        find_task(job_run)


def test_the_latest_attempt_is_the_one_that_stands():
    job_run = {"tasks": [task(result="FAILED", attempt=0), task(attempt=1, run_id=7003)]}
    assert find_task(job_run)["run_id"] == 7003


def test_a_successful_task_carries_its_execution_window():
    read = outcome()
    assert read.ran and read.succeeded
    assert read.started == utc(EXECUTION_STARTED)
    assert read.ended == utc("2026-10-16T22:32:31+00:00")


def test_the_window_leaves_out_what_happened_before_execution():
    """start_time can be when the job service opened the task, well before dbt ran. The
    other stages time themselves from their first cell, so the build has to as well."""
    entry = task(setup_ms=95_000)
    read = task_outcome(entry)
    assert from_epoch_ms(entry["start_time"]) < read.started
    assert (read.ended - read.started).total_seconds() == 11


def test_a_task_that_never_executed_takes_the_whole_attempt():
    """A failed library install reports no execution time. The stage still took the time
    it took, and that is what the row shows."""
    entry = task(result="FAILED", seconds=0)
    read = task_outcome(entry)
    assert read.started == from_epoch_ms(entry["start_time"])
    assert read.ended == from_epoch_ms(entry["end_time"])


def test_an_execution_longer_than_the_attempt_is_clamped_to_it():
    entry = task()
    entry["execution_duration"] = entry["end_time"] - entry["start_time"] + 60_000
    read = task_outcome(entry)
    assert read.started == from_epoch_ms(entry["start_time"])


def test_a_task_that_ran_with_no_end_is_refused():
    entry = task()
    entry["end_time"] = 0
    with pytest.raises(ValueError, match="no end time"):
        task_outcome(entry)


@pytest.mark.parametrize("result", sorted(NEVER_RAN))
def test_a_task_that_never_ran_has_nothing_to_record(result):
    read = task_outcome(never_ran(result))
    assert not read.ran
    assert (read.started, read.ended) == (None, None)


def test_a_skipped_task_has_nothing_to_record():
    """Timed as though it ran, so the lifecycle state alone has to settle it."""
    skipped = task(result=None, life="SKIPPED")
    assert skipped["start_time"] > 0
    assert not task_outcome(skipped).ran


@pytest.mark.parametrize("life", ["PENDING", "RUNNING", "TERMINATING", "QUEUED"])
def test_a_task_still_running_is_refused(life):
    """The recorder runs on ALL_DONE after the build. Meeting a live task means that
    dependency changed, and reading it would record an outcome it had not reached."""
    with pytest.raises(ValueError, match="rather than finished"):
        task_outcome(task(result=None, life=life))


def test_an_internal_error_is_a_failure_that_ran():
    """No result state arrives with it, and the lifecycle state is the only name the
    failure has."""
    read = task_outcome(task(result=None, life="INTERNAL_ERROR", message="driver lost"))
    assert read.ran and not read.succeeded
    assert read.result == "INTERNAL_ERROR"


def test_a_timeout_is_a_failure():
    read = outcome(result="TIMEDOUT", message="Task marts_build timed out after 600s")
    assert read.ran and not read.succeeded


# --------------------------------------------------------------------------- #
# The artifacts
# --------------------------------------------------------------------------- #


def test_the_link_and_its_headers_are_handed_back():
    output = {
        "dbt_output": {
            "artifacts_link": "https://example.invalid/dbt-output.tar.gz",
            "artifacts_headers": {"x-ms-version": "2021-08-06"},
        }
    }
    assert artifacts_request(output) == (
        "https://example.invalid/dbt-output.tar.gz",
        {"x-ms-version": "2021-08-06"},
    )


def test_a_link_without_headers_gets_empty_ones():
    output = {"dbt_output": {"artifacts_link": "https://example.invalid/a.tar.gz"}}
    assert artifacts_request(output)[1] == {}


def test_no_link_is_unreadable_and_carries_what_the_api_said():
    with pytest.raises(MartsResultsUnreadable, match="thirty minutes") as raised:
        artifacts_request({"error": "Output is not available\nfor this run"})
    assert "Output is not available" in str(raised.value)


def test_run_results_are_found_in_a_tarball_by_name():
    """The layout inside the archive is undocumented, so a path is never assumed."""
    data = tarball(
        {
            "dbt-output/logs/dbt.log": b"Done. PASS=4 WARN=1",
            "dbt-output/target/run_results.json": as_bytes("build_with_warning.json"),
        }
    )
    assert run_results_from_archive(data) == fixture("build_with_warning.json")


def test_run_results_are_found_in_a_zip():
    data = zipped({"target/run_results.json": as_bytes("build_with_warning.json")})
    assert run_results_from_archive(data)["args"]["which"] == "build"


def test_an_archive_without_run_results_is_unreadable():
    with pytest.raises(MartsResultsUnreadable, match="hold no run_results.json"):
        run_results_from_archive(tarball({"logs/dbt.log": b"Encountered an error"}))


def test_of_several_copies_the_one_under_target_is_read():
    data = tarball(
        {
            "target/run_results.json": as_bytes("build_with_warning.json"),
            "archive/run_results.json": as_bytes("run_command.json"),
        }
    )
    assert run_results_from_archive(data)["args"]["which"] == "build"


def test_a_package_target_folder_loses_to_the_projects_own():
    """A package can carry a test project with a target folder of its own. The project's
    sits nearer the root."""
    data = tarball(
        {
            "dbt_packages/dbt_utils/integration_tests/target/run_results.json": as_bytes(
                "run_command.json"
            ),
            "target/run_results.json": as_bytes("build_with_warning.json"),
        }
    )
    assert run_results_from_archive(data)["args"]["which"] == "build"


def test_several_copies_with_none_in_a_target_folder_are_unreadable():
    data = tarball(
        {
            "a/run_results.json": as_bytes("build_with_warning.json"),
            "b/run_results.json": as_bytes("run_command.json"),
        }
    )
    with pytest.raises(MartsResultsUnreadable, match="no single one in the project's target"):
        run_results_from_archive(data)


def test_two_target_folders_at_one_depth_are_unreadable():
    data = tarball(
        {
            "one/target/run_results.json": as_bytes("build_with_warning.json"),
            "two/target/run_results.json": as_bytes("run_command.json"),
        }
    )
    with pytest.raises(MartsResultsUnreadable, match="no single one in the project's target"):
        run_results_from_archive(data)


def test_bytes_that_are_no_archive_are_unreadable():
    with pytest.raises(MartsResultsUnreadable, match="neither a tarball nor a zip"):
        run_results_from_archive(b"<html>expired</html>")


def test_run_results_that_are_not_json_are_unreadable():
    with pytest.raises(MartsResultsUnreadable, match="not JSON"):
        run_results_from_archive(tarball({"target/run_results.json": b"{truncated"}))


# --------------------------------------------------------------------------- #
# The results
# --------------------------------------------------------------------------- #


def test_a_build_with_a_warning_reads_back():
    read = summary("build_with_warning.json")
    assert read.command == "build"
    assert [n.name for n in read.models] == ["mart_alpha", "mart_beta"]
    assert len(read.tests) == 3
    assert (len(read.built), len(read.passed), len(read.warned)) == (2, 2, 1)
    assert read.failing == () and read.skipped == ()


def test_a_failing_build_reads_back():
    read = summary("build_failing.json")
    assert [(n.name, n.status) for n in read.failing] == [
        ("mart_broken", "error"),
        ("assert_beta_fails", "fail"),
    ]
    assert len(read.skipped) == 2
    assert [n.name for n in read.built] == ["mart_alpha", "mart_beta"]


def test_a_warning_is_not_a_failure():
    """dbt exits cleanly on a warning, so the build stands; the share records it."""
    assert not node("test.p.assert_x", "warn").failing


def test_a_skipped_node_is_not_a_cause():
    assert not node("model.p.mart_after", "skipped").failing


def test_a_failing_status_is_read_by_resource_type():
    """`error` breaks a view and a test alike, `fail` only exists for a test."""
    assert node("model.p.mart_x", "error").failing
    assert node("test.p.assert_x", "fail").failing
    assert node("test.p.assert_x", "error").failing
    assert node("unit_test.p.mart_x.checks_y", "fail").failing


def test_a_generic_test_drops_its_hash():
    assert node_name("test.fixture.not_null_mart_alpha_id.fa84ebf1a9") == "not_null_mart_alpha_id"


def test_a_singular_test_and_a_view_keep_their_names():
    assert node_name("test.fixture.assert_alpha_warns") == "assert_alpha_warns"
    assert node_name("model.uk_property_intel.mart_district_yield") == "mart_district_yield"


def test_a_unit_test_keeps_the_model_in_front_of_its_name():
    assert node_name("unit_test.p.mart_x.checks_y") == "mart_x.checks_y"


def test_only_a_build_is_accepted():
    """The task runs deps then build, and deps writes no results. Anything else means the
    commands changed and every count would describe some other invocation."""
    with pytest.raises(MartsResultsUnreadable, match="records a dbt run"):
        summary("run_command.json")


def test_a_document_missing_a_field_is_unreadable():
    document = fixture("build_with_warning.json")
    del document["metadata"]["invocation_started_at"]
    with pytest.raises(MartsResultsUnreadable, match="lacks a field"):
        parse_run_results(document)


def test_a_result_missing_its_status_is_unreadable():
    document = fixture("build_with_warning.json")
    del document["results"][0]["status"]
    with pytest.raises(MartsResultsUnreadable, match="lacks a field"):
        parse_run_results(document)


def test_a_file_of_another_schema_is_unreadable():
    document = fixture("build_with_warning.json")
    document["metadata"]["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v12.json"
    with pytest.raises(MartsResultsUnreadable, match="not dbt run results"):
        parse_run_results(document)


def test_dbt_timestamps_read_as_aware_utc():
    read = summary()
    assert read.invoked == utc("2026-10-16T22:32:27.449318+00:00")
    assert read.generated.tzinfo is not None


# --------------------------------------------------------------------------- #
# Whether the results belong to the build
# --------------------------------------------------------------------------- #


def test_results_written_inside_the_window_belong():
    read = summary()
    assert assert_belongs_to(read, outcome()) is read


def test_results_just_outside_the_window_still_belong():
    """Two clocks stamp the pair, the cluster's and the job service's."""
    late = outcome(started="2026-10-16T22:32:40+00:00", seconds=2)
    assert late.started - summary().invoked < CLOCK_SLACK
    assert_belongs_to(summary(), late)


def test_results_from_last_month_are_refused():
    """The case the check exists for: dbt stopped before writing, and an older file sat
    where the packaging looks."""
    next_month = outcome(started="2026-11-16T22:32:20+00:00")
    with pytest.raises(MartsResultsUnreadable, match="belongs to another invocation"):
        assert_belongs_to(summary(), next_month)


def test_results_against_a_task_that_never_ran_are_refused():
    with pytest.raises(MartsResultsUnreadable, match="never ran"):
        assert_belongs_to(summary(), task_outcome(never_ran()))


# --------------------------------------------------------------------------- #
# The verdict
# --------------------------------------------------------------------------- #


def test_a_clean_build_succeeds():
    clean = built(
        node("model.p.mart_x", "success", relation_name="`c`.`marts`.`mart_x`"),
        node("test.p.not_null_mart_x_id.0123456789", "pass"),
    )
    assert decide(outcome(), clean).succeeded


def test_a_warning_leaves_the_build_succeeded():
    verdict = decide(outcome(), summary("build_with_warning.json"))
    assert verdict.succeeded and verdict.message is None


def test_a_failed_build_names_what_broke_and_what_was_skipped():
    verdict = decide(
        outcome(result="FAILED", message="dbt build failed\nsee logs"),
        summary("build_failing.json"),
    )
    assert not verdict.succeeded
    assert verdict.message.startswith("the dbt task ended FAILED: dbt build failed;")
    assert "2 of 9 nodes failed" in verdict.message
    assert "view mart_broken (error: Runtime Error in model mart_broken" in verdict.message
    assert "test assert_beta_fails (fail, 2 rows)" in verdict.message
    assert "2 skipped behind them" in verdict.message


def test_a_timed_out_build_fails_with_the_reason():
    verdict = decide(outcome(result="TIMEDOUT", message="timed out after 600s"), None)
    assert not verdict.succeeded
    assert verdict.message == "the dbt task ended TIMEDOUT: timed out after 600s"


def test_a_success_the_results_contradict_is_a_failure():
    """dbt exits non-zero on these, so a success beside them means the task and the file
    disagree, and the record does not get to pick the happier one."""
    verdict = decide(outcome(), summary("build_failing.json"))
    assert not verdict.succeeded
    assert verdict.message.startswith("the task reported success, yet 2 of 9 nodes failed")


def test_a_build_that_ran_no_tests_fails():
    """A build that checks nothing passes by default, which is not a result."""
    untested = built(node("model.p.mart_x", "success"))
    verdict = decide(outcome(), untested)
    assert not verdict.succeeded
    assert "ran no tests" in verdict.message


def test_a_build_that_ran_no_views_fails():
    viewless = built(node("test.p.assert_x", "pass"))
    assert "ran no views" in decide(outcome(), viewless).message


def test_unreadable_results_do_not_fail_a_build_dbt_passed():
    """The row says what dbt did. The recorder raises on the missing detail afterwards."""
    verdict = decide(outcome(), None, MartsResultsUnreadable("no link"))
    assert verdict.succeeded


def test_unreadable_results_are_named_when_the_build_failed():
    verdict = decide(outcome(result="FAILED"), None, MartsResultsUnreadable("no link"))
    assert not verdict.succeeded
    assert verdict.message == (
        "the dbt task ended FAILED; dbt's results could not be read: no link"
    )


def test_failures_past_the_named_limit_are_counted_not_named():
    broken = built(
        node("model.p.mart_x", "success"),
        *(node(f"test.p.assert_{index:02d}", "fail", failures=1) for index in range(14)),
    )
    message = decide(outcome(result="FAILED"), broken).message
    assert f"and {14 - FAILURES_NAMED} more" in message
    assert "assert_13" not in message


# --------------------------------------------------------------------------- #
# The metrics
# --------------------------------------------------------------------------- #


def test_the_shares_carry_counts_against_their_bases():
    rows = {metric: (value, base) for metric, value, scope, base in summary_metrics(summary())}
    assert rows["marts_built"] == (2, 2)
    assert rows["mart_tests_passed"] == (2, 3)
    assert rows["mart_tests_warned"] == (1, 3)


def test_every_node_records_its_status_under_its_name():
    statuses = {
        scope: value
        for metric, value, scope, _ in summary_metrics(summary("build_failing.json"))
        if metric == "mart_node_status"
    }
    assert len(statuses) == 9
    assert statuses["mart_broken"] == "error"
    assert statuses["not_null_mart_after_broken_id"] == "skipped"
    assert statuses["assert_alpha_warns"] == "warn"


def test_two_nodes_sharing_a_name_are_told_apart_by_id():
    """A versioned model and a unit test can print the same name as something else.
    Recorded under one scope they would read as one node with two outcomes."""
    nodes = (
        node("model.p.mart_x", "success"),
        node("model.q.mart_x", "success"),
        node("model.p.mart_y", "success"),
    )
    scopes = node_scopes(nodes)
    assert scopes["model.p.mart_x"] == "model.p.mart_x"
    assert scopes["model.p.mart_y"] == "mart_y"


def test_every_metric_is_registered_at_the_marts_layer():
    """A name the writer does not hold, or holds at another layer, fails the recorder on
    the cluster after the build already ran. Caught here instead."""
    from databricks_src.quality.audit.writer import LAYERS_OF_METRIC, METRICS

    names = {metric for metric, *_ in summary_metrics(summary("build_failing.json"))}
    names.add("mart_rows")
    assert names <= set(METRICS)
    assert all(LAYERS_OF_METRIC[name] == ("marts",) for name in names)


def test_every_metric_is_accepted_by_a_marts_run():
    """Through measure, so value types and denominators are checked by the code that will
    check them on the cluster."""
    from databricks_src.quality.audit.writer import AuditRun

    read = outcome()
    audit_run = AuditRun(
        source="marts_build",
        layer="marts",
        ingestion_ts=read.started,
        ended_ts=read.ended,
    )
    for metric, value, scope, base in summary_metrics(summary("build_failing.json")):
        audit_run.measure(metric, value, scope=scope, denominator=base)
    assert len(audit_run.buffered) == 3 + 9


def test_only_views_built_this_run_are_counted():
    """A view that errored still exists from last month, and counting it would record
    last month's rows against this build."""
    read = summary("build_failing.json")
    assert [name for name, _ in countable_views(read)] == ["mart_alpha", "mart_beta"]
    assert all(relation for _, relation in countable_views(read))


def test_a_view_with_no_relation_is_not_counted():
    ephemeral = built(node("model.p.mart_x", "success", relation_name=None))
    assert countable_views(ephemeral) == ()
