"""What the marts build did, read back from the job run and from dbt's own results.

The marts are built by a dbt task, which runs the dbt CLI and nothing else, so it cannot
open an `AuditRun` the way every notebook does. The notebook after it in the job opens one
on its behalf, and everything that notebook decides comes from two places:

- The job run, through the Jobs API. It says whether the task ran, how it ended, and when
  its commands executed.
- `run_results.json`, which dbt writes at the end of an invocation and the task archives
  with its logs. It says what happened to each view and each test.

The task state is the verdict. dbt exits non-zero when a view errors or a test fails, and
the task reports that exit, so a task that ended in success built every view and passed
every test. The results are the detail. When they cannot be read the outcome is still
known, so the notebook records it and then fails on the missing detail, rather than
recording a failure dbt never had.

No I/O here. The notebook calls the API and downloads the archive and hands over what came
back, so every decision below is tested without a workspace.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import re
import tarfile
import zipfile
from collections import Counter
from dataclasses import dataclass
from pathlib import PurePosixPath

# The job's key for the dbt task, which job_definition.json declares.
TASK_KEY = "marts_build"

# Lifecycle states a task ends in. The recorder runs after the build on ALL_DONE, so it
# should never meet any other, and a build read before it finished would be recorded as
# an outcome it had not reached.
TERMINAL_STATES = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})

# Result states a task reaches without executing. EXCLUDED is a run-now that named other
# tasks, DISABLED a task switched off in the job, and the two upstream states a run that
# stopped before this task opened. None of them leaves a build to record.
NEVER_RAN = frozenset({"EXCLUDED", "DISABLED", "UPSTREAM_CANCELED", "UPSTREAM_FAILED"})

# What dbt reports for a node that broke. A view erroring and a test failing both make
# dbt exit non-zero. A microbatch model that built some batches and not others counts
# with them, since part of a view is not the view. A warning does not break anything, and
# a node skipped behind a broken one is a consequence rather than a cause.
FAILING: dict[str, frozenset[str]] = {
    "model": frozenset({"error", "partial success"}),
    "test": frozenset({"fail", "error"}),
    "unit_test": frozenset({"fail", "error"}),
}

TEST_TYPES = frozenset({"test", "unit_test"})

# dbt stamps its results on the cluster's clock and the job service times the task on
# its own. A file stamped further than this outside the task's execution window belongs
# to another invocation: builds run a month apart, so ten minutes is generous for drift
# and far too short to admit last month's file.
CLOCK_SLACK = dt.timedelta(minutes=10)

RUN_RESULTS = "run_results.json"

# Failing nodes named in a run's message before the rest are counted instead. The writer
# bounds the message anyway; this keeps the bound from landing mid-name.
FAILURES_NAMED = 10

# A generic test's unique id ends in a ten-character hash that its printed name does not
# carry.
_GENERIC_TEST_HASH = re.compile(r"\.[0-9a-f]{10}$")

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


class MartsBuildFailed(Exception):
    """The build did not succeed: the task said so, or dbt's results did."""


class MartsResultsUnreadable(Exception):
    """dbt's results could not be fetched, opened, or trusted as this build's."""


# --------------------------------------------------------------------------- #
# The task
# --------------------------------------------------------------------------- #


def from_epoch_ms(value: int) -> dt.datetime:
    """A Jobs API timestamp as an aware UTC datetime.

    Built from a timedelta rather than through fromtimestamp, which goes via a float and
    can land a millisecond value a microsecond short.
    """
    return _EPOCH + dt.timedelta(milliseconds=value)


@dataclass(frozen=True)
class TaskOutcome:
    """How the dbt task ended, and when its commands executed.

    started and ended are None where the task never ran.
    """

    run_id: int | None
    result: str
    message: str
    ran: bool
    started: dt.datetime | None
    ended: dt.datetime | None

    @property
    def succeeded(self) -> bool:
        return self.result == "SUCCESS"


def find_task(job_run: dict, task_key: str = TASK_KEY) -> dict:
    """One task's entry in a job run, as runs/get returns it.

    Should the run list more than one attempt at the task, the latest is the outcome that
    stands.

    Raises:
        ValueError: where the job run has no task under that key.
    """
    tasks = job_run.get("tasks") or []
    attempts = [task for task in tasks if task.get("task_key") == task_key]
    if not attempts:
        present = sorted({task.get("task_key", "?") for task in tasks})
        raise ValueError(
            f"marts: job run {job_run.get('run_id', '?')} has no task {task_key!r}, only "
            f"{present}. A task renamed in job_definition.json has to be renamed in "
            "build_record.TASK_KEY too."
        )
    return max(
        attempts,
        key=lambda task: (task.get("attempt_number") or 0, task.get("start_time") or 0),
    )


def execution_window(task: dict) -> tuple[dt.datetime, dt.datetime]:
    """When the task's commands ran.

    Measured back from the end. A task's start_time is when the job service opened it,
    which can be well before anything executed, so execution_duration is counted back
    from the moment execution stopped, which is end_time less cleanup. A task that never
    executed, such as one whose library install failed, reports no execution time, and
    the whole attempt stands as its window.

    Raises:
        ValueError: where the task carries no start or no end, which a task that ran
            always does.
    """
    start_ms = task.get("start_time") or 0
    end_ms = task.get("end_time") or 0
    if start_ms <= 0 or end_ms <= 0:
        raise ValueError(
            f"marts: {task.get('task_key', '?')} ran and carries no "
            f"{'start' if start_ms <= 0 else 'end'} time, so there is no window to "
            "record it against."
        )

    execution_ms = task.get("execution_duration") or 0
    if execution_ms <= 0:
        return from_epoch_ms(start_ms), from_epoch_ms(end_ms)

    stopped = min(end_ms - (task.get("cleanup_duration") or 0), end_ms)
    began = max(stopped - execution_ms, start_ms)
    if began > stopped:
        return from_epoch_ms(start_ms), from_epoch_ms(end_ms)
    return from_epoch_ms(began), from_epoch_ms(stopped)


def task_outcome(task: dict) -> TaskOutcome:
    """Read how a task ended off its runs/get entry.

    `state` is what the project's other scripts read and what the API still returns
    beside its newer `status`. A run that ended in an internal error carries no result
    state, and its lifecycle state stands in for one, so it records as a failure that
    names what happened.

    Raises:
        ValueError: where the task has not finished.
    """
    state = task.get("state") or {}
    life = state.get("life_cycle_state") or ""
    result = state.get("result_state") or life
    message = (state.get("state_message") or "").strip()
    run_id = task.get("run_id")

    if life not in TERMINAL_STATES:
        raise ValueError(
            f"marts: {task.get('task_key', '?')} is {life or 'in no state'} rather than "
            "finished. The recorder runs after the build on ALL_DONE, so its dependency "
            "in job_definition.json has changed."
        )

    ran = life != "SKIPPED" and result not in NEVER_RAN and (task.get("start_time") or 0) > 0
    if not ran:
        return TaskOutcome(run_id, result, message, False, None, None)

    started, ended = execution_window(task)
    return TaskOutcome(run_id, result, message, True, started, ended)


# --------------------------------------------------------------------------- #
# The artifacts
# --------------------------------------------------------------------------- #


def artifacts_request(output: dict) -> tuple[str, dict[str, str]]:
    """The link and headers runs/get-output gives for the task's dbt artifacts.

    The link is issued for thirty minutes after the task ends. The recorder runs straight
    after the build, so inside the job a missing link means the task left no artifacts.

    Raises:
        MartsResultsUnreadable: where the output carries no link.
    """
    dbt_output = output.get("dbt_output") or {}
    link = dbt_output.get("artifacts_link")
    if not link:
        reported = _first_line(output.get("error"))
        raise MartsResultsUnreadable(
            "runs/get-output returned no dbt artifacts link for the task"
            + (f", and reported: {reported}" if reported else "")
            + ". The link exists for thirty minutes after the task ends, so a recorder "
            "run by hand later than that cannot read it."
        )
    return link, dict(dbt_output.get("artifacts_headers") or {})


def run_results_from_archive(data: bytes) -> dict:
    """The run_results.json inside the archive the dbt task leaves.

    Databricks documents the archive as a gzipped tarball of the run's logs, results and
    manifests and does not document its layout, so the file is found by name rather than
    by path, and a zip is read too in case the packaging changes. Nothing is extracted to
    disk. The member is read in memory, which also leaves no path inside the archive able
    to write anywhere.

    Raises:
        MartsResultsUnreadable: where the archive opens as neither format, holds no
            run_results.json, or holds several with no single one in the project's
            target folder.
    """
    found = _run_results_members(data)
    if not found:
        raise MartsResultsUnreadable(
            f"the dbt artifacts hold no {RUN_RESULTS}. dbt writes it when an invocation "
            "finishes, so a task stopped before dbt got that far leaves none."
        )
    if len(found) > 1:
        found = _project_target(found)

    ((path, body),) = found.items()
    try:
        return json.loads(body)
    except ValueError as error:
        raise MartsResultsUnreadable(f"{path} in the dbt artifacts is not JSON: {error}") from error


def _project_target(found: dict[str, bytes]) -> dict[str, bytes]:
    """The one copy in the project's own target folder, where several were found.

    dbt writes into target/ at the project root. A copy anywhere else, such as a
    package's own test project carrying a target folder of its own, sits deeper, so of
    the copies directly inside a target folder the shallowest is the project's.
    """
    in_target = {
        path: body for path, body in found.items() if PurePosixPath(path).parent.name == "target"
    }
    if in_target:
        shallowest = min(len(PurePosixPath(path).parts) for path in in_target)
        in_target = {
            path: body
            for path, body in in_target.items()
            if len(PurePosixPath(path).parts) == shallowest
        }
    if len(in_target) != 1:
        raise MartsResultsUnreadable(
            f"the dbt artifacts hold {len(found)} copies of {RUN_RESULTS} and no single "
            f"one in the project's target folder: {sorted(found)}"
        )
    return in_target


def _run_results_members(data: bytes) -> dict[str, bytes]:
    """Every member named run_results.json, read from a tarball or failing that a zip."""
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
            found = {}
            for member in archive.getmembers():
                if member.isfile() and PurePosixPath(member.name).name == RUN_RESULTS:
                    handle = archive.extractfile(member)
                    if handle is not None:
                        found[member.name] = handle.read()
            return found
    except tarfile.ReadError:
        pass

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            return {
                name: archive.read(name)
                for name in archive.namelist()
                if PurePosixPath(name).name == RUN_RESULTS
            }
    except zipfile.BadZipFile:
        raise MartsResultsUnreadable(
            "the dbt artifacts open as neither a tarball nor a zip"
        ) from None


# --------------------------------------------------------------------------- #
# The results
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Node:
    """One view or test, as run_results.json reports it."""

    unique_id: str
    resource_type: str
    name: str
    status: str
    execution_time: float
    failures: int | None
    message: str | None
    relation_name: str | None

    @property
    def is_test(self) -> bool:
        return self.resource_type in TEST_TYPES

    @property
    def failing(self) -> bool:
        return self.status in FAILING.get(self.resource_type, frozenset())


@dataclass(frozen=True)
class BuildSummary:
    """One dbt build, reduced to what the record needs."""

    command: str
    invoked: dt.datetime
    generated: dt.datetime
    elapsed_seconds: float
    nodes: tuple[Node, ...]

    @property
    def models(self) -> tuple[Node, ...]:
        return tuple(node for node in self.nodes if node.resource_type == "model")

    @property
    def tests(self) -> tuple[Node, ...]:
        return tuple(node for node in self.nodes if node.is_test)

    @property
    def built(self) -> tuple[Node, ...]:
        return tuple(node for node in self.models if node.status == "success")

    @property
    def passed(self) -> tuple[Node, ...]:
        return tuple(node for node in self.tests if node.status == "pass")

    @property
    def warned(self) -> tuple[Node, ...]:
        return tuple(node for node in self.tests if node.status == "warn")

    @property
    def failing(self) -> tuple[Node, ...]:
        return tuple(node for node in self.nodes if node.failing)

    @property
    def skipped(self) -> tuple[Node, ...]:
        return tuple(node for node in self.nodes if node.status == "skipped")


def node_name(unique_id: str) -> str:
    """A node's name as dbt prints it, from its unique id.

    The id is resource type, package and name, and a generic test adds a hash after the
    name, which is dropped. A unit test keeps the model it tests in front of its own name.
    """
    resource_type, _, rest = unique_id.partition(".")
    _, _, name = rest.partition(".")
    if resource_type == "test":
        name = _GENERIC_TEST_HASH.sub("", name)
    return name or unique_id


def parse_run_results(document: dict) -> BuildSummary:
    """Read the parts of run_results.json the record needs, and refuse anything else.

    Only a dbt build is accepted. The task runs `dbt deps` and then `dbt build`, and deps
    writes no results, so a file from any other command means the commands in
    job_definition.json have changed and the counts would describe a different invocation.

    Raises:
        MartsResultsUnreadable: where the document is not dbt run results, came from
            another command, or lacks a field read here.
    """
    try:
        metadata = document["metadata"]
        schema = metadata["dbt_schema_version"]
        command = document["args"]["which"]
        invoked = _parse_utc(metadata["invocation_started_at"])
        generated = _parse_utc(metadata["generated_at"])
        elapsed = float(document["elapsed_time"])
        nodes = tuple(_node(result) for result in document["results"])
    except (KeyError, TypeError, ValueError) as error:
        raise MartsResultsUnreadable(
            f"{RUN_RESULTS} lacks a field the record reads, or holds it in another "
            f"shape: {error!r}"
        ) from error

    if "/run-results/" not in schema:
        raise MartsResultsUnreadable(
            f"the file found as {RUN_RESULTS} declares schema {schema!r}, which is not "
            "dbt run results."
        )
    if command != "build":
        raise MartsResultsUnreadable(
            f"{RUN_RESULTS} records a dbt {command}, and the task is meant to finish on a "
            "dbt build. The commands in job_definition.json have changed."
        )
    return BuildSummary(command, invoked, generated, elapsed, nodes)


def _node(result: dict) -> Node:
    unique_id = result["unique_id"]
    failures = result.get("failures")
    return Node(
        unique_id=unique_id,
        resource_type=unique_id.split(".", 1)[0],
        name=node_name(unique_id),
        status=result["status"],
        execution_time=float(result.get("execution_time") or 0.0),
        failures=None if failures is None else int(failures),
        message=result.get("message"),
        relation_name=result.get("relation_name"),
    )


def assert_belongs_to(summary: BuildSummary, outcome: TaskOutcome) -> BuildSummary:
    """Refuse results that dbt stamped outside the build they are about to describe.

    The archive comes from this task run's own output, so ordinarily the question does
    not arise. It arises where dbt stopped before writing results while a file from an
    earlier invocation sat where the packaging looks, and recording that file would put
    last month's tests against this month's build.

    Raises:
        MartsResultsUnreadable: where dbt's own timestamps fall outside the task's
            execution window by more than CLOCK_SLACK.
    """
    if outcome.started is None or outcome.ended is None:
        raise MartsResultsUnreadable(
            "the task never ran, so there is no window to check its results against"
        )
    if summary.invoked < outcome.started - CLOCK_SLACK or summary.generated > (
        outcome.ended + CLOCK_SLACK
    ):
        raise MartsResultsUnreadable(
            f"{RUN_RESULTS} was written by an invocation running "
            f"{summary.invoked.isoformat()} to {summary.generated.isoformat()}, and this "
            f"build executed {outcome.started.isoformat()} to {outcome.ended.isoformat()}. "
            "It belongs to another invocation."
        )
    return summary


# --------------------------------------------------------------------------- #
# The record
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Verdict:
    """Whether the build succeeded, and the message a failed run carries."""

    succeeded: bool
    message: str | None = None


def decide(
    outcome: TaskOutcome,
    summary: BuildSummary | None,
    unreadable: BaseException | None = None,
) -> Verdict:
    """Settle the build's outcome from the task and, where they were read, the results.

    The task's result decides it, since that is dbt's exit code. The results add what
    broke, and two failures dbt would exit cleanly on: a build that ran no views and one
    that ran no tests. Either means the selection no longer covers the project, and a
    build that checks nothing passes by default.

    A build whose task succeeded is not failed here for results that could not be read.
    The caller records the success and raises on the missing detail afterwards, so the
    gap is loud without the row claiming a failure dbt never had.
    """
    problems: list[str] = []
    if not outcome.succeeded:
        said = _first_line(outcome.message)
        problems.append(f"the dbt task ended {outcome.result}" + (f": {said}" if said else ""))

    if summary is not None:
        if summary.failing:
            lead = "the task reported success, yet " if outcome.succeeded else ""
            problems.append(lead + describe_failures(summary))
        if not summary.models:
            problems.append("the build ran no views")
        if not summary.tests:
            problems.append("the build ran no tests, so nothing it built was checked")
    elif unreadable is not None and not outcome.succeeded:
        problems.append(f"dbt's results could not be read: {unreadable}")

    if not problems:
        return Verdict(True)
    return Verdict(False, "; ".join(problems))


def describe_failures(summary: BuildSummary) -> str:
    """The failing nodes in words, then what was skipped behind them."""
    failing = summary.failing
    named = ", ".join(_describe(node) for node in failing[:FAILURES_NAMED])
    text = f"{len(failing)} of {len(summary.nodes)} nodes failed: {named}"
    if len(failing) > FAILURES_NAMED:
        text += f", and {len(failing) - FAILURES_NAMED} more"
    if summary.skipped:
        text += f"; {len(summary.skipped)} skipped behind them"
    return text


def _describe(node: Node) -> str:
    kind = "view" if node.resource_type == "model" else node.resource_type.replace("_", " ")
    detail = node.status
    if node.is_test and node.failures:
        detail += f", {node.failures:,} row{'' if node.failures == 1 else 's'}"
    if node.status == "error" and node.message:
        detail += f": {' '.join(node.message.split())[:200]}"
    return f"{kind} {node.name} ({detail})"


def node_scopes(nodes: tuple[Node, ...]) -> dict[str, str]:
    """The scope each node's status is recorded under, keyed by unique id.

    The printed name, which is what anyone reading the dashboard will search for, and the
    unique id instead wherever two nodes would share a name.
    """
    counted = Counter(node.name for node in nodes)
    return {
        node.unique_id: node.name if counted[node.name] == 1 else node.unique_id
        for node in nodes
    }


def summary_metrics(
    summary: BuildSummary,
) -> list[tuple[str, object, str | None, int | None]]:
    """What the results support recording, as (metric, value, scope, denominator).

    Row counts are not here. They need a session, and the notebook takes them off the
    views themselves.
    """
    tests = len(summary.tests)
    scopes = node_scopes(summary.nodes)
    return [
        ("marts_built", len(summary.built), None, len(summary.models)),
        ("mart_tests_passed", len(summary.passed), None, tests),
        ("mart_tests_warned", len(summary.warned), None, tests),
        *(
            ("mart_node_status", node.status, scopes[node.unique_id], None)
            for node in summary.nodes
        ),
    ]


def countable_views(summary: BuildSummary) -> tuple[tuple[str, str], ...]:
    """(view name, relation) for every view built this run that has a relation to count.

    An ephemeral model builds nothing to count and carries no relation, so it is left out
    rather than failing the count.
    """
    return tuple(
        (node.name, node.relation_name) for node in summary.built if node.relation_name
    )


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #


def _first_line(text: str | None) -> str:
    if not text:
        return ""
    lines = text.strip().splitlines()
    return lines[0].strip() if lines else ""


def _parse_utc(value: str) -> dt.datetime:
    """An ISO timestamp as dbt writes it, UTC with a trailing Z, as an aware datetime."""
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
