# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Marts: record the dbt build
# MAGIC
# MAGIC The last task of the monthly job. `marts_build` is a dbt task, which runs the dbt CLI
# MAGIC and nothing else, so it cannot open an `AuditRun`. This notebook opens one on its
# MAGIC behalf once the build has finished, which puts the marts in `pipeline_run` beside
# MAGIC every other stage and on the Pipeline Health dashboard with them.
# MAGIC
# MAGIC It reads two things. The job run says whether the task ran, how it ended, and when
# MAGIC its commands executed. `run_results.json`, which dbt writes and the task archives,
# MAGIC says what happened to each view and each test. The task's result is the verdict,
# MAGIC since it is dbt's exit code, and the results are the detail. Every decision lives in
# MAGIC `databricks_src/marts/build_record.py`; this notebook fetches and writes.
# MAGIC
# MAGIC The run is timed to the build, not to this notebook. It opens when dbt's commands
# MAGIC started and closes when they stopped, both read off the job run, so the dashboard's
# MAGIC Gantt shows the build where it happened and for as long as it took.
# MAGIC
# MAGIC **This task ends the way the build did.** It is the job's leaf task, and Databricks
# MAGIC sets a job run's status from its leaf tasks, so a recorder that wrote down a failed
# MAGIC build and exited cleanly would turn a failed run into "succeeded with failures". It
# MAGIC records the failure and raises. It also raises, after recording, when the build
# MAGIC passed and its results could not be read: the row is right about dbt, and the gap is
# MAGIC still a gap.
# MAGIC
# MAGIC Run by hand, the widget takes the id of the job run to record. The artifacts link
# MAGIC lasts thirty minutes after the build ends, so only a recent run can be recorded that
# MAGIC way.

# COMMAND ----------

import time

import requests

from databricks_src.marts.build_record import (
    TASK_KEY,
    MartsBuildFailed,
    MartsResultsUnreadable,
    artifacts_request,
    assert_belongs_to,
    countable_views,
    decide,
    find_task,
    parse_run_results,
    run_results_from_archive,
    summary_metrics,
    task_outcome,
)
from databricks_src.quality.audit.writer import AuditRun

# Each API call returns a small JSON document. The archive holds the build's logs and
# target folder, so its download gets a longer allowance.
API_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 300

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The build, read off the job run
# MAGIC
# MAGIC Passed by the job as `{{job.run_id}}`, like every other task. With the widget empty
# MAGIC there is no build to read, so a hand run stops here and records nothing.
# MAGIC
# MAGIC The token is the one the notebook runs under, which for a job task is the job's
# MAGIC owner, the same identity the dbt task authenticated as.

# COMMAND ----------

dbutils.widgets.text("job_run_id", "")  # noqa: F821
JOB_RUN_ID = dbutils.widgets.get("job_run_id").strip() or None  # noqa: F821

if JOB_RUN_ID is None:
    dbutils.notebook.exit("no job_run_id, so there is no build to record")  # noqa: F821

host = spark.conf.get("spark.databricks.workspaceUrl")  # noqa: F821
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
auth = {"Authorization": f"Bearer {context.apiToken().get()}"}


def get(endpoint: str, **params) -> dict:
    response = requests.get(
        f"https://{host}/api/2.2/jobs/{endpoint}",
        headers=auth,
        params=params,
        timeout=API_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


outcome = task_outcome(find_task(get("runs/get", run_id=JOB_RUN_ID)))

print(f"job_run_id : {JOB_RUN_ID}")
print(f"{TASK_KEY} : {outcome.result}" + (f", {outcome.message}" if outcome.message else ""))
if outcome.ran:
    seconds = (outcome.ended - outcome.started).total_seconds()
    print(f"executed   : {outcome.started:%H:%M:%S} to {outcome.ended:%H:%M:%S} UTC, {seconds:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC A build that never ran has nothing to record. Inside the monthly job that means the
# MAGIC run stopped before reaching it, and the plan `02_post_run_record_state` wrote shows it:
# MAGIC `marts_build` planned to run and no row against it.

# COMMAND ----------

if not outcome.ran:
    dbutils.notebook.exit(f"{TASK_KEY} did not run ({outcome.result}), nothing to record")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Open the run, timed to the build

# COMMAND ----------

run = AuditRun(
    source="marts_build",
    layer="marts",
    ingestion_ts=outcome.started,
    ended_ts=outcome.ended,
    job_run_id=JOB_RUN_ID,
)
run.start()
print(f"run {run.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dbt's results
# MAGIC
# MAGIC Fetched outside `run.step()` on purpose. A failure here is this notebook failing to
# MAGIC read, not the build failing, and the row is not allowed to say otherwise. The error is
# MAGIC kept, handed to the verdict, and raised at the end if nothing else raised first.
# MAGIC Anything at all is caught, so an unforeseen error closes the row rather than leaving
# MAGIC it open at `started`, which would read as a killed cluster.
# MAGIC
# MAGIC The timing table is dbt's own, per node. It is printed rather than recorded: the
# MAGIC registry has no duration kind, and the run row already carries the build's length.

# COMMAND ----------

results = None
unreadable = None

try:
    link, headers = artifacts_request(get("runs/get-output", run_id=outcome.run_id))
    download = requests.get(link, headers=headers, timeout=DOWNLOAD_TIMEOUT)
    download.raise_for_status()
    results = assert_belongs_to(
        parse_run_results(run_results_from_archive(download.content)), outcome
    )
except Exception as error:  # noqa: BLE001
    unreadable = error
    print(f"results unreadable: {type(error).__name__}: {error}")

if results is not None:
    print(
        f"dbt {results.command}: {len(results.built)} of {len(results.models)} views built, "
        f"{len(results.passed)} of {len(results.tests)} tests passed, "
        f"{len(results.warned)} warned, {len(results.failing)} failing, "
        f"{len(results.skipped)} skipped, {results.elapsed_seconds:.0f}s inside dbt\n"
    )
    for node in sorted(results.nodes, key=lambda item: -item.execution_time):
        print(f"{node.execution_time:>8.2f}s  {node.status:<8} {node.resource_type:<5} {node.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Record
# MAGIC
# MAGIC Metrics first and the verdict second, inside one step, so a failed build closes
# MAGIC carrying everything measured before the failure is raised.
# MAGIC
# MAGIC Rows are counted only on views this build rebuilt. A view that errored still stands
# MAGIC from the last build that succeeded, and counting it would record those rows against
# MAGIC this one. The count runs the view, so it costs what a query against the mart costs,
# MAGIC and the seconds printed beside it are that cost on this cluster.

# COMMAND ----------

mart_rows: dict[str, int] = {}

with run.step():
    if results is not None:
        for metric, value, scope, base in summary_metrics(results):
            run.measure(metric, value, scope=scope, denominator=base)
        for view, relation in countable_views(results):
            counting = time.perf_counter()
            mart_rows[view] = spark.sql(  # noqa: F821
                f"SELECT count(*) AS n FROM {relation}"
            ).collect()[0]["n"]
            print(f"{mart_rows[view]:>12,}  {view}, {time.perf_counter() - counting:.1f}s")
            run.measure("mart_rows", mart_rows[view], scope=view)

    verdict = decide(outcome, results, unreadable)
    if not verdict.succeeded:
        raise MartsBuildFailed(verdict.message)

run.succeed()
print(f"{run.source}: succeeded, run {run.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fail on missing detail
# MAGIC
# MAGIC Reached only when the build passed. The row already says so, and carries no test
# MAGIC counts, so the task fails here to make the gap visible on the job rather than only
# MAGIC as a missing line on the dashboard.

# COMMAND ----------

if unreadable is not None:
    raise MartsResultsUnreadable(
        f"{TASK_KEY} succeeded and is recorded as run {run.run_id}, but its results "
        f"could not be read, so the run carries no test counts: {unreadable}"
    ) from unreadable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Hand back a summary
# MAGIC
# MAGIC The exit value is what runs/get-output returns for this task, so the job run page and
# MAGIC `run_job_tasks.py` show what was recorded without opening the notebook. It also ends
# MAGIC a job run here, which leaves the cell below to hand checks.

# COMMAND ----------

dbutils.notebook.exit(  # noqa: F821
    f"recorded run {run.run_id}: {TASK_KEY} succeeded, "
    f"{(outcome.ended - outcome.started).total_seconds():.0f}s executing; "
    f"{len(results.built)} of {len(results.models)} views built, "
    f"{len(results.passed)} of {len(results.tests)} tests passed, "
    f"{len(results.warned)} warned; rows "
    + ", ".join(f"{view} {rows:,}" for view, rows in mart_rows.items())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this run recorded
# MAGIC
# MAGIC For a hand check. `seconds` should match the dbt task's execution time on the job run
# MAGIC page, not this notebook's.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.status, r.started_ts, r.ended_ts,
# MAGIC        timestampdiff(SECOND, r.started_ts, r.ended_ts) AS seconds,
# MAGIC        r.error_type, r.job_run_id,
# MAGIC        m.metric, m.scope, m.value_numeric, m.value_text, m.denominator
# MAGIC FROM uk_property_intel.quality.pipeline_run r
# MAGIC LEFT JOIN uk_property_intel.quality.pipeline_metric m USING (run_id)
# MAGIC WHERE r.source = 'marts_build'
# MAGIC   AND r.started_ts = (SELECT max(started_ts) FROM uk_property_intel.quality.pipeline_run
# MAGIC                       WHERE source = 'marts_build')
# MAGIC ORDER BY m.metric, m.scope
