# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: record what landed
# MAGIC
# MAGIC The first task of the Silver and Gold job, opening once ADF's ForEach has
# MAGIC closed. It has to sit inside the job rather than beside it: every stage filters
# MAGIC `pipeline_run` on `{{job.run_id}}`, so rows written under any other id would be
# MAGIC invisible to all of them and the cascade would never fire while the run reported
# MAGIC success throughout.
# MAGIC
# MAGIC Reads the markers each failed Copy wrote, turns them into one `pipeline_run` row
# MAGIC per source, and advances `last_refreshed` for the sources that came through
# MAGIC clean.
# MAGIC
# MAGIC `01_pre_run_resolve_urls` emptied the log before anything could write to it, so
# MAGIC every marker in there belongs to this run. A source with no marker succeeded.
# MAGIC That covers both good outcomes without telling them apart: a source that
# MAGIC downloaded a new release and a source ADF skipped because it had nothing new
# MAGIC both land here with no marker, and both advance.
# MAGIC
# MAGIC Bronze is the only layer ADF owns, so it is the only one whose outcome has to be
# MAGIC carried across rather than recorded by the notebook that did the work. Every
# MAGIC Silver and Gold notebook opens its own run.
# MAGIC
# MAGIC What the next stage reads is `pipeline_run` filtered on this job run, not this
# MAGIC notebook's output. Nothing downstream depends on the cell order here.

# COMMAND ----------

import datetime as dt
import json

from databricks_src.bronze.watermark_library import registry, source_dependency
from databricks_src.quality.audit.writer import AuditRun

VOLUME = "/Volumes/uk_property_intel/configs/watermark"
WATERMARK_PATH = f"{VOLUME}/watermark.json"
LOG_PATH = f"{VOLUME}/log"

RUN_STARTED = dt.datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The job run
# MAGIC
# MAGIC Every task of one pipeline execution shares this, and each stage filters
# MAGIC `pipeline_run` on it. Without it a stage would read every failure ever recorded
# MAGIC and skip a source over last month's problem.
# MAGIC
# MAGIC Passed by the job as `{{job.run_id}}`. Empty when the notebook is run by hand,
# MAGIC which records null and is the honest reading: it belonged to no pipeline
# MAGIC execution.

# COMMAND ----------

dbutils.widgets.text("job_run_id", "")  # noqa: F821
JOB_RUN_ID = dbutils.widgets.get("job_run_id").strip() or None  # noqa: F821

print(f"job_run_id: {JOB_RUN_ID or 'none, run by hand'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the markers
# MAGIC
# MAGIC One file per failed target, so a `yearly_stepped` source that lost three years
# MAGIC leaves three markers. They are grouped by source, because the cascade decides
# MAGIC per source: one bad year out of thirty-two is a failed source, since a Silver
# MAGIC load over a gap it cannot see is worse than a load that does not run.

# COMMAND ----------

failures: dict[str, list[dict]] = {}

for entry in dbutils.fs.ls(LOG_PATH):  # noqa: F821
    if not entry.name.endswith(".json"):
        continue
    with open(f"{LOG_PATH}/{entry.name}", encoding="utf-8") as handle:
        marker = json.load(handle)
    failures.setdefault(marker["source_name"], []).append(marker)

if not failures:
    print("no markers: every copy in this run succeeded")
else:
    for source_name, markers in sorted(failures.items()):
        print(f"{source_name}: {len(markers)} failed")
        for marker in sorted(markers, key=lambda m: m.get("failed_at", "")):
            print(f"    {marker.get('failed_at', '?')}  {marker.get('target', '?')}")
            print(f"        {marker.get('error', '')[:160]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check the markers name real sources
# MAGIC
# MAGIC A marker naming something the watermark does not carry means ADF and the
# MAGIC watermark disagree about what the sources are. Recording it would open a run
# MAGIC under a name nothing else writes to, and ignoring it would drop a failure.

# COMMAND ----------

with open(WATERMARK_PATH, encoding="utf-8") as handle:
    original = registry.load(handle.read())

known = set(registry.names(original))
unknown = sorted(set(failures) - known)
if unknown:
    raise RuntimeError(
        f"markers name sources the watermark does not carry: {unknown}. It carries "
        f"{sorted(known)}."
    )

untranslatable = sorted(name for name in known if name not in source_dependency.SOURCE_OF)
if untranslatable:
    raise RuntimeError(
        f"watermark sources with no short name in the dependency chain: "
        f"{untranslatable}"
    )

print(f"{len(known)} sources, {len(failures)} with failures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Record one run per source
# MAGIC
# MAGIC `pipeline_run` names sources by the short name the audit registry declares, not
# MAGIC by the watermark's. The two sets are the same six and the map between them is
# MAGIC fixed in the dependency chain.
# MAGIC
# MAGIC A failed source carries the first marker's error into the row, and the count of
# MAGIC markers into the message, so a run that lost one year of thirty-two reads
# MAGIC differently from one that lost the lot.

# COMMAND ----------


class BronzeCopyFailed(Exception):
    """Carries an ADF Copy failure into the audit trail.

    The exception is built here rather than raised: the failure happened in ADF and
    is already over. `AuditRun.fail` records without re-raising, which is what lets
    one source fail and the run continue.
    """


recorded: dict[str, str] = {}

for entry in original:
    watermark_name = entry["source_name"]
    source = source_dependency.SOURCE_OF[watermark_name]
    markers = failures.get(watermark_name, [])

    run = AuditRun(
        source=source,
        layer="bronze",
        ingestion_ts=RUN_STARTED,
        job_run_id=JOB_RUN_ID,
    )
    run.start()

    if markers:
        first = min(markers, key=lambda m: m.get("failed_at", ""))
        run.fail(
            BronzeCopyFailed(
                f"{len(markers)} target(s) failed, first {first.get('target', '?')}: "
                f"{first.get('error', '')}"
            )
        )
        recorded[source] = "failed"
    else:
        run.succeed()
        recorded[source] = "succeeded"

    print(f"  {source:8} {recorded[source]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Advance last_refreshed
# MAGIC
# MAGIC Only for the sources that came through clean. A failed source keeps the date of
# MAGIC its last good run, which is what makes the next run retry it: `latest_release`
# MAGIC still sits at or above a `last_refreshed` that never moved, so ADF fetches it
# MAGIC again with nothing having to remember that it failed.
# MAGIC
# MAGIC `latest_release` is not touched here. That belongs to the publisher and is
# MAGIC written by `01_pre_run_resolve_urls`.

# COMMAND ----------

# Fixed in UTC rather than left to the driver's clock. `latest_release` is read from
# HTTP Last-Modified, which is UTC by protocol, so a local-time date here would run a
# day ahead through the BST boundary hour and skip a release that had landed.
today = RUN_STARTED.astimezone(dt.timezone.utc).date().isoformat()
entries = original

for entry in original:
    watermark_name = entry["source_name"]
    if watermark_name in failures:
        continue
    entries = registry.update(entries, watermark_name, {"last_refreshed": today})

difference = registry.changed_fields(original, entries)

if not difference:
    print("watermark unchanged: every source failed")
else:
    for source_name, fields in difference.items():
        before, after = fields["last_refreshed"]
        print(f"  {source_name:34} {before}  ->  {after}")

    text = registry.dump(entries, original=original)
    with open(WATERMARK_PATH, "w", encoding="utf-8") as handle:
        handle.write(text)
    print(f"\nwritten to {WATERMARK_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Record the plan
# MAGIC
# MAGIC The plan is not passed to the job. Each stage queries `pipeline_run` for this
# MAGIC run's failures and computes it again, so a task run on its own reaches the same
# MAGIC answer as one run in sequence.
# MAGIC
# MAGIC It is still recorded, under a run of its own, because a skipped stage writes a
# MAGIC row saying it waited and a stage the job never reached writes nothing at all.
# MAGIC Absence cannot tell those apart, and a plan recomputed later would answer under
# MAGIC whatever chain is in force then rather than the one that ran. A stage recorded
# MAGIC here as `run` with no run row of its own is one the job never got to.
# MAGIC
# MAGIC This is also the only row saying `02` itself ran. The six above name sources.
# MAGIC
# MAGIC The plan covers the nineteen stages the chain carries. The cross-source check is
# MAGIC not among them: it gates on evidence that no stage has produced yet at Bronze
# MAGIC close, so anything recorded for it here would be a guess.
# MAGIC
# MAGIC Comparing what was planned against what the stages went on to record is what
# MAGIC shows where a run degraded after this point: planned `run` against an actual
# MAGIC skip is a Silver stage that failed mid-run.

# COMMAND ----------

plan_run = AuditRun(
    source="pipeline_plan",
    layer="bronze",
    ingestion_ts=RUN_STARTED,
    job_run_id=JOB_RUN_ID,
)
plan_run.start()

with plan_run.step():
    failed_now = {source for source, status in recorded.items() if status == "failed"}
    cascades, inert = source_dependency.cascading(failed_now)
    plan = source_dependency.plan(cascades)

    running = set(source_dependency.ordered(plan))
    for stage in (
        *source_dependency.SOURCES,
        *source_dependency.DIMENSIONS,
        *source_dependency.FACTS,
    ):
        plan_run.measure(
            "planned_stage", "run" if stage in running else "skip", scope=stage
        )

plan_run.succeed()

print(f"plan run {plan_run.run_id}")
print(f"failed at bronze : {sorted(failed_now) or 'none'}")
if inert:
    print(f"reported only    : {sorted(inert)}")
print(f"silver           : {plan['silver']}")
print(f"dimensions       : {plan['dimensions']}")
print(f"facts            : {plan['facts']}")
print(f"\n{len(running)} of 19 stages will run, recorded as planned_stage")
print(f"finished in {(dt.datetime.now() - RUN_STARTED).total_seconds():.1f}s")
