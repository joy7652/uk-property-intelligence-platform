# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: Bank of England base rate
# MAGIC
# MAGIC **Source:** `bronze/boe/base_rate/baserate.xls`, sheet `Raw Data`
# MAGIC **Target:** `uk_property_intel.silver.boe_base_rate` (Delta, Type 2 SCD)
# MAGIC
# MAGIC Read schema, DQ guard, and the transform live in
# MAGIC `databricks_src/silver/transforms/boe_base_rate.py`. This notebook is the
# MAGIC I/O wrapper: read → validate → transform → write.
# MAGIC
# MAGIC What the run measured is written to `uk_property_intel.quality.pipeline_run`
# MAGIC and `pipeline_metric` rather than only printed. `run.step()` wraps whichever
# MAGIC cells can raise, so a failure records why before it stops the notebook.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.orchestration import stage
from databricks_src.silver.transforms.boe_base_rate import (
    RATE_COLUMNS,
    RAW_DATA_SCHEMA,
    SILVER_COLUMNS,
    assert_rate_columns_consistent,
    transform_boe_base_rate,
)

CATALOG      = "uk_property_intel"
SOURCE_PATH  = f"/Volumes/{CATALOG}/bronze/boe/base_rate/baserate.xls"
TARGET_TABLE = f"{CATALOG}.silver.boe_base_rate"

# The name this source carries in the audit registry and in the dependency chain. The
# only line the two cells below differ by across the six Silver notebooks.
SOURCE = "boe"

# One timestamp for the whole run, stamped on every Silver row and carried on the
# audit row, so the two can be joined.
INGESTION_TS = datetime.now(timezone.utc)

# Set when rebuilding from a Bronze copy kept on purpose. The freshness bound cannot
# tell a deliberate rebuild from a stale release.
SKIP_FRESHNESS = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## The stage plan
# MAGIC
# MAGIC `job_run_id` comes from the job as `{{job.run_id}}` and is empty when this is
# MAGIC run by hand, which plans a full run: it belonged to no pipeline execution, so
# MAGIC there is nothing for it to wait on.
# MAGIC
# MAGIC The plan is recomputed here rather than passed in, so a task run on its own
# MAGIC reaches the same answer as one run in sequence. `orchestration/stage.py` holds
# MAGIC why what failed and what rebuilt are asked as two questions.
# MAGIC
# MAGIC The exit stays here rather than in the module, and outside any `run.step()`
# MAGIC block, since it raises for the notebook runner and `step` would record that as
# MAGIC a failure.

# COMMAND ----------

plan = stage.read_plan(dbutils)  # noqa: F821
run = stage.open_stage(SOURCE, "silver", INGESTION_TS, plan)

if run is None:
    dbutils.notebook.exit(f"skipped: {SOURCE}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the `Raw Data` sheet
# MAGIC
# MAGIC spark-excel with the explicit schema and `dataAddress` anchored at the
# MAGIC header row (file row 2).
# MAGIC
# MAGIC The workbook sits at a fixed path under a fixed name, so neither the run status
# MAGIC nor the filename distinguishes a current release from a stale one. Size is the
# MAGIC only signal the artefact carries; `modificationTime` is not usable, since a
# MAGIC container restructure rewrites it without re-fetching content.

# COMMAND ----------

with run.step():
    entry = dbutils.fs.ls(SOURCE_PATH)[0]  # noqa: F821
    run.measure("source_files", 1)
    run.measure("source_bytes", entry.size)
    print(f"{entry.name}, {entry.size / 1024 ** 2:,.2f} MB")

    raw_df = (
        spark.read  # noqa: F821
        .format("dev.mauch.spark.excel")
        .option("header", "true")
        .option("dataAddress", "'Raw Data'!A2")
        .schema(RAW_DATA_SCHEMA)
        .load(SOURCE_PATH)
    )
    # spark-excel parses the whole workbook per action, and the sheet is read by the
    # measure aggregate, the consistency guard, the transform, and the write.
    raw_df.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Measure the sheet
# MAGIC
# MAGIC Two dates, one pass, and they mean different things.
# MAGIC
# MAGIC The newest day carrying a rate is the freshness signal. The sheet is pre-filled
# MAGIC ahead of its save date to feed an embedded chart, but the pre-filled rows carry
# MAGIC the rate forward rather than blanking it, so the last row of the sheet is the
# MAGIC same day and adds nothing. The filter is kept anyway: were the BoE ever to
# MAGIC blank the pre-fill, this stays correct while an unfiltered `max` would not.
# MAGIC
# MAGIC The last rate change is measured after the transform and is not a freshness
# MAGIC signal. The rate has held since December 2025, so asserting on it would fire
# MAGIC every month while the pipeline is healthy.
# MAGIC
# MAGIC Because the series runs ahead of the save date, the lag reported here understates
# MAGIC the workbook's true age by about a month. `source_bytes` is the signal that the
# MAGIC file itself changed.
# MAGIC
# MAGIC No bound is set for any source yet. The values are recorded and each bound is
# MAGIC read off what the runs report.

# COMMAND ----------

with run.step():
    rate_pct = F.coalesce(*[F.col(name) for name, _ in RATE_COLUMNS])
    sheet = raw_df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.max(F.when(rate_pct.isNotNull(), F.col("date"))).alias("last_published"),
    ).collect()[0]

    run.measure("source_rows", sheet["rows"])
    lag = run.freshness(sheet["last_published"], skip=SKIP_FRESHNESS)

print(f"Daily rows read: {sheet['rows']:,}")
print(f"Last published rate: {sheet['last_published']} ({lag} days before this run)")
raw_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data-quality guard
# MAGIC
# MAGIC Fail loud if any daily row carries conflicting values across the five
# MAGIC rate columns. On the two known regime-changeover days two columns are
# MAGIC populated; this is only acceptable when they agree.

# COMMAND ----------

with run.step():
    assert_rate_columns_consistent(raw_df)
print("Rate-column consistency check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transform: coalesce → collapse daily→event → SCD2

# COMMAND ----------

with run.step():
    silver_df = transform_boe_base_rate(
        raw_df=raw_df,
        source_file=SOURCE_PATH,
        ingestion_ts=INGESTION_TS,
    )
    events = silver_df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.max("effective_date").alias("newest_event"),
    ).collect()[0]
    run.measure("silver_rows", events["rows"])
    run.measure("newest_rate_event_date", events["newest_event"])

print(f"Rate-change events produced: {events['rows']:,}")
print(f"Newest rate change: {events['newest_event']}\n")
print("Earliest 5:")
silver_df.orderBy("effective_date").show(5, truncate=False)
print("Latest 5:")
silver_df.orderBy("effective_date", ascending=False).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create the Silver table and apply CHECK constraints
# MAGIC
# MAGIC The table is created once (`IF NOT EXISTS`); constraints are dropped
# MAGIC and re-added each run so the notebook is idempotent. Single-row
# MAGIC invariants live here; multi-row invariants (exactly one current row,
# MAGIC no overlapping intervals) belong in the chispa test.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.silver.boe_base_rate (
# MAGIC     effective_date  DATE          NOT NULL,
# MAGIC     expiry_date     DATE,
# MAGIC     is_current      BOOLEAN       NOT NULL,
# MAGIC     rate_pct        DECIMAL(6, 4) NOT NULL,
# MAGIC     rate_type       STRING        NOT NULL,
# MAGIC     _source_file    STRING        NOT NULL,
# MAGIC     _ingestion_ts   TIMESTAMP     NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'BoE policy rate as a Type 2 SCD. One row per rate level with validity interval [effective_date, expiry_date]; the current rate has expiry_date IS NULL and is_current = true.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.silver.boe_base_rate
# MAGIC   DROP CONSTRAINT IF EXISTS expiry_after_effective;
# MAGIC ALTER TABLE uk_property_intel.silver.boe_base_rate
# MAGIC   ADD CONSTRAINT expiry_after_effective
# MAGIC   CHECK (expiry_date IS NULL OR expiry_date >= effective_date);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.boe_base_rate
# MAGIC   DROP CONSTRAINT IF EXISTS current_couples_with_open_interval;
# MAGIC ALTER TABLE uk_property_intel.silver.boe_base_rate
# MAGIC   ADD CONSTRAINT current_couples_with_open_interval
# MAGIC   CHECK (
# MAGIC     (is_current = true  AND expiry_date IS NULL)
# MAGIC     OR
# MAGIC     (is_current = false AND expiry_date IS NOT NULL)
# MAGIC   );
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.boe_base_rate
# MAGIC   DROP CONSTRAINT IF EXISTS rate_pct_nonneg;
# MAGIC ALTER TABLE uk_property_intel.silver.boe_base_rate
# MAGIC   ADD CONSTRAINT rate_pct_nonneg
# MAGIC   CHECK (rate_pct >= 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Silver (full refresh)
# MAGIC
# MAGIC BoE refreshes the entire workbook each release, so Silver is a full
# MAGIC overwrite. `INSERT OVERWRITE` preserves the table schema and CHECK
# MAGIC constraints; only the data is replaced.

# COMMAND ----------

with run.step():
    # INSERT OVERWRITE matches on position, so a projection that drifts from the DDL
    # would load values into the wrong columns.
    assert tuple(silver_df.columns) == SILVER_COLUMNS, silver_df.columns

    silver_df.createOrReplaceTempView("_boe_silver_staging")
    spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} TABLE _boe_silver_staging")  # noqa: F821
    written = spark.table(TARGET_TABLE).count()  # noqa: F821

_ = raw_df.unpersist()
run.succeed(rows_written=written)
print(f"Wrote {written:,} rows to {TARGET_TABLE}")
print(f"run {run.run_id} recorded as succeeded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT effective_date, expiry_date, is_current, rate_pct, rate_type
# MAGIC FROM uk_property_intel.silver.boe_base_rate
# MAGIC WHERE is_current = true
# MAGIC    OR effective_date >= DATE '2007-01-01'
# MAGIC ORDER BY effective_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)             AS total_rows,
# MAGIC        COUNT_IF(is_current) AS current_rows,
# MAGIC        MIN(effective_date)  AS earliest,
# MAGIC        MAX(effective_date)  AS latest_change,
# MAGIC        MIN(rate_pct)        AS min_rate,
# MAGIC        MAX(rate_pct)        AS max_rate
# MAGIC FROM uk_property_intel.silver.boe_base_rate;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. What this run recorded
# MAGIC
# MAGIC The audit row and its metrics, which is the first end-to-end check that the
# MAGIC writer works. The three dates should differ: the last published rate near today,
# MAGIC the series end at or past it, and the last rate change months behind both.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.status, r.started_ts, r.ended_ts, r.rows_written, r.error_type,
# MAGIC        m.metric, m.value_numeric, m.value_text, m.value_date, m.denominator
# MAGIC FROM uk_property_intel.quality.pipeline_run r
# MAGIC LEFT JOIN uk_property_intel.quality.pipeline_metric m USING (run_id)
# MAGIC WHERE r.source = 'boe'
# MAGIC ORDER BY r.started_ts DESC, m.metric
