# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: House Price Index (HPI)
# MAGIC
# MAGIC Reads the newest UK House Price Index vintage from the Bronze Volume and
# MAGIC overwrites `uk_property_intel.silver.hpi`.
# MAGIC
# MAGIC The file is cumulative and revises the previous twelve months on every
# MAGIC release, so a full overwrite from the newest vintage is the correct write.
# MAGIC
# MAGIC What the run measured is written to `uk_property_intel.quality.pipeline_run`
# MAGIC and `pipeline_metric` rather than only printed. `run.step()` wraps whichever
# MAGIC cells can raise, so a failure records why before it stops the notebook.

# COMMAND ----------

import re
from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.orchestration import stage
from databricks_src.silver.transforms.hpi import (
    SILVER_COLUMNS,
    native_start_year,
    silver_table_ddl,
    transform_hpi,
)

VOLUME_PATH = "/Volumes/uk_property_intel/bronze/hpi"
TARGET_TABLE = "uk_property_intel.silver.hpi"

# The name this source carries in the audit registry and in the dependency chain.
# The only line the two gate cells below differ by across the six Silver notebooks.
SOURCE = "hpi"
# Case-insensitive: the landed name is lowercased relative to the source URL.
VINTAGE_PATTERN = re.compile(r"^uk-hpi-full-file-(\d{4})-(\d{2})\.csv$", re.IGNORECASE)

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
# MAGIC ## Select the vintage

# COMMAND ----------

with run.step():
    # Vintage comes from the filename, not modificationTime: re-fetching an older
    # release would reorder by timestamp.
    entries = dbutils.fs.ls(VOLUME_PATH)  # noqa: F821
    vintages = [
        (VINTAGE_PATTERN.match(entry.name).groups(), entry)
        for entry in entries
        if VINTAGE_PATTERN.match(entry.name)
    ]
    if not vintages:
        raise FileNotFoundError(
            f"No file matching {VINTAGE_PATTERN.pattern} under {VOLUME_PATH}. "
            f"Present: {[entry.name for entry in entries]}"
        )

    label, source_file = max(vintages, key=lambda item: item[0])
    SOURCE_PATH = f"{VOLUME_PATH}/{source_file.name}"

    run.measure("vintages_present", len(vintages))
    run.measure("vintage_label", "-".join(label))
    run.measure("source_files", 1)
    run.measure("source_bytes", source_file.size)

print(f"{len(vintages)} vintage(s) present, reading {source_file.name}")
print(f"{source_file.size / 1024 ** 2:,.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read

# COMMAND ----------

with run.step():
    # Types are asserted in the transform, never inferred: decimal precision in this
    # file varies between releases.
    raw = (
        spark.read.option("header", True)  # noqa: F821
        .option("inferSchema", False)
        .csv(source_file.path)
    )
    # The transform runs five guards, each an action. Without this the file is parsed
    # once per guard and again per action on the frame the transform returns.
    raw.persist(StorageLevel.DISK_ONLY)
    source_rows = run.measure("source_rows", raw.count())

print(f"{source_rows:,} source rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform

# COMMAND ----------

with run.step():
    silver = transform_hpi(
        raw_df=raw,
        source_file=SOURCE_PATH,
        ingestion_ts=INGESTION_TS,
    )

    # One pass carries every measure below. A global aggregate for the row count and
    # a separate groupBy for coverage would evaluate the transform twice.
    by_geography = (
        silver.groupBy("area_code")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.max("date").alias("last_month"),
            # Constant within a geography, so any aggregate returns it. Read off the
            # module rather than restated here, since it is the rule the floor used.
            F.min(native_start_year()).alias("start_year"),
        )
        .collect()
    )

    silver_rows = sum(row["rows"] for row in by_geography)
    newest_month = max(row["last_month"] for row in by_geography)

    run.measure("silver_rows", silver_rows)
    run.measure("geographies", len(by_geography))
    # The filename labels a release; this is what the file holds. HPI data lags its
    # release label, so these differ by design.
    lag = run.freshness(newest_month, skip=SKIP_FRESHNESS)

print(f"{silver_rows:,} rows after the coverage floor")
print(f"{len(by_geography):,} geographies")
print(f"filename vintage {source_file.name}, "
      f"newest month in data {newest_month}")
print(f"{lag} days between that month and this run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geography coverage
# MAGIC
# MAGIC Which geographies reported in the newest month, against those reporting anywhere
# MAGIC in the last twelve. The panel is not rectangular across its whole history, since
# MAGIC each nation floors at its own measured start, but a geography active in the
# MAGIC recent window and absent from the newest month is a coverage change rather than
# MAGIC a floor.
# MAGIC
# MAGIC Measured rather than asserted. Local authorities merge and split, so an
# MAGIC assertion here would fire on a boundary change that is a fact about the country
# MAGIC rather than a fault in the load.

# COMMAND ----------

with run.step():
    # First month of the twelve ending at newest_month. Month arithmetic on a
    # zero-based ordinal: year * 12 + month - 1 keeps December in its own year, which
    # a year * 12 + month form does not.
    _ordinal = newest_month.year * 12 + newest_month.month - 1 - 11
    window_start = date(_ordinal // 12, _ordinal % 12 + 1, 1)

    active = [row for row in by_geography if row["last_month"] >= window_start]
    absent = sorted(
        row["area_code"] for row in active if row["last_month"] != newest_month
    )

    run.measure(
        "entities_in_newest_period",
        len(active) - len(absent),
        denominator=len(active),
    )
    if absent:
        run.measure("entities_absent_from_newest_period", absent)

    # Months from January of the coverage floor year through newest_month. A geography
    # short of that started late or ended early, which the transform allows and this
    # counts. A hole inside a series aborts there instead.
    complete = [
        row
        for row in by_geography
        if row["rows"]
        == (newest_month.year - row["start_year"]) * 12 + newest_month.month
    ]
    run.measure(
        "geographies_with_a_full_series",
        len(complete),
        denominator=len(by_geography),
    )

print(f"{len(active) - len(absent):,} of {len(active):,} geographies active since "
      f"{window_start} report in {newest_month}")
print(f"absent: {absent or 'none'}")
print(f"{len(complete):,} of {len(by_geography):,} carry every month from their floor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Silver table and apply CHECK constraints
# MAGIC
# MAGIC The column list is generated from the transform module, so the DDL cannot
# MAGIC drift from the cast types. The table is created once (`IF NOT EXISTS`);
# MAGIC constraints are dropped and re-added each run so the notebook is idempotent.
# MAGIC Single-row invariants live here; multi-row invariants (unique grain, per-nation
# MAGIC coverage floors) belong in the chispa test.

# COMMAND ----------

_ = spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    {silver_table_ddl()}
    )
    USING DELTA
    COMMENT 'UK House Price Index as a monthly geography panel. One row per (area_code, date) over each geography measured era: England and Wales from 1995, Scotland from 2004, Northern Ireland from 2005, composites from the latest start they span.'
    """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   DROP CONSTRAINT IF EXISTS date_is_month_start;
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   ADD CONSTRAINT date_is_month_start
# MAGIC   CHECK (day(date) = 1);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   DROP CONSTRAINT IF EXISTS date_at_or_after_coverage_floor;
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   ADD CONSTRAINT date_at_or_after_coverage_floor
# MAGIC   CHECK (date >= DATE '1995-01-01');
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   DROP CONSTRAINT IF EXISTS avg_price_positive;
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   ADD CONSTRAINT avg_price_positive
# MAGIC   CHECK (avg_price IS NULL OR avg_price > 0);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   DROP CONSTRAINT IF EXISTS price_index_positive;
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   ADD CONSTRAINT price_index_positive
# MAGIC   CHECK (price_index IS NULL OR price_index > 0);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   DROP CONSTRAINT IF EXISTS sales_volume_nonneg;
# MAGIC ALTER TABLE uk_property_intel.silver.hpi
# MAGIC   ADD CONSTRAINT sales_volume_nonneg
# MAGIC   CHECK (sales_volume IS NULL OR sales_volume >= 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write (full refresh)
# MAGIC
# MAGIC `INSERT OVERWRITE` preserves the table schema, comment, and CHECK constraints;
# MAGIC only the data is replaced.

# COMMAND ----------

with run.step():
    # INSERT OVERWRITE matches on position, so a projection that drifts from the DDL
    # would load values into the wrong columns.
    assert tuple(silver.columns) == SILVER_COLUMNS, silver.columns

    silver.createOrReplaceTempView("_hpi_silver_staging")
    spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} TABLE _hpi_silver_staging")  # noqa: F821
    written = spark.table(TARGET_TABLE).count()  # noqa: F821

_ = raw.unpersist()
run.succeed(rows_written=written)
print(f"Wrote {written:,} rows to {TARGET_TABLE}")
print(f"run {run.run_id} recorded as succeeded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows,
# MAGIC        count(DISTINCT area_code) AS geographies,
# MAGIC        min(date) AS first_month,
# MAGIC        max(date) AS last_month
# MAGIC FROM uk_property_intel.silver.hpi

# COMMAND ----------

# MAGIC %md
# MAGIC Earliest month per nation should land on each nation's measured start:
# MAGIC 1995 for England and Wales, 2004 for Scotland, 2005 for Northern Ireland,
# MAGIC and a composite on the latest start it spans.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT left(area_code, 1) AS code_prefix,
# MAGIC        min(date) AS first_month,
# MAGIC        count(DISTINCT area_code) AS geographies
# MAGIC FROM uk_property_intel.silver.hpi
# MAGIC GROUP BY left(area_code, 1)
# MAGIC ORDER BY code_prefix

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT area_code, region_name, min(date) AS first_month
# MAGIC FROM uk_property_intel.silver.hpi
# MAGIC WHERE left(area_code, 1) = 'K'
# MAGIC GROUP BY area_code, region_name
# MAGIC ORDER BY area_code

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, region_name, area_code, avg_price, price_index, sales_volume
# MAGIC FROM uk_property_intel.silver.hpi
# MAGIC WHERE area_code = 'K02000001'
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 12

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, sales_volume FROM uk_property_intel.silver.hpi
# MAGIC WHERE area_code = 'K02000001' AND date BETWEEN '2025-01-01' AND '2025-05-01'
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this run recorded

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.status, r.started_ts, r.ended_ts, r.rows_written, r.error_type,
# MAGIC        m.metric, m.value_numeric, m.value_text, m.value_date, m.denominator
# MAGIC FROM uk_property_intel.quality.pipeline_run r
# MAGIC LEFT JOIN uk_property_intel.quality.pipeline_metric m USING (run_id)
# MAGIC WHERE r.source = 'hpi'
# MAGIC ORDER BY r.started_ts DESC, m.metric
