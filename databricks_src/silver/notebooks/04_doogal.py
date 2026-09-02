# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: UK postcode lookup (Doogal)
# MAGIC
# MAGIC Reads the quarterly UK postcode archive from the Bronze Volume and overwrites
# MAGIC `uk_property_intel.silver.doogal`.
# MAGIC
# MAGIC The publisher keeps no history and republishes latest state each quarter, so a
# MAGIC full overwrite is the correct write.
# MAGIC
# MAGIC The archive is a ZIP, which Spark cannot read in place, so the CSV is extracted
# MAGIC to cluster-local disk first. Bronze keeps the archive as published. The local
# MAGIC path makes this notebook single-node only: on a multi-node cluster the
# MAGIC executors do not share the driver's filesystem and the read fails outright,
# MAGIC which is cheap to notice and cheap to fix.
# MAGIC
# MAGIC What the run measured is written to `uk_property_intel.quality.pipeline_run`
# MAGIC and `pipeline_metric` rather than only printed. `run.step()` wraps whichever
# MAGIC cells can raise, so a failure records why before it stops the notebook.

# COMMAND ----------

import os
import shutil
import zipfile
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.orchestration import stage
from databricks_src.silver.transforms.doogal import (
    DOMAINS,
    SILVER_COLUMNS,
    silver_table_ddl,
    transform_doogal,
)

VOLUME_PATH = "/Volumes/uk_property_intel/bronze/doogal"
TARGET_TABLE = "uk_property_intel.silver.doogal"

# The name this source carries in the audit registry and in the dependency chain.
# The only line the two gate cells below differ by across the six Silver notebooks.
SOURCE = "doogal"

STAGE_DIR = "/local_disk0/doogal_stage"
INNER_CSV = "postcodes.csv"

# Local file header of a ZIP archive. The source is a static URL, so a failed fetch
# can land an error page under the expected name and report success.
ZIP_MAGIC_BYTES = b"PK\x03\x04"

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
# MAGIC ## Select the archive
# MAGIC
# MAGIC The archive lands at a fixed URL under a fixed filename, so the name carries no
# MAGIC vintage. Size is the only signal the artefact gives, and it is recorded here
# MAGIC rather than only printed. Not `modificationTime`, which records when a path was
# MAGIC written rather than when the content was fetched.

# COMMAND ----------

with run.step():
    entries = dbutils.fs.ls(VOLUME_PATH)  # noqa: F821
    archives = [entry for entry in entries if entry.name.lower().endswith(".zip")]
    if len(archives) != 1:
        raise FileNotFoundError(
            f"Expected exactly one archive under {VOLUME_PATH}, found "
            f"{[entry.name for entry in archives]}. Present: {[e.name for e in entries]}"
        )

    archive = archives[0]
    SOURCE_PATH = f"{VOLUME_PATH}/{archive.name}"
    run.measure("source_files", 1)
    run.measure("source_bytes", archive.size)

print(f"{archive.name}, {archive.size / 1024 ** 2:,.0f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate and extract
# MAGIC
# MAGIC The magic bytes are checked before the archive is opened. Phase 1 established
# MAGIC that a pipeline reporting success confirms bytes moved, not that the right
# MAGIC bytes moved, and this source fetches from a fixed URL that can answer with an
# MAGIC error page under the same filename.

# COMMAND ----------

with run.step():
    with open(SOURCE_PATH, "rb") as handle:
        magic = handle.read(len(ZIP_MAGIC_BYTES))
    if magic != ZIP_MAGIC_BYTES:
        raise ValueError(
            f"{SOURCE_PATH} is not a ZIP archive. Expected {ZIP_MAGIC_BYTES!r}, "
            f"found {magic!r}."
        )
print(f"magic bytes ok: {magic!r}")

# COMMAND ----------

with run.step():
    # Extracted fresh every run. A partial file from an interrupted run is
    # indistinguishable from a complete one, and the extract costs seconds.
    shutil.rmtree(STAGE_DIR, ignore_errors=True)
    os.makedirs(STAGE_DIR, exist_ok=True)

    with zipfile.ZipFile(SOURCE_PATH) as archive_file:
        members = [
            name for name in archive_file.namelist() if name.lower().endswith(".csv")
        ]
        if members != [INNER_CSV]:
            raise ValueError(
                f"Expected exactly {INNER_CSV} inside {archive.name}, found {members}."
            )
        archive_file.extract(INNER_CSV, STAGE_DIR)

    STAGE_PATH = f"{STAGE_DIR}/{INNER_CSV}"

print(f"extracted {os.path.getsize(STAGE_PATH) / 1024 ** 3:,.2f} GB to {STAGE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read
# MAGIC
# MAGIC Types are asserted in the transform, never inferred. `FAILFAST` aborts on a row
# MAGIC whose field count does not match the header rather than padding it with nulls.
# MAGIC `nullValue` is pinned so blank fields land as null whatever the runtime
# MAGIC default, which the terminated date and every optional geography depend on.

# COMMAND ----------

with run.step():
    raw = (
        spark.read.option("header", True)  # noqa: F821
        .option("inferSchema", False)
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "")
        .option("mode", "FAILFAST")
        .csv(f"file://{STAGE_PATH}")
    )

    # Every guard is an action. Without this the file is parsed once per guard.
    raw.persist(StorageLevel.DISK_ONLY)
    source_rows = run.measure("source_rows", raw.count())

print(f"{source_rows:,} source rows, {len(raw.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery
# MAGIC
# MAGIC The code sets in the transform abort the run on an unrecognised value. Run this
# MAGIC against a full release before trusting them, and again whenever the publisher
# MAGIC changes the column set. Null is expected in every one of them: it is the BFPO
# MAGIC block, which is non-geographic by design.
# MAGIC
# MAGIC Left as a loop rather than folded into one aggregate. The PPD run measured that
# MAGIC fold as no faster once the parse is cached, and a `collect_set` would drop the
# MAGIC nulls this cell exists to show.
# MAGIC
# MAGIC Not recorded as a metric: an unrecognised code aborts the transform, so the
# MAGIC failed run and its message are the record.

# COMMAND ----------

SOURCE_OF_DOMAIN = {
    "country": "Country",
    "positional_quality": "Quality",
    "user_type": "User Type",
    "london_travel_zone": "London zone",
}

for column, values in DOMAINS.items():
    observed = sorted(
        (
            row[0]
            for row in raw.select(f"`{SOURCE_OF_DOMAIN[column]}`").distinct().collect()
        ),
        key=lambda value: (value is None, value),
    )
    print(f"{column:<20}observed={observed}  configured={sorted(values)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform
# MAGIC
# MAGIC Every guard runs here, so a failure aborts before anything is written.
# MAGIC
# MAGIC One aggregate carries the row count, the live and terminated split, and the
# MAGIC freshness date. Terminated postcodes are kept, since transactions from 1995
# MAGIC reference postcodes since withdrawn, so the split is a property of the table
# MAGIC rather than a defect count.
# MAGIC
# MAGIC The BFPO block is excluded from the freshness date alone. It is frozen at 2018
# MAGIC and would mask the release date behind an older value.

# COMMAND ----------

with run.step():
    silver = transform_doogal(
        raw_df=raw,
        source_file=SOURCE_PATH,
        ingestion_ts=INGESTION_TS,
    )

    totals = silver.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count_if(F.col("terminated_date").isNull()).alias("live"),
        F.max(
            F.when(F.col("country").isNotNull(), F.col("source_last_updated"))
        ).alias("newest"),
    ).collect()[0]

    silver_rows = totals["rows"]
    run.measure("silver_rows", silver_rows)
    run.measure("live_postcodes", totals["live"], denominator=silver_rows)
    run.measure(
        "terminated_postcodes", silver_rows - totals["live"], denominator=silver_rows
    )
    lag = run.freshness(totals["newest"], skip=SKIP_FRESHNESS)

print(f"{silver_rows:,} rows after typing")
print(f"{totals['live']:,} live, {silver_rows - totals['live']:,} terminated")
print(f"newest source_last_updated in data {totals['newest']} "
      f"({lag} days before this run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Silver table and apply CHECK constraints
# MAGIC
# MAGIC The column list is generated from the transform module, so the DDL cannot drift
# MAGIC from the cast types. The table is created once (`IF NOT EXISTS`); constraints
# MAGIC are dropped and re-added each run so the notebook is idempotent. Single-row
# MAGIC invariants live here; multi-row invariants (postcode uniqueness, null geography
# MAGIC confined to the BF area) belong in the chispa test.
# MAGIC
# MAGIC No bounding box on the coordinates. The BFPO rows are legitimately in Lisbon,
# MAGIC Naples, and Stavanger.

# COMMAND ----------

_ = spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    {silver_table_ddl()}
    )
    USING DELTA
    COMMENT 'UK postcode lookup mirroring the ONS Postcode Directory, one row per postcode. Terminated postcodes are retained, since transactions reference postcodes since withdrawn. Coordinates are null where the source publishes no grid reference. The BF postcode area is British Forces Post Office and carries no UK geography.'
    """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   DROP CONSTRAINT IF EXISTS positional_quality_known;
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   ADD CONSTRAINT positional_quality_known
# MAGIC   CHECK (positional_quality IS NULL OR positional_quality BETWEEN 1 AND 9);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   DROP CONSTRAINT IF EXISTS user_type_known;
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   ADD CONSTRAINT user_type_known
# MAGIC   CHECK (user_type IS NULL OR user_type IN (0, 1));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   DROP CONSTRAINT IF EXISTS country_known;
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   ADD CONSTRAINT country_known
# MAGIC   CHECK (
# MAGIC     country IS NULL
# MAGIC     OR country IN ('England', 'Scotland', 'Wales', 'Northern Ireland')
# MAGIC   );
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   DROP CONSTRAINT IF EXISTS terminated_at_or_after_introduced;
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   ADD CONSTRAINT terminated_at_or_after_introduced
# MAGIC   CHECK (
# MAGIC     introduced_date IS NULL
# MAGIC     OR terminated_date IS NULL
# MAGIC     OR terminated_date >= introduced_date
# MAGIC   );
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   DROP CONSTRAINT IF EXISTS coordinates_paired;
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   ADD CONSTRAINT coordinates_paired
# MAGIC   CHECK ((latitude IS NULL) = (longitude IS NULL));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   DROP CONSTRAINT IF EXISTS coordinates_not_fabricated;
# MAGIC ALTER TABLE uk_property_intel.silver.doogal
# MAGIC   ADD CONSTRAINT coordinates_not_fabricated
# MAGIC   CHECK (latitude IS NULL OR NOT (latitude = 0 AND longitude = 0));

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

    silver.createOrReplaceTempView("_doogal_silver_staging")
    spark.sql(  # noqa: F821
        f"INSERT OVERWRITE {TARGET_TABLE} TABLE _doogal_silver_staging"
    )
    written = spark.table(TARGET_TABLE).count()  # noqa: F821

run.succeed(rows_written=written)
print(f"Wrote {written:,} rows to {TARGET_TABLE}")
print(f"run {run.run_id} recorded as succeeded")

# COMMAND ----------

# The read is complete, so the extracted copy and the cached parse can go. Local disk
# is shared with the next notebook on this cluster.
_ = raw.unpersist()
shutil.rmtree(STAGE_DIR, ignore_errors=True)
print(f"cleared {STAGE_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows,
# MAGIC        count(DISTINCT postcode) AS distinct_postcodes,
# MAGIC        count_if(terminated_date IS NULL) AS live,
# MAGIC        count_if(terminated_date IS NOT NULL) AS terminated,
# MAGIC        min(introduced_date) AS earliest_introduced,
# MAGIC        max(terminated_date) AS latest_terminated
# MAGIC FROM uk_property_intel.silver.doogal

# COMMAND ----------

# MAGIC %md
# MAGIC A null country is the British Forces Post Office block and nothing else. Those
# MAGIC rows carry coordinates at overseas bases and no UK geography at all.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT coalesce(country, 'none (BFPO)') AS country,
# MAGIC        count(*) AS rows,
# MAGIC        count_if(latitude IS NULL) AS no_coordinates,
# MAGIC        count_if(lsoa_code_2021 IS NULL) AS no_lsoa,
# MAGIC        max(source_last_updated) AS newest_update
# MAGIC FROM uk_property_intel.silver.doogal
# MAGIC GROUP BY 1
# MAGIC ORDER BY rows DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Coordinates are null for positional quality 9 and for nothing else. Every other
# MAGIC quality band should report zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT positional_quality,
# MAGIC        count(*) AS rows,
# MAGIC        count_if(latitude IS NULL) AS no_coordinates,
# MAGIC        count_if(easting IS NULL) AS no_grid_reference
# MAGIC FROM uk_property_intel.silver.doogal
# MAGIC GROUP BY positional_quality
# MAGIC ORDER BY positional_quality

# COMMAND ----------

# MAGIC %md
# MAGIC Postcode coverage against PPD, which is what this table exists for. Transactions
# MAGIC run back to 1995, so a share of them reference postcodes since withdrawn:
# MAGIC `resolved_via_terminated` is the population that would be lost if Silver kept
# MAGIC live postcodes only.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS ppd_postcodes,
# MAGIC        count_if(d.postcode IS NOT NULL) AS resolved,
# MAGIC        count_if(d.terminated_date IS NOT NULL) AS resolved_via_terminated
# MAGIC FROM (
# MAGIC   SELECT DISTINCT postcode
# MAGIC   FROM uk_property_intel.silver.ppd
# MAGIC   WHERE postcode IS NOT NULL
# MAGIC ) p
# MAGIC LEFT JOIN uk_property_intel.silver.doogal d ON d.postcode = p.postcode

# COMMAND ----------

# MAGIC %md
# MAGIC District code is the join key to HPI's local-authority geographies. A district
# MAGIC present here and absent from HPI is a boundary change rather than a fault, but a
# MAGIC large gap means the code vintages have diverged.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT d.district_code) AS doogal_districts,
# MAGIC        count(DISTINCT h.area_code) AS matched_in_hpi
# MAGIC FROM uk_property_intel.silver.doogal d
# MAGIC LEFT JOIN (
# MAGIC   SELECT DISTINCT area_code FROM uk_property_intel.silver.hpi
# MAGIC ) h ON h.area_code = d.district_code
# MAGIC WHERE d.district_code IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT postcode, introduced_date, terminated_date, latitude, longitude,
# MAGIC        positional_quality, country, region, district, lsoa_code_2021
# MAGIC FROM uk_property_intel.silver.doogal
# MAGIC WHERE postcode LIKE 'CT16%'
# MAGIC ORDER BY postcode
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this run recorded

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.status, r.started_ts, r.ended_ts, r.rows_written, r.error_type,
# MAGIC        m.metric, m.value_numeric, m.value_text, m.value_date, m.denominator
# MAGIC FROM uk_property_intel.quality.pipeline_run r
# MAGIC LEFT JOIN uk_property_intel.quality.pipeline_metric m USING (run_id)
# MAGIC WHERE r.source = 'doogal'
# MAGIC ORDER BY r.started_ts DESC, m.metric
