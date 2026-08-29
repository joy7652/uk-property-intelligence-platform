# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Price Paid Data (PPD)
# MAGIC
# MAGIC Reads every yearly Price Paid Data file from the Bronze Volume and overwrites
# MAGIC `uk_property_intel.silver.ppd`.
# MAGIC
# MAGIC Land Registry regenerates the yearly files on each monthly release, so a full
# MAGIC overwrite from the current vintage is the correct write. The monthly
# MAGIC change-only file is a separate feed and is not read here.
# MAGIC
# MAGIC What the run measured is written to `uk_property_intel.quality.pipeline_run`
# MAGIC and `pipeline_metric` rather than only printed. `run.step()` wraps whichever
# MAGIC cells can raise, so a failure records why before it stops the notebook.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.quality.audit.writer import AuditRun
from databricks_src.silver.transforms.ppd import (
    DOMAINS,
    SILVER_COLUMNS,
    silver_table_ddl,
    string_schema,
    transform_ppd,
)

VOLUME_PATH = "/Volumes/uk_property_intel/bronze/ppd"
YEARLY_ROOT = f"{VOLUME_PATH}/yearly"
YEARLY_GLOB = f"{YEARLY_ROOT}/*/pp-*.csv"
TARGET_TABLE = "uk_property_intel.silver.ppd"

# One timestamp for the whole run, stamped on every Silver row and carried on the
# audit row, so the two can be joined.
INGESTION_TS = datetime.now(timezone.utc)

# Set when rebuilding from a Bronze copy kept on purpose. The freshness bound cannot
# tell a deliberate rebuild from a stale release.
SKIP_FRESHNESS = False

# COMMAND ----------

run = AuditRun(source="ppd", layer="silver", ingestion_ts=INGESTION_TS)
run.start()
print(f"run {run.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory
# MAGIC
# MAGIC Every yearly file keeps its name across releases, so the filenames carry no
# MAGIC vintage and cannot distinguish a current fetch from a stale one. Total size is
# MAGIC the signal: Land Registry regenerates all 32 files monthly, and the bytes move
# MAGIC whenever they do. Not `modificationTime`, which records when a path was written
# MAGIC rather than when the content was fetched.

# COMMAND ----------

with run.step():
    # One directory level under yearly/. dbutils.fs.ls returns a file unchanged when
    # given one, so a flat layout would list correctly too.
    inventory = [
        item
        for entry in dbutils.fs.ls(YEARLY_ROOT)  # noqa: F821
        for item in dbutils.fs.ls(entry.path)  # noqa: F821
        if item.name.lower().endswith(".csv")
    ]
    if not inventory:
        raise FileNotFoundError(f"No yearly CSV found under {YEARLY_ROOT}.")

    source_bytes = sum(item.size for item in inventory)
    run.measure("source_files", len(inventory))
    run.measure("source_bytes", source_bytes)

print(f"{len(inventory)} files, {source_bytes / 1024 ** 3:,.2f} GB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read
# MAGIC
# MAGIC Headerless, so the schema is positional. `FAILFAST` aborts on a row whose field
# MAGIC count does not match. `nullValue` is pinned so empty address fields land as
# MAGIC null rather than as empty strings, whatever the runtime default.

# COMMAND ----------

with run.step():
    raw = (
        spark.read.option("header", False)  # noqa: F821
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "")
        .option("mode", "FAILFAST")
        .schema(string_schema())
        .csv(YEARLY_GLOB)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    # Every guard is an action. Without this the 32 files are parsed once per guard.
    raw.persist(StorageLevel.DISK_ONLY)
    source_rows = run.measure("source_rows", raw.count())

print(f"{source_rows:,} source rows across {len(inventory)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery
# MAGIC
# MAGIC The code sets in the transform abort the run on an unrecognised value. Run this
# MAGIC against a full vintage before trusting them, and again whenever Land Registry
# MAGIC changes the published column set.
# MAGIC
# MAGIC One pass over five columns. A `distinct` per column would scan the whole load
# MAGIC five times to answer a question about a handful of single-character codes.
# MAGIC
# MAGIC Not recorded as a metric: an unrecognised code aborts the transform, so the
# MAGIC failed run and its message are the record. A vocabulary that cannot drift
# MAGIC silently does not need trending.

# COMMAND ----------

observed = (
    raw.agg(*[F.collect_set(column).alias(column) for column in DOMAINS])
    .collect()[0]
    .asDict()
)
for column, values in DOMAINS.items():
    print(
        f"{column:<20}observed={sorted(observed[column])}  configured={sorted(values)}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform
# MAGIC
# MAGIC Every guard runs here, so a failure aborts before anything is written.
# MAGIC
# MAGIC TUID uniqueness is asserted inside the transform rather than measured here. A
# MAGIC `countDistinct` over the whole load is a full shuffle to confirm something the
# MAGIC guard has already proved by aborting if it were false.

# COMMAND ----------

with run.step():
    silver = transform_ppd(raw_df=raw, ingestion_ts=INGESTION_TS)

    # One pass. transfer_year has 32 values, so counting it distinctly here is free
    # next to the row count it rides along with.
    totals = silver.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("transfer_year").alias("years"),
        F.max("date_of_transfer").alias("newest"),
    ).collect()[0]

    run.measure("silver_rows", totals["rows"])
    run.measure("transfer_years", totals["years"])
    # The filenames label years; this is what the files hold. Registration lag puts
    # the newest transfer weeks to months behind the release.
    lag = run.freshness(totals["newest"], skip=SKIP_FRESHNESS)

print(f"{totals['rows']:,} rows after typing across {totals['years']} years")
print(f"newest transfer date in data {totals['newest']} ({lag} days before this run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Silver table and apply CHECK constraints
# MAGIC
# MAGIC The column list is generated from the transform module, so the DDL cannot drift
# MAGIC from the cast types. The table is created once (`IF NOT EXISTS`); constraints
# MAGIC are dropped and re-added each run so the notebook is idempotent. Single-row
# MAGIC invariants live here; multi-row invariants (TUID uniqueness, one transfer year
# MAGIC per source file) belong in the chispa test.
# MAGIC
# MAGIC Partitioning by `transfer_year` gives one partition per Bronze file, which is
# MAGIC what the planned reconcile overwrites with `replaceWhere`.

# COMMAND ----------

_ = spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    {silver_table_ddl()}
    )
    USING DELTA
    PARTITIONED BY (transfer_year)
    COMMENT 'HM Land Registry Price Paid Data, one row per residential transaction keyed on TUID, from 1995. Rebuilt in full from the current yearly files; the monthly change-only file is applied separately.'
    """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   DROP CONSTRAINT IF EXISTS transfer_year_matches_date;
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   ADD CONSTRAINT transfer_year_matches_date
# MAGIC   CHECK (transfer_year = year(date_of_transfer));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   DROP CONSTRAINT IF EXISTS transfer_date_at_or_after_series_start;
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   ADD CONSTRAINT transfer_date_at_or_after_series_start
# MAGIC   CHECK (date_of_transfer >= DATE '1995-01-01');
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   DROP CONSTRAINT IF EXISTS price_nonneg;
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   ADD CONSTRAINT price_nonneg
# MAGIC   CHECK (price IS NULL OR price >= 0);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   DROP CONSTRAINT IF EXISTS ppd_category_type_known;
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   ADD CONSTRAINT ppd_category_type_known
# MAGIC   CHECK (ppd_category_type IN ('A', 'B'));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   DROP CONSTRAINT IF EXISTS property_type_known;
# MAGIC ALTER TABLE uk_property_intel.silver.ppd
# MAGIC   ADD CONSTRAINT property_type_known
# MAGIC   CHECK (property_type IN ('D', 'S', 'T', 'F', 'O'));

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write (full refresh)
# MAGIC
# MAGIC `INSERT OVERWRITE` preserves the table schema, comment, partitioning, and CHECK
# MAGIC constraints; only the data is replaced.

# COMMAND ----------

with run.step():
    # INSERT OVERWRITE matches on position, so a projection that drifts from the DDL
    # would load values into the wrong columns.
    assert tuple(silver.columns) == SILVER_COLUMNS, silver.columns

    silver.createOrReplaceTempView("_ppd_silver_staging")
    spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} TABLE _ppd_silver_staging")  # noqa: F821
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
# MAGIC        count(DISTINCT tuid) AS distinct_tuid,
# MAGIC        count(DISTINCT transfer_year) AS years,
# MAGIC        min(date_of_transfer) AS earliest,
# MAGIC        max(date_of_transfer) AS latest
# MAGIC FROM uk_property_intel.silver.ppd

# COMMAND ----------

# MAGIC %md
# MAGIC Row counts by year carry visible market history: near 1.2M through the early
# MAGIC 2000s, a trough around 650,000 across 2008 to 2012, and a 2021 peak from the
# MAGIC stamp duty holiday. A reload that flattens those contours has lost data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT transfer_year, count(*) AS rows
# MAGIC FROM uk_property_intel.silver.ppd
# MAGIC GROUP BY transfer_year
# MAGIC ORDER BY transfer_year

# COMMAND ----------

# MAGIC %md
# MAGIC Category B capture began 14 October 2013. The files key on transfer date rather
# MAGIC than registration date, so a thin tail of earlier transfers appears from
# MAGIC transactions registered after that: under 2,000 rows before 2013 against 1.81M in
# MAGIC total, thickening toward 2013 as registration lag shortens the gap. A large
# MAGIC pre-2013 B population would mean the column is misread.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ppd_category_type,
# MAGIC        min(date_of_transfer) AS first_seen,
# MAGIC        count(*) AS rows
# MAGIC FROM uk_property_intel.silver.ppd
# MAGIC GROUP BY ppd_category_type
# MAGIC ORDER BY ppd_category_type

# COMMAND ----------

# MAGIC %md
# MAGIC Each partition must hold only its own year, which is what `replaceWhere`
# MAGIC assumes. This returns no rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT transfer_year,
# MAGIC        min(date_of_transfer) AS lo,
# MAGIC        max(date_of_transfer) AS hi
# MAGIC FROM uk_property_intel.silver.ppd
# MAGIC GROUP BY transfer_year
# MAGIC HAVING year(min(date_of_transfer)) <> transfer_year
# MAGIC     OR year(max(date_of_transfer)) <> transfer_year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT property_type, duration, count(*) AS rows
# MAGIC FROM uk_property_intel.silver.ppd
# MAGIC GROUP BY property_type, duration
# MAGIC ORDER BY rows DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tuid, price, date_of_transfer, postcode, property_type, duration, town_city
# MAGIC FROM uk_property_intel.silver.ppd
# MAGIC WHERE transfer_year = 2019
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
# MAGIC WHERE r.source = 'ppd'
# MAGIC ORDER BY r.started_ts DESC, m.metric
