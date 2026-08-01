# Databricks notebook source
# MAGIC %md
# MAGIC # PPD to Silver
# MAGIC
# MAGIC Reads every yearly Price Paid Data file from the Bronze Volume and overwrites
# MAGIC `uk_property_intel.silver.ppd`.
# MAGIC
# MAGIC Land Registry regenerates the yearly files on each monthly release, so a full
# MAGIC overwrite from the current vintage is the correct write. The monthly
# MAGIC change-only file is a separate feed and is not read here.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.silver.transforms.ppd import (
    DOMAINS,
    SILVER_COLUMNS,
    silver_table_ddl,
    string_schema,
    transform_ppd,
)

VOLUME_PATH = "/Volumes/uk_property_intel/bronze/ppd"
YEARLY_GLOB = f"{VOLUME_PATH}/yearly/*/pp-*.csv"
TARGET_TABLE = "uk_property_intel.silver.ppd"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read
# MAGIC
# MAGIC Headerless, so the schema is positional. `FAILFAST` aborts on a row whose field
# MAGIC count does not match. `nullValue` is pinned so empty address fields land as
# MAGIC null rather than as empty strings, whatever the runtime default.

# COMMAND ----------

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

files = raw.select("_source_file").distinct().count()
print(f"{raw.count():,} source rows across {files} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery
# MAGIC
# MAGIC The code sets in the transform abort the run on an unrecognised value. Run this
# MAGIC against a full vintage before trusting them, and again whenever Land Registry
# MAGIC changes the published column set.

# COMMAND ----------

for column in DOMAINS:
    observed = sorted(
        str(row[column]) for row in raw.select(column).distinct().collect()
    )
    print(f"{column:<20}observed={observed}  configured={sorted(DOMAINS[column])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform
# MAGIC
# MAGIC Every guard runs here, so a failure aborts before anything is written.

# COMMAND ----------

silver = transform_ppd(raw_df=raw, ingestion_ts=datetime.now(timezone.utc))

print(f"{silver.count():,} rows after typing")

# The filenames label years; this is what the files hold. A gap means the watermark
# is fetching a stale release.
newest = silver.agg(F.max("date_of_transfer")).collect()[0][0]
print(f"newest transfer date in data {newest}")

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

spark.sql(  # noqa: F821
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

# INSERT OVERWRITE matches on position, so a projection that drifts from the DDL
# would load values into the wrong columns.
assert tuple(silver.columns) == SILVER_COLUMNS, silver.columns

silver.createOrReplaceTempView("_ppd_silver_staging")
spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} TABLE _ppd_silver_staging")  # noqa: F821
print(f"Wrote {spark.table(TARGET_TABLE).count():,} rows to {TARGET_TABLE}")  # noqa: F821

_ = raw.unpersist()

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
