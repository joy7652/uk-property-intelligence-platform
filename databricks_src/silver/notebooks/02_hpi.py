# Databricks notebook source
# MAGIC %md
# MAGIC # HPI to Silver
# MAGIC
# MAGIC Reads the newest UK House Price Index vintage from the Bronze Volume and
# MAGIC overwrites `uk_property_intel.silver.hpi`.
# MAGIC
# MAGIC The file is cumulative and revises the previous twelve months on every
# MAGIC release, so a full overwrite from the newest vintage is the correct write.

# COMMAND ----------

import re
from datetime import datetime, timezone

from pyspark.sql import functions as F

from databricks_src.silver.transforms.hpi import (
    SILVER_COLUMNS,
    silver_table_ddl,
    transform_hpi,
)

VOLUME_PATH = "/Volumes/uk_property_intel/bronze/hpi"
TARGET_TABLE = "uk_property_intel.silver.hpi"
# Case-insensitive: the landed name is lowercased relative to the source URL.
VINTAGE_PATTERN = re.compile(r"^uk-hpi-full-file-(\d{4})-(\d{2})\.csv$", re.IGNORECASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the vintage

# COMMAND ----------

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

_, source_file = max(vintages, key=lambda item: item[0])
SOURCE_PATH = f"{VOLUME_PATH}/{source_file.name}"
print(f"{len(vintages)} vintage(s) present, reading {source_file.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read

# COMMAND ----------

# Types are asserted in the transform, never inferred: decimal precision in this
# file varies between releases.
raw = (
    spark.read.option("header", True)  # noqa: F821
    .option("inferSchema", False)
    .csv(source_file.path)
)

print(f"{raw.count():,} source rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform

# COMMAND ----------

silver = transform_hpi(
    raw_df=raw,
    source_file=SOURCE_PATH,
    ingestion_ts=datetime.now(timezone.utc),
)

print(f"{silver.count():,} rows after the coverage floor")

# The filename labels a release; this is what the file holds. HPI data lags its
# release label, so these differ by design, but a large gap means the watermark
# is fetching a stale release.
newest_month = silver.agg(F.max("date")).collect()[0][0]
print(f"filename vintage {source_file.name}, newest month in data {newest_month}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Silver table and apply CHECK constraints
# MAGIC
# MAGIC The column list is generated from the transform module, so the DDL cannot
# MAGIC drift from the cast types. The table is created once (`IF NOT EXISTS`);
# MAGIC constraints are dropped and re-added each run so the notebook is idempotent.
# MAGIC Single-row invariants live here; multi-row invariants (unique grain, per-nation
# MAGIC coverage floors) belong in the chispa test.
# MAGIC
# MAGIC **One-time step.** The table predates this definition, so it carries an
# MAGIC inferred schema with no `NOT NULL` and no comment. Run
# MAGIC `DROP TABLE uk_property_intel.silver.hpi` by hand once before the first run of
# MAGIC this notebook. A `DROP` is deliberately not committed here: a bare drop in a
# MAGIC script that runs every time destroys the table on every rebuild.

# COMMAND ----------

spark.sql(  # noqa: F821
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

# INSERT OVERWRITE matches on position, so a projection that drifts from the DDL
# would load values into the wrong columns.
assert tuple(silver.columns) == SILVER_COLUMNS, silver.columns

silver.createOrReplaceTempView("_hpi_silver_staging")
spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} TABLE _hpi_silver_staging")  # noqa: F821
print(f"Wrote {spark.table(TARGET_TABLE).count():,} rows to {TARGET_TABLE}")  # noqa: F821

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
