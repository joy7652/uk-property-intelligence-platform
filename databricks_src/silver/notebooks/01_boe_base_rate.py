# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Bank of England base rate
# MAGIC
# MAGIC **Source:** `bronze/boe/base_rate/baserate.xls`, sheet `Raw Data`
# MAGIC **Target:** `uk_property_intel.silver.boe_base_rate` (Delta, Type 2 SCD)
# MAGIC
# MAGIC Read schema, DQ guard, and the transform live in
# MAGIC `databricks_src/silver/transforms/boe_base_rate.py`. This notebook is the
# MAGIC I/O wrapper: read → validate → transform → write.

# COMMAND ----------

from datetime import datetime, timezone

from databricks_src.silver.transforms.boe_base_rate import (
    RAW_DATA_SCHEMA,
    SILVER_COLUMNS,
    assert_rate_columns_consistent,
    transform_boe_base_rate,
)

CATALOG      = "uk_property_intel"
SOURCE_PATH  = f"/Volumes/{CATALOG}/bronze/boe/base_rate/baserate.xls"
TARGET_TABLE = f"{CATALOG}.silver.boe_base_rate"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the `Raw Data` sheet
# MAGIC
# MAGIC spark-excel with the explicit schema and `dataAddress` anchored at the
# MAGIC header row (file row 2).

# COMMAND ----------

raw_df = (
    spark.read  # noqa: F821
    .format("dev.mauch.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'Raw Data'!A2")
    .schema(RAW_DATA_SCHEMA)
    .load(SOURCE_PATH)
)

print(f"Daily rows read: {raw_df.count():,}")
raw_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data-quality guard
# MAGIC
# MAGIC Fail loud if any daily row carries conflicting values across the five
# MAGIC rate columns. On the two known regime-changeover days two columns are
# MAGIC populated; this is only acceptable when they agree.

# COMMAND ----------

assert_rate_columns_consistent(raw_df)
print("Rate-column consistency check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transform: coalesce → collapse daily→event → SCD2

# COMMAND ----------

silver_df = transform_boe_base_rate(
    raw_df=raw_df,
    source_file=SOURCE_PATH,
    ingestion_ts=datetime.now(timezone.utc),
)

print(f"Rate-change events produced: {silver_df.count():,}\n")
print("Earliest 5:")
silver_df.orderBy("effective_date").show(5, truncate=False)
print("Latest 5:")
silver_df.orderBy("effective_date", ascending=False).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create the Silver table and apply CHECK constraints
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
# MAGIC ## 5. Write Silver (full refresh)
# MAGIC
# MAGIC BoE refreshes the entire workbook each release, so Silver is a full
# MAGIC overwrite. `INSERT OVERWRITE` preserves the table schema and CHECK
# MAGIC constraints; only the data is replaced.

# COMMAND ----------

# INSERT OVERWRITE matches on position, so a projection that drifts from the DDL
# would load values into the wrong columns.
assert tuple(silver_df.columns) == SILVER_COLUMNS, silver_df.columns

silver_df.createOrReplaceTempView("_boe_silver_staging")
spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} TABLE _boe_silver_staging")  # noqa: F821
print(f"Wrote {spark.table(TARGET_TABLE).count():,} rows to {TARGET_TABLE}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify

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
