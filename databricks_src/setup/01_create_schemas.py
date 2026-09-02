# Databricks notebook source
# MAGIC %md
# MAGIC # Create Unity Catalog schemas
# MAGIC One schema per medallion layer: bronze, silver, gold, quality.
# MAGIC Run this before `02_create_bronze_volumes`.
# MAGIC
# MAGIC Bronze takes no managed location: it holds External Volumes only, so there are no managed objects to place.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS uk_property_intel.bronze
# MAGIC   COMMENT 'Raw files as landed by ADF. External Volumes provide UC governance, lineage, and discoverability over the bronze container without converting raw data to Delta. No managed location by design.';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS uk_property_intel.silver
# MAGIC   MANAGED LOCATION 'abfss://silver@ukpropertyintelligencedl.dfs.core.windows.net/'
# MAGIC   COMMENT 'Cleaned, typed, deduped per-source tables. One table per Bronze source.';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS uk_property_intel.gold
# MAGIC   MANAGED LOCATION 'abfss://gold@ukpropertyintelligencedl.dfs.core.windows.net/'
# MAGIC   COMMENT 'Multi-source enriched marts. Kimball-style facts and dimensions for analytical consumption.';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS uk_property_intel.quality
# MAGIC   MANAGED LOCATION 'abfss://quality@ukpropertyintelligencedl.dfs.core.windows.net/'
# MAGIC   COMMENT 'Data quality framework outputs: quarantine tables for rejected records, rule run history, DQ metrics.';
# MAGIC   
# MAGIC CREATE SCHEMA IF NOT EXISTS uk_property_intel.configs
# MAGIC   COMMENT 'Orchestration state read and written outside the medallion layers. Holds the watermark ADF reads before any compute exists, and the failure markers each Bronze copy writes. External Volume over the configs container so the files ADF depends on carry the same governance and lineage as the data. No managed location by design.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN uk_property_intel;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Root Location must not be the bronze container.
# MAGIC DESCRIBE SCHEMA EXTENDED uk_property_intel.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED uk_property_intel.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED uk_property_intel.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED uk_property_intel.quality;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED uk_property_intel.configs;
