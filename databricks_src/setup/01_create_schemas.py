# Databricks notebook source
# MAGIC %md
# MAGIC # Create Unity Catalog schemas
# MAGIC One schema per medallion layer, bronze, silver, gold and quality, plus configs for orchestration state and marts for the dbt views over Gold.
# MAGIC Run this before `02_create_bronze_volumes`, and before the first `dbt build`: dbt is configured never to create a schema.
# MAGIC
# MAGIC Bronze, configs and marts take no managed location. Bronze and configs hold External Volumes only and marts holds views, so none of them has managed objects to place.

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
# MAGIC   COMMENT 'Kimball-style star over two geography grains: four dimensions and nine facts, each loaded by INSERT OVERWRITE.';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS uk_property_intel.quality
# MAGIC   MANAGED LOCATION 'abfss://quality@ukpropertyintelligencedl.dfs.core.windows.net/'
# MAGIC   COMMENT 'Pipeline audit and data quality results: run history, measured metrics and their registry, and threshold rule results.';
# MAGIC   
# MAGIC CREATE SCHEMA IF NOT EXISTS uk_property_intel.configs
# MAGIC   COMMENT 'Orchestration state read and written outside the medallion layers. Holds the watermark ADF reads before any compute exists, and the failure markers each Bronze copy writes. External Volume over the configs container so the files ADF depends on carry the same governance and lineage as the data. No managed location by design.';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS uk_property_intel.marts
# MAGIC   COMMENT 'Views over the Gold star, built by dbt from the dbt/ folder and each fixed to one grain for direct SQL readers. Created here, never by dbt. No managed location by design: a view stores nothing.';

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

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED uk_property_intel.marts;