# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS configs_managed
# MAGIC   URL 'abfss://configs@ukpropertyintelligencedl.dfs.core.windows.net/'
# MAGIC   WITH (STORAGE CREDENTIAL `uk-property-market-intelligence-platform-sc`)
# MAGIC   COMMENT 'The configs container. Governs the watermark ADF reads before any compute exists, and the per-run Bronze failure markers.';

# COMMAND ----------

# Run after 01_create_schemas.
dbutils.fs.ls("abfss://configs@ukpropertyintelligencedl.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS uk_property_intel.configs.watermark
# MAGIC   LOCATION 'abfss://configs@ukpropertyintelligencedl.dfs.core.windows.net/'
# MAGIC   COMMENT 'The configs container. watermark.json is the single JSON array ADF reads through its Lookup; log/ holds one marker per failed Bronze copy and is cleared at the start of every run, so an empty folder means every copy succeeded.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW VOLUMES lists names only.
# MAGIC SELECT volume_name, volume_type, storage_location
# MAGIC FROM uk_property_intel.information_schema.volumes
# MAGIC WHERE volume_schema = 'configs'
# MAGIC ORDER BY volume_name;

# COMMAND ----------

# End-to-end path resolution: the Silver BoE notebook reads this exact path.
dbutils.fs.ls("/Volumes/uk_property_intel/configs/watermark/")
