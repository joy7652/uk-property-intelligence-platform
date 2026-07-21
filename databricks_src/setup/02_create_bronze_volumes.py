# Databricks notebook source
# Run after 01_create_schemas.
#
# Pre-flight: confirm the bronze external location is reachable and list the
# source roots the volumes below point at.
dbutils.fs.ls("abfss://bronze@ukpropertyintelligencedl.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One External Volume per source. Volume name matches the source identifier
# MAGIC -- used in the watermark and in the Silver table name, so each Silver
# MAGIC -- notebook reads from /Volumes/uk_property_intel/bronze/<source>/ and
# MAGIC -- writes to uk_property_intel.silver.<source>.
# MAGIC --
# MAGIC -- Every LOCATION is a source root, never a dataset subfolder. Notebooks
# MAGIC -- append the dataset path themselves, so a volume rooted one level too
# MAGIC -- deep resolves to a doubled path segment and 404s.
# MAGIC --
# MAGIC -- IF NOT EXISTS will not relocate an existing volume. If a LOCATION
# MAGIC -- changes here, DROP the live volume first (metadata only, no data moves).
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS uk_property_intel.bronze.boe
# MAGIC   LOCATION 'abfss://bronze@ukpropertyintelligencedl.dfs.core.windows.net/boe/'
# MAGIC   COMMENT 'Bank of England rate files. Currently contains base_rate/; structure anticipates additional rate types in future.';
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS uk_property_intel.bronze.hpi
# MAGIC   LOCATION 'abfss://bronze@ukpropertyintelligencedl.dfs.core.windows.net/land_registry/hpi/'
# MAGIC   COMMENT 'HM Land Registry UK House Price Index, cumulative monthly CSV.';
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS uk_property_intel.bronze.ppd
# MAGIC   LOCATION 'abfss://bronze@ukpropertyintelligencedl.dfs.core.windows.net/land_registry/ppd/'
# MAGIC   COMMENT 'HM Land Registry Price Paid Data, yearly CSVs from 1995.';
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS uk_property_intel.bronze.doogal
# MAGIC   LOCATION 'abfss://bronze@ukpropertyintelligencedl.dfs.core.windows.net/doogal/'
# MAGIC   COMMENT 'Doogal UK Postcode Lookup, ONSPD mirror, quarterly ZIP refresh.';
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS uk_property_intel.bronze.ons
# MAGIC   LOCATION 'abfss://bronze@ukpropertyintelligencedl.dfs.core.windows.net/ons/'
# MAGIC   COMMENT 'ONS source root. Currently contains private_rent_index/, monthly XLSX.';
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS uk_property_intel.bronze.police
# MAGIC   LOCATION 'abfss://bronze@ukpropertyintelligencedl.dfs.core.windows.net/police/'
# MAGIC   COMMENT 'UK Police source root. Currently contains crime/: rolling 3-year snapshot ZIPs, 2-year-stepped historical plus monthly latest.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW VOLUMES lists names only. storage_location is what catches a
# MAGIC -- wrong-rooted volume, which is the failure this layout hit once already.
# MAGIC SELECT volume_name, volume_type, storage_location
# MAGIC FROM uk_property_intel.information_schema.volumes
# MAGIC WHERE volume_schema = 'bronze'
# MAGIC ORDER BY volume_name;

# COMMAND ----------

# End-to-end path resolution: the Silver BoE notebook reads this exact path.
# A volume rooted one level too deep resolves to boe/base_rate/base_rate/ and
# fails here rather than three notebooks later.
dbutils.fs.ls("/Volumes/uk_property_intel/bronze/boe/base_rate/")
