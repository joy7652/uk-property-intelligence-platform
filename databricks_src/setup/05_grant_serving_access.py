# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Grant serving-layer access
# MAGIC Read access to gold and quality for the `bi_readers` group, and nothing else.
# MAGIC Run this after `04_create_configs_volumes`.
# MAGIC
# MAGIC Three prerequisites sit outside Unity Catalog and outside this notebook: `bi_readers` exists as an account-level group and is assigned to this workspace, the serving service principal is a member of it, and the group holds CAN USE on the SQL warehouse Power BI connects through. Warehouse permissions are workspace objects and are not reachable from SQL.
# MAGIC
# MAGIC GRANT adds privileges and never removes them, so re-running reconciles nothing: a privilege granted elsewhere survives this notebook. Removal is an explicit REVOKE in a session.
# MAGIC
# MAGIC The verification below confirms the grants exist. It says nothing about whether the service principal can read, since this notebook runs as its owner and both group membership and the warehouse permission sit outside the catalog. The Power BI connection is the test for effective access.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- USE CATALOG permits name resolution. It carries no read on any object beneath it.
# MAGIC GRANT USE CATALOG ON CATALOG uk_property_intel TO `bi_readers`;
# MAGIC
# MAGIC -- Schema-level SELECT by design: every table in these two schemas exists to be read
# MAGIC -- by this consumer, so a new fact or quality table is covered on creation.
# MAGIC GRANT USE SCHEMA ON SCHEMA uk_property_intel.gold TO `bi_readers`;
# MAGIC GRANT SELECT ON SCHEMA uk_property_intel.gold TO `bi_readers`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA uk_property_intel.quality TO `bi_readers`;
# MAGIC GRANT SELECT ON SCHEMA uk_property_intel.quality TO `bi_readers`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Two rows each for gold and quality. No row for bronze, silver, or configs.
# MAGIC SELECT schema_name, privilege_type
# MAGIC FROM uk_property_intel.information_schema.schema_privileges
# MAGIC WHERE grantee = 'bi_readers'
# MAGIC ORDER BY schema_name, privilege_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT privilege_type
# MAGIC FROM uk_property_intel.information_schema.catalog_privileges
# MAGIC WHERE grantee = 'bi_readers';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.country_name, SUM(f.crime_count) AS crimes
# MAGIC FROM uk_property_intel.gold.fact_area_month_crime f
# MAGIC JOIN uk_property_intel.gold.dim_area a ON a.area_code = f.area_code
# MAGIC WHERE a.area_level = 'district'
# MAGIC GROUP BY a.country_name
# MAGIC ORDER BY crimes DESC;
