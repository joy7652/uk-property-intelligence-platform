# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Grant continuous-integration access
# MAGIC What the dbt build in `.github/workflows/tests.yml` needs, granted to the `ci_builders` group, and nothing beyond it: read on each table `dbt/models/_sources.yml` declares, the right to create views in `marts_ci`, and use of the SQL warehouse. Run this after `01_create_schemas`, which creates `marts_ci`, and again whenever a source is added to the dbt project.
# MAGIC
# MAGIC Four prerequisites sit outside Unity Catalog and outside this notebook. A Databricks-managed service principal, `dbt-ci`. An account-level group `ci_builders` with that principal as its member, holding the `Databricks SQL access` entitlement and nothing else. Both assigned to this workspace, the principal directly as well as through the group, since a token is issued only to a principal assigned to the workspace directly. And a federation policy on the principal, created by an account admin in the account console, accepting the tokens GitHub signs for this repository's `databricks-ci` environment. That policy is the whole credential: no secret is stored in GitHub or in Databricks.
# MAGIC
# MAGIC Table-level SELECT by design, where `05_grant_serving_access` grants per schema. Every table in gold and quality exists to be read by Power BI, so a new one is covered on creation there. A CI build reads the dbt sources and nothing else, so the grants are generated from the file that declares them, and a source added without re-running this notebook fails CI with a permission error naming the table.
# MAGIC
# MAGIC `marts_ci` and never `marts`. A pull request builds views from code nobody has merged yet, so the principal can create objects in one schema that nothing reads, and holds no privilege at all on the one the monthly job builds.
# MAGIC
# MAGIC GRANT adds privileges and never removes them, so re-running reconciles nothing: a privilege granted elsewhere survives this notebook. Removal is an explicit REVOKE in a session. The verification reports grants this notebook did not make rather than removing them.
# MAGIC
# MAGIC The verification confirms the grants exist. It says nothing about whether the principal can use them, since this notebook runs as its owner and both group membership and the federation policy sit outside the catalog. The CI run is the test for effective access.

# COMMAND ----------

from pathlib import Path

import requests
import yaml

CATALOG = "uk_property_intel"
GROUP = "ci_builders"
CI_SCHEMA = "marts_ci"

# The warehouse the CI profile connects through, which is also the one the monthly
# job's marts_build task runs its SQL on.
WAREHOUSE_ID = "65e93ffd5f4ef90c"

# A notebook in a Git folder runs from its own directory, so the dbt project sits two
# levels up from here.
SOURCES_FILE = Path("../../dbt/models/_sources.yml").resolve()

# Read the way dbt reads it: a source without a schema takes its own name, and a table
# with an identifier is that relation rather than its name.
declared = yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8"))
SOURCE_TABLES = sorted(
    (
        source.get("database", CATALOG),
        source.get("schema", source["name"]),
        table.get("identifier", table["name"]),
    )
    for source in declared["sources"]
    for table in source["tables"]
)
SOURCE_SCHEMAS = sorted({schema for _, schema, _ in SOURCE_TABLES})

outside = sorted({catalog for catalog, _, _ in SOURCE_TABLES} - {CATALOG})
if outside:
    raise ValueError(
        f"{SOURCES_FILE} declares sources in {outside}. USE CATALOG is granted on "
        f"{CATALOG} alone, so those would fail in CI however they were granted."
    )

print(f"{len(SOURCE_TABLES)} sources in {len(SOURCE_SCHEMAS)} schemas, from {SOURCES_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog grants
# MAGIC
# MAGIC `CREATE TABLE` on `marts_ci` is the privilege Unity Catalog checks before creating a view as well as a table, and the principal owns every view it creates there, which is what lets the next build replace them and set their comments.

# COMMAND ----------

statements = [f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{GROUP}`"]
statements += [
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `{GROUP}`" for schema in SOURCE_SCHEMAS
]
statements += [
    f"GRANT SELECT ON TABLE {catalog}.{schema}.{table} TO `{GROUP}`"
    for catalog, schema, table in SOURCE_TABLES
]
statements += [
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{CI_SCHEMA} TO `{GROUP}`",
    f"GRANT CREATE TABLE ON SCHEMA {CATALOG}.{CI_SCHEMA} TO `{GROUP}`",
]

for statement in statements:
    spark.sql(statement)  # noqa: F821
    print(statement)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The SQL warehouse
# MAGIC
# MAGIC A warehouse is a workspace object, so its permission is set through the permissions API rather than SQL, authenticated with this notebook's own context token as `apply_job_definition.py` is.

# COMMAND ----------

host = spark.conf.get("spark.databricks.workspaceUrl")  # noqa: F821
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
auth = {"Authorization": f"Bearer {context.apiToken().get()}"}
WAREHOUSE_PERMISSIONS = f"https://{host}/api/2.0/permissions/warehouses/{WAREHOUSE_ID}"

# PATCH, not PUT. PUT replaces the warehouse's whole access control list and would strip
# every other principal's permission on it, the serving group's included.
granted = requests.patch(
    WAREHOUSE_PERMISSIONS,
    headers=auth,
    json={"access_control_list": [{"group_name": GROUP, "permission_level": "CAN_USE"}]},
    timeout=60,
)
granted.raise_for_status()
print(f"CAN_USE on warehouse {WAREHOUSE_ID} for {GROUP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Every grant is read back from `information_schema` and the permissions API, which report what is held rather than what was asked for. A missing grant raises. A grant this notebook did not make is printed and left alone, because GRANT cannot take it away.
# MAGIC
# MAGIC Expected: 1 of 1 catalog privilege, 4 of 4 schema privileges, 11 of 11 table privileges, and CAN_USE on the warehouse.

# COMMAND ----------

COLUMNS = {
    "catalog_privileges": "privilege_type",
    "schema_privileges": "schema_name, privilege_type",
    "table_privileges": "table_schema, table_name, privilege_type",
}

EXPECTED = {
    "catalog_privileges": {("USE CATALOG",)},
    "schema_privileges": {(schema, "USE SCHEMA") for schema in SOURCE_SCHEMAS}
    | {(CI_SCHEMA, "USE SCHEMA"), (CI_SCHEMA, "CREATE TABLE")},
    "table_privileges": {(schema, table, "SELECT") for _, schema, table in SOURCE_TABLES},
}

missing, unexpected = [], []
for view, wanted in EXPECTED.items():
    # information_schema spells a privilege with underscores, USE_SCHEMA, where GRANT
    # writes USE SCHEMA. Read back in GRANT's spelling so the two compare.
    held = {
        (*row[:-1], row[-1].replace("_", " "))
        for row in spark.sql(  # noqa: F821
            f"SELECT {COLUMNS[view]} FROM {CATALOG}.information_schema.{view} "
            f"WHERE grantee = '{GROUP}'"
        ).collect()
    }
    missing += [(view, *grant) for grant in sorted(wanted - held)]
    unexpected += [(view, *grant) for grant in sorted(held - wanted)]
    print(f"{view:20}{len(wanted & held)} of {len(wanted)} held")

acl = requests.get(WAREHOUSE_PERMISSIONS, headers=auth, timeout=60)
acl.raise_for_status()
levels = sorted(
    permission["permission_level"]
    for entry in acl.json().get("access_control_list", [])
    if entry.get("group_name") == GROUP
    for permission in entry.get("all_permissions", [])
)
print(f"{'warehouse':20}{levels or 'nothing'}")
if "CAN_USE" not in levels:
    missing.append(("warehouse", WAREHOUSE_ID, "CAN_USE"))

for grant in unexpected:
    print(f"not granted here    {grant}")

if missing:
    raise RuntimeError(f"{len(missing)} grant(s) not held by {GROUP}: {missing}")
