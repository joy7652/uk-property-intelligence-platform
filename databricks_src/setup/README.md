# Databricks workspace setup

Configuration and provisioning artifacts for the project's Databricks workspace.
Running the sequence below takes a workspace that already has its Unity Catalog
foundation to a state where the pipeline notebooks can run, the two jobs exist, and
Power BI can read the serving layer.

The storage account, Access Connector, Unity Catalog metastore, external locations, and
the `uk_property_intel` catalog are provisioned by hand in Phase 2 and are not yet
codified. What is codified here is the cluster definition, the schema and volume DDL,
the quality-table DDL, the job definitions, and the serving-layer grants. For
architecture rationale, see the root `README.md` and `DESIGN.md`.

## Contents

| File | Purpose |
|---|---|
| `cluster_definition.json` | Desired-state spec for the Phase 2 all-purpose cluster and its libraries. Source of truth for the cluster configuration. |
| `job_definition.json` | The twelve-task Silver and Gold job, triggered by ADF once the copies have landed. |
| `job_definition_pre_run.json` | The one-task URL resolver, run before the downloads. |
| `apply_job_definition.py` | Creates or resets one job from one definition file. Idempotent by name lookup. Runs as a notebook on the cluster. |
| `01_create_schemas.py` | Unity Catalog schema DDL. Creates `bronze`, `silver`, `gold`, `quality`, and `configs` under the `uk_property_intel` catalog. Silver, Gold, and Quality each carry a managed location at their matching container; Bronze and Configs carry none, since both hold only External Volumes. Databricks notebook in `.py` source format. |
| `02_create_bronze_volumes.py` | External Volumes, one per Bronze source. |
| `03_create_quality_tables.py` | DDL for `pipeline_run`, `pipeline_metric`, and `rule_result`, generated from the writers. |
| `04_create_configs_volumes.py` | External location `configs_managed` and External Volume `configs.watermark`, both over the configs container. |
| `05_grant_serving_access.py` | Read grants on `gold` and `quality` for the `bi_readers` group. Phase 5. |

## Prerequisites

The following must exist before running anything here.

**Azure.** Storage account `ukpropertyintelligencedl` with the medallion containers
(`configs`, `bronze`, `silver`, `gold`, `quality`, `catalog-root`), and a Databricks
Access Connector (user-assigned managed identity) holding `Storage Blob Data
Contributor` on the storage account.

**Unity Catalog.** Metastore attached to the workspace, a storage credential backed by
the Access Connector, external locations over the `bronze`, `silver`, `gold`, `quality`
and `catalog-root` containers, and the `uk_property_intel` catalog with its managed
location anchored to `catalog-root`. The configs external location is not a
prerequisite; step 5 creates it.

**Tooling.** The Databricks CLI (v0.205+) authenticated against the workspace, and
`jq`. Both are needed for the cluster apply step only, and both run on your own machine,
not inside Databricks. Every numbered notebook and the job apply script run in the
workspace.

**Serving identity.** Required by step 5 only. A Databricks-managed service principal
holding the `Databricks SQL access` entitlement, an account-level group `bi_readers`
with that principal as a member, and the group assigned to the workspace.
Databricks-managed and not Entra ID managed: the Power BI connector refreshes tokens
automatically for the former, and the one-hour workflow limit that applies to
Entra-managed principals does not apply.

## Bootstrap sequence

The cluster must exist before any of the notebooks can run on it.

### 1. Cluster and libraries

`cluster_definition.json` is a project artifact, not a literal API payload: the
Databricks REST API manages cluster configuration and libraries through separate
endpoints. Apply both parts.

```bash
# run from databricks_src/setup/

# cluster configuration
jq '.cluster' cluster_definition.json > /tmp/cluster.json
databricks clusters edit --json @/tmp/cluster.json

# libraries, separate endpoint
jq '{cluster_id: .cluster.cluster_id, libraries: .libraries}' \
  cluster_definition.json > /tmp/libs.json
databricks libraries install --json @/tmp/libs.json
```

The cluster already exists in this workspace, so `clusters edit` is correct. For a
clean-room rebuild, use `clusters create` instead and write the returned `cluster_id`
back into the file.

Two libraries are installed:

| Coordinate | Used by |
|---|---|
| `dev.mauch:spark-excel_2.13:4.0.0_0.31.2` | Bank of England base rate, which reads its `.xls` workbook directly |
| `openpyxl==3.1.5` | ONS private rents, which converts its `.xlsx` sheet to CSV before Spark reads it. See DESIGN Decision 20 for why spark-excel cannot read that file. |

Installation is asynchronous. Poll until both report `INSTALLED`:

```bash
databricks libraries cluster-status <cluster_id>
```

Restart the cluster before running anything that imports openpyxl. A Python library
installed on a running cluster is not visible to a REPL that is already attached.

Both can also be installed interactively via Cluster → Libraries → Install new, Maven
for the first and PyPI for the second. The CLI path above is the reproducible one.

### 2. Unity Catalog schemas

With the cluster running, run `01_create_schemas.py` as a notebook attached to it. This
registers `bronze`, `silver`, `gold`, `quality`, and `configs` under `uk_property_intel`.

Bronze and Configs are declared without a managed location. Both hold External Volumes
only, so there are no managed objects to place, and a managed location on either would
contradict that. The notebook's `DESCRIBE SCHEMA EXTENDED` cells are the check: a Root
Location reported against the bronze or configs container means the schema was created
wrong and must be dropped, not edited.

### 3. Bronze External Volumes

Run `02_create_bronze_volumes.py` on the same cluster. It creates one External Volume
per Bronze source and closes with a `dbutils.fs.ls` against the Bank of England volume
path, the same path the Silver notebook reads, so a mis-rooted volume fails here instead
of downstream.

Verification queries `information_schema.volumes` for `storage_location`. `SHOW VOLUMES`
lists names only, and a wrong-rooted volume is invisible in a name listing.

### 4. Quality tables

Run `03_create_quality_tables.py` on the same cluster to create `pipeline_run`,
`pipeline_metric`, and `rule_result` in the `quality` schema. Every Silver and Gold
notebook writes to these through the audit writer, so they must exist before the job
runs.

### 5. Configs External Volume

Run `04_create_configs_volumes.py` on the same cluster. Unlike the other volume step, it
creates its own external location: `configs_managed` over the configs container, backed by
the same storage credential. The Unity Catalog prerequisites do not cover it.

It then creates the External Volume `uk_property_intel.configs.watermark` at the container
root. That volume holds `watermark.json`, the single JSON array ADF reads through its
Lookup, and `log/`, which takes one marker per failed Bronze copy and is cleared at the
start of every run. An empty `log/` means every copy succeeded.

Verification queries `information_schema.volumes` for `storage_location`, then closes with
a `dbutils.fs.ls` on `/Volumes/uk_property_intel/configs/watermark/` — the exact path the
Silver Bank of England notebook reads, so a path that resolves here is known to resolve
downstream.

This must exist before ADF's first run, since the watermark is read before any Databricks
compute starts.

### 6. Serving-layer grants

Needed only for the Power BI consumption layer. Complete the serving identity
prerequisites first, then run `05_grant_serving_access.py`.

The notebook grants `USE CATALOG` on `uk_property_intel`, and `USE SCHEMA` plus `SELECT`
on `gold` and `quality`, to `bi_readers`. Nothing is granted on `bronze`, `silver`, or
`configs`. Schema-level `SELECT` is deliberate: every table in the two granted schemas
exists to be read by this consumer, and a new one is covered on creation.

Two permissions sit outside the notebook because neither is a Unity Catalog object.
`CAN USE` on the SQL warehouse is granted from the warehouse's Permissions tab, and
group membership is an account console action. Missing either produces a 403 at
connection time that reads like a grants problem and is not.

`GRANT` adds and never removes. The notebook asserts that a set of privileges exists; it
cannot report or repair one granted elsewhere. Removing a privilege is an explicit
`REVOKE` in a session, and a bare `REVOKE` does not belong in a bootstrap script.

### 7. Job definitions

`apply_job_definition.py` runs as a notebook on the cluster, not from the CLI. It reads
the workspace URL from the Spark conf and authenticates with the notebook context's own
API token, so there is nothing to configure outside the file.

It applies one definition per run. Set `DEFINITION` to the file you want, run it, then
set it to the other and run it again.

The script looks the job up by name before writing. One match is reset with the full
settings object, no match is created, and more than one stops the run and prints the
competing ids. Reset replaces the whole settings object, so anything absent from the
file is removed from the job.

Both definition files carry underscore-prefixed keys holding the reasoning behind the
configuration. The API rejects fields it does not declare, so the script strips them
before the call and the keys stay in the file where they are useful.

After applying, the script grants the ADF managed identity `CAN_MANAGE_RUN` on the job
by PATCH. PUT would replace the whole access control list and strip your own ownership
off the job. It then prints the job id, its URL, the resulting permissions, and every
task with its timeout cap.

## Environment-specific fields

`cluster_definition.json` carries four workspace-specific values that must be filled or
verified before the file is applied:

**`cluster_id`, `single_user_name`.** Workspace identifiers.

**`spark_version`, `node_type_id`.** Verify against the live cluster.

Treat the running cluster as the authority. `databricks clusters get <id>` reports live
values, and if the committed `cluster` block and live state diverge, reconcile from
that export.

`apply_job_definition.py` carries two more at the top of the file: `DEFINITION`, the
absolute workspace path of the definition being applied, and
`MANAGED_IDENTITY_CLIENT_ID`, the application id of the ADF managed identity receiving
`CAN_MANAGE_RUN`. The service principal field takes an application id, not a display
name.

## Infrastructure as code

The provisioning in this directory is applied by hand or by script, never reconciled.
`cluster_definition.json` maps field-for-field onto the Terraform `databricks_cluster`
and `databricks_library` resources, and the schema, volume, table, and grant DDL all
have direct Terraform equivalents, so the directory is convertible whole when that work
is done.

Until then one property holds across every file here and is worth stating plainly: they
assert presence. `CREATE ... IF NOT EXISTS` is safe to re-run but says nothing about
desired state, and `GRANT` adds without removing. Neither repairs drift. A wrongly
rooted volume, a schema carrying a managed location it should not have, or an unwanted
privilege has to be corrected explicitly in a session, and that correction belongs in
this runbook and not in the scripts.
