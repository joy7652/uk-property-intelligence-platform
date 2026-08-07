# Databricks workspace setup

Configuration and provisioning artifacts for the project's Databricks workspace.
Running the sequence below takes a workspace that already has its Unity Catalog
foundation to a state where the Silver-layer notebooks can run.

The storage account, Access Connector, Unity Catalog metastore, external locations, and
the `uk_property_intel` catalog are provisioned by hand in Phase 2 and are not yet
codified. What is codified here is the cluster definition and the schema DDL. For
architecture rationale, see the root `README.md` and `DESIGN.md`.

## Contents

| File | Purpose |
|---|---|
| `cluster_definition.json` | Desired-state spec for the Phase 2 all-purpose cluster and its libraries. Source of truth for the cluster configuration. |
| `01_create_schemas.py` | Unity Catalog schema DDL. Creates `silver`, `gold`, and `quality` under the `uk_property_intel` catalog, each with its own managed location. Databricks notebook in `.py` source format. |

## Prerequisites

The following must exist before running anything here.

**Azure.** Storage account `ukpropertyintelligencedl` with the medallion containers
(`config`, `bronze`, `silver`, `gold`, `quality`, `catalog-root`), and a Databricks
Access Connector (user-assigned managed identity) holding `Storage Blob Data
Contributor` on the storage account.

**Unity Catalog.** Metastore attached to the workspace, a storage credential backed by
the Access Connector, external locations over each container, and the
`uk_property_intel` catalog with its managed location anchored to `catalog-root`.

**Tooling.** The Databricks CLI (v0.205+) authenticated against the workspace, and
`jq`, for the cluster apply step. Both run on your own machine, not inside Databricks.

## Bootstrap sequence

The cluster must exist before the schema notebook can run on it.

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
registers the `silver`, `gold`, and `quality` schemas under `uk_property_intel`.

## Environment-specific fields

`cluster_definition.json` carries four workspace-specific values that must be filled or
verified before the file is applied:

**`cluster_id`, `single_user_name`.** Workspace identifiers.

**`spark_version`, `node_type_id`.** Verify against the live cluster.

Treat the running cluster as the authority. `databricks clusters get <id>` reports live
values, and if the committed `cluster` block and live state diverge, reconcile from
that export.

## Phase 4

The manual apply above is provisional. Phase 4 replaces it with infrastructure-as-code.
`cluster_definition.json` maps field-for-field onto the Terraform `databricks_cluster`
and `databricks_library` resources, so the cluster, libraries, and Unity Catalog
objects all redeploy reproducibly from version control.