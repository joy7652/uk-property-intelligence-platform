# Design Document — UK Property Market Intelligence Platform

This document captures the architectural decisions, engineering trade-offs, and delivery plan behind the platform. It complements the project README with deeper rationale and serves as the working design reference across phases.

> **Status:** Phase 1 (Bronze ingestion) complete. Phase 2 (Databricks Silver layer) foundation in place: workspace, Unity Catalog, per-layer storage, identity model, and Bronze Volumes provisioned. Silver-layer transformation notebooks in development.

---

## Table of contents

- [Project overview](#project-overview)
- [Data sources](#data-sources)
- [Architecture](#architecture)
- [Storage architecture](#storage-architecture)
- [ADF implementation (Phase 1)](#adf-implementation-phase-1)
- [Databricks foundation (Phase 2)](#databricks-foundation-phase-2)
- [Key engineering decisions](#key-engineering-decisions)
- [Bugs found and fixed](#bugs-found-and-fixed)
- [Repository and Git workflow](#repository-and-git-workflow)
- [Phase 2 scope — Databricks Silver layer](#phase-2-scope--databricks-silver-layer)

---

## Project overview

### Elevator pitch

A configurable, multi-source data engineering platform that ingests, validates, transforms, and analyses UK residential property market data across six official UK open datasets. It demonstrates a production-grade Azure data pipeline with data quality, observability, incremental loading, CI/CD, and statistical anomaly detection.

### Why these data sources

Property data turns up across finance, consulting, and the public sector, and it's a domain most people already understand. The sources update monthly, which makes this a running pipeline rather than a one-off analysis. The data is genuinely messy, so the transformations are non-trivial, and the config-driven design means the same framework could ingest any other multi-source analytical dataset without code changes.

---

## Data sources

| # | Source | Format | Pattern | Step | URL stability |
|---|--------|--------|---------|------|---------------|
| 1 | HM Land Registry — Price Paid Data (PPD) | CSV per year | `yearly_stepped` | 1 | Stable, fixed |
| 2 | HM Land Registry — UK House Price Index (HPI) | CSV cumulative | `single_file` | — | URL changes monthly (predictable) |
| 3 | Doogal — UK Postcode Lookup (ONSPD mirror) | ZIP | `single_file` | — | Fixed URL, data refreshes quarterly |
| 4 | Bank of England — Official Bank Rate | XLS | `single_file` | — | Stable, fixed |
| 5 | ONS — Price Index of Private Rents | XLSX | `single_file` | — | URL fully rotates monthly |
| 6 | UK Police — Street-level Crime | ZIP (~1.7 GB) | `yearly_stepped` | 2 | Stable historical + monthly rolling snapshot |

### Source selection rationale

**Price Paid Data** is the authoritative record of UK residential transactions since 1995, 24M+ records, and the backbone of any property analysis. **HPI** gives official price indices validated by the same department, which makes it a useful cross-check for any metric I derive myself. **Postcode lookups** handle geocoding and regional aggregation. **Bank of England rates** and **ONS rents** supply the macro context; affordability analysis needs both the price and the cost of money. **Police crime data** adds the classic property-investment overlay of safety against price growth, and it joins cleanly on postcode.

### Source details

**1. Land Registry Price Paid Data**
- Yearly CSV files, 1995 to present
- URL: `http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{YYYY}.csv`
- Incremental URL: `/pp-monthly-update.txt` (cumulative file)
- ~50 MB to ~230 MB per yearly file, 24M+ records total
- No auth, no headers required

**2. UK House Price Index (HPI)**
- Single cumulative CSV per monthly release
- URL: `https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-{YYYY}-{MM}.csv`
- Requires full browser-mimicking headers (User-Agent + Accept + Accept-Language); the Akamai CDN filters aggressively
- URL changes monthly but follows a predictable pattern

**3. Doogal UK Postcode Lookup**
- ZIP file, static URL: `https://www.doogal.co.uk/files/postcodes.zip`
- ~90 MB zipped, ~950 MB unzipped
- Mirrors ONS postcode data under the Open Government Licence. I chose it as a practical alternative when ArcGIS Hub access proved unworkable
- Updated quarterly

**4. Bank of England Base Rate**
- Single XLS file, stable URL
- URL: `https://www.bankofengland.co.uk/-/media/boe/files/monetary-policy/baserate.xls`
- Requires User-Agent header (anti-bot filtering)
- Full rate history since 1694
- The published `baserate.xls` is revised periodically, and the daily `Raw Data` series is pre-filled to roughly a month past the save date (a revision saved 2026-03-31 carried daily rows through 2026-04-30). Between revisions, ADF refreshes land byte-identical copies, so an unchanged row count after re-ingestion is expected rather than a failed fetch; confirm freshness from the blob's `modificationTime`, not a content change.

**5. ONS Price Index of Private Rents**
- Monthly XLSX, URL changes with every release
- Base URL: `https://www.ons.gov.uk/file`
- Relative URL pattern uses `?uri=...` query string
- **Platform quirk:** the relative URL must begin with `?` to stop the query delimiter being URL-encoded

**6. UK Police Street-level Crime**
- Rolling 3-year snapshot ZIPs, ~1.7 GB each, ~4500 CSVs inside (36 months × ~125 files per force/category)
- Historical snapshot URL pattern: `https://data.police.uk/data/archive/{YYYY}-12.zip`
- Latest monthly snapshot URL pattern: `https://data.police.uk/data/archive/{YYYY-MM}.zip`
- No auth
- Full-load iterates 2-yearly (2013-12, 2015-12, …, 2025-12); incremental fetches the latest published monthly snapshot
- Silver-layer strategy: deduplicate overlapping months, keeping the most recent snapshot's version of each `(month, force, category)` tuple. Where overlapping snapshots disagree, that becomes a quality signal.

### Source swap: EPC → Police.uk

The sixth source was originally MHCLG's Energy Performance Certificates (EPC). Partway through the build I found that the existing `epc.opendatacommunities.org` service was retiring on 30 May 2026, and its replacement (`Get energy performance of buildings data`) required GOV.UK One Login, an OAuth2 flow that ADF's native HTTP Basic authentication can't handle. The replacement service had no confirmed production launch date at the time.

I evaluated three options:

1. Build against the legacy service, accepting that the pipeline would break within weeks.
2. Wait for the replacement service to reach production.
3. Swap to a comparable stable source.

I chose option 3. Police.uk crime data meets the structural requirements (stepped yearly pattern, real analytical value) and arguably strengthens the analysis, since crime-against-price overlays are a well-established dimension in property investment.

A smoke test before implementation showed that each police.uk monthly archive is a rolling 3-year snapshot, not a monthly delta. That changed the ingestion pattern from the originally-planned monthly-backfill to `yearly_stepped` with a 2-year step, which avoids duplicate data while keeping full historical coverage.

---

## Architecture

```
watermark.json (ADLS Gen2 config container, Git-mirrored)
              ↓
Azure Data Factory (config-driven orchestration, Git-integrated)
              ↓
              ├── Master orchestrator reads watermark, filters active sources
              ├── Routes per source by load_pattern via Switch activity
              └── Route pipelines handle full-vs-incremental branching
              ↓
ADLS Gen2 bronze/ (raw files, per-source layout, exposed as UC External Volumes)
              ↓
Azure Databricks + Unity Catalog (transformation, quality, governance)
              ↓
ADLS Gen2 silver/ → gold/ (Delta Lake, managed tables, schema-level locations)
   plus ADLS Gen2 quality/ (quarantine + DQ outputs)
              ↓
Azure Synapse Serverless SQL (Phase 3)
              ↓
Microsoft Fabric / Power BI (Phase 5 — Property Dashboard, Pipeline Health Dashboard)
```

### Design principles

- Adding a source means appending a JSON block to the watermark, not writing a new pipeline.
- A small closed set of ingestion patterns (`yearly_stepped`, `single_file`) covers every source; each source declares which one it uses.
- After the first full load, each source tracks its own state in the watermark, so later runs fetch only what's new. What counts as "new" is source-specific, so incremental loading routes by type rather than assuming one mechanism.
- A pipeline reporting success only tells me bytes moved, not that the right bytes moved. Binary files are validated against their expected magic bytes before any downstream processing.
- HTTP linked services are host-agnostic; the base URL is passed at runtime via `@{linkedService().p_base_url}`, so one linked service can serve several hosts that share an authentication shape.
- Data-plane access from Databricks runs through a user-assigned managed identity on the Databricks Access Connector. No SAS tokens, no mounted credentials, no key rotation in the data path.
- Each medallion layer maps one-to-one to a Blob container, a Unity Catalog schema, and a schema-level managed location (or, for Bronze, a set of External Volumes), so the architecture is visible in the storage account.
- Each layer should do work the previous one didn't. Bronze stays as raw files exposed through Volumes rather than copied into Delta, because at this scale that copy step adds nothing the raw files don't already give.

---

## Storage architecture

### Container layout

The data lake uses six containers, each with a single responsibility:

| Container | Purpose | UC mapping |
|---|---|---|
| `config` | Watermark file and (planned) quality-rule definitions | read by ADF directly |
| `bronze` | Raw files as landed by ADF, per-source folder structure | UC external location; exposed as External Volumes under `uk_property_intel.bronze.*` |
| `silver` | Cleaned, typed, deduplicated Delta managed tables | Managed location for `uk_property_intel.silver` |
| `gold` | Joined, enriched, denormalised Delta managed tables | Managed location for `uk_property_intel.gold` |
| `quality` | Quarantine records, rule-run history, DQ metrics | Managed location for `uk_property_intel.quality` |
| `catalog-root` | Anchor for the Unity Catalog's catalog-level managed location | Catalog managed location for `uk_property_intel` |

The watermark lives at `config/watermark.json` directly (no subfolder), in its own container so its lifecycle and access policy stay independent of the data containers.

### Unity Catalog hierarchy

```
uk_property_intel/ (catalog, managed location → catalog-root container)
├── bronze/  (schema, no managed location; holds External Volumes pointing at bronze container)
├── silver/  (schema, managed location → silver container)
├── gold/    (schema, managed location → gold container)
└── quality/ (schema, managed location → quality container)
```

The catalog holds no production tables of its own; every object is created under one of the four schemas. The `bronze` schema holds External Volumes pointing at the `bronze` container, which gives UC governance and lineage over raw files without converting them to Delta (see Decision 11). Silver, Gold, and Quality hold managed Delta tables in their respective containers. The catalog-level managed location exists only because Unity Catalog requires a catalog to have some backing storage even when all its schemas either declare their own or hold only External Volumes. `catalog-root` is the placeholder that satisfies that without using one of the data containers.

### Bronze Volume layout

One External Volume per source under `uk_property_intel.bronze`, named to match the corresponding Silver table:

| Volume | Points at | Silver table |
|---|---|---|
| `bronze.boe` | `bronze/boe/` | `silver.boe_base_rate` (and future BoE rate types) |
| `bronze.hpi` | `bronze/land_registry/hpi/` | `silver.hpi` |
| `bronze.ppd` | `bronze/land_registry/ppd/` | `silver.ppd` |
| `bronze.doogal` | `bronze/doogal/` | `silver.doogal` |
| `bronze.ons` | `bronze/ons/` | `silver.ons` |
| `bronze.police` | `bronze/police/` | `silver.police` |

Silver notebooks reference `/Volumes/uk_property_intel/bronze/<source>/...` rather than `abfss://bronze@...` paths, so all Bronze access flows through Unity Catalog.

### Why per-layer containers rather than subfolders

The alternative was a single `medallion` container with subfolders. I chose per-layer containers for three reasons:

- Lifecycle policies can differ per layer (Bronze long-retains raw data; Silver can keep intermediate artifacts for less time).
- RBAC and cost attribution scope cleanly to the container boundary.
- The medallion structure is visible at the storage-account view without inspecting folder contents.

Cost is identical to the single-container layout, so the trade-off is purely organisational.

---

## ADF implementation (Phase 1)

### Storage

- Azure Data Lake Storage Gen2, container `bronze` (renamed from `raw` during Phase 2 setup)
- Files land at `bronze/<source>/<...>/<file>`, with no redundant `bronze/` subfolder inside the container
- RBAC: ADF managed identity granted `Storage Blob Data Contributor`
- Watermark at `config/watermark.json` (separate container, no subfolder)

### Linked services (named by authentication pattern, not by source)

| Name | Auth / headers | Sources |
|---|---|---|
| `LS_HTTP_Anonymous` | None | PPD, Police.uk |
| `LS_HTTP_User_Agent_Header` | User-Agent only | Doogal |
| `LS_HTTP_Accept_Header` | User-Agent + `Accept: */*` | BoE, ONS |
| `LS_HTTP_LandRegistry_HPI` | Full browser header mimicry (Akamai CDN) | HPI |
| `LS_ADLS` | Managed identity | All sinks |

Each HTTP linked service exposes `p_base_url` and has its Base URL field set to `@{linkedService().p_base_url}`. Datasets chain through via `@dataset().p_base_url`.

### Datasets

**Source datasets** (one per linked service):
- `DS_HTTP_Anonymous_Source`, `DS_HTTP_Accept_Header`, `DS_HTTP_User_Agent`, `DS_HTTP_LandRegistry_HPI`
- All parameterised with `p_base_url`, `p_relative_url`

**Sink datasets:**
- `DS_ADLS_Bronze_CSV`: parameterised with `p_folder_path`, `p_file_name`; operates in binary passthrough mode for all file types (CSV, XLSX, XLS, ZIP)
- `DS_ADLS_Watermark_JSON`: reads the watermark file

### Pipelines

**`PL_Master_Orchestrator`**
- Lookup reads watermark.json as an array
- Filter keeps only active sources: `@or(equals(item().active, null), equals(item().active, true))`
- ForEach iterates filtered items sequentially
- Switch on `load_pattern` dispatches to the appropriate route pipeline

**`PL_Route_Yearly_Stepped`**
- If Condition on `full_load_complete`:
  - False → Execute `PL_Yearly_Stepped_Full_Load`
  - True → Execute `PL_Route_Incremental_Load`

**`PL_Yearly_Stepped_Full_Load`**
- Generic yearly-stepped full-load pipeline
- Iterates an index range `[0, N)` where `N = (end_year - start_year) / step_years + 1`
- Each iteration computes its year as `start_year + (index * step_years)`
- Handles an optional `snapshot_month` suffix in URL and filename via `if(empty(...))` expressions
- Works for both PPD (step=1, no month suffix) and Police.uk (step=2, month suffix `-12`)

**`PL_Single_File_Full_Load`**
- Switch on `linked_service_type` with a case per authentication pattern
- Each case contains a Copy activity using the matching dataset

**`PL_Route_Incremental_Load`**
- If Condition at the top implements month-rate-limiting (skips if this source was already refreshed this calendar month)
- True branch: Switch on `incremental_type`:
  - `static_url` → Execute `PL_Incremental_Load_StaticURL` (PPD-style cumulative update file)
  - `templated_latest` → Execute `PL_Incremental_Load_TemplatedLatest` (Police.uk-style monthly-rotating URL)
- False branch: logs skip reason

**`PL_Incremental_Load_StaticURL`**
- Single Copy activity, handles PPD's `/pp-monthly-update.txt`

**`PL_Incremental_Load_TemplatedLatest`**
- Switch on `linked_service_type` with a case per authentication pattern
- Each case contains a Copy activity that constructs URL and filename from `incremental_relative_url_prefix + incremental_latest_snapshot + file_extension`

### Control-flow cascade rationale

ADF forbids nested control-flow activities (an If inside a Switch inside a ForEach). The three-level pipeline cascade (Master → Route → Incremental Route → Leaf) exists specifically to unwrap one control-flow activity per layer. That constraint drove a clearer separation of concerns: orchestration, then routing, then execution.

### Watermark schema

A single JSON array in ADLS containing one object per source. Each object declares which load pattern applies, its linked-service type, its URL fragments, and state-tracking fields. Example fields:

- Common: `source_name`, `display_name`, `load_pattern`, `linked_service_type`, `base_url`, `active`
- `yearly_stepped` specific: `relative_url_prefix`, `file_extension`, `start_year`, `step_years`, `snapshot_month`, `last_year_ingested`, `full_load_complete`, `parallelism`
- `single_file` specific: `relative_url`, `file_name`, `last_refreshed`
- Incremental: `incremental_type` (`static_url` or `templated_latest`), plus type-specific URL fields

---

## Databricks foundation (Phase 2)

### Workspace and compute

- **Workspace:** Premium tier, in the same region and resource group as the ADLS Gen2 storage account. Premium is required for Unity Catalog, RBAC, audit logs, and secret scopes.
- **Compute:** all-purpose cluster, single-node, Dedicated access mode (formerly "single user"), latest DBR LTS, auto-terminate at 15 minutes.
- **Photon engine:** off initially. I'll decide whether to enable it by testing against a representative Silver transformation (likely the Police.uk dedup) rather than turning it on by default.
- **Serverless SQL warehouse:** used for ad-hoc DDL, catalog administration, and verification queries between notebook runs. Cluster usage is reserved for PySpark notebook work; serverless handles SQL-only operations at lower cost and faster cold-start.

### Identity and access

- **Databricks Access Connector** with a user-assigned managed identity (UAMI) provides the identity Databricks uses to access ADLS.
- UAMI granted `Storage Blob Data Contributor` on the storage account, data-plane only, with no control-plane permissions.
- No DBFS mounts, no SAS tokens, no service-principal secrets; all ADLS access flows through the UAMI via Unity Catalog External Locations and Volumes.
- **File events disabled** on all external locations. Enabling them would require granting the UAMI `Storage Account Contributor` (control plane), `EventGrid EventSubscription Contributor`, and `Storage Queue Data Contributor`, a much broader scope than the data-plane permission the actual data path needs. Batch ingestion at this scale does not benefit from event-driven discovery.

### Unity Catalog setup

- **Metastore:** UK South region (auto-provisioned on workspace creation).
- **Storage credential:** points to the Access Connector's UAMI.
- **External locations:** one per non-config container: `bronze`, `silver`, `gold`, `quality`, `catalog-root`. All connection-tested and confirmed working with file events explicitly off.
- **Catalog:** `uk_property_intel`, managed location at `catalog-root`.
- **Schemas:** `bronze`, `silver`, `gold`, `quality`. Silver, Gold, and Quality have schema-level managed locations at their matching container; Bronze has no managed location since it holds only External Volumes. Created via SQL in `databricks_src/setup/01_create_schemas.py`.
- **External Volumes:** one per Bronze source, defined in `databricks_src/setup/02_create_bronze_volumes.py`. Exposes raw files under `uk_property_intel.bronze.<source>` for UC governance and lineage. Silver notebooks read from `/Volumes/uk_property_intel/bronze/<source>/` rather than direct `abfss://` paths.
- **Default workspace catalog:** dropped after I verified the dedicated catalog was operational, which keeps Catalog Explorer free of placeholder entries.

### Bronze layer state

Phase 1's ADF pipelines write into the `bronze` container directly (no `bronze/` subfolder). The container was renamed from `raw` and the redundant subfolder removed during Phase 2 setup, with ADF pipelines updated and a full end-to-end re-run completed before proceeding. Bronze is registered as a Unity Catalog External Location and surfaced through per-source External Volumes; Silver pipelines read via Volume paths only.

---

## Key engineering decisions

### 1. Load patterns are a closed set

Two patterns cover every ingestion shape I encountered:

- **`yearly_stepped`** — iterate across years with a configurable step, one file per step. Incremental work dispatches to one of two children via `incremental_type`: `static_url` or `templated_latest`.
- **`single_file`** — one URL fetches one file per refresh.

`yearly_stepped` grew out of an earlier `yearly_range` pattern. I added the step parameter only once Police.uk's 2-year snapshot cadence gave me a concrete second use for it, rather than generalising before I had a reason to.

### 2. Linked services organised by authentication pattern

I name and organise HTTP linked services by their authentication shape, not by the data source they first served. Authentication is a cross-cutting concern that several unrelated sources can share, whereas the data source itself is incidental to the connection. An earlier version named one linked service after its first source (`LS_HTTP_LandRegistry`), which hid the fact that Police.uk could reuse it. Renaming it to `LS_HTTP_Anonymous` made the grouping obvious and removed the need for a duplicate.

### 3. Watermark as a JSON array in ADLS

The watermark is stored as a JSON array (not an object) because ADF's Lookup plus ForEach iterates arrays cleanly. It lives in ADLS rather than Azure SQL, which removes an entire database dependency. When Databricks joins the stack in Phase 2, the watermark moves to a Delta table.

### 4. Three-level route cascade

Master → Route → Incremental Route → Leaf, with each layer unwrapping exactly one control-flow activity. ADF's nesting restriction makes the cascade necessary, and it ends up with cleaner separation of concerns than a single monolithic pipeline would have.

### 5. Per-source parallelism as configuration

Parallelism is recorded in the watermark per source (PPD=4, Police.uk=2). Larger files get lower parallelism to avoid saturating integration-runtime bandwidth. ADF's ForEach batch count can't be parameterised at runtime (a platform constraint), so the watermark field serves as documentation; the actual batch count is set on the ForEach activity directly.

### 6. Medallion architecture with a quality layer

Four logical layers backed by per-container physical storage:

- **Bronze** — raw files as received, no transformation, in the dedicated `bronze` container. Exposed via Unity Catalog External Volumes.
- **Silver** — validated, typed, deduplicated, schema-enforced Delta managed tables under `uk_property_intel.silver`.
- **Gold** — joined, enriched, denormalised Delta managed tables under `uk_property_intel.gold`.
- **Quality** — quarantine records, rule-run history, and DQ metrics under `uk_property_intel.quality`.

Each layer maps 1:1 to a container, schema, and managed location (Bronze excepted; see Decision 11).

### 7. Unity Catalog over hive_metastore

I adopted Unity Catalog from day one of Phase 2 rather than the legacy hive_metastore that older Databricks projects (and most tutorials) still use. Reasoning:

- Databricks has confirmed that from 30 September 2026, all new workspaces (Azure included) are provisioned Unity Catalog-only, with no Hive metastore, so building on UC now is the forward-compatible path.
- UC provides centralised access control, lineage, and discovery, which I'd otherwise have to build myself or skip.
- All data access flows through the UAMI on the Databricks Access Connector; no secrets, mounts, or SAS tokens.
- The DP-700 certification covers UC heavily, so building on it doubles as exam preparation.

The same logic applied to using the Access Connector's managed identity rather than a service principal with secrets: the older pattern still works, but the managed-identity path is the documented direction and avoids a future migration.

The complexity trade-off (metastore-level setup, access connector configuration, storage credentials, external locations, volumes) is paid once at the start of Phase 2.

### 8. Per-layer physical containers over single-container subfolders

Each medallion layer is a dedicated Azure Blob container, not a subfolder within a shared container. See [Storage architecture](#storage-architecture) for the full rationale. The sixth container, `catalog-root`, exists purely as the Unity Catalog managed location for the catalog itself and stays empty in practice, because every schema either declares its own managed location or holds only External Volumes.

### 9. Dedicated cluster access mode

Standard (shared) access mode is the newer Databricks default for general data engineering, but the cluster uses Dedicated access mode (formerly "single user"). Reasoning:

- This is a solo project. Standard's multi-user isolation gives me benefits I don't need while imposing restrictions (no RDD APIs, restricted Kafka options, no ML runtime) I might run into in Phase 3 and beyond.
- The anomaly-detection work planned for Phase 3 may use library code outside the Standard sandbox.
- Same DBU cost as Standard, so Dedicated isn't a price premium.

A Standard-mode cluster would also work for Phase 2. Dedicated isn't strictly necessary; it costs nothing and keeps my options open for later phases.

### 10. File events disabled on external locations

Unity Catalog external locations support Azure Event Grid file-change notifications for ingestion performance. Every external location here has the feature turned off. Reasoning:

- Batch ingestion at this scale (sub-GB per source) gains nothing from event-driven discovery.
- Turning file events on would require granting the UAMI three additional roles, including `Storage Account Contributor` (a control-plane role), a much broader scope than the data-plane-only `Storage Blob Data Contributor` the actual data path needs.
- Least privilege is the right call here, and the one I'd make in any regulated environment anyway.

### 11. Bronze exposed as UC Volumes, not Delta tables

Many Databricks tutorials treat Bronze as a Delta-table layer: raw files are read once, written to Delta with added metadata columns, then Silver reads from those Delta tables. This project does it differently. Bronze is exposed via Unity Catalog **External Volumes** pointing at the raw files in the `bronze` container, and Silver notebooks read straight from those Volume paths.

Reasoning:

- File scale is small (sub-GB per source per ingestion), so re-reading raw files during each Silver run is effectively free. There's no performance case for caching as Delta.
- ADF writes durable copies into the `bronze` container, so files aren't ephemeral. There's no preservation case for converting them to Delta.
- The project doesn't query raw data ad-hoc in SQL; Silver is the first SQL-queryable layer. There's no usability case for Delta at the Bronze level.
- Schema evolution is enforced at Silver, where schemas are actually defined. Tracking it at the raw-file layer would be premature.

A Bronze-as-Delta layer here would do nothing the raw files don't already do; it would exist only so the project could point at a Bronze layer. At this scale that isn't worth a layer.

Bronze-as-Volumes does pull its weight, for four reasons:

- **Governance.** Bronze access is subject to Unity Catalog ACLs, not just the external location's identity grants.
- **Lineage.** UC automatically traces which Silver tables read from which Bronze paths.
- **Discoverability.** Bronze appears in Catalog Explorer as a first-class object alongside Silver and Gold, rather than an `abfss://` URI known only to me.
- **Path stability.** Silver notebooks reference `/Volumes/uk_property_intel/bronze/<source>/` rather than container-qualified paths; if the storage layout ever changes, only the Volume definition updates.

This decision would change if I were ingesting very large source files at high frequency, if the source data were ephemeral and needed durable Delta preservation, or if I needed extensive ad-hoc SQL on the raw data before Silver. None of those apply at the moment.

### 12. BoE base rate as an event-grain SCD2

The Bank of England base rate is the first Silver table, modelled as a Type 2 slowly-changing dimension at event grain rather than a daily-grain series.

- **Grain.** One row per rate level, with a validity interval (`effective_date`, `expiry_date`, `is_current`), the rate and its regime label (`rate_pct`, `rate_type`), and lineage columns (`_source_file`, `_ingestion_ts`). `expiry_date` is a deterministic derivation (the day before the next change; null marks the open interval), so it adds information rather than reshaping the data for a particular consumer, which keeps it Silver-appropriate. Daily-grain join surfaces, if a consumer needs them, are Gold's call.
- **Source reality.** `baserate.xls` is a multi-sheet FAME database export. The machine-readable sheet is `Raw Data`, a daily series from 1973-01-01 to present with real datetime cells and the header on row 2. The report sheets (`BOEBASERATE`, `HISTORICAL SINCE 1694`) are human-formatted and skipped.
- **Regime coalescing.** The BoE has renamed the policy rate across five era-specific columns (Bank Rate, Minimum Lending Rate, Minimum Band 1 Dealing Rate, Repo Rate, Official Bank Rate). These coalesce into a single `rate_pct` and a `rate_type`, newest regime first. Both columns are populated only on the two regime-changeover days (1981-08-24 and 1997-05-05), where the two values agree.
- **Collapse on rate value only.** The daily series collapses to change events; a regime relabel that does not move the rate is not an SCD2 event, and `rate_type` records the regime in effect at `effective_date`.
- **1973 cutoff.** Pre-1973 history lives only in the fragile report sheet, and PPD and HPI both start in 1995, so older rates would join to nothing. The first row's `effective_date` is the series start rather than a true change date (left-censored), which is noted in the code docstring.
- **`DecimalType(6, 4)`.** Repo-era rates were quoted in sixteenths (for example 5.9375), so a scale-2 decimal would silently round them. Decimal rather than double also gives exact equality, which the change-detection step depends on.
- **Fail-loud data-quality guard.** `assert_rate_columns_consistent` aborts the run if any row carries conflicting non-null values across the rate columns, rather than letting the coalesce silently pick one.

---

## Bugs found and fixed

### Base URL hardcoded despite the parameter being defined

**Discovered:** during ONS source onboarding, after I modified the ONS URL in the watermark for a new monthly release.

**Symptoms:**
- Pipeline reported "Succeeded" on every run
- Downstream `openpyxl.load_workbook()` failed with `BadZipFile: File is not a zip file`
- A hex dump showed the "XLSX" file was HTML beginning `<!DOCTYPE html>`
- The HTML came from `bankofengland.co.uk`, not `ons.gov.uk`, so the pipeline was fetching from the wrong host entirely

**Root cause:** the HTTP datasets had parameters defined (`p_base_url`, `p_relative_url`) but their Base URL fields were hardcoded rather than using `@dataset().p_base_url`. For five sources this was undetectable because the hardcoded value happened to match the watermark value. When ONS's URL was updated in the watermark for the March release, the dataset carried on using its stale hardcoded URL. That URL coincidentally pointed at BoE's host, which returned 200 OK with a homepage for the bad request path, so bytes transferred and ADF reported success.

**Fix:** replaced every hardcoded URL in the dataset Base URL fields with the appropriate `@dataset()` expression. Re-ran the master pipeline for affected sources. Validated the output files locally by parsing them in their native format.

**Lesson:** pipeline success status confirms bytes moved, not that the right bytes moved. Two follow-ups resulted:
1. Magic-byte validation planned for the Silver-layer ingestion contract: every file checked against its expected format header before parsing.
2. An audit of every parameterised dataset field, to confirm the parameters are actually wired in, not just defined.

### Query-string double-encoding

**Discovered:** the ONS URL returned 400 errors when `?uri=...` appeared mid-string.

**Root cause:** ADF URL-encoded the `?` character when it wasn't at position 0 of the relative URL.

**Fix:** always prefix the relative URL with `?` as its first character. ADF preserves the query-string delimiter when it appears at position 0.

### ADF nested control-flow restriction

**Discovered:** when designing the full-vs-incremental branching logic, and again when adding the incremental-type dispatch.

**Constraint:** ADF forbids nested control-flow activities: If, Switch, ForEach, and Until cannot contain each other.

**Resolution:** extract the inner logic into child pipelines. `PL_Route_Yearly_Stepped` and `PL_Route_Incremental_Load` exist specifically to unwrap one control-flow activity per layer. The result is cleaner separation of concerns, with each pipeline doing one job.

### Repo folder named `databricks` shadowed by the installed SDK

**Discovered:** while building the first importable Silver transform module. Imports from a top-level repo folder named `databricks` never resolved, whatever the path manipulation.

**Root cause:** the installed Databricks SDK owns the `databricks` package name on a cold interpreter start, so a top-level folder of the same name is shadowed and never reaches the import path. A second constraint sat underneath it: Databricks Runtime 16.0 and above cannot import a notebook as a Python module at all, so importable library code has to be a plain workspace file (`.py` with no `# Databricks notebook source` header), created via Add → File rather than Add → Notebook. Committed through Git, a `.py` without the header lands as a File; with the header it becomes a notebook.

**Fix:** renamed the folder to `databricks_src`, and kept importable library code as `.py` workspace Files while runnable notebooks stay in source format. One representation serves both: a source-format notebook renders as a notebook in the workspace but commits as a clean `.py` diff. Reverting the rename once appeared to work, but only because stale `sys.modules` state from earlier path edits in the same session masked the failure, so the real check is a fresh interpreter.

**Lesson:** verify any import fix on restarted compute before trusting it. The `__init__.py` files I first suspected were a red herring: implicit namespace packages resolve fine on Databricks Runtime, and the `wsfs ... Cannot find child __init__.pyc` messages are the FUSE layer logging the import system's file probes, not the failure itself.

### External Volumes rooted at the wrong depth

**Discovered:** the first end-to-end run of the BoE Silver notebook failed with a 404 whose URL contained a doubled segment, `boe/base_rate/base_rate/`. A Volume path resolves as the Volume's storage location plus the relative path the notebook appends, so the doubling identified the fault from the error alone: the Volume was rooted at the dataset folder (`/boe/base_rate/`) instead of the source root (`/boe/`).

**Audit:** I checked all six Volumes against both the Bronze layout table in this document and a direct storage listing (`dbutils.fs.ls` through the external location, since a Volume under audit cannot be trusted to list itself). `ppd` and `doogal` were correct; `ons` and `police` were rooted at their dataset folders, the same class of fault as `boe`; `hpi` pointed at `uk_hpi/`, which matched the deployed storage but exposed that the data itself had landed outside the taxonomy back in Phase 1 (a flat `uk_hpi/` while the sibling source `ppd` nests under `land_registry/`).

**Fix (metadata class: boe, ons, police):** the Volumes were dropped and recreated at their source roots. An external Volume's location cannot be altered in place, so drop-and-recreate is the mechanism, and it touches metadata only.

**Fix (data class: hpi):** corrected at the data layer instead, the same way `doogal` had been handled. The watermark sink folder moved to `land_registry/hpi`, the rotating URL was refreshed to the current release, the master pipeline re-ran to land the file at the new path, and the stale `uk_hpi/` was deleted only after verifying the new file (the Phase 1 lesson: Succeeded is not the same as the right bytes). The Volume was then recreated at `land_registry/hpi/`.

**Script hygiene:** `02_create_bronze_volumes.py` was corrected in three places, and a stray one-time `DROP VOLUME ... doogal` repair line was removed. A lone `DROP` in a committed bootstrap script silently destroys and recreates one of six Volumes on every run, and one-off repairs belong in the runbook or a serverless session. The same reasoning removed the one-time `SHOW SCHEMAS` and `DROP CATALOG ..._ws CASCADE` cells from `01_create_schemas.py`, where a bare `DROP` would abort a fresh rebuild and a new workspace's default catalog carries a different name anyway. Two verification changes also came out of the audit: it moved to an `information_schema.volumes` query selecting `storage_location`, because `SHOW VOLUMES` lists names only and a wrong-rooted Volume is invisible in a name listing; and `02_create_bronze_volumes.py` now closes with a `dbutils.fs.ls` on the BoE Volume path, the same path the Silver notebook reads, so a mis-rooted Volume fails in the bootstrap script rather than downstream.

**Lessons:**

- `CREATE ... IF NOT EXISTS` is safe to re-run but can never repair drift, since create-if-absent says nothing about desired state. Relocating a Volume requires an explicit `DROP`; the corrected script on its own is inert against a live wrong Volume. This is part of the motivation for the Phase 4 Terraform work, which reconciles state rather than asserting absence.
- Verify Volumes against storage listings, not their own definitions. Script, documentation, and deployment can each be wrong independently: here the documentation was right and the deployment wrong for boe, ons, and police, while the deployment was right and the data wrong for hpi.
- Read failing URLs literally. The doubled path segment named the fault before any diagnostic ran.
- A wrong-rooted Volume keeps working until a path convention exposes it, so fix it while nothing downstream holds lineage against it.

### Bronze schema created with a managed location it should never have had

**Discovered:** while converting the setup notebooks from `.ipynb` to `.py` source format. `01_create_schemas` had given `uk_property_intel.bronze` a `MANAGED LOCATION` at the bronze container root, which contradicts the design recorded elsewhere in this document: bronze holds External Volumes only. `DESCRIBE SCHEMA EXTENDED uk_property_intel.bronze` confirmed it live, reporting the Root Location as the bronze container.

**Why it mattered:** a managed location is the default storage path for managed Delta tables and managed volumes. Nothing managed had been created in `bronze`, so nothing had gone wrong yet, but any managed object created there would have written Delta files into the container that holds raw ADF output.

**Why it went unnoticed:** `CREATE SCHEMA IF NOT EXISTS` skips the whole statement, clauses included, when the schema already exists. The `MANAGED LOCATION` clause applied once at creation and was invisible on every re-run afterwards. This is the same masking mechanism as the wrong-rooted Volumes, one level up the hierarchy.

**Documented behaviour against observed:** Databricks documents managed storage locations as unable to overlap external tables or external volumes. All six Bronze External Volumes were nevertheless created beneath the managed bronze root without error. I tested this directly rather than arguing it: a throwaway schema with a managed location at an unused path accepted an external volume created inside that path. The documented rule did not block an external volume created under an existing managed location. I record this as an observation with the probe described, not as a claim about Unity Catalog internals; the fix does not rest on it, since the raw-container argument stands on its own.

**Fix:** the six Volumes were dropped individually, then `DROP SCHEMA uk_property_intel.bronze` without `CASCADE`. A plain drop refuses while the schema still holds objects, which is the guard worth having when the schema's managed root is the container holding every raw file. Both setup scripts were re-run, and the container listing was diffed before and after to confirm no data moved.

**Lessons:**

- A managed location is only meaningful where managed objects exist. Bronze holds External Volumes that carry their own explicit `LOCATION`, so the clause was inert. Inert is not the same as harmless: it set the blast radius of a mistake nobody had made yet.
- `IF NOT EXISTS` concealed a schema-level definition error for two months, the same way it concealed the wrong-rooted Volumes. Create-if-absent DDL can state absence, never desired state.
- Vendor documentation is where verification starts, not where it ends. The overlap rule was quoted, contradicted by the live deployment, then settled by a four-statement probe.

---

## Repository and Git workflow

The repository follows a **trunk-based workflow** with short-lived feature branches:

- One branch per logical unit of work (e.g. `phase2/setup-catalog-schemas`, `phase2/boe-silver`).
- All changes merge to `main` via pull request; `main` is branch-protected against direct pushes.
- ADF Studio is Git-integrated; pipeline JSON commits go to feature branches via the ADF UI, then merge to `main` via PR. Publishing from ADF promotes the live factory.
- Databricks notebooks are managed via Databricks Git folders linked to this repo, committed from the workspace UI on the same feature branches.

Both ADF and Databricks operate on the same Git branches as local development. The repo on `main` always reflects the live state of every component.

### Branch lifecycle

Feature branches are named `phase<N>/<task>` (e.g. `phase2/boe-silver`), reflecting the project's phase structure. Branches are short-lived, typically merged and deleted within days of creation. The git log therefore reads as a project timeline rather than a long-running development trunk.

### Historical note

During Phase 1, ADF used the platform's default `adf-dev` long-lived branch with periodic syncs to `main`. This was migrated to trunk-based feature branches at the start of Phase 2; `adf-dev` was merged to `main` and retired. ADF still operates on whichever feature branch is current, just selected per-task rather than always pointing at one collaboration branch.

---

## Phase 2 scope — Databricks Silver layer

### Completed (foundation)

- Databricks workspace (Premium) and Dedicated-access cluster provisioned
- Unity Catalog metastore configured for the workspace region
- Databricks Access Connector with UAMI; `Storage Blob Data Contributor` granted on the storage account
- Five external locations (`bronze`, `silver`, `gold`, `quality`, `catalog-root`), file events disabled on all
- Dedicated catalog `uk_property_intel` with four schemas (`bronze`, `silver`, `gold`, `quality`)
- Per-source External Volumes under `bronze`, exposing raw files for governed Silver access
- Default workspace catalog dropped; repo skeleton committed including `databricks_src/`, `tests/`, and the schema and volume setup notebooks
- Bronze container restructured: `raw` → `bronze`, redundant subfolder removed, ADF pipelines updated, end-to-end re-run validated
- GitHub integration in Databricks via Git folder, PAT-authenticated, trunk-based workflow operational

### In progress

- Silver-layer ingestion notebooks, one per source, starting with BoE (simplest)

### Planned (Silver scope)

1. **Silver-layer ingestion notebooks, one per source**
   - Bronze → Silver transformations (clean, type, dedupe, Delta-format)
   - Data quality checks applied at ingestion time
   - Magic-byte validation as a direct follow-through from the Phase 1 bug
   - Schema enforcement with Delta Lake's `mergeSchema`
   - Excel sources read via the `dev.mauch:spark-excel_2.13:4.0.0_0.31.2` Spark plugin for Spark-native ingestion consistency
   - Pure transformation functions live in `databricks_src/silver/transforms/` for unit-testability; notebooks import them

2. **Parameterised quality-rules framework**
   - `quality_rules.json` under `config/` defining per-source validation rules
   - Generic validation notebook reads rules and applies them dynamically (PySpark, not SQL, since rule application needs to be programmatic)
   - Rejected records routed to a quarantine Delta table in the `quality` schema
   - Quality scores logged to a `pipeline_audit` Delta table on every run

3. **Police.uk overlapping-snapshot deduplication**
   - Primary: `row_number() over (partition by month, force, category order by snapshot_date desc) = 1`
   - Cross-snapshot consistency check as a bonus quality signal: data that disagrees between overlapping snapshots for the same month is flagged for investigation

4. **Automated watermark updates from Databricks**
   - Replace manual JSON editing
   - Notebook programmatically updates the watermark on successful runs
   - Migration of the watermark from JSON-in-ADLS to a Delta table

5. **pytest + chispa test suite**
   - SparkSession fixture in `tests/conftest.py`
   - Per-source transform tests in `tests/test_silver_transforms/`
   - Quality framework tests in `tests/test_quality_framework/`
   - Runs locally with `pytest`; CI integration deferred to Phase 4

6. **Initial Silver → Gold design**
   - Multi-source joins on postcode
   - Star-schema fact + dimension tables (Kimball)
   - Enrichment: price × rent yield, price × rate affordability, price × crime index

### Planned order of delivery

1. ✅ Databricks workspace + cluster + Unity Catalog + storage layer + Bronze Volumes
2. First Silver notebook against the simplest source (BoE): minimal schema complexity
3. HPI (single CSV, well-structured)
4. PPD (large, multi-year, schema-evolution considerations)
5. Doogal (ZIP unzip, large postcode table)
6. ONS (XLSX with headers and footers to skip)
7. Police.uk (most complex: ZIP containing ~4500 nested CSVs, multi-snapshot deduplication)
8. Quality-rules framework, extracted from patterns observed during (2)–(7)
9. Watermark automation
10. Gold-layer joins

---

*Design document status: reflects state at end of Phase 2 setup: workspace, Unity Catalog, storage layer, and Bronze Volumes provisioned; Silver-layer transformations in active development.*
