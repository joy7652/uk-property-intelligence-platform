# Design Document — UK Property Market Intelligence Platform

This document captures the architectural decisions, engineering trade-offs, and delivery plan behind the platform. It complements the project README with deeper rationale and serves as the working design reference across phases.

> **Status:** Phase 1 (Bronze ingestion) complete. Phase 2 (Databricks Silver layer) in progress: foundation provisioned (workspace, Unity Catalog, per-layer storage, identity model, Bronze Volumes), and three Silver tables live, unit-tested, and committed: the BoE base rate, the UK House Price Index, and Price Paid Data. Three sources remain.

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

**Price Paid Data** is the authoritative record of UK residential transactions since 1995, 31.4M records as of the July 2026 release, and the backbone of any property analysis. **HPI** gives official price indices validated by the same department, which makes it a useful cross-check for any metric I derive myself. **Postcode lookups** handle geocoding and regional aggregation. **Bank of England rates** and **ONS rents** supply the macro context; affordability analysis needs both the price and the cost of money. **Police crime data** adds the classic property-investment overlay of safety against price growth, and it joins cleanly on postcode.

### Source details

**1. Land Registry Price Paid Data**
- Yearly CSV files, 1995 to present, one file per transfer year
- URL: `https://price-paid-data.publicdata.landregistry.gov.uk/pp-{YYYY}.csv`
- The download page published a different host until February 2026, an S3 website endpoint at `prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com`. Both names resolve to the same object, confirmed by identical `Last-Modified` and `Content-Length` on each, so the older host is an alias rather than a stale mirror. The watermark uses the documented host.
- Incremental: `pp-monthly-update.txt`, a static URL under the same host. It carries additions, changes, and deletions rather than a cumulative snapshot, and each release overwrites it, so a missed release cannot be recovered from source. ADF lands it under a `.csv` name, so the Bronze extension does not match the source. Land Registry publishes a second copy at `pp-monthly-update-new-version.csv` at a slightly different size, which this pipeline does not fetch.
- Headerless. 16 columns in the published order, the last being Record Status, which is populated only in the monthly file. Yearly files carry `A` throughout.
- 103 MB to 228 MB per complete yearly file. 31.4M records total as of the July 2026 release. The count rises with every release as registrations complete, so it needs a release attached wherever it is quoted.
- Every yearly file contains exactly one transfer year, confirmed across all 32 files on two separate vintages. TUID is unique across the whole dataset.
- The four code columns carry small closed sets: property type (D, S, T, F, O), old or new build (Y, N), duration (F, L, U), and category (A, B). Every published value appears in the data, including duration U, which is genuine but rare at 532 rows across 31.4M. Silver aborts on an unrecognised code rather than passing it through.
- Category A (standard) runs from January 1995. Category B (additional: company sales, identifiable buy-to-lets, repossessions) has been captured since 14 October 2013, but the yearly files key on transfer date rather than registration date, so a thin tail of earlier transfer dates survives from transactions registered after capture began: 1,857 rows before 2013 against 1,811,011 category B rows in total. The tail thickens toward 2013, from 11 rows in 1995 to 432 in 2012, because the shorter the gap the more likely a transfer was still unregistered when capture started. In the 2019 file the split is 843,004 category A against 169,156 category B.
- Releases land on the 20th working day of each month and carry the previous month's registrations.
- The two most recent years are incomplete and keep growing. Registration lags sale by roughly two weeks to two months, occasionally longer.
- Tenure is not always what the property type implies: 136,761 transactions record a flat as freehold. The combination is legitimate and a Gold model that treats flat and leasehold as equivalent will misclassify them.
- No auth, no headers required

*Row counts by year carry visible market history, which makes them a usable sanity check on any reload.* Volumes run near 1.2M through the early 2000s, fall to roughly 650,000 across 2008 to 2012, and peak at 1,281,653 in 2021 during the stamp duty holiday. A reload that flattens those contours has lost data somewhere.

**2. UK House Price Index (HPI)**
- Single cumulative CSV per monthly release, 54 columns, ~148k rows, ~34 MB
- URL: `https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-{YYYY}-{MM}.csv`
- Requires full browser-mimicking headers (User-Agent + Accept + Accept-Language); the Akamai CDN filters aggressively
- The `{YYYY}-{MM}` token is the **last data month in the file, not the publication month**. The May 2026 file was published on 22 July 2026. The object as landed in Bronze is lower-cased relative to the source URL.
- Grain is one row per `(AreaCode, Date)`, monthly, across 405 geographies: local authorities, regions, nations, and three composites (United Kingdom, Great Britain, England and Wales). Columns are a headline price and index block plus nine breakdowns (property type, funding status, buyer type, build age).
- **Numeric formatting is not stable between releases.** A 2020 vintage published `AveragePrice` to five decimal places; current vintages publish whole pounds. Types are asserted at Silver rather than inferred for exactly this reason.
- The file carries a derived back-series to 1968, built from the historic path of the older ONS index, ahead of native coverage (England and Wales 1995, Scotland 2004, Northern Ireland 2005). See Decision 14.
- The two most recent months are provisional. Sales volumes arrive null and fill in as registrations complete; price estimates are revised.
- **Revision window: 13 months**, extended from 12 following a review of the revision policy. A full overwrite from the newest release is therefore the correct write mode, and an append would retain superseded values alongside corrected ones.

*Publisher issues affecting specific columns:*

- First-time buyer and former owner occupier prices were calculated incorrectly prior to January 2026. Land Registry advises caution comparing those breakdowns either side of the January 2026 estimates. The series therefore carries a discontinuity rather than a gap, which no null check will surface; a Gold model must not treat it as continuous.
- New build and existing resold average prices and percentage changes are no longer published as they were, because there are not currently enough new build transactions for a reliable result. Those columns arrive null in recent months while the corresponding sales volumes stay populated.
- Northern Ireland sales volumes are now published as a monthly estimate, the quarterly total divided by three. The previous approach fed quarterly figures into monthly UK totals and inflated them.

*Documented anomaly, retained as a Phase 4 test case:*

United Kingdom sales volume for March 2025 reads 134,340 against a baseline near 60,000, with April 2025 falling to 44,018. The cause is the SDLT threshold reversion on 1 April 2025: the nil-rate band returned from £250,000 to £125,000 and the first-time buyer threshold from £425,000 to £300,000, pulling completions forward into March. HMRC recorded 77,480 more residential transactions above £40,000 in March 2025 than a year earlier, and RICS had forecast the shape in advance. The spike grew between the October 2025 and May 2026 vintages as late registrations landed, which rules out a methodology artifact. A rolling anomaly detector that misses a 2x spike and an adjacent 0.7x trough with a published external cause is not working, so this is a better validation case than synthetic noise.

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
- An unchanged **event** count is likewise expected between rate changes. The table has held at 278 change events since the December 2025 cut to 3.75%, through unchanged decisions in March, April, and June 2026.

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
- Silver filters on whether data is trustworthy; Gold filters on what a question needs. Measured, clean data stays in Silver even when nothing in the project joins it yet.

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

The Volume namespace is flat while the storage layout beneath it is not. Four sources sit at the container root; the two Land Registry sources nest under a publisher folder, because Land Registry publishes two distinct sources rather than one. A Volume roots at its source folder and never at a dataset folder inside it, so notebooks append any dataset segment themselves: the BoE notebook appends `base_rate/`, while HPI's files sit directly at the Volume root.

This table is confirmed against `information_schema.volumes`, not the other way round. The Volume audit established that a script, a document, and a deployment can each be wrong independently, so the storage locations are read from the catalog rather than assumed from here.

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
- **Compute:** all-purpose cluster, single-node, Dedicated access mode (formerly "single user"), DBR 17.3 LTS (Spark 4.0, Scala 2.13), auto-terminate at 15 minutes. ANSI mode is on by default at this runtime version, which changes cast behaviour across every transform (see Decision 15).
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
- **1973 cutoff.** Pre-1973 history lives only in the fragile report sheet, so the cut is on data quality rather than on what joins. The 1973 to 1995 rates join nothing in this project yet, since PPD and HPI both start in 1995, and they are kept because they are clean measured data. Decision 14 applies the same rule to HPI. The first row's `effective_date` is the series start rather than a true change date (left-censored), which is noted in the code docstring.
- **`DecimalType(6, 4)`.** Repo-era rates were quoted in sixteenths (for example 5.9375), so a scale-2 decimal would silently round them. Decimal rather than double also gives exact equality, which the change-detection step depends on.
- **Fail-loud data-quality guard.** `assert_rate_columns_consistent` aborts the run if any row carries conflicting non-null values across the rate columns, rather than letting the coalesce silently pick one.

### 13. Library dependency scoping

A dependency lands in one of three places: the cluster spec, a notebook-scoped `%pip` install, or `requirements-dev.txt` for local runs and CI.

Pipeline runtime dependencies go on the cluster spec, version-pinned and committed in `databricks_src/setup/cluster_definition.json`. The spark-excel plugin is the only one so far, and it has no alternative: JVM libraries cannot be installed notebook-scoped, and serverless compute cannot load them at all, which is also why the Excel reads run on the Dedicated cluster rather than serverless.

Test and development dependencies stay off the cluster entirely. chispa is installed notebook-scoped in `tests/run_tests.py` at a pinned version, and the same pin sits in `requirements-dev.txt` next to the PySpark and pytest versions the runtime already ships, so a local run matches the cluster. Databricks recommends pinning `%pip` installs rather than letting them float, since unpinned installs do not fit serverless environments.

The split is about blast radius. A cluster library installs for every workload attached to the cluster and cannot be uninstalled from inside a notebook, so a version conflict is resolved at the cluster level and costs a restart. A notebook-scoped install reaches one notebook and disappears with the session, which is the right lifetime for something only the test runner needs. Keeping the cluster spec to what the pipeline needs at runtime also keeps it an accurate description of the pipeline.

This would change if the test suite moved to a scheduled job or to serverless compute, where the pinned environment belongs in the job or bundle environment specification rather than either place above.

### 14. HPI keeps measured data only, with a per-nation coverage floor

The published HPI file extends back to 1968, but not all of it is measured. Under the UK HPI, data is available from 1995 for England and Wales, 2004 for Scotland, and 2005 for Northern Ireland. The earlier portion is a derived series built from the historic path of the older ONS index. Silver keeps the measured era and drops the derived one.

**Per-nation floors, resolved from the ONS area-code prefix:**

| Prefix | Nation | Floor |
|---|---|---|
| `E` | England | 1995 |
| `W` | Wales | 1995 |
| `S` | Scotland | 2004 |
| `N` | Northern Ireland | 2005 |

**Composite geographies floor at the latest native start among the nations they span:**

| Area code | Geography | Floor | Why |
|---|---|---|---|
| `K04000001` | England and Wales | 1995 | both nations native from 1995 |
| `K03000001` | Great Britain | 2004 | includes Scotland |
| `K02000001` | United Kingdom | 2005 | includes Northern Ireland |

A flat 1995 floor would keep United Kingdom and Great Britain rows for 1995 to 2005 whose values are part measured and part derived. A row that is partly derived is harder to reason about than one that is wholly either, so the composite rule exists to remove that category rather than to save storage.

**Why the cut is on reliability rather than on joinability.**

An earlier draft of this decision floored HPI at 1995 on the grounds that PPD starts in 1995 and anything earlier joins nothing in the project. That reasoning does not belong in a Silver table, and applying it consistently would have made things worse: it would also cut the BoE table's 1973 to 1995 rates, which are clean measured data that nothing currently joins either.

The division this project settled on is that Silver filters on whether data is trustworthy, and Gold filters on what a question needs. PPD and Police.uk cover England and Wales only, so Scottish and Northern Irish HPI rows join neither of them. They stay, because they are measured data, and because they do join Doogal on geography and BoE on date. A Gold model joining HPI to PPD will narrow itself to England and Wales as a consequence of the join, without Silver having pre-decided that on its behalf.

The practical argument is reuse. A Silver table narrowed to one consumer's assumptions has to be rebuilt when a second consumer appears, and re-deriving dropped rows means re-ingesting and re-validating the source.

**Unmapped geographies abort the run.**

The floor is applied by comparing each row's year against a start year derived from its area code. A code matching no rule yields null, and a comparison against null is false, so the row would be dropped silently. A guard runs before the filter and fails on any unmapped code, naming it and its region. Land Registry does reorganise local authorities, and a new prefix or a fourth composite is a realistic future event; without the guard, the failure mode is an entire geography quietly disappearing from the table.

**Left-censoring.** The first row for each geography is its first measured month, not a market start. This mirrors the BoE table, whose first row is the start of the clean series rather than a rate change, and it is noted in the transform docstring.

### 15. Typing under ANSI mode

Databricks Runtime 17.0 and above enables ANSI mode by default, following Apache Spark 4.0. Under ANSI, an invalid cast raises a runtime exception rather than returning null.

For a wide source read as all-string, that is the wrong failure mode. The HPI file is 148,000 rows by 54 columns, so one malformed cell aborts the job with a `CAST_INVALID_INPUT` error naming neither the column nor the row, and locating it means bisecting the file.

Disabling ANSI on the cluster trades one problem for another: every malformed value silently becomes null, which contradicts the project's position that nothing fails quietly.

The transforms take a third path:

1. Cast with `try_cast` and `try_to_date`, which return null on a malformed value whatever the ANSI setting. Behaviour stops depending on a cluster-level flag that a future runtime upgrade could flip again.
2. After typing, compare non-null counts per column against the untyped frame. A column whose populated count fell has lost values in the cast, and the error names it.

That gives a loud failure with a diagnosis attached, which neither default provides alone. The cost is one aggregation pass over a small frame.

Key columns are excluded from the count comparison. A dedicated guard already fails on any null date or area code and reports the offending row, which covers strictly more: it catches a date that was empty at source as well as one that failed to parse. Covering `date` in both places produced a defect, recorded below.

**Volume columns cast through decimal first.** `try_cast('388.0' as int)` returns null, because the string-to-integer parser rejects a decimal point. Sales volumes are currently written as whole numbers, but this file has already changed its price formatting between releases, so volumes route through `decimal(18,6)` before `int`.

### 16. PPD retention differs by file kind, and the medallion stays acyclic

Land Registry publishes PPD in two forms that need different handling. The yearly files are state: `pp-2019.csv` is regenerated every month and stays available at a stable URL, so any version can be re-fetched at will. The monthly file is a change feed at a static URL that each release overwrites, so a release missed is a release lost.

Retention follows that asymmetry rather than following the source. Monthly deltas are stamped and kept permanently, because they cannot be recovered. Yearly files overwrite in place, because the current version is the correct one and past vintages answer no question this project asks. HPI reached the same conclusion for the same reason: a source that restates previously published values should be stored as its latest state, not as an accumulation of superseded ones. What makes PPD different is that it carries both kinds at once, so the rule is set per file kind rather than per source.

**The delta is a genuine change feed.** Two releases four months apart carry the same structure: 89,083 additions, 2,962 changes and 1,452 deletions in one, and 85,791 additions, 3,057 changes and 1,439 deletions in the other, both spanning transfer dates from 1995 to the current year. A change row carries the complete corrected record rather than a key and a changed field, so applying a correction never requires re-fetching the yearly file the record came from. Silver applies the file as a single Delta `MERGE` on TUID: insert, update, and delete in one statement.

**The delta filename records when it was fetched, not what it contains.** The landing path is stamped with the ingestion month, and the gap between that stamp and the data month is not constant: it depends on whether the run fell before or after that month's release, which lands on the 20th working day. One file stamped April held February data, another stamped July held June data. Release identity therefore comes from the content, as the largest transfer date in the file, which matched the expected month on both samples. A contiguity check over filenames proves the pipeline ran on schedule, not that consecutive releases are held.

That distinction carries a safety rule. A delta may only be applied when its release is newer than the state Silver already holds. An older delta applied to newer state would overwrite corrected values with superseded ones through its change rows, and resurrect deleted transactions through its additions. The yearly files and the delta from the same release describe the same state, so that delta applied to a table built from those files is a no-op, which makes it a usable idempotency check on the merge rather than something to skip.

**Bronze does no processing.** An earlier sketch had Bronze read each delta, work out which yearly files it affected, and re-fetch those. It is unnecessary, because the delta already contains the corrected records. It is also the wrong shape. A layer deciding what an earlier layer should ingest makes the medallion cyclic, which costs reproducibility, because Bronze can no longer be rebuilt from source alone; it makes lineage meaningless, because the arrows no longer describe dependency; and it lets a defect in Silver corrupt Bronze. Control flow of that kind belongs in the orchestrator, where the dependency runs from orchestrator to layers rather than from one layer back to another. The watermark is already that mechanism.

**Reconcile is an audit, not a completeness mechanism.** Because the delta covers every year, nothing is missing by design. A reconcile catches three other things: an ingestion that was missed, a defect in the merge, and any publisher restatement that reaches the yearly files without matching delta rows. The third is unverified, and the first full run is what tests it.

A full reconcile runs each June across 1995 to the current year. June rather than January because the current-year file appears somewhere between late March and early May, so a January run would report a missing file every year and train me to ignore it. A narrower reconcile over named years runs on demand, triggered when the delta contiguity assertion fires rather than on a calendar. Both re-pull the yearly files, diff against Silver, then overwrite the affected year partitions with `replaceWhere`.

**The reconcile has no year floor.** Two vintages of the complete yearly set, fetched four months apart, differ in every one of the 32 files. The 1995 file gained 18 rows, 2005 gained 34, 2019 gained 124, and 2025 gained 82,798. Recent years move most, because registration lags transfer, but no year is static. The deltas say the same thing independently: both sampled releases carry rows against every year from 1995 onward. Restricting the reconcile to recent years on the assumption that old files are frozen would therefore miss real corrections, and a spot check on one file over one interval could never have established that they were frozen in the first place.

**Why `replaceWhere` on transfer year is safe.** The download page describes the yearly files as transactions received in the calendar period. Read literally that is registration date, which would put a December 2018 sale registered in January 2019 into `pp-2019.csv` and make a transfer-year predicate delete rows belonging to a different file. Measured across all 32 files on two separate vintages, that is not what happens: each file contains exactly one transfer year, and `pp-2019.csv` runs from 2019-01-01 to 2019-12-31. One Bronze file maps to one Silver year partition. The published wording is loose, and the files themselves settled it.

The category B distribution says the same thing from a different direction. Land Registry began capturing category B at registration on 14 October 2013, yet the table holds 1,857 category B rows with earlier transfer dates, rising steadily from 11 in 1995 to 432 in 2012. That gradient is registration lag: the closer a transfer sits to the capture date, the more likely it was still unregistered when capture began. A file keyed on registration date could not produce it, because every category B row would then fall in 2013 or later.

**TUID is unique across the dataset**, 31,430,611 rows against the same number of distinct identifiers in the July 2026 release, and the same equality held on the earlier vintage. Silver asserts uniqueness rather than deduplicating. The two cost the same shuffle, but an assertion names the offending identifiers where a dedup silently discards rows, which is the same reasoning as the unmapped-geography guard in Decision 14. Uniqueness is not permanence: a category correction is published as a delete of the original transaction and an addition under a new identifier, so a category change arrives as a D and an A rather than as a C.

**The diff is recorded before the heal.** `quality.reconcile_run` takes one row per run and year whether or not anything differed, which is what distinguishes a clean run from one that never happened. `quality.reconcile_diff` takes the detail: identifier, difference type, and for a value mismatch the column name and both values. The reconcile then heals automatically. Halting on any difference would be too brittle to leave scheduled, but healing without recording would make the pipeline silently self-correcting, which is the failure mode the rest of this project is built to avoid. The record is what makes the overwrite accountable.

**Row counts cannot serve as the comparison.** A change row leaves the count untouched and a delete offsets an addition, so a year can match exactly on count and differ in every value. The measured gap is large: the 1995 file moved by 18 rows across four releases while a single release's delta carried 31 rows against that year. The diff therefore hashes the business columns on both sides, anti-joins on TUID for presence, then compares hashes for rows present in both, and only rows whose hash differs are unpacked to find which columns moved. Hashing also avoids comparing 31M rows column by column, but correctness is the reason for it rather than cost.

This decision would change if Land Registry stopped applying corrections to the yearly files, which would make the delta the only source of truth and remove the reconcile's basis, or if the delta stopped carrying complete records, which would force a re-fetch of affected yearly files to apply a correction.

### 17. One shared contract across Silver transforms

The first two Silver transforms diverged. BoE took lineage as function parameters and declared its Delta table in SQL with CHECK constraints; HPI let the caller stamp lineage afterwards and let `saveAsTable` infer the schema. Both worked, and neither was obviously wrong on its own, so the divergence only became visible once a third source arrived and the question became which one PPD should copy.

Each half of the split had a better answer, and they were independent, so PPD did not have to choose between them.

Lineage belongs inside the transform, taken as a parameter. Both files were reaching for determinism under test, and passing the timestamp in achieves it without cost: the transform stays pure, its output schema equals the table schema, and the tests can assert lineage. Stamping it in the caller buys the same determinism by removing two columns from the transform's contract, which then cannot be tested and do not match what the table holds.

The table is declared, not inferred. `saveAsTable` with overwrite replaces the table definition, so it cannot carry CHECK constraints or a comment. Declaring the schema once, adding constraints, and writing with `INSERT OVERWRITE` keeps the definition across runs. That matters most for PPD, which will later be modified in place by a merge and by `replaceWhere`: a bad full rebuild is fixed by re-running, while a bad merge corrupts state, and constraints are the backstop for the second case.

The column list is generated from the same constants the casts use, rather than written out again in SQL. HPI declares 56 columns, and a hand-written copy would have been free to drift from the cast without anything failing.

Two rules came out of the guards rather than the structure. A guard reports offenders as a sample and returns its frame so guards chain, but it only counts the full offending population where the frame is small enough that the extra pass is free, which holds for a daily rate series and not for 31M transactions. And a check belongs on whichever frame still carries the column: PPD validates Record Status on the source frame, because the column is deliberately dropped before the typed frame exists.

The convergence was done as a refactor with the logic held still, and verified by running the existing suites unchanged before touching them. It stopped at a shared convention. There is no base class and no shared helper module, because the three sources read a multi-sheet Excel export, a wide CSV panel with a header, and 32 headerless CSVs, and a generic guard over those would hide more than it saved.

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

**Fix (data class: hpi):** corrected at the data layer instead, the same way `doogal` had been handled. The watermark sink folder moved to `land_registry/hpi`, the rotating URL was refreshed, the master pipeline re-ran to land the file at the new path, and the stale `uk_hpi/` was deleted only after verifying the new file (the Phase 1 lesson: Succeeded is not the same as the right bytes). The Volume was then recreated at `land_registry/hpi/`.

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

### HPI Silver built on a seven-month-old release

**Discovered:** validating the first HPI Silver run. The notebook reports the newest month present in the data next to the filename it read. Both said October 2025, while the current release was May 2026.

**Root cause:** HPI's URL rotates with every monthly release, so `relative_url` in the watermark is maintained by hand. It had not been updated since the October 2025 release. Every run afterwards fetched that same URL, landed the same file, and reported success. At the transport layer a stale URL is indistinguishable from a current one. The Volume audit weeks earlier had re-run the pipeline to verify the corrected sink path, which confirmed a file landed in the right place without examining what was in it.

**Fix:** updated `relative_url` to the May 2026 release, re-ran the master pipeline, then re-ran the Silver notebook. Overwrite write mode meant no cleanup step, and the accumulated revisions to previously published months arrived with the new file. Row count rose by exactly 405 × 7, one row per geography per new month, which also confirmed no geography had appeared or disappeared in the interval.

**What the two vintages showed.** Every recent month had been restated: October 2025 moved from £269,862 to £271,441, August 2025 from £272,114 to £271,013, June 2025 from £268,547 to £266,865. Sales volumes that were null or partial filled in as registrations completed, with August 2025 rising from 59,848 to 82,989. That is the 13-month revision window working as documented, and it is the concrete case for overwrite over append: an append would have preserved every superseded value beside its correction.

**Verification:** the rebuilt table's United Kingdom row for May 2026 reads £271,295 with an index of 104.0, matching the published figure exactly.

**Lesson:** Phase 1 established that a success flag confirms bytes moved, not that the right bytes moved. This is the same failure one level up: the right bytes moved, and they were old. Neither the run status nor the filename could tell the difference, because the filename is metadata the pipeline assigned rather than a property of the content. The Silver notebook now prints the newest month found in the data beside the filename it read, so a stalled rotation is visible on every run rather than on inspection.

**Related detail:** the landed object is lower-cased (`uk-hpi-full-file-2026-05.csv`) while the source URL is mixed-case, so vintage selection matches case-insensitively. Selection is by the date token in the filename rather than by `modificationTime`, because a re-fetch of an older release would reorder by timestamp, while the filename token is the file's last data month.

### Two guards covering one column, and the less useful one won

**Discovered:** by a unit test, before the HPI transform ran on real data. A test asserting that an unparseable date fails with a message naming the expected format instead received a message reporting that the `date` column had lost a value during casting.

**Root cause:** the transform ran its cast-preservation check before its key-presence check, and the cast check covered every column including `date`. An unparseable date is a populated string that `try_to_date` turns to null, so both guards were correct about the condition and the first one reached produced the message. What it produced, `date: (1, 0)`, is true and close to useless for diagnosis.

**Fix:** excluded key columns from the cast-preservation check. This is not merely reordering. The key guard is strictly stronger on `date`, because it fails on any null date whatever the cause, while the cast check only detects populated-then-null. Covering the column in both places added no coverage and cost the better error message.

**Lessons:**

- Overlapping validation is not free. When two guards can fire on the same condition, whichever runs first defines the diagnosis, so the question is not whether every failure is caught but which guard owns each failure. That is worth deciding deliberately rather than inheriting from the order the checks happen to sit in.
- I changed the transform rather than the test's expectation. The test was asserting the behaviour I wanted; the code was the part that was wrong. Adjusting the assertion would have made the suite green and left the worse error message in place, which is the failure mode a test suite is supposed to prevent.

### A storage timestamp read as a fetch time

**Discovered:** auditing the PPD Bronze layout before writing the Silver transform. The monthly delta file was stamped `2026-04` in its landing path and its blob `modificationTime` read 14 May 2026, but the newest transfer date inside it was 27 February 2026. A file fetched in May should have carried the release published on 30 April.

**First hypothesis, wrong:** that the S3 website endpoint in the watermark had stopped receiving updates, since the download page switched to a different published host in February 2026. A HEAD request against both hosts returned identical `Last-Modified` and `Content-Length`, so the older name is an alias for the same object rather than a frozen mirror. The URL was never the problem.

**Root cause:** `modificationTime` records when bytes were last written to that path, not when they were fetched from source. The `raw` to `bronze` container restructure earlier in Phase 2 rewrote the timestamp on every object without re-fetching any of them. The content was the release published on 27 March 2026, carrying February registrations, and the May timestamp described the move rather than the data.

**Corroboration:** `pp-2026.csv` was 17.2 MB in Bronze against 41.8 MB live at the same URL. That is independent of the transfer-date reasoning and settled it without argument.

**Fix:** re-ran the master pipeline against the documented host. The reload landed the July 2026 release, and the row count moved from 31,192,683 to 31,430,611.

**Lesson:** this inverts the conclusion recorded against the BoE source. There, byte-identical refreshes between revisions mean an unchanged row count is expected, so freshness has to come from `modificationTime` rather than from the content. Here `modificationTime` was the misleading signal and the content was the honest one. Neither is reliable alone, because a storage timestamp is a property of the path while freshness is a property of the bytes. Where the two disagree the content decides, which means the check that catches it has to read something the pipeline did not assign.

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

- Silver-layer ingestion notebooks, one per source. BoE, HPI, and PPD are complete: transform modules, notebooks, and unit tests are committed, and all three tables are live. BoE and HPI are validated against externally published figures; PPD reconciles three ways on its own row counts, by category split, by property type and tenure, and by transfer year. Doogal is next.
- The PPD delta merge and the reconcile path described in Decision 16 are designed but not built. Silver is currently a full rebuild from the yearly files.

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
   - Raised in priority by the stale-release bug above: the three rotating-URL sources fail silently when their URL is not maintained

5. **pytest + chispa test suite**
   - SparkSession fixture in `tests/conftest.py`. It reuses the cluster's session when one already exists, because builder options are ignored at that point and stopping the session would detach the notebook.
   - Per-source transform tests in `tests/test_silver_transforms/`. BoE is covered by 26 tests: the data-quality guard, an exact end-to-end SCD2 scenario, the multi-row invariants Delta CHECK constraints cannot express (exactly one current row, contiguous non-overlapping intervals), and edge cases including sixteenth-precision decimals and null-rate gaps. HPI is covered by 35 tests: the four integrity guards, the coverage floor at every nation and composite boundary, typing across both published decimal formats and the volume decimal-point case, null preservation, and an end-to-end projection. PPD is covered by 47 tests: the positional read contract, the code sets at every published value, the one-year-per-file rule the partition key rests on, key uniqueness across files, and the cases where a fault would change no row count and raise nothing, such as a reordered projection or a transfer date carrying a real time component. 108 tests in total across the three sources.
   - Quality framework tests in `tests/test_quality_framework/`
   - Runs locally with `pytest` against the versions pinned in `requirements-dev.txt`, and on the cluster through `tests/run_tests.py`. CI integration deferred to Phase 4.
   - Test-only dependencies stay notebook-scoped or local rather than on the cluster spec (see Decision 13)

6. **Initial Silver → Gold design**
   - Multi-source joins on postcode
   - Star-schema fact + dimension tables (Kimball)
   - Enrichment: price × rent yield, price × rate affordability, price × crime index

### Planned order of delivery

1. ✅ Databricks workspace + cluster + Unity Catalog + storage layer + Bronze Volumes
2. ✅ First Silver notebook against the simplest source (BoE): minimal schema complexity
3. ✅ HPI (single cumulative CSV, wide monthly panel, per-nation coverage floor)
4. ✅ PPD (31.4M rows, partitioned on transfer year, TUID uniqueness asserted)
5. Doogal (ZIP unzip, large postcode table)
6. ONS (XLSX with headers and footers to skip)
7. Police.uk (most complex: ZIP containing ~4500 nested CSVs, multi-snapshot deduplication)
8. Quality-rules framework, extracted from patterns observed during (2)–(7)
9. Watermark automation
10. Gold-layer joins

---

*Design document status: Phase 2 in progress. Foundation provisioned, BoE, HPI, and PPD Silver complete and unit-tested, three sources remaining.*
