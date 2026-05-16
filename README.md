# UK Property Market Intelligence Platform
Built by Md. Rais Al Kabir Joy · [GitHub](https://github.com/joy7652)

A config-driven, multi-source Azure data platform ingesting UK residential property market data across six official open datasets. Demonstrates production-grade data engineering practices including incremental ingestion, schema evolution handling, automated data quality validation, and statistical anomaly detection.

> **Status:** Phase 1 complete — Bronze ingestion for all six sources. Phase 2 in progress — Databricks workspace, Unity Catalog, and medallion storage layer provisioned. Silver-layer transformation notebooks in development.

---

## Table of contents

- [Elevator pitch](#elevator-pitch)
- [Architecture](#architecture)
- [Data sources](#data-sources)
- [Key engineering decisions](#key-engineering-decisions)
- [Bugs found and fixed](#bugs-found-and-fixed)
- [Tech stack](#tech-stack)
- [Repository structure](#repository-structure)
- [Repository workflow](#repository-workflow)
- [Running the pipelines](#running-the-pipelines)
- [Roadmap](#roadmap)
- [Architectural talking points](#architectural-talking-points)

---

## Elevator pitch

A configurable, multi-source data engineering platform that ingests, validates, transforms, and analyses UK residential property market data. Pipelines are driven by a single JSON configuration file — new sources are added by editing config, not code. The architecture demonstrates watermark-based incremental loading, parameterised linked services, reusable ingestion patterns, and defensive data-quality validation.

**Why this project:** Property data is immediately recognisable to UK hiring managers and used across finance, consulting, and the public sector. Monthly-updated sources frame this as a production pipeline rather than a one-off analysis. The deliberately messy real-world data surfaces non-trivial transformations, and the config-driven architecture demonstrates senior-level thinking — the same framework could ingest any multi-source analytical dataset with no code changes.

---

## Architecture
![Master pipeline orchestration](docs/screenshots/master_orchestrator.png)
```
watermark.json  (Git-integrated)
              ↓
Azure Data Factory  (config-driven orchestration)
              ↓
ADLS Gen2 bronze/  (raw files, 6 sources, external location)
              ↓
Azure Databricks + Unity Catalog  (transformation, quality, governance)
              ↓
ADLS Gen2 silver/ → gold/  (Delta Lake, managed tables, schema-level locations)
   plus ADLS Gen2 quality/  (quarantine + DQ outputs)
              ↓
Azure Synapse Serverless SQL  (planned)
              ↓
Fabric / Power BI  (planned)
```

### Design principles

- **Config over code.** Adding a source means adding a JSON block to the watermark, not writing a new pipeline.
- **Patterns over sources.** Two ingestion patterns (`yearly_stepped`, `single_file`) cover every source; each source declares which pattern it uses.
- **Trust nothing silently.** Pipeline success status confirms bytes moved, not that the right bytes moved. Binary-format files are validated against expected magic bytes before Silver-layer processing.
- **Parameterised linked services.** HTTP linked services are host-agnostic and configured per-request via `@{linkedService().p_base_url}`, rather than one linked service per host.

---

## Data sources

Six official UK government and regulated open datasets:

| # | Source | Format | Pattern | Step | Update cadence |
|---|--------|--------|---------|------|----------------|
| 1 | HM Land Registry — Price Paid Data (PPD) | CSV, per year | `yearly_stepped` | 1 year | Monthly cumulative increment |
| 2 | HM Land Registry — UK House Price Index (HPI) | CSV, cumulative | `single_file` | — | Monthly |
| 3 | Doogal — UK Postcode Lookup (ONSPD mirror) | ZIP | `single_file` | — | Quarterly |
| 4 | Bank of England — Official Bank Rate | XLS | `single_file` | — | Monthly |
| 5 | ONS — Price Index of Private Rents | XLSX | `single_file` | — | Monthly (URL rotates) |
| 6 | UK Police — Street-level Crime | ZIP (~1.7 GB) | `yearly_stepped` | 2 years | Monthly rolling 3-year snapshot |

### Source rationale

**Price Paid Data** is the authoritative record of UK residential transactions since 1995 — 24M+ records, the backbone of any property analysis. **HPI** provides official price indices validated by the same department, useful as a cross-source oracle for sanity-checking derived metrics. **Postcode lookups** enable geocoding and regional aggregation. **Bank of England rates** and **ONS rents** provide the macro context — affordability analysis needs both the price and the cost of money. **Police crime data** adds a classic property-investment overlay (safety × price growth) that joins cleanly on postcode.

### Source swap: EPC → Police.uk

The original sixth source was MHCLG's Energy Performance Certificates (EPC). During build, I discovered:

1. The existing `epc.opendatacommunities.org` service was being retired on 30 May 2026.
2. Its replacement (`Get energy performance of buildings data` on GOV.UK) required GOV.UK One Login OAuth2 — incompatible with ADF's native HTTP Basic authentication.
3. Waiting for production launch would block the project indefinitely.

I evaluated three paths (push through, wait, or swap) and chose to swap. Police.uk is auth-free, stable since 2010, and has stronger analytical value for a property intelligence platform. The swap required zero new pipeline code — the `single_file` pattern absorbed it directly.

**Key detail discovered through smoke-testing first:** I initially planned a `monthly_backfill` pattern for police.uk, assuming each monthly file was a delta. A pre-build smoke test showed the file was 1.72 GB — revealing each archive is a rolling 3-year snapshot, not a monthly delta. Building a backfill pattern would have wasted 95% of bandwidth and storage. Swapped to `single_file` pattern: latest snapshot only.

---

## Key engineering decisions

### 1. Load patterns are a closed set

Two patterns cover every ingestion shape encountered:

- **`yearly_stepped`** — iterate across years with a configurable step, one file per step. Incremental dispatches to one of two children via `incremental_type`: `static_url` (PPD's cumulative monthly update file) or `templated_latest` (Police.uk's monthly-rotating snapshot URL).
- **`single_file`** — one URL fetches one file per refresh. Used by HPI, Doogal, BoE, ONS.

The `yearly_stepped` pattern was consolidated from an earlier `yearly_range` pattern once Police.uk's 2-year snapshot cadence proved the step parameter's value. Refactored only at the point a concrete second use case demonstrated the abstraction would pay off — deliberate discipline against speculative generalisation upfront.

Incremental logic is itself a three-level route cascade (outer full-vs-incremental decision → month-rate-limiter → type dispatch). The layering exists because Azure Data Factory prohibits nested control-flow activities (If inside Switch inside ForEach), so each layer unwraps exactly one activity.

### 2. Linked services organised by authentication pattern

HTTP linked services are named and organised by their *authentication shape*, not by the data they serve:

| Linked service | Auth/headers | Sources |
|---|---|---|
| `LS_HTTP_Anonymous` | None | PPD, Police.uk |
| `LS_HTTP_User_Agent_Header` | User-Agent only | Doogal |
| `LS_HTTP_Accept_Header` | User-Agent + `Accept: */*` | BoE, ONS |
| `LS_HTTP_LandRegistry_HPI` | Full browser header mimicry (Akamai CDN) | HPI |

This taxonomy is the correct one — auth is a cross-cutting concern that multiple unrelated sources can share, while the data source itself is incidental. An earlier iteration named one linked service after its first data source (`LS_HTTP_LandRegistry`), which obscured the fact that Police.uk could reuse it. Renaming to `LS_HTTP_Anonymous` clarified the taxonomy and eliminated the need for a duplicate linked service.

Each linked service's base URL is parameterised via `@{linkedService().p_base_url}` and passed through at dataset runtime. Datasets in turn expose their own `p_base_url` parameter via `@dataset().p_base_url` on the dataset's Base URL field. This chain means any source can use any linked service, with runtime binding of the host.

### 3. Watermark stored as JSON array in ADLS

The watermark (per-source state: last ingestion date, URL parameters, load pattern) lives at `config/watermark.json` in ADLS. Design decisions:

- **Array, not object** — ADF's Lookup + ForEach only iterates arrays cleanly.
- **ADLS, not Azure SQL** — eliminates an entire database dependency. When Databricks joins the stack, the watermark migrates to a Delta table.
- **Manual updates for now** — Databricks notebook to programmatically update watermark on successful runs is planned.

### 4. Master orchestration pattern

```
PL_Master_Orchestrator
├─ Lookup: read watermark.json
├─ Filter: keep only active sources (handles soft-disable via active: false)
└─ ForEach (sequential):
    └─ Switch on load_pattern:
        ├─ yearly_stepped  → Execute PL_Route_Yearly_Stepped
        ├─ single_file   → Execute PL_Single_File_Full_Load
        └─ Default       → log and skip
```

`PL_Route_Yearly_Stepped` exists because ADF prohibits nested control-flow activities (e.g. `If` inside `Switch`). Extracting the inner logic into a child "route" pipeline preserves the full-vs-incremental branch for yearly sources. This constraint drove cleaner separation of concerns: orchestration → routing → execution.

### 5. Medallion data lake architecture

- **Bronze** — raw files as received, no transformation, in the dedicated `bronze` container. Registered as a Unity Catalog external location; read-only from Silver pipelines.
- **Silver** — validated, typed, deduplicated, schema-enforced. Delta managed tables under `uk_property_intel.silver`, physically stored in the `silver` container.
- **Gold** — joined, enriched, denormalised for analytical consumption. Star-schema fact and dimension tables under `uk_property_intel.gold`, in the `gold` container.
- **Quality** — quarantine records, rule-run history, and DQ metrics under `uk_property_intel.quality`, in the `quality` container.

Each medallion layer maps 1:1 to an Azure Blob container, a Unity Catalog schema, and a schema-level managed location. This deliberate physical-to-logical correspondence keeps cost attribution, lifecycle policy, and RBAC scoping per layer obvious from the storage account view alone.

Bronze is complete. Silver is in active development; Gold and Quality follow.

### 6. Unity Catalog over hive_metastore

Adopted Unity Catalog from day 1 of Phase 2, rather than the legacy hive_metastore that older Databricks projects (including most tutorials) use. Reasoning:

- Databricks confirmed in 2026 that new Azure workspaces created from 30 September 2026 onwards will be UC-only — hive_metastore is being phased out across the product.
- UC provides centralised access control, lineage, and discovery — concerns that would otherwise have to be implemented or skipped.
- All data access flows through the UAMI on the Databricks Access Connector; no secrets, mounts, or SAS tokens.
- The DP-700 exam covers UC; building on it doubles as exam preparation.

The complexity tradeoff (metastore-level setup, access connector configuration, storage credentials, external locations) is paid once at the start of Phase 2.

### 7. Per-layer physical containers over single-container subfolders

Each medallion layer (bronze, silver, gold, quality) is a dedicated Azure Blob container, not a subfolder within a shared container. Reasoning:

- **Lifecycle policies** can differ per layer (e.g. Bronze long-retain raw, Silver shorter retention on intermediate artifacts).
- **RBAC and cost attribution** scope cleanly to the container boundary.
- **Physical and logical alignment** — each container maps 1:1 to a Unity Catalog schema, making the medallion architecture visible in the storage account itself.

A fifth container, `catalog-root`, exists purely as the Unity Catalog managed location for the catalog itself. It remains empty in practice because every schema declares its own managed location, but provides the storage anchor that UC requires at the catalog level.

### 8. Dedicated cluster access mode

Despite Standard (shared) access mode being the newer Databricks default for general data engineering, the cluster uses Dedicated access mode (formerly "single user"). Reasoning:

- Solo-developer project: Standard's multi-user isolation provides benefits I don't need while imposing restrictions (no RDD APIs, restricted Kafka options, no ML runtime) I might encounter in Phase 3+.
- Forward-compatibility with the planned anomaly-detection work in Phase 3, which may use library code outside the Standard sandbox.
- Same DBU cost as Standard — Dedicated isn't a price premium.

A Standard-mode cluster would also work for Phase 2; the choice is a deliberate cost-free hedge against future requirements rather than a hard necessity.

### 9. File events disabled on external locations

Unity Catalog external locations support Azure Event Grid-based file change notifications for ingestion performance. All external locations in this project explicitly disable this feature. Reasoning:

- Batch ingestion at the project's data scale (sub-GB per source) does not benefit from event-driven discovery.
- Enabling file events would require granting the UAMI `Storage Account Contributor` (control plane), `EventGrid EventSubscription Contributor`, and `Storage Queue Data Contributor` — significantly broader scope than the `Storage Blob Data Contributor` (data plane only) required for the actual data path.
- Least-privilege identity model is preferred for portfolio cleanliness and would be the right call in any regulated environment.

---

## Bugs found and fixed

### Latent parameter-shadowing bug in dataset configuration

**Discovered:** During ONS source onboarding — when the ONS URL was updated in the watermark for a new monthly release, the pipeline continued writing files to ADLS under the correct name but with wrong content. ADF reported "Succeeded" for every run.

**Symptoms:**
- `openpyxl.load_workbook()` failed with `BadZipFile: File is not a zip file`.
- Hex-dump of the "XLSX" showed HTML content starting `<!DOCTYPE html>`.
- The HTML was from `bankofengland.co.uk`, not `ons.gov.uk` — meaning the pipeline was fetching from the wrong host.

**Root cause:** HTTP datasets had parameters defined (`p_base_url`, `p_relative_url`) but their Base URL fields were hardcoded rather than using `@dataset().p_base_url`. For five sources this was undetectable because the hardcoded value happened to match the watermark value. When the ONS URL was updated in the watermark, the dataset continued using the stale hardcoded URL — which pointed to BoE's host — and BoE returned a 200 OK HTML homepage for the bad request path.

**Fix:** Replaced every hardcoded URL in dataset Base URL fields with the appropriate `@dataset()` expression. Re-ran master pipeline for affected sources. Validated output files by attempting to parse them in their native format.

**Lesson:** Pipeline success status confirms bytes moved, not that the right bytes moved. Two follow-ups:
1. Adding magic-byte validation to the Silver-layer ingestion contract (every file checked against expected format header before parsing).
2. Audit of every parameterised dataset field to confirm parameters are actually wired, not just defined.

### ADF nested control-flow restriction

**Discovered:** When trying to nest `If Condition` inside `Switch` inside `ForEach` for the yearly-stepped full-vs-incremental logic.

**Fix:** Extract inner logic into child pipelines (`PL_Route_Yearly_Stepped`). Master Switch calls the route pipeline, route pipeline contains the If Condition. Resulted in cleaner architecture with better separation of concerns.

### URL query-string double-encoding (ONS)

**Discovered:** ONS uses relative URLs of the form `?uri=/path/to/file.xlsx`. ADF URL-encoded the `?` if the relative URL started with anything else, corrupting the request.

**Fix:** Always prefix the relative URL with `?` as the first character. ADF preserves the query-string delimiter when it's at position 0 of the relative URL.

---

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory (Git-integrated) |
| Storage | Azure Data Lake Storage Gen2 (per-layer containers) |
| Compute | Azure Databricks (PySpark, Delta Lake, Photon-eligible) |
| Governance | Unity Catalog (managed tables, schema-level managed locations) |
| Identity | User-assigned managed identity via Databricks Access Connector |
| Query (planned) | Azure Synapse Serverless SQL |
| Visualisation (planned) | Microsoft Fabric / Power BI |
| Source control | GitHub (trunk-based, branch-protected main) |
| Testing (planned) | pytest + chispa for PySpark transforms |
| CI/CD (planned) | GitHub Actions |

---

## Repository workflow

This repository follows a **trunk-based workflow** with short-lived feature branches:

- One branch per logical unit of work (e.g. `phase2/setup-catalog-schemas`, `phase2/boe-silver`).
- All changes merged to `main` via pull request, with `main` protected against direct pushes.
- ADF Studio is Git-integrated; pipeline JSON commits go to feature branches, then merge to `main` via PR. Publishing from ADF promotes the live factory.
- Databricks notebooks are managed via Databricks Git folders linked to this repo, committed from the workspace UI on the same feature branches.

Both ADF and Databricks operate on the same Git branches as local development, so the repo on `main` always reflects the live state of every component.

## Repository structure

```
uk-property-intelligence-platform/
├── README.md
├── adf/
│   └── pipelines/                       # JSON definitions, synced via ADF Git integration
│       ├── PL_Master_Orchestrator.json
│       ├── PL_Single_File_Full_Load.json
│       ├── PL_Yearly_Stepped_Full_Load.json
│       ├── PL_Route_Yearly_Stepped.json
│       ├── PL_Route_Incremental_Load.json
│       ├── PL_Incremental_Load_StaticURL.json
│       └── PL_Incremental_Load_TemplatedLatest.json
├── config/
│   ├── watermark.json                   # per-source state, URL parameters, load patterns
│   └── quality_rules.json               # (planned) per-source validation rules for Silver
├── databricks/
│   ├── setup/
│   │   └── 01_create_schemas.py         # Unity Catalog schema definitions (SQL via %sql cells)
│   ├── silver/
│   │   ├── notebooks/                   # one notebook per source
│   │   └── transforms/                  # importable transformation functions (unit-testable)
│   ├── gold/
│   │   ├── notebooks/
│   │   └── transforms/
│   ├── quality/
│   │   ├── rules/                       # JSON rule definitions per source
│   │   └── framework/                   # rule-application engine
│   └── utils/                           # shared constants (paths), Spark helpers, logging
├── tests/
│   ├── conftest.py                      # pytest SparkSession fixture
│   ├── test_silver_transforms/
│   └── test_quality_framework/
├── synapse/                             # (planned) external table definitions
├── .github/
│   └── workflows/                       # (planned) CI/CD
└── docs/
    ├── source_discovery_notes.md        # notes on each source's quirks, auth patterns
    ├── DESIGN.md                        # design document of the entire project
    └── screenshots/
        └── master_orchestrator.png
```

---

## Running the pipelines

### Prerequisites

**Phase 1 (Bronze ingestion):**
- Azure subscription with:
  - Azure Data Factory instance (Git-integrated with this repo)
  - ADLS Gen2 storage account with a `bronze` container
  - ADF managed identity granted `Storage Blob Data Contributor` on the storage account
- Watermark file at `config/watermark.json` in the storage account

**Phase 2 (Silver/Gold transformation):**
- Azure Databricks workspace (Premium tier, Unity Catalog enabled)
- Azure Databricks Access Connector with a user-assigned managed identity
- ADLS Gen2 containers: `bronze`, `silver`, `gold`, `quality`, `catalog-root`
- UAMI granted `Storage Blob Data Contributor` on the storage account
- Unity Catalog: catalog `uk_property_intel` with schemas `silver`, `gold`, `quality`
- Dedicated-access cluster (single-user) on latest DBR LTS

### Initial load

1. Ensure all `active` flags in the watermark are set as desired (set unneeded sources to `active: false` to skip).
2. Trigger `PL_Master_Orchestrator` from ADF Studio or via trigger.
3. On first run, `yearly_stepped` sources (PPD) iterate from `start_year` to current year.

### Incremental loads

For sources with `full_load_complete: true` and a valid `last_year_ingested` or `last_refreshed` date, subsequent runs of the master pipeline fetch only new data.

### Monthly manual updates

Two sources have fully-dynamic URLs that change with each release and require watermark updates:

- **HPI** — update `relative_url` with new monthly filename.
- **ONS Private Rents** — update `relative_url` with new monthly path and filename.
- **Police.uk** — update `relative_url` with latest monthly archive.

Databricks notebook to automate these URL updates via pattern matching is planned.

---

## Roadmap

### Phase 1 — Bronze ingestion ✅

- [x] ADF instance with Git integration (repo: uk-property-intelligence-platform)
- [x] 4 linked services organised by authentication pattern
- [x] Parameterised HTTP + ADLS datasets (base URL parameter chain)
- [x] `PL_Yearly_Stepped_Full_Load` — generalised full-load for stepped yearly patterns
- [x] `PL_Single_File_Full_Load` — switch-routed single-file full-load
- [x] `PL_Route_Incremental_Load` — month-rate-limited incremental router
- [x] `PL_Incremental_Load_StaticURL` — static-URL incremental (PPD)
- [x] `PL_Incremental_Load_TemplatedLatest` — templated-URL incremental (Police.uk)
- [x] `PL_Route_Yearly_Stepped` — full-vs-incremental router for year-stepped sources
- [x] `PL_Master_Orchestrator` — watermark-driven orchestration with active-source filtering
- [x] All 6 sources landing in Bronze with verified file integrity

### Phase 2 — Silver layer (in progress)

- [x] Databricks workspace (Premium) + Dedicated-access cluster
- [x] Unity Catalog: dedicated `uk_property_intel` catalog
- [x] Per-layer container storage layout with schema-level managed locations
- [x] Access via UAMI + Databricks Access Connector (no mounts, no secrets)
- [x] Bronze restructure: container rename `raw` → `bronze`, removed redundant subfolder
- [ ] Silver notebooks for all 6 sources
- [ ] Parameterised quality-rules framework (`quality_rules.json`)
- [ ] Magic-byte validation for binary inputs
- [ ] Quarantine table for rejected records
- [ ] `pipeline_audit` table for per-run quality scores
- [ ] Watermark automation from Databricks (Delta MERGE into watermark table)
- [ ] pytest + chispa test suite for transformation functions

### Phase 3 — Gold layer

- [ ] Multi-source joins on postcode
- [ ] Enrichment: price × rent yield, price × rate affordability, price × crime index
- [ ] Denormalised analytical tables

### Phase 4 — Advanced features

- [ ] Statistical anomaly detection (3-sigma rolling window on price changes)
- [ ] Delta Lake schema evolution demonstration
- [ ] GitHub Actions CI/CD (JSON schema validation for ADF pipelines and config)

### Phase 5 — Consumption

- [ ] Synapse Serverless external tables over Gold
- [ ] Fabric / Power BI dashboards:
  - Property Market Dashboard (price trends, rent yields, crime overlay)
  - Pipeline Health Dashboard (run history, quality scores, anomaly alerts)

---

## Architectural talking points

This project deliberately demonstrates engineering judgement, not just tool fluency:

- **Recognising latent defects** — the ONS bug was caught through local validation, not by trusting pipeline status.
- **Discovery before build** — the police.uk smoke test saved building the wrong pattern.
- **Risk assessment before commitment** — the EPC pivot was a deliberate decision when the source environment changed mid-project.
- **Deferred abstractions** — consolidated `yearly_range` into `yearly_stepped` only when Police.uk's 2-year cadence provided a concrete second use case for the step parameter; refused to abstract speculatively before that.
- **Silent-failure prevention** — magic-byte validation is a design response to the ADF "success" story being insufficient.
- **Unity Catalog adoption from day 1** — chose the current governance model rather than the legacy hive_metastore that older tutorials default to.
- **Identity over secrets** — UAMI + Access Connector means zero rotated credentials in the data path.
- **Physical-logical correspondence** — one container = one schema = one medallion layer is a deliberate clarity choice, not the default.
- **Cost-free hedges** — Dedicated cluster mode and disabled file events both cost nothing and trade marginal aesthetic compromises for capability and least-privilege identity scope respectively.

---

## Licence

© 2026 Md.Rais Al Kabir Joy. All rights reserved.

This repository is published for portfolio and demonstration purposes. The source code, configuration, and documentation may not be copied, modified, distributed, or used in derivative works without express written permission. Viewing and referencing for evaluation purposes (e.g. by prospective employers) is permitted.

---

*Project status: Phase 2 in progress.*
