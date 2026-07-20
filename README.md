# UK Property Market Intelligence Platform
Built by Md. Rais Al Kabir Joy · [GitHub](https://github.com/joy7652)

A multi-source Azure data platform that ingests, validates, and transforms UK residential property data from six official open datasets, built around HM Land Registry's 24M+ residential transactions since 1995. The pipelines run off a single JSON config file, so adding a source means editing config rather than writing code. It loads incrementally from a per-source watermark, validates every file against its expected format before parsing instead of trusting the orchestrator's success flag, and governs all access through Unity Catalog. Later phases add statistical anomaly detection and BI dashboards.

> **Status:** Phase 1 complete — Bronze ingestion for all six sources. Phase 2 in progress — Databricks workspace, Unity Catalog, and the medallion storage layer are provisioned. Silver-layer transformation notebooks are in development.

**Highlights**

- 6 official UK datasets: Land Registry PPD and HPI, ONS rents, BoE base rate, ONS postcodes (via Doogal), Police.uk crime
- 24M+ property transactions since 1995
- Config-driven ingestion: a new source is 1 JSON block in the watermark, not a new pipeline
- Incremental by per-source watermark, with 2 reusable load patterns covering all 6 sources
- Magic-byte validation and a quarantine/quality layer, because a success flag only means bytes moved
- Unity Catalog governance over a medallion lake on ADLS Gen2, with no secrets in the data path

---

## Table of contents

- [Why this project](#why-this-project)
- [Architecture](#architecture)
- [Data sources](#data-sources)
- [Key engineering decisions](#key-engineering-decisions)
- [Bugs found and fixed](#bugs-found-and-fixed)
- [Tech stack](#tech-stack)
- [Repository structure](#repository-structure)
- [Repository workflow](#repository-workflow)
- [Running the pipelines](#running-the-pipelines)
- [Roadmap](#roadmap)

---

## Why this project

Property data shows up across finance, consulting, and the public sector, and the sources refresh monthly, so this runs as a live pipeline rather than a one-off analysis. The data is messy enough to make the transformations real, and the config-driven design would ingest any other multi-source dataset without code changes.

---

## Architecture
![Master pipeline orchestration](docs/screenshots/master_orchestrator.png)
```
watermark.json  (Git-integrated)
              ↓
Azure Data Factory  (config-driven orchestration)
              ↓
ADLS Gen2 bronze/  (raw files, 6 sources, exposed as UC External Volumes)
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

- Adding a source means appending a JSON block to the watermark, not writing a new pipeline.
- Two ingestion patterns (`yearly_stepped` and `single_file`) cover every source. Each source declares which one it uses.
- After the first full load, each source tracks its own state in the watermark, so later runs fetch only what's new. What counts as "new" is source-specific (a cumulative update file for PPD, a fresh rolling snapshot for Police.uk), so incremental loading routes by type rather than assuming one mechanism.
- A pipeline reporting success only tells me bytes moved, not that the right bytes moved. Binary files are checked against their expected magic bytes before any Silver-layer parsing.
- HTTP linked services are host-agnostic and take their base URL per request via `@{linkedService().p_base_url}`, instead of one linked service per host.

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

### Why these sources

**Price Paid Data** is the authoritative record of UK residential transactions since 1995, 24M+ records, and the backbone of any property analysis. **HPI** gives official price indices validated by the same department, which makes it a useful cross-check for any metric I derive myself. **Postcode lookups** handle geocoding and regional aggregation. **Bank of England rates** and **ONS rents** supply the macro picture; affordability needs both the price and the cost of money. **Police crime data** adds the classic property-investment overlay of safety against price growth, and it joins cleanly on postcode.

### Source swap: EPC → Police.uk

The sixth source was originally going to be MHCLG's Energy Performance Certificates (EPC). Partway through the build I hit three problems:

1. The existing `epc.opendatacommunities.org` service was scheduled to retire on 30 May 2026.
2. Its replacement (`Get energy performance of buildings data` on GOV.UK) needed GOV.UK One Login OAuth2, which ADF's native HTTP Basic auth can't handle.
3. Waiting for the replacement to reach production launch would have stalled the project with no firm date.

I weighed three options (push through on the dying service, wait, or swap) and swapped. Police.uk is auth-free, has been stable since 2010, and arguably carries more analytical weight for a property platform than EPC would have. It also fit the existing yearly pattern, so the swap cost almost no new pipeline code.

**What the smoke test caught:** I'd planned a `monthly_backfill` pattern for police.uk on the assumption that each monthly file was a delta. A quick pre-build smoke test showed the archive was 1.72 GB, because each one is a rolling 3-year snapshot rather than a monthly increment. A backfill pattern would have re-downloaded the same months over and over and burned most of the bandwidth and storage. Instead I generalised the existing PPD pattern (`yearly_range`) into `yearly_stepped` with a 2-year step: fetch the December snapshot every other year for history, and the latest published snapshot for incremental runs.

---

## Key engineering decisions

### 1. Load patterns are a closed set

Two patterns cover every ingestion shape I ran into:

- **`yearly_stepped`** — iterate across years with a configurable step, one file per step. Incremental work dispatches to one of two children via `incremental_type`: `static_url` (PPD's cumulative monthly update file) or `templated_latest` (Police.uk's monthly-rotating snapshot URL).
- **`single_file`** — one URL fetches one file per refresh. Used by HPI, Doogal, BoE, and ONS.

`yearly_stepped` grew out of an earlier `yearly_range` pattern. I added the step parameter only once Police.uk's 2-year cadence gave me a real second use for it, rather than generalising before I needed to.

The incremental logic is a three-level route cascade: an outer full-vs-incremental decision, then a month rate-limiter, then the type dispatch. The layering exists because Azure Data Factory won't let control-flow activities nest (an If inside a Switch inside a ForEach), so each layer unwraps exactly one activity.

### 2. Linked services organised by authentication pattern

HTTP linked services are named and grouped by their *authentication shape*, not by the data they serve:

| Linked service | Auth/headers | Sources |
|---|---|---|
| `LS_HTTP_Anonymous` | None | PPD, Police.uk |
| `LS_HTTP_User_Agent_Header` | User-Agent only | Doogal |
| `LS_HTTP_Accept_Header` | User-Agent + `Accept: */*` | BoE, ONS |
| `LS_HTTP_LandRegistry_HPI` | Full browser header mimicry (Akamai CDN) | HPI |

I group them this way because authentication is a cross-cutting concern that several unrelated sources can share, whereas the data source behind a connection is incidental to how you connect to it. Early on I named one linked service after the first source it served (`LS_HTTP_LandRegistry`), which hid the fact that Police.uk could reuse it. Renaming it to `LS_HTTP_Anonymous` made the grouping obvious and saved a duplicate linked service.

Each linked service's base URL is parameterised via `@{linkedService().p_base_url}` and bound at dataset runtime. Datasets in turn expose their own `p_base_url` via `@dataset().p_base_url` on their Base URL field. The chain means any source can use any linked service, with the host resolved at runtime.

### 3. Watermark stored as a JSON array in ADLS

The watermark holds per-source state (last ingestion date, URL parameters, load pattern) at `config/watermark.json` in ADLS. Three decisions shaped it:

- Array, not object: ADF's Lookup plus ForEach iterates arrays cleanly.
- ADLS, not Azure SQL: this drops an entire database dependency. When Databricks joins the stack, the watermark moves to a Delta table.
- Manual updates for now: a Databricks notebook to update the watermark on successful runs is planned.

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

`PL_Route_Yearly_Stepped` exists because ADF won't nest control-flow activities (an If inside a Switch, for instance). Pulling the inner logic into a child route pipeline keeps the full-vs-incremental branch for yearly sources. The constraint ended up forcing a clean split: orchestration, then routing, then execution.

### 5. Medallion data lake architecture

- **Bronze** — raw files as received, no transformation, in a dedicated `bronze` container. Exposed via Unity Catalog External Volumes under `uk_property_intel.bronze.*`, so Silver access flows through UC governance and lineage.
- **Silver** — validated, typed, deduplicated, schema-enforced. Delta managed tables under `uk_property_intel.silver`, physically stored in the `silver` container.
- **Gold** — joined, enriched, denormalised for analytical consumption. Star-schema fact and dimension tables under `uk_property_intel.gold`, in the `gold` container.
- **Quality** — quarantine records, rule-run history, and DQ metrics under `uk_property_intel.quality`, in the `quality` container.

Each layer maps one-to-one to a Blob container and a Unity Catalog schema. Silver, Gold, and Quality use schema-level managed locations; Bronze uses External Volumes pointing at the bronze container (see Decision 10). Keeping the physical and logical layouts aligned means cost attribution, lifecycle policy, and RBAC all scope per layer, and you can read the medallion structure straight off the storage account.

Bronze is complete. Silver is in active development; Gold and Quality follow.

### 6. Unity Catalog over hive_metastore

I adopted Unity Catalog from day one of Phase 2 rather than the legacy hive_metastore that older Databricks projects (and most tutorials) still use. Reasoning:

- Databricks has confirmed that from 30 September 2026, all new workspaces (Azure included) are provisioned Unity Catalog-only, with no Hive metastore, so building on UC now is the forward-compatible path.
- UC gives centralised access control, lineage, and discovery, which I'd otherwise have to build myself or skip.
- All data access flows through the UAMI on the Databricks Access Connector. No secrets, mounts, or SAS tokens.
- The DP-700 exam covers UC, so building on it doubles as exam preparation.

The complexity cost (metastore setup, access connector, storage credentials, external locations) is paid once at the start of Phase 2.

### 7. Per-layer physical containers over single-container subfolders

Each medallion layer (bronze, silver, gold, quality) is its own Azure Blob container, not a subfolder in a shared one. Reasoning:

- Lifecycle policies can differ per layer (Bronze long-retains raw data; Silver can retain intermediate artifacts for less time).
- RBAC and cost attribution scope cleanly to the container boundary.
- Physical and logical layouts line up, so the medallion structure is visible in the storage account itself.

A fifth container, `catalog-root`, exists only as the Unity Catalog managed location for the catalog itself. It stays empty in practice because every schema declares its own managed location, but UC requires a storage anchor at the catalog level and this is it.

### 8. Dedicated cluster access mode

Standard (shared) access mode is the newer Databricks default for general data engineering, but the cluster uses Dedicated access mode (formerly "single user"). Reasoning:

- This is a solo project. Standard's multi-user isolation buys me nothing I need while imposing restrictions (no RDD APIs, restricted Kafka options, no ML runtime) I might run into in Phase 3 and beyond.
- The anomaly-detection work planned for Phase 3 may use library code outside the Standard sandbox.
- Dedicated costs the same DBUs as Standard, so it's not a price premium.

A Standard cluster would also work for Phase 2. Dedicated isn't strictly necessary; it's a free hedge against what later phases might need.

### 9. File events disabled on external locations

Unity Catalog external locations support Azure Event Grid file-change notifications for faster ingestion. Every external location here has the feature turned off. Reasoning:

- Batch ingestion at this scale (sub-GB per source) gains nothing from event-driven discovery.
- Turning file events on would require granting the UAMI `Storage Account Contributor` (control plane), `EventGrid EventSubscription Contributor`, and `Storage Queue Data Contributor` — a much wider scope than the `Storage Blob Data Contributor` (data plane only) the actual data path needs.
- Least privilege is the right call here, and the one I'd make in any regulated environment anyway.

### 10. Bronze exposed as UC Volumes, not Delta tables

Most Databricks tutorials treat Bronze as a Delta-table layer: copy raw files into Delta with some added metadata columns, then have Silver read from those tables. This project does it differently. Bronze is exposed through Unity Catalog **External Volumes** pointing at the raw files in the `bronze` container, and the Silver notebooks read straight from those Volume paths.

At this scale (sub-GB per source, durable raw files, no ad-hoc SQL on the raw data), copying Bronze into Delta would let me put a "Bronze layer" label on the diagram and not much else; it wouldn't do anything the raw files don't already do. Exposing Bronze as Volumes is worth it for a different reason: it adds UC governance, lineage, discoverability, and stable paths without re-writing the data first.

See DESIGN.md Decision 11 for the conditions that would change this call.

---

## Bugs found and fixed

### Latent parameter-shadowing bug in dataset configuration

**Discovered:** During ONS onboarding. When I updated the ONS URL in the watermark for a new monthly release, the pipeline kept writing files to ADLS under the correct name but with the wrong content, and ADF reported "Succeeded" every time.

**Symptoms:**
- `openpyxl.load_workbook()` failed with `BadZipFile: File is not a zip file`.
- A hex dump of the "XLSX" showed HTML content starting `<!DOCTYPE html>`.
- The HTML came from `bankofengland.co.uk`, not `ons.gov.uk`, which meant the pipeline was fetching from the wrong host.

**Root cause:** The HTTP datasets had parameters defined (`p_base_url`, `p_relative_url`) but their Base URL fields were hardcoded instead of using `@dataset().p_base_url`. For five sources this went unnoticed because the hardcoded value happened to match the watermark value. When the ONS URL changed in the watermark, the dataset carried on using its stale hardcoded URL, which pointed at BoE's host, and BoE returned a 200 OK homepage for the bad request path.

**Fix:** Replaced every hardcoded URL in the dataset Base URL fields with the right `@dataset()` expression. Re-ran the master pipeline for the affected sources. Validated the output files by parsing them in their native format.

**Lesson:** A pipeline reporting success confirms bytes moved, not that the right bytes moved. Two follow-ups came out of it:
1. Magic-byte validation in the Silver-layer ingestion contract — every file checked against its expected format header before parsing.
2. An audit of every parameterised dataset field to confirm the parameters are actually wired in, not just defined.

### ADF nested control-flow restriction

**Discovered:** When trying to nest an `If Condition` inside a `Switch` inside a `ForEach` for the yearly-stepped full-vs-incremental logic.

**Fix:** Extract the inner logic into child pipelines (`PL_Route_Yearly_Stepped`). The master Switch calls the route pipeline; the route pipeline holds the If Condition. The result is a cleaner architecture with better separation of concerns.

### URL query-string double-encoding (ONS)

**Discovered:** ONS uses relative URLs of the form `?uri=/path/to/file.xlsx`. ADF URL-encoded the `?` whenever the relative URL started with anything else, which corrupted the request.

**Fix:** Always put `?` as the first character of the relative URL. ADF preserves the query-string delimiter when it sits at position 0.

---

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory (Git-integrated) |
| Storage | Azure Data Lake Storage Gen2 (per-layer containers) |
| Compute | Azure Databricks (PySpark, Delta Lake, Photon-eligible) |
| Governance | Unity Catalog (managed tables, schema-level managed locations, External Volumes for Bronze) |
| Identity | User-assigned managed identity via Databricks Access Connector |
| Query (planned) | Azure Synapse Serverless SQL |
| Visualisation (planned) | Microsoft Fabric / Power BI |
| Source control | GitHub (trunk-based, branch-protected main) |
| Testing (planned) | pytest + chispa for PySpark transforms |
| CI/CD (planned) | GitHub Actions |

---

## Repository workflow

This repository runs on a **trunk-based workflow** with short-lived feature branches:

- One branch per logical unit of work (e.g. `phase2/setup-catalog-schemas`, `phase2/boe-silver`).
- Everything merges to `main` through a pull request, and `main` is protected against direct pushes.
- ADF Studio is Git-integrated. Pipeline JSON commits go to feature branches, then merge to `main` via PR. Publishing from ADF promotes the live factory.
- Databricks notebooks are managed through Databricks Git folders linked to this repo, committed from the workspace UI on the same feature branches.

Both ADF and Databricks work against the same Git branches as local development, so `main` always reflects the live state of every component.

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
│   │   ├── 01_create_schemas.py         # Unity Catalog schema definitions (SQL via %sql cells)
│   │   └── 02_create_bronze_volumes.py  # External Volumes per Bronze source
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
- Unity Catalog: catalog `uk_property_intel` with schemas `bronze`, `silver`, `gold`, `quality`; per-source External Volumes under `bronze`
- Dedicated-access cluster (single-user) on the latest DBR LTS

### Initial load

1. Set the `active` flags in the watermark as needed (set unwanted sources to `active: false` to skip them).
2. Trigger `PL_Master_Orchestrator` from ADF Studio or a trigger.
3. On the first run, `yearly_stepped` sources (PPD) iterate from `start_year` to the current year.

### Incremental loads

For sources with `full_load_complete: true` and a valid `last_year_ingested` or `last_refreshed` date, later runs of the master pipeline fetch only new data.

### Monthly manual updates

Three sources have fully-dynamic URLs that change with each release and need a watermark update:

- **HPI** — update `relative_url` with the new monthly filename.
- **ONS Private Rents** — update `relative_url` with the new monthly path and filename.
- **Police.uk** — update `relative_url` with the latest monthly archive.

A Databricks notebook to automate these URL updates via pattern matching is planned.

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
- [x] Unity Catalog `bronze` schema with per-source External Volumes (UC-governed access from Silver, no abfss paths in notebooks)
- [x] Doogal Bronze folder renamed `postcodes` → `doogal` to align with source-name taxonomy
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

*Project status: Phase 2 in progress.*
