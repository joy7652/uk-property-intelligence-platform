# Design Document — UK Property Market Intelligence Platform

This document captures the architectural decisions, engineering trade-offs, and delivery plan behind the platform. It complements the project README with deeper rationale and serves as the working design reference across phases.

---

## Table of contents

- [Project overview](#project-overview)
- [Data sources](#data-sources)
- [Architecture](#architecture)
- [ADF implementation (Phase 1)](#adf-implementation-phase-1)
- [Key engineering decisions](#key-engineering-decisions)
- [Bugs found and fixed](#bugs-found-and-fixed)
- [Repository and Git workflow](#repository-and-git-workflow)
- [Phase 2 scope — Databricks Silver layer](#phase-2-scope--databricks-silver-layer)
- [Design narratives](#design-narratives)

---

## Project overview

### Elevator pitch

A configurable, multi-source data engineering platform that ingests, validates, transforms, and analyses UK residential property market data across six official UK open datasets. Demonstrates a production-grade Azure data pipeline with data quality, observability, incremental loading, CI/CD, and statistical anomaly detection.

### Why these data sources

Property data is used across finance, consulting, and public sector. Monthly-updated sources frame this as a production pipeline rather than a one-off analysis. The deliberately messy real-world data surfaces non-trivial transformations, and the config-driven architecture means the same framework could ingest any multi-source analytical dataset.

### Tagline

> A config-driven, multi-source Azure data platform for UK property market intelligence, featuring incremental ingestion, schema evolution handling, automated data quality validation, and statistical anomaly detection.

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

**Price Paid Data** is the authoritative record of UK residential transactions since 1995 — 24M+ records, the backbone of any property analysis. **HPI** provides official price indices validated by the same department, useful as a cross-source oracle for sanity-checking derived metrics. **Postcode lookups** enable geocoding and regional aggregation. **Bank of England rates** and **ONS rents** provide the macro context — affordability analysis needs both the price and the cost of money. **Police crime data** adds a classic property-investment overlay (safety × price growth) that joins cleanly on postcode.

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
- Requires full browser-mimicking headers (User-Agent + Accept + Accept-Language) — Akamai CDN filters aggressively
- URL changes monthly but follows a predictable pattern

**3. Doogal UK Postcode Lookup**
- ZIP file, static URL: `https://www.doogal.co.uk/files/postcodes.zip`
- ~90 MB zipped, ~950 MB unzipped
- Mirrors ONS postcode data under Open Government Licence — chosen as a practical alternative when ArcGIS Hub access proved unworkable
- Updated quarterly

**4. Bank of England Base Rate**
- Single XLS file, stable URL
- URL: `https://www.bankofengland.co.uk/-/media/boe/files/monetary-policy/baserate.xls`
- Requires User-Agent header (anti-bot filtering)
- Full rate history since 1694

**5. ONS Price Index of Private Rents**
- Monthly XLSX, URL changes with every release
- Base URL: `https://www.ons.gov.uk/file`
- Relative URL pattern uses `?uri=...` query string
- **Platform quirk:** relative URL must begin with `?` to prevent URL-encoding the query delimiter

**6. UK Police Street-level Crime**
- Rolling 3-year snapshot ZIPs, ~1.7 GB each, ~4500 CSVs inside (36 months × ~125 files per force/category)
- Historical snapshot URL pattern: `https://data.police.uk/data/archive/{YYYY}-12.zip`
- Latest monthly snapshot URL pattern: `https://data.police.uk/data/archive/{YYYY-MM}.zip`
- No auth
- Full-load iterates 2-yearly (2013-12, 2015-12, …, 2025-12); incremental fetches latest published monthly snapshot
- Silver-layer strategy: deduplicate overlapping months, retaining the most recent snapshot's version of each `(month, force, category)` tuple. Disagreements between overlapping snapshots become a quality signal.

### Source swap: EPC → Police.uk

The original sixth source was planned as MHCLG's Energy Performance Certificates (EPC). During implementation the existing `epc.opendatacommunities.org` service was identified as retiring on 30 May 2026, with its replacement (`Get energy performance of buildings data`) requiring GOV.UK One Login — an OAuth2 flow incompatible with ADF's native HTTP Basic authentication. The production launch date of the replacement service was not confirmed at the time of discovery.

Three options were evaluated:

1. Build against the legacy service, accepting that the pipeline would break within weeks
2. Wait for the replacement service's production launch
3. Swap to a comparable stable source

Option 3 was chosen. Police.uk crime data satisfies the structural requirements of the project (stepped yearly pattern, meaningful analytical value) while strengthening the analytical narrative — crime × price overlays are a well-established dimension in property investment analysis.

A smoke test conducted before implementation revealed that each police.uk monthly archive is a rolling 3-year snapshot rather than a monthly delta. This observation changed the ingestion pattern from the originally-planned monthly-backfill to `yearly_stepped` with a 2-year step, avoiding duplicate data while preserving full historical coverage.

---

## Architecture

```
watermark.json (ADLS Gen2 config store)
              ↓
Azure Data Factory (config-driven orchestration, Git-integrated)
              ↓
              ├── Master orchestrator reads watermark, filters active sources
              ├── Routes per source by load_pattern via Switch activity
              └── Route pipelines handle full-vs-incremental branching
              ↓
ADLS Gen2 Bronze (raw files, per-source folder structure)
              ↓
Azure Databricks (Phase 2 — quality checks, transforms, anomaly detection)
              ↓
ADLS Gen2 Silver → Gold (Delta Lake, medallion architecture)
              ↓
Azure Synapse Serverless SQL (Phase 3)
              ↓
Microsoft Fabric / Power BI (Phase 5 — Property Dashboard, Pipeline Health Dashboard)
```

### Design principles

- **Config over code.** Adding a source means appending a JSON block to the watermark, not writing a new pipeline.
- **Patterns over sources.** A small closed set of ingestion patterns (`yearly_stepped`, `single_file`) covers every source; each source declares which pattern it uses.
- **Trust nothing silently.** Pipeline success status confirms bytes moved, not that the right bytes moved. Binary files are validated against expected magic bytes before downstream processing.
- **Parameterised linked services.** HTTP linked services are host-agnostic; base URL is passed at runtime via `@{linkedService().p_base_url}` so a single linked service can serve multiple hosts with the same authentication shape.

---

## ADF implementation (Phase 1)

### Storage
- Azure Data Lake Storage Gen2, container `raw` (used as Bronze)
- RBAC: ADF managed identity granted `Storage Blob Data Contributor`
- Watermark at `config/watermark/watermark.json`

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
- `DS_ADLS_Bronze_CSV` — parameterised with `p_folder_path`, `p_file_name`. Operates in binary passthrough mode for all file types (CSV, XLSX, XLS, ZIP)
- `DS_ADLS_Watermark_JSON` — for reading the watermark file

### Pipelines

**`PL_Master_Orchestrator`**
- Lookup reads watermark.json as an array
- Filter keeps only active sources: `@or(equals(item().active, null), equals(item().active, true))`
- ForEach iterates filtered items sequentially
- Switch on `load_pattern` dispatches to the appropriate route pipeline

**`PL_Route_YearStepped`**
- If Condition on `full_load_complete`:
  - False → Execute `PL_FullLoad_YearStepped`
  - True → Execute `PL_Incremental_Route`

**`PL_FullLoad_YearStepped`**
- Generic yearly-stepped full-load pipeline
- Iterates an index range `[0, N)` where `N = (end_year - start_year) / step_years + 1`
- Each iteration computes its year as `start_year + (index * step_years)`
- Handles optional `snapshot_month` suffix in URL and filename via `if(empty(...))` expressions
- Works for both PPD (step=1, no month suffix) and Police.uk (step=2, month suffix `-12`)

**`PL_FullLoad_SingleFile`**
- Switch on `linked_service_type` with a case per authentication pattern
- Each case contains a Copy activity using the matching dataset

**`PL_Incremental_Route`**
- If Condition at the top implements month-rate-limiting (skips if this source was already refreshed this calendar month)
- True branch: Switch on `incremental_type`:
  - `static_url` → Execute `PL_Incremental_StaticURL` (PPD-style cumulative update file)
  - `templated_latest` → Execute `PL_Incremental_TemplatedLatest` (Police.uk-style monthly-rotating URL)
- False branch: logs skip reason

**`PL_Incremental_StaticURL`**
- Single Copy activity, handles PPD's `/pp-monthly-update.txt`

**`PL_Incremental_TemplatedLatest`**
- Switch on `linked_service_type` with a case per authentication pattern
- Each case contains a Copy activity that constructs URL and filename from `incremental_relative_url_prefix + incremental_latest_snapshot + file_extension`

### Control-flow cascade rationale

ADF forbids nested control-flow activities (If inside Switch inside ForEach). The three-level pipeline cascade (Master → Route → Incremental Route → Leaf) exists specifically to unwrap one control-flow activity per layer. This constraint drove a clearer separation of concerns: orchestration → routing → execution.

### Watermark schema

A single JSON array in ADLS containing one object per source. Each object declares which load pattern applies, its linked-service type, its URL fragments, and state tracking fields. Example fields:

- Common: `source_name`, `display_name`, `load_pattern`, `linked_service_type`, `base_url`, `active`
- `yearly_stepped` specific: `relative_url_prefix`, `file_extension`, `start_year`, `step_years`, `snapshot_month`, `last_year_ingested`, `full_load_complete`, `parallelism`
- `single_file` specific: `relative_url`, `file_name`, `last_refreshed`
- Incremental: `incremental_type` (`static_url` or `templated_latest`), plus type-specific URL fields

---

## Key engineering decisions

### 1. Load patterns are a closed set

Two patterns cover every ingestion shape encountered:

- **`yearly_stepped`** — iterate across years with a configurable step, one file per step. Incremental dispatches to one of two children via `incremental_type`: `static_url` or `templated_latest`.
- **`single_file`** — one URL fetches one file per refresh.

The `yearly_stepped` pattern was consolidated from an earlier `yearly_range` once Police.uk's 2-year snapshot cadence proved the step parameter's value. The consolidation was deliberately deferred until a concrete second use case demonstrated the abstraction would pay off — discipline against speculative generalisation upfront.

### 2. Linked services organised by authentication pattern

HTTP linked services are named and organised by their authentication shape, not by the data source they first served. This taxonomy is the correct one — auth is a cross-cutting concern that multiple unrelated sources can share, while the data source itself is incidental to the connection. An earlier iteration named one linked service after its first data source (`LS_HTTP_LandRegistry`), which obscured the fact that Police.uk could reuse it. Renaming to `LS_HTTP_Anonymous` clarified the taxonomy and eliminated the need for a duplicate linked service.

### 3. Watermark as a JSON array in ADLS

The watermark is stored as a JSON array (not object) because ADF's Lookup + ForEach iterates arrays cleanly. It lives in ADLS rather than Azure SQL — eliminating an entire database dependency. When Databricks joins the stack in Phase 2, the watermark migrates to a Delta table.

### 4. Three-level route cascade

Master → Route → Incremental Route → Leaf. Each layer unwraps exactly one control-flow activity. Necessary due to ADF's nesting restriction; resulted in cleaner separation of concerns than a single monolithic pipeline would have provided.

### 5. Per-source parallelism as configuration

Parallelism is recorded in the watermark per source (PPD=4, Police.uk=2). Larger files get lower parallelism to avoid saturating integration runtime bandwidth. Note: ADF's ForEach batch count cannot be parameterised at runtime (platform constraint), so the watermark field serves as documentation; the actual batch count is set on the ForEach activity directly.

### 6. Medallion layering

- **Bronze** — raw files as received, no transformation, one folder per source
- **Silver** — validated, typed, quality-checked, schema-enforced, Delta format
- **Gold** — joined, enriched, denormalised for analytical consumption

Bronze is complete. Silver and Gold are Phase 2 and beyond.

---

## Bugs found and fixed

### Base URL hardcoded despite parameter being defined

**Discovered:** During ONS source onboarding, after modifying the ONS URL in the watermark for a new monthly release.

**Symptoms:**
- Pipeline reported "Succeeded" on every run
- Downstream `openpyxl.load_workbook()` failed with `BadZipFile: File is not a zip file`
- Hex dump showed the "XLSX" file was HTML beginning `<!DOCTYPE html>`
- The HTML content originated from `bankofengland.co.uk`, not `ons.gov.uk` — the pipeline was fetching from the wrong host entirely

**Root cause:** HTTP datasets had parameters defined (`p_base_url`, `p_relative_url`) but their Base URL fields were hardcoded rather than using `@dataset().p_base_url`. For five sources this was undetectable because the hardcoded value happened to match the watermark value. When ONS's URL was updated in the watermark for the March release, the dataset continued using its stale hardcoded URL. That URL coincidentally pointed to BoE's host, which returned 200 OK with a homepage for the bad request path — so bytes transferred and ADF reported success.

**Fix:** Replaced every hardcoded URL in dataset Base URL fields with the appropriate `@dataset()` expression. Re-ran master pipeline for affected sources. Validated output files locally by parsing them in their native format.

**Lesson:** Pipeline success status confirms bytes moved, not that the right bytes moved. Two follow-ups resulted:
1. Magic-byte validation planned for the Silver-layer ingestion contract — every file checked against expected format header before parsing
2. Audit completed of every parameterised dataset field to confirm parameters are actually wired, not just defined

### Query string double-encoding

**Discovered:** ONS URL returned 400 errors when `?uri=...` appeared mid-string.

**Root cause:** ADF URL-encoded the `?` character when it wasn't at position 0 of the relative URL.

**Fix:** Always prefix the relative URL with `?` as its first character. ADF preserves the query-string delimiter when it appears at position 0.

### ADF nested control-flow restriction

**Discovered:** When designing the full-vs-incremental branching logic, and later when adding the incremental-type dispatch.

**Constraint:** ADF forbids nested control-flow activities — If, Switch, ForEach, and Until cannot contain each other.

**Resolution:** Extract inner logic into child pipelines. `PL_Route_YearStepped` and `PL_Incremental_Route` exist specifically to unwrap one control-flow activity per layer. Resulted in cleaner separation of concerns with each pipeline doing one job.

---

## Repository and Git workflow

Dual-source commits:

- **Local commits** cover README, configuration templates, documentation, and (eventually) Databricks notebooks. Standard GitHub flow via feature branches.
- **ADF Studio commits** cover pipeline, dataset, linked service, and trigger JSON. Configured via ADF's GitHub integration; every Save in ADF commits to the `adf-dev` branch. Publish promotes changes to `adf_publish` and the live factory simultaneously.

The `main` branch is periodically synced from `adf-dev` so the repository accurately reflects current pipeline definitions alongside documentation.

### Branch structure
- `main` — stable, published docs and latest merged pipeline definitions
- `adf-dev` — ADF collaboration branch
- `adf_publish` — ADF auto-generated, tracks live factory state

---

## Phase 2 scope — Databricks Silver layer

### Primary objectives

1. **Databricks workspace and cluster provisioning**
   - Workspace in the same resource group as ADF and ADLS
   - Cluster sizing and access mode to be confirmed
   - ADLS connection via service principal or managed identity
   - Unity Catalog adoption decision

2. **Silver-layer ingestion notebooks — one per source**
   - Bronze → Silver transformations (clean, type, dedupe, Delta-format)
   - Data quality checks applied at ingestion time
   - Magic-byte validation as a direct follow-through from the Phase 1 bug
   - Schema enforcement with Delta Lake's `mergeSchema`

3. **Parameterised quality-rules framework**
   - `quality_rules.json` under `config/` defining per-source validation rules
   - Generic validation notebook reads rules and applies them dynamically
   - Rejected records routed to a quarantine Delta table
   - Quality scores logged to a `pipeline_audit` Delta table on every run

4. **Police.uk overlapping-snapshot deduplication**
   - Primary: `row_number() over (partition by month, force, category order by snapshot_date desc) = 1`
   - Cross-snapshot consistency check as a bonus quality signal — data that disagrees between overlapping snapshots for the same month is flagged for investigation

5. **Automated watermark updates from Databricks**
   - Replace manual JSON editing
   - Notebook programmatically updates watermark on successful runs
   - Migration of watermark from JSON-in-ADLS to a Delta table

6. **Initial Silver → Gold design**
   - Multi-source joins on postcode
   - Enrichment: price × rent yield, price × rate affordability, price × crime index
   - Denormalised analytical tables

### Planned order of delivery

1. Databricks workspace + cluster + ADLS connection
2. First Silver notebook against the simplest source (BoE) — minimal schema complexity
3. HPI (single CSV, well-structured)
4. PPD (large, multi-year, schema-evolution considerations)
5. Doogal (ZIP unzip, large postcode table)
6. ONS (XLSX with headers and footers to skip)
7. Police.uk (most complex — ZIP containing ~4500 nested CSVs, multi-snapshot deduplication)
8. Quality-rules framework extracted from patterns observed during (2)–(7)
9. Watermark automation
10. Gold-layer joins

---

## Design narratives

These sections articulate the rationale behind specific architectural choices.

### On reacting to upstream instability

The planned sixth source (EPC) was deprecated during implementation. Rather than fight the migration or wait for an unknown production launch, the source was swapped for one that preserved architectural goals while improving analytical value. The decision was made by evaluating three paths explicitly (push through, wait, swap) on stability and narrative value. A smoke test conducted before implementation then revealed that the expected data shape of the replacement source was wrong — what looked like a monthly delta was actually a rolling 3-year snapshot. That observation alone saved an entire misaligned pattern from being built. The sequence illustrates a broader principle: discovery before construction, and risk assessment before commitment.

### On recognising latent defects

The base-URL hardcoding bug was invisible for five sources because the hardcoded values happened to match the watermark. It surfaced only when the ONS URL was updated for a new monthly release. The symptom — HTML content where XLSX was expected — was detected through local validation, not trusted from pipeline status. This reinforced a broader operating principle: success status from an orchestrator confirms that bytes moved, not that the right bytes moved. Magic-byte validation in the Silver-layer ingestion contract is the designed response to this class of silent failure.

### On abstraction timing

Two distinct load patterns existed before Police.uk was added: the existing `yearly_range` for PPD and a planned `monthly_backfill` for future use. The temptation to consolidate them upfront was real but deliberately resisted. Only once Police.uk arrived with a concrete 2-year-step requirement did the abstraction earn its cost — at which point `yearly_stepped` absorbed both cases cleanly. This is the discipline of building one thing, building a second, and consolidating when commonality is evidenced rather than imagined.

### On authentication as a taxonomy

Linked services were initially named after the first data source each served. This coupled source identity to connection identity and obscured reuse opportunities. The rename to `LS_HTTP_Anonymous` (and the recognition that the same pattern applied across other linked services) reframed linked services as expressions of authentication shape — a cross-cutting concern that multiple sources can share. The taxonomy change made Police.uk adoption require zero new connection infrastructure.

---

*Design document status: reflects state at end of Phase 1 — Bronze ingestion complete for all six sources. Phase 2 (Databricks Silver layer) in development.*
