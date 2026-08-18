# Design Document — UK Property Market Intelligence Platform

This document captures the architectural decisions, engineering trade-offs, and delivery plan behind the platform. It complements the project README with deeper rationale and serves as the working design reference across phases.

> **Status:** Phase 1 complete, Bronze ingestion for all six sources. Phase 2 complete: the Databricks workspace, Unity Catalog, and medallion storage layer are provisioned, and all six Silver tables are live, unit-tested, and committed. The Bank of England base rate, the UK House Price Index, Land Registry Price Paid Data, the UK postcode lookup, ONS private rents, and Police.uk street-level crime. Phase 3 is in progress: the Gold star schema is designed, its thirteen tables are created, all four dimensions are loaded, and the first two facts are loaded and verified on the cluster. Seven facts remain.

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
- [Phase 3 scope: Databricks Gold layer](#planned-gold-scope)

---

## Project overview

### Elevator pitch

An Azure data platform over six official UK property datasets, built around Land Registry's 31.4M residential transactions and joined with the house price index, private rents, postcodes, the base rate and street-level crime. One watermark file defines every source and two load patterns cover all six. The build covers incremental loading, data quality and observability, with CI/CD and statistical anomaly detection in later phases.

### Why these data sources

Property data turns up across finance, consulting, and the public sector, and it's a domain most people already understand. The sources update monthly, which makes this a running pipeline. The data is genuinely messy, so the transformations are non-trivial, and the config-driven design means the same framework could ingest any other multi-source analytical dataset without code changes.

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
- The download page published a different host until February 2026, an S3 website endpoint at `prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com`. Both names resolve to the same object, confirmed by identical `Last-Modified` and `Content-Length` on each, so the older host is an alias for the same object. The watermark uses the documented host.
- Incremental: `pp-monthly-update.txt`, a static URL under the same host. It carries only additions, changes, and deletions, and each release overwrites it, so a missed release cannot be recovered from source. ADF lands it under a `.csv` name, so the Bronze extension does not match the source. Land Registry publishes a second copy at `pp-monthly-update-new-version.csv` at a slightly different size, which this pipeline does not fetch.
- Headerless. 16 columns in the published order, the last being Record Status, which is populated only in the monthly file. Yearly files carry `A` throughout.
- 103 MB to 228 MB per complete yearly file. 31.4M records total as of the July 2026 release. The count rises with every release as registrations complete, so it needs a release attached wherever it is quoted.
- Every yearly file contains exactly one transfer year, confirmed across all 32 files on two separate vintages. TUID is unique across the whole dataset.
- The four code columns carry small closed sets: property type (D, S, T, F, O), old or new build (Y, N), duration (F, L, U), and category (A, B). Every published value appears in the data, including duration U, which is genuine but rare at 532 rows across 31.4M. Silver aborts on an unrecognised code.
- Category A (standard) runs from January 1995. Category B (additional: company sales, identifiable buy-to-lets, repossessions) has been captured since 14 October 2013, but the yearly files key on transfer date, so a thin tail of earlier transfer dates survives from transactions registered after capture began: 1,857 rows before 2013 against 1,811,011 category B rows in total. The tail thickens toward 2013, from 11 rows in 1995 to 432 in 2012, because the shorter the gap the more likely a transfer was still unregistered when capture started. In the 2019 file the split is 843,004 category A against 169,156 category B.
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
- **Numeric formatting is not stable between releases.** A 2020 vintage published `AveragePrice` to five decimal places; current vintages publish whole pounds. Types are asserted at Silver for exactly this reason.
- The file carries a derived back-series to 1968, built from the historic path of the older ONS index, ahead of native coverage (England and Wales 1995, Scotland 2004, Northern Ireland 2005). See Decision 14.
- The two most recent months are provisional. Sales volumes arrive null and fill in as registrations complete; price estimates are revised.
- **Revision window: 13 months**, extended from 12 following a review of the revision policy. A full overwrite from the newest release is therefore the correct write mode, and an append would retain superseded values alongside corrected ones.

*Publisher issues affecting specific columns:*

- First-time buyer and former owner occupier prices were calculated incorrectly prior to January 2026. Land Registry advises caution comparing those breakdowns either side of the January 2026 estimates. The series therefore carries a discontinuity, not a gap, which no null check will surface; a Gold model must not treat it as continuous.
- New build and existing resold average prices and percentage changes are no longer published as they were, because there are not currently enough new build transactions for a reliable result. Those columns arrive null in recent months while the corresponding sales volumes stay populated.
- Northern Ireland sales volumes are now published as a monthly estimate, the quarterly total divided by three. The previous approach fed quarterly figures into monthly UK totals and inflated them.

*Documented anomaly, retained as a Phase 4 test case:*

United Kingdom sales volume for March 2025 reads 134,340 against a baseline near 60,000, with April 2025 falling to 44,018. The cause is the SDLT threshold reversion on 1 April 2025: the nil-rate band returned from £250,000 to £125,000 and the first-time buyer threshold from £425,000 to £300,000, pulling completions forward into March. HMRC recorded 77,480 more residential transactions above £40,000 in March 2025 than a year earlier, and RICS had forecast the shape in advance. The spike grew between the October 2025 and May 2026 vintages as late registrations landed, which rules out a methodology artifact. A rolling anomaly detector that misses a 2x spike and an adjacent 0.7x trough with a published external cause is not working, so this is a better validation case than synthetic noise.

**3. Doogal UK Postcode Lookup**
- ZIP file, static URL: `https://www.doogal.co.uk/files/postcodes.zip`
- **239 MB zipped, 2.01 GB unzipped, one CSV of 60 columns and 2,713,360 rows** (measured 03-08-2026 against the May 2026 release). The earlier figures in this document, ~90 MB and ~950 MB, were recorded in Phase 1 and the file has more than doubled since.
- Mirrors ONS postcode data under the Open Government Licence, with the publisher's own columns appended. I chose it as a practical alternative when ArcGIS Hub access proved unworkable
- Refreshed each quarter when ONS release the postcode directory. The publisher keeps no history, so only latest state is available and the correct write mode is a full overwrite
- The URL and the filename never change, so neither the run status nor the landed path can distinguish a current release from a stale one. `Last updated` inside the file is the only freshness signal the content carries
- Grain is one row per postcode. Postcode is unique across all 2,713,360 rows, so Silver asserts uniqueness

- 1,798,006 live postcodes and 915,354 terminated. Terminated rows are retained: PPD transfers run to 1995 and 14,395 distinct PPD postcodes resolve only through them
- Header carries a UTF-8 byte order mark. The Spark reader strips it, and the column-set guard is what would surface a reader change that let it through
- The publisher's field documentation lists 61 columns including a "Built up sub-division" the file does not carry, and differs from the file in capitalisation on three more. The header is the contract; the documentation is reference
- No auth, no headers required

*Positional quality and the fabricated coordinate.* The `Quality` column is the ONS positional quality indicator, 1 to 9. Value 9 means no grid reference is available, and the file leaves easting and northing blank on those 11,071 rows while writing zero into latitude and longitude. Zero is a valid coordinate in the Gulf of Guinea, so it survives every range check a consumer might apply; the blank grid columns are the honest signal. Silver nulls the coordinate pair on quality 9 and asserts the two conditions coincide. Value 7 is published as "deliberately left blank" and is absent from the current release, which is a fact about the release, not about the contract. Those rows lose their statistical geography too: without a grid reference the source assigns no LSOA, so quality 9 postcodes join PPD on postcode but cannot join Police.uk on LSOA.

*Non-geographic postcodes.* The BF area is British Forces Post Office, introduced in 2012 to give BFPO addresses UK-style postcodes. Its 48 rows carry coordinates at overseas bases (Sennelager, Ramstein, Lisbon, Naples, Stavanger) and null in every administrative and statistical column, including the introduction date. That block was last updated 2018-10-14 against 2026-05-29 for the rest of the file, and gov.uk has since removed at least one entry that is still present here, so it is a frozen appendix and not a maintained series.

*Columns that look usable and are not.* Population and households are 2011 census figures. Average income is a 2020 model-based estimate at MSOA level. Both sit in a quarterly-refreshed row with nothing marking them as fixed-vintage. The deprivation rank holds four separate national indices at four vintages and four scales under one name, England ranking to 33,755, Wales to 1,909, Scotland to 6,976 and Northern Ireland to 890, with a 0 appearing in the data that falls in none of those ranges. See Decision 18.

*Coverage by nation, from the 03-08-2026 load:* England 2,277,064, Scotland 230,924, Wales 142,084, Northern Ireland 63,240, BFPO 48. Postcodes with no grid reference are unevenly distributed: 4.56% in Northern Ireland against 0.31% in England, a fifteenfold gap that any geography-dependent Gold model inherits.

**4. Bank of England Base Rate**
- Single XLS file, stable URL
- URL: `https://www.bankofengland.co.uk/-/media/boe/files/monetary-policy/baserate.xls`
- Requires User-Agent header (anti-bot filtering)
- Full rate history since 1694
- The published `baserate.xls` is revised periodically, and the daily `Raw Data` series is pre-filled to roughly a month past the save date (a revision saved 2026-03-31 carried daily rows through 2026-04-30). Between revisions, ADF refreshes land byte-identical copies, so an unchanged row count after re-ingestion is expected.
- Freshness therefore comes from the file's size, not from its `modificationTime`. An earlier note here recommended the timestamp; the PPD finding below shows why that was wrong, since a container restructure rewrote every timestamp without re-fetching anything. Size moves whenever the workbook is re-saved, whether or not a rate changed. The Silver notebook records it as `source_bytes`.
- The pre-filled rows carry the rate forward, so the last row of the sheet and the newest day carrying a rate are the same date. Measured equal on two runs. Because the series runs ahead of the save date, the freshness lag the notebook reports understates the workbook's true age by about a month.
- An unchanged **event** count is likewise expected between rate changes. The table has held at 278 change events since the December 2025 cut to 3.75%, through unchanged decisions in March, April, and June 2026.

**5. ONS Price Index of Private Rents**
- Monthly XLSX, URL changes with every release
- Base URL: `https://www.ons.gov.uk/file`
- Relative URL pattern uses `?uri=...` query string
- **Platform quirk:** the relative URL must begin with `?` to stop the query delimiter being URL-encoded

Monthly rent indices and price levels for the UK, its countries, English regions, and local geographies, from January 2015. Base period January 2023 = 100. Not seasonally adjusted. Became official statistics on 20 May 2026, having previously been official statistics in development; the workbook's own cover sheet still carries the older wording, so the status is taken from the bulletin.

*Shape.* One sheet of data, Table 1, in an accessibility-formatted workbook alongside a cover sheet, contents, and notes. Header on row 3, 40 columns: four label columns, then a headline block of index, monthly change, annual change and rental price, then the same four metrics for eight breakdowns, being one, two, three and four-or-more bedrooms, and detached, semi-detached, terraced and flat or maisonette. The July 2026 release carries 49,266 data rows: 357 geographies with all 138 months each, a rectangular panel with no ragged starts.

*Geography is not uniform.* England and Wales report by local authority district, Scotland and Northern Ireland by broad rental market area. The 316 English and Welsh authorities, the nine English regions and the country and UK aggregates all carry GSS codes that match HPI. The 18 Scottish rental market areas carry S33 codes that match nothing else in the project, and the eight Northern Irish areas carry no code at all. City of London and Isles of Scilly are not published, because collection volumes are too low, which is a different pair from the HPI gap.

*Absence is marked, not blank.* `[x]` is data that cannot exist and `[z]` is data that does not apply. In the July 2026 release there are 42,417 instances of `[x]` and every one is structural: annual change across the first twelve months, monthly change in the first month, and all 36 measures for the nine Northern Irish geographies in the two months ONS has not yet published. `[z]` appears only in the area code column, for the eight uncoded areas, and in the parent geography column, for rows that have no parent.

*Northern Ireland lags by two months,* and ONS imputes it forward to publish a UK figure, so the latest two UK rows are part measured and part estimated. Those same two months are revised on the following release under a stated two-month revision policy. Great Britain excludes Northern Ireland and is unaffected.

*The URL cannot be templated.* The dataset path carries a release-date folder, which follows the publication calendar, and a filename with an arbitrary suffix that does not. Across 29 editions the suffixes run 1, 2, 6, 8, 10, 13, 14 with no relationship to the edition sequence, plus one file with a doubled dot and two early ones under a different stem. There is no rule that derives next month's filename from this month's, so the URL has to be read off the dataset page.

**6. UK Police Street-level Crime**
- Snapshot ZIP archives, 1.4 to 1.7 GB each, roughly 4,500 CSVs inside, one per month, force, and dataset
- Historical snapshot URL pattern: `https://data.police.uk/data/archive/{YYYY}-12.zip`
- Latest monthly snapshot URL pattern: `https://data.police.uk/data/archive/{YYYY-MM}.zip`
- No auth
- Each archive holds three datasets: street-level crime, outcomes, and stop and search. Silver takes street only, because the crime-against-price overlay is the analytical purpose and the other two serve it in no way that justifies three times the volume.
- **The archive window is not constant.** Archives from December 2013 to April 2017 carry the full history back to December 2010. From May 2017 they carry a rolling 36 months. The full load runs the December archives at a two-year step, which against a three-year window leaves twelve months of overlap between consecutive archives and no gaps.
- The watermark starts at 2015. The 2013-12 archive was ingested, inventoried, and then deleted: 2015-12 covers every month it held from a later snapshot, and the only slots it uniquely carried were nine header-only files of 130 bytes and zero rows.
- police.uk publish an MD5 beside every archive. All eight in Bronze matched the published digests on 05-08-2026, which makes this the only source in the project where Bronze fidelity is proved instead of preserved by convention.
- Monthly retention is latest-only. Each monthly archive is a full 36-month snapshot, so a missed month is recovered by the next one. That inverts PPD, whose monthly file is a change-only delta at a static URL that cannot be recovered if missed. See Decision 16.
- Bronze at 06-08-2026 holds seven archives spanning 2010-12 to 2026-06, 187 months, no gaps. After path-level deduplication that is 8,288 winning `(month, force)` files and 19.2 GB unzipped, from 3.6 GB of compressed input.
- Crime ID is a one-way hash of the force's offence reference and is blank for anti-social behaviour. Dates are truncated to year and month at anonymisation and coordinates are snapped to shared map points, both deliberately. See Decision 23.

*Force coverage is not constant, and the gaps are large.* Greater Manchester stop after June 2019, with one stray file in August 2022 and nothing since. British Transport Police stop after January 2025, which matters beyond the row count because they were the only force publishing Scottish locations. Gloucestershire's series ends January 2026, short enough to read as filing lag. Together that is roughly 2.8M records against full coverage, near 3%, and almost all of it Manchester. Missing force-months are silent by construction: the load reads whichever archive holds each slot, so a force that backfills reappears on the next run without intervention.

*Anti-social behaviour is 53.8% of records across 2010 to 2014 and 16.4% across 2023 to 2026.* Any total-crime series that does not separate it shows a fall and then a recovery, both of which are artefacts of composition, not movements in crime.

*Crime type vocabulary runs in three eras:* six categories to August 2011, eleven to April 2013, then fourteen from May 2013, when the Home Office split public disorder and weapons into two categories and renamed violent crime. The boundary is clean, with no month carrying both vocabularies.

*Northern Ireland reuses offence references.* 8,654 crime IDs recur monthly across the whole series, covering 1,405,213 rows, so Crime ID identifies nothing for 1.5% of the table. No model should join on it.

*LSOA follows the ONS boundary vintage of the day.* From 2024 every code resolves against the postcode lookup's 2021 codes; before that roughly 1,100 do not, and 2023 carries the most distinct codes of any year because it spans both vintages.

*Force is a reporter, not a geography.* Only 7,353 of about 36,000 LSOAs have a single reporting force, yet every force except City of London has under 1.2% of its rows in an LSOA another force dominates. City of London is 5.9%, which is what an enclave inside the Metropolitan area produces. The two readings agree: the dominant force holds nearly all the volume in each LSOA, and the rest is a thin spray of boundary and distant snap points. Aggregate by LSOA across forces, and never split an LSOA by force.

### Source swap: EPC → Police.uk

The sixth source was originally MHCLG's Energy Performance Certificates (EPC). Partway through the build I found that the existing `epc.opendatacommunities.org` service was retiring on 30 May 2026, and its replacement (`Get energy performance of buildings data`) required GOV.UK One Login, an OAuth2 flow that ADF's native HTTP Basic authentication can't handle. The replacement service had no confirmed production launch date at the time.

I evaluated three options:

1. Build against the legacy service, accepting that the pipeline would break within weeks.
2. Wait for the replacement service to reach production.
3. Swap to a comparable stable source.

I chose option 3. Police.uk crime data meets the structural requirements (stepped yearly pattern, real analytical value) and arguably strengthens the analysis, since crime-against-price overlays are a well-established dimension in property investment.

A smoke test before implementation showed that each police.uk monthly archive is a rolling 3-year snapshot, not a monthly delta. That changed the ingestion pattern from the originally-planned monthly-backfill to `yearly_stepped` with a 2-year step, which avoids duplicate data while keeping full historical coverage.

That reading held for the archives the smoke test looked at and not for the older ones, which the Phase 2 inventory found: everything before May 2017 carries the full history from December 2010. The two-year step was still correct, since a three-year window leaves twelve months of overlap either way, but it means the earliest archive in the chain is far wider than the pattern name suggests.

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
- After the first full load, each source tracks its own state in the watermark, so later runs fetch only what's new. What counts as "new" is source-specific, so incremental loading routes by type. Assuming one mechanism would break the other.
- A pipeline reporting success only tells me bytes moved, not that the right bytes moved. Binary files are validated against their expected magic bytes before any downstream processing.
- HTTP linked services are host-agnostic; the base URL is passed at runtime via `@{linkedService().p_base_url}`, so one linked service can serve several hosts that share an authentication shape.
- Data-plane access from Databricks runs through a user-assigned managed identity on the Databricks Access Connector. No SAS tokens, no mounted credentials, no key rotation in the data path.
- Each medallion layer maps one-to-one to a Blob container, a Unity Catalog schema, and a schema-level managed location (or, for Bronze, a set of External Volumes), so the architecture is visible in the storage account.
- Each layer should do work the previous one didn't. Bronze stays as raw files exposed through Volumes and never copied into Delta, because at this scale that copy step adds nothing the raw files don't already give.
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

The Volume namespace is flat while the storage layout beneath it is not. Four sources sit at the container root; the two Land Registry sources nest under a publisher folder, because Land Registry publishes two distinct sources. A Volume roots at its source folder and never at a dataset folder inside it, so notebooks append any dataset segment themselves: the BoE notebook appends `base_rate/`, while HPI's files sit directly at the Volume root.

This table is confirmed against `information_schema.volumes`, not the other way round. The Volume audit established that a script, a document, and a deployment can each be wrong independently, so the storage locations are read from the catalog.

Silver notebooks reference `/Volumes/uk_property_intel/bronze/<source>/...` over `abfss://bronze@...` paths, so all Bronze access flows through Unity Catalog.

### Why per-layer containers

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
- **Photon engine:** off initially. I'll decide whether to enable it by testing against a representative Silver transformation (likely the Police.uk dedup).
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
- **External Volumes:** one per Bronze source, defined in `databricks_src/setup/02_create_bronze_volumes.py`. Exposes raw files under `uk_property_intel.bronze.<source>` for UC governance and lineage. Silver notebooks read from `/Volumes/uk_property_intel/bronze/<source>/`.
- **Default workspace catalog:** dropped after I verified the dedicated catalog was operational, which keeps Catalog Explorer free of placeholder entries.

### Bronze layer state

Phase 1's ADF pipelines write into the `bronze` container directly (no `bronze/` subfolder). The container was renamed from `raw` and the redundant subfolder removed during Phase 2 setup, with ADF pipelines updated and a full end-to-end re-run completed before proceeding. Bronze is registered as a Unity Catalog External Location and surfaced through per-source External Volumes; Silver pipelines read via Volume paths only.

---

## Key engineering decisions

### 1. Load patterns are a closed set

Two patterns cover every ingestion shape I encountered:

- **`yearly_stepped`** — iterate across years with a configurable step, one file per step. Incremental work dispatches to one of two children via `incremental_type`: `static_url` or `templated_latest`.
- **`single_file`** — one URL fetches one file per refresh.

`yearly_stepped` grew out of an earlier `yearly_range` pattern. I added the step parameter only once Police.uk's 2-year snapshot cadence gave me a concrete second use for it.

### 2. Linked services organised by authentication pattern

I name and organise HTTP linked services by their authentication shape, not by the data source they first served. Authentication is a cross-cutting concern that several unrelated sources can share, whereas the data source itself is incidental to the connection. An earlier version named one linked service after its first source (`LS_HTTP_LandRegistry`), which hid the fact that Police.uk could reuse it. Renaming it to `LS_HTTP_Anonymous` made the grouping obvious and removed the need for a duplicate.

### 3. Watermark as a JSON array in ADLS

The watermark is stored as a JSON array (not an object) because ADF's Lookup plus ForEach iterates arrays cleanly. It lives in ADLS over Azure SQL, which removes an entire database dependency. When Databricks joins the stack in Phase 2, the watermark moves to a Delta table.

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

I adopted Unity Catalog from day one of Phase 2, over the legacy hive_metastore that older Databricks projects (and most tutorials) still use. Reasoning:

- Databricks has confirmed that from 30 September 2026, all new workspaces (Azure included) are provisioned Unity Catalog-only, with no Hive metastore, so building on UC now is the forward-compatible path.
- UC provides centralised access control, lineage, and discovery, which I'd otherwise have to build myself or skip.
- All data access flows through the UAMI on the Databricks Access Connector; no secrets, mounts, or SAS tokens.
- The DP-700 certification covers UC heavily, so building on it doubles as exam preparation.

The same logic applied to using the Access Connector's managed identity over a service principal with secrets: the older pattern still works, but the managed-identity path is the documented direction and avoids a future migration.

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

Bronze-as-Volumes pays for itself four times over:

- **Governance.** Bronze access is subject to Unity Catalog ACLs, not just the external location's identity grants.
- **Lineage.** UC automatically traces which Silver tables read from which Bronze paths.
- **Discoverability.** Bronze appears in Catalog Explorer as a first-class object alongside Silver and Gold, and not an `abfss://` URI known only to me.
- **Path stability.** Silver notebooks reference `/Volumes/uk_property_intel/bronze/<source>/`; if the storage layout ever changes, only the Volume definition updates.

This decision would change if I were ingesting very large source files at high frequency, if the source data were ephemeral and needed durable Delta preservation, or if I needed extensive ad-hoc SQL on the raw data before Silver. None of those apply at the moment.

### 12. BoE base rate as an event-grain SCD2

The Bank of England base rate is the first Silver table, modelled as a Type 2 slowly-changing dimension at event grain, over a daily-grain series.

- **Grain.** One row per rate level, with a validity interval (`effective_date`, `expiry_date`, `is_current`), the rate and its regime label (`rate_pct`, `rate_type`), and lineage columns (`_source_file`, `_ingestion_ts`). `expiry_date` is a deterministic derivation (the day before the next change; null marks the open interval), so it adds information without reshaping the data for a particular consumer, which keeps it Silver-appropriate. Daily-grain join surfaces, if a consumer needs them, are Gold's call.
- **Source reality.** `baserate.xls` is a multi-sheet FAME database export. The machine-readable sheet is `Raw Data`, a daily series from 1973-01-01 to present with real datetime cells and the header on row 2. The report sheets (`BOEBASERATE`, `HISTORICAL SINCE 1694`) are human-formatted and skipped.
- **Regime coalescing.** The BoE has renamed the policy rate across five era-specific columns (Bank Rate, Minimum Lending Rate, Minimum Band 1 Dealing Rate, Repo Rate, Official Bank Rate). These coalesce into a single `rate_pct` and a `rate_type`, newest regime first. Both columns are populated only on the two regime-changeover days (1981-08-24 and 1997-05-05), where the two values agree.
- **Collapse on rate value only.** The daily series collapses to change events; a regime relabel that does not move the rate is not an SCD2 event, and `rate_type` records the regime in effect at `effective_date`.
- **1973 cutoff.** Pre-1973 history lives only in the fragile report sheet, so the cut is on data quality. The 1973 to 1995 rates join nothing in this project yet, since PPD and HPI both start in 1995, and they are kept because they are clean measured data. Decision 14 applies the same rule to HPI. The first row's `effective_date` is the series start, not a true change date (left-censored), which is noted in the code docstring.
- **`DecimalType(6, 4)`.** Repo-era rates were quoted in sixteenths (for example 5.9375), so a scale-2 decimal would silently round them. Decimal also gives exact equality, which the change-detection step depends on.
- **Fail-loud data-quality guard.** `assert_rate_columns_consistent` aborts the run if any row carries conflicting non-null values across the rate columns. Letting the coalesce pick one would hide the conflict.

### 13. Library dependency scoping

A dependency lands in one of three places: the cluster spec, a notebook-scoped `%pip` install, or `requirements-dev.txt` for local runs and CI.

Pipeline runtime dependencies go on the cluster spec, version-pinned and committed in `databricks_src/setup/cluster_definition.json`. The spark-excel plugin is the only one so far, and it has no alternative: JVM libraries cannot be installed notebook-scoped, and serverless compute cannot load them at all, which is also why the Excel reads run on the Dedicated cluster and not serverless.

Test and development dependencies stay off the cluster entirely. chispa is installed notebook-scoped in `tests/run_tests.py` at a pinned version, and the same pin sits in `requirements-dev.txt` next to the PySpark and pytest versions the runtime already ships, so a local run matches the cluster. Databricks recommends pinning `%pip` installs, since unpinned installs do not fit serverless environments.

The split is about blast radius. A cluster library installs for every workload attached to the cluster and cannot be uninstalled from inside a notebook, so a version conflict is resolved at the cluster level and costs a restart. A notebook-scoped install reaches one notebook and disappears with the session, which is the right lifetime for something only the test runner needs. Keeping the cluster spec to what the pipeline needs at runtime also keeps it an accurate description of the pipeline.

This would change if the test suite moved to a scheduled job or to serverless compute, where the pinned environment belongs in the job or bundle environment specification, not either place above.

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

A flat 1995 floor would keep United Kingdom and Great Britain rows for 1995 to 2005 whose values are part measured and part derived. A row that is partly derived is harder to reason about than one that is wholly either, so the composite rule exists to remove that category, not to save storage.

**Why the cut is on reliability rather than on joinability.**

An earlier draft of this decision floored HPI at 1995 on the grounds that PPD starts in 1995 and anything earlier joins nothing in the project. That reasoning does not belong in a Silver table, and applying it consistently would have made things worse: it would also cut the BoE table's 1973 to 1995 rates, which are clean measured data that nothing currently joins either.

The division this project settled on is that Silver filters on whether data is trustworthy, and Gold filters on what a question needs. PPD and Police.uk cover England and Wales only, so Scottish and Northern Irish HPI rows join neither of them. They stay, because they are measured data, and because they do join Doogal on geography and BoE on date. A Gold model joining HPI to PPD will narrow itself to England and Wales as a consequence of the join, without Silver having pre-decided that on its behalf.

The practical argument is reuse. A Silver table narrowed to one consumer's assumptions has to be rebuilt when a second consumer appears, and re-deriving dropped rows means re-ingesting and re-validating the source.

**Unmapped geographies abort the run.**

The floor is applied by comparing each row's year against a start year derived from its area code. A code matching no rule yields null, and a comparison against null is false, so the row would be dropped silently. A guard runs before the filter and fails on any unmapped code, naming it and its region. Land Registry does reorganise local authorities, and a new prefix or a fourth composite is a realistic future event; without the guard, the failure mode is an entire geography quietly disappearing from the table.

**Left-censoring.** The first row for each geography is its first measured month, before which the market is simply unrecorded. This mirrors the BoE table, whose first row is the start of the clean series, not a rate change, and it is noted in the transform docstring.

### 15. Typing under ANSI mode

Databricks Runtime 17.0 and above enables ANSI mode by default, following Apache Spark 4.0. Under ANSI, an invalid cast raises a runtime exception instead of returning null.

For a wide source read as all-string, that is the wrong failure mode. The HPI file is 148,000 rows by 54 columns, so one malformed cell aborts the job with a `CAST_INVALID_INPUT` error naming neither the column nor the row, and locating it means bisecting the file.

Disabling ANSI on the cluster trades one problem for another: every malformed value silently becomes null, which contradicts the project's position that nothing fails quietly.

The transforms take a third path:

1. Cast with `try_cast` and `try_to_date`, which return null on a malformed value whatever the ANSI setting. Behaviour stops depending on a cluster-level flag that a future runtime upgrade could flip again.
2. After typing, compare non-null counts per column against the untyped frame. A column whose populated count fell has lost values in the cast, and the error names it.

That gives a loud failure with a diagnosis attached, which neither default provides alone. The cost is one aggregation pass over a small frame.

Key columns are excluded from the count comparison. A dedicated guard already fails on any null date or area code and reports the offending row, which covers strictly more: it catches a date that was empty at source as well as one that failed to parse. Covering `date` in both places produced a defect, recorded below.

**Volume columns cast through decimal first.** `try_cast('388.0' as int)` returns null, because the string-to-integer parser rejects a decimal point. Sales volumes are currently written as whole numbers, but this file has already changed its price formatting between releases, so volumes route through `decimal(18,6)` before `int`.

### 16. PPD retention differs by file kind, and the changelog is a download list

Land Registry publishes PPD in two forms that need different handling. The yearly files are state: `pp-2019.csv` is regenerated every month and stays available at a stable URL, so any version can be re-fetched at will. Land Registry publishes that as a commitment, stating that the single and yearly files continue to be updated monthly and incorporate the most current data, and release lands on the 20th working day. The monthly change-only file sits at a static URL that each release overwrites, so a release missed is a release lost.

Retention follows that asymmetry. Yearly files overwrite in place, because the current version is the correct one and past vintages answer no question this project asks. HPI reached the same conclusion for the same reason: a source that restates previously published values should be stored as its latest state, not as an accumulation of superseded ones. The change-only file is not retained at any depth. It is fetched, read for the years it names, and deleted before the run ends, so no history of it exists and none is wanted.

**No layer decides what an earlier layer ingests.** An earlier sketch had Bronze read the change-only file, work out which yearly files it affected, and re-fetch those. That is the wrong shape whatever the file contains. A layer deciding what an earlier layer should ingest makes the medallion cyclic, which costs reproducibility, because Bronze can no longer be rebuilt from source alone; it makes lineage meaningless, because the arrows no longer describe dependency; and it lets a defect downstream corrupt Bronze. Control flow of that kind belongs in the orchestrator, where the dependency runs from orchestrator to layers, never from one layer back to another. The watermark is already that mechanism.

What follows is that shape, not a return to the rejected one. The difference is not which component opens the file, it is that the decision originates in the orchestrator and never travels upward from a layer.

**The changelog is a download list.** The orchestrator selects the distinct transfer years present in the current change-only file, re-downloads those yearly files, and deletes it. It reads one column. Nothing parses the operation type, compares against Silver, or derives a measure, because a stale yearly file is fully described by the current one and the changelog only says which to fetch.

A run that never fetched a changelog cannot recover the years it named, and those wait for the annual reconcile. That is accepted and not mitigated. The state stays correct throughout, since the yearly files are authoritative whenever they are pulled; what a missed changelog delays is the trigger, not the data.

The changelog exists only to narrow a refresh, so it is fetched only when the refresh is narrow. A full pass over 1995 to the current year needs no list of what to include, and an on-demand reconcile over named years already has one, so neither fetches it. That makes the changelog redundant in the month the annual reconcile runs, beyond merely unnecessary: re-downloading every yearly file is a strict superset of re-downloading the years it would have named. Wiring both would produce the correct result and hide a pointless fetch, which is why the rule is stated here.

A full reconcile runs each June across 1995 to the current year. June rather than January because the current-year file appears somewhere between late March and early May, so a January run would report a missing file every year and train me to ignore it. A narrower reconcile over named years runs on demand, triggered by the years the current changelog names. Both re-pull the yearly files, diff against Silver, then overwrite the affected year partitions with `replaceWhere`.

**The reconcile has no year floor.** Two vintages of the complete yearly set, fetched four months apart, differ in every one of the 32 files. The 1995 file gained 18 rows, 2005 gained 34, 2019 gained 124, and 2025 gained 82,798. Recent years move most, because registration lags transfer, but no year is static. The deltas say the same thing independently: both sampled releases carry rows against every year from 1995 onward. Restricting the reconcile to recent years on the assumption that old files are frozen would therefore miss real corrections, and a spot check on one file over one interval could never have established that they were frozen in the first place.

**Why `replaceWhere` on transfer year is safe.** The download page describes the yearly files as transactions received in the calendar period. Read literally that is registration date, which would put a December 2018 sale registered in January 2019 into `pp-2019.csv` and make a transfer-year predicate delete rows belonging to a different file. Measured across all 32 files on two separate vintages, that is not what happens: each file contains exactly one transfer year, and `pp-2019.csv` runs from 2019-01-01 to 2019-12-31. One Bronze file maps to one Silver year partition. The published wording is loose, and the files themselves settled it.

The category B distribution says the same thing from a different direction. Land Registry began capturing category B at registration on 14 October 2013, yet the table holds 1,857 category B rows with earlier transfer dates, rising steadily from 11 in 1995 to 432 in 2012. That gradient is registration lag: the closer a transfer sits to the capture date, the more likely it was still unregistered when capture began. A file keyed on registration date could not produce it, because every category B row would then fall in 2013 or later.

**TUID is unique across the dataset**, 31,430,611 rows against the same number of distinct identifiers in the July 2026 release, and the same equality held on the earlier vintage. Silver asserts uniqueness. Deduplicating costs the same shuffle, but an assertion names the offending identifiers where a dedup silently discards rows, which is the same reasoning as the unmapped-geography guard in Decision 14. Uniqueness is not permanence: a category correction is published as a delete of the original transaction and an addition under a new identifier, so a category change arrives as a D and an A, never as a C.

**The diff is recorded before the heal.** `quality.reconcile_run` takes one row per run and year whether or not anything differed, which is what distinguishes a clean run from one that never happened. `quality.reconcile_diff` takes the detail: identifier, difference type, and for a value mismatch the column name and both values. The reconcile then heals automatically. Halting on any difference would be too brittle to leave scheduled, but healing without recording would make the pipeline silently self-correcting, which is the failure mode the rest of this project is built to avoid. The record is what makes the overwrite accountable.

**Row counts cannot serve as the comparison.** A change row leaves the count untouched and a delete offsets an addition, so a year can match exactly on count and differ in every value. The measured gap is large: the 1995 file moved by 18 rows across four releases while a single release's delta carried 31 rows against that year. The diff therefore hashes the business columns on both sides, anti-joins on TUID for presence, then compares hashes for rows present in both, and only rows whose hash differs are unpacked to find which columns moved. Hashing also avoids comparing 31M rows column by column, but correctness is the reason for it, not cost.

This decision would change if Land Registry stopped applying corrections to the yearly files. The change-only file would then be the sole record of them, retaining it would become necessary, and the rebuild would silently go stale. Nothing in the platform would notice, which is the one reason the annual reconcile is scheduled and not run on demand alone.

### 17. One shared contract across Silver transforms

The first two Silver transforms diverged. BoE took lineage as function parameters and declared its Delta table in SQL with CHECK constraints; HPI let the caller stamp lineage afterwards and let `saveAsTable` infer the schema. Both worked, and neither was obviously wrong on its own, so the divergence only became visible once a third source arrived and the question became which one PPD should copy.

Each half of the split had a better answer, and they were independent, so PPD did not have to choose between them.

Lineage belongs inside the transform, taken as a parameter. Both files were reaching for determinism under test, and passing the timestamp in achieves it without cost: the transform stays pure, its output schema equals the table schema, and the tests can assert lineage. Stamping it in the caller buys the same determinism by removing two columns from the transform's contract, which then cannot be tested and do not match what the table holds.

The table is declared, not inferred. `saveAsTable` with overwrite replaces the table definition, so it cannot carry CHECK constraints or a comment. Declaring the schema once, adding constraints, and writing with `INSERT OVERWRITE` keeps the definition across runs. That matters most for PPD, which will later be modified in place by a merge and by `replaceWhere`: a bad full rebuild is fixed by re-running, while a bad merge corrupts state, and constraints are the backstop for the second case.

The column list is generated from the same constants the casts use. HPI declares 56 columns, and a hand-written copy would have been free to drift from the cast without anything failing.

Two rules came out of the guards, not the structure. A guard reports offenders as a sample and returns its frame so guards chain, but it only counts the full offending population where the frame is small enough that the extra pass is free, which holds for a daily rate series and not for 31M transactions. And a check belongs on whichever frame still carries the column: PPD validates Record Status on the source frame, because the column is deliberately dropped before the typed frame exists.

The convergence was done as a refactor with the logic held still, and verified by running the existing suites unchanged before touching them. It stopped at a shared convention. There is no base class and no shared helper module, because the three sources read a multi-sheet Excel export, a wide CSV panel with a header, and 32 headerless CSVs, and a generic guard over those would hide more than it saved.

### 18. Doogal keeps the ONS spine and drops the publisher's additions

The postcode file is two datasets under one header. Most of it mirrors the ONS Postcode Directory: versioned, quarterly, documented, and reproducible from the ONS release itself. The rest is the publisher's own enrichment, computed from unnamed inputs at unstated dates. Silver keeps 42 columns and drops 18.

The 18 fall into four groups, and only one of them is a judgement call.

**Restatements, 5 columns.** Grid reference is a reformatting of easting and northing. Postcode area and postcode district are substrings of the postcode. Plus Code is an encoding of the coordinate pair. The in-use flag is the interesting one: it holds exactly when the termination date is null, with no exception in 2,713,360 rows, so it carries no information at all. It is still checked. The transform asserts the equivalence on the source frame before dropping the column, on the same reasoning as PPD's record status: a break there would mean the publisher changed what a column means, which is worth failing on even for a column that does not survive.

**Wrong grain, 2 columns.** The UPRN list and the road list are comma-separated inside a CSV. A multi-valued attribute at postcode grain is a different table, not a column, and the two of them are a large share of the file's growth from 950 MB to 2.01 GB.

**Derived with no provenance, 9 columns.** Altitude, nearest station, distance to station, distance to sea, and most common property type are computed by the publisher from inputs the file does not name at dates it does not state. Population and households are 2011 census figures and average income is a 2020 model estimate, all three pre-joined down to postcode grain from a coarser one. The output area and MSOA codes are retained, which is what makes those three cheap to drop: a Gold model that wants population or income can join the published source at its real grain instead of inheriting an apportionment with no methodology attached.

**Deprivation, 2 columns.** This is the case that took the most thought. The rank column holds four separate national indices, at four vintages, on four scales: England 2025 ranking to 33,755, Scotland 2020 to 6,976, Wales 2019 to 1,909, Northern Ireland 2017 to 890. Nothing in the row says which applies. A consumer reading it as one series gets an answer that is wrong and looks right, which is the same shape as the HPI derived back-series in Decision 14: a value that is part one thing and part another. The decile column is worse, not better, because 1 to 10 everywhere conceals the incompatibility instead of hinting at it. Both go, and the LSOA codes stay so a published index can be joined properly.

**Why a column rule can be stricter than the row rule.** Decision 14 argues against narrowing Silver to today's consumers, and that argument was about rows. It does not transfer here because the cost of reversal differs. Re-deriving a dropped HPI row means re-ingesting and re-validating a source. Doogal is a full rebuild from a snapshot that is re-read every run, so restoring a column is one line and a few minutes. Cheap to reverse makes a stricter rule affordable. That is a distinction, not an exception.

**What was kept for reasons worth recording.** Terminated postcodes are 915,354 rows and resolve 14,395 distinct PPD postcodes that no longer exist. Both LSOA vintages are kept because Police.uk publishes against whichever ONS boundary vintage was current at the time, so the crime history spans both and needs one join key per era. The county council column is renamed `county_council`. The source's "Local authority" names the wrong thing: it holds the county council area, is unpopulated for unitary authorities and London, and the actual local authority district is the district column. Keeping the source name would have handed every future consumer a wrong join.

**Null geography is legitimate, so it is bounded rather than rejected.** The BF postcodes carry no country, no positional quality, and no user type, because they are not in a UK geography. Those rows are measured data and are kept. That makes the guard the inverse of Decision 14's: there an unmapped area code always signals a fault, here a null is expected for one postcode area and a fault everywhere else. The transform aborts on null geography outside BF, so a geography the publisher has not yet mapped cannot hide among the forces addresses.

**A known join asymmetry, recorded and left in place.** 360 of Doogal's 361 district codes appear in HPI's geographies. The one gap is the Isles of Scilly, which Land Registry states it does not publish because of low monthly sales volumes. PPD records transactions there and Doogal resolves their postcodes, so a Gold join through to HPI drops them. Neither table is wrong, so the answer belongs in the Gold model, not in either Silver table. ONS excludes both the Isles of Scilly and the City of London from the private rents index for the same reason, which is a different pair, so the ONS source cannot inherit this answer.

This decision would change if the publisher began stating the vintage and method of its derived columns, which would make them assessable, or if a consumer needed a measure only available pre-joined here.

### 19. Decompression is transport, not processing

Spark cannot read inside a ZIP archive, so something has to unpack it before Silver can parse anything. Bronze keeps the archive exactly as published, and the Silver notebook extracts to cluster-local disk on each run.

The question looks like a medallion-purity question and is not one. Compression is transport encoding, not content: unwrapping an archive touches no column, no type, and no value, so unpacking at ingest would not have violated the rule that Bronze does no processing any more than decoding base64 would. What the two positions actually disagree about is what Bronze is made of. Where Bronze is a table layer, a ZIP cannot go into it and decompression happens at or before ingest by necessity, which is why most Databricks material treats that as the default. Where Bronze is a landing zone, as it is here under Decision 11, byte fidelity is the stated purpose and unpacking destroys it.

What decides it in practice is how many reads each ingest serves. Doogal publishes quarterly, is ingested quarterly, and is rebuilt quarterly, so decompressing once at ingest amortises across exactly one read. It would buy nothing and cost 2.01 GB of stored output plus the ability to check Bronze against the source.

Police.uk inverts that arithmetic and makes the strongest case in the project for expanding at ingest. Seven archives, every one restating up to three years, all read far more often than they are ingested, and police.uk publish an MD5 for every archive, which turns fidelity from something preserved into something proved and recorded. It still loses, for a reason the Doogal case never raises. Roughly 4,500 CSVs per archive is about 31,500 small files if expanded into blob storage, and the deduplication in Decision 24 discards 40% of them from the file names alone. Expanding at ingest would write every losing copy to storage in order to never read it. So Police.uk keeps the archive too, and extracts at Silver like Doogal, but one archive at a time: the full winning set expands to 19.2 GB against 4.4 GB for the largest single archive, and only the second of those fits comfortably on one node.

Reading from a local path ties these notebooks to a single-node cluster. On a multi-node cluster the executors do not share the driver's filesystem and the read raises file-not-found, so the constraint announces itself instead of returning partial data. That makes it cheap to carry and cheap to fix, which is why it is preferred over staging the extracted file back into managed storage.

### 20. The ONS workbook is converted, not read in place

spark-excel reads the other Excel source in this project, the Bank of England workbook, directly from Bronze. It cannot read this one.

Reading as string returns the cell's display format, not its stored value. ONS holds these figures to six decimal places and displays them to one, so an index of 81.413747 arrives as 81.4. Nothing fails: the row count is right, the column count is right, the casts succeed, and the numbers look reasonable. Five decimal places are gone and no guard in the shared contract would notice.

Reading with an explicit schema recovers the stored value, which is how the Bank of England transform is typed. But the 36 measure columns contain `[x]` and `[z]` as text. Under a numeric schema those collapse to null at best, merging "not available", "not applicable" and "failed to parse" into one indistinguishable state. It also removes the all-string frame that `assert_casts_preserved` compares against, so the guard that turns a silent null into a named failure cannot run at all.

Neither path yields the published values as strings, which is what Decision 17's contract needs. The `usePlainNumberFormat` option does not help: it overrides the General and Text formats, and these cells carry an explicit one-decimal format.

So the Silver notebook converts Table 1 to CSV on cluster-local disk using openpyxl in read-only mode, which returns the stored value and a real date, and reads that CSV with the same all-string pattern every other source uses. Bronze keeps the workbook exactly as published. This is the shape Decision 19 already argues for with the postcode archive, and it inherits the same single-node constraint.

Two guards make the conversion auditable. The sheet's declared dimension gives an expected row count from a different code path than the row iteration, so a reader that skips rows is caught. And floats are serialised through `Decimal`, because Python's shortest round-trip representation switches to scientific notation below 1e-4 and would otherwise hand Spark an exponent to parse.

A secondary reason, discovered first: spark-excel materialises the sheet through POI in the driver heap, and `dataAddress` selects cells after that, without bounding it. On a four-core, 16 GiB single node a 17 MB workbook that is almost entirely one sheet kills the driver. That would have justified a memory workaround; the fidelity problem is what makes conversion the correct answer, over a larger driver.

### 21. ONS keys on area name, because eight geographies have no code

ONS publishes no GSS code for the eight Northern Irish broad rental market areas, writing `[z]` in the area code column instead. Area code therefore cannot be the key, and cannot be `NOT NULL`, which the shared contract requires of key columns.

Area name is unique across the release, so the grain is `(area_name, date)`. Area code stays nullable and is what joins to HPI and the postcode lookup where it exists. A guard fails the run if a null code appears outside Northern Ireland, so a release that starts coding those areas, or that drops a code elsewhere, changes the join surface loudly.

Identifying Northern Ireland needs both a code test and a parent-column test, and each one carries a different set. The country row `N92000002` has `[z]` in its parent column, so only the code test finds it. The eight rental areas have no code, so only the parent test finds them. Either test alone fails on the real file.

### 22. Absence is asserted into position, then nulled

Both markers become null in Silver. Neither is preserved as a value, and no flag column records where they were.

That is safe only because the positions are asserted first. Every `[x]` must fall into one of the three structural cases the source publishes it in, and `[z]` must not reach a measure column at all. A marker anywhere else aborts the run. So the information the marker carries is either already derivable from the row, since "first month of the series" is a fact about the date, or it is a source change that should stop the load instead of becoming a null.

A fourth guard checks that unpublished months are the trailing ones. A nation that lags is missing its latest months; a gap in the middle would pass the structural check, which tests position by column, not by date.

The part-imputed UK rows are kept on the same reasoning turned around. They are ONS's own published headline with a documented method, and not something derived here, and Great Britain covers the same months fully measured for anyone who needs that. Dropping them would delete the most recent UK figures, which is the opposite of useful. Which months are affected moves every release, so it is derivable from the Northern Ireland rows, and it belongs in the table comment, not the schema: a flag column would be meaningful on 138 rows out of 49,266.

This is the same shape as Decision 14, where HPI's derived back-series was dropped, and the outcome differs because the claim is weaker. HPI spliced on a different index entirely. This imputes one nation's two-month lag forward within the same index.

### 23. Duplicates in police street crime are counted, because the source has no key

Every other Silver table asserts a key and fails on a repeat. This one cannot, and the reason lies in the source's design.

Crime ID is a one-way hash of the force's offence reference, and police.uk leave it blank for anti-social behaviour. Blank IDs run to 30.8% of the table and anti-social behaviour to 26.4%, so a further 4.17M rows of other types also arrive without one. Dates are truncated to year and month at anonymisation. Coordinates are snapped to shared map points. Those three together mean two genuine burglaries on one street in one month produce byte-identical rows, and police.uk separately state they suspect some forces of double counting anti-social behaviour. Identical rows therefore have two causes that cannot be told apart from the row, and deleting them would remove real crimes.

So Silver keeps them and measures the population instead: 5,410,380 groups and 10,932,596 rows beyond the first, 11.4% of the table. The obvious objection is that this is an artefact of records with no location, and the split says otherwise. Only 95,723 of those extra rows carry no location at all. The rest are located records at shared snap points, which is what the published format produces, and not evidence of a fault.

Crime ID is no use as a fallback either. 8,654 IDs recur monthly across the whole series, covering 1,405,213 rows, every one of them Northern Ireland. The hash is of a reused reference, not of a crime.

The table therefore names the columns that must be populated without calling them keys. Crime month, crime year, force, and snapshot month are the grain the file selection works at plus the vintage the row came from. None of them identifies a row, and a constant named `KEY_COLUMNS` would have implied otherwise to every later reader.

`snapshot_month` is the other half of this decision. Outcome state is whatever the archive that supplied the row happened to hold, so a 2011 crime has years of settlement behind it and a crime from the newest month has none. Carrying the vintage as a column makes the lag derivable, and a Gold model comparing outcome rates across years is wrong without it.

### 24. Overlapping snapshots are resolved from file names, before anything is decompressed

The planned approach, recorded in Phase 1, was a window function over the loaded rows keeping the newest snapshot's version of each month, force, and category. That would have worked and it would have been the expensive way to do it.

The whole key is in the path. The archive filename gives the snapshot, the inner path gives the month and the force. So the winning copy of each slot is decidable from the ZIP central directories alone, which cost no decompression: 13,887 street members collapse to 8,288, removing 40% of the read before a byte is expanded, and no shuffle is spent on it. It also removes the need for a row key to tell copies apart, which matters given Decision 23, because there is no row key to be had.

The cost is that cross-snapshot disagreement stops being a by-product of loading and becomes a separate comparison. Byte sizes from the same central directories give a cheap approximation, and it found the largest restatement in the source: British Transport Police restated the whole of 2023 downward by roughly 45% between the December 2023 and June 2026 archives. Twelve consecutive months at about half the bytes. Newest-wins keeps the smaller version, which is right, and the table alone would never have shown it.

Deleting the 2013-12 archive was correct and did cost something here, which is worth recording. It held nothing but header-only files that no other archive lacked, so no data was lost. But it was the second copy of 2010-12 to 2013-12, and without it 101 of 187 months are now held by a single snapshot, in alternating years, with no second copy to compare against. Fair trade for 0.86 GB, and it should have been priced at the time.

### 25. Validation runs in one pass where the table is large enough for it to matter

The other five sources declare one guard function per rule, each called as a statement, each an action over the frame. That is the right shape for a table of thousands or a few million rows, and it is what Decision 17's shared contract describes.

Police is 96,092,836 rows. The same shape cost roughly seven full passes before anything was written, and it dominated the run.

Every rule here is a row predicate, so they fold. Each stays a named function with its constraint written out and its own evidence columns, but returns a predicate instead of raising, and one aggregate evaluates all of them together with eight non-fatal measures and two bounded vocabularies. A clean archive costs one pass. A broken one costs one more short read per failing rule, capped, which is affordable because the load is aborting anyway.

Two things improve beyond the speed. Every failing rule is reported in one pass, which matters when each retry costs a twenty-minute extraction. And the cast check becomes sharper: comparing populated counts between the raw and typed frames can only say a column lost values, while a predicate reports the string that failed.

What does not fold is aggregate guards. Uniqueness, grain, and per-file span checks shuffle, and no amount of restructuring makes them share a pass with a row predicate. Police has none of those, which is why the collapse is complete here and would be partial on PPD.

The same lesson has a second instance in the same notebook. `ALTER TABLE ADD CONSTRAINT` validates every existing row before it attaches, so the sibling habit of dropping and re-adding every constraint each run is free on an empty table and nine full scans on a populated one. Reading the attached constraints from Delta's table properties and touching only what differs costs metadata alone.

The general rule, and the one worth carrying forward: read the sibling files, then decide. A convention written for nineteen thousand rows can be wrong for ninety-six million, and nothing about it announces that.

### 26. Aborting is the default, so a quarantine table needs a population to hold

The roadmap has carried a quarantine table since Phase 1, on the standard reasoning that bad rows go aside and good rows proceed. Building all six Silver transforms produced the opposite habit, and the habit is right.

Almost every guard in this platform is a contract check. An unrecognised crime type, a column set that no longer matches, a code outside its published set: each means the source changed shape, and loading the rows that happen to still parse would produce a table whose schema nobody understands any more. Decision 16 states it for PPD, that a new code is a source change, not a row to skip. Quarantining a contract violation converts a loud stop into a quiet partial load, which is the failure mode Principle 4 exists to prevent.

That leaves the population a quarantine table would actually hold: rows that satisfy the contract and fail a plausibility bound. Across six sources and roughly 130M rows the clearest instance is the 24 rows in Decision 23's source where British Transport Police published Scottish stations with corrupted longitudes, and those are better handled by nulling the coordinate and counting it than by removing the crime.

So the table is deferred, and the decision that gates it is which checks are contract and which are value. That split is worth making explicitly when the quality framework is designed, because it also decides which rules belong in config and which stay in code. If the value population stays this thin, the honest outcome is to record why the table was not built.

### 27. The watermark does not obviously belong in Delta

The Phase 1 plan records migrating `watermark.json` to a Delta table as part of watermark automation. That is worth re-deciding, because the cost sits somewhere the original framing did not look.

ADF reads the watermark on every orchestration run, from a JSON file in blob storage, at no compute cost. Moving that state into a Delta table means ADF starts a cluster to read it, on every run, before any data moves. The benefit sought was write-side: Databricks updating state after a successful load, over the state being edited by hand.

Having Databricks write the JSON back gets the write-side benefit and leaves ADF's read path untouched. The Delta version buys transactional history and queryable state, which matter if the watermark grows beyond six source entries or if several writers ever contend for it. Neither is true today.

This is the same shape as Decision 11, where Bronze stayed as Volumes over Delta because the table format earned nothing at that scale. Recorded here so the choice is made on its cost, not on the default that Delta is the answer to state.

### 28. The audit layer is two tables, because a failed run has to leave a row behind

`pipeline_metric` holds one row per measured value; `pipeline_run` holds one row per notebook execution whatever the outcome.

A metrics-only table records nothing when a load fails, so the absence of rows for a source cannot be told apart from the notebook never having been run. That is the same failure the freshness work exists to catch, and it would have been least visible in the record itself. The run row is inserted at start with status `started` and updated on completion, which gives three end states instead of two: a killed cluster leaves an open row instead of disappearing and quietly raising the observed success rate.

Metrics buffer in the run object and flush once, on success or failure, so a load that aborts still records what it measured before it broke. A Delta commit per metric would cost a transaction for every printed count.

Three rules follow from the tables being read by a dashboard in Phase 5. Counts are stored with the base they are a share of, and not as a pre-computed ratio, because counts and bases re-aggregate across runs and percentages do not. Metric names come from a registry in the writer, since a rename that nothing catches looks exactly like a series that was discontinued. And a metric's `kind` lives on that registry, not on the rows, so it joins at read time and applies retroactively to everything already recorded: a share sitting at a third of all rows can be the source's shape and not a fault, and only the registry can say which.

Police needed one addition. It validates seven archives in one run, so the same metric name appears seven times under one run id, distinguished by a `scope` column and each carrying its own archive's row count as the denominator. Summing numerators and denominators before dividing gives the correct share for the load; averaging seven percentages would weight a 264-file archive the same as a 2,196-file one.

### 29. Freshness bounds are read from the data, not from a publisher's calendar

Bronze sat on a four-month-old Police archive until an inventory surfaced it. Nothing failed, because nothing was wrong with the file that was loaded. The gap was that no source asserted anything about how new its content was, and the planned watermark automation would not have closed it either, since ONS cannot be pattern-matched from its URL.

Each Silver notebook now records the newest date its content carries, and each source has a bound in days above which the load aborts. Every bound ships unset, and the first observations show why that matters: the healthy lag runs from 8 days for the BoE workbook to 98 for the HPI release, with PPD at 38, ONS at 67, Police at 67, and Doogal at 69. A single observation cannot say whether a source was measured at the top of its cycle or the middle, and the difference decides the bound. Setting one from a publisher's stated release calendar would be a guess about a cycle nobody here controls, which is the mistake the whole approach exists to avoid.

Two sources needed a signal other than the obvious one, and both are instances of the same thing: the file and the data are different objects.

The BoE rate has held since December 2025 through four unchanged decisions, so the newest rate change is stale by design and asserting on it would fire every month while the pipeline is healthy. The newest day carrying any rate is the signal instead.

ONS is the sharper case. Its URL is rewritten by hand every month, so the landed filename records which release was asked for, which is a different question from the one it served. The workbook's cover sheet carries a publication statement, and the date parsed out of it is the only value in the file that can contradict the filename. On the 2026-07 release those agree: filename claims 2026-07, cover sheet says published 22 July 2026, while the newest data month is June. That gap is publication lag, not staleness, which is exactly why the content date alone cannot answer the question. The parser returns nothing instead of raising if ONS rewords the statement, since that should cost the cross-check and not the load, and the statement is recorded verbatim either way so the change stays visible.

---

### 30. The Gold model is a star over two geography grains

Four screens ask one question in four ways: what a place costs to own, what it costs to rent, what that implies as a yield, and what living there is like. Every screen needs an area, a month, and measures attached to both.

The difficulty is that the sources do not share a geography. Prices and rents are published at local authority district and above, crime at Lower layer Super Output Area, transactions at postcode. Only the transactions move between levels, because a postcode aggregates honestly to either. An index cannot be pushed downward at all: apportioning a district figure across its neighbourhoods invents the distribution it claims to describe, and the invented numbers would be indistinguishable from measured ones on a screen.

So there are two geography dimensions, not one. `dim_area` holds every published area at every level the sources report on, 432 rows. `dim_lsoa` holds the 36,778 small areas in England and Wales that crime or transactions reach. They conform through `district_code`, which is the finest level both can express.

A single dimension was the obvious first answer and it does not work. The textbook shape is a fine-grained dimension with a shrunken rollup above it, which needs the coarse level to be derivable from the fine one. Here the coarse dimension is a superset: it carries counties, regions, nations, composites, Scottish and Northern Irish members, and eighteen Scottish broad rental market areas, none of which any small-area base can generate.

The area dimension is at published-area grain with a level attribute. The house price index publishes 405 geographies and the rent series 357, both as hierarchies, and the area profile compares a district against its region and the country. A regional index is mix-adjusted and published; deriving it by averaging its districts would put an estimate where a measured value already exists. The cost is a mixed-granularity model, where an index exists at every level but a transaction median and a rolled-up crime count only at district and below. That is marked on the dimension, not split into separate tables.

Levels are not one clean hierarchy either. Districts roll up to region and then to nation. County-level areas carry their own published index and no children, because the only district-to-county membership available uses ceremonial counties on a code series whose values collide with the metropolitan county codes the index uses: the same code names Greater Manchester in one source and Bedfordshire in the other. A join on it would silently mis-aggregate. Rental market areas are a separate administrative geography that nests inside a nation and matches nothing below it, which is why Scottish and Northern Irish rent cannot be paired with a price for the same place.

Nine facts sit under those dimensions, plus a daily calendar and a crime type dimension. Keys are natural throughout. Surrogates buy insulation from a code change and cost a sequence, a join to read anything, and instability across a full rebuild, and a full rebuild is what this pipeline does on every run. The eight uncoded Northern Irish rental areas get a deterministic code derived from their name, shaped so that it cannot be mistaken for a published one, and a column recording that it was assigned here.

No aggregate marts. A yield is price over rent and a crime rate is a count over an area, both cheap to express downstream and both premature to store before a screen exists to read them.

**Price appears twice at district grain deliberately.** The index is mix-adjusted and official, the transaction median is raw, and the case they disagree on is exactly the case worth showing: an area whose median rose because more detached houses happened to sell that month. Carrying both demonstrates that.

---

### 31. Crime is located by its published code, not by its coordinate

Police.uk anonymises every crime type the same way: dates truncate to year and month, and coordinates snap to shared map points. The snapping is not precision loss around a true position, it is substitution. The point on the row may serve several streets and exists to prevent an address being identified. Deriving an area measure by point-in-polygon over those coordinates would produce a number that looks precise and describes nothing, so area crime is computed from the published small-area code alone and the map renders areas instead of plotting incidents.

**The published code resolves cleanly.** Of the 36,778 areas in the dimension, 36,696 sit in exactly one district. The 82 that straddle take the district holding most of their postcodes, and every one of them holds at least 92% of them there, so no assignment comes down to a tie-break. The dimension marks them, which keeps the estimate separable from the exact ones.

**Coverage below force level is England and Wales only.** Northern Ireland publishes 2,311,848 rows with no small-area code at all, and Police Scotland does not publish to this source: the table carries 34,763 English and 1,988 Welsh small-area codes and no others, summing to the 36,751 published codes above. A further 1,428,441 English and Welsh rows carry no code, so 3.9% of the table has no geography. Crime therefore sums as far as the England and Wales composite and no further: a United Kingdom total would be a partial count wearing a whole one's label.

**Boundary vintages are a band.** ONS revises small-area boundaries at each census and police.uk publishes against whichever vintage a force's gazetteer held at the time. Both vintages appear together from 2021-01 to 2023-06, thirty months, and 2023-07 onward is clean. That pattern survives every attempt to attribute it to something tidier: each supplying archive covers a contiguous block of months, and the mixing sits inside two of those blocks, not between them, while nearly every force shows both vintages continuously for roughly twenty-nine months.

The tempting move is to restate the older codes onto current boundaries for one continuous series. ONS publishes an exact-fit lookup that would support it, and for unchanged, split and merged areas the arithmetic would be exact, not estimated, since crime counts are additive.

It is still the wrong shape. A boundary revision does not rename an area, it creates a different one, and a crosswalk exists to conceal that. The model keys on the code as published. An area that did not change keeps its code and runs unbroken, which is 33,646 of the codes; an area that did carries two series, which is the truth about that area. The consequence is softer than a break: such a series fades out and in across two and a half years instead of stopping on a date, and the small-area dimension records which codes that applies to.

The vintage does not need to sit on the fact. A changed area has different codes in the two vintages, so the published code already carries the vintage wherever it is determinable, and where it is not, the area did not change and the question does not arise.

That also settles what the crosswalk is. It would have been a seventh source, and it does not qualify. Every existing source is a feed: it refreshes on a cycle and earns a watermark entry, an incremental type, a freshness bound, and audit metrics. A census crosswalk changes once a decade, so putting it through that machinery produces machinery with nothing to do and a freshness bound that stays correct for ten years, which is noise dressed as monitoring. If it is ever wanted it is reference data loaded once.

**Two vocabulary changes, both splits.** The crime type list changed at 2011-09 and again at 2013-05. Both times an existing type lost volume to new types that sum back to it while the untouched types held their level: at 2011-09 "Other crime" fell by 172,623 as five new types appeared totalling 169,645. Nothing entered the count, so an all-types total is comparable across the whole series even though an individual type series is not. The crime type dimension carries each type's first and last published month and what it was split out of, without which no cross-era reconstruction is possible.

**Anti-social behaviour is reported, never totalled.** It is a different kind of record from the rest. Police.uk publish no outcome for it and state that they suspect some forces of double counting it. Its share of the table also moved from roughly 42% in the early years to 16% in the recent ones, so any total containing it falls over time for reasons that have nothing to do with crime. It is held out of the crime fact entirely and reported beside a total that excludes it, so a sum over crime types cannot pick it up by accident. A check constraint on the fact rejects any row carrying it, so the exclusion is structural and not a convention someone has to remember.

---

### 32. Below district, price is annual, because a median needs a population

Transactions resolve to a postcode and therefore to a small area, so a small-area price series is available in principle. At monthly grain it is not worth having. Across 10,867,452 area-month cells, 3,246,648 hold one transaction and 2,740,723 hold two, so for 55% of cells the median describes an individual property, not a market.

At annual grain the same cells are dense: 998,445 of 1,135,051 hold eleven transactions or more. Price below district is therefore an annual fact and crime stays monthly, which also keeps two different coverage windows apart, since transactions run from 1995 and crime from 2010-12.

Transactions are attributed to current boundary codes only. Attributing one to both vintages would double count 5.7% of the table on any total, and attributing by period has no clean rule when the transition is a thirty-month band that each force moves through at its own pace. The cost is that the 1,106 codes exclusive to the older boundaries carry crime and no price, which the dimension records so it is not left to be discovered.

District is assigned from the postcode. The district recorded on the transaction is set aside. The two disagree on 4,704,276 rows, and the rate falls from 24.7% in 1995 to 5.0% in 2026, which is local government reorganisation showing up in an old record, not an error in either source. Resolving through the postcode restates history onto current boundaries, matching both index publishers: every current authority is published back to its nation's coverage floor, not from its own creation date.

### 33. Ancestry is flattened into the dimension, not walked

`dim_area` carries `region_code` and `nation_code` beside `parent_area_code`, so a row states every level it belongs to. A region figure then needs no recursive walk up the parent pointer.

The reason is arithmetic, not convenience. Areas do not all reach the same depth: a
district in England has a region and a nation, a district in Wales has only a nation, and
a composite has neither. A region-level figure has to count every area belonging to that
region whether or not anything sits below it, and a nation-level figure has to count the
areas that reach no further. Walking the pointer answers the first and quietly loses the
second.

`region_code` is null for the 65 districts outside England, which is 18 percent of the
361. That is structural, being every district in three of the four
nations, which is why the column is explicit instead of the raggedness being discovered
through a failed drill-down. A region row carries its own code, matching how
`nation_code` already behaves on a nation row, so a filter reaches the published region
series as well as the districts under it.

Two constraints keep it honest. `region_code` must match `^E12[0-9]{6}$`, and only a
district or a region may carry one. The first is not defensive: a district's region is
resolved by matching the postcode directory's region name against the price index's, and
the index publishes `E12000005` as "West Midlands Region" precisely because it also
publishes the metropolitan county `E11000005` as "West Midlands". A name match landing on the county is a wrong parent, not a missing one, and the constraint is what
separates the two.

### 34. The postcode directory restates the country where a region does not exist

Only England is divided into regions. The directory does not leave its region column
empty for the other three nations: it writes the country name there, so a Scottish
district arrives naming "Scotland" as its region.

The region lookup is therefore gated on the country. Left
ungated, a Welsh district naming "Wales" is indistinguishable from an English district
whose region failed to match, and the guard that catches the second fires on all 65 of
the first.

The same distinction runs through the guard itself. An English district the directory
carries but files under an unmatched region name is a broken join and aborts. An English
district the directory does not carry at all has no region available from anywhere, which
is a coverage gap, and `has_postcodes` already records it. Testing whether a region name
was supplied merges the two cases; testing the country and the directory membership
separates them.

### 35. Postcode resolution crosses a national boundary, and only a geography filter catches it

A postcode unit lying across the Anglo-Scottish border is assigned whole to one district.
Four Land Registry transactions on three postcode units therefore resolve to a Scottish
data zone, and those three codes are excluded from `dim_lsoa`.

The rows are real. Two are Berwick-upon-Tweed properties in Northumberland whose TD15
units the directory places in Scottish Borders, an English town inside a
Scottish-administered postcode area. The third is bad source data: a Kelso town name, a
Blyth Valley district and a Northumberland county on one 1999 row, three fields that
cannot describe one property.

Exclusion is right for both causes at this grain, so they are not separated. What the
finding changes is that membership is filtered where it is assembled, not on either source. Filtering the source that looked likely would have left the one that was not: the
crime source publishes England and Wales codes only, 34,763 and 1,988 of them, and carries
nothing Scottish at all.

This is the national-boundary form of the straddling problem Decision 30 handles one level
down through `majority_share`, except that here the answer is exclusion, not an assignment.

The area-grain facts resolve district through the postcode too, so the same four
transactions would land in a Scottish district carrying an England and Wales price.
Whether `fact_area_month_price` filters on nation, on `has_price_index`, or not at all is a phase 3.4 decision. Phase 3.3 covers the two published panels, whose keys are the publishers' own area codes, so no postcode resolution is involved and the question does not arise there.

### 36. An absent crime count is not a crime count of zero

`dim_lsoa.has_crime` records whether the crime source publishes a code, and it exists
because a zero at small-area grain is otherwise unreadable.

27 of the 36,778 areas carry no crime: the 26 exclusive to the 2021 boundaries, plus one
area present in both vintages. All 27 entered membership through a transaction, never through crime, which is what the union rule predicts and nothing else. The singleton is
`E01032775`, Tower Hamlets, 31 postcodes and an exact district assignment. A real place
with no published crime, most plausibly because the source snaps each crime to a fixed
anonymised point and this area contains none.

The crime facts must not manufacture zero rows for these areas. A generated zero is
indistinguishable on a map from a measured one, and 27 conspicuously safe areas would be an artefact of the source's geography, not a finding about crime. `has_crime` is
the filter that keeps the two apart, which is why it sits on the dimension.

Same shape as the anti-social behaviour exclusion in Decision 31: a compositional artefact
that reads as a measurement unless something separates them explicitly.


### 37. Key and grain checks are one module, because the star declares twenty of them

Unity Catalog records primary and foreign keys and enforces neither. They describe the
model to the optimiser and to Power BI, which is worth having, but a fact naming a
dimension row that does not exist loads without complaint and then disappears from every
rollup keyed on it. A repeated key loads too, and doubles whatever is summed over it. The
load is the only place either is catchable, so both checks run there.

Nine facts point at two geography dimensions, the calendar and the crime type list, and
the small-area dimension points at the area dimension. That is around twenty instances of
the same two checks. One copy per site is one wording per site, and the site that reports
the least is the one nobody notices is wrong.

The shared function takes the child frame, the parent frame, the two key columns and the
names to put in the message. The check that already existed inside the small-area
dimension delegates to it and keeps its own name and signature, because the load order it
forces, the area dimension written before small areas are checked against it, is a
property of that table.

Two things about the reporting were settled by getting them wrong first. The extracted
version replaced a sample of failing rows with the distinct failing values and a count of
the rows carrying each, which is better for a fact, where the failing key is the whole
diagnosis. It is worse for the small-area dimension, where a null district code says
nothing about which small areas produced it and the frame cannot be queried afterwards
because it is pre-write inside a failed load. The function now takes an optional column to
carry one worked example of, lowest value per offender so a rerun names the same one, and
the facts leave it unset. Separately, both the old version and the first extracted one
truncated the offender list silently, so ten values shown could not be told from ten of
four hundred; the total is now reported beside the list, in a second pass that only a
failing check runs. A clean load pays one action, which is what it cost before any of this
detail existed.

The general form extends the rule Decision 25 arrived at, that sibling files are read
and never reconstructed: an extraction is finished when it reports everything the code it
replaced reported, and not when the new code passes.

### 38. The rent fact takes its key from the loaded area dimension

ONS publishes no area code for the eight Northern Irish broad rental market areas, which
is why Decision 21 keys the Silver rent panel on area name and leaves the code null on
those rows. The Gold fact keys on area code, so eight of its 357 geographies have no key
available from the table they come from.

The area dimension already assigns them one, derived from the name and deterministic
across releases, marked as a project-assigned code. Two ways to reach it: recompute the
derivation inside the fact, or join the dimension on name.

Recomputing puts the same rule on both sides of a foreign key with nothing forcing the two
copies to agree. The join makes the dimension authoritative, which is what a dimension is
for, and it fits the rule that a Gold transform's signature names the dimensions its output
conforms to. So the fact takes the loaded area dimension as a parameter and resolves the
uncoded rows against it, restricted to the codes this project assigned.

Measured before it was written: eight names, eight derived codes, one match each, no name
missing and none matching two. No published code carries two names either, which matters
because Silver's grain is name and month while the fact's is code and month, so a shared
code would merge two series and the informational primary key would not stop it.

This is the same shape as the straddling problem in Decision 30, one level up. Where a
small area belongs to two districts the dimension records a majority and marks it; where an
area has no code the dimension issues one and marks it. In both cases the fact reads the
answer off the dimension.

### 39. A row carrying no measure is not a fact

The rent panel holds rows with every measure null. Northern Ireland lags the other nations,
ONS writes its unpublished months as a marker across the whole row, Silver turns those
markers into nulls and keeps the rows. That is right at Silver, where an unpublished month
is absent and not unreliable, and Decision 22 already covers why the markers become
nulls only after their positions are asserted.

At Gold it is wrong. Eighteen rows of 49,266, nine Northern Irish areas across two months,
and the count moves with every release. Nothing renders from them, which was the first
argument for leaving them alone, and it is not the problem. The problem is that every count
taken over the table includes them: the row count recorded against the load, the coverage
figure, and any measure a screen derives for how many months a series covers. The number is
wrong by exactly the lag and the lag moves.

It is also visible on a screen, in the one place it matters. With the rows kept, the latest
published rent for Belfast reads June and renders blank while England renders a figure.
Dropped, it reads April and renders the most recent rent actually measured.

So the fact drops them and the load records how many it dropped. The index panel holds none,
measured and not assumed, so the rule only ever fires on rent. This follows the rule
that Silver filters on reliability and Gold on what a question needs: no screen asks which
months ONS has not published yet, and the check constraint in Decision 31 that keeps
anti-social behaviour out of every crime total is the same move on a different source.

### 40. Seasonal adjustment stops above district

The index publishes seasonally adjusted price and index columns for 15 of the 405
geographies it covers. Every one is a region, a nation or a composite. No district and no
county carries the series at all, and Northern Ireland carries none at nation level
although the United Kingdom composite that contains it does.

The figures divide exactly. Nine regions and three composites are adjusted throughout.
England, Wales and Scotland are adjusted at nation level and Northern Ireland is not, which
accounts for the whole gap: 1,023 adjusted rows of 1,280 at that level, the difference being
Northern Ireland's 257 months from its 2005 coverage floor.

The fact carries both columns regardless. The area profile screen compares an area against
its region and its country, and a national trend line is where seasonal adjustment does the
most work, so the columns serve the benchmark and never the subject. What would have
been wrong is building a screen that offers an adjusted series for a chosen district and
finds nulls, and the other two screens sit at district throughout, so neither can use these
columns at all.

Recorded because it is not derivable from the model. Both columns are nullable in the
declared table like every other measure, and nothing in the schema says the nulls are
structural.

### 41. An interior gap is a fault, a series short at either end is not

The index publishes one row per geography per month over the period that geography existed.
Two different things make a series shorter than its nation's coverage floor implies, and
only one of them is a defect.

An authority created in 2020 has no rows before 2020, and an abolished one has none after
it. Local government reorganisation is a fact about the country, and Decision 14 already
records the Silver notebook measuring geography coverage without asserting on it for
that reason. A month missing between a geography's own first and last month is a different
thing. Nothing in how the file is published produces one, so it means the release lost a
row.

The transform aborts on an interior gap and the notebook measures the rest, recording how
many geographies carry every month from their coverage floor to the newest month in the
release. That was 405 of 405 on the May 2026 file. A boundary change moves the number
without stopping anything; a dropped month stops the load.

The rent series needed neither. ONS writes a row for every area and every month and marks
the unpublished ones, so a missing row is impossible there and the guard in Decision 22
that catches an unpublished month inside the series already covers it. The two sources
differ in whether an absent value is an absent row, which is why one needs a contiguity
check and the other does not.

The distinction took a wrong answer first. The initial proposal was to measure both cases,
on the reasoning that reorganisation should not abort a load. True of a series that starts
late or ends early, and it says nothing about a hole in the middle. Treating them alike
would have left the only case with no legitimate cause unguarded, which is the opposite of
what the reasoning was for.

### 42. A yield is computable for 316 districts, and the shortfall is all structural

The yield map needs a district carrying both a price index and a rent. 316 of 361 do.

The 45 that do not are three populations and no accidents. Scotland's 32 districts and
Northern Ireland's 11 are published on broad rental market areas, which conform to nothing
below nation and pair with no price. The Isles of Scilly carry postcodes and no price. The
City of London carries a price and no rent, which Decision 18 already records from the
Silver side as a known join asymmetry. The area dimension flags all four cases through
`has_price_index` and `has_rent_index`, so the shortfall is derivable from the model
without consulting the sources.

Two of the three composites carry a rent. England and Wales does not, and the dimension
records that as `has_rent_index` false, so the fact and the dimension agree. A United
Kingdom or Great Britain yield is computable at composite level and an England and Wales
one is not.

Loaded to confirm it: 147,453 index rows across 405 areas and 49,248 rent rows across 357,
every key resolving in both the area dimension and the calendar.


---

---

## Bugs found and fixed

### Base URL hardcoded despite the parameter being defined

**Discovered:** during ONS source onboarding, after I modified the ONS URL in the watermark for a new monthly release.

**Symptoms:**
- Pipeline reported "Succeeded" on every run
- Downstream `openpyxl.load_workbook()` failed with `BadZipFile: File is not a zip file`
- A hex dump showed the "XLSX" file was HTML beginning `<!DOCTYPE html>`
- The HTML came from `bankofengland.co.uk`, not `ons.gov.uk`, so the pipeline was fetching from the wrong host entirely

**Root cause:** the HTTP datasets had parameters defined (`p_base_url`, `p_relative_url`) but their Base URL fields were hardcoded instead of reading `@dataset().p_base_url`. For five sources this was undetectable because the hardcoded value happened to match the watermark value. When ONS's URL was updated in the watermark for the March release, the dataset carried on using its stale hardcoded URL. That URL coincidentally pointed at BoE's host, which returned 200 OK with a homepage for the bad request path, so bytes transferred and ADF reported success.

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

**Root cause:** the installed Databricks SDK owns the `databricks` package name on a cold interpreter start, so a top-level folder of the same name is shadowed and never reaches the import path. A second constraint sat underneath it: Databricks Runtime 16.0 and above cannot import a notebook as a Python module at all, so importable library code has to be a plain workspace file (`.py` with no `# Databricks notebook source` header), created via Add → File, not Add → Notebook. Committed through Git, a `.py` without the header lands as a File; with the header it becomes a notebook.

**Fix:** renamed the folder to `databricks_src`, and kept importable library code as `.py` workspace Files while runnable notebooks stay in source format. One representation serves both: a source-format notebook renders as a notebook in the workspace but commits as a clean `.py` diff. Reverting the rename once appeared to work, but only because stale `sys.modules` state from earlier path edits in the same session masked the failure, so the real check is a fresh interpreter.

**Lesson:** verify any import fix on restarted compute before trusting it. The `__init__.py` files I first suspected were a red herring: implicit namespace packages resolve fine on Databricks Runtime, and the `wsfs ... Cannot find child __init__.pyc` messages are the FUSE layer logging the import system's file probes, not the failure itself.

### External Volumes rooted at the wrong depth

**Discovered:** the first end-to-end run of the BoE Silver notebook failed with a 404 whose URL contained a doubled segment, `boe/base_rate/base_rate/`. A Volume path resolves as the Volume's storage location plus the relative path the notebook appends, so the doubling identified the fault from the error alone: the Volume was rooted at the dataset folder (`/boe/base_rate/`) instead of the source root (`/boe/`).

**Audit:** I checked all six Volumes against both the Bronze layout table in this document and a direct storage listing (`dbutils.fs.ls` through the external location, since a Volume under audit cannot be trusted to list itself). `ppd` and `doogal` were correct; `ons` and `police` were rooted at their dataset folders, the same class of fault as `boe`; `hpi` pointed at `uk_hpi/`, which matched the deployed storage but exposed that the data itself had landed outside the taxonomy back in Phase 1 (a flat `uk_hpi/` while the sibling source `ppd` nests under `land_registry/`).

**Fix (metadata class: boe, ons, police):** the Volumes were dropped and recreated at their source roots. An external Volume's location cannot be altered in place, so drop-and-recreate is the mechanism, and it touches metadata only.

**Fix (data class: hpi):** corrected at the data layer instead, the same way `doogal` had been handled. The watermark sink folder moved to `land_registry/hpi`, the rotating URL was refreshed, the master pipeline re-ran to land the file at the new path, and the stale `uk_hpi/` was deleted only after verifying the new file (the Phase 1 lesson: Succeeded is not the same as the right bytes). The Volume was then recreated at `land_registry/hpi/`.

**Script hygiene:** `02_create_bronze_volumes.py` was corrected in three places, and a stray one-time `DROP VOLUME ... doogal` repair line was removed. A lone `DROP` in a committed bootstrap script silently destroys and recreates one of six Volumes on every run, and one-off repairs belong in the runbook or a serverless session. The same reasoning removed the one-time `SHOW SCHEMAS` and `DROP CATALOG ..._ws CASCADE` cells from `01_create_schemas.py`, where a bare `DROP` would abort a fresh rebuild and a new workspace's default catalog carries a different name anyway. Two verification changes also came out of the audit: it moved to an `information_schema.volumes` query selecting `storage_location`, because `SHOW VOLUMES` lists names only and a wrong-rooted Volume is invisible in a name listing; and `02_create_bronze_volumes.py` now closes with a `dbutils.fs.ls` on the BoE Volume path, the same path the Silver notebook reads, so a mis-rooted Volume fails in the bootstrap script, not downstream.

**Lessons:**

- `CREATE ... IF NOT EXISTS` is safe to re-run but can never repair drift, since create-if-absent says nothing about desired state. Relocating a Volume requires an explicit `DROP`; the corrected script on its own is inert against a live wrong Volume. This is part of the motivation for the Phase 4 Terraform work, which reconciles state instead of asserting absence.
- Verify Volumes against storage listings, not their own definitions. Script, documentation, and deployment can each be wrong independently: here the documentation was right and the deployment wrong for boe, ons, and police, while the deployment was right and the data wrong for hpi.
- Read failing URLs literally. The doubled path segment named the fault before any diagnostic ran.
- A wrong-rooted Volume keeps working until a path convention exposes it, so fix it while nothing downstream holds lineage against it.

### Bronze schema created with a managed location it should never have had

**Discovered:** while converting the setup notebooks from `.ipynb` to `.py` source format. `01_create_schemas` had given `uk_property_intel.bronze` a `MANAGED LOCATION` at the bronze container root, which contradicts the design recorded elsewhere in this document: bronze holds External Volumes only. `DESCRIBE SCHEMA EXTENDED uk_property_intel.bronze` confirmed it live, reporting the Root Location as the bronze container.

**Why it mattered:** a managed location is the default storage path for managed Delta tables and managed volumes. Nothing managed had been created in `bronze`, so nothing had gone wrong yet, but any managed object created there would have written Delta files into the container that holds raw ADF output.

**Why it went unnoticed:** `CREATE SCHEMA IF NOT EXISTS` skips the whole statement, clauses included, when the schema already exists. The `MANAGED LOCATION` clause applied once at creation and was invisible on every re-run afterwards. This is the same masking mechanism as the wrong-rooted Volumes, one level up the hierarchy.

**Documented behaviour against observed:** Databricks documents managed storage locations as unable to overlap external tables or external volumes. All six Bronze External Volumes were nevertheless created beneath the managed bronze root without error. I tested this directly: a throwaway schema with a managed location at an unused path accepted an external volume created inside that path. The documented rule did not block an external volume created under an existing managed location. I record this as an observation with the probe described, not as a claim about Unity Catalog internals; the fix does not rest on it, since the raw-container argument stands on its own.

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

**Lesson:** Phase 1 established that a success flag confirms bytes moved, not that the right bytes moved. This is the same failure one level up: the right bytes moved, and they were old. Neither the run status nor the filename could tell the difference, because the filename is metadata the pipeline assigned, not a property of the content. The Silver notebook now prints the newest month found in the data beside the filename it read, so a stalled rotation is visible on every run, not on inspection.

**Related detail:** the landed object is lower-cased (`uk-hpi-full-file-2026-05.csv`) while the source URL is mixed-case, so vintage selection matches case-insensitively. Selection is by the date token in the filename, not by `modificationTime`, because a re-fetch of an older release would reorder by timestamp, while the filename token is the file's last data month.

### Two guards covering one column, and the less useful one won

**Discovered:** by a unit test, before the HPI transform ran on real data. A test asserting that an unparseable date fails with a message naming the expected format instead received a message reporting that the `date` column had lost a value during casting.

**Root cause:** the transform ran its cast-preservation check before its key-presence check, and the cast check covered every column including `date`. An unparseable date is a populated string that `try_to_date` turns to null, so both guards were correct about the condition and the first one reached produced the message. What it produced, `date: (1, 0)`, is true and close to useless for diagnosis.

**Fix:** excluded key columns from the cast-preservation check. This is not merely reordering. The key guard is strictly stronger on `date`, because it fails on any null date whatever the cause, while the cast check only detects populated-then-null. Covering the column in both places added no coverage and cost the better error message.

**Lessons:**

- Overlapping validation is not free. When two guards can fire on the same condition, whichever runs first defines the diagnosis, so the question is not whether every failure is caught but which guard owns each failure. That is worth deciding deliberately, and not inheriting from the order the checks happen to sit in.
- I changed the transform, not the test's expectation. The test was asserting the behaviour I wanted; the code was the part that was wrong. Adjusting the assertion would have made the suite green and left the worse error message in place, which is the failure mode a test suite is supposed to prevent.

### A storage timestamp read as a fetch time

**Discovered:** auditing the PPD Bronze layout before writing the Silver transform. The monthly delta file was stamped `2026-04` in its landing path and its blob `modificationTime` read 14 May 2026, but the newest transfer date inside it was 27 February 2026. A file fetched in May should have carried the release published on 30 April.

**First hypothesis, wrong:** that the S3 website endpoint in the watermark had stopped receiving updates, since the download page switched to a different published host in February 2026. A HEAD request against both hosts returned identical `Last-Modified` and `Content-Length`, so the older name is an alias for the same object, not a frozen mirror. The URL was never the problem.

**Root cause:** `modificationTime` records when bytes were last written to that path. It says nothing about when they were fetched from source. The `raw` to `bronze` container restructure earlier in Phase 2 rewrote the timestamp on every object without re-fetching any of them. The content was the release published on 27 March 2026, carrying February registrations, and the May timestamp described the move, not the data.

**Corroboration:** `pp-2026.csv` was 17.2 MB in Bronze against 41.8 MB live at the same URL. That is independent of the transfer-date reasoning and settled it without argument.

**Fix:** re-ran the master pipeline against the documented host. The reload landed the July 2026 release, and the row count moved from 31,192,683 to 31,430,611.

**Lesson:** this inverts the conclusion originally recorded against the BoE source, which was that byte-identical refreshes make `modificationTime` the only usable signal. Here `modificationTime` was the misleading one and the content was honest. Neither is reliable alone, because a storage timestamp is a property of the path while freshness is a property of the bytes.

The resolution came later, with the audit work. Two signals get recorded per source and they answer different questions. The artefact signal is the file inventory the notebook already reads, its names, count, and total size, which changes when the bytes change and needs no per-source logic. The content signal is the newest date the data carries, which is a different expression for each source. A stale re-fetch moves neither; a publisher restating without extending moves the first and not the second. `modificationTime` is used for neither, because it describes when a path was written.

---

## Repository and Git workflow

The repository follows a **trunk-based workflow** with short-lived feature branches:

- One branch per logical unit of work (e.g. `phase2/setup-catalog-schemas`, `phase2/boe-silver`).
- All changes merge to `main` via pull request; `main` is branch-protected against direct pushes.
- ADF Studio is Git-integrated; pipeline JSON commits go to feature branches via the ADF UI, then merge to `main` via PR. Publishing from ADF promotes the live factory.
- Databricks notebooks are managed via Databricks Git folders linked to this repo, committed from the workspace UI on the same feature branches.

Both ADF and Databricks operate on the same Git branches as local development. The repo on `main` always reflects the live state of every component.

### Branch lifecycle

Feature branches are named `phase<N>/<task>` (e.g. `phase2/boe-silver`), reflecting the project's phase structure. Branches are short-lived, typically merged and deleted within days of creation. The git log therefore reads as a project timeline, not a long-running development trunk.

### Historical note

During Phase 1, ADF used the platform's default `adf-dev` long-lived branch with periodic syncs to `main`. This was migrated to trunk-based feature branches at the start of Phase 2; `adf-dev` was merged to `main` and retired. ADF still operates on whichever feature branch is current, just selected per-task.

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

### Complete (Silver transforms)

- Silver-layer ingestion notebooks, one per source. All six are complete: transform modules, notebooks, and unit tests are committed, and all six tables are live. BoE, HPI, and ONS are validated against externally published figures. PPD reconciles three ways on its own row counts, by category split, by property type and tenure, and by transfer year. Doogal reconciles against PPD on postcode coverage and against HPI on district code. Police reconciles on vintage exclusivity, no month drawing from two archives, and on force coverage by year.

### Phase 2.8 — pipeline audit and freshness

Two Phase 2 items ran ahead of Gold. The audit table because six writers exist now and Gold adds more, so the schema is decided once. The freshness assertion because that failure has already happened, and the planned watermark automation would not have caught the one source whose URL cannot be pattern-matched.

`uk_property_intel.quality` holds `pipeline_run` and `pipeline_metric`, created by `databricks_src/setup/03_create_quality_tables.py` from DDL generated by the writer, so the table definition and the write schema cannot drift. `databricks_src/quality/audit/writer.py` carries the writer and a 29-entry metric registry. All six Silver notebooks record through it, and the folder sits beside `framework/` and `rules/`, not inside them, because it writes what a notebook hands it and evaluates nothing.

Everything the notebooks previously printed and discarded is now recorded: the file inventory, row counts in and out, the newest date the content carries, and per-source measures. Three sources record entity coverage against a rolling twelve-month window, which separates a permanently departed reporter from one that stopped recently. On the Police load that distinguished Greater Manchester and British Transport Police, both long gone and correctly excluded from the denominator, from Gloucestershire, which stopped in January 2026 and is named.

### Carried into later phases

- The PPD reconcile path described in Decision 16 is designed but not built. Silver is a full rebuild from the current yearly files, which is complete, not provisional, because those files are restated on every release.
- The PPD delta merge is cancelled, not deferred. It was scoped when the change-only file was assumed to carry data the yearly files did not. Both are published from the same release and describe the same state, so a merge would add change-data-capture machinery without adding a row. The change-only file survives as an orchestrator download list, in Phase 4.
- The quality-rules framework, watermark automation, and the quarantine table follow Gold. Gold produces a second class of check, covering join integrity, referential coverage, and aggregate reconciliation, that looks nothing like the Silver row checks. Committing to a config schema before seeing it risks a shape Gold has to work around.
- Every freshness bound is unset. The mechanism is built and the values are recorded on every run, but a bound needs to be read off observations spanning a release boundary, and one run per source cannot say where in its cycle a source was measured.
- The eight per-archive Police measures and both crime vocabularies are written by code that has not yet executed against a real load. Staging was complete when the audit wiring landed, so the loop was skipped. They first record on the next monthly archive.
- The audit tables have no way to mark a run as synthetic, so a probe has to claim to be one of the six real sources and be deleted afterwards. Once ADF orchestrates the notebooks, its pipeline run id lands in a `parent_run_id` column and the dashboard's success rate is computed over scheduled runs alone, which keeps development re-runs in the history without distorting the headline.

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
   - Quality metrics logged to `quality.pipeline_run` and `quality.pipeline_metric` on every run

3. **Police.uk overlapping-snapshot deduplication**: built, and not as planned
   - The window function above was replaced by selection from the ZIP central directories, which resolves the same thing without decompressing the losing copies or shuffling. See Decision 24.
   - The cross-snapshot consistency check is no longer a by-product of loading and is not built. A byte-level approximation runs against the archive inventory and already found British Transport Police restating the whole of 2023 downward by roughly 45%.

4. **Automated watermark updates from Databricks**
   - Replace manual JSON editing
   - Notebook programmatically updates the watermark on successful runs
   - Migration of the watermark from JSON-in-ADLS to a Delta table
   - Raised in priority by the stale-release bug above: the three rotating-URL sources fail silently when their URL is not maintained

5. **pytest + chispa test suite**
   - SparkSession fixture in `tests/conftest.py`. It reuses the cluster's session when one already exists, because builder options are ignored at that point and stopping the session would detach the notebook.
   - Per-source transform tests in `tests/test_silver_transforms/`. BoE is covered by 26 tests: the data-quality guard, an exact end-to-end SCD2 scenario, the multi-row invariants Delta CHECK constraints cannot express (exactly one current row, contiguous non-overlapping intervals), and edge cases including sixteenth-precision decimals and null-rate gaps. HPI is covered by 35 tests: the four integrity guards, the coverage floor at every nation and composite boundary, typing across both published decimal formats and the volume decimal-point case, null preservation, and an end-to-end projection. PPD is covered by 47 tests: the positional read contract, the code sets at every published value, the one-year-per-file rule the partition key rests on, key uniqueness across files, and the cases where a fault would change no row count and raise nothing, such as a reordered projection or a transfer date carrying a real time component. Doogal is covered by 53 tests: the column contract that proves every one of the 60 source columns is either mapped or explicitly dropped, the in-use equivalence checked on a column that does not survive the projection, the code sets at every published value, null geography admitted only in the BF postcode area, and the fabricated zero coordinate in both directions. ONS is covered by 45 tests: the marker positions in all three structural cases, the two-branch Northern Ireland test that either half alone fails, and the conversion guards. Police is covered by 103 tests, and its split is different from the others because most of the logic is not Spark: 12 cover the archive selection rule over member names alone, the rest cover the predicate registry, the coordinate box in both directions, and the duplicate measurement helpers. That is 309 across the six sources.
   - The audit writer is covered by a further 46 in `tests/test_quality_audit/`, and it is the only suite that needs no SparkSession: the metric registry, the generated DDL against the write schema, the routing of a value to its column, the freshness verdict, and the failure path. 366 tests in total, all passing on DBR 17.3.
   - Quality framework tests in `tests/test_quality_framework/`
   - Runs locally with `pytest` against the versions pinned in `requirements-dev.txt`, and on the cluster through `tests/run_tests.py`. CI integration deferred to Phase 4.
   - Test-only dependencies stay notebook-scoped or local, never on the cluster spec (see Decision 13)

6. **Initial Silver → Gold design**, settled in Phase 3.1, and the sketch held
   - Multi-source joins on postcode: the transaction join resolves 99.84% of rows and plans as a broadcast at the default threshold, so the one large join in the layer is not a shuffle
   - Star-schema fact + dimension tables (Kimball): confirmed, with two geography dimensions conforming through `district_code`, not one. See Decision 30
   - Enrichment: the yield and the own-versus-rent comparison are computed downstream, not stored. No fact carries a yield column, for the reason in Decision 30. The income-based affordability ratio is out of scope, since the one income column available was dropped at Silver under Decision 18

### Planned (Gold scope)

Four screens, and the model exists to serve them.

1. **Area profile.** One page for a chosen area: what homes sold for and how that moved, what they rent for, the yield that implies, the crime picture, and how each compares against its region and the country.
2. **Own versus rent.** The monthly cost of a mortgage on a typical local home at the base rate of the day, against the monthly rent for the same area. The base rate stops being a decorative line here, because the same price at 0.1% and at 5.25% is a different product.
3. **Yield map with a crime overlay.** Rent over price by area, with crime as a second layer, so the trade-off between the two is visible instead of argued about.
4. **What actually sells here.** Flats against houses, new build against existing, freehold against leasehold, full market value against everything else. An average price hides all of it, and an area up 8% may only have sold more detached houses.

Thirteen tables: four dimensions and nine facts. Measures are curated to what those four screens ask. The house price index publishes 56 columns and the rent series 40; Gold carries eleven and twelve. The rest stay in Silver, where they remain queryable, on the principle that Silver filters on what is reliable and Gold on what a question needs.

Income-based affordability is out of scope. The one income column available was a 2020 model-based estimate at MSOA level and was dropped at Silver for the reasons in Decision 18, so affordability is expressed as own-versus-rent instead, which is the more answerable question.

Coverage is uneven, and the gaps stay visible instead of being filled in. 316 districts carry price, rent and crime together, which is the population where a yield and a crime overlay can both be drawn. City of London has price and postcodes but no rent; Isles of Scilly has postcodes only. Scotland's 32 council areas and Northern Ireland's 11 districts have a price index and postcodes but no matching rent, because the rent series publishes them on broad rental market areas that conform to nothing below nation. Crime stops at England and Wales. Further sources may be added later, since a new source is a config entry.

### Planned order of delivery

1. ✅ Databricks workspace + cluster + Unity Catalog + storage layer + Bronze Volumes
2. ✅ First Silver notebook against the simplest source (BoE): minimal schema complexity
3. ✅ HPI (single cumulative CSV, wide monthly panel, per-nation coverage floor)
4. ✅ PPD (31.4M rows, partitioned on transfer year, TUID uniqueness asserted)
5. ✅ Doogal (ZIP unzip, 2.71M postcodes, ONS spine only)
6. ✅ ONS (XLSX converted, not read in place, 49,266 rows)
7. ✅ Police.uk (most complex: seven archives, 8,288 selected files, 96.1M rows, no natural key)
8. Gold-layer star schema
9. Quality-rules framework, extracted from patterns observed during (2)–(8)
10. Watermark automation

---

*Design document status: Phase 2 complete, Phase 3 in progress. All six Silver tables built, unit-tested, and confirmed on the cluster, with the pipeline audit tables recording every run. The Gold model is designed, its thirteen tables are declared and created, and all four dimensions are loaded and verified: 19,723 calendar days, 432 published areas, 36,778 small areas and 16 crime types. Two of the nine facts are loaded, the house price index at 147,453 rows and the private rent series at 49,248. Decisions 30 to 32 were queued on 08-08-2026 and rewritten before entry, against measurements taken in Phase 3.1 that contradicted parts of the queued text; 33 to 36 came out of loading the dimensions, and 37 to 42 out of building the first two facts. Last updated 18-08-2026.*
