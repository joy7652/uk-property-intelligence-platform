# UK Property Market Intelligence Platform
Built by Md. Rais Al Kabir Joy · [GitHub](https://github.com/joy7652)

An Azure data platform built around HM Land Registry's 31.4M residential transactions since 1995, joined with five more official UK datasets covering house prices, private rents, postcodes, the Bank of England base rate and street-level crime. The pipelines run off a single JSON config file, so adding a source means editing config, not writing code. Loads are incremental from a per-source watermark. Every file is validated against its expected format before parsing, because a pipeline reporting success only proves bytes moved, and all data access is governed through Unity Catalog. Later phases add statistical anomaly detection and BI dashboards.

> **Status:** Phase 1 complete: Bronze ingestion for all six sources. Phase 2 complete: the Databricks workspace, Unity Catalog, and medallion storage layer are provisioned, and all six Silver tables are live, unit-tested, and committed. The Bank of England base rate, the UK House Price Index, Land Registry Price Paid Data, the UK postcode lookup, ONS private rents, and Police.uk street-level crime. Phase 3 is complete: the Gold star schema is designed, its thirteen tables are created, all four dimensions and all nine facts are loaded and verified on the cluster, 36,119,680 fact rows in total, and a transaction-derived price series reconciles against the published index at a count correlation of 0.9998. Phase 4 is complete: continuous integration, the quality layer, watermark automation, orchestration and the failure cascade, and a schema the watermark is checked against before the pipeline reads it. A source that fails at ingestion skips exactly the tables built on it. Phase 5 covers consumption, Synapse Serverless and the dashboards.

**Highlights**

- 6 official UK datasets: Land Registry PPD and HPI, ONS rents, BoE base rate, ONS postcodes (via Doogal), Police.uk crime
- 31.4M property transactions since 1995 (July 2026 release)
- 2.71M postcodes, live and terminated, so transactions back to 1995 still resolve
- Private rents for 357 UK geographies monthly since 2015, reconciling to the published national figure
- 96.1M crime records across 187 months, deduplicated from seven overlapping archives
- 36.1M Gold fact rows across 9 facts and 4 dimensions, every key conforming and every dropped row counted by cause
- Config-driven ingestion: a new source is 1 JSON block in the watermark, not a new pipeline
- Incremental by per-source watermark, with 2 reusable load patterns covering all 6 sources
- Magic-byte validation and a quality layer recording every run, metric and rule result, because a success flag only means bytes moved
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

Property data shows up across finance, consulting, and the public sector, and the sources refresh monthly, so this runs as a live pipeline. The data is messy enough to make the transformations real, and the config-driven design would ingest any other multi-source dataset without code changes.

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
   plus ADLS Gen2 quality/  (run history, metrics, rule results)
              ↓
Azure Synapse Serverless SQL  (planned)
              ↓
Fabric / Power BI  (planned)
```

### Design principles

- Adding a source means appending a JSON block to the watermark, not writing a new pipeline.
- Two ingestion patterns (`yearly_stepped` and `single_file`) cover every source. Each source declares which one it uses.
- After the first full load, each source tracks its own state in the watermark, so later runs fetch only what's new. What counts as "new" is source-specific (a change-only delta for PPD, a fresh rolling snapshot for Police.uk), so incremental loading routes by type. Assuming one mechanism would break the other.
- A pipeline reporting success only tells me bytes moved, not that the right bytes moved. Binary files are checked against their expected magic bytes before any Silver-layer parsing.
- HTTP linked services are host-agnostic and take their base URL per request via `@{linkedService().p_base_url}`, instead of one linked service per host.
- Silver filters on reliability; Gold filters on the question being asked. Data that is measured and clean stays in Silver even when nothing in the project joins it yet.

---

## Data sources

Six official UK government and regulated open datasets:

| # | Source | Format | Pattern | Step | Update cadence |
|---|--------|--------|---------|------|----------------|
| 1 | HM Land Registry — Price Paid Data (PPD) | CSV, per year | `yearly_stepped` | 1 year | Monthly change-only delta |
| 2 | HM Land Registry — UK House Price Index (HPI) | CSV, cumulative | `single_file` | — | Monthly |
| 3 | Doogal — UK Postcode Lookup (ONSPD mirror) | ZIP | `single_file` | — | Quarterly |
| 4 | Bank of England — Official Bank Rate | XLS | `single_file` | — | Monthly |
| 5 | ONS — Price Index of Private Rents | XLSX | `single_file` | — | Monthly (URL not derivable) |
| 6 | UK Police — Street-level Crime | ZIP (1.4 to 1.7 GB) | `yearly_stepped` | 2 years | Monthly snapshot, 3-year window since 2017 |

### Why these sources

**Price Paid Data** is the authoritative record of UK residential transactions since 1995, 31.4M records as of the July 2026 release, and the backbone of any property analysis. **HPI** gives official price indices validated by the same department, which makes it a useful cross-check for any metric I derive myself. **Postcode lookups** handle geocoding and regional aggregation. **Bank of England rates** and **ONS rents** supply the macro picture; affordability needs both the price and the cost of money. **Police crime data** adds the classic property-investment overlay of safety against price growth, and it joins cleanly on postcode.

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

- **`yearly_stepped`** — iterate across years with a configurable step, one file per step. Incremental work dispatches to one of two children via `incremental_type`: `static_url` (PPD's change-only update file) or `templated_latest` (Police.uk's monthly-rotating snapshot URL).
- **`single_file`** — one URL fetches one file per refresh. Used by HPI, Doogal, BoE, and ONS.

`yearly_stepped` grew out of an earlier `yearly_range` pattern. I added the step parameter only once Police.uk's 2-year cadence gave me a real second use for it.

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

The watermark holds per-source state (last ingestion date, URL parameters, load pattern) in its own ADLS container, reached through a Unity Catalog volume at `/Volumes/uk_property_intel/configs/watermark/watermark.json`. It is state rather than source, so it lives beside the data and not in the repository. Three decisions shaped it:

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
- **Quality** — run history, pipeline metrics, and rule results under `uk_property_intel.quality`, in the `quality` container. No quarantine table; decision 22 records why.

Each layer maps one-to-one to a Blob container and a Unity Catalog schema. Silver, Gold, and Quality use schema-level managed locations; Bronze uses External Volumes pointing at the bronze container (see Decision 10). Keeping the physical and logical layouts aligned means cost attribution, lifecycle policy, and RBAC all scope per layer, and you can read the medallion structure straight off the storage account.

The Volume namespace is flat, one Volume per source, while the storage layout underneath is not: four Volumes root at the container root (`boe/`, `doogal/`, `ons/`, `police/`) and the two Land Registry sources root under the publisher folder (`land_registry/hpi/`, `land_registry/ppd/`). A Volume roots at its source folder and never at a dataset folder beneath it, so notebooks append any dataset segment themselves.

Bronze is complete. Silver is in active development; Gold and Quality follow.

### 6. Unity Catalog over hive_metastore

I adopted Unity Catalog from day one of Phase 2, over the legacy hive_metastore that older Databricks projects (and most tutorials) still use. Reasoning:

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
- Turning file events on would require granting the UAMI `Storage Account Contributor` (control plane), `EventGrid EventSubscription Contributor`, and `Storage Queue Data Contributor`, a much wider scope than the `Storage Blob Data Contributor` (data plane only) the actual data path needs.
- Least privilege is the right call here, and the one I'd make in any regulated environment anyway.

### 10. Bronze exposed as UC Volumes, not Delta tables

Most Databricks tutorials treat Bronze as a Delta-table layer: copy raw files into Delta with some added metadata columns, then have Silver read from those tables. This project does it differently. Bronze is exposed through Unity Catalog **External Volumes** pointing at the raw files in the `bronze` container, and the Silver notebooks read straight from those Volume paths.

At this scale (sub-GB per source, durable raw files, no ad-hoc SQL on the raw data), copying Bronze into Delta would let me put a "Bronze layer" label on the diagram and not much else; it wouldn't do anything the raw files don't already do. Exposing Bronze as Volumes is worth it for a different reason: it adds UC governance, lineage, discoverability, and stable paths without re-writing the data first.

See DESIGN.md Decision 11 for the conditions that would change this call.

### 11. BoE base rate as an event-grain SCD2

The first Silver table models the Bank of England policy rate as a Type 2 slowly-changing dimension at event grain: one row per rate level with a validity interval (`effective_date`, `expiry_date`, `is_current`), a rate and its regime label, and lineage columns. The daily source series collapses to change events only, so a day whose rate matches the day before is not a row. The BoE has renamed the policy rate five times since 1973, so the five era-specific rate columns coalesce into one `rate_pct` and a `rate_type`, and a rename that leaves the rate unchanged is not treated as a change. A fail-loud data-quality guard aborts the run if any day carries conflicting rate values across those columns. Coalescing one of them silently would hide the conflict. Daily-grain join surfaces, if a consumer needs them, are Gold's job.

See the BoE base-rate decision in DESIGN.md for the full rationale.

### 12. Library dependency scoping

Pipeline runtime dependencies go on the cluster spec, version-pinned and committed in `cluster_definition.json`. The spark-excel plugin is the only one so far, and JVM libraries leave no choice: they cannot be installed notebook-scoped, and serverless compute cannot load them at all, which is why Excel reads run on the Dedicated cluster. Test and development dependencies stay off the cluster entirely. chispa is installed notebook-scoped in the test runner at a pinned version, and the same pin sits in `requirements-dev.txt` next to the PySpark and pytest versions the runtime already ships, so a local run matches the cluster.

The split is about blast radius: a cluster library installs for every workload attached to that cluster and cannot be uninstalled from inside a notebook, so an unnecessary one is a version conflict that costs a restart to resolve.

See DESIGN.md Decision 13 for the conditions that would change this.

### 13. HPI keeps measured data only, with a per-nation floor

The published HPI file carries a derived back-series to 1968, built from the historic path of the older ONS index, which sits before each nation's native Land Registry coverage. Silver keeps the measured era only: England and Wales from 1995, Scotland from 2004, Northern Ireland from 2005. A composite geography floors at the latest native start among the nations it spans, so United Kingdom rows start in 2005 and Great Britain rows in 2004. Without that rule a UK row for 1996 would be part measured and part derived, which is harder to reason about than either.

The cut is on reliability, not on joinability. PPD and Police.uk cover England and Wales only, so Scottish and Northern Irish rows join neither of them, and they stay anyway because they are measured data that other sources do join. Narrowing a table to what one question needs belongs in Gold; Silver's job is to be correct and reusable. The same reasoning keeps BoE rates from 1973 to 1995, which nothing in the project joins yet.

An area code with no floor mapped aborts the run. A geography whose measured start I haven't established cannot be filtered safely, and a null floor would silently drop every row belonging to it.

See DESIGN.md Decision 14 for the composite mapping and the full reasoning.

### 14. Typing under ANSI mode

Databricks Runtime 17.0 turns ANSI mode on by default, which changes what a failed cast does: it raises instead of returning null. On a 148,000-row, 54-column file, a single malformed cell would abort the run with an error naming neither the column nor the row.

Silver transforms cast with `try_cast`, which yields null on a malformed value whatever the ANSI setting, then compare non-null counts per column before and after typing. A value that was populated and is now null did not survive its cast, and the failure names the column. That converts an opaque runtime exception into a diagnosis, and keeps behaviour stable if the setting changes again.

Key columns are excluded from that comparison. A null date is already caught by a dedicated guard that reports the offending row, which covers more ground than a count check does.

### 15. PPD retention differs by file kind, and the changelog is data rather than a trigger

Land Registry publishes PPD in two forms that need different handling. The yearly files are state: `pp-2019.csv` is regenerated every month at a stable URL, and Land Registry states that the single and yearly files carry the most current data on every release. The monthly change-only file sits at a static URL that each release overwrites, so a release missed is a release lost.

Both are published from the same release and describe the same state, so the change-only file carries no data the yearly files lack. It is not retained: the orchestrator fetches it, selects the distinct transfer years it names, re-downloads those yearly files, and deletes it. One column is read. Nothing parses the operation type or compares anything, because a stale yearly file is fully described by the current one and the changelog only says which to fetch. A run that never fetched one cannot recover the years it named, and those wait for the annual reconcile; the state stays correct throughout, since the yearly files are authoritative whenever pulled.

Silver is rebuilt in full from the current yearly files and partitioned by transfer year. Every file holds exactly one transfer year, confirmed across all 32 files on two separate vintages, so one Bronze file maps to one Silver partition and a reconcile can overwrite a year with `replaceWhere`. TUID is unique across all 31.4M rows, so Silver asserts uniqueness. The two cost the same shuffle, but an assertion names the offending identifiers where a dedup silently discards rows.

An earlier sketch had Bronze read the change-only file, work out which yearly files it affected, and re-fetch those. That is the wrong shape whatever the file contains: a layer deciding what an earlier layer should ingest makes the medallion cyclic, which costs reproducibility and leaves lineage describing something other than dependency. The difference is not which component opens the file, it is that the decision originates in the orchestrator and never travels upward from a layer.

That principle went on to cancel the refresh itself. The changelog is read in Silver and Gold, so any refresh driven by it runs those layers, then Bronze, then those layers again. Putting the orchestrator in the middle relays the decision without reversing the arrow, and a cycle that fails part way leaves no layer holding a complete version of either the old state or the new one. The changelog itself is not cancelled, only the refresh it would have driven. It is downloaded and appended every month, which is how a correction reaches the warehouse between one annual refresh and the next, and the load already places a restated 2003 row in the 2003 partition because that is what the row says. What it cannot do is decide which yearly files to re-download, because nothing knows which years it names without opening it, and the only cycle-free alternative is re-fetching every yearly file every month, which is a full load monthly and the end of incremental loading.

So there are two paths and they never interleave: a monthly append, and one annual refresh in August that takes the full-load branch and skips the changelog entirely, since the yearly files already carry everything it would have said. Miss a month and that month's changes wait for August. That is a freshness cost and never a correctness one.

See DESIGN.md Decision 16 for the reconcile design and the conditions that would change this.

### 16. Doogal keeps the ONS spine and drops the publisher's additions

The postcode file is two datasets under one header. Most of it mirrors the ONS Postcode Directory: versioned, quarterly, documented, reproducible from the ONS release. The rest is the publisher's own enrichment, computed from unnamed inputs at unstated dates. Silver keeps the spine and drops eighteen columns.

Five of those restate something already kept, including an in-use flag that holds exactly when the termination date is null, across all 2.7M rows. Two are comma-separated lists, which are a different table at this grain rather than a column. Nine are publisher-derived with no stated method or vintage, and three of those are worse than merely undated: population and household counts are 2011 census figures and average income is a 2020 model estimate, all three sitting in a row that refreshes quarterly with nothing marking them as on a different clock. The output area and MSOA codes are kept, so a Gold model that wants those measures can join the published source at its real grain.

The deprivation columns are the clearest case. One column holds four separate national indices at four vintages on four scales, England ranking to 33,755 and Northern Ireland to 890, with no qualifier saying which is which. A consumer reading it as one series gets a wrong answer that looks right. The decile column is worse, because 1 to 10 everywhere hides the incompatibility instead of hinting at it.

Terminated postcodes are kept. They are 915,354 rows, a third of the file, and the join proves the point: 1,335,001 of 1,336,342 distinct Price Paid postcodes resolve, and 14,395 of those matches are postcodes that no longer exist.

Two source behaviours needed handling rather than dropping. The British Forces Post Office area is non-geographic, so its 48 rows carry coordinates at overseas bases and no UK geography at all; they are measured data and are kept, with a guard confining null geography to that postcode area so an unmapped geography arriving later cannot hide among them. And where ONS publishes no grid reference, the file leaves easting and northing blank but writes zero into latitude and longitude. Zero is a valid coordinate in the Atlantic, so it passes every range check; Silver treats the grid columns as the honest signal and nulls the fabricated pair on all 11,071 rows.

See DESIGN.md Decision 18 for the full column accounting.

### 17. Archives are decompressed at read time, not at ingest

Spark cannot read inside a ZIP, so something has to unpack it. Bronze keeps the archive exactly as published and the Silver notebook extracts to cluster-local disk on each run.

Decompression changes transport encoding rather than content, so unpacking at ingest would not have violated the rule that Bronze does no processing. What decides it is how many reads each ingest serves. Doogal publishes quarterly, is ingested quarterly, and is rebuilt quarterly, so unpacking once at ingest amortises across exactly one read and buys nothing, while costing 2.01 GB of stored output and the ability to check Bronze against the source byte for byte.

Police.uk makes the opposite case and still lands in the same place. Seven archives, each restating up to three years, all read far more often than they are ingested, and every one published with an MD5 that turns fidelity from something preserved into something proved. What settles it is that the deduplication in Decision 20 discards 40% of the files from their names alone, so expanding at ingest would write about 31,500 small files to storage in order never to read most of them. Police.uk extracts at Silver too, one archive at a time, because the full winning set expands to 19.2 GB against 4.4 GB for the largest single archive.

Reading from local disk ties these notebooks to a single-node cluster. Scaling out raises a file-not-found on the executors, so the constraint announces itself.

### 18. The ONS workbook is converted before Spark reads it

spark-excel reads the Bank of England workbook straight from Bronze. It cannot read the private rents workbook, for a reason worth stating plainly because nothing about it looks like a failure.

Read as string, spark-excel returns each cell's display format rather than its stored value. ONS holds these figures to six decimal places and displays them to one, so an index of 81.413747 arrives as 81.4. The row count is right, the column count is right, every cast succeeds. Five decimal places are gone and no guard would have caught it.

Read with an explicit numeric schema, the stored values come back, but the `[x]` and `[z]` markers ONS writes into its measure columns collapse to null. That merges "not available", "not applicable" and "failed to parse" into one state, and it removes the all-string frame the cast-preservation guard compares against.

So the Silver notebook converts the sheet to CSV on cluster-local disk with openpyxl, which returns stored values and real dates, and reads that with the same all-string pattern every other source uses. Bronze keeps the workbook as published. The conversion is cross-checked against the sheet's own declared dimension, so a reader that drops rows is caught.

The output reconciles to the ONS bulletin: £1,388 average UK monthly rent for June 2026, up 3.3%, and the monthly change column reproduces to six decimals when derived from consecutive index values. Neither check is possible against display-rounded data, which is what makes them worth running.

### 19. Duplicates in police street crime are counted, because the source has no key

Every other Silver table asserts a key and fails on a repeat. This one cannot, and the reason lies in how the source is published.

Crime ID is a one-way hash of the force's offence reference and is blank for anti-social behaviour, which is 31% of the table. Dates are truncated to year and month at anonymisation, and coordinates are snapped to shared map points. Two genuine burglaries on one street in one month therefore produce byte-identical rows, and police.uk separately state they suspect some forces of double counting anti-social behaviour. The two causes cannot be told apart from the row, so removing duplicates would delete real crimes.

Silver keeps them and measures the population instead: 11.4% of rows repeat another row exactly. The obvious objection is that this is an artefact of records with no location, and the split says otherwise, because under 1% of those extra rows carry no location at all. Crime ID is no fallback either: 8,654 identifiers recur monthly across the whole series, all Northern Ireland, covering 1.4M rows, so the hash is of a reused reference rather than of a crime.

The table carries the archive it came from as a column instead. Outcome state is only as settled as the snapshot that supplied the row, so a 2011 crime has years of settlement behind it and a recent one has none. Recording the vintage makes that lag derivable.

See DESIGN.md Decision 23.

### 20. Overlapping archives are resolved from file names, not from the loaded rows

Each police.uk archive restates up to three years, so the same month and force appear in several of them. The planned approach was a window function over the loaded rows, keeping the newest.

The whole key turns out to be in the path: the archive filename gives the snapshot, the inner path gives the month and the force. So the winner is decidable from the ZIP central directories, which cost no decompression at all. 13,887 files collapse to 8,288, removing 40% of the read before a byte is expanded, and no shuffle is spent. It also removes the need for a row key to tell copies apart, which matters given Decision 19, because there is no row key to be had.

The cost is that cross-snapshot disagreement stops being a by-product of loading. Byte sizes from the same directories give a cheap approximation, and it found British Transport Police restating the whole of 2023 downward by roughly 45% between two archives, which the loaded table alone would never have shown.

### 21. Validation folds into one pass where the table is large enough to notice

The other five sources declare one guard per rule, each an action over the frame. That is the right shape for a table of thousands or a few million rows. Police is 96.1M, where the same shape cost roughly seven full passes before anything was written and dominated the run.

Every rule there is a row predicate, so they fold. Each stays a named function with its constraint written out and its own evidence columns, but returns a predicate rather than raising, and one aggregate evaluates all of them together with eight measures and two vocabularies. A clean archive costs one pass; a broken one costs one short read per failing rule.

Two things improve beyond the speed. Every failing rule is reported in one pass, which matters when each retry costs a twenty-minute extraction. And the cast check gets sharper: comparing populated counts between two frames can only say a column lost values, while a predicate reports the string that failed.

The general rule, and the one worth carrying forward: read the sibling files, then decide. A convention written for nineteen thousand rows can be wrong for ninety-six million, and nothing about it announces that.

### 22. Aborting is the default, so a quarantine table needs a population to hold

The roadmap has carried a quarantine table since Phase 1, on the standard reasoning that bad rows go aside and good rows proceed. Building all six Silver transforms produced the opposite habit, and the habit is right.

Almost every guard in this platform is a contract check. An unrecognised crime type, a column set that no longer matches, a code outside its published set: each of those means the source changed shape, and loading the rows that happen to still parse would produce a table whose schema nobody understands any more. PPD states it plainly, that a new code is a source change rather than a row to skip. Quarantining a contract violation converts a loud stop into a quiet partial load.

That leaves the population a quarantine table would actually hold: rows that satisfy the contract and fail a plausibility bound. Across six sources and roughly 130M rows, the clearest instance is 24 rows where British Transport Police published Scottish stations with corrupted longitudes, and those are handled better by nulling the coordinate and counting it than by removing the crime.

So the table is deferred, and the decision that gates it is which checks are contract and which are value. If the value population stays this thin, the honest outcome is to record why the table was not built.

**Closed, not built.** The population never appeared. The rules engine this table was to sit beside was dropped after counting what there was to configure, and the last candidate that could have filled it was the anomaly detector, which measurement rejected: it would have flagged around 1,160 cells a run, and its flag rate rose with area size, meaning it read structure as fault. What is left is contract violations, which stop the load, and the occasional bad value handled in place. Neither needs a table, and recording why is what this decision asked for.


### 23. A failed run has to leave a row behind

The audit layer is two tables, not one. `pipeline_metric` holds one row per measured value; `pipeline_run` holds one row per notebook execution whatever the outcome.

A metrics-only table records nothing when a load fails, so the absence of rows for a source cannot be told apart from the notebook never having been run. That is the failure the freshness work exists to catch, and it would have been the case least visible in the record. The run row is inserted at start with status `started` and updated on completion, which also means a killed cluster leaves an open row rather than disappearing and raising the observed success rate.

Metrics buffer in the run object and flush once, on success or failure, so a load that aborts still records what it measured before it broke. The alternative, a Delta commit per metric, costs a transaction for every printed count.

Two rules follow from the tables being read by a dashboard later. Counts are stored with the base they are a share of, never as a pre-computed ratio, because counts and bases re-aggregate across runs and percentages do not. And metric names come from a registry in the writer, because a rename that nothing catches looks exactly like a series that was discontinued.

### 24. Freshness bounds are read from the data, not from a publisher's calendar

Bronze sat on a four-month-old Police archive until an inventory surfaced it. Nothing failed, because nothing was wrong with the file that was loaded. The gap is that no source asserted anything about how new its content was.

Each Silver notebook now records the newest date its content carries, and each source has a bound in days above which the load aborts. Every bound ships unset. A bound guessed from a publisher's stated release calendar is a guess about a cycle nobody here controls, and the first observations show why that matters: the healthy lag ranges from 8 days for the BoE workbook to 98 for the HPI release, and a single observation cannot say where in its cycle a source was measured.

Two sources needed a signal other than the obvious one. The BoE rate has held since December 2025, so the newest rate change is stale by design and asserting on it would fire every month while the pipeline is healthy; the newest day carrying any rate is the signal instead. ONS cannot be pattern-matched from its URL, so the landed filename records which release was asked for rather than which one was served, and the publication date parsed from the workbook's cover sheet is the only value in the file that can contradict the filename.

### 25. Ancestry is flattened into the area dimension

`dim_area` carries `region_code` and `nation_code` beside `parent_area_code`, so a row states every level it belongs to. A region figure then needs no recursive walk up the parent pointer.

The reason is arithmetic. Areas do not all reach the same depth: a district in England has a region and a nation, a district in Wales has only a nation, and a composite geography has neither. A region-level figure has to count every area belonging to that region whether or not anything sits below it, and a nation-level figure has to count the areas that reach no further. Walking the pointer answers the first and quietly loses the second.

`region_code` is null for the 65 districts outside England, 18% of the 361. That is structural, being every district in three of the four nations, so the column is explicit instead of the raggedness surfacing later as a failed drill-down.

Two check constraints keep it honest. A region code must match `^E12[0-9]{6}$`, and only a district or a region may carry one. The first is not defensive. A district's region is resolved by matching the postcode directory's region name against the price index's, and the index publishes `E12000005` as "West Midlands Region" precisely because it also publishes the metropolitan county `E11000005` as "West Midlands". A name match landing on the county is a wrong parent rather than a missing one, and the constraint is what separates the two.

### 26. A Gold run names the table it builds

The audit writer validates a run's name against a closed vocabulary. A Silver load names the Bronze source it read, which identifies it exactly. A Gold load reads four or five Silver tables at once, so no single source names it and the target table does.

The registry therefore holds two vocabularies, and each name carries the layer it belongs to. A dimension name used under the Silver layer, or a Bronze source name used under Gold, fails when the run object is constructed. A row written under a name no query filters on is a row no dashboard finds.

One run per Gold table. The dimension load writes four tables from one notebook, and a single run across all four would make its row count a sum of unrelated things. Four runs keep each count a real number and leave the metric `scope` field free for genuine subdivisions, which the load uses to record rows per area level and per boundary vintage.

---

### 27. An interior gap is a fault, a short series is a fact about the country

The house price index publishes one row per geography per month over the period that
geography existed. Two different things can make a series shorter than its nation's
coverage floor implies, and only one of them is a defect.

An authority created in 2020 has no rows before 2020, and an authority abolished in 2023
has none after it. Local government reorganisation is a fact about the country, and the
Silver notebook already refuses to assert on geography coverage for that reason. A month
missing between a geography's own first and last month is different. Nothing in the
publication model produces one, so it means the release lost a row.

The transform therefore aborts on an interior gap and the notebook measures the rest. It
records how many geographies carry every month from their coverage floor to the newest
month in the release, which was 405 of 405 on the May 2026 file. A boundary change moves
that number without stopping anything; a dropped month stops the load.

The distinction was not obvious. My first answer was to measure both, on the reasoning
that a reorganisation should not abort a load. That is true of a series that starts late
or ends early and says nothing about a hole in the middle, and treating the two alike
would have left the only case with no legitimate cause unguarded.

### 28. Key and grain checks are one module, because the star declares twenty of them

Unity Catalog does not enforce primary or foreign keys. It records them for the optimiser
and for Power BI and enforces neither, so a fact naming a dimension row that does not
exist loads without complaint and then vanishes from every rollup keyed on it. A repeated
key loads too, and doubles whatever is summed over it. The load is the only place either
is catchable.

Nine facts point at two geography dimensions, the calendar and the crime type list, and
the small-area dimension points at the area dimension. That is roughly twenty instances
of the same two checks. Written per table they would be twenty wordings, and the table
that reports the least is the one nobody notices is wrong.

So they live in one module that takes the child frame, the parent frame and the names to
use in the message. The check that already existed in the small-area dimension delegates
to it and keeps its own name, because the order it imposes on the load, area dimension
written before small areas are checked against it, is a property of that table.

Failure detail costs a second pass that only a failing check runs. A clean load pays one
action, which is what it cost before the detail existed.

### 29. Both published panels are a projection, and the rent key comes from the dimension

The house price index and the private rent series arrive at Gold as monthly area panels.
All eleven index measures and all twelve rent measures already exist in Silver under the
same names at the same types, so both facts are a column projection and one rename, with
nothing computed and nothing cast.

Eight geographies have no key. ONS publishes the Northern Irish broad rental market areas
with no area code, so Silver keys that panel on area name. The area dimension already
assigns each one a code derived from its name, deterministic across releases and flagged
as project-assigned. The fact reads that code off the dimension. Recomputing the
derivation inside the fact would put one rule on both sides of a foreign key with nothing
forcing the two copies to agree.

The lookup is checked for uniqueness before it is joined, because a name carried under
two derived codes would fan one row into two rows with different keys, and the primary
key is informational, so nothing downstream would notice. The join key is null wherever a
row already carries a code, so a published area whose name happens to match a rental
market area cannot be overwritten or duplicated.

Rows carrying no measure are dropped, and the load records how many. Northern Ireland
lags the other nations, ONS marks its unpublished months unavailable across every measure,
and Silver keeps those rows because at that layer an unpublished month is absent and not
unreliable. Eighteen of 49,266 rent rows on the June 2026 release, nine areas across two
months, and the count moves with each release. Kept, they would make the latest published
rent for Belfast render blank in a month where England renders a figure.

Loaded: 147,453 index rows across 405 areas, and 49,248 rent rows across 357. Every key
resolves in both the area dimension and the calendar.

### 30. Seasonal adjustment stops above district, so it can only serve the benchmark

The index publishes seasonally adjusted price and index columns for 15 of its 405
geographies. All 15 are a region, a nation or a composite. No district and no county
carries the series, and Northern Ireland carries none at nation level although the United
Kingdom composite containing it does.

The figures divide exactly. Nine regions and three composites are adjusted throughout.
England, Wales and Scotland are adjusted at nation level, which accounts for the whole
gap: 1,023 adjusted rows of 1,280 at that level, the difference being Northern Ireland's
257 months from its 2005 coverage floor.

The fact carries both columns anyway. The area profile screen compares an area against
its region and its country, and a national trend line is where seasonal adjustment does
the most work, so the columns serve the benchmark and never the subject. Building a screen
that offers an adjusted series for a chosen district would have found nulls.

Worth recording because the schema does not say it. Both columns are nullable like every
other measure, and nothing in the declared table separates a structural null from a sparse
one.

### 31. A yield is computable for 316 districts, and the shortfall is all structural

The yield map needs a district carrying both a price index and a rent. 316 of 361 do.

The 45 that do not are three populations and no accidents. Scotland's 32 districts and
Northern Ireland's 11 are published on broad rental market areas, which conform to
nothing below nation and pair with no price. The Isles of Scilly carry postcodes and no
price. The City of London carries a price and no rent. The area dimension already flags
all four cases through `has_price_index` and `has_rent_index`, so the shortfall is
derivable from the model without consulting the sources.

Two of the three composites carry a rent. England and Wales does not, and the dimension
records that as `has_rent_index` false, so the fact and the dimension agree. A United
Kingdom or Great Britain yield is computable at composite level and an England and Wales
one is not.

### 32. Category B sales are excluded from the price facts and keyed in the composition fact

Land Registry flags each sale as category A, a standard full-market-value transfer, or
category B, which covers repossessions and portfolio transfers. B is near zero in the
table until 2013, steps to 2.4% that year, and settles between 15% and 18% from 2017.
That is a change in what gets published, so a median spanning 2013 and including B
compares two different populations either side of it.

Excluding B costs 14 of 120,109 district-months and 814 of 1,135,047 small-area years,
and costs no area its series: all 318 districts and all 35,672 small areas a transaction
reaches carry at least one category A sale. It also removes all 346 sales above £100M,
where category A tops out at £90M.

The composition fact keeps both categories, because the category is part of what that
screen breaks down. The two tables then disagree on a bare count by design, and the
relationship that does hold is checked at load: the price fact's count equals the
composition fact's count over category A, across all 124,631 cells.

### 33. One resolution serves the facts built from a source

Price Paid Data carries a postcode and no area code, so all three transaction facts need
the same join over 31.4M rows. Running it once per fact would let two tables disagree
about which transactions exist, and that stays invisible until someone sums one against
the other. The resolution is one module, and the load persists its output once: 200 of
200 partitions stayed cached across all three facts, so the join ran once.

Crime works the same way with four facts and a lookup, not a postcode join, since
Police.uk publishes a small-area code on the row.

Every record is labelled with an outcome before anything is filtered. 31,378,089 of
31,430,611 transactions resolve, and 92,352,547 of 96,092,836 crime records. Every count
is recorded against the run that dropped it.

### 34. Counties carry a price index and no transaction price

A transaction resolves to a district through its postcode, and the rollup runs district,
region, nation, England and Wales. `dim_area` records no county on a district row, so the
transaction facts reach 330 of its 432 areas and the 29 counties are not among them. The
house price index does publish counties, so a county screen carries an index with no
transaction price behind it. Closing the gap needs a district-to-county mapping the
postcode directory does not supply.

The other 73 unreached areas need no fixing. 43 are districts outside England and Wales,
26 are the Scottish and Northern Irish rental market areas that carry a rent series and
no transactions by construction, and the rest are the United Kingdom and Great Britain
composites and the two nations behind them.

### 35. Crime areas are summed up from small areas, prices are not

A count is additive, so a district's crime is the sum of the small areas inside it, and
the area facts roll up a 26 million row aggregate instead of counting 68 million records
four times over. The load checks the identity instead of trusting it: the England and
Wales composite reads 67,886,868 by three independent paths.

A median cannot be recovered from the medians below it, so the price facts aggregate from
the transactions at every level separately. The same star carries both rules because the
measures differ, not because the geographies do.

### 36. Anti-social behaviour is counted apart and refused by constraint

Its share of crime records drifts from 42% in 2010 to 16% in 2026, with a rise to 28% in
2020, so a series including it moves with reporting practice and not with crime.
Forces also double count it.

Both type tables refuse it by check constraint, so a row reaching Delta fails the write instead of loading as a number nobody questions. Both total tables carry it in a column
of its own beside a total that excludes it, and both measures come from one aggregate:
102,735 small-area cells hold anti-social behaviour and nothing else, and 1,272,887 hold
none of it, so counting them apart and joining would lose one population or the other.

The exclusion costs no small area its place. 36,751 codes carry a non-anti-social crime,
which is exactly the number flagged as carrying any crime at all.

### 37. A transaction-derived price series reconciles against the published index

The only check here that does not come from the pipeline being checked. It is sharp per
year and coarse in aggregate, for reasons the reconciliation rules record. `fact_area_month_price` is built from 29.6M transactions; `fact_area_month_hpi` is the publisher's own mix-adjusted index over the same areas and months. Two products of one
registry, built by different methods.

The counts agree: the category A transaction count correlates with the publisher's sales volume at 0.9998 across 123,375 cells, with an aggregate ratio of 1.0036.

The prices agree in a way worth knowing. The published average tracks the transaction median, and not the transaction mean: a median ratio of 1.011 against a mean ratio of 1.176. A raw mean carries the right
tail and mix adjustment removes it, so a screen comparing against the published index
should use the median. The gap widens with level, 1.173 at district to 1.355 at the
composite, because mix adjustment removes geographic composition too.

Every outlier is a thin high-value cell and not a fault: City of London at 11 to 55
transactions a month, and Westminster, Kensington and Chelsea, and Camden.

### 38. Owning against renting moves with the base rate, and the model shows it

`dim_date` carries the Bank of England rate in force on every one of its 19,723 days, so
the cost of owning is computed at the rate of the day rather than at a rate chosen once.
Deposit share, mortgage term and the lender's margin are assumptions and stay as
parameters.

Between 2021 and 2024 the mean monthly mortgage across 331 areas moved from £879 to
£1,631, up 86%, while mean rent moved from £886 to £1,061, up 20%. The share of
area-months where owning costs more than renting went from 41.7% to 99.5%. Gross rental
yield is computable for 316 districts at a median of 3.97%, which is the population
Decision 31 predicted before any fact was loaded.


### 39. Schema changes stop the load and get a human decision

The roadmap carried a Delta schema evolution demonstration from Phase 1 to Phase 4. Six
Silver transforms produced the opposite behaviour. Each declares its source column set and
asserts it, so a column appearing or disappearing stops the load, and the column is mapped
or dropped by a decision that reaches the constant and the tests. Doogal's suite proves
each of its 60 published columns is either mapped or explicitly dropped. `mergeSchema`
would accept the column silently, which is the quiet partial load Decision 22 rules out.

A value-set change is a different thing and is already modelled. Police.uk's crime type
list moved through three vocabularies, and `dim_crime_type` carries all 16 types with each
one's first and last published month and what it was split out of. No Delta column moved
either time.

`mergeSchema` fits an append-only landing table taking whatever a publisher sends, and
Decision 10 removed that table when Bronze stayed as Volumes over the raw files. The item
is dropped; a demonstration would have run against invented data.

### 40. Continuous integration runs the layer that does not need a cluster

Two jobs on every push and pull request: ruff, and the test suites. Runner versions track
DBR 17.3 LTS, since a suite passing on a different Spark says nothing about the cluster.
Suite directories are discovered, not listed, so each runs on its own runner and
wall-clock time is the largest one instead of the sum.

Ruff's F rules flag `spark` and `dbutils` as undefined in every notebook, because
Databricks injects them at runtime, so `ruff.toml` declares them as builtins and still
catches a real undefined name. The test step pipes pytest through `tee`, which without
`pipefail` reports a failing suite as green, so `set -o pipefail` is written into the step
and does not depend on how the runner resolves its shell.

A green check covers the transform layer on Apache Spark. It reaches no Delta, no Unity
Catalog, no write path and no data at volume, so the rule that nothing counts as done
until it has run on the cluster is untouched by it.

### 41. Transforms are portable across storage, and were not across SQL dialect

Transform modules take a DataFrame and return one and touch no Delta, Unity Catalog or
ADLS, which is what made them testable off the cluster. The first CI run failed several
hundred tests on one cause: purity had been defined against storage and never against
dialect.

`try_to_date` is a Databricks function, and Apache Spark 4.0 registers 21 `try_*`
functions without it. Seven call sites across five transforms used it, each correct on the
cluster and none runnable anywhere else. `F.expr` builds a string that whichever engine
evaluates it resolves, so the module boundary catches nothing here.

The replacement is `CAST(try_to_timestamp(x, fmt) AS DATE)`, checked against the nine
format shapes these sources publish: same dates, a null and no exception on malformed
input, and stable across session timezones. The seven sites now call
`parsed_date` in `databricks_src/silver/transforms/expressions.py`. Faking `try_to_date`
locally would have been quicker and would have meant every local run exercising an
implementation the cluster never runs.

### 42. The quality framework is a threshold registry

The roadmap carried a parameterised rules framework: `quality_rules.json`, a generic
notebook reading it, rejected rows routed to quarantine. Counting what there was to
configure found 32 guards across the six Silver transforms and four more in
`conformance.py`, and none of the 36 carries a tolerance. The numbers in them are
published facts, a UK bounding box and two sentinel values, and loosening those would
admit wrong rows, not marginal ones.

The real threshold population is small: six freshness bounds, still unset by design, and
a cross-source reconciliation that computed several ratios and asserted none of them. So
`databricks_src/quality/rules/evaluator.py` holds a threshold registry, and
`quality.rule_result` takes one row per rule per run with the bounds in force written
into the row, since widening a bound later must not reinterpret an old result.

Four things defend a result. Bounds live on the rule, so a caller cannot pass the wrong
one. The evaluator refuses NaN before comparing, because Spark orders NaN above every
number and an unguarded one satisfies any floor. A CHECK ties the verdict to the numbers
beside it. And the caller declares which rules it meant to evaluate, because no
constraint fires on a row that was never written, and a rule that stops running looks
exactly like one that keeps passing.

### 43. Three reconciliation rules, with bounds read off thirty-two measured years

A rule is registered only once its bound is known. `ppd_hpi_count_correlation` floors at
0.99 against a measured 0.9998, and its note records that it is coarse: the cells span
district to composite, and a simulation with districts wrong by up to 90% still returns
0.99993. `ppd_hpi_count_ratio_by_year` bands at 0.95 to 1.08, where 1995 to 2026 runs
0.9728 to 1.0641. `ppd_hpi_median_ratio_by_year` bands at 0.93 to 1.08.

The count band is wider below than above, because a shortfall is the failure this
pipeline can cause and a surplus is usually the publisher still reporting. A three-sigma
band computes to 0.9443 to 1.0635 and rejects 2023, an ordinary year.

The mean ratio is stable at 1.1821 and is not registered. Nothing downstream reads it,
and a rule nobody acts on gets widened instead of investigated. The first run recorded
65 results and none breached.

### 44. The gate value is when a publisher released, not what the data is about

Six public bodies publish the datasets behind this platform, and none of them agree on
how to name a file. ONS puts the release date in the path. Land Registry and police.uk
put the month the data covers in the filename, and that trails publication by about two
months: the June 2026 house price index was published on 19 August, and the June 2026
crime archive on 29 July.

The pipeline decides whether to download by comparing the publisher's release date
against the date it last succeeded. Take that release date off a filename carrying a
data month and the comparison never comes out true, because the filename runs two months
behind a value that advances to today on every run. The source is skipped forever, and
because a skipped download and a completed download both look identical from the outside,
the run reports success while ingesting nothing.

So the release date is always read as a release date. Where the publisher puts it in the
URL, it is parsed from there. Where the publisher does not, it comes from the
`Last-Modified` header on the file itself, which all six send. The data month keeps a
separate job: it builds the address and names the landed file, so the copy in storage
says which release it is without being opened.

The interesting part is that the failure would have been invisible. Nothing errors, no
alert fires, and the audit trail records six successful runs. It was caught by tracing
the arithmetic across three consecutive months, not by anything the pipeline itself
could have reported.

### 45. When one source fails, the rest of the pipeline still runs

Six public bodies publish the datasets behind this platform and any of them can have a bad
day. A run where the crime archive is unreachable should not cost the house price index,
and it should not quietly load a crime table from last month's file either.

Which tables a failure reaches is a fact about the model rather than a setting. The crime
data feeds two dimensions and four facts; the postcode directory feeds two dimensions that
between them sit under every fact in the star. Those relationships are written down once,
with tests behind them, and each stage of the run works out for itself what it should do
given what has already failed. Nothing is passed down the pipeline, so a task rerun on its
own reaches the same answer as one running in sequence.

The interesting part was working out what a stage should read to make that decision. The
first version asked whether anything upstream had failed, which is the obvious question and
the wrong one. Three different things stop a table being rebuilt: it failed, the notebook
holding it died before reaching it, or the run never got that far. Only the first leaves a
failure recorded anywhere. The other two leave nothing at all, and nothing is
indistinguishable from success.

So a stage asks the opposite question. Every table it reads must have a completed run of its
own, in this pipeline execution, at the layer that produces it, that last part because a
source is recorded twice, once when the file lands and once when it is loaded, and a landed
file is not a loaded table. One question closes all three cases without having to model any
of them.

The result is that a failed crime download produces a run where the index, rents and
transaction tables all refresh normally, the six crime and geography tables record that they
waited and what they waited on, and the dashboard reads a mix of fresh and unchanged tables
instead of a mix of fresh and wrong ones.

### 46. The pipeline reports success when a download fails

An orchestration run that goes green when one of its downloads failed sounds like a bug
being papered over. Here it is deliberate, and the alternative is worse.

The orchestrator owns exactly one thing: getting six files out of six government websites
and into storage. Everything after that is owned by notebooks that record their own
outcomes into an audit table. So when a download fails, the orchestrator's job is not to go
red. It is to write down what happened somewhere the rest of the pipeline will read.

Each download has a failure path that writes a small marker file naming the source, the
target and the error. That path ends in a step that succeeds, which means the failed
download is no longer the last thing in its branch, the loop moves on to the remaining five
sources, and the orchestrator finishes green. The first task of the processing job then
reads the markers, turns each into a recorded failure, and the cascade takes it from there.

Two things follow that are easy to miss. The loop no longer needs configuring to continue
past a failure, because there is no longer a failed step for it to stop on. And the marker
write is the one step in the whole pipeline that retries, because if it fails the failure is
never recorded, and the next stage loads against a file that was never downloaded. That is
the single direction this design exists to prevent, so after its retries it is allowed to
fail loudly.

Where the run actually failed is a query against the audit table, not a colour in a
monitoring pane. That is the right place for it: the audit table is the thing that gets
built into a pipeline health dashboard, and a colour is not.

### 47. The anomaly detector I planned would have fired hardest on the best data

The roadmap carried a line for statistical anomaly detection from the start: flag house
price movements that sit too far outside an area's own history. It sounds obviously
useful, and measuring it before writing it is what stopped it being built.

The measurement covered 147,444 area-months across 405 areas, using the month-over-month
change the publisher already reports. A three-sigma fence flags 0.788% of those cells,
about 1,160 of them per run. That alone is close to disqualifying, because a check that
produces a thousand flagged rows needs somewhere to put them, and this project decided
against a quarantine table for exactly that reason.

The shape of the tails explains why no tightening or loosening rescues it. Against a
normal distribution three sigma should flag 0.27% and flags 0.788%; two sigma should flag
4.55% and flags 5.014%. The middle of the distribution behaves and the tails are about
three times heavier than the fence assumes, which is what a price index looks like. It is
not a sign of anything wrong.

What settled it was a number running the wrong way. Flag rates climb with area size:
districts 0.742%, counties 1.061%, regions 1.382%, composites 1.549%. Under a
median-based fence the gap is wider still, 0.654% for districts against 2.259% for
nations. Aggregates have the most transactions behind them and the least noise, so a
detector flagging them two to three times as often is not finding faults. It is finding
stamp duty changes, seasonality and the pandemic, all of which are real and all of which
show up cleanly in an aggregate and get buried in the noise of a single district.

A quality check that fires hardest where the data is most trustworthy is measuring the
wrong thing, and no threshold setting fixes that.

The same probe found the detector worth having, in a place I had not thought to look. The
platform reconciles its own transaction counts against the publisher's, per year, and
records the result every run. Asking whether a settled year's ratio moves between runs
gives 23 of 32 years at exactly zero, with the largest movement anywhere being 0.0015 in
the current year. A noise floor of zero needs no distributional assumption at all, and a
settled year that moves means a publisher restatement, which is a real event worth
knowing about. That became two ordinary threshold rules instead of a new kind of check.

### 48. The CI check I did not build, and what looking at it turned up

The roadmap carried a line for validating the Data Factory definitions on every pull request. Microsoft publish an npm package for exactly this and a GitHub Action wrapping it, so the line looked like an afternoon of work.

The reason not to build it is that nothing would reach the check un-validated. Pipeline definitions arrive in this repository through Data Factory Studio's Git integration, and Studio validates before it will save. Opening up the package showed how literally: it is a downloader, fetching its actual validation engine from the Data Factory service at run time and executing it. The job would have asked the same engine the same question a second time, about a file that engine had just written.

Looking at it turned up three things worth knowing for anyone who does need it. The package version pins the downloader and not the validator, so the rules can change without a version bump. The job needs network access to a live Microsoft endpoint on every run. And the fetched response is written to a file and executed without being checked, so a failed request becomes a parse error in a file nobody wrote rather than a network error. The response body is executed either way, so a refused request and a real engine are indistinguishable to the runtime until the parse fails.

That combination is what settles it. A check that fails on someone else's outage, with a message pointing at the wrong thing, teaches you to ignore the check. This project already refused a scheduled reconcile in January for the same reason, because it would have reported a missing file every year until I stopped reading the alert.

A smaller version was worth a look before closing the line: parse the JSON directly and assert every reference resolves. It needs no network and pins cleanly, and it was rejected on the same ground as the vendor tool, because it looks for the same nothing.

## Bugs found and fixed

### A ratio inflated by its own denominator

The cross-source verification reported a count ratio per transfer year: transactions
counted here against the sales the publisher reported. Every year from 1995 to 2025 sat within a few percent
of one. 2026 read 1.5716.

The year-by-year cell divided by `sum(coalesce(sales_volume, 0))`. Two months of 2026
carried a published price and no published volume, so their transactions entered the
numerator with nothing opposite them: five months counted here over three published, at a
true ratio near 0.99, gives about 1.57. The aggregate cell above it was already correct,
filtering on a populated volume, which is why the two disagreed without either looking
obviously wrong.

Restricting both sides to cells carrying both counts moves 2026 to 0.991 and leaves every
settled year untouched, since all 31 carried a volume in every cell. `cells_with_both_counts`
now sits beside `cells` in the output and reads 987 against 1,645 for 2026, which says
the two populations differ before any ratio is computed.

Found by trying to set a threshold on the number.

### A verification query Spark could not plan

The transaction load's verification cell compared the small-area dimension's price flag
against the small areas present in the fact, written as three scalar subqueries in a
select list with one correlated `NOT EXISTS`. On DBR 17.3 it failed with
`INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND`, SQLSTATE XX000, reporting two expression ids for
one column. The query scanned each table twice, and the correlated subquery had been
rewritten into a nested-loop existence join whose outer key was pruned under a different
id than the join looked for.

XX000 is Spark's internal-error class, so the SQL was legal and the planner failed on it.
Rewriting the check as a single left join avoided that path, halved the scans, and made
the reverse direction free: a small area in the fact that the dimension does not flag,
which the original query never asked about.

It failed after all three loads had completed and all three audit runs had closed, so
nothing was left half-written.

### Latent parameter-shadowing bug in dataset configuration

**Discovered:** During ONS onboarding. When I updated the ONS URL in the watermark for a new monthly release, the pipeline kept writing files to ADLS under the correct name but with the wrong content, and ADF reported "Succeeded" every time.

**Symptoms:**
- `openpyxl.load_workbook()` failed with `BadZipFile: File is not a zip file`.
- A hex dump of the "XLSX" showed HTML content starting `<!DOCTYPE html>`.
- The HTML came from `bankofengland.co.uk`, not `ons.gov.uk`, which meant the pipeline was fetching from the wrong host.

**Root cause:** The HTTP datasets had parameters defined (`p_base_url`, `p_relative_url`) but their Base URL fields were hardcoded instead of using `@dataset().p_base_url`. For five sources this went unnoticed because the hardcoded value happened to match the watermark value. When the ONS URL changed in the watermark, the dataset carried on using its stale hardcoded URL, which pointed at BoE's host, and BoE returned a 200 OK homepage for the bad request path.

**Fix:** Replaced every hardcoded URL in the dataset Base URL fields with the right `@dataset()` expression. Re-ran the master pipeline for the affected sources. Validated the output files by parsing them in their native format.

**Lesson:** A pipeline reporting success confirms bytes moved, not that the right bytes moved. Two follow-ups came out of it:
1. Magic-byte validation in the Silver-layer ingestion contract: every file checked against its expected format header before parsing.
2. An audit of every parameterised dataset field to confirm the parameters are actually wired in, not just defined.

### ADF nested control-flow restriction

**Discovered:** When trying to nest an `If Condition` inside a `Switch` inside a `ForEach` for the yearly-stepped full-vs-incremental logic.

**Fix:** Extract the inner logic into child pipelines (`PL_Route_Yearly_Stepped`). The master Switch calls the route pipeline; the route pipeline holds the If Condition. The result is a cleaner architecture with better separation of concerns.

### URL query-string double-encoding (ONS)

**Discovered:** ONS uses relative URLs of the form `?uri=/path/to/file.xlsx`. ADF URL-encoded the `?` whenever the relative URL started with anything else, which corrupted the request.

**Fix:** Always put `?` as the first character of the relative URL. ADF preserves the query-string delimiter when it sits at position 0.

### Import shadowing from a top-level `databricks` folder

**Discovered:** while building importable transform modules for the Silver layer. Imports from a top-level repo folder named `databricks` never resolved.

**Root cause:** the installed Databricks SDK owns the `databricks` package name on a cold interpreter start, so a repo folder of the same name is shadowed and never reaches the import path. A second constraint compounded it: on Databricks Runtime 16.0 and above a notebook cannot be imported as a Python module at all, so importable code has to be a plain workspace file (`.py` with no `# Databricks notebook source` header) rather than a notebook.

**Fix:** renamed the folder to `databricks_src`, and kept importable library code as `.py` workspace Files while runnable notebooks stay in source format. Reverting the rename appeared to work once, but only because stale `sys.modules` state from earlier path edits masked the failure, so the real check is a fresh interpreter.

**Lesson:** verify any import fix on restarted compute before trusting it. The `__init__.py` files I first suspected were a red herring. Implicit namespace packages resolve fine, and the `wsfs ... Cannot find child __init__.pyc` log lines are the import system probing for files, not the cause.

### External Volumes rooted at the wrong depth

**Discovered:** the first end-to-end BoE Silver run failed with a 404 whose path contained a doubled segment, `boe/base_rate/base_rate/`. A Volume path resolves as the Volume's storage location plus the relative path the notebook appends, so the doubling pinpointed the cause: the Volume was rooted at the dataset folder (`/boe/base_rate/`) instead of the source root (`/boe/`).

**Audit:** I checked all six Volumes against both the intended Bronze layout and a direct storage listing, since a Volume can't be trusted to list its own contents when its own root is in question. `ppd` and `doogal` were correct; `ons` and `police` had the same misrooting as `boe`; `hpi` pointed at a `uk_hpi/` folder that matched storage but revealed the data itself had landed outside the source taxonomy back in Phase 1.

**Fix:** for the misrooted Volumes I dropped and recreated them at the source root, since an external Volume's location can't be altered in place and the recreate touches only metadata. For `hpi` the fix was at the data layer instead: repoint the watermark sink to `land_registry/hpi`, re-run the pipeline, and delete the stale folder only after verifying the new file landed. I also corrected the volume-creation script and removed a one-off `DROP VOLUME` line left in a script meant to run on every rebuild.

**Lesson:** `CREATE ... IF NOT EXISTS` is safe to re-run but can't repair drift, since create-if-absent says nothing about desired state, and relocating a Volume needs an explicit drop. A wrong-rooted Volume also keeps working until a path convention exposes it, so verify against storage listings rather than the object's own definition.

### HPI Silver built on a seven-month-old release

**Discovered:** validating the first HPI Silver run. The notebook prints the newest month present in the data next to the filename it read, and both said October 2025. The current release was May 2026.

**Root cause:** HPI's URL rotates with every monthly release, so the watermark's `relative_url` is updated by hand. It hadn't been since the October 2025 release. Every run after that re-fetched the same file and reported success, because at the transport layer a stale URL is indistinguishable from a current one.

**Fix:** updated the watermark to the May 2026 release, re-ran the master pipeline, re-ran the Silver notebook. Overwrite write mode meant no cleanup, and the seven months of revisions to previously published months came with it. The rebuilt table matched the published UK average for May 2026 exactly (£271,295, index 104.0), which is the check that would have caught this months earlier had it been running.

**Lesson:** the Phase 1 version of this was that a success flag confirms bytes moved, not the right bytes. This is the same failure one level up: the right bytes moved, but they were old, and nothing in the pipeline could tell the difference. Freshness has to come from the content, so the Silver notebook now reports the newest month found in the data alongside the filename it read. One is a claim the file makes about itself; the other is a claim the filename makes about the file.

A smaller finding from the same run: the landed object is lower-cased (`uk-hpi-full-file-2026-05.csv`) while the source URL is mixed-case, so vintage selection matches case-insensitively.

### Empty-array indexing under ANSI mode bypassed the guard written for it

**Discovered:** running the `dim_area` test suite. One test failed, the one asserting that an area no publisher names aborts the load. The other 54 passed, including every other test of the same name-selection code.

**Symptoms:**
- The guard reporting an unnamed area never raised
- The exception that surfaced was a Spark error, not the `ValueError` the guard raises, so `pytest.raises(ValueError)` did not catch it
- A diagnostic against the real sources confirmed five areas with no name from any publisher, so the guard's input was correct

**Root cause:** the area name is chosen from an ordered list of candidates filtered to the populated ones, then indexed at position zero. For an area no publisher names that list is empty. ANSI mode has been on since DBR 17.0, and under it indexing an empty array raises `INVALID_ARRAY_INDEX` rather than returning null. The guard was written to report exactly that case and never ran, because the index raised first. Every other test passed because those areas always had at least one candidate, so the array was never empty.

**Fix:** replaced the index with `try_element_at`, which returns null where the position does not exist.

**Lesson:** this is the same rule the Silver transforms already follow with `try_cast` over `cast`, applied somewhere I had not thought to apply it. Under ANSI, prefer the form that yields null so a guard can name what went wrong, over the form that raises with an error naming nothing. The pattern generalises past casting, and the failure mode is specifically that a guard's own exception gets pre-empted by an engine error, which makes it look as though the guard is wrong rather than unreachable.

### An incremental gate that always evaluated true

The condition deciding whether to re-download a source was written as
`last_refreshed != utcnow()`. It read correctly and it never blocked anything.

`last_refreshed` is a date, `2026-08-27`. Raw `utcnow()` is a full timestamp,
`2026-08-28T23:54:50.3498894Z`. The two strings can never be equal, so the condition was
true on every evaluation, including twice on the same day. An earlier variant compared
against `utcnow('YYYY-MM')`, which produced `2026-08` and was equally never equal.

**Fix:** make both sides the same shape, `utcnow('yyyy-MM-dd')`, and compare with `>`
instead of inequality. Format specifiers in that expression language are case-sensitive,
which is what produced the mismatched shapes: `yyyy` is a year, `dd` is a day, and `DD` is
not a specifier at all so it passes through as the literal text `DD`.

**Lesson:** this is about how the defect presented. It was reported as "this works but
isn't right", because downloads were happening. A gate that always passes and a gate that
is working correctly produce the same observable behaviour whenever there is genuinely
something to fetch, and they only diverge on the run where the gate should have said no.

### A resume that skipped work by asking the wrong question

The crime data arrives as seven compressed archives that together expand to nineteen
gigabytes, well past what a single machine holds, so they are unpacked one at a time into a
staging table and promoted in one write. To let a failed run pick up where it stopped, the
loop skips any archive already staged.

Keeping that staging table between monthly runs looks obviously right. Six of the seven
archives cover closed periods that no later release touches, and re-unpacking seventy-eight
million rows of settled history every month buys nothing. The archives themselves never
change once published.

What changes is which archive is authoritative. Each monthly release is a rolling
three-year window, so consecutive releases overlap almost entirely, and the load resolves
every month to whichever archive supplies it most recently. A resume that skips on "is this
archive already staged" would leave the previous release in place, unpack the new one in
full, and promote both: the same crimes twice, under two publication vintages. The load
already had a verification query for exactly that condition, checking that every month
comes from exactly one archive, and it would have started returning rows.

**Fix:** change the question the resume asks. Not "is this archive present" but "do the
months this archive holds still match the months it is authoritative for". Equal means
skip, different means restage it for whatever it has left, and nothing means remove it.
Each archive is written under a predicate scoped to itself, so restaging replaces its rows
instead of adding a second copy.

**Lesson:** what made this findable was a query rather than an argument. Grouping the
staging table by publication vintage showed the seven archives covering 187 consecutive
months with no overlap at all, which was the reason keeping them looked safe, and also the
thing that was about to stop being true.

---

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory (Git-integrated) |
| Storage | Azure Data Lake Storage Gen2 (per-layer containers) |
| Compute | Azure Databricks (PySpark, Delta Lake, DBR 17.3 LTS, Photon-eligible) |
| Governance | Unity Catalog (managed tables, schema-level managed locations, External Volumes for Bronze) |
| Identity | User-assigned managed identity via Databricks Access Connector |
| Query (planned) | Azure Synapse Serverless SQL |
| Visualisation (planned) | Microsoft Fabric / Power BI |
| Source control | GitHub (trunk-based, branch-protected main) |
| Testing | pytest + chispa for PySpark transforms |
| CI/CD | GitHub Actions (lint and the test suite on every push and pull request) |

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
├── .github/
│   └── workflows/
│       └── tests.yml                    # lint + discovered suite matrix, on push and PR
├── ruff.toml                            # rules, and the Databricks runtime builtins
├── config/
│   └── watermark.schema.json            # the watermark contract, one branch per load pattern
├── adf/
│   └── pipelines/                       # JSON definitions, synced via ADF Git integration
│       ├── PL_Master_Orchestrator.json
│       ├── PL_Single_File_Full_Load.json
│       ├── PL_Yearly_Stepped_Full_Load.json
│       ├── PL_Route_Yearly_Stepped.json
│       ├── PL_Route_Incremental_Load.json
│       ├── PL_Incremental_Load_StaticURL.json
│       └── PL_Incremental_Load_TemplatedLatest.json
├── databricks_src/
│   ├── setup/
│   │   ├── README.md                    # bootstrap runbook
│   │   ├── cluster_definition.json      # cluster + library spec
│   │   ├── job_definition.json          # the twelve-task Silver and Gold job
│   │   ├── job_definition_pre_run.json  # one-task URL resolver, run before the downloads
│   │   ├── apply_job_definition.py      # creates or resets a job from either file, idempotent
│   │   ├── 01_create_schemas.py         # Unity Catalog schema definitions (SQL via %sql cells)
│   │   ├── 02_create_bronze_volumes.py  # External Volumes per Bronze source
│   │   └── 03_create_quality_tables.py  # pipeline_run, pipeline_metric and rule_result, DDL generated from the writers
│   ├── bronze/
│   │   ├── notebooks/
│   │   │   ├── 01_pre_run_resolve_urls.py    # resolves every source's URL and release date
│   │   │   └── 02_post_run_record_state.py   # reads the download markers, records state, computes the plan
│   │   └── watermark_library/
│   │       ├── ons.py                   # reads the dataset page, since the file suffix follows no rule
│   │       ├── hpi.py                   # constructs candidate addresses, confirms by HEAD
│   │       ├── police.py                # crime-last-updated endpoint, archive confirmed by HEAD
│   │       ├── resolution.py            # shared failure type and the Last-Modified parse
│   │       ├── registry.py              # watermark lookup and field merge
│   │       ├── schema.py                # key-level contract, and the invariants a schema cannot hold
│   │       └── source_dependency.py     # the dependency chain and the skip planner
│   ├── silver/
│   │   ├── notebooks/                   # one notebook per source
│   │   │   ├── 01_boe_base_rate.py      # BoE base rate → Silver (event-grain SCD2)
│   │   │   ├── 02_hpi.py                # UK HPI → Silver (monthly geography panel)
│   │   │   ├── 03_ppd.py                # Price Paid Data → Silver (one partition per transfer year)
│   │   │   ├── 04_doogal.py             # Doogal postcodes → Silver (unzip, ONS spine)
│   │   │   ├── 05_ons.py                # ONS private rents → Silver (workbook converted, then read)
│   │   │   └── 06_police.py             # Police crime → Silver (per-archive extract, staged, promoted)
│   │   └── transforms/                  # importable transform functions (unit-testable)
│   │       ├── boe_base_rate.py         # pure BoE transform + DQ guard
│   │       ├── hpi.py                   # pure HPI transform + coverage floor + guards
│   │       ├── ppd.py                   # pure PPD transform + typing, code-set, and key guards
│   │       ├── doogal.py                # pure Doogal transform + code sets, BFPO and grid guards
│   │       ├── ons.py                   # pure ONS transform + marker position guards
│   │       ├── police.py                # archive selection + single-pass rule registry
│   │       └── expressions.py           # column expressions shared across sources (date parse)
│   ├── gold/
│   │   ├── notebooks/                   # table DDL, dimension load, fact loads
│   │   ├── transforms/                  # one module per table, plus shared conformance checks
│   │   └── exploration/                 # read-only measurement behind the model's figures
│   ├── quality/
│   │   ├── audit/
│   │   │   └── writer.py                # run and metric writer, metric name registry, freshness bounds
│   │   └── rules/
│   │       └── evaluator.py             # threshold registry, evaluator, rule_result
│   ├── orchestration/
│   │   └── stage.py                     # the gate every Silver and Gold notebook opens with
│   └── utils/                           # shared constants (paths), Spark helpers, logging
├── requirements-dev.txt                 # local + CI test stack (pyspark, pytest, chispa)
├── tests/
│   ├── conftest.py                      # SparkSession fixture; reuses the cluster session
│   ├── run_tests_boe.py                 # one runner per source: its Silver suite, then every Gold suite reading it
│   ├── run_tests_hpi.py
│   ├── run_tests_ppd.py
│   ├── run_tests_doogal.py
│   ├── run_tests_ons.py
│   ├── run_tests_police.py
│   ├── run_tests_audit.py               # the audit writer
│   ├── run_tests_shared.py              # suites belonging to no single source
│   ├── run_tests_watermark.py           # resolvers, registry, dependency chain
│   ├── run_tests_orchestration.py       # the stage gate
│   ├── test_silver_transforms/
│   │   ├── test_boe_base_rate.py        # BoE transform + DQ guard
│   │   ├── test_hpi.py                  # HPI transform, coverage floor, typing
│   │   ├── test_ppd.py                  # PPD transform, partition key, code sets
│   │   ├── test_doogal.py               # Doogal transform, column contract, BFPO, coordinates
│   │   ├── test_ons.py                  # ONS transform, marker positions, cover-sheet date parser
│   │   ├── test_police.py               # archive selection, rule registry, coordinate box
│   │   └── test_expressions.py          # shared date parse: formats, null handling, timezone
│   ├── test_gold_transforms/
│   │   ├── test_dim_date.py             # calendar expansion, rate interval chain
│   │   ├── test_dim_area.py             # levels, ancestry, name precedence, derived codes
│   │   ├── test_dim_lsoa.py             # district assignment, boundary vintage, conformance
│   │   ├── test_dim_crime_type.py       # vocabulary map, publication window
│   │   └── test_conformance.py          # shared key and grain checks
│   ├── test_bronze_watermark/
│   │   ├── test_schema.py               # the schema, against entries wrong in one way each
│   │   ├── test_registry.py             # watermark lookup, field merge
│   │   ├── test_source_dependency.py    # dependency chain, skip planner, check inputs
│   │   ├── test_resolution.py           # failure type, Last-Modified parse
│   │   ├── test_ons.py                  # dataset page read
│   │   ├── test_hpi.py                  # candidate address walk
│   │   └── test_police.py               # endpoint read, archive confirmation
│   ├── test_orchestration/
│   │   └── test_stage.py                # plan read, both gate questions, the skip branch
│   ├── test_quality_audit/
│   │   └── test_writer.py               # metric registry, generated DDL, value routing, freshness verdict
│   └── test_quality_rules/
│       └── test_evaluator.py           # registry, refused values, DDL, verdict constraint
├── synapse/                             # (planned) external table definitions
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
- Watermark file in its own `configs` container, exposed as the Unity Catalog volume `uk_property_intel.configs.watermark`

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

### Monthly runs

Nothing in the watermark is edited by hand between runs. A pre-run job resolves every source's current URL and release date before the first download, and each source is asked the way its publisher addresses things: ONS reads the dataset page, because its filename carries a suffix that follows no rule; HPI builds candidate addresses newest first and confirms one with a HEAD; Police reads the crime-last-updated endpoint and confirms the archive; and the three sources published at a fixed address are asked only when they last changed.

The download gate then compares that release date against the date the source last succeeded, so a run fetches only what has been republished since. Three sources used to need a hand-edited URL every month, and a missed edit was silent: the pipeline re-fetched the previous release and reported success. That is the failure the gate exists to remove.

After the downloads, a twelve-task Databricks job runs the six Silver notebooks, the four Gold load notebooks and the cross-source check. A source that failed to download skips exactly the tables built on it, and each skipped stage records what it was waiting on.

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

### Phase 2 — Silver layer ✅

- [x] Databricks workspace (Premium) + Dedicated-access cluster
- [x] Unity Catalog: dedicated `uk_property_intel` catalog
- [x] Per-layer container storage layout with schema-level managed locations
- [x] Access via UAMI + Databricks Access Connector (no mounts, no secrets)
- [x] Bronze restructure: container rename `raw` → `bronze`, removed redundant subfolder
- [x] Unity Catalog `bronze` schema with per-source External Volumes (UC-governed access from Silver, no abfss paths in notebooks)
- [x] Doogal Bronze folder renamed `postcodes` → `doogal` to align with source-name taxonomy
- [x] Silver: BoE base rate (event-grain SCD2, spark-excel read, fail-loud DQ guard)
- [x] Silver: HPI (monthly geography panel, per-nation coverage floor, ANSI-safe typing)
- [x] Silver: PPD (31.4M transactions, one partition per transfer year, TUID uniqueness asserted)
- [x] Silver: Doogal postcodes (2.71M postcodes, ONS spine only, fabricated coordinates nulled)
- [x] Silver: ONS private rents (357 geographies, workbook converted before reading)
- [x] Silver: Police.uk crime (96.1M rows from seven overlapping archives, no natural key)
- [x] Magic-byte validation for binary inputs (postcode archive, ONS workbook, all seven crime archives)
- [x] Pipeline audit tables for per-run quality metrics — `quality.pipeline_run` and `quality.pipeline_metric`, written by all six Silver notebooks
- [x] Freshness value recorded per Silver source, with a per-source bound that aborts the load
- [x] pytest + chispa harness (SparkSession fixture, cluster runner) across the Silver transforms, the Gold tables, the audit writer and the threshold rules

### Phase 3 — Gold layer ✅

- [x] Dimensional model design: grain, keys and dimension structure settled against measurement before any transform was written
- [x] Declared DDL for thirteen tables, four dimensions and nine facts, created on the cluster with informational keys and enforced check constraints
- [x] All four dimensions loaded: 19,723 calendar days carrying the base rate, 432 published areas, 36,778 small areas with the majority-district assignment for the 82 that straddle a boundary, and 16 crime types across three vocabulary eras
- [x] Index and rent facts loaded, the two published area panels: 147,453 and 49,248 rows
- [x] Transaction facts loaded: monthly price by published area at 124,631 rows, the composition breakdown at 1,552,988, and annual price by small area at 1,134,233, all three from one resolution of 31.4M transactions through the postcode directory
- [x] Crime facts loaded at both grains, with anti-social behaviour held out of every total by constraint: 25,984,439 and 6,328,185 rows by small area, 736,822 and 61,681 by published area, summed up from the small-area aggregate and checked against a direct count
- [x] Own-versus-rent monthly cost at the base rate of the day, and rent yield, computed downstream from the facts: 45,346 area-months across 331 areas, a yield for 316 districts at a 3.97% median, and the rate cycle moving the share of area-months where owning costs more from 41.7% to 99.5%
- [x] Join-integrity and referential-coverage checks recorded through the audit writer: every foreign key on all nine facts checked against the loaded dimension after write, and coverage recorded against `dim_area`, `dim_lsoa` and `dim_crime_type`
- [x] Cross-source reconciliation of a transaction-derived price series against the published index: counts correlating at 0.9998 across 123,375 cells, and the published mix-adjusted average tracking the transaction median to within 1.1%

### Phase 4 — Advanced features

- [x] Threshold rule framework: `quality/rules/evaluator.py` and `quality.rule_result`, with three reconciliation rules whose bounds were read off thirty-two measured years. The planned JSON rule file was dropped after counting what there was to configure
- [x] GitHub Actions CI/CD: ruff and the full test suite on every push and pull request, suite directories discovered rather than listed
- [x] Watermark automation — every source resolves its own current URL and release date before the run, and the pipeline fetches only what has actually been republished
- [x] Orchestration — one pipeline runs a pre-run job, gates and copies six sources, then runs a twelve-task Databricks job end to end
- [x] Failure cascade — a source that fails at ingestion skips exactly the tables built on it, and every stage that waits records why
- [x] Watermark schema validation: a key-level contract for the watermark, checked after the read, before the write and on what landed, and joining the existing test suite instead of becoming a separate continuous integration job
- [x] Statistical anomaly detection, measured and answered. Fact-level detection was rejected on evidence; the drift between runs became two ordinary threshold rules and needed no new rule kind
- [x] PPD annual reconcile: a refresh month configured on the watermark and read by the yearly-stepped router, so August re-fetches every yearly file and both layers rebuild from it
- [x] PPD changelog-driven refresh, cancelled rather than built. Driving a refresh from a file read in Silver and Gold makes the medallion cyclic, and nothing can know which years it names without the cyclic read. The changelog still loads monthly as an append; only the trigger is gone
- [x] Quarantine table, decided against and recorded rather than left pending. Contract violations stop the load and the thin value population is handled in place, so nothing generates rows needing somewhere to go
- [x] Validation of the ADF pipeline definitions, investigated and rejected. Studio validates before it will save, and the tooling downloads that same engine at run time, so the check would re-ask a question already answered while adding a live-service dependency that fails as a syntax error

### Phase 5 — Consumption

- [ ] Synapse Serverless external tables over Gold
- [ ] Fabric / Power BI dashboards:
  - Property Market Dashboard, four screens: area profile against regional and national benchmarks; own-versus-rent monthly cost at the base rate of the day; yield map with a crime overlay; and what actually sells in an area by property type, build age, tenure and sale category
  - Pipeline Health Dashboard (run history, quality scores, anomaly alerts)
- [ ] Boundary polygons for the map layers, sourced at dashboard time. The platform holds postcode points, not area shapes, so a choropleth needs geometry the pipeline does not carry


### Future work — held for observation

Both items below are built as mechanism and unset as configuration, so neither is waiting on code. Each needs a bound read off values that only the publication calendar can produce, which is why they sit after the phases instead of leaving one open.

- [ ] The six freshness bounds. Every source records its age on every run, and a bound has to be read off observations spanning a release boundary before it means anything
- [ ] The two run-to-run drift rules. Both need a measured definition of when a transfer year is settled, which is a fact about Land Registry's correction window rather than something to choose

Nothing is lost by the wait. Both series have been recorded since the layers that produce them were built, so either bound can be computed retrospectively over everything accumulated by the time it is set.
---

*Project status: Phases 1 to 4 complete. Bronze ingestion for all six sources, the Silver layer, the Gold star schema, and the quality, automation and orchestration work, all verified on the cluster. Phase 5 covers consumption: Synapse Serverless and the dashboards. Two bounds sit in Future work, waiting on the publication calendar rather than on code. Last updated 03-09-2026.*
