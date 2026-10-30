# dbt marts

Three views over the Gold star, each holding one grain, for anyone querying the warehouse in SQL. They live in `uk_property_intel.marts`. The monthly job builds and tests them after every Gold rebuild, and continuous integration builds the same views into `marts_ci`.

| Mart | One row per | Holds |
|---|---|---|
| `mart_district_month_market` | district and month | the published index and average price, private rent, the transaction median and recorded crime, each null outside its own coverage |
| `mart_district_yield` | district and month | gross rental yield, twelve months of rent over the published average price, where both are published |
| `mart_lsoa_year_crime_price` | small area and year | annual recorded crime beside the annual transaction median, for complete years and for codes the 2021 boundary revision left unchanged |

Every column is declared in `models/marts/_marts.yml` under an enforced contract, so a renamed column or a changed type fails the build before a reader sees it. The reasoning behind each part is in [`docs/DESIGN.md`](../docs/DESIGN.md), Decisions 102 to 109.

## Why the layer exists

`gold.dim_area` holds six area levels in one table, and the levels contain each other: a crime in England sits in its district, its region, England, and the England and Wales composite. A sum written straight against Gold without a level filter counts each crime about four times, and nothing in the warehouse warns. The semantic model refuses that sum with a guard measure, but a SQL query never meets the guard. A mart holds one grain, so the same sum cannot be written against it.

[`examples/`](examples/) measures what each mart refuses. Each file asks one question twice: once against Gold, the way a reader new to the model would write it, and once against the mart. Measured 28-10-2026:

| File | Against Gold | Against the mart |
|---|---|---|
| `01_sum_across_area_levels.sql` | 19,225,323 crimes in 2024, every level summed | 4,869,305 |
| `02_annual_price_joined_to_monthly_crime.sql` | 8,037,250 sales in unchanged small areas in 2024, each counted once per month of crime | 709,975 |
| `03_small_areas_redrawn_in_2021.sql` | 2,023 small areas with sales and no crime in 2019 | 4 |
| `04_year_in_progress.sql` | crime down 51.1% in 2026, on six months of data | down 2.0% in 2025 |

## Why it sits beside the semantic model

Power BI goes on reading the star directly, and no report reads a mart. The semantic model was built and verified on the star before the marts existed, and marts made the only route to the data would tie every later consumer to questions shaped for three views. dbt also stays out of Silver, whose work is reading files a SELECT cannot reach, and out of Gold, whose transaction join runs once on a persisted frame where one model per table would run it three times.

## Who reads it

Anyone querying the warehouse in SQL, from the SQL editor or a notebook. No group holds a grant on `marts` yet. `bi_readers`, the group Power BI reads as, is left out on purpose: nothing reads the marts through Power BI, and a grant nothing uses is a permission with no purpose.

## Tests

24 run with every build, 18 generic and 6 singular. None compares against a stored figure. Each sets two routes to the same number against each other, or reads a verdict the pipeline recorded.

The generic tests, six per mart, hold the grain and the keys: one row per grain key, keys never null, and every key resolving in its dimension. District keys must be districts in `dim_area`, small-area keys must be codes `dim_lsoa` marks as unchanged in 2021, and months and years must be in `dim_date`. The market mart also refuses a row carrying no measure, the yield mart a null yield, and the small-area mart a null crime count.

| Singular test | Fails when |
|---|---|
| `assert_district_crime_equals_small_area_crime` | crime summed over the market mart's districts differs from crime summed over the small-area fact, which a slipped level filter or a fanned-out join would cause |
| `assert_lsoa_year_crime_conserved` | the small-area mart's annual crime differs from the monthly fact summed over the same small areas and years |
| `assert_yield_districts_match_dim_area_flags` | a district has a yield and `dim_area` does not flag it with both a price index and a rent, or the reverse; the failure names each district |
| `assert_latest_cross_source_verification_passed` | the latest cross-source verification recorded a breached rule, did not succeed, or recorded no verdict at all; the verdict is read from `quality.rule_result` and `quality.pipeline_run`, never recomputed |
| `assert_marts_schema_holds_views_only` | anything other than a view appears in the schema, which has no managed location, so a table would be written to the catalog's own location |
| `assert_rent_and_index_end_within_two_months` | a warning only: the rent and index panels end more than two months apart, which means a release is missing, and the yield mart stops at the older of the two |

The price series in the marts rest on the reconciliation between the transaction series and the published index, which is why its verdict is a test. The crime series rest on the total holding across the published category changes, and the crime load checks that before it writes any crime table, so a crime fact in Gold has already passed it.

No source freshness is configured. dbt reports freshness only against a threshold, and the platform's freshness bounds stay unset until observed runs set them.

## How it runs

**The monthly job.** `marts_build` is a dbt task running `dbt deps` and `dbt build` on the serverless SQL warehouse, with dbt-core 1.12.5 and dbt-databricks 1.12.6, the pair `requirements.txt` pins. It follows the cross-source check and runs whatever that check concluded, since the verdict is one of its tests. `record_marts_build` follows it and writes the build into `quality.pipeline_run`, taking the verdict from the task's result and the views built and tests passed from `run_results.json`. It raises after recording a failed build, because it is the job's last task and a job run takes its outcome from that task.

**Continuous integration.** Two jobs in `.github/workflows/tests.yml`, on every push to `main` and every pull request that changes more than documentation. `dbt-parse` parses the project with warnings as errors and no warehouse behind it, and keeps the generated docs site for 30 days. `dbt-build` builds and tests the views in `marts_ci` as the `dbt-ci` service principal. It holds no stored secret: GitHub signs a token naming this repository and its `databricks-ci` environment, and Databricks exchanges it for a short-lived one. Pull requests from forks and from Dependabot skip that job.

**By hand.** `databricks_src/setup/run_job_tasks.py` runs `marts_build` and `record_marts_build` from the deployed job and nothing upstream of them, so they read whatever the last run left in Gold.

The repository holds one profile, `ci/profiles.yml`, and only a GitHub Actions run in the `databricks-ci` environment can obtain the token it authenticates with. The monthly job runs on a profile Databricks generates for the task.

## Schemas

`marts` and `marts_ci` are created by `databricks_src/setup/01_create_schemas.py` and carry no managed location, since a view stores nothing. dbt never creates a schema: `macros/create_schema.sql` replaces the adapter's `CREATE SCHEMA IF NOT EXISTS` with an error naming that notebook. What the CI build may read and create is granted by `databricks_src/setup/06_grant_ci_access.py`.

## A gap the small-area mart cannot see

`mart_lsoa_year_crime_price` keeps a year once the crime table holds all twelve of its months, judged across the whole police.uk release. One force missing does not make a release short. Greater Manchester Police has published a single month since June 2019, so 1,278 of its small areas with sales in 2024 read as recording no crime that year, and the force's crime is missing or short in every complete year from 2019. Three Devon districts have crime rows in only ten or eleven months of 2022, and the mart keeps that year for them as well. Telling a quiet small area from an unpublished one needs Gold to record which force published which month, and that work is open.

## When a report should read a mart

When a screen needs a fixed grain the star serves badly: an operational table, or a one-page summary that is never sliced. A visual reading a mart can drop the DAX level guard, because the mart already holds one grain. Until a screen like that exists, Power BI reads the star.

## Layout

```
dbt/
├── dbt_project.yml          # views only, contracts enforced, persist_docs on
├── packages.yml             # dbt_utils 1.4.1
├── requirements.txt         # dbt-core 1.12.5 and dbt-databricks 1.12.6, the pair the job installs
├── ci/
│   └── profiles.yml         # the CI profile: OIDC federation into marts_ci, no credential
├── models/
│   ├── _sources.yml         # the Gold and quality tables read
│   └── marts/               # the three views, and _marts.yml with their contracts and generic tests
├── macros/
│   └── create_schema.sql    # refuses to create a schema
├── tests/                   # the six singular tests
└── examples/                # four query pairs, Gold against the mart, results measured 28-10-2026
```
