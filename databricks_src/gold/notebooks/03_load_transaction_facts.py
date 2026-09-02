# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold: transaction fact load
# MAGIC
# MAGIC Loads the three facts built from Price Paid Data: `fact_area_month_price`,
# MAGIC `fact_area_month_transaction_mix` and `fact_lsoa_year_price`.
# MAGIC
# MAGIC One notebook per source family, as with the panel facts. These three are together
# MAGIC for a reason those two were not: they share the join. Every transaction has to be
# MAGIC resolved through the postcode directory before any of them can be built, over 31.4
# MAGIC million rows, and three notebooks would run it three times and could disagree
# MAGIC about the result.
# MAGIC
# MAGIC Three `AuditRun` instances, one per table. `rows_written` is then a real number
# MAGIC rather than a total across whatever this notebook happened to write.
# MAGIC #
# MAGIC Each is gated on its own. All three stand behind `dim_area` and `dim_lsoa` and
# MAGIC behind nothing else, so they are planned together or not at all, which is what
# MAGIC lets the join below sit inside the first run and be read by the other two. That
# MAGIC is an invariant of the chain rather than of this notebook: were it to break, the
# MAGIC later sections would raise on a name the first never bound, which is loud.
# MAGIC
# MAGIC The resolution is measured before it is filtered. Each run records the same six
# MAGIC population counts under `source_rows`, scoped by population, so a run row says
# MAGIC what its source was reduced to without a reader cross-referencing another run.
# MAGIC Those six partition the rows read and carry no denominator. The two further
# MAGIC reductions do carry one, against the resolved population rather than the whole
# MAGIC read, since that is the base each was applied to.
# MAGIC
# MAGIC No DDL here. The Gold contract is declared once in `00_create_gold_tables.py`, and
# MAGIC the column order every write depends on is read back off the created table.
# MAGIC
# MAGIC No freshness. A bound belongs to a source release, and a Gold run reads Silver
# MAGIC tables whose own runs already recorded when their data was published.
# MAGIC
# MAGIC All three run their conformance checks after the write, against the loaded
# MAGIC dimensions rather than the frames that produced them. The foreign keys are
# MAGIC informational, so a key with no dimension row reaches Delta and then disappears
# MAGIC from every rollup keyed on it.
# MAGIC
# MAGIC Precondition: `01_load_dimensions.py` has run. `dim_area` is an input rather than
# MAGIC only a check here, since district ancestry is read off it, and `dim_lsoa` is the
# MAGIC parent the small-area fact conforms to.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.gold.transforms.conformance import (
    assert_keys_conform,
    measure_dimension_coverage,
)
from databricks_src.gold.transforms.fact_area_month_price import (
    transform_fact_area_month_price,
)
from databricks_src.gold.transforms.fact_area_month_transaction_mix import (
    transform_fact_area_month_transaction_mix,
)
from databricks_src.gold.transforms.fact_lsoa_year_price import (
    no_small_area,
    transform_fact_lsoa_year_price,
)
from databricks_src.gold.transforms.transactions import (
    POPULATION_COLUMN,
    RESOLVED,
    is_full_market_value,
    is_resolved,
    resolve_transactions,
)
from databricks_src.orchestration import stage
from databricks_src.utils.gold_write import overwrite

CATALOG = "uk_property_intel"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

PPD = f"{SILVER}.ppd"
DOOGAL = f"{SILVER}.doogal"

DIM_AREA = f"{GOLD}.dim_area"
DIM_DATE = f"{GOLD}.dim_date"
DIM_LSOA = f"{GOLD}.dim_lsoa"

FACT_PRICE = f"{GOLD}.fact_area_month_price"
FACT_MIX = f"{GOLD}.fact_area_month_transaction_mix"
FACT_LSOA_PRICE = f"{GOLD}.fact_lsoa_year_price"

# One timestamp for the whole notebook, carried on all three audit rows so a single load
# reads as one event even though it is three runs.
INGESTION_TS = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The stage plan
# MAGIC
# MAGIC All three answer the same way, since they stand behind the same two dimensions.
# MAGIC `dim_date` is not among them: these facts conform against the calendar without
# MAGIC being built from it, so a calendar a load behind still lets them run and
# MAGIC `assert_keys_conform` is what catches it.

# COMMAND ----------

plan = stage.read_plan(dbutils)  # noqa: F821

for _fact in (
    "fact_area_month_price",
    "fact_area_month_transaction_mix",
    "fact_lsoa_year_price",
):
    _waiting = plan.waiting_on(_fact)
    print(f"{_fact:32}: {'waiting on ' + ', '.join(_waiting) if _waiting else 'runs'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared helpers
# MAGIC
# MAGIC `record_populations` writes the same six counts to whichever run is passed. They
# MAGIC are measured once, on the frame every fact reads, and recorded three times because
# MAGIC a run that cannot say what its source was reduced to is a run someone has to join
# MAGIC to another one to understand.
# MAGIC
# MAGIC `record_area_conformance` mirrors the panel load: both foreign keys checked, then
# MAGIC coverage recorded against the geography dimension only. The calendar is daily and
# MAGIC these facts are monthly or annual, so a fact reaching every period it publishes
# MAGIC still reads as a few percent of `dim_date` and the number carries no signal at any
# MAGIC value. Whether every period resolves is the question worth asking there, and the
# MAGIC check above asks it.

# COMMAND ----------


def record_populations(audit_run, counts: list) -> None:
    """Record the resolution breakdown on one run, one scope per population.

    No denominator. The six scopes partition the rows read, so the base is their sum,
    and a denominator equal to that sum would state it six more times. Matches how the
    dimension load scopes `gold_rows` by area level and by boundary vintage.
    """
    for row in counts:
        audit_run.measure(
            "source_rows", row["rows"], scope=row[POPULATION_COLUMN]
        )


def record_area_conformance(audit_run, fact_table: str, fact_name: str) -> None:
    """Check both foreign keys against the loaded dimensions, then measure coverage."""
    fact = spark.table(fact_table)  # noqa: F821
    areas = spark.table(DIM_AREA)  # noqa: F821

    assert_keys_conform(
        fact,
        areas,
        child_column="area_code",
        parent_column="area_code",
        child_name=fact_name,
        parent_name="dim_area",
    )
    assert_keys_conform(
        fact,
        spark.table(DIM_DATE),  # noqa: F821
        child_column="month_start_date",
        parent_column="date_key",
        child_name=fact_name,
        parent_name="dim_date",
    )

    reached, total = measure_dimension_coverage(fact, areas, "area_code", "area_code")
    audit_run.measure(
        "dimension_rows_with_facts", reached, scope="dim_area", denominator=total
    )
    print(f"{fact_name} keys resolve in dim_area and dim_date")
    print(f"{reached:,} of {total:,} areas carry rows")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve once
# MAGIC
# MAGIC The join runs here and nowhere else. The labelled frame is persisted because five
# MAGIC passes run over it: the population counts, and one aggregate for each of the three
# MAGIC facts, and the small-area fact's unlocated count.
# MAGIC
# MAGIC `DISK_ONLY`, as everywhere else in this project. The node has 16 GB and this frame
# MAGIC is eleven narrow columns over 31.4 million rows; a memory level would spill and
# MAGIC recompute the join rather than reading it back.
# MAGIC
# MAGIC Nothing is dropped in this cell. `resolve_transactions` labels every transaction
# MAGIC and the filters run per fact, so the counts below are taken before anything is
# MAGIC discarded.
# MAGIC
# MAGIC Expected on the July 2026 release: 31,430,611 read, 31,378,089 resolved, 50,505
# MAGIC with no postcode, 2,013 on postcodes the directory does not hold, 4 in Scottish
# MAGIC Borders, and zero in the two populations that were empty when this was measured.

# COMMAND ----------

price_run = stage.open_stage("fact_area_month_price", "gold", INGESTION_TS, plan)

if price_run is not None:
    with price_run.step():
        labelled = resolve_transactions(
            spark.table(PPD),  # noqa: F821
            spark.table(DOOGAL),  # noqa: F821
            spark.table(DIM_AREA),  # noqa: F821
        )
        labelled.persist(StorageLevel.DISK_ONLY)

        # One pass for every population. The base is the denominator rather than a
        # metric of its own: a Gold run reads Silver tables whose own runs recorded
        # what they hold.
        population_counts = (
            labelled.groupBy(POPULATION_COLUMN)
            .agg(F.count(F.lit(1)).alias("rows"))
            .collect()
        )
        source_rows = sum(row["rows"] for row in population_counts)
        resolved_rows = next(
            row["rows"]
            for row in population_counts
            if row[POPULATION_COLUMN] == RESOLVED
        )
        record_populations(price_run, population_counts)

        resolved = labelled.filter(is_resolved())

    for row in sorted(population_counts, key=lambda item: -item["rows"]):
        print(f"{row['rows']:>12,}  {row[POPULATION_COLUMN]}")
    print(f"{source_rows:>12,}  read")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. fact_area_month_price
# MAGIC
# MAGIC Category A only, at four levels. Each level aggregates from the transactions
# MAGIC themselves rather than from the level below it, because a median cannot be
# MAGIC recovered from the medians under it.
# MAGIC
# MAGIC Expected: 124,631 rows. That figure came off the probe under the same rules this
# MAGIC transform implements, so a disagreement means the two differ and one is wrong.

# COMMAND ----------

if price_run is not None:
    with price_run.step():
        excluded = resolved.filter(~is_full_market_value()).count()
        price_run.measure(
            "source_rows",
            excluded,
            scope="excluded_category_b",
            denominator=resolved_rows,
        )

        facts = transform_fact_area_month_price(resolved)
        written = overwrite(facts, FACT_PRICE)
        price_run.measure("gold_rows", written)

    print(f"{excluded:,} category B transactions excluded")
    print(f"{written:,} rows written to {FACT_PRICE}")

# COMMAND ----------

if price_run is not None:
    with price_run.step():
        record_area_conformance(price_run, FACT_PRICE, "fact_area_month_price")

    price_run.succeed(rows_written=written)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. fact_area_month_transaction_mix
# MAGIC
# MAGIC Both sale categories, at the same four levels. The category is part of the key
# MAGIC here, so the population this table covers is larger than the price fact's by
# MAGIC exactly the rows that fact excluded.
# MAGIC
# MAGIC Expected: 1,552,988 rows.

# COMMAND ----------

mix_run = stage.open_stage(
    "fact_area_month_transaction_mix", "gold", INGESTION_TS, plan
)

if mix_run is not None:
    with mix_run.step():
        record_populations(mix_run, population_counts)

        facts = transform_fact_area_month_transaction_mix(resolved)
        written = overwrite(facts, FACT_MIX)
        mix_run.measure("gold_rows", written)

    print(f"{written:,} rows written to {FACT_MIX}")

# COMMAND ----------

if mix_run is not None:
    with mix_run.step():
        record_area_conformance(mix_run, FACT_MIX, "fact_area_month_transaction_mix")

    mix_run.succeed(rows_written=written)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. fact_lsoa_year_price
# MAGIC
# MAGIC Category A only, annual, keyed on the small area alone. No level explode: the
# MAGIC rollup to district and above is `fact_area_month_price`, aggregated from the same
# MAGIC transactions rather than from these medians.
# MAGIC
# MAGIC `no_small_area` was zero on the July 2026 release. It is recorded anyway, because
# MAGIC a zero nobody measured is worth less than a zero someone did, and a directory
# MAGIC release that started placing postcodes in a district without an output area would
# MAGIC otherwise shrink this table silently.
# MAGIC
# MAGIC Expected: 1,134,233 rows over 35,672 small areas.

# COMMAND ----------

lsoa_run = stage.open_stage("fact_lsoa_year_price", "gold", INGESTION_TS, plan)

if lsoa_run is not None:
    with lsoa_run.step():
        record_populations(lsoa_run, population_counts)
        lsoa_run.measure(
            "source_rows",
            excluded,
            scope="excluded_category_b",
            denominator=resolved_rows,
        )

        unlocated = resolved.filter(no_small_area() & is_full_market_value()).count()
        lsoa_run.measure(
            "source_rows", unlocated, scope="no_small_area", denominator=resolved_rows
        )

        facts = transform_fact_lsoa_year_price(resolved)
        written = overwrite(facts, FACT_LSOA_PRICE)
        lsoa_run.measure("gold_rows", written)

    print(f"{unlocated:,} category A transactions carry no small-area code")
    print(f"{written:,} rows written to {FACT_LSOA_PRICE}")

# COMMAND ----------

if lsoa_run is not None:
    with lsoa_run.step():
        fact = spark.table(FACT_LSOA_PRICE)  # noqa: F821
        small_areas = spark.table(DIM_LSOA)  # noqa: F821

        assert_keys_conform(
            fact,
            small_areas,
            child_column="lsoa_code",
            parent_column="lsoa_code",
            child_name="fact_lsoa_year_price",
            parent_name="dim_lsoa",
        )
        assert_keys_conform(
            fact,
            spark.table(DIM_DATE),  # noqa: F821
            child_column="year_start_date",
            parent_column="date_key",
            child_name="fact_lsoa_year_price",
            parent_name="dim_date",
        )

        reached, total = measure_dimension_coverage(
            fact, small_areas, "lsoa_code", "lsoa_code"
        )
        lsoa_run.measure(
            "dimension_rows_with_facts", reached, scope="dim_lsoa", denominator=total
        )

    print("fact_lsoa_year_price keys resolve in dim_lsoa and dim_date")
    print(f"{reached:,} of {total:,} small areas carry rows")

    lsoa_run.succeed(rows_written=written)

# COMMAND ----------

# Released against the run that built it, not against the last one to read it, since
# the three are gated apart even though the chain plans them together.
if price_run is not None:
    _ = labelled.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Row counts against the probe. 124,631, 1,552,988 and 1,134,233.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_area_month_price' AS table, count(*) AS rows,
# MAGIC        count(DISTINCT area_code) AS areas,
# MAGIC        min(month_start_date) AS first_period, max(month_start_date) AS last_period
# MAGIC FROM uk_property_intel.gold.fact_area_month_price
# MAGIC UNION ALL
# MAGIC SELECT 'fact_area_month_transaction_mix', count(*), count(DISTINCT area_code),
# MAGIC        min(month_start_date), max(month_start_date)
# MAGIC FROM uk_property_intel.gold.fact_area_month_transaction_mix
# MAGIC UNION ALL
# MAGIC SELECT 'fact_lsoa_year_price', count(*), count(DISTINCT lsoa_code),
# MAGIC        min(year_start_date), max(year_start_date)
# MAGIC FROM uk_property_intel.gold.fact_lsoa_year_price
# MAGIC ORDER BY table

# COMMAND ----------

# MAGIC %md
# MAGIC Cells per level. Expected 120,095 districts, 3,402 regions, 756 nations and 378
# MAGIC composite months, and every district-month the price fact holds should hold a mix
# MAGIC row too.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.area_level,
# MAGIC        count(DISTINCT a.area_code) AS areas,
# MAGIC        count(*) AS price_rows,
# MAGIC        min(p.month_start_date) AS first_month,
# MAGIC        max(p.month_start_date) AS last_month
# MAGIC FROM uk_property_intel.gold.fact_area_month_price p
# MAGIC JOIN uk_property_intel.gold.dim_area a USING (area_code)
# MAGIC GROUP BY a.area_level
# MAGIC ORDER BY a.area_level

# COMMAND ----------

# MAGIC %md
# MAGIC The reconciliation between the two area facts. They do not agree on a bare count,
# MAGIC because the mix fact keeps both sale categories and the price fact keeps category
# MAGIC A. The relationship that does hold is that the price fact's count equals the mix
# MAGIC fact's count over category A, and every row below should read zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH mix_a AS (
# MAGIC   SELECT area_code, month_start_date, sum(transaction_count) AS mix_count
# MAGIC   FROM uk_property_intel.gold.fact_area_month_transaction_mix
# MAGIC   WHERE ppd_category_type = 'A'
# MAGIC   GROUP BY area_code, month_start_date
# MAGIC )
# MAGIC SELECT count(*) AS cells_compared,
# MAGIC        count_if(p.transaction_count IS NULL) AS missing_from_price,
# MAGIC        count_if(m.mix_count IS NULL) AS missing_from_mix,
# MAGIC        count_if(p.transaction_count <> m.mix_count) AS disagreeing_counts
# MAGIC FROM uk_property_intel.gold.fact_area_month_price p
# MAGIC FULL OUTER JOIN mix_a m USING (area_code, month_start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC `dim_lsoa.has_price` was set from every transaction, and this table carries
# MAGIC category A alone. The probe measured the two as the same 35,672 codes, so both
# MAGIC differences below should read zero. A non-zero `flagged_without_rows` means a
# MAGIC small area whose only sales were category B, which is the case the exclusion was
# MAGIC checked against before it was adopted.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH fact_codes AS (
# MAGIC    SELECT DISTINCT lsoa_code
# MAGIC    FROM uk_property_intel.gold.fact_lsoa_year_price
# MAGIC  )
# MAGIC  SELECT count_if(d.has_price) AS flagged_in_dimension,
# MAGIC         count(f.lsoa_code) AS codes_in_fact,
# MAGIC         count_if(d.has_price AND f.lsoa_code IS NULL) AS flagged_without_rows,
# MAGIC         count_if(f.lsoa_code IS NOT NULL AND NOT d.has_price) AS rows_without_the_flag
# MAGIC  FROM uk_property_intel.gold.dim_lsoa d
# MAGIC  LEFT JOIN fact_codes f ON d.lsoa_code = f.lsoa_code

# COMMAND ----------

# MAGIC %md
# MAGIC ## What these runs recorded

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.source, r.status, r.rows_written, r.error_type,
# MAGIC        m.metric, m.scope, m.value_numeric, m.denominator
# MAGIC FROM uk_property_intel.quality.pipeline_run r
# MAGIC LEFT JOIN uk_property_intel.quality.pipeline_metric m USING (run_id)
# MAGIC WHERE r.layer = 'gold' AND r.source IN (
# MAGIC   'fact_area_month_price', 'fact_area_month_transaction_mix', 'fact_lsoa_year_price'
# MAGIC )
# MAGIC ORDER BY r.started_ts DESC, r.source, m.metric, m.scope
