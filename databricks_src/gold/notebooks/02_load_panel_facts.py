# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold: area panel fact load
# MAGIC
# MAGIC Loads the two facts built from a published area panel: `fact_area_month_hpi` and
# MAGIC `fact_area_month_rent`.
# MAGIC
# MAGIC One notebook per source family. These two share no read, unlike the crime facts that
# MAGIC follow, so grouping them saves nothing at runtime. They are together because they are
# MAGIC the same shape, they land in one phase, and they total under 200,000 rows. Two
# MAGIC notebooks for that would be ceremony.
# MAGIC
# MAGIC Two `AuditRun` instances, one per table, as in the dimension load. `rows_written` is
# MAGIC then a real number rather than a total across whatever this notebook happened to
# MAGIC write.
# MAGIC
# MAGIC No DDL here. The Gold contract is declared once in `00_create_gold_tables.py`, and
# MAGIC the column order every write depends on is read back off the created table.
# MAGIC
# MAGIC No freshness. A bound belongs to a source release, and a Gold run reads Silver
# MAGIC tables whose own runs already recorded when their data was published.
# MAGIC
# MAGIC Both facts run their conformance checks after the write, against the loaded
# MAGIC dimensions rather than the frames that produced them. The foreign keys are
# MAGIC informational, so a key with no dimension row reaches Delta and then disappears from
# MAGIC every rollup keyed on it.
# MAGIC
# MAGIC Precondition: `01_load_dimensions.py` has run. The rent transform reads `dim_area`
# MAGIC to resolve the eight rental market areas ONS publishes with no code, so for that fact
# MAGIC the dimension is an input rather than only a check.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.storagelevel import StorageLevel

from databricks_src.gold.transforms.conformance import (
    assert_keys_conform,
    measure_dimension_coverage,
)
from databricks_src.gold.transforms.fact_area_month_hpi import (
    no_measure as hpi_no_measure,
)
from databricks_src.gold.transforms.fact_area_month_hpi import (
    transform_fact_area_month_hpi,
)
from databricks_src.gold.transforms.fact_area_month_rent import (
    no_measure as rent_no_measure,
)
from databricks_src.gold.transforms.fact_area_month_rent import (
    transform_fact_area_month_rent,
)
from databricks_src.quality.audit.writer import AuditRun
from databricks_src.utils.gold_write import overwrite

CATALOG = "uk_property_intel"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

HPI = f"{SILVER}.hpi"
ONS = f"{SILVER}.ons_private_rents"

DIM_AREA = f"{GOLD}.dim_area"
DIM_DATE = f"{GOLD}.dim_date"
FACT_HPI = f"{GOLD}.fact_area_month_hpi"
FACT_RENT = f"{GOLD}.fact_area_month_rent"

# One timestamp for the whole notebook, carried on both audit rows so a single load reads
# as one event even though it is two runs.
INGESTION_TS = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conformance helper
# MAGIC
# MAGIC The write helpers this notebook used to define are shared with the dimension
# MAGIC load and the transaction load, in `databricks_src/utils/gold_write.py`. The
# MAGIC column order every write depends on is still read off the created table there:
# MAGIC the table is the contract, and a second copy of the list would be free to
# MAGIC disagree with it.

# COMMAND ----------


def record_area_conformance(audit_run, fact_table: str, fact_name: str) -> None:
    """Check both foreign keys against the loaded dimensions, then measure coverage.

    Coverage is recorded against `dim_area` only. The calendar is daily and both facts
    are monthly, so a fact reaching every month it publishes still reads as a few percent
    of `dim_date` and the number carries no signal at any value. Whether every month
    resolves is the question worth asking there, and the check above asks it.
    """
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
# MAGIC ## 1. fact_area_month_hpi
# MAGIC
# MAGIC A projection and a rename. Every measure exists in Silver under the same name at the
# MAGIC same type, so nothing is computed and nothing is cast.
# MAGIC
# MAGIC The Silver frame is persisted because three passes run over it: the count of rows
# MAGIC carrying no measure, the grain check inside the transform, and the write.
# MAGIC
# MAGIC `rows_without_a_measure` is expected to be zero here. It is recorded anyway, because
# MAGIC a zero nobody measured is worth less than a zero someone did, and a release that
# MAGIC started publishing empty rows would otherwise change the table silently.

# COMMAND ----------

hpi_run = AuditRun(
    source="fact_area_month_hpi", layer="gold", ingestion_ts=INGESTION_TS
)
hpi_run.start()
print(f"fact_area_month_hpi run {hpi_run.run_id}")

with hpi_run.step():
    hpi = spark.table(HPI)  # noqa: F821
    hpi.persist(StorageLevel.DISK_ONLY)

    # One pass for both. The base is the denominator rather than a metric of its own:
    # a Gold run reads Silver tables whose own runs recorded what they hold.
    read_rows = hpi.count()
    without_measure = hpi.filter(hpi_no_measure()).count()
    hpi_run.measure(
        "rows_without_a_measure", without_measure, denominator=read_rows
    )

    facts = transform_fact_area_month_hpi(hpi)
    written = overwrite(facts, FACT_HPI)
    hpi_run.measure("gold_rows", written)

_ = hpi.unpersist()
print(f"{read_rows:,} Silver rows read, {without_measure:,} carrying no measure")
print(f"{written:,} rows written to {FACT_HPI}")

# COMMAND ----------

with hpi_run.step():
    record_area_conformance(hpi_run, FACT_HPI, "fact_area_month_hpi")

hpi_run.succeed(rows_written=written)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. fact_area_month_rent
# MAGIC
# MAGIC The same shape plus one thing the index fact does not need. ONS publishes no area
# MAGIC code for the eight Northern Irish rental market areas, so the transform reads
# MAGIC `dim_area` and resolves them by name against the codes this project assigned.
# MAGIC
# MAGIC `rows_without_a_measure` is the population that rule was written for. Northern Ireland
# MAGIC lags the other nations and ONS marks its unpublished months unavailable across every
# MAGIC measure, so the count is expected to be small, non-zero, and to move with each
# MAGIC release.

# COMMAND ----------

rent_run = AuditRun(
    source="fact_area_month_rent", layer="gold", ingestion_ts=INGESTION_TS
)
rent_run.start()
print(f"fact_area_month_rent run {rent_run.run_id}")

with rent_run.step():
    ons = spark.table(ONS)  # noqa: F821
    ons.persist(StorageLevel.DISK_ONLY)

    read_rows = ons.count()
    without_measure = ons.filter(rent_no_measure()).count()
    rent_run.measure(
        "rows_without_a_measure", without_measure, denominator=read_rows
    )

    facts = transform_fact_area_month_rent(ons, spark.table(DIM_AREA))  # noqa: F821
    written = overwrite(facts, FACT_RENT)
    rent_run.measure("gold_rows", written)

_ = ons.unpersist()
print(f"{read_rows:,} Silver rows read, {without_measure:,} carrying no measure")
print(f"{written:,} rows written to {FACT_RENT}")

# COMMAND ----------

with rent_run.step():
    record_area_conformance(rent_run, FACT_RENT, "fact_area_month_rent")

rent_run.succeed(rows_written=written)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_area_month_hpi' AS table, count(*) AS rows,
# MAGIC        count(DISTINCT area_code) AS areas,
# MAGIC        min(month_start_date) AS first_month, max(month_start_date) AS last_month
# MAGIC FROM uk_property_intel.gold.fact_area_month_hpi
# MAGIC UNION ALL
# MAGIC SELECT 'fact_area_month_rent', count(*), count(DISTINCT area_code),
# MAGIC        min(month_start_date), max(month_start_date)
# MAGIC FROM uk_property_intel.gold.fact_area_month_rent
# MAGIC ORDER BY table

# COMMAND ----------

# MAGIC %md
# MAGIC The eight rental market areas ONS publishes with no code should all carry rows here,
# MAGIC under the codes `dim_area` assigned them. Eight rows, every one with a count.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.area_code, a.area_name, count(*) AS months,
# MAGIC        min(f.month_start_date) AS first_month, max(f.month_start_date) AS last_month
# MAGIC FROM uk_property_intel.gold.fact_area_month_rent f
# MAGIC JOIN uk_property_intel.gold.dim_area a USING (area_code)
# MAGIC WHERE a.code_source = 'derived'
# MAGIC GROUP BY a.area_code, a.area_name
# MAGIC ORDER BY a.area_name

# COMMAND ----------

# MAGIC %md
# MAGIC Where the seasonally adjusted series exists. Region, nation and composite only, and
# MAGIC Northern Ireland carries none at nation level although the United Kingdom composite
# MAGIC containing it does. District and county should be zero throughout, which is why those
# MAGIC two columns serve a benchmark line and never the area a screen is about.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.area_level,
# MAGIC        count(*) AS rows,
# MAGIC        count(DISTINCT f.area_code) AS areas,
# MAGIC        count_if(f.price_index_seasonally_adjusted IS NOT NULL) AS with_sa
# MAGIC FROM uk_property_intel.gold.fact_area_month_hpi f
# MAGIC JOIN uk_property_intel.gold.dim_area a USING (area_code)
# MAGIC GROUP BY a.area_level
# MAGIC ORDER BY a.area_level

# COMMAND ----------

# MAGIC %md
# MAGIC Areas carrying both a price index and a rent, which is where a yield can be computed.
# MAGIC Districts only: the rent series publishes Scotland and Northern Ireland on broad
# MAGIC rental market areas, which conform to nothing below nation and so pair with no price.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.area_level,
# MAGIC        count(DISTINCT a.area_code) AS areas,
# MAGIC        count(DISTINCT h.area_code) AS with_price,
# MAGIC        count(DISTINCT r.area_code) AS with_rent,
# MAGIC        count(DISTINCT CASE WHEN h.area_code IS NOT NULL AND r.area_code IS NOT NULL
# MAGIC                            THEN a.area_code END) AS with_both
# MAGIC FROM uk_property_intel.gold.dim_area a
# MAGIC LEFT JOIN (SELECT DISTINCT area_code FROM uk_property_intel.gold.fact_area_month_hpi) h
# MAGIC   USING (area_code)
# MAGIC LEFT JOIN (SELECT DISTINCT area_code FROM uk_property_intel.gold.fact_area_month_rent) r
# MAGIC   USING (area_code)
# MAGIC GROUP BY a.area_level
# MAGIC ORDER BY a.area_level

# COMMAND ----------

# MAGIC %md
# MAGIC ## What these runs recorded

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.source, r.status, r.rows_written, r.error_type,
# MAGIC        m.metric, m.scope, m.value_numeric, m.denominator
# MAGIC FROM uk_property_intel.quality.pipeline_run r
# MAGIC LEFT JOIN uk_property_intel.quality.pipeline_metric m USING (run_id)
# MAGIC WHERE r.layer = 'gold' AND r.source LIKE 'fact_%'
# MAGIC ORDER BY r.started_ts DESC, r.source, m.metric, m.scope
