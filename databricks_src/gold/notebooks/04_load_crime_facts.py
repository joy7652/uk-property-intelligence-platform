# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold: crime fact load
# MAGIC
# MAGIC Loads the four facts built from street-level crime: `fact_lsoa_month_crime`,
# MAGIC `fact_lsoa_month_crime_total`, `fact_area_month_crime` and
# MAGIC `fact_area_month_crime_total`.
# MAGIC
# MAGIC One notebook, as with the transaction facts, and for the same reason: all four
# MAGIC need the same 92 million resolved records, and four notebooks would resolve them
# MAGIC four times and could disagree about the result.
# MAGIC
# MAGIC Small-area tables first, area tables from them. A count is additive, so summing
# MAGIC the small-area aggregate at each level gives what counting the records there
# MAGIC would, and the Verify section checks that it does against the composite. The
# MAGIC price facts could not do this, because a median cannot be recovered from the
# MAGIC medians below it.
# MAGIC
# MAGIC Four `AuditRun` instances, one per table, so `rows_written` is a real number
# MAGIC rather than a total across whatever this notebook wrote. All four names are
# MAGIC already in `GOLD_TABLES`.
# MAGIC
# MAGIC Anti-social behaviour never reaches the two type tables. It is dropped in the
# MAGIC shared aggregate and refused by check constraint as well, so a row arriving there
# MAGIC fails the write rather than loading as a number nobody questions.
# MAGIC
# MAGIC No DDL here. The Gold contract is declared once in `00_create_gold_tables.py`, and
# MAGIC the column order every write depends on is read back off the created table.
# MAGIC
# MAGIC No freshness. A bound belongs to a source release, and a Gold run reads a Silver
# MAGIC table whose own run already recorded when its data was published.
# MAGIC
# MAGIC Precondition: `01_load_dimensions.py` has run. `dim_lsoa` and `dim_area` are
# MAGIC inputs here rather than only checks, since the district a small area belongs to
# MAGIC and the levels above it are both read off them.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.gold.transforms.conformance import (
    assert_keys_conform,
    measure_dimension_coverage,
)
from databricks_src.gold.transforms.crime import (
    POPULATION_COLUMN,
    is_resolved,
    resolve_crime,
    small_area_totals,
    small_area_type_counts,
)
from databricks_src.gold.transforms.fact_area_month_crime import (
    transform_fact_area_month_crime,
)
from databricks_src.gold.transforms.fact_area_month_crime_total import (
    transform_fact_area_month_crime_total,
)
from databricks_src.gold.transforms.fact_lsoa_month_crime import (
    transform_fact_lsoa_month_crime,
)
from databricks_src.gold.transforms.fact_lsoa_month_crime_total import (
    transform_fact_lsoa_month_crime_total,
)
from databricks_src.quality.audit.writer import AuditRun
from databricks_src.utils.gold_write import overwrite

CATALOG = "uk_property_intel"
POLICE = f"{CATALOG}.silver.police_street_crime"

DIM_AREA = f"{CATALOG}.gold.dim_area"
DIM_DATE = f"{CATALOG}.gold.dim_date"
DIM_LSOA = f"{CATALOG}.gold.dim_lsoa"
DIM_CRIME_TYPE = f"{CATALOG}.gold.dim_crime_type"

FACT_LSOA_CRIME = f"{CATALOG}.gold.fact_lsoa_month_crime"
FACT_LSOA_TOTAL = f"{CATALOG}.gold.fact_lsoa_month_crime_total"
FACT_AREA_CRIME = f"{CATALOG}.gold.fact_area_month_crime"
FACT_AREA_TOTAL = f"{CATALOG}.gold.fact_area_month_crime_total"

# One timestamp for the whole notebook, carried on all four audit rows so a single load
# reads as one event even though it is four runs.
INGESTION_TS = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared helpers
# MAGIC
# MAGIC `record_populations` writes the same counts to whichever run is passed. They are
# MAGIC measured once, on the frame every fact reads, and recorded four times because a
# MAGIC run that cannot say what its source was reduced to is a run someone has to join
# MAGIC to another one to understand. No denominator: the scopes partition the records
# MAGIC read, so the base is their sum.
# MAGIC
# MAGIC `record_conformance` takes the dimension a table keys on, because the two grains
# MAGIC key on different ones. Coverage is recorded against that dimension only. The
# MAGIC calendar is daily and these facts are monthly, so a fact reaching every month it
# MAGIC publishes still reads as a few percent of `dim_date`; whether every month resolves
# MAGIC is the question worth asking there, and the check above asks it.

# COMMAND ----------


def record_populations(audit_run, counts: list) -> None:
    """Record the resolution breakdown on one run, one scope per population."""
    for row in counts:
        audit_run.measure(
            "source_rows", row["records"], scope=row[POPULATION_COLUMN]
        )


def record_conformance(
    audit_run,
    fact_table: str,
    fact_name: str,
    key_column: str,
    dimension_table: str,
    dimension_name: str,
) -> None:
    """Check the geography and calendar keys, then measure geographic coverage."""
    fact = spark.table(fact_table)  # noqa: F821
    dimension = spark.table(dimension_table)  # noqa: F821

    assert_keys_conform(
        fact,
        dimension,
        child_column=key_column,
        parent_column=key_column,
        child_name=fact_name,
        parent_name=dimension_name,
    )
    assert_keys_conform(
        fact,
        spark.table(DIM_DATE),  # noqa: F821
        child_column="month_start_date",
        parent_column="date_key",
        child_name=fact_name,
        parent_name="dim_date",
    )

    reached, total = measure_dimension_coverage(
        fact, dimension, key_column, key_column
    )
    audit_run.measure(
        "dimension_rows_with_facts",
        reached,
        scope=dimension_name,
        denominator=total,
    )
    print(f"{fact_name} keys resolve in {dimension_name} and dim_date")
    print(f"{reached:,} of {total:,} {dimension_name} rows carry crime")


def check_crime_types(audit_run, fact_table: str, fact_name: str) -> None:
    """The third key on the two type tables, checked and measured like the other two.

    Coverage here is 15 of 16 by design: anti-social behaviour has a row in the
    dimension and none in these tables. Recorded anyway, because 14 of 16 would mean a
    published type reached no fact and nothing else would say so.
    """
    fact = spark.table(fact_table)  # noqa: F821
    types = spark.table(DIM_CRIME_TYPE)  # noqa: F821

    assert_keys_conform(
        fact,
        types,
        child_column="crime_type",
        parent_column="crime_type",
        child_name=fact_name,
        parent_name="dim_crime_type",
    )

    reached, total = measure_dimension_coverage(fact, types, "crime_type", "crime_type")
    audit_run.measure(
        "dimension_rows_with_facts",
        reached,
        scope="dim_crime_type",
        denominator=total,
    )
    print(f"{reached} of {total} crime types carry rows in {fact_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve once
# MAGIC
# MAGIC The join runs here and nowhere else. Crime already carries a small-area code, so
# MAGIC this is a lookup of the district and the levels above it rather than a postcode
# MAGIC resolution, and no coordinate is read.
# MAGIC
# MAGIC `DISK_ONLY`, as everywhere else in this project. Seven narrow columns over 96
# MAGIC million rows; the transaction frame compressed to 0.23 GiB on twelve columns over
# MAGIC 31.4 million, so this should sit near a gigabyte, and a memory level would spill
# MAGIC and recompute rather than reading back.
# MAGIC
# MAGIC Nothing is dropped in this cell. `resolve_crime` labels every record and the
# MAGIC filters run per aggregate, so the counts below are taken before anything is
# MAGIC discarded.
# MAGIC
# MAGIC Expected on the July 2026 release: 96,092,836 read, 92,352,547 resolved,
# MAGIC 3,740,289 carrying no small-area code, and zero in the three populations that were
# MAGIC empty when this was measured.

# COMMAND ----------

lsoa_crime_run = AuditRun(
    source="fact_lsoa_month_crime", layer="gold", ingestion_ts=INGESTION_TS
)
lsoa_crime_run.start()
print(f"fact_lsoa_month_crime run {lsoa_crime_run.run_id}")

with lsoa_crime_run.step():
    labelled = resolve_crime(
        spark.table(POLICE),  # noqa: F821
        spark.table(DIM_LSOA),  # noqa: F821
        spark.table(DIM_AREA),  # noqa: F821
    )
    labelled.persist(StorageLevel.DISK_ONLY)

    population_counts = (
        labelled.groupBy(POPULATION_COLUMN)
        .agg(F.count(F.lit(1)).alias("records"))
        .collect()
    )
    source_rows = sum(row["records"] for row in population_counts)
    record_populations(lsoa_crime_run, population_counts)

    resolved = labelled.filter(is_resolved())

for row in sorted(population_counts, key=lambda item: -item["records"]):
    print(f"{row['records']:>12,}  {row[POPULATION_COLUMN]}")
print(f"{source_rows:>12,}  read")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate once, at small-area grain
# MAGIC
# MAGIC Two frames, both persisted, and every one of the four facts is a projection or a
# MAGIC rollup of one of them. The type counts feed `fact_lsoa_month_crime` and
# MAGIC `fact_area_month_crime`; the totals feed the other two. A small-area table and its
# MAGIC area table therefore cannot disagree about what a small area holds.
# MAGIC
# MAGIC Expected: 25,984,439 and 6,328,185 rows.

# COMMAND ----------

with lsoa_crime_run.step():
    type_counts = small_area_type_counts(resolved)
    type_counts.persist(StorageLevel.DISK_ONLY)
    totals = small_area_totals(resolved)
    totals.persist(StorageLevel.DISK_ONLY)
    print(f"{type_counts.count():>12,}  small-area type counts")
    print(f"{totals.count():>12,}  small-area totals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. fact_lsoa_month_crime
# MAGIC
# MAGIC Expected: 25,984,439 rows over 36,751 small areas. `CLUSTER BY (month_start_date,
# MAGIC lsoa_code)`, the first liquid-clustered table this project writes, so the write
# MAGIC time here is a new measurement rather than something inherited from phase 3.4.

# COMMAND ----------

with lsoa_crime_run.step():
    facts = transform_fact_lsoa_month_crime(type_counts)
    written = overwrite(facts, FACT_LSOA_CRIME)
    lsoa_crime_run.measure("gold_rows", written)

print(f"{written:,} rows written to {FACT_LSOA_CRIME}")

# COMMAND ----------

with lsoa_crime_run.step():
    record_conformance(
        lsoa_crime_run,
        FACT_LSOA_CRIME,
        "fact_lsoa_month_crime",
        "lsoa_code",
        DIM_LSOA,
        "dim_lsoa",
    )
    check_crime_types(lsoa_crime_run, FACT_LSOA_CRIME, "fact_lsoa_month_crime")

lsoa_crime_run.succeed(rows_written=written)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. fact_lsoa_month_crime_total
# MAGIC
# MAGIC Expected: 6,328,185 rows, of which 102,735 carry anti-social behaviour and no
# MAGIC other crime, and 1,272,887 carry no anti-social behaviour. Both measures come from
# MAGIC one aggregate, so both zeroes are counts.

# COMMAND ----------

lsoa_total_run = AuditRun(
    source="fact_lsoa_month_crime_total", layer="gold", ingestion_ts=INGESTION_TS
)
lsoa_total_run.start()
print(f"fact_lsoa_month_crime_total run {lsoa_total_run.run_id}")

with lsoa_total_run.step():
    record_populations(lsoa_total_run, population_counts)
    facts = transform_fact_lsoa_month_crime_total(totals)
    written = overwrite(facts, FACT_LSOA_TOTAL)
    lsoa_total_run.measure("gold_rows", written)

print(f"{written:,} rows written to {FACT_LSOA_TOTAL}")

# COMMAND ----------

with lsoa_total_run.step():
    record_conformance(
        lsoa_total_run,
        FACT_LSOA_TOTAL,
        "fact_lsoa_month_crime_total",
        "lsoa_code",
        DIM_LSOA,
        "dim_lsoa",
    )

lsoa_total_run.succeed(rows_written=written)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. fact_area_month_crime
# MAGIC
# MAGIC Summed up from the type counts at four levels. Expected: 736,822 rows over 330
# MAGIC areas. The declared estimate was roughly 940,000, which assumed a flat 15 types in
# MAGIC every area-month; three published vocabularies were in use across the period, so
# MAGIC the reachable ceiling is nearer 758,670.

# COMMAND ----------

area_crime_run = AuditRun(
    source="fact_area_month_crime", layer="gold", ingestion_ts=INGESTION_TS
)
area_crime_run.start()
print(f"fact_area_month_crime run {area_crime_run.run_id}")

with area_crime_run.step():
    record_populations(area_crime_run, population_counts)
    facts = transform_fact_area_month_crime(type_counts)
    written = overwrite(facts, FACT_AREA_CRIME)
    area_crime_run.measure("gold_rows", written)

print(f"{written:,} rows written to {FACT_AREA_CRIME}")

# COMMAND ----------

with area_crime_run.step():
    record_conformance(
        area_crime_run,
        FACT_AREA_CRIME,
        "fact_area_month_crime",
        "area_code",
        DIM_AREA,
        "dim_area",
    )
    check_crime_types(area_crime_run, FACT_AREA_CRIME, "fact_area_month_crime")

area_crime_run.succeed(rows_written=written)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. fact_area_month_crime_total
# MAGIC
# MAGIC Expected: 61,681 rows, against 61,710 possible across 330 areas and 187 months.
# MAGIC Nine of them carry anti-social behaviour and no other crime.

# COMMAND ----------

area_total_run = AuditRun(
    source="fact_area_month_crime_total", layer="gold", ingestion_ts=INGESTION_TS
)
area_total_run.start()
print(f"fact_area_month_crime_total run {area_total_run.run_id}")

with area_total_run.step():
    record_populations(area_total_run, population_counts)
    facts = transform_fact_area_month_crime_total(totals)
    written = overwrite(facts, FACT_AREA_TOTAL)
    area_total_run.measure("gold_rows", written)

print(f"{written:,} rows written to {FACT_AREA_TOTAL}")

# COMMAND ----------

with area_total_run.step():
    record_conformance(
        area_total_run,
        FACT_AREA_TOTAL,
        "fact_area_month_crime_total",
        "area_code",
        DIM_AREA,
        "dim_area",
    )

area_total_run.succeed(rows_written=written)

# COMMAND ----------

_ = totals.unpersist()
_ = type_counts.unpersist()
_ = labelled.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Row counts against the probe. 25,984,439, 6,328,185, 736,822 and 61,681.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_lsoa_month_crime' AS table, count(*) AS rows,
# MAGIC        count(DISTINCT lsoa_code) AS keys,
# MAGIC        min(month_start_date) AS first_month, max(month_start_date) AS last_month
# MAGIC FROM uk_property_intel.gold.fact_lsoa_month_crime
# MAGIC UNION ALL
# MAGIC SELECT 'fact_lsoa_month_crime_total', count(*), count(DISTINCT lsoa_code),
# MAGIC        min(month_start_date), max(month_start_date)
# MAGIC FROM uk_property_intel.gold.fact_lsoa_month_crime_total
# MAGIC UNION ALL
# MAGIC SELECT 'fact_area_month_crime', count(*), count(DISTINCT area_code),
# MAGIC        min(month_start_date), max(month_start_date)
# MAGIC FROM uk_property_intel.gold.fact_area_month_crime
# MAGIC UNION ALL
# MAGIC SELECT 'fact_area_month_crime_total', count(*), count(DISTINCT area_code),
# MAGIC        min(month_start_date), max(month_start_date)
# MAGIC FROM uk_property_intel.gold.fact_area_month_crime_total
# MAGIC ORDER BY table

# COMMAND ----------

# MAGIC %md
# MAGIC The rollup identity. Summing the small-area table gives what the area table holds
# MAGIC at every level, and the composite equals the whole non-anti-social population,
# MAGIC 67,886,868. Both differences should read zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT sum(crime_count) FROM uk_property_intel.gold.fact_lsoa_month_crime)
# MAGIC     AS small_area_total,
# MAGIC   (SELECT sum(crime_count) FROM uk_property_intel.gold.fact_area_month_crime
# MAGIC    WHERE area_code = 'K04000001') AS composite_total,
# MAGIC   (SELECT sum(crime_count_excl_asb)
# MAGIC    FROM uk_property_intel.gold.fact_area_month_crime_total
# MAGIC    WHERE area_code = 'K04000001') AS composite_total_table

# COMMAND ----------

# MAGIC %md
# MAGIC Each total table against its own type table. `crime_count_excl_asb` is the sum of
# MAGIC every type for that key, which is what the column comment states. Every column
# MAGIC below should read zero except the cells compared.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH summed AS (
# MAGIC   SELECT area_code, month_start_date, sum(crime_count) AS type_sum
# MAGIC   FROM uk_property_intel.gold.fact_area_month_crime
# MAGIC   GROUP BY area_code, month_start_date
# MAGIC )
# MAGIC SELECT count(*) AS cells_compared,
# MAGIC        count_if(t.crime_count_excl_asb IS NULL) AS missing_from_total,
# MAGIC        count_if(s.type_sum IS NULL AND t.crime_count_excl_asb > 0)
# MAGIC          AS missing_from_types,
# MAGIC        count_if(t.crime_count_excl_asb <> coalesce(s.type_sum, 0))
# MAGIC          AS disagreeing_counts,
# MAGIC        count_if(t.crime_count_excl_asb = 0) AS anti_social_only
# MAGIC FROM uk_property_intel.gold.fact_area_month_crime_total t
# MAGIC FULL OUTER JOIN summed s USING (area_code, month_start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Areas reached per level, which should be the same 330 the transaction facts reach:
# MAGIC 318 districts, 9 regions, 2 nations and the England and Wales composite.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.area_level, count(DISTINCT a.area_code) AS areas, count(*) AS rows
# MAGIC FROM uk_property_intel.gold.fact_area_month_crime_total t
# MAGIC JOIN uk_property_intel.gold.dim_area a USING (area_code)
# MAGIC GROUP BY a.area_level
# MAGIC ORDER BY a.area_level

# COMMAND ----------

# MAGIC %md
# MAGIC Anti-social behaviour reaches neither type table, which the check constraints also
# MAGIC refuse. Both counts should read zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM uk_property_intel.gold.fact_lsoa_month_crime
# MAGIC    WHERE crime_type = 'Anti-social behaviour') AS in_small_area_table,
# MAGIC   (SELECT count(*) FROM uk_property_intel.gold.fact_area_month_crime
# MAGIC    WHERE crime_type = 'Anti-social behaviour') AS in_area_table

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
# MAGIC   'fact_lsoa_month_crime', 'fact_lsoa_month_crime_total',
# MAGIC   'fact_area_month_crime', 'fact_area_month_crime_total'
# MAGIC )
# MAGIC ORDER BY r.started_ts DESC, r.source, m.metric, m.scope
