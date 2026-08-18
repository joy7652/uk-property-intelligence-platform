# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold: dimension load
# MAGIC
# MAGIC Loads all four dimensions of the star schema from Silver:
# MAGIC `dim_date`, `dim_area`, `dim_crime_type` and `dim_lsoa`.
# MAGIC
# MAGIC One notebook rather than four, for two reasons. The crime table is scanned once
# MAGIC and feeds both `dim_crime_type` and `dim_lsoa`; splitting them would scan 96
# MAGIC million rows twice. And every step from 3.3 onwards has one precondition, that all
# MAGIC four dimensions exist, rather than three different sets to track.
# MAGIC
# MAGIC Four `AuditRun` instances all the same, one per table. `rows_written` is then a
# MAGIC real number rather than a total across whatever this notebook happened to write,
# MAGIC and the existing dashboard query works unchanged with `source = 'dim_lsoa'`.
# MAGIC
# MAGIC No DDL here. The Gold contract is declared once in `00_create_gold_tables.py`, and
# MAGIC the column order every write depends on is read back off the created table rather
# MAGIC than restated.
# MAGIC
# MAGIC No freshness either. A bound belongs to a source release, and a Gold run reads
# MAGIC Silver tables whose own runs already recorded when their data was published.
# MAGIC
# MAGIC Order matters in one place: `dim_area` is written before `dim_lsoa` is checked
# MAGIC against it, because the conformance check runs against the loaded dimension rather
# MAGIC than the frame that produced it.

# COMMAND ----------

from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.gold.transforms.dim_area import (
    measure_published_areas,
    transform_dim_area,
)
from databricks_src.gold.transforms.dim_crime_type import (
    measure_publication_window,
    transform_dim_crime_type,
)
from databricks_src.gold.transforms.dim_date import transform_dim_date
from databricks_src.gold.transforms.dim_lsoa import (
    assert_districts_conform,
    measure_small_areas,
    transform_dim_lsoa,
)
from databricks_src.quality.audit.writer import AuditRun

CATALOG = "uk_property_intel"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

BOE = f"{SILVER}.boe_base_rate"
HPI = f"{SILVER}.hpi"
ONS = f"{SILVER}.ons_private_rents"
DOOGAL = f"{SILVER}.doogal"
POLICE = f"{SILVER}.police_street_crime"
PPD = f"{SILVER}.ppd"

DIM_DATE = f"{GOLD}.dim_date"
DIM_AREA = f"{GOLD}.dim_area"
DIM_CRIME_TYPE = f"{GOLD}.dim_crime_type"
DIM_LSOA = f"{GOLD}.dim_lsoa"

# One timestamp for the whole notebook, carried on all four audit rows so a single load
# reads as one event even though it is four runs.
INGESTION_TS = datetime.now(timezone.utc)

# The calendar runs to the end of the year this load happens in, so every fact date has
# a row waiting for it. Passed to the transform rather than computed inside it, which is
# what keeps the transform deterministic under test.
CALENDAR_END = date(INGESTION_TS.year, 12, 31)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write helpers
# MAGIC
# MAGIC `INSERT OVERWRITE` matches on position, so a projection that has drifted from the
# MAGIC declared order would load values into the wrong columns without failing. The order
# MAGIC is read off the target rather than written out here: the table is the contract, and
# MAGIC a second copy of the column list in this notebook would be free to disagree with it.

# COMMAND ----------


def target_columns(table: str) -> list[str]:
    """Column order as the created table declares it."""
    return [field.name for field in spark.table(table).schema.fields]  # noqa: F821


def overwrite(df, table: str) -> int:
    """Replace a Gold table's contents, guarding the projection against the target."""
    declared = target_columns(table)
    if list(df.columns) != declared:
        raise ValueError(
            f"{table}: the transform produces {list(df.columns)}, the table declares "
            f"{declared}. INSERT OVERWRITE matches on position, so this would load "
            "values into the wrong columns."
        )
    view = f"_staging_{table.rsplit('.', 1)[-1]}"
    df.createOrReplaceTempView(view)
    spark.sql(f"INSERT OVERWRITE {table} TABLE {view}")  # noqa: F821
    return spark.table(table).count()  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_date
# MAGIC
# MAGIC The rate intervals expanded to their days. Independent of everything else here and
# MAGIC the cheapest of the four, so it goes first: a failure costs nothing to rerun.

# COMMAND ----------

date_run = AuditRun(source="dim_date", layer="gold", ingestion_ts=INGESTION_TS)
date_run.start()
print(f"dim_date run {date_run.run_id}")

with date_run.step():
    calendar = transform_dim_date(
        base_rate_df=spark.table(BOE),  # noqa: F821
        end_date=CALENDAR_END,
    )
    written = overwrite(calendar, DIM_DATE)
    date_run.measure("gold_rows", written)

date_run.succeed(rows_written=written)
print(f"{written:,} days written to {DIM_DATE}, through {CALENDAR_END}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_area
# MAGIC
# MAGIC The union of the three published area sets. Written before `dim_lsoa` below, whose
# MAGIC districts are checked against this table once it holds rows.
# MAGIC
# MAGIC The measured frame is persisted because the transform runs several guards over it
# MAGIC and the write is one more action. It shuffles the postcode directory to district
# MAGIC grain, which is the only expensive part of this step.
# MAGIC
# MAGIC `gold_rows` is recorded once for the table and once per level. A level appearing or
# MAGIC vanishing is then visible without a registry edit, which is what `scope` is for.

# COMMAND ----------

area_run = AuditRun(source="dim_area", layer="gold", ingestion_ts=INGESTION_TS)
area_run.start()
print(f"dim_area run {area_run.run_id}")

with area_run.step():
    measured_areas = measure_published_areas(
        hpi_df=spark.table(HPI),  # noqa: F821
        ons_df=spark.table(ONS),  # noqa: F821
        doogal_df=spark.table(DOOGAL),  # noqa: F821
    )
    measured_areas.persist(StorageLevel.DISK_ONLY)

    areas = transform_dim_area(measured_areas)
    written = overwrite(areas, DIM_AREA)
    area_run.measure("gold_rows", written)

# COMMAND ----------

with area_run.step():
    # One pass over 432 rows for both breakdowns.
    profile = (
        spark.table(DIM_AREA)  # noqa: F821
        .groupBy("area_level")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.count_if(F.col("code_source") == F.lit("derived")).alias("derived"),
        )
        .collect()
    )
    for row in profile:
        area_run.measure("gold_rows", row["rows"], scope=row["area_level"])

    derived = sum(row["derived"] for row in profile)
    area_run.measure("derived_area_codes", derived)

_ = measured_areas.unpersist()
area_run.succeed(rows_written=written)
print(f"{written:,} areas written to {DIM_AREA}")
print(f"{derived} carry a code this project assigned")
for row in sorted(profile, key=lambda item: item["area_level"]):
    print(f"  {row['area_level']:<20}{row['rows']:>5}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crime projection
# MAGIC
# MAGIC Three columns off the crime table, persisted. `dim_crime_type` aggregates it to
# MAGIC type grain and `dim_lsoa` distincts its codes, so without this the 96 million rows
# MAGIC are scanned twice for two questions one pass answers.
# MAGIC
# MAGIC The read sits inside `dim_crime_type`'s run because it is the first consumer. A
# MAGIC failure here records against that table and stops the notebook, so `dim_lsoa`'s run
# MAGIC never opens and no row is left dangling.

# COMMAND ----------

crime_run = AuditRun(source="dim_crime_type", layer="gold", ingestion_ts=INGESTION_TS)
crime_run.start()
print(f"dim_crime_type run {crime_run.run_id}")

with crime_run.step():
    crime = spark.table(POLICE).select("lsoa_code", "crime_type", "crime_month")  # noqa: F821
    crime.persist(StorageLevel.DISK_ONLY)
    crime_rows = crime.count()

print(f"{crime_rows:,} crime rows projected to three columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_crime_type
# MAGIC
# MAGIC Sixteen rows. The publication window is measured from the crime frame and the era
# MAGIC and predecessor come from the authored map, which the transform checks against the
# MAGIC months actually published.

# COMMAND ----------

with crime_run.step():
    measured_types = measure_publication_window(crime)
    measured_types.persist(StorageLevel.DISK_ONLY)

    crime_types = transform_dim_crime_type(measured_types)
    written = overwrite(crime_types, DIM_CRIME_TYPE)
    crime_run.measure("gold_rows", written)

_ = measured_types.unpersist()
crime_run.succeed(rows_written=written)
print(f"{written} crime types written to {DIM_CRIME_TYPE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT crime_type, first_published_month, last_published_month, is_current,
# MAGIC        vocabulary_era, predecessor_crime_type
# MAGIC FROM uk_property_intel.gold.dim_crime_type
# MAGIC ORDER BY vocabulary_era, crime_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_lsoa
# MAGIC
# MAGIC The heaviest step. The crime frame is already cached; the transaction join is the
# MAGIC second full scan and exists only to answer `has_price`, since the postcode
# MAGIC directory says which areas hold postcodes and never which hold transactions.
# MAGIC
# MAGIC Conformance runs after the write, against the loaded `dim_area`. The foreign key is
# MAGIC informational, so an area naming a district with no row would reach Delta and drop
# MAGIC out of every rollup silently.

# COMMAND ----------

lsoa_run = AuditRun(source="dim_lsoa", layer="gold", ingestion_ts=INGESTION_TS)
lsoa_run.start()
print(f"dim_lsoa run {lsoa_run.run_id}")

with lsoa_run.step():
    measured_areas_small = measure_small_areas(
        doogal_df=spark.table(DOOGAL),  # noqa: F821
        police_df=crime,
        ppd_df=spark.table(PPD),  # noqa: F821
    )
    measured_areas_small.persist(StorageLevel.DISK_ONLY)

    small_areas = transform_dim_lsoa(measured_areas_small)
    written = overwrite(small_areas, DIM_LSOA)
    lsoa_run.measure("gold_rows", written)

_ = crime.unpersist()
_ = measured_areas_small.unpersist()
print(f"{written:,} small areas written to {DIM_LSOA}")

# COMMAND ----------

with lsoa_run.step():
    assert_districts_conform(spark.table(DIM_LSOA), spark.table(DIM_AREA))  # noqa: F821
print(f"every district in {DIM_LSOA} resolves in {DIM_AREA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What dim_lsoa measured
# MAGIC
# MAGIC The three populations the design cites, each against the whole table. A share
# MAGIC rather than a count, because the total moves with each release and a bare count
# MAGIC read two releases apart would compare against a base that changed underneath it.

# COMMAND ----------

with lsoa_run.step():
    # One pass for every figure below.
    totals = (
        spark.table(DIM_LSOA)  # noqa: F821
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.count_if(F.col("district_assignment") == F.lit("majority")).alias("majority"),
            F.count_if(F.col("has_crime")).alias("with_crime"),
            F.count_if(F.col("has_price")).alias("with_price"),
        )
        .collect()[0]
    )
    by_vintage = (
        spark.table(DIM_LSOA)  # noqa: F821
        .groupBy("boundary_vintage")
        .agg(F.count(F.lit(1)).alias("rows"))
        .collect()
    )

    total = totals["rows"]
    lsoa_run.measure("majority_assigned_small_areas", totals["majority"], denominator=total)
    lsoa_run.measure("small_areas_with_crime", totals["with_crime"], denominator=total)
    lsoa_run.measure("small_areas_with_price", totals["with_price"], denominator=total)
    for row in by_vintage:
        lsoa_run.measure("gold_rows", row["rows"], scope=row["boundary_vintage"])

lsoa_run.succeed(rows_written=written)
print(f"{totals['majority']:,} of {total:,} areas straddle two districts")
print(f"{totals['with_crime']:,} carry crime, {totals['with_price']:,} carry a price")
for row in sorted(by_vintage, key=lambda item: item["boundary_vintage"]):
    print(f"  {row['boundary_vintage']:<12}{row['rows']:>8,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_date' AS table, count(*) AS rows FROM uk_property_intel.gold.dim_date
# MAGIC UNION ALL SELECT 'dim_area', count(*) FROM uk_property_intel.gold.dim_area
# MAGIC UNION ALL SELECT 'dim_crime_type', count(*) FROM uk_property_intel.gold.dim_crime_type
# MAGIC UNION ALL SELECT 'dim_lsoa', count(*) FROM uk_property_intel.gold.dim_lsoa
# MAGIC ORDER BY table

# COMMAND ----------

# MAGIC %md
# MAGIC Every straddling area should sit well clear of a half. The measured floor is 80
# MAGIC percent, so a row near 0.5 means the postcode counts have moved.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT district_assignment,
# MAGIC        count(*) AS areas,
# MAGIC        min(majority_share) AS weakest,
# MAGIC        max(majority_share) AS strongest
# MAGIC FROM uk_property_intel.gold.dim_lsoa
# MAGIC GROUP BY district_assignment
# MAGIC ORDER BY district_assignment

# COMMAND ----------

# MAGIC %md
# MAGIC Codes exclusive to the 2011 boundaries carry crime and no price, which the table
# MAGIC constrains. This is the same fact read as a distribution rather than as a check.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT boundary_vintage,
# MAGIC        count(*) AS areas,
# MAGIC        count_if(has_crime) AS with_crime,
# MAGIC        count_if(has_price) AS with_price
# MAGIC FROM uk_property_intel.gold.dim_lsoa
# MAGIC GROUP BY boundary_vintage
# MAGIC ORDER BY boundary_vintage

# COMMAND ----------

# MAGIC %md
# MAGIC Districts outside England carry no region, which is structural rather than a gap:
# MAGIC only England is divided into regions. The count should be 65 of 361.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country_name,
# MAGIC        count(*) AS districts,
# MAGIC        count_if(region_code IS NOT NULL) AS with_region
# MAGIC FROM uk_property_intel.gold.dim_area
# MAGIC WHERE area_level = 'district'
# MAGIC GROUP BY country_name
# MAGIC ORDER BY country_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## What these runs recorded

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.source, r.status, r.rows_written, r.error_type,
# MAGIC        m.metric, m.scope, m.value_numeric, m.denominator
# MAGIC FROM uk_property_intel.quality.pipeline_run r
# MAGIC LEFT JOIN uk_property_intel.quality.pipeline_metric m USING (run_id)
# MAGIC WHERE r.layer = 'gold'
# MAGIC ORDER BY r.started_ts DESC, r.source, m.metric, m.scope
