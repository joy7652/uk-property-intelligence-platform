# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Street-level crime (Police.uk)
# MAGIC
# MAGIC Reads every police.uk archive from the Bronze Volume and overwrites
# MAGIC `uk_property_intel.silver.police_street_crime`.
# MAGIC
# MAGIC Each archive is a rolling snapshot restating up to 36 months, so the same
# MAGIC (month, force) file appears in several of them. The newest copy of each is
# MAGIC chosen from the member names before anything is decompressed, which removes the
# MAGIC overlap without a shuffle and without needing a row key to tell copies apart.
# MAGIC
# MAGIC Spark cannot read inside a ZIP, so the winning members are extracted to
# MAGIC cluster-local disk. All seven archives expand past what one node holds, so they
# MAGIC are handled one at a time into a staging table and promoted in a single write.
# MAGIC The local path makes this notebook single-node only: on a multi-node cluster the
# MAGIC executors do not share the driver's filesystem and the read fails outright.
# MAGIC
# MAGIC **Staging carries the same CHECK constraints as the target**, so a violation
# MAGIC fails on the archive that caused it rather than at promotion after all seven.
# MAGIC
# MAGIC **The loop resumes.** Archives already in staging are skipped, so a fault part
# MAGIC way through costs only the archives after it. Set `RESET_STAGING` when the
# MAGIC transform itself changes, or the table will mix old and new output.
# MAGIC
# MAGIC What the run measured is written to `uk_property_intel.quality.pipeline_run`
# MAGIC and `pipeline_metric` rather than only printed. `check_rules` already returns
# MAGIC eight measures and two vocabularies per archive, so this is mostly a matter of
# MAGIC recording what the validation pass computed. Per-archive rows carry the archive
# MAGIC in `scope`.

# COMMAND ----------

import os
import re
import shutil
import zipfile
from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from databricks_src.quality.audit.writer import AuditRun
from databricks_src.silver.transforms.police import (
    DATASET,
    SILVER_COLUMNS,
    check_rules,
    coordinate_box_check,
    crime_id_month_spread,
    crime_type_check,
    identical_row_duplicates,
    select_newest,
    shape_police,
    silver_table_ddl,
    unusable_members,
)

VOLUME_PATH = "/Volumes/uk_property_intel/bronze/police"
TARGET_TABLE = "uk_property_intel.silver.police_street_crime"
STAGING_TABLE = "uk_property_intel.silver.police_street_crime_staging"

STAGE_DIR = "/local_disk0/police_stage"

# Set when rebuilding from a Bronze copy kept on purpose. The freshness bound cannot
# tell a deliberate rebuild from a stale release.
SKIP_FRESHNESS = False

# Rebuild staging from nothing. Set this whenever the transform module changes: the
# rows already in staging were produced by the old code and the resume cannot tell.
RESET_STAGING = False

# Local file header of a ZIP archive. The archives are fetched from a static URL
# pattern, so a failed fetch can land an error page under the expected name.
ZIP_MAGIC_BYTES = b"PK\x03\x04"

# Snapshot label from the landed filename. ADF names the blob, not police.uk.
SNAPSHOT = re.compile(r"(\d{4}-\d{2})")

# One timestamp for the whole load, so every row carries the same one whichever
# archive it came out of. A resumed run stamps its own, which is correct: the rows it
# writes were read then.
INGESTION_TS = datetime.now(timezone.utc)

# Single-row invariants. Applied to both tables from one list, so staging cannot
# accept a row the target would reject. The domain and box expressions are generated
# from the transform module's own constants.
CONSTRAINTS = [
    ("crime_month_is_month_start", "day(crime_month) = 1"),
    ("snapshot_month_is_month_start", "day(snapshot_month) = 1"),
    ("crime_year_matches_month", "crime_year = year(crime_month)"),
    ("crime_month_at_or_after_series_start", "crime_month >= DATE '2010-12-01'"),
    ("crime_month_at_or_before_snapshot", "crime_month <= snapshot_month"),
    ("coordinates_paired", "(latitude IS NULL) = (longitude IS NULL)"),
    ("coordinates_located", "latitude IS NULL OR NOT (latitude = 0 AND longitude = 0)"),
    ("coordinates_in_range", coordinate_box_check()),
    ("crime_type_known", crime_type_check()),
]

# COMMAND ----------

run = AuditRun(source="police", layer="silver", ingestion_ts=INGESTION_TS)
run.start()
print(f"run {run.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locate the archives
# MAGIC
# MAGIC The Volume roots at the source folder and the full-load and incremental
# MAGIC archives land under different folders, so this walks the tree.

# COMMAND ----------


def walk(path):
    for entry in dbutils.fs.ls(path):  # noqa: F821
        if entry.isDir():
            yield from walk(entry.path.rstrip("/"))
        else:
            yield entry


# COMMAND ----------

with run.step():
    located = {}
    sizes = {}
    for entry in walk(VOLUME_PATH):
        if not entry.name.lower().endswith(".zip"):
            continue
        match = SNAPSHOT.search(entry.name)
        if not match:
            raise ValueError(
                f"{entry.name} carries no snapshot month. The snapshot decides which "
                "copy of a slot wins and cannot be guessed."
            )
        if match.group(1) in located:
            raise ValueError(
                f"Two archives label themselves {match.group(1)}. The snapshot is the "
                "dedup tiebreak and must be unique."
            )
        located[match.group(1)] = entry.path.replace("dbfs:", "", 1)
        sizes[match.group(1)] = entry.size

    if not located:
        raise FileNotFoundError(f"No archive under {VOLUME_PATH}")

    for snapshot, path in sorted(located.items()):
        with open(path, "rb") as handle:
            magic = handle.read(len(ZIP_MAGIC_BYTES))
        if magic != ZIP_MAGIC_BYTES:
            raise ValueError(f"{path} is not a ZIP archive. Found {magic!r}.")
        print(f"{snapshot}  {path}  {sizes[snapshot] / 1024 ** 3:.2f} GB")

    # The snapshot labels come from ADF's blob names rather than police.uk, so they
    # move whenever the pipeline rewrites a name. Total size is the signal that the
    # archives themselves changed.
    run.measure("source_files", len(located))
    run.measure("source_bytes", sum(sizes.values()))

print(f"\n{len(located)} archive(s), magic bytes ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan the load
# MAGIC
# MAGIC Central directories only. This decides which member of which archive supplies
# MAGIC each (month, force) before a single byte is decompressed.

# COMMAND ----------

with run.step():
    listings = {}
    for snapshot, path in sorted(located.items()):
        with zipfile.ZipFile(path) as archive:
            listings[snapshot] = [
                info.filename for info in archive.infolist() if not info.is_dir()
            ]

    unusable = unusable_members(listings)
    if unusable:
        raise ValueError(
            "Police archives hold members the naming convention does not fit, so the "
            f"month or force they belong to is unknown: {unusable}"
        )

    selection = select_newest(listings)

    plan = {}
    for (month, force), (snapshot, name) in selection.items():
        plan.setdefault(snapshot, []).append(name)

    months = sorted({month for month, _ in selection})
    run.measure("winning_slots", len(selection))
print(f"{len(selection):,} winning {DATASET} slots over {len(months)} months, "
      f"{months[0]} to {months[-1]}\n")
for snapshot in sorted(located):
    names = plan.get(snapshot, [])
    covered = sorted(
        month for (month, _), (owner, _) in selection.items() if owner == snapshot
    )
    span = f"{covered[0]} to {covered[-1]}" if covered else "nothing"
    print(f"  {snapshot}: {len(names):>5} file(s)   {span}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create both tables
# MAGIC
# MAGIC The column list is generated from the transform module, so the DDL cannot drift
# MAGIC from the cast types, and the domain and box constraints are generated from the
# MAGIC same constants the transform enforces. Tables are created once
# MAGIC (`IF NOT EXISTS`). Single-row invariants live here; everything multi-row belongs
# MAGIC in the chispa test.
# MAGIC
# MAGIC `ADD CONSTRAINT` validates every existing row before it attaches, so on a
# MAGIC populated table it is a full scan. Dropping and re-adding all nine each run
# MAGIC costs nine scans of the target for no change, so what is already attached is
# MAGIC read first and only differences are touched.

# COMMAND ----------

TARGET_COMMENT = (
    "Police.uk street-level crime and anti-social behaviour from December 2010. "
    "Territorial forces cover England, Wales and Northern Ireland; British Transport "
    "Police additionally cover Great Britain, so Scottish railway locations appear "
    "although no Scottish territorial force publishes here. One row per published "
    "incident record. The source has no natural key: Crime ID is blank for "
    "anti-social behaviour, and Northern Ireland reuses a pool of references so a "
    "small set of ids recurs monthly across the whole series and identifies nothing. "
    "Dates are truncated to month and coordinates snapped to shared points, so "
    "identical rows are usually distinct incidents rather than duplicates and are "
    "counted rather than removed. Built from overlapping rolling snapshots, keeping "
    "the newest copy of each (month, force) file. snapshot_month is the archive that "
    "supplied the row: outcome state is only as settled as that vintage, so outcome "
    "rates are not comparable across years without it. Greater Manchester stops "
    "supplying after June 2019. Coordinates are null where the source could not place "
    "the crime within 20km or published a position outside the UK."
)

_ = spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    {silver_table_ddl()}
    )
    USING DELTA
    PARTITIONED BY (crime_year)
    COMMENT '{TARGET_COMMENT}'
    """
)

if RESET_STAGING:
    spark.sql(f"DROP TABLE IF EXISTS {STAGING_TABLE}")  # noqa: F821
    print(f"dropped {STAGING_TABLE}")

_ = spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
    {silver_table_ddl()}
    )
    USING DELTA
    PARTITIONED BY (crime_year)
    COMMENT 'Scratch table for the police street crime load. Carries the same CHECK constraints as the target so a violation fails on the archive that caused it. Dropped once promoted.'
    """
)

# COMMAND ----------


def attached_constraints(table):
    """The CHECK constraints Delta already holds, read from table properties."""
    return {
        row["key"].split("delta.constraints.", 1)[1]: row["value"]
        for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()  # noqa: F821
        if row["key"].startswith("delta.constraints.")
    }


def apply_constraints(table, constraints):
    """Attach only what is missing or has changed, and drop what is no longer
    configured.

    Delta may store the expression in a normalised form rather than as written. A
    difference is printed with both texts, so a re-add shows up as a reported
    mismatch rather than as unexplained minutes.
    """
    attached = attached_constraints(table)
    for name, expression in constraints:
        current = attached.get(name)
        if current == expression:
            print(f"  {name}: unchanged")
            continue
        if current is None:
            print(f"  {name}: adding")
        else:
            print(f"  {name}: differs")
            print(f"      attached   {current}")
            print(f"      configured {expression}")
            spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")  # noqa: F821
        spark.sql(  # noqa: F821
            f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression})"
        )
    for name in sorted(set(attached) - {name for name, _ in constraints}):
        print(f"  {name}: attached but no longer configured, dropping")
        spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")  # noqa: F821


# COMMAND ----------

for table in (TARGET_TABLE, STAGING_TABLE):
    print(table)
    apply_constraints(table, CONSTRAINTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load, one archive at a time
# MAGIC
# MAGIC Each archive is extracted, validated in a single pass, transformed, appended,
# MAGIC and its extracted copy deleted before the next one starts, so local disk holds
# MAGIC one archive's winners at a time.
# MAGIC
# MAGIC `check_rules` returns the counts and vocabularies it observed, so the discovery
# MAGIC output below costs nothing beyond the pass that was already needed to validate.

# COMMAND ----------

done = {
    row[0].strftime("%Y-%m")
    for row in spark.table(STAGING_TABLE)  # noqa: F821
    .select("snapshot_month")
    .distinct()
    .collect()
}
pending = [snapshot for snapshot in sorted(plan) if snapshot not in done]
# What this run extracts and validates, which is fewer than the archives present when
# the loop resumes. The measures below are scoped to these archives alone.
run.measure("archives_read", len(pending))
print(f"already in staging: {sorted(done) or 'nothing'}")
print(f"to load           : {pending or 'nothing, staging is complete'}")

# COMMAND ----------

observed = {}

with run.step():
    for snapshot in pending:
        names = plan[snapshot]

        shutil.rmtree(STAGE_DIR, ignore_errors=True)
        os.makedirs(STAGE_DIR, exist_ok=True)
        with zipfile.ZipFile(located[snapshot]) as archive:
            for name in names:
                archive.extract(name, STAGE_DIR)
        extracted = sum(
            os.path.getsize(os.path.join(root, file))
            for root, _, files in os.walk(STAGE_DIR)
            for file in files
        )
        print(f"\n{snapshot}: extracted {len(names):,} file(s), "
              f"{extracted / 1024 ** 3:.2f} GB")

        # Types are asserted in the transform, never inferred. FAILFAST aborts on a row
        # whose field count does not match the header rather than padding it with nulls.
        # nullValue is pinned so a blank Crime ID lands as null whatever the runtime
        # default, which every anti-social behaviour row depends on.
        raw = (
            spark.read.option("header", True)  # noqa: F821
            .option("inferSchema", False)
            .option("quote", '"')
            .option("escape", '"')
            .option("nullValue", "")
            .option("mode", "FAILFAST")
            .csv([f"file://{STAGE_DIR}/{name}" for name in names])
            .withColumn("_member_path", F.col("_metadata.file_path"))
        )
        # The validation pass and the write are two actions over this frame. Without the
        # persist the CSVs are parsed for both.
        raw.persist(StorageLevel.DISK_ONLY)

        result = check_rules(raw, snapshot)
        observed[snapshot] = result
        print(f"{snapshot}: {result.rows:,} rows, every rule clean")
        for name, count in result.measures.items():
            print(f"    {name:<34}{count:>12,}  {count / result.rows:7.3%}")
        for column, values in result.vocabularies.items():
            print(f"    {column}: {len(values)} value(s)")

        # The validation pass already computed these. Each measure carries the
        # archive's own row count as its base, so summing numerator and denominator
        # across archives gives the correct share for the load.
        for name, count in result.measures.items():
            run.measure(name, count, scope=snapshot, denominator=result.rows)

        silver = shape_police(
            raw_df=raw,
            source_file=located[snapshot],
            snapshot=snapshot,
            ingestion_ts=INGESTION_TS,
        )
        # saveAsTable matches on name, so a missing column would append nulls rather than
        # fail. The projection is checked against the order the DDL was generated from
        # instead.
        assert tuple(silver.columns) == SILVER_COLUMNS, silver.columns
        silver.write.mode("append").saveAsTable(STAGING_TABLE)

        _ = raw.unpersist()
        shutil.rmtree(STAGE_DIR, ignore_errors=True)
        print(f"{snapshot}: appended to staging")

# Summed across the archives this run read, not per archive, so it stays comparable
# with the other five sources. Not recorded at all when the loop was skipped: a zero
# would claim the archives were read and found empty, and archives_read already says
# how many were read.
if observed:
    run.measure("source_rows", sum(result.rows for result in observed.values()))

print(f"\nstaging holds {spark.table(STAGING_TABLE).count():,} rows")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC The crime type vocabulary across every archive loaded in this run. A category
# MAGIC absent from the transform's set would have failed the load, so this is the
# MAGIC record of what was accepted rather than a check.

# COMMAND ----------

for column in ("crime_type", "last_outcome_category"):
    values = sorted(
        {value for result in observed.values() for value in result.vocabularies[column]}
    )
    if values:
        run.measure(f"{column}_values", values)
    print(f"{column}: {len(values)} value(s)")
    for value in values:
        print(f"  {value}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Force coverage
# MAGIC
# MAGIC Which forces filed in the newest month, against those filing anywhere in the
# MAGIC last twelve. Measured rather than asserted: Greater Manchester stops after June
# MAGIC 2019, and any force that stops permanently would make a per-force assertion fire
# MAGIC forever. The twelve-month window keeps a long-departed force out of the
# MAGIC denominator while still catching one that stopped recently.
# MAGIC
# MAGIC One `groupBy` over a 45-value key carries the row count, the force count, the
# MAGIC newest month, and the coverage list, so nothing below needs its own scan.

# COMMAND ----------

staged = spark.table(STAGING_TABLE)  # noqa: F821
staged.persist(StorageLevel.DISK_ONLY)

with run.step():
    by_force = (
        staged.groupBy("force")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.max("crime_month").alias("last_month"),
        )
        .collect()
    )

    rows = sum(item["rows"] for item in by_force)
    newest_month = max(item["last_month"] for item in by_force)

    run.measure("silver_rows", rows)
    lag = run.freshness(newest_month, skip=SKIP_FRESHNESS)

    # First month of the twelve ending at newest_month. Month arithmetic on a
    # zero-based ordinal: year * 12 + month - 1 keeps December in its own year, which
    # a year * 12 + month form does not.
    _ordinal = newest_month.year * 12 + newest_month.month - 1 - 11
    window_start = date(_ordinal // 12, _ordinal % 12 + 1, 1)

    active = [item for item in by_force if item["last_month"] >= window_start]
    absent = sorted(
        item["force"] for item in active if item["last_month"] != newest_month
    )

    run.measure(
        "entities_in_newest_period",
        len(active) - len(absent),
        denominator=len(active),
    )
    if absent:
        run.measure("entities_absent_from_newest_period", absent)

print(f"{rows:,} rows, {len(by_force)} forces, newest month {newest_month} "
      f"({lag} days before this run)")
print(f"{len(active) - len(absent)} of {len(active)} forces active since "
      f"{window_start} filed for {newest_month}")
print(f"stopped filing: {absent or 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate measurement
# MAGIC
# MAGIC The source has no key, so the size of each duplicate population is what decides
# MAGIC whether anything further is needed. Both of these are shuffles over the whole
# MAGIC table and are the most expensive thing in the notebook after the extraction.

# COMMAND ----------

# staged is already bound and cached by the coverage cell above.
identical = identical_row_duplicates(staged)
extra = identical.agg(F.sum(F.col("count") - 1)).collect()[0][0] or 0
groups = identical.count()
run.measure("rows_identical_to_another", extra, denominator=rows)
print(f"{rows:,} rows")
print(f"identical rows: {groups:,} group(s), {extra:,} beyond the first "
      f"({extra / rows:.3%})")
identical.select(
    "crime_month", "force", "crime_type", "location", "latitude", "count"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Split on locatability. A record with no coordinates, no LSOA and no crime id
# MAGIC carries nothing that could distinguish it from the next one in the same
# MAGIC force-month, so those collapse for want of detail rather than through double
# MAGIC counting.

# COMMAND ----------

identical.select(
    F.when(F.col("latitude").isNull(), "unlocated").otherwise("located").alias("kind"),
    "count",
).groupBy("kind").agg(
    F.count("*").alias("groups"),
    F.sum(F.col("count") - 1).alias("rows_beyond_the_first"),
    F.max("count").alias("largest_group"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Crime ids under more than one month. A handful of months is a force correcting
# MAGIC a date. A span of years is an id that is not a per-crime reference at all.

# COMMAND ----------

spread = crime_id_month_spread(staged)
spread.select(
    F.when(F.col("months") > 60, "over 60")
    .when(F.col("months") > 12, "13 to 60")
    .when(F.col("months") > 3, "4 to 12")
    .otherwise(F.col("months").cast("string"))
    .alias("months_spanned"),
    "rows",
    "forces",
).groupBy("months_spanned").agg(
    F.count("*").alias("crime_ids"),
    F.sum("rows").alias("rows"),
    F.max("forces").alias("max_forces_for_one_id"),
).orderBy(F.col("crime_ids").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote
# MAGIC
# MAGIC `INSERT OVERWRITE` preserves the table schema, comment, partitioning, and CHECK
# MAGIC constraints; only the data is replaced. Staging already satisfies the same
# MAGIC constraints, so this is a copy rather than a second validation.

# COMMAND ----------

with run.step():
    spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} TABLE {STAGING_TABLE}")  # noqa: F821
    written = spark.table(TARGET_TABLE).count()  # noqa: F821

_ = staged.unpersist()
run.succeed(rows_written=written)
print(f"Wrote {written:,} rows to {TARGET_TABLE}")
print(f"run {run.run_id} recorded as succeeded")

# COMMAND ----------

# MAGIC %md
# MAGIC Staging is dropped by hand, not here. Keeping it until the verification below
# MAGIC has been read means a fault found at that point costs a promotion rather than a
# MAGIC reload.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows,
# MAGIC        count(DISTINCT force) AS forces,
# MAGIC        count(DISTINCT crime_month) AS months,
# MAGIC        min(crime_month) AS earliest,
# MAGIC        max(crime_month) AS latest,
# MAGIC        count(DISTINCT snapshot_month) AS vintages
# MAGIC FROM uk_property_intel.silver.police_street_crime

# COMMAND ----------

# MAGIC %md
# MAGIC Every month must come from exactly one archive. Two vintages for one month means
# MAGIC the file selection let both copies through.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT crime_month, count(DISTINCT snapshot_month) AS vintages
# MAGIC FROM uk_property_intel.silver.police_street_crime
# MAGIC GROUP BY crime_month
# MAGIC HAVING count(DISTINCT snapshot_month) > 1
# MAGIC ORDER BY crime_month

# COMMAND ----------

# MAGIC %md
# MAGIC Rows and forces by year. A year that drops against its neighbours is a force
# MAGIC that stopped supplying rather than a fall in crime: Greater Manchester leaves
# MAGIC after June 2019 and never returns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT crime_year,
# MAGIC        count(*) AS rows,
# MAGIC        count(DISTINCT force) AS forces,
# MAGIC        count_if(force = 'greater-manchester') AS manchester_rows
# MAGIC FROM uk_property_intel.silver.police_street_crime
# MAGIC GROUP BY crime_year
# MAGIC ORDER BY crime_year

# COMMAND ----------

# MAGIC %md
# MAGIC Observation lag, which is what `snapshot_month` exists for. A crime recorded
# MAGIC shortly before its archive was built has had no time for an outcome; one from
# MAGIC years earlier has. Outcome rates are not comparable across these bands.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT snapshot_month,
# MAGIC        min(months_between(snapshot_month, crime_month)) AS min_lag_months,
# MAGIC        max(months_between(snapshot_month, crime_month)) AS max_lag_months,
# MAGIC        count(*) AS rows,
# MAGIC        round(100 * count_if(last_outcome_category IS NULL) / count(*), 1)
# MAGIC          AS pct_no_outcome
# MAGIC FROM uk_property_intel.silver.police_street_crime
# MAGIC GROUP BY snapshot_month
# MAGIC ORDER BY snapshot_month

# COMMAND ----------

# MAGIC %md
# MAGIC British Transport Police are the only force publishing Scottish locations, which
# MAGIC is why the coordinate box reaches to 61N. Any other force north of 55.9 would be
# MAGIC a fault.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT force, count(*) AS rows, round(max(latitude), 4) AS max_lat
# MAGIC FROM uk_property_intel.silver.police_street_crime
# MAGIC WHERE latitude > 55.9
# MAGIC GROUP BY force
# MAGIC ORDER BY rows DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Crime type vocabulary by era. The pre-2013 categories should stop where the
# MAGIC current ones begin, with no month carrying both vocabularies.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT crime_type,
# MAGIC        min(crime_month) AS first_month,
# MAGIC        max(crime_month) AS last_month,
# MAGIC        count(*) AS rows
# MAGIC FROM uk_property_intel.silver.police_street_crime
# MAGIC GROUP BY crime_type
# MAGIC ORDER BY first_month, rows DESC

# COMMAND ----------

# MAGIC %md
# MAGIC LSOA is the join to Doogal and the route to postcode-level analysis. Police.uk
# MAGIC follows the ONS boundary vintage of the day, so older months carry 2011 codes and
# MAGIC recent ones carry 2021 codes. This is the measurement of that.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.crime_year,
# MAGIC        count(DISTINCT p.lsoa_code) AS lsoa_codes,
# MAGIC        count(DISTINCT d.lsoa_code_2021) AS matched_2021
# MAGIC FROM uk_property_intel.silver.police_street_crime p
# MAGIC LEFT JOIN (
# MAGIC   SELECT DISTINCT lsoa_code_2021 FROM uk_property_intel.silver.doogal
# MAGIC   WHERE lsoa_code_2021 IS NOT NULL
# MAGIC ) d ON d.lsoa_code_2021 = p.lsoa_code
# MAGIC WHERE p.lsoa_code IS NOT NULL
# MAGIC GROUP BY p.crime_year
# MAGIC ORDER BY p.crime_year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT crime_id, crime_month, force, reported_by, crime_type,
# MAGIC        last_outcome_category, lsoa_code, latitude, longitude, snapshot_month
# MAGIC FROM uk_property_intel.silver.police_street_crime
# MAGIC WHERE crime_year = 2025
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Once the above reads correctly:
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE uk_property_intel.silver.police_street_crime_staging
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this run recorded
# MAGIC
# MAGIC Per-archive rows carry the archive in `scope`; run-level rows leave it null. A
# MAGIC share across the whole load sums the numerators and denominators before
# MAGIC dividing, rather than averaging the per-archive percentages, which would weight
# MAGIC a small archive the same as a large one.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.status, r.started_ts, r.ended_ts, r.rows_written, r.error_type,
# MAGIC        m.metric, m.scope, m.value_numeric, m.value_text, m.value_date,
# MAGIC        m.denominator
# MAGIC FROM uk_property_intel.quality.pipeline_run r
# MAGIC LEFT JOIN uk_property_intel.quality.pipeline_metric m USING (run_id)
# MAGIC WHERE r.source = 'police'
# MAGIC ORDER BY r.started_ts DESC, m.metric, m.scope

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT m.metric,
# MAGIC        sum(m.value_numeric) AS value,
# MAGIC        sum(m.denominator) AS base,
# MAGIC        round(100 * sum(m.value_numeric) / nullif(sum(m.denominator), 0), 4) AS pct
# MAGIC FROM uk_property_intel.quality.pipeline_metric m
# MAGIC JOIN uk_property_intel.quality.pipeline_run r USING (run_id)
# MAGIC WHERE r.source = 'police' AND m.denominator IS NOT NULL
# MAGIC   AND r.started_ts = (SELECT max(started_ts)
# MAGIC                       FROM uk_property_intel.quality.pipeline_run
# MAGIC                       WHERE source = 'police')
# MAGIC GROUP BY m.metric
# MAGIC ORDER BY pct DESC
