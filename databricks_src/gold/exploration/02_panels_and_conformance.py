# Databricks notebook source
# MAGIC %md
# MAGIC # Gold exploration - published panels and conformance
# MAGIC
# MAGIC Measures the two published area panels, the house price index and the private rent series,
# MAGIC against the dimensions they will key on. Read-only: it creates no tables and writes nothing.
# MAGIC
# MAGIC Unlike `01_grain_and_geography`, most of what this measures has no recorded value yet, so
# MAGIC most lines report rather than check. The six figures already carried in the documents get a
# MAGIC `DRIFT` verdict on the same terms as `01`.
# MAGIC
# MAGIC This one reads Gold, so it runs after `01_load_dimensions`. Runtime is seconds: the largest
# MAGIC input is 147,000 rows.
# MAGIC
# MAGIC What it settles, in order: whether either panel breaks the grain its fact declares, whether
# MAGIC every key resolves in `dim_area` and `dim_date`, how many rows carry no measure at all, which
# MAGIC levels the seasonally adjusted series reaches, and whether anything in either panel would be
# MAGIC rejected by the fact's own check constraints.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "uk_property_intel"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# Measured 2026-08, from the Silver loads and the phase 3.2 dimension load.
RECORDED = {
    "hpi_rows": 147_453,
    "hpi_area_codes": 405,
    "ons_rows": 49_266,
    "ons_area_names": 357,
    "dim_area_rows": 432,
    "dim_date_rows": 19_723,
}

# Measures each fact carries, in the order its DDL declares them. Every one exists in Silver
# under the same name and at the same type, so both facts are a projection and a rename.
HPI_MEASURES = (
    "avg_price",
    "avg_price_seasonally_adjusted",
    "price_index",
    "price_index_seasonally_adjusted",
    "pct_change_1m",
    "pct_change_12m",
    "sales_volume",
    "detached_price",
    "semi_detached_price",
    "terraced_price",
    "flat_price",
)

ONS_MEASURES = (
    "rental_price",
    "price_index",
    "pct_change_1m",
    "pct_change_12m",
    "one_bed_rental_price",
    "two_bed_rental_price",
    "three_bed_rental_price",
    "four_or_more_bed_rental_price",
    "detached_rental_price",
    "semi_detached_rental_price",
    "terraced_rental_price",
    "flat_maisonette_rental_price",
)

SEASONALLY_ADJUSTED = ("avg_price_seasonally_adjusted", "price_index_seasonally_adjusted")

RENT_SERIES_START = "2015-01-01"


def check(label, measured, key):
    recorded = RECORDED[key]
    verdict = "ok" if measured == recorded else "DRIFT"
    print(f"{label:<40} {measured:>12,}   recorded {recorded:>12,}   {verdict}")


def report(label, measured):
    print(f"{label:<40} {measured:>12,}")


def all_measures_null(measures):
    """True where every measure the fact carries is null on the row."""
    condition = None
    for name in measures:
        test = F.col(name).isNull()
        condition = test if condition is None else condition & test
    return condition


# COMMAND ----------

# Projected to what each fact will carry, plus the keys. Several aggregates follow, and the
# projection is what the fact is, so measuring anything wider would describe a different table.
hpi = (
    spark.table(f"{SILVER}.hpi")
    .select("area_code", "date", *HPI_MEASURES)
    .cache()
)
ons = (
    spark.table(f"{SILVER}.ons_private_rents")
    .select("area_code", "area_name", "date", *ONS_MEASURES)
    .cache()
)
areas = (
    spark.table(f"{GOLD}.dim_area")
    .select(
        "area_code",
        "area_name",
        "area_level",
        "code_source",
        "has_price_index",
        "has_rent_index",
    )
    .cache()
)
dates = spark.table(f"{GOLD}.dim_date").select("date_key").cache()

check("dim_area rows", areas.count(), "dim_area_rows")
check("dim_date rows", dates.count(), "dim_date_rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Panel shape and grain
# MAGIC
# MAGIC Both facts declare `(area_code, month_start_date)` as their primary key. It is informational
# MAGIC and unenforced, so a repeat would load and the two series would be indistinguishable
# MAGIC afterwards.
# MAGIC
# MAGIC The rent panel is the one at risk. Silver keys it on `(area_name, date)`, because the eight
# MAGIC Northern Irish rental market areas carry no published code, so the Gold key is a different
# MAGIC key rather than the same one renamed.

# COMMAND ----------

hpi_shape = hpi.agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct("area_code").alias("areas"),
    F.min("date").alias("first_month"),
    F.max("date").alias("last_month"),
    F.count_if(F.dayofmonth("date") != 1).alias("not_month_start"),
).first()

check("hpi rows", hpi_shape["rows"], "hpi_rows")
check("hpi area codes", hpi_shape["areas"], "hpi_area_codes")
report("hpi rows not on a month start", hpi_shape["not_month_start"])
print(f"hpi months {hpi_shape['first_month']} to {hpi_shape['last_month']}")

hpi_repeats = (
    hpi.groupBy("area_code", "date").agg(F.count(F.lit(1)).alias("rows")).where("rows > 1")
)
report("hpi repeated (area_code, month)", hpi_repeats.count())

# COMMAND ----------

ons_shape = ons.agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct("area_name").alias("names"),
    F.countDistinct("area_code").alias("codes"),
    F.min("date").alias("first_month"),
    F.max("date").alias("last_month"),
    F.count_if(F.dayofmonth("date") != 1).alias("not_month_start"),
    F.count_if(F.col("area_code").isNull()).alias("rows_uncoded"),
).first()

check("ons rows", ons_shape["rows"], "ons_rows")
check("ons area names", ons_shape["names"], "ons_area_names")
report("ons published area codes", ons_shape["codes"])
report("ons rows with no code", ons_shape["rows_uncoded"])
report("ons rows not on a month start", ons_shape["not_month_start"])
print(f"ons months {ons_shape['first_month']} to {ons_shape['last_month']}")

# COMMAND ----------

# Two names under one code would merge two series when the fact rekeys on the code, and the
# informational primary key would not stop it.
shared_codes = (
    ons.where(F.col("area_code").isNotNull())
    .select("area_code", "area_name")
    .distinct()
    .groupBy("area_code")
    .agg(F.count(F.lit(1)).alias("names"), F.collect_set("area_name").alias("name_set"))
    .where("names > 1")
)
report("ons codes carrying two names", shared_codes.count())
display(shared_codes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Keys against dim_area
# MAGIC
# MAGIC Both directions. A panel key with no dimension row is a fact that drops out of every
# MAGIC rollup; a dimension row flagged as published with no fact rows behind it is the coverage
# MAGIC figure the audit metric will carry.

# COMMAND ----------

hpi_codes = hpi.select("area_code").distinct()
area_codes = areas.select("area_code")

hpi_dangling = hpi_codes.join(area_codes, "area_code", "left_anti")
report("hpi codes with no dim_area row", hpi_dangling.count())
display(hpi_dangling.limit(20))

hpi_unreached = (
    areas.where(F.col("has_price_index"))
    .select("area_code", "area_name")
    .join(hpi_codes, "area_code", "left_anti")
)
report("areas flagged price, no hpi rows", hpi_unreached.count())
display(hpi_unreached.limit(20))

# COMMAND ----------

ons_coded = ons.where(F.col("area_code").isNotNull()).select("area_code").distinct()

ons_dangling = ons_coded.join(area_codes, "area_code", "left_anti")
report("ons codes with no dim_area row", ons_dangling.count())
display(ons_dangling.limit(20))

# Expected to be the eight uncoded areas and nothing else. They are unreachable by code and are
# resolved by name below, so this states something about the join key rather than about coverage.
# Anything beyond eight here is a coded area the rent series does not actually publish.
ons_unreached = (
    areas.where(F.col("has_rent_index"))
    .select("area_code", "area_name", "code_source")
    .join(ons_coded, "area_code", "left_anti")
)
report("areas flagged rent, no coded rows", ons_unreached.count())
display(ons_unreached)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The uncoded rent areas
# MAGIC
# MAGIC The fact resolves these by joining the trimmed area name to `dim_area`, restricted to the
# MAGIC codes this project assigned. Each name has to reach exactly one. A name reaching none leaves
# MAGIC a null in a `NOT NULL` key, and a name reaching two would take whichever the join returned.
# MAGIC
# MAGIC The resolved code set is built here rather than in section 7, so coverage is measured
# MAGIC through the same resolution the fact will use and beside the check that proves it total.

# COMMAND ----------

uncoded_names = (
    ons.where(F.col("area_code").isNull())
    .select(F.trim("area_name").alias("area_name"))
    .distinct()
)
derived_areas = (
    areas.where(F.col("code_source") == F.lit("derived"))
    .select(F.trim("area_name").alias("area_name"), "area_code")
)

resolution = (
    uncoded_names.join(derived_areas, "area_name", "left")
    .groupBy("area_name")
    .agg(
        F.count("area_code").alias("codes_matched"),
        F.min("area_code").alias("sample_code"),
    )
)

report("ons uncoded names", uncoded_names.count())
report("derived codes in dim_area", derived_areas.count())
report("names matching no code", resolution.where("codes_matched = 0").count())
report("names matching two or more", resolution.where("codes_matched > 1").count())
display(resolution.orderBy("area_name"))

# COMMAND ----------

ons_resolved_codes = (
    ons_coded.unionByName(
        uncoded_names.join(derived_areas, "area_name", "inner").select("area_code")
    )
    .distinct()
    .cache()
)
report("ons area codes after resolution", ons_resolved_codes.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Months against dim_date
# MAGIC
# MAGIC The calendar runs from 1973 to the end of the load year, so both panels should sit well
# MAGIC inside it. Measured rather than assumed, because the calendar's end is the year the
# MAGIC dimension load ran in and a panel published into the next year would fall outside it.

# COMMAND ----------

for label, panel in (("hpi", hpi), ("ons", ons)):
    months = panel.select(F.col("date").alias("date_key")).distinct()
    report(f"{label} distinct months", months.count())
    report(f"{label} months not in dim_date", months.join(dates, "date_key", "left_anti").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Rows carrying no measure
# MAGIC
# MAGIC The rent panel holds these by construction. Northern Ireland lags the other nations, so ONS
# MAGIC publishes `[x]` across every measure for its unpublished months and Silver keeps the rows.
# MAGIC The count moves with each release.
# MAGIC
# MAGIC Whether the index panel holds any is unmeasured, which is what this section is for. A row
# MAGIC carrying no measure is not a fact, and it counts once in every total taken over the table.

# COMMAND ----------

hpi_nulls = hpi.agg(
    *[F.count_if(F.col(name).isNull()).alias(name) for name in HPI_MEASURES],
    F.count_if(all_measures_null(HPI_MEASURES)).alias("_all_null"),
).first().asDict()

print(f"{'hpi measure':<40}{'null rows':>12}{'share':>10}")
for name in HPI_MEASURES:
    share = hpi_nulls[name] / hpi_shape["rows"]
    print(f"{name:<40}{hpi_nulls[name]:>12,}{share:>10.1%}")
print()
report("hpi rows with no measure at all", hpi_nulls["_all_null"])

# COMMAND ----------

ons_nulls = ons.agg(
    *[F.count_if(F.col(name).isNull()).alias(name) for name in ONS_MEASURES],
    F.count_if(all_measures_null(ONS_MEASURES)).alias("_all_null"),
).first().asDict()

print(f"{'ons measure':<40}{'null rows':>12}{'share':>10}")
for name in ONS_MEASURES:
    share = ons_nulls[name] / ons_shape["rows"]
    print(f"{name:<40}{ons_nulls[name]:>12,}{share:>10.1%}")
print()
report("ons rows with no measure at all", ons_nulls["_all_null"])

# COMMAND ----------

# Which areas and which months. The expectation is Northern Ireland only, in trailing months,
# which is what the Silver guards already assert. Anything else changes what dropping them means.
display(
    ons.where(all_measures_null(ONS_MEASURES))
    .groupBy("area_name", "area_code")
    .agg(
        F.count(F.lit(1)).alias("months"),
        F.min("date").alias("first_month"),
        F.max("date").alias("last_month"),
    )
    .orderBy("area_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Where the seasonally adjusted series exists
# MAGIC
# MAGIC Both seasonally adjusted columns are null on 96 percent of the index panel, which is a shape
# MAGIC rather than sparseness: seasonal adjustment is published for some levels and not others. The
# MAGIC area profile screen compares a district against its region and the country, so if the series
# MAGIC stops above district then those two columns serve the benchmark and never the subject.
# MAGIC
# MAGIC This changes no code. It decides what a screen can put on one axis.

# COMMAND ----------

by_level = (
    hpi.join(areas.select("area_code", "area_level"), "area_code")
    .groupBy("area_level")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("area_code").alias("areas"),
        *[F.count_if(F.col(name).isNotNull()).alias(name) for name in SEASONALLY_ADJUSTED],
    )
    .orderBy("area_level")
)
display(by_level)

# The two columns are assumed to be published together. Populated on different rows would mean
# one is usable where the other is not, and a screen reading both would disagree with itself.
asymmetric = hpi.agg(
    F.count_if(
        F.col("avg_price_seasonally_adjusted").isNotNull()
        != F.col("price_index_seasonally_adjusted").isNotNull()
    ).alias("rows")
).first()
report("rows carrying one sa column only", asymmetric["rows"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Against the fact check constraints
# MAGIC
# MAGIC Each count below is a row the declared constraint would reject, aborting the insert after
# MAGIC the transform had already run. Silver constrains the two headline columns on both sources,
# MAGIC so a non-zero here means the two layers disagree about what they admit.

# COMMAND ----------

hpi_rejects = hpi.agg(
    F.count_if(F.col("avg_price") <= 0).alias("avg_price_not_positive"),
    F.count_if(F.col("sales_volume") < 0).alias("sales_volume_negative"),
    F.count_if(F.col("date") != F.trunc("date", "MM")).alias("month_start"),
).first().asDict()

for name, count in hpi_rejects.items():
    report(f"hpi {name}", count)

# COMMAND ----------

ons_rejects = ons.agg(
    F.count_if(F.col("rental_price") <= 0).alias("rental_price_not_positive"),
    F.count_if(F.col("date") != F.trunc("date", "MM")).alias("month_start"),
    F.count_if(F.col("date") < F.lit(RENT_SERIES_START).cast("date")).alias("before_series_start"),
).first().asDict()

for name, count in ons_rejects.items():
    report(f"ons {name}", count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Coverage, as the audit metric will record it
# MAGIC
# MAGIC Dimension rows the fact reaches, against all rows of that dimension. Recorded as a count and
# MAGIC a base rather than a share, so two runs re-aggregate.
# MAGIC
# MAGIC `dim_area` only. The calendar is daily and both facts are monthly, so a fact reaching every
# MAGIC month it publishes still reads as a few percent of `dim_date`, and the number carries no
# MAGIC signal at any value. Whether every month resolves is the question worth asking there, and
# MAGIC section 3 asks it.
# MAGIC
# MAGIC The rent figure is taken after name resolution. Measured on the published code alone it
# MAGIC understates by the eight areas that have no published code, which is the population the
# MAGIC resolution exists for.

# COMMAND ----------

dim_area_rows = areas.count()

for label, codes in (("hpi", hpi_codes), ("ons", ons_resolved_codes)):
    reached = codes.join(area_codes, "area_code", "inner").count()
    print(f"{label} reaches {reached:,} of {dim_area_rows:,} dim_area rows")

# COMMAND ----------

for df in (hpi, ons, areas, dates, ons_resolved_codes):
    df.unpersist()
