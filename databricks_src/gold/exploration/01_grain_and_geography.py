# Databricks notebook source
# MAGIC %md
# MAGIC # Gold exploration - grain and geography
# MAGIC
# MAGIC Reproduces the measurements the Gold model is built on. Read-only: it creates no tables and
# MAGIC writes nothing, so it is safe to run against a populated Silver layer at any time.
# MAGIC
# MAGIC Each check prints the value measured now beside the value recorded in DESIGN. A `DRIFT`
# MAGIC line means Silver has changed since the model was designed and the affected decision needs
# MAGIC revisiting, not that the notebook is broken.
# MAGIC
# MAGIC Runtime is about six minutes on a single-node four-core cluster, with no single cell over a
# MAGIC minute. Full scans of the 96M-row crime table and the 31M-row transaction table dominate.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "uk_property_intel"
SILVER = f"{CATALOG}.silver"

# Measured 2026-08. Cited in DESIGN under the Gold model decisions.
RECORDED = {
    "crime_source_rows": 96_092_836,
    "crime_lsoa_month": 6_328_185,
    "crime_lsoa_month_type": 31_039_737,
    "crime_distinct_lsoa": 36_751,
    "crime_distinct_months": 187,
    "crime_distinct_types": 16,
    "crime_rows_no_lsoa": 3_740_289,
    "crime_rows_northern_ireland": 2_311_848,
    "vintage_codes_only_2011": 1_106,
    "vintage_codes_only_2021": 1_999,
    "vintage_mixed_months": 30,
    "lsoa_single_district": 36_671,
    "lsoa_multi_district": 80,
    "districts_with_crime": 318,
    "ppd_source_rows": 31_430_611,
    "ppd_resolved": 31_378_093,
    "ppd_null_postcode": 50_505,
    "ppd_unresolved": 2_013,
    "ppd_district_mismatch": 4_704_276,
    "ppd_lsoa_month_cells": 10_867_452,
    "ppd_lsoa_month_thin_cells": 5_987_371,
    "ppd_lsoa_year_cells": 1_135_051,
    "ppd_lsoa_year_dense_cells": 998_445,
    "dim_lsoa_rows": 36_781,
    "dim_area_rows": 432,
    "boe_intervals": 278,
    "boe_discontinuities": 0,
    "hpi_area_codes": 405,
    "ons_area_names": 357,
    "doogal_district_codes": 361,
    "districts_price_rent_crime": 316,
}


def check(label, measured, key):
    recorded = RECORDED[key]
    verdict = "ok" if measured == recorded else "DRIFT"
    print(f"{label:<40} {measured:>12,}   recorded {recorded:>12,}   {verdict}")


# COMMAND ----------

# The postcode lookup narrowed to the columns Gold uses. Small enough to hold in memory and to
# broadcast, which is what keeps the transaction join off a shuffle.
lookup = (
    spark.table(f"{SILVER}.doogal")
    .select(
        "postcode",
        "district_code",
        "district",
        "region",
        "country",
        "lsoa_code_2011",
        "lsoa_code_2021",
    )
    .cache()
)

lookup.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crime cardinality
# MAGIC
# MAGIC Crime type carries almost the whole cardinality of the finest crime grain: adding it to
# MAGIC `(lsoa, month)` multiplies the cell count roughly fivefold. That is what makes crime type a
# MAGIC dimension row rather than a set of columns.

# COMMAND ----------

crime = spark.table(f"{SILVER}.police_street_crime")

card = crime.agg(
    F.count("*").alias("source_rows"),
    F.countDistinct("crime_month", "lsoa_code").alias("lsoa_month"),
    F.countDistinct("crime_month", "lsoa_code", "crime_type").alias("lsoa_month_type"),
    F.countDistinct("lsoa_code").alias("distinct_lsoa"),
    F.countDistinct("crime_month").alias("distinct_months"),
    F.countDistinct("crime_type").alias("distinct_types"),
    F.sum(F.col("lsoa_code").isNull().cast("long")).alias("rows_no_lsoa"),
).first()

check("crime source rows", card.source_rows, "crime_source_rows")
check("cells at (lsoa, month)", card.lsoa_month, "crime_lsoa_month")
check("cells at (lsoa, month, type)", card.lsoa_month_type, "crime_lsoa_month_type")
check("distinct lsoa codes", card.distinct_lsoa, "crime_distinct_lsoa")
check("distinct months", card.distinct_months, "crime_distinct_months")
check("distinct crime types", card.distinct_types, "crime_distinct_types")
check("rows with no lsoa code", card.rows_no_lsoa, "crime_rows_no_lsoa")

print(f"\ntype multiplies cell count by {card.lsoa_month_type / card.lsoa_month:.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Geographic coverage below force level
# MAGIC
# MAGIC Northern Ireland publishes no small-area code at all, so crime exists there at force grain
# MAGIC and nowhere below it. Police Scotland does not publish to this source.

# COMMAND ----------

by_force = (
    crime.groupBy("force")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("lsoa_code").isNull().cast("long")).alias("rows_no_lsoa"),
        F.countDistinct("lsoa_code").alias("distinct_lsoa"),
    )
    .withColumn("pct_no_lsoa", F.round(100 * F.col("rows_no_lsoa") / F.col("rows"), 2))
    .cache()
)

check(
    "northern ireland rows",
    by_force.where(F.col("force") == "northern-ireland").first().rows,
    "crime_rows_northern_ireland",
)

display(by_force.orderBy(F.desc("pct_no_lsoa"), F.desc("rows")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The 2011 to 2021 boundary transition
# MAGIC
# MAGIC A code present in both vintage columns describes an area that did not change, so it joins
# MAGIC correctly either way. Only codes exclusive to one vintage carry era information, and a
# MAGIC month is mixed when it holds at least one of each.
# MAGIC
# MAGIC The transition is a band, not a cutover. Forces update their gazetteers independently, so
# MAGIC both vintages appear together for two and a half years.

# COMMAND ----------

codes_11 = (
    lookup.select(F.col("lsoa_code_2011").alias("code"))
    .where(F.col("code").isNotNull())
    .distinct()
    .withColumn("in_11", F.lit(True))
)
codes_21 = (
    lookup.select(F.col("lsoa_code_2021").alias("code"))
    .where(F.col("code").isNotNull())
    .distinct()
    .withColumn("in_21", F.lit(True))
)

vintage = (
    crime.select(F.col("lsoa_code").alias("code"))
    .where(F.col("code").isNotNull())
    .distinct()
    .join(codes_11, "code", "left")
    .join(codes_21, "code", "left")
    .fillna(False, ["in_11", "in_21"])
    .withColumn(
        "vintage",
        F.when(F.col("in_11") & F.col("in_21"), "both")
        .when(F.col("in_11"), "only_2011")
        .when(F.col("in_21"), "only_2021")
        .otherwise("unmatched"),
    )
    .select("code", "vintage")
    .cache()
)

counts = {r.vintage: r.n for r in vintage.groupBy("vintage").agg(F.count("*").alias("n")).collect()}
check("codes only in 2011 vintage", counts.get("only_2011", 0), "vintage_codes_only_2011")
check("codes only in 2021 vintage", counts.get("only_2021", 0), "vintage_codes_only_2021")
print(f"codes in both vintages                 {counts.get('both', 0):>12,}")
print(f"codes matching neither                 {counts.get('unmatched', 0):>12,}")

# COMMAND ----------

per_month = (
    crime.select("crime_month", F.col("lsoa_code").alias("code"))
    .join(vintage, "code", "inner")
    .groupBy("crime_month")
    .agg(
        F.countDistinct(F.when(F.col("vintage") == "only_2011", F.col("code"))).alias("n_only_11"),
        F.countDistinct(F.when(F.col("vintage") == "only_2021", F.col("code"))).alias("n_only_21"),
    )
    .withColumn("mixed", (F.col("n_only_11") > 0) & (F.col("n_only_21") > 0))
    .cache()
)

mixed = per_month.where("mixed").agg(
    F.count("*").alias("n"), F.min("crime_month").alias("first"), F.max("crime_month").alias("last")
).first()

check("months carrying both vintages", mixed.n, "vintage_mixed_months")
print(f"band runs {mixed.first} to {mixed.last}")

display(per_month.orderBy("crime_month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crime type eras
# MAGIC
# MAGIC Two vocabulary changes, at 2011-09 and 2013-05. Both are categorical splits: an existing
# MAGIC type loses volume to new types that sum back to it, and the untouched types hold their
# MAGIC level. Nothing new enters the count, so an all-types total is comparable across the whole
# MAGIC series even though individual type series are not.

# COMMAND ----------

by_type = (
    crime.groupBy("crime_type", "crime_month").agg(F.count("*").alias("crimes")).cache()
)

display(
    by_type.groupBy("crime_type")
    .agg(
        F.min("crime_month").alias("first_month"),
        F.max("crime_month").alias("last_month"),
        F.countDistinct("crime_month").alias("n_months"),
        F.sum("crimes").alias("crimes"),
    )
    .orderBy("first_month", "crime_type")
)

# COMMAND ----------

# Monthly volume by type across both transitions. Read the columns that fall against those that
# appear in the same month.
display(
    by_type.where(F.col("crime_month").between("2011-06-01", "2011-12-01"))
    .groupBy("crime_month")
    .pivot("crime_type")
    .agg(F.sum("crimes"))
    .orderBy("crime_month")
)

display(
    by_type.where(F.col("crime_month").between("2013-02-01", "2013-08-01"))
    .groupBy("crime_month")
    .pivot("crime_type")
    .agg(F.sum("crimes"))
    .orderBy("crime_month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti-social behaviour share over time
# MAGIC
# MAGIC ASB falls from roughly 42% of all records to 16%. Force-level double counting and Home
# MAGIC Office counting rule differences make the series unfit for area comparison, and a shift of
# MAGIC this size corrupts any total that includes it. ASB is therefore held apart from the crime
# MAGIC fact rather than filtered out of it by convention.

# COMMAND ----------

yearly = by_type.withColumn("crime_year", F.year("crime_month")).groupBy("crime_year").agg(
    F.sum("crimes").alias("all_crimes"),
    F.sum(F.when(F.col("crime_type") == "Anti-social behaviour", F.col("crimes")).otherwise(0)).alias("asb"),
)

display(
    yearly.withColumn("asb_pct", F.round(100 * F.col("asb") / F.col("all_crimes"), 1)).orderBy(
        "crime_year"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Published area codes and levels
# MAGIC
# MAGIC Three publishers, three different area sets. The district codes carried by all three are
# MAGIC the population where a rental yield can be calculated.

# COMMAND ----------

hpi = spark.table(f"{SILVER}.hpi")
ons = spark.table(f"{SILVER}.ons_private_rents")

hpi_codes = hpi.select(F.col("area_code").alias("code")).distinct().withColumn("in_hpi", F.lit(True))
ons_codes = (
    ons.where(F.col("area_code").isNotNull())
    .select(F.col("area_code").alias("code"))
    .distinct()
    .withColumn("in_ons", F.lit(True))
)
doogal_codes = (
    lookup.where(F.col("district_code").isNotNull())
    .select(F.col("district_code").alias("code"))
    .distinct()
    .withColumn("in_doogal", F.lit(True))
)

check("hpi area codes", hpi_codes.count(), "hpi_area_codes")
check("ons area names", ons.select("area_name").distinct().count(), "ons_area_names")
check("doogal district codes", doogal_codes.count(), "doogal_district_codes")

membership = (
    hpi_codes.join(ons_codes, "code", "full_outer")
    .join(doogal_codes, "code", "full_outer")
    .fillna(False, ["in_hpi", "in_ons", "in_doogal"])
    .withColumn("prefix", F.substring("code", 1, 3))
    .cache()
)

check(
    "districts with price, rent and crime",
    membership.where("in_hpi AND in_ons AND in_doogal").count(),
    "districts_price_rent_crime",
)
check("total coded areas", membership.count() + ons.where(F.col("area_code").isNull()).select("area_name").distinct().count(), "dim_area_rows")

display(
    membership.groupBy("prefix", "in_hpi", "in_ons", "in_doogal")
    .agg(F.count("*").alias("n_codes"), F.min("code").alias("sample"))
    .orderBy("prefix")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hierarchy
# MAGIC
# MAGIC Region membership comes from the postcode lookup, which carries region as a name. Eight of
# MAGIC nine English regions match HPI by name; HPI publishes E12000005 as "West Midlands Region"
# MAGIC to keep it distinct from the metropolitan county of the same name.
# MAGIC
# MAGIC County membership is not usable. The lookup's county codes are ceremonial counties on a
# MAGIC code series whose values collide with HPI's metropolitan counties, so the same code names
# MAGIC a different area in each source. Districts roll up to region and then to nation, and the
# MAGIC county-level areas stand alone with their published series and no children.

# COMMAND ----------

hpi_regions = (
    hpi.where(F.col("area_code").startswith("E12"))
    .select(F.col("area_code").alias("hpi_code"), F.col("region_name").alias("name"))
    .distinct()
)
lookup_regions = (
    lookup.where(F.col("region").isNotNull())
    .select(F.col("region").alias("name"))
    .distinct()
    .withColumn("in_lookup", F.lit(True))
)

display(
    hpi_regions.join(lookup_regions, "name", "full_outer")
    .fillna(False, ["in_lookup"])
    .orderBy(F.col("hpi_code").isNotNull(), "name")
)

# COMMAND ----------

# The collision, shown directly. Matching names are the shire counties; the rest name different
# areas under the same code.
hpi_counties = (
    hpi.where(F.col("area_code").rlike("^(E10|E11|E13)"))
    .select(F.col("area_code").alias("code"), F.col("region_name").alias("hpi_name"))
    .distinct()
)
lookup_counties = (
    spark.table(f"{SILVER}.doogal")
    .where(F.col("county_code").isNotNull())
    .select(F.col("county_code").alias("code"), F.col("county").alias("lookup_name"))
    .distinct()
)

display(
    hpi_counties.join(lookup_counties, "code", "inner")
    .withColumn("name_match", F.col("hpi_name") == F.col("lookup_name"))
    .orderBy("name_match", "code")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. LSOA to district resolution
# MAGIC
# MAGIC Rolling crime up to district is only additive if each small area sits in one district. The
# MAGIC areas that straddle are assigned to the district holding most of their postcodes, and the
# MAGIC dimension flags them so the assignment stays separable from the exact ones.

# COMMAND ----------

lsoa_district = (
    lookup.select("postcode", "district_code", F.col("lsoa_code_2011").alias("code"))
    .union(lookup.select("postcode", "district_code", F.col("lsoa_code_2021").alias("code")))
    .where(F.col("code").isNotNull() & F.col("district_code").isNotNull())
    .distinct()
    .join(vintage.select("code"), "code", "inner")
    .groupBy("code", "district_code")
    .agg(F.count("*").alias("postcodes"))
    .cache()
)

w = Window.partitionBy("code")
ranked = (
    lsoa_district.withColumn("total", F.sum("postcodes").over(w))
    .withColumn("n_districts", F.count("*").over(w))
    .withColumn("rank", F.row_number().over(w.orderBy(F.desc("postcodes"), "district_code")))
    .where(F.col("rank") == 1)
    .withColumn("share", F.col("postcodes") / F.col("total"))
    .cache()
)

check("lsoa resolving to one district", ranked.where("n_districts = 1").count(), "lsoa_single_district")
check("lsoa straddling districts", ranked.where("n_districts > 1").count(), "lsoa_multi_district")
check("districts reached by crime", ranked.select("district_code").distinct().count(), "districts_with_crime")

# Majority share for the straddlers. An assignment rule needs these concentrated.
display(
    ranked.where("n_districts > 1")
    .withColumn(
        "band",
        F.when(F.col("share") >= 0.95, "a >=95%")
        .when(F.col("share") >= 0.80, "b 80-95%")
        .when(F.col("share") >= 0.60, "c 60-80%")
        .otherwise("d <60%"),
    )
    .groupBy("band")
    .agg(F.count("*").alias("n_lsoa"))
    .orderBy("band")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Transactions against the postcode lookup
# MAGIC
# MAGIC The transaction table carries its own district name. It disagrees with the lookup on a
# MAGIC seventh of rows, and the disagreement shrinks steadily over time, which is the signature of
# MAGIC local government reorganisation rather than error. Resolving district through the postcode
# MAGIC restates history onto current boundaries, matching what both index publishers already do.

# COMMAND ----------

ppd = spark.table(f"{SILVER}.ppd").select("transfer_year", "postcode", "district", "date_of_transfer")

resolution = (
    ppd.join(
        lookup.select(
            "postcode",
            F.col("district_code").alias("pc_district_code"),
            F.col("district").alias("pc_district"),
        ),
        "postcode",
        "left",
    )
    .withColumn(
        "status",
        F.when(F.col("postcode").isNull(), "null_postcode")
        .when(F.col("pc_district_code").isNull(), "unresolved")
        .otherwise("resolved"),
    )
    .withColumn(
        "district_match", F.upper(F.trim("district")) == F.upper(F.trim("pc_district"))
    )
    .cache()
)

totals = resolution.agg(
    F.count("*").alias("rows"),
    F.sum((F.col("status") == "resolved").cast("long")).alias("resolved"),
    F.sum((F.col("status") == "null_postcode").cast("long")).alias("null_postcode"),
    F.sum((F.col("status") == "unresolved").cast("long")).alias("unresolved"),
    F.sum((~F.col("district_match")).cast("long")).alias("district_mismatch"),
).first()

check("transaction rows", totals.rows, "ppd_source_rows")
check("resolved to a postcode", totals.resolved, "ppd_resolved")
check("no postcode published", totals.null_postcode, "ppd_null_postcode")
check("postcode matching nothing", totals.unresolved, "ppd_unresolved")
check("district name disagreements", totals.district_mismatch, "ppd_district_mismatch")

display(
    resolution.groupBy("transfer_year")
    .agg(
        F.count("*").alias("rows"),
        F.round(100 * F.avg((~F.col("district_match")).cast("int")), 1).alias("pct_mismatch"),
    )
    .orderBy("transfer_year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Transaction density at small-area grain
# MAGIC
# MAGIC A median needs a population behind it. At monthly small-area grain most cells hold one or
# MAGIC two sales, so the median describes an individual property. At annual grain the same cells
# MAGIC are dense enough for the statistic to mean something, which is why price sits at
# MAGIC `(lsoa, year)` while crime sits at `(lsoa, month)`.

# COMMAND ----------

ppd_lsoa = (
    spark.table(f"{SILVER}.ppd")
    .select("postcode", "date_of_transfer")
    .join(
        lookup.select("postcode", F.col("lsoa_code_2021").alias("lsoa")).where(
            F.col("lsoa_code_2021").isNotNull()
        ),
        "postcode",
        "inner",
    )
    .select(
        "lsoa",
        F.trunc("date_of_transfer", "MM").alias("month"),
        F.year("date_of_transfer").alias("year"),
    )
)


def density(period_col, label, cells_key, band_key, band_expr):
    cells = ppd_lsoa.groupBy("lsoa", period_col).agg(F.count("*").alias("txns")).cache()
    check(f"{label} cells", cells.count(), cells_key)
    check(f"{label} {band_key}", cells.where(band_expr).count(), band_key)
    display(
        cells.withColumn(
            "band",
            F.when(F.col("txns") == 1, "a 1")
            .when(F.col("txns") == 2, "b 2")
            .when(F.col("txns") <= 5, "c 3-5")
            .when(F.col("txns") <= 10, "d 6-10")
            .otherwise("e 11+"),
        )
        .groupBy("band")
        .agg(F.count("*").alias("cells"), F.sum("txns").alias("txns"))
        .orderBy("band")
    )
    cells.unpersist()


density("month", "lsoa x month", "ppd_lsoa_month_cells", "ppd_lsoa_month_thin_cells", F.col("txns") <= 2)
density("year", "lsoa x year", "ppd_lsoa_year_cells", "ppd_lsoa_year_dense_cells", F.col("txns") >= 11)

# COMMAND ----------

# The small-area dimension covers everything either source reaches. Codes exclusive to the 2011
# vintage carry crime and no price, since transactions are attributed to 2021 codes only.
crime_lsoa = vintage.select("code")
ppd_lsoa_codes = ppd_lsoa.select(F.col("lsoa").alias("code")).distinct()

check("small areas in the dimension", crime_lsoa.union(ppd_lsoa_codes).distinct().count(), "dim_lsoa_rows")
print(f"crime only  {crime_lsoa.subtract(ppd_lsoa_codes).count():>12,}")
print(f"price only  {ppd_lsoa_codes.subtract(crime_lsoa).count():>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Base rate interval chain
# MAGIC
# MAGIC Four instrument names over the period, one at a time and never concurrent, with each
# MAGIC regime closing the day before the next begins. That makes the rate on a date a single
# MAGIC value, so it resolves to a daily attribute on the date dimension rather than needing a
# MAGIC range join.

# COMMAND ----------

boe = spark.table(f"{SILVER}.boe_base_rate")
wb = Window.orderBy("effective_date")

check("rate intervals", boe.count(), "boe_intervals")

discontinuities = (
    boe.withColumn("next_effective", F.lead("effective_date").over(wb))
    .where(F.col("next_effective").isNotNull())
    .where(F.datediff("next_effective", "expiry_date") != 1)
)
check("gaps or overlaps", discontinuities.count(), "boe_discontinuities")

display(
    boe.groupBy("rate_type").agg(
        F.count("*").alias("intervals"),
        F.min("effective_date").alias("first_effective"),
        F.max("effective_date").alias("last_effective"),
        F.min("rate_pct").alias("min_rate"),
        F.max("rate_pct").alias("max_rate"),
    ).orderBy("first_effective")
)

# COMMAND ----------

for df in (lookup, by_force, vintage, per_month, by_type, membership, lsoa_district, ranked, resolution):
    df.unpersist()
