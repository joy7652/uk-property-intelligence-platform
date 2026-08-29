# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold: cross-source verification
# MAGIC
# MAGIC Reads only. Nothing is created, written or overwritten.
# MAGIC
# MAGIC Three questions the star was built to answer, asked against the loaded tables
# MAGIC rather than against a design document. Each closes a phase 3 roadmap item.
# MAGIC
# MAGIC The first reconciles a price series derived from 29.6 million transactions against
# MAGIC the published index built from the same registry by a different method. Two
# MAGIC independent products of one source agreeing is the strongest evidence available
# MAGIC that the transaction pipeline is right, and the only evidence that does not come
# MAGIC from the pipeline itself.
# MAGIC
# MAGIC The second and third compute rent yield and the monthly cost of owning against
# MAGIC renting, downstream of the facts as the model intends. Neither is a table. The
# MAGIC point is to establish that the star answers them and over what population, so the
# MAGIC dashboard inherits a measured coverage figure rather than discovering one.
# MAGIC
# MAGIC Owning cost rests on three assumptions that are choices rather than measurements:
# MAGIC the deposit share, the mortgage term, and the margin a lender adds to the base
# MAGIC rate. They are named constants here and belong in the dashboard as controls. The
# MAGIC base rate itself is measured, off `dim_date`, on the day each month opens.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "uk_property_intel"
GOLD = f"{CATALOG}.gold"

DIM_AREA = f"{GOLD}.dim_area"
DIM_DATE = f"{GOLD}.dim_date"
FACT_PRICE = f"{GOLD}.fact_area_month_price"
FACT_HPI = f"{GOLD}.fact_area_month_hpi"
FACT_RENT = f"{GOLD}.fact_area_month_rent"

# Assumptions, not measurements. A repayment mortgage on the balance after a deposit,
# at the base rate in force plus a lender's margin.
DEPOSIT_SHARE = 0.20
TERM_YEARS = 25
LENDER_MARGIN_PCT = 1.00

MONTHS = TERM_YEARS * 12

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A transaction-derived series against the published index
# MAGIC
# MAGIC `fact_area_month_price` holds the median and mean of category A transactions.
# MAGIC `fact_area_month_hpi` holds the publisher's mix-adjusted average for the same area
# MAGIC and month. They are built from one registry by two methods, so they should track
# MAGIC rather than match: mix adjustment removes composition, a raw mean does not.
# MAGIC
# MAGIC The sharper check is the count. `transaction_count` is category A transactions
# MAGIC resolved through the postcode directory; `sales_volume` is what the publisher
# MAGIC reports for the same cell. Those are the same underlying sales counted by two
# MAGIC pipelines, so they should agree closely.

# COMMAND ----------

price = spark.table(FACT_PRICE).select(  # noqa: F821
    "area_code", "month_start_date", "median_price", "mean_price", "transaction_count"
)
hpi = spark.table(FACT_HPI).select(  # noqa: F821
    "area_code", "month_start_date", "avg_price", "sales_volume"
)

both = price.join(hpi, ["area_code", "month_start_date"], "inner").withColumn(
    "avg_price_int", F.col("avg_price").cast("double")
)

display(  # noqa: F821
    price.alias("p")
    .join(hpi.alias("h"), ["area_code", "month_start_date"], "full_outer")
    .agg(
        F.count(F.lit(1)).alias("cells_either_side"),
        F.count_if(F.col("median_price").isNotNull() & F.col("avg_price").isNotNull()).alias(
            "cells_in_common"
        ),
        F.count_if(F.col("avg_price").isNull()).alias("price_only"),
        F.count_if(F.col("median_price").isNull()).alias("index_only"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC How the two prices relate. A ratio near one on the mean says the transaction
# MAGIC pipeline lands where the publisher does; the median sits below both, since the
# MAGIC distribution is right-skewed.

# COMMAND ----------

ratios = both.where(F.col("avg_price_int") > 0).select(
    "area_code",
    "month_start_date",
    (F.col("mean_price") / F.col("avg_price_int")).alias("mean_over_published"),
    (F.col("median_price") / F.col("avg_price_int")).alias("median_over_published"),
)

display(  # noqa: F821
    ratios.select(
        F.expr("percentile(mean_over_published, array(0.01, 0.25, 0.5, 0.75, 0.99))").alias(
            "mean_ratio_p01_p25_p50_p75_p99"
        ),
        F.expr(
            "percentile(median_over_published, array(0.01, 0.25, 0.5, 0.75, 0.99))"
        ).alias("median_ratio_p01_p25_p50_p75_p99"),
    )
)

# COMMAND ----------

# The count check. Equal counts would mean the two pipelines selected identical sales;
# a stable small gap means one includes something the other does not, which is what
# category B and the resolution drops would produce.
counts = both.where(F.col("sales_volume").isNotNull() & (F.col("sales_volume") > 0))

display(  # noqa: F821
    counts.agg(
        F.count(F.lit(1)).alias("cells_with_both_counts"),
        F.sum("transaction_count").alias("our_transactions"),
        F.sum("sales_volume").alias("published_sales"),
        F.round(F.sum("transaction_count") / F.sum("sales_volume"), 4).alias("ratio"),
        F.corr("transaction_count", "sales_volume").alias("correlation"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Whether the relationship holds over time, or drifts. A step at a particular year
# MAGIC would mean one pipeline changed and the other did not.

# COMMAND ----------

display(  # noqa: F821
    both.where(F.col("avg_price_int") > 0)
    .groupBy(F.year("month_start_date").alias("year"))
    .agg(
        F.count(F.lit(1)).alias("cells"),
        F.round(F.avg(F.col("mean_price") / F.col("avg_price_int")), 4).alias(
            "mean_ratio"
        ),
        F.round(F.avg(F.col("median_price") / F.col("avg_price_int")), 4).alias(
            "median_ratio"
        ),
        F.round(
            F.sum("transaction_count") / F.sum(F.coalesce(F.col("sales_volume"), F.lit(0))),
            4,
        ).alias("count_ratio"),
    )
    .orderBy("year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC By level, and the cells that disagree most. A composite or nation row diverging
# MAGIC would be a systematic fault; a thin district diverging is a thin district.

# COMMAND ----------

levels = spark.table(DIM_AREA).select("area_code", "area_level", "area_name")  # noqa: F821

display(  # noqa: F821
    ratios.join(levels, "area_code", "inner")
    .groupBy("area_level")
    .agg(
        F.count(F.lit(1)).alias("cells"),
        F.round(F.expr("percentile(mean_over_published, 0.5)"), 4).alias("median_ratio"),
        F.round(F.min("mean_over_published"), 3).alias("lowest"),
        F.round(F.max("mean_over_published"), 3).alias("highest"),
    )
    .orderBy("area_level")
)

# COMMAND ----------

display(  # noqa: F821
    ratios.join(levels, "area_code", "inner")
    .join(price, ["area_code", "month_start_date"], "inner")
    .select(
        "area_name",
        "area_level",
        "month_start_date",
        "transaction_count",
        F.round("mean_over_published", 3).alias("mean_over_published"),
    )
    .orderBy(F.desc("mean_over_published"))
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rent yield
# MAGIC
# MAGIC Annual rent over price, as a percentage. Rent is published monthly per property,
# MAGIC so twelve months of it against the published average price for the same area and
# MAGIC month.
# MAGIC
# MAGIC The published index is used for the price rather than the transaction median,
# MAGIC because it is mix-adjusted and a yield compares a typical rent against a typical
# MAGIC price. The coverage figure below is the population the dashboard inherits.

# COMMAND ----------

rent = spark.table(FACT_RENT).select(  # noqa: F821
    "area_code", "month_start_date", "rental_price"
)

yields = (
    rent.join(hpi, ["area_code", "month_start_date"], "inner")
    .where(F.col("rental_price").isNotNull() & (F.col("avg_price") > 0))
    .select(
        "area_code",
        "month_start_date",
        "rental_price",
        "avg_price",
        (F.lit(12) * F.col("rental_price") / F.col("avg_price") * F.lit(100)).alias(
            "gross_yield_pct"
        ),
    )
)

display(  # noqa: F821
    yields.agg(
        F.count(F.lit(1)).alias("cells"),
        F.countDistinct("area_code").alias("areas"),
        F.min("month_start_date").alias("first_month"),
        F.max("month_start_date").alias("last_month"),
        F.round(F.expr("percentile(gross_yield_pct, 0.5)"), 3).alias("median_yield_pct"),
    )
)

# COMMAND ----------

# Coverage against decision 42, which put a yield within reach of 316 districts.
display(  # noqa: F821
    yields.join(levels, "area_code", "inner")
    .groupBy("area_level")
    .agg(
        F.countDistinct("area_code").alias("areas_with_a_yield"),
        F.round(F.expr("percentile(gross_yield_pct, 0.5)"), 3).alias("median_yield_pct"),
    )
    .orderBy("area_level")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Owning against renting, at the base rate of the day
# MAGIC
# MAGIC A repayment mortgage on the balance after a deposit, amortised over the term at
# MAGIC the base rate in force when the month opened plus a lender's margin.
# MAGIC
# MAGIC The base rate is read off `dim_date` at the month start rather than averaged over
# MAGIC the month, so the figure is reproducible and the join is to the calendar the model
# MAGIC already declares.
# MAGIC
# MAGIC Deposit, term and margin are the assumptions named at the top. Everything else
# MAGIC comes from a loaded table.

# COMMAND ----------

rates = spark.table(DIM_DATE).select(  # noqa: F821
    F.col("date_key").alias("month_start_date"),
    F.col("base_rate_pct").cast("double").alias("base_rate_pct"),
    F.col("base_rate_type"),
)

monthly_rate = (F.col("base_rate_pct") + F.lit(LENDER_MARGIN_PCT)) / F.lit(1200)
loan = F.col("avg_price") * F.lit(1 - DEPOSIT_SHARE)
growth = F.pow(F.lit(1) + monthly_rate, F.lit(MONTHS))

cost = (
    rent.join(hpi, ["area_code", "month_start_date"], "inner")
    .join(rates, "month_start_date", "inner")
    .where(F.col("rental_price").isNotNull() & (F.col("avg_price") > 0))
    .withColumn("monthly_mortgage", loan * monthly_rate * growth / (growth - F.lit(1)))
    .select(
        "area_code",
        "month_start_date",
        "base_rate_pct",
        "base_rate_type",
        "rental_price",
        "avg_price",
        F.round("monthly_mortgage").cast("int").alias("monthly_mortgage"),
        F.round(F.col("monthly_mortgage") - F.col("rental_price")).cast("int").alias(
            "owning_premium"
        ),
    )
)

display(  # noqa: F821
    cost.agg(
        F.count(F.lit(1)).alias("cells"),
        F.countDistinct("area_code").alias("areas"),
        F.min("month_start_date").alias("first_month"),
        F.max("month_start_date").alias("last_month"),
        F.count_if(F.col("owning_premium") > 0).alias("cells_where_owning_costs_more"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC The series the comparison exists to show: as the base rate moves, the cost of
# MAGIC owning moves and the rent does not follow immediately.

# COMMAND ----------

display(  # noqa: F821
    cost.groupBy(F.year("month_start_date").alias("year"))
    .agg(
        F.round(F.avg("base_rate_pct"), 3).alias("mean_base_rate_pct"),
        F.round(F.avg("monthly_mortgage")).cast("int").alias("mean_monthly_mortgage"),
        F.round(F.avg("rental_price")).cast("int").alias("mean_rent"),
        F.round(
            F.count_if(F.col("owning_premium") > 0) / F.count(F.lit(1)), 4
        ).alias("share_owning_costs_more"),
    )
    .orderBy("year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC The latest month, by area, which is the shape the dashboard screen takes.

# COMMAND ----------

latest = cost.agg(F.max("month_start_date")).collect()[0][0]

display(  # noqa: F821
    cost.where(F.col("month_start_date") == F.lit(latest))
    .join(levels, "area_code", "inner")
    .where(F.col("area_level") == F.lit("district"))
    .select(
        "area_name",
        "base_rate_pct",
        "avg_price",
        "rental_price",
        "monthly_mortgage",
        "owning_premium",
    )
    .orderBy(F.desc("owning_premium"))
    .limit(20)
)
