# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold: cross-source verification
# MAGIC
# MAGIC Reads the Gold tables and writes nothing back to them. It does open an audit
# MAGIC run and append to `quality.rule_result`, so the reconciliation is recorded on
# MAGIC every execution rather than read off a screen once.
# MAGIC
# MAGIC Three questions the star was built to answer, asked against the loaded tables
# MAGIC rather than against a design document. Each closes a phase 3 roadmap item.
# MAGIC
# MAGIC The first reconciles a price series derived from 29.6 million transactions against
# MAGIC the published index built from the same registry by a different method. Two
# MAGIC independent products of one source agreeing is the only evidence here that does
# MAGIC not come from the pipeline itself.
# MAGIC
# MAGIC That section evaluates three registered rules and records every result with the
# MAGIC bounds in force. The aggregate correlation is a coarse check: its cells span
# MAGIC district to composite, so the two counts cover four orders of magnitude and the
# MAGIC correlation stays above 0.999 even where districts disagree badly. The per-year
# MAGIC ratios are the sharper test.
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

from datetime import datetime, timezone

from pyspark.sql import functions as F

from databricks_src.orchestration import stage
from databricks_src.quality.rules.evaluator import (
    RULE_TABLE,
    assert_rules_reported,
    evaluate,
    failures,
    rule_frame,
)

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

# The rules this run promises to evaluate. Named here rather than derived from what
# the run happens to produce, because a rule that stops running is the failure no
# table constraint can catch.
EXPECTED_RULES = (
    "ppd_hpi_count_correlation",
    "ppd_hpi_count_ratio_by_year",
    "ppd_hpi_median_ratio_by_year",
)

# UTC by construction rather than by whatever the driver's clock is set to, as
# everywhere else that stamps a row.
INGESTION_TS = datetime.now(timezone.utc)

# Results accumulate across the cells below and are written once, at the end.
results: list = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## The stage plan
# MAGIC
# MAGIC The run opens on what the three rules require, which `CHECK_INPUTS` declares:
# MAGIC the transaction series, the published index, and the areas they are broken down
# MAGIC by. Rent and the calendar are read only by sections that record nothing, so they
# MAGIC gate themselves below rather than closing the whole check.
# MAGIC
# MAGIC That distinction is the point of the gate here. A rule evaluated over a table
# MAGIC that did not rebuild reconciles this month against last and writes the answer to
# MAGIC `rule_result` as a reading, which is worse than reporting nothing.
# MAGIC
# MAGIC Sections 2 and 3 read a table each that the rules do not. Losing the
# MAGIC reconciliation because rent failed would skip the one piece of evidence here that
# MAGIC does not come from the pipeline itself.

# COMMAND ----------

plan = stage.read_plan(dbutils)  # noqa: F821
run = stage.open_stage("cross_source_verification", "gold", INGESTION_TS, plan)

if run is None:
    dbutils.notebook.exit("skipped: cross_source_verification")  # noqa: F821

RENT_REBUILT = plan.rebuilt_this_run("fact_area_month_rent", "gold")
CALENDAR_REBUILT = plan.rebuilt_this_run("dim_date", "gold")

print(f"rent yield          : {'runs' if RENT_REBUILT else 'skipped, rent did not rebuild'}")
print(
    "owning against rent : "
    + ("runs" if RENT_REBUILT and CALENDAR_REBUILT else "skipped, an input did not rebuild")
)

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

summary = counts.agg(
    F.count(F.lit(1)).alias("cells_with_both_counts"),
    F.sum("transaction_count").alias("our_transactions"),
    F.sum("sales_volume").alias("published_sales"),
    F.round(F.sum("transaction_count") / F.sum("sales_volume"), 4).alias("ratio"),
    F.corr("transaction_count", "sales_volume").alias("correlation"),
).collect()[0]

# our_transactions sums across four area levels, so it counts each sale about four
# times and is not a transaction total. The ratio is unaffected: both sides are summed
# over the same cells.
results.append(evaluate("ppd_hpi_count_correlation", summary["correlation"]))

display([summary.asDict()])  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC Whether the relationship holds over time, or drifts. A step at a particular year
# MAGIC would mean one pipeline changed and the other did not.

# COMMAND ----------

# Both sides of the count ratio are restricted to cells carrying both counts.
# Coalescing a missing sales_volume to zero leaves our transactions in the numerator
# with nothing opposite them, which inflates any year the publisher has not finished
# reporting: 2026 read 1.5716 that way against 0.991 like for like.
counted = F.col("sales_volume") > 0

yearly = (
    both.where(F.col("avg_price_int") > 0)
    .groupBy(F.year("month_start_date").alias("year"))
    .agg(
        F.count(F.lit(1)).alias("cells"),
        F.count_if(counted).alias("cells_with_both_counts"),
        F.round(F.avg(F.col("mean_price") / F.col("avg_price_int")), 4).alias(
            "mean_ratio"
        ),
        F.round(F.avg(F.col("median_price") / F.col("avg_price_int")), 4).alias(
            "median_ratio"
        ),
        F.round(
            F.sum(F.when(counted, F.col("transaction_count")))
            / F.sum(F.when(counted, F.col("sales_volume"))),
            4,
        ).alias("count_ratio"),
    )
    .orderBy("year")
    .collect()
)

# A year the publisher has reported no volume for at all yields a null ratio, which is
# an absence of evidence rather than a breach. Those years are skipped and counted, so
# a growing number of them is visible rather than silent.
skipped = []
for row in yearly:
    year = str(row["year"])
    if row["count_ratio"] is None:
        skipped.append(year)
    else:
        results.append(
            evaluate("ppd_hpi_count_ratio_by_year", row["count_ratio"], scope=year)
        )
    if row["median_ratio"] is not None:
        results.append(
            evaluate("ppd_hpi_median_ratio_by_year", row["median_ratio"], scope=year)
        )

print(f"{len(yearly)} years, {len(skipped)} with no published volume: {skipped or 'none'}")
display([row.asDict() for row in yearly])  # noqa: F821

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

if RENT_REBUILT:
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

if RENT_REBUILT:
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

if RENT_REBUILT and CALENDAR_REBUILT:
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

if RENT_REBUILT and CALENDAR_REBUILT:
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

if RENT_REBUILT and CALENDAR_REBUILT:
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record the verdict
# MAGIC
# MAGIC Results are written before the verdict is read, so a breach leaves the evidence
# MAGIC behind rather than only a failed run. Completeness is asserted after the write
# MAGIC for the same reason: a rule that did not report is a fault worth seeing
# MAGIC alongside the ones that did.
# MAGIC
# MAGIC Every rule is evaluated before any of them raises. Stopping at the first breach
# MAGIC would leave the rest unevaluated, and the completeness check would then have
# MAGIC nothing to check.

# COMMAND ----------

evaluated = tuple(results)

with run.step():
    rule_frame(spark, run.run_id, evaluated).write.mode(  # noqa: F821
        "append"
    ).saveAsTable(RULE_TABLE)
    assert_rules_reported(evaluated, EXPECTED_RULES)

print(f"{len(evaluated)} results written to {RULE_TABLE}")

# COMMAND ----------

breached = failures(evaluated)

with run.step():
    if breached:
        raise ValueError(
            f"{len(breached)} of {len(evaluated)} rules breached their bounds: "
            f"{[(r.rule, r.scope, r.observed, r.lower_bound, r.upper_bound) for r in breached[:10]]}"
        )

run.succeed(rows_written=len(evaluated))
print(f"{run.source}: {len(evaluated)} rules passed")
