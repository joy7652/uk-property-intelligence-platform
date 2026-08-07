# Databricks notebook source
# MAGIC %md
# MAGIC # Create the quality tables
# MAGIC
# MAGIC Run after `01_create_schemas`. Independent of `02_create_bronze_volumes`.
# MAGIC
# MAGIC Two tables. `pipeline_run` takes one row per notebook execution whatever the
# MAGIC outcome; `pipeline_metric` takes one row per measured value, keyed on that run.
# MAGIC Both are written by `quality/audit/writer.py`, which every Silver notebook
# MAGIC imports and every Gold notebook will.
# MAGIC
# MAGIC They are created here rather than by whichever notebook runs first, because a
# MAGIC shared table created as a side effect of one source's load is an ordering
# MAGIC dependency that only shows up on a rebuild.
# MAGIC
# MAGIC **The column definitions are generated, not written here.** The other setup
# MAGIC notebooks declare their DDL literally, which is right for schemas and volumes
# MAGIC because nothing else in the repo restates them. These two tables have a second
# MAGIC declaration in the writer's own schema, and the test suite asserts the two
# MAGIC agree. Written literally in a `%sql` cell, that assertion would have nothing to
# MAGIC compare against.
# MAGIC
# MAGIC `IF NOT EXISTS` will not alter a table that already exists. A column change
# MAGIC needs the table dropped first, by hand: a bare `DROP` committed in a
# MAGIC re-runnable script destroys the history on every run.

# COMMAND ----------

from databricks_src.quality.audit.writer import (
    FRESHNESS_BOUND_DAYS,
    KINDS,
    METRIC_COMMENT,
    METRIC_CONSTRAINTS,
    METRIC_TABLE,
    METRICS,
    RUN_COMMENT,
    RUN_CONSTRAINTS,
    RUN_TABLE,
    metric_table_ddl,
    run_table_ddl,
    sql_literal,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create

# COMMAND ----------

# MAGIC %md
# MAGIC Delete Quality Table to reset.
# MAGIC ```sql
# MAGIC DROP TABLE uk_property_intel.quality.pipeline_run;
# MAGIC DROP TABLE uk_property_intel.quality.pipeline_metric;
# MAGIC ```

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {RUN_TABLE} (
    {run_table_ddl()}
    )
    USING DELTA
    COMMENT {sql_literal(RUN_COMMENT)}
    """
)

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {METRIC_TABLE} (
    {metric_table_ddl()}
    )
    USING DELTA
    COMMENT {sql_literal(METRIC_COMMENT)}
    """
)

print(f"{RUN_TABLE}\n{METRIC_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply CHECK constraints
# MAGIC
# MAGIC Dropped and re-added each run so this notebook is idempotent. `ADD CONSTRAINT`
# MAGIC validates every existing row before it attaches, which is why the police
# MAGIC notebook reads what is attached and touches only differences. These two tables
# MAGIC hold a handful of rows per load and stay far below the threshold where that
# MAGIC matters, so the simpler sibling form is kept.
# MAGIC
# MAGIC The expressions are generated from the same constants the writer sets, so the
# MAGIC table cannot reject a status or a value shape the code produces.

# COMMAND ----------

TABLES = ((RUN_TABLE, RUN_CONSTRAINTS), (METRIC_TABLE, METRIC_CONSTRAINTS))

for table, constraints in TABLES:
    print(table)
    for name, expression in constraints:
        spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")  # noqa: F821
        spark.sql(  # noqa: F821
            f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression})"
        )
        print(f"  {name}: {expression}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Both tables present and managed. An EXTERNAL table here would mean the
# MAGIC -- schema's managed location was not picked up.
# MAGIC SELECT table_name, table_type, comment
# MAGIC FROM uk_property_intel.information_schema.tables
# MAGIC WHERE table_schema = 'quality'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW COLUMNS gives names only. Types are what the writer's schema has to
# MAGIC -- match on append.
# MAGIC SELECT table_name, ordinal_position, column_name, full_data_type, is_nullable
# MAGIC FROM uk_property_intel.information_schema.columns
# MAGIC WHERE table_schema = 'quality'
# MAGIC ORDER BY table_name, ordinal_position;

# COMMAND ----------

# Delta holds CHECK constraints as table properties, so this is where an attached
# constraint is readable. A configured constraint missing here did not attach.
for table, constraints in TABLES:
    attached = {
        row["key"].split("delta.constraints.", 1)[1]: row["value"]
        for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()  # noqa: F821
        if row["key"].startswith("delta.constraints.")
    }
    missing = sorted({name for name, _ in constraints} - set(attached))
    print(f"{table}: {len(attached)} attached, missing {missing or 'none'}")
    for name, expression in sorted(attached.items()):
        print(f"  {name}: {expression}")

# COMMAND ----------

# MAGIC %md
# MAGIC The metric registry, which is what the `metric` column is constrained to by
# MAGIC convention rather than by the table. A name outside this set is rejected by the
# MAGIC writer before it reaches Delta, since a free-text name would open a series
# MAGIC nothing else writes to and read as a metric that was discontinued.

# COMMAND ----------

for kind, meaning in KINDS.items():
    named = sorted(name for name, metric in METRICS.items() if metric.kind == kind)
    print(f"{kind}  ({meaning})")
    for name in named:
        print(f"    {name:<38}{METRICS[name].note}")
    print()

# COMMAND ----------

# Bounds are read off what the first recorded runs report, not set from a publisher's
# release calendar. A source at None records its freshness value and asserts nothing.
for source, bound in FRESHNESS_BOUND_DAYS.items():
    print(f"{source:<10}{bound if bound is not None else 'unset, recording only'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read shape
# MAGIC
# MAGIC Empty on first run. These are the two queries the Phase 5 dashboard is built
# MAGIC from, kept here so the table is verified against its intended read rather than
# MAGIC against its definition alone.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run success rate by source. A run left at 'started' is a load that was
# MAGIC -- killed rather than one that failed, and the two are counted separately.
# MAGIC SELECT source,
# MAGIC        count(*) AS runs,
# MAGIC        count_if(status = 'succeeded') AS succeeded,
# MAGIC        count_if(status = 'failed') AS failed,
# MAGIC        count_if(status = 'started') AS never_finished,
# MAGIC        round(100 * count_if(status = 'succeeded') / count(*), 1) AS pct_succeeded,
# MAGIC        max(ended_ts) AS last_completed
# MAGIC FROM uk_property_intel.quality.pipeline_run
# MAGIC GROUP BY source
# MAGIC ORDER BY source;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A share over time, summed across a run's subdivisions before it is divided.
# MAGIC -- Averaging the per-archive percentages instead would weight a small archive
# MAGIC -- the same as a large one.
# MAGIC SELECT r.source,
# MAGIC        m.metric,
# MAGIC        date(r.started_ts) AS run_date,
# MAGIC        sum(m.value_numeric) AS value,
# MAGIC        sum(m.denominator) AS base,
# MAGIC        round(100 * sum(m.value_numeric) / nullif(sum(m.denominator), 0), 4) AS pct
# MAGIC FROM uk_property_intel.quality.pipeline_metric m
# MAGIC JOIN uk_property_intel.quality.pipeline_run r USING (run_id)
# MAGIC WHERE m.denominator IS NOT NULL
# MAGIC GROUP BY r.source, m.metric, date(r.started_ts)
# MAGIC ORDER BY r.source, m.metric, run_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Freshness as recorded. This is the query the per-source bounds are read off
# MAGIC -- before any of them is set.
# MAGIC SELECT r.source,
# MAGIC        r.started_ts,
# MAGIC        m.value_date AS newest_data_date,
# MAGIC        datediff(date(r.started_ts), m.value_date) AS lag_days
# MAGIC FROM uk_property_intel.quality.pipeline_metric m
# MAGIC JOIN uk_property_intel.quality.pipeline_run r USING (run_id)
# MAGIC WHERE m.metric = 'newest_data_date'
# MAGIC ORDER BY r.source, r.started_ts DESC;
