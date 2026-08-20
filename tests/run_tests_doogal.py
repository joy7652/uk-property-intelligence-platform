# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "chispa==0.12.0",
# ]
# ///
# MAGIC %md
# MAGIC # Tests: Postcode lookup
# MAGIC
# MAGIC Run all after any change to `databricks_src/silver/transforms/doogal.py` or to
# MAGIC a Gold transform reading it. The Silver suite runs first, then every Gold table
# MAGIC built on it, so a broken contract is reported at the layer that broke it rather
# MAGIC than at the layer that noticed.
# MAGIC
# MAGIC The widest closure: seven Gold tables read the postcode lookup. It resolves district for every area-grain fact, supplies the small-area codes both boundary vintages are compared against, and its district counts decide the majority assignment for the 80 straddling areas. A change to its column selection reaches almost all of Gold.
# MAGIC
# MAGIC A Gold table reading several sources appears in each of their notebooks. That is
# MAGIC the point: whichever source moved, one notebook covers what it can reach.
# MAGIC
# MAGIC Local runs use `pytest` from the repo root with `requirements-dev.txt`
# MAGIC installed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Test-only dependency
# MAGIC
# MAGIC chispa is notebook-scoped. pytest and PySpark ship with the runtime;
# MAGIC installing either here would shadow it.

# COMMAND ----------

# MAGIC %pip install chispa==0.12.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run from the repo root
# MAGIC
# MAGIC pytest's rootdir determines which `conftest.py` files load, so the run must start
# MAGIC at the repo root: `tests/conftest.py` holds the `spark` fixture and puts the repo
# MAGIC root on `sys.path`.
# MAGIC
# MAGIC `run` drops the project's own modules before each call. `pytest.main` reuses
# MAGIC whatever is already in `sys.modules`, so without this a second run after editing a
# MAGIC transform tests the copy loaded by the first run and reports a result that
# MAGIC describes code no longer on disk. Extra arguments pass through, so
# MAGIC `run(path, "-k", "freshness")` narrows to matching tests.
# MAGIC
# MAGIC This cell is repeated in each of the six notebooks rather than imported. Importing
# MAGIC it would need the repo root on `sys.path` first, which is the thing the cell
# MAGIC works out, and a notebook that cannot be opened and run on its own defeats the
# MAGIC split.

# COMMAND ----------

import os
import sys

import pytest

# Repo root from the notebook's own path. CWD is not dependable inside Git folders.
# Two levels, so this notebook must stay in the folder it was written into.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
REPO_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(_ctx.notebookPath().get()))

os.chdir(REPO_ROOT)
sys.dont_write_bytecode = True  # the workspace filesystem rejects .pyc writes

print(f"rootdir: {REPO_ROOT}")


def run(target: str, *args: str) -> None:
    """Run one test file or directory on freshly imported modules."""
    stale = [
        name
        for name in sys.modules
        if name.startswith("databricks_src") or name.startswith("test_")
    ]
    for name in stale:
        del sys.modules[name]
    retcode = pytest.main([target, "-v", "-p", "no:cacheprovider", *args])
    assert retcode == 0, f"{target}: pytest exit code {retcode}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver transform
# MAGIC
# MAGIC Asserts against synthetic frames through the `spark` fixture. No Bronze file is
# MAGIC read and no Delta table is touched.

# COMMAND ----------

run("tests/test_silver_transforms/test_doogal.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold tables built from this source
# MAGIC
# MAGIC Seven tables read this source.
# MAGIC
# MAGIC The remaining tables are declared in
# MAGIC `databricks_src/gold/notebooks/00_create_gold_tables.py` and load from phase
# MAGIC 3.3 onwards. Their cells are written and commented out below, so adding one
# MAGIC is uncommenting a line rather than working out which tables belong here.

# COMMAND ----------

run("tests/test_gold_transforms/test_dim_area.py")

# COMMAND ----------

run("tests/test_gold_transforms/test_dim_lsoa.py")

# COMMAND ----------

run("tests/test_gold_transforms/test_fact_area_month_price.py")

# COMMAND ----------

run("tests/test_gold_transforms/test_fact_area_month_transaction_mix.py")

# COMMAND ----------

run("tests/test_gold_transforms/test_fact_lsoa_year_price.py")

# COMMAND ----------

run("tests/test_gold_transforms/test_transactions.py")

# COMMAND ----------

run("tests/test_gold_transforms/test_fact_area_month_crime.py")

# COMMAND ----------

run("tests/test_gold_transforms/test_fact_area_month_crime_total.py")
