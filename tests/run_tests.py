# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "chispa==0.12.0",
# ]
# ///
# MAGIC %md
# MAGIC # Run the pytest suite on the cluster
# MAGIC
# MAGIC One cell per test file, so a change to one source costs one file's tests rather
# MAGIC than all of them. The whole suite runs from the last cell, which is the check
# MAGIC worth doing before a commit.
# MAGIC
# MAGIC A new test file gets a new cell following the folder and file pattern below.
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
# MAGIC pytest's rootdir determines which `conftest.py` files load, so the run
# MAGIC must start at the repo root: `tests/conftest.py` holds the `spark`
# MAGIC fixture and puts the repo root on `sys.path`.
# MAGIC
# MAGIC `run` drops the project's own modules before each call. `pytest.main` reuses
# MAGIC whatever is already in `sys.modules`, so without this a second run after
# MAGIC editing a transform tests the copy loaded by the first run and reports a result
# MAGIC that describes code no longer on disk. Extra arguments pass through, so
# MAGIC `run(path, "-k", "freshness")` narrows to matching tests.

# COMMAND ----------

import os
import sys

import pytest

# Repo root from the notebook's own path. CWD is not dependable inside Git folders.
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
# MAGIC ## 3. Silver transforms
# MAGIC
# MAGIC Each of these builds a SparkSession through the `spark` fixture and asserts
# MAGIC against synthetic frames. No Bronze file is read and no Delta table is touched.

# COMMAND ----------

run("tests/test_silver_transforms/test_boe_base_rate.py")

# COMMAND ----------

run("tests/test_silver_transforms/test_hpi.py")

# COMMAND ----------

run("tests/test_silver_transforms/test_ppd.py")

# COMMAND ----------

run("tests/test_silver_transforms/test_doogal.py")

# COMMAND ----------

run("tests/test_silver_transforms/test_ons.py")

# COMMAND ----------

run("tests/test_silver_transforms/test_police.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quality audit
# MAGIC
# MAGIC The only suite needing no SparkSession: it covers the metric registry, the
# MAGIC generated DDL, value routing, and the freshness verdict, all of which are pure
# MAGIC Python. Every Delta write is excluded and is verified by running a Silver
# MAGIC notebook instead.

# COMMAND ----------

run("tests/test_quality_audit/test_writer.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Everything
# MAGIC
# MAGIC Before a commit, and after any change to a shared module. A file missing a cell
# MAGIC above still runs here, so this is also the check that the cells stayed in step
# MAGIC with the folder.

# COMMAND ----------

run("tests")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT m.metric, m.value_text, m.value_date
# MAGIC FROM uk_property_intel.quality.pipeline_metric m
# MAGIC JOIN uk_property_intel.quality.pipeline_run r USING (run_id)
# MAGIC WHERE r.source = 'ons'
# MAGIC   AND m.metric IN ('vintage_label', 'published_line', 'published_date');
