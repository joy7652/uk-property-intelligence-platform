# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "chispa==0.12.0",
# ]
# ///
# MAGIC %md
# MAGIC # Tests: shared modules
# MAGIC
# MAGIC Suites for modules that belong to no single source and to no single table. A source
# MAGIC notebook covers what one publisher's change can reach; these are the modules every
# MAGIC table calls, where a change reaches all of them at once.
# MAGIC
# MAGIC Run all after any change to a module below. Run it as well as the source notebooks
# MAGIC rather than instead of them: this proves the shared behaviour, and the source
# MAGIC notebooks prove that the tables calling it still hold their own contracts.
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
# MAGIC `run(path, "-k", "grain")` narrows to matching tests.
# MAGIC
# MAGIC This cell is repeated in each of the test notebooks rather than imported. Importing
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
# MAGIC ## 3. Silver Expressions
# MAGIC
# MAGIC The `parsed_date` column expression that is shared across the Silver transforms.
# MAGIC
# MAGIC Asserts against synthetic frames through the `spark` fixture. No Delta table is
# MAGIC touched.

# COMMAND ----------

run("tests/test_silver_transforms/test_expressions.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold conformance
# MAGIC
# MAGIC The key and grain checks every fact calls, and `dim_lsoa` through
# MAGIC `assert_districts_conform`. Unity Catalog enforces neither the primary nor the
# MAGIC foreign keys, so a break here is a break in the only thing that catches them.
# MAGIC
# MAGIC Asserts against synthetic frames through the `spark` fixture. No Delta table is
# MAGIC touched.

# COMMAND ----------

run("tests/test_gold_transforms/test_conformance.py")
