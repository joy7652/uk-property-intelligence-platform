# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "chispa==0.12.0",
# ]
# ///
# MAGIC %md
# MAGIC # Tests: audit writer
# MAGIC
# MAGIC Run all after any change to `databricks_src/quality/audit/writer.py`.
# MAGIC
# MAGIC The writer sits outside the six source notebooks because nothing they test
# MAGIC imports it. Silver and Gold transforms are pure: they take DataFrames and return
# MAGIC one, and never record. Recording happens in the load notebooks, which this suite
# MAGIC does not run. So a writer change reaches every load and no transform, and it
# MAGIC needs its own file rather than a cell repeated in six that would prove nothing
# MAGIC about the source each of those files is named for.
# MAGIC
# MAGIC Two vocabularies gate a run. `SOURCES` names the Bronze sources a Silver load
# MAGIC reads; `GOLD_TABLES` names the tables a Gold load builds. Each name carries the
# MAGIC layer it belongs to, so adding a Gold table means adding it to `GOLD_TABLES` and
# MAGIC running this notebook.
# MAGIC
# MAGIC Local runs use `pytest` from the repo root with `requirements-dev.txt`
# MAGIC installed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Test-only dependency
# MAGIC
# MAGIC chispa is notebook-scoped. pytest and PySpark ship with the runtime;
# MAGIC installing either here would shadow it.
# MAGIC
# MAGIC This suite needs neither chispa nor a SparkSession: the registry, the generated
# MAGIC DDL, value routing and the freshness verdict are pure Python, and every Delta
# MAGIC write is excluded and verified by running a load notebook instead. The install
# MAGIC is kept so the seven test notebooks start the same way.

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
# MAGIC This cell is repeated in each notebook rather than imported. Importing it would
# MAGIC need the repo root on `sys.path` first, which is the thing the cell works out,
# MAGIC and a notebook that cannot be opened and run on its own defeats the split.

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
# MAGIC ## 3. Audit writer
# MAGIC
# MAGIC The metric registry, the two run-name vocabularies and their layer pairing, the
# MAGIC generated DDL against the write schema, value routing, and the freshness verdict.

# COMMAND ----------

run("tests/test_quality_audit/test_writer.py")
