# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Tests: marts build record
# MAGIC
# MAGIC Run after any change to `databricks_src/marts/build_record.py`, and after a change to
# MAGIC the metric registry in the audit writer, whose names this suite holds the recorder's
# MAGIC metrics against.
# MAGIC
# MAGIC The module reads the dbt task's outcome off the job run and its views and tests off
# MAGIC `run_results.json`, and decides what the marts build records. Wrong one way, a failed
# MAGIC build records as succeeded. Wrong the other, a build dbt passed fails the recorder,
# MAGIC and since the recorder is the job's leaf task, the whole job run reads as failed with
# MAGIC nothing wrong in the data.
# MAGIC
# MAGIC No chispa and no `restartPython`. Nothing in this suite builds a DataFrame. The dbt
# MAGIC results are fixture files captured from dbt-core 1.12.5 and kept in
# MAGIC `tests/test_marts/fixtures/`, and the job run entries are built in the tests.
# MAGIC
# MAGIC Local runs use `pytest` from the repo root with `requirements-dev.txt` installed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Run from the repo root
# MAGIC
# MAGIC pytest's rootdir determines which `conftest.py` files load, so the run must start
# MAGIC at the repo root: `tests/conftest.py` holds the `spark` fixture and puts the repo
# MAGIC root on `sys.path`. Nothing here requests that fixture, but the path setup is what
# MAGIC makes `databricks_src` importable.
# MAGIC
# MAGIC `run` drops the project's own modules before each call. `pytest.main` reuses
# MAGIC whatever is already in `sys.modules`, so without this a second run after editing a
# MAGIC transform tests the copy loaded by the first run and reports a result that
# MAGIC describes code no longer on disk. Extra arguments pass through, so
# MAGIC `run(path, "-k", "verdict")` narrows to matching tests.
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
# MAGIC ## 2. The build record
# MAGIC
# MAGIC Four things, in the order the recorder meets them. Reading the task: whether it ran,
# MAGIC how it ended, and its execution window measured back from the end, since a task's
# MAGIC start time can be when the job service opened it. Finding `run_results.json` in an
# MAGIC archive whose layout Databricks does not document. Refusing a file that is not this
# MAGIC build's, by dbt's own timestamps against the window. And the verdict, where the
# MAGIC task's result decides and the results add what broke.
# MAGIC
# MAGIC The whole folder rather than one file, so a suite added to the package joins this
# MAGIC notebook without an edit.

# COMMAND ----------

run("tests/test_marts")
