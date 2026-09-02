# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Tests: orchestration
# MAGIC
# MAGIC The gate every Silver and Gold notebook opens with. `stage.py` decides whether one
# MAGIC stage runs and records the answer either way, so a change here reaches eleven
# MAGIC notebooks at once and none of them would look unusual while it did: a gate that
# MAGIC opens too far loads against tables that never rebuilt, and one that closes too far
# MAGIC skips a stage nothing was wrong with. Both report success.
# MAGIC
# MAGIC Run after any change to `stage.py`. Run `run_tests_watermark` as well after a
# MAGIC change to `source_dependency`, which this calls through for the cascade itself and
# MAGIC which is covered there rather than here.
# MAGIC
# MAGIC No chispa and no `restartPython`. Nothing in this suite builds a DataFrame:
# MAGIC `dbutils` arrives as a parameter and the two Delta readers are replaced with what
# MAGIC they would have returned, which is what lets the widget read and the skip path be
# MAGIC exercised at all.
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
# MAGIC `run(path, "-k", "skip")` narrows to matching tests.
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
# MAGIC ## 2. The stage gate
# MAGIC
# MAGIC `read_plan` reads the job run id from the notebook's own widget and both questions
# MAGIC the gate asks: what this run recorded as failed, which the chain cascades forward,
# MAGIC and what actually rebuilt, name and layer together. `open_stage` opens the run and
# MAGIC either hands it back or records the skip and returns nothing.
# MAGIC
# MAGIC What is proved here is the wiring rather than the cascade. That the job run id
# MAGIC reaches the run row, without which no later stage reads the failure and a broken
# MAGIC run reports success throughout. That absent evidence stays distinct from empty
# MAGIC evidence, since collapsing them blocks every stage of a hand run. And that nothing
# MAGIC in the module ends a notebook: the fake `dbutils.notebook.exit` raises if it is
# MAGIC called, because a skipped table inside `01_load_dimensions` must not stop the three
# MAGIC beside it.
# MAGIC
# MAGIC The whole folder rather than one file, so a suite added to the package joins this
# MAGIC notebook without an edit.

# COMMAND ----------

run("tests/test_orchestration")
