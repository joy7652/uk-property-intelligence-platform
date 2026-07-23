# Databricks notebook source
# MAGIC %md
# MAGIC # Run the pytest suite on the cluster
# MAGIC
# MAGIC Runs everything under `tests/` against real DBR semantics. Local runs use
# MAGIC `pytest` from the repo root with `requirements-dev.txt` installed.

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

# COMMAND ----------

retcode = pytest.main(["tests", "-v", "-p", "no:cacheprovider"])
assert retcode == 0, f"pytest exit code {retcode}"
