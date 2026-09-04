# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Tests: Bronze watermark
# MAGIC
# MAGIC Run all after any change to `databricks_src/bronze/watermark_library/`. The
# MAGIC registry runs first, then each source resolver, so a broken merge is reported
# MAGIC where it broke rather than inside the resolver that noticed.
# MAGIC
# MAGIC These suites cover the file ADF reads before anything else runs. A wrong URL is
# MAGIC fetched exactly as written and lands in Bronze under whatever name the watermark
# MAGIC gives it, so the failure surfaces in the Silver reader as a malformed file,
# MAGIC several steps from its cause.
# MAGIC
# MAGIC Unlike the source notebooks, nothing here touches a Gold table. The watermark
# MAGIC reaches Silver only by deciding which file Silver reads.
# MAGIC
# MAGIC Local runs use `pytest` from the repo root with `requirements-dev.txt`
# MAGIC installed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. No test-only dependency
# MAGIC
# MAGIC Both suites are pure Python: no chispa, no SparkSession, no network. The page is
# MAGIC fetched by `01_pre_run_resolve_urls` and passed in as text, which is what keeps
# MAGIC the parse testable here. pytest ships with the runtime.

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
# MAGIC module tests the copy loaded by the first run and reports a result that describes
# MAGIC code no longer on disk. Extra arguments pass through, so
# MAGIC `run(path, "-k", "suffix")` narrows to matching tests.
# MAGIC
# MAGIC This cell is repeated in each runner notebook rather than imported. Importing it
# MAGIC would need the repo root on `sys.path` first, which is the thing the cell works
# MAGIC out, and a notebook that cannot be opened and run on its own defeats the split.

# COMMAND ----------

import os
import sys

import pytest

# Repo root from the notebook's own path. CWD is not dependable inside Git folders.
# Two levels, so this notebook must stay in the folder it was written into.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
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
# MAGIC ## 3. Registry
# MAGIC
# MAGIC Lookup and field merge over the entry array, shared by both notebooks. Covers
# MAGIC what has to stay still: five entries untouched, order preserved, and a field the
# MAGIC entry does not already carry refused rather than added.

# COMMAND ----------

run("tests/test_bronze_watermark/test_registry.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Source dependency
# MAGIC
# MAGIC The three-step chain and the skip cascade. The three worked examples were settled by
# MAGIC hand before the code existed and are pinned as tests, so a change to the cascade that
# MAGIC quietly alters one of them fails here rather than replanning a run.
# MAGIC
# MAGIC Also covers what the chain refuses: a failed fact, which nothing is built behind, and
# MAGIC a name no chain carries. A stage reads whatever this run recorded as failed, and
# MAGIC raising on a name that decides nothing would stop a run for no reason.

# COMMAND ----------

run("tests/test_bronze_watermark/test_source_dependency.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Source resolvers
# MAGIC
# MAGIC One per source whose URL moves between releases, plus the failure type the three
# MAGIC share. The fixtures are answers read off the live publishers, so a parse that assumed
# MAGIC a pattern a publisher does not follow fails here.
# MAGIC
# MAGIC `test_resolution` is not a fourth resolver. It checks that all three raise one
# MAGIC catchable type, which no per-source suite can: each imports the name from its own
# MAGIC module, so all three would keep passing against a local copy that the notebook's
# MAGIC single `except` had stopped catching.
# MAGIC
# MAGIC PPD, Doogal and BoE publish at fixed URLs and have no resolver. Nothing for them
# MAGIC belongs in this section until that changes.

# COMMAND ----------

run("tests/test_bronze_watermark/test_resolution.py")

# COMMAND ----------

run("tests/test_bronze_watermark/test_ons.py")

# COMMAND ----------

run("tests/test_bronze_watermark/test_hpi.py")

# COMMAND ----------

run("tests/test_bronze_watermark/test_police.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Watermark schema
# MAGIC
# MAGIC The key-level contract for the watermark, and the invariants no schema keyword can
# MAGIC hold. A key an ADF expression names and an entry does not carry fails the run outright,
# MAGIC and at that point a typo and an omission are the same fault, so both are caught here
# MAGIC instead of at half past nine on a release morning.
# MAGIC
# MAGIC `registry.load` already refuses the array-level faults: invalid JSON, a non-array, an
# MAGIC empty array, a missing `source_name`, a repeated one. Nothing here repeats them. What
# MAGIC this adds is the per-entry shape, which keys each load pattern brings with it, and the
# MAGIC two checks JSON Schema has no keyword for. A whole number written as a decimal reads as
# MAGIC an integer to a validator and as `8.0` to ADF's typed parameter. Two sources sharing a
# MAGIC Bronze folder satisfy the schema one entry at a time while one overwrites the other.
# MAGIC
# MAGIC No value is asserted. Not that a release date is recent, not that a refresh month is
# MAGIC the one intended, not that a URL resolves. A watermark can pass every check in this
# MAGIC section and be pointed at last year's file.

# COMMAND ----------

run("tests/test_bronze_watermark/test_schema.py")
