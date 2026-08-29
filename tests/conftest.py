"""Shared pytest fixtures for the Silver transform test suite.

The session-scoped SparkSession mirrors the cluster's relevant semantics
(Spark 4.0, UTC session timezone) while staying cheap to start: local mode,
one shuffle partition, UI off. Transform modules under test are pure --
no Delta, no Unity Catalog, no ADLS -- so a bare local session is enough.

On Databricks the fixture reuses the cluster's session: builder options are
ignored once a session exists, and stopping it would detach the notebook.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Make the repo root importable so tests can import
# databricks_src.silver.transforms.* without an editable install.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole test run.

    UTC session timezone keeps date and timestamp literals deterministic
    wherever the suite runs; a single shuffle partition keeps the tiny test
    DataFrames fast. On a cluster the timezone is set for the duration of the
    run and restored afterwards, since the session outlives the suite.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        previous_tz = active.conf.get("spark.sql.session.timeZone")
        active.conf.set("spark.sql.session.timeZone", "UTC")
        yield active
        active.conf.set("spark.sql.session.timeZone", previous_tz)
        return

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("uk-property-intel-silver-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
