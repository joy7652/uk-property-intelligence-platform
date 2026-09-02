"""Whether one stage runs, and the run row that records the answer either way.

Every Silver and Gold notebook opens with the same two questions, and neither is
answerable from inside the notebook. What failed in this job run decides what the
chain cascades forward. What actually rebuilt decides whether the tables a stage
reads are this month's, and that is a different question: a dependency that failed,
one that skipped, and one the job never reached all leave no successful run behind
them, and all three mean the same thing to whatever reads the table.

Both are asked here so that eleven notebooks carry a call rather than a copy. A copy
that loses the skip branch, or names the wrong stage, runs a load that should have
waited and reports success for it.

`dbutils` is passed in rather than imported. The runtime injects it into a notebook's
namespace, so a library reaching for it depends on how that injection works and
cannot be tested without it.

Nothing here ends a notebook. `open_stage` returns None and the caller decides, since
`dbutils.notebook.exit` raises for the notebook runner to catch, and a skipped table
inside `01_load_dimensions` must not stop the three beside it.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from databricks_src.bronze.watermark_library import source_dependency
from databricks_src.quality.audit.writer import (
    AuditRun,
    failed_in_run,
    succeeded_in_run,
)

WIDGET = "job_run_id"


@dataclass(frozen=True)
class Plan:
    """What this job run has recorded so far, read once at the top of a notebook.

    Attributes:
        job_run_id: None where the notebook is not running under a job.
        failed: Names recorded as failed, without layers. A source failing at bronze
            and at silver cascades the same way, so the layer is not part of it.
        rebuilt: (name, layer) pairs recorded as succeeded, or None where there is no
            job run and so no evidence to read. None is not an empty set: gating on
            absent evidence would make every notebook unrunnable by hand.
    """

    job_run_id: str | None
    failed: frozenset[str]
    rebuilt: frozenset[tuple[str, str]] | None

    def waiting_on(self, item: str) -> list[str]:
        """Everything stopping one item running. Empty means it runs."""
        return source_dependency.waiting_on(item, self.failed, self.rebuilt)

    def rebuilt_this_run(self, name: str, layer: str) -> bool:
        """Whether one named table has a successful run of its own in this job run.

        A different question from `waiting_on`, which asks whether something would run
        given what its own dependencies did. This asks whether a table a reader is
        about to open actually rebuilt, which is what a measurement recorded against a
        stale table cannot tell you afterwards.

        True where there is no job run at all, since the evidence does not exist to be
        absent and a notebook has to stay runnable by hand.
        """
        return self.rebuilt is None or (name, layer) in self.rebuilt


def read_plan(dbutils) -> Plan:
    """Read the job run id from the notebook's widget, then what the run recorded.

    The widget keeps a value already entered, so re-running this cell does not clear
    it. `dbutils.widgets.remove` is what resets it.
    """
    dbutils.widgets.text(WIDGET, "")
    job_run_id = dbutils.widgets.get(WIDGET).strip() or None

    rebuilt = succeeded_in_run(job_run_id)
    plan = Plan(
        job_run_id=job_run_id,
        failed=frozenset(failed_in_run(job_run_id)),
        rebuilt=None if rebuilt is None else frozenset(rebuilt),
    )

    print(f"job_run_id    : {job_run_id or 'none, run by hand'}")
    print(f"failed so far : {sorted(plan.failed) or 'none'}")
    return plan


def open_stage(
    item: str,
    layer: str,
    ingestion_ts: dt.datetime,
    plan: Plan,
) -> AuditRun | None:
    """Open the run for one stage, or record the skip and return None.

    The run is opened before the decision either way, so a skip is inserted and
    closed like any other outcome. A stage that waited is then distinguishable from
    one the job never reached, which leaves no row at all.

    Returns:
        The started run, or None where the stage is waiting on something.
    """
    waiting = plan.waiting_on(item)
    run = AuditRun(
        source=item,
        layer=layer,
        ingestion_ts=ingestion_ts,
        job_run_id=plan.job_run_id,
    )
    run.start()

    if not waiting:
        print(f"{item}: runs, run {run.run_id}")
        return run

    run.skip(waiting)
    print(f"{item}: skipped, waiting on {', '.join(waiting)}")
    return None
