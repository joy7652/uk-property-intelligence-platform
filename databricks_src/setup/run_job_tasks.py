import time

import requests

# --- fill these two -------------------------------------------------------------
JOB_NAME = "ukpropertyintel-silver-gold"
TASKS = ["marts_build"]
# --------------------------------------------------------------------------------

POLL_SECONDS = 30
LOG_LINES = 150

host = spark.conf.get("spark.databricks.workspaceUrl")  # noqa: F821
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
auth = {"Authorization": f"Bearer {context.apiToken().get()}"}


def get(endpoint: str, **params) -> dict:
    response = requests.get(
        f"https://{host}/api/2.2/jobs/{endpoint}", headers=auth, params=params, timeout=60
    )
    response.raise_for_status()
    return response.json()


def outcome(state: dict) -> str:
    # A run that is skipped or fails internally ends with no result state.
    return state.get("result_state", state["life_cycle_state"])


matches = [
    job
    for job in get("list", name=JOB_NAME, limit=25).get("jobs", [])
    if job["settings"]["name"] == JOB_NAME
]
if len(matches) != 1:
    raise RuntimeError(
        f"expected one job named {JOB_NAME!r}, found {len(matches)}: "
        f"{[job['job_id'] for job in matches]}"
    )
JOB_ID = matches[0]["job_id"]

# only runs the named tasks and nothing upstream of them, so each reads whatever the
# last full run left in the tables it depends on.
started = requests.post(
    f"https://{host}/api/2.2/jobs/run-now",
    headers=auth,
    json={"job_id": JOB_ID, "only": TASKS},
    timeout=60,
)
started.raise_for_status()
RUN_ID = started.json()["run_id"]

run = get("runs/get", run_id=RUN_ID)
print(f"started  {TASKS} in {JOB_NAME}")
print(f"open     {run['run_page_url']}")

while run["state"]["life_cycle_state"] not in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
    time.sleep(POLL_SECONDS)
    run = get("runs/get", run_id=RUN_ID)

print(f"result   {outcome(run['state'])}")

# Output is fetched per task: the API refuses it for a run holding several.
for task in run.get("tasks", []):
    output = get("runs/get-output", run_id=task["run_id"])
    seconds = task.get("execution_duration", 0) // 1000
    print(f"\n{task['task_key']}  {outcome(task['state'])}  {seconds}s")
    if output.get("error"):
        print(output["error"])
    if output.get("logs"):
        if output.get("logs_truncated"):
            print("log truncated by the API; the full log is on the run page")
        print("\n".join(output["logs"].splitlines()[-LOG_LINES:]))
