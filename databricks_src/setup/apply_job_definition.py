import json

import requests

# --- fill these two -------------------------------------------------------------
DEFINITION = (
    "/Workspace/Users/kiitkkat@gmail.com/uk-property-intelligence-platform"
    "/databricks_src/setup/job_definition_pre_run.json"
)
MANAGED_IDENTITY_CLIENT_ID = "7fb707ce-1e9e-4ad0-b452-922d76395756"
# --------------------------------------------------------------------------------

host = spark.conf.get("spark.databricks.workspaceUrl")  # noqa: F821
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
auth = {"Authorization": f"Bearer {context.apiToken().get()}"}

with open(DEFINITION, encoding="utf-8") as handle:
    settings = json.load(handle)

# The underscore keys carry the reasoning for readers of the file. The API rejects
# fields it does not declare, so they are stripped before the call rather than left
# out of the file.
settings = {key: value for key, value in settings.items() if not key.startswith("_")}
name = settings["name"]

# Create makes a new job every time it is called, so a second run would leave two jobs
# with the same name and ADF pointing at whichever id was recorded first. Look first.
listed = requests.get(
    f"https://{host}/api/2.2/jobs/list",
    headers=auth,
    params={"name": name, "limit": 25},
    timeout=60,
)
listed.raise_for_status()
matches = [job for job in listed.json().get("jobs", []) if job["settings"]["name"] == name]

if len(matches) > 1:
    raise RuntimeError(
        f"{len(matches)} jobs already named {name!r}: "
        f"{[job['job_id'] for job in matches]}. Delete the ones ADF does not point at "
        "before running this again."
    )

if matches:
    JOB_ID = matches[0]["job_id"]
    # reset replaces the whole settings object, so the full file is sent rather than a
    # fragment. Anything absent here is removed from the job.
    updated = requests.post(
        f"https://{host}/api/2.1/jobs/reset",
        headers=auth,
        json={"job_id": JOB_ID, "new_settings": settings},
        timeout=60,
    )
    updated.raise_for_status()
    print(f"updated  {name}")
else:
    created = requests.post(
        f"https://{host}/api/2.1/jobs/create",
        headers=auth,
        json=settings,
        timeout=60,
    )
    created.raise_for_status()
    JOB_ID = created.json()["job_id"]
    print(f"created  {name}")

print(f"job_id   {JOB_ID}")
print(f"open     https://{host}/jobs/{JOB_ID}")

# PATCH, not PUT. PUT replaces the whole access control list and would strip your own
# ownership off the job. service_principal_name takes the application id, not the
# display name.
granted = requests.patch(
    f"https://{host}/api/2.0/permissions/jobs/{JOB_ID}",
    headers=auth,
    json={
        "access_control_list": [
            {
                "service_principal_name": MANAGED_IDENTITY_CLIENT_ID,
                "permission_level": "CAN_MANAGE_RUN",
            }
        ]
    },
    timeout=60,
)
granted.raise_for_status()

print("\npermissions")
for entry in requests.get(
    f"https://{host}/api/2.0/permissions/jobs/{JOB_ID}", headers=auth, timeout=60
).json()["access_control_list"]:
    who = (
        entry.get("service_principal_name")
        or entry.get("user_name")
        or entry.get("group_name")
    )
    levels = [permission["permission_level"] for permission in entry["all_permissions"]]
    print(f"  {who:44}{levels}")

print(f"\ntasks    {len(settings['tasks'])}")
for task in settings["tasks"]:
    cap = task.get("timeout_seconds")
    print(f"  {task['task_key']:24}{str(cap // 60) + 'm' if cap else 'no cap'}")
