# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: resolve URLs before the run
# MAGIC
# MAGIC Runs first in the pipeline, ahead of ADF's Lookup. Two jobs: clear the failure
# MAGIC log so an empty folder means this run, and give every source a `latest_release`
# MAGIC the gate can read.
# MAGIC
# MAGIC Three sources move. ONS rotates its whole path monthly and buries an arbitrary
# MAGIC suffix in the file name, so its URL is read off the dataset page. HPI carries the
# MAGIC data month in a fixed pattern, so candidate addresses are built and confirmed by
# MAGIC HEAD. Police names its archive by data month, which an endpoint reports directly.
# MAGIC PPD, Doogal and BoE publish at fixed addresses and are only asked when they last
# MAGIC changed.
# MAGIC
# MAGIC `latest_release` is what the publisher offers now; `last_refreshed` is when this
# MAGIC pipeline last succeeded for that source. ADF fetches where the first is at or
# MAGIC after the second, so a source with nothing new is skipped and a source whose last
# MAGIC run failed is retried without anything having to remember that it failed.
# MAGIC
# MAGIC Every `latest_release` written here is a publication date, never a data month.
# MAGIC The two are the same thing for ONS, whose URL carries the release date, and two
# MAGIC months apart for HPI and Police, whose URLs carry the month of the data inside.
# MAGIC A month written into this field would sit permanently behind a `last_refreshed`
# MAGIC that advances on every successful run, and the gate would read skip even on a run
# MAGIC that had just found a file the watermark does not hold.
# MAGIC
# MAGIC A resolver that cannot read its publisher writes a marker and leaves the
# MAGIC watermark alone. That source then looks unchanged to ADF, which is why the marker
# MAGIC matters: without it a failure to resolve would be indistinguishable from a month
# MAGIC with no release.

# COMMAND ----------

import datetime as dt
import json
import urllib.error
import urllib.request
import uuid

from databricks_src.bronze.watermark_library import (
    hpi,
    ons,
    police,
    registry,
    resolution,
    schema,
)

VOLUME = "/Volumes/uk_property_intel/configs/watermark"
WATERMARK_PATH = f"{VOLUME}/watermark.json"
LOG_PATH = f"{VOLUME}/log"

# Read from the repository and checked against its own meta-schema once, then passed
# to each call site. The schema is code and lives with the code; the watermark is
# state and lives on the volume beside the data.
WATERMARK_SCHEMA = schema.load_schema()

# Sent on every publisher request. ONS returns 403 to some default clients, and a
# request that identifies itself is the courtesy a scraped page is owed.
USER_AGENT = "uk-property-intelligence-platform/1.0"

FETCH_TIMEOUT_SECONDS = 60

RUN_STARTED = dt.datetime.now()

# The run date is fixed in UTC rather than left to the driver's clock, so a cluster
# whose OS timezone is changed later cannot move one side of the gate without moving
# the other. `astimezone` reads a naive datetime as local time, so this is a no-op on
# a cluster already on UTC and a correction on one that is not.
RUN_DATE = RUN_STARTED.astimezone(dt.timezone.utc).date()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clear the failure log
# MAGIC
# MAGIC Deleted and recreated rather than filtered on read. An empty folder then means
# MAGIC every fetch in this run succeeded, with no timestamp comparison to get wrong and
# MAGIC no chance of reading last month's markers as this month's.
# MAGIC
# MAGIC First, so that a resolver failing below leaves a marker in a folder that is
# MAGIC already known to be this run's.

# COMMAND ----------

dbutils.fs.rm(LOG_PATH, recurse=True)  # noqa: F821
dbutils.fs.mkdirs(LOG_PATH)  # noqa: F821

print(f"{LOG_PATH}: cleared, {len(dbutils.fs.ls(LOG_PATH))} markers")  # noqa: F821


def record_failure(source_name: str, target: str, error: str) -> None:
    """Write one marker, in the shape a failed Copy activity writes.

    One file per failure and a guid in the name, so nothing collides and nothing has
    to be parsed out of a filename that also has to round-trip.
    """
    marker = {
        "source_name": source_name,
        "failed_at": dt.datetime.now().isoformat(timespec="seconds"),
        "target": target,
        "error": error[:2000],
    }
    path = f"{LOG_PATH}/{source_name}__{uuid.uuid4()}.json"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(marker, handle, indent=2)
    print(f"  marker written: {path}")


def head(url: str) -> tuple[int, dict]:
    """Status and headers for one object, without its body.

    A 404 is returned rather than raised. It is an ordinary answer twice over: a month
    HPI has not published yet, and the window between police.uk advancing its endpoint
    and the matching archive appearing. Every other status raises and is recorded,
    because a fixed address answering 403 or 500 is a real failure.
    """
    request = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
            return response.status, response.headers
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return 404, error.headers
        raise


# Raised by a resolver, and caught alongside the network errors that reach the same
# place. One type covers all four, which is why it lives in its own module.
RESOLUTION_FAILED = (resolution.ResolutionError, urllib.error.URLError, OSError)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the watermark
# MAGIC
# MAGIC One JSON array, the same file ADF's Lookup reads. Loading it through the
# MAGIC registry checks it is an array of uniquely named entries before anything is
# MAGIC changed, since a malformed watermark reaches ADF exactly as found.
# MAGIC
# MAGIC The schema check that follows is the per-entry half: which keys each load
# MAGIC pattern carries, and their types. A key an ADF expression names and an entry
# MAGIC does not carry fails the pipeline run outright, so a hand-edit that arrived
# MAGIC since the last run stops here rather than three activities later.

# COMMAND ----------

with open(WATERMARK_PATH, encoding="utf-8") as handle:
    original = registry.load(handle.read())

schema.validate(original, WATERMARK_SCHEMA)
schema.assert_invariants(original)

print(f"{len(original)} sources: {', '.join(registry.names(original))}")
print("watermark matches the schema")

entries = original

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ONS private rents
# MAGIC
# MAGIC The dataset page lists every edition with its file link. The release date is
# MAGIC parsed out of each and the newest wins, so a reordering upstream cannot hand back
# MAGIC an older file than the one already ingested.
# MAGIC
# MAGIC The published suffix is not a sequence. Across five consecutive months it has run
# MAGIC 10, none, 13, 14, none, which is why the URL is read rather than constructed.
# MAGIC Bronze renames on the release month so the landing file says which release it is.
# MAGIC
# MAGIC The edition segment is the release date, so this is the one moving source where
# MAGIC the URL answers the gate's question directly.

# COMMAND ----------

ONS_SOURCE = "ons_private_rent_index"
ONS_DATASET_PATH = (
    "/economy/inflationandpriceindices/datasets/"
    "priceindexofprivaterentsukmonthlypricestatistics"
)

current = registry.find(entries, ONS_SOURCE)
print(f"watermark holds  {current['latest_release']}  {current['file_name']}")

try:
    request = urllib.request.Request(
        ons.ONS_HOST + ONS_DATASET_PATH,
        headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
    )
    with urllib.request.urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
        page = response.read().decode("utf-8", "replace")

    released, relative_url, changed = ons.resolve(
        page, ONS_DATASET_PATH, current["relative_url"]
    )
    print(f"publisher offers {released}  ({'new' if changed else 'unchanged'})")

    entries = registry.update(
        entries,
        ONS_SOURCE,
        {
            "relative_url": relative_url,
            "file_name": ons.landing_file_name(released),
            "latest_release": released.isoformat(),
        },
    )
except RESOLUTION_FAILED as error:
    # The watermark is left alone, so ADF sees a source with nothing new. The marker
    # is the only thing separating that from a month with no release.
    print(f"ONS resolution failed: {error}")
    record_failure(ONS_SOURCE, ONS_DATASET_PATH, f"{type(error).__name__}: {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Land Registry HPI
# MAGIC
# MAGIC The address is built rather than looked up. The bucket does answer a listing at
# MAGIC its root, but the prefix parameter is stripped before it reaches S3, so a listing
# MAGIC comes back as the first thousand keys of the whole bucket with none of the full
# MAGIC files among them.
# MAGIC
# MAGIC Candidates run from this calendar month down to the month already ingested, and
# MAGIC the first that answers is the newest published. Ending on a month known to exist
# MAGIC is what separates a release that has not landed yet, which is ordinary, from an
# MAGIC address pattern that has moved, which is not.
# MAGIC
# MAGIC The month names the file; `Last-Modified` on the object that answered is the
# MAGIC release. Roughly two months apart, and only the second can be compared against
# MAGIC `last_refreshed`.

# COMMAND ----------

HPI_SOURCE = "land_registry_hpi"

current = registry.find(entries, HPI_SOURCE)
print(f"watermark holds  {current['latest_release']}  {current['file_name']}")

try:
    found, header = None, ""
    for month in hpi.candidate_months(RUN_DATE, current["relative_url"]):
        status, headers = head(hpi.HPI_HOST + hpi.relative_url(month))
        print(f"  {month:%Y-%m}  {status}")
        if status == 200:
            found, header = month, headers.get("Last-Modified", "")
            break

    month, published, relative_url = hpi.resolve(found, header, current["relative_url"])
    print(f"publisher offers {month:%Y-%m}, released {published}")

    entries = registry.update(
        entries,
        HPI_SOURCE,
        {
            "relative_url": relative_url,
            "file_name": hpi.landing_file_name(month),
            "latest_release": published.isoformat(),
        },
    )
except RESOLUTION_FAILED as error:
    print(f"HPI resolution failed: {error}")
    record_failure(HPI_SOURCE, hpi.FULL_FILE_PREFIX, f"{type(error).__name__}: {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Police street crime
# MAGIC
# MAGIC The endpoint reports the data month of the latest release, so nothing is probed
# MAGIC to find it. The archive's own `Last-Modified` is the release date, which is why
# MAGIC the address is asked for even on a run where the month has not moved: without it
# MAGIC a watermark seeded at 1900 would stay gated shut while reporting success.
# MAGIC
# MAGIC A 404 means the endpoint has moved ahead of the archive. That is not a failure
# MAGIC and writes no marker: nothing new can be fetched, and the run that finds the
# MAGIC archive posted will advance the snapshot. The archive already held is asked
# MAGIC instead, so the release date is still recorded.

# COMMAND ----------

POLICE_SOURCE = "police_uk_street_crime"

current = registry.find(entries, POLICE_SOURCE)
held = current["incremental_latest_snapshot"]
print(f"watermark holds  {current['latest_release']}  {held}")

try:
    request = urllib.request.Request(
        police.POLICE_HOST + police.LAST_UPDATED_PATH,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
        body = response.read().decode("utf-8", "replace")

    offered = police.latest_month(body)
    status, headers = head(police.POLICE_HOST + police.archive_relative_url(offered))
    print(f"  {offered:%Y-%m}  {status}")

    if status == 404:
        print(f"  archive not posted yet, holding at {held}")
        status, headers = head(
            police.POLICE_HOST + police.archive_relative_url(police.month_of(held))
        )
        entries = registry.update(
            entries,
            POLICE_SOURCE,
            {
                "latest_release": police.release_date(
                    headers.get("Last-Modified", "")
                ).isoformat()
            },
        )
    else:
        month, published = police.resolve(body, held, headers.get("Last-Modified", ""))
        print(f"publisher offers {month:%Y-%m}, released {published}")
        entries = registry.update(
            entries,
            POLICE_SOURCE,
            {
                "incremental_latest_snapshot": police.snapshot_token(month),
                "latest_release": published.isoformat(),
            },
        )
except RESOLUTION_FAILED as error:
    print(f"Police resolution failed: {error}")
    record_failure(POLICE_SOURCE, police.LAST_UPDATED_PATH, f"{type(error).__name__}: {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The fixed addresses
# MAGIC
# MAGIC PPD, Doogal and BoE publish at one address that never moves, so there is nothing
# MAGIC to resolve. What they still need is a release date, or the gate has nothing above
# MAGIC the seeded 1900 to compare and the source is skipped on every run while `02`
# MAGIC records it as succeeded.
# MAGIC
# MAGIC `Last-Modified` answers it. All three were measured on 28-08-2026 and all three
# MAGIC send one. A header that moves without the content changing costs one redundant
# MAGIC download; content changing without the header moving would be a publisher fault
# MAGIC that this cannot see, and settles itself on their next publication.
# MAGIC
# MAGIC PPD is asked at its monthly change file rather than a yearly one, because that is
# MAGIC the object that moves when Land Registry publish.

# COMMAND ----------

# source -> the entry field holding the address below its host
FIXED_ADDRESS_SOURCES = {
    "land_registry_ppd": "incremental_relative_url_prefix",
    "doogal_uk_postcode": "relative_url",
    "boe_official_bank_rate_history": "relative_url",
}

for source_name, address_field in FIXED_ADDRESS_SOURCES.items():
    entry = registry.find(entries, source_name)
    url = entry["base_url"] + entry[address_field]

    try:
        status, headers = head(url)
        if status != 200:
            raise resolution.ResolutionError(
                f"{url} answered {status}. A fixed address that stops resolving is a "
                "publisher change rather than a month with no release."
            )

        published = resolution.release_date(headers.get("Last-Modified", ""))
        print(f"  {source_name:34}{entry['latest_release']}  ->  {published}")

        entries = registry.update(
            entries, source_name, {"latest_release": published.isoformat()}
        )
    except RESOLUTION_FAILED as error:
        print(f"  {source_name:34}failed: {error}")
        record_failure(source_name, url, f"{type(error).__name__}: {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write the watermark back
# MAGIC
# MAGIC What changed is printed before the write, so the run says what it did rather than
# MAGIC only that it did something. Every section above writes unconditionally and this
# MAGIC is what reports the difference, which is why no resolver returns a flag saying
# MAGIC whether it moved.
# MAGIC
# MAGIC `dump` is given the array as it was read, so an entry lost or reordered stops the
# MAGIC notebook instead of reaching ADF.
# MAGIC
# MAGIC The schema runs again immediately before the write, and this is the call that
# MAGIC matters most. `update` refuses a field an entry does not already carry, so a
# MAGIC resolver cannot invent a key, but nothing stops it writing the wrong type into
# MAGIC a key that exists. This is the last point where that can be caught with the
# MAGIC good watermark still on the volume.

# COMMAND ----------

difference = registry.changed_fields(original, entries)

if not difference:
    print("no source moved, watermark left as it is")
else:
    for source_name, fields in difference.items():
        print(source_name)
        for field, (before, after) in sorted(fields.items()):
            print(f"  {field}: {before}  ->  {after}")

    schema.validate(entries, WATERMARK_SCHEMA)
    schema.assert_invariants(entries)

    text = registry.dump(entries, original=original)
    with open(WATERMARK_PATH, "w", encoding="utf-8") as handle:
        handle.write(text)
    print(f"\n{WATERMARK_PATH}: written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Read back from the volume rather than from what was just held in memory, since
# MAGIC the write is what ADF depends on.
# MAGIC
# MAGIC `active` is ADF's own filter and is not read here, so a source can show `fetch`
# MAGIC below and still not be fetched.
# MAGIC
# MAGIC The schema runs on what was read back. `dump` round-trips in memory, so it
# MAGIC cannot see a write that landed truncated; this can.

# COMMAND ----------

with open(WATERMARK_PATH, encoding="utf-8") as handle:
    written = registry.load(handle.read())

schema.validate(written, WATERMARK_SCHEMA)

print(f"{'source':34}{'latest_release':16}{'last_refreshed':16}{'active':8}fetch")
for entry in written:
    latest = entry.get("latest_release", "")
    refreshed = entry.get("last_refreshed", "")
    fetch = "yes" if latest >= refreshed else "skip"
    active = "yes" if entry.get("active") else "no"
    print(f"{entry['source_name']:34}{latest:16}{refreshed:16}{active:8}{fetch}")

markers = dbutils.fs.ls(LOG_PATH)  # noqa: F821
print(f"\n{len(markers)} failure marker(s) so far: {[m.name for m in markers] or 'none'}")
