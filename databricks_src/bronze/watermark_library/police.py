"""Resolve the current archive for police.uk street-level crime.

police.uk publish one archive per month at a key carrying its data month, and each is
a point-in-time snapshot. The publisher's downloads page describes the files as data
as held at a particular point in history and states that every archive but the newest
is out of date, and the changelog records corrections as a force resubmitting data,
which lands in the next snapshot rather than rewriting an existing one. So the newest
archive is the only one worth resolving, and the ones already in Bronze do not need
re-fetching to pick up a restatement.

The month comes from `/api/crime-last-updated`, which answers with the data month of
the latest release. That is not a release date. The data month trails publication by
about two months, and writing it into `latest_release` would leave the source
permanently below a `last_refreshed` that advances on every successful run. The
release date is read off `Last-Modified` on the archive, which is why the caller makes
a HEAD request and passes the header in.

That HEAD is made on every run rather than only when the month moves. It costs one
request, and it is what seeds `latest_release` on a first run where the month already
matches what the watermark holds.

No I/O here. Both the API body and the header are fetched by the caller and passed in
as text, which is what makes the selection testable away from the network.
"""

from __future__ import annotations

import json
import re
from datetime import date

from databricks_src.bronze.watermark_library.resolution import (
    ResolutionError,
    release_date,
)

POLICE_HOST = "https://data.police.uk"

LAST_UPDATED_PATH = "/api/crime-last-updated"

# The same value the watermark carries as incremental_relative_url_prefix. The URL is
# built here so the address confirmed by the HEAD request is the one ADF rebuilds from
# the prefix and the snapshot, rather than two constructions that can drift apart.
ARCHIVE_PREFIX = "/data/archive/"

# The last-updated endpoint writes a full date and keeps the day only so the format is
# standard; the availability endpoint writes the month alone. Both read.
_MONTH = re.compile(r"^(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])(?:-\d{2})?$")


def month_of(token: str, source: str = "month token") -> date:
    """The month a `YYYY-MM` token names.

    Reads the watermark's snapshot and the endpoint's own `YYYY-MM-DD` alike, so the
    caller can turn a held snapshot back into an address without a second parser.
    """
    match = _MONTH.match(token.strip())
    if match is None:
        raise ResolutionError(f"{source} reads {token!r}, which is not a YYYY-MM month.")
    return date(int(match.group("year")), int(match.group("month")), 1)


def latest_month(body: str) -> date:
    """The data month of the latest release, from the last-updated endpoint."""
    try:
        answer = json.loads(body)
    except json.JSONDecodeError as error:
        raise ResolutionError(
            f"last-updated endpoint did not answer JSON: {error}"
        ) from None

    if isinstance(answer, list):
        raise ResolutionError(
            "last-updated endpoint answered a list, which is the shape of "
            "/api/crimes-street-dates. This resolver reads /api/crime-last-updated."
        )
    if not isinstance(answer, dict):
        raise ResolutionError(
            f"last-updated endpoint answered a {type(answer).__name__} rather than an "
            "object carrying a date."
        )
    if "date" not in answer:
        raise ResolutionError(
            f"last-updated endpoint answered an object with no date field. It carries "
            f"{sorted(answer)}."
        )
    return month_of(str(answer["date"]), "last-updated endpoint")


def snapshot_token(month: date) -> str:
    """The month as the watermark stores it."""
    return f"{month:%Y-%m}"


def archive_relative_url(month: date) -> str:
    """The archive's address below the host."""
    return f"{ARCHIVE_PREFIX}{snapshot_token(month)}.zip"


def resolve(
    body: str, current_snapshot: str | None, last_modified: str
) -> tuple[date, date]:
    """The newest archive, checked against the snapshot the watermark already holds.

    Returns the data month and the release date. The caller writes both whether or not
    the month moved: a run finding nothing new is ordinary, police.uk publish monthly
    and the pipeline runs more often than that, and `registry.changed_fields` already
    reports what actually moved before anything is written.

    Raises where the newest month is older than the one already ingested. The
    publisher does not withdraw archives, so that means the endpoint is being read
    wrongly rather than that a release was pulled.
    """
    month = latest_month(body)
    published = release_date(last_modified)

    # An absent snapshot is treated as nothing to compare against rather than as an
    # error. The endpoint has just said which month is current, so writing it repairs
    # the entry, where raising would leave ADF rebuilding an address from a null.
    current = None
    if current_snapshot and current_snapshot.strip():
        current = month_of(current_snapshot, "watermark snapshot")
        if month < current:
            raise ResolutionError(
                f"Latest month reported is {snapshot_token(month)}, older than the "
                f"{snapshot_token(current)} archive the watermark already holds."
            )

    return month, published
