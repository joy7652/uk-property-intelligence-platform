"""Resolve the current file URL for the UK House Price Index full file.

Land Registry publish the full file at an address carrying its data month, one object
per month, and the pattern never varies. So the address is constructed rather than
looked up, and a HEAD against it answers the only question
worth asking: whether that month has been published.

The bucket does list. An anonymous GET at its root returns a `ListBucketResult`, and
an earlier version of this module read the published months off it. That was measured
on 28-08-2026 and does not work: the prefix parameter is stripped before it reaches
S3, so the listing comes back as the first thousand keys of the whole bucket with none
of the full files among them. `Marker` would be stripped by whatever strips `prefix`,
which rules out paging to them.

Constructing and confirming is the better answer regardless. A listing says a key
exists; a HEAD says the exact address the Copy activity will fetch resolves, which is
what the watermark is about to be pointed at.

Two dates come out of one HEAD and they are not interchangeable. The month is the one
that was asked for, and it builds the address and the landing name. `Last-Modified` is
when Land Registry wrote the object, and that is what the watermark records as the
release.

The month cannot serve as the release date. It trails publication by about two months,
so a `latest_release` derived from it sits permanently behind a `last_refreshed` that
advances on every successful run, and the gate reads skip even on a run that has just
found a file it does not hold. Every other source in the project avoids this by
accident of how its publisher addresses things; HPI is the only one where the data
month is what the URL carries.

No I/O here. The caller walks the candidates, makes the requests, and passes back the
month that answered and the header it carried.
"""

from __future__ import annotations

import re
from datetime import date

from databricks_src.bronze.watermark_library.resolution import (
    ResolutionError,
    release_date,
)

HPI_HOST = "https://publicdata.landregistry.gov.uk"

# Everything before the month in the address. Held here rather than passed in, because
# the same value builds the URL and reads it back.
FULL_FILE_PREFIX = "/market-trend-data/house-price-index-data/UK-HPI-full-file-"

# Bronze landing name. The published address is mixed case and the landed object is
# not, which is why Silver matches vintages case-insensitively.
LANDING_STEM = "uk-hpi-full-file"

# How far back to probe when the watermark holds no readable month. Twelve is past any
# publication lag, and a walk that finds nothing in a year is a publisher change worth
# stopping on rather than a slow month.
DEFAULT_MONTHS_BACK = 12

# Anchored both sides so a variant published under the same prefix is not read as the
# full file. Months outside 01 to 12 do not match, so a malformed address is ignored
# here rather than raising out of the date call.
_ADDRESS = re.compile(
    re.escape(FULL_FILE_PREFIX) + r"(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])\.csv$",
    re.IGNORECASE,
)


def data_month(address: str) -> date | None:
    """The data month an address names, or None where it names none.

    Reads what this module builds and what the watermark holds alike, which is what
    lets a resolved month be compared against the one already ingested.
    """
    match = _ADDRESS.search(address)
    if match is None:
        return None
    return date(int(match.group("year")), int(match.group("month")), 1)


def relative_url(month: date) -> str:
    """The address below the host, as the watermark stores it."""
    return f"{FULL_FILE_PREFIX}{month:%Y-%m}.csv"


def landing_file_name(month: date) -> str:
    """The name the file is stored under in Bronze.

    Lower case, where the published address is mixed. The month is the one the address
    already carries, so the landing name says which release it is without the file
    being opened.
    """
    return f"{LANDING_STEM}-{month:%Y-%m}.csv"


def _step_back(month: date) -> date:
    if month.month == 1:
        return date(month.year - 1, 12, 1)
    return date(month.year, month.month - 1, 1)


def candidate_months(
    today: date,
    current_relative_url: str,
    months_back: int = DEFAULT_MONTHS_BACK,
) -> list[date]:
    """The months to probe, newest first.

    Runs from the current calendar month down to the month the watermark already
    holds, so the walk ends on a month known to have been published rather than after
    an arbitrary count. Where the watermark holds nothing readable, it runs back
    `months_back` instead.

    Ending on the current month is what makes a run with nothing new cost the same as
    one with a new release, and what lets `latest_release` be seeded without waiting
    for the publisher to move.
    """
    newest = date(today.year, today.month, 1)
    current = data_month(current_relative_url)

    if current is not None and current > newest:
        raise ResolutionError(
            f"The watermark holds {current:%Y-%m}, which is later than the current "
            f"month {newest:%Y-%m}. A data month cannot lead its own publication, so "
            "the entry has been written wrongly and probing would find nothing."
        )

    months, month = [], newest
    while True:
        months.append(month)
        if current is not None:
            if month <= current:
                break
        elif len(months) >= months_back:
            break
        month = _step_back(month)
    return months


def resolve(
    found: date | None, last_modified: str, current_relative_url: str
) -> tuple[date, date, str]:
    """The newest published month, its release date, and the address that fetches it.

    Takes the month whose HEAD answered and the header that response carried. The
    caller writes the result whether or not it moved: re-fetching a file already held
    costs a download and nothing else, and `registry.changed_fields` already reports
    what actually moved before anything is written.

    Raises where nothing answered, and where what answered is older than the month
    already ingested. Land Registry do not withdraw releases, so the second means the
    address pattern has changed under the walk.
    """
    if found is None:
        raise ResolutionError(
            "No candidate month resolved. Every address probed returned nothing, "
            "including the month the watermark already points at, so the pattern has "
            "changed rather than a release being late."
        )

    current = data_month(current_relative_url)
    if current is not None and found < current:
        raise ResolutionError(
            f"Newest month found is {found:%Y-%m}, older than the {current:%Y-%m} file "
            "the watermark already points at."
        )

    return found, release_date(last_modified), relative_url(found)
