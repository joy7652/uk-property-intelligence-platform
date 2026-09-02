"""Resolve the current file URL for a source whose URL moves between releases.

ONS publishes each edition of a dataset at a path carrying its release date, and the
file name carries an arbitrary numeric suffix: across one dataset's history the same
workbook has been published as `...statistics.xlsx`, `...statistics1.xlsx`,
`...statistics7.xlsx` and `...statistics..xlsx`. No rule generates that, so the URL is
read off the dataset page rather than constructed.

The page lists every edition. Order on the page is not trusted: the release date is
parsed out of each link and the newest wins, so a reordering upstream cannot hand back
an older file than the one already ingested.

No I/O here. The page is fetched by the caller and passed in as text, which is what
makes the selection testable away from the network.
"""

from __future__ import annotations

import re
from datetime import date

from databricks_src.bronze.watermark_library.resolution import ResolutionError

# ONS writes the edition segment as a bare date, e.g. `22july2026`, `19august2026`.
MONTHS: dict[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

_MONTH_ALTERNATION = "|".join(MONTHS)

# Matches the download links regardless of the markup around them, so a template
# change that leaves the links intact does not break the parse.
_FILE_HREF = re.compile(
    # The page markup carries /file?uri=...; the watermark stores the query alone.
    r"(?:/file)?\?uri=(?P<path>/[\w/]+?)"
    r"/(?P<day>\d{1,2})(?P<month>" + _MONTH_ALTERNATION + r")(?P<year>\d{4})"
    r"/(?P<file>[\w.\-]+\.xlsx)",
    re.IGNORECASE,
)

ONS_HOST = "https://www.ons.gov.uk"

# Bronze landing name, which does not follow the published file name.
LANDING_STEM = "priceindexofprivaterents"


def edition_links(page: str, dataset_path: str) -> list[tuple[date, str, str]]:
    """Every edition on the page, as (release date, edition segment, file name).

    Links to other datasets are ignored: an ONS page carries navigation and related
    items, and only the paths under the dataset being resolved are its own editions.
    """
    wanted = dataset_path.rstrip("/").lower()
    found: list[tuple[date, str, str]] = []
    for match in _FILE_HREF.finditer(page):
        if match.group("path").lower() != wanted:
            continue
        released = date(
            int(match.group("year")),
            MONTHS[match.group("month").lower()],
            int(match.group("day")),
        )
        segment = f"{match.group('day')}{match.group('month').lower()}{match.group('year')}"
        found.append((released, segment, match.group("file")))
    return found


def newest_edition(page: str, dataset_path: str) -> tuple[date, str]:
    """The most recently released edition, and the relative URL that fetches it.

    Selected on the parsed release date rather than on position, so the page's own
    ordering carries no weight.
    """
    editions = edition_links(page, dataset_path)
    if not editions:
        raise ResolutionError(
            f"No edition links found under {dataset_path}. Either the page moved or "
            "its markup changed, and continuing would fetch whatever the watermark "
            "still holds without saying so."
        )

    released, segment, file_name = max(editions, key=lambda edition: edition[0])
    relative_url = f"?uri={dataset_path}/{segment}/{file_name}"

    # ONS serves the file only when the query follows the host and path with nothing
    # between them, so the watermark splits the address at the query mark and the
    # relative half has to carry it. A relative URL starting anywhere else fetches a
    # page rather than a workbook, and the failure lands in the Silver reader.
    if not relative_url.startswith("?"):
        raise ResolutionError(
            f"Resolved relative URL {relative_url!r} does not open with '?'. ONS "
            "requires the query mark to sit at the start of the relative half."
        )
    return released, relative_url


def landing_file_name(released: date) -> str:
    """The name the workbook is stored under in Bronze.

    The published name carries an arbitrary suffix and says nothing about which
    release it is, so Bronze renames on the release month. The suffix on the source
    file is not a sequence: across five consecutive months it has run 10, none, 13,
    14, none.
    """
    return f"{LANDING_STEM}-{released:%Y-%m}.xlsx"


def resolve(
    page: str, dataset_path: str, current_relative_url: str
) -> tuple[date, str, bool]:
    """The newest edition, checked against what the watermark already points at.

    Returns the release date, the relative URL, and whether it differs from the
    current one. A run that finds nothing new is ordinary: publishers do not all
    release in the same week, and re-fetching an unchanged file is idempotent.

    Raises where the newest edition is older than the one already ingested, which
    means the page is being read wrongly rather than that the publisher withdrew a
    release.
    """
    released, relative_url = newest_edition(page, dataset_path)

    current = edition_links(current_relative_url, dataset_path)
    if current:
        current_released = current[0][0]
        if released < current_released:
            raise ResolutionError(
                f"Newest edition found is {released}, older than the {current_released} "
                "edition the watermark already points at."
            )

    return released, relative_url, relative_url != current_relative_url
