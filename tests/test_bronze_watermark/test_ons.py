"""Tests for the ONS edition resolver.

Pure Python: the page is fetched by the notebook and passed in as text, so nothing
here needs a session or the network.

The editions below are the five most recent read off the live dataset page on
24-08-2026. Their suffixes run 10, none, 13, 14, none across consecutive months,
which is why the URL is read off the page instead of constructed.
"""

from __future__ import annotations

from datetime import date

import pytest

from databricks_src.bronze.watermark_library.ons import (
    ResolutionError,
    edition_links,
    landing_file_name,
    newest_edition,
    resolve,
)

PATH = (
    "/economy/inflationandpriceindices/datasets/"
    "priceindexofprivaterentsukmonthlypricestatistics"
)

STEM = "priceindexofprivaterentsukmonthlypricestatistics"

# (release date, edition segment, published file name), newest first.
LIVE = [
    (date(2026, 8, 19), "19august2026", f"{STEM}.xlsx"),
    (date(2026, 7, 22), "22july2026", f"{STEM}14.xlsx"),
    (date(2026, 6, 17), "17june2026", f"{STEM}13.xlsx"),
    (date(2026, 5, 20), "20may2026", f"{STEM}.xlsx"),
    (date(2026, 4, 22), "22april2026", f"{STEM}10.xlsx"),
]

CURRENT = f"?uri={PATH}/22july2026/{STEM}14.xlsx"


def page(editions=None, path=PATH, extra=""):
    """A page body carrying the given editions, in the given order."""
    rows = LIVE if editions is None else editions
    return (
        "\n".join(
            f'<h3>{segment} edition</h3>'
            f'<a href="/file?uri={path}/{segment}/{name}">xlsx (185.4 KB)</a>'
            for _, segment, name in rows
        )
        + extra
    )


# --------------------------------------------------------------------------- #
# Reading the page
# --------------------------------------------------------------------------- #


def test_every_live_edition_is_found():
    assert len(edition_links(page(), PATH)) == len(LIVE)


def test_each_release_date_is_parsed():
    found = {released for released, _, _ in edition_links(page(), PATH)}
    assert found == {released for released, _, _ in LIVE}


@pytest.mark.parametrize(
    "name", [f"{STEM}.xlsx", f"{STEM}14.xlsx", f"{STEM}13.xlsx", f"{STEM}10.xlsx"]
)
def test_each_published_suffix_survives(name):
    """The suffix is not a sequence. A parse that assumed one would drop the months
    carrying no number at all."""
    found = {file_name for _, _, file_name in edition_links(page(), PATH)}
    assert name in found


def test_a_link_to_another_dataset_is_ignored():
    """An ONS page carries navigation and related items. Only paths under the dataset
    being resolved are its own editions."""
    noise = '<a href="/file?uri=/economy/other/datasets/somethingelse/31december2026/x.xlsx">x</a>'
    assert len(edition_links(page(extra=noise), PATH)) == len(LIVE)


def test_the_watermark_value_parses_as_an_edition():
    """The page writes /file?uri=... and the watermark stores the query alone. Both
    have to read, or the check that the newest is not older than the current cannot
    run at all."""
    assert edition_links(CURRENT, PATH) == [(date(2026, 7, 22), "22july2026", f"{STEM}14.xlsx")]


# --------------------------------------------------------------------------- #
# Choosing an edition
# --------------------------------------------------------------------------- #


def test_the_newest_release_wins():
    released, _ = newest_edition(page(), PATH)
    assert released == date(2026, 8, 19)


def test_page_order_carries_no_weight():
    """Selection is on the parsed date. A reordering upstream must not hand back an
    older file."""
    assert newest_edition(page(), PATH) == newest_edition(page(list(reversed(LIVE))), PATH)


def test_the_resolved_url_opens_with_a_query_mark():
    """ONS serves the file only when the query follows host and path directly, which
    is why the watermark splits the address there."""
    _, relative_url = newest_edition(page(), PATH)
    assert relative_url.startswith("?uri=")


def test_the_resolved_url_rebuilds_the_published_address():
    _, relative_url = newest_edition(page(), PATH)
    assert (
        "https://www.ons.gov.uk/file" + relative_url
        == f"https://www.ons.gov.uk/file?uri={PATH}/19august2026/{STEM}.xlsx"
    )


def test_a_page_with_no_editions_raises():
    """An empty return would reach a Copy activity as a malformed URL and fail two
    steps from its cause."""
    with pytest.raises(ResolutionError, match="No edition links"):
        newest_edition("<html><body>Service unavailable</body></html>", PATH)


# --------------------------------------------------------------------------- #
# Against what the watermark already holds
# --------------------------------------------------------------------------- #


def test_a_newer_edition_reports_a_change():
    released, relative_url, changed = resolve(page(), PATH, CURRENT)
    assert (released, changed) == (date(2026, 8, 19), True)
    assert "19august2026" in relative_url


def test_an_unchanged_edition_reports_no_change():
    """Publishers do not all release in the same week. A run finding nothing new is
    ordinary, and re-fetching an unchanged file is idempotent."""
    _, _, changed = resolve(page(LIVE[1:]), PATH, CURRENT)
    assert changed is False


def test_an_older_newest_edition_raises():
    """The publisher does not withdraw releases, so this means the page is being read
    wrongly."""
    with pytest.raises(ResolutionError, match="older than"):
        resolve(page(LIVE[2:]), PATH, CURRENT)


# --------------------------------------------------------------------------- #
# The Bronze landing name
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "released, expected",
    [
        (date(2026, 7, 22), "priceindexofprivaterents-2026-07.xlsx"),
        (date(2026, 8, 19), "priceindexofprivaterents-2026-08.xlsx"),
        (date(2026, 12, 17), "priceindexofprivaterents-2026-12.xlsx"),
        (date(2027, 1, 21), "priceindexofprivaterents-2027-01.xlsx"),
    ],
)
def test_the_landing_name_tracks_the_release_month(released, expected):
    """Bronze renames on the release month because the published name carries an
    arbitrary suffix and says nothing about which release it is. A URL advanced
    without this drops the new workbook onto the old file under the old name."""
    assert landing_file_name(released) == expected


def test_the_landing_name_matches_what_the_watermark_holds():
    """The July value in the watermark was written by hand. This pins the derivation
    to it."""
    assert landing_file_name(date(2026, 7, 22)) == "priceindexofprivaterents-2026-07.xlsx"
