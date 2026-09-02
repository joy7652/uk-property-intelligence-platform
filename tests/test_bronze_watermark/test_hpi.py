"""Tests for the Land Registry HPI resolver.

Pure Python: the caller walks the candidate months, makes the HEAD requests and passes
back the month that answered, so nothing here needs a session or the network.

The address pattern is the one the watermark holds, and the header is the live value
read off the sibling Land Registry object on 28-08-2026. An earlier version of this
module read the months off the bucket listing instead; the probe that day showed the
prefix parameter never reaches S3, which is why the address is constructed and
confirmed rather than looked up.
"""

from __future__ import annotations

from datetime import date

import pytest

from databricks_src.bronze.watermark_library.hpi import (
    FULL_FILE_PREFIX,
    ResolutionError,
    candidate_months,
    data_month,
    landing_file_name,
    relative_url,
    resolve,
)

# What the watermark points at now.
CURRENT = "/market-trend-data/house-price-index-data/UK-HPI-full-file-2026-05.csv"

# The June index published on 19 August 2026, the same day ONS released the private
# rents edition the sibling resolver reads. Land Registry rewrite every object in the
# directory on each release, so this is the shape their header takes.
PUBLISHED = "Wed, 19 Aug 2026 08:20:10 GMT"


TODAY = date(2026, 8, 28)


# --------------------------------------------------------------------------- #
# Reading and building the address
# --------------------------------------------------------------------------- #


def test_the_watermark_value_parses():
    assert data_month(CURRENT) == date(2026, 5, 1)


def test_the_prefix_is_the_one_the_watermark_holds():
    assert CURRENT.startswith(FULL_FILE_PREFIX)


def test_building_and_reading_are_inverse():
    assert data_month(relative_url(date(2026, 6, 1))) == date(2026, 6, 1)


def test_the_address_rebuilds_the_published_url():
    assert (
        "https://publicdata.landregistry.gov.uk" + relative_url(date(2026, 6, 1))
        == "https://publicdata.landregistry.gov.uk/market-trend-data/"
        "house-price-index-data/UK-HPI-full-file-2026-06.csv"
    )


def test_a_neighbouring_series_names_no_month():
    """The directory holds one series per published statistic and several carry the
    same date token. Anchoring on the prefix is what tells them apart."""
    other = "/market-trend-data/house-price-index-data/Annual-price-change-2026-06.csv"
    assert data_month(other) is None


def test_a_variant_under_the_same_prefix_names_no_month():
    """Anchored at both ends, so a revision or a nation-specific cut published under
    the prefix is not read as the file itself."""
    assert data_month(f"{FULL_FILE_PREFIX}2026-06-revision.csv") is None


@pytest.mark.parametrize("token", ["2026-13", "2026-00", "26-06"])
def test_an_impossible_month_names_no_month(token):
    assert data_month(f"{FULL_FILE_PREFIX}{token}.csv") is None


def test_the_prefix_reads_in_any_case():
    """Nothing guarantees the publisher keeps the mixed case it uses now, and the
    watermark carries whatever was written into it by hand."""
    assert data_month(CURRENT.lower()) == date(2026, 5, 1)


# --------------------------------------------------------------------------- #
# Choosing what to probe
# --------------------------------------------------------------------------- #


def test_the_walk_opens_on_the_current_calendar_month():
    """Newest first, so the first address that answers is the newest published."""
    assert candidate_months(TODAY, CURRENT)[0] == date(2026, 8, 1)


def test_the_walk_ends_on_the_month_the_watermark_holds():
    """A month known to have been published, rather than an arbitrary count. The walk
    reaching it and finding nothing means the pattern moved, which is worth stopping
    on."""
    assert candidate_months(TODAY, CURRENT)[-1] == date(2026, 5, 1)


def test_the_walk_covers_the_publication_lag():
    """HPI trails by about two months, so the June file is what August should find."""
    assert date(2026, 6, 1) in candidate_months(TODAY, CURRENT)


def test_the_walk_is_ordered_newest_first():
    months = candidate_months(TODAY, CURRENT)
    assert months == sorted(months, reverse=True)


def test_an_unchanged_month_is_still_probed():
    """This is what makes a run with nothing new cost the same as one with a release,
    and what seeds latest_release without waiting for the publisher to move."""
    assert candidate_months(date(2026, 5, 20), CURRENT) == [
        date(2026, 5, 1),
    ]


def test_the_walk_crosses_a_year_boundary():
    current = f"{FULL_FILE_PREFIX}2025-11.csv"
    assert candidate_months(date(2026, 1, 15), current) == [
        date(2026, 1, 1),
        date(2025, 12, 1),
        date(2025, 11, 1),
    ]


def test_an_unreadable_watermark_falls_back_to_a_fixed_count():
    """A first run, or an entry written wrongly. Twelve months is past any publication
    lag, and finding nothing in a year is a publisher change rather than a slow
    month."""
    assert len(candidate_months(TODAY, "", months_back=12)) == 12


def test_the_fallback_still_runs_newest_first():
    months = candidate_months(TODAY, "", months_back=3)
    assert months == [date(2026, 8, 1), date(2026, 7, 1), date(2026, 6, 1)]


def test_a_watermark_month_ahead_of_today_raises():
    """A data month cannot lead its own publication. Probing would return an empty
    list and read as a withdrawn dataset."""
    with pytest.raises(ResolutionError, match="later than the current month"):
        candidate_months(TODAY, f"{FULL_FILE_PREFIX}2026-09.csv")


# --------------------------------------------------------------------------- #
# Against what the watermark already holds
# --------------------------------------------------------------------------- #


def test_the_release_date_comes_from_the_header_not_the_month():
    """The month cannot serve as the release date. It trails publication by about two
    months, so a latest_release derived from it sits permanently behind a
    last_refreshed that advances on every successful run, and the gate reads skip even
    on a run that has just found a file the watermark does not hold."""
    month, published, _ = resolve(date(2026, 6, 1), PUBLISHED, CURRENT)
    assert (month, published) == (date(2026, 6, 1), date(2026, 8, 19))


def test_the_release_date_leads_the_data_month_by_the_publication_lag():
    _, published, _ = resolve(date(2026, 6, 1), PUBLISHED, CURRENT)
    assert (published - date(2026, 6, 1)).days > 30


def test_a_header_that_is_not_a_date_raises():
    with pytest.raises(ResolutionError, match="not an HTTP date"):
        resolve(date(2026, 6, 1), "last tuesday", CURRENT)


def test_a_newer_month_resolves_to_its_own_address():
    month, _, address = resolve(date(2026, 6, 1), PUBLISHED, CURRENT)
    assert month == date(2026, 6, 1)
    assert address.endswith("2026-06.csv")


def test_an_unchanged_month_resolves_rather_than_reporting_nothing():
    """HPI publishes on the ONS release calendar, so a run lands before a new file
    about half the time. The result is returned either way, so the ordinary case has
    the same shape as a new release and latest_release is written without waiting for
    the publisher to move."""
    assert resolve(date(2026, 5, 1), PUBLISHED, CURRENT) == (
        date(2026, 5, 1),
        date(2026, 8, 19),
        CURRENT,
    )


def test_an_older_month_raises():
    """Land Registry do not withdraw releases, so this means the address pattern has
    changed under the walk."""
    with pytest.raises(ResolutionError, match="older than"):
        resolve(date(2026, 4, 1), PUBLISHED, CURRENT)


def test_nothing_answering_raises():
    """Every address probed returned nothing, including the month already ingested.
    An empty return would reach a Copy activity as whatever the watermark still
    holds."""
    with pytest.raises(ResolutionError, match="No candidate month resolved"):
        resolve(None, PUBLISHED, CURRENT)


def test_nothing_answering_is_reported_before_the_header_is_read():
    """A walk that found nothing has no response and therefore no header. Reading it
    first would report a missing header where the cause was a missing file."""
    with pytest.raises(ResolutionError, match="No candidate month resolved"):
        resolve(None, "", CURRENT)


def test_an_unseeded_watermark_still_resolves():
    """A first run has to be able to write a month."""
    month, _, address = resolve(date(2026, 6, 1), PUBLISHED, "")
    assert (month, address) == (date(2026, 6, 1), relative_url(date(2026, 6, 1)))


def test_the_address_is_always_the_constructed_one():
    """A watermark carrying a different case is corrected on the next run, because the
    address written is the one this module builds rather than the one it read."""
    _, _, address = resolve(date(2026, 5, 1), PUBLISHED, CURRENT.lower())
    assert address == CURRENT


# --------------------------------------------------------------------------- #
# The Bronze landing name
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "month, expected",
    [
        (date(2026, 5, 1), "uk-hpi-full-file-2026-05.csv"),
        (date(2026, 6, 1), "uk-hpi-full-file-2026-06.csv"),
        (date(2026, 12, 1), "uk-hpi-full-file-2026-12.csv"),
        (date(2027, 1, 1), "uk-hpi-full-file-2027-01.csv"),
    ],
)
def test_the_landing_name_tracks_the_data_month(month, expected):
    assert landing_file_name(month) == expected


def test_the_landing_name_is_lower_case_where_the_address_is_not():
    assert "UK-HPI" in relative_url(date(2026, 6, 1))
    assert landing_file_name(date(2026, 6, 1)).islower()


def test_the_landing_name_matches_what_the_watermark_holds():
    """The May value in the watermark was written by hand. This pins the derivation to
    it."""
    assert landing_file_name(date(2026, 5, 1)) == "uk-hpi-full-file-2026-05.csv"
