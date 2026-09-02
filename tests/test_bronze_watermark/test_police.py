"""Tests for the police.uk archive resolver.

Pure Python: the API body and the archive's Last-Modified header are fetched by the
notebook and passed in as text, so nothing here needs a session or the network.

The response shape is the one published in the endpoint's own documentation, read on
28-08-2026, where the day is carried only to keep the format standard. The
Last-Modified value is constructed rather than observed: a HEAD request was not
reachable from where these were written, so whether police.uk sends that header at all
is settled by the first cluster run and not by this suite.
"""

from __future__ import annotations

from datetime import date

import pytest

from databricks_src.bronze.watermark_library.police import (
    ARCHIVE_PREFIX,
    ResolutionError,
    archive_relative_url,
    latest_month,
    month_of,
    resolve,
    snapshot_token,
)

# What /api/crime-last-updated answers.
LATEST = '{"date": "2026-06-01"}'

# What the watermark holds, and what the entry's incremental fields rebuild from it.
CURRENT_SNAPSHOT = "2026-06"
WATERMARK_PREFIX = "/data/archive/"
FILE_EXTENSION = ".zip"

PUBLISHED = "Wed, 29 Jul 2026 14:43:38 GMT"


def body(month):
    return f'{{"date": "{month}"}}'


# --------------------------------------------------------------------------- #
# Reading the endpoint
# --------------------------------------------------------------------------- #


def test_the_documented_response_parses():
    assert latest_month(LATEST) == date(2026, 6, 1)


def test_the_day_is_dropped():
    """The endpoint documents the day as present only to keep the format standard, so
    reading it as meaningful would put the month on the wrong grain."""
    assert latest_month(body("2026-06-28")) == latest_month(body("2026-06-01"))


def test_a_bare_month_parses_too():
    """The availability endpoint on the same API writes the month alone. Accepting
    both costs nothing and means a trimmed response is not a failed run."""
    assert latest_month(body("2026-06")) == date(2026, 6, 1)


def test_the_availability_shape_is_refused_by_name():
    """Pointing this at /api/crimes-street-dates returns a list of every month and
    force. Failing with the endpoint named is what stops that costing an afternoon."""
    with pytest.raises(ResolutionError, match="crimes-street-dates"):
        latest_month('[{"date": "2026-06", "stop-and-search": []}]')


def test_a_non_json_answer_raises():
    """A CDN error page reaches the parser as text. Without this it would reach the
    month regex instead and fail somewhere less obvious."""
    with pytest.raises(ResolutionError, match="did not answer JSON"):
        latest_month("<html><body>503 Service Unavailable</body></html>")


def test_an_answer_without_a_date_field_names_what_it_carries():
    with pytest.raises(ResolutionError, match="last-updated"):
        latest_month('{"updated": "2026-06-01"}')


@pytest.mark.parametrize("token", ["2026-13", "2026-00", "26-06", "June 2026", ""])
def test_a_token_that_is_not_a_month_raises(token):
    with pytest.raises(ResolutionError, match="not a YYYY-MM month"):
        latest_month(body(token))


# --------------------------------------------------------------------------- #
# Building the address
# --------------------------------------------------------------------------- #


def test_the_snapshot_token_is_what_the_watermark_stores():
    assert snapshot_token(date(2026, 6, 1)) == CURRENT_SNAPSHOT


def test_the_archive_url_rebuilds_from_the_watermark_fields():
    """ADF builds the address from incremental_relative_url_prefix, the snapshot and
    the extension. The resolver builds it here so the HEAD confirms the same URL, and
    this pins the two together."""
    month = date(2026, 7, 1)
    assert archive_relative_url(month) == (
        f"{WATERMARK_PREFIX}{snapshot_token(month)}{FILE_EXTENSION}"
    )


def test_the_prefix_matches_the_watermark():
    assert ARCHIVE_PREFIX == WATERMARK_PREFIX


def test_the_archive_url_is_the_published_address():
    assert (
        "https://data.police.uk" + archive_relative_url(date(2026, 6, 1))
        == "https://data.police.uk/data/archive/2026-06.zip"
    )


# --------------------------------------------------------------------------- #
# The release date
# --------------------------------------------------------------------------- #


def test_the_release_date_is_not_the_data_month():
    """These are the whole reason the resolver returns two dates. Writing 2026-06-01
    into latest_release would leave it permanently below a last_refreshed that
    advances on every successful run."""
    month, published = resolve(LATEST, CURRENT_SNAPSHOT, PUBLISHED)
    assert month == date(2026, 6, 1)
    assert published == date(2026, 7, 29)
    assert published > month


# --------------------------------------------------------------------------- #
# Against what the watermark already holds
# --------------------------------------------------------------------------- #


def test_a_newer_month_resolves_to_itself():
    assert resolve(body("2026-07-01"), CURRENT_SNAPSHOT, PUBLISHED)[0] == date(2026, 7, 1)


def test_an_unchanged_month_still_carries_a_release_date():
    """This is what seeds latest_release. police.uk publish monthly and the pipeline
    runs more often than that, so returning nothing when the month has not moved would
    leave a watermark seeded at 1900 gated shut until it did."""
    assert resolve(LATEST, CURRENT_SNAPSHOT, PUBLISHED) == (
        date(2026, 6, 1),
        date(2026, 7, 29),
    )


def test_an_older_month_raises():
    """The publisher does not withdraw archives, so this means the endpoint is being
    read wrongly."""
    with pytest.raises(ResolutionError, match="older than"):
        resolve(body("2026-05-01"), CURRENT_SNAPSHOT, PUBLISHED)


@pytest.mark.parametrize("snapshot", [None, "", "   "])
def test_an_absent_snapshot_resolves_rather_than_raises(snapshot):
    """The endpoint has just said which month is current, so writing it repairs the
    entry. Raising would leave ADF rebuilding an address from a null."""
    assert resolve(LATEST, snapshot, PUBLISHED)[0] == date(2026, 6, 1)


def test_a_held_snapshot_reads_back_as_an_address():
    """The notebook's 404 path turns the snapshot it is holding at back into an
    address, so the token has to parse as well as format."""
    assert month_of(CURRENT_SNAPSHOT) == date(2026, 6, 1)
    assert archive_relative_url(month_of(CURRENT_SNAPSHOT)) == "/data/archive/2026-06.zip"


def test_a_snapshot_that_is_not_a_month_raises():
    """Distinct from absent. Something is in the field and it is not a month, which is
    a broken entry rather than an unseeded one."""
    with pytest.raises(ResolutionError, match="watermark snapshot"):
        resolve(LATEST, "June", PUBLISHED)


def test_the_month_is_read_before_the_header():
    """A bad body and a bad header together report the body, because the header is
    only fetched for the URL the body names."""
    with pytest.raises(ResolutionError, match="did not answer JSON"):
        resolve("<html>503</html>", CURRENT_SNAPSHOT, "")
