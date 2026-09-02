"""Tests for the shared pieces the resolvers are built from.

Pure Python. Two things are checked here that no per-source suite can.

The first is that every resolver raises one catchable type. The pre-run notebook
catches `ResolutionError` once, so a module declaring its own class again would raise
something that clause misses, and the run would die at the resolver rather than
writing a failure marker and carrying on. Each per-source suite imports the name from
its own module, so all of them would keep passing against a local copy.

The second is the release date parse against real headers. The four below were read
off the live publishers on 28-08-2026, across three server stacks: S3 for the two Land
Registry and police.uk objects, IIS for the Bank of England, and an unidentified one
for Doogal.
"""

from __future__ import annotations

from datetime import date

import pytest

from databricks_src.bronze.watermark_library import hpi, ons, police
from databricks_src.bronze.watermark_library.resolution import (
    ResolutionError,
    release_date,
)

RESOLVERS = {"hpi": hpi, "ons": ons, "police": police}

# (Last-Modified as sent, the date it means), by source.
LIVE = {
    "land_registry_ppd": ("Fri, 28 Aug 2026 05:12:47 GMT", date(2026, 8, 28)),
    "doogal_uk_postcode": ("Fri, 17 Jul 2026 20:47:58 GMT", date(2026, 7, 17)),
    "boe_official_bank_rate_history": ("Thu, 30 Jul 2026 11:07:53 GMT", date(2026, 7, 30)),
    "police_uk_street_crime": ("Wed, 29 Jul 2026 14:43:38 GMT", date(2026, 7, 29)),
}


# --------------------------------------------------------------------------- #
# One type across every resolver
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("module", RESOLVERS.values(), ids=RESOLVERS)
def test_every_resolver_exposes_the_shared_type(module):
    assert module.ResolutionError is ResolutionError


@pytest.mark.parametrize(
    "failing",
    [
        lambda: hpi.resolve(None, "", ""),
        lambda: ons.newest_edition("<html>service unavailable</html>", "/economy/x"),
        lambda: police.latest_month("<html>503</html>"),
    ],
    ids=RESOLVERS,
)
def test_one_except_clause_catches_every_resolver(failing):
    """What the notebook actually does with them."""
    with pytest.raises(ResolutionError):
        failing()


def test_the_police_resolver_uses_the_shared_parse():
    """It reads the header for its own archive and the notebook reads it for three
    fixed-URL sources. Two copies would drift."""
    assert police.release_date is release_date


# --------------------------------------------------------------------------- #
# The release date
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "header, expected", LIVE.values(), ids=LIVE
)
def test_a_live_header_parses(header, expected):
    """Doogal's is the one that matters most: 20:47 GMT falls on the previous day in
    several western zones and the next in several eastern ones, so a parse that
    converted out of UTC would open the gate on the wrong day."""
    assert release_date(header) == expected


def test_a_missing_header_raises():
    """Every publisher measured sends one, but a source added later may not, and the
    gate has nothing to compare against without it. A silent fallback would read as a
    source with no release rather than as a source that cannot be gated."""
    with pytest.raises(ResolutionError, match="no Last-Modified"):
        release_date("")


def test_a_blank_header_raises():
    with pytest.raises(ResolutionError, match="no Last-Modified"):
        release_date("   ")


def test_a_header_that_is_not_a_date_raises():
    with pytest.raises(ResolutionError, match="not an HTTP date"):
        release_date("last tuesday")


def test_a_numeric_offset_parses_as_well_as_gmt():
    """All six write GMT now. RFC 7231 allows an offset and a publisher changing its
    server stack could start sending one."""
    assert release_date("Wed, 29 Jul 2026 14:43:38 +0000") == date(2026, 7, 29)
