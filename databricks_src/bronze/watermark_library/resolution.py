"""Shared pieces the source resolvers are built from.

Each source whose URL or release date has to be worked out has its own resolver, and
each reads a different kind of answer: a dataset page, a JSON endpoint, a constructed
address. Two things are common to all of them and live here rather than several times
over.

The failure type, so the pre-run notebook names one exception instead of one per
source, and a resolver added later cannot quietly fall outside a tuple nobody
remembered to extend.

The release date parse, because once the address is known every publisher in this
project states its release the same way: an HTTP date in `Last-Modified` on the object
itself. That was measured on 28-08-2026 across all six sources and three different
server stacks, and it holds for the sources with a fixed URL as much as for the ones
whose URL moves.
"""

from __future__ import annotations

from datetime import date
from email.utils import parsedate_to_datetime


class ResolutionError(Exception):
    """Raised where a publisher's answer cannot be read as a released edition.

    A resolver that returns something wrong is worse than one that stops: the URL it
    produces is fetched by the orchestrator without further inspection. Stopping
    leaves the watermark as it was, so the source looks unchanged to ADF, and the
    failure marker the caller writes is the only thing separating that from a month
    with no release.
    """


def release_date(last_modified: str) -> date:
    """The date an object was published, from its Last-Modified header.

    Every publisher here writes the object when it releases, so the header moves with
    the release rather than tracking edits to a file that stays where it is. A rewrite
    carrying no new data, such as a bucket-wide restructure, costs one redundant fetch
    and nothing else.

    The date is read in the zone the publisher wrote in, which is UTC for all six.
    Converting to a local zone would roll an evening release onto the wrong day and
    open the gate early or late by one.
    """
    if not last_modified or not last_modified.strip():
        raise ResolutionError(
            "Object carries no Last-Modified header, so its release date cannot be "
            "read and the gate has nothing to compare against last_refreshed."
        )
    try:
        return parsedate_to_datetime(last_modified).date()
    except (TypeError, ValueError) as error:
        raise ResolutionError(
            f"Last-Modified reads {last_modified!r}, which is not an HTTP date: {error}"
        ) from None
