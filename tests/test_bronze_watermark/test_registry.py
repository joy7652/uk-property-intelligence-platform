"""Tests for the watermark registry.

Pure Python: the ADLS read and write live in the notebook, and what is checked here
is that one entry changes and five do not.

The fixture is cut down from the live watermark, keeping the fields the write-back
touches and the shape of the two load patterns.
"""

from __future__ import annotations

import json

import pytest

from databricks_src.bronze.watermark_library.registry import (
    WatermarkError,
    changed_fields,
    dump,
    find,
    load,
    names,
    update,
)

WATERMARK = json.dumps(
    [
        {
            "source_name": "land_registry_ppd",
            "load_pattern": "yearly_stepped",
            "relative_url_prefix": "/pp-",
            "start_year": 1995,
            "last_year_ingested": 1900,
            "active": False,
            "full_load_complete": False,
            "last_refreshed": "1900-01-01",
        },
        {
            "source_name": "land_registry_hpi",
            "load_pattern": "single_file",
            "relative_url": "/market-trend-data/UK-HPI-full-file-2026-05.csv",
            "file_name": "uk-hpi-full-file-2026-05.csv",
            "active": False,
            "last_refreshed": "1900-01-01",
        },
        {
            "source_name": "ons_private_rent_index",
            "load_pattern": "single_file",
            "relative_url": "?uri=/economy/x/22july2026/y14.xlsx",
            "file_name": "priceindexofprivaterents-2026-07.xlsx",
            "active": False,
            "last_refreshed": "1900-01-01",
        },
        {
            "source_name": "police_uk_street_crime",
            "load_pattern": "yearly_stepped",
            "last_year_ingested": 2025,
            "incremental_latest_snapshot": "2026-06",
            "active": True,
            "last_refreshed": "1900-01-01",
        },
    ],
    indent=2,
)

ONS = "ons_private_rent_index"

AUGUST = {
    "relative_url": "?uri=/economy/x/19august2026/y.xlsx",
    "file_name": "priceindexofprivaterents-2026-08.xlsx",
}


@pytest.fixture
def entries():
    return load(WATERMARK)


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #


def test_the_array_loads_in_declared_order(entries):
    """ADF iterates the array as it finds it, so order is part of the contract."""
    assert names(entries) == [
        "land_registry_ppd",
        "land_registry_hpi",
        ONS,
        "police_uk_street_crime",
    ]


def test_malformed_json_raises():
    with pytest.raises(WatermarkError, match="not valid JSON"):
        load("[{,}]")


def test_an_object_rather_than_an_array_raises():
    """ADF's Lookup reads an array. An object would fetch nothing and report success."""
    with pytest.raises(WatermarkError, match="array"):
        load('{"source_name": "x"}')


def test_an_empty_array_raises():
    with pytest.raises(WatermarkError, match="empty"):
        load("[]")


def test_an_entry_without_a_source_name_raises():
    with pytest.raises(WatermarkError, match="carry no source_name"):
        load('[{"relative_url": "?uri=x"}]')


def test_a_repeated_source_name_raises():
    """Two entries under one name make every lookup ambiguous, and the one that wins
    depends on declaration order."""
    with pytest.raises(WatermarkError, match="repeated"):
        load('[{"source_name": "a"}, {"source_name": "a"}]')


def test_find_returns_the_entry(entries):
    assert find(entries, ONS)["file_name"] == "priceindexofprivaterents-2026-07.xlsx"


def test_finding_an_unknown_source_names_what_is_there(entries):
    with pytest.raises(WatermarkError, match="land_registry_hpi"):
        find(entries, "ons_private_rents")


# --------------------------------------------------------------------------- #
# Updating
# --------------------------------------------------------------------------- #


def test_the_named_entry_takes_the_change(entries):
    updated = update(entries, ONS, AUGUST)
    assert find(updated, ONS)["relative_url"] == AUGUST["relative_url"]
    assert find(updated, ONS)["file_name"] == AUGUST["file_name"]


def test_every_other_entry_is_untouched(entries):
    updated = update(entries, ONS, AUGUST)
    for name in names(entries):
        if name != ONS:
            assert find(updated, name) == find(entries, name)


def test_the_original_array_is_not_mutated(entries):
    update(entries, ONS, AUGUST)
    assert find(entries, ONS)["file_name"] == "priceindexofprivaterents-2026-07.xlsx"


def test_order_survives_an_update(entries):
    assert names(update(entries, ONS, AUGUST)) == names(entries)


def test_fields_not_named_are_left_alone(entries):
    """A merge, not a replacement. Setting the URL must not clear last_refreshed."""
    updated = update(entries, ONS, {"relative_url": AUGUST["relative_url"]})
    assert find(updated, ONS)["last_refreshed"] == "1900-01-01"
    assert find(updated, ONS)["file_name"] == "priceindexofprivaterents-2026-07.xlsx"


def test_setting_a_field_the_entry_does_not_carry_raises(entries):
    """A typo would add a key nothing reads, and the load would go on using the old
    value with no sign the write had missed."""
    with pytest.raises(WatermarkError, match="relative_uri"):
        update(entries, ONS, {"relative_uri": "?uri=x"})


def test_the_message_lists_the_fields_that_do_exist(entries):
    with pytest.raises(WatermarkError, match="relative_url"):
        update(entries, ONS, {"relatve_url": "?uri=x"})


def test_an_update_with_no_fields_raises(entries):
    """A caller reaching here with nothing to write has lost its result."""
    with pytest.raises(WatermarkError, match="no fields"):
        update(entries, ONS, {})


def test_a_field_on_one_load_pattern_is_not_on_the_other(entries):
    """single_file entries carry no last_year_ingested, and writing one would look
    like a yearly source that had never run."""
    with pytest.raises(WatermarkError, match="last_year_ingested"):
        update(entries, ONS, {"last_year_ingested": 2026})


# --------------------------------------------------------------------------- #
# Reporting the difference
# --------------------------------------------------------------------------- #


def test_the_difference_names_only_what_moved(entries):
    difference = changed_fields(entries, update(entries, ONS, AUGUST))
    assert set(difference) == {ONS}
    assert set(difference[ONS]) == {"relative_url", "file_name"}


def test_the_difference_carries_both_values(entries):
    difference = changed_fields(entries, update(entries, ONS, AUGUST))
    assert difference[ONS]["file_name"] == (
        "priceindexofprivaterents-2026-07.xlsx",
        "priceindexofprivaterents-2026-08.xlsx",
    )


def test_no_change_reports_nothing(entries):
    assert changed_fields(entries, entries) == {}


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #


def test_the_serialised_watermark_reads_back(entries):
    assert json.loads(dump(entries)) == entries


def test_the_serialised_watermark_ends_with_a_newline(entries):
    assert dump(entries).endswith("\n")


def test_dropping_an_entry_raises(entries):
    """ADF fetches what the file says. A source silently absent stops being ingested
    and nothing reports it."""
    with pytest.raises(WatermarkError, match="order changed"):
        dump(entries[:-1], original=entries)


def test_reordering_entries_raises(entries):
    with pytest.raises(WatermarkError, match="order changed"):
        dump(list(reversed(entries)), original=entries)


def test_an_updated_array_passes_the_original_check(entries):
    updated = update(entries, ONS, AUGUST)
    assert json.loads(dump(updated, original=entries)) == updated
