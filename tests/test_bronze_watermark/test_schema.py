"""The schema, against entries that are wrong in one specific way each.

There is no watermark in the repository. It is state, written by
`01_pre_run_resolve_urls` to the volume at
`/Volumes/uk_property_intel/configs/watermark/watermark.json`, and CI has no volume. So
this suite tests the contract rather than the file: given an entry with a mistyped key,
a month where a date belongs, or a route key from the other route, does the schema
refuse it. The live file is checked by the runner notebook, which does have a volume.

That makes the fixtures below hand-written, which normally drifts from what it
represents. Two tests hold them to the schema instead: every key the schema requires
appears in a fixture, and every key a fixture carries is one the schema declares. A key
added to the schema and forgotten here fails immediately, in both directions.

Nothing asserts a value. That `full_load_yearly_refresh_month` is 7 or 8 is a decision
recorded in DESIGN and changed by editing the volume copy; this holds only that it is an
integer inside 0 to 12 and present on the route that reads it.
"""

from __future__ import annotations

import copy
import json

import pytest

from databricks_src.bronze.watermark_library import registry
from databricks_src.bronze.watermark_library.schema import (
    SCHEMA_PATH,
    WHOLE_NUMBER_FIELDS,
    assert_invariants,
    load_schema,
    validate,
)

# Minimal valid entries, one per route, as the base every mutation starts from. Values
# are the least interesting thing that satisfies each keyword: the point is which keys
# are here, not what they hold.
_COMMON: dict = {
    "display_name": "Example Publisher — Example Dataset",
    "_comment": "Fixture. Not a real source.",
    "linked_service_type": "anonymous",
    "base_url": "https://example.gov.uk",
    "active": True,
    "full_load_complete": False,
    "last_refreshed": "2026-01-01",
    "latest_release": "2026-01-01",
}

SINGLE_FILE: dict = {
    **_COMMON,
    "source_name": "example_single_file",
    "load_pattern": "single_file",
    "folder_name": "example/single/",
    "relative_url": "/example.csv",
    "file_name": "example.csv",
}

YEARLY_STEPPED: dict = {
    **_COMMON,
    "source_name": "example_yearly_stepped",
    "load_pattern": "yearly_stepped",
    "folder_name": "example/yearly/",
    "relative_url_prefix": "/example-",
    "file_name_prefix": "example-",
    "file_extension": ".csv",
    "start_year": 2000,
    "step_years": 1,
    "snapshot_month": None,
    "last_year_ingested": 1900,
    "full_load_yearly_refresh_month": 8,
    "incremental_type": "static_url",
    "incremental_relative_url_prefix": "/example-latest.txt",
    "incremental_folder_name": "example/monthly/",
    "incremental_file_name_prefix": "example-latest-",
    "incremental_latest_snapshot": None,
}

FIXTURES: dict[str, dict] = {
    "single_file": SINGLE_FILE,
    "yearly_stepped": YEARLY_STEPPED,
}


@pytest.fixture(scope="module")
def entries() -> list[dict]:
    return [copy.deepcopy(SINGLE_FILE), copy.deepcopy(YEARLY_STEPPED)]


@pytest.fixture(scope="module")
def schema() -> dict:
    return load_schema()


def one(entries: list[dict], load_pattern: str) -> dict:
    """A fresh copy of one route's fixture, as the base for a mutation."""
    del entries
    return copy.deepcopy(FIXTURES[load_pattern])


# --------------------------------------------------------------------------- #
# The fixtures are faithful to the schema
# --------------------------------------------------------------------------- #


def test_schema_is_a_valid_schema() -> None:
    load_schema()


def test_the_fixtures_pass(entries, schema) -> None:
    """Every mutation below starts from these, so a fixture that is already broken
    would make each of them pass for the wrong reason."""
    validate(entries, schema)
    assert_invariants(entries)


def test_the_fixtures_carry_every_key_the_schema_requires(schema) -> None:
    """The drift guard in the direction that matters.

    A key added to the schema and not to a fixture leaves every mutation test running
    against an entry that no longer represents the contract, and all of them keep
    passing.
    """
    item = schema["items"]
    common = set(item["required"])
    for clause in item["allOf"]:
        route = clause["if"]["properties"]["load_pattern"]["const"]
        required = common | set(clause["then"]["required"])
        missing = sorted(required - set(FIXTURES[route]))
        assert not missing, f"{route} fixture is missing schema-required keys: {missing}"


def test_the_fixtures_carry_no_key_the_schema_does_not_declare(schema) -> None:
    """The other direction, which `unevaluatedProperties` would also catch, reported as
    a list of names instead of one error per fixture."""
    item = schema["items"]
    declared = set(item["properties"])
    for clause in item["allOf"]:
        declared |= set(clause["then"]["properties"])
    for route, fixture in FIXTURES.items():
        undeclared = sorted(set(fixture) - declared)
        assert not undeclared, f"{route} fixture carries undeclared keys: {undeclared}"


def test_the_whole_number_fields_are_all_declared_as_integers(schema) -> None:
    """`assert_invariants` checks these by name, so a field renamed in the schema and
    not in that tuple would stop being checked with nothing to show for it."""
    declared = dict(schema["items"]["properties"])
    for clause in schema["items"]["allOf"]:
        declared.update(clause["then"]["properties"])
    for field in WHOLE_NUMBER_FIELDS:
        assert field in declared, f"{field} is checked as a whole number and not in the schema"
        assert declared[field].get("type") == "integer", (
            f"{field} is checked as a whole number and the schema types it as "
            f"{declared[field].get('type')!r}"
        )


# --------------------------------------------------------------------------- #
# The schema refuses what it should
# --------------------------------------------------------------------------- #


def test_a_missing_common_key_is_refused(entries, schema) -> None:
    broken = one(entries, "single_file")
    del broken["latest_release"]
    with pytest.raises(registry.WatermarkError, match="latest_release"):
        validate([broken], schema)


def test_a_mistyped_key_is_refused(entries, schema) -> None:
    """The fault a typo produces, which is otherwise identical to a missing key."""
    broken = one(entries, "single_file")
    broken["relative_urI"] = broken.pop("relative_url")
    with pytest.raises(registry.WatermarkError) as raised:
        validate([broken], schema)
    assert "relative_urI" in str(raised.value)
    assert "relative_url" in str(raised.value)


def test_an_unknown_load_pattern_is_refused(entries, schema) -> None:
    broken = one(entries, "single_file")
    broken["load_pattern"] = "monthly_stepped"
    with pytest.raises(registry.WatermarkError, match="monthly_stepped"):
        validate([broken], schema)


def test_an_unknown_linked_service_is_refused(entries, schema) -> None:
    broken = one(entries, "single_file")
    broken["linked_service_type"] = "bearer_token"
    with pytest.raises(registry.WatermarkError, match="bearer_token"):
        validate([broken], schema)


def test_a_yearly_key_on_a_single_file_source_is_refused(entries, schema) -> None:
    """Keys that belong to the other route are refused, not ignored.

    A single-file entry carrying `start_year` reads as configured for a stepped load
    that will never run, and the next reader believes it.
    """
    broken = one(entries, "single_file")
    broken["start_year"] = 1995
    with pytest.raises(registry.WatermarkError, match="start_year"):
        validate([broken], schema)


def test_a_yearly_source_missing_its_refresh_month_is_refused(entries, schema) -> None:
    """ADF fails the run on a key its expression names and the entry does not carry."""
    broken = one(entries, "yearly_stepped")
    del broken["full_load_yearly_refresh_month"]
    with pytest.raises(registry.WatermarkError, match="full_load_yearly_refresh_month"):
        validate([broken], schema)


@pytest.mark.parametrize("month", [-1, 13, "8", None])
def test_a_refresh_month_outside_the_calendar_is_refused(entries, schema, month) -> None:
    """Zero is the off switch and 1 to 12 are months. Everything else is a typo.

    The string case matters because ADF compares against a typed integer parameter, so
    a quoted month compares unequal every run while looking correct in the file. The
    float case is not here: JSON Schema counts 8.0 as an integer, so it is an invariant.
    """
    broken = one(entries, "yearly_stepped")
    broken["full_load_yearly_refresh_month"] = month
    with pytest.raises(registry.WatermarkError):
        validate([broken], schema)


def test_a_refresh_month_inside_the_calendar_is_accepted(entries, schema) -> None:
    for month in (0, 1, 7, 8, 12):
        entry = one(entries, "yearly_stepped")
        entry["full_load_yearly_refresh_month"] = month
        validate([entry], schema)


def test_a_release_date_that_is_not_a_date_is_refused(entries, schema) -> None:
    """A month written where a date belongs is what the gate was first built on."""
    broken = one(entries, "single_file")
    broken["latest_release"] = "2026-06"
    with pytest.raises(registry.WatermarkError, match="latest_release"):
        validate([broken], schema)


def test_a_calendar_day_that_does_not_exist_is_refused(entries, schema) -> None:
    """Shape alone would accept this, which is why the format checker is switched on."""
    broken = one(entries, "single_file")
    broken["latest_release"] = "2026-02-30"
    with pytest.raises(registry.WatermarkError, match="latest_release"):
        validate([broken], schema)


def test_a_folder_without_its_trailing_slash_is_refused(entries, schema) -> None:
    broken = one(entries, "single_file")
    broken["folder_name"] = broken["folder_name"].rstrip("/")
    with pytest.raises(registry.WatermarkError, match="folder_name"):
        validate([broken], schema)


def test_every_fault_is_reported_in_one_pass(entries, schema) -> None:
    """A hand-edited watermark carries several faults of one kind, and fixing them one
    run at a time is what the batching exists to avoid."""
    broken = one(entries, "single_file")
    del broken["latest_release"]
    broken["load_pattern"] = "monthly_stepped"
    broken["active"] = "true"

    with pytest.raises(registry.WatermarkError) as raised:
        validate([broken, broken], schema)

    message = str(raised.value)
    # Both entries report, and every seeded fault reports. The total is not asserted:
    # an unknown load_pattern also leaves the route keys unevaluated, so one edit
    # legitimately produces more than one line.
    assert message.count("latest_release") == 2
    assert message.count("monthly_stepped") == 2
    assert message.count("'true' is not of type 'boolean'") == 2


def test_the_faulting_entry_is_named_by_its_source(entries, schema) -> None:
    """An index alone means counting entries in a file to find the one that broke."""
    broken = one(entries, "single_file")
    broken["active"] = "yes"
    with pytest.raises(registry.WatermarkError, match=broken["source_name"]):
        validate([broken], schema)


def test_an_entry_with_no_name_is_reported_by_index(entries, schema) -> None:
    """`registry.load` refuses this first, so the message only has to be readable."""
    broken = one(entries, "single_file")
    del broken["source_name"]
    with pytest.raises(registry.WatermarkError, match="entry 0"):
        validate([broken], schema)


def test_an_empty_array_is_refused(schema) -> None:
    with pytest.raises(registry.WatermarkError):
        validate([], schema)


# --------------------------------------------------------------------------- #
# Invariants across entries
# --------------------------------------------------------------------------- #


def test_two_sources_sharing_a_bronze_folder_are_refused(entries) -> None:
    first, second = copy.deepcopy(entries[0]), copy.deepcopy(entries[1])
    second["folder_name"] = first["folder_name"]
    with pytest.raises(registry.WatermarkError, match="folder_name"):
        assert_invariants([first, second])


@pytest.mark.parametrize(
    "field", ["start_year", "step_years", "last_year_ingested", "full_load_yearly_refresh_month"]
)
def test_a_whole_number_written_as_a_decimal_is_refused(entries, field) -> None:
    """JSON Schema reads 8.0 as an integer and json.dumps writes it back as 8.0.

    ADF compares these against typed integer parameters, so the check is here rather
    than in the schema, which has no keyword that can tell the two apart.
    """
    broken = one(entries, "yearly_stepped")
    broken[field] = float(broken[field])
    with pytest.raises(registry.WatermarkError, match=field):
        assert_invariants([broken])


def test_a_source_landing_both_loads_in_one_folder_is_refused(entries) -> None:
    broken = one(entries, "yearly_stepped")
    broken["incremental_folder_name"] = broken["folder_name"]
    with pytest.raises(registry.WatermarkError, match="same folder"):
        assert_invariants([broken])


def test_a_missing_schema_file_is_a_watermark_error() -> None:
    with pytest.raises(registry.WatermarkError, match="not found"):
        load_schema(SCHEMA_PATH.with_name("watermark.schema.absent.json"))


def test_a_schema_that_is_not_json_is_a_watermark_error(tmp_path) -> None:
    broken = tmp_path / "watermark.schema.json"
    broken.write_text("{", encoding="utf-8")
    with pytest.raises(registry.WatermarkError, match="not valid JSON"):
        load_schema(broken)


def test_the_schema_file_is_formatted_consistently() -> None:
    """Two-space JSON, as the watermark itself is written, so a diff on either reads
    the same way."""
    text = SCHEMA_PATH.read_text(encoding="utf-8")
    assert text == json.dumps(json.loads(text), indent=2, ensure_ascii=False) + "\n"
