"""The watermark's key-level contract, and the invariants a schema cannot hold.

`registry.load` already checks the minimum ADF's Lookup needs: valid JSON, a non-empty
array, every entry carrying a `source_name`, and no name repeated. That is what has to
be true before a lookup means anything, and it stays there.

This module checks the rest. Every key ADF or a resolver reads is declared in
`config/watermark.schema.json`, because a key absent from an entry fails the pipeline
run outright rather than evaluating to nothing, and a mistyped key is indistinguishable
from a missing one at that point. Catching it at commit time costs a second; catching it
at run time costs a monthly window.

The split between the two checks here follows what JSON Schema can and cannot say. A
schema states which keys exist, what type each holds, and which set of keys a load
pattern brings with it. It cannot compare one entry against another, and it cannot
compare the file against anything outside itself. Those are `assert_invariants`.

Shape, never values. Nothing here asserts that a date is recent, that a refresh month is
the one intended, or that a URL resolves. A watermark can satisfy every check in this
module and still be pointed at the wrong release.

No I/O. Both functions take parsed objects, so the CI suite can read the committed file
and a notebook can read the live one in the configs container without either path being
built into the check.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from databricks_src.bronze.watermark_library.registry import KEY, WatermarkError

Entry = dict[str, Any]

# JSON Schema counts 8.0 as an integer, and json.dumps writes it back as 8.0. ADF
# compares these against a typed integer parameter, so the float form is checked here
# instead.
WHOLE_NUMBER_FIELDS: tuple[str, ...] = (
    "start_year",
    "step_years",
    "last_year_ingested",
    "full_load_yearly_refresh_month",
)

# config/ sits beside databricks_src/ at the repository root.
SCHEMA_PATH = Path(__file__).resolve().parents[3] / "config" / "watermark.schema.json"


def load_schema(path: Path | None = None) -> dict[str, Any]:
    """Read and compile-check the schema itself.

    A schema with a malformed keyword validates nothing and reports no error, so it is
    checked against its own meta-schema before it is used on data.
    """
    target = SCHEMA_PATH if path is None else path
    try:
        schema = json.loads(target.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise WatermarkError(f"watermark schema not found at {target}") from None
    except json.JSONDecodeError as error:
        raise WatermarkError(f"watermark schema is not valid JSON: {error}") from None

    Draft202012Validator.check_schema(schema)
    return schema


def validate(entries: list[Entry], schema: dict[str, Any] | None = None) -> None:
    """Check every entry against the schema, reporting all failures at once.

    `iter_errors` rather than `validate`, because a hand-edited watermark tends to carry
    several faults of the same kind and fixing them one run at a time wastes the run.

    Raises:
        WatermarkError: naming each failing entry by source name where it has one, and
            by index where the fault is that it does not.
    """
    validator = Draft202012Validator(
        load_schema() if schema is None else schema,
        format_checker=FormatChecker(),
    )
    faults = []
    for error in sorted(validator.iter_errors(entries), key=lambda e: list(e.absolute_path)):
        path = list(error.absolute_path)
        where = "watermark"
        if path and isinstance(path[0], int):
            entry = entries[path[0]]
            named = entry.get(KEY) if isinstance(entry, dict) else None
            where = f"{named!r}" if named else f"entry {path[0]}"
            field = ".".join(str(part) for part in path[1:])
            if field:
                where = f"{where}.{field}"
        faults.append(f"{where}: {error.message}")

    if faults:
        raise WatermarkError(
            f"{len(faults)} watermark fault(s) against the schema:\n  "
            + "\n  ".join(faults)
        )


def assert_invariants(entries: list[Entry]) -> None:
    """Check what holds across entries rather than within one.

    Two kinds live here. Facts about the array, such as two sources landing in the same
    Bronze folder, which each entry satisfies alone while one overwrites the other. And
    facts a schema keyword cannot express, such as an integer that is not a float.
    `source_name` uniqueness is absent because `registry.load` already refuses it.

    Raises:
        WatermarkError: naming every breach found, not the first.
    """
    faults: list[str] = []

    faults.extend(_repeated("folder_name", entries))
    faults.extend(_repeated("incremental_folder_name", entries))

    for entry in entries:
        name = entry.get(KEY, "?")
        for field in WHOLE_NUMBER_FIELDS:
            value = entry.get(field)
            if value is not None and not isinstance(value, int):
                faults.append(
                    f"{name!r}: {field} is {value!r}, which serialises as a decimal. "
                    "ADF compares it against a typed integer parameter."
                )

        folder = entry.get("folder_name")
        incremental = entry.get("incremental_folder_name")
        if folder is not None and folder == incremental:
            faults.append(
                f"{name!r}: the full load and the incremental land in "
                f"the same folder {folder!r}, so a monthly file would sit among the "
                "yearly ones and the yearly reader would take it."
            )

    if faults:
        raise WatermarkError(
            f"{len(faults)} watermark invariant breach(es):\n  " + "\n  ".join(faults)
        )


def _repeated(field: str, entries: list[Entry]) -> list[str]:
    """Every value of `field` carried by more than one entry."""
    seen: dict[Any, list[str]] = {}
    for entry in entries:
        value = entry.get(field)
        if value is None:
            continue
        seen.setdefault(value, []).append(str(entry.get(KEY, "?")))
    return [
        f"{field} {value!r} is used by {names}, and one would overwrite the other."
        for value, names in seen.items()
        if len(names) > 1
    ]
