"""Lookup and field merge over the watermark array.

The watermark is one JSON array in the `configs` container, read by ADF's Lookup
before anything else runs. Six entries, each keyed on `source_name`. A notebook
changing one entry has to leave the other five byte-identical, because a reformat or
a reordering shows up as a change to every source and hides the one that mattered.

Every function here takes the parsed array and returns a new one. The read from ADLS
and the write back live in the notebook, as with every other module in this project.

Fields are merged rather than replaced, and only fields the entry already carries can
be set. A typo would otherwise add a key nothing reads, and the load would go on
using the old value with no sign that the write had missed.
"""

from __future__ import annotations

import copy
import json
from typing import Any

Entry = dict[str, Any]

KEY = "source_name"


class WatermarkError(Exception):
    """Raised where the array cannot be read or changed as expected.

    ADF fetches whatever the file says without inspecting it, so a wrong write is
    acted on. Stopping is the only safe failure.
    """


def load(text: str) -> list[Entry]:
    """Parse the watermark, checking it is the shape the rest of this module assumes."""
    try:
        entries = json.loads(text)
    except json.JSONDecodeError as error:
        raise WatermarkError(f"watermark is not valid JSON: {error}") from None

    if not isinstance(entries, list):
        raise WatermarkError(
            f"watermark is a {type(entries).__name__}, and ADF's Lookup reads an array."
        )
    if not entries:
        raise WatermarkError("watermark is empty, so no source would be fetched.")

    nameless = [index for index, entry in enumerate(entries) if KEY not in entry]
    if nameless:
        raise WatermarkError(f"entries at {nameless} carry no {KEY}.")

    names = [entry[KEY] for entry in entries]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise WatermarkError(
            f"{KEY} repeated in the watermark, so a lookup would be ambiguous: {repeated}"
        )
    return entries


def names(entries: list[Entry]) -> list[str]:
    """Every source name, in the order the array declares them."""
    return [entry[KEY] for entry in entries]


def find(entries: list[Entry], source_name: str) -> Entry:
    """The entry for one source.

    Absent is an error rather than a skip: a resolver naming a source the watermark
    does not carry has been pointed at the wrong file or the name has been renamed
    under it, and continuing would resolve a URL nothing fetches.
    """
    for entry in entries:
        if entry[KEY] == source_name:
            return entry
    raise WatermarkError(
        f"{source_name!r} is not in the watermark. It carries {names(entries)}."
    )


def update(entries: list[Entry], source_name: str, changes: dict[str, Any]) -> list[Entry]:
    """A copy of the array with one entry's fields changed.

    Only fields already on the entry may be set, and every other entry is carried
    through untouched and in place.
    """
    if not changes:
        raise WatermarkError(
            f"no fields given for {source_name!r}. An update that changes nothing "
            "still rewrites the file, and a caller reaching here has lost its result."
        )

    target = find(entries, source_name)
    unknown = sorted(set(changes) - set(target))
    if unknown:
        raise WatermarkError(
            f"{source_name!r} carries no field {unknown}. Adding one would write a key "
            f"nothing reads, and the load would keep using the old value. The entry "
            f"carries {sorted(target)}."
        )

    updated = copy.deepcopy(entries)
    for entry in updated:
        if entry[KEY] == source_name:
            entry.update(changes)
    return updated


def changed_fields(before: list[Entry], after: list[Entry]) -> dict[str, dict[str, Any]]:
    """Every field that differs, by source, as {source: {field: (old, new)}}.

    Printed by the notebook before the write, so a run says what it is about to
    change rather than only that it changed something.
    """
    difference: dict[str, dict[str, Any]] = {}
    for old_entry, new_entry in zip(before, after):
        moved = {
            field: (old_entry.get(field), new_entry.get(field))
            for field in set(old_entry) | set(new_entry)
            if old_entry.get(field) != new_entry.get(field)
        }
        if moved:
            difference[new_entry[KEY]] = moved
    return difference


def dump(entries: list[Entry], original: list[Entry] | None = None) -> str:
    """Serialise the array, checking it reads back as what was passed in.

    A half-written or reordered watermark is fetched by ADF exactly as found, so the
    output is validated before the caller writes it. Passing the original array also
    checks the entry count and order are unchanged, which no round-trip can catch on
    its own.
    """
    if original is not None:
        if names(original) != names(entries):
            raise WatermarkError(
                f"source names or their order changed: {names(original)} became "
                f"{names(entries)}."
            )

    text = json.dumps(entries, indent=2, ensure_ascii=False) + "\n"

    if json.loads(text) != entries:
        raise WatermarkError("serialised watermark does not read back as itself.")
    return text
