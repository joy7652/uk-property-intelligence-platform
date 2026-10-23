"""Every metric a notebook records is one its run's layer admits.

AuditRun.measure refuses a name its layer does not admit, but only when the notebook
runs, which for a Gold fact load is once a month. Read statically here, every metric
name a notebook passes as a literal is checked against the pairing on every commit, so
a change to the pairing that strands a notebook fails in CI rather than in the job.

A notebook's layer is read off the folder it sits in. Two notebooks also record names
they do not spell out. The marts recorder's come from build_record, and test_marts
holds them to the marts layer. The police notebook's are the counts its validation pass
returns, named in the Silver transform, and the last test here reads them from there.

Notebooks are found as files, with or without the .py extension, so the same read works
on a repository checkout and in a Databricks Git folder.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from databricks_src.quality.audit.writer import LAYERS_OF_METRIC

REPO = Path(__file__).resolve().parents[2]

LAYER_OF_FOLDER: dict[str, str] = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "marts": "marts",
}

NOTEBOOKS: list[Path] = sorted(
    path
    for folder in REPO.glob("databricks_src/*/notebooks")
    for path in folder.iterdir()
    if path.is_file() and path.suffix in ("", ".py")
)


def layer_of(path: Path) -> str:
    return LAYER_OF_FOLDER[path.parent.parent.name]


def recorded_metrics(path: Path) -> list[tuple[int, str]]:
    """Line and name of every measure call whose first argument is a string literal."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return [
        (node.lineno, node.args[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "measure"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ]


def test_notebooks_are_found_in_every_layer_folder():
    """A glob that matched nothing would pass every check below."""
    assert {path.parent.parent.name for path in NOTEBOOKS} == set(LAYER_OF_FOLDER)


def test_every_notebook_folder_maps_to_a_layer():
    """A new folder of notebooks has to say which layer its runs open at, or the check
    below would have nothing to compare its metrics against."""
    folders = {folder.parent.name for folder in REPO.glob("databricks_src/*/notebooks")}
    assert folders <= set(LAYER_OF_FOLDER), sorted(folders - set(LAYER_OF_FOLDER))


def test_the_read_reaches_the_gold_fact_loads():
    """The two loads this check was written for, so a change in how they call measure
    cannot leave it silently checking nothing there."""
    gold = {
        name
        for path in NOTEBOOKS
        if layer_of(path) == "gold"
        for _, name in recorded_metrics(path)
    }
    assert {"gold_rows", "resolution_rows", "excluded_rows"} <= gold


@pytest.mark.parametrize(
    "path", NOTEBOOKS, ids=lambda path: f"{path.parent.parent.name}/{path.stem}"
)
def test_every_literal_metric_is_admitted_at_the_notebooks_layer(path: Path):
    layer = layer_of(path)
    refused = [
        (line, name, list(LAYERS_OF_METRIC.get(name, ())))
        for line, name in recorded_metrics(path)
        if layer not in LAYERS_OF_METRIC.get(name, ())
    ]
    assert not refused, (
        f"{path.name} runs at {layer} and records names that layer does not admit, "
        f"as (line, name, admitted at): {refused}"
    )


def test_the_police_validation_counts_are_admitted_at_silver():
    """The police notebook records each count the validation pass returns under the
    name the transform gives it, and two vocabularies under names it builds from the
    column, so none of them appears in the notebook as a literal."""
    transform = REPO / "databricks_src/silver/transforms/police.py"
    tree = ast.parse(transform.read_text(encoding="utf-8"), filename=str(transform))
    measures = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "measures"
    )
    counted = [
        call.args[0].value
        for call in ast.walk(measures)
        if isinstance(call, ast.Call) and getattr(call.func, "id", None) == "Measure"
    ]
    assert counted, "police.measures declares no Measure"

    names = [*counted, "crime_type_values", "last_outcome_category_values"]
    refused = [name for name in names if "silver" not in LAYERS_OF_METRIC.get(name, ())]
    assert not refused, refused
