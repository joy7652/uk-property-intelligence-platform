"""The three-step chain from a Bronze source to the Gold tables built on it.

Each source declares two steps: the dimensions built from it, and the facts built
behind those dimensions. Nothing here is derived from the star's foreign keys. A fact
conforming against a dimension is not the same as a fact whose rows depend on it, and
the difference is what decides whether a load waits.

The chain is hardcoded because it is a fact about the model rather than a setting. It
changes when a table is added, which is a code change with tests behind it.

When a step fails, everything downstream of it waits, and so does everything reached
through the same dimension by any other source. A fact rebuilt against a dimension
that did not rebuild in the same run is the case this exists to prevent.
"""

from __future__ import annotations

from collections.abc import Iterable

# Watermark source_name to the short name used by the audit writer and everything
# downstream. One entry per source, and both sides are closed sets.
SOURCE_OF: dict[str, str] = {
    "land_registry_ppd": "ppd",
    "land_registry_hpi": "hpi",
    "doogal_uk_postcode": "doogal",
    "boe_official_bank_rate_history": "boe",
    "ons_private_rent_index": "ons",
    "police_uk_street_crime": "police",
}

# source -> (dimensions built from it, facts built behind those dimensions)
CHAIN: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "boe": (
        ("dim_date",),
        (),
    ),
    "doogal": (
        ("dim_area", "dim_lsoa"),
        (
            "fact_area_month_price",
            "fact_area_month_transaction_mix",
            "fact_lsoa_year_price",
            "fact_area_month_crime",
            "fact_area_month_crime_total",
        ),
    ),
    "hpi": (
        ("dim_area",),
        ("fact_area_month_hpi",),
    ),
    "ons": (
        ("dim_area",),
        ("fact_area_month_rent",),
    ),
    "police": (
        ("dim_lsoa", "dim_crime_type"),
        (
            "fact_area_month_crime",
            "fact_area_month_crime_total",
            "fact_lsoa_month_crime",
            "fact_lsoa_month_crime_total",
        ),
    ),
    "ppd": (
        ("dim_lsoa",),
        (
            "fact_area_month_price",
            "fact_area_month_transaction_mix",
            "fact_lsoa_year_price",
        ),
    ),
}

SOURCES: tuple[str, ...] = tuple(CHAIN)

# Load order. Every dimension is written before any fact, so a fact never reads a
# dimension mid-write, and one stage per layer means a table reached by two sources
# is planned once.
DIMENSIONS: tuple[str, ...] = ("dim_date", "dim_area", "dim_lsoa", "dim_crime_type")

FACTS: tuple[str, ...] = (
    "fact_area_month_hpi",
    "fact_area_month_rent",
    "fact_area_month_price",
    "fact_area_month_transaction_mix",
    "fact_area_month_crime",
    "fact_area_month_crime_total",
    "fact_lsoa_month_crime",
    "fact_lsoa_month_crime_total",
    "fact_lsoa_year_price",
)


class DependencyError(Exception):
    """Raised where the chain cannot answer for something it was asked about."""


def assert_chain_consistent() -> None:
    """Fail on a chain that does not describe the star.

    Runs at import, so a table added to one constant and not the other stops the
    planner rather than producing a run that silently omits it.
    """
    if set(SOURCE_OF.values()) != set(SOURCES):
        raise DependencyError(
            f"watermark names map to {sorted(set(SOURCE_OF.values()))}, and the chain "
            f"declares {sorted(SOURCES)}."
        )
    if len(set(SOURCE_OF)) != len(SOURCE_OF):
        raise DependencyError("a watermark source_name is mapped more than once.")

    declared_dims = {dim for dims, _ in CHAIN.values() for dim in dims}
    declared_facts = {fact for _, facts in CHAIN.values() for fact in facts}

    orphan_dims = sorted(declared_dims - set(DIMENSIONS))
    orphan_facts = sorted(declared_facts - set(FACTS))
    if orphan_dims or orphan_facts:
        raise DependencyError(
            f"the chain names tables absent from the load order: "
            f"{orphan_dims + orphan_facts}"
        )

    unreachable = sorted((set(DIMENSIONS) | set(FACTS)) - (declared_dims | declared_facts))
    if unreachable:
        raise DependencyError(
            f"tables in the load order that no source reaches, so nothing would ever "
            f"plan them: {unreachable}"
        )

    # A check gated on a table the star does not declare would never be gated at all,
    # because the name could not appear in any plan.
    unknown_inputs = sorted(
        {
            table
            for inputs in CHECK_INPUTS.values()
            for table in inputs
            if table not in set(DIMENSIONS) | set(FACTS)
        }
    )
    if unknown_inputs:
        raise DependencyError(
            f"a check reads tables the load order does not declare, so its gate could "
            f"never close: {unknown_inputs}"
        )

    ungated = sorted(name for name, inputs in CHECK_INPUTS.items() if not inputs)
    if ungated:
        raise DependencyError(
            f"checks declaring no inputs, which would run against anything: {ungated}"
        )


# What each check must have rebuilt to report the rules it registers. A check builds no
# table, so nothing waits on it and it never cascades, but a rule evaluated over a table
# that did not rebuild reconciles a fresh table against a stale one and writes the
# result to `rule_result` as a reading, which is worse than not running.
#
# The rules, not everything the notebook touches. A section that only measures and
# records nothing gates itself on `Plan.rebuilt_this_run`, so a table it alone reads
# does not close the whole check: the reconciliation between the transaction series and
# the published index needs neither rent nor the calendar.
#
# Held here rather than in the notebook for the same reason CHAIN is: it is a fact about
# the model, it changes when a table is added, and a literal in one notebook is not
# reached by the tests that keep the rest of this file honest.
CHECK_INPUTS: dict[str, tuple[str, ...]] = {
    "cross_source_verification": (
        "fact_area_month_price",
        "fact_area_month_hpi",
        "dim_area",
    ),
}


assert_chain_consistent()


def dimensions_of(source: str) -> tuple[str, ...]:
    """The dimensions built from one source."""
    if source not in CHAIN:
        raise DependencyError(f"{source!r} is not a source. The chain declares {SOURCES}.")
    return CHAIN[source][0]


def facts_of(source: str) -> tuple[str, ...]:
    """The facts built behind one source's dimensions."""
    if source not in CHAIN:
        raise DependencyError(f"{source!r} is not a source. The chain declares {SOURCES}.")
    return CHAIN[source][1]


def steps_of(source: str) -> set[str]:
    """Every item on one source's chain, the source itself included."""
    return {source, *dimensions_of(source), *facts_of(source)}


def skip_list(failed: str) -> set[str]:
    """Everything that waits because one step failed.

    Two passes. The first names what the failure took with it directly: the item, and
    where the item is a source, the dimensions built from it. The second walks every
    source and, where its chain touches any of those, adds the facts behind it, since
    those facts sit behind a dimension that will not rebuild.

    A failed dimension carries no source of its own by the time it runs, because every
    Silver load has already finished. It is the second pass that finds the facts
    waiting on it, which is why the failed item itself is carried into that pass.
    """
    if failed in CHAIN:
        blocked = {failed, *dimensions_of(failed)}
    elif failed in DIMENSIONS:
        blocked = {failed}
    elif failed in FACTS:
        raise DependencyError(
            f"{failed!r} is a fact, and nothing is built behind a fact. A failed fact "
            "is recorded against its own run and stops there."
        )
    else:
        raise DependencyError(f"{failed!r} is not on any chain.")

    waiting: set[str] = set(blocked)
    for source in SOURCES:
        if blocked & steps_of(source):
            waiting.update(facts_of(source))
    return waiting


def cascading(names: Iterable[str]) -> tuple[set[str], set[str]]:
    """Split recorded failures into those that cascade and those that do not.

    A stage reads `pipeline_run` for this job run's failures and gets back whatever
    failed, which is not always something the chain carries. The cross-source
    verification builds no table. A fact has nothing behind it. Both are real
    failures and neither decides what loads next, so they are returned separately
    rather than raised on, and the caller prints them.

    Returns:
        (cascading, inert). The first is what to pass to `plan`.
    """
    cascades, inert = set(), set()
    for name in names:
        (cascades if name in CHAIN or name in DIMENSIONS else inert).add(name)
    return cascades, inert


def plan(failed: set[str] | None = None) -> dict[str, list[str]]:
    """What to run, by stage, given the steps that failed.

    Stages run in order and each is complete before the next opens: every Silver load,
    then every dimension, then every fact. A table two sources reach appears once.
    """
    skipped: set[str] = set()
    for item in sorted(failed or set()):
        skipped |= skip_list(item)

    return {
        "silver": [source for source in SOURCES if source not in skipped],
        "dimensions": [dim for dim in DIMENSIONS if dim not in skipped],
        "facts": [fact for fact in FACTS if fact not in skipped],
    }


def ordered(plan_by_stage: dict[str, list[str]]) -> list[str]:
    """The plan flattened into the sequence the job runs."""
    return [
        item
        for stage in ("silver", "dimensions", "facts")
        for item in plan_by_stage[stage]
    ]


def running_tables(plan_by_stage: dict[str, list[str]]) -> set[str]:
    """Every Gold table the plan keeps, dimensions and facts together.

    A Gold notebook asks whether one of its own tables is in the plan and does not
    care which stage that table belongs to.
    """
    return set(plan_by_stage["dimensions"]) | set(plan_by_stage["facts"])


def blocked_by(item: str, failed: Iterable[str]) -> list[str]:
    """Which of the recorded failures put one item in the skip set.

    The reverse of `skip_list`, and what lets a skipped run name its cause instead of
    only its status. Recomputing this later would answer under whatever chain is in
    force then, so the stage records it at the time.

    Names that do not cascade are ignored rather than raised on, matching
    `cascading`: a stage reads back whatever failed, and a failed fact or check is a
    real failure that blocks nothing.
    """
    cascades, _ = cascading(failed)
    return sorted(name for name in cascades if item in skip_list(name))

def depends_on(item: str) -> tuple[tuple[str, str], ...]:
    """What must have rebuilt in this run before one item can be written.

    Direct predecessors only. A dimension that succeeded in this run could not have
    done so had its sources failed, so naming them again would only make a recorded
    reason harder to follow.

    Pairs rather than names, because the layer is not a property of the name. A
    source records twice, at bronze when its file lands and at silver when the
    notebook reads it, so a Silver load waiting on the Bronze copy and a Gold load
    waiting on the Silver table are different questions about the same word.
    """
    if item in CHECK_INPUTS:
        return tuple((table, "gold") for table in sorted(CHECK_INPUTS[item]))
    if item in CHAIN:
        return ((item, "bronze"),)
    if item in DIMENSIONS:
        return tuple(
            (source, "silver") for source in SOURCES if item in dimensions_of(source)
        )
    if item in FACTS:
        return tuple(
            (dim, "gold")
            for dim in DIMENSIONS
            if any(
                dim in dimensions_of(source)
                for source in SOURCES
                if item in facts_of(source)
            )
        )
    raise DependencyError(f"{item!r} is not on any chain and declares no inputs.")


def waiting_on(
    item: str,
    failed: Iterable[str],
    rebuilt: Iterable[tuple[str, str]] | None = None,
) -> list[str]:
    """Everything stopping one item running, as a stage records it.

    Two questions, answered separately because they fail in different ways. `failed`
    is what this run recorded as broken, and a name reached through the chain from
    one of those is named plainly. `rebuilt` is the set of (name, layer) pairs
    recorded as succeeded, and a dependency missing from it with no failure behind it
    is named with its layer, because that combination means the job never reached it
    rather than that anything went wrong.

    An empty list means the item runs.

    Args:
        rebuilt: None where the caller is outside a job run and no evidence exists.
            The evidence clause is then skipped rather than treated as unmet, which
            is what keeps a notebook runnable by hand.
    """
    causes: dict[str, str | None] = dict.fromkeys(blocked_by(item, failed))
    # A cause naming the item itself is that item's own earlier run, and the row being
    # written already carries the name, so the layer is the whole of what it says. Every
    # Silver load waiting on its own Bronze copy reads this way.
    if item in causes:
        causes[item] = next(
            (layer for name, layer in depends_on(item) if name == item), None
        )
    if rebuilt is not None:
        have = set(rebuilt)
        for name, layer in depends_on(item):
            if (name, layer) not in have and name not in causes:
                causes[name] = layer
    return sorted(
        name if layer is None else f"{name} ({layer})"
        for name, layer in causes.items()
    )
