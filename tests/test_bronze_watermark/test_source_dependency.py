"""Tests for the three-step source dependency chain.

Pure Python. The chain is a fact about the model, so most of what matters here is
that it stays consistent with the star and that the skip cascade reaches everything
standing behind a failure.

The three worked examples are pinned as tests. They were settled by hand before the
code existed, so a change to the cascade that quietly alters them fails here.
"""

from __future__ import annotations

import pytest

from databricks_src.bronze.watermark_library.source_dependency import (
    CHAIN,
    CHECK_INPUTS,
    DIMENSIONS,
    FACTS,
    SOURCE_OF,
    SOURCES,
    DependencyError,
    assert_chain_consistent,
    blocked_by,
    cascading,
    depends_on,
    dimensions_of,
    facts_of,
    ordered,
    plan,
    running_tables,
    skip_list,
    waiting_on,
)

DOOGAL_FACTS = {
    "fact_area_month_price",
    "fact_area_month_transaction_mix",
    "fact_lsoa_year_price",
    "fact_area_month_crime",
    "fact_area_month_crime_total",
}
POLICE_FACTS = {
    "fact_area_month_crime",
    "fact_area_month_crime_total",
    "fact_lsoa_month_crime",
    "fact_lsoa_month_crime_total",
}
PPD_FACTS = {
    "fact_area_month_price",
    "fact_area_month_transaction_mix",
    "fact_lsoa_year_price",
}


# --------------------------------------------------------------------------- #
# The chain against the star
# --------------------------------------------------------------------------- #


def test_chain_is_consistent():
    """Runs at import too, so a table added to one constant and not the other stops
    the planner rather than producing a run that omits it."""
    assert_chain_consistent()


def test_every_source_has_a_chain():
    assert set(CHAIN) == set(SOURCES)


def test_every_watermark_name_maps_to_a_source():
    """The watermark writes long names and everything downstream uses short ones.
    A name on neither side would resolve to nothing at plan time."""
    assert set(SOURCE_OF.values()) == set(SOURCES)
    assert len(SOURCE_OF) == len(SOURCES)


def test_every_gold_table_is_reached_by_some_source():
    reached = {table for dims, facts in CHAIN.values() for table in (*dims, *facts)}
    assert reached == set(DIMENSIONS) | set(FACTS)


def test_dimensions_and_facts_do_not_overlap():
    assert not set(DIMENSIONS) & set(FACTS)


def test_the_star_has_four_dimensions_and_nine_facts():
    assert (len(DIMENSIONS), len(FACTS)) == (4, 9)


@pytest.mark.parametrize("source", sorted(SOURCES))
def test_no_source_declares_a_fact_without_a_dimension(source):
    """A fact is built behind a dimension. One declared with no dimension above it
    would never be reached by a cascade through that dimension."""
    assert dimensions_of(source) or not facts_of(source)


# --------------------------------------------------------------------------- #
# The worked examples
# --------------------------------------------------------------------------- #


def test_boe_failing_takes_only_the_calendar():
    """Nothing is declared behind dim_date, so the cascade stops there."""
    assert skip_list("boe") == {"boe", "dim_date"}


def test_doogal_failing_takes_every_fact():
    """Doogal feeds both geography dimensions, so every fact stands behind it."""
    assert skip_list("doogal") == {"doogal", "dim_area", "dim_lsoa", *FACTS}


def test_doogal_failing_leaves_the_other_two_dimensions():
    assert not skip_list("doogal") & {"dim_date", "dim_crime_type"}


def test_a_failed_dimension_reaches_the_facts_behind_it():
    """By the time a dimension fails every Silver load has finished, so the failure
    carries no source with it. The facts waiting on it are found by walking every
    source whose chain touches it."""
    assert skip_list("dim_lsoa") == {"dim_lsoa", *POLICE_FACTS, *PPD_FACTS, *DOOGAL_FACTS}


def test_a_failed_dimension_does_not_skip_a_sibling_dimension():
    """dim_crime_type is built from Police like dim_lsoa is, and neither stands
    behind the other."""
    assert "dim_crime_type" not in skip_list("dim_lsoa")


def test_one_source_failing_reaches_facts_of_other_sources():
    """fact_area_month_hpi is built from HPI alone, and sits behind dim_area, which
    Doogal also feeds."""
    assert "fact_area_month_hpi" in skip_list("doogal")


# --------------------------------------------------------------------------- #
# Refusals
# --------------------------------------------------------------------------- #


def test_a_failed_fact_is_refused():
    """Nothing is built behind a fact, so a failure there is recorded against its own
    run. Asking for a cascade would skip its siblings for no reason."""
    with pytest.raises(DependencyError, match="is a fact"):
        skip_list("fact_area_month_rent")


def test_an_unknown_name_is_refused():
    with pytest.raises(DependencyError, match="not on any chain"):
        skip_list("dim_areaa")


def test_asking_for_an_unknown_source_names_what_exists():
    with pytest.raises(DependencyError, match="ppd"):
        facts_of("land_registry_ppd")


# --------------------------------------------------------------------------- #
# Reading a run's failures
# --------------------------------------------------------------------------- #


def test_sources_and_dimensions_cascade():
    cascades, inert = cascading(["ons", "dim_area"])
    assert cascades == {"ons", "dim_area"}
    assert inert == set()


def test_a_failed_check_does_not_cascade():
    """cross_source_verification builds no table, so its failure decides nothing
    about what loads. It is still a real failure and is reported."""
    cascades, inert = cascading(["cross_source_verification"])
    assert (cascades, inert) == (set(), {"cross_source_verification"})


def test_a_failed_fact_does_not_cascade():
    """Nothing is built behind a fact."""
    cascades, inert = cascading(["fact_area_month_rent"])
    assert (cascades, inert) == (set(), {"fact_area_month_rent"})


def test_a_name_the_chain_does_not_carry_is_returned_not_raised():
    """A stage reads whatever failed. Raising there would stop a run over a name that
    decides nothing."""
    assert cascading(["something_else"])[1] == {"something_else"}


def test_bronze_and_silver_failures_for_one_source_collapse():
    """Both rows name the same source, and both cascade the same way."""
    assert cascading(["ons", "ons"])[0] == {"ons"}


def test_the_output_feeds_plan_directly():
    cascades, _ = cascading(["ons", "cross_source_verification"])
    assert plan(cascades)["facts"] == [
        "fact_lsoa_month_crime",
        "fact_lsoa_month_crime_total",
    ]


# --------------------------------------------------------------------------- #
# Planning
# --------------------------------------------------------------------------- #


def test_nothing_failing_runs_everything():
    assert len(ordered(plan())) == len(SOURCES) + len(DIMENSIONS) + len(FACTS)


def test_stages_run_silver_then_dimensions_then_facts():
    """Every dimension is written before any fact, so a fact never reads a dimension
    mid-write."""
    sequence = ordered(plan())
    last_source = max(sequence.index(source) for source in SOURCES)
    first_dim = min(sequence.index(dim) for dim in DIMENSIONS)
    last_dim = max(sequence.index(dim) for dim in DIMENSIONS)
    first_fact = min(sequence.index(fact) for fact in FACTS)
    assert last_source < first_dim
    assert last_dim < first_fact


def test_a_table_two_sources_reach_is_planned_once():
    """dim_lsoa is built from Doogal, Police and PPD. One stage per layer is what
    makes that one entry."""
    sequence = ordered(plan())
    assert sequence.count("dim_lsoa") == 1
    assert sequence.count("fact_area_month_price") == 1


def test_a_failed_source_drops_out_of_the_silver_stage():
    assert "ons" not in plan({"ons"})["silver"]


def test_the_surviving_sources_still_run():
    assert set(plan({"ons"})["silver"]) == set(SOURCES) - {"ons"}


def test_ons_failing_leaves_only_the_crime_lsoa_facts():
    """Everything behind dim_area waits. The two small-area crime facts sit behind
    dim_lsoa and dim_crime_type alone."""
    assert set(plan({"ons"})["facts"]) == {
        "fact_lsoa_month_crime",
        "fact_lsoa_month_crime_total",
    }


def test_doogal_failing_plans_no_facts_at_all():
    assert plan({"doogal"})["facts"] == []


def test_two_sources_failing_union_their_cascades():
    combined = plan({"ons", "police"})
    assert set(combined["dimensions"]) == {"dim_date"}
    assert combined["facts"] == []


def test_every_source_failing_plans_nothing():
    """A legitimate outcome, not an error. Each failure is already recorded against
    the source that caused it."""
    assert ordered(plan(set(SOURCES))) == []


def test_the_plan_preserves_declared_order():
    sequence = ordered(plan({"ons"}))
    assert sequence == [item for item in ordered(plan()) if item in set(sequence)]

# --------------------------------------------------------------------------- #
# What a check reads
# --------------------------------------------------------------------------- #


def test_the_check_declares_what_its_rules_require():
    """A check builds nothing, so the chain cannot derive its inputs. Declared here
    rather than in the notebook, where no test would reach them."""
    assert set(CHECK_INPUTS["cross_source_verification"]) == {
        "fact_area_month_price",
        "fact_area_month_hpi",
        "dim_area",
    }


def test_a_check_does_not_declare_what_only_its_measurements_read():
    """The reconciliation between the transaction series and the published index needs
    neither rent nor the calendar. Declaring them would close the whole check over a
    section that records nothing."""
    declared = set(CHECK_INPUTS["cross_source_verification"])
    assert not declared & {"fact_area_month_rent", "dim_date"}


def test_a_check_input_the_star_does_not_declare_is_refused(monkeypatch):
    """A gate on a name no plan can carry would never close, so the check would run
    against whatever happened to be in the table."""
    monkeypatch.setitem(CHECK_INPUTS, "cross_source_verification", ("fact_made_up",))
    with pytest.raises(DependencyError, match="gate could never close"):
        assert_chain_consistent()


def test_a_check_declaring_no_inputs_is_refused(monkeypatch):
    monkeypatch.setitem(CHECK_INPUTS, "cross_source_verification", ())
    with pytest.raises(DependencyError, match="no inputs"):
        assert_chain_consistent()


# --------------------------------------------------------------------------- #
# Reading a plan
# --------------------------------------------------------------------------- #


def test_running_tables_merges_both_gold_stages():
    """A Gold notebook asks whether one of its own tables is planned and does not
    care which stage the table belongs to."""
    assert running_tables(plan()) == set(DIMENSIONS) | set(FACTS)


def test_running_tables_drops_what_the_cascade_took():
    assert running_tables(plan({"doogal"})) == {"dim_date", "dim_crime_type"}


# --------------------------------------------------------------------------- #
# Naming the cause of a skip
# --------------------------------------------------------------------------- #


def test_blocked_by_names_the_failure_behind_a_skip():
    assert blocked_by("dim_lsoa", {"police"}) == ["police"]


def test_blocked_by_names_every_cause_not_only_the_first():
    """Two sources can put one fact in the skip set, and a row naming one of them
    sends the reader to the wrong place."""
    assert blocked_by("fact_area_month_price", {"hpi", "ppd"}) == ["hpi", "ppd"]


def test_blocked_by_is_empty_for_something_that_runs():
    """AuditRun.skip rejects an empty cause, so a gate skipping something the plan
    kept fails at the write instead of recording a skip nobody asked for."""
    assert blocked_by("fact_area_month_hpi", {"ppd"}) == []


def test_blocked_by_ignores_a_failure_that_cascades_nothing():
    """A stage reads back whatever failed. A failed fact or check is real and blocks
    nothing, so it is passed over rather than raised on."""
    assert blocked_by("dim_lsoa", {"ppd", "cross_source_verification"}) == ["ppd"]


# --------------------------------------------------------------------------- #
# Direct dependencies
# --------------------------------------------------------------------------- #


def test_a_source_depends_on_its_own_bronze_copy():
    """The Silver load waits on the file landing, which is a different run from its
    own even though both carry the same name."""
    assert depends_on("ons") == (("ons", "bronze"),)


def test_a_dimension_depends_on_the_sources_that_build_it():
    assert set(depends_on("dim_lsoa")) == {
        ("doogal", "silver"),
        ("police", "silver"),
        ("ppd", "silver"),
    }


def test_a_fact_depends_on_its_dimensions_and_not_on_what_is_behind_them():
    """Direct predecessors only. A dimension that succeeded in this run could not
    have done so had its sources failed, so repeating them adds nothing to a
    recorded reason and makes it harder to follow."""
    assert set(depends_on("fact_lsoa_month_crime")) == {
        ("dim_lsoa", "gold"),
        ("dim_crime_type", "gold"),
    }


def test_an_area_fact_depends_on_the_area_dimension_too():
    """The area crime facts are summed up through district ancestry, which the
    small-area ones never read."""
    assert ("dim_area", "gold") in depends_on("fact_area_month_crime")
    assert ("dim_area", "gold") not in depends_on("fact_lsoa_month_crime")


def test_a_check_depends_on_the_tables_it_reads():
    assert set(depends_on("cross_source_verification")) == {
        (table, "gold") for table in CHECK_INPUTS["cross_source_verification"]
    }


def test_no_fact_depends_on_the_calendar():
    """dim_date is conformance and not dependency. A fact written against a calendar
    that did not rebuild fails loudly at assert_keys_conform, which is the decision,
    so nothing here may quietly turn that into a skip."""
    assert not [fact for fact in FACTS if ("dim_date", "gold") in depends_on(fact)]


def test_every_dependency_carries_a_declared_layer():
    """The layer is not a property of the name: a source records at bronze and at
    silver, and a bronze success is not a Silver rebuild."""
    every = {
        pair
        for item in (*SOURCES, *DIMENSIONS, *FACTS, *CHECK_INPUTS)
        for pair in depends_on(item)
    }
    assert {layer for _, layer in every} <= {"bronze", "silver", "gold"}


def test_a_name_on_no_chain_has_no_dependencies_to_give():
    with pytest.raises(DependencyError, match="declares no inputs"):
        depends_on("dim_areaa")


# --------------------------------------------------------------------------- #
# The gate a stage actually calls
# --------------------------------------------------------------------------- #


def rebuilt(*missing: tuple[str, str]) -> set[tuple[str, str]]:
    """Every stage recorded as succeeded this run, less the ones named."""
    everything = (
        {(source, "bronze") for source in SOURCES}
        | {(source, "silver") for source in SOURCES}
        | {(table, "gold") for table in (*DIMENSIONS, *FACTS)}
    )
    return everything - set(missing)


def test_nothing_waits_on_a_clean_run():
    assert waiting_on("fact_lsoa_month_crime", set(), rebuilt()) == []
    assert waiting_on("cross_source_verification", set(), rebuilt()) == []


def test_a_failed_leaf_blocks_the_check_that_reads_it():
    """Nothing is built behind a fact, so a failed one is inert and stays in the
    plan. The check reading it would reconcile last month's table and write the
    result as a measurement."""
    failed = {"fact_area_month_price"}
    assert blocked_by("cross_source_verification", failed) == []
    assert waiting_on(
        "cross_source_verification", failed, rebuilt(("fact_area_month_price", "gold"))
    ) == ["fact_area_month_price (gold)"]


def test_a_dimension_that_never_ran_blocks_the_facts_behind_it():
    """01_load_dimensions writes four tables in one notebook. dim_area failing stops
    it before the last two, which then record nothing at all while the plan still
    keeps them."""
    failed = {"dim_area"}
    missing = (("dim_area", "gold"), ("dim_crime_type", "gold"), ("dim_lsoa", "gold"))
    assert "fact_lsoa_month_crime" in running_tables(plan(failed))
    assert waiting_on("fact_lsoa_month_crime", failed, rebuilt(*missing)) == [
        "dim_crime_type (gold)",
        "dim_lsoa (gold)",
    ]


def test_a_recorded_failure_is_named_without_a_layer():
    """The two readings are worth telling apart in the row: a bare name failed and
    said so, a name with a layer never ran and nothing reported it."""
    assert waiting_on("dim_lsoa", {"police"}, rebuilt(("police", "silver"))) == ["police"]


def test_a_cause_is_not_named_twice():
    """A failure that also leaves its dependency unrebuilt is one cause, not two."""
    causes = waiting_on(
        "dim_lsoa",
        {"police"},
        rebuilt(("police", "silver"), ("doogal", "silver")),
    )
    assert causes == ["doogal (silver)", "police"]


def test_a_bronze_success_does_not_satisfy_a_silver_dependency():
    """The same name records twice. Reading the pair as a name would let the file
    landing stand in for the table being rebuilt."""
    assert waiting_on("dim_area", set(), rebuilt(("hpi", "silver"))) == ["hpi (silver)"]


def test_a_source_waiting_on_its_own_bronze_copy_is_named_at_bronze():
    """The row being written already says boe, so the bare name would repeat what the
    reader can see and leave out the only part that means anything."""
    assert waiting_on("boe", {"boe"}, rebuilt(("boe", "bronze"))) == ["boe (bronze)"]


def test_a_missing_bronze_copy_is_named_at_bronze():
    """02 not running leaves no failure and no evidence, and the layer is what says
    which of the two runs behind this name is absent."""
    assert waiting_on("ons", set(), rebuilt(("ons", "bronze"))) == ["ons (bronze)"]


def test_a_hand_run_gates_on_the_chain_alone():
    """Outside a job there is no evidence to read, and treating its absence as unmet
    would make every notebook unrunnable by hand."""
    assert waiting_on("fact_lsoa_month_crime", set(), None) == []
    assert waiting_on("fact_area_month_hpi", {"hpi"}, None) == ["hpi"]
