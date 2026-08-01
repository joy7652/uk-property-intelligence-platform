"""Tests for the BoE base rate Bronze to Silver transform.

Covers the DQ guard (assert_rate_columns_consistent) and the SCD2 transform
(transform_boe_base_rate). The transform is pure, so every test builds a small
synthetic 'Raw Data' frame with RAW_DATA_SCHEMA and asserts on the output. No Excel,
no Delta, no I/O.

Multi-row invariants the Delta CHECK constraints cannot express (exactly one current
row, contiguous non-overlapping intervals) are asserted here, per the notebook's note
that they belong in the test suite.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from chispa import assert_df_equality
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_src.silver.transforms.boe_base_rate import (
    RATE_COLUMNS,
    RAW_DATA_SCHEMA,
    SILVER_COLUMNS,
    assert_rate_columns_consistent,
    transform_boe_base_rate,
)

SOURCE_FILE = "/Volumes/uk_property_intel/bronze/boe/base_rate/baserate.xls"
INGESTION_TS = datetime(2026, 6, 12, 9, 30, 0)

SILVER_SCHEMA = StructType(
    [
        StructField("effective_date", DateType(), nullable=False),
        StructField("expiry_date", DateType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=False),
        StructField("rate_pct", DecimalType(6, 4), nullable=True),
        StructField("rate_type", StringType(), nullable=True),
        StructField("_source_file", StringType(), nullable=False),
        StructField("_ingestion_ts", TimestampType(), nullable=True),
    ]
)

# Adjacent regime pairs, derived from the precedence order itself so a new regime
# extends the coverage rather than needing a new case.
ADJACENT_REGIMES = [
    (RATE_COLUMNS[i], RATE_COLUMNS[i + 1]) for i in range(len(RATE_COLUMNS) - 1)
]


def _dec(value: str | None) -> Decimal | None:
    """str or None to Decimal or None, quantised to the schema's (6, 4) scale."""
    return None if value is None else Decimal(value).quantize(Decimal("0.0001"))


def day(when: date, **rates: str | None) -> tuple:
    """One synthetic 'Raw Data' row, in RAW_DATA_SCHEMA order.

    Keywords are the rate column names, so a test names a regime the same way the
    transform does.
    """
    values: dict[str, str | None] = {name: None for name, _ in RATE_COLUMNS}
    values.update(rates)
    return (
        when,
        _dec(values["bank_rate"]),
        0,
        _dec(values["min_lending_rate"]),
        _dec(values["min_band_1_dealing_rate"]),
        _dec(values["repo_rate"]),
        _dec(values["official_bank_rate"]),
    )


def raw(spark, rows: list[tuple]):
    return spark.createDataFrame(rows, schema=RAW_DATA_SCHEMA)


def transform(spark, rows: list[tuple]):
    return transform_boe_base_rate(
        raw_df=raw(spark, rows),
        source_file=SOURCE_FILE,
        ingestion_ts=INGESTION_TS,
    )


def silver_row(effective, expiry, current, rate, rate_type) -> tuple:
    return (effective, expiry, current, _dec(rate), rate_type, SOURCE_FILE, INGESTION_TS)


# One synthetic series exercising every rule at once.
#
#   01: MLR 9.00            -> event (left-censored series start)
#   02: MLR 9.00            -> no event (unchanged)
#   03: MLR 9.50            -> event (rate moved)
#   04: Band1 9.50          -> NO event (regime relabel, rate unchanged)
#   05: Band1 10.00         -> event (rate moved; Band1 now the label)
#   06: Repo+Official 10.50 -> event; changeover day, both columns agree, the newer
#                              regime supplies rate_type
#   07: Official 10.50      -> no event (unchanged)
SCENARIO_ROWS = [
    day(date(1973, 1, 1), min_lending_rate="9.0000"),
    day(date(1973, 1, 2), min_lending_rate="9.0000"),
    day(date(1973, 1, 3), min_lending_rate="9.5000"),
    day(date(1973, 1, 4), min_band_1_dealing_rate="9.5000"),
    day(date(1973, 1, 5), min_band_1_dealing_rate="10.0000"),
    day(date(1973, 1, 6), repo_rate="10.5000", official_bank_rate="10.5000"),
    day(date(1973, 1, 7), official_bank_rate="10.5000"),
]


@pytest.fixture()
def scenario_output(spark):
    return transform(spark, SCENARIO_ROWS).orderBy("effective_date").collect()


# --------------------------------------------------------------------------- #
# Read contract
# --------------------------------------------------------------------------- #


def test_raw_data_schema_matches_the_sheet_layout():
    """The spark-excel read is positional, so a reordered schema loads values into
    the wrong columns and every other test would agree with the mistake."""
    assert [(f.name, f.dataType) for f in RAW_DATA_SCHEMA.fields] == [
        ("date", DateType()),
        ("bank_rate", DecimalType(6, 4)),
        ("zero_line", IntegerType()),
        ("min_lending_rate", DecimalType(6, 4)),
        ("min_band_1_dealing_rate", DecimalType(6, 4)),
        ("repo_rate", DecimalType(6, 4)),
        ("official_bank_rate", DecimalType(6, 4)),
    ]


def test_output_column_order_is_canonical(spark):
    """INSERT OVERWRITE matches on position, so a reordered projection would load
    values into the wrong table columns without failing."""
    assert tuple(transform(spark, SCENARIO_ROWS).columns) == SILVER_COLUMNS


# --------------------------------------------------------------------------- #
# DQ guard: assert_rate_columns_consistent
# --------------------------------------------------------------------------- #


def test_passes_when_one_column_populated_per_row(spark):
    df = raw(
        spark,
        [
            day(date(1997, 5, 1), min_band_1_dealing_rate="6.0000"),
            day(date(1997, 5, 2), repo_rate="6.2500"),
        ],
    )
    assert_rate_columns_consistent(df)


def test_passes_on_changeover_day_when_columns_agree(spark):
    # Two regimes on one day is legal iff the values match, as on the real
    # changeover days (1981-08-24, 1997-05-05).
    df = raw(
        spark,
        [day(date(1997, 5, 5), min_band_1_dealing_rate="6.2500", repo_rate="6.2500")],
    )
    assert_rate_columns_consistent(df)


def test_raises_on_conflicting_values(spark):
    df = raw(
        spark,
        [day(date(1997, 5, 5), min_band_1_dealing_rate="6.2500", repo_rate="6.5000")],
    )
    with pytest.raises(ValueError, match="1 row\\(s\\)"):
        assert_rate_columns_consistent(df)


def test_counts_every_conflicting_row(spark):
    df = raw(
        spark,
        [
            day(date(1997, 5, 5), min_band_1_dealing_rate="6.2500", repo_rate="6.5000"),
            day(date(1997, 5, 6), repo_rate="6.5000", official_bank_rate="6.7500"),
            day(date(1997, 5, 7), repo_rate="6.7500"),
        ],
    )
    with pytest.raises(ValueError, match="2 row\\(s\\)"):
        assert_rate_columns_consistent(df)


def test_ignores_rows_with_no_rate_at_all(spark):
    # greatest/least of all-null is null; the guard must not trip on it.
    df = raw(spark, [day(date(1997, 5, 5)), day(date(1997, 5, 6), repo_rate="6.2500")])
    assert_rate_columns_consistent(df)


# --------------------------------------------------------------------------- #
# Regime precedence
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "newer, older",
    ADJACENT_REGIMES,
    ids=[newer[0] for newer, _ in ADJACENT_REGIMES],
)
def test_rate_column_precedence_is_newest_first(spark, newer, older):
    """A day carrying two regimes at one rate takes the newer label.

    Precedence lives in RATE_COLUMNS order alone. Reordering it would relabel history
    without moving a rate or changing a row count, so nothing else would notice.
    """
    newer_column, newer_label = newer
    older_column, _ = older
    rates = {newer_column: "6.0000", older_column: "6.0000"}
    assert transform(spark, [day(date(1997, 5, 5), **rates)]).collect()[0].rate_type == (
        newer_label
    )


def test_zero_line_never_supplies_a_rate(spark):
    """zero_line is chart scaffolding, and it sits between the rate columns in the
    sheet. Including it in RATE_COLUMNS would turn every unpublished day into a
    zero-rate event."""
    out = transform(
        spark,
        [day(date(2020, 3, 1)), day(date(2020, 3, 2), official_bank_rate="0.2500")],
    ).collect()
    assert len(out) == 1
    assert out[0].rate_pct == _dec("0.2500")


# --------------------------------------------------------------------------- #
# Transform: end-to-end scenario
# --------------------------------------------------------------------------- #


def test_matches_expected_scd2_output_exactly(spark):
    expected = spark.createDataFrame(
        [
            silver_row(
                date(1973, 1, 1), date(1973, 1, 2), False, "9.0000",
                "Minimum Lending Rate",
            ),
            silver_row(
                date(1973, 1, 3), date(1973, 1, 4), False, "9.5000",
                "Minimum Lending Rate",
            ),
            silver_row(
                date(1973, 1, 5), date(1973, 1, 5), False, "10.0000",
                "Minimum Band 1 Dealing Rate",
            ),
            silver_row(date(1973, 1, 6), None, True, "10.5000", "Official Bank Rate"),
        ],
        schema=SILVER_SCHEMA,
    )
    assert_df_equality(
        transform(spark, SCENARIO_ROWS),
        expected,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_relabel_day_stays_inside_prior_interval(spark):
    # The 9.5 interval runs 03 to 04: the relabel day extends the prior event's
    # validity rather than opening a new one, and rate_type keeps the regime in
    # effect at effective_date.
    row = (
        transform(spark, SCENARIO_ROWS)
        .filter("effective_date = DATE '1973-01-03'")
        .collect()[0]
    )
    assert row.expiry_date == date(1973, 1, 4)
    assert row.rate_type == "Minimum Lending Rate"


def test_changeover_day_takes_newer_regime_label(spark):
    row = transform(spark, SCENARIO_ROWS).filter("is_current = true").collect()[0]
    assert row.rate_type == "Official Bank Rate"
    assert row.rate_pct == _dec("10.5000")


def test_input_row_order_does_not_change_the_output(spark):
    """The window sorts by date, so a sheet that arrives unordered must collapse to
    the same events. Relying on input order would pass on real data and fail on a
    re-exported workbook."""
    assert_df_equality(
        transform(spark, list(reversed(SCENARIO_ROWS))),
        transform(spark, SCENARIO_ROWS),
        ignore_nullable=True,
        ignore_row_order=True,
    )


# --------------------------------------------------------------------------- #
# Transform: SCD2 multi-row invariants
# --------------------------------------------------------------------------- #


def test_exactly_one_current_row(scenario_output):
    assert sum(1 for row in scenario_output if row.is_current) == 1


def test_only_the_current_row_has_open_expiry(scenario_output):
    for row in scenario_output:
        assert row.is_current == (row.expiry_date is None)


def test_intervals_are_contiguous_and_non_overlapping(scenario_output):
    # Each closed interval must end exactly one day before the next opens: no gaps,
    # no overlaps, anywhere in the chain.
    for prev, nxt in zip(scenario_output, scenario_output[1:]):
        assert (nxt.effective_date - prev.expiry_date).days == 1


def test_first_event_is_series_start(scenario_output):
    # Left-censored by design: the first effective_date is the start of the 'Raw
    # Data' series, kept even though no prior rate exists.
    assert scenario_output[0].effective_date == date(1973, 1, 1)


# --------------------------------------------------------------------------- #
# Transform: edge behaviour
# --------------------------------------------------------------------------- #


def test_single_day_series_yields_one_open_row(spark):
    out = transform(spark, [day(date(2026, 1, 1), official_bank_rate="3.7500")])
    expected = spark.createDataFrame(
        [silver_row(date(2026, 1, 1), None, True, "3.7500", "Official Bank Rate")],
        schema=SILVER_SCHEMA,
    )
    assert_df_equality(out, expected, ignore_nullable=True)


def test_days_with_no_rate_are_dropped_without_phantom_events(spark):
    # A null-rate gap mid-series must not split one rate level into two events:
    # lag() sees the last non-null day across the gap.
    out = transform(
        spark,
        [
            day(date(2020, 3, 1), official_bank_rate="0.2500"),
            day(date(2020, 3, 2)),
            day(date(2020, 3, 3), official_bank_rate="0.2500"),
            day(date(2020, 3, 4), official_bank_rate="0.1000"),
        ],
    )
    assert out.count() == 2
    assert [r.rate_pct for r in out.orderBy("effective_date").collect()] == [
        _dec("0.2500"),
        _dec("0.1000"),
    ]


def test_series_starting_with_null_rates_begins_at_first_published_day(spark):
    """A leading gap must not produce an event dated before any rate existed."""
    out = transform(
        spark,
        [
            day(date(1973, 1, 1)),
            day(date(1973, 1, 2)),
            day(date(1973, 1, 3), min_lending_rate="9.0000"),
        ],
    ).collect()
    assert len(out) == 1
    assert out[0].effective_date == date(1973, 1, 3)


def test_rate_returning_to_a_previous_level_is_a_new_event(spark):
    """Change detection compares against the previous day, not against every level
    seen before, so a rate that comes back opens a fresh interval."""
    out = (
        transform(
            spark,
            [
                day(date(2020, 1, 1), official_bank_rate="0.7500"),
                day(date(2020, 1, 2), official_bank_rate="0.2500"),
                day(date(2020, 1, 3), official_bank_rate="0.7500"),
            ],
        )
        .orderBy("effective_date")
        .collect()
    )
    assert [r.rate_pct for r in out] == [
        _dec("0.7500"),
        _dec("0.2500"),
        _dec("0.7500"),
    ]


def test_sixteenth_precision_survives_exactly(spark):
    # Repo-era rates quoted in sixteenths; (6, 4) decimals must carry 5.9375
    # unrounded, and change detection must treat 5.9375 vs 5.94 as a genuine move.
    out = (
        transform(
            spark,
            [
                day(date(1998, 6, 4), repo_rate="5.9375"),
                day(date(1998, 6, 5), repo_rate="5.9400"),
            ],
        )
        .orderBy("effective_date")
        .collect()
    )
    assert len(out) == 2
    assert out[0].rate_pct == Decimal("5.9375")
    assert out[1].rate_pct == Decimal("5.9400")


def test_lineage_stamped_on_every_row(spark):
    out = transform(spark, SCENARIO_ROWS).collect()
    assert all(row._source_file == SOURCE_FILE for row in out)
    assert all(row._ingestion_ts == INGESTION_TS for row in out)
