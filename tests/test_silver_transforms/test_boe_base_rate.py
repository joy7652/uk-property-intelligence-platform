"""Tests for databricks_src/silver/transforms/boe_base_rate.py.

Covers the DQ guard (assert_rate_columns_consistent) and the SCD2 transform
(transform_boe_base_rate). The transform is pure, so every test builds a
small synthetic 'Raw Data' frame with RAW_DATA_SCHEMA and asserts on the
output -- no Excel, no Delta, no I/O.

Multi-row invariants the Delta CHECK constraints cannot express (exactly one
current row, contiguous non-overlapping intervals) are asserted here, per the
notebook's note that they belong in the test suite.
"""

from datetime import date, datetime
from decimal import Decimal

import pytest
from chispa import assert_df_equality
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_src.silver.transforms.boe_base_rate import (
    RAW_DATA_SCHEMA,
    assert_rate_columns_consistent,
    transform_boe_base_rate,
)

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

SOURCE_FILE = "/Volumes/uk_property_intel/bronze/boe/base_rate/baserate.xls"
INGESTION_TS = datetime(2026, 6, 12, 9, 30, 0)

SILVER_SCHEMA = StructType([
    StructField("effective_date", DateType(),        nullable=False),
    StructField("expiry_date",    DateType(),        nullable=True),
    StructField("is_current",     BooleanType(),     nullable=False),
    StructField("rate_pct",       DecimalType(6, 4), nullable=True),
    StructField("rate_type",      StringType(),      nullable=True),
    StructField("_source_file",   StringType(),      nullable=False),
    StructField("_ingestion_ts",  TimestampType(),   nullable=True),
])


def _dec(value):
    """str/None -> Decimal/None, quantised to the schema's (6, 4) scale."""
    return None if value is None else Decimal(value).quantize(Decimal("0.0001"))


def day(d, *, bank=None, mlr=None, band1=None, repo=None, official=None):
    """One synthetic 'Raw Data' row, columns in RAW_DATA_SCHEMA order."""
    return (d, _dec(bank), 0, _dec(mlr), _dec(band1), _dec(repo), _dec(official))


def raw(spark, rows):
    return spark.createDataFrame(rows, schema=RAW_DATA_SCHEMA)


def transform(spark, rows):
    return transform_boe_base_rate(
        raw_df=raw(spark, rows),
        source_file=SOURCE_FILE,
        ingestion_ts=INGESTION_TS,
    )


def silver_row(effective, expiry, current, rate, rate_type):
    return (
        effective,
        expiry,
        current,
        _dec(rate),
        rate_type,
        SOURCE_FILE,
        INGESTION_TS,
    )


# ---------------------------------------------------------------------------
# DQ guard: assert_rate_columns_consistent
# ---------------------------------------------------------------------------

class TestRateColumnConsistencyGuard:

    def test_passes_when_one_column_populated_per_row(self, spark):
        df = raw(spark, [
            day(date(1997, 5, 1), band1="6.0000"),
            day(date(1997, 5, 2), repo="6.2500"),
        ])
        assert_rate_columns_consistent(df)  # must not raise

    def test_passes_on_changeover_day_when_columns_agree(self, spark):
        # Two regimes populated on one day is legal iff the values match,
        # as on the real changeover days (1981-08-24, 1997-05-05).
        df = raw(spark, [
            day(date(1997, 5, 5), band1="6.2500", repo="6.2500"),
        ])
        assert_rate_columns_consistent(df)

    def test_raises_on_conflicting_values(self, spark):
        df = raw(spark, [
            day(date(1997, 5, 5), band1="6.2500", repo="6.5000"),
        ])
        with pytest.raises(ValueError, match="1 row\\(s\\)"):
            assert_rate_columns_consistent(df)

    def test_counts_every_conflicting_row(self, spark):
        df = raw(spark, [
            day(date(1997, 5, 5), band1="6.2500", repo="6.5000"),
            day(date(1997, 5, 6), repo="6.5000", official="6.7500"),
            day(date(1997, 5, 7), repo="6.7500"),
        ])
        with pytest.raises(ValueError, match="2 row\\(s\\)"):
            assert_rate_columns_consistent(df)

    def test_ignores_rows_with_no_rate_at_all(self, spark):
        # greatest/least of all-null is null; the guard must not trip on it.
        df = raw(spark, [
            day(date(1997, 5, 5)),
            day(date(1997, 5, 6), repo="6.2500"),
        ])
        assert_rate_columns_consistent(df)


# ---------------------------------------------------------------------------
# Transform: end-to-end scenario
# ---------------------------------------------------------------------------

class TestTransformScenario:
    """One synthetic series exercising every rule at once, asserted exactly.

    Day-by-day:
      01: MLR 9.00            -> event (left-censored series start)
      02: MLR 9.00            -> no event (unchanged)
      03: MLR 9.50            -> event (rate moved)
      04: Band1 9.50          -> NO event (regime relabel, rate unchanged)
      05: Band1 10.00         -> event (rate moved; Band1 now the label)
      06: Repo+Official 10.50 -> event; changeover day, both columns agree,
                                 the newer regime supplies rate_type
      07: Official 10.50      -> no event (unchanged)
    """

    ROWS = [
        day(date(1973, 1, 1), mlr="9.0000"),
        day(date(1973, 1, 2), mlr="9.0000"),
        day(date(1973, 1, 3), mlr="9.5000"),
        day(date(1973, 1, 4), band1="9.5000"),
        day(date(1973, 1, 5), band1="10.0000"),
        day(date(1973, 1, 6), repo="10.5000", official="10.5000"),
        day(date(1973, 1, 7), official="10.5000"),
    ]

    def test_matches_expected_scd2_output_exactly(self, spark):
        expected = spark.createDataFrame(
            [
                silver_row(date(1973, 1, 1), date(1973, 1, 2), False, "9.0000",
                           "Minimum Lending Rate"),
                silver_row(date(1973, 1, 3), date(1973, 1, 4), False, "9.5000",
                           "Minimum Lending Rate"),
                silver_row(date(1973, 1, 5), date(1973, 1, 5), False, "10.0000",
                           "Minimum Band 1 Dealing Rate"),
                silver_row(date(1973, 1, 6), None, True, "10.5000",
                           "Official Bank Rate"),
            ],
            schema=SILVER_SCHEMA,
        )
        assert_df_equality(
            transform(spark, self.ROWS),
            expected,
            ignore_nullable=True,
            ignore_row_order=True,
        )

    def test_relabel_day_stays_inside_prior_interval(self, spark):
        # The 9.5 interval runs 03 -> 04: the relabel day extends the prior
        # event's validity rather than opening a new one, and rate_type keeps
        # the regime in effect at effective_date.
        row = (
            transform(spark, self.ROWS)
            .filter("effective_date = DATE '1973-01-03'")
            .collect()[0]
        )
        assert row.expiry_date == date(1973, 1, 4)
        assert row.rate_type == "Minimum Lending Rate"

    def test_changeover_day_takes_newer_regime_label(self, spark):
        row = (
            transform(spark, self.ROWS)
            .filter("is_current = true")
            .collect()[0]
        )
        assert row.rate_type == "Official Bank Rate"
        assert row.rate_pct == _dec("10.5000")


# ---------------------------------------------------------------------------
# Transform: SCD2 multi-row invariants (per the notebook, these live here)
# ---------------------------------------------------------------------------

class TestScd2Invariants:

    @pytest.fixture()
    def output(self, spark):
        rows = TestTransformScenario.ROWS
        return (
            transform(spark, rows)
            .orderBy("effective_date")
            .collect()
        )

    def test_exactly_one_current_row(self, output):
        assert sum(1 for r in output if r.is_current) == 1

    def test_only_the_current_row_has_open_expiry(self, output):
        for r in output:
            assert r.is_current == (r.expiry_date is None)

    def test_intervals_are_contiguous_and_non_overlapping(self, output):
        # Each closed interval must end exactly one day before the next opens:
        # no gaps, no overlaps, anywhere in the chain.
        for prev, nxt in zip(output, output[1:]):
            assert (nxt.effective_date - prev.expiry_date).days == 1

    def test_first_event_is_series_start(self, output):
        # Left-censored by design: the first effective_date is the start of
        # the 'Raw Data' series, kept even though no prior rate exists.
        assert output[0].effective_date == date(1973, 1, 1)


# ---------------------------------------------------------------------------
# Transform: edge behaviour
# ---------------------------------------------------------------------------

class TestTransformEdges:

    def test_single_day_series_yields_one_open_row(self, spark):
        out = transform(spark, [day(date(2026, 1, 1), official="3.7500")])
        expected = spark.createDataFrame(
            [silver_row(date(2026, 1, 1), None, True, "3.7500",
                        "Official Bank Rate")],
            schema=SILVER_SCHEMA,
        )
        assert_df_equality(out, expected, ignore_nullable=True)

    def test_days_with_no_rate_are_dropped_without_phantom_events(self, spark):
        # A null-rate gap mid-series must not split one rate level into two
        # events: lag() sees the last non-null day across the gap.
        out = transform(spark, [
            day(date(2020, 3, 1), official="0.2500"),
            day(date(2020, 3, 2)),                      # no rate published
            day(date(2020, 3, 3), official="0.2500"),
            day(date(2020, 3, 4), official="0.1000"),
        ])
        assert out.count() == 2
        assert [r.rate_pct for r in out.orderBy("effective_date").collect()] \
            == [_dec("0.2500"), _dec("0.1000")]

    def test_sixteenth_precision_survives_exactly(self, spark):
        # Repo-era rates quoted in sixteenths; (6, 4) decimals must carry
        # 5.9375 unrounded, and change detection must treat 5.9375 vs 5.94
        # as a genuine move, not noise.
        out = transform(spark, [
            day(date(1998, 6, 4), repo="5.9375"),
            day(date(1998, 6, 5), repo="5.9400"),
        ]).orderBy("effective_date").collect()
        assert len(out) == 2
        assert out[0].rate_pct == Decimal("5.9375")
        assert out[1].rate_pct == Decimal("5.9400")

    def test_lineage_stamped_on_every_row(self, spark):
        out = transform(spark, TestTransformScenario.ROWS).collect()
        assert all(r._source_file == SOURCE_FILE for r in out)
        assert all(r._ingestion_ts == INGESTION_TS for r in out)
