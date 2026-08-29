"""Tests for the police street-level crime Bronze to Silver transform.

Two halves. The archive selection functions are plain Python over member names and
need no Spark; they carry the dedup rule, which is the only thing standing between
overlapping snapshots and every month being loaded several times. The transform is
pure, so every other test builds a small synthetic frame in FRAME_COLUMNS and asserts
on the output. No ZIP, no Delta, no I/O.

The source has no natural key, so there is no uniqueness assertion to test. What is
tested instead is that the duplicate-measurement helpers count what they claim to.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime
from decimal import Decimal

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_src.silver.transforms.police import (
    COLUMN_MAP,
    DERIVED_COLUMNS,
    DOMAINS,
    FRAME_COLUMNS,
    LATITUDE_RANGE,
    LINEAGE_COLUMNS,
    LONGITUDE_RANGE,
    MEMBER_PATH,
    RULES,
    SILVER_COLUMNS,
    SOURCE_COLUMNS,
    TYPED_COLUMNS,
    VOCABULARY_COLUMNS,
    assert_snapshot_label,
    check_rules,
    coordinate_box_check,
    crime_id_month_spread,
    crime_type_check,
    identical_row_duplicates,
    measures,
    parse_archive_member,
    select_newest,
    shape_police,
    silver_table_ddl,
    transform_police,
    unusable_members,
)

SOURCE_FILE = "/Volumes/uk_property_intel/bronze/police/crime/yearly/2017/police-crime-2017-12.zip"
SNAPSHOT = "2017-12"
INGESTION_TS = datetime(2026, 8, 5, 9, 30, 0)

STAGE = "file:///local_disk0/police_stage"

RAW_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in FRAME_COLUMNS]
)


def path_for(month: str, force: str = "kent", file_month: str | None = None) -> str:
    """A staged member path. file_month differs from month only in the guard tests."""
    return f"{STAGE}/{month}/{file_month or month}-{force}-street.csv"


DEFAULTS: dict[str, str | None] = {
    "Crime ID": None,
    "Month": "2015-06",
    "Reported by": "Kent Police",
    "Falls within": "Kent Police",
    "Longitude": "0.521830",
    "Latitude": "51.279430",
    "Location": "On or near Supermarket",
    "LSOA code": "E01024005",
    "LSOA name": "Ashford 010A",
    "Crime type": "Anti-social behaviour",
    "Last outcome category": None,
    "Context": None,
    MEMBER_PATH: path_for("2015-06"),
}


def raw_row(**overrides: str | None) -> dict[str, str | None]:
    """One source row. Anti-social behaviour by default, which is the case with no
    crime id and no outcome."""
    row = dict(DEFAULTS)
    row.update(overrides)
    return row


def crime_row(**overrides: str | None) -> dict[str, str | None]:
    """A recorded crime rather than anti-social behaviour: it carries an id and an
    outcome."""
    return raw_row(
        **{
            "Crime ID": "a" * 64,
            "Crime type": "Burglary",
            "Last outcome category": "Under investigation",
            **overrides,
        }
    )


def raw(spark, rows: list[dict[str, str | None]]):
    return spark.createDataFrame(
        [[row[name] for name in FRAME_COLUMNS] for row in rows], RAW_SCHEMA
    )


def transform(spark, rows: list[dict[str, str | None]], snapshot: str = SNAPSHOT):
    return transform_police(
        raw_df=raw(spark, rows),
        source_file=SOURCE_FILE,
        snapshot=snapshot,
        ingestion_ts=INGESTION_TS,
    )


# --------------------------------------------------------------------------- #
# Archive selection
# --------------------------------------------------------------------------- #


def test_member_name_yields_month_force_and_dataset():
    assert parse_archive_member("2015-06/2015-06-kent-street.csv") == {
        "month": "2015-06",
        "force": "kent",
        "dataset": "street",
        "month_agrees": True,
    }


def test_hyphenated_force_is_not_split_by_the_dataset_suffix():
    """The force segment carries hyphens and so does stop-and-search. A split on
    hyphen would take 'search' as the dataset and truncate the force."""
    parsed = parse_archive_member(
        "2019-01/2019-01-devon-and-cornwall-stop-and-search.csv"
    )
    assert parsed["force"] == "devon-and-cornwall"
    assert parsed["dataset"] == "stop-and-search"


def test_top_level_folder_inside_the_archive_still_parses():
    parsed = parse_archive_member("archive/2019-01/2019-01-btp-street.csv")
    assert parsed["month"] == "2019-01" and parsed["force"] == "btp"


@pytest.mark.parametrize(
    "name",
    ["readme.txt", "2019-01/2019-01-kent-street.txt", "2019-01-kent-street.csv", ""],
)
def test_names_outside_the_convention_do_not_parse(name):
    assert parse_archive_member(name) is None


def test_disagreeing_months_are_flagged_rather_than_resolved():
    parsed = parse_archive_member("2016-02/2016-01-kent-street.csv")
    assert parsed["month_agrees"] is False


def test_newest_snapshot_supplies_a_shared_slot():
    """The whole overlap strategy rests on this. Every archive restates up to 36
    months, so most slots appear several times and only the latest is read."""
    listings = {
        "2015-12": ["2015-06/2015-06-kent-street.csv"],
        "2017-12": ["2015-06/2015-06-kent-street.csv"],
    }
    assert select_newest(listings)[("2015-06", "kent")][0] == "2017-12"


def test_a_slot_only_one_archive_holds_is_kept():
    listings = {
        "2015-12": ["2010-12/2010-12-kent-street.csv"],
        "2017-12": ["2016-01/2016-01-kent-street.csv"],
    }
    assert set(select_newest(listings)) == {
        ("2010-12", "kent"),
        ("2016-01", "kent"),
    }


def test_other_datasets_are_not_selected():
    listings = {
        "2017-12": [
            "2016-01/2016-01-kent-street.csv",
            "2016-01/2016-01-kent-outcomes.csv",
            "2016-01/2016-01-kent-stop-and-search.csv",
        ]
    }
    assert set(select_newest(listings)) == {("2016-01", "kent")}


def test_selection_does_not_depend_on_listing_order():
    """Dict iteration order must not decide a winner: the tiebreak is the snapshot
    label alone."""
    listings = {
        "2015-12": ["2015-06/2015-06-kent-street.csv"],
        "2017-12": ["2015-06/2015-06-kent-street.csv"],
        "2019-12": ["2015-06/2015-06-kent-street.csv"],
    }
    reordered = {key: listings[key] for key in reversed(list(listings))}
    assert select_newest(reordered) == select_newest(listings)
    assert select_newest(listings)[("2015-06", "kent")][0] == "2019-12"


def test_ambiguous_month_is_not_selected():
    listings = {"2017-12": ["2016-02/2016-01-kent-street.csv"]}
    assert select_newest(listings) == {}


def test_unusable_members_reports_both_kinds():
    listings = {
        "2015-12": ["readme.txt", "2015-06/2015-06-kent-street.csv"],
        "2017-12": ["2016-02/2016-01-kent-street.csv"],
    }
    assert unusable_members(listings) == {
        "2015-12": ["readme.txt"],
        "2017-12": ["2016-02/2016-01-kent-street.csv"],
    }


def test_a_clean_listing_reports_nothing_unusable():
    assert unusable_members({"2017-12": ["2016-01/2016-01-kent-street.csv"]}) == {}


def test_ambiguous_month_on_another_dataset_is_not_reported():
    """Only the dataset being loaded matters. An outcomes file with disagreeing
    months would abort a load that never reads it."""
    assert unusable_members({"2017-12": ["2016-02/2016-01-kent-outcomes.csv"]}) == {}


# --------------------------------------------------------------------------- #
# Read and write contract
# --------------------------------------------------------------------------- #


def test_column_map_targets_are_unique():
    assert len(set(COLUMN_MAP.values())) == len(COLUMN_MAP)


def test_typed_columns_cover_the_map_and_the_derived_set():
    """A column in neither would have no cast expression and would fail at select
    time; one in both would mean the source is being overwritten by a derivation."""
    assert set(TYPED_COLUMNS) == set(COLUMN_MAP.values()) | set(DERIVED_COLUMNS)
    assert not set(COLUMN_MAP.values()) & set(DERIVED_COLUMNS)


def test_table_ddl_matches_the_transform_output(spark):
    """The DDL and the cast expressions each declare a type. Drift between them would
    load values into a column that truncates or nulls them on write."""
    declared = spark.createDataFrame(
        [], schema=silver_table_ddl().replace(" NOT NULL", "")
    ).schema
    produced = transform(spark, [raw_row()]).schema
    assert [(f.name, f.dataType) for f in declared.fields] == [
        (f.name, f.dataType) for f in produced.fields
    ]


def test_crime_type_check_covers_every_configured_value():
    """The CHECK constraint and the transform guard read the same set, so a value
    accepted by one cannot be rejected by the other."""
    check = crime_type_check()
    for value in DOMAINS["crime_type"]:
        assert f"'{value}'" in check
    assert check.count("'") == 2 * len(DOMAINS["crime_type"])


# --------------------------------------------------------------------------- #
# Source column guard
# --------------------------------------------------------------------------- #


def test_expected_column_set_passes(spark):
    assert transform(spark, [raw_row()]).count() == 1


def test_missing_source_column_raises(spark):
    df = raw(spark, [raw_row()]).drop("Context")
    with pytest.raises(ValueError, match="Context"):
        transform_police(df, SOURCE_FILE, SNAPSHOT, INGESTION_TS)


def test_missing_member_path_raises(spark):
    """Without it the load has no month and no force: both live only in the path."""
    df = raw(spark, [raw_row()]).drop(MEMBER_PATH)
    with pytest.raises(ValueError, match=MEMBER_PATH):
        transform_police(df, SOURCE_FILE, SNAPSHOT, INGESTION_TS)


def test_unexpected_source_column_raises(spark):
    df = raw(spark, [raw_row()]).withColumn("Outcome date", F.lit("2015-07"))
    with pytest.raises(ValueError, match="Outcome date"):
        transform_police(df, SOURCE_FILE, SNAPSHOT, INGESTION_TS)


# --------------------------------------------------------------------------- #
# Member path
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "path",
    [
        f"{STAGE}/2015-06/2015-06-kent-outcomes.csv",
        f"{STAGE}/2015-06/kent-street.csv",
        f"{STAGE}/2015-06-kent-street.csv",
        "",
    ],
)
def test_unparseable_member_path_raises(spark, path):
    """regexp_extract yields an empty string rather than null on no match, so an
    unparseable path would otherwise give a blank force its own group."""
    with pytest.raises(ValueError, match="member_path_parses"):
        transform(spark, [raw_row(**{MEMBER_PATH: path})])


def test_folder_and_filename_month_disagree_raises(spark):
    with pytest.raises(ValueError, match="month_consistent"):
        transform(
            spark,
            [
                raw_row(
                    Month="2015-06",
                    **{MEMBER_PATH: path_for("2015-07", file_month="2015-06")},
                )
            ],
        )


def test_month_column_disagreeing_with_the_path_raises(spark):
    """A file whose rows belong to another month would pass every other guard and
    land under the wrong partition."""
    with pytest.raises(ValueError, match="month_consistent"):
        transform(spark, [raw_row(Month="2015-05")])


def test_null_month_column_raises(spark):
    with pytest.raises(ValueError, match="month_consistent"):
        transform(spark, [raw_row(Month=None)])


# --------------------------------------------------------------------------- #
# Domains
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "crime_type",
    ["Anti-social behaviour", "Violence and sexual offences", "Bicycle theft"],
)
def test_current_crime_type_passes(spark, crime_type):
    assert transform(spark, [raw_row(**{"Crime type": crime_type})]).count() == 1


@pytest.mark.parametrize(
    "crime_type", ["Violent crime", "Public disorder and weapons"]
)
def test_historical_crime_type_passes(spark, crime_type):
    """The Home Office renamed and split categories in 2013 and the older months keep
    the vocabulary of their day, so both eras have to load."""
    assert transform(spark, [raw_row(**{"Crime type": crime_type})]).count() == 1


def test_domain_holds_only_observed_categories():
    """The set was seeded from documentation and trimmed to what a full load actually
    carried. A category the publisher never emits widens the CHECK for nothing."""
    assert len(DOMAINS["crime_type"]) == 16
    assert "Other burglary" not in DOMAINS["crime_type"]


def test_unknown_crime_type_raises_naming_the_value(spark):
    with pytest.raises(ValueError, match="Cyber fraud"):
        transform(spark, [raw_row(**{"Crime type": "Cyber fraud"})])


def test_null_crime_type_raises(spark):
    with pytest.raises(ValueError, match="crime_type_known"):
        transform(spark, [raw_row(**{"Crime type": None})])


def test_outcome_category_is_not_guarded(spark):
    """Court-result vocabulary has changed repeatedly across sixteen years, so a
    fixed set would abort on a rename rather than on a fault."""
    row = transform(
        spark, [crime_row(**{"Last outcome category": "Some 2027 category"})]
    ).collect()[0]
    assert row["last_outcome_category"] == "Some 2027 category"


# --------------------------------------------------------------------------- #
# Coordinates
# --------------------------------------------------------------------------- #


def test_unlocated_sentinel_becomes_null(spark):
    """(0, 0) is what the publisher writes when no map point sits within 20km. Kept
    as a value it would place the crime in the Gulf of Guinea."""
    row = transform(spark, [raw_row(Longitude="0.000000", Latitude="0.000000")]).collect()[0]
    assert row["longitude"] is None
    assert row["latitude"] is None


def test_greenwich_longitude_survives(spark):
    """Longitude zero alone is the prime meridian and a real UK location. Only the
    pair is the sentinel."""
    row = transform(spark, [raw_row(Longitude="0.000000", Latitude="51.477928")]).collect()[0]
    assert row["longitude"] == Decimal("0.000000")
    assert row["latitude"] == Decimal("51.477928")


def test_one_coordinate_without_the_other_raises(spark):
    with pytest.raises(ValueError, match="coordinates_paired"):
        transform(spark, [raw_row(Longitude=None)])


def test_both_coordinates_absent_passes(spark):
    row = transform(spark, [raw_row(Longitude=None, Latitude=None)]).collect()[0]
    assert row["longitude"] is None and row["latitude"] is None


def test_malformed_coordinate_raises_naming_the_value(spark):
    """A count comparison between the raw and typed frames can only say a column lost
    values. The predicate form reports the string that failed."""
    with pytest.raises(ValueError, match="not a coordinate"):
        transform(spark, [raw_row(Longitude="not a coordinate")])


def test_negative_longitude_survives_typing(spark):
    row = transform(spark, [raw_row(Longitude="-2.575411", Latitude="54.991255")]).collect()[0]
    assert row["longitude"] == Decimal("-2.575411")


# --------------------------------------------------------------------------- #
# Typing
# --------------------------------------------------------------------------- #


def test_output_schema_types(spark):
    schema = {f.name: f.dataType for f in transform(spark, [raw_row()]).schema}
    assert schema["crime_id"] == StringType()
    assert schema["crime_month"] == DateType()
    assert schema["snapshot_month"] == DateType()
    assert schema["crime_year"] == IntegerType()
    assert schema["force"] == StringType()
    assert schema["longitude"] == DecimalType(9, 6)
    assert schema["latitude"] == DecimalType(9, 6)
    assert schema["_source_file"] == StringType()
    assert schema["_ingestion_ts"] == TimestampType()


def test_output_column_order_is_canonical(spark):
    """INSERT OVERWRITE matches on position, so a reordered projection would load
    values into the wrong table columns without failing."""
    assert tuple(transform(spark, [raw_row()]).columns) == SILVER_COLUMNS


def test_no_source_column_names_survive(spark):
    result = transform(spark, [raw_row()])
    assert not set(result.columns) & set(SOURCE_COLUMNS)
    assert MEMBER_PATH not in result.columns


def test_month_parsed_to_the_first_of_the_month(spark):
    assert transform(spark, [raw_row()]).collect()[0]["crime_month"] == dt.date(2015, 6, 1)


def test_crime_year_derived_from_the_month(spark):
    assert transform(spark, [raw_row()]).collect()[0]["crime_year"] == 2015


def test_impossible_month_number_raises(spark):
    """A month of 13 matches the path pattern and agrees with the filename, so only
    the parse catches it."""
    with pytest.raises(ValueError, match="month_parses"):
        transform(
            spark,
            [raw_row(Month="2015-13", **{MEMBER_PATH: path_for("2015-13")})],
        )


@pytest.mark.parametrize(
    "force", ["kent", "devon-and-cornwall", "city-of-london", "btp"]
)
def test_force_comes_from_the_path(spark, force):
    row = transform(
        spark, [raw_row(**{MEMBER_PATH: path_for("2015-06", force)})]
    ).collect()[0]
    assert row["force"] == force


def test_force_and_reported_by_are_different_vocabularies(spark):
    """The path carries the publisher's slug and the file carries the display name.
    Collapsing them would lose the join key one of them is."""
    row = transform(spark, [raw_row()]).collect()[0]
    assert row["force"] == "kent"
    assert row["reported_by"] == "Kent Police"


def test_snapshot_month_comes_from_the_parameter(spark):
    row = transform(spark, [raw_row()], snapshot="2019-12").collect()[0]
    assert row["snapshot_month"] == dt.date(2019, 12, 1)


# --------------------------------------------------------------------------- #
# Snapshot bounds
# --------------------------------------------------------------------------- #


def test_month_after_the_snapshot_raises(spark):
    """An archive cannot report a month it predates, so this means the staged files
    came from an archive other than the one named."""
    with pytest.raises(ValueError, match="month_within_snapshot"):
        transform(
            spark,
            [raw_row(Month="2018-06", **{MEMBER_PATH: path_for("2018-06")})],
            snapshot="2017-12",
        )


def test_month_equal_to_the_snapshot_passes(spark):
    assert (
        transform(
            spark,
            [raw_row(Month="2017-12", **{MEMBER_PATH: path_for("2017-12")})],
            snapshot="2017-12",
        ).count()
        == 1
    )


def test_month_before_the_series_started_raises(spark):
    with pytest.raises(ValueError, match="month_within_snapshot"):
        transform(
            spark, [raw_row(Month="2010-11", **{MEMBER_PATH: path_for("2010-11")})]
        )


def test_first_month_of_the_series_passes(spark):
    assert (
        transform(
            spark, [raw_row(Month="2010-12", **{MEMBER_PATH: path_for("2010-12")})]
        ).count()
        == 1
    )


# --------------------------------------------------------------------------- #
# Lineage
# --------------------------------------------------------------------------- #


def test_lineage_stamped_on_every_row(spark):
    rows = [
        raw_row(),
        raw_row(**{MEMBER_PATH: path_for("2015-06", "essex"), "Reported by": "Essex Police"}),
    ]
    out = transform(spark, rows).collect()
    assert all(row["_source_file"] == SOURCE_FILE for row in out)
    assert all(row["_ingestion_ts"] == INGESTION_TS for row in out)


def test_lineage_columns_are_last(spark):
    assert tuple(transform(spark, [raw_row()]).columns[-2:]) == LINEAGE_COLUMNS


def test_transform_is_deterministic(spark):
    """Lineage is a parameter, not generated inside, so the same inputs must give the
    same frame. A current_timestamp() call in the transform would break this."""
    assert_df_equality(
        transform(spark, [raw_row()]),
        transform(spark, [raw_row()]),
        ignore_nullable=True,
    )


# --------------------------------------------------------------------------- #
# Duplicate measurement
# --------------------------------------------------------------------------- #


def test_identical_rows_are_counted_not_removed(spark):
    """Month truncation and coordinate snapping make two genuine incidents on one
    street identical. They stay in the table and are reported."""
    rows = [raw_row(), raw_row(), crime_row()]
    out = transform(spark, rows)
    assert out.count() == 3
    duplicates = identical_row_duplicates(out).collect()
    assert len(duplicates) == 1
    assert duplicates[0]["count"] == 2


def test_rows_differing_in_one_published_column_are_not_duplicates(spark):
    rows = [raw_row(), raw_row(Location="On or near Park")]
    assert identical_row_duplicates(transform(spark, rows)).count() == 0


def test_vintage_does_not_mask_a_duplicate(spark):
    """snapshot_month is excluded from the comparison. Including it would let the
    same record counted twice pass as two vintages."""
    both = transform(spark, [raw_row()], snapshot="2017-12").unionByName(
        transform(spark, [raw_row()], snapshot="2019-12")
    )
    duplicates = identical_row_duplicates(both).collect()
    assert len(duplicates) == 1 and duplicates[0]["count"] == 2


def test_crime_id_under_two_months_is_reported(spark):
    """A crime sits in the month it was recorded and is restated in place, so an id
    in two months means a force moved its date."""
    rows = [
        crime_row(),
        crime_row(Month="2015-07", **{MEMBER_PATH: path_for("2015-07")}),
    ]
    spread = crime_id_month_spread(transform(spark, rows)).collect()
    assert len(spread) == 1
    assert spread[0]["months"] == 2
    assert spread[0]["first_month"] == dt.date(2015, 6, 1)
    assert spread[0]["last_month"] == dt.date(2015, 7, 1)


def test_crime_id_twice_in_one_month_is_not_a_spread(spark):
    rows = [crime_row(), crime_row()]
    assert crime_id_month_spread(transform(spark, rows)).count() == 0


def test_rows_without_a_crime_id_are_excluded_from_the_spread(spark):
    """Anti-social behaviour carries no id, so grouping on it without this would
    collapse every such row in the table into one group."""
    rows = [
        raw_row(),
        raw_row(Month="2015-07", **{MEMBER_PATH: path_for("2015-07")}),
    ]
    assert crime_id_month_spread(transform(spark, rows)).count() == 0


# --------------------------------------------------------------------------- #
# End to end
# --------------------------------------------------------------------------- #


def test_end_to_end_projection(spark):
    rows = [
        crime_row(
            **{
                "Crime ID": "b" * 64,
                "Month": "2016-01",
                "Reported by": "Norfolk Constabulary",
                "Falls within": "Norfolk Constabulary",
                "Longitude": "1.029399",
                "Latitude": "52.746422",
                "Location": "On or near Blackhorse Meadow",
                "LSOA code": "E01026436",
                "LSOA name": "Breckland 001A",
                "Crime type": "Burglary",
                "Last outcome category": "Investigation complete; no suspect identified",
                MEMBER_PATH: path_for("2016-01", "norfolk"),
            }
        ),
        raw_row(
            **{
                "Month": "2016-01",
                "Reported by": "Norfolk Constabulary",
                "Falls within": "Norfolk Constabulary",
                "Longitude": "0.000000",
                "Latitude": "0.000000",
                "Location": "No Location",
                "LSOA code": None,
                "LSOA name": None,
                "Crime type": "Anti-social behaviour",
                MEMBER_PATH: path_for("2016-01", "norfolk"),
            }
        ),
    ]

    actual = transform(spark, rows).select(
        "crime_id",
        "crime_month",
        "crime_year",
        "force",
        "reported_by",
        "longitude",
        "latitude",
        "lsoa_code",
        "crime_type",
        "last_outcome_category",
        "snapshot_month",
        "_source_file",
        "_ingestion_ts",
    )

    expected_schema = StructType(
        [
            StructField("crime_id", StringType(), True),
            StructField("crime_month", DateType(), False),
            StructField("crime_year", IntegerType(), False),
            StructField("force", StringType(), False),
            StructField("reported_by", StringType(), True),
            StructField("longitude", DecimalType(9, 6), True),
            StructField("latitude", DecimalType(9, 6), True),
            StructField("lsoa_code", StringType(), True),
            StructField("crime_type", StringType(), True),
            StructField("last_outcome_category", StringType(), True),
            StructField("snapshot_month", DateType(), False),
            StructField("_source_file", StringType(), False),
            StructField("_ingestion_ts", TimestampType(), False),
        ]
    )
    expected = spark.createDataFrame(
        [
            (
                "b" * 64,
                dt.date(2016, 1, 1),
                2016,
                "norfolk",
                "Norfolk Constabulary",
                Decimal("1.029399"),
                Decimal("52.746422"),
                "E01026436",
                "Burglary",
                "Investigation complete; no suspect identified",
                dt.date(2017, 12, 1),
                SOURCE_FILE,
                INGESTION_TS,
            ),
            (
                None,
                dt.date(2016, 1, 1),
                2016,
                "norfolk",
                "Norfolk Constabulary",
                None,
                None,
                None,
                "Anti-social behaviour",
                None,
                dt.date(2017, 12, 1),
                SOURCE_FILE,
                INGESTION_TS,
            ),
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


# --------------------------------------------------------------------------- #
# Rule registry
# --------------------------------------------------------------------------- #


def test_rule_names_are_unique():
    """The names are the aggregate aliases and the keys of CheckResult.violations. A
    repeat would silently drop one rule's count."""
    names = [build(SNAPSHOT).name for build in RULES]
    assert len(names) == len(set(names))


@pytest.mark.parametrize("build", RULES, ids=[build.__name__ for build in RULES])
def test_every_rule_states_its_constraint_and_shows_evidence(build):
    """A rule that fires with no evidence tells the reader a count and nothing they
    can act on."""
    rule = build(SNAPSHOT)
    assert rule.constraint
    assert rule.evidence
    assert build.__doc__


def test_measure_names_are_unique():
    names = [item.name for item in measures(SNAPSHOT)]
    assert len(names) == len(set(names))


def test_vocabulary_columns_are_bounded_cardinality():
    """collect_set materialises every distinct value on the driver. crime type has
    nineteen and outcome category under thirty; location has millions."""
    assert set(VOCABULARY_COLUMNS) <= set(COLUMN_MAP.values())
    assert not set(VOCABULARY_COLUMNS) & {
        "location",
        "lsoa_code",
        "lsoa_name",
        "crime_id",
    }


@pytest.mark.parametrize("label", ["2010-12", "2017-01", "2026-06"])
def test_snapshot_label_accepts_a_real_month(label):
    assert assert_snapshot_label(label) == label


@pytest.mark.parametrize(
    "label", ["2015-13", "2015-00", "201512", "2015-1", "", None, "latest"]
)
def test_snapshot_label_rejects_anything_else(label):
    """Checked once per call rather than per row: a bad label would put the same null
    in every row of the archive."""
    with pytest.raises(ValueError, match="yyyy-MM"):
        assert_snapshot_label(label)


def test_coordinate_box_check_is_generated_from_the_transform_constants():
    """The CHECK and the nulling read the same bounds, so the table cannot reject a
    coordinate the transform decided to keep."""
    check = coordinate_box_check()
    for bound in LATITUDE_RANGE + LONGITUDE_RANGE:
        assert str(bound) in check


def test_coordinate_box_admits_the_northernmost_railway_station():
    """British Transport Police cover Great Britain, so Thurso at 58.59N is a real
    published location even though no Scottish territorial force publishes here."""
    assert LATITUDE_RANGE[1] > 58.59


def test_coordinate_box_excludes_the_corrupted_scottish_longitudes():
    """BTP published Edinburgh Waverley at 2.0577E in early 2021, which is the North
    Sea. Lowestoft Ness at 1.76E is the easternmost real point."""
    assert 1.76 < LONGITUDE_RANGE[1] < 2.0577


# --------------------------------------------------------------------------- #
# Single-pass validation
# --------------------------------------------------------------------------- #


def test_check_rules_returns_what_it_observed(spark):
    """The counts and vocabularies come back from the pass that had to run anyway, so
    the notebook reports them without reading the frame again."""
    result = check_rules(raw(spark, [raw_row(), crime_row()]), SNAPSHOT)
    assert result.rows == 2
    assert set(result.violations) == {build(SNAPSHOT).name for build in RULES}
    assert all(count == 0 for count in result.violations.values())
    assert result.measures["rows_without_crime_id"] == 1
    assert result.measures["rows_without_outcome"] == 1
    assert result.measures["rows_where_falls_within_differs"] == 0
    assert result.vocabularies["crime_type"] == ["Anti-social behaviour", "Burglary"]


def test_check_rules_reports_every_failing_rule_at_once(spark):
    """Guards that raise one at a time send the caller round the extraction loop once
    per fault. At ninety-six million rows that is the whole cost of the load."""
    rows = [
        raw_row(**{"Crime type": "Cyber fraud"}),
        raw_row(Longitude=None),
        raw_row(Month="2015-05"),
    ]
    with pytest.raises(ValueError) as caught:
        check_rules(raw(spark, rows), SNAPSHOT)
    message = str(caught.value)
    for name in ("crime_type_known", "coordinates_paired", "month_consistent"):
        assert name in message, name
    assert "broke 3 of" in message


def test_check_rules_caps_the_sample(spark):
    """A rule that fires on every row must still report a readable failure."""
    rows = [raw_row(**{"Crime type": "Cyber fraud"}) for _ in range(10)]
    with pytest.raises(ValueError) as caught:
        check_rules(raw(spark, rows), SNAPSHOT, samples=2)
    assert str(caught.value).count("Cyber fraud") <= 3


def test_check_rules_counts_out_of_area_without_failing(spark):
    """A corrupt coordinate is the publisher's error, not a load fault, so it is
    measured and nulled rather than aborting the archive."""
    rows = [raw_row(Longitude="2.057678", Latitude="55.952668"), raw_row()]
    result = check_rules(raw(spark, rows), SNAPSHOT)
    assert result.measures["rows_out_of_area"] == 1
    assert all(count == 0 for count in result.violations.values())


def test_shape_police_runs_no_checks(spark):
    """It is the projection alone, so the notebook can validate once and shape once.
    A frame that would fail check_rules still shapes."""
    out = shape_police(
        raw(spark, [raw_row(**{"Crime type": "Cyber fraud"})]),
        SOURCE_FILE,
        SNAPSHOT,
        INGESTION_TS,
    )
    assert out.collect()[0]["crime_type"] == "Cyber fraud"


# --------------------------------------------------------------------------- #
# Coordinates outside the UK
# --------------------------------------------------------------------------- #


def test_out_of_area_coordinates_are_nulled(spark):
    row = transform(
        spark, [raw_row(Longitude="2.057678", Latitude="55.952668")]
    ).collect()[0]
    assert row["longitude"] is None
    assert row["latitude"] is None


def test_scottish_railway_coordinates_survive(spark):
    """Edinburgh Waverley's real position. Only the corrupted longitude puts it out of
    the box."""
    row = transform(
        spark, [raw_row(Longitude="-3.188267", Latitude="55.952668")]
    ).collect()[0]
    assert row["latitude"] == Decimal("55.952668")
    assert row["longitude"] == Decimal("-3.188267")


def test_a_row_with_out_of_area_coordinates_is_kept(spark):
    """The location is dropped, not the crime."""
    out = transform(spark, [raw_row(Longitude="2.898169", Latitude="56.457532")])
    assert out.count() == 1
    assert out.collect()[0]["crime_type"] == "Anti-social behaviour"
