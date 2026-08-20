"""Gold conformance and column guards, shared by every fact and by dim_lsoa.

Unity Catalog does not enforce primary or foreign keys. They describe the model to the
optimiser and to Power BI, and nothing else. A fact naming a dimension row that does not
exist therefore loads without complaint and disappears from every rollup keyed on it,
and a repeated key loads and doubles whatever is summed over it. The load is the only
place either can be caught, so these run there.

Both checks are here rather than in each table's own module because the star declares
twenty of them: nine facts against two geography dimensions, the calendar, and the crime
type list, plus dim_lsoa against dim_area. One copy per site is one wording per site,
and the fact that reports the least is the one nobody notices is wrong.

Failure detail is collected in a second pass, run only once something has failed. A
clean load pays one action per check, which is what it cost before the detail existed.

This module breaks the folder's one-module-per-table convention, which is why it is
named for what it does rather than for a table.

No I/O here, as with every module in this folder. Frames in, frames out, and the reads
live in the notebook.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Distinct offending values listed, and rows sampled for a duplicate. Matches the
# sibling convention: ten where distinct values are being listed, five where rows are.
SAMPLE_VALUES = 10
SAMPLE_ROWS = 5


def assert_column_present(
    df: DataFrame, column: str, subject: str, owner: str
) -> DataFrame:
    """Fail unless a frame carries the column a check is about to read.

    Caught before the join rather than left to Spark. An `AnalysisException` naming a
    resolution failure says nothing about which of the two frames was passed wrongly,
    and passing the pre-write frame where the loaded table was wanted is the mistake
    this exists for.
    """
    if column not in df.columns:
        raise ValueError(
            f"{subject} conformance check needs {owner}'s {column} column, and the "
            f"frame carries {sorted(df.columns)}."
        )
    return df


def assert_columns_present(
    df: DataFrame, required: tuple[str, ...], reader: str
) -> DataFrame:
    """Fail unless a frame carries the columns something reads from it.

    One direction only. Gold reads a projection of tables it does not own, so a missing
    column is a fault and an extra one is not.

    Callers pass their own name, because a fact losing a column has to say which fact
    rather than which shared module noticed. Every missing column is named rather than
    the first one found, since a frame passed from the wrong place is usually missing
    several, and one name per run turns that into one run per name.

    The singular assert_column_present above guards one join key for a conformance
    check and words its failure that way. This guards a read list, and both fact
    families call it through their shared resolutions.
    """
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"{reader} is missing columns it reads: {missing}")
    return df


def assert_keys_conform(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_column: str,
    parent_column: str,
    child_name: str,
    parent_name: str,
    example_column: str | None = None,
) -> DataFrame:
    """Fail where a child key has no row in the parent dimension.

    A null child key is reported alongside a genuinely unmatched one. Equality is false
    against null, so an anti-join returns it either way, and both mean the same thing at
    this point: a row whose key resolves to nothing. The rent fact resolves eight of its
    keys by name lookup, where a miss produces exactly that null.

    Offenders are the distinct failing values with the number of child rows carrying
    each, ordered so a rerun reports them the same way. A count is what says whether one
    code is missing from one row or from a whole series. The list is capped, so the
    total number of distinct failing values is reported beside it: without that, ten
    values shown cannot be told from ten of four hundred.

    Args:
        child_df: the fact or dimension holding the foreign key.
        parent_df: the dimension it points at, read from the loaded table rather than
            from the frame that produced it.
        child_column: foreign key column on the child.
        parent_column: key column on the parent.
        child_name: table name used in the error, e.g. fact_area_month_hpi.
        parent_name: dimension name used in the error, e.g. dim_area.
        example_column: child column to carry one worked example of into the message,
            lowest value per offender so a rerun names the same one. Worth passing
            where the failing key is not itself enough to trace the fault: a null
            district_code says nothing about which small areas produced it, and the
            frame is pre-write inside a failed load and cannot be queried afterwards.
            On a fact the key is the whole diagnosis, so it is left unset.

    Returns:
        child_df unchanged, so this can be called as a statement in sequence.
    """
    assert_column_present(parent_df, parent_column, child_name, parent_name)
    assert_column_present(child_df, child_column, child_name, child_name)

    projection = [F.col(child_column).alias("_key")]
    detail = [F.count(F.lit(1)).alias("rows")]
    reported = [F.col("_key").alias(child_column), "rows"]
    if example_column is not None:
        assert_column_present(child_df, example_column, child_name, child_name)
        projection.append(F.col(example_column).alias("_example"))
        detail.append(F.min("_example").alias(f"example_{example_column}"))
        reported.append(f"example_{example_column}")

    unmatched = child_df.select(*projection).join(
        parent_df.select(F.col(parent_column).alias("_key")).distinct(),
        "_key",
        "left_anti",
    )

    dangling = (
        unmatched.groupBy("_key")
        .agg(*detail)
        .orderBy(F.desc("rows"), "_key")
        .limit(SAMPLE_VALUES)
        .select(*reported)
        .collect()
    )
    if not dangling:
        return child_df

    offenders = unmatched.select("_key").distinct().count()
    raise ValueError(
        f"{child_name} rows carry a {child_column} with no row in {parent_name}, so "
        f"they would drop out of every rollup keyed on it. {offenders:,} distinct "
        f"values, showing up to {SAMPLE_VALUES}: {[row.asDict() for row in dangling]}"
    )


def assert_grain_unique(
    df: DataFrame, key_columns: tuple[str, ...], subject: str
) -> DataFrame:
    """Fail on a repeated key, which is the table's declared grain.

    The primary key is informational and unenforced, so a repeat reaches Delta and
    doubles every measure aggregated over it. Nothing downstream can separate the two
    rows afterwards, since the key is all that identifies either.
    """
    for column in key_columns:
        assert_column_present(df, column, subject, subject)

    repeated = (
        df.groupBy(*key_columns)
        .agg(F.count(F.lit(1)).alias("rows"))
        .where(F.col("rows") > 1)
    )

    duplicates = (
        repeated.orderBy(F.desc("rows"), *key_columns).limit(SAMPLE_ROWS).collect()
    )
    if not duplicates:
        return df

    offenders = repeated.count()
    raise ValueError(
        f"{subject} grain broken, ({', '.join(key_columns)}) is not unique. "
        f"{offenders:,} repeated keys, showing up to {SAMPLE_ROWS}: "
        f"{[row.asDict() for row in duplicates]}"
    )


def measure_dimension_coverage(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_column: str,
    parent_column: str,
) -> tuple[int, int]:
    """Dimension rows the child reaches, and the dimension's total rows.

    Recorded as a count and a base rather than a share, so two runs re-aggregate. The
    figure is coverage rather than integrity: a dimension row no fact reaches is
    ordinary, which is why this measures and assert_keys_conform asserts.

    Returns:
        (reached, total). Call this after assert_keys_conform, since a child key
        matching nothing contributes to neither number and would otherwise pass here
        unnoticed.
    """
    parent_keys = parent_df.select(F.col(parent_column).alias("_key")).distinct()
    reached = (
        child_df.select(F.col(child_column).alias("_key"))
        .distinct()
        .join(parent_keys, "_key", "inner")
        .count()
    )
    return reached, parent_keys.count()
