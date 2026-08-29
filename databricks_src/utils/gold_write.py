"""Delta write helpers shared by the Gold load notebooks.

`INSERT OVERWRITE` matches on position, so a projection that has drifted from the
declared order would load values into the wrong columns without failing. The order is
read off the target rather than written out in a notebook: the table is the contract, and
a second copy of the column list is free to disagree with it.

Copied in `01_load_dimensions.py` and `02_load_panel_facts.py` while there were two
callers, on the grounds that extracting them meant editing a notebook whose rerun is the
96 million row crime scan. `03_load_transaction_facts.py` is the third caller, which is
the point the copies were to be revisited at. The rerun cost turned out not to apply:
replacing two identical definitions with an import is proved by running the import cell,
not the notebook.

The session is taken off the frame rather than read from a notebook global, so these are
importable and a caller cannot pass a frame belonging to one session and a table name
resolved against another.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def target_columns(spark: SparkSession, table: str) -> list[str]:
    """Column order as the created table declares it."""
    return [field.name for field in spark.table(table).schema.fields]


def overwrite(df: DataFrame, table: str) -> int:
    """Replace a Gold table's contents, guarding the projection against the target.

    Args:
        df: the frame to write, whose column order must equal the target's.
        table: fully qualified name of the created Delta table.

    Returns:
        Rows in the table after the write, read back rather than counted on the frame,
        so the figure describes what landed.
    """
    spark = df.sparkSession
    declared = target_columns(spark, table)
    if list(df.columns) != declared:
        raise ValueError(
            f"{table}: the transform produces {list(df.columns)}, the table declares "
            f"{declared}. INSERT OVERWRITE matches on position, so this would load "
            "values into the wrong columns."
        )
    view = f"_staging_{table.rsplit('.', 1)[-1]}"
    df.createOrReplaceTempView(view)
    spark.sql(f"INSERT OVERWRITE {table} TABLE {view}")
    return spark.table(table).count()
