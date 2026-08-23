"""Column expressions shared across the Silver transforms.

Every source publishes its dates in its own format and every one of them arrives as
a string, so each transform parses dates and each needs the same parse to behave
identically. The format stays with the caller, since it is a property of the source.

Apache Spark has no try_to_date. Databricks provides it, so a transform calling it
runs on the cluster and fails anywhere else, which is invisible until something off
the cluster tries to run the code. The parse routes through try_to_timestamp, which
both engines register, and casts the result to DATE. The cast reads back in the
session timezone it was parsed in, so the date survives whatever the session is set
to.

Backticks quote every raw column name. Published headers carry spaces and percent
signs and some open with a digit.

No I/O here, and no source knowledge: these take a column name and a format and
return a Column.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def parsed_date(source_column: str, date_format: str) -> Column:
    """A raw string column parsed to DATE, null where it does not parse.

    Unaliased. Callers that write into a named column add their own alias, and
    callers that wrap the result do not want one.
    """
    return F.expr(
        f"CAST(try_to_timestamp(`{source_column}`, '{date_format}') AS DATE)"
    )
