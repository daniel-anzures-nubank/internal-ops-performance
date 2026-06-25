"""Shared helpers for the metrics layer (PySpark).

Every metric table has the same tidy "long" shape — one row per
``(agent, date_reference, date_granularity)`` with ``metric / numerator /
denominator / metric_value`` — and the same day/week/month aggregation rules:

* ``day``      → the date itself
* ``week``     → Monday of that week (matches Spark ``DATE_TRUNC('WEEK', ...)``)
* ``month``    → first day of the month
* ``quarter``  → first day of the calendar quarter (Jan/Apr/Jul/Oct 1)
* ``semester`` → first day of the half-year (Jan 1 or Jul 1)
* ``year``     → first day of the year
* hierarchy/dimension fields take their **most-recent value within the bucket**
  (legacy ``FIRST_VALUE(... ORDER BY date DESC)``)
* ``metric_value`` = ``numerator / denominator * 100`` (percentage; NULL when
  the denominator is 0)

``aggregate_long`` centralizes that so each metric module only has to produce a
per-(agent, date) frame with a numerator and denominator column.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Roster dimension columns carried through every metric (most-recent value
# within each period bucket). Order matters — it defines the output column order.
DIM_COLS: tuple[str, ...] = (
    "xforce",
    "xplead",
    "team",
    "squad",
    "district",
    "shift",
)

GRANULARITIES: tuple[str, ...] = (
    "day",
    "week",
    "month",
    "quarter",
    "semester",
    "year",
)

# The shared tidy output columns, in order.
METRIC_COLUMNS: tuple[str, ...] = (
    "agent",
    *DIM_COLS,
    "date_reference",
    "date_granularity",
    "metric",
    "numerator",
    "denominator",
    "metric_value",
)

# Spark schema for the tidy long output (used to build empty frames).
METRIC_OUTPUT_SCHEMA: T.StructType = T.StructType(
    [
        T.StructField("agent", T.StringType()),
        *[T.StructField(c, T.StringType()) for c in DIM_COLS],
        T.StructField("date_reference", T.DateType()),
        T.StructField("date_granularity", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("numerator", T.DoubleType()),
        T.StructField("denominator", T.DoubleType()),
        T.StructField("metric_value", T.DoubleType()),
    ]
)


def bucket_date(date_col: Column, granularity: str) -> Column:
    """Map a date/timestamp Column to its period-bucket start (DateType)."""
    d = F.to_date(date_col)
    if granularity == "day":
        return d
    if granularity == "week":
        return F.to_date(F.date_trunc("week", d))
    if granularity == "month":
        return F.to_date(F.date_trunc("month", d))
    if granularity == "quarter":
        return F.to_date(F.date_trunc("quarter", d))
    if granularity == "year":
        return F.to_date(F.date_trunc("year", d))
    if granularity == "semester":
        year_start = F.date_trunc("year", d)
        second_half = F.add_months(year_start, 6)
        return F.to_date(
            F.when(F.month(d) <= 6, year_start).otherwise(second_half)
        )
    raise ValueError(f"unknown granularity: {granularity!r}")


def empty_metric_frame(spark: SparkSession) -> DataFrame:
    """An empty frame with the shared metric columns (for empty inputs)."""
    return spark.createDataFrame([], METRIC_OUTPUT_SCHEMA)


def latest_dims(
    df: DataFrame,
    *,
    order_col: str,
    keys: tuple[str, ...] = ("agent", "date_reference"),
    dim_cols: tuple[str, ...] = DIM_COLS,
) -> DataFrame:
    """The most-recent dimension values within each ``keys`` group.

    Mirrors legacy ``FIRST_VALUE(... ORDER BY date DESC)``: pick the row with the
    latest ``order_col`` per group and keep its dimension columns.
    """
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    return ranked.filter(F.col("_rn") == 1).select(*keys, *dim_cols)


def aggregate_long(
    df: DataFrame,
    *,
    numerator_col: str,
    denominator_col: str,
    metric_name: str,
    date_col: str = "date",
    dim_cols: tuple[str, ...] = DIM_COLS,
    granularities: tuple[str, ...] = GRANULARITIES,
    scale: float = 100.0,
) -> DataFrame:
    """Aggregate a per-(agent, date) frame into tidy day/week/month metric rows.

    Args:
        df: rows carrying ``agent``, ``date_col``, the ``dim_cols``, and the
            numerator/denominator columns. Multiple rows per (agent, date) are
            summed.
        numerator_col / denominator_col: columns summed into ``numerator`` /
            ``denominator``.
        metric_name: value for the ``metric`` column.
        date_col: the source date column (default ``date``).
        scale: multiplier for ``metric_value = numerator / denominator * scale``.
            ``100.0`` for ratio metrics expressed as a percentage; pass ``1.0``
            when the numerator is already on a 0-100 scale.

    Returns:
        Tidy long-format frame with :data:`METRIC_COLUMNS`.
    """
    spark = df.sparkSession
    parts: list[DataFrame] = []
    for granularity in granularities:
        work = df.withColumn("date_reference", bucket_date(F.col(date_col), granularity))

        sums = work.groupBy("agent", "date_reference").agg(
            F.sum(F.col(numerator_col)).cast("double").alias("numerator"),
            F.sum(F.col(denominator_col)).cast("double").alias("denominator"),
        )
        latest = latest_dims(work, order_col=date_col, dim_cols=dim_cols)

        out = (
            sums.join(latest, on=["agent", "date_reference"], how="left")
            .withColumn("date_granularity", F.lit(granularity))
            .withColumn("metric", F.lit(metric_name))
            .withColumn(
                "metric_value",
                F.when(
                    F.col("denominator") > 0,
                    F.col("numerator") / F.col("denominator") * F.lit(scale),
                ).otherwise(F.lit(None).cast("double")),
            )
            .select(*METRIC_COLUMNS)
        )
        parts.append(out)

    if not parts:
        return empty_metric_frame(spark)

    result = parts[0]
    for extra in parts[1:]:
        result = result.unionByName(extra)
    return result
