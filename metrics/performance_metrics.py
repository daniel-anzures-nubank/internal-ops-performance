"""performance_metrics — the consolidated performance-metrics table, PySpark.

The one-stop reporting table: the **UNION ALL of all 16 finished
``io_*_metric`` tables** with the same 13 tidy columns, except ``team`` is
replaced by a **display team** (``Core`` / ``Fraud`` / ``Social Media`` /
``Content`` / ``Quality``). No rows are filtered, aggregated, or recomputed —
one input row in, one output row out.

Display-team cascade (exact order)
----------------------------------
1. **Direct team map** of the source ``team`` column:
   ``core -> Core``, ``fraud -> Fraud``, ``social media -> Social Media``,
   ``content -> Content``.
2. **Squad map** (support squads legacy keeps with ``team = NULL``):
   ``quality -> Quality``, ``enablement -> Content``.
3. else, rows with ``squad`` NOT NULL: the **modal display team of that squad**
   at the same ``(date_reference, date_granularity)``.
4. else, rows with ``squad`` NULL and ``xforce`` NOT NULL: the modal display
   team of that **xforce**.
5. else, ``xforce`` NULL and ``xplead`` NOT NULL: the modal display team of
   that **xplead**.
6. else, all of those NULL and ``district`` NOT NULL: the modal display team of
   that **district**.
7. else NULL.

The cascade **branches on which dimension is populated**, not on lookup
success: a ``squad`` NOT NULL row whose squad has no modal team stays NULL — it
never falls through to the xforce/xplead/district lookups. That is what makes
the ``planning`` support squad deliberately end up NULL (``planning`` maps to
no display team, and its own adherence rows never enter the modal dims).

Modal dims (steps 3-6)
----------------------
Built ONLY from ``io_adherence_metric`` rows — adherence is the driver metric
(every agent in the pipeline has adherence rows carrying the full roster
dimensions). Each adherence row is labeled with its own steps-1-2 display team;
rows whose display team is NULL (e.g. ``planning``) are dropped. Per
``(dimension value, date_reference, date_granularity)`` the **modal** display
team is the one with the highest row count, ties broken alphabetically.

This backfills the roll-up metrics that legacy emits without a ``team`` (or
with ``team = NULL``): the XForce roll-ups (``ntpj_xforce``,
``improved_benchmark_xforce``, ``xpeers_in_target``, ``average_xpeer_index``,
``xforce_index``, ``shrinkage_xforce``), the XPLead roll-ups
(``xpeers_in_target_xplead``, ``shrinkage_xplead``, ``average_xforce_index``),
and the squad/district ``nuvinhos_performance`` roll-ups.

Output — the shared tidy long format
------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value`` — identical to
every input except ``team`` now carries the display team.
"""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from metric_utils import METRIC_COLUMNS, empty_metric_frame

# Step 1 — direct map of the source team column (lowercased).
TEAM_DISPLAY: tuple[tuple[str, str], ...] = (
    ("core", "Core"),
    ("fraud", "Fraud"),
    ("social media", "Social Media"),
    ("content", "Content"),
)

# Step 2 — support-squad map (legacy keeps these with team = NULL). `planning`
# is deliberately absent: it maps to no display team and stays NULL.
SQUAD_DISPLAY: tuple[tuple[str, str], ...] = (
    ("quality", "Quality"),
    ("enablement", "Content"),
)

# Modal fallback dimensions, in cascade order (steps 3-6).
_MODAL_DIMS: tuple[str, ...] = ("squad", "xforce", "xplead", "district")
_BUCKET_KEYS: tuple[str, ...] = ("date_reference", "date_granularity")


def _display_team(team: Column, squad: Column) -> Column:
    """Steps 1-2 of the cascade: direct team map, then support-squad map."""
    t = F.lower(team)
    s = F.lower(squad)
    expr: Column | None = None
    for key, display in TEAM_DISPLAY:
        cond = t == F.lit(key)
        expr = F.when(cond, display) if expr is None else expr.when(cond, display)
    assert expr is not None
    for key, display in SQUAD_DISPLAY:
        expr = expr.when(s == F.lit(key), display)
    return expr  # falls through to NULL


def _modal_team_dim(adherence: DataFrame, dim: str) -> DataFrame:
    """``(dim, date_reference, date_granularity) -> modal display team``.

    Built from adherence rows whose own steps-1-2 display team is non-NULL.
    "Modal" = the display team with the highest row count; ties broken
    alphabetically (ascending team name).
    """
    labeled = adherence.select(
        dim,
        *_BUCKET_KEYS,
        _display_team(F.col("team"), F.col("squad")).alias("_team"),
    ).filter(F.col(dim).isNotNull() & F.col("_team").isNotNull())

    counts = labeled.groupBy(dim, *_BUCKET_KEYS, "_team").agg(
        F.count(F.lit(1)).alias("_cnt")
    )
    w = Window.partitionBy(dim, *_BUCKET_KEYS).orderBy(
        F.col("_cnt").desc(), F.col("_team").asc()
    )
    return (
        counts.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(dim, *_BUCKET_KEYS, F.col("_team").alias(f"_{dim}_team"))
    )


def compute_performance_metrics(
    adherence_metric: DataFrame,
    other_metrics: Sequence[DataFrame],
) -> DataFrame:
    """Union the 16 metric tables and replace ``team`` with the display team.

    Args:
        adherence_metric: ``io_adherence_metric`` — included in the union AND
            the source of the modal display-team dims (steps 3-6).
        other_metrics: the other 15 ``io_*_metric`` DataFrames, each already in
            the shared tidy long shape.

    Returns:
        The UNION ALL of all 16 inputs (row-for-row, no loss/dup) with ``team``
        replaced by the display team; same 13 :data:`METRIC_COLUMNS`.
    """
    spark = adherence_metric.sparkSession

    unioned = adherence_metric.select(*METRIC_COLUMNS)
    for frame in other_metrics:
        unioned = unioned.unionByName(frame.select(*METRIC_COLUMNS))
    if len(unioned.take(1)) == 0:
        return empty_metric_frame(spark)

    out = unioned.withColumn(
        "_direct", _display_team(F.col("team"), F.col("squad"))
    )
    for dim in _MODAL_DIMS:
        out = out.join(
            _modal_team_dim(adherence_metric, dim),
            on=[dim, *_BUCKET_KEYS],
            how="left",
        )

    # Steps 3-6 branch on which dimension is populated (a populated dimension
    # whose lookup misses stays NULL — no fall-through to the next dimension).
    fallback = (
        F.when(F.col("squad").isNotNull(), F.col("_squad_team"))
        .when(F.col("xforce").isNotNull(), F.col("_xforce_team"))
        .when(F.col("xplead").isNotNull(), F.col("_xplead_team"))
        .when(F.col("district").isNotNull(), F.col("_district_team"))
    )
    out = out.withColumn("team", F.coalesce(F.col("_direct"), fallback))

    return out.select(*METRIC_COLUMNS)


IO_PERFORMANCE_METRICS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date_reference", "DATE"),
    ("date_granularity", "STRING"),
    ("metric", "STRING"),
    ("numerator", "DOUBLE"),
    ("denominator", "DOUBLE"),
    ("metric_value", "DOUBLE"),
)
