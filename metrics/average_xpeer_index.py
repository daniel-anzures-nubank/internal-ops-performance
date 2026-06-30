"""average_xpeer_index — the XForce-level Average Xpeer Index (all teams), PySpark.

Legacy ``average_index_agent``: the simple mean of the **agent-level Xpeer
Index** rolled up to the XForce.

    Average Xpeer Index = AVG(agent Xpeer Index) per (xforce, xplead)

It reads the finished agent-level index table (``io_xpeer_index_metric``) and
averages ``metric_value`` per ``(deck, xforce, xplead, date_reference,
date_granularity)``.

Deck grouping (legacy parity)
-----------------------------
Legacy runs a **separate notebook per deck** and each groups its own
``average_index_agent`` by ``(xforce, xplead)`` (GROUP BY ALL, no team column).
To reproduce that in the unified pipeline we group by a synthetic **deck**:

* ``core`` / ``fraud`` / NULL-team (the main-deck support squads) → ``main``;
* ``social media`` → ``sm``;
* ``content`` → ``content``.

This MERGES the core+fraud cross-team case into one ``main`` row (e.g.
``brenda.aguilar``, who has both core and fraud agents — legacy emits one main
row, not two), while KEEPING a cross-deck xforce split across decks as legacy
does (e.g. ``marcela.garduno`` has core AND social-media agents — legacy emits a
separate main row and SM row, with very different values, so they must NOT be
averaged together). ``team`` is left NULL on output (legacy carries none); the
deck is a grouping device only.

Granularity
-----------
Legacy materialized only **week + month**. For ``date_reference < 2026-07-01`` we
emit only those two grains (the broader set is allowed from the cutover onward).

Numerator / denominator convention
-----------------------------------
Legacy left ``numerator`` / ``denominator`` NULL and used ``AVG()``. We instead
fill ``numerator = Σ agent index`` and ``denominator = agent count`` so the row
is self-describing; ``metric_value = numerator / denominator`` is the identical
mean.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "average_xpeer_index"

# Legacy-parity cutover: before it, emit only the week + month grain legacy
# materialized (average_index_agent unions only *_monthly + *_weekly).
LEGACY_CUTOVER: date = date(2026, 7, 1)

CORE = "core"
FRAUD = "fraud"
SOCIAL_MEDIA = "social media"
CONTENT = "content"


def compute_average_xpeer_index(xpeer_index: DataFrame) -> DataFrame:
    """Average the agent-level Xpeer Index to the XForce level (per deck).

    Args:
        xpeer_index: ``io_xpeer_index_metric`` — agent-level index rows
            (``metric_value`` is each agent's index).

    Returns:
        Tidy long-format metric rows (XForce grain); week + month only before the
        2026-07-01 cutover.
    """
    spark = xpeer_index.sparkSession
    if len(xpeer_index.take(1)) == 0:
        return empty_metric_frame(spark)

    work = xpeer_index.withColumn(
        "_mv", F.col("metric_value").cast("double")
    ).filter(F.col("_mv").isNotNull())

    # Pre-cutover: legacy only materialized week + month.
    pre_cutover = F.col("date_reference") < F.lit(LEGACY_CUTOVER)
    work = work.filter(
        (~pre_cutover) | F.col("date_granularity").isin("week", "month")
    )
    if len(work.take(1)) == 0:
        return empty_metric_frame(spark)

    team = F.lower(F.col("team"))
    deck = (
        F.when(team.isin(CORE, FRAUD) | team.isNull(), F.lit("main"))
        .when(team == F.lit(SOCIAL_MEDIA), F.lit("sm"))
        .when(team == F.lit(CONTENT), F.lit("content"))
        .otherwise(F.lit("other"))
    )
    work = work.withColumn("_deck", deck)

    grp = work.groupBy(
        "_deck", "xforce", "xplead", "date_reference", "date_granularity"
    ).agg(
        F.sum("_mv").alias("numerator"),
        F.count(F.lit(1)).alias("denominator"),
    )

    out = grp.select(
        F.lit(None).cast("string").alias("agent"),
        F.col("xforce"),
        F.col("xplead"),
        F.lit(None).cast("string").alias("team"),
        F.lit(None).cast("string").alias("squad"),
        F.lit(None).cast("string").alias("district"),
        F.lit(None).cast("string").alias("shift"),
        F.col("date_reference"),
        F.col("date_granularity"),
        F.lit(METRIC_NAME).alias("metric"),
        F.col("numerator").cast("double").alias("numerator"),
        F.col("denominator").cast("double").alias("denominator"),
        F.when(
            F.col("denominator") > 0,
            F.col("numerator") / F.col("denominator"),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("metric_value"),
    )
    return out.select(*METRIC_COLUMNS)


IO_AVERAGE_XPEER_INDEX_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
