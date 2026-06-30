"""average_xforce_index — the XPLead-level Average XForce Index (all teams), PySpark.

Legacy ``average_index_xforce``: the simple mean of the **XForce-level
``xforce_index``** rolled up to the XPLead.

    Average XForce Index = AVG(xforce_index) per xplead

It reads the finished XForce Index table (``io_xforce_index_metric``) and
averages ``metric_value`` per ``(deck, xplead, date_reference,
date_granularity)``. Applies to **all four teams** (Core / Fraud / Social Media
/ Content).

Deck grouping (legacy parity)
-----------------------------
Legacy runs a **separate notebook per deck** and each groups its own
``average_index_xforce`` by ``xplead`` (GROUP BY ALL, no team column). To
reproduce that in the unified pipeline — and to mirror the sibling
``average_xpeer_index`` one level up — we group by a synthetic **deck**:

* ``core`` / ``fraud`` / NULL-team (the main-deck support squads) → ``main``;
* ``social media`` → ``sm``;
* ``content`` → ``content``.

This MERGES the core+fraud case into one ``main`` row (legacy's main notebook
averages the XForce Index over both core and fraud xforces of an xplead) while
KEEPING a cross-deck xplead split across decks as legacy does (the SM notebook
and the main notebook each emit their own row, with different values, so they
must NOT be averaged together). In practice the upstream ``io_xforce_index_metric``
is already one row per ``(team, xforce, xplead)``, so an xplead almost never
spans decks; the deck key is a safe device that reproduces the per-notebook
behavior whether or not it does. ``team`` is left NULL on output (legacy carries
none); the deck is a grouping device only.

Granularity
-----------
Legacy materialized only **week + month** (``average_index_xforce`` unions only
``*_monthly`` + ``*_weekly``; quarter/semester/year are commented out). For
``date_reference < 2026-07-01`` we emit only those two grains (the broader set is
allowed from the cutover onward).

Numerator / denominator convention
-----------------------------------
Legacy left ``numerator`` / ``denominator`` NULL and used ``AVG()``. We instead
fill ``numerator = Σ xforce_index`` and ``denominator = XForce count`` so the row
is self-describing; ``metric_value = numerator / denominator`` is the identical
mean.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "average_xforce_index"
XFORCE_INDEX_METRIC = "xforce_index"

# Legacy-parity cutover: before it, emit only the week + month grain legacy
# materialized (average_index_xforce unions only *_monthly + *_weekly).
LEGACY_CUTOVER: date = date(2026, 7, 1)

CORE = "core"
FRAUD = "fraud"
SOCIAL_MEDIA = "social media"
CONTENT = "content"


def compute_average_xforce_index(xforce_index: DataFrame) -> DataFrame:
    """Average the XForce Index to the XPLead level (per deck).

    Args:
        xforce_index: ``io_xforce_index_metric`` — XForce-grain index rows
            (``metric_value`` is each XForce's index).

    Returns:
        Tidy long-format metric rows (XPLead grain); week + month only before the
        2026-07-01 cutover.
    """
    spark = xforce_index.sparkSession
    if len(xforce_index.take(1)) == 0:
        return empty_metric_frame(spark)

    work = xforce_index
    if "metric" in work.columns:
        work = work.filter(F.col("metric") == F.lit(XFORCE_INDEX_METRIC))

    work = work.withColumn(
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
        "_deck", "xplead", "date_reference", "date_granularity"
    ).agg(
        F.sum("_mv").alias("numerator"),
        F.count(F.lit(1)).alias("denominator"),
    )

    out = grp.select(
        F.lit(None).cast("string").alias("agent"),
        F.lit(None).cast("string").alias("xforce"),
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


IO_AVERAGE_XFORCE_INDEX_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
