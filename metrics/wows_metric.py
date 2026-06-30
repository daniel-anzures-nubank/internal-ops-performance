"""wows — the WoWs performance metric (Social Media only), PySpark.

(Module file is ``wows_metric.py`` — not ``wows.py`` — because the raw-layer
module is already ``metrics_data/wows.py``; the two share a name and would
collide on the import path. The metric itself is still ``wows``.)

Part of the **metrics layer**: consumes the raw ``io_wows_raw`` table (one row
per WoW experience) and produces a finished, agent-level metric at day / week /
month / quarter / semester / year grain.

WoWs is a **count** metric, not a ratio — the number of distinct WoW experiences
an agent delivered in the period:

    wows = COUNT(DISTINCT case_id)

**Monthly target >= 5.** Only **Social Media** has WoWs (the source sheet only
contains social agents' WoWs — see ``docs/metrics_definitions.md``).

Output convention (differs from the ratio metrics)
--------------------------------------------------
Because WoWs is a raw count, ``metric_value`` is the **count itself** (it is
*not* ``numerator / denominator * 100``), so this module does NOT use
``aggregate_long`` (that sums + computes a ratio). It runs a small custom
``countDistinct(case_id)`` aggregation per (agent, bucket) instead:
* ``numerator``   = the WoW count (same as ``metric_value``);
* ``denominator`` = the monthly target (``5``), carried for reference only
  (legacy ``MAX(monthly_target)``);
* ``metric_value``= the WoW count.

Input
-----
The ``io_wows_raw`` table (``metrics_data/wows.py``), one row per WoW experience.
Required columns: ``agent, xforce, xplead, team, squad, district, shift, date,
case_id``.

Filters / rules applied here (deferred by the raw layer)
--------------------------------------------------------
* **Team scope** — keep ``team = 'social media'`` (defensive; source is social-only).
* **Outage-date exclusion** — drop ``date = 2026-03-27`` for ``date < WOWS_CUTOVER``
  (2026-07-01). Legacy DROPS the 2026-03-27 "general access problems" day for
  WoWs (the legacy ``wows_agent`` day grain carries no 2026-03-27 row). The drop
  happens on the raw rows BEFORE bucketing, so week/month/etc. counts exclude the
  outage WoWs too (same idiom as ``metrics/tnps.py``). WoWs is SM-only, so only
  03-27 is dropped (not the Core/Fraud 04-09). From the cutover onward the day is
  kept (correction era).
* **Count** — ``COUNT(DISTINCT case_id)`` per ``(agent, period)``.

Output — one row per (agent, date_reference, granularity)
---------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import (
    GRANULARITIES,
    METRIC_COLUMNS,
    bucket_date,
    empty_metric_frame,
    latest_dims,
)

METRIC_NAME = "wows"

# WoWs only apply to Social Media.
WOWS_TEAM = "social media"

# Monthly target (legacy ``monthly_target`` constant), carried in ``denominator``.
MONTHLY_TARGET = 5

# Cutover before which the legacy quirks (the outage-date drop) apply. From this
# date onward the correction takes effect (the outage day is kept).
WOWS_CUTOVER: date = date(2026, 7, 1)

# Legacy SM-only outage-date drop (general access problems). The legacy WoWs day
# grain has no 2026-03-27 row at all. SM-only — the Core/Fraud 2026-04-09 outage
# does not apply to the social WoWs source.
OUTAGE_DATES: set[date] = {date(2026, 3, 27)}


def _aggregate(work: DataFrame, granularity: str) -> DataFrame:
    """One WoWs row per (agent, bucket) for a single granularity.

    ``countDistinct(case_id)`` within the bucket — NOT ``aggregate_long`` (which
    sums + ratios). ``metric_value`` is the count itself.
    """
    work = work.withColumn("date_reference", bucket_date(F.col("date"), granularity))

    counts = work.groupBy("agent", "date_reference").agg(
        F.countDistinct("case_id").cast("double").alias("numerator")
    )
    latest = latest_dims(work, order_col="date")

    return (
        counts.join(latest, on=["agent", "date_reference"], how="left")
        .withColumn("date_granularity", F.lit(granularity))
        .withColumn("metric", F.lit(METRIC_NAME))
        .withColumn("denominator", F.lit(float(MONTHLY_TARGET)))
        .withColumn("metric_value", F.col("numerator"))
        .select(*METRIC_COLUMNS)
    )


def compute_wows(wows: DataFrame) -> DataFrame:
    """Compute the WoWs metric at all granularities.

    Args:
        wows: the ``io_wows_raw`` table (one row per WoW experience).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    spark = wows.sparkSession

    work = wows.filter(F.lower(F.col("team")) == F.lit(WOWS_TEAM))

    # --- outage-date exclusion (pre-cutover, SM-only) -----------------------
    # Drop the outage rows on the RAW grain, BEFORE bucketing, so the week /
    # month / quarter / ... counts also exclude the outage WoWs (matches tnps).
    cal = F.to_date(F.col("date"))
    outage_drop = (cal < F.lit(WOWS_CUTOVER)) & cal.isin(list(OUTAGE_DATES))
    work = work.filter(~outage_drop)

    if len(work.take(1)) == 0:
        return empty_metric_frame(spark)

    parts = [_aggregate(work, g) for g in GRANULARITIES]
    result = parts[0]
    for extra in parts[1:]:
        result = result.unionByName(extra)
    return result


IO_WOWS_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
