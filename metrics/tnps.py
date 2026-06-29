"""tnps — the Human tNPS performance metric (Social Media only), PySpark.

Part of the **metrics layer**: consumes the raw ``io_tnps_responses_raw`` table
(one row per survey response) and produces a finished, agent-level metric at
day / week / month / quarter / semester / year grain.

Human tNPS is the transactional Net Promoter Score of the surveys attributable
to a human social agent:

    tnps = (promoters − detractors) / valid_responses

**Target >= 88%.** ``metric_value`` is on the NPS scale and can be **negative**
(a squad with more detractors than promoters). Only **Social Media** has tNPS —
the source (``sprinklr_tnps_data``) only contains surveys for cases handled by a
human social agent (see ``docs/metrics_definitions.md``).

Classify-then-COUNT(DISTINCT) — NOT dedup-then-classify
-------------------------------------------------------
The legacy SM notebook (``[IO] Performance 2026 - Social Media.sql`` lines
2080-2089, ``tnps_base``) does **not** collapse a case to a single response. It
classifies **every** validity-window-surviving response row, then takes
``COUNT(DISTINCT case_number)`` **independently per classification**::

    numerator   = COUNT(DISTINCT CASE WHEN promoter  THEN case_number END)
                - COUNT(DISTINCT CASE WHEN detractor THEN case_number END)
    denominator = COUNT(DISTINCT CASE WHEN valid     THEN case_number END)

So a single case with BOTH a valid promoter response (score >= 9) AND a valid
detractor response (score <= 6) contributes **+1 to the promoter distinct count
AND +1 to the detractor distinct count** (net 0 in the numerator) and **+1 to
the denominator**. Collapsing the case to one row (the old pandas code did this)
would yield net -1 for such a case — non-byte-for-byte. We reproduce the legacy
semantics by reducing each ``(agent, date, case_number)`` to one row carrying
per-case ``has_promoter`` / ``has_detractor`` / ``has_valid`` flags (the MAX of
the per-response flags), then summing those flags per (agent, date). A case
closes on exactly one day, so summing the per-case distinct flags within any
coarser bucket equals ``COUNT(DISTINCT case_number)`` within that bucket.

Filters / rules applied here (deferred by the raw layer)
--------------------------------------------------------
* **Validity window** — keep responses where ``survey_response_date <= date + 1
  day`` (legacy ``survey_response_date <= case_closure_time + INTERVAL 1 DAY``).
  Both source columns are DATE-grained in ``sprinklr_tnps_data`` (no time
  component), so the DATE comparison is byte-for-byte equivalent to legacy's
  TIMESTAMP arithmetic. Rows with a NULL ``survey_response_date`` fall outside.
* **Classification** — promoter ``>= 9``, detractor ``<= 6``, neutral 7-8; a
  response is *valid* (denominator) when ``survey_score`` is not null.
* **Outage-date exclusion** — drop ``date = 2026-03-27`` for ``date <
  TNPS_CUTOVER`` (2026-07-01). Legacy ``tnps_base`` line 2087 drops
  ``DATE_TRUNC('DAY', case_closure_time) != '2026-03-27'``. The ``DATE`` cast
  makes the legacy filter genuinely effective (unlike the broken Quality outage
  no-op), so this is byte-for-byte. tNPS is SM-only, so only 03-27 is dropped
  (not the Core/Fraud 04-09).

Era-split snapshot pin — N/A for our window
-------------------------------------------
Legacy pins responses with closure date < 2025-12-01 to the 2025-12-01 roster
snapshot (``tnps_base_2025``, lines 2106-2120). That pinning lives in the roster
join (the raw layer); the validation window starts 2026-01-01, so no response
reaches before Dec-2025 and this rule never fires. Documented as N/A — not
implemented.

Output — one row per (agent, date_reference, granularity)
---------------------------------------------------------
Tidy "long" metric shape shared across the metrics layer:
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``. ``numerator`` =
promoters − detractors, ``denominator`` = valid responses, ``metric_value`` =
``numerator / denominator * 100`` (NULL when the denominator is 0).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metric_utils import aggregate_long, empty_metric_frame

METRIC_NAME = "tnps"

# Human tNPS only applies to Social Media.
TNPS_TEAM = "social media"

# Inclusive classification bounds (legacy tnps_base_classification, lines 2066-2070).
PROMOTER_MIN_SCORE = 9
DETRACTOR_MAX_SCORE = 6

# Cutover before which the legacy quirks (the outage-date drop) apply. From this
# date onward the correction takes effect (the outage day is kept).
TNPS_CUTOVER: date = date(2026, 7, 1)

# Legacy SM-only outage-date drop (general access problems). Legacy tnps_base
# (line 2087) filters DATE(case_closure_time) != '2026-03-27'. SM-only — the
# Core/Fraud 2026-04-09 outage does not apply to the Sprinklr SM tNPS source.
OUTAGE_DATES: tuple[date, ...] = (date(2026, 3, 27),)


def compute_tnps(tnps_responses: DataFrame) -> DataFrame:
    """Compute the Human tNPS metric at all granularities.

    Args:
        tnps_responses: the ``io_tnps_responses_raw`` table (one row per response).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    spark = tnps_responses.sparkSession

    work = tnps_responses.filter(F.lower(F.col("team")) == F.lit(TNPS_TEAM))

    # --- validity window: survey_response_date <= date + 1 day --------------
    # Both columns are DATE-grained in the source, so this DATE comparison is
    # byte-for-byte equivalent to legacy's TIMESTAMP `case_closure_time +
    # INTERVAL 1 DAY`. A NULL survey_response_date falls outside the window.
    close = F.to_date(F.col("date"))
    resp = F.to_date(F.col("survey_response_date"))
    work = work.filter(resp.isNotNull() & (resp <= F.date_add(close, 1)))

    # --- outage-date exclusion (pre-cutover, SM-only) -----------------------
    cal = F.to_date(F.col("date"))
    outage_drop = (cal < F.lit(TNPS_CUTOVER)) & cal.isin(list(OUTAGE_DATES))
    work = work.filter(~outage_drop)

    if len(work.take(1)) == 0:
        return empty_metric_frame(spark)

    score = F.col("survey_score").cast("int")
    valid = score.isNotNull()
    promoter = valid & (score >= F.lit(PROMOTER_MIN_SCORE))
    detractor = valid & (score <= F.lit(DETRACTOR_MAX_SCORE))

    work = (
        work.withColumn("_valid", valid.cast("int"))
        .withColumn("_promoter", promoter.cast("int"))
        .withColumn("_detractor", detractor.cast("int"))
    )

    # --- classify-then-COUNT(DISTINCT): collapse to one row per case --------
    # Reduce each (agent, date, case_number) to a single row carrying per-case
    # presence flags (MAX over the case's responses). A case with both a valid
    # promoter and a valid detractor response gets has_promoter=1 AND
    # has_detractor=1 (net 0) and has_valid=1. Summing these per (agent, date)
    # in aggregate_long is exactly COUNT(DISTINCT case_number) per class, because
    # there is now one row per distinct case. The dimension columns are constant
    # within a (agent, date) bucket (one roster snapshot per response), so MAX is
    # a safe carry-through for the FIRST_VALUE-by-date latest-dims rule.
    dim_first = lambda c: F.first(F.col(c), ignorenulls=True).alias(c)  # noqa: E731
    per_case = work.groupBy("agent", "date", "case_number").agg(
        F.max("_promoter").alias("has_promoter"),
        F.max("_detractor").alias("has_detractor"),
        F.max("_valid").alias("has_valid"),
        dim_first("xforce"),
        dim_first("xplead"),
        dim_first("team"),
        dim_first("squad"),
        dim_first("district"),
        dim_first("shift"),
    )

    # numerator per response-class row = has_promoter - has_detractor; summed per
    # (agent, date) this is (#distinct promoter cases) - (#distinct detractor
    # cases). denominator = #distinct valid cases.
    per_case = per_case.withColumn(
        "net_flag", F.col("has_promoter") - F.col("has_detractor")
    )

    return aggregate_long(
        per_case,
        numerator_col="net_flag",
        denominator_col="has_valid",
        metric_name=METRIC_NAME,
    )


IO_TNPS_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
