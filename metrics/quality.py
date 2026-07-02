"""quality — the Quality (QA) performance metric (Core / Fraud / Social Media), PySpark.

Quality is the simple average of an agent's QA evaluation scores for the period:

    quality = SUM(qa_score) / COUNT(DISTINCT evaluation_id)   (mean of the scores)

**Target >= 95%.** Scores are already on a 0-100 scale, so ``metric_value`` is the
mean directly (``numerator = SUM(qa_score)``, ``denominator = # distinct
evaluations``, ``scale = 1``).

Team coverage
-------------
Core, Fraud, and **Social Media** all score Quality as the mean of their QA
evaluation scores. **Content is excluded**: its quality of record is the separate
**Quality (CSAT)** metric. ``compute_quality`` excludes only ``team == 'content'``;
the social team string is ``'social media'`` (with a space).

Sources (Playvox + Sprinklr SM — a union, like legacy)
------------------------------------------------------
Quality is scored from two feeds prepared by the raw layer
(``io_quality_evaluations_raw``), **unioned exactly like legacy** (``[IO]
Performance 2026 - Social Media Temp Fix.sql`` ``qa_base``, lines 2988-3028):
Playvox (Core / Fraud / Social Media, **no upper date bound** — SM Playvox
evaluations keep flowing until they naturally end after 2026-05-15) and Sprinklr
SM (Social-Media case QA, floored at 2026-05-01 like legacy's ``sm.report_date
>= "2026-05-01"``). In early May an SM agent can contribute BOTH a Playvox and
a Sprinklr evaluation to the same period's mean — that is legacy behavior, not
double-counting. The raw table carries a ``source`` column ('playvox' /
'sprinklr_sm'). Dedup here is **per (source, evaluation_id)** so a Playvox
``evaluation__id`` and a Sprinklr ``case_number`` can never collide and
silently drop a row (legacy dedups within each single-source notebook; the two
id spaces are disjoint, so cross-source dedup is a no-op).

Input
-----
``io_quality_evaluations_raw`` (one row per evaluation), via
``metrics_data/quality_evaluations.py``. Required columns: ``agent, xforce,
xplead, team, squad, district, shift, date, created_at, evaluation_id,
team_name, scorecard_id, source, qa_score``.

Legacy parity steps applied here (deferred by the raw layer)
------------------------------------------------------------
* **Team-scoped blacklists** (date < 2026-07-01), verified against legacy:
    - Core / Fraud (``[IO] Quality Dataset.sql`` ``qa_base``, lines 162/166): drop
      ``scorecard_id IN BLACKLIST_SCORECARD_IDS`` AND ``evaluation_id IN
      BLACKLIST_EVALUATION_IDS``.
    - Social Media (``[IO] Performance 2026 - Social Media.sql`` ``qa_base``,
      line 2920): drop ONLY ``scorecard_id == SM_BLACKLIST_SCORECARD_ID`` — NO
      evaluation_id blacklist.
* **NO date drops** — no outage-date exclusion is applied. The 2026-06-30
  legacy re-export re-included the 2026-03-27 / 2026-04-09 outage rows: the
  published ``usr.mx__cx.quality_io`` and ``usr.danielanzures.sm_temp_quality``
  (both rebuilt 2026-07-01/02) carry rows on both dates, so current legacy
  drops no quality dates and neither do we. (An earlier revision dropped them,
  ported from a pre-re-export legacy snapshot; reverted for parity.)
* **Latest record per (source, evaluation_id)** by ``created_at DESC`` (legacy
  ``ROW_NUMBER() OVER (PARTITION BY evaluation_id ORDER BY
  local_mx_evaluation__created_at DESC)``, keep rn=1).
* **Drop Content** (``team == 'content'``) and **drop null ``qa_score``** rows.

Era-gating (legacy ``qa_score_2025`` snapshot pin) — N/A for our window
-----------------------------------------------------------------------
Legacy pins evaluations with ``created_at < 2025-12-01`` to the 2025-12-01 roster
snapshot. That pinning lives in the roster join (``metrics_data``); the validation
window starts 2026-01-01, so no evaluation reaches before Dec-2025 and this rule
never fires. Documented as N/A — not implemented here.

Output — tidy long format, one row per (agent, date_reference, granularity)
---------------------------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value`` where
``numerator`` = sum of scores, ``denominator`` = # distinct evaluations,
``metric_value`` = mean score.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from metric_utils import aggregate_long, empty_metric_frame

METRIC_NAME = "quality"

# Content's quality of record is CSAT, not the Playvox QA mean.
EXCLUDED_TEAMS: tuple[str, ...] = ("content",)

# Performance team string for Social Media (note the space). Used to scope the
# team-asymmetric blacklist rules.
SOCIAL_MEDIA_TEAM = "social media"

# Cutover before which the legacy QA blacklists apply. From this date onward
# they are dropped (corrections take effect).
QUALITY_CUTOVER: date = date(2026, 7, 1)

# --- Team-scoped blacklists (legacy QA artifacts, pre-cutover only) ---------
#
# Core / Fraud — legacy/[IO] Quality Dataset.sql qa_base:
#   scorecard__id NOT IN (...)  (line 162)
#   evaluation__id NOT IN (...)  (line 166)
BLACKLIST_SCORECARD_IDS: tuple[str, ...] = (
    "68def79b3f83da8cc9cb5299",
    "6812b3e46abeabb0653d197e",
    "688017f4bb266bb43b6c9565",
    "68680819336107d9f140d1ce",
)
BLACKLIST_EVALUATION_IDS: tuple[str, ...] = (
    "68646ed2f093c149757ba038",
    "687704e7a077fb121012dd5d",
    "688017f4bb266bb43b6c9565",
    "68680819336107d9f140d1ce",
)
# Social Media — legacy/[IO] Performance 2026 - Social Media.sql qa_base
#   scorecard__id NOT IN ("68def79b3f83da8cc9cb5299")  (line 2920); NO eval_id list.
SM_BLACKLIST_SCORECARD_IDS: tuple[str, ...] = ("68def79b3f83da8cc9cb5299",)


def _is_social_media(col: "F.Column") -> "F.Column":
    """True where the ``team`` column is the Social-Media team string."""
    return F.lower(col) == F.lit(SOCIAL_MEDIA_TEAM)


def _apply_blacklists(evals: DataFrame) -> DataFrame:
    """Drop blacklisted evaluations, team-scoped, for date < cutover.

    Core/Fraud: drop scorecard_id-in-list OR evaluation_id-in-list.
    Social Media: drop scorecard_id == the single SM scorecard only.
    Rows on/after the cutover are never blacklisted.
    """
    cal = F.to_date(F.col("date"))
    pre_cutover = cal < F.lit(QUALITY_CUTOVER)
    is_sm = _is_social_media(F.col("team"))

    core_fraud_hit = F.col("scorecard_id").isin(list(BLACKLIST_SCORECARD_IDS)) | F.col(
        "evaluation_id"
    ).isin(list(BLACKLIST_EVALUATION_IDS))
    sm_hit = F.col("scorecard_id").isin(list(SM_BLACKLIST_SCORECARD_IDS))

    blacklisted = pre_cutover & F.when(is_sm, sm_hit).otherwise(core_fraud_hit)
    return evals.filter(~blacklisted)


def _dedup_latest_per_evaluation(evals: DataFrame) -> DataFrame:
    """Keep the latest row per ``(source, evaluation_id)`` by ``created_at DESC``.

    Mirrors legacy ``ROW_NUMBER() OVER (PARTITION BY evaluation_id ORDER BY
    created_at DESC)`` keep rn=1, dedup'd WITHIN each source (legacy dedups inside
    each single-source notebook) so a Playvox id and a Sprinklr case_number can
    never collide. Falls back to ``date`` then ``qa_score`` as deterministic
    tiebreakers when ``created_at`` ties.
    """
    w = Window.partitionBy("source", "evaluation_id").orderBy(
        F.col("created_at").desc_nulls_last(),
        F.to_date(F.col("date")).desc_nulls_last(),
        F.col("qa_score").desc_nulls_last(),
    )
    ranked = evals.withColumn("_rn", F.row_number().over(w))
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


def compute_quality(quality_evaluations: DataFrame) -> DataFrame:
    """Compute the Quality metric at all granularities.

    Args:
        quality_evaluations: the ``io_quality_evaluations_raw`` table (one row
            per evaluation).

    Returns:
        Tidy long-format metric rows (see module docstring).
    """
    spark = quality_evaluations.sparkSession

    evals = quality_evaluations.filter(F.col("qa_score").isNotNull())
    evals = evals.filter(
        ~F.lower(F.col("team")).isin(list(EXCLUDED_TEAMS))
    )

    # Legacy blacklists run in qa_base, i.e. BEFORE the dedup window — a
    # blacklisted revision must not be eligible to win the dedup.
    evals = _apply_blacklists(evals)

    # No outage-date drops: the 2026-06-30 legacy re-export re-included the
    # 2026-03-27 / 2026-04-09 rows, so current legacy drops no quality dates
    # and neither do we (see module docstring).
    evals = _dedup_latest_per_evaluation(evals)

    if len(evals.take(1)) == 0:
        return empty_metric_frame(spark)

    # One (deduped) row per evaluation -> SUM(qa_score) / COUNT = mean. The
    # denominator counts distinct evaluations; after the per-(source,
    # evaluation_id) dedup, one row == one distinct evaluation, so a row count is
    # exactly COUNT(DISTINCT evaluation_id). scale=1 (qa_score is already 0-100).
    evals = evals.withColumn("_one", F.lit(1.0))
    return aggregate_long(
        evals,
        numerator_col="qa_score",
        denominator_col="_one",
        metric_name=METRIC_NAME,
        scale=1.0,
    )


IO_QUALITY_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
