"""quality — the Quality (QA) performance metric (Core / Fraud / Social Media).

Quality is the simple average of an agent's QA evaluation scores for the period:

    quality = SUM(qa_score) / COUNT(evaluations)   (mean of the scores)

**Target ≥ 95%.** Scores are already on a 0-100 scale, so ``metric_value`` is the
mean directly (``numerator = SUM(qa_score)``, ``denominator = # evaluations``,
``scale = 1``).

Team coverage
-------------
Core, Fraud, and **Social Media** all score Quality as the mean of their QA
evaluation scores. **Content is excluded**: its quality of record is the
separate **Quality (CSAT)** metric (`metrics/content_csat.py` →
`io_content_csat_*`).

Sources (Playvox + Sprinklr SM)
-------------------------------
Quality is scored from two feeds, unioned in the raw layer
(``io_quality_evaluations_raw``):

* **Playvox** (``qmo_playvox_consolidated``) — Core / Fraud / Content, and
  historically Social Media too.
* **Sprinklr SM** (``social_media_case_summary_information``) — Social-Media
  case QA, from ``2026-05-01`` onward (``UNION ALL``-ed on top of Playvox).

Both are on the same 0-100 scale, and the raw table carries a ``source`` column
('playvox' / 'sprinklr_sm') for provenance. This metric averages all (deduped)
evaluations regardless of source, so a social agent with both Playvox and
Sprinklr evaluations contributes both. (Legacy carried the Sprinklr ``UNION ALL``
only in the Core/Fraud dataset, where it was dead code — the active-roster join
excluded ``social`` — so the new pipeline is the first to actually score SM from
Sprinklr.)

Input
-----
``io_quality_evaluations_raw`` (one row per evaluation), via
``metrics_data/quality_evaluations.py``. Required columns: ``agent, xforce,
xplead, team, squad, district, shift, date, evaluation_id, team_name, source,
qa_score``.

Steps applied here (deferred by the raw layer)
----------------------------------------------
* **Latest record per ``evaluation_id``** (legacy ``ROW_NUMBER() OVER (PARTITION
  BY evaluation_id ORDER BY created_at DESC)``, keep rn=1). The raw table only
  carries day-grain ``date`` (not the original ``created_at`` timestamp), so we
  keep the row with the latest ``date`` per ``evaluation_id`` (same-day re-scores
  can't be ordered finer — see caveat below).
* **Drop Content**: ``team == 'content'`` rows are removed (CSAT is content's
  quality).
* Rows with a null ``qa_score`` are dropped.

NOT applied here (future Adjustments layer)
-------------------------------------------
* The ``scorecard_id`` / ``evaluation_id`` blacklists.
* Outage-date exclusions (2026-03-27, 2026-04-09).

Output — tidy long format, one row per (agent, date_reference, granularity)
---------------------------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value`` where
``numerator`` = sum of scores, ``denominator`` = # of evaluations,
``metric_value`` = mean score.
"""

from __future__ import annotations

import pandas as pd

from metric_utils import aggregate_long, empty_metric_frame

METRIC_NAME = "quality"

# Content's quality of record is CSAT, not the Playvox QA mean.
EXCLUDED_TEAMS: tuple[str, ...] = ("content",)


def _dedup_latest_per_evaluation(evals: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest row per ``evaluation_id`` (by ``date``).

    Mirrors legacy ``ROW_NUMBER() ... ORDER BY created_at DESC`` at day grain.
    """
    work = evals.copy()
    work["_date"] = pd.to_datetime(work["date"])
    work = work.sort_values("_date")
    work = work.drop_duplicates(subset=["evaluation_id"], keep="last")
    return work.drop(columns=["_date"])


def compute_quality(quality_evaluations: pd.DataFrame) -> pd.DataFrame:
    """Compute the Quality metric at all granularities.

    Args:
        quality_evaluations: the ``io_quality_evaluations_raw`` table (one row
            per evaluation).

    Returns:
        Tidy long-format metric rows (see module docstring).
    """
    if quality_evaluations.empty:
        return empty_metric_frame()

    evals = quality_evaluations[quality_evaluations["qa_score"].notna()].copy()
    evals = evals[~evals["team"].astype("string").str.lower().isin(EXCLUDED_TEAMS)]
    if evals.empty:
        return empty_metric_frame()

    evals = _dedup_latest_per_evaluation(evals)

    # One row per evaluation → SUM(qa_score) / COUNT = mean. scale=1 because
    # qa_score is already a 0-100 percentage.
    evals["_one"] = 1.0
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
