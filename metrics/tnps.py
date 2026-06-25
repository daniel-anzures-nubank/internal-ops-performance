"""tnps — the Human tNPS performance metric (Social Media only).

Part of the **metrics layer**: consumes the raw ``io_tnps_responses_raw`` table
(one row per survey response) and produces a finished, agent-level metric at
day / week / month / quarter / semester / year grain.

Human tNPS is the transactional Net Promoter Score of the surveys attributable
to a human social agent:

    tnps = (promoters − detractors) / valid_responses

**Target ≥ 88%.** ``metric_value`` is on the NPS scale and can be **negative**
(a squad with more detractors than promoters). Only **Social Media** has tNPS —
the source (`sprinklr_tnps_data`) only contains surveys for cases handled by a
human social agent (see ``docs/metrics_definitions.md``).

Input
-----
The ``io_tnps_responses_raw`` table (``metrics_data/tnps_responses.py``), one row
per survey response. Required columns: ``agent, xforce, xplead, team, squad,
district, shift, date, case_number, survey_response_date, survey_score``.

Filters / rules applied here (deferred by the raw layer)
--------------------------------------------------------
* **Validity window** — keep responses where ``survey_response_date <= date + 1
  day`` (legacy ``survey_response_date <= case_closure_time + INTERVAL 1 DAY``).
  Rows with a NULL ``survey_response_date`` fall outside the window.
* **One response per case** — dedup to a single row per ``(agent, case_number)``
  (legacy ``COUNT(DISTINCT case_number)``), preferring a scored row and the
  latest ``survey_response_date``.
* **Classification** — promoter ``>= 9`` (+1), detractor ``<= 6`` (−1), neutral
  7–8 (0); a response is *valid* (denominator) when ``survey_score`` is not null.

NOT applied here (future Adjustments layer)
-------------------------------------------
* The outage-date exclusion ``date = 2026-03-27`` (legacy drops it for "general
  access problems") — deferred to match the other metric modules.

Output — one row per (agent, date_reference, granularity)
---------------------------------------------------------
Tidy "long" metric shape shared across the metrics layer:
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``. ``numerator`` =
promoters − detractors, ``denominator`` = valid responses, ``metric_value`` =
``numerator / denominator * 100`` (NULL when the denominator is 0).
"""

from __future__ import annotations

import pandas as pd

from metric_utils import aggregate_long, empty_metric_frame

METRIC_NAME = "tnps"

# Human tNPS only applies to Social Media.
TNPS_TEAM = "social media"

PROMOTER_MIN_SCORE = 9
DETRACTOR_MAX_SCORE = 6


def compute_tnps(tnps_responses: pd.DataFrame) -> pd.DataFrame:
    """Compute the Human tNPS metric at all granularities.

    Args:
        tnps_responses: the ``io_tnps_responses_raw`` table (one row per response).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    if tnps_responses.empty:
        return empty_metric_frame()

    work = tnps_responses.copy()
    work = work[work["team"].astype("string").str.lower() == TNPS_TEAM].copy()
    if work.empty:
        return empty_metric_frame()

    # --- validity window: survey_response_date <= date + 1 day --------------
    def _naive(series: pd.Series) -> pd.Series:
        s = pd.to_datetime(series)
        if getattr(s.dt, "tz", None) is not None:
            s = s.dt.tz_localize(None)
        return s

    close = _naive(work["date"])
    resp = _naive(work["survey_response_date"])
    work = work.loc[resp <= (close + pd.Timedelta(days=1))].copy()
    if work.empty:
        return empty_metric_frame()

    work["survey_score"] = pd.to_numeric(work["survey_score"], errors="coerce")

    # --- one response per (agent, case_number): prefer scored + latest ------
    work["_scored"] = work["survey_score"].notna()
    work["_resp"] = pd.to_datetime(work["survey_response_date"])
    work = (
        work.sort_values(["_scored", "_resp"])
        .groupby(["agent", "case_number"], as_index=False, dropna=False)
        .tail(1)
        .copy()
    )

    # --- classification flags ----------------------------------------------
    valid = work["survey_score"].notna()
    promoter = valid & (work["survey_score"] >= PROMOTER_MIN_SCORE)
    detractor = valid & (work["survey_score"] <= DETRACTOR_MAX_SCORE)
    work["valid_flag"] = valid.astype("int64")
    work["net_flag"] = promoter.astype("int64") - detractor.astype("int64")

    return aggregate_long(
        work,
        numerator_col="net_flag",
        denominator_col="valid_flag",
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
