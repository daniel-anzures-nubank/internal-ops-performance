"""wows — the WoWs performance metric (Social Media only).

(Module file is ``wows_metric.py`` — not ``wows.py`` — because the raw-layer
module is already ``metrics_data/wows.py``; the two share a name and would
collide on the import path. The metric itself is still ``wows``.)

Part of the **metrics layer**: consumes the raw ``io_wows_raw`` table (one row
per WoW experience) and produces a finished, agent-level metric at day / week /
month / quarter / semester / year grain.

WoWs is a **count** metric, not a ratio — the number of distinct WoW experiences
an agent delivered in the period:

    wows = COUNT(DISTINCT case_id)

**Monthly target ≥ 5.** Only **Social Media** has WoWs (the source sheet only
contains social agents' WoWs — see ``docs/metrics_definitions.md``).

Output convention (differs from the ratio metrics)
--------------------------------------------------
Because WoWs is a raw count, ``metric_value`` is the **count itself** (it is
*not* ``numerator / denominator * 100``):
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
* **Count** — ``COUNT(DISTINCT case_id)`` per ``(agent, period)``.

NOT applied here (future Adjustments layer)
-------------------------------------------
* The outage-date exclusion ``date = 2026-03-27`` (legacy drops it for "general
  access problems") — deferred to match the other metric modules.

Output — one row per (agent, date_reference, granularity)
---------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
"""

from __future__ import annotations

import pandas as pd

from metric_utils import (
    GRANULARITIES,
    METRIC_COLUMNS,
    bucket_dates,
    empty_metric_frame,
    latest_dims,
)

METRIC_NAME = "wows"

# WoWs only apply to Social Media.
WOWS_TEAM = "social media"

# Monthly target (legacy ``monthly_target`` constant), carried in ``denominator``.
MONTHLY_TARGET = 5


def _aggregate(work: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """One WoWs row per (agent, bucket) for a single granularity."""
    work = work.copy()
    work["_date"] = pd.to_datetime(work["date"])
    if getattr(work["_date"].dt, "tz", None) is not None:
        work["_date"] = work["_date"].dt.tz_localize(None)
    work["date_reference"] = bucket_dates(work["_date"], granularity)

    sums = work.groupby(["agent", "date_reference"], as_index=False, dropna=False).agg(
        numerator=("case_id", "nunique")
    )
    sums["numerator"] = sums["numerator"].astype("float64")
    sums["denominator"] = float(MONTHLY_TARGET)
    sums["metric_value"] = sums["numerator"]

    out = sums.merge(latest_dims(work), on=["agent", "date_reference"], how="left")
    out["date_granularity"] = granularity
    out["metric"] = METRIC_NAME
    out["date_reference"] = out["date_reference"].dt.date
    return out[list(METRIC_COLUMNS)]


def compute_wows(wows: pd.DataFrame) -> pd.DataFrame:
    """Compute the WoWs metric at all granularities.

    Args:
        wows: the ``io_wows_raw`` table (one row per WoW experience).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    if wows.empty:
        return empty_metric_frame()

    work = wows[wows["team"].astype("string").str.lower() == WOWS_TEAM].copy()
    if work.empty:
        return empty_metric_frame()

    parts = [_aggregate(work, g) for g in GRANULARITIES]
    result = pd.concat(parts, ignore_index=True)
    return result.sort_values(
        ["date_granularity", "date_reference", "agent"]
    ).reset_index(drop=True)


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
