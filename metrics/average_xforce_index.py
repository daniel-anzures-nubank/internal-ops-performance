"""average_xforce_index — the XPLead-level Average XForce Index (all teams).

Legacy ``average_index_xforce``: the simple mean of the **XForce-level
``xforce_index``** rolled up to the XPLead.

    Average XForce Index = AVG(xforce_index) per xplead

It reads the finished XForce Index table (``io_xforce_index_metric``) and
averages ``metric_value`` per ``(team, xplead, date_reference,
date_granularity)``. Applies to **all four teams** (Core / Fraud / Social Media
/ Content).

Output grain
------------
One row per ``(team, xplead)`` per period. ``agent``, ``xforce``, ``squad``,
``district``, ``shift`` are NULL. All six granularities (the legacy notebook
materialized week + month; the metric layer emits whatever the XForce Index
provides).

Numerator / denominator convention
-----------------------------------
Legacy left ``numerator`` / ``denominator`` NULL and used ``AVG()``. We instead
fill ``numerator = Σ xforce_index`` and ``denominator = XForce count`` so the row
is self-describing; ``metric_value = numerator / denominator`` is the identical
mean.
"""

from __future__ import annotations

import pandas as pd

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "average_xforce_index"
XFORCE_INDEX_METRIC = "xforce_index"


def compute_average_xforce_index(xforce_index: pd.DataFrame) -> pd.DataFrame:
    """Average the XForce Index to the XPLead level.

    Args:
        xforce_index: ``io_xforce_index_metric`` — XForce-grain index rows
            (``metric_value`` is each XForce's index, all six granularities).

    Returns:
        Tidy long-format metric rows (XPLead grain), all granularities present
        in the input.
    """
    if xforce_index is None or xforce_index.empty:
        return empty_metric_frame()

    work = xforce_index
    if "metric" in work.columns:
        work = work[work["metric"] == XFORCE_INDEX_METRIC]
    work = work.copy()
    work["metric_value"] = pd.to_numeric(work["metric_value"], errors="coerce")
    work = work[work["metric_value"].notna()]
    if work.empty:
        return empty_metric_frame()

    keys = ["team", "xplead", "date_reference", "date_granularity"]
    grp = work.groupby(keys, as_index=False, dropna=False).agg(
        numerator=("metric_value", "sum"),
        denominator=("metric_value", "size"),
    )

    out = pd.DataFrame(index=grp.index)
    out["agent"] = None
    out["xforce"] = None
    out["xplead"] = grp["xplead"].values
    out["team"] = grp["team"].values
    out["squad"] = None
    out["district"] = None
    out["shift"] = None
    out["date_reference"] = grp["date_reference"].values
    out["date_granularity"] = grp["date_granularity"].values
    out["metric"] = METRIC_NAME
    out["numerator"] = grp["numerator"].astype(float).values
    out["denominator"] = grp["denominator"].astype(float).values
    out["metric_value"] = (
        (grp["numerator"] / grp["denominator"]).where(grp["denominator"] > 0).values
    )

    return (
        out[list(METRIC_COLUMNS)]
        .sort_values(
            ["date_granularity", "date_reference", "team", "xplead"],
            na_position="last",
        )
        .reset_index(drop=True)
    )


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
