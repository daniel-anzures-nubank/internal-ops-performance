"""average_xpeer_index — the XForce-level Average Xpeer Index (all teams).

Legacy ``average_index_agent``: the simple mean of the **agent-level Xpeer
Index** rolled up to the XForce.

    Average Xpeer Index = AVG(agent Xpeer Index) per (xforce, xplead)

It reads the finished agent-level index table (``io_xpeer_index_metric``) and
averages ``metric_value`` per ``(team, xforce, xplead, date_reference,
date_granularity)``. Applies to **all four teams** (Core / Fraud / Social Media
/ Content) — the agent index already encodes each team's era-specific
composition, so this layer is a pure average over whatever agents have an index.

Output grain
------------
One row per ``(team, xforce, xplead)`` per period. ``agent``, ``squad``,
``district``, ``shift`` are NULL. All six granularities (``day`` / ``week`` /
``month`` / ``quarter`` / ``semester`` / ``year``) — the legacy notebook only
materialized week + month, but our metric layer emits whatever the agent index
provides.

Numerator / denominator convention
-----------------------------------
Legacy left ``numerator`` / ``denominator`` NULL and used ``AVG()``. We instead
fill ``numerator = Σ agent index`` and ``denominator = agent count`` so the row
is self-describing; ``metric_value = numerator / denominator`` is the identical
mean.
"""

from __future__ import annotations

import pandas as pd

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "average_xpeer_index"


def compute_average_xpeer_index(xpeer_index: pd.DataFrame) -> pd.DataFrame:
    """Average the agent-level Xpeer Index to the XForce level.

    Args:
        xpeer_index: ``io_xpeer_index_metric`` — agent-level index rows
            (``metric_value`` is each agent's index, all six granularities).

    Returns:
        Tidy long-format metric rows (XForce grain), all granularities present
        in the input.
    """
    if xpeer_index is None or xpeer_index.empty:
        return empty_metric_frame()

    work = xpeer_index.copy()
    work["metric_value"] = pd.to_numeric(work["metric_value"], errors="coerce")
    # Agents with no computable index are dropped from the average (AVG ignores
    # NULLs in SQL).
    work = work[work["metric_value"].notna()]
    if work.empty:
        return empty_metric_frame()

    keys = ["team", "xforce", "xplead", "date_reference", "date_granularity"]
    grp = work.groupby(keys, as_index=False, dropna=False).agg(
        numerator=("metric_value", "sum"),
        denominator=("metric_value", "size"),
    )

    out = pd.DataFrame(index=grp.index)
    out["agent"] = None
    out["xforce"] = grp["xforce"].values
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
            ["date_granularity", "date_reference", "team", "xforce"],
            na_position="last",
        )
        .reset_index(drop=True)
    )


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
