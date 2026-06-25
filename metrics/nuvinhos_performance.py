"""nuvinhos_performance — the Nuvinhos Performance metric (all teams).

Compares the average **Xpeer Index** of *Nuvinhos* (recently-hired / recently-
moved agents) against the average Xpeer Index of tenured ("old") agents, so
managers can see whether new agents are ramping:

    Nuvinhos Performance = avg Index(Nuvinhos) / avg Index(Old) * 100

This is an **index-level** metric (no agent grain). Like Improved Benchmarks it
reads a finished metric (here ``io_xpeer_index_metric``) rather than a raw
table, plus the roster tenure from ``agent_information``.

Who is a Nuvinho? (legacy ``nuvinhos_performance_base``)
-------------------------------------------------------
An agent is a *Nuvinho* for a bucket when the bucket's **month** falls in
``[month(last_change_date), month(last_change_date) + 2 months]`` — i.e. the
hire/squad-change month plus the next two. Everyone else (incl. agents with no
``last_change_date``, e.g. the temp Content roster) is *old*.

Output grains (one ``io_*`` table, three ``metric`` names)
----------------------------------------------------------
We implement the **documented formula** — a flat ``mean(Index | Nuvinho) /
mean(Index | old)`` over agents — rather than the legacy SQL's two-level cohort
average. (Legacy first averages per ``(xforce, xplead, squad, district,
nuvinho)`` cohort, then averages those means with the opposite-flag zeros
included; that biases the ratio by the *number* of Nuvinho vs old cohorts and
collapses to the flat formula only when an XForce has one cohort. The
definitions doc is the source of truth, so we average agents directly.)

Three roll-ups are emitted for **every** team (legacy only built the XForce
roll-up for Core/Fraud; SM/Content also built squad + district — we extend
squad + district to all teams for a consistent table):

* ``nuvinhos_performance``           — per ``(team, xforce, xplead)``
* ``nuvinhos_performance_squad``     — per ``(team, squad)``
* ``nuvinhos_performance_district``  — per ``(team, district)``

``agent`` and ``shift`` are always NULL; the dimensions not in a roll-up's key
are NULL too (e.g. squad/district NULL on the XForce roll-up).

Output convention
-----------------
``numerator`` = mean Index of Nuvinhos, ``denominator`` = mean Index of old
agents (both flat averages over the agents in the roll-up key),
``metric_value`` = ``numerator / denominator * 100`` (NULL when there are no old
agents). When a roll-up key has no Nuvinhos, ``numerator`` is 0 and
``metric_value`` is 0.

NOT applied here (future Adjustments layer)
-------------------------------------------
Per-agent legacy carve-outs inherited from the Xpeer Index. Content yields a
degenerate result (no Nuvinhos) until the real Content roster with hire dates
replaces the temp BDX source.

Output — tidy long format
-------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
"""

from __future__ import annotations

import pandas as pd

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_XFORCE = "nuvinhos_performance"
METRIC_SQUAD = "nuvinhos_performance_squad"
METRIC_DISTRICT = "nuvinhos_performance_district"

# The Xpeer Index rows this metric is derived from.
XPEER_INDEX_METRIC = "xpeer_index"

# Nuvinho window: the change month plus the next N months (legacy INTERVAL 2 MONTH).
NUVINHO_WINDOW_MONTHS = 2


def _month_start(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s)
    if getattr(d.dt, "tz", None) is not None:
        d = d.dt.tz_localize(None)
    return d.dt.to_period("M").dt.to_timestamp()


def _rollup(idx: pd.DataFrame, *, by: list[str], metric_name: str) -> pd.DataFrame:
    """Flat mean Index of Nuvinhos / old agents per ``(team, *by, date)``."""
    grp_keys = ["team", *by, "date_reference", "date_granularity"]
    work = idx.copy()
    work["_nuv"] = work["metric_value"].where(work["nuvinho"])
    work["_old"] = work["metric_value"].where(~work["nuvinho"])
    grp = work.groupby(grp_keys, as_index=False, dropna=False).agg(
        numerator=("_nuv", "mean"),
        denominator=("_old", "mean"),
    )
    # No Nuvinhos in the cohort → numerator 0 (rather than NaN) so the ratio is 0.
    grp["numerator"] = grp["numerator"].fillna(0.0)
    # Drop roll-up rows whose key dimension is missing (e.g. NULL squad).
    for col in by:
        grp = grp[grp[col].notna()]
    grp = grp.reset_index(drop=True)

    out = pd.DataFrame(index=grp.index)
    out["agent"] = None
    out["xforce"] = grp["xforce"].values if "xforce" in by else None
    out["xplead"] = grp["xplead"].values if "xplead" in by else None
    out["team"] = grp["team"].values
    out["squad"] = grp["squad"].values if "squad" in by else None
    out["district"] = grp["district"].values if "district" in by else None
    out["shift"] = None
    out["date_reference"] = grp["date_reference"].values
    out["date_granularity"] = grp["date_granularity"].values
    out["metric"] = metric_name
    out["numerator"] = grp["numerator"].astype("float64").values
    out["denominator"] = grp["denominator"].astype("float64").values
    out["metric_value"] = (
        (out["numerator"] / out["denominator"]).where(out["denominator"] > 0) * 100
    )
    return out[list(METRIC_COLUMNS)]


def compute_nuvinhos_performance(
    xpeer_index: pd.DataFrame,
    agent_tenure: pd.DataFrame,
) -> pd.DataFrame:
    """Compute Nuvinhos Performance at XForce / squad / district roll-ups.

    Args:
        xpeer_index: ``io_xpeer_index_metric`` (agent-level Xpeer Index, all
            granularities). Only ``metric == 'xpeer_index'`` rows are used.
        agent_tenure: the ``agent_information`` extractor (one row per
            ``(agent, snapshot_month)``), providing ``last_change_date``.

    Returns:
        Tidy long-format metric rows (the three roll-ups), all granularities
        present in ``xpeer_index``.
    """
    if xpeer_index is None or xpeer_index.empty:
        return empty_metric_frame()

    idx = xpeer_index[xpeer_index["metric"] == XPEER_INDEX_METRIC].copy()
    if idx.empty:
        return empty_metric_frame()

    idx["_snap"] = _month_start(idx["date_reference"])

    # Attach each agent's last_change_date for the bucket's month.
    ten = agent_tenure[["agent", "snapshot_month", "last_change_date"]].copy()
    ten["snapshot_month"] = _month_start(ten["snapshot_month"])
    ten = ten.drop_duplicates(["agent", "snapshot_month"], keep="last")
    idx = idx.merge(
        ten,
        left_on=["agent", "_snap"],
        right_on=["agent", "snapshot_month"],
        how="left",
    )

    lc_month = _month_start(idx["last_change_date"])
    window_end = lc_month + pd.offsets.MonthBegin(NUVINHO_WINDOW_MONTHS)
    idx["nuvinho"] = (
        (idx["_snap"] >= lc_month) & (idx["_snap"] <= window_end)
    ).fillna(False)

    rows = pd.concat(
        [
            _rollup(idx, by=["xforce", "xplead"], metric_name=METRIC_XFORCE),
            _rollup(idx, by=["squad"], metric_name=METRIC_SQUAD),
            _rollup(idx, by=["district"], metric_name=METRIC_DISTRICT),
        ],
        ignore_index=True,
    )
    return rows.sort_values(
        ["metric", "date_granularity", "date_reference", "team"], na_position="last"
    ).reset_index(drop=True)


IO_NUVINHOS_PERFORMANCE_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
