"""xpeer_index — the agent-level Xpeer Index (all teams).

The Xpeer Index folds an agent's other performance metrics into a single
comparable score (legacy ``index_agent``). Unlike every other module in this
folder it does **not** read an ``io_*_raw`` table — it consumes the already
aggregated **per-agent metric tables** and combines their ``metric_value``s per
``(agent, date_reference, date_granularity)``.

Inputs (each a tidy long ``io_*_metric`` table, agent grain)
-----------------------------------------------------------
* ``io_adherence_metric``              → Adherence (the **driver**; an agent is
  in the index iff it has an Adherence row for that bucket)
* ``io_ntpj_metric``                   → NTPJ (Core / Fraud / Content)
* ``io_normalized_occupancy_metric``   → NO (all teams, from March 2026)
* ``io_quality_metric``                → Quality (Core / Fraud / Social Media)
* ``io_tnps_metric``                   → Human tNPS (Social Media)
* ``io_wows_metric``                   → WoWs (Social Media)
* ``io_content_csat_metric``           → CSAT, the Content "quality" term

Component transforms (legacy ``index_agents_final``)
----------------------------------------------------
* **Adherence**: ``COALESCE(0)``, taken as-is.
* **NTPJ** (lower-is-better, folded around 100): ``≤100 → 100``;
  ``100 < x ≤ 200 → 200 − x``; ``>200 or NULL → 0``.
* **NO** (truncated): ``≥100 → 100``; ``<100 → x``; ``NULL → 0``.
* **WoWs** (Social Media): ``≥5 → 100``; ``<5 → x/5*100``; ``NULL → 0``.
* **tNPS / Quality / CSAT**: used raw (tNPS may be negative).

Composition — which terms enter the average, by team and **era**
----------------------------------------------------------------
The index is a simple mean of the included components. The roster of components
grew over the 2026 rollout, so it is **anchored on the bucket's month**:

* **Core / Fraud**: Adherence + NTPJ always; ``+ Quality`` from **Feb 2026**
  (when present); ``+ NO`` from **March 2026**, except the approved
  ``nitza.zarza`` Apr-May 2026 carve-out.
* **Content**:      Adherence + NTPJ always; ``+ NO`` and ``+ CSAT`` (when
  present) from **March 2026** (Jan & Feb are Adherence + NTPJ only).
* **Social Media**: Adherence + WoWs always; ``+ tNPS`` whenever present;
  ``+ Quality`` from **Feb 2026** (when present); ``+ NO`` from **March 2026**.

Quality / CSAT / tNPS terms drop out of both the sum **and** the divisor when
the agent has no value for the bucket. NO is always counted in the divisor once
its era starts (a missing NO contributes 0).

Era anchoring across granularities
-----------------------------------
``day`` / ``week`` / ``month`` buckets sit inside one calendar month, so the era
is that month. ``quarter`` / ``semester`` / ``year`` buckets straddle the
cutovers, so they anchor on the bucket's **last month** (its end) — a longer
aggregation therefore includes every component active by the end of the period.

Output convention
------------------
To keep the shared ``metric_value = numerator / denominator * 100`` contract,
``numerator`` is the **sum of the included component %s** and ``denominator`` is
``n_components * 100``; ``metric_value`` is then their mean (the Index %).

Output — tidy long format, one row per (agent, date_reference, granularity)
---------------------------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from metric_utils import DIM_COLS, METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "xpeer_index"

# Rollout cutovers (anchored on the bucket's month, see module docstring).
NO_CUTOVER = pd.Timestamp("2026-03-01")  # NO joins the Index
QUALITY_CUTOVER = pd.Timestamp("2026-02-01")  # Quality joins (Core / Fraud / SM)
QUALITY_CUTOVER_CONTENT = pd.Timestamp("2026-03-01")  # CSAT joins (Content)

# The Index is a 2026 construct; earlier buckets have no defined era.
ERA_FLOOR = pd.Timestamp("2026-01-01")

SOCIAL_MEDIA = "social media"
CONTENT = "content"

NITZA_NO_SUPPRESSION_AGENT = "nitza.zarza"
NITZA_NO_SUPPRESSION_MONTHS = {
    pd.Timestamp("2026-04-01"),
    pd.Timestamp("2026-05-01"),
}

_KEYS = ["agent", "date_reference", "date_granularity"]
# Component metric tables → the column name we stage them under.
_COMPONENTS = {
    "ntpj": "ntpj",
    "normalized_occupancy": "nocc",
    "quality": "quality",
    "tnps": "tnps",
    "wows": "wows",
    "content_csat": "csat",
}


def _fold_ntpj(s: pd.Series) -> pd.Series:
    """Fold NTPJ around 100 (lower-is-better); NULL/>200 → 0."""
    out = pd.Series(0.0, index=s.index)
    out = out.mask(s <= 100, 100.0)
    out = out.mask((s > 100) & (s <= 200), 200.0 - s)
    return out


def _truncate_nocc(s: pd.Series) -> pd.Series:
    """Truncate NO at 100; NULL → 0."""
    out = pd.Series(0.0, index=s.index)
    out = out.mask(s >= 100, 100.0)
    out = out.mask(s < 100, s)
    return out


def _fold_wows(s: pd.Series) -> pd.Series:
    """WoWs count → 0-100 (target 5/month); NULL → 0."""
    out = pd.Series(0.0, index=s.index)
    out = out.mask(s >= 5, 100.0)
    out = out.mask(s < 5, s / 5.0 * 100.0)
    return out


def _era_anchor_month(date_reference: pd.Series, granularity: pd.Series) -> pd.Series:
    """The month that decides a bucket's era (its last month, see docstring)."""
    dr = pd.to_datetime(date_reference)
    if getattr(dr.dt, "tz", None) is not None:
        dr = dr.dt.tz_localize(None)
    anchor = dr.copy()
    anchor = anchor.mask(granularity == "quarter", dr + pd.offsets.MonthBegin(2))
    anchor = anchor.mask(granularity == "semester", dr + pd.offsets.MonthBegin(5))
    anchor = anchor.mask(granularity == "year", dr + pd.offsets.MonthBegin(11))
    return anchor.dt.to_period("M").dt.to_timestamp()


def _component(df: pd.DataFrame | None, name: str) -> pd.DataFrame | None:
    """Project a metric table to its keys + a renamed ``metric_value`` column."""
    if df is None or df.empty:
        return None
    return df[[*_KEYS, "metric_value"]].rename(columns={"metric_value": name})


def compute_xpeer_index(
    adherence: pd.DataFrame,
    ntpj: pd.DataFrame | None = None,
    normalized_occupancy: pd.DataFrame | None = None,
    quality: pd.DataFrame | None = None,
    tnps: pd.DataFrame | None = None,
    wows: pd.DataFrame | None = None,
    content_csat: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute the agent-level Xpeer Index at all granularities.

    Args:
        adherence: ``io_adherence_metric`` — the driver. An agent appears in the
            Index iff it has an Adherence row for the bucket; dimensions and
            ``team`` are taken from this table.
        ntpj / normalized_occupancy / quality / tnps / wows / content_csat: the
            corresponding ``io_*_metric`` tables. Any may be ``None``/empty (the
            term is then simply absent for the affected teams).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    if adherence is None or adherence.empty:
        return empty_metric_frame()

    base = adherence[[*["agent"], *DIM_COLS, *["date_reference", "date_granularity"]]].copy()
    base["adherence"] = adherence["metric_value"].to_numpy()

    sources = {
        "ntpj": ntpj,
        "normalized_occupancy": normalized_occupancy,
        "quality": quality,
        "tnps": tnps,
        "wows": wows,
        "content_csat": content_csat,
    }
    for metric_table, col in _COMPONENTS.items():
        comp = _component(sources[metric_table], col)
        if comp is not None:
            # Guard against duplicate keys in a component table.
            comp = comp.drop_duplicates(subset=_KEYS, keep="last")
            base = base.merge(comp, on=_KEYS, how="left")
        if col not in base:
            base[col] = np.nan

    # Restrict to the 2026+ window where the era rollout is defined.
    era = _era_anchor_month(base["date_reference"], base["date_granularity"])
    base = base.loc[era >= ERA_FLOOR].reset_index(drop=True)
    if base.empty:
        return empty_metric_frame()
    era = _era_anchor_month(base["date_reference"], base["date_granularity"])

    team = base["team"].astype("string").str.lower()
    is_sm = team == SOCIAL_MEDIA
    is_content = team == CONTENT
    # core, fraud, and any unrecognized team use the Core/Fraud composition.
    is_cf = ~(is_sm | is_content)

    mar_plus = era >= NO_CUTOVER
    qual_cutover = pd.Series(QUALITY_CUTOVER, index=base.index).mask(
        is_content, QUALITY_CUTOVER_CONTENT
    )
    qual_era_ok = era >= qual_cutover

    adh = base["adherence"].astype(float).fillna(0.0)
    ntpj_t = _fold_ntpj(base["ntpj"])
    nocc_t = _truncate_nocc(base["nocc"])
    wows_t = _fold_wows(base["wows"])
    tnps_v = base["tnps"].astype(float)
    # Content's quality term is CSAT; everyone else's is Playvox Quality.
    qual_v = base["quality"].astype(float).where(~is_content, base["csat"].astype(float))

    # Accumulate the numerator (sum of included %s) and the term count.
    num = adh.copy()
    cnt = pd.Series(1, index=base.index, dtype="int64")  # Adherence is always in.

    use_ntpj = is_cf | is_content
    num = num + ntpj_t.where(use_ntpj, 0.0)
    cnt = cnt + use_ntpj.astype("int64")

    num = num + wows_t.where(is_sm, 0.0)
    cnt = cnt + is_sm.astype("int64")

    use_tnps = is_sm & tnps_v.notna()
    num = num + tnps_v.where(use_tnps, 0.0)
    cnt = cnt + use_tnps.astype("int64")

    # Approved manual adjustment from `Ajustes Index`: for Apr-May 2026, NO is
    # removed from nitza.zarza's Xpeer Index and the index is recomputed with
    # the remaining active components (legacy index_agent behavior).
    suppress_nitza_no = (base["agent"] == NITZA_NO_SUPPRESSION_AGENT) & era.isin(
        NITZA_NO_SUPPRESSION_MONTHS
    )
    use_nocc = mar_plus & ~suppress_nitza_no
    num = num + nocc_t.where(use_nocc, 0.0)
    cnt = cnt + use_nocc.astype("int64")

    use_qual = qual_v.notna() & qual_era_ok
    num = num + qual_v.where(use_qual, 0.0)
    cnt = cnt + use_qual.astype("int64")

    out = base[["agent", *DIM_COLS, "date_reference", "date_granularity"]].copy()
    out["metric"] = METRIC_NAME
    out["numerator"] = num.astype(float)
    out["denominator"] = (cnt * 100).astype(float)
    out["metric_value"] = (out["numerator"] / out["denominator"]) * 100.0

    return (
        out[list(METRIC_COLUMNS)]
        .sort_values(["date_granularity", "date_reference", "agent"])
        .reset_index(drop=True)
    )


IO_XPEER_INDEX_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
