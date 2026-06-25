"""xforce_index — the composite XForce Index (all teams).

The headline XForce score: the **mean of up to four normalized 0-100
components**, per XForce per period. (Legacy ``index_xforce``; renamed
``xforce_index`` to match the ``xpeer_index`` naming convention.)

    xforce_index = (shrinkage + xpeers_in_target + average_xpeer_index
                    [+ improved_benchmark]) / N

where ``N`` is the number of active components (3 or 4) and each component is
mapped to a 0-100 scale:

================  ================================================  ============================
component         transform (legacy ``index_xforces_final``)        source
================  ================================================  ============================
shrinkage         ``<= 20 → 100``; ``> 20 → 120 - shrinkage``;       ``io_shrinkage_metric``
                  NULL → 0                                          (agent rows, summed → XForce)
xpeers_in_target  raw value, NULL → 0                                ``io_xpeers_in_target_metric``
average_xpeer_    raw value, NULL → 0                                ``io_average_xpeer_index_metric``
index
improved_         ``>= 60 → 100``; ``< 60 → improved / 0.6``;         ``io_improved_benchmarks_metric``
benchmark         NULL → 0                                          (``improved_benchmark_xforce``)
================  ================================================  ============================

``shrinkage_xforce`` is the slot-weighted XForce shrinkage — we **sum** the
agent ``io_shrinkage_metric`` numerator/denominator per XForce (identical to
legacy's ``SUM(shrinkage_slot)/SUM(required_slot)``), not an average of agent
percentages.

The improved_benchmark component (Core / Fraud era logic)
--------------------------------------------------------
We add the improved_benchmark component **iff a matching
``improved_benchmark_xforce`` row exists** for the bucket. Because
``improved_benchmarks`` is Core/Fraud-only, month-only, and already suppressed
after each team's cutover (Core ≥ 2026-04, Fraud ≥ 2026-05), this presence test
encodes all the business rules with no extra date logic:

* **Core**: 4 components for **month** buckets Jan–Mar 2026; 3 thereafter.
  The approved ``david.fernandez`` Apr-2026 carve-out also removes Improved
  Benchmarks from his XForce Index if that component is present.
* **Fraud**: 4 components for **month** buckets Jan–Apr 2026; 3 thereafter.
* **Social Media / Content**: always **3 components** (no improved_benchmark).
* Non-month granularities (day / week / quarter / semester / year) are always
  **3 components** — Improved Benchmarks is month-grain only.

Output grain
------------
One row per ``(team, xforce, xplead)`` per period (driven by the XForce-rolled
shrinkage). ``agent``, ``squad``, ``district``, ``shift`` are NULL.
``numerator`` = Σ active components, ``denominator`` = ``100 * N`` (300 or 400),
``metric_value`` = ``numerator / denominator * 100`` (the component mean).
"""

from __future__ import annotations

import pandas as pd

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "xforce_index"
IMPROVED_BENCHMARK_XFORCE_METRIC = "improved_benchmark_xforce"

DAVID_IMPROVED_SUPPRESSION_XPLEAD = "david.fernandez"
DAVID_IMPROVED_SUPPRESSION_MONTH = pd.Timestamp("2026-04-01")

_JOIN_KEYS = ["xforce", "date_reference", "date_granularity"]


def _shrinkage_component(s: pd.Series) -> pd.Series:
    """``<= 20 → 100``; ``> 20 → 120 - shrinkage``; NULL → 0."""
    out = pd.Series(0.0, index=s.index)
    out = out.mask(s <= 20, 100.0)
    out = out.mask(s > 20, 120.0 - s)
    return out


def _improved_component(s: pd.Series) -> pd.Series:
    """``>= 60 → 100``; ``< 60 → improved / 0.6``; NULL → 0."""
    out = pd.Series(0.0, index=s.index)
    out = out.mask(s >= 60, 100.0)
    out = out.mask(s < 60, s / 0.6)
    return out


def _xforce_value(
    df: pd.DataFrame | None, col: str, *, metric: str | None = None
) -> pd.DataFrame:
    """Project an XForce-grain metric table to ``_JOIN_KEYS`` + ``col``."""
    cols = pd.DataFrame(columns=[*_JOIN_KEYS, col])
    if df is None or df.empty:
        return cols
    work = df
    if metric is not None:
        work = work[work["metric"] == metric]
    if work.empty:
        return cols
    out = work[[*_JOIN_KEYS, "metric_value"]].rename(columns={"metric_value": col})
    # One row per XForce/period already; guard against accidental dupes.
    return out.drop_duplicates(_JOIN_KEYS)


def compute_xforce_index(
    shrinkage: pd.DataFrame,
    xpeers_in_target: pd.DataFrame,
    average_xpeer_index: pd.DataFrame,
    improved_benchmarks: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute the composite XForce Index at all granularities.

    Args:
        shrinkage: ``io_shrinkage_metric`` (agent grain) — the driver; summed to
            the XForce for the slot-weighted shrinkage.
        xpeers_in_target: ``io_xpeers_in_target_metric`` (XForce grain).
        average_xpeer_index: ``io_average_xpeer_index_metric`` (XForce grain).
        improved_benchmarks: ``io_improved_benchmarks_metric`` (we use only the
            ``improved_benchmark_xforce`` rows). May be ``None``/empty.

    Returns:
        Tidy long-format metric rows (XForce grain), all granularities present
        in the shrinkage input.
    """
    if shrinkage is None or shrinkage.empty:
        return empty_metric_frame()

    # The shrinkage table now also carries shrinkage_xforce / shrinkage_xplead
    # roll-up rows; roll up from the **agent** rows only to avoid double counting.
    shrinkage = shrinkage[shrinkage["metric"] == "shrinkage"]
    if shrinkage.empty:
        return empty_metric_frame()

    keys = ["team", "xforce", "xplead", "date_reference", "date_granularity"]
    sh = shrinkage.groupby(keys, as_index=False, dropna=False).agg(
        _num=("numerator", "sum"), _den=("denominator", "sum")
    )
    sh["shrinkage_xforce"] = (sh["_num"] / sh["_den"]).where(sh["_den"] > 0) * 100
    base = sh.drop(columns=["_num", "_den"])

    base = base.merge(
        _xforce_value(xpeers_in_target, "xit", metric="xpeers_in_target"),
        on=_JOIN_KEYS,
        how="left",
    )
    base = base.merge(
        _xforce_value(average_xpeer_index, "avg_idx"), on=_JOIN_KEYS, how="left"
    )
    base = base.merge(
        _xforce_value(
            improved_benchmarks, "improved", metric=IMPROVED_BENCHMARK_XFORCE_METRIC
        ),
        on=_JOIN_KEYS,
        how="left",
    )

    s = _shrinkage_component(pd.to_numeric(base["shrinkage_xforce"], errors="coerce"))
    x = pd.to_numeric(base["xit"], errors="coerce").fillna(0.0)
    a = pd.to_numeric(base["avg_idx"], errors="coerce").fillna(0.0)

    improved = pd.to_numeric(base["improved"], errors="coerce")
    has_improved = improved.notna()
    # Approved manual adjustment from `Ajustes Index`: for Apr 2026,
    # david.fernandez's XForces exclude Improved Benchmarks from XForce Index.
    # This is intentionally narrow; it only changes the composite index divisor
    # and numerator, not the standalone improved_benchmarks metric.
    bucket_month = (
        pd.to_datetime(base["date_reference"]).dt.to_period("M").dt.to_timestamp()
    )
    suppress_david_improved = (
        (base["xplead"] == DAVID_IMPROVED_SUPPRESSION_XPLEAD)
        & (bucket_month == DAVID_IMPROVED_SUPPRESSION_MONTH)
        & (base["date_granularity"] == "month")
    )
    has_improved = has_improved & ~suppress_david_improved
    i = _improved_component(improved)

    num = s + x + a + i.where(has_improved, 0.0)
    n_components = pd.Series(3, index=base.index) + has_improved.astype(int)
    den = (n_components * 100).astype(float)

    out = pd.DataFrame(index=base.index)
    out["agent"] = None
    out["xforce"] = base["xforce"].values
    out["xplead"] = base["xplead"].values
    out["team"] = base["team"].values
    out["squad"] = None
    out["district"] = None
    out["shift"] = None
    out["date_reference"] = base["date_reference"].values
    out["date_granularity"] = base["date_granularity"].values
    out["metric"] = METRIC_NAME
    out["numerator"] = num.astype(float).values
    out["denominator"] = den.values
    out["metric_value"] = ((num / den) * 100).values

    return (
        out[list(METRIC_COLUMNS)]
        .sort_values(
            ["date_granularity", "date_reference", "team", "xforce"],
            na_position="last",
        )
        .reset_index(drop=True)
    )


IO_XFORCE_INDEX_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
