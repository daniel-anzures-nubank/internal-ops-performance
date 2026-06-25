"""xpeers_in_target — the XForce-level Xpeers In Target metric (Core/Fraud + SM).

Measures, per XForce, the share of its Xpeers' **metric targets** that were met:

    Xpeers In Target = Σ targets achieved / Σ targets * 100

A "target" is one agent-metric: for every active component metric, each Xpeer
contributes one target (the denominator) and counts as achieved (the numerator)
when their value clears that metric's threshold. **Target ≥ 70%.**

Like the Xpeer Index, this reads the finished **agent-level metric tables**
(``io_*_metric``) rather than a raw table, then aggregates to the XForce.

Component targets (legacy ``*_xforce`` in-target counts)
-------------------------------------------------------
======================  ==========  =============================
component               threshold   teams
======================  ==========  =============================
adherence               ``>= 95``   Core / Fraud / Social Media
ntpj                    ``<= 100``  Core / Fraud
normalized_occupancy    ``>= 100``  Core / Fraud / Social Media
quality                 ``>= 95``   Core / Fraud / Social Media
tnps                    ``>= 88``   Social Media
wows                    ``>= 5``    Social Media
======================  ==========  =============================

An agent counts toward a component's **denominator** when they have a row for
that metric in the bucket (``COUNT(DISTINCT agent)``), and toward the
**numerator** only when their ``metric_value`` clears the threshold (NULLs fail).

Era windows (anchored on the bucket's month, like the Index)
------------------------------------------------------------
* **Core / Fraud**: adherence + ntpj always; ``+ quality`` from **Feb 2026**;
  ``+ normalized_occupancy`` from **March 2026**.
* **Social Media**: adherence + tnps + wows always; ``+ quality`` from **Feb
  2026**; ``+ normalized_occupancy`` from **March 2026**.

**Content has no Xpeers In Target** (the legacy Content notebook doesn't build
it), so Content XForces are excluded. ``day`` / ``week`` / ``month`` buckets use
their own month; ``quarter`` / ``semester`` / ``year`` anchor on the period's
end month. Buckets ending before 2026 are dropped.

Output grain
------------
Two roll-ups land in the same table (legacy ``*_xforce`` / ``*_xplead``):

* ``xpeers_in_target`` — one row per ``(team, xforce, xplead)`` per period.
* ``xpeers_in_target_xplead`` — one row per ``(team, xplead)`` per period
  (``xforce`` NULL); the same in-target/total counts aggregated to the XPLead.

For both, ``agent`` / ``squad`` / ``district`` / ``shift`` are NULL,
``numerator`` = targets achieved, ``denominator`` = total targets, and
``metric_value`` = ``numerator / denominator * 100``.
"""

from __future__ import annotations

import pandas as pd

from metric_utils import METRIC_COLUMNS, empty_metric_frame

METRIC_NAME = "xpeers_in_target"  # XForce grain
XPLEAD_METRIC_NAME = "xpeers_in_target_xplead"  # XPLead roll-up grain

QUALITY_CUTOVER = pd.Timestamp("2026-02-01")  # quality target joins
NO_CUTOVER = pd.Timestamp("2026-03-01")  # NO target joins
ERA_FLOOR = pd.Timestamp("2026-01-01")  # the metric is a 2026 construct

CORE_FRAUD = ("core", "fraud")
SOCIAL_MEDIA = "social media"

# component column (in the metric tables) -> ("ge"|"le", threshold)
_TARGETS = {
    "adherence": ("ge", 95.0),
    "ntpj": ("le", 100.0),
    "normalized_occupancy": ("ge", 100.0),
    "quality": ("ge", 95.0),
    "tnps": ("ge", 88.0),
    "wows": ("ge", 5.0),
}


def _era_anchor_month(date_reference: pd.Series, granularity: pd.Series) -> pd.Series:
    """The month deciding a bucket's era (its last month for multi-month grains)."""
    dr = pd.to_datetime(date_reference)
    if getattr(dr.dt, "tz", None) is not None:
        dr = dr.dt.tz_localize(None)
    anchor = dr.copy()
    anchor = anchor.mask(granularity == "quarter", dr + pd.offsets.MonthBegin(2))
    anchor = anchor.mask(granularity == "semester", dr + pd.offsets.MonthBegin(5))
    anchor = anchor.mask(granularity == "year", dr + pd.offsets.MonthBegin(11))
    return anchor.dt.to_period("M").dt.to_timestamp()


def _aggregate(df: pd.DataFrame, name: str, *, keys: list[str]) -> pd.DataFrame:
    """Per-XForce in-target / total agent counts for one component."""
    comparator, threshold = _TARGETS[name]
    work = df.copy()
    mv = pd.to_numeric(work["metric_value"], errors="coerce")
    passed = mv >= threshold if comparator == "ge" else mv <= threshold
    work["_pass"] = passed.fillna(False).astype("int64")
    agg = work.groupby(keys, as_index=False, dropna=False).agg(
        **{f"{name}_in": ("_pass", "sum"), f"{name}_tot": ("agent", "nunique")}
    )
    return agg


def _compute_grain(
    adherence: pd.DataFrame,
    sources: dict[str, pd.DataFrame | None],
    *,
    grain: str,
) -> pd.DataFrame:
    """Targets-achieved / total-targets, aggregated to ``grain`` (xforce|xplead)."""
    if adherence is None or adherence.empty:
        return empty_metric_frame()

    if grain == "xforce":
        # Group with xplead so it rides along; join components on xforce only.
        base_keys = ["team", "xforce", "xplead", "date_reference", "date_granularity"]
        join_keys = ["team", "xforce", "date_reference", "date_granularity"]
        metric_name = METRIC_NAME
    elif grain == "xplead":
        base_keys = ["team", "xplead", "date_reference", "date_granularity"]
        join_keys = base_keys
        metric_name = XPLEAD_METRIC_NAME
    else:  # pragma: no cover - guarded by callers
        raise ValueError(f"unknown grain: {grain!r}")

    base = _aggregate(adherence, "adherence", keys=base_keys)

    for name, df in sources.items():
        if df is not None and not df.empty:
            base = base.merge(
                _aggregate(df, name, keys=join_keys), on=join_keys, how="left"
            )
        for col in (f"{name}_in", f"{name}_tot"):
            if col not in base:
                base[col] = 0
            base[col] = base[col].fillna(0)

    team = base["team"].astype("string").str.lower()
    included = team.isin(CORE_FRAUD) | (team == SOCIAL_MEDIA)
    era = _era_anchor_month(base["date_reference"], base["date_granularity"])
    base = base.loc[included & (era >= ERA_FLOOR)].reset_index(drop=True)
    if base.empty:
        return empty_metric_frame()

    team = base["team"].astype("string").str.lower()
    is_cf = team.isin(CORE_FRAUD)
    is_sm = team == SOCIAL_MEDIA
    era = _era_anchor_month(base["date_reference"], base["date_granularity"])
    qa_ok = era >= QUALITY_CUTOVER
    no_ok = era >= NO_CUTOVER

    num = pd.Series(0.0, index=base.index)
    den = pd.Series(0.0, index=base.index)

    def add(col_prefix: str, mask: pd.Series) -> None:
        nonlocal num, den
        num = num + base[f"{col_prefix}_in"].where(mask, 0.0).astype(float)
        den = den + base[f"{col_prefix}_tot"].where(mask, 0.0).astype(float)

    add("adherence", is_cf | is_sm)
    add("ntpj", is_cf)
    add("tnps", is_sm)
    add("wows", is_sm)
    add("quality", (is_cf | is_sm) & qa_ok)
    add("normalized_occupancy", (is_cf | is_sm) & no_ok)

    out = pd.DataFrame(index=base.index)
    out["agent"] = None
    out["xforce"] = base["xforce"].values if grain == "xforce" else None
    out["xplead"] = base["xplead"].values
    out["team"] = base["team"].values
    out["squad"] = None
    out["district"] = None
    out["shift"] = None
    out["date_reference"] = base["date_reference"].values
    out["date_granularity"] = base["date_granularity"].values
    out["metric"] = metric_name
    out["numerator"] = num.values
    out["denominator"] = den.values
    out["metric_value"] = (num / den).where(den > 0).values * 100

    sort_key = "xforce" if grain == "xforce" else "xplead"
    return (
        out[list(METRIC_COLUMNS)]
        .sort_values(["date_granularity", "date_reference", "team", sort_key],
                     na_position="last")
        .reset_index(drop=True)
    )


def compute_xpeers_in_target(
    adherence: pd.DataFrame,
    ntpj: pd.DataFrame | None = None,
    normalized_occupancy: pd.DataFrame | None = None,
    quality: pd.DataFrame | None = None,
    tnps: pd.DataFrame | None = None,
    wows: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute XForce Xpeers In Target (metric ``xpeers_in_target``).

    Args:
        adherence: ``io_adherence_metric`` — the driver (defines the XForce
            universe; ``xplead`` is taken from here).
        ntpj / normalized_occupancy / quality / tnps / wows: the corresponding
            ``io_*_metric`` tables. Any may be ``None``/empty.

    Returns:
        Tidy long-format metric rows (XForce grain), all granularities.
    """
    sources = {
        "ntpj": ntpj,
        "normalized_occupancy": normalized_occupancy,
        "quality": quality,
        "tnps": tnps,
        "wows": wows,
    }
    return _compute_grain(adherence, sources, grain="xforce")


def compute_xpeers_in_target_xplead(
    adherence: pd.DataFrame,
    ntpj: pd.DataFrame | None = None,
    normalized_occupancy: pd.DataFrame | None = None,
    quality: pd.DataFrame | None = None,
    tnps: pd.DataFrame | None = None,
    wows: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute XPLead Xpeers In Target (metric ``xpeers_in_target_xplead``).

    Identical target/era logic to the XForce version, but the in-target and
    total agent counts are aggregated per ``(team, xplead)`` instead of per
    XForce. ``xforce`` is NULL on these rows.
    """
    sources = {
        "ntpj": ntpj,
        "normalized_occupancy": normalized_occupancy,
        "quality": quality,
        "tnps": tnps,
        "wows": wows,
    }
    return _compute_grain(adherence, sources, grain="xplead")


IO_XPEERS_IN_TARGET_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
