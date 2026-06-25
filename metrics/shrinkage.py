"""shrinkage — the Shrinkage performance metric (Core / Fraud / Social Media / Content).

Part of the **metrics layer**: consumes the raw ``io_shrinkage_slots_raw`` table
(one row per DIME slot, already flagged for shrinkage) and produces a finished,
agent-level metric at day / week / month / quarter / semester / year grain.

Shrinkage = the share of an agent's *dimensioned* (required) time spent on
non-productive activities:

    shrinkage = SUM(shrinkage_flag) / SUM(required_slot)

over the agent's required slots for the period. **Target ≤ 20%.** Same
definition for all teams (see ``docs/metrics_definitions.md``).

The numerator (``shrinkage_flag``) is computed by the raw layer (the pre/post-
2026-03-01 slot-level rule). This module only applies the **denominator**
("required slot") rule that the raw layer deferred.

Denominator rule (legacy ``required_slot``, applied here)
---------------------------------------------------------
* ``lunch_break`` slots never count (numerator or denominator) — legacy
  ``shrinkage_base`` drops them up front.
* A slot is a *required* slot unless, by era, its ``activity_type_required`` is:
    - **pre-cutover** (``date < 2026-03-01``): ``dime_invalid_notation``
    - **post-cutover** (``date >= 2026-03-01``): ``time_off``
  (The 2025 path uses the same ``dime_invalid_notation`` rule as pre-cutover.)

Every shrinkage slot is, by construction, also a required slot, so the ratio is
always in ``[0, 1]``.

Input
-----
The ``io_shrinkage_slots_raw`` table (one row per DIME slot), via
``metrics_data/shrinkage_slots.py`` / ``read_table``. Required columns:
``agent, xforce, xplead, team, squad, district, shift, date,
activity_type_required, shrinkage_flag``.

NOT applied here (future Adjustments layer — keeps this a clean baseline)
------------------------------------------------------------------------
* Per-agent maternity / vacation reclassifications (e.g. maria.reyes, the
  hardcoded vacation dates) that legacy folds into ``shrinkage_slot``.
* Training / shadowing slot exclusions and outage-date carve-outs.
* Legacy DIME-squad business exclusions (``wfm`` / ``enablement`` / …); note
  ``quality`` and ``planning`` are already excluded upstream by the extractors.

Output — one row per (agent, date_reference, granularity)
---------------------------------------------------------
Tidy "long" metric shape shared across the metrics layer:
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``.
``metric_value`` is a **percentage** (``numerator / denominator * 100``); it is
NULL when the denominator is 0.

Roll-ups (``compute_shrinkage_rollups``)
----------------------------------------
``compute_shrinkage`` produces the agent grain (``metric = 'shrinkage'``). The
SOT also reports Shrinkage at the **XForce** and **XPLead** levels, so the build
script additionally emits ``shrinkage_xforce`` (per ``team, xforce, xplead``) and
``shrinkage_xplead`` (per ``team, xplead``; ``xforce`` NULL) into the same table.
Both are **slot-weighted** (sum the agent numerator/denominator, then divide) —
identical to legacy and to the shrinkage component of ``xforce_index``.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from metric_utils import METRIC_COLUMNS, aggregate_long, empty_metric_frame
from adjustments.manual import (
    apply_no_shrinkage,
    drop_slot_windows,
    reclassify_dime_slots,
)

METRIC_NAME = "shrinkage"  # agent grain
XFORCE_METRIC_NAME = "shrinkage_xforce"  # XForce roll-up
XPLEAD_METRIC_NAME = "shrinkage_xplead"  # XPLead roll-up

# Cutover for the denominator (required_slot) rule — same date as the raw
# layer's shrinkage_flag formula switch.
SHRINKAGE_FORMULA_CUTOVER: date = date(2026, 3, 1)

# Slots that never count toward the metric (numerator or denominator): legacy
# ``shrinkage_base`` drops lunch_break before any counting.
SHRINKAGE_EXCLUDED_ACTIVITY_TYPES: tuple[str, ...] = ("lunch_break",)

# activity_type_required that is NOT a required slot, by era.
PRE_CUTOVER_NON_REQUIRED_ACTIVITY = "dime_invalid_notation"
POST_CUTOVER_NON_REQUIRED_ACTIVITY = "time_off"


def compute_shrinkage(
    shrinkage_slots: pd.DataFrame,
    *,
    general_exclusions: pd.DataFrame | None = None,
    dime_inconsistencies: pd.DataFrame | None = None,
    training: pd.DataFrame | None = None,
    shadowing: pd.DataFrame | None = None,
    no_shrinkage: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute the Shrinkage metric at all granularities.

    Args:
        shrinkage_slots: the ``io_shrinkage_slots_raw`` table (one row per slot).

    Returns:
        Tidy long-format metric rows (see module docstring / schema).
    """
    if shrinkage_slots.empty:
        return empty_metric_frame()

    work = reclassify_dime_slots(shrinkage_slots, dime_inconsistencies)
    work = drop_slot_windows(work, general_exclusions, training, shadowing)
    work = apply_no_shrinkage(work, no_shrinkage)
    act = work["activity_type_required"].astype("string").str.lower()

    # Drop lunch_break (never counted on either side).
    work = work.loc[~act.isin(SHRINKAGE_EXCLUDED_ACTIVITY_TYPES)].copy()
    if work.empty:
        return empty_metric_frame()

    act = work["activity_type_required"].astype("string").str.lower()

    if pd.api.types.is_datetime64_any_dtype(work["date"]):
        slot_date = work["date"].dt.date
    else:
        slot_date = pd.to_datetime(work["date"]).dt.date
    is_post_cutover = slot_date >= SHRINKAGE_FORMULA_CUTOVER

    non_required = pd.Series(POST_CUTOVER_NON_REQUIRED_ACTIVITY, index=work.index)
    non_required = non_required.where(
        is_post_cutover, PRE_CUTOVER_NON_REQUIRED_ACTIVITY
    )
    work["required_slot_flag"] = (act != non_required).astype("int64")
    work["shrinkage_flag"] = work["shrinkage_flag"].astype("int64")

    return aggregate_long(
        work,
        numerator_col="shrinkage_flag",
        denominator_col="required_slot_flag",
        metric_name=METRIC_NAME,
    )


def _rollup(agent_metric: pd.DataFrame, *, level: str) -> pd.DataFrame:
    """Slot-weighted shrinkage roll-up of the agent metric to ``level``.

    The roll-up is **slot-weighted** (sum the agent numerator/denominator, then
    divide) — identical to legacy ``shrinkage_xforce`` / ``shrinkage_xplead`` and
    to the shrinkage component inside ``xforce_index``. It is NOT a flat average
    of the per-agent percentages.
    """
    if level == "xforce":
        keys = ["team", "xforce", "xplead", "date_reference", "date_granularity"]
        metric_name = XFORCE_METRIC_NAME
    elif level == "xplead":
        keys = ["team", "xplead", "date_reference", "date_granularity"]
        metric_name = XPLEAD_METRIC_NAME
    else:  # pragma: no cover - guarded by caller
        raise ValueError(f"unknown level: {level!r}")

    agg = (
        agent_metric.groupby(keys, as_index=False, dropna=False)
        .agg(numerator=("numerator", "sum"), denominator=("denominator", "sum"))
    )

    out = pd.DataFrame(index=agg.index)
    out["agent"] = None
    out["xforce"] = agg["xforce"].values if level == "xforce" else None
    out["xplead"] = agg["xplead"].values
    out["team"] = agg["team"].values
    out["squad"] = None
    out["district"] = None
    out["shift"] = None
    out["date_reference"] = agg["date_reference"].values
    out["date_granularity"] = agg["date_granularity"].values
    out["metric"] = metric_name
    num = pd.to_numeric(agg["numerator"], errors="coerce")
    den = pd.to_numeric(agg["denominator"], errors="coerce")
    out["numerator"] = num.values
    out["denominator"] = den.values
    out["metric_value"] = (num / den).where(den > 0).values * 100
    return out[list(METRIC_COLUMNS)]


def compute_shrinkage_rollups(agent_metric: pd.DataFrame) -> pd.DataFrame:
    """XForce + XPLead slot-weighted roll-ups of the agent shrinkage metric.

    Args:
        agent_metric: the agent-level shrinkage metric (output of
            ``compute_shrinkage``, ``metric == 'shrinkage'``).

    Returns:
        ``shrinkage_xforce`` and ``shrinkage_xplead`` rows (``agent`` NULL;
        ``xforce`` NULL on the XPLead rows), all granularities.
    """
    if agent_metric is None or agent_metric.empty:
        return empty_metric_frame()
    work = agent_metric[agent_metric["metric"] == METRIC_NAME].copy()
    if work.empty:
        return empty_metric_frame()
    rolled = pd.concat(
        [_rollup(work, level="xforce"), _rollup(work, level="xplead")],
        ignore_index=True,
    )
    return rolled.sort_values(
        ["date_granularity", "date_reference", "team", "metric", "xplead"],
        na_position="last",
    ).reset_index(drop=True)


IO_SHRINKAGE_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
