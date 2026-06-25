"""normalized_occupancy — the Normalized Occupancy (NO) metric (all teams).

NO compares an agent's occupancy against the average occupancy of their
**district + shift** cohort that month:

    occupancy = SUM(occupancy_minutes) / SUM(required_minutes)   (per agent)
    NO        = occupancy / occupancy_benchmark

**Target ≥ 100%.** All four teams have NO (Core / Fraud / Social Media /
Content); Social Media occupancy comes from Sprinklr, already folded into the
raw table via ``sm_jobs``.

Input
-----
``io_occupancy_time_raw`` (one row per agent per DIME slot), via
``metrics_data/occupancy_time.py``. Required columns: ``agent, xforce, xplead,
team, squad, district, shift, date, activity_type_required, required_minutes,
occupancy_minutes``.

The benchmark (matches legacy `[IO] Normalized Occupancy Dataset.sql`)
----------------------------------------------------------------------
Two-step, per **month**:

1. Per ``(month, district, shift, squad)``: ``SUM(occupancy_minutes) /
   SUM(required_minutes)`` — that squad-cohort's occupancy ratio.
2. Per ``(month, district, shift)``: the **average of the squad ratios** from
   step 1 (equal-weight across squads — the legacy ``AVG(occupancy_monthly)``
   over the squads sharing a district + shift).

Each slot carries its ``(month, district, shift)`` benchmark; when rolled up to
a multi-month bucket the benchmark is averaged weighted by required minutes (a
single-month bucket therefore just keeps that month's benchmark).

Output convention
-----------------
To keep the shared ``metric_value = numerator / denominator * 100`` contract,
``numerator`` is the agent's **occupancy %** and ``denominator`` is the
**benchmark %**, so ``metric_value`` is NO %.

Filters applied here (deferred by the raw layer)
------------------------------------------------
* Drop non-productive slots: ``activity_type_required`` in
  ``{lunch_break, time_off, shrinkage}`` (case-insensitive). The remaining slots
  feed both the agent occupancy and the benchmark.
* Manual approved adjustment from ``Ajustes Index``: suppress
  ``nitza.zarza`` from the NO metric output for Apr-May 2026. Her slots still
  feed the district/shift benchmark, matching the legacy placement of this
  filter after benchmark construction.

NOT applied here (future Adjustments layer)
-------------------------------------------
* Legacy ``dimensioned_activity`` meeting/leave carve-outs and the per-agent
  ``time_off`` reclassifications (the raw table doesn't carry
  ``dimensioned_activity``).
* DIME-squad exclusions (``wfm`` / ``credit_evolution`` / ``dote``) — so they
  currently still feed the benchmark.
* Per-agent vacation / outage-date exclusions (e.g. 2026-03-27, 2026-04-09).

Output — tidy long format, one row per (agent, date_reference, granularity)
---------------------------------------------------------------------------
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
from adjustments.manual import drop_slot_windows, reclassify_dime_slots

METRIC_NAME = "normalized_occupancy"

NITZA_NO_SUPPRESSION_AGENT = "nitza.zarza"
NITZA_NO_SUPPRESSION_START = pd.Timestamp("2026-04-01")
NITZA_NO_SUPPRESSION_END = pd.Timestamp("2026-05-31")

# Same non-productive activity types excluded from adherence.
EXCLUDED_ACTIVITY_TYPES: tuple[str, ...] = (
    "lunch_break",
    "time_off",
    "shrinkage",
)


def _occupancy_benchmark(slots: pd.DataFrame) -> pd.DataFrame:
    """Per ``(month, district, shift)`` benchmark = mean of squad occupancy ratios."""
    squad = slots.groupby(
        ["month", "district", "shift", "squad"], as_index=False, dropna=False
    ).agg(
        occ=("occupancy_minutes", "sum"),
        req=("required_minutes", "sum"),
    )
    squad["ratio"] = (squad["occ"] / squad["req"]).where(squad["req"] > 0)
    bench = squad.groupby(
        ["month", "district", "shift"], as_index=False, dropna=False
    ).agg(benchmark=("ratio", "mean"))
    return bench


def _aggregate(slots: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """One NO row per (agent, bucket) for a single granularity."""
    work = slots.copy()
    work["_date"] = pd.to_datetime(work["date"])
    if getattr(work["_date"].dt, "tz", None) is not None:
        work["_date"] = work["_date"].dt.tz_localize(None)
    work["date_reference"] = bucket_dates(work["_date"], granularity)
    work["_bench_weighted"] = work["benchmark"] * work["required_minutes"]

    grp = work.groupby(["agent", "date_reference"], as_index=False, dropna=False)
    sums = grp.agg(
        _occ=("occupancy_minutes", "sum"),
        _req=("required_minutes", "sum"),
        _bench_w=("_bench_weighted", "sum"),
    )

    # Agent occupancy % and the required-minute-weighted benchmark %.
    sums["numerator"] = (sums["_occ"] / sums["_req"]).where(sums["_req"] > 0) * 100
    sums["denominator"] = (sums["_bench_w"] / sums["_req"]).where(
        sums["_req"] > 0
    ) * 100
    sums["metric_value"] = (sums["numerator"] / sums["denominator"]).where(
        sums["denominator"] > 0
    ) * 100

    out = sums.merge(latest_dims(work), on=["agent", "date_reference"], how="left")
    out["date_granularity"] = granularity
    out["metric"] = METRIC_NAME
    out["date_reference"] = out["date_reference"].dt.date
    return out[list(METRIC_COLUMNS)]


def compute_normalized_occupancy(
    occupancy_time: pd.DataFrame,
    *,
    general_exclusions: pd.DataFrame | None = None,
    dime_inconsistencies: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute the Normalized Occupancy metric at all granularities.

    Args:
        occupancy_time: the ``io_occupancy_time_raw`` table (one row per slot).

    Returns:
        Tidy long-format metric rows (see module docstring).
    """
    if occupancy_time.empty:
        return empty_metric_frame()

    work = reclassify_dime_slots(occupancy_time, dime_inconsistencies)
    work = drop_slot_windows(work, general_exclusions)

    productive = work.loc[
        ~work["activity_type_required"]
        .astype("string")
        .str.lower()
        .isin(EXCLUDED_ACTIVITY_TYPES)
    ].copy()
    if productive.empty:
        return empty_metric_frame()

    d = pd.to_datetime(productive["date"])
    if getattr(d.dt, "tz", None) is not None:
        d = d.dt.tz_localize(None)
    productive["month"] = d.dt.to_period("M").dt.to_timestamp()

    bench = _occupancy_benchmark(productive)
    # Null-heavy keys (notably Content `shift`) can come back with different
    # pandas dtypes after groupby; align them before merging the benchmark.
    for col in ["district", "shift"]:
        productive[col] = productive[col].astype("object")
        bench[col] = bench[col].astype("object")
    productive = productive.merge(
        bench, on=["month", "district", "shift"], how="left"
    )

    # Approved manual adjustment, captured in the `Ajustes Index` tab:
    # nitza.zarza's NO is excluded from her metric output in Apr-May 2026, but
    # not from the peer benchmark that other agents are compared against.
    correction_date = pd.to_datetime(productive["date"])
    suppress_nitza_no = (
        (productive["agent"] == NITZA_NO_SUPPRESSION_AGENT)
        & correction_date.between(NITZA_NO_SUPPRESSION_START, NITZA_NO_SUPPRESSION_END)
    )
    productive = productive.loc[~suppress_nitza_no].copy()
    if productive.empty:
        return empty_metric_frame()

    parts = [_aggregate(productive, g) for g in GRANULARITIES]
    result = pd.concat(parts, ignore_index=True)
    return result.sort_values(
        ["date_granularity", "date_reference", "agent"]
    ).reset_index(drop=True)


IO_NORMALIZED_OCCUPANCY_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
