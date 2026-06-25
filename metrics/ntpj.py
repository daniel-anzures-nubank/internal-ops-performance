"""ntpj — Normalized Time Per Job (Core / Fraud / Content).

NTPJ compares the time an agent spends on jobs against a monthly expected-time
benchmark:

    ntpj = SUM(actual job duration) / SUM(exp_duration_job * job_count)

per agent per period. **Target ≤ 100%** (lower = faster than benchmark).

Social Media has **no NTPJ** — social jobs aren't in the shuffle/OOS sources, so
social agents simply have no rows in the input table.

Input
-----
``io_jobs_raw`` (one row per job), via ``metrics_data/jobs_raw.py``. Required
columns: ``agent, xforce, xplead, team, squad, district, shift, date,
job_id, activity_type, status, duration_seconds, required_activity_on_day_flag``.

> The input must include a **benchmark look-back** before the output period (the
> build script reads ~4 extra months), because a month's benchmark can use a
> trailing window — see below.

How the benchmark works (matches legacy `[IO] NTPJ Dataset.sql`)
---------------------------------------------------------------
* ``exp_duration_job(job_id, month)`` = ``SUM(duration) / SUM(count)`` across
  **all finished jobs** of that ``job_id`` (every agent), over a month window:
    - target month **≤ 2026-03** → trailing window ``[M-4 … M]`` (5 calendar
      months inclusive — the legacy "4-month window");
    - target month **≥ 2026-04** → the current month only.
* The benchmark is computed from **all finished jobs** (no required-day filter);
  the agent's own contribution rows are restricted to required activities.

Filters applied here
--------------------
* Finished jobs only (``status == 'finished'``; OOS jobs are synthesized as
  ``finished`` in the raw table).
* Agent contribution rows: ``required_activity_on_day_flag == 1`` (the agent was
  scheduled for that job's ``activity_type`` that day — the legacy
  "required_hours IS NOT NULL" filter, precomputed into the raw flag). The
  benchmark itself is NOT restricted this way.

NOT applied here (future Adjustments layer)
-------------------------------------------
* Cross-support queue exclusions (per-agent/queue/date carve-outs).
* Per-agent vacation / maternity / day-control exclusions.
* Outage-date exclusions (2026-03-27, 2026-04-09).
* The Content "always 4-month window" rule — this module applies the unified
  legacy cutover (≤2026-03 trailing, ≥2026-04 current month) to all teams; the
  SOT doc says Content should stay on the trailing window. Flagged for the
  Adjustments/benchmark layer.

Output — tidy long format, one row per (agent, date_reference, granularity)
---------------------------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value`` where
``numerator`` = actual job seconds, ``denominator`` = expected job seconds,
``metric_value`` = ``numerator / denominator * 100`` (NULL if denominator 0).
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from metric_utils import aggregate_long, empty_metric_frame
from adjustments.manual import (
    drop_cross_support_jobs,
    drop_excluded_jobs,
    drop_slot_windows,
)

METRIC_NAME = "ntpj"

FINISHED_STATUS = "finished"

# Month at/after which the benchmark uses the current month only; earlier months
# use the trailing window. (Spark `DATE_TRUNC('MONTH', ...)` first-of-month.)
BENCHMARK_CUTOVER_MONTH = pd.Period("2026-04", freq="M")

# Trailing-window length for pre-cutover months: months b with M-4 <= b <= M
# (5 calendar months inclusive — the legacy "4-month window").
TRAILING_WINDOW_MONTHS = 4


def _benchmark_window(target: pd.Period) -> list[pd.Period]:
    """Source months whose jobs feed ``target``'s benchmark."""
    if target >= BENCHMARK_CUTOVER_MONTH:
        return [target]
    return [target - k for k in range(TRAILING_WINDOW_MONTHS, -1, -1)]


def _expected_duration_by_month(monthly_totals: pd.DataFrame) -> pd.DataFrame:
    """Per (job_id, target_month) expected duration from the windowed totals.

    ``monthly_totals`` has columns ``job_id, month (Period[M]), tot_duration,
    tot_count``. Returns ``job_id, month, exp_duration_job``.
    """
    out: list[pd.DataFrame] = []
    for target in sorted(monthly_totals["month"].unique()):
        window = _benchmark_window(target)
        src = monthly_totals[monthly_totals["month"].isin(window)]
        agg = src.groupby("job_id", as_index=False).agg(
            tot_duration=("tot_duration", "sum"),
            tot_count=("tot_count", "sum"),
        )
        agg["exp_duration_job"] = (agg["tot_duration"] / agg["tot_count"]).where(
            agg["tot_count"] > 0
        )
        agg["month"] = target
        out.append(agg[["job_id", "month", "exp_duration_job"]])
    if not out:
        return pd.DataFrame(
            columns=["job_id", "month", "exp_duration_job"]
        )
    return pd.concat(out, ignore_index=True)


def compute_ntpj(
    jobs_raw: pd.DataFrame,
    period_start: date,
    period_end: date,
    *,
    general_exclusions: pd.DataFrame | None = None,
    cross_support: pd.DataFrame | None = None,
    job_exclusions: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute the NTPJ metric at day/week/month grain.

    Args:
        jobs_raw: the ``io_jobs_raw`` table, including the benchmark look-back
            months before ``period_start``.
        period_start / period_end: inclusive output window (rows outside are
            used only for benchmarks, not emitted).

    Returns:
        Tidy long-format metric rows (see module docstring).
    """
    if jobs_raw.empty:
        return empty_metric_frame()

    adjusted = jobs_raw.copy()
    if not adjusted.empty:
        adjusted["slot_time"] = pd.to_datetime(adjusted["start_time"]).dt.strftime(
            "%H:%M:%S"
        )
        adjusted = drop_slot_windows(adjusted, general_exclusions).drop(
            columns=["slot_time"], errors="ignore"
        )
        adjusted = drop_cross_support_jobs(adjusted, cross_support)
        adjusted = drop_excluded_jobs(adjusted, job_exclusions)

    finished = adjusted[
        adjusted["status"].astype("string").str.lower() == FINISHED_STATUS
    ].copy()
    if finished.empty:
        return empty_metric_frame()

    finished["_date"] = pd.to_datetime(finished["date"])
    if getattr(finished["_date"].dt, "tz", None) is not None:
        finished["_date"] = finished["_date"].dt.tz_localize(None)
    finished["month"] = finished["_date"].dt.to_period("M")

    # --- benchmark: from ALL finished jobs, windowed by month ---------------
    monthly_totals = finished.groupby(["job_id", "month"], as_index=False).agg(
        tot_duration=("duration_seconds", "sum"),
        tot_count=("job_id", "size"),
    )
    expected = _expected_duration_by_month(monthly_totals)

    # --- agent contribution: required activities only -----------------------
    contrib = finished[finished["required_activity_on_day_flag"] == 1].copy()
    if contrib.empty:
        return empty_metric_frame()

    base = contrib.groupby(
        ["agent", "xforce", "xplead", "team", "squad", "district", "shift",
         "_date", "month", "job_id"],
        as_index=False,
        dropna=False,
    ).agg(
        count=("job_id", "size"),
        actual_seconds=("duration_seconds", "sum"),
    )

    base = base.merge(expected, on=["job_id", "month"], how="left")
    base["expected_seconds"] = base["exp_duration_job"] * base["count"]

    # Drop rows with no benchmark (job_id never seen in the window) — they
    # cannot contribute an expected value (mirrors legacy inner benchmark join).
    base = base[base["expected_seconds"].notna()].copy()

    # --- restrict OUTPUT to the requested period (look-back rows drop out) --
    start = pd.Timestamp(period_start)
    end = pd.Timestamp(period_end)
    base = base[(base["_date"] >= start) & (base["_date"] <= end)].copy()
    if base.empty:
        return empty_metric_frame()

    base = base.rename(columns={"_date": "date"})
    return aggregate_long(
        base,
        numerator_col="actual_seconds",
        denominator_col="expected_seconds",
        metric_name=METRIC_NAME,
    )


IO_NTPJ_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
