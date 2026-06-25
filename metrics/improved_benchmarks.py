"""improved_benchmarks — the Improved Benchmarks metric (Core / Fraud only).

Part of the **metrics layer**, but structurally different from the agent-grain
metrics: Improved Benchmarks is a **squad-level and district-level** roll-up of
*month-over-month benchmark improvements*. It answers "what share of this
squad's / district's benchmarks improved vs. the previous month?".

    improved_benchmarks = COUNT(improved benchmarks) / COUNT(comparable benchmarks)

**Target ≥ 60%.** Output is **month grain only** (benchmarks are monthly).

Two benchmark families are compared (matches legacy
`[IO] Performance 2026 - S&D.sql`):

* **NTPJ benchmark** — per ``job_id`` (job type): the monthly
  ``exp_duration_job`` (cohort-wide expected seconds, with the NTPJ trailing-
  window rule). **"Improved" = benchmark ≤ previous month** (faster is better).
* **Occupancy benchmark** — per ``district + shift``: the monthly NO benchmark
  (mean of squad occupancy ratios). **"Improved" = benchmark ≥ previous month**
  (higher occupancy is better). Only from ``2026-02-01`` onward.

Ties ("stayed the same") count as **improved**. A benchmark's first month (no
previous month to compare) is **not counted** (numerator or denominator).

Benchmark units are formed per ``(benchmark_key, xforce, month)`` — the set of
job types / district-shifts that an XForce's agents worked — then rolled up:
* ``improved_benchmark_squad``    — summed over the XForces in each squad;
* ``improved_benchmark_district`` — summed over the XForces in each district;
* ``improved_benchmark_xforce``   — summed over the benchmark units of each
  XForce (legacy ``improved_benchmark``). This XForce roll-up is what the
  composite ``xforce_index`` metric consumes.

Scope (per the SOT + product guidance)
---------------------------------------
* **Core / Fraud only** — Social Media and Content never had Improved Benchmarks.
* **Removed from each team after its cutover**: Core from **2026-04**, Fraud from
  **2026-05** (months ≥ the cutover are not emitted).

Inputs
------
* ``io_jobs_raw`` (NTPJ benchmark) — needs a benchmark look-back before the
  output period (the build script reads ~6 extra months).
* ``io_occupancy_time_raw`` (occupancy benchmark) — needs ~2 extra months.

NOT applied here (future Adjustments layer)
-------------------------------------------
* Everything the NTPJ / NO raw layers defer (cross-support exclusions, per-agent
  carve-outs, outage dates, DIME-squad exclusions). Improved Benchmarks inherits
  whatever those benchmarks are.

Output — tidy long format (squad / district / xforce rows)
-----------------------------------------------------------
``agent, xforce, xplead, team, squad, district, shift, date_reference,
date_granularity, metric, numerator, denominator, metric_value``. ``metric`` is
one of ``improved_benchmark_squad`` (squad set), ``improved_benchmark_district``
(district set), or ``improved_benchmark_xforce`` (``xforce`` + ``xplead`` set).
``agent`` and ``shift`` are always NULL; ``date_granularity`` is always
``month``. ``metric_value = numerator / denominator * 100`` (NULL if denominator 0).
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from metric_utils import METRIC_COLUMNS, empty_metric_frame
from ntpj import FINISHED_STATUS, _expected_duration_by_month
from adjustments.manual import (
    drop_cross_support_jobs,
    drop_excluded_jobs,
    drop_slot_windows,
    reclassify_dime_slots,
)

SQUAD_METRIC = "improved_benchmark_squad"
DISTRICT_METRIC = "improved_benchmark_district"
XFORCE_METRIC = "improved_benchmark_xforce"

# Improved Benchmarks only ever applied to Core and Fraud.
IMPROVED_BENCHMARKS_TEAMS: tuple[str, ...] = ("core", "fraud")

# First month each team no longer emits Improved Benchmarks (removed from then on).
TEAM_REMOVAL_MONTH: dict[str, pd.Period] = {
    "core": pd.Period("2026-04", freq="M"),
    "fraud": pd.Period("2026-05", freq="M"),
}

# Occupancy benchmark only exists from this month (legacy `WHERE date >= '2026-02-01'`).
OCCUPANCY_BENCHMARK_START_MONTH = pd.Period("2026-02", freq="M")

# Non-productive slots excluded from the occupancy benchmark (same as NO).
NO_EXCLUDED_ACTIVITY_TYPES: tuple[str, ...] = ("lunch_break", "time_off", "shrinkage")


def _to_month(series: pd.Series) -> pd.Series:
    d = pd.to_datetime(series)
    if getattr(d.dt, "tz", None) is not None:
        d = d.dt.tz_localize(None)
    return d.dt.to_period("M")


def _xforce_dims(df: pd.DataFrame) -> pd.DataFrame:
    """Most-common ``(team, squad, district, xplead)`` per ``(xforce, month)``."""
    g = (
        df.groupby(
            ["xforce", "month", "team", "squad", "district", "xplead"], dropna=False
        )
        .size()
        .reset_index(name="_n")
    )
    g = g.sort_values("_n").groupby(["xforce", "month"], as_index=False).tail(1)
    return g[["xforce", "month", "team", "squad", "district", "xplead"]]


def _ntpj_benchmark_units(jobs_raw: pd.DataFrame) -> pd.DataFrame:
    """Per ``(job_id, xforce, month)`` NTPJ benchmark + improvement flag."""
    finished = jobs_raw[
        jobs_raw["status"].astype("string").str.lower() == FINISHED_STATUS
    ].copy()
    if finished.empty:
        return _empty_units()
    finished["month"] = _to_month(finished["date"])

    monthly_totals = finished.groupby(["job_id", "month"], as_index=False).agg(
        tot_duration=("duration_seconds", "sum"),
        tot_count=("job_id", "size"),
    )
    expected = _expected_duration_by_month(monthly_totals)  # job_id, month, exp_duration_job

    contrib = finished[finished["required_activity_on_day_flag"] == 1]
    if contrib.empty:
        return _empty_units()

    keys = contrib[["job_id", "xforce", "month"]].drop_duplicates()
    keys = keys.merge(expected, on=["job_id", "month"], how="inner")
    keys = keys.merge(_xforce_dims(contrib), on=["xforce", "month"], how="left")
    keys = keys.rename(columns={"job_id": "key", "exp_duration_job": "benchmark"})
    return _flag_improved(keys, direction="lower")


def _occupancy_benchmark_units(occupancy_time: pd.DataFrame) -> pd.DataFrame:
    """Per ``(district-shift, xforce, month)`` occupancy benchmark + flag."""
    act = occupancy_time["activity_type_required"].astype("string").str.lower()
    productive = occupancy_time.loc[~act.isin(NO_EXCLUDED_ACTIVITY_TYPES)].copy()
    if productive.empty:
        return _empty_units()
    productive["month"] = _to_month(productive["date"])
    productive = productive[productive["month"] >= OCCUPANCY_BENCHMARK_START_MONTH]
    if productive.empty:
        return _empty_units()

    # NO benchmark: mean of squad occupancy ratios per (month, district, shift).
    squad = productive.groupby(
        ["month", "district", "shift", "squad"], as_index=False, dropna=False
    ).agg(occ=("occupancy_minutes", "sum"), req=("required_minutes", "sum"))
    squad["ratio"] = (squad["occ"] / squad["req"]).where(squad["req"] > 0)
    bench = squad.groupby(
        ["month", "district", "shift"], as_index=False, dropna=False
    ).agg(benchmark=("ratio", "mean"))

    keys = productive[["district", "shift", "xforce", "month"]].drop_duplicates()
    keys["key"] = (
        keys["district"].astype("string") + " - " + keys["shift"].astype("string")
    )
    keys = keys.merge(bench, on=["month", "district", "shift"], how="inner")
    # The slot's own district == the xforce's district; take team/squad from the
    # xforce dims (drop its district to avoid colliding with the key's district).
    dims = _xforce_dims(productive)[["xforce", "month", "team", "squad", "xplead"]]
    keys = keys.merge(dims, on=["xforce", "month"], how="left")
    keys = keys[
        ["key", "xforce", "xplead", "month", "benchmark", "team", "squad", "district"]
    ]
    return _flag_improved(keys, direction="higher")


def _flag_improved(units: pd.DataFrame, *, direction: str) -> pd.DataFrame:
    """Add ``improved`` / ``counted`` via month-over-month LAG within (key, xforce)."""
    units = units.sort_values(["key", "xforce", "month"]).copy()
    units["_prev"] = units.groupby(["key", "xforce"], dropna=False)["benchmark"].shift(1)
    if direction == "lower":
        improved = units["benchmark"] <= units["_prev"]
    else:
        improved = units["benchmark"] >= units["_prev"]
    units["counted"] = units["_prev"].notna().astype("int64")
    units["improved"] = (improved & units["_prev"].notna()).astype("int64")
    return units[
        ["key", "xforce", "xplead", "month", "team", "squad", "district",
         "improved", "counted"]
    ]


def _empty_units() -> pd.DataFrame:
    return pd.DataFrame(
        {
            c: pd.Series(dtype="object")
            for c in (
                "key", "xforce", "xplead", "month", "team", "squad", "district",
                "improved", "counted",
            )
        }
    )


def _rollup(units: pd.DataFrame, *, level: str, metric_name: str) -> pd.DataFrame:
    """Sum improved / counted per ``(team, <level>, month)`` into metric rows.

    ``level`` is ``squad`` / ``district`` (key sets that column, ``xforce`` /
    ``xplead`` NULL) or ``xforce`` (sets ``xforce`` + ``xplead``, squad/district
    NULL — legacy ``improved_benchmark``).
    """
    if level == "xforce":
        keys = ["team", "xforce", "xplead", "month"]
        key_col = "xforce"
    else:
        keys = ["team", level, "month"]
        key_col = level
    grp = units.groupby(keys, as_index=False, dropna=False).agg(
        numerator=("improved", "sum"), denominator=("counted", "sum")
    )
    grp = grp[grp[key_col].notna()].copy()
    out = pd.DataFrame(index=grp.index)
    out["agent"] = None
    out["xforce"] = grp["xforce"].values if level == "xforce" else None
    out["xplead"] = grp["xplead"].values if level == "xforce" else None
    out["team"] = grp["team"].values
    out["squad"] = grp[level].values if level == "squad" else None
    out["district"] = grp[level].values if level == "district" else None
    out["shift"] = None
    out["date_reference"] = grp["month"].apply(lambda m: m.to_timestamp().date()).values
    out["date_granularity"] = "month"
    out["metric"] = metric_name
    out["numerator"] = grp["numerator"].astype("float64").values
    out["denominator"] = grp["denominator"].astype("float64").values
    out["metric_value"] = (
        (out["numerator"] / out["denominator"]).where(out["denominator"] > 0) * 100
    )
    return out[list(METRIC_COLUMNS)]


def compute_improved_benchmarks(
    jobs_raw: pd.DataFrame,
    occupancy_time: pd.DataFrame,
    period_start: date,
    period_end: date,
    *,
    general_exclusions: pd.DataFrame | None = None,
    dime_inconsistencies: pd.DataFrame | None = None,
    cross_support: pd.DataFrame | None = None,
    job_exclusions: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute Improved Benchmarks (squad + district, month grain, Core/Fraud).

    Args:
        jobs_raw: ``io_jobs_raw`` incl. a benchmark look-back before period_start.
        occupancy_time: ``io_occupancy_time_raw`` incl. a short look-back.
        period_start / period_end: inclusive output window (by month). Look-back
            rows are used only for benchmarks / the previous-month comparison.

    Returns:
        Tidy long-format metric rows (squad + district), month grain.
    """
    if jobs_raw.empty and occupancy_time.empty:
        return empty_metric_frame()

    jobs_source = jobs_raw.copy()
    if not jobs_source.empty and "start_time" in jobs_source.columns:
        jobs_source["slot_time"] = pd.to_datetime(jobs_source["start_time"]).dt.strftime(
            "%H:%M:%S"
        )
        jobs_source = drop_slot_windows(jobs_source, general_exclusions).drop(
            columns=["slot_time"], errors="ignore"
        )
    if not jobs_source.empty:
        jobs_source = drop_cross_support_jobs(jobs_source, cross_support)
        jobs_source = drop_excluded_jobs(jobs_source, job_exclusions)

    occ_source = reclassify_dime_slots(occupancy_time, dime_inconsistencies)
    occ_source = drop_slot_windows(occ_source, general_exclusions)

    jobs = jobs_source[
        jobs_source["team"].astype("string").str.lower().isin(IMPROVED_BENCHMARKS_TEAMS)
    ] if not jobs_source.empty else jobs_source
    occ = occ_source[
        occ_source["team"].astype("string").str.lower().isin(IMPROVED_BENCHMARKS_TEAMS)
    ] if not occ_source.empty else occ_source

    parts = []
    if not jobs.empty:
        parts.append(_ntpj_benchmark_units(jobs))
    if not occ.empty:
        parts.append(_occupancy_benchmark_units(occ))
    parts = [p for p in parts if not p.empty]
    if not parts:
        return empty_metric_frame()

    units = pd.concat(parts, ignore_index=True)
    units["team"] = units["team"].astype("string").str.lower()

    # Suppress months at/after each team's removal cutover.
    keep = pd.Series(True, index=units.index)
    for team, cutover in TEAM_REMOVAL_MONTH.items():
        keep &= ~((units["team"] == team) & (units["month"] >= cutover))
    units = units[keep]

    # Restrict OUTPUT to the requested period (look-back months drop out).
    start_m = pd.Period(period_start, freq="M")
    end_m = pd.Period(period_end, freq="M")
    units = units[(units["month"] >= start_m) & (units["month"] <= end_m)]
    if units.empty:
        return empty_metric_frame()

    squad_rows = _rollup(units, level="squad", metric_name=SQUAD_METRIC)
    district_rows = _rollup(units, level="district", metric_name=DISTRICT_METRIC)
    xforce_rows = _rollup(units, level="xforce", metric_name=XFORCE_METRIC)

    result = pd.concat(
        [squad_rows, district_rows, xforce_rows], ignore_index=True
    )
    return result.sort_values(
        ["metric", "date_reference", "team"], na_position="last"
    ).reset_index(drop=True)


IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA: tuple[tuple[str, str], ...] = (
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
