"""Parity check: new `compute_nocc` vs. legacy `usr.mx__cx.normalized_occupancy`.

What this does
--------------
1. Runs the four extractors (agent_information, dime_slots, shuffle_jobs,
   oos_jobs) for the requested period — with the **data window widened to
   ``floor_month(period_start) .. --data-end-date``** so the monthly
   ``occupancy_exp`` benchmark matches legacy's view.
2. Calls ``compute_nocc`` to get the new pipeline's per-(agent, date,
   slot_start) frame.
3. Pulls the legacy table for the same period.
4. Outer-joins them on (agent, date, slot_start) and reports:
     * Coverage (rows in both / only-new / only-legacy).
     * Per-column exact-match rate (occupancy_time, job_time, occupancy_exp).
     * Delta distributions and the largest individual divergences.
     * A "known adjustments" breakdown — anything that lands in a known
       legacy carve-out is flagged separately so the remaining
       ``unexplained`` rows are the real investigation target.

Why this is a script and not a pytest test
------------------------------------------
Parity here is not binary. The legacy table has manual adjustments baked
in that we deliberately do not replicate (the per-agent ``time_off``
reclassifications in ``dime_table_occupancy``, the outage-date
exclusions, the per-agent/per-date carve-outs at the bottom of
``occupancy_agents_information_2026``). So we'd expect any
"all-or-nothing" assertion to fail on perfectly correct output. The
right tool is a structured diagnostic report.

The diff classes (in priority order)
------------------------------------
* **pre-2026-03-01 scope** — legacy ``normalized_occupancy_final``
  applies a final ``WHERE date >= '2026-03-01'``. Earlier dates are
  legitimately missing from legacy; flagged as ``pre-2026-03-01 (out of
  legacy scope)``.
* **outage-date** — legacy drops rows where
  ``date IN ('2026-03-27', '2026-04-09')``. Shows up as only-in-new.
* **agent-date carve-out** — full-day exclusions
  (jose.velez et al. 2026-03-24..28; jonathan.pineda 2026-02-26;
  maria.reyes Feb 2026 maternity). Only-in-new.
* **content-roster (only-in-legacy)** — legacy unions a Google-Sheets
  content roster (``gsheets.sheets.mx_content_bdx``) on top of BDX. Our
  pipeline is BDX-only. These rows carry ``squad_district='content'``
  in legacy and are flagged as expected.
* **benchmark drift (downstream of carve-outs)** —
  ``occupancy_exp`` is the per-(district, shift, month) average
  ``SUM(occupancy) / SUM(job_time)``. The carve-out classes above
  remove rows from that aggregation in legacy; our pipeline keeps them,
  so the new benchmark drifts by an amount bounded by the carve-outs'
  contribution. Small drifts (|Δ| ≤ ``BENCHMARK_DRIFT_TOL``, default
  0.05) are flagged expected; larger drifts are unexplained.
* **unexplained** — everything else. The investigative target.

A note on the monthly benchmark
-------------------------------
``occupancy_exp`` is a per-month district/shift average. To compare
fairly against legacy (refreshed against the warehouse at some point),
both sides need to be computed off the same volume of monthly data:

* Legacy: whatever was in the table when its job ran. Roughly "the
  current state of DIME / shuffle / OOS at refresh time".
* New:    whatever we pull. By default we pull ``floor_month(period_start)
  .. today``, so the current-month benchmark reflects every row available
  right now.

If the legacy table was refreshed days ago and the warehouse has since
landed new data, ``occupancy_exp`` will drift slightly. The
``--data-end-date`` flag lets a caller cap our extraction to the same
moment legacy was last refreshed.

Usage
-----
::

    uv run python tests/parity/parity_check_nocc.py \\
        --period-start 2026-05-11 --period-end 2026-05-17

    uv run python tests/parity/parity_check_nocc.py \\
        --period-start 2026-03-01 --period-end 2026-05-24 \\
        --csv-out /tmp/nocc_diff.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor  # noqa: E402
from nocc import compute_nocc  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.parity.nocc")

LEGACY_TABLE = "usr.mx__cx.normalized_occupancy"

# Legacy ``normalized_occupancy_final`` clips its output to dates >= this.
# Any new rows for earlier dates are "out of scope" and flagged as such.
LEGACY_NOCC_FIRST_DATE: date = date(2026, 3, 1)


# ---------------------------------------------------------------------------
# Known legacy carve-outs
# ---------------------------------------------------------------------------

# Full-day outage exclusions (``a.date NOT IN (...)`` in
# ``occupancy_agents_information_2026``).
LEGACY_OUTAGE_DATES: frozenset[date] = frozenset(
    {date(2026, 3, 27), date(2026, 4, 9)}
)


def _expand_agent_date_carve_outs() -> frozenset[tuple[str, date]]:
    """Collect the full-day, all-slot agent×date exclusions.

    Mirrors the ``NOT (a.agent IN (...) AND a.date IN (...))`` and
    ``NOT (a.agent = '...' AND a.date = '...')`` blocks in
    ``occupancy_agents_information_2026``, plus the
    ``dime_table_occupancy`` per-agent reclassifications that effectively
    drop rows in 2026 (the only one is maria.reyes Feb 2026 — and she's
    also in the explicit list, but we keep both for robustness).
    """
    out: set[tuple[str, date]] = set()
    # jonathan.pineda one-off
    out.add(("jonathan.pineda", date(2026, 2, 26)))
    # March 24-28 access-problem cluster
    for agent in (
        "jose.velez",
        "carlos.gonzalez",
        "jorge.ortega",
        "luisa.castaneda",
        "janet.castro",
        "karen.ortega",
    ):
        for day in (24, 25, 26, 27, 28):
            out.add((agent, date(2026, 3, day)))
    # maria.reyes February 2026 maternity (also covered by the legacy
    # dime-side time_off reclassification; the date-list exclusion makes
    # it explicit at the row-drop level).
    d = date(2026, 2, 1)
    while d < date(2026, 3, 1):
        out.add(("maria.reyes", d))
        d += timedelta(days=1)
    return frozenset(out)


LEGACY_AGENT_DATE_CARVE_OUTS: frozenset[tuple[str, date]] = (
    _expand_agent_date_carve_outs()
)


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _floor_month(d: date) -> date:
    return d.replace(day=1)


def _run_new_pipeline(
    conn,
    period_start: date,
    period_end: date,
    data_end: date,
) -> pd.DataFrame:
    """Replay scripts/build_nocc.py in-memory.

    The DIME / jobs / roster windows are widened to
    ``floor_month(period_start) .. data_end`` so the monthly
    ``occupancy_exp`` benchmark is computed off the same volume of data
    legacy's refresh would have seen. The output is then clipped to
    ``period_start .. period_end`` before being returned.
    """
    data_start = _floor_month(period_start)
    LOGGER.info(
        "Data window: %s .. %s (output filter: %s .. %s)",
        data_start,
        data_end,
        period_start,
        period_end,
    )

    LOGGER.info("Pulling agent_information ...")
    t0 = time.perf_counter()
    agent_info = run_extractor(conn, "agent_information", data_start, data_end)
    LOGGER.info(
        "  %s rows in %.1fs", f"{len(agent_info):,}", time.perf_counter() - t0
    )

    LOGGER.info("Pulling dime_slots ...")
    t0 = time.perf_counter()
    dime = run_extractor(conn, "dime_slots", data_start, data_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(dime):,}", time.perf_counter() - t0)

    LOGGER.info("Pulling shuffle_jobs ...")
    t0 = time.perf_counter()
    shuffle = run_extractor(conn, "shuffle_jobs", data_start, data_end)
    LOGGER.info(
        "  %s rows in %.1fs", f"{len(shuffle):,}", time.perf_counter() - t0
    )

    LOGGER.info("Pulling oos_jobs ...")
    t0 = time.perf_counter()
    oos = run_extractor(conn, "oos_jobs", data_start, data_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(oos):,}", time.perf_counter() - t0)

    LOGGER.info("Computing NOcc ...")
    t0 = time.perf_counter()
    new = compute_nocc(agent_info, dime, shuffle, oos)
    LOGGER.info(
        "  %s output rows in %.1fs", f"{len(new):,}", time.perf_counter() - t0
    )

    new = new[
        (new["date"] >= period_start) & (new["date"] <= period_end)
    ].copy()
    return new


# ---------------------------------------------------------------------------
# Legacy puller
# ---------------------------------------------------------------------------


def _pull_legacy(conn, period_start: date, period_end: date) -> pd.DataFrame:
    LOGGER.info("Pulling legacy %s ...", LEGACY_TABLE)
    t0 = time.perf_counter()
    # squad_district is selected so we can classify Content-roster rows
    # as expected (their squad_district='content' in the legacy union).
    sql = f"""
        SELECT
          agent,
          CAST(date AS DATE) AS date,
          slot_start,
          squad,
          squad_district,
          shift,
          occupancy_time,
          job_time,
          occupancy_exp
        FROM {LEGACY_TABLE}
        WHERE date >= :period_start AND date <= :period_end
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            parameters={"period_start": period_start, "period_end": period_end},
        )
        df = cur.fetchall_arrow().to_pandas()
    LOGGER.info("  %s rows in %.1fs", f"{len(df):,}", time.perf_counter() - t0)
    return df


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


KEY_COLS = ["agent", "date", "slot_start"]
INT_COMPARED = ("occupancy_time", "job_time")
FLOAT_COMPARED = ("occupancy_exp",)
LEGACY_AUX_COLS = ("squad_district",)

# Float tolerance for occupancy_exp "exact" match. The benchmark is a
# ratio of seconds / seconds, max value 1.0. 1e-6 is comfortable below
# any meaningful divergence and above pure float64 round-off.
FLOAT_TOL = 1e-6

# Maximum acceptable |Δ| for occupancy_exp before a mismatch is treated
# as unexplained. occupancy_exp is the monthly (district, shift)
# benchmark, computed off SUM(occupancy_time) / SUM(job_time) across the
# month. Legacy's row-level carve-outs (outage-date, agent-date
# exclusions) remove rows from that aggregation — our pipeline doesn't,
# so the benchmark drifts by an amount bounded by the carve-outs' size.
# Empirically (Mar-May 2026) this drift maxes out around 0.012 (1.2%).
# We accept anything ≤ 0.05 as "benchmark drift (downstream of
# carve-outs)" and only flag larger drifts as a real divergence.
BENCHMARK_DRIFT_TOL = 0.05


def _normalize_for_join(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Coerce date and numeric columns into stable types for the merge.

    Legacy returns ``occupancy_exp`` as ``decimal.Decimal``; our pipeline
    produces ``float64``. Same for ``slot_start`` (BIGINT vs int64). We
    cast every column we'll subtract or compare so the rest of the
    report doesn't need to care.
    """
    out = df.copy()
    if pd.api.types.is_datetime64_any_dtype(out["date"]):
        out["date"] = out["date"].dt.date
    if "slot_start" in out.columns:
        out["slot_start"] = pd.to_numeric(out["slot_start"], errors="coerce").astype(
            "Int64"
        )
    for col in INT_COMPARED:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    for col in FLOAT_COMPARED:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    return out[[c for c in cols if c in out.columns]]


def _classify_only_in_new(row) -> str:
    """Bucket an only-in-new row by its known adjustment cause."""
    d: date = row["date"]
    agent: str = row["agent"]
    if d < LEGACY_NOCC_FIRST_DATE:
        return "pre-2026-03-01 (out of legacy scope)"
    if d in LEGACY_OUTAGE_DATES:
        return "outage-date"
    if (agent, d) in LEGACY_AGENT_DATE_CARVE_OUTS:
        return "agent-date carve-out"
    return "unexplained"


def _classify_only_in_legacy(row) -> str:
    """Bucket an only-in-legacy row by its known cause.

    The dominant only-in-legacy class is the Content roster — legacy
    unions ``gsheets.sheets.mx_content_bdx`` on top of BDX, and those
    rows carry ``squad_district='content'``. Our pipeline is BDX-only.
    """
    squad_district = row.get("squad_district")
    if isinstance(squad_district, str) and squad_district.lower() == "content":
        return "content-roster (out of scope)"
    return "unexplained"


def _classify_value_mismatch(row) -> str:
    """Same buckets as only-in-new, applied to in-both rows.

    Used to explain value mismatches (occupancy_time, occupancy_exp) by
    the same known carve-out causes — although for in-both rows the
    only expected carve-out class today is the BKO-style dim-activity
    carve-out we don't model in NOcc (NOcc's legacy notebook has no
    ``manual_adjustments_nocc`` equivalent).
    """
    return _classify_only_in_new(row)


def _format_int(n: int) -> str:
    return f"{n:,}"


def _pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "n/a"
    return f"{100.0 * numerator / denominator:.2f}%"


def report(
    new: pd.DataFrame,
    legacy: pd.DataFrame,
    period_start: date,
    period_end: date,
    top_n: int = 10,
    csv_out: Path | None = None,
) -> int:
    """Print the parity report and return an exit code.

    Returns 0 if every divergence falls in a known-adjustment bucket;
    1 otherwise.
    """
    new_cols = KEY_COLS + list(INT_COMPARED) + list(FLOAT_COMPARED)
    leg_cols = new_cols + list(LEGACY_AUX_COLS)
    new_n = _normalize_for_join(new, new_cols)
    leg_n = _normalize_for_join(legacy, leg_cols)

    merged = new_n.merge(
        leg_n, on=KEY_COLS, how="outer", suffixes=("_new", "_legacy"), indicator=True
    )

    only_new = merged[merged["_merge"] == "left_only"].copy()
    only_legacy = merged[merged["_merge"] == "right_only"].copy()
    both = merged[merged["_merge"] == "both"].copy()

    if not only_new.empty:
        only_new["cause"] = only_new.apply(_classify_only_in_new, axis=1)
        cause_counts = only_new["cause"].value_counts()
    else:
        only_new["cause"] = pd.Series(dtype="object")
        cause_counts = pd.Series(dtype="int64")

    if not only_legacy.empty:
        only_legacy["cause"] = only_legacy.apply(_classify_only_in_legacy, axis=1)
        only_legacy_cause_counts = only_legacy["cause"].value_counts()
    else:
        only_legacy["cause"] = pd.Series(dtype="object")
        only_legacy_cause_counts = pd.Series(dtype="int64")

    # Per-column deltas. Cast through float64 so the pandas nullable
    # Int64 doesn't propagate masked values awkwardly.
    for col in INT_COMPARED:
        both[f"delta_{col}"] = (
            both[f"{col}_new"].astype("float64")
            - both[f"{col}_legacy"].astype("float64")
        )
    for col in FLOAT_COMPARED:
        both[f"delta_{col}"] = (
            both[f"{col}_new"].astype("float64")
            - both[f"{col}_legacy"].astype("float64")
        )

    exact_occ = (both["delta_occupancy_time"] == 0).sum()
    exact_job = (both["delta_job_time"] == 0).sum()
    exact_exp = (both["delta_occupancy_exp"].abs() < FLOAT_TOL).sum()

    # Classify value mismatches (occupancy_time only — job_time is always
    # 1800 and any difference is itself a bug, not an adjustment).
    occ_mismatch = both[both["delta_occupancy_time"] != 0].copy()
    if not occ_mismatch.empty:
        occ_mismatch["cause"] = occ_mismatch.apply(_classify_value_mismatch, axis=1)
        occ_cause_counts = occ_mismatch["cause"].value_counts()
    else:
        occ_mismatch["cause"] = pd.Series(dtype="object")
        occ_cause_counts = pd.Series(dtype="int64")

    # Classify occupancy_exp mismatches. Small drifts are the expected
    # downstream effect of legacy carve-outs that change the benchmark
    # denominator; larger drifts indicate a real divergence.
    exp_mismatch = both[both["delta_occupancy_exp"].abs() >= FLOAT_TOL].copy()
    if not exp_mismatch.empty:
        small_drift = (
            exp_mismatch["delta_occupancy_exp"].abs() <= BENCHMARK_DRIFT_TOL
        )
        exp_mismatch["cause"] = "unexplained"
        exp_mismatch.loc[small_drift, "cause"] = (
            "benchmark drift (downstream of carve-outs)"
        )
        exp_cause_counts = exp_mismatch["cause"].value_counts()
    else:
        exp_mismatch["cause"] = pd.Series(dtype="object")
        exp_cause_counts = pd.Series(dtype="int64")

    unexplained_only_new = int(
        (only_new["cause"] == "unexplained").sum() if not only_new.empty else 0
    )
    unexplained_only_legacy = int(
        (only_legacy["cause"] == "unexplained").sum() if not only_legacy.empty else 0
    )
    unexplained_occ_mismatch = int(
        (occ_mismatch["cause"] == "unexplained").sum() if not occ_mismatch.empty else 0
    )
    unexplained_exp_mismatch = int(
        (exp_mismatch["cause"] == "unexplained").sum() if not exp_mismatch.empty else 0
    )

    # ----- print -----
    print()
    print("=" * 78)
    print(f" NOcc parity — {period_start} .. {period_end}")
    print(f" New pipeline output  vs  legacy `{LEGACY_TABLE}`")
    print(" Key: (agent, date, slot_start)")
    print("=" * 78)

    print()
    print("Coverage")
    print("--------")
    print(f"  In both:                {_format_int(len(both)):>10}")
    print(
        f"  Only in new:            {_format_int(len(only_new)):>10}"
        f"  ({_pct(len(only_new), len(merged))})"
    )
    print(
        f"  Only in legacy:         {_format_int(len(only_legacy)):>10}"
        f"  ({_pct(len(only_legacy), len(merged))})"
    )

    if not cause_counts.empty:
        print()
        print("  Only-in-new — by known cause:")
        for cause, n in cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<40} {_format_int(int(n)):>8}{tag}")

    if not only_legacy_cause_counts.empty:
        print()
        print("  Only-in-legacy — by known cause:")
        for cause, n in only_legacy_cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<40} {_format_int(int(n)):>8}{tag}")
        unexplained = only_legacy[only_legacy["cause"] == "unexplained"]
        if not unexplained.empty:
            ol_summary = (
                unexplained.groupby(["agent", "date"], as_index=False)
                .size()
                .sort_values("size", ascending=False)
                .head(10)
            )
            print("  Top unexplained (agent, date) only-in-legacy:")
            print(ol_summary.to_string(index=False))

    print()
    print("Matching (rows in both)")
    print("-----------------------")
    print(
        f"  occupancy_time exact match: {_format_int(int(exact_occ)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(exact_occ), len(both))})"
    )
    print(
        f"  job_time       exact match: {_format_int(int(exact_job)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(exact_job), len(both))})"
    )
    print(
        f"  occupancy_exp  ≈ match (|Δ|<{FLOAT_TOL:g}): "
        f"{_format_int(int(exact_exp)):>10} / {_format_int(len(both))}  "
        f"({_pct(int(exact_exp), len(both))})"
    )

    if not occ_cause_counts.empty:
        print()
        print("  occupancy_time mismatch — by known cause:")
        for cause, n in occ_cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<40} {_format_int(int(n)):>8}{tag}")

    if not exp_cause_counts.empty:
        print()
        print(
            f"  occupancy_exp mismatch (|Δ|>={FLOAT_TOL:g}) — by known cause:"
        )
        for cause, n in exp_cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<40} {_format_int(int(n)):>8}{tag}")

    if not both.empty:
        print()
        print("Delta distribution — occupancy_time seconds (new − legacy)")
        print("----------------------------------------------------------")
        print(both["delta_occupancy_time"].describe().to_string())

        print()
        print("Delta distribution — job_time seconds (new − legacy)")
        print("----------------------------------------------------")
        print(both["delta_job_time"].describe().to_string())

        print()
        print("Delta distribution — occupancy_exp ratio (new − legacy)")
        print("-------------------------------------------------------")
        print(both["delta_occupancy_exp"].describe().to_string())

        # Top divergences by combined |Δ|. Normalize to a comparable
        # scale: occupancy_time seconds, job_time seconds (rarely
        # mismatched), occupancy_exp × 1800 (puts the ratio on a
        # seconds-equivalent scale).
        both["abs_delta_score"] = (
            both["delta_occupancy_time"].abs()
            + both["delta_job_time"].abs()
            + both["delta_occupancy_exp"].abs() * 1800.0
        )
        worst = both.sort_values("abs_delta_score", ascending=False).head(top_n)
        if (worst["abs_delta_score"] > 0).any():
            print()
            print(f"Top {top_n} divergences (rows in both, by combined |Δ|)")
            print("-" * 78)
            display_cols = [
                "agent",
                "date",
                "slot_start",
                "occupancy_time_new",
                "occupancy_time_legacy",
                "delta_occupancy_time",
                "delta_job_time",
                "delta_occupancy_exp",
            ]
            with pd.option_context("display.max_colwidth", 60):
                print(worst[display_cols].to_string(index=False))

    if csv_out is not None:
        csv_out.parent.mkdir(parents=True, exist_ok=True)
        merged_for_csv = merged.copy()
        for col in INT_COMPARED:
            merged_for_csv[f"delta_{col}"] = (
                merged_for_csv.get(f"{col}_new", 0)
                - merged_for_csv.get(f"{col}_legacy", 0)
            )
        for col in FLOAT_COMPARED:
            merged_for_csv[f"delta_{col}"] = (
                merged_for_csv.get(f"{col}_new", 0)
                - merged_for_csv.get(f"{col}_legacy", 0)
            )
        merged_for_csv["cause"] = pd.NA
        if not only_new.empty:
            merged_for_csv.loc[only_new.index, "cause"] = only_new["cause"]
        if not only_legacy.empty:
            merged_for_csv.loc[only_legacy.index, "cause"] = only_legacy["cause"]
        merged_for_csv.to_csv(csv_out, index=False)
        print()
        print(f"Wrote full diff to {csv_out}")

    print()
    print("=" * 78)
    if (
        unexplained_only_new == 0
        and unexplained_only_legacy == 0
        and unexplained_occ_mismatch == 0
        and unexplained_exp_mismatch == 0
    ):
        print(" Verdict: every divergence is explained by a known legacy adjustment.")
        verdict = 0
    else:
        print(
            f" Verdict: {unexplained_only_new:,} unexplained only-in-new + "
            f"{unexplained_only_legacy:,} unexplained only-in-legacy + "
            f"{unexplained_occ_mismatch:,} unexplained occupancy_time mismatches + "
            f"{unexplained_exp_mismatch:,} unexplained occupancy_exp mismatches. "
            "Investigate."
        )
        verdict = 1
    print("=" * 78)
    print()

    return verdict


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--data-end-date",
        type=date.fromisoformat,
        default=None,
        help=(
            "Upper bound on DIME / jobs / roster extraction. Defaults to "
            "today's date. The monthly occupancy_exp benchmark uses every "
            "row available at run time. For matching a recently-refreshed "
            "legacy table, set this to the date legacy was last refreshed."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="How many largest divergences to print (default: 10).",
    )
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=None,
        help="If set, write the full per-row diff to this CSV.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(levelname)s %(name)s: %(message)s"
    )

    if args.period_end < args.period_start:
        LOGGER.error("--period-end must be >= --period-start")
        return 2

    data_end = args.data_end_date or datetime.now().date()
    if data_end < args.period_end:
        LOGGER.warning(
            "--data-end-date %s precedes --period-end %s; the monthly "
            "benchmark will use less data than a freshly-refreshed legacy "
            "table.",
            data_end,
            args.period_end,
        )

    conn = open_connection()
    try:
        new_df = _run_new_pipeline(
            conn, args.period_start, args.period_end, data_end
        )
        legacy_df = _pull_legacy(conn, args.period_start, args.period_end)
    finally:
        conn.close()

    return report(
        new=new_df,
        legacy=legacy_df,
        period_start=args.period_start,
        period_end=args.period_end,
        top_n=args.top_n,
        csv_out=args.csv_out,
    )


if __name__ == "__main__":
    sys.exit(main())
