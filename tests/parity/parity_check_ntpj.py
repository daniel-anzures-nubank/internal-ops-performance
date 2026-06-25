"""Parity check: new `compute_ntpj` vs. legacy `usr.mx__cx.normalized_time_per_job`.

What this does
--------------
1. Runs the four extractors (agent_information, dime_slots, shuffle_jobs,
   oos_jobs) for the requested period (with auto-lookback on jobs).
2. Calls `compute_ntpj` to get the new pipeline's per-(agent, start_date,
   job_id) frame.
3. Pulls the legacy table for the same period.
4. Outer-joins them on (agent, start_date, job_id) and reports:
     * Coverage (rows in both / only-new / only-legacy).
     * Per-column exact-match rate (count, duration, exp_duration_job,
       required_hours).
     * Delta distribution and the largest individual divergences.
     * A "known adjustments" breakdown — divergences that fall on dates
       or agent×date combinations the legacy hardcodes as outages /
       carve-outs are flagged separately.

Why this is a script and not a pytest test
------------------------------------------
Parity here is not binary. The legacy table has manual adjustments baked
in that we deliberately do not replicate (the `manual_adjustments_ntpj`
CASE block plus the outage-date / agent-date carve-outs). So we'd expect
any "all-or-nothing" assertion to fail on perfectly correct output. The
right tool is a structured diagnostic report.

The diff classes (in priority order)
------------------------------------
* **outage-date** — legacy drops rows where `start_date IN ('2026-03-27',
  '2026-04-09')`. Our new pipeline keeps them. Shows up as only-in-new.
* **agent-date carve-out** — full-day exclusions in legacy
  (jonathan.pineda 2026-02-26, the jose.velez et al. 2026-03-24..28
  cluster, maria.reyes February-2026 maternity). Shows up as only-in-new.
* **dimensioned-activity carve-out** — slot-level exclusions in the
  legacy ``manual_adjustments_ntpj`` view, scoped to specific agents +
  ``bko_lcyc`` / ``bko_cta_tskf`` activity + date ranges. Effect: in
  legacy, the corresponding backoffice ``required_hours`` for those
  agents/dates is REDUCED. We don't apply the reduction → expect a
  required_hours delta on those (agent, start_date, job_id) rows whose
  activity_type is ``backoffice``. We flag these rows as
  ``dim-activity carve-out`` so they don't muddy the "unexplained"
  bucket.
* **unexplained** — everything else. The investigative target.

Usage
-----
::

    uv run python tests/parity/parity_check_ntpj.py \\
        --period-start 2026-05-11 --period-end 2026-05-17

    uv run python tests/parity/parity_check_ntpj.py \\
        --period-start 2026-04-01 --period-end 2026-04-30 \\
        --csv-out /tmp/ntpj_diff.csv
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
from ntpj import compute_ntpj  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.parity.ntpj")

LEGACY_TABLE = "usr.mx__cx.normalized_time_per_job"

# Months of jobs lookback before period_start (matches scripts/build_ntpj.py).
DEFAULT_BASELINE_LOOKBACK_MONTHS = 4


# ---------------------------------------------------------------------------
# Known legacy carve-outs — anything that lands here is "expected to differ"
# ---------------------------------------------------------------------------

# Full-day outage exclusions (legacy `WHERE start_date NOT IN (...)` in two
# places — `expected_duration_per_job_ntpj` and `ntpj_calculations`).
LEGACY_OUTAGE_DATES: frozenset[date] = frozenset(
    {date(2026, 3, 27), date(2026, 4, 9)}
)


def _expand_agent_date_carve_outs() -> frozenset[tuple[str, date]]:
    """Collect the full-day, all-activity-type agent×date exclusions.

    Mirrors the legacy `ntpj_all_info_2026` view's hardcoded NOT-IN list
    plus the dime-side ``maria.reyes`` maternity carve-out. Each entry
    here means: legacy has NO rows for this (agent, date); we expect to
    have some.
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
    # maria.reyes February 2026 maternity
    d = date(2026, 2, 1)
    while d < date(2026, 3, 1):
        out.add(("maria.reyes", d))
        d += timedelta(days=1)
    return frozenset(out)


LEGACY_AGENT_DATE_CARVE_OUTS: frozenset[tuple[str, date]] = (
    _expand_agent_date_carve_outs()
)


# Dimensioned-activity carve-outs from `manual_adjustments_ntpj`. Each entry
# is (agent, dimensioned_activity, start_date, end_date). The ranges are
# INCLUSIVE on both ends. These exclusions slot-level — they don't drop
# rows, they reduce backoffice required_hours for the matching (agent,
# date). We flag rows that fall in these windows so a required_hours
# delta on them doesn't get bucketed as "unexplained".
#
# `dimensioned_activity` values from legacy map to our shuffle activity_type
# 'backoffice' (legacy ``bko_lcyc`` / ``bko_cta_tskf`` are both BKO queues
# that show up under activity_type='backoffice' in jobs_base). So an
# (agent, date) hit on this list affects every row whose job_id starts
# with ``'bko - '``.
_DimCarveOut = tuple[str, date, date]  # (agent, start_date, end_date)
LEGACY_DIM_ACTIVITY_CARVE_OUTS: tuple[_DimCarveOut, ...] = (
    # BKO_LCYC (Lifecycle backoffice), Mar 10-27 2026
    ("elizabeth.martinez", date(2026, 3, 10), date(2026, 3, 27)),
    ("daniel.cano",        date(2026, 3, 11), date(2026, 3, 27)),
    ("bertha.sanchez",     date(2026, 3, 10), date(2026, 3, 27)),
    ("jonathan.pineda",    date(2026, 3, 10), date(2026, 3, 27)),
    ("sofia.orozco",       date(2026, 3, 10), date(2026, 3, 27)),
    ("jessica.gonzalez",   date(2026, 3, 10), date(2026, 3, 27)),
    ("jorge.ortega",       date(2026, 3, 10), date(2026, 3, 27)),
    ("nitza.zarza",        date(2026, 3, 10), date(2026, 3, 27)),
    # BKO_LCYC (EMI variant), Mar 10-29 2026
    ("fernanda.ibanez",    date(2026, 3, 10), date(2026, 3, 29)),
    ("jose.velez",         date(2026, 3, 10), date(2026, 3, 29)),
    ("ivette.melendez",    date(2026, 3, 10), date(2026, 3, 29)),
    ("erik.licona",        date(2026, 3, 10), date(2026, 3, 29)),
    # BKO_CTA_TSKF, ongoing from 2026-04-09 (adriana.marquez from 4-10,
    # jorge.severiano from 4-13). Open-ended on the right — legacy uses
    # 2099-12-31 so we mirror that.
    ("elizabeth.martinez", date(2026, 4, 9),  date(2099, 12, 31)),
    ("daniel.cano",        date(2026, 4, 9),  date(2099, 12, 31)),
    ("jonathan.pineda",    date(2026, 4, 9),  date(2099, 12, 31)),
    ("adriana.marquez",    date(2026, 4, 10), date(2099, 12, 31)),
    ("javier.balanzar",    date(2026, 4, 9),  date(2099, 12, 31)),
    ("carlos.gonzalez",    date(2026, 4, 9),  date(2099, 12, 31)),
    ("eden.martinez",      date(2026, 4, 9),  date(2099, 12, 31)),
    ("mariana.infante",    date(2026, 4, 9),  date(2099, 12, 31)),
    ("jorge.severiano",    date(2026, 4, 13), date(2099, 12, 31)),
    ("fernanda.ibanez",    date(2026, 4, 9),  date(2099, 12, 31)),
    ("jose.velez",         date(2026, 4, 9),  date(2099, 12, 31)),
    ("ivette.melendez",    date(2026, 4, 9),  date(2099, 12, 31)),
    ("rocio.rodriguez",    date(2026, 4, 9),  date(2099, 12, 31)),
)


def _is_dim_carved_out(agent: str, d: date) -> bool:
    """True if (agent, d) is inside any BKO carve-out window."""
    for a, s, e in LEGACY_DIM_ACTIVITY_CARVE_OUTS:
        if a == agent and s <= d <= e:
            return True
    return False


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _floor_month(d: date) -> date:
    return d.replace(day=1)


def _subtract_months(d: date, months: int) -> date:
    total = d.year * 12 + (d.month - 1) - months
    year, month = divmod(total, 12)
    return date(year, month + 1, 1)


def _run_new_pipeline(
    conn,
    period_start: date,
    period_end: date,
    baseline_lookback_months: int,
    jobs_end: date,
) -> pd.DataFrame:
    """Replay scripts/build_ntpj.py in-memory.

    ``jobs_end`` is the upper bound on the shuffle/OOS extraction; it can
    extend past ``period_end`` so the monthly ``exp_duration_job`` baseline
    sees the same volume of jobs the legacy table did. DIME and the roster
    stay clamped to ``period_end`` (no point pulling DIME the warehouse
    hasn't published yet).
    """
    jobs_start = _subtract_months(
        _floor_month(period_start), baseline_lookback_months
    )
    LOGGER.info(
        "Jobs window: %s .. %s (lookback %d months, end overridable). "
        "DIME / roster: %s .. %s",
        jobs_start,
        jobs_end,
        baseline_lookback_months,
        period_start,
        period_end,
    )

    LOGGER.info("Pulling agent_information ...")
    t0 = time.perf_counter()
    agent_info = run_extractor(conn, "agent_information", period_start, period_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(agent_info):,}", time.perf_counter() - t0)

    LOGGER.info("Pulling dime_slots ...")
    t0 = time.perf_counter()
    dime = run_extractor(conn, "dime_slots", period_start, period_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(dime):,}", time.perf_counter() - t0)

    LOGGER.info("Pulling shuffle_jobs (lookback included) ...")
    t0 = time.perf_counter()
    shuffle = run_extractor(conn, "shuffle_jobs", jobs_start, jobs_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(shuffle):,}", time.perf_counter() - t0)

    LOGGER.info("Pulling oos_jobs (lookback included) ...")
    t0 = time.perf_counter()
    oos = run_extractor(conn, "oos_jobs", jobs_start, jobs_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(oos):,}", time.perf_counter() - t0)

    LOGGER.info("Computing NTPJ ...")
    t0 = time.perf_counter()
    new = compute_ntpj(agent_info, dime, shuffle, oos)
    LOGGER.info("  %s output rows in %.1fs", f"{len(new):,}", time.perf_counter() - t0)

    # Restrict to the requested period — the new pipeline only outputs rows
    # for dates with DIME slots in the period, but we make this explicit for
    # safety in case the extractor ever leaks rows.
    new = new[
        (new["start_date"] >= period_start) & (new["start_date"] <= period_end)
    ].copy()
    return new


# ---------------------------------------------------------------------------
# Legacy puller
# ---------------------------------------------------------------------------


def _pull_legacy(conn, period_start: date, period_end: date) -> pd.DataFrame:
    LOGGER.info("Pulling legacy %s ...", LEGACY_TABLE)
    t0 = time.perf_counter()
    # `squad_district` is selected so we can classify Content-roster rows
    # as expected (Content agents come from `gsheets.sheets.mx_content_bdx`
    # in legacy; their `squad_district` is set to 'content' on output).
    sql = f"""
        SELECT
          agent,
          CAST(start_date AS DATE) AS start_date,
          job_id,
          activity_type,
          squad,
          squad_district,
          count,
          duration,
          exp_duration_job,
          required_hours
        FROM {LEGACY_TABLE}
        WHERE start_date >= :period_start AND start_date <= :period_end
    """
    with conn.cursor() as cur:
        cur.execute(
            sql, parameters={"period_start": period_start, "period_end": period_end}
        )
        df = cur.fetchall_arrow().to_pandas()
    LOGGER.info("  %s rows in %.1fs", f"{len(df):,}", time.perf_counter() - t0)
    return df


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

KEY_COLS = ["agent", "start_date", "job_id"]
INT_COMPARED = ("count", "duration")
FLOAT_COMPARED = ("exp_duration_job", "required_hours")
# Columns we keep around the merge purely for classification — they're not
# compared, but we use them to bucket only-in-legacy rows.
LEGACY_AUX_COLS = ("squad_district",)
# A delta smaller than this is "noise" (float precision). required_hours
# values are 0.5-increments; exp_duration_job is in seconds where 0.01s
# of diff is meaningless. One threshold covers both.
FLOAT_TOL = 0.01


def _normalize_for_join(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Coerce date and numeric columns into stable types for the merge.

    The legacy warehouse returns DECIMAL columns (``exp_duration_job``,
    ``required_hours``) as ``decimal.Decimal`` Python objects, which don't
    interoperate with our float64 pipeline output (Pandas raises
    ``TypeError`` on the subtraction). We cast every compared column up
    front so the rest of the report logic doesn't need to know which side
    came from where.
    """
    out = df.copy()
    if pd.api.types.is_datetime64_any_dtype(out["start_date"]):
        out["start_date"] = out["start_date"].dt.date
    for col in INT_COMPARED:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    for col in FLOAT_COMPARED:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    # Auxiliary classification columns (e.g. squad_district): pass through
    # as-is, just ensure they exist.
    return out[[c for c in cols if c in out.columns]]


def _classify_only_in_new(row) -> str:
    """Bucket an only-in-new row by its known adjustment cause.

    Order matters: outage-date is the broadest (whole-day, all agents),
    then agent-date carve-out (whole-day, specific agent), then dim-activity
    (specific agent × backoffice job_id × date range). The ``unexplained``
    bucket is what we want to drive to zero.
    """
    agent: str = row["agent"]
    d: date = row["start_date"]
    job_id: str = row["job_id"]

    if d in LEGACY_OUTAGE_DATES:
        return "outage-date"
    if (agent, d) in LEGACY_AGENT_DATE_CARVE_OUTS:
        return "agent-date carve-out"
    if job_id.startswith("bko - ") and _is_dim_carved_out(agent, d):
        return "dim-activity carve-out (BKO)"
    return "unexplained"


def _classify_only_in_legacy(row) -> str:
    """Bucket an only-in-legacy row by its known cause.

    Out-of-scope content agents end up here because legacy unions a
    Google-Sheets-based content roster on top of the BDX-based core
    roster. Their legacy output rows carry ``squad_district='content'``,
    which is the most reliable signal.
    """
    squad_district = row.get("squad_district")
    if isinstance(squad_district, str) and squad_district.lower() == "content":
        return "content-roster (out of scope)"
    return "unexplained"


def _classify_required_hours_delta(row) -> str:
    """Same buckets as `_classify_only_in_new`, applied to in-both rows.

    Only used to explain required_hours mismatches — the BKO carve-outs
    REDUCE legacy required_hours, so we expect a non-zero ``delta_required``
    on those rows.
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

    # Classify only-in-new rows
    if not only_new.empty:
        only_new["cause"] = only_new.apply(_classify_only_in_new, axis=1)
        cause_counts = only_new["cause"].value_counts()
    else:
        only_new["cause"] = pd.Series(dtype="object")
        cause_counts = pd.Series(dtype="int64")

    # Classify only-in-legacy rows
    if not only_legacy.empty:
        only_legacy["cause"] = only_legacy.apply(_classify_only_in_legacy, axis=1)
        only_legacy_cause_counts = only_legacy["cause"].value_counts()
    else:
        only_legacy["cause"] = pd.Series(dtype="object")
        only_legacy_cause_counts = pd.Series(dtype="int64")

    # Per-column deltas for rows in both. Cast through float64 so the
    # pandas nullable Int64 doesn't propagate masked values awkwardly.
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

    exact_count = (both["delta_count"] == 0).sum()
    exact_duration = (both["delta_duration"] == 0).sum()
    exact_exp = (both["delta_exp_duration_job"].abs() < FLOAT_TOL).sum()
    exact_req = (both["delta_required_hours"].abs() < FLOAT_TOL).sum()

    # Classify required_hours mismatches by carve-out window
    req_mismatch = both[both["delta_required_hours"].abs() >= FLOAT_TOL].copy()
    if not req_mismatch.empty:
        req_mismatch["cause"] = req_mismatch.apply(_classify_required_hours_delta, axis=1)
        req_cause_counts = req_mismatch["cause"].value_counts()
    else:
        req_mismatch["cause"] = pd.Series(dtype="object")
        req_cause_counts = pd.Series(dtype="int64")

    unexplained_only_new = int(
        (only_new["cause"] == "unexplained").sum() if not only_new.empty else 0
    )
    unexplained_only_legacy = int(
        (only_legacy["cause"] == "unexplained").sum()
        if not only_legacy.empty
        else 0
    )
    unexplained_req_mismatch = int(
        (req_mismatch["cause"] == "unexplained").sum() if not req_mismatch.empty else 0
    )

    # ----- print -----
    print()
    print("=" * 78)
    print(f" NTPJ parity — {period_start} .. {period_end}")
    print(f" New pipeline output  vs  legacy `{LEGACY_TABLE}`")
    print(" Key: (agent, start_date, job_id)")
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
            print(f"    {cause:<32} {_format_int(int(n)):>8}{tag}")

    if not only_legacy_cause_counts.empty:
        print()
        print("  Only-in-legacy — by known cause:")
        for cause, n in only_legacy_cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<32} {_format_int(int(n)):>8}{tag}")

        # Surface the top "unexplained" (agent, start_date) groups so a
        # human can chase any real divergence quickly.
        unexplained = only_legacy[only_legacy["cause"] == "unexplained"]
        if not unexplained.empty:
            ol_summary = (
                unexplained.groupby(["agent", "start_date"], as_index=False)
                .size()
                .sort_values("size", ascending=False)
                .head(10)
            )
            print("  Top unexplained (agent, start_date) only-in-legacy:")
            print(ol_summary.to_string(index=False))

    print()
    print("Matching (rows in both)")
    print("-----------------------")
    print(
        f"  count             exact match: {_format_int(int(exact_count)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(exact_count), len(both))})"
    )
    print(
        f"  duration          exact match: {_format_int(int(exact_duration)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(exact_duration), len(both))})"
    )
    print(
        f"  exp_duration_job  ≈ match (|Δ|<{FLOAT_TOL}): "
        f"{_format_int(int(exact_exp)):>10} / {_format_int(len(both))}  "
        f"({_pct(int(exact_exp), len(both))})"
    )
    print(
        f"  required_hours    ≈ match (|Δ|<{FLOAT_TOL}): "
        f"{_format_int(int(exact_req)):>10} / {_format_int(len(both))}  "
        f"({_pct(int(exact_req), len(both))})"
    )

    if not req_cause_counts.empty:
        print()
        print("  required_hours mismatch — by known cause:")
        for cause, n in req_cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<32} {_format_int(int(n)):>8}{tag}")

    if not both.empty:
        print()
        print("Delta distribution — count (new − legacy)")
        print("-----------------------------------------")
        print(both["delta_count"].describe().to_string())

        print()
        print("Delta distribution — duration seconds (new − legacy)")
        print("----------------------------------------------------")
        print(both["delta_duration"].describe().to_string())

        print()
        print("Delta distribution — exp_duration_job seconds (new − legacy)")
        print("------------------------------------------------------------")
        print(both["delta_exp_duration_job"].describe().to_string())

        print()
        print("Delta distribution — required_hours (new − legacy)")
        print("--------------------------------------------------")
        print(both["delta_required_hours"].describe().to_string())

        # Top divergences by total absolute delta (normalize columns to a
        # roughly comparable scale: divide duration / exp_duration_job by
        # 60 to put them in minutes, leave count and required_hours alone).
        both["abs_delta_score"] = (
            both["delta_count"].abs()
            + both["delta_duration"].abs() / 60.0
            + both["delta_exp_duration_job"].abs() / 60.0
            + both["delta_required_hours"].abs() * 60.0
        )
        worst = both.sort_values("abs_delta_score", ascending=False).head(top_n)
        if (worst["abs_delta_score"] > 0).any():
            print()
            print(f"Top {top_n} divergences (rows in both, by combined |Δ|)")
            print("-" * 78)
            display_cols = [
                "agent",
                "start_date",
                "job_id",
                "count_new",
                "count_legacy",
                "delta_count",
                "duration_new",
                "duration_legacy",
                "delta_duration",
                "delta_exp_duration_job",
                "delta_required_hours",
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
        merged_for_csv.to_csv(csv_out, index=False)
        print()
        print(f"Wrote full diff to {csv_out}")

    print()
    print("=" * 78)
    if (
        unexplained_only_new == 0
        and unexplained_req_mismatch == 0
        and unexplained_only_legacy == 0
    ):
        print(" Verdict: every divergence is explained by a known legacy adjustment.")
        verdict = 0
    else:
        print(
            f" Verdict: {unexplained_only_new:,} unexplained only-in-new + "
            f"{unexplained_only_legacy:,} unexplained only-in-legacy + "
            f"{unexplained_req_mismatch:,} unexplained required_hours mismatches. "
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
        "--baseline-lookback-months",
        type=int,
        default=DEFAULT_BASELINE_LOOKBACK_MONTHS,
        help="Months of shuffle/OOS jobs lookback (default: 4, matches legacy).",
    )
    parser.add_argument(
        "--jobs-end-date",
        type=date.fromisoformat,
        default=None,
        help=(
            "Upper bound on the shuffle/OOS jobs extraction. Defaults to "
            "today's date. For the rolling exp_duration_job baseline to "
            "match a recently-refreshed legacy table, this should match "
            "(or exceed) the date the legacy was last computed — the "
            "current-month baseline uses every job available at that time, "
            "not just jobs up through --period-end."
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

    jobs_end = args.jobs_end_date or datetime.now().date()
    if jobs_end < args.period_end:
        LOGGER.warning(
            "--jobs-end-date %s precedes --period-end %s; the baseline will "
            "use less data than a freshly-refreshed legacy table.",
            jobs_end,
            args.period_end,
        )

    conn = open_connection()
    try:
        new_df = _run_new_pipeline(
            conn,
            args.period_start,
            args.period_end,
            args.baseline_lookback_months,
            jobs_end,
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
