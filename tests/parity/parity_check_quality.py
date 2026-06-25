"""Parity check: new ``compute_quality`` vs. legacy ``usr.mx__cx.quality_io``.

What this does
--------------
1. Runs the two extractors (``agent_information``,
   ``playvox_evaluations``) for the requested period.
2. Calls ``compute_quality`` to get the new pipeline's
   per-(date, agent) frame.
3. Pulls the legacy table for the same period.
4. Outer-joins them on ``(date, agent)`` and reports:
     * Coverage (rows in both / only-new / only-legacy).
     * Per-column match rate (qa_score within tolerance, evaluations
       exact).
     * Delta distributions and the largest individual divergences.
     * A "known adjustments" breakdown.

Why we join on (date, agent) instead of all 6 keys
--------------------------------------------------
The legacy GROUP BY also includes ``xplead``, ``xforce``, ``squad``,
``squad_district`` — but those are functionally dependent on
``(agent, snapshot_month)``. If our pipeline picks a different
snapshot than legacy did (most commonly: pre-2025-12-01 where legacy
uses Dec-2025 frozen and we use the natural month), those 4 columns
end up different even though the row is logically the same. Joining
only on ``(date, agent)`` makes those mismatches show up as a roster
drift on the right axis, not as phantom only-in-new / only-in-legacy
rows.

Why this is a script and not a pytest test
------------------------------------------
Parity here is not binary. The legacy table has manual adjustments
baked in that we deliberately do not replicate:

* The hardcoded ``scorecard_id`` blacklist (4 specific IDs).
* The hardcoded ``evaluation_id`` blacklist (4 specific IDs).
* The hardcoded outage dates ``2026-03-27`` and ``2026-04-09``.
* The pre-2025-12-01 frozen-snapshot historical backfill.

So we'd expect any "all-or-nothing" assertion to fail on perfectly
correct output. The right tool is a structured diagnostic report.

The diff classes (in priority order)
------------------------------------
* **pre-2025-12-01 (frozen snapshot in legacy)** — legacy uses the
  Dec-2025 BDX snapshot for every row before this date; ours uses
  natural month. Can show up as only-in-new / only-in-legacy /
  value-mismatch depending on what the snapshot disagreement does to
  the agent's roster attribution.
* **outage date** — 2026-03-27 / 2026-04-09. Legacy drops these dates
  entirely; ours keeps them. Shows up as only-in-new.
* **scorecard/eval-id blacklist** — legacy drops evaluations whose
  ``scorecard_id`` or ``evaluation_id`` is in the hardcoded blacklist.
  Per-(agent, date) effect is either:
    - the row disappears from legacy (only-in-new) if ALL its
      evaluations were blacklisted, or
    - the row has different ``qa_score`` / ``evaluations`` if SOME
      were blacklisted.
  We pre-pull the blacklisted (agent, date) tuples from Playvox to
  flag affected rows.
* **roster drift** — the row exists in both with matching qa_score
  and evaluations, but ``xplead`` / ``xforce`` / ``squad`` /
  ``squad_district`` differ. Always benign once we've accepted that
  legacy and ours can pick different snapshots for the same logical
  row; reported separately so we can spot it growing.
* **unexplained** — everything else. The investigative target.

Usage
-----
::

    uv run python tests/parity/parity_check_quality.py \\
        --period-start 2026-05-11 --period-end 2026-05-17

    uv run python tests/parity/parity_check_quality.py \\
        --period-start 2026-01-01 --period-end 2026-05-24 \\
        --csv-out /tmp/quality_diff.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor  # noqa: E402
from quality import compute_quality  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.parity.quality")

LEGACY_TABLE = "usr.mx__cx.quality_io"

# Date at which the legacy table transitions from the 2025 "frozen
# Dec-2025 snapshot" path to the 2026 "natural snapshot_month" path.
LEGACY_FROZEN_SNAPSHOT_BOUNDARY: date = date(2025, 12, 1)

# Hardcoded outage-date exclusions from legacy (`NOT IN (...)`).
LEGACY_OUTAGE_DATES: frozenset[date] = frozenset(
    {date(2026, 3, 27), date(2026, 4, 9)}
)

# Hardcoded blacklists from legacy `qa_base`. Evaluations matching
# either of these IDs are dropped from the legacy aggregation but kept
# in ours (intentional — they're adjustments-layer concerns).
LEGACY_SCORECARD_ID_BLACKLIST: frozenset[str] = frozenset(
    {
        "68def79b3f83da8cc9cb5299",
        "6812b3e46abeabb0653d197e",
        "688017f4bb266bb43b6c9565",
        "68680819336107d9f140d1ce",
    }
)
LEGACY_EVALUATION_ID_BLACKLIST: frozenset[str] = frozenset(
    {
        "68646ed2f093c149757ba038",
        "687704e7a077fb121012dd5d",
        "688017f4bb266bb43b6c9565",
        "68680819336107d9f140d1ce",
    }
)

# Floating-point tolerance for qa_score parity. AVG vs. MEAN on the
# same numbers should be bit-identical in principle, but
# Spark↔pandas↔Arrow can drift at machine epsilon. 1e-6 is plenty for
# scores that live on the 0–100 scale.
QA_SCORE_TOL: float = 1e-6


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _run_new_pipeline(
    conn, period_start: date, period_end: date
) -> pd.DataFrame:
    """Replay scripts/build_quality.py in-memory."""
    LOGGER.info("Pulling agent_information ...")
    t0 = time.perf_counter()
    agent_info = run_extractor(conn, "agent_information", period_start, period_end)
    LOGGER.info(
        "  %s rows in %.1fs", f"{len(agent_info):,}", time.perf_counter() - t0
    )

    LOGGER.info("Pulling playvox_evaluations ...")
    t0 = time.perf_counter()
    playvox = run_extractor(
        conn, "playvox_evaluations", period_start, period_end
    )
    LOGGER.info("  %s rows in %.1fs", f"{len(playvox):,}", time.perf_counter() - t0)

    LOGGER.info("Computing quality ...")
    t0 = time.perf_counter()
    new = compute_quality(agent_info, playvox)
    LOGGER.info(
        "  %s output rows in %.1fs", f"{len(new):,}", time.perf_counter() - t0
    )
    return new


# ---------------------------------------------------------------------------
# Legacy puller + blacklist enumerator
# ---------------------------------------------------------------------------


def _pull_legacy(conn, period_start: date, period_end: date) -> pd.DataFrame:
    LOGGER.info("Pulling legacy %s ...", LEGACY_TABLE)
    t0 = time.perf_counter()
    sql = f"""
        SELECT
          CAST(date AS DATE) AS date,
          agent,
          CAST(qa_score AS DOUBLE)  AS qa_score,
          CAST(evaluations AS BIGINT) AS evaluations,
          xplead,
          xforce,
          squad,
          squad_district
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


def _pull_blacklisted_agent_dates(
    conn, period_start: date, period_end: date
) -> frozenset[tuple[str, date]]:
    """Return distinct (agent, date) tuples that have at least one
    Playvox evaluation matching the scorecard / evaluation-id blacklist.

    These are the (agent, date) tuples where we expect legacy to have a
    smaller ``evaluations`` count (or potentially no row at all if all
    of the day's evaluations were blacklisted).
    """
    LOGGER.info("Pulling Playvox blacklist hits ...")
    t0 = time.perf_counter()

    scorecard_in = ", ".join(f"'{s}'" for s in sorted(LEGACY_SCORECARD_ID_BLACKLIST))
    eval_in = ", ".join(f"'{s}'" for s in sorted(LEGACY_EVALUATION_ID_BLACKLIST))
    sql = f"""
        SELECT DISTINCT
          LOWER(REGEXP_EXTRACT(evaluation__agent_email, '^[a-zA-Z]+\\\\.[a-zA-Z]+', 0)) AS agent,
          CAST(DATE_TRUNC('DAY', local_mx_evaluation__created_at) AS DATE) AS date
        FROM etl.mx__dataset.qmo_playvox_consolidated
        WHERE local_mx_evaluation__created_at >= :period_start
          AND local_mx_evaluation__created_at <  DATE_ADD(:period_end, 1)
          AND (
                TRY_CAST(scorecard__id  AS STRING) IN ({scorecard_in})
             OR TRY_CAST(evaluation__id AS STRING) IN ({eval_in})
          )
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            parameters={"period_start": period_start, "period_end": period_end},
        )
        df = cur.fetchall_arrow().to_pandas()
    LOGGER.info("  %s tuples in %.1fs", f"{len(df):,}", time.perf_counter() - t0)

    if df.empty:
        return frozenset()
    if pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = df["date"].dt.date
    return frozenset((row.agent, row.date) for row in df.itertuples(index=False))


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


KEY_COLS = ["date", "agent"]
ROSTER_COLS = ["xplead", "xforce", "squad", "squad_district"]
METRIC_COLS = ["qa_score", "evaluations"]


def _normalize_for_join(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if pd.api.types.is_datetime64_any_dtype(out["date"]):
        out["date"] = out["date"].dt.date
    out["qa_score"] = pd.to_numeric(out["qa_score"], errors="coerce").astype(
        "float64"
    )
    out["evaluations"] = pd.to_numeric(out["evaluations"], errors="coerce").astype(
        "Int64"
    )
    return out[KEY_COLS + METRIC_COLS + ROSTER_COLS]


def _format_int(n: int) -> str:
    return f"{n:,}"


def _pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "n/a"
    return f"{100.0 * numerator / denominator:.2f}%"


def _classify_only_in_new(row, blacklist: frozenset[tuple[str, date]]) -> str:
    d: date = row["date"]
    agent: str = row["agent"]
    if d < LEGACY_FROZEN_SNAPSHOT_BOUNDARY:
        return "pre-2025-12-01 (frozen snapshot in legacy)"
    if d in LEGACY_OUTAGE_DATES:
        return "outage date"
    if (agent, d) in blacklist:
        return "scorecard/eval-id blacklist"
    return "unexplained"


def _classify_only_in_legacy(row) -> str:
    d: date = row["date"]
    if d < LEGACY_FROZEN_SNAPSHOT_BOUNDARY:
        return "pre-2025-12-01 (frozen snapshot in legacy)"
    return "unexplained"


def _classify_value_mismatch(
    row, blacklist: frozenset[tuple[str, date]]
) -> str:
    d: date = row["date"]
    agent: str = row["agent"]
    if d < LEGACY_FROZEN_SNAPSHOT_BOUNDARY:
        return "pre-2025-12-01 (frozen snapshot in legacy)"
    if d in LEGACY_OUTAGE_DATES:
        # Defensive: outage dates should be only-in-new, not in-both.
        return "outage date"
    if (agent, d) in blacklist:
        return "scorecard/eval-id blacklist"
    return "unexplained"


def report(
    new: pd.DataFrame,
    legacy: pd.DataFrame,
    blacklist: frozenset[tuple[str, date]],
    period_start: date,
    period_end: date,
    top_n: int = 10,
    csv_out: Path | None = None,
) -> int:
    new_n = _normalize_for_join(new)
    leg_n = _normalize_for_join(legacy)

    merged = new_n.merge(
        leg_n,
        on=KEY_COLS,
        how="outer",
        suffixes=("_new", "_legacy"),
        indicator=True,
    )

    only_new = merged[merged["_merge"] == "left_only"].copy()
    only_legacy = merged[merged["_merge"] == "right_only"].copy()
    both = merged[merged["_merge"] == "both"].copy()

    if not only_new.empty:
        only_new["cause"] = only_new.apply(
            lambda r: _classify_only_in_new(r, blacklist), axis=1
        )
        cause_only_new = only_new["cause"].value_counts()
    else:
        only_new["cause"] = pd.Series(dtype="object")
        cause_only_new = pd.Series(dtype="int64")

    if not only_legacy.empty:
        only_legacy["cause"] = only_legacy.apply(_classify_only_in_legacy, axis=1)
        cause_only_legacy = only_legacy["cause"].value_counts()
    else:
        only_legacy["cause"] = pd.Series(dtype="object")
        cause_only_legacy = pd.Series(dtype="int64")

    both["delta_qa_score"] = (
        both["qa_score_new"].astype("float64")
        - both["qa_score_legacy"].astype("float64")
    )
    both["delta_evaluations"] = (
        both["evaluations_new"].astype("float64")
        - both["evaluations_legacy"].astype("float64")
    )

    qa_match = (both["delta_qa_score"].abs() <= QA_SCORE_TOL).sum()
    eval_match = (both["delta_evaluations"] == 0).sum()

    # Metric mismatch (qa_score outside tol OR evaluations differ).
    metric_diff = both[
        (both["delta_qa_score"].abs() > QA_SCORE_TOL)
        | (both["delta_evaluations"].fillna(-1) != 0)
    ].copy()
    if not metric_diff.empty:
        metric_diff["cause"] = metric_diff.apply(
            lambda r: _classify_value_mismatch(r, blacklist), axis=1
        )
        metric_cause_counts = metric_diff["cause"].value_counts()
    else:
        metric_diff["cause"] = pd.Series(dtype="object")
        metric_cause_counts = pd.Series(dtype="int64")

    # Roster drift = row in both, metric columns agree, but at least
    # one of the 4 attribution columns differs. Always classified as
    # benign (it's downstream of snapshot disagreement).
    same_metrics = (both["delta_qa_score"].abs() <= QA_SCORE_TOL) & (
        both["delta_evaluations"].fillna(0) == 0
    )
    roster_diff_mask = same_metrics & False  # init
    for col in ROSTER_COLS:
        roster_diff_mask = roster_diff_mask | (
            (both[f"{col}_new"].fillna("") != both[f"{col}_legacy"].fillna(""))
            & same_metrics
        )
    roster_drift_count = int(roster_diff_mask.sum())

    unexplained_only_new = int(
        (only_new["cause"] == "unexplained").sum() if not only_new.empty else 0
    )
    unexplained_only_legacy = int(
        (only_legacy["cause"] == "unexplained").sum()
        if not only_legacy.empty
        else 0
    )
    unexplained_mismatch = int(
        (metric_diff["cause"] == "unexplained").sum()
        if not metric_diff.empty
        else 0
    )

    # ----- print -----
    print()
    print("=" * 78)
    print(f" Quality parity — {period_start} .. {period_end}")
    print(f" New pipeline output  vs  legacy `{LEGACY_TABLE}`")
    print(" Key: (date, agent)")
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

    if not cause_only_new.empty:
        print()
        print("  Only-in-new — by known cause:")
        for cause, n in cause_only_new.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<48} {_format_int(int(n)):>8}{tag}")

    if not cause_only_legacy.empty:
        print()
        print("  Only-in-legacy — by known cause:")
        for cause, n in cause_only_legacy.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<48} {_format_int(int(n)):>8}{tag}")

    print()
    print("Matching (rows in both)")
    print("-----------------------")
    print(
        f"  qa_score within {QA_SCORE_TOL:g}: {_format_int(int(qa_match)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(qa_match), len(both))})"
    )
    print(
        f"  evaluations exact match:    {_format_int(int(eval_match)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(eval_match), len(both))})"
    )
    print(
        f"  Roster drift (metrics OK):  {_format_int(roster_drift_count):>10} "
        f"  [EXPECTED — snapshot choice]"
    )

    if not metric_cause_counts.empty:
        print()
        print("  Metric mismatch — by known cause:")
        for cause, n in metric_cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<48} {_format_int(int(n)):>8}{tag}")

    if not both.empty:
        print()
        print("Delta distribution — qa_score (new − legacy)")
        print("--------------------------------------------")
        print(both["delta_qa_score"].describe().to_string())

        print()
        print("Delta distribution — evaluations (new − legacy)")
        print("-----------------------------------------------")
        print(both["delta_evaluations"].describe().to_string())

        both["abs_delta_score"] = (
            both["delta_qa_score"].abs() * 0.0  # qa_score dominated by 0–100 vs counts
            + both["delta_evaluations"].abs().fillna(0)
        )
        # Bring real qa_score deltas in for rows where evaluations agree.
        both["abs_delta_score"] = (
            both["abs_delta_score"]
            + both["delta_qa_score"].abs().fillna(0) / 100.0
        )
        worst = both.sort_values("abs_delta_score", ascending=False).head(top_n)
        if (worst["abs_delta_score"] > 0).any():
            print()
            print(f"Top {top_n} divergences (rows in both, by combined |Δ|)")
            print("-" * 78)
            display_cols = [
                "date",
                "agent",
                "qa_score_new",
                "qa_score_legacy",
                "delta_qa_score",
                "evaluations_new",
                "evaluations_legacy",
                "delta_evaluations",
            ]
            with pd.option_context("display.max_colwidth", 30):
                print(worst[display_cols].to_string(index=False))

    if csv_out is not None:
        csv_out.parent.mkdir(parents=True, exist_ok=True)
        merged_for_csv = merged.copy()
        merged_for_csv["delta_qa_score"] = (
            merged_for_csv.get("qa_score_new", 0)
            - merged_for_csv.get("qa_score_legacy", 0)
        )
        merged_for_csv["delta_evaluations"] = (
            merged_for_csv.get("evaluations_new", 0)
            - merged_for_csv.get("evaluations_legacy", 0)
        )
        merged_for_csv["cause"] = pd.NA
        if not only_new.empty:
            merged_for_csv.loc[only_new.index, "cause"] = only_new["cause"]
        if not only_legacy.empty:
            merged_for_csv.loc[only_legacy.index, "cause"] = only_legacy["cause"]
        if not metric_diff.empty:
            merged_for_csv.loc[metric_diff.index, "cause"] = metric_diff["cause"]
        merged_for_csv.to_csv(csv_out, index=False)
        print()
        print(f"Wrote full diff to {csv_out}")

    print()
    print("=" * 78)
    if (
        unexplained_only_new == 0
        and unexplained_only_legacy == 0
        and unexplained_mismatch == 0
    ):
        print(" Verdict: every divergence is explained by a known legacy adjustment.")
        verdict = 0
    else:
        print(
            f" Verdict: {unexplained_only_new:,} unexplained only-in-new + "
            f"{unexplained_only_legacy:,} unexplained only-in-legacy + "
            f"{unexplained_mismatch:,} unexplained metric mismatches. "
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
    parser.add_argument("--top-n", type=int, default=10)
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

    conn = open_connection()
    try:
        new_df = _run_new_pipeline(conn, args.period_start, args.period_end)
        legacy_df = _pull_legacy(conn, args.period_start, args.period_end)
        blacklist = _pull_blacklisted_agent_dates(
            conn, args.period_start, args.period_end
        )
    finally:
        conn.close()

    return report(
        new=new_df,
        legacy=legacy_df,
        blacklist=blacklist,
        period_start=args.period_start,
        period_end=args.period_end,
        top_n=args.top_n,
        csv_out=args.csv_out,
    )


if __name__ == "__main__":
    sys.exit(main())
