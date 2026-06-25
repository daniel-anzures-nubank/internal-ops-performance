"""Parity check: new `compute_adherence` vs. legacy `usr.mx__cx.adherence_io`.

What this does
--------------
1. Runs the three extractors (agent_information, dime_slots, productivity)
   for the requested period.
2. Calls `compute_adherence` to get the new pipeline's per-(agent, date)
   frame.
3. Pulls the legacy table for the same period.
4. Outer-joins them on (agent, date) and reports:
     * Coverage (rows in both / only-new / only-legacy).
     * Per-column exact-match rate.
     * Delta distribution and the largest individual divergences.
     * A "known adjustments" breakdown — divergences that fall on dates the
       legacy hardcodes as outages / carve-outs are flagged separately so
       it's obvious whether the remaining mismatches are bugs.

Why this is a script and not a pytest test
------------------------------------------
Parity here is not binary. The legacy table has manual adjustments baked in
that we deliberately do not replicate (Categories C/D/E from the
adjustments catalog) — they'll be re-applied later via the Google-Sheets
adjustments module. So we'd expect any "all-or-nothing" assertion to fail
on perfectly correct output. The right tool is a structured diagnostic
report a human reads.

Usage
-----
::

    uv run python tests/parity/parity_check_adherence.py \\
        --period-start 2026-04-14 --period-end 2026-04-20

    uv run python tests/parity/parity_check_adherence.py \\
        --period-start 2026-04-14 --period-end 2026-04-20 \\
        --csv-out /tmp/adherence_diff.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

# tests/parity/adherence.py is two levels deep from the repo root.
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor  # noqa: E402
from adherence import (  # noqa: E402
    SLOT_DURATION_SECONDS,
    compute_adherence,
    count_unmatched_slots,
)

LOGGER = logging.getLogger("cx_metrics.parity.adherence")

LEGACY_TABLE = "usr.mx__cx.adherence_io"

# Dates the legacy code hardcodes as full-day exclusions (Category C in the
# adjustments catalog). Any (agent, date) row that lands on these dates
# is expected to be missing from the legacy table and present in ours.
LEGACY_OUTAGE_DATES: frozenset[date] = frozenset(
    {date(2026, 3, 27), date(2026, 4, 9)}
)

# Specific (agent, date) carve-outs the legacy hardcodes (Category D).
# Same expectation: missing from legacy, present in ours.
LEGACY_AGENT_DATE_CARVE_OUTS: frozenset[tuple[str, date]] = frozenset(
    {
        ("jonathan.pineda", date(2026, 2, 26)),
        *[
            (agent, date(2026, 3, day))
            for agent in (
                "jose.velez",
                "carlos.gonzalez",
                "jorge.ortega",
                "luisa.castaneda",
                "janet.castro",
                "karen.ortega",
            )
            for day in (24, 25, 26, 27, 28)
        ],
    }
)


# ---------------------------------------------------------------------------
# Pipeline runner — produce the new frame
# ---------------------------------------------------------------------------


def _run_new_pipeline(
    conn, period_start: date, period_end: date
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Replay scripts/build_adherence.py in-memory.

    Returns ``(new_output, dime_raw, productivity_raw)``. The raw frames are
    returned alongside the final output so the caller can re-derive
    intermediates (e.g. unmatched-slot counts for legacy-bug replication)
    without re-fetching from the warehouse.
    """
    LOGGER.info("Pulling agent_information ...")
    t0 = time.perf_counter()
    agent_info = run_extractor(conn, "agent_information", period_start, period_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(agent_info):,}", time.perf_counter() - t0)

    LOGGER.info("Pulling dime_slots ...")
    t0 = time.perf_counter()
    dime = run_extractor(conn, "dime_slots", period_start, period_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(dime):,}", time.perf_counter() - t0)

    LOGGER.info("Pulling productivity ...")
    t0 = time.perf_counter()
    prod = run_extractor(conn, "productivity", period_start, period_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(prod):,}", time.perf_counter() - t0)

    LOGGER.info("Computing adherence ...")
    t0 = time.perf_counter()
    new = compute_adherence(agent_info, dime, prod)
    LOGGER.info(
        "  %s output rows in %.1fs", f"{len(new):,}", time.perf_counter() - t0
    )
    return new, dime, prod


# ---------------------------------------------------------------------------
# Legacy puller — read directly from the materialized table
# ---------------------------------------------------------------------------


def _pull_legacy(conn, period_start: date, period_end: date) -> pd.DataFrame:
    LOGGER.info("Pulling legacy %s ...", LEGACY_TABLE)
    t0 = time.perf_counter()
    sql = f"""
        SELECT agent, date, delivered_hours, required_hours
        FROM {LEGACY_TABLE}
        WHERE date >= :period_start AND date <= :period_end
    """
    with conn.cursor() as cur:
        cur.execute(
            sql, parameters={"period_start": period_start, "period_end": period_end}
        )
        df = cur.fetchall_arrow().to_pandas()
    LOGGER.info("  %s rows in %.1fs", f"{len(df):,}", time.perf_counter() - t0)
    return df


# ---------------------------------------------------------------------------
# Comparison + reporting
# ---------------------------------------------------------------------------


KEY_COLS = ["agent", "date"]
COMPARED_COLS = ["delivered_hours", "required_hours"]


def _normalize_for_join(df: pd.DataFrame) -> pd.DataFrame:
    """Make key columns join-safe across the two frames.

    The new frame's `date` may be a pandas Timestamp (from groupby on a
    datetime column); the legacy frame's `date` is a Python `datetime.date`
    from the warehouse. We normalize both to `datetime.date` so the merge
    behaves predictably.
    """
    out = df.copy()
    if pd.api.types.is_datetime64_any_dtype(out["date"]):
        out["date"] = out["date"].dt.date
    return out


def _classify_only_in_new(row) -> str:
    """Bucket an only-in-new row by its known adjustment cause."""
    d = row["date"]
    if d in LEGACY_OUTAGE_DATES:
        return "outage-date (Cat. C)"
    if (row["agent"], d) in LEGACY_AGENT_DATE_CARVE_OUTS:
        return "agent-date carve-out (Cat. D)"
    return "unexplained"


def _format_int(n: int) -> str:
    return f"{n:,}"


def _pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "n/a"
    return f"{100.0 * numerator / denominator:.2f}%"


def _apply_legacy_bug_replication(
    new: pd.DataFrame, unmatched: pd.DataFrame
) -> pd.DataFrame:
    """Inflate `delivered_hours` to mimic the legacy phantom-adherence bug.

    Mutates a copy of ``new`` by adding ``unmatched_slots * 1800`` to
    ``delivered_hours`` for each (agent, date). After this transform,
    deltas against the legacy table reflect *everything except* the
    phantom-adherence bug.
    """
    merged = new.merge(
        _normalize_for_join(unmatched), on=KEY_COLS, how="left"
    )
    merged["unmatched_slots"] = (
        merged["unmatched_slots"].fillna(0).astype("int64")
    )
    merged["delivered_hours"] = (
        merged["delivered_hours"] + merged["unmatched_slots"] * SLOT_DURATION_SECONDS
    )
    return merged.drop(columns=["unmatched_slots"])


def report(
    new: pd.DataFrame,
    legacy: pd.DataFrame,
    period_start: date,
    period_end: date,
    top_n: int = 10,
    csv_out: Path | None = None,
    bug_replication: pd.DataFrame | None = None,
) -> int:
    """Print the parity report and return an exit code.

    Exit code 0 if no unexplained divergence; 1 otherwise (so this could
    be wired into a CI gate later if we ever want to).

    If ``bug_replication`` is provided (a frame from
    `count_unmatched_slots`), the new pipeline's ``delivered_hours`` is
    inflated by ``unmatched_slots * 1800`` before the comparison, so the
    report shows residual divergence *after accounting for the legacy
    phantom-adherence bug*.
    """
    new_n = _normalize_for_join(new[KEY_COLS + COMPARED_COLS])
    leg_n = _normalize_for_join(legacy[KEY_COLS + COMPARED_COLS])

    bug_replicated_mode = bug_replication is not None
    if bug_replicated_mode:
        new_n = _apply_legacy_bug_replication(new_n, bug_replication)

    merged = new_n.merge(
        leg_n, on=KEY_COLS, how="outer", suffixes=("_new", "_legacy"), indicator=True
    )

    only_new = merged[merged["_merge"] == "left_only"].copy()
    only_legacy = merged[merged["_merge"] == "right_only"].copy()
    both = merged[merged["_merge"] == "both"].copy()

    # Bucket only-new rows by known adjustment causes.
    if not only_new.empty:
        only_new["cause"] = only_new.apply(_classify_only_in_new, axis=1)
        cause_counts = only_new["cause"].value_counts()
    else:
        only_new["cause"] = pd.Series(dtype="object")
        cause_counts = pd.Series(dtype="int64")

    # Per-column delta distribution for rows in both.
    both["delta_delivered"] = both["delivered_hours_new"] - both["delivered_hours_legacy"]
    both["delta_required"] = both["required_hours_new"] - both["required_hours_legacy"]

    exact_delivered = (both["delta_delivered"] == 0).sum()
    exact_required = (both["delta_required"] == 0).sum()

    unexplained_only_new = int(
        (only_new["cause"] == "unexplained").sum() if not only_new.empty else 0
    )

    # ----- print -----
    print()
    print("=" * 78)
    print(f" Adherence parity — {period_start} .. {period_end}")
    if bug_replicated_mode:
        print(
            f" New pipeline (+ legacy phantom-adherence bug replicated)  "
            f"vs  legacy `{LEGACY_TABLE}`"
        )
        print(" delivered_hours_new += unmatched_slots × 1800")
    else:
        print(f" New pipeline output  vs  legacy `{LEGACY_TABLE}`")
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
            print(f"    {cause:<30} {_format_int(int(n)):>8}{tag}")

    if len(only_legacy) > 0:
        print()
        print(
            "  [INVESTIGATE] Rows present in legacy but not in new "
            "— usually a roster join bug, missing extractor data, "
            "or a column-type mismatch on the join key."
        )

    print()
    print("Matching (rows in both)")
    print("-----------------------")
    print(
        f"  delivered_hours exact match: {_format_int(int(exact_delivered)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(exact_delivered), len(both))})"
    )
    print(
        f"  required_hours  exact match: {_format_int(int(exact_required)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(exact_required), len(both))})"
    )

    if not both.empty:
        print()
        print("Delta distribution — required_hours (new − legacy, seconds)")
        print("-----------------------------------------------------------")
        print(both["delta_required"].describe().to_string())

        print()
        print("Delta distribution — delivered_hours (new − legacy, seconds)")
        print("------------------------------------------------------------")
        print(both["delta_delivered"].describe().to_string())

        # Top-N divergences by absolute combined delta.
        both["abs_delta"] = both["delta_delivered"].abs() + both["delta_required"].abs()
        worst = both.sort_values("abs_delta", ascending=False).head(top_n)
        if (worst["abs_delta"] > 0).any():
            print()
            print(f"Top {top_n} divergences (rows in both, sorted by total |delta|)")
            print("-" * 78)
            display_cols = [
                "agent",
                "date",
                "delivered_hours_new",
                "delivered_hours_legacy",
                "delta_delivered",
                "required_hours_new",
                "required_hours_legacy",
                "delta_required",
            ]
            print(worst[display_cols].to_string(index=False))

    if csv_out is not None:
        csv_out.parent.mkdir(parents=True, exist_ok=True)
        # Stable schema for the diff CSV: all rows + _merge label + deltas.
        merged_for_csv = merged.assign(
            delta_delivered=merged.get("delivered_hours_new", 0)
            - merged.get("delivered_hours_legacy", 0),
            delta_required=merged.get("required_hours_new", 0)
            - merged.get("required_hours_legacy", 0),
        )
        # Re-attach cause for left_only rows.
        merged_for_csv["cause"] = pd.NA
        merged_for_csv.loc[only_new.index, "cause"] = only_new["cause"]
        merged_for_csv.to_csv(csv_out, index=False)
        print()
        print(f"Wrote full diff to {csv_out}")

    print()
    print("=" * 78)
    if unexplained_only_new == 0 and len(only_legacy) == 0:
        print(" Verdict: every divergence is explained by a known legacy adjustment.")
        verdict = 0
    else:
        print(
            f" Verdict: {unexplained_only_new:,} unexplained only-in-new rows + "
            f"{len(only_legacy):,} only-in-legacy rows. Investigate."
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
    parser.add_argument(
        "--replicate-legacy-bug",
        action="store_true",
        help=(
            "Inflate the new pipeline's delivered_hours by "
            "(unmatched_slots × 1800) before comparing, to mimic the "
            "legacy phantom-adherence bug. Residual deltas then reflect "
            "*other* sources of divergence (manual adjustments, table "
            "staleness, upstream data drift)."
        ),
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
        new_df, dime_df, prod_df = _run_new_pipeline(
            conn, args.period_start, args.period_end
        )
        legacy_df = _pull_legacy(conn, args.period_start, args.period_end)
    finally:
        conn.close()

    bug_replication: pd.DataFrame | None = None
    if args.replicate_legacy_bug:
        LOGGER.info("Counting unmatched slots to replicate the legacy bug ...")
        t0 = time.perf_counter()
        bug_replication = count_unmatched_slots(dime_df, prod_df)
        LOGGER.info(
            "  %s (agent, date) rows in %.1fs",
            f"{len(bug_replication):,}",
            time.perf_counter() - t0,
        )

    return report(
        new=new_df,
        legacy=legacy_df,
        period_start=args.period_start,
        period_end=args.period_end,
        top_n=args.top_n,
        csv_out=args.csv_out,
        bug_replication=bug_replication,
    )


if __name__ == "__main__":
    sys.exit(main())
