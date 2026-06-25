"""Parity check: new `compute_shrinkage` vs. legacy `usr.mx__cx.shrinkage_io`.

What this does
--------------
1. Runs the two extractors (agent_information, dime_slots) for the
   requested period.
2. Calls ``compute_shrinkage`` to get the new pipeline's per-(agent,
   old_squad, date, activity_type_required, dimensioned_activity) frame.
3. Pulls the legacy table for the same period.
4. Outer-joins them on
   ``(agent, old_squad, date, activity_type_required, dimensioned_activity)``
   and reports:
     * Coverage (rows in both / only-new / only-legacy).
     * Per-column exact-match rate (shrinkage_slot, required_slot).
     * Delta distributions and the largest individual divergences.
     * A "known adjustments" breakdown.

Why this is a script and not a pytest test
------------------------------------------
Parity here is not binary. The legacy table has manual adjustments baked
in that we deliberately do not replicate:

* ``manual_adjustments_shrinkage`` view — slot-level time-window
  exclusions for training and shadowing on specific dates (10-20 (agent,
  date, time-window) tuples for 2026-03-10, 2026-04-09, 2026-04-10,
  2026-04-13). When applied, these REMOVE matching slots from the
  legacy aggregation entirely.
* ``maria.reyes`` Feb 2026 maternity reclassification (legacy counts
  ALL her February non-NULL slots as shrinkage).
* ``jose.velez`` et al. 2026-03-24..28 agent-date carve-out (legacy
  drops every slot for those agents on those dates).
* Pre-2025-12-01 dates use a frozen Dec-2025 BDX snapshot in legacy
  (different roster than ours).

So we'd expect any "all-or-nothing" assertion to fail on perfectly
correct output. The right tool is a structured diagnostic report.

The diff classes (in priority order)
------------------------------------
* **pre-2025-12-01 scope** — legacy applies the 2025 path with a frozen
  Dec-2025 BDX snapshot; our pipeline uses the natural snapshot_month
  per row's date. The resulting roster (district/shift/squad) can
  differ. Flagged as ``pre-2025-12-01 (frozen snapshot in legacy)``.
* **agent-date carve-out** — full-day exclusions in legacy
  (jose.velez et al. 2026-03-24..28). Shows up as only-in-new.
* **manual-window exclusion** — when a (agent, date) tuple is in the
  ``manual_adjustments_shrinkage`` list, slots in the excluded time
  window are removed from the legacy aggregation, but NOT from ours.
  This shows up as either:
    - rows present in BOTH pipelines but with reduced legacy counts
      (when only SOME of a group's slots fell in the window), or
    - rows present only in ours (when ALL of a group's slots fell in
      the window).
  We flag these by (agent, date) on the assumption that any row
  touching one of those dates is potentially affected. Genuine exact
  matches will simply not appear in the mismatch buckets.
* **maria.reyes Feb 2026 maternity** — legacy counts ALL of her Feb
  2026 non-NULL slots as shrinkage. Her rows show shrinkage_slot
  mismatch (legacy > ours).
* **unexplained** — everything else. The investigative target.

Usage
-----
::

    uv run python tests/parity/parity_check_shrinkage.py \\
        --period-start 2026-05-11 --period-end 2026-05-17

    uv run python tests/parity/parity_check_shrinkage.py \\
        --period-start 2026-01-01 --period-end 2026-05-24 \\
        --csv-out /tmp/shrinkage_diff.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor  # noqa: E402
from shrinkage import compute_shrinkage  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.parity.shrinkage")

LEGACY_TABLE = "usr.mx__cx.shrinkage_io"

# Date at which the legacy table transitions from the 2025 "frozen
# Dec-2025 snapshot" path to the 2026 "natural snapshot_month" path.
# Rows before this in our pipeline use natural snapshots — possibly
# different from legacy's frozen one.
LEGACY_FROZEN_SNAPSHOT_BOUNDARY: date = date(2025, 12, 1)


# ---------------------------------------------------------------------------
# Known legacy carve-outs
# ---------------------------------------------------------------------------


# Full-day agent×date exclusions (legacy's
# ``NOT (a.agent IN (...) AND a.date IN (...))`` block in
# ``shrinkage_final_2026``).
def _expand_agent_date_carve_outs() -> frozenset[tuple[str, date]]:
    out: set[tuple[str, date]] = set()
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
    return frozenset(out)


LEGACY_AGENT_DATE_CARVE_OUTS: frozenset[tuple[str, date]] = (
    _expand_agent_date_carve_outs()
)


# Manual training / shadowing time-window exclusions from
# ``manual_adjustments_shrinkage``. Each entry below is one (agent,
# date) tuple touched by ANY exclusion window. We don't need to track
# the exact time windows for parity — for classification purposes, any
# row matching a tuple here is "potentially affected".
def _expand_manual_window_carve_outs() -> frozenset[tuple[str, date]]:
    out: set[tuple[str, date]] = set()

    # Training rules
    # CREDIT -> LIFECYCLE training 2026-03-10 11:00-13:00
    for agent in (
        "elizabeth.martinez",
        "daniel.cano",
        "jonathan.pineda",
        "jessica.gonzalez",
        "nitza.zarza",
    ):
        out.add((agent, date(2026, 3, 10)))
    # CREDIT -> LIFECYCLE training 2026-03-10 18:00-19:00
    for agent in ("bertha.sanchez", "sofia.orozco", "jorge.ortega"):
        out.add((agent, date(2026, 3, 10)))
    # CREDIT -> CUENTA training (Apr 9 / Apr 10)
    for agent in ("elizabeth.martinez",):
        out.add((agent, date(2026, 4, 9)))
        out.add((agent, date(2026, 4, 10)))
    for agent in ("daniel.cano", "jonathan.pineda"):
        out.add((agent, date(2026, 4, 9)))
    # COLLECTIONS -> CUENTA training (Apr 9 / Apr 10)
    for agent in ("adriana.marquez", "eden.martinez", "mariana.infante"):
        out.add((agent, date(2026, 4, 9)))
        out.add((agent, date(2026, 4, 10)))
    for agent in ("javier.balanzar", "carlos.gonzalez"):
        out.add((agent, date(2026, 4, 9)))
    # EMI -> CUENTA training (Apr 9 / Apr 10)
    out.add(("fernanda.ibanez", date(2026, 4, 9)))
    for agent in ("jose.velez", "ivette.melendez", "rocio.rodriguez"):
        out.add((agent, date(2026, 4, 9)))
        out.add((agent, date(2026, 4, 10)))
    # EMI -> LIFECYCLE training 2026-03-10 11:00-15:00
    for agent in ("fernanda.ibanez", "jose.velez", "ivette.melendez"):
        out.add((agent, date(2026, 3, 10)))
    # EMI -> LIFECYCLE training 2026-03-10 18:00-19:30
    out.add(("erik.licona", date(2026, 3, 10)))

    # Shadowing rules
    # CREDIT -> LIFECYCLE shadowing 2026-03-10 13:00-15:00
    for agent in (
        "elizabeth.martinez",
        "daniel.cano",
        "jonathan.pineda",
        "jessica.gonzalez",
        "nitza.zarza",
    ):
        out.add((agent, date(2026, 3, 10)))
    # CREDIT -> LIFECYCLE shadowing 2026-03-10 19:00-20:00
    for agent in ("bertha.sanchez", "sofia.orozco", "jorge.ortega"):
        out.add((agent, date(2026, 3, 10)))
    # CREDIT / COLLECTIONS / EMI -> CUENTA shadowing 2026-04-13
    for agent in (
        "elizabeth.martinez",
        "daniel.cano",
        "jonathan.pineda",
        "adriana.marquez",
        "javier.balanzar",
        "carlos.gonzalez",
        "mariana.infante",
        "fernanda.ibanez",
        "ivette.melendez",
        "rocio.rodriguez",
        "jorge.severiano",
    ):
        out.add((agent, date(2026, 4, 13)))

    return frozenset(out)


LEGACY_MANUAL_WINDOW_CARVE_OUTS: frozenset[tuple[str, date]] = (
    _expand_manual_window_carve_outs()
)


def _maria_reyes_maternity_dates() -> frozenset[tuple[str, date]]:
    """All maria.reyes February-2026 dates (legacy maternity reclass.)."""
    out: set[tuple[str, date]] = set()
    d = date(2026, 2, 1)
    while d < date(2026, 3, 1):
        out.add(("maria.reyes", d))
        d += timedelta(days=1)
    return frozenset(out)


MARIA_REYES_MATERNITY_DATES: frozenset[tuple[str, date]] = (
    _maria_reyes_maternity_dates()
)


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _run_new_pipeline(
    conn, period_start: date, period_end: date
) -> pd.DataFrame:
    """Replay scripts/build_shrinkage.py in-memory."""
    LOGGER.info("Pulling agent_information ...")
    t0 = time.perf_counter()
    agent_info = run_extractor(conn, "agent_information", period_start, period_end)
    LOGGER.info(
        "  %s rows in %.1fs", f"{len(agent_info):,}", time.perf_counter() - t0
    )

    LOGGER.info("Pulling dime_slots ...")
    t0 = time.perf_counter()
    dime = run_extractor(conn, "dime_slots", period_start, period_end)
    LOGGER.info("  %s rows in %.1fs", f"{len(dime):,}", time.perf_counter() - t0)

    LOGGER.info("Computing shrinkage ...")
    t0 = time.perf_counter()
    new = compute_shrinkage(agent_info, dime)
    LOGGER.info(
        "  %s output rows in %.1fs", f"{len(new):,}", time.perf_counter() - t0
    )
    return new


# ---------------------------------------------------------------------------
# Legacy puller
# ---------------------------------------------------------------------------


def _pull_legacy(conn, period_start: date, period_end: date) -> pd.DataFrame:
    LOGGER.info("Pulling legacy %s ...", LEGACY_TABLE)
    t0 = time.perf_counter()
    sql = f"""
        SELECT
          agent,
          old_squad,
          CAST(date AS DATE) AS date,
          activity_type_required,
          dimensioned_activity,
          shrinkage_slot,
          required_slot
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


KEY_COLS = [
    "agent",
    "old_squad",
    "date",
    "activity_type_required",
    "dimensioned_activity",
]
INT_COMPARED = ("shrinkage_slot", "required_slot")

# Sentinel for NULL values in the join keys. Pandas' merge on NaN is
# subtle (NaN != NaN in some dtypes); we coerce NULLs to a string
# sentinel so the merge is deterministic.
NULL_SENTINEL = "<NULL>"


def _normalize_for_join(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Coerce date / numeric / NULL-bearing columns into stable types.

    * ``date`` → ``datetime.date``
    * ``shrinkage_slot`` / ``required_slot`` → ``Int64``
    * ``dimensioned_activity`` (and any other key column that can be
      NULL) → string with ``NULL`` replaced by ``NULL_SENTINEL``.
    """
    out = df.copy()
    if pd.api.types.is_datetime64_any_dtype(out["date"]):
        out["date"] = out["date"].dt.date
    for col in INT_COMPARED:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    # Coerce any object-typed key column's NULLs to the sentinel so the
    # merge matches them.
    for col in KEY_COLS:
        if col in out.columns and out[col].dtype == object:
            out[col] = out[col].fillna(NULL_SENTINEL)
    return out[[c for c in cols if c in out.columns]]


def _classify_only_in_new(row) -> str:
    """Bucket an only-in-new row by its known adjustment cause."""
    agent: str = row["agent"]
    d: date = row["date"]
    if d < LEGACY_FROZEN_SNAPSHOT_BOUNDARY:
        return "pre-2025-12-01 (frozen snapshot in legacy)"
    if (agent, d) in LEGACY_AGENT_DATE_CARVE_OUTS:
        return "agent-date carve-out"
    if (agent, d) in LEGACY_MANUAL_WINDOW_CARVE_OUTS:
        return "manual-window exclusion"
    return "unexplained"


def _classify_only_in_legacy(row) -> str:
    """Bucket an only-in-legacy row.

    Shrinkage has no content-roster union or other structural source of
    legacy-only rows. The most likely cause for any only-in-legacy row
    is a roster-snapshot disagreement on pre-2025-12-01 dates (legacy
    uses frozen Dec-2025, we use natural month → district/shift/squad
    differ → different group keys).
    """
    d: date = row["date"]
    if d < LEGACY_FROZEN_SNAPSHOT_BOUNDARY:
        return "pre-2025-12-01 (frozen snapshot in legacy)"
    return "unexplained"


def _classify_value_mismatch(row) -> str:
    """Bucket an in-both row whose counts differ.

    Same as ``_classify_only_in_new`` plus the maria.reyes maternity
    case (which is a value-level reclassification, not a row drop).
    """
    agent: str = row["agent"]
    d: date = row["date"]
    if d < LEGACY_FROZEN_SNAPSHOT_BOUNDARY:
        return "pre-2025-12-01 (frozen snapshot in legacy)"
    if (agent, d) in LEGACY_AGENT_DATE_CARVE_OUTS:
        # Should be impossible: full-day carve-out means the row is
        # ONLY in new, not in both. But classify defensively.
        return "agent-date carve-out"
    if (agent, d) in MARIA_REYES_MATERNITY_DATES:
        return "maria.reyes Feb 2026 maternity"
    if (agent, d) in LEGACY_MANUAL_WINDOW_CARVE_OUTS:
        return "manual-window exclusion"
    return "unexplained"


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
    cols = KEY_COLS + list(INT_COMPARED)
    new_n = _normalize_for_join(new, cols)
    leg_n = _normalize_for_join(legacy, cols)

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

    exact_shrink = (both["delta_shrinkage_slot"] == 0).sum()
    exact_req = (both["delta_required_slot"] == 0).sum()

    # Classify mismatched rows
    mismatch = both[
        (both["delta_shrinkage_slot"] != 0) | (both["delta_required_slot"] != 0)
    ].copy()
    if not mismatch.empty:
        mismatch["cause"] = mismatch.apply(_classify_value_mismatch, axis=1)
        mismatch_cause_counts = mismatch["cause"].value_counts()
    else:
        mismatch["cause"] = pd.Series(dtype="object")
        mismatch_cause_counts = pd.Series(dtype="int64")

    unexplained_only_new = int(
        (only_new["cause"] == "unexplained").sum() if not only_new.empty else 0
    )
    unexplained_only_legacy = int(
        (only_legacy["cause"] == "unexplained").sum()
        if not only_legacy.empty
        else 0
    )
    unexplained_mismatch = int(
        (mismatch["cause"] == "unexplained").sum() if not mismatch.empty else 0
    )

    # ----- print -----
    print()
    print("=" * 78)
    print(f" Shrinkage parity — {period_start} .. {period_end}")
    print(f" New pipeline output  vs  legacy `{LEGACY_TABLE}`")
    print(" Key: (agent, old_squad, date, activity_type_required, dimensioned_activity)")
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
            print(f"    {cause:<48} {_format_int(int(n)):>8}{tag}")

    if not only_legacy_cause_counts.empty:
        print()
        print("  Only-in-legacy — by known cause:")
        for cause, n in only_legacy_cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<48} {_format_int(int(n)):>8}{tag}")

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
        f"  shrinkage_slot exact match: {_format_int(int(exact_shrink)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(exact_shrink), len(both))})"
    )
    print(
        f"  required_slot  exact match: {_format_int(int(exact_req)):>10} "
        f"/ {_format_int(len(both))}  ({_pct(int(exact_req), len(both))})"
    )

    if not mismatch_cause_counts.empty:
        print()
        print("  Count mismatch — by known cause:")
        for cause, n in mismatch_cause_counts.items():
            tag = "  [EXPECTED]" if cause != "unexplained" else "  [INVESTIGATE]"
            print(f"    {cause:<48} {_format_int(int(n)):>8}{tag}")

    if not both.empty:
        print()
        print("Delta distribution — shrinkage_slot (new − legacy)")
        print("--------------------------------------------------")
        print(both["delta_shrinkage_slot"].describe().to_string())

        print()
        print("Delta distribution — required_slot (new − legacy)")
        print("-------------------------------------------------")
        print(both["delta_required_slot"].describe().to_string())

        both["abs_delta_score"] = (
            both["delta_shrinkage_slot"].abs()
            + both["delta_required_slot"].abs()
        )
        worst = both.sort_values("abs_delta_score", ascending=False).head(top_n)
        if (worst["abs_delta_score"] > 0).any():
            print()
            print(f"Top {top_n} divergences (rows in both, by combined |Δ|)")
            print("-" * 78)
            display_cols = [
                "agent",
                "date",
                "activity_type_required",
                "dimensioned_activity",
                "shrinkage_slot_new",
                "shrinkage_slot_legacy",
                "delta_shrinkage_slot",
                "required_slot_new",
                "required_slot_legacy",
                "delta_required_slot",
            ]
            with pd.option_context("display.max_colwidth", 30):
                print(worst[display_cols].to_string(index=False))

    if csv_out is not None:
        csv_out.parent.mkdir(parents=True, exist_ok=True)
        merged_for_csv = merged.copy()
        for col in INT_COMPARED:
            merged_for_csv[f"delta_{col}"] = (
                merged_for_csv.get(f"{col}_new", 0)
                - merged_for_csv.get(f"{col}_legacy", 0)
            )
        merged_for_csv["cause"] = pd.NA
        if not only_new.empty:
            merged_for_csv.loc[only_new.index, "cause"] = only_new["cause"]
        if not only_legacy.empty:
            merged_for_csv.loc[only_legacy.index, "cause"] = only_legacy["cause"]
        if not mismatch.empty:
            merged_for_csv.loc[mismatch.index, "cause"] = mismatch["cause"]
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
            f"{unexplained_mismatch:,} unexplained count mismatches. "
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

    conn = open_connection()
    try:
        new_df = _run_new_pipeline(conn, args.period_start, args.period_end)
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
