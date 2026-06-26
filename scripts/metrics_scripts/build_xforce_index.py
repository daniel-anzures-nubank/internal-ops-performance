"""Build the composite XForce Index metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/xforce_index.py`` and is covered by
``tests/metrics/test_xforce_index.py``. Here we only:

  1. Open a Databricks SQL connection (shared `db.open_connection`).
  2. Read the component metric tables for the period (via `db.read_table`,
     scoped on ``date_reference``).
  3. Call ``compute_xforce_index`` to combine them at all granularities.
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

Tables
------
* Inputs (override with the matching flag):
    - ``usr.danielanzures.io_shrinkage_metric``               (driver, agent grain)
    - ``usr.danielanzures.io_xpeers_in_target_metric``
    - ``usr.danielanzures.io_average_xpeer_index_metric``
    - ``usr.danielanzures.io_improved_benchmarks_metric``     (xforce roll-up)
* Output: ``usr.danielanzures.io_xforce_index_metric`` (override `--target`).

Manual adjustments
------------------
None applied here (no adjustments layer yet).

Usage
-----
::

    uv run python scripts/metrics_scripts/build_xforce_index.py \\
        --period-start 2026-05-01 --period-end 2026-05-31 --dry-run

    uv run python scripts/metrics_scripts/build_xforce_index.py \\
        --period-start 2026-01-01 --period-end 2026-05-31
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics"))

from db import open_connection, read_table, publish  # noqa: E402
from metric_utils import GRANULARITIES  # noqa: E402
from xforce_index import (  # noqa: E402
    IO_XFORCE_INDEX_METRIC_SCHEMA,
    compute_xforce_index,
)

LOGGER = logging.getLogger("cx_metrics.xforce_index")

# (flag suffix, default table, compute kwarg)
INPUTS: tuple[tuple[str, str, str], ...] = (
    ("shrinkage", "usr.danielanzures.io_shrinkage_metric", "shrinkage"),
    (
        "xpeers-in-target",
        "usr.danielanzures.io_xpeers_in_target_metric",
        "xpeers_in_target",
    ),
    (
        "average-xpeer-index",
        "usr.danielanzures.io_average_xpeer_index_metric",
        "average_xpeer_index",
    ),
    (
        "improved-benchmarks",
        "usr.danielanzures.io_improved_benchmarks_metric",
        "improved_benchmarks",
    ),
)

DEFAULT_TARGET = "usr.danielanzures.io_xforce_index_metric"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
    for flag, default, _ in INPUTS:
        parser.add_argument(
            f"--{flag}-source",
            default=default,
            help=f"Input metric table (default: {default}).",
        )
    parser.add_argument(
        "--target",
        default=DEFAULT_TARGET,
        help=f"Fully-qualified table to replace (default: {DEFAULT_TARGET}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and summarize but do NOT write to the warehouse.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Run id to tag this write in the snapshot/registry tables "
        "(default: PIPELINE_RUN_ID env var, else a generated UTC id).",
    )
    parser.add_argument(
        "--no-snapshot",
        action="store_true",
        help="Skip writing the append-only {target}_snapshots history table.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _log_step(label: str):
    class _Timer:
        def __enter__(self):
            self.start = time.perf_counter()
            LOGGER.info("Step: %s ...", label)
            return self

        def __exit__(self, *_):
            LOGGER.info("  %s done in %.1fs", label, time.perf_counter() - self.start)

    return _Timer()


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
        frames: dict[str, object] = {}
        for flag, _, kwarg in INPUTS:
            source = getattr(args, f"{flag.replace('-', '_')}_source")
            with _log_step(f"read {source}"):
                df = read_table(
                    conn,
                    source,
                    args.period_start,
                    args.period_end,
                    date_col="date_reference",
                )
                LOGGER.info("  %s rows", f"{len(df):,}")
            frames[kwarg] = df

        with _log_step("compute_xforce_index"):
            result = compute_xforce_index(**frames)
            LOGGER.info("  %s metric rows", f"{len(result):,}")

        expected_cols = [c for c, _ in IO_XFORCE_INDEX_METRIC_SCHEMA]
        result = result[expected_cols]

        if args.dry_run:
            LOGGER.info("Dry run — not writing. Summary:")
            print()
            for g in GRANULARITIES:
                sub = result[result["date_granularity"] == g]
                print(f"{g:>9}: {len(sub):,} rows, {sub['xforce'].nunique():,} xforces")
            print(f"\nTeams: {', '.join(sorted(result['team'].dropna().unique()))}")
            # Component count split (3 vs 4) at month grain.
            mo = result[result["date_granularity"] == "month"]
            four = (mo["denominator"] == 400).sum()
            print(f"month rows: {len(mo):,}  (4-component: {four:,}, 3-component: {len(mo) - four:,})")
            if not result.empty:
                mv = result["metric_value"].dropna()
                print(
                    f"metric_value: min={mv.min():.1f}  max={mv.max():.1f}  "
                    f"mean={mv.mean():.1f}"
                )
            print("\nHead (month grain):")
            print(mo.head(10).to_string(index=False))
            return 0

        with _log_step(f"write {args.target}"):
            run = publish(
                conn,
                result,
                args.target,
                IO_XFORCE_INDEX_METRIC_SCHEMA,
                layer="metrics",
                period_start=args.period_start,
                period_end=args.period_end,
                run_id=args.run_id,
                snapshot=not args.no_snapshot,
            )
            LOGGER.info(
                "  wrote %s rows to %s (run_id=%s)",
                f"{run.row_count:,}", args.target, run.run_id,
            )
            if run.snapshot_table:
                LOGGER.info("  snapshot -> %s", run.snapshot_table)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
