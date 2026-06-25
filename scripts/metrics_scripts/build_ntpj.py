"""Build the NTPJ metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in ``metrics/ntpj.py``
and is covered by ``tests/metrics/test_ntpj.py``. Here we only:

  1. Open a Databricks SQL connection (shared `db.open_connection`).
  2. Read the raw input table ``io_jobs_raw`` for the period **plus a 4-month
     benchmark look-back** (NTPJ's monthly benchmark can use a trailing window;
     see `metrics/ntpj.py`). The look-back rows are used only to build
     benchmarks — they are not emitted.
  3. Call ``compute_ntpj`` to get day/week/month metric rows for the period.
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

Tables
------
* Input:  ``usr.danielanzures.io_jobs_raw`` (override `--source`).
* Output: ``usr.danielanzures.io_ntpj_metric`` (override `--target`).

Manual adjustments
------------------
None applied here (no adjustments layer yet) — see `metrics/ntpj.py` for the
list of legacy cross-support / outage carve-outs deferred to that layer.

Usage
-----
::

    uv run python scripts/metrics_scripts/build_ntpj.py \\
        --period-start 2026-05-01 --period-end 2026-05-24 --dry-run

    uv run python scripts/metrics_scripts/build_ntpj.py \\
        --period-start 2026-01-01 --period-end 2026-05-24
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
sys.path.insert(0, str(REPO_ROOT / "metrics"))

from db import open_connection, read_table, publish  # noqa: E402
from metric_utils import GRANULARITIES  # noqa: E402
from ntpj import IO_NTPJ_METRIC_SCHEMA, compute_ntpj  # noqa: E402
from adjustments.manual import read_adjustment_csv  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.ntpj")

DEFAULT_SOURCE = "usr.danielanzures.io_jobs_raw"
DEFAULT_TARGET = "usr.danielanzures.io_ntpj_metric"

# Extra months read before period_start so the trailing-window benchmark
# (months ≤ 2026-03 use M-4 … M) has its source data.
BENCHMARK_LOOKBACK_MONTHS = 4


def _lookback_start(period_start: date) -> date:
    """First day of the month that is BENCHMARK_LOOKBACK_MONTHS before period_start."""
    p = pd.Period(period_start, freq="M") - BENCHMARK_LOOKBACK_MONTHS
    return p.to_timestamp().date()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Raw input table to read (default: {DEFAULT_SOURCE}).",
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

    lookback_start = _lookback_start(args.period_start)
    conn = open_connection()
    try:
        with _log_step(f"read {args.source} (look-back from {lookback_start})"):
            jobs = read_table(conn, args.source, lookback_start, args.period_end)
            LOGGER.info("  %s raw job rows (incl. look-back)", f"{len(jobs):,}")

        with _log_step("compute_ntpj"):
            result = compute_ntpj(
                jobs,
                args.period_start,
                args.period_end,
                general_exclusions=read_adjustment_csv("exclusiones_generales"),
                cross_support=read_adjustment_csv("cross_support"),
                job_exclusions=read_adjustment_csv("exclusiones_jobs"),
            )
            LOGGER.info("  %s metric rows", f"{len(result):,}")

        expected_cols = [c for c, _ in IO_NTPJ_METRIC_SCHEMA]
        result = result[expected_cols]

        if args.dry_run:
            LOGGER.info("Dry run — not writing. Summary:")
            print()
            for g in GRANULARITIES:
                sub = result[result["date_granularity"] == g]
                print(f"{g:>9}: {len(sub):,} rows, {sub['agent'].nunique():,} agents")
            print(f"\nAgents: {result['agent'].nunique():,}   "
                  f"Teams: {', '.join(sorted(result['team'].dropna().unique()))}")
            if not result.empty:
                mv = result["metric_value"].dropna()
                print(f"metric_value (%): min={mv.min():.1f}  "
                      f"max={mv.max():.1f}  mean={mv.mean():.1f}")
            print("\nHead (day grain):")
            print(
                result[result["date_granularity"] == "day"]
                .head(10)
                .to_string(index=False)
            )
            return 0

        with _log_step(f"write {args.target}"):
            run = publish(
                conn,
                result,
                args.target,
                IO_NTPJ_METRIC_SCHEMA,
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
    sys.exit(main())
