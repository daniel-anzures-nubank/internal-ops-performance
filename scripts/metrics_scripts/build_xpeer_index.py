"""Build the agent-level Xpeer Index metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/xpeer_index.py`` and is covered by
``tests/metrics/test_xpeer_index.py``. Here we only:

  1. Open a Databricks SQL connection (shared `db.open_connection`).
  2. Read the seven per-agent metric tables for the period (via `db.read_table`,
     scoped on ``date_reference``).
  3. Call ``compute_xpeer_index`` to combine them into the Index at all
     granularities (day/week/month/quarter/semester/year).
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

Because the Index composition is **month-anchored** (NO from March 2026,
Quality from Feb, etc.), prefer running with whole-month periods.

Tables
------
* Inputs (override with the matching flag):
    - ``usr.danielanzures.io_adherence_metric``              (driver)
    - ``usr.danielanzures.io_ntpj_metric``
    - ``usr.danielanzures.io_normalized_occupancy_metric``
    - ``usr.danielanzures.io_quality_metric``
    - ``usr.danielanzures.io_tnps_metric``
    - ``usr.danielanzures.io_wows_metric``
    - ``usr.danielanzures.io_content_csat_metric``
* Output: ``usr.danielanzures.io_xpeer_index_metric`` (override `--target`).

Manual adjustments
------------------
None applied here (no adjustments layer yet) — see `metrics/xpeer_index.py` for
the deferred per-agent legacy carve-outs.

Usage
-----
::

    uv run python scripts/metrics_scripts/build_xpeer_index.py \\
        --period-start 2026-05-01 --period-end 2026-05-31 --dry-run

    uv run python scripts/metrics_scripts/build_xpeer_index.py \\
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
from xpeer_index import (  # noqa: E402
    IO_XPEER_INDEX_METRIC_SCHEMA,
    compute_xpeer_index,
)

LOGGER = logging.getLogger("cx_metrics.xpeer_index")

# (flag suffix, default table, compute kwarg)
INPUTS: tuple[tuple[str, str, str], ...] = (
    ("adherence", "usr.danielanzures.io_adherence_metric", "adherence"),
    ("ntpj", "usr.danielanzures.io_ntpj_metric", "ntpj"),
    (
        "normalized-occupancy",
        "usr.danielanzures.io_normalized_occupancy_metric",
        "normalized_occupancy",
    ),
    ("quality", "usr.danielanzures.io_quality_metric", "quality"),
    ("tnps", "usr.danielanzures.io_tnps_metric", "tnps"),
    ("wows", "usr.danielanzures.io_wows_metric", "wows"),
    ("content-csat", "usr.danielanzures.io_content_csat_metric", "content_csat"),
)

DEFAULT_TARGET = "usr.danielanzures.io_xpeer_index_metric"


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

        with _log_step("compute_xpeer_index"):
            result = compute_xpeer_index(**frames)
            LOGGER.info("  %s metric rows", f"{len(result):,}")

        expected_cols = [c for c, _ in IO_XPEER_INDEX_METRIC_SCHEMA]
        result = result[expected_cols]

        if args.dry_run:
            LOGGER.info("Dry run — not writing. Summary:")
            print()
            for g in GRANULARITIES:
                sub = result[result["date_granularity"] == g]
                print(f"{g:>9}: {len(sub):,} rows, {sub['agent'].nunique():,} agents")
            print(
                f"\nAgents: {result['agent'].nunique():,}   "
                f"Teams: {', '.join(sorted(result['team'].dropna().unique()))}"
            )
            if not result.empty:
                mv = result["metric_value"].dropna()
                print(
                    f"Index (%): min={mv.min():.1f}  max={mv.max():.1f}  "
                    f"mean={mv.mean():.1f}"
                )
            print("\nHead (month grain):")
            print(
                result[result["date_granularity"] == "month"]
                .head(10)
                .to_string(index=False)
            )
            return 0

        with _log_step(f"write {args.target}"):
            run = publish(
                conn,
                result,
                args.target,
                IO_XPEER_INDEX_METRIC_SCHEMA,
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
