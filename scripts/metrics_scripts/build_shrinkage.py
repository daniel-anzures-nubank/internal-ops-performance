"""Build the Shrinkage metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/shrinkage.py`` and is covered by ``tests/metrics/test_shrinkage.py``.
Here we only:

  1. Open a Databricks SQL connection (shared `db.open_connection`).
  2. Read the raw input table ``io_shrinkage_slots_raw`` for the period
     (via `db.read_table` — the metrics layer reads the already-built
     `io_*_raw` tables).
  3. Call ``compute_shrinkage`` (agent grain) and ``compute_shrinkage_rollups``
     (XForce + XPLead slot-weighted roll-ups) and concat them at all
     granularities.
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

Tables
------
* Input:  ``usr.danielanzures.io_shrinkage_slots_raw`` (override `--source`).
* Output: ``usr.danielanzures.io_shrinkage_metric`` (override `--target`).

Manual adjustments
------------------
None applied here (no adjustments layer yet) — see `metrics/shrinkage.py` for
the list of legacy carve-outs deferred to the future Adjustments layer.

Usage
-----
::

    uv run python scripts/metrics_scripts/build_shrinkage.py \\
        --period-start 2026-05-01 --period-end 2026-05-24 --dry-run

    uv run python scripts/metrics_scripts/build_shrinkage.py \\
        --period-start 2026-01-01 --period-end 2026-05-24
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

import pandas as pd  # noqa: E402

from db import open_connection, read_table, publish  # noqa: E402
from metric_utils import GRANULARITIES  # noqa: E402
from shrinkage import (  # noqa: E402
    IO_SHRINKAGE_METRIC_SCHEMA,
    compute_shrinkage,
    compute_shrinkage_rollups,
)
from adjustments.manual import read_adjustment_csv  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.shrinkage")

DEFAULT_SOURCE = "usr.danielanzures.io_shrinkage_slots_raw"
DEFAULT_TARGET = "usr.danielanzures.io_shrinkage_metric"


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

    conn = open_connection()
    try:
        with _log_step(f"read {args.source}"):
            shrinkage_slots = read_table(
                conn, args.source, args.period_start, args.period_end
            )
            LOGGER.info("  %s raw slot rows", f"{len(shrinkage_slots):,}")

        with _log_step("compute_shrinkage"):
            agent = compute_shrinkage(
                shrinkage_slots,
                general_exclusions=read_adjustment_csv("exclusiones_generales"),
                dime_inconsistencies=read_adjustment_csv("inconsistencias_dime"),
                training=read_adjustment_csv("training"),
                shadowing=read_adjustment_csv("shadowing"),
                no_shrinkage=read_adjustment_csv("no_shrinkage"),
            )
            rollups = compute_shrinkage_rollups(agent)
            result = pd.concat([agent, rollups], ignore_index=True)
            LOGGER.info(
                "  %s metric rows (%s agent + %s roll-up)",
                f"{len(result):,}", f"{len(agent):,}", f"{len(rollups):,}",
            )

        expected_cols = [c for c, _ in IO_SHRINKAGE_METRIC_SCHEMA]
        result = result[expected_cols]

        if args.dry_run:
            LOGGER.info("Dry run — not writing. Summary:")
            print()
            for g in GRANULARITIES:
                sub = result[result["date_granularity"] == g]
                ag = sub[sub["metric"] == "shrinkage"]
                print(f"{g:>9}: {len(sub):,} rows, {ag['agent'].nunique():,} agents")
            print("\nRows by metric: " + ", ".join(
                f"{name}={cnt:,}"
                for name, cnt in result["metric"].value_counts().items()
            ))
            print(f"Agents: {result['agent'].nunique():,}   "
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
                IO_SHRINKAGE_METRIC_SCHEMA,
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
