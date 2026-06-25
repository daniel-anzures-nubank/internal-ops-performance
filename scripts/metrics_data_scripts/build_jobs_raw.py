"""Build the jobs_raw dataset and (optionally) write it to Databricks.

This script is a thin orchestrator. The math lives in
``metrics_data/jobs_raw.py`` and is covered by ``tests/test_jobs_raw.py``.
Here we only:

  1. Open a Databricks SQL connection (shared `db.open_connection`).
  2. Run the four extractors needed:
       * ``agent_information``  (roster)
       * ``dime_slots``         (slot schedule, for required_activity_on_day_flag)
       * ``shuffle_jobs``       (queue-routed work — ALL statuses kept)
       * ``oos_jobs``           (taskmaster out-of-shuffle work)
  3. Call ``compute_jobs_raw`` to get one row per individual job.
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

jobs_raw is a RAW per-job feed (no aggregation, no monthly benchmark — those
move to the metrics layer), so it only needs the requested period; there is
no rolling-baseline lookback.

Target table
------------
Default: ``usr.danielanzures.io_jobs_raw``. Override with ``--target``.

Manual adjustments
------------------
None are applied here. The Google-Sheets-driven adjustments layer is a
follow-up; this script intentionally produces the "clean" baseline so the
two can be diffed.

Usage
-----
::

    uv run python scripts/metrics_data_scripts/build_jobs_raw.py \\
        --period-start 2026-04-01 --period-end 2026-04-30 --dry-run

    uv run python scripts/metrics_data_scripts/build_jobs_raw.py \\
        --period-start 2025-12-01 --period-end 2026-05-24
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
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor, publish  # noqa: E402
from jobs_raw import IO_JOBS_RAW_SCHEMA, compute_jobs_raw  # noqa: E402
from adjustments.manual import append_missing_dime_slots, read_adjustment_csv  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.jobs_raw")

DEFAULT_TARGET = "usr.danielanzures.io_jobs_raw"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
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
    """Context manager-ish helper that times each pipeline stage."""

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
        with _log_step("agent_information"):
            roster = run_extractor(
                conn, "agent_information", args.period_start, args.period_end
            )
            LOGGER.info("  %s roster rows", f"{len(roster):,}")

        with _log_step("dime_slots"):
            dime = run_extractor(
                conn, "dime_slots", args.period_start, args.period_end
            )
            dime = append_missing_dime_slots(dime)
            LOGGER.info("  %s DIME slot rows", f"{len(dime):,}")

        with _log_step("shuffle_jobs"):
            shuffle_jobs = run_extractor(
                conn, "shuffle_jobs", args.period_start, args.period_end
            )
            LOGGER.info("  %s shuffle job rows", f"{len(shuffle_jobs):,}")

        with _log_step("oos_jobs"):
            oos_jobs = run_extractor(
                conn, "oos_jobs", args.period_start, args.period_end
            )
            LOGGER.info("  %s OOS job rows", f"{len(oos_jobs):,}")

        with _log_step("compute_jobs_raw"):
            result = compute_jobs_raw(
                roster,
                dime,
                shuffle_jobs,
                oos_jobs,
                dime_inconsistencies=read_adjustment_csv("inconsistencias_dime"),
            )
            LOGGER.info("  %s rows in final jobs_raw frame", f"{len(result):,}")

        # Reorder to match the declared schema before writing.
        expected_cols = [c for c, _ in IO_JOBS_RAW_SCHEMA]
        result = result[expected_cols]

        if args.dry_run:
            LOGGER.info("Dry run — not writing. Summary:")
            print()
            print(f"Rows (jobs):      {len(result):,}")
            print(f"Agents:           {result['agent'].nunique():,}")
            print(f"Dates:            {result['date'].nunique():,}")
            print(
                f"Squads:           {result['squad'].nunique():,} "
                f"({', '.join(sorted(result['squad'].dropna().unique()))})"
            )
            print(f"Distinct job_ids: {result['job_id'].nunique():,}")
            print(f"Activity types:   {sorted(result['activity_type'].dropna().unique())}")
            print(f"Statuses:         {sorted(result['status'].dropna().unique())}")
            flagged = int(result["required_activity_on_day_flag"].sum())
            print(
                f"required_activity_on_day_flag = 1: {flagged:,} / {len(result):,} jobs"
            )
            print()
            print("Head:")
            print(result.head(10).to_string(index=False))
            return 0

        with _log_step(f"write {args.target}"):
            run = publish(
                conn,
                result,
                args.target,
                IO_JOBS_RAW_SCHEMA,
                layer="metrics_data",
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
