"""Build the occupancy_time raw dataset and (optionally) write it to Databricks.

This script is a thin orchestrator. The math lives in
``metrics_data/occupancy_time.py`` and is covered by
``tests/test_occupancy_time.py``. Here we only:

  1. Open a Databricks SQL connection (shared ``db.open_connection``).
  2. Run the four extractors needed:
       * ``agent_information``  (roster — for xforce/xplead/squad/shift/district)
       * ``dime_slots``         (slot schedule)
       * ``shuffle_jobs``       (queue-routed work — occupancy keeps
                                 status in {finished, transferred, skipped})
       * ``oos_jobs``           (taskmaster out-of-shuffle work)
       * ``sm_jobs``            (Sprinklr Social-Media case assignments —
                                 occupancy source for social agents)
  3. Call ``compute_occupancy_time`` to get one row per (agent, date, slot).
  4. Filter to the requested period.
  5. Either print a summary (``--dry-run``) or replace the target Delta table.

Target table
------------
Default: ``usr.danielanzures.io_occupancy_time_raw``. Override with ``--target``.

Data window
-----------
occupancy_time is a per-slot raw table with no monthly benchmark (the
benchmark moved to the metrics layer). The orchestrator still extracts a
slightly wider data window — ``floor_month(period_start) ... --data-end-date``
— and filters the final output to ``period_start ... period_end``; the wider
window is harmless and kept so a future benchmark step can reuse it.

Manual adjustments
------------------
None are applied here. The Google-Sheets-driven adjustments layer is a
follow-up; this script intentionally produces the "clean" baseline so the
two can be diffed.

Usage
-----
::

    uv run python scripts/metrics_data_scripts/build_occupancy_time.py \\
        --period-start 2026-05-11 --period-end 2026-05-17 --dry-run

    uv run python scripts/metrics_data_scripts/build_occupancy_time.py \\
        --period-start 2026-03-01 --period-end 2026-05-24
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor, publish  # noqa: E402
from occupancy_time import IO_OCCUPANCY_TIME_SCHEMA, compute_occupancy_time  # noqa: E402
from adjustments.manual import append_missing_dime_slots  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.occupancy_time")

DEFAULT_TARGET = "usr.danielanzures.io_occupancy_time_raw"


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
        "--data-end-date",
        type=date.fromisoformat,
        default=None,
        help=(
            "Upper bound on DIME / jobs / roster extraction. Defaults to "
            "today's date. The monthly benchmark uses every row available "
            "at run time, not just rows up through --period-end — so we "
            "pull as far forward as today by default."
        ),
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


def _floor_month(d: date) -> date:
    """Return the first day of ``d``'s month."""
    return d.replace(day=1)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(levelname)s %(name)s: %(message)s"
    )

    if args.period_end < args.period_start:
        LOGGER.error("--period-end must be >= --period-start")
        return 2

    data_start = _floor_month(args.period_start)
    data_end = args.data_end_date or datetime.now().date()
    if data_end < args.period_end:
        # Caller explicitly capped data; warn but respect it.
        LOGGER.warning(
            "--data-end-date %s is BEFORE --period-end %s; the monthly "
            "benchmark for any partial month at the end of the period will "
            "reflect only --data-end-date's slice.",
            data_end,
            args.period_end,
        )
    LOGGER.info(
        "Pulling DIME / jobs / roster from %s through %s; "
        "filtering output to %s ... %s",
        data_start,
        data_end,
        args.period_start,
        args.period_end,
    )

    conn = open_connection()
    try:
        with _log_step("agent_information"):
            roster = run_extractor(
                conn, "agent_information", data_start, data_end
            )
            LOGGER.info("  %s roster rows", f"{len(roster):,}")

        with _log_step("dime_slots"):
            dime = run_extractor(conn, "dime_slots", data_start, data_end)
            dime = append_missing_dime_slots(dime)
            LOGGER.info("  %s DIME slot rows", f"{len(dime):,}")

        with _log_step("shuffle_jobs"):
            shuffle_jobs = run_extractor(conn, "shuffle_jobs", data_start, data_end)
            LOGGER.info("  %s shuffle job rows", f"{len(shuffle_jobs):,}")

        with _log_step("oos_jobs"):
            oos_jobs = run_extractor(conn, "oos_jobs", data_start, data_end)
            LOGGER.info("  %s OOS job rows", f"{len(oos_jobs):,}")

        with _log_step("sm_jobs"):
            sm_jobs = run_extractor(conn, "sm_jobs", data_start, data_end)
            LOGGER.info("  %s SM (Sprinklr) job rows", f"{len(sm_jobs):,}")

        with _log_step("compute_occupancy_time"):
            result = compute_occupancy_time(
                roster, dime, shuffle_jobs, oos_jobs, sm_jobs
            )
            LOGGER.info("  %s rows in raw occupancy_time frame (pre-period filter)",
                        f"{len(result):,}")

        # Restrict output to the requested slice. The wider data window is
        # only there to make the benchmark match legacy.
        result = result.loc[
            (result["date"] >= args.period_start)
            & (result["date"] <= args.period_end)
        ].copy()
        LOGGER.info("  %s rows after period filter", f"{len(result):,}")

        # Reorder to match the declared schema before writing.
        expected_cols = [c for c, _ in IO_OCCUPANCY_TIME_SCHEMA]
        result = result[expected_cols]

        if args.dry_run:
            LOGGER.info("Dry run — not writing. Summary:")
            print()
            print(f"Rows:             {len(result):,}")
            print(f"Agents:           {result['agent'].nunique():,}")
            print(f"Dates:            {result['date'].nunique():,}")
            print(
                f"Squads:           {result['squad'].nunique():,} "
                f"({', '.join(sorted(result['squad'].dropna().unique()))})"
            )
            print(
                f"Districts:        {result['district'].nunique():,} "
                f"({', '.join(sorted(result['district'].dropna().unique()))})"
            )
            print(
                f"occupancy_minutes: min={result['occupancy_minutes'].min()}, "
                f"max={result['occupancy_minutes'].max()}, "
                f"mean={result['occupancy_minutes'].mean():.1f}"
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
                IO_OCCUPANCY_TIME_SCHEMA,
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
