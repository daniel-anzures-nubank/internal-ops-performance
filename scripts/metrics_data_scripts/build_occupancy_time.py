"""Build the occupancy_time raw dataset and (optionally) write it to Databricks.

This script is a thin orchestrator. The math lives in
``metrics_data/occupancy_time.py`` and is covered by
``tests/metrics_data/test_occupancy_time.py``. Here we only:

  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Run the extractors needed (as Spark DataFrames):
       * ``agent_information``  (roster — xforce/xplead/squad/shift/district)
       * ``dime_slots``         (slot schedule)
       * ``shuffle_jobs``       (queue-routed work — occupancy keeps
                                 status in {finished, transferred, skipped})
       * ``oos_jobs``           (taskmaster out-of-shuffle work)
       * ``sm_jobs``            (Sprinklr Social-Media case assignments —
                                 occupancy source for social agents)
  3. Call ``compute_occupancy_time`` to get one row per (agent, date, slot).
  4. Either print a summary (``--dry-run``) or replace the target Delta table.

Target table
------------
Default: ``usr.danielanzures.io_occupancy_time_raw``. Override with ``--target``.

Manual adjustments
------------------
The committed missing-DIME-slot rows (``slots_faltantes_dime``, legacy
``h1_missing_dime_slots``) are unioned into the dime extractor output. No
metric-layer adjustments are applied here — they belong to the metrics layer.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_data_scripts/build_occupancy_time.py \\
        --period-start 2026-05-11 --period-end 2026-05-17 --dry-run

    python scripts/metrics_data_scripts/build_occupancy_time.py \\
        --period-start 2026-03-01 --period-end 2026-05-24
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path


# Locate the repo root (contains `db.py` + `extractors/`) so the sibling
# top-level modules import. We can't rely on `__file__`: Databricks runs a
# git-sourced `spark_python_task` via `exec()` with no `__file__` set, so we
# search upward from every path we can discover (file, argv, cwd).
def _repo_root() -> Path:
    starts: list[Path] = []
    try:
        starts.append(Path(__file__).resolve())
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        starts.append(Path(sys.argv[0]).resolve())
    starts.append(Path.cwd().resolve())
    for start in starts:
        for cand in (start, *start.parents):
            if (cand / "db.py").is_file() and (cand / "extractors").is_dir():
                return cand
    return Path.cwd()


REPO_ROOT = _repo_root()
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import (  # noqa: E402
    open_connection,
    run_extractor,
    publish,
    resolve_period_end,
    MAX_DIME_SENTINEL,
)
from occupancy_time import (  # noqa: E402
    IO_OCCUPANCY_TIME_SCHEMA,
    compute_occupancy_time,
)
from adjustments.manual import (  # noqa: E402
    append_missing_dime_slots,
    read_adjustment_table,
)

LOGGER = logging.getLogger("cx_metrics.occupancy_time")

DEFAULT_TARGET = "usr.danielanzures.io_occupancy_time_raw"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--period-end",
        required=True,
        help=f"ISO date (YYYY-MM-DD), or '{MAX_DIME_SENTINEL}' to resolve to "
        "the max ingested DIME date at run time.",
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

    spark = open_connection()

    use_max_dime = args.period_end == MAX_DIME_SENTINEL
    args.period_end = resolve_period_end(args.period_end, spark)
    if use_max_dime:
        LOGGER.info(
            "--period-end %s resolved to %s", MAX_DIME_SENTINEL, args.period_end
        )

    if args.period_end < args.period_start:
        LOGGER.error("--period-end must be >= --period-start")
        return 2

    with _log_step("agent_information"):
        roster = run_extractor(
            spark, "agent_information", args.period_start, args.period_end
        )

    with _log_step("dime_slots"):
        dime = run_extractor(spark, "dime_slots", args.period_start, args.period_end)
        dime = append_missing_dime_slots(
            dime, read_adjustment_table(spark, "slots_faltantes_dime")
        )

    with _log_step("shuffle_jobs"):
        shuffle_jobs = run_extractor(
            spark, "shuffle_jobs", args.period_start, args.period_end
        )

    with _log_step("oos_jobs"):
        oos_jobs = run_extractor(spark, "oos_jobs", args.period_start, args.period_end)

    with _log_step("sm_jobs"):
        sm_jobs = run_extractor(spark, "sm_jobs", args.period_start, args.period_end)

    with _log_step("compute_occupancy_time"):
        result = compute_occupancy_time(roster, dime, shuffle_jobs, oos_jobs, sm_jobs)

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        agg = result.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("agent").alias("agents"),
            F.countDistinct("date").alias("dates"),
            F.min("occupancy_minutes").alias("occ_min"),
            F.max("occupancy_minutes").alias("occ_max"),
            F.avg("occupancy_minutes").alias("occ_mean"),
        ).collect()[0]
        print(f"Rows:    {agg['rows']:,}")
        print(f"Agents:  {agg['agents']:,}")
        print(f"Dates:   {agg['dates']:,}")
        squads = [
            r["squad"]
            for r in result.select("squad").distinct().collect()
            if r["squad"] is not None
        ]
        print(f"Squads:  {len(squads):,} ({', '.join(sorted(squads))})")
        if agg["occ_min"] is not None:
            print(
                f"occupancy_minutes: min={agg['occ_min']}, "
                f"max={agg['occ_max']}, mean={agg['occ_mean']:.1f}"
            )
        print()
        print("Head:")
        result.show(10, truncate=False)
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
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

    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
