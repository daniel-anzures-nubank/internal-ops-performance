"""Build the jobs_raw raw dataset and (optionally) write it to Databricks.

This script is a thin orchestrator. The math lives in
``metrics_data/jobs_raw.py`` and is covered by
``tests/metrics_data/test_jobs_raw.py``. Here we only:

  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Run the extractors needed (as Spark DataFrames):
       * ``agent_information``  (roster — xforce/xplead/squad/shift/district/status)
       * ``dime_slots``         (slot schedule, for required_activity_on_day_flag)
       * ``shuffle_jobs``       (queue-routed work — ALL statuses kept)
       * ``oos_jobs``           (taskmaster out-of-shuffle work)
  3. Call ``compute_jobs_raw`` to get one row per individual job.
  4. Either print a summary (``--dry-run``) or replace the target Delta table.

jobs_raw is a RAW per-job feed (no aggregation, no monthly benchmark — those
move to the metrics layer), so it only needs the requested period; there is
no rolling-baseline lookback here (the metrics layer reads its own look-back).

Missing DIME slots
------------------
NTPJ does **not** append the ``h1_missing_dime_slots`` / ``slots_faltantes_dime``
rows when deriving its required-activity flag: legacy ``dime_ntpj`` (the source
of NTPJ's "required" set) reads ``agent_dimensioned_activities`` ONLY — the
missing-slots union appears only in legacy ``manual_adjustments_ntpj`` (the
cross-support / dimensioned-activity carve-out logic, applied later via the
adjustment tables). So, unlike occupancy/adherence, this builder does not call
``append_missing_dime_slots``.

Target table
------------
Default: ``usr.danielanzures.io_jobs_raw``. Override with ``--target``.

Manual adjustments
------------------
None are applied here. The Google-Sheets-driven adjustments layer is applied at
the metrics layer (``metrics/ntpj.py``); this script produces the "clean"
per-job baseline.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_data_scripts/build_jobs_raw.py \\
        --period-start 2026-04-01 --period-end 2026-04-30 --dry-run

    python scripts/metrics_data_scripts/build_jobs_raw.py \\
        --period-start 2025-12-01 --period-end 2026-05-24
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

from db import open_connection, run_extractor, publish  # noqa: E402
from jobs_raw import IO_JOBS_RAW_SCHEMA, compute_jobs_raw  # noqa: E402

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

    spark = open_connection()

    with _log_step("agent_information"):
        roster = run_extractor(
            spark, "agent_information", args.period_start, args.period_end
        )

    with _log_step("dime_slots"):
        dime = run_extractor(spark, "dime_slots", args.period_start, args.period_end)

    with _log_step("shuffle_jobs"):
        shuffle_jobs = run_extractor(
            spark, "shuffle_jobs", args.period_start, args.period_end
        )

    with _log_step("oos_jobs"):
        oos_jobs = run_extractor(spark, "oos_jobs", args.period_start, args.period_end)

    with _log_step("compute_jobs_raw"):
        result = compute_jobs_raw(roster, dime, shuffle_jobs, oos_jobs)

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        agg = result.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("agent").alias("agents"),
            F.countDistinct("date").alias("dates"),
            F.countDistinct("job_id").alias("job_ids"),
            F.sum("required_activity_on_day_flag").alias("flagged"),
        ).collect()[0]
        print(f"Rows (jobs):      {agg['rows']:,}")
        print(f"Agents:           {agg['agents']:,}")
        print(f"Dates:            {agg['dates']:,}")
        print(f"Distinct job_ids: {agg['job_ids']:,}")
        flagged = agg["flagged"] or 0
        print(
            f"required_activity_on_day_flag = 1: {int(flagged):,} / {agg['rows']:,} jobs"
        )
        squads = [
            r["squad"]
            for r in result.select("squad").distinct().collect()
            if r["squad"] is not None
        ]
        print(f"Squads:           {len(squads):,} ({', '.join(sorted(squads))})")
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

    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
