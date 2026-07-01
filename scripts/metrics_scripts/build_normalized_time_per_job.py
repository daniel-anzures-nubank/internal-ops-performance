"""Build the NTPJ benchmark substrate (``normalized_time_per_job``) and write it.

Metrics-layer script. Thin orchestrator — the math lives in ``metrics/ntpj.py``
(``compute_normalized_time_per_job``, sharing ``_ntpj_base`` with the NTPJ metric)
and is covered by ``tests/metrics/test_ntpj.py``. This materializes legacy's
``usr.mx__cx.normalized_time_per_job``: one row per ``(agent, job_id,
benchmark_month, xforce, xplead, team, squad, district)`` with the **cohort-wide**
``exp_duration_job``. ``improved_benchmarks`` consumes it for the NTPJ benchmark
family, so its benchmark values + attribution match the shipped NTPJ metric
exactly (not re-derived from raw jobs).

Here we only:
  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Read ``io_jobs_raw`` with the NTPJ benchmark look-back (trailing window).
  3. Call ``compute_normalized_time_per_job`` (emits the requested months).
  4. Either print a summary (``--dry-run``) or replace the target Delta table.

Tables
------
* Input:  ``usr.danielanzures.io_jobs_raw`` (override ``--source``).
* Output: ``usr.danielanzures.io_normalized_time_per_job`` (override ``--target``).

Window note
-----------
``improved_benchmarks`` needs one **previous** month before its output start for
the month-over-month LAG. Run this for ``--period-start`` = (improved_benchmarks
output start − 1 month) so the comparator month is materialized. The NTPJ
trailing benchmark (months ≤ 2026-03 use M-4 … M) needs ~4 more months of
``io_jobs_raw`` before that — read automatically via the look-back.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_normalized_time_per_job.py \\
        --period-start 2025-12-01 --period-end 2026-04-30 --dry-run
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
sys.path.insert(0, str(REPO_ROOT / "metrics"))

from db import open_connection, read_table, publish  # noqa: E402
from ntpj import (  # noqa: E402
    IO_NORMALIZED_TIME_PER_JOB_SCHEMA,
    compute_normalized_time_per_job,
)
from adjustments.manual import read_adjustment_table  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.normalized_time_per_job")

DEFAULT_SOURCE = "usr.danielanzures.io_jobs_raw"
DEFAULT_TARGET = "usr.danielanzures.io_normalized_time_per_job"

# Extra months read before period_start so the trailing-window benchmark
# (months <= 2026-03 use M-4 ... M) has its source data. Matches build_ntpj.
BENCHMARK_LOOKBACK_MONTHS = 4


def _lookback_start(period_start: date) -> date:
    """First day of the month BENCHMARK_LOOKBACK_MONTHS before period_start."""
    total = period_start.year * 12 + (period_start.month - 1) - BENCHMARK_LOOKBACK_MONTHS
    year, month = divmod(total, 12)
    return date(year, month + 1, 1)


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
    spark = open_connection()

    with _log_step(f"read {args.source} (look-back from {lookback_start})"):
        jobs = read_table(spark, args.source, lookback_start, args.period_end)

    with _log_step("compute_normalized_time_per_job"):
        result = compute_normalized_time_per_job(
            jobs,
            args.period_start,
            args.period_end,
            general_exclusions=read_adjustment_table(spark, "exclusiones_generales"),
            cross_support=read_adjustment_table(spark, "cross_support"),
            job_exclusions=read_adjustment_table(spark, "exclusiones_jobs"),
        )

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        by_month = (
            result.groupBy("benchmark_month")
            .agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("job_id").alias("jobs"),
                F.countDistinct("agent").alias("agents"),
            )
            .orderBy("benchmark_month")
            .collect()
        )
        for r in by_month:
            print(
                f"{r['benchmark_month']}: {r['rows']:,} rows, "
                f"{r['jobs']:,} job_ids, {r['agents']:,} agents"
            )
        stats = result.agg(
            F.min("exp_duration_job").alias("e_min"),
            F.max("exp_duration_job").alias("e_max"),
            F.avg("exp_duration_job").alias("e_mean"),
            F.sum(F.col("exp_duration_job").isNull().cast("int")).alias("e_null"),
        ).collect()[0]
        if stats["e_min"] is not None:
            print(
                f"\nexp_duration_job (s): min={stats['e_min']:.1f}  "
                f"max={stats['e_max']:.1f}  mean={stats['e_mean']:.1f}  "
                f"null={stats['e_null']:,}"
            )
        print("\nHead:")
        result.orderBy("benchmark_month", "job_id").show(10, truncate=False)
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
            result,
            args.target,
            IO_NORMALIZED_TIME_PER_JOB_SCHEMA,
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

    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
