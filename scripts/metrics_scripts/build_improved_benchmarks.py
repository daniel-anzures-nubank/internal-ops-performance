"""Build the Improved Benchmarks metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/improved_benchmarks.py`` and is covered by
``tests/metrics/test_improved_benchmarks.py``. Improved Benchmarks is
**squad / district / xforce grain, month only, Core/Fraud only**. The squad /
district roll-ups have no month gate; the XForce roll-up
(``improved_benchmark_xforce``, which feeds the composite ``xforce_index``) is
gated to ``date_reference < 2026-05-01`` plus the ``david.fernandez`` Apr-2026
carve-out.

What we do here:
  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Read the two raw inputs **with a benchmark look-back** before the output
     period (month-over-month comparison + the NTPJ trailing window):
       * ``io_jobs_raw``            — 6-month look-back.
       * ``io_occupancy_time_raw``  — 2-month look-back.
  3. Call ``compute_improved_benchmarks`` (emits only the requested months).
  4. Either print a summary (``--dry-run``) or replace the target Delta table.

Tables
------
* Inputs:  ``usr.danielanzures.io_jobs_raw``, ``usr.danielanzures.io_occupancy_time_raw``.
* Output:  ``usr.danielanzures.io_improved_benchmarks_metric`` (override ``--target``).

Manual adjustments
------------------
``exclusiones_generales`` (slot/date windows), ``inconsistencias_dime``
(DIME reclassification), ``cross_support`` (queue exclusions), and
``exclusiones_jobs`` (job exclusions) are read from their synced ``adj_*`` Delta
tables, if present, and applied inside ``compute_improved_benchmarks``.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_improved_benchmarks.py \\
        --period-start 2026-03-01 --period-end 2026-03-31 --dry-run

    python scripts/metrics_scripts/build_improved_benchmarks.py \\
        --period-start 2026-01-01 --period-end 2026-04-30
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
from improved_benchmarks import (  # noqa: E402
    IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA,
    compute_improved_benchmarks,
)
from adjustments.manual import read_adjustment_table  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.improved_benchmarks")

DEFAULT_JOBS_SOURCE = "usr.danielanzures.io_jobs_raw"
DEFAULT_OCC_SOURCE = "usr.danielanzures.io_occupancy_time_raw"
DEFAULT_TARGET = "usr.danielanzures.io_improved_benchmarks_metric"

JOBS_LOOKBACK_MONTHS = 6
OCC_LOOKBACK_MONTHS = 2


def _lookback_start(period_start: date, months: int) -> date:
    """First day of the month that is ``months`` before ``period_start``."""
    total = period_start.year * 12 + (period_start.month - 1) - months
    year, month = divmod(total, 12)
    return date(year, month + 1, 1)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
    parser.add_argument("--jobs-source", default=DEFAULT_JOBS_SOURCE)
    parser.add_argument("--occupancy-source", default=DEFAULT_OCC_SOURCE)
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

    jobs_start = _lookback_start(args.period_start, JOBS_LOOKBACK_MONTHS)
    occ_start = _lookback_start(args.period_start, OCC_LOOKBACK_MONTHS)

    spark = open_connection()

    with _log_step(f"read {args.jobs_source} (from {jobs_start})"):
        jobs = read_table(spark, args.jobs_source, jobs_start, args.period_end)

    with _log_step(f"read {args.occupancy_source} (from {occ_start})"):
        occ = read_table(spark, args.occupancy_source, occ_start, args.period_end)

    with _log_step("compute_improved_benchmarks"):
        result = compute_improved_benchmarks(
            jobs,
            occ,
            args.period_start,
            args.period_end,
            general_exclusions=read_adjustment_table(spark, "exclusiones_generales"),
            dime_inconsistencies=read_adjustment_table(spark, "inconsistencias_dime"),
            cross_support=read_adjustment_table(spark, "cross_support"),
            job_exclusions=read_adjustment_table(spark, "exclusiones_jobs"),
        )

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        by_metric = (
            result.groupBy("metric")
            .agg(F.count(F.lit(1)).alias("rows"))
            .collect()
        )
        for r in sorted(by_metric, key=lambda x: x["metric"]):
            teams = sorted(
                t["team"]
                for t in result.filter(F.col("metric") == r["metric"])
                .select("team")
                .distinct()
                .collect()
                if t["team"] is not None
            )
            print(f"{r['metric']}: {r['rows']:,} rows  teams={teams}")
        stats = result.agg(
            F.min("metric_value").alias("mv_min"),
            F.max("metric_value").alias("mv_max"),
            F.avg("metric_value").alias("mv_mean"),
        ).collect()[0]
        if stats["mv_min"] is not None:
            print(
                f"\nmetric_value (%): min={stats['mv_min']:.1f} "
                f"max={stats['mv_max']:.1f} mean={stats['mv_mean']:.1f}"
            )
        print("\nHead:")
        result.show(15, truncate=False)
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
            result,
            args.target,
            IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA,
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
