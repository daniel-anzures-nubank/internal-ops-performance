"""Build the NTPJ metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in ``metrics/ntpj.py``
and is covered by ``tests/metrics/test_ntpj.py``. Here we only:

  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Read the raw input table ``io_jobs_raw`` for the period **plus a 4-month
     benchmark look-back** (NTPJ's monthly benchmark can use a trailing window;
     see ``metrics/ntpj.py``). The look-back rows are used only to build
     benchmarks — they are not emitted.
  3. Call ``compute_ntpj`` to get the day/week/month/quarter/semester/year
     metric rows for the period.
  4. Either print a summary (``--dry-run``) or replace the target Delta table.

Content is a different metric
-----------------------------
Content NTPJ is an **SLA-weighted compliance** metric, not the duration ratio. We
compute Core/Fraud NTPJ from ``io_jobs_raw`` as usual (Content still feeds the
cohort-wide benchmark), then **drop the Content output rows and union** the
SLA-based Content NTPJ from ``io_jobs_within_sla_raw`` (``metrics/content_sla_ntpj.py``)
— so ``io_ntpj_metric`` stays one standardized ``metric='ntpj'`` table.

Tables
------
* Inputs:  ``usr.danielanzures.io_jobs_raw`` (override ``--source``),
  ``usr.danielanzures.io_jobs_within_sla_raw`` (override ``--sla-source``).
* Output: ``usr.danielanzures.io_ntpj_metric`` (override ``--target``).

Manual adjustments
------------------
``exclusiones_generales`` (slot/date windows to drop), ``cross_support`` (queue
exclusions), and ``exclusiones_jobs`` (job exclusions) are read from their synced
``adj_*`` Delta tables, if present, and applied inside ``compute_ntpj`` BEFORE the
benchmark groupby (so an excluded job leaves both the benchmark and the
contribution, matching legacy).

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_ntpj.py \\
        --period-start 2026-05-01 --period-end 2026-05-24 --dry-run

    python scripts/metrics_scripts/build_ntpj.py \\
        --period-start 2026-01-01 --period-end 2026-05-24
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

from pyspark.sql import functions as F  # noqa: E402

from db import open_connection, read_table, publish  # noqa: E402
from metric_utils import GRANULARITIES  # noqa: E402
from ntpj import IO_NTPJ_METRIC_SCHEMA, compute_ntpj  # noqa: E402
from content_sla_ntpj import compute_content_sla_ntpj  # noqa: E402
from adjustments.manual import read_adjustment_table  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.ntpj")

DEFAULT_SOURCE = "usr.danielanzures.io_jobs_raw"
DEFAULT_SLA_SOURCE = "usr.danielanzures.io_jobs_within_sla_raw"
DEFAULT_TARGET = "usr.danielanzures.io_ntpj_metric"

# Extra months read before period_start so the trailing-window benchmark
# (months <= 2026-03 use M-4 ... M) has its source data.
BENCHMARK_LOOKBACK_MONTHS = 4


def _lookback_start(period_start: date) -> date:
    """First day of the month that is BENCHMARK_LOOKBACK_MONTHS before period_start."""
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
        "--sla-source",
        default=DEFAULT_SLA_SOURCE,
        help="Content jobs-within-SLA raw table, unioned in as Content NTPJ "
        f"(default: {DEFAULT_SLA_SOURCE}).",
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

    with _log_step("compute_ntpj"):
        result = compute_ntpj(
            jobs,
            args.period_start,
            args.period_end,
            general_exclusions=read_adjustment_table(spark, "exclusiones_generales"),
            cross_support=read_adjustment_table(spark, "cross_support"),
            job_exclusions=read_adjustment_table(spark, "exclusiones_jobs"),
        )

    # Content NTPJ is a DIFFERENT metric — SLA-weighted compliance, not duration.
    # Replace the duration-based Content rows with the SLA-based ones so
    # io_ntpj_metric stays one standardized table (metric='ntpj'). Content still
    # fed the cohort-wide duration benchmark above; we filter the OUTPUT (not the
    # input), so the Core/Fraud benchmark is unchanged. The `not_content` mask is
    # NULL-safe: NULL-team (main-deck support squads) survive.
    with _log_step(f"read {args.sla_source} + union Content SLA NTPJ"):
        jw = read_table(spark, args.sla_source, args.period_start, args.period_end)
        content_ntpj = compute_content_sla_ntpj(jw, args.period_start, args.period_end)
        not_content = ~F.coalesce(
            F.lower(F.col("team")) == F.lit("content"), F.lit(False)
        )
        result = result.filter(not_content).unionByName(content_ntpj)

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        by_gran = {
            r["date_granularity"]: (r["rows"], r["agents"])
            for r in result.groupBy("date_granularity")
            .agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("agent").alias("agents"),
            )
            .collect()
        }
        for g in GRANULARITIES:
            rows, agents = by_gran.get(g, (0, 0))
            print(f"{g:>9}: {rows:,} rows, {agents:,} agents")
        teams = sorted(
            r["team"]
            for r in result.select("team").distinct().collect()
            if r["team"] is not None
        )
        stats = result.agg(
            F.countDistinct("agent").alias("agents"),
            F.min("metric_value").alias("mv_min"),
            F.max("metric_value").alias("mv_max"),
            F.avg("metric_value").alias("mv_mean"),
        ).collect()[0]
        print(f"\nAgents: {stats['agents']:,}   Teams: {', '.join(teams)}")
        if stats["mv_min"] is not None:
            print(
                f"NTPJ metric_value (%): min={stats['mv_min']:.1f}  "
                f"max={stats['mv_max']:.1f}  mean={stats['mv_mean']:.1f}"
            )
        print("\nHead (day grain):")
        result.filter(F.col("date_granularity") == "day").show(10, truncate=False)
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
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

    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
