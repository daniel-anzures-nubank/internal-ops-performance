"""Build the consolidated performance-metrics table and (optionally) write it.

Metrics-layer script. Thin orchestrator — the union + display-team cascade
lives in ``metrics/performance_metrics.py`` and is covered by
``tests/metrics/test_performance_metrics.py``. Here we only:

  1. Get the ambient SparkSession (shared `db.open_connection`).
  2. Read the 16 finished metric tables for the period (via `db.read_table`,
     scoped on ``date_reference``).
  3. Call ``compute_performance_metrics`` to UNION ALL them, replacing ``team``
     with the display team (Core / Fraud / Social Media / Content / Quality;
     modal backfill from adherence for the roll-up rows).
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

Tables
------
* Inputs (override with the matching flag): the 16
  ``usr.danielanzures.io_*_metric`` tables — adherence (also the modal-dim
  driver), ntpj, normalized_occupancy, quality, shrinkage, tnps, wows,
  content_csat, ntpj_xforce, improved_benchmarks, xpeer_index,
  nuvinhos_performance, xpeers_in_target, average_xpeer_index, xforce_index,
  average_xforce_index.
* Output: ``usr.danielanzures.io_performance_metrics`` (override `--target`).

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_performance_metrics.py \\
        --period-start 2026-01-01 --period-end 2026-06-30 --dry-run

    python scripts/metrics_scripts/build_performance_metrics.py \\
        --period-start 2026-01-01 --period-end 2026-06-30
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
from performance_metrics import (  # noqa: E402
    IO_PERFORMANCE_METRICS_SCHEMA,
    compute_performance_metrics,
)

LOGGER = logging.getLogger("cx_metrics.performance_metrics")

# (flag suffix, default table). Adherence first — it is also the driver of the
# modal display-team dims; the other 15 are unioned as-is.
INPUTS: tuple[tuple[str, str], ...] = (
    ("adherence", "usr.danielanzures.io_adherence_metric"),
    ("ntpj", "usr.danielanzures.io_ntpj_metric"),
    (
        "normalized-occupancy",
        "usr.danielanzures.io_normalized_occupancy_metric",
    ),
    ("quality", "usr.danielanzures.io_quality_metric"),
    ("shrinkage", "usr.danielanzures.io_shrinkage_metric"),
    ("tnps", "usr.danielanzures.io_tnps_metric"),
    ("wows", "usr.danielanzures.io_wows_metric"),
    ("content-csat", "usr.danielanzures.io_content_csat_metric"),
    ("ntpj-xforce", "usr.danielanzures.io_ntpj_xforce_metric"),
    (
        "improved-benchmarks",
        "usr.danielanzures.io_improved_benchmarks_metric",
    ),
    ("xpeer-index", "usr.danielanzures.io_xpeer_index_metric"),
    (
        "nuvinhos-performance",
        "usr.danielanzures.io_nuvinhos_performance_metric",
    ),
    ("xpeers-in-target", "usr.danielanzures.io_xpeers_in_target_metric"),
    (
        "average-xpeer-index",
        "usr.danielanzures.io_average_xpeer_index_metric",
    ),
    ("xforce-index", "usr.danielanzures.io_xforce_index_metric"),
    (
        "average-xforce-index",
        "usr.danielanzures.io_average_xforce_index_metric",
    ),
)

DEFAULT_TARGET = "usr.danielanzures.io_performance_metrics"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
    for flag, default in INPUTS:
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

    spark = open_connection()

    frames = []
    for flag, _ in INPUTS:
        source = getattr(args, f"{flag.replace('-', '_')}_source")
        with _log_step(f"read {source}"):
            frames.append(
                read_table(
                    spark,
                    source,
                    args.period_start,
                    args.period_end,
                    date_col="date_reference",
                )
            )

    with _log_step("compute_performance_metrics"):
        result = compute_performance_metrics(frames[0], frames[1:])

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        for r in (
            result.groupBy("metric")
            .agg(F.count(F.lit(1)).alias("rows"))
            .orderBy("metric")
            .collect()
        ):
            print(f"{r['metric']:>32}: {r['rows']:,} rows")
        teams = sorted(
            r["team"]
            for r in result.select("team").distinct().collect()
            if r["team"] is not None
        )
        null_team = result.filter(F.col("team").isNull()).count()
        print(f"\nTotal: {result.count():,} rows   Teams: {', '.join(teams)}")
        print(f"NULL display team: {null_team:,} rows")
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
            result,
            args.target,
            IO_PERFORMANCE_METRICS_SCHEMA,
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
