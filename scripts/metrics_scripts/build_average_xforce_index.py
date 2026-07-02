"""Build the Average XForce Index metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/average_xforce_index.py`` and is covered by
``tests/metrics/test_average_xforce_index.py``. Here we only:

  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Read the XForce-level XForce Index for the period (via ``db.read_table``,
     scoped on ``date_reference``).
  3. Call ``compute_average_xforce_index`` to average to the XPLead level (per
     deck; week + month only before the 2026-07-01 cutover).
  4. Either print a summary (``--dry-run``) or replace the target Delta table.

Tables
------
* Input:  ``usr.danielanzures.io_xforce_index_metric`` (override ``--source``).
* Output: ``usr.danielanzures.io_average_xforce_index_metric`` (override ``--target``).

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_average_xforce_index.py \\
        --period-start 2026-05-01 --period-end 2026-05-31 --dry-run

    python scripts/metrics_scripts/build_average_xforce_index.py \\
        --period-start 2026-01-01 --period-end 2026-05-31
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

from db import (  # noqa: E402
    open_connection,
    read_table,
    publish,
    resolve_period_end,
    MAX_DIME_SENTINEL,
)
from metric_utils import GRANULARITIES  # noqa: E402
from average_xforce_index import (  # noqa: E402
    IO_AVERAGE_XFORCE_INDEX_METRIC_SCHEMA,
    compute_average_xforce_index,
)

LOGGER = logging.getLogger("cx_metrics.average_xforce_index")

DEFAULT_SOURCE = "usr.danielanzures.io_xforce_index_metric"
DEFAULT_TARGET = "usr.danielanzures.io_average_xforce_index_metric"


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
        "--source",
        default=DEFAULT_SOURCE,
        help=f"XForce-level XForce Index table (default: {DEFAULT_SOURCE}).",
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

    with _log_step(f"read {args.source}"):
        idx = read_table(
            spark, args.source, args.period_start, args.period_end,
            date_col="date_reference",
        )

    with _log_step("compute_average_xforce_index"):
        result = compute_average_xforce_index(idx)

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        by_gran = {
            r["date_granularity"]: (r["rows"], r["xpleads"])
            for r in result.groupBy("date_granularity")
            .agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("xplead").alias("xpleads"),
            )
            .collect()
        }
        for g in GRANULARITIES:
            rows, xpleads = by_gran.get(g, (0, 0))
            print(f"{g:>9}: {rows:,} rows, {xpleads:,} xpleads")
        stats = result.agg(
            F.min("metric_value").alias("mv_min"),
            F.max("metric_value").alias("mv_max"),
            F.avg("metric_value").alias("mv_mean"),
        ).collect()[0]
        if stats["mv_min"] is not None:
            print(
                f"\nmetric_value: min={stats['mv_min']:.1f}  "
                f"max={stats['mv_max']:.1f}  mean={stats['mv_mean']:.1f}"
            )
        print("\nHead (month grain):")
        result.filter(F.col("date_granularity") == "month").show(10, truncate=False)
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
            result,
            args.target,
            IO_AVERAGE_XFORCE_INDEX_METRIC_SCHEMA,
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
