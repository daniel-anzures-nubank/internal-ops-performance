"""Build the Content Quality (CSAT) metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/content_csat_metric.py`` and is covered by
``tests/metrics/test_content_csat_metric.py``. Here we only:

  1. Get the ambient SparkSession (shared `db.open_connection`).
  2. Read the raw input table ``io_content_csat_raw`` for the period
     (via `db.read_table`).
  3. Call ``compute_content_csat`` to get the metric rows at all granularities.
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

CSAT only applies to **Content** (Core / Fraud / Social Media use the Playvox
``quality`` metric instead).

Tables
------
* Input:  ``usr.danielanzures.io_content_csat_raw`` (override `--source`).
* Output: ``usr.danielanzures.io_content_csat_metric`` (override `--target`).

February seed
-------------
Legacy carries a seeded February (the CSAT survey sheet has no February
responses, so the raw/compute path can never produce it). ``--feb-seed``
(``usr.danielanzures.content_csat_feb_2026``, 17 content agents at month grain)
is unioned into the metric, scoped to the run window, to reproduce legacy's
February month rows.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_content_csat.py \\
        --period-start 2026-05-01 --period-end 2026-05-24 --dry-run

    python scripts/metrics_scripts/build_content_csat.py \\
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

from db import open_connection, read_table, publish  # noqa: E402
from metric_utils import GRANULARITIES  # noqa: E402
from content_csat_metric import (  # noqa: E402
    IO_CONTENT_CSAT_METRIC_SCHEMA,
    compute_content_csat,
)

LOGGER = logging.getLogger("cx_metrics.content_csat")

DEFAULT_SOURCE = "usr.danielanzures.io_content_csat_raw"
DEFAULT_TARGET = "usr.danielanzures.io_content_csat_metric"
# Static February seed: legacy `qa_score_agent` carries a seeded February (the
# source CSAT sheet has no February responses). Materialized to this table and
# unioned into the metric below so the output reproduces legacy's February rows.
DEFAULT_FEB_SEED = "usr.danielanzures.content_csat_feb_2026"


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
        "--feb-seed",
        default=DEFAULT_FEB_SEED,
        help="Static February seed table to union into the metric "
        f"(default: {DEFAULT_FEB_SEED}; pass '' to disable).",
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


def _append_feb_seed(spark, result, feb_table, period_start, period_end):
    """Union the static February seed into the metric, scoped to the window.

    Legacy ``qa_score_agent`` carries a seeded February (17 content agents at
    month grain) even though the CSAT survey sheet has no February responses, so
    the raw/compute path can never produce it. We reproduce it from
    ``content_csat_feb_2026``. Missing / empty / out-of-window -> no-op (never
    fails the build).
    """
    from pyspark.sql import functions as F

    if not feb_table:
        return result
    try:
        seed = spark.table(feb_table)
    except Exception as exc:  # table absent / unreadable -> skip
        LOGGER.warning("  February seed %s unavailable (%s) — skipping", feb_table, exc)
        return result
    seed = seed.filter(
        (F.col("date_reference") >= F.lit(period_start))
        & (F.col("date_reference") <= F.lit(period_end))
    ).select(*[c for c, _ in IO_CONTENT_CSAT_METRIC_SCHEMA])
    if len(seed.take(1)) == 0:
        return result
    LOGGER.info("  unioning February seed from %s", feb_table)
    return result.unionByName(seed)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(levelname)s %(name)s: %(message)s"
    )

    if args.period_end < args.period_start:
        LOGGER.error("--period-end must be >= --period-start")
        return 2

    spark = open_connection()

    with _log_step(f"read {args.source}"):
        csat = read_table(spark, args.source, args.period_start, args.period_end)

    with _log_step("compute_content_csat"):
        result = compute_content_csat(csat)

    with _log_step("append February seed"):
        result = _append_feb_seed(
            spark, result, args.feb_seed, args.period_start, args.period_end
        )

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
                f"metric_value (%): min={stats['mv_min']:.1f}  "
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
            IO_CONTENT_CSAT_METRIC_SCHEMA,
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
