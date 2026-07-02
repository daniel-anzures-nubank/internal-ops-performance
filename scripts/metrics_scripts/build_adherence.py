"""Build the Adherence metric and (optionally) write it to Databricks.

First script of the **metrics layer**. Thin orchestrator — the math lives in
``metrics/adherence.py`` and is covered by ``tests/metrics/test_adherence.py``.
Here we only:

  1. Get the ambient SparkSession (shared `db.open_connection`).
  2. Read the raw input table ``io_adherent_time_raw`` for the period
     (via `db.read_table`, not an extractor — the metrics layer reads the
     already-built `io_*_raw` tables).
  3. Call ``compute_adherence`` to get day/week/month metric rows.
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

Tables
------
* Input:  ``usr.danielanzures.io_adherent_time_raw`` (override `--source`).
* Output: ``usr.danielanzures.io_adherence_metric`` (override `--target`).

Manual adjustments
------------------
``excluziones_generales`` (slot windows to drop) and ``inconsistencias_dime``
(slot relabels) are read from their synced ``adj_*`` Delta tables, if present.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_adherence.py \\
        --period-start 2026-05-01 --period-end 2026-05-24 --dry-run

    python scripts/metrics_scripts/build_adherence.py \\
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

from db import (  # noqa: E402
    open_connection,
    read_table,
    publish,
    resolve_period_end,
    MAX_DIME_SENTINEL,
)
from metric_utils import GRANULARITIES  # noqa: E402
from adherence import IO_ADHERENCE_METRIC_SCHEMA, compute_adherence  # noqa: E402
from adjustments.manual import read_adjustment_table  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.adherence")

DEFAULT_SOURCE = "usr.danielanzures.io_adherent_time_raw"
DEFAULT_TARGET = "usr.danielanzures.io_adherence_metric"


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
        adherent_time = read_table(
            spark, args.source, args.period_start, args.period_end
        )

    with _log_step("compute_adherence"):
        result = compute_adherence(
            adherent_time,
            general_exclusions=read_adjustment_table(spark, "exclusiones_generales"),
            dime_inconsistencies=read_adjustment_table(spark, "inconsistencias_dime"),
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
            F.min("metric_value").alias("mn"),
            F.max("metric_value").alias("mx"),
            F.avg("metric_value").alias("mean"),
        ).collect()[0]
        print(f"\nAgents: {stats['agents']:,}   Teams: {', '.join(teams)}")
        if stats["mn"] is not None:
            print(
                f"metric_value (%): min={stats['mn']:.1f}  "
                f"max={stats['mx']:.1f}  mean={stats['mean']:.1f}"
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
            IO_ADHERENCE_METRIC_SCHEMA,
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
