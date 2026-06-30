"""Build the composite XForce Index metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/xforce_index.py`` and is covered by
``tests/metrics/test_xforce_index.py``. Here we only:

  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Read the component metric tables for the period (via ``db.read_table``,
     scoped on ``date_reference``).
  3. Call ``compute_xforce_index`` to combine them (per deck; week + month only
     before the 2026-07-01 cutover).
  4. Either print a summary (``--dry-run``) or replace the target Delta table.

Tables
------
* Inputs (override with the matching flag):
    - ``usr.danielanzures.io_shrinkage_metric``               (driver, agent grain)
    - ``usr.danielanzures.io_xpeers_in_target_metric``
    - ``usr.danielanzures.io_average_xpeer_index_metric``
    - ``usr.danielanzures.io_improved_benchmarks_metric``     (xforce roll-up)
* Output: ``usr.danielanzures.io_xforce_index_metric`` (override ``--target``).

``io_improved_benchmarks_metric`` is currently DEFERRED (not yet at parity). It
may be **absent or empty**; the build degrades gracefully — the improved
component folds to 0 and the 4th-component count still follows legacy's date
rule. Until it lands, this metric cannot be parity-validated on the cluster.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_xforce_index.py \\
        --period-start 2026-05-01 --period-end 2026-05-31 --dry-run

    python scripts/metrics_scripts/build_xforce_index.py \\
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

from db import open_connection, read_table, publish  # noqa: E402
from metric_utils import GRANULARITIES  # noqa: E402
from xforce_index import (  # noqa: E402
    IO_XFORCE_INDEX_METRIC_SCHEMA,
    compute_xforce_index,
)

LOGGER = logging.getLogger("cx_metrics.xforce_index")

# (flag suffix, default table, compute kwarg, optional?)
# improved-benchmarks is OPTIONAL: the table may not exist yet (deferred). A
# missing/empty improved input is handled by compute_xforce_index.
INPUTS: tuple[tuple[str, str, str, bool], ...] = (
    ("shrinkage", "usr.danielanzures.io_shrinkage_metric", "shrinkage", False),
    (
        "xpeers-in-target",
        "usr.danielanzures.io_xpeers_in_target_metric",
        "xpeers_in_target",
        False,
    ),
    (
        "average-xpeer-index",
        "usr.danielanzures.io_average_xpeer_index_metric",
        "average_xpeer_index",
        False,
    ),
    (
        "improved-benchmarks",
        "usr.danielanzures.io_improved_benchmarks_metric",
        "improved_benchmarks",
        True,
    ),
)

DEFAULT_TARGET = "usr.danielanzures.io_xforce_index_metric"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
    for flag, default, _, optional in INPUTS:
        suffix = " (optional — may not exist yet)" if optional else ""
        parser.add_argument(
            f"--{flag}-source",
            default=default,
            help=f"Input metric table (default: {default}).{suffix}",
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


def _read_optional(spark, source, period_start, period_end):
    """Read an optional source table; return ``None`` if it doesn't exist.

    The improved_benchmarks table is deferred and may not be built yet. We treat
    a missing table (AnalysisException / table-not-found) as ``None`` so the
    build still produces a (3-component-numerator) result rather than failing.
    """
    try:
        return read_table(
            spark, source, period_start, period_end, date_col="date_reference"
        )
    except Exception as exc:  # noqa: BLE001 - any read failure -> treat as absent
        LOGGER.warning(
            "Optional source %s unavailable (%s); proceeding without it.",
            source,
            type(exc).__name__,
        )
        return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(levelname)s %(name)s: %(message)s"
    )

    if args.period_end < args.period_start:
        LOGGER.error("--period-end must be >= --period-start")
        return 2

    spark = open_connection()

    frames: dict[str, object] = {}
    for flag, _, kwarg, optional in INPUTS:
        source = getattr(args, f"{flag.replace('-', '_')}_source")
        with _log_step(f"read {source}"):
            if optional:
                frames[kwarg] = _read_optional(
                    spark, source, args.period_start, args.period_end
                )
            else:
                frames[kwarg] = read_table(
                    spark,
                    source,
                    args.period_start,
                    args.period_end,
                    date_col="date_reference",
                )

    with _log_step("compute_xforce_index"):
        result = compute_xforce_index(**frames)

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        by_gran = {
            r["date_granularity"]: (r["rows"], r["xforces"])
            for r in result.groupBy("date_granularity")
            .agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("xforce").alias("xforces"),
            )
            .collect()
        }
        for g in GRANULARITIES:
            rows, xforces = by_gran.get(g, (0, 0))
            print(f"{g:>9}: {rows:,} rows, {xforces:,} xforces")

        # Component-count split (3 vs 4) at month grain.
        mo = result.filter(F.col("date_granularity") == "month")
        four = mo.filter(F.col("denominator") == 400).count()
        total = mo.count()
        print(
            f"\nmonth rows: {total:,}  "
            f"(4-component: {four:,}, 3-component: {total - four:,})"
        )

        stats = result.agg(
            F.min("metric_value").alias("mv_min"),
            F.max("metric_value").alias("mv_max"),
            F.avg("metric_value").alias("mv_mean"),
        ).collect()[0]
        if stats["mv_min"] is not None:
            print(
                f"metric_value: min={stats['mv_min']:.1f}  "
                f"max={stats['mv_max']:.1f}  mean={stats['mv_mean']:.1f}"
            )
        print("\nHead (month grain):")
        mo.show(10, truncate=False)
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
            result,
            args.target,
            IO_XFORCE_INDEX_METRIC_SCHEMA,
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
