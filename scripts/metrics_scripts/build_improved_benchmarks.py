"""Build the Improved Benchmarks metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/improved_benchmarks.py`` and is covered by
``tests/metrics/test_improved_benchmarks.py``. Improved Benchmarks emits the
**``improved_benchmark_xforce`` metric only** (legacy main-deck
``improved_benchmark``), **XForce grain, month only, Core/Fraud only** — this is
the component the composite ``xforce_index`` consumes. It is gated to
``date_reference < 2026-05-01`` plus the ``david.fernandez`` Apr-2026 carve-out.
(The S&D-deck squad/district roll-ups are out of scope — see the module docstring.)

What we do here:
  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Read the inputs:
       * ``io_normalized_time_per_job`` — the NTPJ benchmark substrate, read with
         **one previous month** before the output period (the LAG comparator; the
         benchmark values are already baked in).
       * ``io_occupancy_time_raw``      — 2-month look-back (occupancy benchmark).
       * ``io_ntpj_xforce_metric``      — the gate driver (output period only —
         the gate keys on the emitted month).
  3. Call ``compute_improved_benchmarks`` (emits only the requested months).
  4. Either print a summary (``--dry-run``) or replace the target Delta table.

Tables
------
* Inputs:  ``usr.danielanzures.io_normalized_time_per_job`` (NTPJ benchmark),
  ``usr.danielanzures.io_occupancy_time_raw``,
  ``usr.danielanzures.io_ntpj_xforce_metric`` (the ``ntpj_xforce`` gate).
* Output:  ``usr.danielanzures.io_improved_benchmarks_metric`` (override ``--target``).

Depends on ``build_normalized_time_per_job`` and ``build_ntpj_xforce`` (both
built from the NTPJ chain) — the substrate must cover [output start − 1 month,
output end] and the gate table the output months, first.

Manual adjustments
------------------
The NTPJ-side adjustments (``exclusiones_generales`` slot windows,
``cross_support`` queue exclusions, ``exclusiones_jobs``) are applied **upstream**
in ``build_normalized_time_per_job``. Here we apply only the occupancy-side
adjustments: ``exclusiones_generales`` (slot/date windows) and
``inconsistencias_dime`` (DIME reclassification), read from their synced ``adj_*``
Delta tables if present.

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

from db import (  # noqa: E402
    open_connection,
    read_table,
    publish,
    resolve_period_end,
    MAX_DIME_SENTINEL,
)
from improved_benchmarks import (  # noqa: E402
    IO_IMPROVED_BENCHMARKS_METRIC_SCHEMA,
    compute_improved_benchmarks,
)
from adjustments.manual import read_adjustment_table  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.improved_benchmarks")

DEFAULT_NTPJ_SOURCE = "usr.danielanzures.io_normalized_time_per_job"
DEFAULT_OCC_SOURCE = "usr.danielanzures.io_occupancy_time_raw"
DEFAULT_NTPJ_XFORCE_SOURCE = "usr.danielanzures.io_ntpj_xforce_metric"
DEFAULT_TARGET = "usr.danielanzures.io_improved_benchmarks_metric"

# The NTPJ substrate already carries the trailing-window benchmark VALUES; we
# only need one PREVIOUS month before period_start for the month-over-month LAG.
NTPJ_LOOKBACK_MONTHS = 1
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
    parser.add_argument(
        "--period-end",
        required=True,
        help=f"ISO date (YYYY-MM-DD), or '{MAX_DIME_SENTINEL}' to resolve to "
        "the max ingested DIME date at run time.",
    )
    parser.add_argument(
        "--ntpj-source",
        default=DEFAULT_NTPJ_SOURCE,
        help="NTPJ benchmark substrate (normalized_time_per_job) "
        f"(default: {DEFAULT_NTPJ_SOURCE}).",
    )
    parser.add_argument("--occupancy-source", default=DEFAULT_OCC_SOURCE)
    parser.add_argument(
        "--ntpj-xforce-source",
        default=DEFAULT_NTPJ_XFORCE_SOURCE,
        help="ntpj_xforce metric table gating the benchmark units "
        f"(default: {DEFAULT_NTPJ_XFORCE_SOURCE}).",
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

    # The NTPJ substrate needs one PREVIOUS month before period_start for the
    # month-over-month LAG; the benchmark VALUES are already baked in.
    ntpj_start = _lookback_start(args.period_start, NTPJ_LOOKBACK_MONTHS)
    occ_start = _lookback_start(args.period_start, OCC_LOOKBACK_MONTHS)

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

    with _log_step(f"read {args.ntpj_source} (from {ntpj_start})"):
        ntpj = read_table(
            spark, args.ntpj_source, ntpj_start, args.period_end,
            date_col="benchmark_month",
        )

    with _log_step(f"read {args.occupancy_source} (from {occ_start})"):
        occ = read_table(spark, args.occupancy_source, occ_start, args.period_end)

    # ntpj_xforce gate: only the OUTPUT months' rows are needed (the gate keys on
    # the emitted month), so scope on the output period, not the look-back.
    with _log_step(f"read {args.ntpj_xforce_source}"):
        ntpj_xforce = read_table(
            spark, args.ntpj_xforce_source, args.period_start, args.period_end,
            date_col="date_reference",
        )

    with _log_step("compute_improved_benchmarks"):
        result = compute_improved_benchmarks(
            ntpj,
            occ,
            args.period_start,
            args.period_end,
            ntpj_xforce=ntpj_xforce,
            general_exclusions=read_adjustment_table(spark, "exclusiones_generales"),
            dime_inconsistencies=read_adjustment_table(spark, "inconsistencias_dime"),
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
