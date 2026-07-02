"""Build the agent-level Xpeer Index metric and (optionally) write it to Databricks.

Metrics-layer script. Thin orchestrator — the math lives in
``metrics/xpeer_index.py`` and is covered by
``tests/metrics/test_xpeer_index.py``. Here we only:

  1. Get the ambient SparkSession (shared `db.open_connection`).
  2. Read the seven per-agent metric tables for the period (via `db.read_table`,
     scoped on ``date_reference``).
  3. Call ``compute_xpeer_index`` to combine them into the Index. Pre-cutover
     (``date_reference < 2026-07-01``) only the week + month grain is emitted
     (legacy unions index_agents_weekly + index_agents_monthly).
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

Because the Index composition is **month-anchored** (NO from March 2026,
Quality from Feb, etc.), prefer running with whole-month periods.

Tables
------
* Inputs (override with the matching flag):
    - ``usr.danielanzures.io_adherence_metric``              (driver)
    - ``usr.danielanzures.io_ntpj_metric``
    - ``usr.danielanzures.io_normalized_occupancy_metric``
    - ``usr.danielanzures.io_quality_metric``
    - ``usr.danielanzures.io_tnps_metric``
    - ``usr.danielanzures.io_wows_metric``
    - ``usr.danielanzures.io_content_csat_metric``
* Output: ``usr.danielanzures.io_xpeer_index_metric`` (override `--target`).

Manual adjustments
------------------
None read from sheets. The approved ``nitza.zarza`` Apr-May 2026 NO carve-out is
a hardcoded, era-gated constant applied inside ``compute_xpeer_index``.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_scripts/build_xpeer_index.py \\
        --period-start 2026-05-01 --period-end 2026-05-31 --dry-run

    python scripts/metrics_scripts/build_xpeer_index.py \\
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
from xpeer_index import (  # noqa: E402
    IO_XPEER_INDEX_METRIC_SCHEMA,
    compute_xpeer_index,
)

LOGGER = logging.getLogger("cx_metrics.xpeer_index")

# (flag suffix, default table, compute kwarg)
INPUTS: tuple[tuple[str, str, str], ...] = (
    ("adherence", "usr.danielanzures.io_adherence_metric", "adherence"),
    ("ntpj", "usr.danielanzures.io_ntpj_metric", "ntpj"),
    (
        "normalized-occupancy",
        "usr.danielanzures.io_normalized_occupancy_metric",
        "normalized_occupancy",
    ),
    ("quality", "usr.danielanzures.io_quality_metric", "quality"),
    ("tnps", "usr.danielanzures.io_tnps_metric", "tnps"),
    ("wows", "usr.danielanzures.io_wows_metric", "wows"),
    ("content-csat", "usr.danielanzures.io_content_csat_metric", "content_csat"),
)

DEFAULT_TARGET = "usr.danielanzures.io_xpeer_index_metric"


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
    for flag, default, _ in INPUTS:
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

    frames: dict[str, object] = {}
    for flag, _, kwarg in INPUTS:
        source = getattr(args, f"{flag.replace('-', '_')}_source")
        with _log_step(f"read {source}"):
            frames[kwarg] = read_table(
                spark,
                source,
                args.period_start,
                args.period_end,
                date_col="date_reference",
            )

    with _log_step("compute_xpeer_index"):
        result = compute_xpeer_index(**frames)

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
                f"Index (%): min={stats['mv_min']:.1f}  "
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
            IO_XPEER_INDEX_METRIC_SCHEMA,
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
