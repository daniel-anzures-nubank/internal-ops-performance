"""Build the jobs_within_sla raw dataset and (optionally) write it to Databricks.

Thin orchestrator. The math lives in ``metrics_data/jobs_within_sla.py`` and is
covered by ``tests/metrics_data/test_jobs_within_sla.py``. Here we only:

  1. Get the ambient SparkSession (shared ``db.open_connection``).
  2. Run the extractors: ``oos_jobs`` (taskmaster OOS, incl. ``ticket__id``) and
     ``agent_information`` (roster — Content comes from the Google-Sheet roster).
  3. Read the **mandatory** SLA map from the synced ``adj_content_slas`` table
     (the "Content - SLAs" sheet tab) via ``read_adjustment_table`` + ``parse_sla_map``.
  4. Call ``compute_jobs_within_sla`` → one row per Content OOS job with its SLA
     threshold and on-time flag.
  5. Either print a summary (``--dry-run``) or replace the target Delta table.

This is a RAW per-job feed (no aggregation) — it reads only the requested period;
no benchmark look-back (Content NTPJ has no cohort benchmark). The date scoping
(``>= 2025-12-01``, dropping the outage dates) is applied inside the module before
the content_id grouping.

Tables
------
* Inputs:  ``oos_jobs`` + ``agent_information`` extractors;
  ``usr.danielanzures.adj_content_slas`` (the SLA map).
* Output:  ``usr.danielanzures.io_jobs_within_sla_raw`` (override ``--target``).

Depends on ``sync_adjustments`` having synced the "Content - SLAs" tab first.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_data_scripts/build_jobs_within_sla.py \\
        --period-start 2026-03-01 --period-end 2026-03-31 --dry-run

    python scripts/metrics_data_scripts/build_jobs_within_sla.py \\
        --period-start 2025-12-01 --period-end 2026-06-30
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
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor, publish  # noqa: E402
from jobs_within_sla import (  # noqa: E402
    IO_JOBS_WITHIN_SLA_SCHEMA,
    compute_jobs_within_sla,
    parse_sla_map,
)
from adjustments.manual import read_adjustment_table  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.jobs_within_sla")

DEFAULT_TARGET = "usr.danielanzures.io_jobs_within_sla_raw"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
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

    with _log_step("oos_jobs"):
        oos_jobs = run_extractor(spark, "oos_jobs", args.period_start, args.period_end)

    with _log_step("agent_information"):
        roster = run_extractor(
            spark, "agent_information", args.period_start, args.period_end
        )

    with _log_step("adj_content_slas (SLA map)"):
        sla_map = parse_sla_map(read_adjustment_table(spark, "content_slas"))

    with _log_step("compute_jobs_within_sla"):
        result = compute_jobs_within_sla(oos_jobs, roster, sla_map)

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        agg = result.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("agent").alias("agents"),
            F.countDistinct("job_type").alias("job_types"),
            F.sum("within_sla").alias("within"),
        ).collect()[0]
        print(f"Rows (jobs):   {agg['rows']:,}")
        print(f"Agents:        {agg['agents']:,}")
        print(f"Job types:     {agg['job_types']:,}")
        within = agg["within"] or 0
        print(f"within_sla=1:  {int(within):,} / {agg['rows']:,}")
        print("\nHead:")
        result.show(10, truncate=False)
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
            result,
            args.target,
            IO_JOBS_WITHIN_SLA_SCHEMA,
            layer="metrics_data",
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
