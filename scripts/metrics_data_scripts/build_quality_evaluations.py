"""Build the quality_evaluations dataset and (optionally) write it to Databricks.

Thin orchestrator. The math lives in ``metrics_data/quality_evaluations.py``
and is covered by ``tests/test_quality_evaluations.py``. Here we only:

  1. Open a Databricks SQL connection (shared `db.open_connection`).
  2. Run the three extractors needed:
       * ``agent_information``        (roster)
       * ``playvox_evaluations``      (Playvox QA evaluations)
       * ``sprinklr_sm_evaluations``  (Sprinklr Social-Media case QA, >= 2026-05-01)
  3. Call ``compute_quality_evaluations`` to get one row per evaluation
     (Playvox UNION ALL Sprinklr SM).
  4. Either print a summary (`--dry-run`) or replace the target Delta
     table.

Target table
------------
Default: ``usr.danielanzures.io_quality_evaluations_raw``. Override with ``--target``.

Manual adjustments
------------------
None are applied here. The Google-Sheets-driven adjustments layer is a
follow-up; this script intentionally produces the "clean" baseline so
the two can be diffed.

Usage
-----
::

    uv run python scripts/metrics_data_scripts/build_quality_evaluations.py \\
        --period-start 2026-05-01 --period-end 2026-05-17 --dry-run

    uv run python scripts/metrics_data_scripts/build_quality_evaluations.py \\
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
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor, publish  # noqa: E402
from quality_evaluations import (  # noqa: E402
    IO_QUALITY_EVALUATIONS_SCHEMA,
    compute_quality_evaluations,
)

LOGGER = logging.getLogger("cx_metrics.quality_evaluations")

DEFAULT_TARGET = "usr.danielanzures.io_quality_evaluations_raw"


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

    with _log_step("agent_information"):
        roster = run_extractor(
            spark, "agent_information", args.period_start, args.period_end
        )

    with _log_step("playvox_evaluations"):
        playvox = run_extractor(
            spark, "playvox_evaluations", args.period_start, args.period_end
        )

    with _log_step("sprinklr_sm_evaluations"):
        sprinklr_sm = run_extractor(
            spark, "sprinklr_sm_evaluations", args.period_start, args.period_end
        )

    with _log_step("compute_quality_evaluations"):
        result = compute_quality_evaluations(roster, playvox, sprinklr_sm)

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        agg = result.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("agent").alias("agents"),
            F.countDistinct("date").alias("dates"),
            F.avg("qa_score").alias("qa_mean"),
            F.min("qa_score").alias("qa_min"),
            F.max("qa_score").alias("qa_max"),
        ).collect()[0]
        print(f"Rows (evaluations): {agg['rows']:,}")
        by_source = {
            r["source"]: r["cnt"]
            for r in result.groupBy("source")
            .agg(F.count(F.lit(1)).alias("cnt"))
            .collect()
        }
        if by_source:
            print(
                "By source:   "
                + ", ".join(f"{k}={v:,}" for k, v in sorted(by_source.items()))
            )
        print(f"Agents:      {agg['agents']:,}")
        print(f"Dates:       {agg['dates']:,}")
        squads = sorted(
            r["squad"]
            for r in result.select("squad").distinct().collect()
            if r["squad"] is not None
        )
        print(f"Squads:      {len(squads):,} ({', '.join(squads)})")
        if agg["qa_mean"] is not None:
            print(
                f"qa_score:    mean={agg['qa_mean']:.3f}  "
                f"min={agg['qa_min']:.3f}  max={agg['qa_max']:.3f}"
            )
        print()
        print("Head:")
        result.show(10, truncate=False)
        result.unpersist()
        return 0

    with _log_step(f"write {args.target}"):
        run = publish(
            spark,
            result,
            args.target,
            IO_QUALITY_EVALUATIONS_SCHEMA,
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
