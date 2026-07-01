"""Build the content_csat dataset and (optionally) write it to Databricks.

Thin orchestrator. The math lives in ``metrics_data/content_csat.py`` and is
covered by ``tests/metrics_data/test_content_csat.py``. Here we only:

  1. Get the ambient SparkSession (shared `db.open_connection`).
  2. Run the two extractors needed:
       * ``agent_information``  (roster, incl. content `target_squad`)
       * ``content_csat``       (Content CSAT survey responses)
  3. Call ``compute_content_csat`` to fan each response out to the content agents
     serving that `target_squad`.
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

Content CSAT only applies to Content; the source sheet only covers content.

Target table
------------
Default: ``usr.danielanzures.io_content_csat_raw``. Override with ``--target``.

Manual adjustments
------------------
None are applied here. Per-agent aggregation and any adjustments are deferred to
the metrics layer; this script produces the raw per-response (× agent) baseline.

Usage
-----
Runs on a Databricks cluster (``spark-submit`` / a Databricks job task)::

    python scripts/metrics_data_scripts/build_content_csat.py \\
        --period-start 2026-03-01 --period-end 2026-05-31 --dry-run

    python scripts/metrics_data_scripts/build_content_csat.py \\
        --period-start 2025-12-01 --period-end 2026-05-31
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

from db import (  # noqa: E402
    open_connection,
    run_extractor,
    publish,
    resolve_period_end,
    MAX_DIME_SENTINEL,
)
from content_csat import (  # noqa: E402
    IO_CONTENT_CSAT_SCHEMA,
    compute_content_csat,
)

LOGGER = logging.getLogger("cx_metrics.content_csat")

DEFAULT_TARGET = "usr.danielanzures.io_content_csat_raw"


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

    with _log_step("agent_information"):
        roster = run_extractor(
            spark, "agent_information", args.period_start, args.period_end
        )

    with _log_step("content_csat"):
        csat = run_extractor(
            spark, "content_csat", args.period_start, args.period_end
        )

    with _log_step("compute_content_csat"):
        result = compute_content_csat(roster, csat)

    if args.dry_run:
        from pyspark.sql import functions as F

        result = result.persist()
        LOGGER.info("Dry run — not writing. Summary:")
        print()
        agg = result.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("agent").alias("agents"),
            F.countDistinct("date").alias("dates"),
            F.min("csat_score").alias("score_min"),
            F.max("csat_score").alias("score_max"),
            F.avg("csat_score").alias("score_mean"),
        ).collect()[0]
        distinct_responses = (
            result.select("survey_timestamp", "requested_by", "target_squad")
            .distinct()
            .count()
        )
        print(f"Rows (response x agent): {agg['rows']:,}")
        print(f"Distinct responses:      {distinct_responses:,}")
        print(f"Content agents:          {agg['agents']:,}")
        print(f"Distinct ref dates:      {agg['dates']:,}")
        squads = sorted(
            r["target_squad"]
            for r in result.select("target_squad").distinct().collect()
            if r["target_squad"] is not None
        )
        print(f"Target squads:           {len(squads):,} ({', '.join(squads)})")
        if agg["score_min"] is not None:
            print(
                f"csat_score: min={agg['score_min']:.3f}  "
                f"max={agg['score_max']:.3f}  mean={agg['score_mean']:.3f}"
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
            IO_CONTENT_CSAT_SCHEMA,
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
