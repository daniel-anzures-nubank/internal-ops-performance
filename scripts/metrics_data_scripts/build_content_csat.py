"""Build the content_csat dataset and (optionally) write it to Databricks.

Thin orchestrator. The math lives in ``metrics_data/content_csat.py`` and is
covered by ``tests/test_content_csat.py``. Here we only:

  1. Open a Databricks SQL connection (shared `db.open_connection`).
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
::

    uv run python scripts/metrics_data_scripts/build_content_csat.py \\
        --period-start 2026-03-01 --period-end 2026-05-31 --dry-run

    uv run python scripts/metrics_data_scripts/build_content_csat.py \\
        --period-start 2025-12-01 --period-end 2026-05-31
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor, publish  # noqa: E402
from content_csat import IO_CONTENT_CSAT_SCHEMA, compute_content_csat  # noqa: E402

LOGGER = logging.getLogger("cx_metrics.content_csat")

DEFAULT_TARGET = "usr.danielanzures.io_content_csat_raw"


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

    conn = open_connection()
    try:
        with _log_step("agent_information"):
            roster = run_extractor(
                conn, "agent_information", args.period_start, args.period_end
            )
            LOGGER.info("  %s roster rows", f"{len(roster):,}")

        with _log_step("content_csat"):
            csat = run_extractor(
                conn, "content_csat", args.period_start, args.period_end
            )
            LOGGER.info("  %s survey responses", f"{len(csat):,}")

        with _log_step("compute_content_csat"):
            result = compute_content_csat(roster, csat)
            LOGGER.info("  %s rows in final content_csat frame", f"{len(result):,}")

        expected_cols = [c for c, _ in IO_CONTENT_CSAT_SCHEMA]
        result = result[expected_cols]

        if args.dry_run:
            LOGGER.info("Dry run — not writing. Summary:")
            print()
            print(f"Rows (response x agent): {len(result):,}")
            print(f"Distinct responses:      "
                  f"{result[['survey_timestamp', 'requested_by', 'target_squad']].drop_duplicates().shape[0]:,}")
            print(f"Content agents:          {result['agent'].nunique():,}")
            print(f"Target squads:           {result['target_squad'].nunique():,} "
                  f"({', '.join(sorted(result['target_squad'].dropna().unique()))})")
            print(f"Distinct ref dates:      {result['date'].nunique():,}")
            if not result.empty:
                print(
                    f"csat_score: min={result['csat_score'].min():.3f}  "
                    f"max={result['csat_score'].max():.3f}  "
                    f"mean={result['csat_score'].mean():.3f}"
                )
            print()
            print("Head:")
            print(result.head(10).to_string(index=False))
            return 0

        with _log_step(f"write {args.target}"):
            run = publish(
                conn,
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
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
