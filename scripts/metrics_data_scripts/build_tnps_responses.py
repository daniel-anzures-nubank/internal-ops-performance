"""Build the tnps_responses dataset and (optionally) write it to Databricks.

Thin orchestrator. The math lives in ``metrics_data/tnps_responses.py`` and is
covered by ``tests/test_tnps_responses.py``. Here we only:

  1. Open a Databricks SQL connection (shared `db.open_connection`).
  2. Run the two extractors needed:
       * ``agent_information``  (roster)
       * ``tnps_responses``     (Sprinklr Social-Media tNPS survey responses)
  3. Call ``compute_tnps_responses`` to get one row per survey response.
  4. Either print a summary (`--dry-run`) or replace the target Delta table.

tNPS only applies to Social Media; the source only contains surveys attributable
to human social agents.

Target table
------------
Default: ``usr.danielanzures.io_tnps_responses_raw``. Override with ``--target``.

Manual adjustments
------------------
None are applied here. The Google-Sheets-driven adjustments layer is a
follow-up; this script intentionally produces the "clean" baseline.

Usage
-----
::

    uv run python scripts/metrics_data_scripts/build_tnps_responses.py \\
        --period-start 2026-05-01 --period-end 2026-05-17 --dry-run

    uv run python scripts/metrics_data_scripts/build_tnps_responses.py \\
        --period-start 2026-01-01 --period-end 2026-05-24
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "metrics_data"))

from db import open_connection, run_extractor, publish  # noqa: E402
from tnps_responses import (  # noqa: E402
    IO_TNPS_RESPONSES_SCHEMA,
    compute_tnps_responses,
)

LOGGER = logging.getLogger("cx_metrics.tnps_responses")

DEFAULT_TARGET = "usr.danielanzures.io_tnps_responses_raw"


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

        with _log_step("tnps_responses"):
            tnps = run_extractor(
                conn, "tnps_responses", args.period_start, args.period_end
            )
            LOGGER.info("  %s tNPS survey rows", f"{len(tnps):,}")

        with _log_step("compute_tnps_responses"):
            result = compute_tnps_responses(roster, tnps)
            LOGGER.info("  %s rows in final tnps_responses frame", f"{len(result):,}")

        expected_cols = [c for c, _ in IO_TNPS_RESPONSES_SCHEMA]
        result = result[expected_cols]

        if args.dry_run:
            LOGGER.info("Dry run — not writing. Summary:")
            print()
            print(f"Rows (responses): {len(result):,}")
            print(f"Agents:      {result['agent'].nunique():,}")
            print(f"Dates:       {result['date'].nunique():,}")
            print(
                f"Squads:      {result['squad'].nunique():,} "
                f"({', '.join(sorted(result['squad'].dropna().unique()))})"
            )
            if not result.empty:
                score = pd.to_numeric(result["survey_score"], errors="coerce")
                valid = int(score.notna().sum())
                promoters = int((score >= 9).sum())
                detractors = int((score <= 6).sum())
                print(
                    f"survey_score: valid={valid:,}  "
                    f"promoters(>=9)={promoters:,}  detractors(<=6)={detractors:,}  "
                    f"mean={score.mean():.2f}"
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
                IO_TNPS_RESPONSES_SCHEMA,
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
