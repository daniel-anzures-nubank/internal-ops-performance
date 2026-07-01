"""Run extractor data-quality checks against the warehouse.

Thin orchestrator. The actual check logic lives in ``tests/checks.py`` —
that module is pure pandas and covered by unit tests in ``tests/test_checks.py``.
This script only adds:

  * Transport (``db.open_connection()`` → SparkSession) — how to get a DataFrame.
  * CLI (argparse) — which extractor(s) to run, for what period.
  * Reporting — pretty-printed pass/fail summary.

The extractors read Unity Catalog tables, so the SparkSession needs UC access —
in practice, run this on Databricks. A *local* SparkSession cannot resolve the
``etl.*`` / ``usr.*`` / ``gsheets.*`` catalogs; for ad-hoc local validation, run
the extractor SQL through the ``databricks-sql`` MCP instead.

Usage
-----
::

    python scripts/check_extractor_data_quality.py \\
        --period-start 2026-04-15 --period-end 2026-04-21

    # Filter to a subset:
    python scripts/check_extractor_data_quality.py \\
        --period-start 2026-04-15 --period-end 2026-04-21 \\
        --extractors dime_slots productivity

Exit codes
----------
* 0 — all checks pass (or only WARN-level failures, unless ``--fail-on-warn``).
* 1 — at least one ERROR-level check failed.
* 2 — bad invocation (e.g. unknown extractor name).
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Iterable

# Make sibling top-level modules (`db.py`, `tests/checks.py`) importable.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "tests"))

from db import open_connection, run_extractor  # noqa: E402
from checks import (  # noqa: E402
    EXTRACTOR_SPECS,
    CheckResult,
    ExtractorSpec,
    run_checks_for_extractor,
)

LOGGER = logging.getLogger("cx_metrics.dq")


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_report(all_results: Iterable[CheckResult]) -> tuple[int, int]:
    """Pretty-print results grouped by extractor. Returns (n_errors, n_warns)."""
    by_extractor: dict[str, list[CheckResult]] = defaultdict(list)
    for r in all_results:
        by_extractor[r.extractor].append(r)

    print()
    for name, results in by_extractor.items():
        n_pass = sum(1 for r in results if r.passed)
        n_fail = sum(1 for r in results if not r.passed)
        print(f"=== {name}  —  {n_pass} passed, {n_fail} failed ===")
        for r in results:
            marker = "  ok " if r.passed else f"{r.severity:>4}"
            print(f"  [{marker}] {r.check}: {r.detail}")
        print()

    n_err = sum(1 for r in all_results if not r.passed and r.severity == "ERROR")
    n_warn = sum(1 for r in all_results if not r.passed and r.severity == "WARN")
    n_extractors = len(by_extractor)
    print(f"Summary: {n_err} ERROR(s), {n_warn} WARN(s) across {n_extractors} extractor(s).")
    return n_err, n_warn


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--extractors",
        nargs="*",
        help="Filter to these extractor names (default: all).",
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="Exit with status 1 when any WARN-level check fails.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _select_specs(requested: list[str] | None) -> list[ExtractorSpec] | None:
    """Return the specs to run, or None if the user asked for unknown names."""
    if not requested:
        return list(EXTRACTOR_SPECS)
    unknown = set(requested) - {s.name for s in EXTRACTOR_SPECS}
    if unknown:
        LOGGER.error(
            "Unknown extractor(s): %s. Available: %s",
            sorted(unknown),
            [s.name for s in EXTRACTOR_SPECS],
        )
        return None
    return [s for s in EXTRACTOR_SPECS if s.name in requested]


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(levelname)s %(name)s: %(message)s")

    if args.period_end < args.period_start:
        LOGGER.error("--period-end must be >= --period-start")
        return 2

    specs = _select_specs(args.extractors)
    if specs is None:
        return 2

    spark = open_connection()
    all_results: list[CheckResult] = []
    for spec in specs:
        LOGGER.info("Running %s for %s..%s", spec.name, args.period_start, args.period_end)
        # checks.py is pure pandas; extractor outputs are small per-period pulls.
        df = run_extractor(spark, spec.name, args.period_start, args.period_end).toPandas()
        LOGGER.info("  %s rows returned", f"{len(df):,}")
        all_results.extend(run_checks_for_extractor(df, spec))
    # No close(): the "connection" is the shared SparkSession (db.open_connection).

    n_err, n_warn = print_report(all_results)

    if n_err:
        return 1
    if n_warn and args.fail_on_warn:
        return 1
    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
