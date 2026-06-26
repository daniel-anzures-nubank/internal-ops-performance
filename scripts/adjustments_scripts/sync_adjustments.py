"""Sync the manual-adjustments Google Sheet (and the missing-DIME-slots CSV) to Delta.

Adjustments-layer **sync** step. Where ``download_adjustments.py`` dumps the
sheet to local CSVs, this script writes one small Delta table per tab —
``{schema}.adj_{slug}`` (e.g. ``usr.danielanzures.adj_exclusiones_generales``) —
so the Spark metric builds can read the adjustments via
``adjustments.manual.read_adjustment_table``.

It runs **on Databricks** (it writes Delta via the ambient SparkSession) and
reads the sheet through ``gsheets.py``.

Validation pass (the important part)
------------------------------------
Before writing anything, every targeted tab is validated with the same checks
``download_adjustments.py`` uses (via the shared helpers):

* ``Fecha *`` columns must be valid ``YYYY-MM-DD`` and ``Fecha Inicio <= Fecha Fin``.
* ``Hora *`` columns must be valid 24h ``HH:MM`` (full-day windows use ``00:00``-``23:59``).
* ``Estatus`` / ``Equipo`` values must be known; ``Agente`` non-empty.

Because the metric builds parse these values driver-side (``_to_date`` /
``_time_to_minutes`` in ``adjustments/manual.py``), a malformed cell would crash
a downstream build. This step is the gate that keeps that from happening:

* **strict (default):** if ANY tab has a format error, the sync writes
  **nothing** and exits non-zero. The adjustment tables keep their last-good
  contents; fix the sheet and re-run. (Atomic — never a partial update.)
* **``--skip-invalid``:** tabs with errors are skipped (their existing Delta
  table is left untouched); the clean tabs are written. Exits 0 with warnings.

Column normalization
---------------------
Sheet headers are slugified to snake_case (``Fecha Inicio`` -> ``fecha_inicio``,
``Job (Clasificación)`` -> ``job_clasificacion``) so Delta accepts them and the
``adjustments/manual.py`` ``COL_*`` constants line up. All values are stored as
strings (the apply layer parses them).

Missing DIME slots
------------------
``--include-slots`` also loads the committed ``adjustments/slots_faltantes_dime.csv``
into ``{schema}.adj_slots_faltantes_dime``, transformed to the exact column shape
of the ``dime_slots`` extractor (so ``append_missing_dime_slots`` unions cleanly).

Deployment notes (Databricks)
-----------------------------
The sync task needs, on its cluster:
  * ``gspread`` + ``google-auth`` installed (task/cluster libraries), and
  * the service-account key in env ``GOOGLE_SERVICE_ACCOUNT_JSON`` — wire a
    Databricks **secret scope** to that env var. (The metric builds need none of
    this; only this sync talks to Google.)

Usage
-----
::

    python scripts/adjustments_scripts/sync_adjustments.py --dry-run
    python scripts/adjustments_scripts/sync_adjustments.py --include-slots
    python scripts/adjustments_scripts/sync_adjustments.py --skip-invalid
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


# Locate the repo root (contains `db.py` + `extractors/`) without relying on
# `__file__`: Databricks runs a git-sourced spark_python_task via exec() with no
# `__file__` set. Search upward from every path we can discover.
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
sys.path.insert(0, str(REPO_ROOT / "scripts" / "adjustments_scripts"))

import gsheets  # noqa: E402
from db import open_connection  # noqa: E402
from download_adjustments import (  # noqa: E402
    SKIP_TABS,
    SPREADSHEET_ID,
    TabReport,
    approved_only,
    check_tab,
    slugify,
)

LOGGER = logging.getLogger("cx_adjustments.sync")

DEFAULT_SCHEMA = "usr.danielanzures"
TABLE_PREFIX = "adj_"
SLOTS_CSV = REPO_ROOT / "adjustments" / "slots_faltantes_dime.csv"
SLOTS_TABLE = "slots_faltantes_dime"


def adjustment_table(slug: str, schema: str) -> str:
    return f"{schema}.{TABLE_PREFIX}{slug}"


def _normalize_columns(pdf):
    """Slugify the sheet headers to snake_case (Delta-safe, matches manual.py)."""
    return pdf.rename(columns={c: slugify(str(c)) for c in pdf.columns})


def _pandas_to_spark_strings(spark, pdf):
    """Build an all-StringType Spark DataFrame from a (string) pandas frame.

    gsheets reads every cell as a string; we keep them strings in Delta and let
    the apply layer parse. An explicit schema makes empty frames work too.
    """
    import pandas as pd
    from pyspark.sql import types as T

    schema = T.StructType([T.StructField(str(c), T.StringType()) for c in pdf.columns])
    rows = pdf.astype(object).where(pd.notna(pdf), None).values.tolist()
    return spark.createDataFrame([tuple(r) for r in rows], schema)


def _write_delta(spark, df, table: str) -> int:
    df = df.persist()
    try:
        count = df.count()
        LOGGER.info("Replacing %s (%d rows)", table, count)
        (
            df.write.mode("overwrite")
            .option("overwriteSchema", "true")
            .format("delta")
            .saveAsTable(table)
        )
    finally:
        df.unpersist()
    return count


# ---------------------------------------------------------------------------
# Missing DIME slots: CSV -> dime_slots extractor shape
# ---------------------------------------------------------------------------


def _missing_dime_slots_df(spark):
    """Load slots_faltantes_dime.csv and shape it like the ``dime_slots`` extractor.

    The extractor derives ``agent`` (email prefix), ``date``, ``squad`` and the
    ``slot_*_local_unix`` columns from the raw source columns; we replicate that
    here so ``append_missing_dime_slots`` can union the result with no NULL keys.
    Relies on the UTC session tz (set in ``db.get_spark``) so ``unix_timestamp``
    reads the local timestamp the same way the extractor's ``UNIX_TIMESTAMP`` does.
    """
    import pandas as pd
    from pyspark.sql import functions as F

    pdf = pd.read_csv(SLOTS_CSV, dtype=str).where(lambda d: d.notna(), None)
    raw = _pandas_to_spark_strings(spark, pdf)

    local_ts = F.to_timestamp(F.col("local_timestamp_dime_slot_starts_at"))
    start_unix = F.unix_timestamp(local_ts)
    return raw.select(
        F.lower(F.regexp_extract(F.col("agent"), r"^[a-zA-Z]+\.[a-zA-Z]+", 0)).alias(
            "agent"
        ),
        F.to_date(F.col("dime_date")).alias("date"),
        F.col("agent_dime_squad").alias("squad"),
        F.col("affiliation"),
        F.col("activity_type_required"),
        F.col("shuffle_status_required"),
        F.col("dimensioned_activity"),
        local_ts.alias("local_timestamp_dime_slot_starts_at"),
        start_unix.cast("bigint").alias("slot_start_local_unix"),
        (start_unix + 30 * 60).cast("bigint").alias("slot_end_local_unix"),
    )


# ---------------------------------------------------------------------------
# Sheet tabs
# ---------------------------------------------------------------------------


def _collect_tabs(client, spreadsheet: str, requested: list[str] | None):
    """Return (targets, source_frames) for the requested tabs (all but Guía)."""
    sh = client.open_by_key(gsheets.extract_sheet_id(spreadsheet))
    titles = [ws.title for ws in sh.worksheets()]
    targets = requested if requested else [t for t in titles if t not in SKIP_TABS]
    missing = [t for t in targets if t not in titles]
    if missing:
        raise SystemExit(f"Tabs not found in the spreadsheet: {missing}")
    frames = {
        tab: gsheets.read_worksheet(spreadsheet, tab, client=client) for tab in targets
    }
    return targets, frames


def _validate(targets, frames):
    """Validate every tab; return (reports, ok_tabs, bad_tabs)."""
    reports: list[TabReport] = []
    bad_tabs: set[str] = set()
    for tab in targets:
        report = TabReport(tab)
        check_tab(approved_only(frames[tab]), report)
        reports.append(report)
        if report.errors:
            bad_tabs.add(tab)
    ok_tabs = [t for t in targets if t not in bad_tabs]
    return reports, ok_tabs, bad_tabs


def _print_report(reports) -> tuple[int, int]:
    n_errors = sum(len(r.errors) for r in reports)
    n_warnings = sum(len(r.warnings) for r in reports)
    print("\n========== validation report ==========")
    for r in reports:
        status = "OK" if not r.errors and not r.warnings else (
            "ERRORS" if r.errors else "warnings"
        )
        print(f"\n[{status}] {r.tab}")
        for msg in r.errors:
            print(f"  ERROR   {msg}")
        for msg in r.warnings:
            print(f"  warning {msg}")
    print(f"\n{len(reports)} tabs, {n_errors} errors, {n_warnings} warnings.")
    return n_errors, n_warnings


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--spreadsheet", default=SPREADSHEET_ID)
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help=f"Target schema for the adj_* tables (default: {DEFAULT_SCHEMA}).",
    )
    parser.add_argument(
        "--tabs", nargs="*", default=None, help="Only sync these tabs (default: all but Guía)."
    )
    parser.add_argument(
        "--include-slots",
        action="store_true",
        help="Also sync slots_faltantes_dime.csv -> adj_slots_faltantes_dime.",
    )
    parser.add_argument(
        "--skip-invalid",
        action="store_true",
        help="Skip tabs with format errors (keep last-good) instead of failing the whole sync.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and summarize but do NOT write any Delta table.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(levelname)s %(name)s: %(message)s"
    )

    client = gsheets.open_client()
    targets, frames = _collect_tabs(client, args.spreadsheet, args.tabs)

    reports, ok_tabs, bad_tabs = _validate(targets, frames)
    n_errors, _ = _print_report(reports)

    if bad_tabs and not args.skip_invalid:
        LOGGER.error(
            "Validation failed for %d tab(s): %s. Nothing written (use --skip-invalid "
            "to write the clean tabs and keep last-good for the rest).",
            len(bad_tabs), sorted(bad_tabs),
        )
        return 1

    write_tabs = ok_tabs if args.skip_invalid else targets
    if bad_tabs:
        LOGGER.warning("Skipping invalid tabs (kept last-good): %s", sorted(bad_tabs))

    spark = open_connection()

    for tab in write_tabs:
        slug = slugify(tab)
        table = adjustment_table(slug, args.schema)
        approved = _normalize_columns(approved_only(frames[tab]))
        if args.dry_run:
            LOGGER.info("[dry-run] %-32s -> %s (%d rows)", tab, table, len(approved))
            continue
        df = _pandas_to_spark_strings(spark, approved)
        _write_delta(spark, df, table)

    if args.include_slots:
        table = adjustment_table(SLOTS_TABLE, args.schema)
        if not SLOTS_CSV.is_file():
            LOGGER.error("Missing slots CSV: %s", SLOTS_CSV)
            return 2
        slots = _missing_dime_slots_df(spark)
        if args.dry_run:
            LOGGER.info("[dry-run] %-32s -> %s (%d rows)", "slots_faltantes_dime.csv",
                        table, slots.count())
        else:
            _write_delta(spark, slots, table)

    LOGGER.info(
        "Done. %s %d tab table(s)%s.",
        "Would write" if args.dry_run else "Wrote",
        len(write_tabs),
        " + slots" if args.include_slots else "",
    )
    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
