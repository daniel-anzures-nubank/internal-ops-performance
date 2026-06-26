"""Download the manual-adjustments Google Sheet tabs to CSV, with sanity checks.

Adjustments-layer script. Reads every tab of the adjustments spreadsheet
(**except ``Guía``**, which is documentation, not data), saves approved rows from
each one as a CSV under ``adjustments/data/`` (gitignored), and runs per-tab
sanity checks:

* ``Fecha *`` columns must be valid ``YYYY-MM-DD`` dates, and
  ``Fecha Inicio <= Fecha Fin`` row by row. Open-ended adjustments use the
  sentinel ``9000-01-01`` in ``Fecha Fin``.
* ``Hora *`` columns must be valid 24h ``HH:MM`` times. Full-day windows use
  ``00:00`` to ``23:59``.
* ``Estatus`` must be non-empty and one of the known values
  (``Aprobado`` / ``Pendiente`` / ``Denegado``).
* ``Equipo`` tokens (comma-separated) must be known teams
  (``Core`` / ``Fraud`` / ``Social Media`` / ``Content`` / ``Quality`` /
  ``Planning`` / ``Todos``).
* ``Agente`` must be non-empty.
* Fully duplicated rows are reported.

Violations of the date/hour formats are **errors** (exit code 1, the offending
CSV is still written so it can be inspected); the rest are **warnings**.
Empty tabs are written as header-only/empty CSVs and noted.
Tabs with an ``Estatus`` column only export rows where ``Estatus = 'Aprobado'``;
tabs without that column are exported unchanged.

Source of truth
---------------
https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU
(must be shared with the service account — see ``gsheets.py`` for credentials).

Usage
-----
::

    uv run --group sheets python scripts/adjustments_scripts/download_adjustments.py
    uv run --group sheets python scripts/adjustments_scripts/download_adjustments.py \\
        --output-dir /tmp/adjustments --tabs "Cross Support" "Training"
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import gsheets  # noqa: E402

LOGGER = logging.getLogger("cx_adjustments.download")

SPREADSHEET_ID = "1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU"
SKIP_TABS = ("Guía",)  # documentation tab, not data
DEFAULT_OUTPUT_DIR = REPO_ROOT / "adjustments" / "data"

KNOWN_STATUSES = {"aprobado", "pendiente", "denegado"}
KNOWN_TEAMS = {
    "core",
    "fraud",
    "social media",
    "content",
    "quality",
    "planning",
    "todos",
}

_HHMM_RE = re.compile(r"^([01]?\d|2[0-3]):[0-5]\d$")


def slugify(name: str) -> str:
    """``'Inconsistencias DIME Approved'`` -> ``'inconsistencias_dime_approved'``."""
    norm = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", norm.lower()).strip("_")


def _normalize_token(value: object) -> str:
    return str(value).strip().lower()


def approved_only(df: pd.DataFrame) -> pd.DataFrame:
    """Return only approved adjustment rows when the tab has an Estatus column."""
    if "Estatus" not in df.columns:
        return df.copy()
    return df.loc[df["Estatus"].map(_normalize_token) == "aprobado"].copy()


def _parse_date(value: str) -> dt.date | None:
    try:
        return dt.date.fromisoformat(value.strip())
    except ValueError:
        return None


def _is_valid_hour(value: str) -> bool:
    v = value.strip()
    return bool(_HHMM_RE.match(v))


class TabReport:
    """Collects errors/warnings for one tab."""

    def __init__(self, tab: str):
        self.tab = tab
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


def check_tab(df: pd.DataFrame, report: TabReport) -> None:
    """Run the heuristic, column-name-driven sanity checks on one tab."""
    if df.empty:
        report.warn("tab is empty (no data rows)")
        return

    # Sheet row number of each df row (header is row 1).
    def rownum(idx: int) -> int:
        return idx + 2

    date_cols = [c for c in df.columns if c.lower().startswith("fecha")]
    hour_cols = [c for c in df.columns if c.lower().startswith("hora")]

    # --- dates -------------------------------------------------------------
    parsed: dict[str, list[dt.date | None]] = {}
    for col in date_cols:
        parsed[col] = []
        for idx, raw in df[col].items():
            raw = str(raw).strip()
            if not raw:
                parsed[col].append(None)
                report.error(f"row {rownum(idx)}: '{col}' is empty")
                continue
            d = _parse_date(raw)
            parsed[col].append(d)
            if d is None:
                report.error(
                    f"row {rownum(idx)}: '{col}' = {raw!r} is not a valid "
                    "YYYY-MM-DD date"
                )

    same_day: list[bool] = [False] * len(df)
    if "Fecha Inicio" in df.columns and "Fecha Fin" in df.columns:
        for i, (start, end) in enumerate(
            zip(parsed["Fecha Inicio"], parsed["Fecha Fin"])
        ):
            if start and end:
                same_day[i] = start == end
                if start > end:
                    report.error(
                        f"row {rownum(i)}: Fecha Inicio ({start}) is after "
                        f"Fecha Fin ({end})"
                    )

    # --- hours ---------------------------------------------------------------
    for col in hour_cols:
        for idx, raw in df[col].items():
            raw = str(raw).strip()
            if not raw:
                report.error(f"row {rownum(idx)}: '{col}' is empty")
            elif not _is_valid_hour(raw):
                report.error(
                    f"row {rownum(idx)}: '{col}' = {raw!r} is not a valid "
                    "HH:MM time"
                )

    if "Hora Inicio" in df.columns and "Hora Fin" in df.columns:
        for i, (h0, h1) in enumerate(zip(df["Hora Inicio"], df["Hora Fin"])):
            h0, h1 = str(h0).strip(), str(h1).strip()
            if (
                same_day[i]
                and _HHMM_RE.match(h0)
                and _HHMM_RE.match(h1)
                and h0.zfill(5) >= h1.zfill(5)
            ):
                report.error(
                    f"row {rownum(i)}: Hora Inicio ({h0}) is not before "
                    f"Hora Fin ({h1}) on a same-day window"
                )

    # --- categorical / required fields (warnings) ----------------------------
    if "Estatus" in df.columns:
        for idx, raw in df["Estatus"].items():
            v = str(raw).strip().lower()
            if not v:
                report.warn(f"row {rownum(idx)}: 'Estatus' is empty")
            elif v not in KNOWN_STATUSES:
                report.warn(
                    f"row {rownum(idx)}: 'Estatus' = {raw!r} is not one of "
                    f"{sorted(KNOWN_STATUSES)}"
                )

    if "Equipo" in df.columns:
        for idx, raw in df["Equipo"].items():
            tokens = [t.strip().lower() for t in str(raw).split(",") if t.strip()]
            if not tokens:
                report.warn(f"row {rownum(idx)}: 'Equipo' is empty")
            for t in tokens:
                if t not in KNOWN_TEAMS:
                    report.warn(
                        f"row {rownum(idx)}: 'Equipo' token {t!r} is not a "
                        f"known team {sorted(KNOWN_TEAMS)}"
                    )

    if "Agente" in df.columns:
        for idx, raw in df["Agente"].items():
            if not str(raw).strip():
                report.warn(f"row {rownum(idx)}: 'Agente' is empty")

    dupes = df[df.duplicated(keep="first")]
    for idx in dupes.index:
        report.warn(f"row {rownum(idx)}: fully duplicated row")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--spreadsheet",
        default=SPREADSHEET_ID,
        help="Adjustments spreadsheet id or URL.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for the CSVs (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--tabs",
        nargs="*",
        default=None,
        help="Only download these tabs (default: all except 'Guía').",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(levelname)s %(name)s: %(message)s"
    )

    client = gsheets.open_client()
    sh = client.open_by_key(gsheets.extract_sheet_id(args.spreadsheet))
    titles = [ws.title for ws in sh.worksheets()]
    targets = args.tabs if args.tabs else [t for t in titles if t not in SKIP_TABS]

    missing = [t for t in targets if t not in titles]
    if missing:
        LOGGER.error("Tabs not found in the spreadsheet: %s", missing)
        return 2

    args.output_dir.mkdir(parents=True, exist_ok=True)

    reports: list[TabReport] = []
    for tab in targets:
        source_df = gsheets.read_worksheet(args.spreadsheet, tab, client=client)
        df = approved_only(source_df)
        path = args.output_dir / f"{slugify(tab)}.csv"
        df.to_csv(path, index=False)
        if "Estatus" in source_df.columns:
            LOGGER.info(
                "%-32s -> %s (%s/%s approved rows)",
                tab,
                path,
                len(df),
                len(source_df),
            )
        else:
            LOGGER.info("%-32s -> %s (%s rows)", tab, path, len(df))

        report = TabReport(tab)
        check_tab(df, report)
        reports.append(report)

    n_errors = sum(len(r.errors) for r in reports)
    n_warnings = sum(len(r.warnings) for r in reports)

    print("\n========== sanity check report ==========")
    for r in reports:
        status = "OK" if not r.errors and not r.warnings else (
            "ERRORS" if r.errors else "warnings"
        )
        print(f"\n[{status}] {r.tab}")
        for msg in r.errors:
            print(f"  ERROR   {msg}")
        for msg in r.warnings:
            print(f"  warning {msg}")
    print(
        f"\n{len(reports)} tabs, {n_errors} errors, {n_warnings} warnings. "
        f"CSVs in {args.output_dir}"
    )

    return 1 if n_errors else 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
