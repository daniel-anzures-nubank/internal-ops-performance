"""Google Sheets transport — pure-Python, runs locally (no Databricks/Spark).

A small, dependency-light wrapper around ``gspread`` for reading/writing Google
Sheets from this repo **without** any Databricks runtime. It's the local-friendly
counterpart to ``db.py``: where ``db.py`` talks to the warehouse, this talks to
the Sheets API, so the same logic that lives in Databricks ``%run`` helper
notebooks can be exercised on a laptop.

What it deliberately does NOT do (unlike the Databricks notebook it replaces):
    * No ``dbutils.secrets`` — credentials come from environment variables.
    * No Spark / ``saveAsTable`` — reads return a plain ``pandas.DataFrame``.
    * No ``%run`` of external notebooks — the functions live here.

Credentials (a Google **service account**), checked in this order:
    1. ``GOOGLE_SERVICE_ACCOUNT_JSON``  — the service-account JSON, inline.
    2. ``GOOGLE_SERVICE_ACCOUNT_FILE``  — path to the service-account JSON file.
    3. ``GOOGLE_APPLICATION_CREDENTIALS`` — the Google-standard path env var.
    4. ``GOOGLE_SA_*`` per-field vars — reassemble the JSON from one env var per
       field (``GOOGLE_SA_TYPE``, ``GOOGLE_SA_PROJECT_ID``, ``GOOGLE_SA_PRIVATE_KEY``,
       ``GOOGLE_SA_CLIENT_EMAIL``, …; see ``_SA_FIELD_ENV_VARS``).

The production service account is the ``nu-mx-internal-ops`` SA (its
``client_email``, address form ``<name>@<project>.iam.gserviceaccount.com``).
On Databricks its key lives in the secret scope
``nu-mx-internal-ops-sa-secret``, which stores
the JSON **decomposed into one secret key per field** (no single-JSON key). The
job cluster wires each secret key to the matching ``GOOGLE_SA_*`` env var
(e.g. ``GOOGLE_SA_PRIVATE_KEY = {{secrets/nu-mx-internal-ops-sa-secret/private_key}}``),
so path 4 above reassembles the credentials at runtime.

For local dev, put one of paths 1-3 in a repo-root ``.env`` (gitignored). NEVER
hardcode the key in source. Share the target sheet (Viewer) with the service
account's ``client_email``.

CLI (quick local check)::

    uv run --with gspread --with google-auth python gsheets.py list  <sheet_id_or_url>
    uv run --with gspread --with google-auth python gsheets.py read  <sheet_id_or_url> "WoWs Base"

(If ``uv`` is pinned to a private index, add ``--default-index https://pypi.org/simple``.)
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

# python-dotenv is only needed to load the gitignored .env for off-cluster runs.
# On a Databricks cluster it isn't installed (creds come from a secret-scope env
# var instead), so the import is best-effort.
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - cluster has no python-dotenv
    load_dotenv = None

if TYPE_CHECKING:  # only for type hints; not imported at runtime
    import gspread

REPO_ROOT = Path(__file__).resolve().parent
if load_dotenv is not None:
    load_dotenv(REPO_ROOT / ".env")

# Read + write. Use ``.../auth/spreadsheets.readonly`` if you only ever read.
DEFAULT_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)

# Pull the file id out of a full Sheets/Drive URL (or pass the bare id through).
_URL_ID_RE = re.compile(r"/d/([a-zA-Z0-9-_]+)")


def extract_sheet_id(spreadsheet: str) -> str:
    """Return the bare file id from a Sheets URL, or pass a bare id through."""
    match = _URL_ID_RE.search(spreadsheet)
    return match.group(1) if match else spreadsheet


# Map each ``GOOGLE_SA_<FIELD>`` env var to its service-account JSON key. This is
# the per-field path used on Databricks, where the ``nu-mx-internal-ops-sa-secret``
# secret scope stores the SA key decomposed into one secret key per JSON field
# (no single-JSON key exists), each wired to the matching env var in the job
# cluster's ``spark_env_vars``.
_SA_FIELD_ENV_VARS = {
    "GOOGLE_SA_TYPE": "type",
    "GOOGLE_SA_PROJECT_ID": "project_id",
    "GOOGLE_SA_PRIVATE_KEY_ID": "private_key_id",
    "GOOGLE_SA_PRIVATE_KEY": "private_key",
    "GOOGLE_SA_CLIENT_EMAIL": "client_email",
    "GOOGLE_SA_CLIENT_ID": "client_id",
    "GOOGLE_SA_AUTH_URI": "auth_uri",
    "GOOGLE_SA_TOKEN_URI": "token_uri",
    "GOOGLE_SA_AUTH_PROVIDER_X509_CERT_URL": "auth_provider_x509_cert_url",
    "GOOGLE_SA_CLIENT_X509_CERT_URL": "client_x509_cert_url",
    "GOOGLE_SA_UNIVERSE_DOMAIN": "universe_domain",
}


def _credentials_from_fields(environ) -> dict | None:
    """Reassemble the SA dict from ``GOOGLE_SA_*`` env vars, or ``None`` if unset.

    Assembles whatever ``GOOGLE_SA_*`` vars are present and lets google-auth raise
    if a required field is missing. The ``private_key`` is un-escaped
    (``\\n`` -> real newline) so a value stored with a literal ``\\n`` still parses;
    it's a no-op when the value already carries real newlines.
    """
    info = {
        json_key: environ[env_var]
        for env_var, json_key in _SA_FIELD_ENV_VARS.items()
        if env_var in environ
    }
    if not info:
        return None
    if "private_key" in info:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    return info


def _load_credentials_info() -> dict:
    """Load the service-account JSON from the environment (see module docstring)."""
    inline = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if inline:
        # ``strict=False`` tolerates the literal newlines inside the PEM key.
        return json.loads(inline, strict=False)

    path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE") or os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS"
    )
    if path:
        return json.loads(Path(path).expanduser().read_text(), strict=False)

    fields = _credentials_from_fields(os.environ)
    if fields is not None:
        return fields

    raise SystemExit(
        "No Google credentials found. Set one of GOOGLE_SERVICE_ACCOUNT_JSON, "
        "GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_APPLICATION_CREDENTIALS, or the "
        "per-field GOOGLE_SA_* vars (see gsheets.py docstring) — typically in a "
        "repo-root .env for local dev, or a secret scope on Databricks."
    )


def open_client(scopes: tuple[str, ...] = DEFAULT_SCOPES) -> "gspread.Client":
    """Authorize and return a ``gspread`` client.

    Lazy-imports ``gspread`` / ``google-auth`` so importing this module (or
    running ``--help``) doesn't require them to be installed.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as exc:
        raise SystemExit(
            "gspread / google-auth are not installed. Install with e.g.\n"
            "  uv run --with gspread --with google-auth python gsheets.py ...\n"
            "or add the 'sheets' dependency group (see pyproject.toml)."
        ) from exc

    creds = Credentials.from_service_account_info(
        _load_credentials_info(), scopes=list(scopes)
    )
    return gspread.authorize(creds)


def list_worksheets(
    spreadsheet: str, *, client: "gspread.Client | None" = None
) -> list[str]:
    """Return the worksheet (tab) titles of a spreadsheet."""
    client = client or open_client()
    sh = client.open_by_key(extract_sheet_id(spreadsheet))
    return [ws.title for ws in sh.worksheets()]


def read_worksheet(
    spreadsheet: str,
    worksheet_name: str | None = None,
    *,
    header: bool = True,
    client: "gspread.Client | None" = None,
) -> pd.DataFrame:
    """Read a worksheet into a ``pandas.DataFrame``.

    Args:
        spreadsheet: spreadsheet id or full Sheets URL.
        worksheet_name: tab name; ``None`` reads the first worksheet.
        header: treat the first row as column names (else integer columns).
        client: reuse an existing client (else one is created).
    """
    client = client or open_client()
    sh = client.open_by_key(extract_sheet_id(spreadsheet))
    ws = sh.worksheet(worksheet_name) if worksheet_name else sh.sheet1
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    if header:
        cols = values[0]
        return pd.DataFrame(values[1:], columns=cols)
    return pd.DataFrame(values)


def write_dataframe(
    spreadsheet: str,
    df: pd.DataFrame,
    worksheet_name: str = "Sheet1",
    *,
    include_header: bool = True,
    create_if_missing: bool = True,
    client: "gspread.Client | None" = None,
) -> int:
    """Overwrite a worksheet with the contents of ``df``. Returns rows written.

    The worksheet is cleared first, then filled. NaNs become empty cells and all
    values are coerced to strings (Sheets is untyped). Share the sheet with the
    service account's ``client_email`` first, or this raises a permission error.
    """
    client = client or open_client()
    sh = client.open_by_key(extract_sheet_id(spreadsheet))
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        if not create_if_missing:
            raise
        rows = max(len(df) + 1, 1)
        cols = max(len(df.columns), 1)
        ws = sh.add_worksheet(title=worksheet_name, rows=rows, cols=cols)

    body = df.astype(object).where(pd.notna(df), "").astype(str).values.tolist()
    values = ([list(map(str, df.columns))] + body) if include_header else body
    ws.clear()
    if values:
        ws.update(values, value_input_option="RAW")
    return len(body)


def _main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Local Google Sheets helper.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="List worksheet tabs.")
    p_list.add_argument("spreadsheet")

    p_read = sub.add_parser("read", help="Read a tab and print a preview.")
    p_read.add_argument("spreadsheet")
    p_read.add_argument("worksheet", nargs="?", default=None)
    p_read.add_argument("--rows", type=int, default=5, help="Preview row count.")

    args = parser.parse_args(argv)

    if args.cmd == "list":
        for name in list_worksheets(args.spreadsheet):
            print(name)
        return 0

    df = read_worksheet(args.spreadsheet, args.worksheet)
    print(f"shape: {df.shape[0]} rows x {df.shape[1]} cols")
    print(df.head(args.rows).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
