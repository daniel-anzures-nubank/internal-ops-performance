"""Ad-hoc: build the 'Cohort x Scenario' tab in the buffer-methodology sheet.

One-off deliverable (2026-07-01; not part of the pipeline). Reads a saved
Databricks MCP result from a past session (SAVED below) — kept under
scripts/adhoc/ for the record; re-running requires regenerating that file.

Grain: one row per squad/district/shift/month/metric/scenario, with cohort avg,
the on-target cutoff under that scenario, count + % on target, and whether the
buffer came from the cohort or the team-month fallback.

Reads the saved Databricks result (JSON) and writes via gsheets.py using the
dedicated service-account key. Requires the sheet shared (Editor) with that SA.
"""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import gsheets  # noqa: E402

SHEET = "https://docs.google.com/spreadsheets/d/1YQQpfPFJQdldr_hWydek3tdTMXIYCodXsGzQ4rLskG4/edit"
TAB = "Cohort x Scenario"
KEY_FILE = "/Users/daniel.anzures/Documents/nu-mx-internal-ops-sa-key.json"

SAVED = ("/Users/daniel.anzures/.claude/projects/"
         "-Users-daniel-anzures-Documents-internal-ops-performance/"
         "d5abba1d-98a4-4dcf-9bbb-dd8f558dca0a/tool-results/"
         "mcp-databricks-sql-execute_sql_read_only-1782931030009.txt")

HEADER = ["Squad", "District", "Shift", "Month", "Metric", "Scenario",
          "Buffer source", "Agents (n)", "Avg %", "On-target cutoff %",
          "In target (n)", "% on target (currently)", "% on target (scenario)"]


def fetch() -> pd.DataFrame:
    data = json.load(open(SAVED))
    rows = [[v.get("string_value") or "" for v in r["values"]]
            for r in data["result"]["data_array"]]
    df = pd.DataFrame(rows, columns=HEADER)
    return df


def main() -> int:
    df = fetch()
    # Drop the percentile ("Drop worst q%") scenarios per stakeholder request.
    df = df[~df["Scenario"].str.contains("Drop worst")].reset_index(drop=True)
    print(f"rows={len(df)}  cols={len(df.columns)}")
    print(df.head(6).to_string(index=False))
    print("metrics:", sorted(df["Metric"].unique().tolist()))
    print("scenarios:", df["Scenario"].nunique())
    print("buffer sources:", df["Buffer source"].value_counts().to_dict())
    if "--write" in sys.argv:
        from google.oauth2.service_account import Credentials
        import gspread
        creds = Credentials.from_service_account_file(KEY_FILE, scopes=list(gsheets.DEFAULT_SCOPES))
        client = gspread.authorize(creds)
        n = gsheets.write_dataframe(SHEET, df, TAB, client=client)
        print(f"WROTE {n} rows to '{TAB}'")
    else:
        print("(dry run — pass --write to push to the sheet)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
