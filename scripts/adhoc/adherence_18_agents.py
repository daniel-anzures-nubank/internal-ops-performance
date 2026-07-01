"""Ad-hoc: dump adherence / days off / vacations CSVs for 18 MX agents.

One-off deliverable (2026-06-30; not part of the pipeline). Pulls from the
legacy-produced tables and writes three CSVs under output/ (consumed by the
sheets-writer MCP). Kept under scripts/adhoc/ for the record.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

OUT = Path("output")

AGENTS = [
    "alejandra.erazo", "azucena.ruiz", "israel.cadena", "fernanda.ibanez", "shadia.hasbun",
    "lucia.espinosa", "maximiliano.lopez", "ameyali.ramirez", "adriana.marquez", "bertha.sanchez",
    "elias.caudillo", "erick.zeron", "jimena.valencia", "kenia.hernandez", "luis.contreras",
    "luis.delvalle", "omar.ramirez", "rodrigo.ramirez",
]
START, END = "2026-05-18", "2026-06-26"
SHARE_WITH = "daniel.anzures@nubank.com.mx"

_in = ",".join(f"'{a}'" for a in AGENTS)

ADHERENCE_SQL = f"""
WITH perf AS (
  SELECT 'Core/Fraud' AS team, agent, xforce, xplead, squad, squad_district, date_reference, date_granularity, metric, numerator, denominator, metric_value FROM usr.mx__cx.internal_ops_performance_2026
  UNION ALL SELECT 'Content', agent, xforce, xplead, squad, squad_district, date_reference, date_granularity, metric, numerator, denominator, metric_value FROM usr.mx__cx.internal_ops_performance_2026_content
  UNION ALL SELECT 'Social Media', agent, xforce, xplead, squad, squad_district, date_reference, date_granularity, metric, numerator, denominator, metric_value FROM usr.mx__cx.internal_ops_performance_2026_social_media
)
SELECT agent AS Agent,
       CONCAT(agent,'@nubank.com.mx') AS Email,
       team AS Team,
       xforce AS XForce, xplead AS XPLead, squad AS Squad, squad_district AS District,
       CAST(DATE(date_reference) AS STRING) AS Date,
       DATE_FORMAT(date_reference,'EEEE') AS Weekday,
       ROUND(denominator/3600.0,2) AS `Required Hours`,
       ROUND(numerator/3600.0,2) AS `Delivered Hours`,
       ROUND(metric_value,2) AS `Adherence %`
FROM perf
WHERE metric='adherence_agent' AND date_granularity='day'
  AND DATE(date_reference) BETWEEN DATE'{START}' AND DATE'{END}'
  AND agent IN ({_in})
ORDER BY agent, DATE(date_reference)
"""

DAYSOFF_SQL = f"""
WITH off AS (
  SELECT REGEXP_EXTRACT(agent,'^[a-zA-Z]+\\\\.[a-zA-Z]+',0) AS agent, dime_date
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE dime_date BETWEEN DATE'{START}' AND DATE'{END}'
    AND REGEXP_EXTRACT(agent,'^[a-zA-Z]+\\\\.[a-zA-Z]+',0) IN ({_in})
  GROUP BY 1, dime_date
  HAVING SUM(CASE WHEN activity_type_required NOT IN ('time_off','lunch_break') THEN 1 ELSE 0 END)=0
     AND SUM(CASE WHEN activity_type_required='time_off' THEN 1 ELSE 0 END)>0
),
bdx AS (
  SELECT REGEXP_EXTRACT(actor_email,'^[a-zA-Z]+\\\\.[a-zA-Z]+',0) AS agent, squad, district
  FROM etl.mx__series_contract.cx_mx_bdx_snapshots
  WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM etl.mx__series_contract.cx_mx_bdx_snapshots)
    AND REGEXP_EXTRACT(actor_email,'^[a-zA-Z]+\\\\.[a-zA-Z]+',0) IN ({_in})
)
SELECT off.agent AS Agent,
       CONCAT(off.agent,'@nubank.com.mx') AS Email,
       CASE WHEN bdx.squad='content' THEN 'Content'
            WHEN bdx.squad='social' THEN 'Social Media'
            ELSE 'Core/Fraud' END AS Team,
       bdx.squad AS Squad, bdx.district AS District,
       CAST(off.dime_date AS STRING) AS `Day Off Date`,
       DATE_FORMAT(off.dime_date,'EEEE') AS Weekday
FROM off LEFT JOIN bdx USING (agent)
ORDER BY off.agent, off.dime_date
"""

VACATIONS_SQL = f"""
SELECT REGEXP_EXTRACT(work_email,'^[a-zA-Z]+\\\\.[a-zA-Z]+',0) AS Agent,
       work_email AS `Work Email`,
       absence_type AS `Absence Type`,
       COALESCE(absence_reason,'') AS `Absence Reason`,
       CAST(absence_date AS STRING) AS `Absence Date`,
       DATE_FORMAT(absence_date,'EEEE') AS Weekday,
       district AS District,
       COALESCE(step,'') AS Step
FROM usr.cross_x_mx.planning__xpeer_approved_absence_events
WHERE absence_date BETWEEN DATE'{START}' AND DATE'{END}'
  AND REGEXP_EXTRACT(work_email,'^[a-zA-Z]+\\\\.[a-zA-Z]+',0) IN ({_in})
ORDER BY Agent, absence_date
"""


def fetch(cur, query: str) -> pd.DataFrame:
    cur.execute(query)
    cols = [c[0] for c in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def main() -> int:
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    with conn.cursor() as cur:
        adherence = fetch(cur, ADHERENCE_SQL)
        days_off = fetch(cur, DAYSOFF_SQL)
        vacations = fetch(cur, VACATIONS_SQL)
    conn.close()

    OUT.mkdir(exist_ok=True)
    for name, df in [("adherence", adherence), ("days_off", days_off), ("vacations", vacations)]:
        path = OUT / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"{path}: {len(df)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
