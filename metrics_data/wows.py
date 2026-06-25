"""wows — one row per Social-Media WoW experience.

This is a RAW dataset, not a finished metric. It exposes every WoW experience
logged for an active social agent, one row per WoW, with its case id. A
downstream ``metrics`` layer counts these (``COUNT(DISTINCT case_id)`` per
agent / period) into the **WoWs** metric (monthly target >= 5).

WoWs only apply to **Social Media**. The source sheet only contains social
agents' WoWs.

Public API
----------
``compute_wows(agent_info, wows)`` returns one row per WoW experience.

Source tables (via extractors)
------------------------------
* ``agent_information`` → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``wows``             → ``gsheets.sheets.mx_wows_daniel_temp`` (WoWs Google Sheet).

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* Drop rows whose ``agent`` did not resolve (empty string).
* Roster: ``status = 'active'`` and non-null ``squad`` (inner join attaches the
  dimensions / scopes output to active agents).

Filters deferred to the future metrics layer (NOT applied here)
---------------------------------------------------------------
* ``COUNT(DISTINCT case_id)`` aggregation and the monthly target (>= 5).
* The outage-date exclusion (``2026-03-27``).

Output schema (one row per WoW experience)
------------------------------------------
    agent       STRING
    xforce      STRING
    xplead      STRING
    team        STRING   performance team (from roster; see team_squad_mapping)
    squad       STRING   roster squad
    district    STRING   roster district (was ``squad_district``)
    shift       STRING   roster shift
    date        DATE     day the WoW was logged (MX local)
    case_id     STRING   the WoW's case identifier
"""

from __future__ import annotations

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Roster-level squad exclusions. Currently empty — scope is source-driven
# (only social agents appear in the WoWs sheet).
WOWS_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Small utility
# ---------------------------------------------------------------------------


def _as_naive_datetime(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-naive ``datetime64[ns]`` for merge keys."""
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        return s.dt.tz_localize(None)
    return s


# ---------------------------------------------------------------------------
# Orchestrator — roster join (no aggregation)
# ---------------------------------------------------------------------------


def compute_wows(
    agent_info: pd.DataFrame,
    wows: pd.DataFrame,
) -> pd.DataFrame:
    """End-to-end wows pipeline (one row per WoW experience)."""
    if wows.empty:
        return pd.DataFrame({c: pd.Series(dtype="object") for c, _ in IO_WOWS_SCHEMA})

    rows = wows.copy()
    rows = rows[rows["agent"].notna() & (rows["agent"] != "")].copy()

    if rows.empty:
        return pd.DataFrame({c: pd.Series(dtype="object") for c, _ in IO_WOWS_SCHEMA})

    # --- roster join --------------------------------------------------------
    roster = agent_info.loc[
        (agent_info["status"] == "active")
        & agent_info["squad"].notna()
        & ~agent_info["squad"].isin(WOWS_OUT_OF_SCOPE_SQUADS),
        [
            "agent",
            "xforce",
            "xplead",
            "team",
            "squad",
            "squad_district",
            "shift",
            "snapshot_month",
        ],
    ].copy()
    roster = roster.rename(columns={"squad_district": "district"})
    roster["snapshot_month"] = _as_naive_datetime(roster["snapshot_month"])

    rows["snapshot_month"] = _as_naive_datetime(
        pd.to_datetime(rows["date"]).dt.to_period("M").dt.to_timestamp()
    )
    enriched = rows.merge(roster, on=["agent", "snapshot_month"], how="inner")

    enriched["case_id"] = enriched["case_id"].astype("string")

    out = enriched[[
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "date",
        "case_id",
    ]].copy()

    return out.sort_values(["date", "agent", "case_id"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_wows.py
# ---------------------------------------------------------------------------

IO_WOWS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("case_id", "STRING"),
)
