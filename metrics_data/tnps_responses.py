"""tnps_responses — one row per Social-Media tNPS survey response.

This is a RAW dataset, not a finished metric. It exposes every transactional-NPS
(tNPS) survey response attributable to an active social agent, one row per
response, with its raw 0-10 score. A downstream ``metrics`` layer classifies each
response (promoter ``>= 9`` / detractor ``<= 6`` / neutral 7-8) and computes the
**Human tNPS** metric: ``(promoters - detractors) / valid_responses``.

tNPS only applies to **Social Media**. The source
(``sprinklr_tnps_data``) only contains surveys for cases handled by a human social
agent, so every row here is a human social agent's survey.

Public API
----------
``compute_tnps_responses(agent_info, tnps)`` returns one row per survey response.

Source tables (via extractors)
------------------------------
* ``agent_information`` → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``tnps``             → ``usr.sprinklr_api_data_integration.sprinklr_tnps_data``.

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* Drop rows whose ``agent`` did not resolve (empty string) — i.e. unattributable
  / non-human surveys.
* Roster: ``status = 'active'`` and non-null ``squad`` (inner join attaches the
  dimensions / scopes output to active agents).

Filters deferred to the future metrics layer (NOT applied here)
---------------------------------------------------------------
* Promoter / detractor / neutral classification and the NPS ratio.
* The validity window ``survey_response_date <= date + 1 day``.
* The outage-date exclusion (``2026-03-27``).
* Dedup to one response per ``case_number`` (legacy counts DISTINCT case_number).

Output schema (one row per survey response)
--------------------------------------------
    agent                 STRING
    xforce                STRING
    xplead                STRING
    team                  STRING   performance team (from roster; see team_squad_mapping)
    squad                 STRING   roster squad
    district              STRING   roster district (was ``squad_district``)
    shift                 STRING   roster shift
    date                  DATE     case closure day (MX local) the response is attributed to
    case_number           STRING   the case / survey identifier
    survey_response_date  DATE     when the customer answered the survey
    survey_score          INT      raw 0-10 NPS score (nullable)
"""

from __future__ import annotations

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Roster-level squad exclusions. Currently empty — scope is source-driven
# (only social agents appear in the tNPS source).
TNPS_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()


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


def compute_tnps_responses(
    agent_info: pd.DataFrame,
    tnps: pd.DataFrame,
) -> pd.DataFrame:
    """End-to-end tnps_responses pipeline (one row per survey response)."""
    if tnps.empty:
        return pd.DataFrame(
            {c: pd.Series(dtype="object") for c, _ in IO_TNPS_RESPONSES_SCHEMA}
        )

    responses = tnps.copy()
    responses = responses[
        responses["agent"].notna() & (responses["agent"] != "")
    ].copy()

    if responses.empty:
        return pd.DataFrame(
            {c: pd.Series(dtype="object") for c, _ in IO_TNPS_RESPONSES_SCHEMA}
        )

    # --- roster join --------------------------------------------------------
    roster = agent_info.loc[
        (agent_info["status"] == "active")
        & agent_info["squad"].notna()
        & ~agent_info["squad"].isin(TNPS_OUT_OF_SCOPE_SQUADS),
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

    responses["snapshot_month"] = _as_naive_datetime(
        pd.to_datetime(responses["date"]).dt.to_period("M").dt.to_timestamp()
    )
    enriched = responses.merge(roster, on=["agent", "snapshot_month"], how="inner")

    enriched["survey_score"] = pd.to_numeric(
        enriched["survey_score"], errors="coerce"
    ).astype("Int64")

    out = enriched[[
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "date",
        "case_number",
        "survey_response_date",
        "survey_score",
    ]].copy()

    return out.sort_values(["date", "agent", "case_number"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_tnps_responses.py
# ---------------------------------------------------------------------------

IO_TNPS_RESPONSES_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("case_number", "STRING"),
    ("survey_response_date", "DATE"),
    ("survey_score", "INT"),
)
