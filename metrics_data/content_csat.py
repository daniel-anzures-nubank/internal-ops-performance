"""content_csat — one row per Content CSAT survey response × content agent.

This is a RAW dataset feeding the Content **Quality (CSAT)** metric. Each Content
CSAT survey response rates how well the Content (enablement) team supported a
given squad that month, across 8 questions scored 1-5 (a "promoter" is any
answer >= 4). The per-response score is ``promoters / number_of_questions``.

CSAT is attributed by **target_squad**, not by individual agent: a single survey
response is credited to *every* active content agent who supports that squad
(``target_squad``). This module reproduces the legacy fan-out — joining each
response to the content roster on ``target_squad`` — so the grain here is **one
row per (survey response × content agent)**, carrying the standard shared
dimensions. The metrics layer then aggregates per agent as
``SUM(promoters) / SUM(number_of_questions)``.

CSAT only applies to **Content**.

Public API
----------
``compute_content_csat(agent_info, csat)``.

Source tables (via extractors)
------------------------------
* ``agent_information`` → BDX snapshots + ``mx_content_bdx`` (the content roster,
  which carries each content agent's ``target_squad``).
* ``content_csat``     → ``gsheets.sheets.mx_content_csat_daniel_anz_temp``.

Filters applied here (minimal — raw table)
------------------------------------------
* Roster: ``status = 'active'`` and non-null ``target_squad`` (inner join on
  ``(target_squad, snapshot_month)`` — this scopes output to content and fans the
  response out to that month's content agents serving the squad).

Deferred to the metrics layer (NOT applied here)
------------------------------------------------
* Per-agent / per-period aggregation (``SUM(promoters) / SUM(number_of_questions)``).
* Any per-agent manual adjustments / outage-date carve-outs.

Output schema (one row per survey response × content agent)
-----------------------------------------------------------
    agent              STRING
    xforce             STRING
    xplead             STRING
    team               STRING   always 'content'
    squad              STRING   roster squad (content agents: 'enablement')
    district           STRING   roster district (content agents: 'content')
    shift              STRING   roster shift (NULL for content)
    date               DATE     month rated (DATE of date_reference)
    target_squad       STRING   the supported squad the survey is about (join key)
    requested_by       STRING   respondent email prefix
    survey_timestamp   TIMESTAMP when the survey was filled
    promoters          INT      # of the 8 questions answered >= 4
    number_of_questions INT     always 8
    csat_score         DOUBLE   promoters / number_of_questions (0-1, per response)
"""

from __future__ import annotations

import pandas as pd

# The 8 CSAT questions (each scored 1-5; promoter if >= 4).
_QUESTION_COLS: tuple[str, ...] = (
    "facilidad",
    "comprension",
    "comunicacion",
    "calidad",
    "tiempo",
    "manejo_de_cambios",
    "expectativas",
    "aportacion_estrategica",
)

NUMBER_OF_QUESTIONS: int = len(_QUESTION_COLS)  # 8
PROMOTER_THRESHOLD: int = 4

# Display-form squad labels that map to the 'emi_general' target_squad (legacy).
_EMI_GENERAL_LABELS: tuple[str, ...] = (
    "E.M.I.",
    "GENERAL (CHANNEL SOLUTIONS, PLANNING, SERVICE EXCELLENCE, QA, OPS DEFENSE)",
)


def _as_naive_datetime(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-naive ``datetime64[ns]`` for merge keys."""
    s = pd.to_datetime(series)
    if getattr(s.dt, "tz", None) is not None:
        return s.dt.tz_localize(None)
    return s


def _normalize_target_squad(squad: pd.Series) -> pd.Series:
    """Map the survey's display-form ``squad`` to the roster ``target_squad`` key.

    Matches legacy: 'E.M.I.' and the long 'GENERAL (...)' label both become
    'emi_general'; everything else is lowercased.
    """
    lowered = squad.astype("string").str.lower()
    return lowered.mask(squad.isin(_EMI_GENERAL_LABELS), "emi_general")


def compute_content_csat(
    agent_info: pd.DataFrame,
    csat: pd.DataFrame,
) -> pd.DataFrame:
    """End-to-end content CSAT pipeline (one row per response × content agent)."""
    if csat.empty:
        return pd.DataFrame(
            {c: pd.Series(dtype="object") for c, _ in IO_CONTENT_CSAT_SCHEMA}
        )

    rows = csat.copy()

    # --- per-response promoter count -------------------------------------
    promoter_flags = pd.DataFrame(index=rows.index)
    for col in _QUESTION_COLS:
        score = pd.to_numeric(rows.get(col), errors="coerce")
        promoter_flags[col] = (score >= PROMOTER_THRESHOLD).astype("int64")
    rows["promoters"] = promoter_flags.sum(axis=1).astype("int64")
    rows["number_of_questions"] = NUMBER_OF_QUESTIONS
    rows["csat_score"] = rows["promoters"] / rows["number_of_questions"]

    # --- join key normalization ------------------------------------------
    rows["target_squad"] = _normalize_target_squad(rows["squad"])
    ref = _as_naive_datetime(rows["date_reference"])
    rows["date"] = ref.dt.date
    rows["snapshot_month"] = ref.dt.to_period("M").dt.to_timestamp()
    # Drop the raw display `squad` so it doesn't collide with the roster's `squad`
    # (the agent's roster squad, e.g. 'enablement') on the merge below.
    rows = rows.drop(columns=["squad"])

    # --- fan-out join to the content roster on target_squad --------------
    roster = agent_info.loc[
        (agent_info["status"] == "active")
        & agent_info["target_squad"].notna()
        & (agent_info["target_squad"] != ""),
        [
            "agent",
            "xforce",
            "xplead",
            "team",
            "squad",
            "squad_district",
            "shift",
            "target_squad",
            "snapshot_month",
        ],
    ].copy()
    roster = roster.rename(columns={"squad_district": "district"})
    roster["snapshot_month"] = _as_naive_datetime(roster["snapshot_month"])

    enriched = rows.merge(
        roster, on=["target_squad", "snapshot_month"], how="inner"
    )

    if enriched.empty:
        return pd.DataFrame(
            {c: pd.Series(dtype="object") for c, _ in IO_CONTENT_CSAT_SCHEMA}
        )

    out = enriched[[
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "date",
        "target_squad",
        "requested_by",
        "survey_timestamp",
        "promoters",
        "number_of_questions",
        "csat_score",
    ]].copy()

    return out.sort_values(
        ["date", "target_squad", "agent", "survey_timestamp"]
    ).reset_index(drop=True)


IO_CONTENT_CSAT_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("target_squad", "STRING"),
    ("requested_by", "STRING"),
    ("survey_timestamp", "TIMESTAMP"),
    ("promoters", "INT"),
    ("number_of_questions", "INT"),
    ("csat_score", "DOUBLE"),
)
