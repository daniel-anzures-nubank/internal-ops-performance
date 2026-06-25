"""quality_evaluations — one row per individual QA evaluation.

This is a RAW dataset, not a finished metric. It exposes every QA evaluation
attributed to an active roster agent, one row per evaluation, with its score. A
downstream ``metrics`` layer averages these into the Quality score at whatever
grain it wants (agent/day, squad/month, …) by re-averaging the raw ``qa_score``
values.

Sources (two, unioned)
----------------------
* **Playvox** (``qmo_playvox_consolidated``) — Quality of record for Core,
  Fraud and Content, and historically for Social Media too.
* **Sprinklr SM** (``social_media_case_summary_information``) — Social-Media
  case QA. Social Media QA is logged against Sprinklr cases, so from the
  ``SPRINKLR_SM_CUTOVER`` (2026-05-01) onward we ``UNION ALL`` this feed on top
  of Playvox. Earlier dates stay Playvox-only (no retroactive change). Both
  feeds report ``qa_score`` on the same 0-100 scale, so they union directly.
  A ``source`` column ('playvox' / 'sprinklr_sm') tags each row's provenance.

Note this differs from legacy: legacy carried the Sprinklr ``UNION ALL`` only in
the Core/Fraud Quality dataset, where it was dead code (the active-roster join
excluded ``social``). Here the new roster keeps social agents, so the Sprinklr
SM rows actually reach output and are scored for Social Media.

Public API
----------
``compute_quality_evaluations(agent_info, playvox, sprinklr_sm=None)`` returns
one row per evaluation. ``sprinklr_sm`` is optional for backwards-compatibility;
when omitted the table is Playvox-only.

Source tables (via extractors)
------------------------------
* ``agent_information``       → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``playvox_evaluations``     → Playvox QA evaluations (one row per evaluation).
* ``sprinklr_sm_evaluations`` → Sprinklr SM case QA (one row per evaluation, >= cutover).

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* Playvox: ``team_name NOT IN ('REGULATORY SOLUTIONS', 'AML')`` and the
  Nubank-MX agent-email regex ``^[a-z]+\\.[a-z]+[0-9]*@nu\\.com\\.mx$``
  (these mirror legacy's source-level ``qa_base`` gate). Sprinklr SM rows are
  NOT run through this Playvox-specific gate; their source-level filtering
  (agent mapping, monitor exclusion, cutover) lives in the extractor.
* Sprinklr SM: a defensive ``date >= SPRINKLR_SM_CUTOVER`` floor (the extractor
  already enforces it; re-applied here so the module is self-contained).
* Roster: ``status='active'`` and non-null ``squad`` (inner join attaches the
  dimensions / scopes output).

Filters deferred to the future metrics layer (NOT applied here)
---------------------------------------------------------------
* The ``scorecard_id`` / ``evaluation_id`` blacklists.
* The hardcoded outage-date exclusions (2026-03-27, 2026-04-09).
* Any narrower squad scoping.

Output schema (one row per evaluation)
--------------------------------------
    agent            STRING
    xforce           STRING
    xplead           STRING
    team             STRING   performance team (from roster; see team_squad_mapping)
    squad            STRING   roster squad
    district         STRING   roster district (was ``squad_district``)
    shift            STRING   roster shift
    date             DATE     calendar day the evaluation was logged (MX local)
    evaluation_id    STRING
    team_name        STRING   source team / scorecard team
    source           STRING   'playvox' | 'sprinklr_sm'
    qa_score         DOUBLE   the evaluation's score
"""

from __future__ import annotations

import re

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PLAYVOX_TEAM_NAME_EXCLUSIONS: tuple[str, ...] = (
    "REGULATORY SOLUTIONS",
    "AML",
)

# Roster-level squad exclusions. Currently empty — all squads in scope.
QUALITY_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# Source tags written to the ``source`` provenance column.
SOURCE_PLAYVOX = "playvox"
SOURCE_SPRINKLR_SM = "sprinklr_sm"

# Social Media only started being scored from Sprinklr in May 2026. Sprinklr SM
# evaluations before this date are dropped so SM quality stays Playvox-only
# historically (no retroactive change). The extractor enforces the same floor;
# this is the module-level defensive guard.
SPRINKLR_SM_CUTOVER = pd.Timestamp("2026-05-01")

# Legacy affiliation regex (Playvox path): lowercase "first.last" with an
# optional trailing integer suffix on the @nu.com.mx domain.
_NUBANK_EMAIL_REGEX = re.compile(r"^[a-z]+\.[a-z]+[0-9]*@nu\.com\.mx$", re.IGNORECASE)


def _is_nubank_email(email: object) -> bool:
    """Return True iff `email` matches the Nubank-MX agent email pattern."""
    if not isinstance(email, str):
        return False
    return _NUBANK_EMAIL_REGEX.match(email) is not None


def _as_naive_datetime(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-naive ``datetime64[ns]`` for merge keys."""
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        return s.dt.tz_localize(None)
    return s


def _floor_to_date(series: pd.Series) -> pd.Series:
    """Truncate a timestamp / date series to ``datetime.date``.

    Playvox's ``created_at`` arrives tz-naive in MX local time; coerce any
    tz-aware values to UTC-naive before flooring to a plain calendar date.
    """
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return s.dt.date


# ---------------------------------------------------------------------------
# Step 1: Playvox-only filter
# ---------------------------------------------------------------------------


def filter_playvox(playvox: pd.DataFrame) -> pd.DataFrame:
    """Apply the Playvox source gate (team_name + Nubank-email regex)."""
    if playvox.empty:
        return playvox.copy()

    mask = ~playvox["team_name"].isin(PLAYVOX_TEAM_NAME_EXCLUSIONS) & playvox[
        "agent_email"
    ].map(_is_nubank_email)
    return playvox.loc[mask].copy()


# ---------------------------------------------------------------------------
# Step 2: build the per-evaluation frame (one row per evaluation)
# ---------------------------------------------------------------------------


_EVAL_COLS: tuple[str, ...] = (
    "evaluation_id",
    "agent",
    "qa_score",
    "team_name",
    "created_at",
)

def build_evaluations(playvox: pd.DataFrame) -> pd.DataFrame:
    """Shape (already-filtered) Playvox rows into per-evaluation rows.

    Result has one row per evaluation with:
        evaluation_id, agent, qa_score, team_name, date
    where ``date`` = floor(created_at) in MX local time.
    """
    if playvox is None or playvox.empty:
        return pd.DataFrame(
            {c: pd.Series(dtype="object") for c in _EVAL_COLS + ("date",)}
        )

    sub = playvox[list(_EVAL_COLS)].copy()
    sub["date"] = _floor_to_date(sub["created_at"])
    sub = sub[sub["agent"].notna() & (sub["agent"] != "")]
    return sub.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 3: orchestrator — roster join (no aggregation)
# ---------------------------------------------------------------------------


def _build_sprinklr_sm(sprinklr_sm: pd.DataFrame | None) -> pd.DataFrame:
    """Shape Sprinklr SM evaluations and enforce the SM cutover floor.

    Returns a frame with the same columns as :func:`build_evaluations` plus a
    ``source`` column. ``None`` / empty input yields an empty frame.
    """
    if sprinklr_sm is None or sprinklr_sm.empty:
        return build_evaluations(None).assign(source=pd.Series(dtype="object"))

    sm_evals = build_evaluations(sprinklr_sm)
    if not sm_evals.empty:
        sm_dates = pd.to_datetime(sm_evals["date"])
        sm_evals = sm_evals.loc[sm_dates >= SPRINKLR_SM_CUTOVER].copy()
    sm_evals["source"] = SOURCE_SPRINKLR_SM
    return sm_evals


def compute_quality_evaluations(
    agent_info: pd.DataFrame,
    playvox: pd.DataFrame,
    sprinklr_sm: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """End-to-end quality_evaluations pipeline (one row per evaluation).

    ``sprinklr_sm`` is the optional Sprinklr SM case-QA feed; when provided, its
    rows on/after :data:`SPRINKLR_SM_CUTOVER` are ``UNION ALL``-ed on top of
    Playvox (tagged ``source='sprinklr_sm'``).
    """
    playvox_f = filter_playvox(playvox)
    playvox_evals = build_evaluations(playvox_f)
    playvox_evals["source"] = SOURCE_PLAYVOX

    sm_evals = _build_sprinklr_sm(sprinklr_sm)

    # Concat only the non-empty parts so empty all-NA frames don't trip the
    # pandas empty-concat dtype FutureWarning (and to keep the union exact).
    parts = [p for p in (playvox_evals, sm_evals) if not p.empty]
    evals = (
        pd.concat(parts, ignore_index=True)
        if parts
        else playvox_evals  # both empty → keep the schema'd empty playvox frame
    )

    if evals.empty:
        return pd.DataFrame(
            {c: pd.Series(dtype="object") for c, _ in IO_QUALITY_EVALUATIONS_SCHEMA}
        )

    # --- roster join --------------------------------------------------------
    roster = agent_info.loc[
        (agent_info["status"] == "active")
        & agent_info["squad"].notna()
        & ~agent_info["squad"].isin(QUALITY_OUT_OF_SCOPE_SQUADS),
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

    evals["snapshot_month"] = _as_naive_datetime(
        pd.to_datetime(evals["date"]).dt.to_period("M").dt.to_timestamp()
    )
    enriched = evals.merge(roster, on=["agent", "snapshot_month"], how="inner")

    enriched["qa_score"] = enriched["qa_score"].astype("float64")

    out = enriched[[
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "date",
        "evaluation_id",
        "team_name",
        "source",
        "qa_score",
    ]].copy()

    return out.sort_values(
        ["date", "agent", "evaluation_id"]
    ).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_quality_evaluations.py
# ---------------------------------------------------------------------------

IO_QUALITY_EVALUATIONS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("evaluation_id", "STRING"),
    ("team_name", "STRING"),
    ("source", "STRING"),
    ("qa_score", "DOUBLE"),
)
