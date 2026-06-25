"""shrinkage_slots — one row per DIME slot, flagged for shrinkage.

This is a RAW dataset, not a finished metric. It exposes every DIME slot an
active agent was scheduled for, with boolean flags marking whether the slot is
shrinkage (and, if so, whether that shrinkage is controllable or not). A
downstream ``metrics`` layer turns these into the Shrinkage ratio.

Public API
----------
``compute_shrinkage_slots(agent_info, dime)`` returns one row per DIME slot
with ``shrinkage_flag`` / ``controllable_shrinkage_flag`` /
``uncontrollable_shrinkage_flag``.

Source tables (via extractors)
------------------------------
* ``agent_information`` → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``dime_slots``        → ``etl.mx__series_contract.agent_dimensioned_activities``
  (``affiliation = 'nubank'``).

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* DIME: keep slots with ``activity_type_required IS NOT NULL`` only.
* Roster: ``status='active'`` (inner join attaches dimensions / scopes output).

What the flags mean
-------------------
``shrinkage_flag`` uses the legacy slot-level shrinkage rule, which switches
at 2026-03-01:
  * Pre-cutover  (date < 2026-03-01): ``activity_type_required == 'shrinkage'``.
  * Post-cutover (date >= 2026-03-01): ``activity_type_required == 'shrinkage'``
    OR (``activity_type_required == 'dime_invalid_notation'`` AND
    ``dimensioned_activity`` is a meeting/leave annotation — Mouring / Weekly /
    Permiso Medico / Permiso medico / Huddle / Licencia / Vacacion).

Among shrinkage slots, the controllable/uncontrollable split is by
``dimensioned_activity``:
  * ``uncontrollable_shrinkage_flag`` — ``dimensioned_activity`` in
    (``Licencia`` / ``licencia`` / ``SKR_LCNC``); these are leave/licencia,
    outside the operation's control.
  * ``controllable_shrinkage_flag``   — every other shrinkage slot.

Non-shrinkage slots get all three flags = 0. (``controllable`` +
``uncontrollable`` always sums to ``shrinkage_flag``.)

Deferred to the future metrics layer (NOT done here)
----------------------------------------------------
* The required/denominator definition (legacy ``required_slot``: pre-cutover
  drops ``dime_invalid_notation``, post-cutover drops ``time_off``). The raw
  slot universe is here; the metrics layer applies the denominator rule.
* DIME squad / activity-type business exclusions.
* All training/shadowing/maternity/outage manual adjustments.

Output schema (one row per DIME slot)
-------------------------------------
    agent                          STRING
    xforce                         STRING
    xplead                         STRING
    team                           STRING   performance team (from roster; see team_squad_mapping)
    squad                          STRING   roster squad
    district                       STRING   roster district (was ``squad_district``)
    shift                          STRING   roster shift
    date                           DATE
    slot_time                      STRING   local time-of-day "HH:MM:SS" of the slot start
    activity_type_required         STRING
    dimensioned_activity           STRING
    shrinkage_flag                 INT      1 if the slot is shrinkage, else 0
    controllable_shrinkage_flag    INT      1 if shrinkage AND controllable, else 0
    uncontrollable_shrinkage_flag  INT      1 if shrinkage AND uncontrollable, else 0
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from shift_attribution import night_agent_months, shift_start_date

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Roster-level squad exclusions. Currently empty — all squads in scope.
SHRINKAGE_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# dimensioned_activity values that, post-cutover, get re-classified as
# shrinkage when activity_type_required is 'dime_invalid_notation'.
SHRINKAGE_MEETING_LEAVE_DIMENSIONED_ACTIVITIES: tuple[str, ...] = (
    "Mouring",
    "Weekly",
    "Permiso Medico",
    "Permiso medico",
    "Huddle",
    "Licencia",
    "Vacacion",
)

# dimensioned_activity values (lowercased) that mark a shrinkage slot as
# UNCONTROLLABLE (leave / licencia). Everything else that is shrinkage is
# controllable. Matched case-insensitively so 'Licencia' and 'licencia' both
# count.
UNCONTROLLABLE_DIMENSIONED_ACTIVITIES_LOWER: frozenset[str] = frozenset(
    {"licencia", "skr_lcnc"}
)

# Cutover for the shrinkage formula switch.
SHRINKAGE_FORMULA_CUTOVER: date = date(2026, 3, 1)

ACTIVITY_SHRINKAGE = "shrinkage"
ACTIVITY_INVALID_NOTATION = "dime_invalid_notation"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _as_naive_datetime(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-naive ``datetime64[ns]`` for merge keys."""
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        return s.dt.tz_localize(None)
    return s


def _seconds_to_hms(unix_seconds: pd.Series) -> pd.Series:
    """Format a LOCAL unix-seconds Series as a time-of-day string "HH:MM:SS"."""
    if len(unix_seconds) == 0:
        return pd.Series([], dtype="object", index=unix_seconds.index)
    tod = (unix_seconds.astype("int64") % 86400).astype("int64")
    h = (tod // 3600).map("{:02d}".format)
    m = ((tod % 3600) // 60).map("{:02d}".format)
    s = (tod % 60).map("{:02d}".format)
    return h + ":" + m + ":" + s


# ---------------------------------------------------------------------------
# Step 1: DIME filter (minimal — raw slot universe)
# ---------------------------------------------------------------------------


def filter_dime(dime: pd.DataFrame) -> pd.DataFrame:
    """Keep every DIME slot with a non-null ``activity_type_required``."""
    return dime.loc[dime["activity_type_required"].notna()].copy()


# ---------------------------------------------------------------------------
# Step 2: slot flags (the pre/post-cutover shrinkage rule + control split)
# ---------------------------------------------------------------------------


def classify_slots(dime_filtered: pd.DataFrame) -> pd.DataFrame:
    """Tag each slot with shrinkage / controllable / uncontrollable flags.

    Adds three int64 {0,1} columns:
      ``shrinkage_flag`` / ``controllable_shrinkage_flag`` /
      ``uncontrollable_shrinkage_flag``.
    """
    out = dime_filtered.copy()
    if pd.api.types.is_datetime64_any_dtype(out["date"]):
        slot_date = out["date"].dt.date
    else:
        slot_date = out["date"]

    is_post_cutover = slot_date >= SHRINKAGE_FORMULA_CUTOVER
    is_shrinkage_act = out["activity_type_required"] == ACTIVITY_SHRINKAGE
    is_invalid_notation = out["activity_type_required"] == ACTIVITY_INVALID_NOTATION
    is_meeting_leave_dim = out["dimensioned_activity"].isin(
        SHRINKAGE_MEETING_LEAVE_DIMENSIONED_ACTIVITIES
    )

    pre_shrink = is_shrinkage_act & ~is_post_cutover
    post_shrink = is_post_cutover & (
        is_shrinkage_act | (is_invalid_notation & is_meeting_leave_dim)
    )
    shrinkage_flag = (pre_shrink | post_shrink)

    is_uncontrollable_dim = (
        out["dimensioned_activity"]
        .astype("string")
        .str.lower()
        .isin(UNCONTROLLABLE_DIMENSIONED_ACTIVITIES_LOWER)
        .fillna(False)
    )

    out["shrinkage_flag"] = shrinkage_flag.astype("int64")
    out["uncontrollable_shrinkage_flag"] = (
        shrinkage_flag & is_uncontrollable_dim
    ).astype("int64")
    out["controllable_shrinkage_flag"] = (
        shrinkage_flag & ~is_uncontrollable_dim
    ).astype("int64")

    return out


# ---------------------------------------------------------------------------
# Step 3: orchestrator — roster join (one row per slot)
# ---------------------------------------------------------------------------


def compute_shrinkage_slots(
    agent_info: pd.DataFrame,
    dime: pd.DataFrame,
) -> pd.DataFrame:
    """End-to-end shrinkage_slots pipeline (one row per DIME slot)."""
    dime_f = filter_dime(dime)
    classified = classify_slots(dime_f)
    classified["slot_time"] = _seconds_to_hms(classified["slot_start_local_unix"])

    # Night-shift agents that cross midnight are re-attributed to the day their
    # shift started (>= 2026-07-01 only). Done after the shrinkage-flag
    # classification (whose 2026-03-01 formula switch keys off the calendar
    # date) and while the local slot unix is still available.
    night_months = night_agent_months(agent_info)
    classified["_local_ts"] = pd.to_datetime(
        classified["slot_start_local_unix"], unit="s"
    )
    classified["date"] = shift_start_date(
        classified,
        agent_col="agent",
        local_ts_col="_local_ts",
        calendar_date_col="date",
        night_months=night_months,
    )
    classified = classified.drop(columns="_local_ts")

    # DIME carries its own ``squad`` column which would collide with the
    # roster's ``squad`` on merge. Output uses the roster squad, so keep only
    # the columns we need from the DIME side before joining.
    classified = classified[
        [
            "agent",
            "date",
            "slot_time",
            "activity_type_required",
            "dimensioned_activity",
            "shrinkage_flag",
            "controllable_shrinkage_flag",
            "uncontrollable_shrinkage_flag",
        ]
    ].copy()

    # --- roster join --------------------------------------------------------
    roster = agent_info.loc[
        (agent_info["status"] == "active")
        & ~agent_info["squad"].isin(SHRINKAGE_OUT_OF_SCOPE_SQUADS),
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

    classified["snapshot_month"] = _as_naive_datetime(
        pd.to_datetime(classified["date"]).dt.to_period("M").dt.to_timestamp()
    )
    enriched = classified.merge(roster, on=["agent", "snapshot_month"], how="inner")

    out = enriched[[
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "date",
        "slot_time",
        "activity_type_required",
        "dimensioned_activity",
        "shrinkage_flag",
        "controllable_shrinkage_flag",
        "uncontrollable_shrinkage_flag",
    ]].copy()

    for col in (
        "shrinkage_flag",
        "controllable_shrinkage_flag",
        "uncontrollable_shrinkage_flag",
    ):
        out[col] = out[col].astype("int64")

    return out.sort_values(["date", "agent", "slot_time"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_shrinkage_slots.py
# ---------------------------------------------------------------------------

IO_SHRINKAGE_SLOTS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("date", "DATE"),
    ("slot_time", "STRING"),
    ("activity_type_required", "STRING"),
    ("dimensioned_activity", "STRING"),
    ("shrinkage_flag", "INT"),
    ("controllable_shrinkage_flag", "INT"),
    ("uncontrollable_shrinkage_flag", "INT"),
)
