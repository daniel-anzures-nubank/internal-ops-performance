"""adherent_time — raw per-slot adherent/required minutes (one row per DIME slot).

This is a RAW dataset, not a finished metric. It exposes, for every DIME
slot an active agent was scheduled for, how many minutes the agent was
"adherent" (connected/working per productivity) during that slot and how
long the slot was. A downstream ``metrics`` layer turns these into the
Adherence ratio (and applies the business exclusions — see below).

Public API
----------
``compute_adherent_time(agent_info, dime, productivity)`` returns one row
per (agent, date, slot) with ``adherent_minutes`` and ``required_minutes``.

Source tables (via extractors)
------------------------------
* ``agent_information``  → ``etl.mx__series_contract.cx_mx_bdx_snapshots``
  (+ ``ops_actors``). Roster: agent, xforce, xplead, squad, district, shift,
  status, snapshot_month.
* ``dime_slots``         → ``etl.mx__series_contract.agent_dimensioned_activities``
  (``affiliation = 'nubank'``). One row per 30-min DIME slot.
* ``productivity``       → agent productivity / status log (UTC timestamps).

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* DIME: keep slots with ``activity_type_required IS NOT NULL`` only.
* Productivity: keep "connected" rows (the legacy ``agent_productivity``
  WHERE filter — see ``filter_productivity``).
* Roster: ``status = 'active'`` (inner join attaches the dimensions and
  scopes output to active agents).

Filters deferred to the future metrics layer (NOT applied here)
---------------------------------------------------------------
* Activity-type exclusions (``lunch_break`` / ``time_off`` / ``shrinkage``).
* ``dimensioned_activity`` exclusions (Mouring / Weekly / Permiso Medico /
  Permiso medico / Huddle / Licencia / Vacacion).
* DIME squad exclusions (``wfm`` / ``credit_evolution`` / ``dote`` / …).
* All Category C/D/E/H manual adjustments (agent-date carve-outs, training/
  shadowing windows, maternity leave, outage dates).

Output schema (one row per agent per DIME slot)
-----------------------------------------------
    agent                    STRING
    xforce                   STRING
    xplead                   STRING
    team                     STRING   performance team (from roster; see team_squad_mapping)
    squad                    STRING   roster squad
    district                 STRING   roster district (was ``squad_district``)
    shift                    STRING   roster shift
    date                     DATE
    slot_time                STRING   local time-of-day "HH:MM:SS" of the slot start
    activity_type_required   STRING   DIME activity type for the slot
    required_minutes         DOUBLE   slot length in minutes (always 30.0)
    adherent_minutes         DOUBLE   adherent minutes in the slot (= adherent seconds / 60)
"""

from __future__ import annotations

import pandas as pd

from shift_attribution import night_agent_months, shift_start_date

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Roster-level squad exclusions. Currently empty — all squads (including
# social-media and content) are in scope. Kept so a future narrower scope
# is a one-line edit.
CORE_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# Mexico City has been UTC-6 with no DST since 2022, so a fixed offset is
# correct for any data this project will ever process. DIME stores slot start
# times as a local-time timestamp; productivity stores UTC. We push DIME
# forward by 6h to compare.
MEXICO_UTC_OFFSET_SECONDS: int = 6 * 60 * 60

# DIME slot length: 30 minutes. Used as the per-slot cap on adherent time
# and as the per-slot required minutes.
SLOT_DURATION_SECONDS: int = 30 * 60


# ---------------------------------------------------------------------------
# Step 1: filter DIME (minimal — raw slot universe)
# ---------------------------------------------------------------------------


def filter_dime(dime: pd.DataFrame) -> pd.DataFrame:
    """Keep every DIME slot with a non-null ``activity_type_required``.

    This is the raw slot universe: no activity-type / dimensioned-activity /
    squad business exclusions (those move to the metrics layer).

    Adds two computed columns used by the overlap join:
      * ``slot_start``: UTC unix = ``slot_start_local_unix + 6h``
      * ``slot_end``:   UTC unix = ``slot_end_local_unix   + 6h``
    """
    mask = dime["activity_type_required"].notna()
    out = dime.loc[mask].copy()
    out["slot_start"] = out["slot_start_local_unix"] + MEXICO_UTC_OFFSET_SECONDS
    out["slot_end"] = out["slot_end_local_unix"] + MEXICO_UTC_OFFSET_SECONDS
    return out


# ---------------------------------------------------------------------------
# Step 2: filter productivity
# ---------------------------------------------------------------------------

# After Jan 22, 2026 the staffing-hero status backfill stopped, so the legacy
# explicitly trusts productivity rows even when status is NULL. The constant
# is tz-aware UTC so it compares cleanly against the warehouse's tz-aware
# `agent_productivity.timestamp` column (which arrives as datetime64[us, Etc/UTC]).
NULL_STATUS_TRUST_DATE = pd.Timestamp("2026-01-22", tz="UTC")


def _as_utc(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-aware UTC.

    Warehouse `TIMESTAMP` columns arrive tz-aware (UTC); hand-built test
    frames are usually tz-naive. We normalize both to UTC so downstream
    comparisons against `NULL_STATUS_TRUST_DATE` work in either world.
    """
    s = pd.to_datetime(series)
    if s.dt.tz is None:
        return s.dt.tz_localize("UTC")
    return s.dt.tz_convert("UTC")


def _as_naive_datetime(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-naive ``datetime64[ns]``.

    Used for merge keys: pandas refuses to merge tz-naive against tz-aware
    columns. The warehouse returns `DATE_TRUNC('month', ...)` as tz-aware
    UTC even though we documented `snapshot_month` as a DATE — stripping
    the tz here keeps `(agent, snapshot_month)` joins predictable across
    warehouse data and hand-built test frames.
    """
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        return s.dt.tz_localize(None)
    return s


def _seconds_to_hms(unix_seconds: pd.Series) -> pd.Series:
    """Format a unix-seconds Series as a local time-of-day string "HH:MM:SS".

    Takes the seconds-into-day (``unix % 86400``) and renders zero-padded
    ``HH:MM:SS``. Callers must pass a LOCAL-time unix (so the time-of-day is
    the wall-clock slot time, not UTC). Standardized across adherent_time,
    occupancy_time and shrinkage_slots so the slot column has an identical
    shape in every table.
    """
    if len(unix_seconds) == 0:
        return pd.Series([], dtype="object", index=unix_seconds.index)
    tod = (unix_seconds.astype("int64") % 86400).astype("int64")
    h = (tod // 3600).map("{:02d}".format)
    m = ((tod % 3600) // 60).map("{:02d}".format)
    s = (tod % 60).map("{:02d}".format)
    return h + ":" + m + ":" + s


def filter_productivity(prod: pd.DataFrame) -> pd.DataFrame:
    """Apply the legacy `agent_productivity` WHERE filter.

    Keeps a row if ANY of:
      * `inferred_status IN ('available', 'oos', 'training')`
      * `inferred_status == 'pause' AND level_3 == 'paused_with_jobs'`
      * `active_jobs > 0`
      * `timestamp >= 2026-01-22 AND inferred_status IS NULL`

    The legacy WHERE has a known operator-precedence quirk where the
    `AND timestamp >= '2025-01-01'` only binds to the last OR clause; that
    bound is also redundant once the period filter is applied upstream, so
    we drop it.
    """
    inferred = prod["inferred_status"]
    timestamp = _as_utc(prod["timestamp"])
    active_jobs = prod["active_jobs"].fillna(0)

    mask = (
        inferred.isin(["available", "oos", "training"])
        | ((inferred == "pause") & (prod["level_3"] == "paused_with_jobs"))
        | (active_jobs > 0)
        | ((timestamp >= NULL_STATUS_TRUST_DATE) & inferred.isna())
    )
    return prod.loc[mask].copy()


# ---------------------------------------------------------------------------
# Step 3: per-slot adherent seconds (the overlap join + math)
# ---------------------------------------------------------------------------


def compute_slot_adherence(
    slots: pd.DataFrame, productivity: pd.DataFrame
) -> pd.DataFrame:
    """For each DIME slot, sum adherent seconds and cap at 1800.

    Algorithm:
      1. Per agent, build the cartesian product (slot × productivity) and
         filter to pairs that temporally overlap.
         Overlap rule (equivalent to the legacy 3-condition disjunction):
             ``activity_end >= slot_start AND activity_start < slot_end``
      2. Per pair, ``overlap = LEAST(end) - GREATEST(start)``, clipped to
         [0, 1800].
      3. Per slot, sum overlaps and clip at 1800. Slots that matched nothing
         get 0 (LEFT-JOIN semantics — required so unworked-but-scheduled
         slots still appear with adherent_minutes = 0).

    Returns one row per (agent, date, slot_start, activity_type_required)
    with the additional column `adherent_time_final` (int seconds).
    """
    SLOT_KEYS = ["agent", "date", "slot_start", "activity_type_required"]
    all_slots = slots[SLOT_KEYS].drop_duplicates().reset_index(drop=True)

    common_agents = set(slots["agent"].unique()) & set(productivity["agent"].unique())

    pair_frames: list[pd.DataFrame] = []
    if common_agents:
        slots_by_agent = slots.groupby("agent")
        prod_by_agent = productivity.groupby("agent")
        for agent in common_agents:
            s = slots_by_agent.get_group(agent)
            p = prod_by_agent.get_group(agent)
            # Cartesian within this agent — bounded by ~slots × ~prod_rows.
            merged = s.merge(p, on="agent", suffixes=("", "_prod"))
            # Overlap rule (single-line equivalent of the 3-OR legacy form).
            ok = (merged["activity_end_unix"] >= merged["slot_start"]) & (
                merged["activity_start_unix"] < merged["slot_end"]
            )
            merged = merged.loc[ok]
            if merged.empty:
                continue
            # Per-pair overlap seconds, clipped to [0, 1800].
            overlap = (
                merged[["activity_end_unix", "slot_end"]].min(axis=1)
                - merged[["activity_start_unix", "slot_start"]].max(axis=1)
            ).clip(lower=0, upper=SLOT_DURATION_SECONDS)
            merged = merged.assign(overlap_seconds=overlap)
            pair_frames.append(merged[SLOT_KEYS + ["overlap_seconds"]])

    if pair_frames:
        pairs = pd.concat(pair_frames, ignore_index=True)
        per_slot = (
            pairs.groupby(SLOT_KEYS, as_index=False)["overlap_seconds"]
            .sum()
            .rename(columns={"overlap_seconds": "adherent_time_final"})
        )
        per_slot["adherent_time_final"] = per_slot["adherent_time_final"].clip(
            upper=SLOT_DURATION_SECONDS
        )
    else:
        per_slot = pd.DataFrame(
            {**{k: pd.Series(dtype=all_slots[k].dtype) for k in SLOT_KEYS},
             "adherent_time_final": pd.Series(dtype="float64")}
        )

    out = all_slots.merge(per_slot, on=SLOT_KEYS, how="left")
    out["adherent_time_final"] = (
        out["adherent_time_final"].fillna(0).astype("int64")
    )
    return out


# ---------------------------------------------------------------------------
# Step 4: orchestrator — join roster + per-slot output
# ---------------------------------------------------------------------------


def compute_adherent_time(
    agent_info: pd.DataFrame,
    dime: pd.DataFrame,
    productivity: pd.DataFrame,
) -> pd.DataFrame:
    """End-to-end pipeline: extractor frames in, raw per-slot result out.

    The intermediate steps are each exposed as standalone functions above so
    they can be unit-tested in isolation.
    """
    dime_f = filter_dime(dime)
    prod_f = filter_productivity(productivity)
    slot_adherence = compute_slot_adherence(dime_f, prod_f)

    # Night-shift agents that cross midnight are re-attributed to the day their
    # shift started (>= 2026-07-01 only). `slot_start` here is UTC unix
    # (filter_dime added +6h), so subtract the offset to recover the local
    # slot timestamp before re-attributing.
    night_months = night_agent_months(agent_info)
    slot_adherence["_local_ts"] = pd.to_datetime(
        slot_adherence["slot_start"] - MEXICO_UTC_OFFSET_SECONDS, unit="s"
    )
    slot_adherence["date"] = shift_start_date(
        slot_adherence,
        agent_col="agent",
        local_ts_col="_local_ts",
        calendar_date_col="date",
        night_months=night_months,
    )
    slot_adherence = slot_adherence.drop(columns="_local_ts")

    # Roster: active agents only. `CORE_OUT_OF_SCOPE_SQUADS` is currently
    # empty so this `.isin(())` filter is effectively a no-op.
    roster = agent_info.loc[
        (agent_info["status"] == "active")
        & ~agent_info["squad"].isin(CORE_OUT_OF_SCOPE_SQUADS),
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

    # Attribute each slot's date to a calendar month, then join to that
    # month's roster snapshot. Both sides normalized to tz-naive
    # datetime64[ns] so pandas allows the merge.
    slot_dates = pd.to_datetime(slot_adherence["date"])
    slot_adherence = slot_adherence.assign(
        snapshot_month=_as_naive_datetime(
            slot_dates.dt.to_period("M").dt.to_timestamp()
        )
    )
    roster["snapshot_month"] = _as_naive_datetime(roster["snapshot_month"])

    enriched = slot_adherence.merge(
        roster, on=["agent", "snapshot_month"], how="inner"
    )

    # `slot_start` here is UTC (filter_dime added +6h); shift back to local
    # before deriving the wall-clock time-of-day.
    enriched["slot_time"] = _seconds_to_hms(
        enriched["slot_start"] - MEXICO_UTC_OFFSET_SECONDS
    )
    enriched["adherent_minutes"] = (
        enriched["adherent_time_final"].astype("float64") / 60.0
    )
    # Each row is exactly one DIME slot, so required time is the slot length.
    enriched["required_minutes"] = float(SLOT_DURATION_SECONDS) / 60.0

    out = enriched[
        [
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
            "required_minutes",
            "adherent_minutes",
        ]
    ].copy()
    out["required_minutes"] = out["required_minutes"].astype("float64")
    out["adherent_minutes"] = out["adherent_minutes"].astype("float64")

    return out.sort_values(["date", "agent", "slot_time"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Output schema declaration — used by the writer in scripts/metrics_data_scripts/build_adherent_time.py
# ---------------------------------------------------------------------------

IO_ADHERENT_TIME_SCHEMA: tuple[tuple[str, str], ...] = (
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
    ("required_minutes", "DOUBLE"),
    ("adherent_minutes", "DOUBLE"),
)
