"""occupancy_time — raw per-slot occupancy/required minutes (one row per DIME slot).

This is a RAW dataset, not a finished metric. It is the occupancy twin of
``adherent_time``: for every DIME slot an active agent was scheduled for, how
many minutes the agent spent actually working jobs whose ``activity_type``
matches the slot's ``activity_type_required``, and how long the slot was. A
downstream ``metrics`` layer turns these into the Normalized-Occupancy ratio
and its district/shift benchmark.

Public API
----------
``compute_occupancy_time(agent_info, dime, shuffle_jobs, oos_jobs, sm_jobs=None)``
returns one row per (agent, date, slot) with ``occupancy_minutes`` and
``required_minutes``.

Source tables (via extractors)
------------------------------
* ``agent_information``  → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``dime_slots``         → ``etl.mx__series_contract.agent_dimensioned_activities``
  (``affiliation = 'nubank'``).
* ``shuffle_jobs``       → ``etl.mx__dataset.ops_canonical_time_spent_activities``
  (``actor_affiliation = 'nubank'``).
* ``oos_jobs``           → ``etl.mx__dataset.taskmaster_consolidated_registry``.
* ``sm_jobs`` (optional) → ``usr.sprinklr_api_data_integration.sprinklr_normalized_occupancy_data``.
  Social-Media case assignments — the occupancy source for social agents, who do
  not appear in the shuffle / taskmaster job tables. Unioned in as ``oos`` jobs.

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* DIME: keep slots with ``activity_type_required IS NOT NULL`` only.
* DIME systemic reclassifications are KEPT (they are part of the occupancy
  matching logic, not a business exclusion):
    - ``dimensioned_activity`` in ('Control MC', 'xMC Debit Fraud') →
      ``activity_type_required = 'oos'``
    - ``activity_type_required == 'dime_invalid_notation'`` →
      ``activity_type_required = 'oos'``
* Jobs: shuffle ``status IN ('finished', 'transferred', 'skipped')`` (NOcc
  counts attempted work, wider than NTPJ's 'finished'); OOS and SM rows get a
  synthetic ``activity_type = 'oos'``.
* Approved raw-data correction from ``Correcciones Generales Datos``:
  ``luis.contreras`` Content Taskmaster/OOS job timestamps are shifted forward
  before overlap math (+2h through 2026-03-08, +1h from 2026-03-09 to
  2026-05-19).
* Roster: ``status = 'active'`` (inner join attaches dimensions / scopes output).

Filters deferred to the future metrics layer (NOT applied here)
---------------------------------------------------------------
* Activity-type exclusions (``lunch_break`` / ``time_off`` / ``shrinkage``).
* ``dimensioned_activity`` exclusions (Mouring / Weekly / Permiso Medico /
  Permiso medico / Huddle / Licencia / Vacacion).
* DIME squad exclusions (``wfm`` / ``credit_evolution`` / ``dote`` / …).
* The monthly district/shift occupancy benchmark (``occupancy_exp``) — that is
  a metric-layer computation, removed from this raw table.
* All per-agent manual adjustments / outage-date carve-outs.

Why the occupancy calc needs interval dedup
--------------------------------------------
A single slot can have multiple overlapping jobs of the same activity type
(an agent juggling two chats). Naively summing per-job overlaps would
double-count the overlapping portion. We merge overlapping same-activity
intervals with the classic ``prev_max_end`` running-max trick (legacy used a
window function).

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
    activity_type_required   STRING   DIME activity type (after systemic reclassification)
    required_minutes         DOUBLE   slot length in minutes (always 30.0)
    occupancy_minutes        DOUBLE   minutes occupied (= occupancy seconds / 60), <= 30
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from shift_attribution import night_agent_months, shift_start_date

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Systemic activity-type reclassifications (kept; NOT manual adjustments).
DIMENSIONED_ACTIVITY_TO_OOS: tuple[str, ...] = ("Control MC", "xMC Debit Fraud")
DIME_INVALID_NOTATION_VALUE: str = "dime_invalid_notation"

# Roster-level squad exclusions. Currently empty — all squads in scope.
NOCC_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# Shuffle status filter: occupancy counts work the agent ATTEMPTED, not just
# work that succeeded. So we keep transferred/skipped in addition to
# 'finished'. (NTPJ, which measures throughput, uses only 'finished'.)
SHUFFLE_OCCUPIED_STATUSES: tuple[str, ...] = ("finished", "transferred", "skipped")

# Slot duration — 30 minutes.
SLOT_DURATION_SECONDS: int = 30 * 60

LUIS_CONTRERAS_AGENT = "luis.contreras"


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def _as_naive_datetime(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-naive ``datetime64[ns]``.

    Strips warehouse-returned UTC tz off month-truncated timestamps so
    snapshot_month joins behave predictably across hand-built test frames
    and real warehouse data.
    """
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        return s.dt.tz_localize(None)
    return s


def _seconds_to_hms(unix_seconds: pd.Series) -> pd.Series:
    """Format a LOCAL unix-seconds Series as a time-of-day string "HH:MM:SS".

    Renders the seconds-into-day (``unix % 86400``) as zero-padded
    ``HH:MM:SS``. occupancy_time's ``slot_start`` is already local-time unix,
    so the result is the wall-clock slot time. Standardized with
    adherent_time so the slot column has an identical shape in both tables.
    """
    if len(unix_seconds) == 0:
        return pd.Series([], dtype="object", index=unix_seconds.index)
    tod = (unix_seconds.astype("int64") % 86400).astype("int64")
    h = (tod // 3600).map("{:02d}".format)
    m = ((tod % 3600) // 60).map("{:02d}".format)
    s = (tod % 60).map("{:02d}".format)
    return h + ":" + m + ":" + s


def _apply_luis_contreras_oos_timestamp_correction(
    oos_jobs: pd.DataFrame,
) -> pd.DataFrame:
    """Shift approved Content OOS job timestamps for luis.contreras.

    Source: `Correcciones Generales Datos`.
    His laptop clock lagged behind Taskmaster during H1 2026, so the recorded
    local start/stop times must move forward before NOCC overlap calculations.
    This correction is deliberately hardcoded because the tab is not a scalable
    rules table yet.
    """
    if oos_jobs.empty:
        return oos_jobs.copy()

    out = oos_jobs.copy()
    start_date = pd.to_datetime(out["local_start_date"])
    agent = out["agent"].astype("string").str.lower()
    squad = out["squad"].astype("string").str.lower()
    is_luis_content_oos = (
        (agent == LUIS_CONTRERAS_AGENT)
        & squad.str.contains("content", regex=False, na=False)
    )

    correction_hours = pd.Series(0, index=out.index, dtype="int64")
    correction_hours = correction_hours.mask(
        is_luis_content_oos
        & start_date.dt.date.between(
            pd.Timestamp("2026-01-01").date(),
            pd.Timestamp("2026-03-08").date(),
        ),
        2,
    )
    correction_hours = correction_hours.mask(
        is_luis_content_oos
        & start_date.dt.date.between(
            pd.Timestamp("2026-03-09").date(),
            pd.Timestamp("2026-05-19").date(),
        ),
        1,
    )
    needs_correction = correction_hours > 0
    if not needs_correction.any():
        return out

    delta = pd.to_timedelta(correction_hours, unit="h")
    out.loc[needs_correction, "local_start_date"] = (
        pd.to_datetime(out.loc[needs_correction, "local_start_date"])
        + delta.loc[needs_correction]
    )
    out.loc[needs_correction, "local_stop_date"] = (
        pd.to_datetime(out.loc[needs_correction, "local_stop_date"])
        + delta.loc[needs_correction]
    )
    out.loc[needs_correction, "activity_start_unix"] = (
        pd.to_numeric(out.loc[needs_correction, "activity_start_unix"], errors="coerce")
        + correction_hours.loc[needs_correction] * 3600
    )
    out.loc[needs_correction, "activity_end_unix"] = (
        pd.to_numeric(out.loc[needs_correction, "activity_end_unix"], errors="coerce")
        + correction_hours.loc[needs_correction] * 3600
    )
    out.loc[needs_correction, "date"] = pd.to_datetime(
        out.loc[needs_correction, "local_start_date"]
    ).dt.date
    return out


# ---------------------------------------------------------------------------
# Step 1: DIME filter + systemic activity-type reclassifications
# ---------------------------------------------------------------------------


def filter_dime(dime: pd.DataFrame) -> pd.DataFrame:
    """Keep raw slots and apply occupancy's systemic reclassifications.

    Drops only:
      * NULL ``activity_type_required``

    Then applies two systemic reclassifications (NOT manual adjustments) so
    the job-matching logic is identical to the legacy NOcc dataset:
      * ``dimensioned_activity`` in ('Control MC', 'xMC Debit Fraud') →
        ``activity_type_required = 'oos'``
      * ``activity_type_required == 'dime_invalid_notation'`` →
        ``activity_type_required = 'oos'``

    Business exclusions (activity-type / dimensioned_activity / squad) are
    NOT applied here — they move to the metrics layer.
    """
    mask = dime["activity_type_required"].notna()
    out = dime.loc[mask].copy()

    is_fraud_oos = out["dimensioned_activity"].isin(DIMENSIONED_ACTIVITY_TO_OOS)
    is_invalid_notation = out["activity_type_required"] == DIME_INVALID_NOTATION_VALUE
    out.loc[is_fraud_oos | is_invalid_notation, "activity_type_required"] = "oos"

    return out


# ---------------------------------------------------------------------------
# Step 2: union shuffle + OOS jobs
# ---------------------------------------------------------------------------


def build_jobs_union(
    shuffle_jobs: pd.DataFrame,
    oos_jobs: pd.DataFrame,
    sm_jobs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Concatenate shuffle, OOS, and Social-Media jobs into one overlap frame.

    * Shuffle: filter to ``status IN ('finished', 'transferred', 'skipped')``.
    * OOS: synthesize ``activity_type='oos'`` (taskmaster has no activity_type).
    * SM (Sprinklr ``sm_jobs``): each social case assignment is an occupancy
      interval; synthesize ``activity_type='oos'`` so it matches DIME slots whose
      ``activity_type_required='oos'`` — the social-agent equivalent of OOS work,
      exactly as the legacy SM notebook treats it.

    Output columns:
        agent STRING, date DATE, activity_type STRING,
        activity_start_unix BIGINT, activity_end_unix BIGINT
    """
    cols = ["agent", "date", "activity_type", "activity_start_unix", "activity_end_unix"]

    def _empty() -> pd.DataFrame:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    if shuffle_jobs.empty:
        shuffle_part = _empty()
    else:
        s = shuffle_jobs.loc[
            shuffle_jobs["status"].isin(SHUFFLE_OCCUPIED_STATUSES)
        ].copy()
        shuffle_part = s[cols]

    if oos_jobs.empty:
        oos_part = _empty()
    else:
        o = _apply_luis_contreras_oos_timestamp_correction(oos_jobs)
        o["activity_type"] = "oos"
        oos_part = o[cols]

    if sm_jobs is None or sm_jobs.empty:
        sm_part = _empty()
    else:
        m = sm_jobs.copy()
        m["activity_type"] = "oos"
        sm_part = m[cols]

    return pd.concat([shuffle_part, oos_part, sm_part], ignore_index=True)


# ---------------------------------------------------------------------------
# Step 3: per-slot occupancy (the overlap join + interval dedup math)
# ---------------------------------------------------------------------------


def compute_slot_occupancy(
    dime_filtered: pd.DataFrame, jobs: pd.DataFrame
) -> pd.DataFrame:
    """For each DIME slot, sum occupied seconds and cap at 1800.

    Algorithm (mirrors the legacy ``slot_jobs`` -> ``occupancy_base`` ->
    ``occupancy_agg`` chain):

      1. Per agent, build (slot × job) pairs and keep only temporally
         overlapping ones: ``job_end > slot_start AND job_start < slot_end``.
      2. Clip each job to the slot bounds.
      3. Within partition ``(slot_keys, job.activity_type)``, sort by
         ``(cjob_start, cjob_end)`` and compute the running max of previous
         ``cjob_end`` (``prev_max_end``; slot's first row → slot_start).
      4. For each job, ``contribution = activity_occuped × max(0,
         cjob_end - max(cjob_start, prev_max_end))`` where
         ``activity_occuped = 1`` iff
         ``slot.activity_type_required == job.activity_type``.
      5. Sum contributions per slot. Cap at 1800. LEFT-JOIN semantics: slots
         with no matching job keep ``occupancy_time = 0``.

    Returns one row per (agent, squad, date, slot_start, slot_end,
    activity_type_required) with ``occupancy_time`` (int seconds).
    """
    SLOT_KEYS = [
        "agent",
        "squad",
        "date",
        "slot_start",
        "slot_end",
        "activity_type_required",
    ]

    if dime_filtered.empty:
        return pd.DataFrame(
            {**{k: pd.Series(dtype=dime_filtered[k].dtype if k in dime_filtered.columns else "object")
                for k in SLOT_KEYS},
             "occupancy_time": pd.Series(dtype="int64")}
        )

    slots = (
        dime_filtered.rename(
            columns={
                "slot_start_local_unix": "slot_start",
                "slot_end_local_unix": "slot_end",
            }
        )[SLOT_KEYS]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    common_agents = set(slots["agent"].unique()) & set(jobs["agent"].unique())

    pair_frames: list[pd.DataFrame] = []
    if common_agents:
        slots_by_agent = slots.groupby("agent")
        jobs_by_agent = jobs.groupby("agent")
        for agent in common_agents:
            s = slots_by_agent.get_group(agent)
            j = jobs_by_agent.get_group(agent)
            m = s.merge(j, on=["agent", "date"], suffixes=("", "_j"))
            if m.empty:
                continue
            # Half-open overlap rule, vectorized
            ok = (m["activity_end_unix"] > m["slot_start"]) & (
                m["activity_start_unix"] < m["slot_end"]
            )
            m = m.loc[ok].copy()
            if m.empty:
                continue
            # Clip job interval to slot bounds
            m["cjob_start"] = m[["slot_start", "activity_start_unix"]].max(axis=1)
            m["cjob_end"] = m[["slot_end", "activity_end_unix"]].min(axis=1)
            # Activity match
            m["activity_occuped"] = (
                m["activity_type_required"] == m["activity_type"]
            ).astype("int64")
            # Interval-dedup: cumulative max of cjob_end within partition,
            # shifted by one row, defaulting to slot_start.
            partition_keys = SLOT_KEYS + ["activity_type"]
            m = m.sort_values(partition_keys + ["cjob_start", "cjob_end"]).reset_index(
                drop=True
            )
            m["prev_max_end"] = m.groupby(partition_keys, dropna=False)[
                "cjob_end"
            ].transform(lambda x: x.cummax().shift(1))
            m["prev_max_end"] = np.where(
                m["prev_max_end"].isna(),
                m["slot_start"],
                m["prev_max_end"],
            )
            # Contribution
            effective_start = m[["cjob_start", "prev_max_end"]].max(axis=1)
            m["contribution"] = np.where(
                m["activity_occuped"] == 1,
                (m["cjob_end"] - effective_start).clip(lower=0),
                0,
            )
            pair_frames.append(m[SLOT_KEYS + ["contribution"]])

    if pair_frames:
        pairs = pd.concat(pair_frames, ignore_index=True)
        per_slot = (
            pairs.groupby(SLOT_KEYS, as_index=False, dropna=False)["contribution"]
            .sum()
            .rename(columns={"contribution": "occupancy_time"})
        )
        per_slot["occupancy_time"] = per_slot["occupancy_time"].clip(
            upper=SLOT_DURATION_SECONDS
        )
    else:
        per_slot = pd.DataFrame(
            {**{k: pd.Series(dtype=slots[k].dtype) for k in SLOT_KEYS},
             "occupancy_time": pd.Series(dtype="float64")}
        )

    # LEFT-JOIN to keep slots with no matching job (occupancy_time = 0).
    out = slots.merge(per_slot, on=SLOT_KEYS, how="left")
    out["occupancy_time"] = pd.to_numeric(
        out["occupancy_time"], errors="coerce"
    ).fillna(0).astype("int64")
    return out


# ---------------------------------------------------------------------------
# Step 4: orchestrator — roster join + final shape
# ---------------------------------------------------------------------------


def compute_occupancy_time(
    agent_info: pd.DataFrame,
    dime: pd.DataFrame,
    shuffle_jobs: pd.DataFrame,
    oos_jobs: pd.DataFrame,
    sm_jobs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """End-to-end occupancy_time pipeline (raw per-slot occupancy minutes).

    ``sm_jobs`` (Sprinklr ``sm_jobs`` extractor) is optional: when provided, the
    Social-Media case assignments are unioned in as ``oos``-typed jobs so social
    agents' occupancy is populated from Sprinklr (they have no shuffle/taskmaster
    jobs). It is harmless for non-social agents (no SM jobs match them).
    """
    # --- DIME side ----------------------------------------------------------
    dime_f = filter_dime(dime)

    # --- jobs side ----------------------------------------------------------
    jobs = build_jobs_union(shuffle_jobs, oos_jobs, sm_jobs)

    # --- per-slot occupancy -------------------------------------------------
    per_slot = compute_slot_occupancy(dime_f, jobs)
    # The DIME ``squad`` was only needed for the per-slot interval-dedup
    # partition; drop it before the roster join to avoid colliding with the
    # roster's own ``squad``.
    per_slot = per_slot.drop(columns=["squad"])

    # Night-shift agents that cross midnight are re-attributed to the day their
    # shift started (>= 2026-07-01 only). `slot_start` is already local-time
    # unix, so it renders straight to the local slot timestamp.
    night_months = night_agent_months(agent_info)
    per_slot["_local_ts"] = pd.to_datetime(per_slot["slot_start"], unit="s")
    per_slot["date"] = shift_start_date(
        per_slot,
        agent_col="agent",
        local_ts_col="_local_ts",
        calendar_date_col="date",
        night_months=night_months,
    )
    per_slot = per_slot.drop(columns="_local_ts")

    # --- roster join --------------------------------------------------------
    roster = agent_info.loc[
        (agent_info["status"] == "active")
        & ~agent_info["squad"].isin(NOCC_OUT_OF_SCOPE_SQUADS),
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

    per_slot["snapshot_month"] = _as_naive_datetime(
        pd.to_datetime(per_slot["date"]).dt.to_period("M").dt.to_timestamp()
    )
    enriched = per_slot.merge(roster, on=["agent", "snapshot_month"], how="inner")

    # --- final shape --------------------------------------------------------
    enriched["occupancy_time"] = enriched["occupancy_time"].clip(
        upper=SLOT_DURATION_SECONDS
    )
    # `slot_start` is local-time unix, so it renders straight to wall-clock.
    enriched["slot_time"] = _seconds_to_hms(enriched["slot_start"])
    enriched["occupancy_minutes"] = enriched["occupancy_time"].astype("float64") / 60.0
    enriched["required_minutes"] = float(SLOT_DURATION_SECONDS) / 60.0

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
        "required_minutes",
        "occupancy_minutes",
    ]].copy()

    out["required_minutes"] = out["required_minutes"].astype("float64")
    out["occupancy_minutes"] = out["occupancy_minutes"].astype("float64")

    return out.sort_values(["date", "agent", "slot_time"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_occupancy_time.py
# ---------------------------------------------------------------------------

IO_OCCUPANCY_TIME_SCHEMA: tuple[tuple[str, str], ...] = (
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
    ("occupancy_minutes", "DOUBLE"),
)
