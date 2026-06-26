"""adherent_time — raw per-slot adherent/required minutes (one row per DIME slot, PySpark).

This is a RAW dataset, not a finished metric. It exposes, for every DIME
slot an active agent was scheduled for, how many minutes the agent was
"adherent" (connected/working per productivity) during that slot and how
long the slot was. A downstream ``metrics`` layer turns these into the
Adherence ratio (and applies the business exclusions — see below).

Public API
----------
``compute_adherent_time(agent_info, dime, productivity)`` takes three Spark
DataFrames (the extractor outputs) and returns one Spark DataFrame with one
row per (agent, date, slot) carrying ``adherent_minutes`` and
``required_minutes``.

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
* DIME: keep slots with ``activity_type_required IS NOT NULL`` and
  ``dimensioned_activity`` not in the meeting/leave list (a fixed legacy DIME
  filter — see ``MEETING_LEAVE_DIMENSIONED_ACTIVITIES``).
* Productivity: keep "connected" rows (the legacy ``agent_productivity``
  WHERE filter — see ``filter_productivity``).
* Roster: ``status = 'active'`` (inner join attaches the dimensions and
  scopes output to active agents).

Legacy parity (pre-2026-07-01)
------------------------------
Before the ``LEGACY_PHANTOM_CUTOVER`` (2026-07-01) a slot that matched no
productivity is counted as *fully* adherent (1800s), reproducing the legacy
phantom-adherence bug so historical metrics stay byte-for-byte with legacy.
From the cutover on, an unmatched slot correctly scores 0.

Output schema (one row per agent per DIME slot)
-----------------------------------------------
    agent                    STRING
    xforce                   STRING
    xplead                   STRING
    team                     STRING   performance team (from roster)
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

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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

# Legacy-parity cutover. The new pipeline reproduces legacy output byte-for-byte
# for dates BEFORE this date, including the legacy "phantom-adherence" bug: a
# scheduled slot that matched no productivity was counted as FULLY adherent
# (a whole 1800s slot) instead of 0. From this date on, an unmatched slot
# correctly scores 0. Same 2026-07-01 migration cutover the night-shift
# re-attribution uses (shift_attribution.NIGHT_SHIFT_CUTOVER).
LEGACY_PHANTOM_CUTOVER: date = date(2026, 7, 1)

# After Jan 22, 2026 the staffing-hero status backfill stopped, so the legacy
# explicitly trusts productivity rows even when status is NULL.
NULL_STATUS_TRUST_DATE: str = "2026-01-22"

# Per-slot result keys (one row per scheduled DIME slot).
SLOT_KEYS: tuple[str, ...] = ("agent", "date", "slot_start", "activity_type_required")

# Meeting/leave dimensioned_activity tokens excluded from adherence. This is a
# fixed DIME data filter (NOT a manual adjustment): these slots are leave
# (Licencia, Vacacion, Permiso Medico) or meetings (Mouring, Weekly, Huddle), so
# they are not adherence-eligible scheduled work. Legacy excludes them with the
# same `dimensioned_activity NOT IN (...)` filter at the DIME stage; exact legacy
# list, incl. the 'Permiso Medico'/'Permiso medico' case variants.
MEETING_LEAVE_DIMENSIONED_ACTIVITIES: tuple[str, ...] = (
    "Mouring",
    "Weekly",
    "Permiso Medico",
    "Permiso medico",
    "Huddle",
    "Licencia",
    "Vacacion",
)


# ---------------------------------------------------------------------------
# Step 1: filter DIME (minimal — raw slot universe)
# ---------------------------------------------------------------------------


def filter_dime(dime: DataFrame) -> DataFrame:
    """Keep DIME slots eligible for adherence, applying the two fixed DIME
    filters legacy applies at the DIME stage:

      * ``activity_type_required IS NOT NULL``;
      * ``dimensioned_activity`` not in
        :data:`MEETING_LEAVE_DIMENSIONED_ACTIVITIES` — leave/meeting slots that
        aren't adherence-eligible scheduled work (a NULL ``dimensioned_activity``
        is kept). This is a fixed data filter, **not** a manual adjustment.

    Activity-type (``lunch_break`` / ``time_off`` / ``shrinkage``) and squad
    business exclusions still move to the metrics layer.

    Adds two computed columns used by the overlap join:
      * ``slot_start``: UTC unix = ``slot_start_local_unix + 6h``
      * ``slot_end``:   UTC unix = ``slot_end_local_unix   + 6h``
    """
    return (
        dime.filter(F.col("activity_type_required").isNotNull())
        .filter(
            F.col("dimensioned_activity").isNull()
            | ~F.col("dimensioned_activity").isin(
                list(MEETING_LEAVE_DIMENSIONED_ACTIVITIES)
            )
        )
        .withColumn(
            "slot_start", F.col("slot_start_local_unix") + MEXICO_UTC_OFFSET_SECONDS
        )
        .withColumn(
            "slot_end", F.col("slot_end_local_unix") + MEXICO_UTC_OFFSET_SECONDS
        )
    )


# ---------------------------------------------------------------------------
# Step 2: filter productivity
# ---------------------------------------------------------------------------


def filter_productivity(prod: DataFrame) -> DataFrame:
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
    inferred = F.col("inferred_status")
    timestamp = F.col("timestamp").cast("timestamp")
    active_jobs = F.coalesce(F.col("active_jobs"), F.lit(0))

    mask = (
        inferred.isin("available", "oos", "training")
        | ((inferred == "pause") & (F.col("level_3") == "paused_with_jobs"))
        | (active_jobs > 0)
        | (
            (timestamp >= F.to_timestamp(F.lit(NULL_STATUS_TRUST_DATE)))
            & inferred.isNull()
        )
    )
    return prod.filter(mask)


# ---------------------------------------------------------------------------
# Step 3: per-slot adherent seconds (the overlap join + math)
# ---------------------------------------------------------------------------


def compute_slot_adherence(slots: DataFrame, productivity: DataFrame) -> DataFrame:
    """For each DIME slot, sum adherent seconds and cap at 1800.

    Algorithm:
      1. Join slot × productivity on ``agent`` and keep temporally-overlapping
         pairs: ``activity_end >= slot_start AND activity_start < slot_end``.
      2. Per pair, ``overlap = LEAST(end) - GREATEST(start)``, clipped to
         [0, 1800].
      3. Per slot, sum overlaps and clip at 1800. Slots that matched nothing
         appear via LEFT-JOIN semantics and are filled per the cutover below.

    Legacy-phantom replication (pre-2026-07-01): legacy counted a slot that
    matched no productivity as *fully* adherent (1800s), not 0. We reproduce
    that for dates before ``LEGACY_PHANTOM_CUTOVER`` so historical adherence is
    byte-for-byte with legacy; from the cutover on, an unmatched slot scores 0.

    Returns one row per (agent, date, slot_start, activity_type_required)
    with the additional column ``adherent_time_final`` (long seconds).
    """
    keys = list(SLOT_KEYS)
    all_slots = slots.select(*keys).distinct()

    slots_min = slots.select(
        "agent", "date", "slot_start", "slot_end", "activity_type_required"
    )
    prod_min = productivity.select("agent", "activity_start_unix", "activity_end_unix")

    joined = slots_min.join(prod_min, on="agent", how="inner").filter(
        (F.col("activity_end_unix") >= F.col("slot_start"))
        & (F.col("activity_start_unix") < F.col("slot_end"))
    )

    overlap = F.least(F.col("activity_end_unix"), F.col("slot_end")) - F.greatest(
        F.col("activity_start_unix"), F.col("slot_start")
    )
    clipped = F.greatest(F.lit(0), F.least(F.lit(SLOT_DURATION_SECONDS), overlap))

    per_slot = (
        joined.withColumn("overlap_seconds", clipped)
        .groupBy(*keys)
        .agg(F.sum("overlap_seconds").alias("adherent_time_final"))
        .withColumn(
            "adherent_time_final",
            F.least(F.col("adherent_time_final"), F.lit(SLOT_DURATION_SECONDS)),
        )
    )

    # Unmatched slots (no overlapping productivity) come back NULL from the LEFT
    # join. They score 0 — EXCEPT before the cutover, where we reproduce the
    # legacy phantom-adherence bug (unmatched slot counted as a full 1800s).
    # Gated on the slot's calendar date so pre-cutover output stays byte-for-byte.
    unmatched_fill = F.when(
        F.to_date(F.col("date")) < F.lit(LEGACY_PHANTOM_CUTOVER),
        F.lit(SLOT_DURATION_SECONDS),
    ).otherwise(F.lit(0))

    return all_slots.join(per_slot, on=keys, how="left").withColumn(
        "adherent_time_final",
        F.coalesce(F.col("adherent_time_final"), unmatched_fill).cast("long"),
    )


# ---------------------------------------------------------------------------
# Step 4: orchestrator — join roster + per-slot output
# ---------------------------------------------------------------------------


def compute_adherent_time(
    agent_info: DataFrame,
    dime: DataFrame,
    productivity: DataFrame,
) -> DataFrame:
    """End-to-end pipeline: extractor frames in, raw per-slot result out.

    The intermediate steps are each exposed as standalone functions above so
    they can be unit-tested in isolation.
    """
    dime_f = filter_dime(dime)
    prod_f = filter_productivity(productivity)
    slot_adherence = compute_slot_adherence(dime_f, prod_f)

    # Night-shift agents that cross midnight are re-attributed to the day their
    # shift started (>= 2026-07-01 only). `slot_start` here is UTC unix
    # (filter_dime added +6h), so subtract the offset to recover the local slot
    # timestamp before re-attributing. With the session tz set to UTC,
    # `timestamp_seconds(local_unix)` renders the local wall clock.
    night_months = night_agent_months(agent_info)
    slot_adherence = slot_adherence.withColumn(
        "_local_ts",
        F.timestamp_seconds(F.col("slot_start") - MEXICO_UTC_OFFSET_SECONDS),
    )
    slot_adherence = shift_start_date(
        slot_adherence,
        agent_col="agent",
        local_ts_col="_local_ts",
        calendar_date_col="date",
        night_months=night_months,
    ).drop("_local_ts")

    # Roster: active agents only. `CORE_OUT_OF_SCOPE_SQUADS` is currently empty,
    # so the squad filter is skipped.
    roster = agent_info.filter(F.col("status") == "active")
    if CORE_OUT_OF_SCOPE_SQUADS:
        roster = roster.filter(~F.col("squad").isin(list(CORE_OUT_OF_SCOPE_SQUADS)))
    roster = roster.select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        F.col("squad_district").alias("district"),
        "shift",
        F.trunc(F.to_date(F.col("snapshot_month")), "month").alias("snapshot_month"),
    )

    # Attribute each slot's (possibly re-attributed) date to its calendar month,
    # then inner-join to that month's roster snapshot.
    enriched = slot_adherence.withColumn(
        "snapshot_month", F.trunc(F.to_date(F.col("date")), "month")
    ).join(roster, on=["agent", "snapshot_month"], how="inner")

    # `slot_start` here is UTC (filter_dime added +6h); shift back to local
    # before deriving the wall-clock time-of-day.
    local_unix = F.col("slot_start") - MEXICO_UTC_OFFSET_SECONDS
    out = (
        enriched.withColumn(
            "slot_time", F.date_format(F.timestamp_seconds(local_unix), "HH:mm:ss")
        )
        .withColumn(
            "adherent_minutes", (F.col("adherent_time_final") / F.lit(60.0)).cast("double")
        )
        .withColumn(
            "required_minutes", F.lit(float(SLOT_DURATION_SECONDS) / 60.0).cast("double")
        )
        .select(
            "agent",
            "xforce",
            "xplead",
            "team",
            "squad",
            "district",
            "shift",
            F.to_date(F.col("date")).alias("date"),
            "slot_time",
            "activity_type_required",
            "required_minutes",
            "adherent_minutes",
        )
    )

    return out.orderBy("date", "agent", "slot_time")


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
