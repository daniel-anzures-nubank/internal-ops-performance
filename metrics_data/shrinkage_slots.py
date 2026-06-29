"""shrinkage_slots — one row per DIME slot, flagged for shrinkage (PySpark).

This is a RAW dataset, not a finished metric. It exposes every DIME slot an
active agent was scheduled for, with boolean flags marking whether the slot is
shrinkage (and, if so, whether that shrinkage is controllable or not). A
downstream ``metrics`` layer turns these into the Shrinkage ratio.

Public API
----------
``compute_shrinkage_slots(agent_info, dime)`` takes Spark DataFrames (the
extractor outputs) and returns one Spark DataFrame with one row per DIME slot
carrying ``shrinkage_flag`` / ``controllable_shrinkage_flag`` /
``uncontrollable_shrinkage_flag``.

Source tables (via extractors)
------------------------------
* ``agent_information`` → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``dime_slots``        → ``etl.mx__series_contract.agent_dimensioned_activities``
  (``affiliation = 'nubank'``).

Filters applied here (matching legacy at the DIME stage; see legacy parity below)
---------------------------------------------------------------------------------
* DIME: keep slots with ``activity_type_required IS NOT NULL``.
* DIME fixed legacy SQUAD filter (applied here at the slot stage so both the
  numerator AND denominator exclude them — legacy ``shrinkage_base`` lines
  249-250): ``agent_dime_squad`` (the DIME ``squad`` column) is non-NULL and not
  in :data:`SHRINKAGE_DIME_SQUAD_EXCLUSIONS`
  (``content`` / ``planning`` / ``quality`` / ``social`` / ``wfm`` /
  ``enablement``). NOTE this set is shrinkage-specific — it is BROADER on the
  org-support side than adherence/occupancy (which use
  ``wfm`` / ``credit_evolution`` / ``dote``) and it excludes neither
  ``credit_evolution`` nor ``dote``.
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
``uncontrollable`` always sums to ``shrinkage_flag``.) These controllable /
uncontrollable flags are net-new (legacy has no such split); they are carried
through but the metric ratio only consumes ``shrinkage_flag``.

Deferred to the future metrics / adjustments layer (NOT done here)
------------------------------------------------------------------
* The required/denominator definition (legacy ``required_slot``: pre-cutover
  drops ``dime_invalid_notation``, post-cutover drops ``time_off``). The raw
  slot universe is here; the metrics layer applies the denominator rule.
* The ``lunch_break`` drop (legacy ``shrinkage_base`` line 248) — moved to the
  metrics layer alongside the denominator rule.
* All training / shadowing / maternity / vacation / outage manual adjustments
  (legacy ``manual_adjustments_shrinkage`` + the per-agent carve-outs in
  ``shrinkage_final_2026``). Applied at the Adjustments layer via
  ``reclassify_dime_slots`` / ``drop_slot_windows`` / ``apply_no_shrinkage``.

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

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from shift_attribution import night_agent_months, shift_start_date

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Roster-level squad exclusions. Currently empty — the DIME-squad filter
# (below) already removes the org-support squads at the slot level, matching
# legacy. Kept so a future narrower roster scope is a one-line edit.
SHRINKAGE_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# DIME squads excluded from shrinkage — a fixed legacy filter on the DIME
# ``agent_dime_squad`` (the ``squad`` column out of the dime_slots extractor).
# Legacy ``shrinkage_base`` ([IO] Shrinkage Dataset.sql lines 249-250) keeps only
# ``agent_dime_squad IS NOT NULL AND agent_dime_squad NOT IN (...)``. The
# exclusion changes BOTH numerator and denominator (legacy counts shrinkage_slot
# / required_slot FROM this already-filtered base), so it must be applied at the
# slot stage, before the roster merge drops the DIME squad column.
#
# NOTE this set is shrinkage-specific. It is BROADER on the org-support side than
# adherence/occupancy's ``DIME_SQUAD_EXCLUSIONS`` (wfm / credit_evolution / dote):
# shrinkage excludes content / planning / quality / social / wfm / enablement and
# excludes neither credit_evolution nor dote. Do NOT reuse the adherence list.
SHRINKAGE_DIME_SQUAD_EXCLUSIONS: tuple[str, ...] = (
    "content",
    "planning",
    "quality",
    "social",
    "wfm",
    "enablement",
)

# dimensioned_activity values that, post-cutover, get re-classified as
# shrinkage when activity_type_required is 'dime_invalid_notation'. Exact legacy
# list ([IO] Shrinkage Dataset.sql line 263), incl. the 'Permiso Medico' /
# 'Permiso medico' case variants.
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
UNCONTROLLABLE_DIMENSIONED_ACTIVITIES_LOWER: tuple[str, ...] = (
    "licencia",
    "skr_lcnc",
)

# Cutover for the shrinkage formula switch.
SHRINKAGE_FORMULA_CUTOVER: date = date(2026, 3, 1)

ACTIVITY_SHRINKAGE = "shrinkage"
ACTIVITY_INVALID_NOTATION = "dime_invalid_notation"


# ---------------------------------------------------------------------------
# Step 1: DIME filter (non-null activity_type_required + DIME-squad exclusion)
# ---------------------------------------------------------------------------


def filter_dime(dime: DataFrame) -> DataFrame:
    """Keep DIME slots eligible for shrinkage, applying the two fixed DIME
    filters legacy applies at the DIME stage:

      * ``activity_type_required IS NOT NULL``;
      * the DIME ``squad`` column (``agent_dime_squad``) is non-NULL and not in
        :data:`SHRINKAGE_DIME_SQUAD_EXCLUSIONS` — drops the org-support squads
        (``content`` / ``planning`` / ``quality`` / ``social`` / ``wfm`` /
        ``enablement``), matching legacy ``shrinkage_base``. Applied
        unconditionally on ALL dates, BEFORE the roster merge drops the DIME
        squad, so it constrains both the shrinkage numerator and the required
        denominator.

    The ``lunch_break`` drop and the required/denominator activity-type rule
    move to the metrics layer (``metrics/shrinkage.py``).
    """
    return dime.filter(F.col("activity_type_required").isNotNull()).filter(
        F.col("squad").isNotNull()
        & ~F.col("squad").isin(list(SHRINKAGE_DIME_SQUAD_EXCLUSIONS))
    )


# ---------------------------------------------------------------------------
# Step 2: slot flags (the pre/post-cutover shrinkage rule + control split)
# ---------------------------------------------------------------------------


def classify_slots(dime_filtered: DataFrame) -> DataFrame:
    """Tag each slot with shrinkage / controllable / uncontrollable flags.

    Adds three int {0,1} columns:
      ``shrinkage_flag`` / ``controllable_shrinkage_flag`` /
      ``uncontrollable_shrinkage_flag``.
    """
    cal = F.to_date(F.col("date"))
    is_post_cutover = cal >= F.lit(SHRINKAGE_FORMULA_CUTOVER)
    is_shrinkage_act = F.col("activity_type_required") == F.lit(ACTIVITY_SHRINKAGE)
    is_invalid_notation = (
        F.col("activity_type_required") == F.lit(ACTIVITY_INVALID_NOTATION)
    )
    is_meeting_leave_dim = F.col("dimensioned_activity").isin(
        list(SHRINKAGE_MEETING_LEAVE_DIMENSIONED_ACTIVITIES)
    )

    pre_shrink = is_shrinkage_act & ~is_post_cutover
    post_shrink = is_post_cutover & (
        is_shrinkage_act | (is_invalid_notation & is_meeting_leave_dim)
    )
    shrinkage_flag = pre_shrink | post_shrink

    is_uncontrollable_dim = F.coalesce(
        F.lower(F.col("dimensioned_activity")).isin(
            list(UNCONTROLLABLE_DIMENSIONED_ACTIVITIES_LOWER)
        ),
        F.lit(False),
    )

    return (
        dime_filtered.withColumn("shrinkage_flag", shrinkage_flag.cast("int"))
        .withColumn(
            "uncontrollable_shrinkage_flag",
            (shrinkage_flag & is_uncontrollable_dim).cast("int"),
        )
        .withColumn(
            "controllable_shrinkage_flag",
            (shrinkage_flag & ~is_uncontrollable_dim).cast("int"),
        )
    )


# ---------------------------------------------------------------------------
# Step 3: orchestrator — roster join (one row per slot)
# ---------------------------------------------------------------------------


def compute_shrinkage_slots(
    agent_info: DataFrame,
    dime: DataFrame,
) -> DataFrame:
    """End-to-end shrinkage_slots pipeline (one row per DIME slot)."""
    dime_f = filter_dime(dime)
    classified = classify_slots(dime_f)

    # Night-shift agents that cross midnight are re-attributed to the day their
    # shift started (>= 2026-07-01 only). Done after the shrinkage-flag
    # classification (whose 2026-03-01 formula switch keys off the calendar
    # date) and while the local slot unix is still available. `slot_start_local_unix`
    # is local-time unix, so it renders straight to the local slot timestamp.
    night_months = night_agent_months(agent_info)
    classified = classified.withColumn(
        "_local_ts", F.timestamp_seconds(F.col("slot_start_local_unix"))
    )
    classified = shift_start_date(
        classified,
        agent_col="agent",
        local_ts_col="_local_ts",
        calendar_date_col="date",
        night_months=night_months,
    )

    # Wall-clock time-of-day of the slot start (local-time unix renders straight).
    classified = classified.withColumn(
        "slot_time",
        F.date_format(F.timestamp_seconds(F.col("slot_start_local_unix")), "HH:mm:ss"),
    )

    # DIME carries its own ``squad`` column (agent_dime_squad) which would collide
    # with the roster's ``squad`` on merge. Output uses the roster squad, so keep
    # only the columns we need from the DIME side before joining.
    classified = classified.select(
        "agent",
        F.to_date(F.col("date")).alias("date"),
        "slot_time",
        "activity_type_required",
        "dimensioned_activity",
        "shrinkage_flag",
        "controllable_shrinkage_flag",
        "uncontrollable_shrinkage_flag",
    )

    # --- roster join --------------------------------------------------------
    roster = agent_info.filter(F.col("status") == "active")
    if SHRINKAGE_OUT_OF_SCOPE_SQUADS:
        roster = roster.filter(
            ~F.col("squad").isin(list(SHRINKAGE_OUT_OF_SCOPE_SQUADS))
        )
    roster = roster.select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        F.col("squad_district").alias("district"),
        "shift",
        F.to_date(F.col("snapshot_date")).alias("_snapshot_date"),
        F.trunc(F.to_date(F.col("snapshot_month")), "month").alias("snapshot_month"),
    )

    # Deduplicate the roster to exactly ONE row per (agent, snapshot_month) BEFORE
    # the slot join. The content branch of `agent_information` cross-joins each
    # Google-Sheet content row against every month, so a content agent on >1 sheet
    # row yields >=2 rows per (agent, snapshot_month) identical on every column
    # this view selects (only `target_squad`, unused here, differs). Without this
    # dedup the inner join below fans out and every slot double-counts. Keep the
    # latest snapshot deterministically (recency, then stable tiebreaks).
    roster_dedup_window = Window.partitionBy("agent", "snapshot_month").orderBy(
        F.col("_snapshot_date").desc_nulls_last(),
        F.col("squad").asc_nulls_last(),
        F.col("district").asc_nulls_last(),
        F.col("shift").asc_nulls_last(),
    )
    roster = (
        roster.withColumn("_roster_rn", F.row_number().over(roster_dedup_window))
        .filter(F.col("_roster_rn") == 1)
        .drop("_roster_rn", "_snapshot_date")
    )

    enriched = classified.withColumn(
        "snapshot_month", F.trunc(F.to_date(F.col("date")), "month")
    ).join(roster, on=["agent", "snapshot_month"], how="inner")

    out = enriched.select(
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
        "dimensioned_activity",
        "shrinkage_flag",
        "controllable_shrinkage_flag",
        "uncontrollable_shrinkage_flag",
    )

    return out.orderBy("date", "agent", "slot_time")


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
