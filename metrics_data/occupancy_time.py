"""occupancy_time — raw per-slot occupancy/required minutes (one row per DIME slot, PySpark).

This is a RAW dataset, not a finished metric. It is the occupancy twin of
``adherent_time``: for every DIME slot an active agent was scheduled for, how
many minutes the agent spent actually working jobs whose ``activity_type``
matches the slot's ``activity_type_required``, and how long the slot was. A
downstream ``metrics`` layer turns these into the Normalized-Occupancy ratio
and its district/shift benchmark.

Public API
----------
``compute_occupancy_time(agent_info, dime, shuffle_jobs, oos_jobs, sm_jobs=None)``
takes Spark DataFrames (the extractor outputs) and returns one Spark DataFrame
with one row per (agent, date, slot) carrying ``occupancy_minutes`` and
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

Filters applied here (matching legacy at the DIME stage; see legacy parity below)
---------------------------------------------------------------------------------
* DIME: keep slots with ``activity_type_required IS NOT NULL``.
* DIME systemic reclassifications are KEPT (they are part of the occupancy
  matching logic, not a business exclusion):
    - ``dimensioned_activity`` in ('Control MC', 'xMC Debit Fraud') →
      ``activity_type_required = 'oos'``
    - ``activity_type_required == 'dime_invalid_notation'`` →
      ``activity_type_required = 'oos'``
* DIME fixed legacy filters (applied here at the slot stage so both the agent
  occupancy AND the per-squad benchmark exclude them — see legacy parity below):
    - ``dimensioned_activity`` not in
      :data:`MEETING_LEAVE_DIMENSIONED_ACTIVITIES` (leave/meeting slots; NULL
      kept).
    - ``agent_dime_squad`` non-NULL and not in
      :data:`NOCC_DIME_SQUAD_EXCLUSIONS` (wfm / credit_evolution / dote /
      ``social``). NOTE the occupancy list INCLUDES ``social`` (unlike
      adherence); the ``social`` exclusion is cutover-gated (see below).
* Jobs: shuffle ``status IN ('finished', 'transferred', 'skipped')`` (NOcc
  counts attempted work, wider than NTPJ's 'finished'); OOS and SM rows get a
  synthetic ``activity_type = 'oos'``.
* Approved raw-data correction from ``Correcciones Generales Datos``:
  ``luis.contreras`` Content Taskmaster/OOS job timestamps are shifted forward
  before overlap math (+2h through 2026-03-08, +1h from 2026-03-09 to
  2026-05-19).
* Roster: ``status = 'active'`` (inner join attaches dimensions / scopes output).

Legacy parity (pre-2026-07-01)
------------------------------
The new pipeline reproduces legacy ``[IO] Normalized Occupancy Dataset.sql``
byte-for-byte for dates BEFORE the ``SOCIAL_MEDIA_OCCUPANCY_CUTOVER``
(2026-07-01), including the legacy Social-Media omission:

* Legacy excluded ``agent_dime_squad = 'social'`` DIME slots AND had no Sprinklr
  ``sm_jobs`` source (its ``jobs_join`` was shuffle ∪ oos only). So before the
  cutover we (a) DROP ``agent_dime_squad = 'social'`` DIME slots and (b) SKIP the
  ``sm_jobs`` union; from the cutover on we keep ``social`` and union ``sm_jobs``
  so Social-Media occupancy turns on. This mirrors the
  ``shift_attribution.NIGHT_SHIFT_CUTOVER`` / adherence phantom cutover handling.

The two **fixed** DIME filters (meeting/leave ``dimensioned_activity`` drop and
the wfm/credit_evolution/dote DIME-squad drop) apply on ALL dates — only the
``social`` slice and the ``sm_jobs`` union are cutover-gated.

Filters deferred to the metrics layer (NOT applied here)
--------------------------------------------------------
* Activity-type exclusions (``lunch_break`` / ``time_off`` / ``shrinkage``).
* The monthly district/shift occupancy benchmark (``occupancy_exp``) — that is
  a metric-layer computation, removed from this raw table.
* All per-agent manual adjustments / outage-date carve-outs.

Why the occupancy calc needs interval dedup
--------------------------------------------
A single slot can have multiple overlapping jobs of the same activity type
(an agent juggling two chats). Naively summing per-job overlaps would
double-count the overlapping portion. We merge overlapping same-activity
intervals with the classic ``prev_max_end`` running-max trick — legacy used a
window function and we port it to a Spark ``Window.partitionBy(...).orderBy(...)``
running max via ``lag`` over the unbounded-preceding frame.

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

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from shift_attribution import night_agent_months, shift_start_date

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Systemic activity-type reclassifications (kept; NOT manual adjustments).
DIMENSIONED_ACTIVITY_TO_OOS: tuple[str, ...] = ("Control MC", "xMC Debit Fraud")
DIME_INVALID_NOTATION_VALUE: str = "dime_invalid_notation"

# Roster-level squad exclusions. Currently empty — all squads in scope.
NOCC_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# Meeting/leave dimensioned_activity tokens excluded from occupancy. Fixed DIME
# data filter (NOT a manual adjustment): leave (Licencia, Vacacion, Permiso
# Medico) or meetings (Mouring, Weekly, Huddle). Legacy excludes them with the
# same `dimensioned_activity NOT IN (...)` filter at the DIME stage (NOcc dataset
# line 234), incl. the 'Permiso Medico'/'Permiso medico' case variants. Applied
# on ALL dates.
MEETING_LEAVE_DIMENSIONED_ACTIVITIES: tuple[str, ...] = (
    "Mouring",
    "Weekly",
    "Permiso Medico",
    "Permiso medico",
    "Huddle",
    "Licencia",
    "Vacacion",
)

# DIME squads excluded from occupancy — a fixed legacy filter on the DIME
# `agent_dime_squad` (NOcc dataset line 236: `NOT IN ('wfm', 'credit_evolution',
# 'dote', 'social')`). NOTE this set INCLUDES `social` — unlike adherence's
# DIME_SQUAD_EXCLUSIONS (wfm/credit_evolution/dote only). The wfm/credit_evolution/
# dote drop is unconditional; the `social` drop is cutover-gated (see
# SOCIAL_DIME_SQUAD / SOCIAL_MEDIA_OCCUPANCY_CUTOVER below).
NOCC_DIME_SQUAD_EXCLUSIONS: tuple[str, ...] = (
    "wfm",
    "credit_evolution",
    "dote",
    "social",
)

# The DIME squad that legacy dropped only because it had no Sprinklr source. We
# keep it from the cutover on (when sm_jobs is also unioned).
SOCIAL_DIME_SQUAD: str = "social"

# Legacy-parity cutover for Social-Media occupancy. BEFORE this date we
# reproduce legacy: drop `agent_dime_squad = 'social'` DIME slots and skip the
# sm_jobs union (legacy had no Sprinklr source). FROM this date on, social slots
# are kept and sm_jobs is unioned in, turning Social-Media occupancy on. Same
# 2026-07-01 migration cutover the night-shift re-attribution uses
# (shift_attribution.NIGHT_SHIFT_CUTOVER).
SOCIAL_MEDIA_OCCUPANCY_CUTOVER: date = date(2026, 7, 1)

# Shuffle status filter: occupancy counts work the agent ATTEMPTED, not just
# work that succeeded. So we keep transferred/skipped in addition to
# 'finished'. (NTPJ, which measures throughput, uses only 'finished'.)
SHUFFLE_OCCUPIED_STATUSES: tuple[str, ...] = ("finished", "transferred", "skipped")

# Slot duration — 30 minutes.
SLOT_DURATION_SECONDS: int = 30 * 60

LUIS_CONTRERAS_AGENT = "luis.contreras"

# Per-slot interval-dedup partition / result keys.
SLOT_KEYS: tuple[str, ...] = (
    "agent",
    "squad",
    "date",
    "slot_start",
    "slot_end",
    "activity_type_required",
)


# ---------------------------------------------------------------------------
# Step 1: DIME filter + systemic activity-type reclassifications
# ---------------------------------------------------------------------------


def filter_dime(dime: DataFrame, cutover: date = SOCIAL_MEDIA_OCCUPANCY_CUTOVER) -> DataFrame:
    """Keep raw slots, apply occupancy's systemic reclassifications, and the
    fixed legacy DIME filters.

    Drops:
      * NULL ``activity_type_required``;
      * ``dimensioned_activity`` in
        :data:`MEETING_LEAVE_DIMENSIONED_ACTIVITIES` (leave/meeting; NULL kept);
      * ``squad`` (the DIME ``agent_dime_squad``) NULL or in
        :data:`NOCC_DIME_SQUAD_EXCLUSIONS`. The wfm/credit_evolution/dote part is
        unconditional; the ``social`` part is gated on ``date < cutover``
        (pre-cutover reproduces legacy, which had no Sprinklr source for social).

    Then applies two systemic reclassifications (NOT manual adjustments) so the
    job-matching logic is identical to the legacy NOcc dataset:
      * ``dimensioned_activity`` in ('Control MC', 'xMC Debit Fraud') →
        ``activity_type_required = 'oos'``
      * ``activity_type_required == 'dime_invalid_notation'`` →
        ``activity_type_required = 'oos'``

    Activity-type (``lunch_break`` / ``time_off`` / ``shrinkage``) exclusions
    still move to the metrics layer.
    """
    out = dime.filter(F.col("activity_type_required").isNotNull())

    # Meeting/leave drop (fixed; all dates). NULL dimensioned_activity is kept.
    out = out.filter(
        F.col("dimensioned_activity").isNull()
        | ~F.col("dimensioned_activity").isin(
            list(MEETING_LEAVE_DIMENSIONED_ACTIVITIES)
        )
    )

    # DIME-squad drop. `squad` here is the DIME `agent_dime_squad`.
    # wfm/credit_evolution/dote unconditionally; `social` only before the cutover.
    unconditional = [
        s for s in NOCC_DIME_SQUAD_EXCLUSIONS if s != SOCIAL_DIME_SQUAD
    ]
    is_social = F.col("squad") == F.lit(SOCIAL_DIME_SQUAD)
    social_excluded = is_social & (F.to_date(F.col("date")) < F.lit(cutover))
    out = out.filter(
        F.col("squad").isNotNull()
        & ~F.col("squad").isin(unconditional)
        & ~social_excluded
    )

    # Systemic reclassifications → 'oos'.
    is_fraud_oos = F.col("dimensioned_activity").isin(
        list(DIMENSIONED_ACTIVITY_TO_OOS)
    )
    is_invalid_notation = (
        F.col("activity_type_required") == F.lit(DIME_INVALID_NOTATION_VALUE)
    )
    out = out.withColumn(
        "activity_type_required",
        F.when(
            is_fraud_oos | is_invalid_notation, F.lit("oos")
        ).otherwise(F.col("activity_type_required")),
    )

    return out


# ---------------------------------------------------------------------------
# Step 2: union shuffle + OOS (+ optional Social-Media) jobs
# ---------------------------------------------------------------------------


def _apply_luis_contreras_oos_timestamp_correction(oos_jobs: DataFrame) -> DataFrame:
    """Shift approved Content OOS job timestamps for luis.contreras.

    Source: `Correcciones Generales Datos`. His laptop clock lagged Taskmaster
    during H1 2026, so the recorded local start/stop times must move forward
    before NOCC overlap calculations: +2h through 2026-03-08, +1h from 2026-03-09
    to 2026-05-19. Hardcoded because the tab is not a scalable rules table yet.
    """
    start_date = F.to_date(F.col("local_start_date"))
    agent = F.lower(F.col("agent"))
    squad = F.lower(F.col("squad"))
    is_luis_content_oos = (agent == F.lit(LUIS_CONTRERAS_AGENT)) & squad.contains(
        "content"
    )

    correction_hours = (
        F.when(
            is_luis_content_oos
            & (start_date >= F.lit(date(2026, 1, 1)))
            & (start_date <= F.lit(date(2026, 3, 8))),
            F.lit(2),
        )
        .when(
            is_luis_content_oos
            & (start_date >= F.lit(date(2026, 3, 9)))
            & (start_date <= F.lit(date(2026, 5, 19))),
            F.lit(1),
        )
        .otherwise(F.lit(0))
    )
    secs = correction_hours.cast("long") * F.lit(3600)

    return (
        oos_jobs.withColumn("_corr_hours", correction_hours)
        .withColumn("_corr_secs", secs)
        .withColumn(
            "activity_start_unix",
            F.col("activity_start_unix").cast("long") + F.col("_corr_secs"),
        )
        .withColumn(
            "activity_end_unix",
            F.col("activity_end_unix").cast("long") + F.col("_corr_secs"),
        )
        # Re-derive `date` from the shifted start so a correction that crosses
        # midnight re-attributes the row (matches the pandas behaviour). The unix
        # columns are local-time unix (legacy reads the local timestamp via
        # UNIX_TIMESTAMP as if UTC), so timestamp_seconds renders the local wall
        # clock under the UTC session tz.
        .withColumn(
            "date",
            F.when(
                F.col("_corr_hours") > 0,
                F.to_date(F.timestamp_seconds(F.col("activity_start_unix"))),
            ).otherwise(F.col("date")),
        )
        .drop("_corr_hours", "_corr_secs")
    )


def build_jobs_union(
    shuffle_jobs: DataFrame,
    oos_jobs: DataFrame,
    sm_jobs: DataFrame | None = None,
) -> DataFrame:
    """Concatenate shuffle, OOS, and (optional) Social-Media jobs into one frame.

    * Shuffle: filter to ``status IN ('finished', 'transferred', 'skipped')``.
    * OOS: synthesize ``activity_type='oos'`` (taskmaster has no activity_type),
      after the luis.contreras timestamp correction.
    * SM (Sprinklr ``sm_jobs``): each social case assignment is an occupancy
      interval; synthesize ``activity_type='oos'`` so it matches DIME slots whose
      ``activity_type_required='oos'`` — exactly as the legacy SM notebook treats
      it. Passed in only from the Social-Media cutover on (see module docstring).

    Output columns:
        agent STRING, date DATE, activity_type STRING,
        activity_start_unix BIGINT, activity_end_unix BIGINT
    """
    cols = [
        "agent",
        "date",
        "activity_type",
        "activity_start_unix",
        "activity_end_unix",
    ]

    shuffle_part = shuffle_jobs.filter(
        F.col("status").isin(list(SHUFFLE_OCCUPIED_STATUSES))
    ).select(*cols)

    oos_part = (
        _apply_luis_contreras_oos_timestamp_correction(oos_jobs)
        .withColumn("activity_type", F.lit("oos"))
        .select(*cols)
    )

    union = shuffle_part.unionByName(oos_part)

    if sm_jobs is not None:
        sm_part = sm_jobs.withColumn("activity_type", F.lit("oos")).select(*cols)
        union = union.unionByName(sm_part)

    return union


# ---------------------------------------------------------------------------
# Step 3: per-slot occupancy (the overlap join + interval-dedup math)
# ---------------------------------------------------------------------------


def compute_slot_occupancy(dime_filtered: DataFrame, jobs: DataFrame) -> DataFrame:
    """For each DIME slot, sum occupied seconds and cap at 1800.

    Algorithm (mirrors the legacy ``slot_jobs`` -> ``occupancy_base`` ->
    ``occupancy_agg`` chain):

      1. Per agent+date, build (slot × job) pairs and keep only temporally
         overlapping ones: ``job_end > slot_start AND job_start < slot_end``.
      2. Clip each job to the slot bounds.
      3. Within partition ``(slot_keys, job.activity_type)``, order by
         ``(cjob_start, cjob_end)`` and take the running max of previous
         ``cjob_end`` over ``ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING``
         (``prev_max_end``; first row → slot_start) — legacy's window ported to
         a Spark ``Window`` + ``lag``-style unbounded-preceding running max.
      4. ``contribution = activity_occuped × max(0, cjob_end -
         max(cjob_start, prev_max_end))`` where ``activity_occuped = 1`` iff
         ``slot.activity_type_required == job.activity_type``.
      5. Sum contributions per slot. Cap at 1800. LEFT-JOIN semantics: slots
         with no matching job keep ``occupancy_time = 0``.

    Returns one row per (agent, squad, date, slot_start, slot_end,
    activity_type_required) with ``occupancy_time`` (long seconds).
    """
    keys = list(SLOT_KEYS)

    slots = (
        dime_filtered.withColumnRenamed("slot_start_local_unix", "slot_start")
        .withColumnRenamed("slot_end_local_unix", "slot_end")
        .select(*keys)
        .distinct()
    )

    # Half-open overlap, equivalent to legacy's 3-clause OR for positive-duration
    # jobs (the rare zero-duration-at-slot-start edge is documented in the build
    # plan as a known parity risk).
    joined = slots.join(jobs, on=["agent", "date"], how="inner").filter(
        (F.col("activity_end_unix") > F.col("slot_start"))
        & (F.col("activity_start_unix") < F.col("slot_end"))
    )

    cjob_start = F.greatest(F.col("slot_start"), F.col("activity_start_unix"))
    cjob_end = F.least(F.col("slot_end"), F.col("activity_end_unix"))
    activity_occuped = (
        F.col("activity_type_required") == F.col("activity_type")
    ).cast("int")

    clipped = joined.select(
        *keys,
        "activity_type",
        cjob_start.alias("cjob_start"),
        cjob_end.alias("cjob_end"),
        activity_occuped.alias("activity_occuped"),
    )

    # Interval-dedup: running max of cjob_end over the rows strictly preceding the
    # current one, within (slot keys + job activity_type), ordered by
    # (cjob_start, cjob_end). NULL on the first row -> slot_start.
    dedup_window = (
        Window.partitionBy(*keys, "activity_type")
        .orderBy(F.col("cjob_start"), F.col("cjob_end"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    prev_max_end = F.coalesce(
        F.max("cjob_end").over(dedup_window), F.col("slot_start")
    )

    effective_start = F.greatest(F.col("cjob_start"), prev_max_end)
    contribution = F.when(
        F.col("activity_occuped") == 1,
        F.greatest(F.lit(0).cast("long"), F.col("cjob_end") - effective_start),
    ).otherwise(F.lit(0).cast("long"))

    per_slot = (
        clipped.withColumn("contribution", contribution)
        .groupBy(*keys)
        .agg(F.sum("contribution").alias("occupancy_time"))
        .withColumn(
            "occupancy_time",
            F.least(F.col("occupancy_time"), F.lit(SLOT_DURATION_SECONDS)),
        )
    )

    # LEFT-JOIN back to keep slots with no matching job (occupancy_time = 0).
    return slots.join(per_slot, on=keys, how="left").withColumn(
        "occupancy_time",
        F.coalesce(F.col("occupancy_time"), F.lit(0)).cast("long"),
    )


# ---------------------------------------------------------------------------
# Step 4: orchestrator — roster join + final shape
# ---------------------------------------------------------------------------


def compute_occupancy_time(
    agent_info: DataFrame,
    dime: DataFrame,
    shuffle_jobs: DataFrame,
    oos_jobs: DataFrame,
    sm_jobs: DataFrame | None = None,
    cutover: date = SOCIAL_MEDIA_OCCUPANCY_CUTOVER,
) -> DataFrame:
    """End-to-end occupancy_time pipeline (raw per-slot occupancy minutes).

    ``sm_jobs`` (Sprinklr ``sm_jobs`` extractor) is optional: when provided, the
    Social-Media case assignments are unioned in as ``oos``-typed jobs so social
    agents' occupancy is populated from Sprinklr (they have no shuffle/taskmaster
    jobs). To reproduce legacy byte-for-byte before the cutover, the union (and
    the keeping of ``agent_dime_squad = 'social'`` DIME slots) is gated per-slot
    on ``date >= cutover`` (see ``filter_dime`` / module docstring): pre-cutover
    rows neither keep social DIME slots nor receive SM occupancy, exactly as
    legacy did.
    """
    # --- DIME side ----------------------------------------------------------
    dime_f = filter_dime(dime, cutover=cutover)

    # --- jobs side ----------------------------------------------------------
    # sm_jobs only matter for social slots, which are themselves only kept from
    # the cutover on; the per-slot social gate in filter_dime means a pre-cutover
    # social SM job can never match a slot, so this is safe to always pass when
    # provided. Pre-cutover social slots are already dropped.
    jobs = build_jobs_union(shuffle_jobs, oos_jobs, sm_jobs)

    # --- per-slot occupancy -------------------------------------------------
    per_slot = compute_slot_occupancy(dime_f, jobs)
    # The DIME ``squad`` was only needed for the per-slot interval-dedup
    # partition; drop it before the roster join to avoid colliding with the
    # roster's own ``squad``.
    per_slot = per_slot.drop("squad")

    # Night-shift agents that cross midnight are re-attributed to the day their
    # shift started (>= 2026-07-01 only). `slot_start` is local-time unix, so it
    # renders straight to the local slot timestamp.
    night_months = night_agent_months(agent_info)
    per_slot = per_slot.withColumn(
        "_local_ts", F.timestamp_seconds(F.col("slot_start"))
    )
    per_slot = shift_start_date(
        per_slot,
        agent_col="agent",
        local_ts_col="_local_ts",
        calendar_date_col="date",
        night_months=night_months,
    ).drop("_local_ts")

    # --- roster join --------------------------------------------------------
    roster = agent_info.filter(F.col("status") == "active")
    if NOCC_OUT_OF_SCOPE_SQUADS:
        roster = roster.filter(~F.col("squad").isin(list(NOCC_OUT_OF_SCOPE_SQUADS)))
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

    enriched = per_slot.withColumn(
        "snapshot_month", F.trunc(F.to_date(F.col("date")), "month")
    ).join(roster, on=["agent", "snapshot_month"], how="inner")

    # --- final shape --------------------------------------------------------
    enriched = enriched.withColumn(
        "occupancy_time",
        F.least(F.col("occupancy_time"), F.lit(SLOT_DURATION_SECONDS)),
    )
    # `slot_start` is local-time unix, so it renders straight to wall-clock.
    out = (
        enriched.withColumn(
            "slot_time",
            F.date_format(F.timestamp_seconds(F.col("slot_start")), "HH:mm:ss"),
        )
        .withColumn(
            "occupancy_minutes",
            (F.col("occupancy_time").cast("double") / F.lit(60.0)).cast("double"),
        )
        .withColumn(
            "required_minutes",
            F.lit(float(SLOT_DURATION_SECONDS) / 60.0).cast("double"),
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
            "occupancy_minutes",
        )
    )

    return out.orderBy("date", "agent", "slot_time")


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
