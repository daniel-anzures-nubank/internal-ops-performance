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
      :data:`NOCC_DIME_SQUAD_EXCLUSIONS` (wfm / credit_evolution / dote).
      ``social`` DIME slots are KEPT on all dates — Social-Media occupancy is
      sourced from Sprinklr ``sm_jobs`` and is intentionally ON for the whole
      history (see "Legacy parity" below).
* Jobs: shuffle ``status IN ('finished', 'transferred', 'skipped')`` (NOcc
  counts attempted work, wider than NTPJ's 'finished'); OOS and SM rows get a
  synthetic ``activity_type = 'oos'``.
* Approved raw-data correction from ``Correcciones Generales Datos``:
  ``luis.contreras`` Content Taskmaster/OOS job timestamps are shifted forward
  before overlap math (+2h through 2026-03-08, +1h from 2026-03-09 to
  2026-05-19).
* Roster: ``status = 'active'`` (inner join attaches dimensions / scopes output).

Legacy parity (and the Social-Media divergence)
-----------------------------------------------
The new pipeline reproduces legacy ``[IO] Normalized Occupancy Dataset.sql``
byte-for-byte (including legacy's bugs) per the project's legacy-parity decision,
**with one deliberate, documented exception: Social-Media occupancy.**

* Legacy excluded ``agent_dime_squad = 'social'`` DIME slots AND had no Sprinklr
  ``sm_jobs`` source (its ``jobs_join`` was shuffle ∪ oos only), so legacy
  produced NO Social-Media occupancy. The data owner has since confirmed that
  Social-Media Normalized Occupancy data genuinely EXISTS for the whole history,
  sourced from Sprinklr ``sm_jobs``. We therefore KEEP ``agent_dime_squad =
  'social'`` DIME slots and union ``sm_jobs`` on ALL dates, turning Social-Media
  occupancy ON for the entire period. This is an intentional divergence from
  legacy (which had no Sprinklr source), approved as an exception to the
  byte-for-byte rule — not the night-shift / phantom cutover handling.

* NOTE this is distinct from the night-shift re-attribution, which still uses its
  own ``shift_attribution.NIGHT_SHIFT_CUTOVER`` (untouched).

* **Social-Media empty-slot full credit (pre-cutover parity quirk).** The legacy
  SM deck (``legacy/[IO] Performance 2026 - Social Media Temp Fix.sql``) counts a
  dimensioned SM slot with NO overlapping matching-activity Sprinklr case as
  FULLY occupied: ``occupancy_agg`` (lines 1123-1135) computes
  ``SUM(CASE WHEN activity_occuped = 1 THEN duration END)`` — NULL when no
  overlapping case matches the slot's activity type — and the downstream
  ``CASE WHEN SUM(occupancy_time) <= 1800 THEN SUM(occupancy_time) ELSE 1800 END``
  (lines 1189 and 1223) evaluates ``NULL <= 1800`` to NULL, so an empty slot
  falls through to ``ELSE 1800``. Partially covered slots keep their actual
  overlap seconds (the ``WHEN`` branch); only slots with zero matching cases get
  the 1800 default. We reproduce this quirk for SM DIME slots
  (:data:`SM_DIME_SQUADS`) dated **before**
  :data:`SM_EMPTY_SLOT_FULL_CREDIT_CUTOVER` (2026-07-01), restricted to the
  productive slot universe legacy scored (its DIME filter, lines 1064/1079,
  drops ``lunch_break`` / ``dime_invalid_notation`` / ``time_off`` /
  ``shrinkage`` — the eligibility flag is decided on the PRE-reclass
  ``activity_type_required``, since this module relabels ``dime_invalid_notation``
  to ``'oos'``). On/after the cutover the corrected behavior applies: an empty
  slot is 0.

The two **fixed** DIME filters (meeting/leave ``dimensioned_activity`` drop and
the wfm/credit_evolution/dote DIME-squad drop) apply on ALL dates.

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

from dime_filters import DIME_SQUAD_EXCLUSIONS, MEETING_LEAVE_DIMENSIONED_ACTIVITIES
from shift_attribution import night_agent_months, shift_start_date

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Systemic activity-type reclassifications (kept; NOT manual adjustments).
DIMENSIONED_ACTIVITY_TO_OOS: tuple[str, ...] = ("Control MC", "xMC Debit Fraud")
DIME_INVALID_NOTATION_VALUE: str = "dime_invalid_notation"

# Roster-level squad exclusions. Currently empty — all squads in scope.
NOCC_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# MEETING_LEAVE_DIMENSIONED_ACTIVITIES now lives in the shared
# metrics_data/dime_filters.py (imported above). Applied on ALL dates.

# DIME squads excluded from occupancy — the shared DIME_SQUAD_EXCLUSIONS from
# metrics_data/dime_filters.py, kept under this module's public name. Legacy's
# NOcc dataset (line 236) used
# `NOT IN ('wfm', 'credit_evolution', 'dote', 'social')`, but we intentionally
# DROP `social` from this exclusion set: Social-Media occupancy genuinely exists
# for the whole history (sourced from Sprinklr `sm_jobs`), so `social` DIME slots
# are kept on ALL dates. This is a documented divergence from legacy (which had
# no Sprinklr source); see module docstring. The remaining set matches
# adherence's DIME_SQUAD_EXCLUSIONS (wfm / credit_evolution / dote).
NOCC_DIME_SQUAD_EXCLUSIONS = DIME_SQUAD_EXCLUSIONS

# Shuffle status filter: occupancy counts work the agent ATTEMPTED, not just
# work that succeeded. So we keep transferred/skipped in addition to
# 'finished'. (NTPJ, which measures throughput, uses only 'finished'.)
SHUFFLE_OCCUPIED_STATUSES: tuple[str, ...] = ("finished", "transferred", "skipped")

# Slot duration — 30 minutes.
SLOT_DURATION_SECONDS: int = 30 * 60

# --- Social-Media empty-slot full credit (pre-cutover parity quirk) ---------
# The legacy SM deck scores only these DIME squads
# (`[IO] Performance 2026 - Social Media Temp Fix.sql` line 1065:
# `agent_dime_squad IN ('social', 'social_social')`).
SM_DIME_SQUADS: tuple[str, ...] = ("social", "social_social")

# Slots dated BEFORE this reproduce the legacy quirk (a slot with no
# matching-activity overlapping Sprinklr case earns the full 1800 s — the
# `ELSE 1800` fall-through at legacy lines 1189/1223); on/after it the
# corrected behavior applies (an empty slot is 0). See module docstring.
SM_EMPTY_SLOT_FULL_CREDIT_CUTOVER: date = date(2026, 7, 1)

# Legacy's SM occupancy DIME filter (lines 1064/1079) drops these activity
# types outright, so such slots never reach its `ELSE 1800` and must not earn
# the full-credit default here. `dime_invalid_notation` is in this list even
# though this module reclassifies it to 'oos' — legacy dropped the slot before
# any reclassification, so eligibility is decided on the PRE-reclass value.
SM_FULL_CREDIT_EXCLUDED_ACTIVITY_TYPES: tuple[str, ...] = (
    "lunch_break",
    "dime_invalid_notation",
    "time_off",
    "shrinkage",
)

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


def filter_dime(dime: DataFrame) -> DataFrame:
    """Keep raw slots, apply occupancy's systemic reclassifications, and the
    fixed legacy DIME filters.

    Drops:
      * NULL ``activity_type_required``;
      * ``dimensioned_activity`` in
        :data:`MEETING_LEAVE_DIMENSIONED_ACTIVITIES` (leave/meeting; NULL kept);
      * ``squad`` (the DIME ``agent_dime_squad``) NULL or in
        :data:`NOCC_DIME_SQUAD_EXCLUSIONS` (wfm / credit_evolution / dote). This
        drop is unconditional on ALL dates. ``social`` DIME slots are KEPT on all
        dates — Social-Media occupancy is sourced from Sprinklr ``sm_jobs`` and is
        intentionally ON for the whole history (a documented divergence from
        legacy; see module docstring).

    Then applies two systemic reclassifications (NOT manual adjustments) so the
    job-matching logic is identical to the legacy NOcc dataset:
      * ``dimensioned_activity`` in ('Control MC', 'xMC Debit Fraud') →
        ``activity_type_required = 'oos'``
      * ``activity_type_required == 'dime_invalid_notation'`` →
        ``activity_type_required = 'oos'``

    Activity-type (``lunch_break`` / ``time_off`` / ``shrinkage``) exclusions
    still move to the metrics layer.

    Also tags each slot with the boolean ``sm_empty_slot_full_credit`` —
    eligibility for the legacy SM empty-slot 1800-second default (see module
    docstring), consumed by :func:`compute_slot_occupancy`. Decided BEFORE the
    reclassifications because legacy's SM deck (line 1064) drops
    ``dime_invalid_notation`` slots outright: a slot reclassified to ``'oos'``
    here was never scored by legacy and must not earn the default.
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
    # wfm/credit_evolution/dote unconditionally (all dates). `social` is kept on
    # all dates (Sprinklr-sourced Social-Media occupancy, ON for the whole
    # history — see module docstring).
    out = out.filter(
        F.col("squad").isNotNull()
        & ~F.col("squad").isin(list(NOCC_DIME_SQUAD_EXCLUSIONS))
    )

    # SM empty-slot full-credit eligibility (pre-cutover parity quirk; module
    # docstring). Computed on the PRE-reclass activity_type_required so slots
    # legacy's own DIME filter dropped (e.g. `dime_invalid_notation`, which the
    # step below relabels 'oos') stay ineligible.
    out = out.withColumn(
        "sm_empty_slot_full_credit",
        F.col("squad").isin(list(SM_DIME_SQUADS))
        & (F.col("date") < F.lit(SM_EMPTY_SLOT_FULL_CREDIT_CUTOVER))
        & ~F.col("activity_type_required").isin(
            list(SM_FULL_CREDIT_EXCLUDED_ACTIVITY_TYPES)
        ),
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
      it. Unioned in on ALL dates so Social-Media occupancy is populated for the
      whole history (see module docstring).

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
         with no matching job keep ``occupancy_time = 0`` — EXCEPT eligible
         Social-Media slots (``sm_empty_slot_full_credit``, tagged by
         :func:`filter_dime`): pre-cutover SM slots with NO overlapping
         matching-activity job earn the full 1800 s, reproducing legacy's
         ``ELSE 1800`` fall-through (module docstring). A slot with a matching
         overlap keeps its actual (capped) seconds.

    Returns one row per (agent, squad, date, slot_start, slot_end,
    activity_type_required) with ``occupancy_time`` (long seconds).
    """
    keys = list(SLOT_KEYS)

    # `max` on the eligibility flag (rather than including it in a distinct)
    # keeps exactly one row per slot even if post-reclass duplicates were to
    # carry different flags.
    slots = (
        dime_filtered.withColumnRenamed("slot_start_local_unix", "slot_start")
        .withColumnRenamed("slot_end_local_unix", "slot_end")
        .groupBy(*keys)
        .agg(F.max("sm_empty_slot_full_credit").alias("sm_empty_slot_full_credit"))
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
        .agg(
            F.sum("contribution").alias("occupancy_time"),
            F.max("activity_occuped").alias("_matching_overlap"),
        )
        .withColumn(
            "occupancy_time",
            F.least(F.col("occupancy_time"), F.lit(SLOT_DURATION_SECONDS)),
        )
    )

    # LEFT-JOIN back to keep slots with no matching job (occupancy_time = 0)
    # — except eligible pre-cutover SM slots, which reproduce legacy's
    # `ELSE 1800`. Legacy's `SUM(CASE WHEN activity_occuped = 1 THEN duration
    # END)` (line 1129) is NULL whenever NO overlapping job matches the slot's
    # activity type — both "no overlapping job at all" and "overlapping jobs
    # but none matching" — so the default keys off `_matching_overlap`, not
    # merely the absence of a per_slot row.
    joined_back = slots.join(per_slot, on=keys, how="left")
    sm_full_credit = F.col("sm_empty_slot_full_credit") & (
        F.coalesce(F.col("_matching_overlap"), F.lit(0)) == 0
    )
    return (
        joined_back.withColumn(
            "occupancy_time",
            F.when(sm_full_credit, F.lit(SLOT_DURATION_SECONDS))
            .otherwise(F.coalesce(F.col("occupancy_time"), F.lit(0)))
            .cast("long"),
        )
        .drop("sm_empty_slot_full_credit", "_matching_overlap")
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
) -> DataFrame:
    """End-to-end occupancy_time pipeline (raw per-slot occupancy minutes).

    ``sm_jobs`` (Sprinklr ``sm_jobs`` extractor) is optional: when provided, the
    Social-Media case assignments are unioned in as ``oos``-typed jobs so social
    agents' occupancy is populated from Sprinklr (they have no shuffle/taskmaster
    jobs). ``social`` DIME slots are kept and ``sm_jobs`` is unioned on ALL dates
    (see ``filter_dime`` / module docstring): Social-Media occupancy is
    intentionally ON for the whole history, a documented divergence from legacy
    (which had no Sprinklr source).
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
    per_slot = per_slot.drop("squad")

    # Collapse to ONE row per physical slot, summing occupancy across any
    # duplicate slot rows, then (re)cap at the slot length. A single slot can
    # appear more than once at this point because the same (agent, date,
    # slot_start) can carry >1 DIME ``agent_dime_squad`` — most commonly because
    # ``append_missing_dime_slots`` re-adds a now-backfilled slot under a
    # different squad label (e.g. ``Content`` vs ``content_content``), which the
    # ``.distinct()`` on slot keys keeps as separate rows. Jobs match on
    # agent/date/time (not squad), so each duplicate carries identical occupancy;
    # without this collapse a 30-min slot would double to 60. Legacy reproduces
    # exactly this — its final ``normalized_occupancy_final`` groups to one row
    # per ``slot_start`` and applies ``LEAST(SUM(occupancy_time), 1800)`` AFTER
    # summing — so we sum here and let the post-roster cap below bound it.
    per_slot = per_slot.groupBy(
        "agent", "date", "slot_start", "slot_end", "activity_type_required"
    ).agg(F.sum("occupancy_time").alias("occupancy_time"))

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
        F.to_date(F.col("snapshot_date")).alias("_snapshot_date"),
        F.trunc(F.to_date(F.col("snapshot_month")), "month").alias("snapshot_month"),
    )

    # Deduplicate the roster to exactly ONE row per (agent, snapshot_month) BEFORE
    # the slot join. The content branch of `agent_information` (see
    # extractors/agent_information.sql `content_monthly`) cross-joins each Google-
    # Sheet content row against every month, so a content agent who appears on
    # more than one sheet row (e.g. supporting multiple `target_squad`s) yields
    # ≥2 rows per (agent, snapshot_month) that are identical on every column this
    # view selects (only `target_squad`, which occupancy does not use, differs).
    # Without this dedup the inner join below fans out — every slot is duplicated,
    # so a 30-min slot sums to 60 min and the per-slot ≤30-min invariant breaks.
    # Keep the latest snapshot deterministically (recency, then stable tiebreaks).
    roster_dedup_window = Window.partitionBy("agent", "snapshot_month").orderBy(
        F.col("_snapshot_date").desc_nulls_last(),
        F.col("squad").asc_nulls_last(),
        F.col("district").asc_nulls_last(),
        F.col("shift").asc_nulls_last(),
    )
    roster = (
        roster.withColumn(
            "_roster_rn", F.row_number().over(roster_dedup_window)
        )
        .filter(F.col("_roster_rn") == 1)
        .drop("_roster_rn", "_snapshot_date")
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
