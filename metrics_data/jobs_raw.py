"""jobs_raw — one row per individual job (shuffle + OOS), with start/end time (PySpark).

This is a RAW dataset, not a finished metric. It is the per-job feed that a
downstream ``metrics`` layer aggregates into NTPJ (count, duration, the
monthly expected-duration benchmark, and the NTPJ ratio). Here we keep every
job as its own row with its raw start/end timestamps, classification fields,
a flag for whether the agent was scheduled for that activity that day, and the
roster attribution (left-joined, so every job survives for the benchmark).

Public API
----------
``compute_jobs_raw(agent_info, dime, shuffle_jobs, oos_jobs)`` takes Spark
DataFrames (the extractor outputs) and returns one Spark DataFrame with one
row per job (shuffle + OOS).

Source tables (via extractors)
------------------------------
* ``agent_information``  → ``etl.mx__series_contract.cx_mx_bdx_snapshots`` (+ ``ops_actors``).
* ``dime_slots``         → ``etl.mx__series_contract.agent_dimensioned_activities``
  (``affiliation = 'nubank'``). Used ONLY to derive ``required_activity_on_day_flag``.
* ``shuffle_jobs``       → ``etl.mx__dataset.ops_canonical_time_spent_activities``
  (``actor_affiliation = 'nubank'``). One row per shuffle job execution.
* ``oos_jobs``           → ``etl.mx__dataset.taskmaster_consolidated_registry``.
  One row per Out-of-Shuffle job execution.

Filters applied here (deliberately minimal — this is a raw table)
-----------------------------------------------------------------
* Shuffle jobs: ALL statuses kept (finished / transferred / skipped / …). No
  status filter — that's a metric decision (legacy NTPJ keeps only 'finished').
* OOS jobs: synthetic ``activity_type='oos'`` and ``status='finished'``
  (taskmaster exposes neither). Content-squad ``job_classification`` cleanup
  is applied so ``job_id`` matches legacy whenever a content source exists.
* Roster: LEFT join (NOT inner). Legacy builds the expected-duration benchmark
  from the un-roster-filtered job pool (``jobs_base_ntpj``) and applies the
  ``status = 'active'`` / roster filter only to the agent CONTRIBUTION
  downstream (``ntpj_all_info_2025/2026``). So we attach roster dimensions and
  the roster ``status`` via a LEFT join here, keep every job (so the benchmark
  pool is complete), and let the metric layer scope the contribution to
  ``roster_status == 'active'``. ``roster_status`` is NULL for a job with no
  matching roster row that month (still a valid benchmark contributor).

Legacy 2025 roster pinning
--------------------------
Legacy ``ntpj_all_info_2025`` pins every 2025-01-01 … 2025-11-30 job to the
``snapshot_month = '2025-12-01'`` roster snapshot (its squad/xforce/xplead
attribution), while ``ntpj_all_info_2026`` joins each ≥ 2025-12-01 job to its
own month. We reproduce that: a job whose date is before
:data:`ROSTER_PIN_2025_MONTH` joins the Dec-2025 snapshot; everything else
joins its own calendar month.

``required_activity_on_day_flag`` (the one derived field)
---------------------------------------------------------
1 if the agent was SCHEDULED (had required DIME hours) for that job's
``activity_type`` on that day, else 0. "Scheduled / required" uses the NTPJ
DIME definition: slots with non-null ``activity_type_required`` not in
(lunch_break / shrinkage / time_off), non-null ``agent_dime_squad`` not in
(wfm / credit_evolution / dote), and ``shuffle_status_required IN
('available', 'oos')``. Jobs done for an activity the agent wasn't scheduled
for that day (e.g. cross-support) get flag 0.

Deferred to the metrics layer (NOT done here)
---------------------------------------------
* Aggregation to (agent, date, job_id) with count / duration.
* The monthly expected-duration benchmark (``exp_duration_job``) and its
  4-month-window / current-month cutover.
* The NTPJ ratio.
* All per-agent / per-date manual adjustments, cross-support queue
  exclusions, and outage-date carve-outs.

Output schema (one row per job)
-------------------------------
    agent                          STRING
    xforce                         STRING
    xplead                         STRING
    team                           STRING   performance team (from roster; see team_squad_mapping)
    squad                          STRING   roster squad
    district                       STRING   roster district (was ``squad_district``)
    shift                          STRING   roster shift
    roster_status                  STRING   roster status ('active'/…); NULL if no roster row
    date                           DATE
    start_time                     TIMESTAMP  job start (local time)
    end_time                       TIMESTAMP  job end (local time)
    job_type                       STRING
    activity_type                  STRING   'email'/'backoffice'/'chat'/… or 'oos'
    status                         STRING
    job_id                         STRING   legacy job_id naming (used for benchmark joins)
    duration_seconds               BIGINT   net time spent on the job
    required_activity_on_day_flag  INT      1 if scheduled for that activity that day, else 0
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from shift_attribution import night_agent_months, shift_start_date

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# DIME filters used to derive ``required_activity_on_day_flag`` (the NTPJ
# definition of "scheduled / required"). NTPJ also requires
# ``shuffle_status_required IN ('available', 'oos')``.
DIME_ACTIVITY_TYPE_EXCLUSIONS: tuple[str, ...] = (
    "lunch_break",
    "shrinkage",
    "time_off",
)

DIME_SQUAD_EXCLUSIONS: tuple[str, ...] = (
    "wfm",
    "credit_evolution",
    "dote",
)

DIME_SHUFFLE_STATUS_VALUES: tuple[str, ...] = ("available", "oos")

# Roster-level squad exclusions. Currently empty — all squads in scope.
# Legacy NTPJ keeps content (via the gsheets roster) and does not NTPJ-filter
# social (which simply has no shuffle/OOS jobs), so no roster-squad drop.
NTPJ_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# Legacy 2025 roster pinning: jobs before this month are pinned to the
# 2025-12-01 roster snapshot; from this month on, each job joins its own month.
ROSTER_PIN_2025_MONTH: date = date(2025, 12, 1)


# ---------------------------------------------------------------------------
# job_id derivation (must match legacy verbatim for downstream benchmark joins)
# ---------------------------------------------------------------------------


def _shuffle_job_id(
    activity_type: "F.Column", job_type: "F.Column", status: "F.Column"
) -> "F.Column":
    """Vectorized version of the legacy CASE for shuffle job_id.

    Legacy SQL:
        WHEN activity_type = 'email'      THEN 'email - ' || received_source_q || ' - ' || status
        WHEN activity_type = 'backoffice' THEN 'bko - '   || received_source_q || ' - ' || status
        ELSE activity_type || ' - ' || status

    Email/backoffice include the queue (job_type); chat / voice / etc. do not.
    """
    jt = F.coalesce(job_type, F.lit(""))
    st = F.coalesce(status, F.lit(""))
    at = F.coalesce(activity_type, F.lit(""))
    return (
        F.when(
            activity_type == F.lit("email"),
            F.concat(F.lit("email - "), jt, F.lit(" - "), st),
        )
        .when(
            activity_type == F.lit("backoffice"),
            F.concat(F.lit("bko - "), jt, F.lit(" - "), st),
        )
        .otherwise(F.concat(at, F.lit(" - "), st))
    )


def _clean_oos_job_classification(
    job_classification: "F.Column", squad: "F.Column"
) -> "F.Column":
    """Apply the legacy content-squad cleanup.

    For OOS jobs whose ``squad`` matches ``'%content%'``:
        LOWER(REPLACE(TRIM(REPLACE(job_classification, '(OOS_CONT)', '')), ' ', '_'))
    Non-content rows pass ``job_classification`` through untouched.
    """
    cleaned = F.lower(
        F.regexp_replace(
            F.trim(F.regexp_replace(job_classification, r"\(OOS_CONT\)", "")),
            " ",
            "_",
        )
    )
    is_content = F.coalesce(squad, F.lit("")).contains("content")
    return F.when(is_content, cleaned).otherwise(job_classification)


# ---------------------------------------------------------------------------
# Step 1: build the per-job union (no aggregation)
# ---------------------------------------------------------------------------

_JOB_COLS: tuple[str, ...] = (
    "agent",
    "date",
    "start_time",
    "end_time",
    "job_type",
    "activity_type",
    "status",
    "job_id",
    "duration_seconds",
)


def build_shuffle_jobs_raw(shuffle_jobs: DataFrame) -> DataFrame:
    """One row per shuffle job (ALL statuses), with start/end + derived job_id."""
    out = shuffle_jobs.select(
        F.col("agent"),
        F.col("date"),
        F.col("local_start_time").alias("start_time"),
        F.col("local_stop_time").alias("end_time"),
        F.col("job_type"),
        F.col("activity_type"),
        F.col("status"),
        F.col("net_time_spent_seconds").alias("duration_seconds"),
    )
    out = out.withColumn(
        "job_id",
        _shuffle_job_id(F.col("activity_type"), F.col("job_type"), F.col("status")),
    )
    return out.select(*_JOB_COLS)


def build_oos_jobs_raw(oos_jobs: DataFrame) -> DataFrame:
    """One row per OOS job, with synthetic activity_type/status + derived job_id."""
    df = oos_jobs.withColumn(
        "job_classification",
        _clean_oos_job_classification(F.col("job_classification"), F.col("squad")),
    )
    out = df.select(
        F.col("agent"),
        F.col("date"),
        F.col("local_start_date").alias("start_time"),
        F.col("local_stop_date").alias("end_time"),
        F.col("job_classification").alias("job_type"),
        F.lit("oos").alias("activity_type"),
        F.lit("finished").alias("status"),
        F.col("net_time_spent_seconds").alias("duration_seconds"),
        F.concat(F.lit("oos - "), F.col("job_classification")).alias("job_id"),
    )
    return out.select(*_JOB_COLS)


def build_jobs_union(shuffle_jobs: DataFrame, oos_jobs: DataFrame) -> DataFrame:
    """Concatenate the per-job shuffle and OOS frames."""
    shuffle_part = build_shuffle_jobs_raw(shuffle_jobs)
    oos_part = build_oos_jobs_raw(oos_jobs)
    return shuffle_part.unionByName(oos_part)


# ---------------------------------------------------------------------------
# Step 2: DIME filter + required-activity set (for the flag)
# ---------------------------------------------------------------------------


def filter_dime(dime: DataFrame) -> DataFrame:
    """Apply NTPJ's DIME filter set (used only to compute the required flag).

    Drops:
      * NULL ``activity_type_required``
      * ``activity_type_required`` in lunch_break / shrinkage / time_off
      * NULL ``squad`` (the DIME ``agent_dime_squad``)
      * ``squad`` in wfm / credit_evolution / dote
      * ``shuffle_status_required`` NOT IN ('available', 'oos')

    Mirrors legacy ``dime_ntpj``.
    """
    return (
        dime.filter(F.col("activity_type_required").isNotNull())
        .filter(~F.col("activity_type_required").isin(list(DIME_ACTIVITY_TYPE_EXCLUSIONS)))
        .filter(F.col("squad").isNotNull())
        .filter(~F.col("squad").isin(list(DIME_SQUAD_EXCLUSIONS)))
        .filter(F.col("shuffle_status_required").isin(list(DIME_SHUFFLE_STATUS_VALUES)))
    )


def compute_required_activities(dime_filtered: DataFrame) -> DataFrame:
    """Distinct ``(agent, date, activity_type)`` with at least one required slot.

    Returns one row per ``(agent, date, activity_type)`` triple that has a
    required DIME slot, with a constant ``required_flag = 1``. Used to left-join
    the jobs and fill the flag (missing → 0).
    """
    return (
        dime_filtered.select(
            F.col("agent"),
            F.col("date"),
            F.col("activity_type_required").alias("activity_type"),
        )
        .distinct()
        .withColumn("required_flag", F.lit(1))
    )


# ---------------------------------------------------------------------------
# Step 3: orchestrator — union jobs + flag + roster join
# ---------------------------------------------------------------------------


def compute_jobs_raw(
    agent_info: DataFrame,
    dime: DataFrame,
    shuffle_jobs: DataFrame,
    oos_jobs: DataFrame,
) -> DataFrame:
    """End-to-end jobs_raw pipeline (one row per job)."""
    jobs = build_jobs_union(shuffle_jobs, oos_jobs)

    # Night-shift agents that cross midnight are re-attributed to the day their
    # shift started (>= 2026-07-01 only). Both the jobs and the DIME
    # required-set are re-attributed with the SAME rule (jobs keyed off their
    # local ``start_time``, DIME off ``slot_start_local_unix``) so the
    # ``(agent, date, activity_type)`` required-flag join below stays aligned.
    night_months = night_agent_months(agent_info)
    jobs = shift_start_date(
        jobs,
        agent_col="agent",
        local_ts_col="start_time",
        calendar_date_col="date",
        night_months=night_months,
    )

    # --- required-activity flag ---------------------------------------------
    dime_f = filter_dime(dime)
    dime_f = dime_f.withColumn(
        "_local_ts", F.timestamp_seconds(F.col("slot_start_local_unix"))
    )
    dime_f = shift_start_date(
        dime_f,
        agent_col="agent",
        local_ts_col="_local_ts",
        calendar_date_col="date",
        night_months=night_months,
    ).drop("_local_ts")
    required = compute_required_activities(dime_f)

    jobs = jobs.join(required, on=["agent", "date", "activity_type"], how="left")
    jobs = jobs.withColumn(
        "required_activity_on_day_flag",
        (F.coalesce(F.col("required_flag"), F.lit(0)) > 0).cast("int"),
    ).drop("required_flag")

    # --- roster join (LEFT, with legacy 2025 pinning) -----------------------
    # Legacy builds the benchmark from the un-roster-filtered pool and applies
    # status='active' only to the contribution, so we LEFT join (keep every job)
    # and carry roster_status for the metric layer to scope the contribution.
    roster = agent_info
    if NTPJ_OUT_OF_SCOPE_SQUADS:
        roster = roster.filter(~F.col("squad").isin(list(NTPJ_OUT_OF_SCOPE_SQUADS)))
    roster = roster.select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        F.col("squad_district").alias("district"),
        "shift",
        F.col("status").alias("roster_status"),
        F.trunc(F.to_date(F.col("snapshot_month")), "month").alias("snapshot_month"),
    )

    # Deduplicate the roster to ONE row per (agent, snapshot_month) before the
    # join. The content branch of ``agent_information`` cross-joins each
    # Google-Sheet content row against every month, so a content agent who
    # appears on >1 sheet row (supporting multiple ``target_squad``s) yields ≥2
    # rows per (agent, snapshot_month) identical on every column NTPJ uses (only
    # ``target_squad``, unused here, differs). Without this dedup a LEFT join
    # fans out and double-counts that agent's jobs. Keep the latest snapshot
    # deterministically.
    roster_dedup_window = Window.partitionBy("agent", "snapshot_month").orderBy(
        F.col("roster_status").asc_nulls_last(),
        F.col("squad").asc_nulls_last(),
        F.col("district").asc_nulls_last(),
        F.col("shift").asc_nulls_last(),
    )
    roster = (
        roster.withColumn("_roster_rn", F.row_number().over(roster_dedup_window))
        .filter(F.col("_roster_rn") == 1)
        .drop("_roster_rn")
    )

    # Legacy 2025 pinning: jobs before 2025-12-01 attach the 2025-12-01 roster
    # snapshot; from 2025-12-01 on, each job attaches its own calendar month.
    job_month = F.trunc(F.to_date(F.col("date")), "month")
    jobs = jobs.withColumn(
        "snapshot_month",
        F.when(
            F.to_date(F.col("date")) < F.lit(ROSTER_PIN_2025_MONTH),
            F.lit(ROSTER_PIN_2025_MONTH),
        ).otherwise(job_month),
    )

    enriched = jobs.join(roster, on=["agent", "snapshot_month"], how="left").drop(
        "snapshot_month"
    )

    out = enriched.select(
        "agent",
        "xforce",
        "xplead",
        "team",
        "squad",
        "district",
        "shift",
        "roster_status",
        F.to_date(F.col("date")).alias("date"),
        "start_time",
        "end_time",
        "job_type",
        "activity_type",
        "status",
        "job_id",
        F.col("duration_seconds").cast("long").alias("duration_seconds"),
        F.col("required_activity_on_day_flag").cast("int").alias(
            "required_activity_on_day_flag"
        ),
    )

    return out.orderBy("date", "agent", "start_time")


# ---------------------------------------------------------------------------
# Output schema declaration — used by scripts/metrics_data_scripts/build_jobs_raw.py
# ---------------------------------------------------------------------------

IO_JOBS_RAW_SCHEMA: tuple[tuple[str, str], ...] = (
    ("agent", "STRING"),
    ("xforce", "STRING"),
    ("xplead", "STRING"),
    ("team", "STRING"),
    ("squad", "STRING"),
    ("district", "STRING"),
    ("shift", "STRING"),
    ("roster_status", "STRING"),
    ("date", "DATE"),
    ("start_time", "TIMESTAMP"),
    ("end_time", "TIMESTAMP"),
    ("job_type", "STRING"),
    ("activity_type", "STRING"),
    ("status", "STRING"),
    ("job_id", "STRING"),
    ("duration_seconds", "BIGINT"),
    ("required_activity_on_day_flag", "INT"),
)
