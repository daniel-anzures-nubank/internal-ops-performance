"""jobs_raw — one row per individual job (shuffle + OOS), with start/end time.

This is a RAW dataset, not a finished metric. It is the per-job feed that a
downstream ``metrics`` layer aggregates into NTPJ (count, duration, the
monthly expected-duration benchmark, and the NTPJ ratio). Here we keep every
job as its own row with its raw start/end timestamps, classification fields,
and a flag for whether the agent was scheduled for that activity that day.

Public API
----------
``compute_jobs_raw(agent_info, dime, shuffle_jobs, oos_jobs)`` returns one
row per job (shuffle + OOS) attributed to an active roster agent.

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
  status filter — that's a metric decision.
* OOS jobs: synthetic ``activity_type='oos'`` and ``status='finished'``
  (taskmaster exposes neither). Content-squad ``job_classification`` cleanup
  is applied so ``job_id`` matches legacy whenever a content source exists.
* Roster: ``status='active'`` (inner join attaches dimensions / scopes output).

``required_activity_on_day_flag`` (the one derived field)
---------------------------------------------------------
1 if the agent was SCHEDULED (had required DIME hours) for that job's
``activity_type`` on that day, else 0. "Scheduled / required" uses the NTPJ
DIME definition: slots with non-null ``activity_type_required`` not in
(lunch_break / shrinkage / time_off), non-null squad not in
(wfm / credit_evolution / dote), and ``shuffle_status_required IN
('available', 'oos')``. Jobs done for an activity the agent wasn't scheduled
for that day (e.g. cross-support) get flag 0.

Deferred to the future metrics layer (NOT done here)
----------------------------------------------------
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

import pandas as pd

from shift_attribution import night_agent_months, shift_start_date
from adjustments.manual import reclassify_dime_slots

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
NTPJ_OUT_OF_SCOPE_SQUADS: tuple[str, ...] = ()

# Slot length — 30 minutes. Used to turn a slot count into scheduled hours.
SLOT_DURATION_HOURS: float = 0.5


# ---------------------------------------------------------------------------
# Small utility
# ---------------------------------------------------------------------------


def _as_naive_datetime(series: pd.Series) -> pd.Series:
    """Coerce a datetime Series to tz-naive ``datetime64[ns]``.

    Used for merge keys. The warehouse returns ``DATE_TRUNC('month', ...)``
    as tz-aware UTC even though we documented ``snapshot_month`` as a DATE;
    stripping the tz keeps ``(agent, snapshot_month)`` joins predictable.
    """
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        return s.dt.tz_localize(None)
    return s


# ---------------------------------------------------------------------------
# job_id derivation (must match legacy verbatim for downstream benchmark joins)
# ---------------------------------------------------------------------------


def _shuffle_job_id(
    activity_type: pd.Series, job_type: pd.Series, status: pd.Series
) -> pd.Series:
    """Vectorized version of the legacy CASE for shuffle job_id.

    Legacy SQL:
        WHEN activity_type = 'email'      THEN 'email - ' || received_source_q || ' - ' || status
        WHEN activity_type = 'backoffice' THEN 'bko - '   || received_source_q || ' - ' || status
        ELSE activity_type || ' - ' || status

    Email/backoffice include the queue (job_type); chat / voice / etc. do not.
    """
    job_type_str = job_type.fillna("").astype(str)
    status_str = status.fillna("").astype(str)
    out = activity_type.fillna("").astype(str) + " - " + status_str
    email_mask = activity_type == "email"
    bko_mask = activity_type == "backoffice"
    out = out.mask(email_mask, "email - " + job_type_str + " - " + status_str)
    out = out.mask(bko_mask, "bko - " + job_type_str + " - " + status_str)
    return out


def _clean_oos_job_classification(
    job_classification: pd.Series, squad: pd.Series
) -> pd.Series:
    """Apply the legacy content-squad cleanup.

    For OOS jobs whose ``squad`` matches ``'%content%'``:
        LOWER(REPLACE(TRIM(REPLACE(job_classification, '(OOS_CONT)', '')), ' ', '_'))
    Non-content rows pass ``job_classification`` through untouched.
    """
    job_str = job_classification.astype(str)
    cleaned = (
        job_str.str.replace("(OOS_CONT)", "", regex=False)
        .str.strip()
        .str.replace(" ", "_", regex=False)
        .str.lower()
    )
    is_content = squad.fillna("").astype(str).str.contains("content", regex=False)
    return job_str.mask(is_content, cleaned)


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


def _empty_jobs() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in _JOB_COLS})


def build_shuffle_jobs_raw(shuffle_jobs: pd.DataFrame) -> pd.DataFrame:
    """One row per shuffle job (ALL statuses), with start/end + derived job_id."""
    if shuffle_jobs.empty:
        return _empty_jobs()

    df = shuffle_jobs.copy()
    out = pd.DataFrame(
        {
            "agent": df["agent"],
            "date": df["date"],
            "start_time": df["local_start_time"],
            "end_time": df["local_stop_time"],
            "job_type": df["job_type"],
            "activity_type": df["activity_type"],
            "status": df["status"],
            "duration_seconds": df["net_time_spent_seconds"],
        }
    )
    out["job_id"] = _shuffle_job_id(out["activity_type"], out["job_type"], out["status"])
    return out[list(_JOB_COLS)]


def build_oos_jobs_raw(oos_jobs: pd.DataFrame) -> pd.DataFrame:
    """One row per OOS job, with synthetic activity_type/status + derived job_id."""
    if oos_jobs.empty:
        return _empty_jobs()

    df = oos_jobs.copy()
    df["job_classification"] = _clean_oos_job_classification(
        df["job_classification"], df["squad"]
    )
    out = pd.DataFrame(
        {
            "agent": df["agent"],
            "date": df["date"],
            "start_time": df["local_start_date"],
            "end_time": df["local_stop_date"],
            "job_type": df["job_classification"],
            "activity_type": "oos",
            "status": "finished",
            "duration_seconds": df["net_time_spent_seconds"],
        }
    )
    out["job_id"] = "oos - " + df["job_classification"].astype(str)
    return out[list(_JOB_COLS)]


def build_jobs_union(
    shuffle_jobs: pd.DataFrame, oos_jobs: pd.DataFrame
) -> pd.DataFrame:
    """Concatenate the per-job shuffle and OOS frames."""
    shuffle_part = build_shuffle_jobs_raw(shuffle_jobs)
    oos_part = build_oos_jobs_raw(oos_jobs)
    parts = [p[list(_JOB_COLS)] for p in (shuffle_part, oos_part) if not p.empty]
    if not parts:
        return _empty_jobs()
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------------------
# Step 2: DIME filter + required-activity set (for the flag)
# ---------------------------------------------------------------------------


def filter_dime(dime: pd.DataFrame) -> pd.DataFrame:
    """Apply NTPJ's DIME filter set (used only to compute the required flag).

    Drops:
      * NULL ``activity_type_required``
      * ``activity_type_required`` in lunch_break / shrinkage / time_off
      * NULL ``squad``
      * ``squad`` in wfm / credit_evolution / dote
      * ``shuffle_status_required`` NOT IN ('available', 'oos')
    """
    mask = (
        dime["activity_type_required"].notna()
        & ~dime["activity_type_required"].isin(DIME_ACTIVITY_TYPE_EXCLUSIONS)
        & dime["squad"].notna()
        & ~dime["squad"].isin(DIME_SQUAD_EXCLUSIONS)
        & dime["shuffle_status_required"].isin(DIME_SHUFFLE_STATUS_VALUES)
    )
    return dime.loc[mask].copy()


def compute_required_activities(dime_filtered: pd.DataFrame) -> pd.DataFrame:
    """Per (agent, date, activity_type), 1 if the agent was scheduled that day.

    Returns the distinct ``(agent, date, activity_type)`` triples that have at
    least one required DIME slot, with a constant ``required_flag = 1``. Used
    to left-join the jobs and fill the flag (missing → 0).
    """
    grouped = (
        dime_filtered.groupby(
            ["agent", "date", "activity_type_required"], as_index=False, dropna=False
        )
        .size()
        .rename(columns={"activity_type_required": "activity_type"})
    )
    grouped = grouped.loc[grouped["size"] > 0].copy()
    grouped["required_flag"] = 1
    return grouped[["agent", "date", "activity_type", "required_flag"]]


# ---------------------------------------------------------------------------
# Step 3: orchestrator — union jobs + flag + roster join
# ---------------------------------------------------------------------------


def compute_jobs_raw(
    agent_info: pd.DataFrame,
    dime: pd.DataFrame,
    shuffle_jobs: pd.DataFrame,
    oos_jobs: pd.DataFrame,
    *,
    dime_inconsistencies: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """End-to-end jobs_raw pipeline (one row per job)."""
    jobs = build_jobs_union(shuffle_jobs, oos_jobs)

    # Night-shift agents that cross midnight are re-attributed to the day their
    # shift started (>= 2026-07-01 only). Both the jobs and the DIME
    # required-set are re-attributed with the SAME rule (jobs keyed off their
    # local ``start_time``, DIME off ``slot_start_local_unix``) so the
    # ``(agent, date, activity_type)`` required-flag join below stays aligned.
    night_months = night_agent_months(agent_info)
    jobs["date"] = shift_start_date(
        jobs,
        agent_col="agent",
        local_ts_col="start_time",
        calendar_date_col="date",
        night_months=night_months,
    )

    # --- required-activity flag ---------------------------------------------
    dime_f = filter_dime(reclassify_dime_slots(dime, dime_inconsistencies))
    dime_f["_local_ts"] = pd.to_datetime(dime_f["slot_start_local_unix"], unit="s")
    dime_f["date"] = shift_start_date(
        dime_f,
        agent_col="agent",
        local_ts_col="_local_ts",
        calendar_date_col="date",
        night_months=night_months,
    )
    dime_f = dime_f.drop(columns="_local_ts")
    required = compute_required_activities(dime_f)
    jobs = jobs.merge(required, on=["agent", "date", "activity_type"], how="left")
    jobs["required_activity_on_day_flag"] = (
        jobs["required_flag"].fillna(0) > 0
    ).astype("int64")

    # --- roster join --------------------------------------------------------
    roster = agent_info.loc[
        (agent_info["status"] == "active")
        & ~agent_info["squad"].isin(NTPJ_OUT_OF_SCOPE_SQUADS),
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

    jobs["snapshot_month"] = _as_naive_datetime(
        pd.to_datetime(jobs["date"]).dt.to_period("M").dt.to_timestamp()
    )
    enriched = jobs.merge(roster, on=["agent", "snapshot_month"], how="inner")

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
            "start_time",
            "end_time",
            "job_type",
            "activity_type",
            "status",
            "job_id",
            "duration_seconds",
            "required_activity_on_day_flag",
        ]
    ].copy()

    out["duration_seconds"] = pd.to_numeric(
        out["duration_seconds"], errors="coerce"
    ).astype("Int64")
    out["required_activity_on_day_flag"] = out[
        "required_activity_on_day_flag"
    ].astype("int64")

    return out.sort_values(["date", "agent", "start_time"]).reset_index(drop=True)


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
