-- =====================================================================================
-- Shuffle jobs — raw extractor (ops_canonical_time_spent_activities)
-- =====================================================================================
--
-- Purpose
--   Returns one row per shuffle job execution from
--   `etl.mx__dataset.ops_canonical_time_spent_activities`. Used by NTPJ and NOcc.
--
-- Scope of this query (what IS done here)
--   * Pull every shuffle job in the period.
--   * Normalize agent to email prefix (lowercased).
--   * Scope to `actor_affiliation = 'nubank'` (this is project scope, not metric
--     logic — the entire MX CX project is nubank-only). The metric layer can still
--     observe the raw `actor_affiliation` via the column below.
--   * Expose raw `status`, `activity_type`, `received_source_q`, `net_time_spent`.
--
-- Out of scope (handled by the metrics layer)
--   * Status filtering. Legacy NTPJ keeps `status = 'finished'`; legacy NOcc keeps
--     `status IN ('finished', 'transferred', 'skipped')`. Each metric applies its own.
--   * Activity-type filtering / matching against DIME's `activity_type_required`.
--   * Aggregation per (agent, date, job_id).
--   * Computing expected duration / occupancy benchmarks.
--   * Joining the BDX roster (xforce / xplead / squad / status='active').
--
-- Out of scope (handled by the Adjustments layer)
--   * Per-agent / per-date hardcoded exclusions.
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `local_start_time`
--   :period_end   DATE  inclusive upper bound on `local_start_time`
--
-- Output schema (one row per shuffle job execution)
--   agent                   STRING     email prefix, lowercased
--   actor_affiliation       STRING     raw `actor_affiliation` (already filtered to 'nubank')
--   date                    DATE       `DATE(local_start_time)`
--   job_type                STRING     `received_source_q`
--   activity_type           STRING     raw `activity_type` (e.g. 'email', 'backoffice', 'chat')
--   status                  STRING     raw `status` (e.g. 'finished', 'transferred', 'skipped')
--   net_time_spent_seconds  BIGINT     `net_time_spent`, in seconds
--   local_start_time        TIMESTAMP  raw job start (local time)
--   local_stop_time         TIMESTAMP  raw job end (local time)
--   activity_start_unix     BIGINT     `UNIX_TIMESTAMP(local_start_time)` (local-time unix)
--   activity_end_unix       BIGINT     `UNIX_TIMESTAMP(local_stop_time)`  (local-time unix)
--
-- Timezone notes
--   * Both `local_start_time` and `local_stop_time` are stored in **local time**.
--     The unix conversions therefore yield a "local-time unix" — directly comparable
--     to `dime_slots.slot_start_local_unix` without offset adjustment.
-- =====================================================================================

SELECT
  LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
  actor_affiliation,
  DATE(local_start_time)                                    AS date,
  received_source_q                                         AS job_type,
  activity_type,
  status,
  CAST(net_time_spent AS BIGINT)                            AS net_time_spent_seconds,
  local_start_time,
  local_stop_time,
  UNIX_TIMESTAMP(local_start_time)                          AS activity_start_unix,
  UNIX_TIMESTAMP(local_stop_time)                           AS activity_end_unix
FROM etl.mx__dataset.ops_canonical_time_spent_activities
WHERE actor_affiliation = 'nubank'
  AND local_start_time >= :period_start
  AND local_start_time <  DATE_ADD(:period_end, 1)
