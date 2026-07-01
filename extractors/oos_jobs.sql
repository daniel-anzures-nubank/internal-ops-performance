-- =====================================================================================
-- OOS jobs — raw extractor (taskmaster_consolidated_registry)
-- =====================================================================================
--
-- Purpose
--   Returns one row per Out-of-Shuffle (OOS) job execution from
--   `etl.mx__dataset.taskmaster_consolidated_registry`. Used by NTPJ and NOcc.
--
-- Scope of this query (what IS done here)
--   * Pull every OOS job in the period.
--   * Normalize agent to email prefix (lowercased).
--   * Expose raw `job_classification`, `net_time_spent_seconds`, `squad`, `comment`.
--
-- Out of scope (handled by the metrics layer)
--   * Status filtering (legacy implicitly treats every taskmaster row as `finished`;
--     if `taskmaster_consolidated_registry` ever exposes a `status` we care about,
--     the metric decides what to do with it).
--   * Affiliation filtering (taskmaster doesn't have an `actor_affiliation` column
--     equivalent; legacy doesn't filter — relies on roster join to keep only nubank
--     agents).
--   * Content-specific `job_classification` cleanup (legacy: when `squad LIKE '%content%'`,
--     it strips the `(OOS_CONT)` prefix and replaces spaces with underscores).
--   * MOS-NNNN `content_id` extraction from `comment` / `ticket__id`.
--   * Activity-type matching / aggregation / benchmark / roster join.
--
-- Out of scope (handled by the Adjustments layer)
--   * Per-agent / per-date hardcoded exclusions.
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `local_start_date`
--   :period_end   DATE  inclusive upper bound on `local_start_date`
--
-- Output schema (one row per OOS job execution)
--   agent                   STRING     email prefix, lowercased
--   date                    DATE       `DATE(local_start_date)`
--   job_classification      STRING     raw `job_classification`
--   net_time_spent_seconds  BIGINT     `net_time_spent_seconds`
--   local_start_date        TIMESTAMP  raw job start (local time)
--   local_stop_date         TIMESTAMP  raw job end (local time)
--   activity_start_unix     BIGINT     `UNIX_TIMESTAMP(local_start_date)`
--   activity_end_unix       BIGINT     `UNIX_TIMESTAMP(local_stop_date)`
--   squad                   STRING     squad assigned to the job at log time (raw)
--   comment                 STRING     free-text `comment` (used by NTPJ for MOS-NNNN parsing)
--   ticket__id              STRING     raw `ticket__id` (second source for MOS-NNNN parsing)
--
-- Timezone notes
--   * `local_start_date` / `local_stop_date` are stored in **local time**. The unix
--     conversions yield "local-time unix" — directly comparable to
--     `dime_slots.slot_start_local_unix` without offset adjustment.
-- =====================================================================================

SELECT
  LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
  DATE(local_start_date)                                    AS date,
  job_classification,
  CAST(net_time_spent_seconds AS BIGINT)                    AS net_time_spent_seconds,
  local_start_date,
  local_stop_date,
  UNIX_TIMESTAMP(local_start_date)                          AS activity_start_unix,
  UNIX_TIMESTAMP(local_stop_date)                           AS activity_end_unix,
  squad,
  comment,
  ticket__id
FROM etl.mx__dataset.taskmaster_consolidated_registry
WHERE local_start_date >= :period_start
  AND local_start_date <= :period_end
