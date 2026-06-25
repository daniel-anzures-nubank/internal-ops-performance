-- =====================================================================================
-- SM jobs — Social-Media occupancy "jobs" extractor (Sprinklr)
-- =====================================================================================
--
-- Purpose
--   Returns one row per Social-Media case assignment from
--   `usr.sprinklr_api_data_integration.sprinklr_normalized_occupancy_data`. This is
--   the Social-Media equivalent of `oos_jobs`: each case assignment is an interval
--   of time the agent was occupied on a social case. Used by occupancy_time to fill
--   in Social-Media occupancy (social agents do their work in Sprinklr, not in the
--   shuffle / taskmaster job tables).
--
-- Scope of this query (what IS done here)
--   * Pull every case assignment in the period.
--   * Normalize agent to email prefix (lowercased) from `agent_email_id`.
--   * Expose the assignment start/end both as raw timestamps and as "local-time
--     unix" — `UNIX_TIMESTAMP(...)`, the SAME convention `dime_slots` and
--     `oos_jobs` use — so occupancy_time can overlap these intervals against DIME
--     slots with no offset adjustment (this matches the legacy SM notebook, which
--     compares `unix_timestamp(case_assignment_time)` directly against the DIME
--     slot unix; verified empirically — social agents' DIME `oos` slot hours line
--     up with these assignment hours).
--   * Drop rows with a NULL assignment or unassignment time (an open/unmeasurable
--     case contributes no occupancy).
--
-- Out of scope (handled by the metrics layer)
--   * Activity-type matching (occupancy_time stamps every SM job `activity_type='oos'`
--     and matches it against DIME `activity_type_required='oos'`, mirroring legacy).
--   * The per-slot interval dedup, the 1800s cap, the monthly district/shift
--     benchmark, and the NOcc ratio.
--   * Roster join / active filter / squad scoping.
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `DATE(case_assignment_time)`
--   :period_end   DATE  inclusive upper bound on `DATE(case_assignment_time)`
--
-- Output schema (one row per SM case assignment)
--   agent                   STRING     email prefix, lowercased
--   date                    DATE       `DATE(case_assignment_time)`
--   net_time_spent_seconds  BIGINT     assignment duration in seconds (end - start)
--   case_assignment_time    TIMESTAMP  raw case assignment start
--   case_unassignment_time  TIMESTAMP  raw case assignment end
--   activity_start_unix     BIGINT     `UNIX_TIMESTAMP(case_assignment_time)`
--   activity_end_unix       BIGINT     `UNIX_TIMESTAMP(case_unassignment_time)`
-- =====================================================================================

SELECT
  LOWER(REGEXP_EXTRACT(agent_email_id, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
  DATE(case_assignment_time)                                         AS date,
  CAST(
    UNIX_TIMESTAMP(case_unassignment_time) - UNIX_TIMESTAMP(case_assignment_time)
    AS BIGINT
  )                                                                  AS net_time_spent_seconds,
  case_assignment_time,
  case_unassignment_time,
  UNIX_TIMESTAMP(case_assignment_time)                               AS activity_start_unix,
  UNIX_TIMESTAMP(case_unassignment_time)                             AS activity_end_unix
FROM usr.sprinklr_api_data_integration.sprinklr_normalized_occupancy_data
WHERE case_assignment_time IS NOT NULL
  AND case_unassignment_time IS NOT NULL
  AND DATE(case_assignment_time) >= :period_start
  AND DATE(case_assignment_time) <= :period_end
