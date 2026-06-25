-- =====================================================================================
-- DIME slots — raw extractor
-- =====================================================================================
--
-- Purpose
--   Returns one row per 30-minute DIME slot from `agent_dimensioned_activities`,
--   with no business filters applied. Shared by Adherence, NTPJ, NOcc, and Shrinkage.
--
-- Scope of this query (what IS done here)
--   * Pull every DIME slot in the period.
--   * Normalize agent to email prefix (lowercased).
--   * Scope to `affiliation = 'nubank'`. This is project scope (the entire MX CX
--     project is nubank-only) and every legacy consumer applies it at the source.
--   * Expose the raw `local_timestamp_dime_slot_starts_at` so timezone handling is
--     metric-layer's concern (see "Timezone notes" below).
--   * Expose `activity_type_required`, `dimensioned_activity`, `agent_dime_squad`,
--     and `shuffle_status_required` raw, unfiltered.
--
-- Out of scope (handled by the metrics layer)
--   * Activity-type filtering (`lunch_break`, `time_off`, `shrinkage`).
--   * `dimensioned_activity` exclusion list (`Mouring`, `Weekly`, `Permiso Medico`,
--     `Permiso medico`, `Huddle`, `Licencia`, `Vacacion`).
--   * Squad exclusions (`wfm`, `credit_evolution`, `dote`, `social`, `content`, etc.).
--   * `shuffle_status_required IN ('available', 'oos')` (used by NTPJ).
--   * Any `+6h` timezone offset for joining against UTC-stored data.
--   * Dropping rows with `dime_date > current_period_end_for_published_data` (legacy
--     uses `DATE_SUB(DATE_TRUNC('WEEK', CURRENT_DATE()), 1)` to exclude the in-flight
--     week; the orchestration layer decides what period to publish).
--
-- Out of scope (handled by the Adjustments layer)
--   * The legacy hardcoded `time_off` backfills (specific agents/dates re-classified
--     as `time_off`).
--   * The `Control MC` / `xMC Debit Fraud` → `oos` remapping.
--   * Other per-agent activity-type rewrites.
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `dime_date`
--   :period_end   DATE  inclusive upper bound on `dime_date`
--
-- Output schema (one row per DIME slot)
--   agent                                STRING     email prefix, lowercased
--   date                                 DATE       `dime_date`
--   squad                                STRING     raw `agent_dime_squad`
--   affiliation                          STRING     raw `affiliation`
--   activity_type_required               STRING     raw, no filter applied
--   shuffle_status_required              STRING     raw, no filter applied (NTPJ uses
--                                                   `IN ('available', 'oos')`)
--   dimensioned_activity                 STRING     raw, no filter applied
--   local_timestamp_dime_slot_starts_at  TIMESTAMP  raw DIME slot start (local time)
--   slot_start_local_unix                BIGINT     `UNIX_TIMESTAMP(local_timestamp)`
--   slot_end_local_unix                  BIGINT     `slot_start_local_unix + 1800`
--
-- Timezone notes
--   * `local_timestamp_dime_slot_starts_at` stores the slot start in **local time**
--     (Mexico City). `UNIX_TIMESTAMP(local_timestamp)` reads it as if it were UTC,
--     which is what legacy does. The resulting `slot_start_local_unix` is therefore
--     a "local-time unix" — useful for joining against other local-time sources
--     (e.g. `ops_canonical_time_spent_activities.local_start_time`,
--     `taskmaster_consolidated_registry.local_start_date`).
--   * To compare against UTC-stored data (e.g. `agent_productivity.timestamp`), the
--     metric layer must add `+6 * 60 * 60` seconds to `slot_start_local_unix`. The
--     legacy adherence pipeline does exactly this.
-- =====================================================================================

SELECT
  LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0))             AS agent,
  dime_date                                                              AS date,
  agent_dime_squad                                                       AS squad,
  affiliation,
  activity_type_required,
  shuffle_status_required,
  dimensioned_activity,
  local_timestamp_dime_slot_starts_at,
  UNIX_TIMESTAMP(local_timestamp_dime_slot_starts_at)                    AS slot_start_local_unix,
  UNIX_TIMESTAMP(local_timestamp_dime_slot_starts_at) + (30 * 60)        AS slot_end_local_unix
FROM etl.mx__series_contract.agent_dimensioned_activities
WHERE affiliation = 'nubank'
  AND dime_date >= :period_start
  AND dime_date <= :period_end
