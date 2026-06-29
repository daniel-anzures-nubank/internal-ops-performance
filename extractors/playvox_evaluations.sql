-- =====================================================================================
-- Playvox QA evaluations — raw extractor (qmo_playvox_consolidated)
-- =====================================================================================
--
-- Purpose
--   Returns one row per Playvox QA evaluation from
--   `etl.mx__dataset.qmo_playvox_consolidated`. Used by the Quality metric (Core/Fraud
--   and Content).
--
-- Scope of this query (what IS done here)
--   * Pull every evaluation in the period.
--   * Normalize agent to email prefix (lowercased).
--   * Cast `evaluation__id` and `scorecard__id` to STRING for consistent typing.
--   * Expose `team_name`, `agent_email`, `qa_score`, and timestamps raw.
--   * Intrinsic email filters (these aren't real Nubank agents, analogous to
--     `affiliation = 'nubank'` on other sources):
--       NOT IN (CONCAT('testuser', '@', 'nu.com.mx'))
--       NOT LIKE '%consorcio%'  (outsourced — not in Nubank's roster)
--       NOT LIKE '%conjur%'     (outsourced — not in Nubank's roster)
--   * Dedup by `evaluation_id` keeping the latest version. `qmo_playvox_consolidated`
--     carries every revision of an evaluation as a separate row. Legacy `qa_deduped`
--     picks the winner with `ROW_NUMBER() OVER (PARTITION BY evaluation__id ORDER BY
--     local_mx_evaluation__created_at DESC)`, so we order by `created_at DESC` FIRST
--     (legacy primary key) with `updated_at DESC` only as a deterministic tiebreaker.
--     `created_at` is carried through end-to-end so the metric layer's re-dedup uses
--     the same `created_at DESC` order (a no-op here, but it keeps the two layers'
--     winner selection identical).
--
-- Out of scope (handled by the metrics layer)
--   * Team-name exclusions (legacy: `NOT IN ("REGULATORY SOLUTIONS", "AML")`).
--   * Scorecard-id exclusions (the hardcoded blacklist — metric-specific).
--   * Specific `evaluation__id` exclusions (also hardcoded blacklist).
--   * Affiliation classification (`RLIKE` regex on email -> 'nubank').
--   * Roster join, `status = 'active'` filter, GROUP BY (agent, date) aggregation.
--
-- Out of scope (handled by the Adjustments layer)
--   * The hardcoded date exclusion list `NOT IN ('2026-03-27', '2026-04-09')`.
--   * The hardcoded `evaluation__id` / `scorecard__id` blacklists (these should
--     migrate to the manual-exclusions sheet).
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `local_mx_evaluation__created_at`
--   :period_end   DATE  inclusive upper bound on `local_mx_evaluation__created_at`
--
-- Output schema (one row per evaluation)
--   evaluation_id  STRING     `evaluation__id` cast to STRING
--   agent          STRING     email prefix, lowercased
--   agent_email    STRING     raw `evaluation__agent_email`
--   team_name      STRING     raw `evaluation__team_name`
--   scorecard_id   STRING     `scorecard__id` cast to STRING
--   qa_score       DOUBLE     `evaluation__score_avg`
--   created_at     TIMESTAMP  `local_mx_evaluation__created_at`
--   updated_at     TIMESTAMP  `local_mx_evaluation__updated_at`
-- =====================================================================================

WITH playvox_filtered AS (
  SELECT
    evaluation__id,
    evaluation__agent_email,
    evaluation__team_name,
    scorecard__id,
    evaluation__score_avg,
    local_mx_evaluation__created_at,
    local_mx_evaluation__updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY evaluation__id
      ORDER BY local_mx_evaluation__created_at DESC NULLS LAST,
               local_mx_evaluation__updated_at DESC NULLS LAST
    ) AS rn
  FROM etl.mx__dataset.qmo_playvox_consolidated
  WHERE local_mx_evaluation__created_at >= :period_start
    AND local_mx_evaluation__created_at <  DATE_ADD(:period_end, 1)
    AND evaluation__agent_email NOT IN (CONCAT('testuser', '@', 'nu.com.mx'))
    AND evaluation__agent_email NOT LIKE '%consorcio%'
    AND evaluation__agent_email NOT LIKE '%conjur%'
)
SELECT
  TRY_CAST(evaluation__id AS STRING)                                          AS evaluation_id,
  LOWER(REGEXP_EXTRACT(evaluation__agent_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
  evaluation__agent_email                                                     AS agent_email,
  evaluation__team_name                                                       AS team_name,
  TRY_CAST(scorecard__id AS STRING)                                           AS scorecard_id,
  CAST(evaluation__score_avg AS DOUBLE)                                       AS qa_score,
  local_mx_evaluation__created_at                                             AS created_at,
  local_mx_evaluation__updated_at                                             AS updated_at
FROM playvox_filtered
WHERE rn = 1
