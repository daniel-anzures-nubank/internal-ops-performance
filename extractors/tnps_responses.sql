-- =====================================================================================
-- tNPS responses — Social-Media transactional-NPS survey extractor (Sprinklr)
-- =====================================================================================
--
-- Purpose
--   Returns one row per Social-Media tNPS survey response from
--   `usr.sprinklr_api_data_integration.sprinklr_tnps_data`. This is the raw feed for
--   the **Human tNPS** metric (NPS of survey responses attributable to a human
--   social agent). tNPS only applies to Social Media — this source only contains
--   surveys for cases handled by a human social agent (no bot/unattributed rows).
--
-- Scope of this query (what IS done here)
--   * Pull every tNPS survey response in the period.
--   * Normalize agent to email prefix (lowercased) from `agent_email_id`.
--   * Attribute the response to the case's closure day (`date`) — the legacy SM
--     notebook groups tNPS by `DATE_TRUNC('DAY', case_closure_time)`.
--   * Expose `survey_answer_score` raw (the 0-10 NPS score) plus `case_number`,
--     `survey_response_date`, and `case_closure_time`.
--
-- Out of scope (handled by the metrics layer)
--   * Promoter / detractor / neutral classification (`>= 9` promoter, `<= 6`
--     detractor, 7-8 neutral) and the `(promoters - detractors) / valid_responses`
--     NPS formula.
--   * The validity window `survey_response_date <= case_closure_time + INTERVAL 1 DAY`.
--   * The outage-date exclusion (`2026-03-27`).
--   * Dedup to one response per `case_number` (legacy counts DISTINCT case_number).
--   * Roster join / active filter / squad scoping.
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `case_closure_time`
--   :period_end   DATE  inclusive upper bound on `case_closure_time`
--
-- Output schema (one row per tNPS survey response)
--   agent                 STRING   email prefix, lowercased (empty if unattributed)
--   agent_email_id        STRING   raw agent email from the survey
--   case_number           STRING   the case / survey identifier
--   date                  DATE     `DATE(case_closure_time)` — closure day
--   survey_response_date  DATE     when the customer answered the survey
--   case_closure_time     DATE     when the case was closed
--   survey_score          INT      raw `survey_answer_score` (0-10; nullable)
-- =====================================================================================

SELECT
  LOWER(REGEXP_EXTRACT(agent_email_id, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
  agent_email_id,
  CAST(case_number AS STRING)            AS case_number,
  DATE(case_closure_time)                AS date,
  survey_response_date,
  case_closure_time,
  CAST(survey_answer_score AS INT)       AS survey_score
FROM usr.sprinklr_api_data_integration.sprinklr_tnps_data
WHERE case_closure_time >= :period_start
  AND case_closure_time <= :period_end
