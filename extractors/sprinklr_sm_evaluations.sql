-- =====================================================================================
-- Sprinklr Social-Media case QA evaluations — raw extractor
-- =====================================================================================
--
-- Purpose
--   Returns one row per Sprinklr social-media case-QA evaluation from
--   `etl.mx__series_contract.social_media_case_summary_information`. This is the
--   Social-Media-only complement to the Playvox feed: Social Media QA is logged
--   against Sprinklr cases, not Playvox. Used (UNION ALL with Playvox) by the
--   Quality metric for Social Media.
--
-- Scope of this query (what IS done here)
--   * Pull every SM case evaluation in the period that is on/after the SM
--     cutover (2026-05-01) — see "Cutover" below.
--   * Map `agent_name` -> Nubank email -> agent prefix via `sprinklr_sm_users`.
--     Rows whose agent can't be mapped (no `user_email`) yield an empty `agent`
--     and are dropped downstream in `build_evaluations`.
--   * Expose the same shape `build_evaluations` consumes for Playvox:
--     evaluation_id, agent, qa_score, team_name, created_at.
--   * `qa_score` (`score_avg`) is already on the same 0-100 scale as Playvox
--     (verified on live data), so no rescaling is needed before the UNION.
--   * Intrinsic monitor filter (mirrors legacy `qa_base` Sprinklr branch):
--       sm_monitor.user_email NOT IN (CONCAT('testuser', '@', 'nu.com.mx'))
--   * Dedup by `evaluation_id` (the Sprinklr `case_number`) keeping the latest
--     revision (latest `checklist_modified_date`), analogous to the Playvox
--     `ROW_NUMBER()` dedup.
--
-- Cutover (>= 2026-05-01)
--   Social Media only started being scored from Sprinklr in May 2026, so this
--   feed is hard-floored at 2026-05-01 regardless of `:period_start`. Earlier SM
--   quality stays Playvox-only (the floor is also enforced defensively in
--   `metrics_data/quality_evaluations.py` via `SPRINKLR_SM_CUTOVER`).
--
-- Out of scope (handled by the metrics_data / metrics layers)
--   * Roster join, `status = 'active'` filter, squad/team derivation.
--   * The `scorecard_id` / `evaluation_id` blacklists and outage-date exclusions.
--   * `evaluation_id` cross-source dedup (Sprinklr case numbers and Playvox ids
--     are disjoint id spaces, so no collision).
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `report_date`
--   :period_end   DATE  inclusive upper bound on `report_date`
--
-- Output schema (one row per evaluation)
--   evaluation_id  STRING     `case_number`
--   agent          STRING     email prefix, lowercased (mapped from agent_name)
--   qa_score       DOUBLE     `score_avg`
--   team_name      STRING     literal 'SM'
--   created_at     TIMESTAMP  `report_date` cast to TIMESTAMP (MX local day)
-- =====================================================================================

WITH sm_filtered AS (
  SELECT
    sm.case_number,
    sm.score_avg,
    sm.report_date,
    sm.checklist_modified_date,
    ag.user_email                                                       AS agent_email,
    ROW_NUMBER() OVER (
      PARTITION BY sm.case_number
      ORDER BY sm.checklist_modified_date DESC NULLS LAST,
               sm.report_date DESC NULLS LAST
    ) AS rn
  FROM etl.mx__series_contract.social_media_case_summary_information sm
  LEFT JOIN usr.mx__enablement.sprinklr_sm_users ag  ON sm.agent_name = ag.user_name
  LEFT JOIN usr.mx__enablement.sprinklr_sm_users mon ON sm.auditor    = mon.user_name
  WHERE sm.report_date >= :period_start
    AND sm.report_date <  DATE_ADD(:period_end, 1)
    AND sm.report_date >= DATE '2026-05-01'
    AND COALESCE(mon.user_email, '') NOT IN (CONCAT('testuser', '@', 'nu.com.mx'))
)
SELECT
  TRY_CAST(case_number AS STRING)                                        AS evaluation_id,
  LOWER(REGEXP_EXTRACT(agent_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0))        AS agent,
  CAST(score_avg AS DOUBLE)                                              AS qa_score,
  'SM'                                                                   AS team_name,
  CAST(report_date AS TIMESTAMP)                                         AS created_at
FROM sm_filtered
WHERE rn = 1
