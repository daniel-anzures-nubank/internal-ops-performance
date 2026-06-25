-- Databricks notebook source
-- DBTITLE 1,Agent information
CREATE OR REPLACE TEMPORARY VIEW monthly_snapshots AS (
  SELECT 
    REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS actor_name,
    squad,
    snapshot_date,
    DATE_TRUNC('month', snapshot_date) AS snapshot_month,
    ROW_NUMBER() OVER (PARTITION BY REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0), DATE_TRUNC('month', snapshot_date) ORDER BY snapshot_date DESC) AS rn
  FROM etl.mx__series_contract.cx_mx_bdx_snapshots
  WHERE actor_name IS NOT NULL
);

CREATE OR REPLACE TEMPORARY VIEW latest_per_month AS (
  SELECT 
    REGEXP_EXTRACT(a.actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS actor_name,
    a.xforce_email,
    a.xplead_email,
    a.squad,
    a.district,
    a.status,
    a.shift_name,
    a.snapshot_date,
    a.hire_start_date,
    DATE_TRUNC('month', a.snapshot_date) AS snapshot_month
  FROM etl.mx__series_contract.cx_mx_bdx_snapshots AS a
  INNER JOIN monthly_snapshots AS b
    ON REGEXP_EXTRACT(a.actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) = b.actor_name 
    AND a.snapshot_date = b.snapshot_date
  WHERE b.rn = 1
);

CREATE OR REPLACE TEMPORARY VIEW squad_changes AS (
  SELECT 
    actor_name,
    squad,
    snapshot_date,
    snapshot_month,
    LAG(squad) OVER (PARTITION BY actor_name ORDER BY snapshot_month) AS previous_squad,
    LAG(snapshot_month) OVER (PARTITION BY actor_name ORDER BY snapshot_month) AS previous_month
  FROM monthly_snapshots
  WHERE rn = 1
);

CREATE OR REPLACE TEMPORARY VIEW agent_information AS(
  SELECT 
  REGEXP_EXTRACT(a.actor_name, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent,
  REGEXP_EXTRACT(a.xplead_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS xplead,
  REGEXP_EXTRACT(a.xforce_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS xforce,
  a.squad,
  a.district AS squad_district,
  a.status,
  a.shift_name AS shift,
  a.snapshot_date,
  a.snapshot_month,
  CASE 
    WHEN b.previous_squad IS NULL THEN a.hire_start_date
    WHEN b.previous_squad != a.squad THEN b.snapshot_date
    ELSE a.hire_start_date
  END AS last_change_date
FROM latest_per_month AS a
LEFT JOIN squad_changes AS b 
  ON a.actor_name = b.actor_name 
  AND a.snapshot_month = b.snapshot_month
WHERE a.squad NOT IN ('social', 'content')
);

-- SELECT * FROM agent_information


-- COMMAND ----------

-- DBTITLE 1,Quality code
CREATE OR REPLACE TEMPORARY VIEW qa_base AS(
  SELECT
    local_mx_evaluation__created_at
    , local_mx_evaluation__updated_at
    , TRY_CAST(scorecard__id AS STRING) AS scorecard__id
    , TRY_CAST(evaluation__id AS STRING) AS evaluation__id 
    , evaluation__team_name
    , REGEXP_EXTRACT(evaluation__agent_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
    , evaluation__score_avg AS qa_score
    , CASE
        WHEN evaluation__agent_email RLIKE "^[a-zA-Z]+[.][a-zA-Z]+[0-9]*@nu[.]com[.]mx$" THEN 'nubank'
      ELSE NULL
      END AS affiliation
  FROM etl.mx__dataset.qmo_playvox_consolidated
  WHERE local_mx_evaluation__created_at >= "2025-07-01"
    AND evaluation__team_name NOT IN ("REGULATORY SOLUTIONS", "AML")
    AND evaluation__agent_email NOT IN (CONCAT('testuser', '@', 'nu.com.mx')) 
    AND scorecard__id NOT IN ("68def79b3f83da8cc9cb5299","6812b3e46abeabb0653d197e", '688017f4bb266bb43b6c9565', '68680819336107d9f140d1ce')
    AND evaluation__agent_email NOT LIKE '%consorcio%'
    AND evaluation__agent_email NOT LIKE '%conjur%'
    AND evaluation__id NOT IN ('68646ed2f093c149757ba038', '687704e7a077fb121012dd5d', '688017f4bb266bb43b6c9565', '68680819336107d9f140d1ce')

    UNION ALL

  SELECT
    sm.report_date AS local_mx_evaluation__created_at
    , sm.checklist_modified_date AS local_mx_evaluation__updated_at
    , 'SprinklrScorecardV1' AS scorecard__id
    , TRY_CAST(sm.case_number AS STRING) AS evaluation__id
    , 'SM' AS evaluation__team_name
    , REGEXP_EXTRACT(sm_agent.user_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)  AS agent
    , sm.score_avg AS qa_score
    , 'nubank' AS affiliation
  FROM mx__series_contract.social_media_case_summary_information sm
  LEFT JOIN usr.mx__enablement.sprinklr_sm_users sm_agent ON sm.agent_name = sm_agent.user_name
  LEFT JOIN usr.mx__enablement.sprinklr_sm_users sm_monitor ON sm.auditor = sm_monitor.user_name
  WHERE sm.report_date >= "2026-01-01"
  AND sm_monitor.user_email NOT IN (CONCAT('testuser', '@', 'nu.com.mx'))
);

CREATE OR REPLACE TEMPORARY VIEW qa_deduped AS (
  SELECT 
    *
    , ROW_NUMBER() OVER (PARTITION BY evaluation__id ORDER BY local_mx_evaluation__created_at DESC) AS rn
  FROM qa_base
  WHERE affiliation IS NOT NULL
);

CREATE OR REPLACE TEMPORARY VIEW qa_score_2026 AS (
SELECT 
  DATE_TRUNC('DAY', a.local_mx_evaluation__created_at) AS date
  , a.agent
  , AVG(a.qa_score) AS qa_score
  , COUNT(DISTINCT a.evaluation__id) AS evaluations
  , b.xplead
  , b.xforce
  , b.squad
  , b.squad_district
FROM qa_deduped a
LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.local_mx_evaluation__created_at) = b.snapshot_month
WHERE a.local_mx_evaluation__created_at >= '2025-12-01'
    AND b.status = 'active'
    AND a.rn = 1
    AND a.local_mx_evaluation__created_at NOT IN ('2026-03-27', '2026-04-09')
GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_score_2025 AS (
SELECT 
  DATE_TRUNC('DAY', a.local_mx_evaluation__created_at) AS date
  , a.agent
  , AVG(a.qa_score) AS qa_score
  , COUNT(DISTINCT a.evaluation__id) AS evaluations
  , b.xplead
  , b.xforce
  , b.squad
  , b.squad_district
FROM qa_deduped a
LEFT JOIN agent_information AS b
    ON a.agent = b.agent
  WHERE a.local_mx_evaluation__created_at < '2025-12-01'
    AND a.local_mx_evaluation__created_at >= '2025-01-01'
    AND b.snapshot_month = '2025-12-01'
    AND b.status = 'active'
    AND a.rn = 1
    AND DATE_TRUNC('DAY', a.local_mx_evaluation__created_at) NOT IN ('2026-03-27', '2026-04-09') -- deleting data with general access problems
GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_score_base AS(
  SELECT * FROM qa_score_2025
  UNION ALL
  SELECT * FROM qa_score_2026
);

--SELECT * FROM qa_score_base

-- COMMAND ----------

CREATE OR REPLACE TABLE usr.mx__cx.quality_io AS
SELECT * FROM qa_score_base