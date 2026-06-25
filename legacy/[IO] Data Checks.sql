-- Databricks notebook source
-- DBTITLE 1,Check in the final dataset
SELECT
  *
FROM usr.mx__cx.internal_ops_performance_2026
WHERE 1=1
  -- AND metric = 'adherence_agent'
  AND date_granularity = 'month'
  AND date_reference = '2026-03-01'
  AND xforce = 'luz.castillo'
  -- AND agent = 'tania.llamas'

-- COMMAND ----------

-- DBTITLE 1,Check on the BDX
SELECT 
  squad,
  xplead_name,
  xforce_name,
  actor_email,
  DATE_TRUNC('month', snapshot_date) AS snapshot_month,
  ROW_NUMBER() OVER (PARTITION BY actor_name, DATE_TRUNC('month', snapshot_date) 
    ORDER BY snapshot_date DESC) AS rn
FROM etl.mx__series_contract.cx_mx_bdx_snapshots
WHERE 1=1
  AND actor_email LIKE '%janet.castro%'
  -- AND xforce_email LIKE '%luz.castillo%'
  -- AND snapshot_date = '2026-02-18'
  AND DATE_TRUNC('month', snapshot_date) = '2026-05-01'

-- COMMAND ----------

SELECT
  *
FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE affiliation = 'nubank'
    AND dime_date >= '2026-04-01' -- change here the date
    AND REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) = 'janet.castro' -- change here the name of the agent
    AND activity_type_required IS NOT NULL
    AND activity_type_required NOT IN ('time_off', 'lunch_break')
ORDER BY dime_date DESC

-- COMMAND ----------

-- DBTITLE 1,Check on DIME
SELECT
*
FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE
      affiliation = 'nubank'
      AND dime_date IN ('2026-02-26')
      -- AND DATE_TRUNC('MONTH', dime_date) = '2026-02-01'
      -- AND REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) IN ('aurora.cruz', 'daniela.cornejo', 'erik.calleja', 'evelyn.caraoia', 'fernanda.huerta', 'gabriel.molina', 'giovanni.romero', 'gustavo.castillo', 'jair.esquivel', 'jonathan.cervantes', 'jose.vaca')
      AND REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) IN ('jonathan.pineda')
      -- AND agent_dime_squad = 'social'
      AND activity_type_required IS NOT NULL
      -- AND activity_type_required NOT IN ('time_off', 'shrinkage', 'lunch_break')

-- COMMAND ----------

-- DBTITLE 1,Check agent productivity
CREATE OR REPLACE TEMPORARY VIEW agent_id AS(
  SELECT DISTINCT
    REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
    , actor__id
  FROM etl.mx__dataset.ops_actors
);

CREATE OR REPLACE TEMPORARY VIEW agent_productivity AS(
  SELECT
    b.agent
    , unix_timestamp(a.timestamp) AS activity_start
    , unix_timestamp(a.next_event_time) AS activity_end
    , a.channel_active AS channel
    , a.status
    , a.timestamp AS date
    , a.level_3
    , a.active_jobs
  FROM etl.mx__dataset.agent_productivity AS a
  INNER JOIN agent_id AS b
    ON a.actor_id = b.actor__id
  WHERE a.status IN ('available', 'oos', 'training')
    OR (a.status = 'pause' AND a.level_3 = 'paused_with_jobs')
    OR a.active_jobs > 0
  --   AND a.timestamp >= '2025-01-01'
);

SELECT *
FROM agent_productivity
WHERE agent LIKE '%adriana.ruvalcaba%'
  AND DATE_TRUNC('DAY', date) = '2026-03-10'

-- COMMAND ----------

-- DBTITLE 1,Check OCTSA
SELECT
*
FROM etl.mx__dataset.ops_canonical_time_spent_activities
  WHERE DATE_TRUNC('DAY', local_start_time) = '2026-01-18'
    AND agent LIKE '%ignacio.herbert%'

-- COMMAND ----------

-- DBTITLE 1,Check Taskmaster
SELECT
*
FROM etl.mx__dataset.taskmaster_consolidated_registry
WHERE 1=1
  AND DATE_TRUNC('DAY', local_start_date) = '2026-01-18'
  AND agent LIKE '%ignacio.herbert%'

-- COMMAND ----------

-- DBTITLE 1,Check all activities
CREATE OR REPLACE TEMPORARY VIEW jobs_shuffle AS (
  SELECT
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , DATE(local_start_time) AS date
    , activity_type
    , UNIX_TIMESTAMP(local_start_time) AS activity_start
    , UNIX_TIMESTAMP(local_stop_time) AS activity_end
    , net_time_spent
  FROM etl.mx__dataset.ops_canonical_time_spent_activities
  WHERE status = 'finished'
    AND actor_affiliation = 'nubank'
    AND local_start_time >= '2025-01-01'
);

CREATE OR REPLACE TEMPORARY VIEW jobs_oos AS(
  SELECT
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , DATE(local_start_date) AS date
    , 'oos' AS activity_type
    , UNIX_TIMESTAMP(local_start_date) AS activity_start
    , UNIX_TIMESTAMP(local_stop_date) AS activity_end
    , net_time_spent_seconds AS net_time_spent
  FROM etl.mx__dataset.taskmaster_consolidated_registry
  WHERE local_start_date >= '2025-01-01'
);

CREATE OR REPLACE TEMPORARY VIEW jobs_join AS(
  SELECT * FROM jobs_shuffle
  UNION ALL
  SELECT * FROM jobs_oos
);

SELECT
  *
FROM jobs_join
WHERE agent = 'ignacio.herbert'
  AND date = '2026-02-01'

-- COMMAND ----------

-- DBTITLE 1,Check Adherence
CREATE OR REPLACE TEMPORARY VIEW adherence AS(
  SELECT
    *
  FROM usr.mx__cx.adherence_io
);

SELECT *
FROM adherence
WHERE agent = 'janet.castro'
  AND date >= '2026-03-01'

-- COMMAND ----------

SELECT
  *
FROM
  -- adherence_final
  adherence_agents_daily
  -- adherence_by_slot
  -- data_calculations
  -- dime_table
WHERE 1=1
  -- AND date = '2026-03-10'
  AND date_reference = '2026-03-10'
  -- AND agent IN ('aurora.cruz', 'daniela.cornejo', 'erik.calleja', 'evelyn.caraoia', 'fernanda.huerta', 'gabriel.molina', 'giovanni.romero', 'gustavo.castillo', 'jair.esquivel', 'jonathan.cervantes', 'jose.vaca')
  AND agent = 'adriana.ruvalcaba'

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW agent_id AS(
  SELECT DISTINCT
    REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
    , actor__id
  FROM etl.mx__dataset.ops_actors
);

CREATE OR REPLACE TEMPORARY VIEW agent_productivity AS(
  SELECT
    b.agent
    , unix_timestamp(a.timestamp) AS activity_start
    , unix_timestamp(a.next_event_time) AS activity_end
    , a.channel_active AS channel
    , a.status
    , a.timestamp AS date
    , a.level_3
    , a.active_jobs
    , -(unix_timestamp(a.timestamp) - unix_timestamp(a.next_event_time)) AS duration
  FROM etl.mx__dataset.agent_productivity AS a
  INNER JOIN agent_id AS b
    ON a.actor_id = b.actor__id
  WHERE a.status IN ('available', 'oos', 'training')
    OR (a.status = 'pause' AND a.level_3 = 'paused_with_jobs')
    OR a.active_jobs > 0
  --   AND a.timestamp >= '2025-01-01'
);

SELECT 
  agent
  , SUM(duration)
FROM agent_productivity
WHERE agent LIKE '%adriana.ruvalcaba%'
  AND DATE_TRUNC('DAY', date) = '2026-03-10'
GROUP BY ALL

-- COMMAND ----------

-- DBTITLE 1,Licencia slots leaking through current filters
-- Impact analysis: Adding 'Licencia' to dimensioned_activity NOT IN filter
-- Shows agents/days currently leaking through with Licencia slots
-- These slots have activity_type_required = 'dime_invalid_notation' and dimensioned_activity = 'Licencia'

SELECT
  LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
  , dime_date
  , activity_type_required
  , dimensioned_activity
  , agent_dime_squad
  , COUNT(*) AS slots_affected
  , COUNT(*) / 2.0 AS hours_affected
FROM etl.mx__series_contract.agent_dimensioned_activities
WHERE affiliation = 'nubank'
  AND dime_date >= '2026-01-01'
  AND activity_type_required IS NOT NULL
  AND activity_type_required NOT IN ('lunch_break', 'time_off', 'shrinkage')
  AND agent_dime_squad IS NOT NULL
  AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote')
  AND dimensioned_activity = 'Licencia'
GROUP BY ALL
ORDER BY dime_date DESC, agent

-- COMMAND ----------

-- DBTITLE 1,Adherence impact: current vs fixed (Licencia excluded)
-- Adherence impact: compare current vs fixed for affected agents
-- Current: these 'Licencia' slots generate required_hours with 0 delivered (adherence = 0%)
-- Fixed: these slots would be excluded entirely

WITH licencia_agents AS (
  SELECT DISTINCT
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , dime_date AS date
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE affiliation = 'nubank'
    AND dime_date >= '2026-01-01'
    AND dimensioned_activity = 'Licencia'
    AND activity_type_required = 'dime_invalid_notation'
    AND agent_dime_squad IS NOT NULL
    AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote')
),
current_adherence AS (
  SELECT
    agent
    , DATE_TRUNC('MONTH', date) AS month
    , SUM(delivered_hours) AS delivered
    , SUM(required_hours) AS required
    , TRY_DIVIDE(SUM(delivered_hours), SUM(required_hours)) * 100 AS adherence_current
  FROM usr.mx__cx.adherence_io
  WHERE agent IN (SELECT agent FROM licencia_agents)
    AND date >= '2026-01-01'
  GROUP BY ALL
),
fixed_adherence AS (
  SELECT
    a.agent
    , DATE_TRUNC('MONTH', a.date) AS month
    , SUM(a.delivered_hours) AS delivered
    , SUM(a.required_hours) AS required
    , TRY_DIVIDE(SUM(a.delivered_hours), SUM(a.required_hours)) * 100 AS adherence_fixed
  FROM usr.mx__cx.adherence_io AS a
  LEFT JOIN licencia_agents AS b
    ON a.agent = b.agent AND a.date = b.date
  WHERE a.agent IN (SELECT agent FROM licencia_agents)
    AND a.date >= '2026-01-01'
    AND b.agent IS NULL  -- exclude Licencia days
  GROUP BY ALL
)
SELECT
  COALESCE(c.agent, f.agent) AS agent
  , COALESCE(c.month, f.month) AS month
  , c.adherence_current
  , f.adherence_fixed
  , (f.adherence_fixed - c.adherence_current) AS difference
  , c.required AS current_required_hours
  , f.required AS fixed_required_hours
FROM current_adherence AS c
FULL OUTER JOIN fixed_adherence AS f
  ON c.agent = f.agent AND c.month = f.month
ORDER BY agent, month

-- COMMAND ----------

-- DBTITLE 1,NOCC impact: current vs fixed (Licencia excluded)
-- Normalized Occupancy impact: check affected agents
-- In the occupancy notebook, dime_invalid_notation gets remapped to 'oos'
-- So Licencia slots are treated as OOS slots, generating false occupancy metrics

WITH licencia_agents AS (
  SELECT DISTINCT
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , dime_date AS date
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE affiliation = 'nubank'
    AND dime_date >= '2026-03-01'
    AND dimensioned_activity = 'Licencia'
    AND activity_type_required = 'dime_invalid_notation'
    AND agent_dime_squad IS NOT NULL
    AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote', 'social')
),
current_nocc AS (
  SELECT
    agent
    , DATE_TRUNC('MONTH', date) AS month
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS raw_occupancy_current
    , MAX(occupancy_exp) AS occupancy_exp
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)), MAX(occupancy_exp)) * 100 AS nocc_current
  FROM usr.mx__cx.normalized_occupancy
  WHERE agent IN (SELECT agent FROM licencia_agents)
    AND date >= '2026-03-01'
  GROUP BY ALL
),
fixed_nocc AS (
  SELECT
    a.agent
    , DATE_TRUNC('MONTH', a.date) AS month
    , TRY_DIVIDE(SUM(a.occupancy_time), SUM(a.job_time)) AS raw_occupancy_fixed
    , MAX(a.occupancy_exp) AS occupancy_exp
    , TRY_DIVIDE(TRY_DIVIDE(SUM(a.occupancy_time), SUM(a.job_time)), MAX(a.occupancy_exp)) * 100 AS nocc_fixed
  FROM usr.mx__cx.normalized_occupancy AS a
  LEFT JOIN licencia_agents AS b
    ON a.agent = b.agent AND a.date = b.date
  WHERE a.agent IN (SELECT agent FROM licencia_agents)
    AND a.date >= '2026-03-01'
    AND b.agent IS NULL  -- exclude Licencia days
  GROUP BY ALL
)
SELECT
  COALESCE(c.agent, f.agent) AS agent
  , COALESCE(c.month, f.month) AS month
  , c.nocc_current
  , f.nocc_fixed
  , (f.nocc_fixed - c.nocc_current) AS difference
FROM current_nocc AS c
FULL OUTER JOIN fixed_nocc AS f
  ON c.agent = f.agent AND c.month = f.month
ORDER BY agent, month