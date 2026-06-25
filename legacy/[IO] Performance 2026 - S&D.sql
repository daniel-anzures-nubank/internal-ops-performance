-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Agents Informations

-- COMMAND ----------

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

-- MAGIC %md
-- MAGIC ## Manual Adjustments

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW manual_adjustments_shrinkage AS (
  SELECT
    *,
    CASE
      /* ========= Training ========= */

      /* CREDIT → LIFECYCLE — training 10/03 11:00–13:00 */
      WHEN agent IN ('elizabeth.martinez', 'daniel.cano', 'jonathan.pineda', 'jessica.gonzalez', 'nitza.zarza')
        AND dime_date = DATE '2026-03-10'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 11:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 13:00:00')) THEN TRUE

      /* CREDIT → LIFECYCLE — training 10/03 18:00–19:00 */
      WHEN agent IN ('bertha.sanchez', 'sofia.orozco', 'jorge.ortega')
        AND dime_date = DATE '2026-03-10'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 18:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 19:00:00')) THEN TRUE

      /* CREDIT → CUENTA — training */
      WHEN agent = 'elizabeth.martinez'
        AND dime_date IN (DATE '2026-04-09', DATE '2026-04-10')
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 14:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE
      WHEN agent IN ('daniel.cano', 'jonathan.pineda')
        AND dime_date = DATE '2026-04-09'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 14:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE

      /* COLLECTIONS → CUENTA — training */
      WHEN agent IN ('adriana.marquez', 'eden.martinez', 'mariana.infante')
        AND dime_date IN (DATE '2026-04-09', DATE '2026-04-10')
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 14:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE
      WHEN agent IN ('javier.balanzar', 'carlos.gonzalez')
        AND dime_date = DATE '2026-04-09'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 14:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE

      /* EMI → CUENTA — training */
      WHEN agent = 'fernanda.ibanez'
        AND dime_date = DATE '2026-04-09'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 14:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE
      WHEN agent IN ('jose.velez', 'ivette.melendez', 'rocio.rodriguez')
        AND dime_date IN (DATE '2026-04-09', DATE '2026-04-10')
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 14:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE

      /* EMI → LIFECYCLE — training */
      WHEN agent IN ('fernanda.ibanez', 'jose.velez', 'ivette.melendez')
        AND dime_date = DATE '2026-03-10'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 11:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE
      WHEN agent = 'erik.licona'
        AND dime_date = DATE '2026-03-10'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 18:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 19:30:00')) THEN TRUE

      /* ========= Shadowing ========= */

      /* CREDIT → LIFECYCLE — shadowing 10/03 13:00–15:00 */
      WHEN agent IN ('elizabeth.martinez', 'daniel.cano', 'jonathan.pineda', 'jessica.gonzalez', 'nitza.zarza')
        AND dime_date = DATE '2026-03-10'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 13:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE

      /* CREDIT → LIFECYCLE — shadowing 10/03 19:00–20:00 */
      WHEN agent IN ('bertha.sanchez', 'sofia.orozco', 'jorge.ortega')
        AND dime_date = DATE '2026-03-10'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 19:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 20:00:00')) THEN TRUE

      /* CREDIT / COLLECTIONS / EMI → CUENTA — shadowing 13/04 14:00–15:00 */
      WHEN agent IN (
          'elizabeth.martinez', 'daniel.cano', 'jonathan.pineda', 'adriana.marquez',
          'javier.balanzar', 'carlos.gonzalez', 'mariana.infante',
          'fernanda.ibanez', 'ivette.melendez', 'rocio.rodriguez'
        )
        AND dime_date = DATE '2026-04-13'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 14:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE

      WHEN agent = 'jorge.severiano'
        AND dime_date = DATE '2026-04-13'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 14:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 15:00:00')) THEN TRUE

      ELSE FALSE
    END AS exclude
  FROM (
    SELECT
      LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
      CAST(dime_date AS DATE) AS dime_date,
      unix_timestamp(local_timestamp_dime_slot_starts_at) + (6 * 60 * 60) AS slot_start,
      local_timestamp_dime_slot_starts_at,
      dimensioned_activity
    FROM etl.mx__series_contract.agent_dimensioned_activities
    WHERE affiliation = 'nubank'
      AND dime_date >= DATE '2025-01-01'
  ) AS dime_slots
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Xpeers Metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adherence

-- COMMAND ----------

-- DBTITLE 1,Adherence Base
CREATE OR REPLACE TEMPORARY VIEW adherence AS(
  SELECT
    *
  FROM usr.mx__cx.internal_ops_performance_2026
  WHERE metric = 'adherence_agent'
  AND date_granularity IN ('month', 'week')
);

-- SELECT * FROM adherence

-- COMMAND ----------

-- DBTITLE 1,Adherence Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW adherence_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'adherence_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM adherence
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'adherence_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM adherence
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_squad AS (
  SELECT * FROM adherence_squad_monthly
  UNION ALL
  SELECT * FROM adherence_squad_weekly
);

-- SELECT * FROM adherence_squad

-- COMMAND ----------

-- DBTITLE 1,Adherence District Calculations
CREATE OR REPLACE TEMPORARY VIEW adherence_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'adherence_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM adherence
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'adherence_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM adherence
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_district AS (
  SELECT * FROM adherence_district_monthly
  UNION ALL
  SELECT * FROM adherence_district_weekly
);

-- SELECT * FROM adherence_district

-- COMMAND ----------

-- DBTITLE 1,Adherence S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW adherence_sd AS(
  -- SELECT * FROM adherence_agents
  -- UNION ALL
  -- SELECT * FROM adherence_agents_general_quartile
  -- UNION ALL
  -- SELECT * FROM adherence_agents_team_quartile
  -- UNION ALL
  -- SELECT * FROM adherence_xforces
  -- UNION ALL
  -- SELECT * FROM adherence_xpleads
  -- UNION ALL
  SELECT * FROM adherence_squad
  UNION ALL
  SELECT * FROM adherence_district
);

-- SELECT * FROM adherence

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Normalized Time per Job

-- COMMAND ----------

-- DBTITLE 1,NTPJ Base
CREATE OR REPLACE TEMPORARY VIEW ntpj AS(
  SELECT
    *
  FROM usr.mx__cx.internal_ops_performance_2026
  WHERE metric = 'ntpj_agent'
  AND date_granularity IN ('month', 'week')
);

-- SELECT * FROM ntpj

-- COMMAND ----------

-- DBTITLE 1,NTPJ Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM ntpj
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM ntpj
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_squad AS (
  SELECT * FROM ntpj_squad_monthly
  UNION ALL
  SELECT * FROM ntpj_squad_weekly
);

-- SELECT * FROM ntpj_squad

-- COMMAND ----------

-- DBTITLE 1,NTPJ District Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM ntpj
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM ntpj
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_district AS (
  SELECT * FROM ntpj_district_monthly
  UNION ALL
  SELECT * FROM ntpj_district_weekly
);

-- SELECT * FROM ntpj_district

-- COMMAND ----------

-- DBTITLE 1,NTPJ S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW ntpj_sd AS(
  -- SELECT * FROM ntpj_agents
  -- UNION ALL
  -- SELECT * FROM ntpj_agents_general_quartile
  -- UNION ALL
  -- SELECT * FROM ntpj_agents_team_quartile
  -- UNION ALL
  -- SELECT * FROM ntpj_xforces
  -- UNION ALL
  -- SELECT * FROM ntpj_xpleads
  -- UNION ALL
  SELECT * FROM ntpj_squad
  UNION ALL
  SELECT * FROM ntpj_district
);

-- SELECT * FROM ntpj

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Normalized Occupancy

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Base
CREATE OR REPLACE TEMPORARY VIEW nocc AS(
  SELECT
    *
  FROM usr.mx__cx.internal_ops_performance_2026
  WHERE metric = 'nocc_agent'
  AND date_granularity IN ('month', 'week')
);

-- SELECT * FROM nocc

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW nocc_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nocc_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM nocc
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nocc_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM nocc
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_squad AS (
  SELECT * FROM nocc_squad_monthly
  UNION ALL
  SELECT * FROM nocc_squad_weekly
);

-- SELECT * FROM nocc_squad

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy District Calculations
CREATE OR REPLACE TEMPORARY VIEW nocc_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'nocc_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM nocc
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'nocc_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM nocc
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_district AS (
  SELECT * FROM nocc_district_monthly
  UNION ALL
  SELECT * FROM nocc_district_weekly
);

SELECT * FROM nocc_district

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW nocc_sd AS(
  -- SELECT * FROM nocc_agents
  -- UNION ALL
  -- SELECT * FROM nocc_agents_general_quartile
  -- UNION ALL
  -- SELECT * FROM nocc_agents_team_quartile
  -- UNION ALL
  -- SELECT * FROM nocc_xforces
  -- UNION ALL
  -- SELECT * FROM nocc_xpleads
  -- UNION ALL
  SELECT * FROM nocc_squad
  UNION ALL
  SELECT * FROM nocc_district
);

-- SELECT * FROM nocc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quality Metric

-- COMMAND ----------

-- DBTITLE 1,Quality Base (Bruno's version)
CREATE OR REPLACE TEMPORARY VIEW qa_base AS(
  SELECT
    *
  FROM usr.mx__cx.internal_ops_performance_2026
  WHERE metric = 'qa_score_agent'
  AND date_granularity IN ('month', 'week')
);

-- SELECT * FROM qa_base

-- COMMAND ----------

-- DBTITLE 1,QA Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'qa_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) AS metric_value 
  FROM qa_base
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'qa_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) AS metric_value 
  FROM qa_base
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_squad AS (
  SELECT * FROM qa_squad_monthly
  UNION ALL
  SELECT * FROM qa_squad_weekly
);

-- SELECT * FROM qa_squad

-- COMMAND ----------

-- DBTITLE 1,QA District Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'qa_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) AS metric_value 
  FROM qa_base
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'qa_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) AS metric_value 
  FROM qa_base
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_district AS (
  SELECT * FROM qa_district_monthly
  UNION ALL
  SELECT * FROM qa_district_weekly
);

-- SELECT * FROM qa_district

-- COMMAND ----------

-- DBTITLE 1,QA S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW quality_sd AS(
--   SELECT * FROM qa_score_agents
-- UNION ALL
-- SELECT * FROM qa_agents_general_quartile
-- UNION ALL
-- SELECT * FROM qa_agents_team_quartile
-- UNION ALL
-- SELECT * FROM qa_xforces
-- UNION ALL
-- SELECT * FROM qa_xpleads
-- UNION ALL
SELECT * FROM qa_squad
UNION ALL
SELECT * FROM qa_district
);

-- SELECT * FROM quality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Index Agents

-- COMMAND ----------

-- DBTITLE 1,Index Agents Base
CREATE OR REPLACE TEMPORARY VIEW index_agent AS(
  SELECT
    *
  FROM usr.mx__cx.internal_ops_performance_2026
  WHERE metric = 'index_agent'
  AND date_granularity IN ('month', 'week')
);

-- SELECT * FROM index_agent

-- COMMAND ----------

-- DBTITLE 1,Index Agents Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW index_agent_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'index_agent_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value 
  FROM index_agent
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agent_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'index_agent_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value 
  FROM index_agent
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agent_squad AS (
  SELECT * FROM index_agent_squad_monthly
  UNION ALL
  SELECT * FROM index_agent_squad_weekly
);

-- SELECT * FROM index_agent_squad

-- COMMAND ----------

-- DBTITLE 1,Index Agents District Calculations
CREATE OR REPLACE TEMPORARY VIEW index_agent_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value 
  FROM index_agent
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agent_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value 
  FROM index_agent
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agent_district AS (
  SELECT * FROM index_agent_district_monthly
  UNION ALL
  SELECT * FROM index_agent_district_weekly
);

SELECT * FROM index_agent_district

-- COMMAND ----------

-- DBTITLE 1,Idex Agents S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW index_agents_join_sd AS(
  -- SELECT * FROM index_agents
  -- UNION ALL
  -- SELECT * FROM index_agents_general_quartile
  -- UNION ALL
  -- SELECT * FROM index_agents_team_quartile
  -- UNION ALL
  SELECT * FROM index_agent_squad
  UNION ALL
  SELECT * FROM index_agent_district
);

-- SELECT * FROM index_agents_join

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #XForces Metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Shrinkage

-- COMMAND ----------

-- DBTITLE 1,Base
CREATE OR REPLACE TEMPORARY VIEW shrinkage_base AS(
  SELECT
    LOWER(REGEXP_EXTRACT(a.agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , a.agent_dime_squad AS old_squad
    , a.dime_date AS date
    , a.activity_type_required
    , a.dimensioned_activity
    , b.exclude
  FROM etl.mx__series_contract.agent_dimensioned_activities AS a
  LEFT JOIN manual_adjustments_shrinkage AS b
    ON LOWER(REGEXP_EXTRACT(a.agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) = b.agent
    AND a.dime_date = b.dime_date
    AND unix_timestamp(a.local_timestamp_dime_slot_starts_at) + (6 * 60 * 60) = b.slot_start
  WHERE a.affiliation = 'nubank'
    AND a.dime_date >= '2025-01-01'
    AND a.activity_type_required IS NOT NULL
    AND a.activity_type_required != ('lunch_break')
    AND a.agent_dime_squad IS NOT NULL
    AND a.agent_dime_squad NOT IN ('content', 'planning', 'quality', 'social', 'wfm', 'enablement')
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_final_2026 AS(
  SELECT
    a.*
    , CASE 
        WHEN a.date < '2026-03-01' THEN COUNT(CASE WHEN a.activity_type_required = 'shrinkage' THEN 1 END)
        ELSE COUNT(CASE 
          WHEN a.activity_type_required = 'shrinkage' THEN 1 
          WHEN a.activity_type_required = 'dime_invalid_notation' AND a.dimensioned_activity IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle') THEN 1 END)
        END AS shrinkage_slot
    , CASE 
        WHEN a.date < '2026-03-01' THEN COUNT(CASE WHEN a.activity_type_required != 'dime_invalid_notation' THEN 1 END)
        ELSE COUNT(CASE WHEN a.activity_type_required != 'time_off' THEN 1 END)
      END AS required_slot
    , b.xplead
    , b.xforce
    , b.squad_district
    , b.shift
    , b.squad
  FROM shrinkage_base AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
  WHERE b.status = 'active'
    AND a.date >= '2025-12-01'
    AND (a.agent NOT IN ('jose.velez', 'carlos.gonzalez', 'jorge.ortega', 'luisa.castaneda', 'janet.castro', 'karen.ortega')
      AND a.date NOT IN ('2026-03-24', '2026-03-25', '2026-03-26', '2026-03-27', '2026-03-28'))
    AND a.exclude IS NOT TRUE
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_final_2025 AS(
  SELECT
    a.*
    , COUNT(CASE WHEN a.activity_type_required = 'shrinkage' THEN 1 END) AS shrinkage_slot
    , COUNT(CASE WHEN a.activity_type_required != 'dime_invalid_notation' THEN 1 END) AS required_slot
    , b.xplead
    , b.xforce
    , b.squad_district
    , b.shift
    , b.squad
  FROM shrinkage_base AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
  WHERE b.status = 'active'
    AND a.date < '2025-12-01'
    AND a.date >= '2025-01-01'
    AND b.snapshot_month = '2025-12-01'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_final AS(
  SELECT * FROM shrinkage_final_2025
  UNION ALL
  SELECT * FROM shrinkage_final_2026
);

-- SELECT * FROM shrinkage_final

-- COMMAND ----------

-- DBTITLE 1,Shirinkage Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW shrinkage_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'shrinkage_squad' AS metric
    , SUM(shrinkage_slot) AS numerator
    , SUM(required_slot) AS denominator
    , TRY_DIVIDE(SUM(shrinkage_slot) , SUM(required_slot)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM shrinkage_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'shrinkage_squad' AS metric
    , SUM(shrinkage_slot) AS numerator
    , SUM(required_slot) AS denominator
    , TRY_DIVIDE(SUM(shrinkage_slot) , SUM(required_slot)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM shrinkage_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_squad AS (
  SELECT * FROM shrinkage_squad_monthly
  UNION ALL
  SELECT * FROM shrinkage_squad_weekly
);

-- SELECT * FROM shrinkage_squad

-- COMMAND ----------

-- DBTITLE 1,Shirinkage Districts Calculations
CREATE OR REPLACE TEMPORARY VIEW shrinkage_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'shrinkage_district' AS metric
    , SUM(shrinkage_slot) AS numerator
    , SUM(required_slot) AS denominator
    , TRY_DIVIDE(SUM(shrinkage_slot) , SUM(required_slot)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM shrinkage_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'shrinkage_district' AS metric
    , SUM(shrinkage_slot) AS numerator
    , SUM(required_slot) AS denominator
    , TRY_DIVIDE(SUM(shrinkage_slot) , SUM(required_slot)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM shrinkage_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_district AS (
  SELECT * FROM shrinkage_district_monthly
  UNION ALL
  SELECT * FROM shrinkage_district_weekly
);

-- SELECT * FROM shrinkage_district

-- COMMAND ----------

-- DBTITLE 1,Shrinkage S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW shrinkage_sd AS(
  -- SELECT * FROM shrinkage_xforces
  -- UNION ALL
  -- SELECT * FROM shrinkage_xpleads
  -- UNION ALL
  SELECT * FROM shrinkage_squad
  UNION ALL
  SELECT * FROM shrinkage_district
);

-- SELECT * FROM nocc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Xpeers in Target for XForces

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XForces Base
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_base AS(
  SELECT 
    a.agent
    , a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.numerator AS adherence_in_target
    , b.numerator AS ntpj_in_target
    , c.numerator AS nocc_in_target
    , d.numerator AS qa_in_target
    , a.denominator AS adherence_xpeers
    , b.denominator AS ntpj_xpeers
    , c.denominator AS nocc_xpeers
    , d.denominator AS qa_xpeers
  FROM usr.mx__cx.internal_ops_performance_2026 AS a
  LEFT JOIN usr.mx__cx.internal_ops_performance_2026 AS b
    ON a.xforce = b.xforce
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'ntpj_xforce'
  LEFT JOIN usr.mx__cx.internal_ops_performance_2026 AS c
    ON a.xforce = c.xforce
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_xforce'
  LEFT JOIN usr.mx__cx.internal_ops_performance_2026 AS d
    ON a.xforce = d.xforce
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'qa_xforce'
  WHERE a.date_granularity IN ('week', 'month')
    AND a.metric = 'adherence_xforce'
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_final AS(
  SELECT
    *
    , CASE
        WHEN date_reference < '2026-02-01' THEN (adherence_in_target + ntpj_in_target)
        WHEN date_reference < '2026-03-01' THEN (adherence_in_target + ntpj_in_target + qa_in_target)
        ELSE (adherence_in_target + ntpj_in_target + nocc_in_target + qa_in_target)
      END AS xpeers_in_target
    , CASE 
        WHEN date_reference < '2026-02-01' THEN (adherence_xpeers + ntpj_xpeers)
        WHEN date_reference < '2026-03-01' THEN (adherence_xpeers + ntpj_xpeers + qa_xpeers)
        ELSE (adherence_xpeers + ntpj_xpeers + nocc_xpeers + qa_xpeers)
      END AS xpeers
  FROM xpeers_in_target_base
);

-- SELECT * FROM xpeers_in_target_final

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XForces Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xforce_squad' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xforce_squad' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_squad AS (
  SELECT * FROM xpeers_in_target_xforces_squad_monthly
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_squad_weekly
);

-- SELECT * FROM xpeers_in_target_xforces_squad

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XForces District Calculations
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xforce_district' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xforce_district' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_district AS (
  SELECT * FROM xpeers_in_target_xforces_district_monthly
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_district_weekly
);

-- SELECT * FROM xpeers_in_target_xforces_district

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XForces S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_join_sd AS (
  -- SELECT * FROM xpeers_in_target_xforces
  -- UNION ALL
  SELECT * FROM xpeers_in_target_xforces_squad
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Average Index Agent

-- COMMAND ----------

-- DBTITLE 1,Average Index Agent Base
CREATE OR REPLACE TEMPORARY VIEW average_index_agent AS(
  SELECT
    *
  FROM usr.mx__cx.internal_ops_performance_2026
  WHERE metric = 'index_agent'
  AND date_granularity IN ('month', 'week')
);

-- SELECT * FROM average_index_agent

-- COMMAND ----------

-- DBTITLE 1,Average Index Agents Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW average_index_agent_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'average_index_agent_squad' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(*) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_agent
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_agent_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'average_index_agent_squad' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(*) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_agent
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_agent_squad AS (
  SELECT * FROM average_index_agent_squad_monthly
  UNION ALL
  SELECT * FROM average_index_agent_squad_weekly
);

-- SELECT * FROM average_index_agent_squad

-- COMMAND ----------

-- DBTITLE 1,Average Index Agents District Calculations
CREATE OR REPLACE TEMPORARY VIEW average_index_agent_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'average_index_agent_district' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(*) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_agent
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_agent_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'average_index_agent_district' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(*) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_agent
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_agent_district AS (
  SELECT * FROM average_index_agent_district_monthly
  UNION ALL
  SELECT * FROM average_index_agent_district_weekly
);

-- SELECT * FROM average_index_agent_district

-- COMMAND ----------

-- DBTITLE 1,Average Index Agents S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW average_index_agent_join_sd AS (
  -- SELECT * FROM average_index_agent
  -- UNION ALL
  SELECT * FROM average_index_agent_squad
  UNION ALL
  SELECT * FROM average_index_agent_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Nuvinhos Performance

-- COMMAND ----------

-- DBTITLE 1,Nuvinhos Performance Base
CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_base AS(
  SELECT
    a.*
    , CASE
        WHEN DATE_TRUNC('MONTH', a.date_reference) >= DATE_TRUNC('MONTH', b.last_change_date) 
          AND DATE_TRUNC('MONTH', a.date_reference) <= (DATE_TRUNC('MONTH', b.last_change_date) + INTERVAL 2 MONTH)
          THEN 'nuvinho'
        ELSE 'old'
      END AS nuvinho
  FROM index_agent AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date_reference) = b.snapshot_month
  WHERE a.date_reference >= '2025-12-01'
    AND a.metric = 'index_agent'
);

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_final AS(
  SELECT
    xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , CASE WHEN nuvinho = 'nuvinho' THEN AVG(metric_value)
        ELSE 0
      END AS nuvinhos_average
    , CASE WHEN nuvinho = 'old' THEN AVG(metric_value)
        ELSE 0
      END AS old_average
    , nuvinho
  FROM nuvinhos_performance_base
  WHERE date_granularity IN ('week', 'month')
  GROUP BY ALL
);

-- SELECT * FROM nuvinhos_performance_final

-- COMMAND ----------

-- DBTITLE 1,Nuvinhos Performance Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nuvinhos_performance_squad' AS metric
    , AVG(nuvinhos_average) AS numerator
    , AVG(old_average) AS denominator
    , TRY_DIVIDE(AVG(nuvinhos_average), AVG(old_average)) * 100 AS metric_value
  FROM nuvinhos_performance_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nuvinhos_performance_squad' AS metric
    , AVG(nuvinhos_average) AS numerator
    , AVG(old_average) AS denominator
    , TRY_DIVIDE(AVG(nuvinhos_average), AVG(old_average)) * 100 AS metric_value
  FROM nuvinhos_performance_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_squad AS (
  SELECT * FROM nuvinhos_performance_squad_monthly
  UNION ALL
  SELECT * FROM nuvinhos_performance_squad_weekly
);

-- SELECT * FROM nuvinhos_performance_squad

-- COMMAND ----------

-- DBTITLE 1,Nuvinhos Performance District Calculations
CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'nuvinhos_performance_district' AS metric
    , AVG(nuvinhos_average) AS numerator
    , AVG(old_average) AS denominator
    , TRY_DIVIDE(AVG(nuvinhos_average), AVG(old_average)) * 100 AS metric_value
  FROM nuvinhos_performance_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'nuvinhos_performance_district' AS metric
    , AVG(nuvinhos_average) AS numerator
    , AVG(old_average) AS denominator
    , TRY_DIVIDE(AVG(nuvinhos_average), AVG(old_average)) * 100 AS metric_value
  FROM nuvinhos_performance_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_district AS (
  SELECT * FROM nuvinhos_performance_district_monthly
  UNION ALL
  SELECT * FROM nuvinhos_performance_district_weekly
);

-- SELECT * FROM nuvinhos_performance_district

-- COMMAND ----------

-- DBTITLE 1,Nuvinhos Performance S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_join_sd AS (
  -- SELECT * FROM nuvinhos_performance
  -- UNION ALL
  SELECT * FROM nuvinhos_performance_squad
  UNION ALL
  SELECT * FROM nuvinhos_performance_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Improved Benchmarks

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark Base
CREATE OR REPLACE TEMPORARY VIEW ntpj_benchmark AS(
  SELECT
    a.job_id
    , a.agent
    , AVG(a.exp_duration_job) AS exp_duration_job
    , DATE_TRUNC('MONTH', a.start_date) AS benchmark_month
    , b.xforce
    , b.xplead
    , b.squad
    , b.squad_district
  FROM usr.mx__cx.normalized_time_per_job AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.start_date) = b.snapshot_month
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_benchmark_agg AS(
  SELECT
    job_id
    , ROUND(AVG(exp_duration_job), 5) AS benchmark
    , benchmark_month
    , xforce
    , xplead
    , squad
    , squad_district
  FROM ntpj_benchmark
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_benchmark_base AS(
  SELECT
    *
    , LAG(benchmark) OVER (PARTITION BY job_id, xforce ORDER BY benchmark_month) AS previous_benchmark
  FROM ntpj_benchmark_agg
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_benchmark_final AS(
  SELECT
    xforce
    , xplead
    , job_id
    , benchmark_month
    , benchmark
    , previous_benchmark
    , CASE
        WHEN benchmark <= previous_benchmark THEN 'improved'
        WHEN benchmark > previous_benchmark THEN 'degraded'
        ELSE NULL
      END AS benchmark_status
    , squad
    , squad_district
  FROM ntpj_benchmark_base
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_benchmark AS(
  SELECT
    a.agent
    , a.xforce
    , a.xplead
    , CONCAT(a.squad_district, ' - ', b.shift) AS job_id
    , ROUND(AVG(a.occupancy_exp), 5) AS benchmark
    , DATE_TRUNC('MONTH', a.date) AS benchmark_month
    , a.squad
    , a.squad_district
    , b.shift
  FROM usr.mx__cx.normalized_occupancy AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
  WHERE date >= '2026-02-01'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_benchmark_base AS(
  SELECT
    *
    , LAG(benchmark) OVER (PARTITION BY job_id, xforce ORDER BY benchmark_month) AS previous_benchmark
  FROM occupancy_benchmark
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_benchmark_final AS(
  SELECT
    xforce
    , xplead
    , job_id
    , benchmark_month
    , benchmark
    , previous_benchmark
    , CASE
        WHEN benchmark >= previous_benchmark THEN 'improved'
        WHEN benchmark < previous_benchmark THEN 'degraded'
        ELSE NULL
      END AS benchmark_status
    , squad
    , squad_district
  FROM occupancy_benchmark_base
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_base AS(
  SELECT * FROM ntpj_benchmark_final
  UNION ALL
  SELECT * FROM occupancy_benchmark_final
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_final AS(
  SELECT
    a.date_reference
    , a.date_granularity
    , b.xforce
    , b.xplead
    , b.benchmark_month
    , COUNT(DISTINCT CASE WHEN b.benchmark_status = 'improved' THEN b.job_id END) AS improved_jobs
    , COUNT(DISTINCT CASE WHEN b.benchmark_status IS NOT NULL THEN b.job_id END) AS jobs
    , b.squad
    , b.squad_district
  FROM usr.mx__cx.internal_ops_performance_2026 AS a
  LEFT JOIN improved_benchmark_base AS b
    ON DATE_TRUNC('MONTH', date_reference) = b.benchmark_month
    AND a.xforce = b.xforce
  WHERE a.date_granularity IN ('week', 'month')
    AND a.metric = 'ntpj_xforce'
  GROUP BY ALL
);

-- SELECT * FROM improved_benchmark_final

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'improved_benchmark_squad' AS metric
    , SUM(improved_jobs) AS numerator
    , SUM(jobs) AS denominator
    , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
  FROM improved_benchmark_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'improved_benchmark_squad' AS metric
    , SUM(improved_jobs) AS numerator
    , SUM(jobs) AS denominator
    , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
  FROM improved_benchmark_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_squad AS (
  SELECT * FROM improved_benchmark_squad_monthly
  UNION ALL
  SELECT * FROM improved_benchmark_squad_weekly
);

-- SELECT * FROM improved_benchmark_squad

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark District Calculations
CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'improved_benchmark_district' AS metric
    , SUM(improved_jobs) AS numerator
    , SUM(jobs) AS denominator
    , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
  FROM improved_benchmark_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'improved_benchmark_district' AS metric
    , SUM(improved_jobs) AS numerator
    , SUM(jobs) AS denominator
    , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
  FROM improved_benchmark_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_district AS (
  SELECT * FROM improved_benchmark_district_monthly
  UNION ALL
  SELECT * FROM improved_benchmark_district_weekly
);

-- SELECT * FROM improved_benchmark_district

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_join_sd AS (
  -- SELECT * FROM improved_benchmark
  -- UNION ALL
  SELECT * FROM improved_benchmark_squad
  UNION ALL
  SELECT * FROM improved_benchmark_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Index XForces

-- COMMAND ----------

-- DBTITLE 1,Index XForces Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad_base AS(
  SELECT 
    a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.metric_value AS shrinkage_xforce
    , b.metric_value AS xpeers_in_target_xforce
    , c.metric_value AS average_index_agent
    , d.metric_value AS improved_benchmark
  FROM shrinkage_sd AS a
  LEFT JOIN xpeers_in_target_xforces_join_sd AS b
    ON a.squad = b.squad
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'xpeers_in_target_xforce_squad'
  LEFT JOIN average_index_agent_join_sd AS c
    ON a.squad = c.squad
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'average_index_agent_squad'
  LEFT JOIN improved_benchmark_join_sd AS d
    ON a.squad = d.squad
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'improved_benchmark_squad'
  WHERE a.date_granularity IN ('week', 'month')
    AND a.metric = 'shrinkage_squad'
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad_final AS(
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , CASE
        WHEN shrinkage_xforce <= 20 THEN 100
        WHEN shrinkage_xforce > 20 THEN 120 - shrinkage_xforce
        ELSE 0
      END AS shrinkage
    , COALESCE(xpeers_in_target_xforce, 0) AS xpeers_in_target_xforce
    , COALESCE(average_index_agent, 0) AS average_index_agent
    , CASE
        WHEN improved_benchmark >= 60 THEN 100
        WHEN improved_benchmark < 60 THEN improved_benchmark / 0.6
        ELSE 0
      END AS improved_benchmark
  FROM index_xforces_squad_base
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_xforce_squad' AS metric
    , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) AS numerator
    , 400 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4 AS metric_value
  FROM index_xforces_squad_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_xforce_squad' AS metric
    , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) AS numerator
    , 400 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4 AS metric_value
  FROM index_xforces_squad_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad AS (
  SELECT * FROM index_xforces_squad_monthly
  UNION ALL
  SELECT * FROM index_xforces_squad_weekly
);

-- SELECT * FROM index_xforces_squad

-- COMMAND ----------

-- DBTITLE 1,Index XForces District Calculations
CREATE OR REPLACE TEMPORARY VIEW index_xforces_district_base AS(
  SELECT 
    a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.metric_value AS shrinkage_xforce
    , b.metric_value AS xpeers_in_target_xforce
    , c.metric_value AS average_index_agent
    , d.metric_value AS improved_benchmark
  FROM shrinkage_sd AS a
  LEFT JOIN xpeers_in_target_xforces_join_sd AS b
    ON a.squad = b.squad
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'xpeers_in_target_xforce_district'
  LEFT JOIN average_index_agent_join_sd AS c
    ON a.squad = c.squad
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'average_index_agent_district'
  LEFT JOIN improved_benchmark_join_sd AS d
    ON a.squad = d.squad
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'improved_benchmark_district'
  WHERE a.date_granularity IN ('week', 'month')
    AND a.metric = 'shrinkage_district'
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_district_final AS(
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , CASE
        WHEN shrinkage_xforce <= 20 THEN 100
        WHEN shrinkage_xforce > 20 THEN 120 - shrinkage_xforce
        ELSE 0
      END AS shrinkage
    , COALESCE(xpeers_in_target_xforce, 0) AS xpeers_in_target_xforce
    , COALESCE(average_index_agent, 0) AS average_index_agent
    , CASE
        WHEN improved_benchmark >= 60 THEN 100
        WHEN improved_benchmark < 60 THEN improved_benchmark / 0.6
        ELSE 0
      END AS improved_benchmark
  FROM index_xforces_district_base
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_district_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_xforce_district' AS metric
    , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) AS numerator
    , 400 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4 AS metric_value
  FROM index_xforces_district_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_district_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_xforce_district' AS metric
    , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) AS numerator
    , 400 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4 AS metric_value
  FROM index_xforces_district_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_district AS (
  SELECT * FROM index_xforces_district_monthly
  UNION ALL
  SELECT * FROM index_xforces_district_weekly
);

-- SELECT * FROM index_xforces_district

-- COMMAND ----------

-- DBTITLE 1,Index XForces S&D Dataset
CREATE OR REPLACE TEMPORARY VIEW index_xforces_join_sd AS (
  -- SELECT * FROM index_xforces
  -- UNION ALL
  SELECT * FROM index_xforces_squad
  UNION ALL
  SELECT * FROM index_xforces_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Joins and Save

-- COMMAND ----------

-- DBTITLE 1,Joins S&D
CREATE OR REPLACE TEMPORARY VIEW dataset_sd AS (
  SELECT * FROM adherence_sd
  UNION ALL
  SELECT * FROM ntpj_sd
  UNION ALL
  SELECT * FROM nocc_sd
  UNION ALL
  SELECT * FROM shrinkage_sd
  UNION ALL
  SELECT * FROM quality_sd
  UNION ALL
  SELECT * FROM index_agents_join_sd
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_join_sd
  UNION ALL
  SELECT * FROM average_index_agent_join_sd
  UNION ALL
  SELECT * FROM improved_benchmark_join_sd
  UNION ALL
  SELECT * FROM index_xforces_join_sd
  UNION ALL
  SELECT * FROM nuvinhos_performance_join_sd
);

-- SELECT DISTINCT metric FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Save table S&D
CREATE OR REPLACE TABLE usr.mx__cx.internal_ops_performance_2026_sd AS
SELECT * FROM dataset_sd

-- COMMAND ----------

-- DBTITLE 1,Table Sharing
-- GRANT SELECT ON TABLE usr.mx__cx.internal_ops_performance_2026 TO `59e52f0a-0aa5-44b9-90f9-3d781cc0e097`;
-- SELECT * FROM usr.mx__cx.internal_ops_performance_2026

-- COMMAND ----------

-- GRANT SELECT ON TABLE usr.mx__cx.internal_ops_performance_2026_sd TO `59e52f0a-0aa5-44b9-90f9-3d781cc0e097`;
-- SELECT * FROM usr.mx__cx.internal_ops_performance_2026_sd