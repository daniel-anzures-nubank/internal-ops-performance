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
-- MAGIC #Xpeers Metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adherence

-- COMMAND ----------

-- DBTITLE 1,Adherence Base
CREATE OR REPLACE TEMPORARY VIEW adherence_final AS(
  SELECT
    *
  FROM usr.mx__cx.adherence_io
);

-- SELECT * FROM adherence_final

-- COMMAND ----------

-- DBTITLE 1,Adherence Agents Calculations
CREATE OR REPLACE TEMPORARY VIEW adherence_agents_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('DAY', date) AS date_reference
    , 'day' AS date_granularity
    , 'adherence_agent' AS metric
    , SUM(delivered_hours) AS numerator
    , SUM(required_hours) AS denominator
    , TRY_DIVIDE(SUM(delivered_hours) , SUM(required_hours)) *100 AS metric_value
  FROM adherence_final
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_agents_weekly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'adherence_agent' AS metric
    , SUM(delivered_hours) AS numerator
    , SUM(required_hours) AS denominator
    , TRY_DIVIDE(SUM(delivered_hours) , SUM(required_hours)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM adherence_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_agents_monthly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'adherence_agent' AS metric
    , SUM(delivered_hours) AS numerator
    , SUM(required_hours) AS denominator
    , TRY_DIVIDE(SUM(delivered_hours) , SUM(required_hours)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM adherence_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_agents AS (
  SELECT * FROM adherence_agents_daily
  UNION ALL
  SELECT * FROM adherence_agents_weekly
  UNION ALL
  SELECT * FROM adherence_agents_monthly
);

-- SELECT * FROM adherence_agents

-- COMMAND ----------

-- DBTITLE 0,Adherence Agents General Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW adherence_agents_general_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'adherence_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM adherence_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_agents_general_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'adherence_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM adherence_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_agents_general_quartile AS (
  SELECT * FROM adherence_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM adherence_agents_general_quartile_weekly
);

-- SELECT * FROM adherence_agents_general_quartile

-- COMMAND ----------

-- DBTITLE 1,Adherence Agents Team Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW adherence_agents_team_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'adherence_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM adherence_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_agents_team_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'adherence_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM adherence_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_agents_team_quartile AS (
  SELECT * FROM adherence_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM adherence_agents_team_quartile_weekly
);

-- SELECT * FROM adherence_agents_team_quartile

-- COMMAND ----------

-- DBTITLE 1,Adherence XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW adherence_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'adherence_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM adherence_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_xforces_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'adherence_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM adherence_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_xforces AS (
  SELECT * FROM adherence_xforces_monthly
  UNION ALL
  SELECT * FROM adherence_xforces_weekly
);

-- SELECT * FROM adherence_xforces

-- COMMAND ----------

-- DBTITLE 0,Adherence XPLeads Calculations
CREATE OR REPLACE TEMPORARY VIEW adherence_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'adherence_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM adherence_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'adherence_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM adherence_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_xpleads AS (
  SELECT * FROM adherence_xpleads_monthly
  UNION ALL
  SELECT * FROM adherence_xpleads_weekly
);

-- SELECT * FROM adherence_xpleads

-- COMMAND ----------

-- DBTITLE 1,Adherence General Dataset
CREATE OR REPLACE TEMPORARY VIEW adherence AS(
  SELECT * FROM adherence_agents
  UNION ALL
  SELECT * FROM adherence_agents_general_quartile
  UNION ALL
  SELECT * FROM adherence_agents_team_quartile
  UNION ALL
  SELECT * FROM adherence_xforces
  UNION ALL
  SELECT * FROM adherence_xpleads
);

-- SELECT * FROM adherence

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Normalized Time per Job 

-- COMMAND ----------

-- DBTITLE 1,NTPJ Base
CREATE OR REPLACE TEMPORARY VIEW ntpj_final AS(
  SELECT
    *
  FROM usr.mx__cx.normalized_time_per_job
);

SELECT * FROM ntpj_final

-- COMMAND ----------

-- DBTITLE 1,NTPJ Agents Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('DAY', start_date) AS date_reference
    , 'day' AS date_granularity
    , 'ntpj_agent' AS metric
    , SUM(duration) AS numerator
    , SUM(exp_duration_job * count) AS denominator
    , TRY_DIVIDE(SUM(duration) , SUM(exp_duration_job * count)) *100 AS metric_value
  FROM ntpj_final
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_weekly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', start_date) AS date_reference
    , 'week' AS date_granularity
    , 'ntpj_agent' AS metric
    , SUM(duration) AS numerator
    , SUM(exp_duration_job * count) AS denominator
    , TRY_DIVIDE(SUM(duration) , SUM(exp_duration_job * count)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', start_date) ORDER BY start_date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', start_date) ORDER BY start_date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', start_date) ORDER BY start_date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', start_date) ORDER BY start_date DESC) AS first_squad_district
    FROM ntpj_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_monthly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', start_date) AS date_reference
    , 'month' AS date_granularity
    , 'ntpj_agent' AS metric
    , SUM(duration) AS numerator
    , SUM(exp_duration_job * count) AS denominator
    , TRY_DIVIDE(SUM(duration) , SUM(exp_duration_job * count)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', start_date) ORDER BY start_date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', start_date) ORDER BY start_date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', start_date) ORDER BY start_date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', start_date) ORDER BY start_date DESC) AS first_squad_district
    FROM ntpj_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents AS (
  SELECT * FROM ntpj_agents_daily
  UNION ALL
  SELECT * FROM ntpj_agents_weekly
  UNION ALL
  SELECT * FROM ntpj_agents_monthly
);

SELECT * FROM ntpj_agents

-- COMMAND ----------

-- DBTITLE 1,NTPJ Agents General Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_general_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) ASC) AS metric_value
  FROM ntpj_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_general_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) ASC) AS metric_value
  FROM ntpj_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_general_quartile AS (
  SELECT * FROM ntpj_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM ntpj_agents_general_quartile_weekly
);

-- COMMAND ----------

-- DBTITLE 1,NTPJ Agents Team Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_team_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) ASC) AS metric_value
  FROM ntpj_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_team_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) ASC) AS metric_value
  FROM ntpj_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_team_quartile AS (
  SELECT * FROM ntpj_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM ntpj_agents_team_quartile_weekly
);

-- SELECT * FROM ntpj_agents_team_quartile

-- COMMAND ----------

-- DBTITLE 1,NTPJ XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM ntpj_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_xforces_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM ntpj_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_xforces AS (
  SELECT * FROM ntpj_xforces_monthly
  UNION ALL
  SELECT * FROM ntpj_xforces_weekly
);

-- SELECT * FROM ntpj_xforces

-- COMMAND ----------

-- DBTITLE 1,NTPJ XPLeads Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM ntpj_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM ntpj_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_xpleads AS (
  SELECT * FROM ntpj_xpleads_monthly
  UNION ALL
  SELECT * FROM ntpj_xpleads_weekly
);

-- SELECT * FROM ntpj_xpleads

-- COMMAND ----------

-- DBTITLE 1,NTPJ General Dataset
CREATE OR REPLACE TEMPORARY VIEW ntpj AS(
  SELECT * FROM ntpj_agents
  UNION ALL
  SELECT * FROM ntpj_agents_general_quartile
  UNION ALL
  SELECT * FROM ntpj_agents_team_quartile
  UNION ALL
  SELECT * FROM ntpj_xforces
  UNION ALL
  SELECT * FROM ntpj_xpleads
);

-- SELECT * FROM ntpj

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Normalized Occupancy

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Base
CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy_final AS(
  SELECT
    *
  FROM usr.mx__cx.normalized_occupancy
  WHERE date >= '2026-03-01'
);

SELECT * FROM normalized_occupancy_final

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Agents Calculations
CREATE OR REPLACE TEMPORARY VIEW nocc_agents_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('DAY', date) AS date_reference
    , 'day' AS date_granularity
    , 'nocc_agent' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) * 100 AS numerator
    , MAX(occupancy_exp) * 100 AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM normalized_occupancy_final
  WHERE NOT (agent = 'nitza.zarza' AND DATE_TRUNC('MONTH', date) IN (DATE '2026-04-01', DATE '2026-05-01'))
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_weekly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'nocc_agent' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) * 100 AS numerator
    , MAX(occupancy_exp) * 100 AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM normalized_occupancy_final
    WHERE NOT (agent = 'nitza.zarza' AND DATE_TRUNC('MONTH', date) IN (DATE '2026-04-01', DATE '2026-05-01'))
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_monthly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'nocc_agent' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) * 100 AS numerator
    , MAX(occupancy_exp) * 100 AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM normalized_occupancy_final
    WHERE NOT (agent = 'nitza.zarza' AND DATE_TRUNC('MONTH', date) IN (DATE '2026-04-01', DATE '2026-05-01'))
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents AS (
  SELECT * FROM nocc_agents_daily
  UNION ALL
  SELECT * FROM nocc_agents_weekly
  UNION ALL
  SELECT * FROM nocc_agents_monthly
);

-- SELECT * FROM nocc_agents

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy General Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW nocc_agents_general_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'nocc_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM nocc_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_general_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'nocc_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM nocc_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_general_quartile AS (
  SELECT * FROM nocc_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM nocc_agents_general_quartile_weekly
);

-- SELECT * FROM nocc_agents_general_quartile

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Team Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW nocc_agents_team_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'nocc_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM nocc_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_team_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'nocc_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM nocc_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_team_quartile AS (
  SELECT * FROM nocc_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM nocc_agents_team_quartile_weekly
);

-- SELECT * FROM nocc_agents_team_quartile

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW nocc_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nocc_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM nocc_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_xforces_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nocc_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM nocc_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_xforces AS (
  SELECT * FROM nocc_xforces_monthly
  UNION ALL
  SELECT * FROM nocc_xforces_weekly
);

-- SELECT * FROM nocc_xforces

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy XPLeads Calculations
CREATE OR REPLACE TEMPORARY VIEW nocc_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nocc_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM nocc_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nocc_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM nocc_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_xpleads AS (
  SELECT * FROM nocc_xpleads_monthly
  UNION ALL
  SELECT * FROM nocc_xpleads_weekly
);

-- SELECT * FROM nocc_xpleads

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy General Dataset
CREATE OR REPLACE TEMPORARY VIEW nocc AS(
  SELECT * FROM nocc_agents
  UNION ALL
  SELECT * FROM nocc_agents_general_quartile
  UNION ALL
  SELECT * FROM nocc_agents_team_quartile
  UNION ALL
  SELECT * FROM nocc_xforces
  UNION ALL
  SELECT * FROM nocc_xpleads
);

-- SELECT * FROM nocc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quality Metric

-- COMMAND ----------

-- DBTITLE 1,Quality Base (Bruno's version)
CREATE OR REPLACE TEMPORARY VIEW qa_score_base AS(
  SELECT 
    * 
  FROM usr.mx__cx.quality_io
);

--SELECT * FROM qa_score_base

-- COMMAND ----------

-- DBTITLE 1,QA Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_agents_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('DAY', date) AS date_reference
    , 'day' AS date_granularity
    , 'qa_score_agent' AS metric
    , SUM(qa_score * evaluations) AS numerator
    , SUM(evaluations) AS denominator
    , TRY_DIVIDE(SUM(qa_score * evaluations), SUM(evaluations)) AS metric_value 
  FROM qa_score_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_agents_weekly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'qa_score_agent' AS metric
    , SUM(qa_score * evaluations) AS numerator
    , SUM(evaluations) AS denominator
    , TRY_DIVIDE(SUM(qa_score * evaluations), SUM(evaluations)) AS metric_value 
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM qa_score_base
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_agents_monthly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'qa_score_agent' AS metric
    , SUM(qa_score * evaluations) AS numerator
    , SUM(evaluations) AS denominator
    , TRY_DIVIDE(SUM(qa_score * evaluations), SUM(evaluations)) AS metric_value 
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM qa_score_base
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_score_agents AS (
  SELECT * FROM qa_agents_daily
  UNION ALL
  SELECT * FROM qa_agents_weekly
  UNION ALL
  SELECT * FROM qa_agents_monthly
);

-- SELECT * FROM qa_score_agents

-- COMMAND ----------

-- DBTITLE 1,QA General Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_agents_general_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'qa_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM qa_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_agents_general_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'qa_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM qa_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_agents_general_quartile AS (
  SELECT * FROM qa_agents_general_quartile_weekly
  UNION ALL
  SELECT * FROM qa_agents_general_quartile_monthly
);

-- SELECT * FROM qa_agents_general_quartile_monthly

-- COMMAND ----------

-- DBTITLE 1,QA Team Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_agents_team_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'qa_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM qa_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_agents_team_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'qa_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM qa_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_agents_team_quartile AS (
  SELECT * FROM qa_agents_team_quartile_weekly
  UNION ALL
  SELECT * FROM qa_agents_team_quartile_monthly
);

-- SELECT * FROM qa_agents_team_quartile_monthly

-- COMMAND ----------

-- DBTITLE 1,QA Xforces Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'qa_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM qa_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_xforces_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'qa_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM qa_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_xforces AS (
  SELECT * FROM qa_xforces_weekly
  UNION ALL
  SELECT * FROM qa_xforces_monthly
);

-- SELECT * FROM qa_xforces_monthly

-- COMMAND ----------

-- DBTITLE 1,QA XPLeads Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'qa_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM qa_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'qa_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM qa_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_xpleads AS (
  SELECT * FROM qa_xpleads_weekly
  UNION ALL
  SELECT * FROM qa_xpleads_monthly
);

-- SELECT * FROM qa_xpleads_monthly

-- COMMAND ----------

-- DBTITLE 1,QA General Dataset
CREATE OR REPLACE TEMPORARY VIEW quality AS(
  SELECT * FROM qa_score_agents
UNION ALL
SELECT * FROM qa_agents_general_quartile
UNION ALL
SELECT * FROM qa_agents_team_quartile
UNION ALL
SELECT * FROM qa_xforces
UNION ALL
SELECT * FROM qa_xpleads
);

-- SELECT * FROM quality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Index Agents

-- COMMAND ----------

-- DBTITLE 1,Index Agents Base
CREATE OR REPLACE TEMPORARY VIEW index_agents_base AS(
  SELECT 
    a.agent
    , a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.metric_value AS adherence
    , b.metric_value AS ntpj
    , c.metric_value AS nocc
    , d.metric_value AS quality
  FROM adherence AS a
  LEFT JOIN ntpj AS b
    ON a.agent = b.agent
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'ntpj_agent'
  LEFT JOIN nocc AS c
    ON a.agent = c.agent
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_agent'
  LEFT JOIN quality AS d
    ON a.agent = d.agent
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'qa_score_agent'
  WHERE a.date_granularity IN ('week', 'month', 'quarter', 'semester', 'year')
    AND a.metric = 'adherence_agent'
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_final AS(
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , COALESCE(adherence, 0) AS adherence
    , CASE
        WHEN ntpj <= 100 THEN 100
        WHEN ntpj > 100 AND ntpj <= 200 THEN (200 - ntpj)
        WHEN ntpj > 200 THEN 0
        ELSE 0
      END AS ntpj
    , CASE
        WHEN agent = 'nitza.zarza' AND date_reference IN (DATE '2026-04-01', DATE '2026-05-01') THEN NULL
        WHEN nocc >= 100 THEN 100
        WHEN nocc <= 100 THEN nocc
        ELSE 0
      END AS nocc
    , quality
    FROM index_agents_base
);

-- SELECT * FROM index_agents_final

-- COMMAND ----------

-- DBTITLE 1,Index Agents Calculations
CREATE OR REPLACE TEMPORARY VIEW index_agents_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent' AS metric
    , CASE
        WHEN agent = 'nitza.zarza' AND date_reference IN (DATE '2026-04-01', DATE '2026-05-01') AND quality IS NULL
          THEN (adherence + ntpj)
        WHEN agent = 'nitza.zarza' AND date_reference IN (DATE '2026-04-01', DATE '2026-05-01') AND quality IS NOT NULL
          THEN (adherence + ntpj + quality)
        WHEN date_reference = '2026-01-01' 
          THEN (adherence + ntpj) 
        WHEN date_reference = '2026-02-01' AND quality IS NULL
          THEN (adherence + ntpj)
        WHEN date_reference = '2026-02-01' AND quality IS NOT NULL
          THEN (adherence + ntpj + quality)
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc)
        ELSE (adherence + ntpj + nocc + quality) 
      END AS numerator
    , CASE
        WHEN agent = 'nitza.zarza' AND date_reference IN (DATE '2026-04-01', DATE '2026-05-01') AND quality IS NULL
          THEN 200
        WHEN agent = 'nitza.zarza' AND date_reference IN (DATE '2026-04-01', DATE '2026-05-01') AND quality IS NOT NULL
          THEN 300
        WHEN date_reference = '2026-01-01' 
          THEN 200 
        WHEN date_reference = '2026-02-01' AND quality IS NULL
          THEN 200
        WHEN date_reference = '2026-02-01' AND quality IS NOT NULL
          THEN 300
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN agent = 'nitza.zarza' AND date_reference IN (DATE '2026-04-01', DATE '2026-05-01') AND quality IS NULL
          THEN (adherence + ntpj) / 2
        WHEN agent = 'nitza.zarza' AND date_reference IN (DATE '2026-04-01', DATE '2026-05-01') AND quality IS NOT NULL
          THEN (adherence + ntpj + quality) / 3
        WHEN date_reference = '2026-01-01' 
          THEN (adherence + ntpj) / 2
        WHEN date_reference = '2026-02-01' AND quality IS NULL
          THEN (adherence + ntpj) / 2
        WHEN date_reference = '2026-02-01' AND quality IS NOT NULL
          THEN (adherence + ntpj + quality) / 3
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc) / 3
        ELSE (adherence + ntpj + nocc + quality) / 4
      END AS metric_value
  FROM index_agents_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent' AS metric
    , CASE
        WHEN agent = 'nitza.zarza' AND date_reference >= '2026-04-01' AND date_reference < '2026-06-01' AND quality IS NULL
          THEN (adherence + ntpj)
        WHEN agent = 'nitza.zarza' AND date_reference >= '2026-04-01' AND date_reference < '2026-06-01' AND quality IS NOT NULL
          THEN (adherence + ntpj + quality)
        WHEN date_reference <= '2026-01-31' 
          THEN (adherence + ntpj) 
        WHEN date_reference <= '2026-02-28' AND quality IS NULL
          THEN (adherence + ntpj)
        WHEN date_reference <= '2026-02-28' AND quality IS NOT NULL
          THEN (adherence + ntpj + quality)
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc)
        ELSE (adherence + ntpj + nocc + quality) 
      END AS numerator
    , CASE
        WHEN agent = 'nitza.zarza' AND date_reference >= '2026-04-01' AND date_reference < '2026-06-01' AND quality IS NULL
          THEN 200
        WHEN agent = 'nitza.zarza' AND date_reference >= '2026-04-01' AND date_reference < '2026-06-01' AND quality IS NOT NULL
          THEN 300
        WHEN date_reference <= '2026-01-31' 
          THEN 200 
        WHEN date_reference <= '2026-02-28' AND quality IS NULL
          THEN 200
        WHEN date_reference <= '2026-02-28' AND quality IS NOT NULL
          THEN 300
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN agent = 'nitza.zarza' AND date_reference >= '2026-04-01' AND date_reference < '2026-06-01' AND quality IS NULL
          THEN (adherence + ntpj) / 2
        WHEN agent = 'nitza.zarza' AND date_reference >= '2026-04-01' AND date_reference < '2026-06-01' AND quality IS NOT NULL
          THEN (adherence + ntpj + quality) / 3
        WHEN date_reference <= '2026-01-31' 
          THEN (adherence + ntpj) / 2
        WHEN date_reference <= '2026-02-28' AND quality IS NULL
          THEN (adherence + ntpj) / 2
        WHEN date_reference <= '2026-02-28' AND quality IS NOT NULL
          THEN (adherence + ntpj + quality) / 3
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc) / 3
        ELSE (adherence + ntpj + nocc + quality) / 4
      END AS metric_value
  FROM index_agents_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents AS (
  SELECT * FROM index_agents_monthly
  UNION ALL
  SELECT * FROM index_agents_weekly
);

-- SELECT * FROM index_agents_monthly

-- COMMAND ----------

-- DBTITLE 1,Index Agents General Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW index_agents_general_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM index_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_general_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM index_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_general_quartile AS (
  SELECT * FROM index_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM index_agents_general_quartile_weekly
);

-- SELECT * FROM index_agents_general_quartile

-- COMMAND ----------

-- DBTITLE 1,Index Agents Team Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW index_agents_team_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM index_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_team_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference, squad, CASE WHEN squad IN ('idsec', 'txn') THEN squad_district ELSE squad END ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM index_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_team_quartile AS (
  SELECT * FROM index_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM index_agents_team_quartile_weekly
);

-- SELECT * FROM index_agents_team_quartile

-- COMMAND ----------

-- DBTITLE 1,Index Agents General Dataset
CREATE OR REPLACE TEMPORARY VIEW index_agents_join AS(
  SELECT * FROM index_agents
  UNION ALL
  SELECT * FROM index_agents_general_quartile
  UNION ALL
  SELECT * FROM index_agents_team_quartile
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
CREATE OR REPLACE TEMPORARY VIEW shrinkage_final AS(
  SELECT 
    * 
  FROM usr.mx__cx.shrinkage_io
);

-- SELECT * FROM shrinkage_final

-- COMMAND ----------

-- DBTITLE 1,Shrinkage XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW shrinkage_xforces_monthly AS (
  SELECT
    NULL AS agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , NULL AS squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'shrinkage_xforce' AS metric
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

CREATE OR REPLACE TEMPORARY VIEW shrinkage_xforces_weekly AS (
  SELECT
    NULL AS agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , NULL AS squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'shrinkage_xforce' AS metric
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

CREATE OR REPLACE TEMPORARY VIEW shrinkage_xforces AS (
  SELECT * FROM shrinkage_xforces_monthly
  UNION ALL
  SELECT * FROM shrinkage_xforces_weekly
);

-- SELECT * FROM shrinkage_xforces

-- COMMAND ----------

-- DBTITLE 1,Shrinkage XPLeads Calculations
CREATE OR REPLACE TEMPORARY VIEW shrinkage_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'shrinkage_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END) AS numerator
    , COUNT(DISTINCT xforce) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END), COUNT(DISTINCT xforce)) *100 AS metric_value
  FROM shrinkage_xforces_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'shrinkage_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END) AS numerator
    , COUNT(DISTINCT xforce) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END), COUNT(DISTINCT xforce)) *100 AS metric_value
  FROM shrinkage_xforces_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_xpleads AS (
  SELECT * FROM shrinkage_xpleads_monthly
  UNION ALL
  SELECT * FROM shrinkage_xpleads_weekly
);

-- SELECT * FROM shrinkage_xpleads

-- COMMAND ----------

-- DBTITLE 1,Shrinkage General Dataset
CREATE OR REPLACE TEMPORARY VIEW shrinkage AS(
  SELECT * FROM shrinkage_xforces
  UNION ALL
  SELECT * FROM shrinkage_xpleads
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
  FROM adherence AS a
  LEFT JOIN ntpj AS b
    ON a.xforce = b.xforce
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'ntpj_xforce'
  LEFT JOIN nocc AS c
    ON a.xforce = c.xforce
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_xforce'
  LEFT JOIN quality AS d
    ON a.xforce = d.xforce
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'qa_xforce'
  WHERE a.date_granularity IN ('week', 'month', 'quarter', 'semester', 'year')
    AND a.metric = 'adherence_xforce'
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_final AS(
  SELECT
    *
    , CASE
        WHEN date_reference < '2026-02-01' THEN (adherence_in_target + ntpj_in_target)
        WHEN date_reference < '2026-03-01' THEN (adherence_in_target + ntpj_in_target + COALESCE(qa_in_target, 0))
        ELSE (adherence_in_target + ntpj_in_target + COALESCE(nocc_in_target, 0) + COALESCE(qa_in_target, 0))
      END AS xpeers_in_target
    , CASE 
        WHEN date_reference < '2026-02-01' THEN (adherence_xpeers + ntpj_xpeers)
        WHEN date_reference < '2026-03-01' THEN (adherence_xpeers + ntpj_xpeers + COALESCE(qa_xpeers, 0))
        ELSE (adherence_xpeers + ntpj_xpeers + COALESCE(nocc_xpeers, 0) + COALESCE(qa_xpeers, 0))
      END AS xpeers
  FROM xpeers_in_target_base
);

-- SELECT * FROM xpeers_in_target_final

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xforce' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xforce' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces AS (
  SELECT * FROM xpeers_in_target_xforces_monthly
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_weekly
);

-- SELECT * FROM xpeers_in_target_xforces_monthly

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XForces General Dataset
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_join AS (
  SELECT * FROM xpeers_in_target_xforces
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Average Index Agent

-- COMMAND ----------

-- DBTITLE 1,Average Index Agent Calculations
CREATE OR REPLACE TEMPORARY VIEW average_index_agent_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'average_index_agent' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_agents_monthly
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_agent_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'average_index_agent' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_agents_weekly
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_agent AS (
  SELECT * FROM average_index_agent_monthly
  UNION ALL
  SELECT * FROM average_index_agent_weekly
);

-- SELECT * FROM average_index_agent

-- COMMAND ----------

-- DBTITLE 1,Average Index Agents General Dataset
CREATE OR REPLACE TEMPORARY VIEW average_index_agent_join AS (
  SELECT * FROM average_index_agent
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
  FROM index_agents AS a
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
        ELSE NULL
      END AS nuvinhos_average
    , CASE WHEN nuvinho = 'old' THEN AVG(metric_value)
        ELSE NULL
      END AS old_average
    , nuvinho
  FROM nuvinhos_performance_base
  WHERE date_granularity IN ('week', 'month')
  GROUP BY ALL
);

-- SELECT * FROM nuvinhos_performance_final

-- COMMAND ----------

-- DBTITLE 1,Nuvinhos Performance Calculations
CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nuvinhos_performance' AS metric
    , AVG(nuvinhos_average) AS numerator
    , AVG(old_average) AS denominator
    , TRY_DIVIDE(AVG(nuvinhos_average), AVG(old_average)) * 100 AS metric_value
  FROM nuvinhos_performance_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'nuvinhos_performance' AS metric
    , AVG(nuvinhos_average) AS numerator
    , AVG(old_average) AS denominator
    , TRY_DIVIDE(AVG(nuvinhos_average), AVG(old_average)) * 100 AS metric_value
  FROM nuvinhos_performance_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance AS (
  SELECT * FROM nuvinhos_performance_monthly
  UNION ALL
  SELECT * FROM nuvinhos_performance_weekly
);

-- SELECT * FROM nuvinhos_performance

-- COMMAND ----------

-- DBTITLE 1,Nuvinhos Performance General Dataset
CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_join AS (
  SELECT * FROM nuvinhos_performance
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Improved Benchmarks

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark Base
CREATE OR REPLACE TEMPORARY VIEW ntpj_benchmark AS(
  SELECT
    job_id
    , agent
    , AVG(exp_duration_job) AS exp_duration_job
    , DATE_TRUNC('MONTH', start_date) AS benchmark_month
    , xforce
    , xplead
    , squad
    , squad_district
  FROM usr.mx__cx.normalized_time_per_job
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
    xforce
    , xplead
    , CONCAT(squad_district, ' - ', shift) AS job_id
    , ROUND(AVG(occupancy_exp), 5) AS benchmark
    , DATE_TRUNC('MONTH', date) AS benchmark_month
    , squad
    , squad_district
  FROM usr.mx__cx.normalized_occupancy
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
  FROM ntpj_xforces AS a
  LEFT JOIN improved_benchmark_base AS b
    ON DATE_TRUNC('MONTH', date_reference) = b.benchmark_month
    AND a.xforce = b.xforce
  WHERE a.date_granularity IN ('week', 'month')
  GROUP BY ALL
);

-- SELECT * FROM improved_benchmark_final

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark Calculations
CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'improved_benchmark' AS metric
    , SUM(improved_jobs) AS numerator
    , SUM(jobs) AS denominator
    , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
  FROM improved_benchmark_final
  WHERE date_granularity = 'month'
    AND date_reference < '2026-05-01'
    AND NOT (xplead = 'david.fernandez' AND date_reference >= '2026-04-01')
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'improved_benchmark' AS metric
    , SUM(improved_jobs) AS numerator
    , SUM(jobs) AS denominator
    , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
  FROM improved_benchmark_final
  WHERE date_granularity = 'week'
    AND date_reference < '2026-05-01'
    AND NOT (xplead = 'david.fernandez' AND date_reference >= '2026-04-01')
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark AS (
  SELECT * FROM improved_benchmark_monthly
  UNION ALL
  SELECT * FROM improved_benchmark_weekly
);

-- SELECT * FROM improved_benchmark

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark General Dataset
CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_join AS (
  SELECT * FROM improved_benchmark
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Index XForces

-- COMMAND ----------

-- DBTITLE 1,Index XForces Base
CREATE OR REPLACE TEMPORARY VIEW index_xforces_base AS(
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
  FROM shrinkage AS a
  LEFT JOIN xpeers_in_target_xforces AS b
    ON a.xforce = b.xforce
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'xpeers_in_target_xforce'
  LEFT JOIN average_index_agent AS c
    ON a.xforce = c.xforce
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'average_index_agent'
  LEFT JOIN improved_benchmark AS d
    ON a.xforce = d.xforce
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'improved_benchmark'
  WHERE a.date_granularity IN ('week', 'month', 'quarter', 'semester', 'year')
    AND a.metric = 'shrinkage_xforce'
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_final AS(
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    -- Shrinkage target: 23% for May/June 2026, 20% otherwise
    -- Score: 100 at or below target; drops 1pt per % above (= (100 + target) - actual)
    , CASE
        WHEN date_reference IN (DATE '2026-05-01', DATE '2026-06-01') AND shrinkage_xforce <= 23 THEN 100
        WHEN date_reference IN (DATE '2026-05-01', DATE '2026-06-01') AND shrinkage_xforce >  23 THEN (100 + 23) - shrinkage_xforce
        WHEN shrinkage_xforce <= 20 THEN 100
        WHEN shrinkage_xforce >  20 THEN (100 + 20) - shrinkage_xforce
        ELSE 0
      END AS shrinkage
    -- On target (>= 70%): linearly scale from 90 (at threshold) to 100 (at max)
    -- Off target (< 70%): use actual value as-is
    , CASE
        WHEN COALESCE(xpeers_in_target_xforce, 0) >= 70
          THEN 90 + (COALESCE(xpeers_in_target_xforce, 0) - 70) * (100 - 90) / (100 - 70)
        ELSE COALESCE(xpeers_in_target_xforce, 0)
      END AS xpeers_in_target_xforce
    , COALESCE(average_index_agent, 0) AS average_index_agent
    , CASE
        WHEN improved_benchmark >= 60 THEN 100
        WHEN improved_benchmark < 60 THEN improved_benchmark / 0.6
        ELSE 0
      END AS improved_benchmark
  FROM index_xforces_base
);

-- SELECT * FROM index_xforces_final

-- COMMAND ----------

-- DBTITLE 1,Index XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW index_xforces_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_xforce' AS metric
    , CASE
        WHEN xplead = 'david.fernandez' AND date_reference >= '2026-04-01'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent)
        WHEN date_reference >= '2026-05-01'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent)
        ELSE (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark)
      END AS numerator
    , CASE
        WHEN xplead = 'david.fernandez' AND date_reference >= '2026-04-01' THEN 300
        WHEN date_reference >= '2026-05-01' THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN xplead = 'david.fernandez' AND date_reference >= '2026-04-01'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3
        WHEN date_reference >= '2026-05-01'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3
        ELSE (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4
      END AS metric_value
  FROM index_xforces_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_xforce' AS metric
    , CASE
        WHEN xplead = 'david.fernandez' AND date_reference >= '2026-04-01'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent)
        WHEN date_reference >= '2026-05-01'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent)
        ELSE (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark)
      END AS numerator
    , CASE
        WHEN xplead = 'david.fernandez' AND date_reference >= '2026-04-01' THEN 300
        WHEN date_reference >= '2026-05-01' THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN xplead = 'david.fernandez' AND date_reference >= '2026-04-01'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3
        WHEN date_reference >= '2026-05-01'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3
        ELSE (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4
      END AS metric_value
  FROM index_xforces_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces AS (
  SELECT * FROM index_xforces_monthly
  UNION ALL
  SELECT * FROM index_xforces_weekly
);

-- SELECT * FROM index_xforces

-- COMMAND ----------

-- DBTITLE 1,Index XForces General Dataset
CREATE OR REPLACE TEMPORARY VIEW index_xforces_join AS (
  SELECT * FROM index_xforces
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #XPLeads Metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Xpeers in Target for XPLead

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XPLead Base
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xplead_base AS(
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
  FROM adherence AS a
  LEFT JOIN ntpj AS b
    ON a.xplead = b.xplead
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'ntpj_xplead'
  LEFT JOIN nocc AS c
    ON a.xplead = c.xplead
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_xplead'
  LEFT JOIN quality AS d
    ON a.xplead = d.xplead
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'qa_xplead'
  WHERE a.date_granularity IN ('week', 'month', 'quarter', 'semester', 'year')
    AND a.metric = 'adherence_xplead'
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xplead_final AS(
  SELECT
    *
    , CASE
        WHEN date_reference <= '2026-01-01' THEN (adherence_in_target + ntpj_in_target)
        WHEN date_reference <= '2026-02-01' THEN (adherence_in_target + ntpj_in_target + COALESCE(qa_in_target, 0))
        ELSE (adherence_in_target + ntpj_in_target + COALESCE(nocc_in_target, 0) + COALESCE(qa_in_target, 0))
      END AS xpeers_in_target
    , CASE 
        WHEN date_reference <= '2026-01-01' THEN (adherence_xpeers + ntpj_xpeers)
        WHEN date_reference <= '2026-02-01' THEN (adherence_xpeers + ntpj_xpeers + COALESCE(qa_xpeers, 0))
        ELSE (adherence_xpeers + ntpj_xpeers + COALESCE(nocc_xpeers, 0) + COALESCE(qa_xpeers, 0))
      END AS xpeers
  FROM xpeers_in_target_xplead_base
);

-- SELECT * FROM xpeers_in_target_xplead_final

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XPLead Calculations
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xplead' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_xplead_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xplead' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_xplead_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads AS (
  SELECT * FROM xpeers_in_target_xpleads_monthly
  UNION ALL
  SELECT * FROM xpeers_in_target_xpleads_weekly
);

-- SELECT * FROM xpeers_in_target_xpleads_monthly

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XPLead General Dataset
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_join AS (
  SELECT * FROM xpeers_in_target_xpleads
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Average Index XForces

-- COMMAND ----------

-- DBTITLE 1,Average Index XForces Base
CREATE OR REPLACE TEMPORARY VIEW average_index_xforces_base AS(
  SELECT
    *
  FROM index_xforces_join
);

-- SELECT * FROM average_index_xforces_base

-- COMMAND ----------

-- DBTITLE 1,Average Index XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'average_index_xforce' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforces_base
  WHERE date_granularity = 'month'
    AND metric = 'index_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'average_index_xforce' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforces_base
  WHERE date_granularity = 'week'
    AND metric = 'index_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_xforce AS (
  SELECT * FROM average_index_xforce_monthly
  UNION ALL
  SELECT * FROM average_index_xforce_weekly
);

-- SELECT * FROM average_index_xforce

-- COMMAND ----------

-- DBTITLE 1,Average Index XForces General Dataset
CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_join AS (
  SELECT * FROM average_index_xforce
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Joins and Save

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC table = "usr.mx__cx.internal_ops_performance_2026"
-- MAGIC
-- MAGIC metrics = [
-- MAGIC     "adherence",
-- MAGIC     "ntpj",
-- MAGIC     "nocc",
-- MAGIC     "shrinkage",
-- MAGIC     "quality",
-- MAGIC     "index_agents_join",
-- MAGIC     "xpeers_in_target_xforces_join",
-- MAGIC     "average_index_agent_join",
-- MAGIC     "improved_benchmark_join",
-- MAGIC     "index_xforces_join",
-- MAGIC     "xpeers_in_target_xpleads_join",
-- MAGIC     "average_index_xforce_join",
-- MAGIC     "nuvinhos_performance_join",
-- MAGIC ]
-- MAGIC
-- MAGIC # ---------- Table creation ----------
-- MAGIC print("Creating table...")
-- MAGIC try:
-- MAGIC     spark.sql(f"""
-- MAGIC         CREATE OR REPLACE TABLE {table} (
-- MAGIC             agent STRING,
-- MAGIC             xforce STRING,
-- MAGIC             xplead STRING,
-- MAGIC             squad STRING,
-- MAGIC             squad_district STRING,
-- MAGIC             date_reference TIMESTAMP,
-- MAGIC             date_granularity STRING,
-- MAGIC             metric STRING,
-- MAGIC             numerator DOUBLE,
-- MAGIC             denominator DOUBLE,
-- MAGIC             metric_value DOUBLE
-- MAGIC         ) USING DELTA
-- MAGIC     """)
-- MAGIC     print(f"✓ {table} table created\n")
-- MAGIC except Exception as e:
-- MAGIC     raise RuntimeError(f"Fail to create table. Aborting.\nErro: {e}")
-- MAGIC
-- MAGIC # ---------- Addition per metric ----------
-- MAGIC saved_metrics = []
-- MAGIC metrics_with_error = []
-- MAGIC
-- MAGIC for metric in metrics:
-- MAGIC     start = datetime.now()
-- MAGIC     print(f"[{start.strftime('%H:%M:%S')}] Adding: {metric}...")
-- MAGIC     try:
-- MAGIC         spark.sql(f"""
-- MAGIC             INSERT INTO {table}
-- MAGIC             SELECT * FROM {metric}
-- MAGIC             WHERE date_reference >= '2026-01-01'
-- MAGIC         """)
-- MAGIC         duration = (datetime.now() - start).seconds
-- MAGIC         saved_metrics.append(metric)
-- MAGIC         print(f"  ✓ {metric} ({duration}s)\n")
-- MAGIC     except Exception as e:
-- MAGIC         metrics_with_error.append((metric, str(e)))
-- MAGIC         print(f"  ✗ {metric} — ERRO: {e}\n")
-- MAGIC
-- MAGIC # ---------- Resume ----------
-- MAGIC print("=" * 50)
-- MAGIC print(f"Succesfully saved ({len(saved_metrics)}/{len(metrics)}):")
-- MAGIC for m in saved_metrics:
-- MAGIC     print(f"  ✓ {m}")
-- MAGIC
-- MAGIC if metrics_with_error:
-- MAGIC     print(f"\nErrors ({len(metrics_with_error)}):")
-- MAGIC     for m, erro in metrics_with_error:
-- MAGIC         print(f"  ✗ {m}: {erro}")
-- MAGIC     print("\nTo run only the errors, use:")
-- MAGIC     nomes = [m for m, _ in metrics_with_error]
-- MAGIC     print(f"  metrics = {nomes}")
-- MAGIC else:
-- MAGIC     print("\nAll metrics saved!")

-- COMMAND ----------

-- DBTITLE 1,Joins General
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW dataset AS (
-- MAGIC   SELECT * FROM adherence
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM ntpj
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM nocc
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM shrinkage
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM quality
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM index_agents_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM xpeers_in_target_xforces_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM average_index_agent_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM improved_benchmark_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM index_xforces_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM xpeers_in_target_xpleads_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM average_index_xforce_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM nuvinhos_performance_join
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW dataset_2026 AS (
-- MAGIC   SELECT *
-- MAGIC   FROM dataset
-- MAGIC   WHERE date_reference >= '2026-01-01'
-- MAGIC );
-- MAGIC -- SELECT DISTINCT metric FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Save table General
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TABLE usr.mx__cx.internal_ops_performance_2026 AS
-- MAGIC SELECT * FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Table Sharing
-- MAGIC %skip
-- MAGIC GRANT SELECT ON TABLE usr.mx__cx.internal_ops_performance_2026 TO `59e52f0a-0aa5-44b9-90f9-3d781cc0e097`;
-- MAGIC SELECT * FROM usr.mx__cx.internal_ops_performance_2026