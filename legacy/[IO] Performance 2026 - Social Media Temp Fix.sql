-- Databricks notebook source
-- MAGIC %md
-- MAGIC # [IO] Performance 2026 - Social Media (Temp Fix)
-- MAGIC
-- MAGIC Auto-generated optimization of `[IO] Performance 2026 - Social Media`.
-- MAGIC **Same data, same output table** (`usr.mx__cx.internal_ops_performance_2026_social_media`).
-- MAGIC
-- MAGIC Two changes vs. the original, both output-preserving:
-- MAGIC
-- MAGIC 1. **Scope pushdown** — a small `social_agents` set (from the social-only roster)
-- MAGIC    is pushed into the three bases that otherwise scan the whole org (adherence,
-- MAGIC    quality, shrinkage). Those bases already narrow to social agents via their
-- MAGIC    `agent_information` join, so pre-filtering only removes rows that were going to
-- MAGIC    be discarded — identical result, far less data through the expensive joins.
-- MAGIC 2. **Materialization** — the roster plus the agent-level metric/index datasets
-- MAGIC    (adherence, nocc, tnps, wows, quality, index_agents_final, index_agents) are
-- MAGIC    written to Delta tables (`[Temp Fix] Materialize` cells) and the view names
-- MAGIC    re-pointed at them, so each downstream join reads cached data instead of
-- MAGIC    re-deriving the full lineage (which OOMs the composite-index stages). Xforce/
-- MAGIC    squad-only views (shrinkage, index_xforces_final, ...) stay lazy: their `agent`
-- MAGIC    column is all-NULL (VOID) and Delta would drop it on write.
-- MAGIC
-- MAGIC Intermediate tables live in `usr.danielanzures` with prefix `sm_temp_`; a commented
-- MAGIC cleanup cell at the end drops them. Run top-to-bottom in Databricks like the original.

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Setup staging schema
-- [Temp Fix] Schema that holds the intermediate (cached) tables. Created if missing.
CREATE SCHEMA IF NOT EXISTS usr.danielanzures;


-- COMMAND ----------

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
WHERE a.squad = 'social'
);

-- SELECT * FROM agent_information

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: agent_information
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_agent_information AS SELECT * FROM agent_information;
CREATE OR REPLACE TEMPORARY VIEW agent_information AS SELECT * FROM usr.danielanzures.sm_temp_agent_information;
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_social_agents AS
  SELECT DISTINCT LOWER(agent) AS agent FROM agent_information
  WHERE agent IS NOT NULL AND agent != '';
CREATE OR REPLACE TEMPORARY VIEW social_agents AS SELECT agent FROM usr.danielanzures.sm_temp_social_agents;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Xpeers Metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adherence

-- COMMAND ----------

-- DBTITLE 1,Adherence Base
CREATE OR REPLACE TEMPORARY VIEW agent_id AS(
  SELECT DISTINCT
    REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
    , actor__id
  FROM etl.mx__dataset.ops_actors
  WHERE LOWER(REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM social_agents)
);

CREATE OR REPLACE TEMPORARY VIEW status_historical_information AS(
  SELECT
    a.*
    , b.actor_status__actor_id
    , so.status_option__name
    , status_option__type
  FROM etl.mx__contract.staffing_hero__actor_status_status_history AS a
  LEFT JOIN etl.mx__contract.staffing_hero__actor_statuses AS b
    ON a.actor_status__id = b.actor_status__id
  LEFT JOIN  etl.mx__contract.staffing_hero__status_options AS so
    ON so.status_option__id = a.status_option__id
  WHERE a.db__tx_instant >= date_sub(current_date(), 180)
);

CREATE OR REPLACE TEMPORARY VIEW agent_productivity_base AS (
  SELECT 
    agent_productivity.*,
    status_historical_information.status_option__name,
    status_historical_information.db__tx_instant,
    row_number() over (partition by actor_id, timestamp order by db__tx_instant desc) as rn
  FROM etl.mx__dataset.agent_productivity
  LEFT JOIN status_historical_information
    ON status_historical_information.actor_status__actor_id = agent_productivity.actor_id
    AND status_historical_information.db__tx_instant <= agent_productivity.timestamp
  WHERE timestamp >= '2026-01-01' and status is null
    AND actor_id IN (SELECT actor__id FROM agent_id)
);

CREATE OR REPLACE TEMPORARY VIEW agent_productivity AS(
  SELECT
    b.agent
    , unix_timestamp(a.timestamp) AS activity_start
    , unix_timestamp(a.next_event_time) AS activity_end
    , a.channel_active AS channel
    , CASE
        WHEN coalesce(a.status, c.status_option__name) = 'oos' THEN 'oos'
        WHEN coalesce(a.status, c.status_option__name) IS NULL THEN 'null'
        ELSE 'shuffle'
      END AS status
    , a.timestamp
    , coalesce(a.status, c.status_option__name) as inferred_status
  FROM etl.mx__dataset.agent_productivity AS a
  INNER JOIN agent_id AS b
    ON a.actor_id = b.actor__id
  LEFT JOIN agent_productivity_base AS c
    ON c.actor_id = a.actor_id
    AND c.timestamp = a.timestamp
    AND c.rn = 1
  WHERE coalesce(a.status, c.status_option__name) IN ('available', 'oos', 'training')
    OR (coalesce(a.status, c.status_option__name) = 'pause' AND a.level_3 = 'paused_with_jobs')
    OR a.active_jobs > 0
    OR (a.timestamp >= '2026-01-22' AND coalesce(a.status, c.status_option__name) IS NULL)
    AND a.timestamp >= '2025-01-01'
);

CREATE OR REPLACE TEMPORARY VIEW dime_table AS(
  SELECT
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , agent_dime_squad AS squad
    , dime_date AS date
    , REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)  AS agent_name_extracted
    , unix_timestamp(local_timestamp_dime_slot_starts_at) + (6 * 60 * 60) AS slot_start
    , unix_timestamp(local_timestamp_dime_slot_starts_at) + (6 * 60 * 60) + (30 * 60) AS slot_end
    , CASE
        WHEN dimensioned_activity IN ('Control MC', 'xMC Debit Fraud')
          THEN 'oos'
        WHEN (agent LIKE '%mariana.najera%' OR agent LIKE '%hitzagari.leon%') AND dime_date = '2025-07-07'
          THEN 'time_off'
        WHEN (agent LIKE '%ana.torres%' OR agent LIKE '%antonio.perez%') AND dime_date >= '2025-07-01' AND dime_date <= '2025-08-31' AND activity_type_required = 'oos'
          THEN 'time_off'
        WHEN (agent LIKE '%jonathan.wade%' AND dime_date >= '2025-10-27' AND dime_date <= '2025-10-30' AND HOUR(local_timestamp_dime_slot_starts_at) = 6)
          THEN 'time_off'
        WHEN (agent LIKE '%erik.calleja%' AND dime_date = '2025-10-25')
          THEN 'time_off'
        WHEN (agent LIKE '%evelyn.carapia%' AND dime_date = '2025-10-20')
          THEN 'time_off'
        WHEN (agent LIKE '%david.ruiz%' AND dime_date = '2025-10-26')
          THEN 'time_off'
        WHEN (agent LIKE '%mario.buendia%' AND dime_date IN ('2025-10-28', '2025-10-30'))
          THEN 'time_off'
        WHEN (agent LIKE '%jorge.severiano%' AND dime_date IN ('2025-10-28', '2025-10-29', '2025-11-01'))
          THEN 'time_off'
        ELSE activity_type_required
      END AS activity_type_required
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE
      affiliation = 'nubank'
      AND dime_date >= '2025-01-01'
      AND activity_type_required IS NOT NULL
      AND activity_type_required NOT IN ('lunch_break', 'time_off', 'shrinkage')
      AND dimensioned_activity NOT IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle')
      AND agent_dime_squad IS NOT NULL
      AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote')
      AND dime_date <= DATE_SUB(DATE_TRUNC('WEEK', CURRENT_DATE()), 1)
      AND LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM social_agents)
);

CREATE OR REPLACE TEMPORARY VIEW joins AS(
  SELECT
    a.*
    , b.activity_start
    , b.activity_end
    , b.status
    , b.channel
  FROM dime_table AS a
  LEFT JOIN agent_productivity AS b
    ON b.agent = a.agent
    AND ((b.activity_start >= a.slot_start AND b.activity_start < a.slot_end)
    OR (b.activity_end >= a.slot_start AND b.activity_end < a.slot_end)
    OR (b.activity_start < a.slot_start AND b.activity_end >= a.slot_end))
  WHERE a.activity_type_required != 'time_off'
);

CREATE OR REPLACE TEMPORARY VIEW data_calculations AS(
  SELECT
    *
    , LEAST(COALESCE(LEAST(activity_end, slot_end) - GREATEST(activity_start, slot_start), 0), 30 * 60) AS adherent_time_final
  FROM joins
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_by_slot_2026 AS(
  SELECT
    a.agent
    , a.date
    , a.slot_start
    , a.activity_type_required
    -- , CASE
    --     WHEN SUM(COALESCE(a.adherent_time_final, 0)) <= 1800 THEN SUM(COALESCE(a.adherent_time_final, 0))
    --     ELSE 1800
    --   END AS adherent_time_final
    , LEAST(COALESCE(SUM(a.adherent_time_final), 0), 1800) AS adherent_time_final
    , b.xplead
    , b.xforce
    , b.squad
    , b.squad_district
  FROM data_calculations AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
  WHERE (a.date <= '2025-11-05' OR a.date >= '2025-11-20') 
    AND a.date >= '2025-12-01'
    AND b.status = 'active'
    AND a.date NOT IN ('2026-03-27', '2026-04-09')
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_by_slot_2025 AS(
  SELECT
    a.agent
    , a.date
    , a.slot_start
    , a.activity_type_required
    -- , CASE
    --     WHEN SUM(COALESCE(a.adherent_time_final, 0)) <= 1800 THEN SUM(COALESCE(a.adherent_time_final, 0))
    --     ELSE 1800
    --   END AS adherent_time_final
    , LEAST(COALESCE(SUM(a.adherent_time_final), 0), 1800) AS adherent_time_final
    , b.xplead
    , b.xforce
    , b.squad
    , b.squad_district
  FROM data_calculations AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
  WHERE (a.date <= '2025-11-05' OR a.date >= '2025-11-20') 
    AND a.date < '2025-12-01'
    AND a.date >= '2025-01-01'
    AND b.snapshot_month = '2025-12-01'
    AND b.status = 'active'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_by_slot AS(
  SELECT * FROM adherence_by_slot_2025
  UNION ALL
  SELECT * FROM adherence_by_slot_2026
);

CREATE OR REPLACE TEMPORARY VIEW adherence_final AS(
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date
    , SUM(adherent_time_final) AS delivered_hours
    , COUNT(DISTINCT slot_start) * 1800 AS required_hours
  FROM adherence_by_slot
  WHERE (COALESCE(adherent_time_final, 0) > 0 
    OR activity_type_required IS NOT NULL)
    GROUP BY ALL
);

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

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , DATE_TRUNC('QUARTER', date) AS date_reference
--     , 'quarter' AS date_granularity
--     , 'adherence_agent' AS metric
--     , SUM(delivered_hours) AS numerator
--     , SUM(required_hours) AS denominator
--     , TRY_DIVIDE(SUM(delivered_hours) , SUM(required_hours)) *100 AS metric_value
--   FROM adherence_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , CASE
--         WHEN date < '2026-07-01' THEN '2026-01-01'
--         WHEN date >= '2026-07-01' THEN '2026-07-01'
--         ELSE NULL
--       END AS date_reference
--     , 'semester' AS date_granularity
--     , 'adherence_agent' AS metric
--     , SUM(delivered_hours) AS numerator
--     , SUM(required_hours) AS denominator
--     , TRY_DIVIDE(SUM(delivered_hours) , SUM(required_hours)) *100 AS metric_value
--   FROM adherence_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , DATE_TRUNC('YEAR', date) AS date_reference
--     , 'year' AS date_granularity
--     , 'adherence_agent' AS metric
--     , SUM(delivered_hours) AS numerator
--     , SUM(required_hours) AS denominator
--     , TRY_DIVIDE(SUM(delivered_hours) , SUM(required_hours)) *100 AS metric_value
--   FROM adherence_final
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW adherence_agents AS (
  SELECT * FROM adherence_agents_daily
  UNION ALL
  SELECT * FROM adherence_agents_weekly
  UNION ALL
  SELECT * FROM adherence_agents_monthly
  -- UNION ALL
  -- SELECT * FROM adherence_agents_quarterly
  -- UNION ALL
  -- SELECT * FROM adherence_agents_semesterly
  -- UNION ALL
  -- SELECT * FROM adherence_agents_yearly
);

-- SELECT * FROM adherence_agents

-- COMMAND ----------

-- DBTITLE 1,Adherence Agents General Quartile Calculations
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

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_general_quartile_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM adherence_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_general_quartile_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM adherence_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_general_quartile_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM adherence_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW adherence_agents_general_quartile AS (
  SELECT * FROM adherence_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM adherence_agents_general_quartile_weekly
--   UNION ALL
--   SELECT * FROM adherence_agents_general_quartile_semesterly
--   UNION ALL
--   SELECT * FROM adherence_agents_general_quartile_yearly
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
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM adherence_agents_weekly
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_team_quartile_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM adherence_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_team_quartile_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM adherence_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_agents_team_quartile_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM adherence_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW adherence_agents_team_quartile AS (
  SELECT * FROM adherence_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM adherence_agents_team_quartile_weekly
--   UNION ALL
--   SELECT * FROM adherence_agents_team_quartile_semesterly
--   UNION ALL
--   SELECT * FROM adherence_agents_team_quartile_yearly
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

-- CREATE OR REPLACE TEMPORARY VIEW adherence_xforces_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM adherence_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_xforces_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM adherence_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_xforces_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM adherence_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW adherence_xforces AS (
  SELECT * FROM adherence_xforces_monthly
  UNION ALL
  SELECT * FROM adherence_xforces_weekly
--   UNION ALL
--   SELECT * FROM adherence_xforces_semesterly
--   UNION ALL
--   SELECT * FROM adherence_xforces_yearly
);

-- SELECT * FROM adherence_xforces

-- COMMAND ----------

-- DBTITLE 1,Adherence XPLeads Calculations
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


-- CREATE OR REPLACE TEMPORARY VIEW adherence_xpleads_quarterly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM adherence_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_xpleads_semesterly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM adherence_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW adherence_xpleads_yearly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'adherence_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM adherence_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW adherence_xpleads AS (
  SELECT * FROM adherence_xpleads_monthly
  UNION ALL
  SELECT * FROM adherence_xpleads_weekly
--   UNION ALL
--   SELECT * FROM adherence_xpleads_semesterly
--   UNION ALL
--   SELECT * FROM adherence_xpleads_yearly
);

-- SELECT * FROM adherence_xpleads

-- COMMAND ----------

-- DBTITLE 1,Adherence Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW adherence_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'adherence_squad' AS metric
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

CREATE OR REPLACE TEMPORARY VIEW adherence_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'adherence_squad' AS metric
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
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'adherence_district' AS metric
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

CREATE OR REPLACE TEMPORARY VIEW adherence_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'adherence_district' AS metric
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

CREATE OR REPLACE TEMPORARY VIEW adherence_district AS (
  SELECT * FROM adherence_district_monthly
  UNION ALL
  SELECT * FROM adherence_district_weekly
);

-- SELECT * FROM adherence_district

-- COMMAND ----------

-- DBTITLE 1,Adherence  Dataset
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
  UNION ALL
  SELECT * FROM adherence_squad
  UNION ALL
  SELECT * FROM adherence_district
);

-- SELECT * FROM adherence

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: adherence
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_adherence AS SELECT * FROM adherence;
CREATE OR REPLACE TEMPORARY VIEW adherence AS SELECT * FROM usr.danielanzures.sm_temp_adherence;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Normalized Occupancy

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Base
CREATE OR REPLACE TEMPORARY VIEW jobs_base AS(
  SELECT
    *
    , DATE(case_assignment_time) AS date
    , unix_timestamp(TO_TIMESTAMP(case_assignment_time)) AS activity_start
    , unix_timestamp(TO_TIMESTAMP(case_unassignment_time)) AS activity_end
    , REGEXP_EXTRACT(agent_email_id, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent_name_extracted
    , 'oos' AS activity_type
  FROM usr.sprinklr_api_data_integration.sprinklr_normalized_occupancy_data
);

CREATE OR REPLACE TEMPORARY VIEW dime_table_occupancy AS(
  SELECT
    agent
    , agent_dime_squad AS squad
    , dime_date AS date
    , REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)  AS agent_name_extracted
    , unix_timestamp(local_timestamp_dime_slot_starts_at) AS slot_start
    , unix_timestamp(local_timestamp_dime_slot_starts_at) + (30 * 60) AS slot_end
    , activity_type_required
    , dimensioned_activity
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE
      affiliation = 'nubank'
      AND dime_date >= '2024-12-30'
      AND activity_type_required IS NOT NULL
      AND activity_type_required NOT IN ('lunch_break', 'dime_invalid_notation', 'time_off')
      AND agent_dime_squad IN ('social', 'social_social')
      AND dime_date <= DATE_SUB(DATE_TRUNC('WEEK', CURRENT_DATE()), 1)
);

CREATE OR REPLACE TEMPORARY VIEW dime_occupancy AS(
  SELECT
    REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
    , squad
    , DATE(date) AS date
    , slot_start
    , slot_end
    , activity_type_required
    , dimensioned_activity
  FROM dime_table_occupancy
  WHERE activity_type_required NOT IN ('time_off', 'shrinkage')
);

CREATE OR REPLACE TEMPORARY VIEW slot_jobs AS(
  SELECT
    a.agent
    , a.squad
    , a.date
    , a.slot_start
    , a.slot_end
    , a.activity_type_required
    , b.activity_type AS job_activity_type
    , b.activity_start AS job_start
    , b.activity_end AS job_end
  FROM dime_occupancy AS a
  LEFT JOIN jobs_base AS b
    ON a.agent = b.agent_name_extracted
    AND a.date = b.date
    AND ((b.activity_start >= a.slot_start AND b.activity_start < a.slot_end)
      OR (b.activity_end > a.slot_start AND b.activity_end <= a.slot_end)
      OR (b.activity_start < a.slot_start AND b.activity_end >= a.slot_end))
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_base AS(
  SELECT
    *
    , CASE 
        WHEN activity_type_required = job_activity_type
          THEN 1
          ELSE 0
        END AS activity_occuped
    , CASE
        WHEN job_start >= slot_start AND job_end <= slot_end
          THEN job_end - job_start
        WHEN job_start < slot_start AND job_end <= slot_end
          THEN job_end - slot_start
        WHEN job_start >= slot_start AND job_end > slot_end
          THEN slot_end - job_start
        WHEN job_start < slot_start AND job_end > slot_end
          THEN slot_end - slot_start
        END AS duration
  FROM slot_jobs
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_agg AS(
  SELECT
    REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) as agent
    , squad AS old_squad
    , date
    , slot_start
    , SUM(CASE WHEN activity_occuped = 1 THEN duration END) AS occupancy_time
    , 1800 AS slot_duration
    , activity_type_required
    , job_activity_type
  FROM occupancy_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_agents_information_2026 AS (
  SELECT
    a.*
    , b.xplead
    , b.xforce
    , b.squad_district
    , b.squad
    , b.shift
  FROM occupancy_agg AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
  WHERE b.status = 'active'
    AND a.date >= '2025-12-01'
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_agents_information_2025 AS (
  SELECT
    a.*
    , b.xplead
    , b.xforce
    , b.squad_district
    , b.squad
    , b.shift
  FROM occupancy_agg AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
  WHERE b.status = 'active'
    AND a.date < '2025-12-01'
    AND a.date >= '2025-01-01'
    AND b.snapshot_month = '2025-12-01'
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_agents_information AS (
  SELECT * FROM occupancy_agents_information_2025
  UNION ALL
  SELECT * FROM occupancy_agents_information_2026
);

CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy_benchmark AS (
  SELECT
    DATE_TRUNC('MONTH', date) AS month
    , squad_district
    , shift
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS occupancy_monthly
  FROM (
    SELECT
      agent
      , squad_district
      , slot_start
      , shift
      , date
      , CASE WHEN SUM(occupancy_time) <= 1800 THEN SUM(occupancy_time) ELSE 1800 END AS occupancy_time
      , SUM(slot_duration) AS job_time
    FROM occupancy_agents_information
    GROUP BY ALL
  )
  WHERE date != '2026-03-27' -- deleting data with general access problems
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy AS (
  SELECT
    a.*
    , AVG(b.occupancy_monthly) AS occupancy_exp
  FROM occupancy_agents_information AS a
  LEFT JOIN normalized_occupancy_benchmark AS b
    ON a.squad_district = b.squad_district
    AND a.shift = b.shift
    -- AND DATE_TRUNC('MONTH', a.date) >= b.month 
    -- AND DATE_TRUNC('MONTH', a.date) <= b.month + INTERVAL 4 MONTHS
    AND DATE_TRUNC('MONTH', a.date) = b.month 
  WHERE b.month >= '2026-01-01'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy_final AS(
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , slot_start
    , shift
    , date
    , CASE WHEN SUM(occupancy_time) <= 1800 THEN SUM(occupancy_time) ELSE 1800 END AS occupancy_time
    , SUM(slot_duration) AS job_time
    , occupancy_exp AS occupancy_exp
  FROM normalized_occupancy
  WHERE date >= '2026-03-01'
    AND date != '2026-03-27' -- deleting data with general access problems
  GROUP BY ALL
);

-- SELECT * FROM normalized_occupancy_final

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Base
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW jobs_base AS(
-- MAGIC   SELECT
-- MAGIC     *
-- MAGIC     , Case_Assignment_Time AS date
-- MAGIC     , unix_timestamp(TO_TIMESTAMP(Case_Assignment_Time)) AS activity_start
-- MAGIC     , unix_timestamp(TO_TIMESTAMP(Case_Assignment_Time + (
-- MAGIC         CAST(split(Case_User_SLA_SUM, ':')[0] AS INT) * 3600 + 
-- MAGIC         CAST(split(Case_User_SLA_SUM, ':')[1] AS INT) * 60 +  
-- MAGIC         CAST(split(Case_User_SLA_SUM, ':')[2] AS INT)         
-- MAGIC     ) * INTERVAL 1 SECOND)) AS activity_end
-- MAGIC     , REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent_name
-- MAGIC     , 'oos' AS activity_type
-- MAGIC   FROM gsheets.sheets.mx_time_spent_social_media
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW dime_table_occupancy AS(
-- MAGIC   SELECT
-- MAGIC     agent
-- MAGIC     , agent_dime_squad AS squad
-- MAGIC     , dime_date AS date
-- MAGIC     , REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)  AS agent_name_extracted
-- MAGIC     , unix_timestamp(local_timestamp_dime_slot_starts_at) AS slot_start
-- MAGIC     , unix_timestamp(local_timestamp_dime_slot_starts_at) + (30 * 60) AS slot_end
-- MAGIC     , activity_type_required
-- MAGIC     , dimensioned_activity
-- MAGIC   FROM etl.mx__series_contract.agent_dimensioned_activities
-- MAGIC   WHERE
-- MAGIC       affiliation = 'nubank'
-- MAGIC       AND dime_date >= '2024-12-30'
-- MAGIC       AND activity_type_required IS NOT NULL
-- MAGIC       AND activity_type_required NOT IN ('lunch_break', 'dime_invalid_notation', 'time_off')
-- MAGIC       AND agent_dime_squad = 'social'
-- MAGIC       AND dime_date <= DATE_SUB(DATE_TRUNC('WEEK', CURRENT_DATE()), 1)
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW dime_occupancy AS(
-- MAGIC   SELECT
-- MAGIC     REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
-- MAGIC     , squad
-- MAGIC     , DATE(date) AS date
-- MAGIC     , slot_start
-- MAGIC     , slot_end
-- MAGIC     , activity_type_required
-- MAGIC     , dimensioned_activity
-- MAGIC   FROM dime_table_occupancy
-- MAGIC   WHERE activity_type_required NOT IN ('time_off', 'shrinkage')
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW slot_jobs AS(
-- MAGIC   SELECT
-- MAGIC     a.agent
-- MAGIC     , a.squad
-- MAGIC     , a.date
-- MAGIC     , a.slot_start
-- MAGIC     , a.slot_end
-- MAGIC     , a.activity_type_required
-- MAGIC     , b.activity_type AS job_activity_type
-- MAGIC     , b.activity_start AS job_start
-- MAGIC     , b.activity_end AS job_end
-- MAGIC   FROM dime_occupancy AS a
-- MAGIC   LEFT JOIN jobs_base AS b
-- MAGIC     ON a.agent = b.agent_name
-- MAGIC     AND a.date = b.date
-- MAGIC     AND ((b.activity_start >= a.slot_start AND b.activity_start < a.slot_end)
-- MAGIC       OR (b.activity_end > a.slot_start AND b.activity_end <= a.slot_end)
-- MAGIC       OR (b.activity_start < a.slot_start AND b.activity_end >= a.slot_end))
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW occupancy_base AS(
-- MAGIC   SELECT
-- MAGIC     *
-- MAGIC     , CASE 
-- MAGIC         WHEN activity_type_required = job_activity_type
-- MAGIC           THEN 1
-- MAGIC           ELSE 0
-- MAGIC         END AS activity_occuped
-- MAGIC     , CASE
-- MAGIC         WHEN job_start >= slot_start AND job_end <= slot_end
-- MAGIC           THEN job_end - job_start
-- MAGIC         WHEN job_start < slot_start AND job_end <= slot_end
-- MAGIC           THEN job_end - slot_start
-- MAGIC         WHEN job_start >= slot_start AND job_end > slot_end
-- MAGIC           THEN slot_end - job_start
-- MAGIC         WHEN job_start < slot_start AND job_end > slot_end
-- MAGIC           THEN slot_end - slot_start
-- MAGIC         END AS duration
-- MAGIC   FROM slot_jobs
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW occupancy_agg AS(
-- MAGIC   SELECT
-- MAGIC     REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) as agent
-- MAGIC     , squad AS old_squad
-- MAGIC     , date
-- MAGIC     , slot_start
-- MAGIC     , SUM(CASE WHEN activity_occuped = 1 THEN duration END) AS occupancy_time
-- MAGIC     , 1800 AS slot_duration
-- MAGIC     , activity_type_required
-- MAGIC     , job_activity_type
-- MAGIC   FROM occupancy_base
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW occupancy_agents_information_2026 AS (
-- MAGIC   SELECT
-- MAGIC     a.*
-- MAGIC     , b.xplead
-- MAGIC     , b.xforce
-- MAGIC     , b.squad_district
-- MAGIC     , b.squad
-- MAGIC     , b.shift
-- MAGIC   FROM occupancy_agg AS a
-- MAGIC   LEFT JOIN agent_information AS b
-- MAGIC     ON a.agent = b.agent
-- MAGIC     AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
-- MAGIC   WHERE b.status = 'active'
-- MAGIC     AND a.date >= '2025-12-01'
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW occupancy_agents_information_2025 AS (
-- MAGIC   SELECT
-- MAGIC     a.*
-- MAGIC     , b.xplead
-- MAGIC     , b.xforce
-- MAGIC     , b.squad_district
-- MAGIC     , b.squad
-- MAGIC     , b.shift
-- MAGIC   FROM occupancy_agg AS a
-- MAGIC   LEFT JOIN agent_information AS b
-- MAGIC     ON a.agent = b.agent
-- MAGIC   WHERE b.status = 'active'
-- MAGIC     AND a.date < '2025-12-01'
-- MAGIC     AND a.date >= '2025-01-01'
-- MAGIC     AND b.snapshot_month = '2025-12-01'
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW occupancy_agents_information AS (
-- MAGIC   SELECT * FROM occupancy_agents_information_2025
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM occupancy_agents_information_2026
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy_benchmark AS (
-- MAGIC   SELECT
-- MAGIC     DATE_TRUNC('MONTH', date) AS month
-- MAGIC     , squad_district
-- MAGIC     , shift
-- MAGIC     , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS occupancy_monthly
-- MAGIC   FROM (
-- MAGIC     SELECT
-- MAGIC       agent
-- MAGIC       , squad_district
-- MAGIC       , slot_start
-- MAGIC       , shift
-- MAGIC       , date
-- MAGIC       , CASE WHEN SUM(occupancy_time) <= 1800 THEN SUM(occupancy_time) ELSE 1800 END AS occupancy_time
-- MAGIC       , SUM(slot_duration) AS job_time
-- MAGIC     FROM occupancy_agents_information
-- MAGIC     GROUP BY ALL
-- MAGIC   )
-- MAGIC   WHERE date != '2026-03-27' -- deleting data with general access problems
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy AS (
-- MAGIC   SELECT
-- MAGIC     a.*
-- MAGIC     , AVG(b.occupancy_monthly) AS occupancy_exp
-- MAGIC   FROM occupancy_agents_information AS a
-- MAGIC   LEFT JOIN normalized_occupancy_benchmark AS b
-- MAGIC     ON a.squad_district = b.squad_district
-- MAGIC     AND a.shift = b.shift
-- MAGIC     -- AND DATE_TRUNC('MONTH', a.date) >= b.month 
-- MAGIC     -- AND DATE_TRUNC('MONTH', a.date) <= b.month + INTERVAL 4 MONTHS
-- MAGIC     AND DATE_TRUNC('MONTH', a.date) = b.month 
-- MAGIC   WHERE b.month >= '2026-01-01'
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy_final AS(
-- MAGIC   SELECT
-- MAGIC     agent
-- MAGIC     , xforce
-- MAGIC     , xplead
-- MAGIC     , squad
-- MAGIC     , squad_district
-- MAGIC     , slot_start
-- MAGIC     , shift
-- MAGIC     , date
-- MAGIC     , CASE WHEN SUM(occupancy_time) <= 1800 THEN SUM(occupancy_time) ELSE 1800 END AS occupancy_time
-- MAGIC     , SUM(slot_duration) AS job_time
-- MAGIC     , occupancy_exp AS occupancy_exp
-- MAGIC   FROM normalized_occupancy
-- MAGIC   WHERE date >= '2026-03-01'
-- MAGIC     AND date != '2026-03-27' -- deleting data with general access problems
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC -- SELECT * FROM normalized_occupancy_final

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
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
    , MAX(occupancy_exp) AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM normalized_occupancy_final
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'nocc_agent' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
    , MAX(occupancy_exp) AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM normalized_occupancy_final
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'nocc_agent' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
    , MAX(occupancy_exp) AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM normalized_occupancy_final
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , DATE_TRUNC('QUARTER', date) AS date_reference
--     , 'quarter' AS date_granularity
--     , 'nocc_agent' AS metric
--     , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
--     , MAX(occupancy_exp) AS denominator
--     , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
--   FROM normalized_occupancy_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , CASE
--         WHEN date < '2026-07-01' THEN '2026-01-01'
--         WHEN date >= '2026-07-01' THEN '2026-07-01'
--         ELSE NULL
--       END AS date_reference
--     , 'semester' AS date_granularity
--     , 'nocc_agent' AS metric
--     , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
--     , MAX(occupancy_exp) AS denominator
--     , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
--   FROM normalized_occupancy_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , DATE_TRUNC('YEAR', date) AS date_reference
--     , 'year' AS date_granularity
--     , 'nocc_agent' AS metric
--     , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
--     , MAX(occupancy_exp) AS denominator
--     , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
--   FROM normalized_occupancy_final
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW nocc_agents AS (
  SELECT * FROM nocc_agents_daily
  UNION ALL
  SELECT * FROM nocc_agents_weekly
  UNION ALL
  SELECT * FROM nocc_agents_monthly
--   UNION ALL
--   SELECT * FROM nocc_agents_quarterly
--   UNION ALL
--   SELECT * FROM nocc_agents_semesterly
--   UNION ALL
--   SELECT * FROM nocc_agents_yearly
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

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_general_quartile_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM nocc_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_general_quartile_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM nocc_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_general_quartile_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM nocc_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_general_quartile AS (
  SELECT * FROM nocc_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM nocc_agents_general_quartile_weekly
--   UNION ALL
--   SELECT * FROM nocc_agents_general_quartile_semesterly
--   UNION ALL
--   SELECT * FROM nocc_agents_general_quartile_yearly
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
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM nocc_agents_weekly
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_team_quartile_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM nocc_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_team_quartile_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM nocc_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_agents_team_quartile_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM nocc_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW nocc_agents_team_quartile AS (
  SELECT * FROM nocc_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM nocc_agents_team_quartile_weekly
--   UNION ALL
--   SELECT * FROM nocc_agents_team_quartile_semesterly
--   UNION ALL
--   SELECT * FROM nocc_agents_team_quartile_yearly
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

-- CREATE OR REPLACE TEMPORARY VIEW nocc_xforces_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM nocc_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_xforces_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM nocc_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_xforces_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM nocc_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW nocc_xforces AS (
  SELECT * FROM nocc_xforces_monthly
  UNION ALL
  SELECT * FROM nocc_xforces_weekly
--   UNION ALL
--   SELECT * FROM nocc_xforces_semesterly
--   UNION ALL
--   SELECT * FROM nocc_xforces_yearly
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

-- CREATE OR REPLACE TEMPORARY VIEW nocc_xpleads_quarterly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM nocc_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_xpleads_semesterly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM nocc_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nocc_xpleads_yearly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'nocc_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM nocc_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW nocc_xpleads AS (
  SELECT * FROM nocc_xpleads_monthly
  UNION ALL
  SELECT * FROM nocc_xpleads_weekly
--   UNION ALL
--   SELECT * FROM nocc_xpleads_semesterly
--   UNION ALL
--   SELECT * FROM nocc_xpleads_yearly
);

-- SELECT * FROM nocc_xpleads

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW nocc_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'nocc_squad' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
    , MAX(occupancy_exp) AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM normalized_occupancy_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'nocc_squad' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
    , MAX(occupancy_exp) AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM normalized_occupancy_final
    GROUP BY ALL)
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
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'nocc_district' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
    , MAX(occupancy_exp) AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM normalized_occupancy_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'nocc_district' AS metric
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS numerator
    , MAX(occupancy_exp) AS denominator
    , TRY_DIVIDE(TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) , MAX(occupancy_exp)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM normalized_occupancy_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW nocc_district AS (
  SELECT * FROM nocc_district_monthly
  UNION ALL
  SELECT * FROM nocc_district_weekly
);

-- SELECT * FROM nocc_district

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Dataset
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
  UNION ALL
  SELECT * FROM nocc_squad
  UNION ALL
  SELECT * FROM nocc_district
);

-- SELECT * FROM nocc

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: nocc
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_nocc AS SELECT * FROM nocc;
CREATE OR REPLACE TEMPORARY VIEW nocc AS SELECT * FROM usr.danielanzures.sm_temp_nocc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## tNPS

-- COMMAND ----------

-- DBTITLE 1,tNPS Base
CREATE OR REPLACE TEMPORARY VIEW tnps_initial_base AS (
  SELECT
    *
    , REGEXP_EXTRACT(agent_email_id, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent_name
  FROM usr.sprinklr_api_data_integration.sprinklr_tnps_data
);

CREATE OR REPLACE TEMPORARY VIEW tnps_base_classification AS (
  SELECT
    *
    , CASE
        WHEN survey_answer_score >= 9 THEN 'promoter'
        WHEN survey_answer_score <= 6 THEN 'detractor'
        WHEN survey_answer_score > 6 AND survey_answer_score < 9 THEN 'neutral'
        ELSE NULL
      END AS evaluation_classification
    , CASE
        WHEN survey_answer_score IS NOT NULL THEN 1
        ELSE 0
      END AS evaluation_validation
    FROM tnps_initial_base
    WHERE survey_response_date <= case_closure_time + INTERVAL 1 DAY
);

CREATE OR REPLACE TEMPORARY VIEW tnps_base AS (
  SELECT
    DATE_TRUNC('DAY', case_closure_time) AS date
    , agent_name AS agent
    , COUNT(DISTINCT CASE WHEN evaluation_classification = 'promoter' THEN case_number END) - COUNT(DISTINCT CASE WHEN evaluation_classification = 'detractor' THEN case_number END) AS numerator
    , COUNT(DISTINCT CASE WHEN evaluation_validation = 1 THEN case_number END) AS denominator
  FROM tnps_base_classification
  WHERE DATE_TRUNC('DAY', case_closure_time) != '2026-03-27' -- deleting data with general access problems
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_base_2026 AS (
  SELECT
    a.*
    , b.xplead
    , b.xforce
    , b.squad
    , b.squad_district
  FROM tnps_base AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
  WHERE a.date >= '2025-12-01'
    AND b.status = 'active'
);

CREATE OR REPLACE TEMPORARY VIEW tnps_base_2025 AS (
  SELECT
    a.*
    , b.xplead
    , b.xforce
    , b.squad
    , b.squad_district
  FROM tnps_base AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
  WHERE a.date < '2025-12-01'
    AND a.date >= '2025-01-01'
    AND b.status = 'active'
    AND b.snapshot_month = '2025-12-01'
);

CREATE OR REPLACE TEMPORARY VIEW tnps_final_base AS(
  SELECT * FROM tnps_base_2025
  UNION ALL
  SELECT * FROM tnps_base_2026
);

SELECT * FROM tnps_final_base

-- COMMAND ----------

-- DBTITLE 1,tNPS Base
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW tnps_initial_base AS (
-- MAGIC   SELECT
-- MAGIC     *
-- MAGIC     , REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent_name
-- MAGIC   FROM gsheets.sheets.mx_tnps_social_media
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW tnps_base_classification AS (
-- MAGIC   SELECT
-- MAGIC     *
-- MAGIC     , CASE
-- MAGIC         WHEN survey_tnps >= 9 THEN 'promoter'
-- MAGIC         WHEN survey_tnps <= 6 THEN 'detractor'
-- MAGIC         WHEN survey_tnps > 6 AND survey_tnps < 9 THEN 'neutral'
-- MAGIC         ELSE NULL
-- MAGIC       END AS evaluation_classification
-- MAGIC     , CASE
-- MAGIC         WHEN survey_tnps IS NOT NULL THEN 1
-- MAGIC         ELSE 0
-- MAGIC       END AS evaluation_validation
-- MAGIC     FROM tnps_initial_base
-- MAGIC     WHERE survey_response_date <= ticket_close_date + INTERVAL 1 DAY
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW tnps_base AS (
-- MAGIC   SELECT
-- MAGIC     DATE_TRUNC('DAY', ticket_close_date) AS date
-- MAGIC     , agent_name AS agent
-- MAGIC     , COUNT(DISTINCT CASE WHEN evaluation_classification = 'promoter' THEN case_id END) - COUNT(DISTINCT CASE WHEN evaluation_classification = 'detractor' THEN case_id END) AS numerator
-- MAGIC     , COUNT(DISTINCT CASE WHEN evaluation_validation = 1 THEN case_id END) AS denominator
-- MAGIC   FROM tnps_base_classification
-- MAGIC   WHERE DATE_TRUNC('DAY', ticket_close_date) != '2026-03-27' -- deleting data with general access problems
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW tnps_base_2026 AS (
-- MAGIC   SELECT
-- MAGIC     a.*
-- MAGIC     , b.xplead
-- MAGIC     , b.xforce
-- MAGIC     , b.squad
-- MAGIC     , b.squad_district
-- MAGIC   FROM tnps_base AS a
-- MAGIC   LEFT JOIN agent_information AS b
-- MAGIC     ON a.agent = b.agent
-- MAGIC     AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
-- MAGIC   WHERE a.date >= '2025-12-01'
-- MAGIC     AND b.status = 'active'
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW tnps_base_2025 AS (
-- MAGIC   SELECT
-- MAGIC     a.*
-- MAGIC     , b.xplead
-- MAGIC     , b.xforce
-- MAGIC     , b.squad
-- MAGIC     , b.squad_district
-- MAGIC   FROM tnps_base AS a
-- MAGIC   LEFT JOIN agent_information AS b
-- MAGIC     ON a.agent = b.agent
-- MAGIC   WHERE a.date < '2025-12-01'
-- MAGIC     AND a.date >= '2025-01-01'
-- MAGIC     AND b.status = 'active'
-- MAGIC     AND b.snapshot_month = '2025-12-01'
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW tnps_final_base AS(
-- MAGIC   SELECT * FROM tnps_base_2025
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM tnps_base_2026
-- MAGIC );
-- MAGIC
-- MAGIC -- SELECT * FROM tnps_final_base

-- COMMAND ----------

-- DBTITLE 1,tNPS Agents Calculation
CREATE OR REPLACE TEMPORARY VIEW tnps_agents_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'tnps_agent' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM tnps_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_agents_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'tnps_agent' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM tnps_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_agents_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date AS date_reference
    , 'day' AS date_granularity
    , 'tnps_agent' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM tnps_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_agents AS (
  SELECT * FROM tnps_agents_daily
  UNION ALL
  SELECT * FROM tnps_agents_weekly
  UNION ALL
  SELECT * FROM tnps_agents_monthly
);

-- SELECT * FROM tnps_agents

-- COMMAND ----------

-- DBTITLE 1,tNPS Agents Team Quartile Calculations
CREATE OR REPLACE TEMPORARY VIEW tnps_agents_team_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'tnps_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM tnps_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_agents_team_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'tnps_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM tnps_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_agents_team_quartile AS (
  SELECT * FROM tnps_agents_team_quartile_weekly
  UNION ALL
  SELECT * FROM tnps_agents_team_quartile_monthly
);

-- SELECT * FROM tnps_agents_team_quartile_monthly

-- COMMAND ----------

-- DBTITLE 1,tNPS XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW tnps_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'tnps_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 88 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 88 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM tnps_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_xforces_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'tnps_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 88 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 88 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM tnps_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_xforces AS (
  SELECT * FROM tnps_xforces_weekly
  UNION ALL
  SELECT * FROM tnps_xforces_monthly
);

-- SELECT * FROM tnps_xforces_monthly

-- COMMAND ----------

-- DBTITLE 1,tNPS XPLeads Calculations
CREATE OR REPLACE TEMPORARY VIEW tnps_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'tnps_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 88 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 88 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM tnps_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'tnps_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 88 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 88 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM tnps_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_xpleads AS (
  SELECT * FROM tnps_xpleads_weekly
  UNION ALL
  SELECT * FROM tnps_xpleads_monthly
);

-- SELECT * FROM tnps_xpleads_monthly

-- COMMAND ----------

-- DBTITLE 1,tNPS Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW tnps_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'tnps_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM tnps_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'tnps_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM tnps_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_squad AS (
  SELECT * FROM tnps_squad_monthly
  UNION ALL
  SELECT * FROM tnps_squad_weekly
);

-- SELECT * FROM tnps_squad

-- COMMAND ----------

-- DBTITLE 1,tNPS District Calculations
CREATE OR REPLACE TEMPORARY VIEW tnps_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'tnps_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM tnps_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'tnps_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator) , SUM(denominator)) *100 AS metric_value
  FROM tnps_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW tnps_district AS (
  SELECT * FROM tnps_district_monthly
  UNION ALL
  SELECT * FROM tnps_district_weekly
);

-- SELECT * FROM tnps_district

-- COMMAND ----------

-- DBTITLE 1,tNPS Dataset
CREATE OR REPLACE TEMPORARY VIEW tnps AS (
  SELECT * FROM tnps_agents
  UNION ALL
  SELECT * FROM tnps_agents_team_quartile
  UNION ALL
  SELECT * FROM tnps_xforces
  UNION ALL
  SELECT * FROM tnps_xpleads
  UNION ALL
  SELECT * FROM tnps_squad
  UNION ALL
  SELECT * FROM tnps_district
);

-- SELECT * FROM tnps

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: tnps
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_tnps AS SELECT * FROM tnps;
CREATE OR REPLACE TEMPORARY VIEW tnps AS SELECT * FROM usr.danielanzures.sm_temp_tnps;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## WoWs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## !! Change temporary fix: gsheets.sheets.mx_wows_social_media

-- COMMAND ----------

-- DBTITLE 1,WOWs Base
CREATE OR REPLACE TEMPORARY VIEW wows_initial_base AS (
  SELECT 
    * 
    , REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent_name
  FROM gsheets.sheets.mx_wows_daniel_temp
  WHERE date != ''
);

CREATE OR REPLACE TEMPORARY VIEW wows_base AS (
  SELECT
    DATE_TRUNC('DAY', date) AS date
    , agent_name AS agent
    , COUNT(DISTINCT case_id) AS wows
    , 5 AS monthly_target
  FROM wows_initial_base
  WHERE DATE_TRUNC('DAY', date) != '2026-03-27' -- deleting data with general access problems
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_base_2026 AS (
  SELECT
    a.*
    , b.xplead
    , b.xforce
    , b.squad
    , b.squad_district
  FROM wows_base AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
  WHERE a.date >= '2025-12-01'
    AND b.status = 'active'
);

CREATE OR REPLACE TEMPORARY VIEW wows_base_2025 AS (
  SELECT
    a.*
    , b.xplead
    , b.xforce
    , b.squad
    , b.squad_district
  FROM wows_base AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
  WHERE DATE_TRUNC('MONTH', a.date) < '2025-12-01'
    AND DATE_TRUNC('MONTH', a.date) > '2025-01-01'
    AND b.status = 'active'
    AND b.snapshot_month = '2025-12-01'
);

CREATE OR REPLACE TEMPORARY VIEW wows_final_base AS(
  SELECT * FROM wows_base_2025
  UNION ALL
  SELECT * FROM wows_base_2026
); 

-- SELECT * FROM wows_final_base

-- COMMAND ----------

-- DBTITLE 1,WOWs Agent Calculation
CREATE OR REPLACE TEMPORARY VIEW wows_agents_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'wows_agent' AS metric
    , SUM(wows) AS numerator
    , MAX(monthly_target) AS denominator
    , SUM(wows) AS metric_value
  FROM wows_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_agents_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'wows_agent' AS metric
    , SUM(wows) AS numerator
    , MAX(monthly_target) AS denominator
    , SUM(wows) AS metric_value
  FROM wows_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_agents_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date AS date_reference
    , 'day' AS date_granularity
    , 'wows_agent' AS metric
    , SUM(wows) AS numerator
    , MAX(monthly_target) AS denominator
    , SUM(wows) AS metric_value
  FROM wows_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_agents AS (
  SELECT * FROM wows_agents_daily
  UNION ALL
  SELECT * FROM wows_agents_weekly
  UNION ALL
  SELECT * FROM wows_agents_monthly
);

-- SELECT * FROM wows_agents

-- COMMAND ----------

-- DBTITLE 1,WOWs Agents Team Quartile Calculation
CREATE OR REPLACE TEMPORARY VIEW wows_agents_team_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'wows_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM wows_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_agents_team_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'wows_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM wows_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_agents_team_quartile AS (
  SELECT * FROM wows_agents_team_quartile_weekly
  UNION ALL
  SELECT * FROM wows_agents_team_quartile_monthly
);

-- SELECT * FROM wows_agents_team_quartile

-- COMMAND ----------

-- DBTITLE 1,WOWs XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW wows_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'wows_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 5 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , COUNT(DISTINCT CASE WHEN metric_value >= 5 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
  FROM wows_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_xforces_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'wows_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 5 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , COUNT(DISTINCT CASE WHEN metric_value >= 5 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
  FROM wows_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_xforces AS (
  SELECT * FROM wows_xforces_weekly
  UNION ALL
  SELECT * FROM wows_xforces_monthly
);

-- SELECT * FROM wows_xforces_monthly

-- COMMAND ----------

-- DBTITLE 1,WOWs XPLeads Calculations
CREATE OR REPLACE TEMPORARY VIEW wows_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'wows_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 5 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , COUNT(DISTINCT CASE WHEN metric_value >= 5 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
  FROM wows_agents_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'wows_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 5 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , COUNT(DISTINCT CASE WHEN metric_value >= 5 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
  FROM wows_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_xpleads AS (
  SELECT * FROM wows_xpleads_weekly
  UNION ALL
  SELECT * FROM wows_xpleads_monthly
)

-- SELECT * FROM wows_xpleads

-- COMMAND ----------

-- DBTITLE 1,WOWs Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW wows_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'wows_squad' AS metric
    , SUM(wows) AS numerator
    , SUM(monthly_target) AS denominator
    , SUM(wows) AS metric_value
  FROM wows_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'wows_squad' AS metric
    , SUM(wows) AS numerator
    , SUM(monthly_target) AS denominator
    , SUM(wows) AS metric_value
  FROM wows_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_squad AS (
  SELECT * FROM wows_squad_monthly
  UNION ALL
  SELECT * FROM wows_squad_weekly
);

-- SELECT * FROM wows_squad

-- COMMAND ----------

-- DBTITLE 1,WOWs District Calculations
CREATE OR REPLACE TEMPORARY VIEW wows_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'wows_district' AS metric
    , SUM(wows) AS numerator
    , SUM(monthly_target) AS denominator
    , SUM(wows) AS metric_value
  FROM wows_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'wows_district' AS metric
    , SUM(wows) AS numerator
    , SUM(monthly_target) AS denominator
    , SUM(wows) AS metric_value
  FROM wows_final_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW wows_district AS (
  SELECT * FROM wows_district_monthly
  UNION ALL
  SELECT * FROM wows_district_weekly
);

-- SELECT * FROM wows_district

-- COMMAND ----------

-- DBTITLE 1,WOWs Dataset
CREATE OR REPLACE TEMPORARY VIEW wows AS (
  SELECT * FROM wows_agents
  UNION ALL
  SELECT * FROM wows_agents_team_quartile
  UNION ALL
  SELECT * FROM wows_xforces
  UNION ALL
  SELECT * FROM wows_xpleads
  UNION ALL
  SELECT * FROM wows_squad
  UNION ALL
  SELECT * FROM wows_district
);

-- SELECT * FROM wows

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: wows
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_wows AS SELECT * FROM wows;
CREATE OR REPLACE TEMPORARY VIEW wows AS SELECT * FROM usr.danielanzures.sm_temp_wows;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quality Metric

-- COMMAND ----------

-- DBTITLE 1,Quality Base
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
    AND scorecard__id NOT IN ("68def79b3f83da8cc9cb5299","6812b3e46abeabb0653d197e", "688017f4bb266bb43b6c9565", "68680819336107d9f140d1ce")
    AND evaluation__agent_email NOT LIKE '%consorcio%'
    AND evaluation__agent_email NOT LIKE '%conjur%'
    AND evaluation__id NOT IN ('68646ed2f093c149757ba038', '687704e7a077fb121012dd5d', '688017f4bb266bb43b6c9565', '68680819336107d9f140d1ce')
    AND LOWER(REGEXP_EXTRACT(evaluation__agent_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM social_agents)

    UNION ALL

  SELECT
    sm.report_date AS local_mx_evaluation__created_at
    , sm.checklist_modified_date AS local_mx_evaluation__updated_at
    , 'SprinklrScorecardV1' AS scorecard__id
    , TRY_CAST(sm.case_number AS STRING) AS evaluation__id
    , 'SM' AS evaluation__team_name
    , REGEXP_EXTRACT(sm_agent.user_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
    , sm.score_avg AS qa_score
    , 'nubank' AS affiliation
  FROM mx__series_contract.social_media_case_summary_information sm
  LEFT JOIN usr.mx__enablement.sprinklr_sm_users sm_agent ON sm.agent_name = sm_agent.user_name
  LEFT JOIN usr.mx__enablement.sprinklr_sm_users sm_monitor ON sm.auditor = sm_monitor.user_name
  WHERE sm.report_date >= "2026-05-01"
    AND sm_monitor.user_email NOT IN (CONCAT('testuser', '@', 'nu.com.mx'))
    AND LOWER(REGEXP_EXTRACT(sm_agent.user_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM social_agents)
);

CREATE OR REPLACE TEMPORARY VIEW qa_deduped AS (
  SELECT 
    *
    , ROW_NUMBER() OVER (PARTITION BY evaluation__id ORDER BY local_mx_evaluation__created_at DESC) AS rn
  FROM qa_base
  WHERE affiliation IS NOT NULL
    AND DATE_TRUNC('DAY', local_mx_evaluation__created_at) NOT IN ('2026-03-27', '2026-04-09') -- deleting data with general access problems
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
GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_score_base AS(
  SELECT * FROM qa_score_2025
  UNION ALL
  SELECT * FROM qa_score_2026
);

-- SELECT * FROM qa_score_base

-- COMMAND ----------

-- DBTITLE 1,QA Calculation
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

-- DBTITLE 1,QA General Quartile Calculation
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

-- SELECT * FROM qa_agents_general_quartile

-- COMMAND ----------

-- DBTITLE 1,QA Team Quartile Calculation
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
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM qa_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_agents_team_quartile AS (
  SELECT * FROM qa_agents_team_quartile_weekly
  UNION ALL
  SELECT * FROM qa_agents_team_quartile_monthly
);

-- SELECT * FROM qa_agents_team_quartile

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
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
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
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
  FROM qa_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_xforces AS (
  SELECT * FROM qa_xforces_monthly
  UNION ALL
  SELECT * FROM qa_xforces_weekly
)

-- SELECT * FROM qa_xforces

-- COMMAND ----------

-- DBTITLE 1,QA XPLead Calculations
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
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
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
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
  FROM qa_agents_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_xpleads AS (
  SELECT * FROM qa_xpleads_monthly
  UNION ALL
  SELECT * FROM qa_xpleads_weekly
);

-- SELECT * FROM qa_xpleads

-- COMMAND ----------

-- DBTITLE 1,QA Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'qa_squad' AS metric
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

CREATE OR REPLACE TEMPORARY VIEW qa_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'qa_squad' AS metric
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
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'qa_district' AS metric
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

CREATE OR REPLACE TEMPORARY VIEW qa_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'qa_district' AS metric
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

CREATE OR REPLACE TEMPORARY VIEW qa_district AS (
  SELECT * FROM qa_district_monthly
  UNION ALL
  SELECT * FROM qa_district_weekly
);

-- SELECT * FROM qa_district

-- COMMAND ----------

-- DBTITLE 1,QA Dataset
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
UNION ALL
SELECT * FROM qa_squad
UNION ALL
SELECT * FROM qa_district
);

-- SELECT * FROM quality

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: quality
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_quality AS SELECT * FROM quality;
CREATE OR REPLACE TEMPORARY VIEW quality AS SELECT * FROM usr.danielanzures.sm_temp_quality;

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
    , b.metric_value AS tnps
    , c.metric_value AS nocc
    , d.metric_value AS wows
    , e.metric_value AS quality
  FROM adherence AS a
  LEFT JOIN tnps AS b
    ON a.agent = b.agent
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'tnps_agent'
  LEFT JOIN nocc AS c
    ON a.agent = c.agent
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_agent'
  LEFT JOIN wows AS d
    ON a.agent = d.agent
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'wows_agent'
  LEFT JOIN quality AS e
    ON a.agent = e.agent
    AND a.date_reference = e.date_reference
    AND a.date_granularity = e.date_granularity
    AND e.metric = 'qa_score_agent'
  WHERE a.date_granularity IN ('month', 'week')
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
        WHEN wows >= 5 THEN 100
        WHEN wows < 5 THEN try_divide(wows, 5) * 100
        ELSE 0
      END AS wows
    , CASE
        WHEN nocc >= 100 THEN 100
        WHEN nocc <= 100 THEN nocc
        ELSE 0
      END AS nocc
    , tnps
    , quality
  FROM index_agents_base
  WHERE date_reference >= '2026-01-01'
);

-- SELECT * FROM index_agents_final

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: index_agents_final
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_index_agents_final AS SELECT * FROM index_agents_final;
CREATE OR REPLACE TEMPORARY VIEW index_agents_final AS SELECT * FROM usr.danielanzures.sm_temp_index_agents_final;

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
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' AND tnps IS NOT NULL
          THEN (adherence + tnps + wows) 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' AND tnps IS NULL
          THEN (adherence + wows)
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN (adherence + tnps + wows + quality)
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN (adherence + wows + quality)
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN (adherence + tnps + wows)
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NULL AND quality IS NULL
          THEN (adherence + wows)
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN (adherence + tnps + wows + quality + nocc)
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN (adherence + wows + quality + nocc)
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN (adherence + tnps + wows + nocc)
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NULL
          THEN (adherence + wows + nocc)
        ELSE (adherence + tnps + wows + quality + nocc) 
      END AS numerator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' AND tnps IS NOT NULL
          THEN 300 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' AND tnps IS NULL
          THEN 200
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN 400
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN 300
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN 300
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NULL AND quality IS NULL
          THEN 200
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN 500
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN 400
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN 400
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NULL
          THEN 300
        ELSE 500
      END AS denominator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' AND tnps IS NOT NULL
          THEN (adherence + tnps + wows) / 3
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' AND tnps IS NULL
          THEN (adherence + wows) / 2
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN (adherence + tnps + wows + quality) / 4
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN (adherence + wows + quality) / 3
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN (adherence + tnps + wows) / 3
        WHEN CAST(date_reference AS DATE) = DATE '2026-02-01' AND tnps IS NULL AND quality IS NULL
          THEN (adherence + wows) / 2
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN (adherence + tnps + wows + quality + nocc) / 5
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN (adherence + wows + quality + nocc) / 4
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN (adherence + tnps + wows + nocc) / 4
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NULL
          THEN (adherence + wows + nocc) / 3
        ELSE (adherence + tnps + wows + quality + nocc) / 5
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
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' AND tnps IS NOT NULL
          THEN (adherence + tnps + wows) 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' AND tnps IS NULL
          THEN (adherence + wows)
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN (adherence + tnps + wows + quality)
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NULL AND quality IS NOT NULL
          THEN (adherence + wows + quality)
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NOT NULL AND quality IS NULL
          THEN (adherence + tnps + wows)
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NULL AND quality IS NULL
          THEN (adherence + wows)
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN (adherence + tnps + wows + quality + nocc)
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN (adherence + wows + quality + nocc)
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN (adherence + tnps + wows + nocc)
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NULL
          THEN (adherence + wows + nocc)
        ELSE (adherence + tnps + wows + quality + nocc) 
      END AS numerator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' AND tnps IS NOT NULL
          THEN 300 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' AND tnps IS NULL
          THEN 200
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN 400
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NULL AND quality IS NOT NULL
          THEN 300
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NOT NULL AND quality IS NULL
          THEN 300
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NULL AND quality IS NULL
          THEN 200
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN 500
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN 400
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN 400
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NULL
          THEN 300
        ELSE 500
      END AS denominator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' AND tnps IS NOT NULL
          THEN (adherence + tnps + wows) / 3
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' AND tnps IS NULL
          THEN (adherence + wows) / 2
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN (adherence + tnps + wows + quality) / 4
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NULL AND quality IS NOT NULL
          THEN (adherence + wows + quality) / 3
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NOT NULL AND quality IS NULL
          THEN (adherence + tnps + wows) / 3
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' AND tnps IS NULL AND quality IS NULL
          THEN (adherence + wows) / 2
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NOT NULL
          THEN (adherence + tnps + wows + quality + nocc) / 5
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NOT NULL
          THEN (adherence + wows + quality + nocc) / 4
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NOT NULL AND quality IS NULL
          THEN (adherence + tnps + wows + nocc) / 4
        WHEN CAST(date_reference AS DATE) >= DATE '2026-03-01' AND tnps IS NULL AND quality IS NULL
          THEN (adherence + wows + nocc) / 3
        ELSE (adherence + tnps + wows + quality + nocc) / 5
      END AS metric_value
  FROM index_agents_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agent' AS metric
--     , (adherence + ntpj + nocc) AS numerator
--     , 300 AS denominator
--     , (adherence + ntpj + nocc) / 3 AS metric_value
--   FROM index_agents_final
--   WHERE date_granularity = 'quarter'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agent' AS metric
--     , (adherence + ntpj + nocc) AS numerator
--     , 300 AS denominator
--     , (adherence + ntpj + nocc) / 3 AS metric_value
--   FROM index_agents_final
--   WHERE date_granularity = 'semester'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agent' AS metric
--     , (adherence + ntpj + nocc) AS numerator
--     , 300 AS denominator
--     , (adherence + ntpj + nocc) / 3 AS metric_value
--   FROM index_agents_final
--   WHERE date_granularity = 'year'
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW index_agents AS (
  SELECT * FROM index_agents_monthly
  UNION ALL
  SELECT * FROM index_agents_weekly
--   UNION ALL
--   SELECT * FROM index_agents_semesterly
--   UNION ALL
--   SELECT * FROM index_agents_yearly
);

-- SELECT * FROM index_agents_monthly

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: index_agents
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.sm_temp_index_agents AS SELECT * FROM index_agents;
CREATE OR REPLACE TEMPORARY VIEW index_agents AS SELECT * FROM usr.danielanzures.sm_temp_index_agents;

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

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_general_quartile_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM index_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_general_quartile_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM index_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_general_quartile_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM index_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW index_agents_general_quartile AS (
  SELECT * FROM index_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM index_agents_general_quartile_weekly
--   UNION ALL
--   SELECT * FROM index_agents_general_quartile_semesterly
--   UNION ALL
--   SELECT * FROM index_agents_general_quartile_yearly
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
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM index_agents_weekly
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_team_quartile_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM index_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_team_quartile_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM index_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW index_agents_team_quartile_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM index_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW index_agents_team_quartile AS (
  SELECT * FROM index_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM index_agents_team_quartile_weekly
--   UNION ALL
--   SELECT * FROM index_agents_team_quartile_semesterly
--   UNION ALL
--   SELECT * FROM index_agents_team_quartile_yearly
);

-- SELECT * FROM index_agents_team_quartile

-- COMMAND ----------

-- DBTITLE 1,Index Agents Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW index_agents_squad_base AS(
  SELECT 
    a.agent
    , a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.metric_value AS adherence
    , b.metric_value AS tnps
    , c.metric_value AS nocc
    , d.metric_value AS wows
    , d.denominator AS wows_target
    , e.metric_value AS quality
  FROM adherence AS a
  LEFT JOIN tnps AS b
    ON a.agent = b.agent
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'tnps_squad'
  LEFT JOIN nocc AS c
    ON a.agent = c.agent
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_squad'
  LEFT JOIN wows AS d
    ON a.agent = d.agent
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'wows_squad'
  LEFT JOIN quality AS e
    ON a.agent = e.agent
    AND a.date_reference = e.date_reference
    AND a.date_granularity = e.date_granularity
    AND e.metric = 'qa_squad'
  WHERE a.date_granularity IN ('month', 'week')
    AND a.metric = 'adherence_squad'
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_squad_final AS(
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
        WHEN wows >= 5 THEN 100
        WHEN wows < 5 THEN try_divide(wows, wows_target) * 100
        ELSE 0
      END AS wows
    , CASE
        WHEN nocc >= 100 THEN 100
        WHEN nocc <= 100 THEN nocc
        ELSE 0
      END AS nocc
    , COALESCE(tnps, 0) AS tnps
    , COALESCE(quality, 0) AS quality
  FROM index_agents_squad_base
  WHERE date_reference >= '2026-01-01'
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'index_agent_squad' AS metric
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN (adherence + tnps + wows) 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN (adherence + tnps + wows + quality)
        ELSE (adherence + tnps + wows + quality + nocc) 
      END AS numerator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN 300
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN 400 
        ELSE 500
      END AS denominator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN (adherence + tnps + wows) / 3
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN (adherence + tnps + wows + quality) / 4
        ELSE (adherence + tnps + wows + quality + nocc) / 5
      END AS metric_value
  FROM index_agents_squad_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'index_agent_squad' AS metric
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' THEN (adherence + tnps + wows) 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' THEN (adherence + tnps + wows + quality)
        ELSE (adherence + tnps + wows + quality + nocc) 
      END AS numerator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' THEN 300
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' THEN 400 
        ELSE 500
      END AS denominator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' THEN (adherence + tnps + wows) / 3
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' THEN (adherence + tnps + wows + quality) / 4
        ELSE (adherence + tnps + wows + quality + nocc) / 5
      END AS metric_value
  FROM index_agents_squad_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_squad AS (
  SELECT * FROM index_agents_squad_monthly
  UNION ALL
  SELECT * FROM index_agents_squad_weekly
);

-- SELECT * FROM index_agents_squad

-- COMMAND ----------

-- DBTITLE 1,Index Agents District Calculations
CREATE OR REPLACE TEMPORARY VIEW index_agents_district_base AS(
  SELECT 
    a.agent
    , a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.metric_value AS adherence
    , b.metric_value AS tnps
    , c.metric_value AS nocc
    , d.metric_value AS wows
    , d.denominator AS wows_target
    , e.metric_value AS quality
  FROM adherence AS a
  LEFT JOIN tnps AS b
    ON a.agent = b.agent
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'tnps_district'
  LEFT JOIN nocc AS c
    ON a.agent = c.agent
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_district'
  LEFT JOIN wows AS d
    ON a.agent = d.agent
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'wows_district'
  LEFT JOIN quality AS e
    ON a.agent = e.agent
    AND a.date_reference = e.date_reference
    AND a.date_granularity = e.date_granularity
    AND e.metric = 'qa_district'
  WHERE a.date_granularity IN ('month', 'week')
    AND a.metric = 'adherence_district'
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_district_final AS(
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
        WHEN wows >= 5 THEN 100
        WHEN wows < 5 THEN try_divide(wows, wows_target) * 100
        ELSE 0
      END AS wows
    , CASE
        WHEN nocc >= 100 THEN 100
        WHEN nocc <= 100 THEN nocc
        ELSE 0
      END AS nocc
    , COALESCE(tnps, 0) AS tnps
    , COALESCE(quality, 0) AS quality
  FROM index_agents_district_base
  WHERE date_reference >= '2026-01-01'
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent_district' AS metric
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN (adherence + tnps + wows) 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN (adherence + tnps + wows + quality)
        ELSE (adherence + tnps + wows + quality + nocc) 
      END AS numerator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN 300
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN 400 
        ELSE 500
      END AS denominator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN (adherence + tnps + wows) / 3
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN (adherence + tnps + wows + quality) / 4
        ELSE (adherence + tnps + wows + quality + nocc) / 5
      END AS metric_value
  FROM index_agents_district_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent_district' AS metric
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' THEN (adherence + tnps + wows) 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' THEN (adherence + tnps + wows + quality)
        ELSE (adherence + tnps + wows + quality + nocc) 
      END AS numerator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' THEN 300
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' THEN 400 
        ELSE 500
      END AS denominator
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-31' THEN (adherence + tnps + wows) / 3
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-28' THEN (adherence + tnps + wows + quality) / 4
        ELSE (adherence + tnps + wows + quality + nocc) / 5
      END AS metric_value
  FROM index_agents_district_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_district AS (
  SELECT * FROM index_agents_district_monthly
  UNION ALL
  SELECT * FROM index_agents_district_weekly
);

-- SELECT * FROM index_agents_district

-- COMMAND ----------

-- DBTITLE 1,Index Agents Dataset
CREATE OR REPLACE TEMPORARY VIEW index_agents_join AS(
  SELECT * FROM index_agents
  UNION ALL
  SELECT * FROM index_agents_general_quartile
  UNION ALL
  SELECT * FROM index_agents_team_quartile
  UNION ALL
  SELECT * FROM index_agents_squad
  UNION ALL
  SELECT * FROM index_agents_district
);

-- SELECT * FROM index_agents_join

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #XForce Metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Shrinkage

-- COMMAND ----------

-- DBTITLE 1,Base
CREATE OR REPLACE TEMPORARY VIEW shrinkage_base AS(
  SELECT
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , agent_dime_squad AS old_squad
    , dime_date AS date
    , activity_type_required
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE affiliation = 'nubank'
    AND dime_date >= '2025-01-01'
    AND activity_type_required IS NOT NULL
    AND activity_type_required NOT IN ('lunch_break', 'dime_invalid_notation')
    AND agent_dime_squad IS NOT NULL
    AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote')
    AND LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM social_agents)
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_final_2026 AS(
  SELECT
    a.*
    , COUNT(CASE WHEN a.activity_type_required IN ('shrinkage', 'timeoff') THEN 1 END) AS shrinkage_slot
    , COUNT(*) AS required_slot
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
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_final_2025 AS(
  SELECT
    a.*
    , COUNT(CASE WHEN a.activity_type_required IN ('shrinkage', 'timeoff') THEN 1 END) AS shrinkage_slot
    , COUNT(*) AS required_slot
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

-- CREATE OR REPLACE TEMPORARY VIEW shrinkage_xforces_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , DATE_TRUNC('QUARTER', date) AS date_reference
--     , 'quarter' AS date_granularity
--     , 'shrinkage_xforce' AS metric
--     , SUM(shrinkage_slot) AS numerator
--     , SUM(required_slot) AS denominator
--     , TRY_DIVIDE(SUM(shrinkage_slot) , SUM(required_slot)) *100 AS metric_value
--   FROM shrinkage_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW shrinkage_xforces_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , CASE
--         WHEN date < '2026-07-01' THEN '2026-01-01'
--         WHEN date >= '2026-07-01' THEN '2026-07-01'
--         ELSE NULL
--       END AS date_reference
--     , 'semester' AS date_granularity
--     , 'shrinkage_xforce' AS metric
--     , SUM(shrinkage_slot) AS numerator
--     , SUM(required_slot) AS denominator
--     , TRY_DIVIDE(SUM(shrinkage_slot) , SUM(required_slot)) *100 AS metric_value
--   FROM shrinkage_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW shrinkage_xforces_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , DATE_TRUNC('YEAR', date) AS date_reference
--     , 'year' AS date_granularity
--     , 'shrinkage_xforce' AS metric
--     , SUM(shrinkage_slot) AS numerator
--     , SUM(required_slot) AS denominator
--     , TRY_DIVIDE(SUM(shrinkage_slot) , SUM(required_slot)) *100 AS metric_value
--   FROM shrinkage_final
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW shrinkage_xforces AS (
  SELECT * FROM shrinkage_xforces_monthly
  UNION ALL
  SELECT * FROM shrinkage_xforces_weekly
--   UNION ALL
--   SELECT * FROM shrinkage_xforces_semesterly
--   UNION ALL
--   SELECT * FROM shrinkage_xforces_yearly
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
    , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END)/ COUNT(DISTINCT xforce) *100 AS metric_value
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
    , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END)/ COUNT(DISTINCT xforce) *100 AS metric_value
  FROM shrinkage_xforces_weekly
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW shrinkage_xpleads_quarterly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'shrinkage_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END) AS numerator
--     , COUNT(DISTINCT xforce) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END)/ COUNT(DISTINCT xforce) *100 AS metric_value
--   FROM shrinkage_xforces_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW shrinkage_xpleads_semesterly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'shrinkage_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END) AS numerator
--     , COUNT(DISTINCT xforce) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END)/ COUNT(DISTINCT xforce) *100 AS metric_value
--   FROM shrinkage_xforces_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW shrinkage_xpleads_yearly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'shrinkage_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END) AS numerator
--     , COUNT(DISTINCT xforce) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 20 THEN xforce END)/ COUNT(DISTINCT xforce) *100 AS metric_value
--   FROM shrinkage_xforces_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW shrinkage_xpleads AS (
  SELECT * FROM shrinkage_xpleads_monthly
  UNION ALL
  SELECT * FROM shrinkage_xpleads_weekly
--   UNION ALL
--   SELECT * FROM shrinkage_xpleads_semesterly
--   UNION ALL
--   SELECT * FROM shrinkage_xpleads_yearly
);

-- SELECT * FROM shrinkage_xpleads

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

-- DBTITLE 1,Shrinkage Dataset
CREATE OR REPLACE TEMPORARY VIEW shrinkage AS(
  SELECT * FROM shrinkage_xforces
  UNION ALL
  SELECT * FROM shrinkage_xpleads
  UNION ALL
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
    , COALESCE(a.numerator,0) AS adherence_in_target
    , COALESCE(b.numerator,0) AS tnps_in_target
    , COALESCE(c.numerator,0) AS nocc_in_target
    , COALESCE(d.numerator,0) AS wows_in_target
    , COALESCE(e.numerator,0) AS qa_in_target
    , COALESCE(a.denominator,0) AS adherence_xpeers
    , COALESCE(b.denominator,0) AS tnps_xpeers
    , COALESCE(c.denominator,0) AS nocc_xpeers
    , COALESCE(d.denominator,0) AS wows_xpeers
    , COALESCE(e.denominator,0) AS qa_xpeers
  FROM adherence AS a
  LEFT JOIN tnps AS b
    ON a.xforce = b.xforce
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'tnps_xforce'
  LEFT JOIN nocc AS c
    ON a.xforce = c.xforce
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_xforce'
  LEFT JOIN wows AS d
    ON a.xforce = d.xforce
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'wows_xforce'
  LEFT JOIN quality AS e
    ON a.xforce = e.xforce
    AND a.date_reference = e.date_reference
    AND a.date_granularity = e.date_granularity
    AND e.metric = 'qa_xforce'
  WHERE a.date_granularity IN ('week', 'month', 'quarter', 'semester', 'year')
    AND a.metric = 'adherence_xforce'
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_final AS(
  SELECT
    *
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN (adherence_in_target + tnps_in_target + wows_in_target) 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN (adherence_in_target + tnps_in_target + wows_in_target + qa_in_target) 
        ELSE (adherence_in_target + tnps_in_target + wows_in_target + qa_in_target + nocc_in_target) 
      END AS xpeers_in_target
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN (adherence_xpeers + tnps_xpeers + wows_xpeers)  
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN (adherence_xpeers + tnps_xpeers + wows_xpeers + qa_xpeers)
        ELSE (adherence_xpeers + tnps_xpeers + wows_xpeers + qa_xpeers + nocc_xpeers)
        END AS xpeers
  FROM xpeers_in_target_base
);

-- SELECT * FROM xpeers_in_target_final

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XForces Calculation
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
    , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
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
    , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'xpeers_in_target_xforce' AS metric
--     , SUM(xpeers_in_target) AS numerator
--     , SUM(xpeers) AS denominator
--     , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
--   FROM xpeers_in_target_final
--   WHERE date_granularity = 'quarter'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'xpeers_in_target_xforce' AS metric
--     , SUM(xpeers_in_target) AS numerator
--     , SUM(xpeers) AS denominator
--     , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
--   FROM xpeers_in_target_final
--   WHERE date_granularity = 'semester'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'xpeers_in_target_xforce' AS metric
--     , SUM(xpeers_in_target) AS numerator
--     , SUM(xpeers) AS denominator
--     , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
--   FROM xpeers_in_target_final
--   WHERE date_granularity = 'year'
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces AS (
  SELECT * FROM xpeers_in_target_xforces_monthly
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_weekly
--   UNION ALL
--   SELECT * FROM xpeers_in_target_xforces_semesterly
--   UNION ALL
--   SELECT * FROM xpeers_in_target_xforces_yearly
);

-- SELECT * FROM xpeers_in_target_xforces_monthly

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

-- DBTITLE 1,Xpeers in Target for XForces Dataset
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xforces_join AS (
  SELECT * FROM xpeers_in_target_xforces
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_squad
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_district
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

-- CREATE OR REPLACE TEMPORARY VIEW average_index_agent_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'average_index_agent' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , AVG(metric_value) AS metric_value
--   FROM average_index_agent_base
--   WHERE date_granularity = 'quarter'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW average_index_agent_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'average_index_agent' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , AVG(metric_value) AS metric_value
--   FROM average_index_agent_base
--   WHERE date_granularity = 'semester'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW average_index_agent_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'average_index_agent' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , AVG(metric_value) AS metric_value
--   FROM average_index_agent_base
--   WHERE date_granularity = 'year'
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW average_index_agent AS (
  SELECT * FROM average_index_agent_monthly
  UNION ALL
  SELECT * FROM average_index_agent_weekly
--   UNION ALL
--   SELECT * FROM average_index_agent_semesterly
--   UNION ALL
--   SELECT * FROM average_index_agent_yearly
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
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_agents_monthly
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
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_agents_weekly
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
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_agents_monthly
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
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_agents_weekly
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

-- DBTITLE 1,Average Index Agents Dataset
CREATE OR REPLACE TEMPORARY VIEW average_index_agent_join AS (
  SELECT * FROM average_index_agent
  UNION ALL
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

-- CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , 'quarter' AS date_granularity
--     , 'nuvinhos_performance' AS metric
--     , AVG(nuvinhos_average) AS numerator
--     , AVG(old_average) AS denominator
--     , AVG(nuvinhos_average) / AVG(old_average) * 100 AS metric_value
--   FROM average_index_agent_base
--   WHERE date_granularity = 'quarter'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , DATE_TRUNC('QUARTER', date_reference) AS date_reference
--     , CASE
--         WHEN date_reference < '2026-07-01' THEN '2026-01-01'
--         WHEN date_reference >= '2026-07-01' THEN '2026-07-01'
--         ELSE NULL
--       END AS date_reference
--     , 'nuvinhos_performance' AS metric
--     , AVG(nuvinhos_average) AS numerator
--     , AVG(old_average) AS denominator
--     , AVG(nuvinhos_average) / AVG(old_average) * 100 AS metric_value
--   FROM average_index_agent_base
--   WHERE date_granularity = 'semester'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , DATE_TRUNC('YEAR', date_reference) AS date_reference
--     , 'year' AS date_granularity
--     , 'nuvinhos_performance_agent' AS metric
--     , AVG(nuvinhos_average) AS numerator
--     , AVG(old_average) AS denominator
--     , AVG(nuvinhos_average) / AVG(old_average) * 100 AS metric_value
--   FROM average_index_agent_base
--   WHERE date_granularity = 'year'
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance AS (
  SELECT * FROM nuvinhos_performance_monthly
  UNION ALL
  SELECT * FROM nuvinhos_performance_weekly
--   UNION ALL
--   SELECT * FROM nuvinhos_performance_semesterly
--   UNION ALL
--   SELECT * FROM nuvinhos_performance_yearly
);

-- SELECT * FROM nuvinhos_performance

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

-- DBTITLE 1,Nuvinhos Performance Dataset
CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_join AS (
  SELECT * FROM nuvinhos_performance
  UNION ALL
  SELECT * FROM nuvinhos_performance_squad
  UNION ALL
  SELECT * FROM nuvinhos_performance_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Index XForces

-- COMMAND ----------

-- DBTITLE 1,Index XForces Base
CREATE OR REPLACE TEMPORARY VIEW index_xforces_base AS(
  SELECT 
    a.agent
    , a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.metric_value AS shrinkage_xforce
    , b.metric_value AS xpeers_in_target_xforce
    , c.metric_value AS average_index_agent
  FROM shrinkage_xforces_monthly AS a
  LEFT JOIN xpeers_in_target_xforces_monthly AS b
    ON a.xforce = b.xforce
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'xpeers_in_target_xforce'
  LEFT JOIN average_index_agent_monthly AS c
    ON a.xforce = c.xforce
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'average_index_agent'
  WHERE a.date_granularity IN ('week','month', 'quarter', 'semester', 'year')
    AND a.metric = 'shrinkage_xforce'
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_final AS(
  SELECT
    agent
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
        WHEN shrinkage_xforce >  20 THEN 120 - shrinkage_xforce
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
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
    , 300 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
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
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
    , 300 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
  FROM index_xforces_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW index_xforces_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_xforce' AS metric
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
--     , 300 AS denominator
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
--   FROM index_xforces_final
--   WHERE date_granularity = 'quarter'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW index_xforces_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_xforce' AS metric
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
--     , 300 AS denominator
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
--   FROM index_xforces_final
--   WHERE date_granularity = 'semester'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW index_xforces_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'index_xforce' AS metric
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
--     , 300 AS denominator
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
--   FROM index_xforces_final
--   WHERE date_granularity = 'year'
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW index_xforces AS (
  SELECT * FROM index_xforces_monthly
  UNION ALL
  SELECT * FROM index_xforces_weekly
--   UNION ALL
--   SELECT * FROM index_xforces_semesterly
--   UNION ALL
--   SELECT * FROM index_xforces_yearly
);

-- SELECT * FROM index_agents

-- COMMAND ----------

-- DBTITLE 1,Index XForces Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad_base AS(
  SELECT 
    a.agent
    , a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.metric_value AS shrinkage_xforce
    , b.metric_value AS xpeers_in_target_xforce
    , c.metric_value AS average_index_agent
  FROM shrinkage AS a
  LEFT JOIN xpeers_in_target_xforces AS b
    ON a.xforce = b.xforce
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'xpeers_in_target_xforce_squad'
  LEFT JOIN average_index_agent AS c
    ON a.xforce = c.xforce
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'average_index_agent_squad'
  WHERE a.date_granularity IN ('week','month', 'quarter', 'semester', 'year')
    AND a.metric = 'shrinkage_squad'
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad_final AS(
  SELECT
    agent
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
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
    , 300 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
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
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
    , 300 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
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
    a.agent
    , a.xforce
    , a.xplead
    , a.squad
    , a.squad_district
    , a.date_reference
    , a.date_granularity
    , a.metric_value AS shrinkage_xforce
    , b.metric_value AS xpeers_in_target_xforce
    , c.metric_value AS average_index_agent
  FROM shrinkage AS a
  LEFT JOIN xpeers_in_target_xforces AS b
    ON a.xforce = b.xforce
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'xpeers_in_target_xforce_district'
  LEFT JOIN average_index_agent AS c
    ON a.xforce = c.xforce
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'average_index_agent_district'
  WHERE a.date_granularity IN ('week','month', 'quarter', 'semester', 'year')
    AND a.metric = 'shrinkage_district'
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_district_final AS(
  SELECT
    agent
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
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
    , 300 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
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
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) AS numerator
    , 300 AS denominator
    , (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3 AS metric_value
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

-- DBTITLE 1,Index XForces Dataset
CREATE OR REPLACE TEMPORARY VIEW index_xforces_join AS (
  SELECT * FROM index_xforces
  UNION ALL
  SELECT * FROM index_xforces_squad
  UNION ALL
  SELECT * FROM index_xforces_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # XPLeads Metrics

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
    , COALESCE(a.numerator,0) AS adherence_in_target
    , COALESCE(b.numerator,0) AS tnps_in_target
    , COALESCE(c.numerator,0) AS nocc_in_target
    , COALESCE(d.numerator,0) AS wows_in_target
    , COALESCE(e.numerator,0) AS qa_in_target
    , COALESCE(a.denominator,0) AS adherence_xpeers
    , COALESCE(b.denominator,0) AS tnps_xpeers
    , COALESCE(c.denominator,0) AS nocc_xpeers
    , COALESCE(d.denominator,0) AS wows_xpeers
    , COALESCE(e.denominator,0) AS qa_xpeers
  FROM adherence AS a
  LEFT JOIN tnps AS b
    ON a.xplead = b.xplead
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'tnps_xplead'
  LEFT JOIN nocc AS c
    ON a.xplead = c.xplead
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_xplead'
  LEFT JOIN wows AS d
    ON a.xplead = d.xplead
    AND a.date_reference = d.date_reference
    AND a.date_granularity = d.date_granularity
    AND d.metric = 'wows_xplead'
  LEFT JOIN quality AS e
    ON a.xplead = e.xplead
    AND a.date_reference = e.date_reference
    AND a.date_granularity = e.date_granularity
    AND e.metric = 'qa_xplead'
  WHERE a.date_granularity IN ('week','month', 'quarter', 'semester', 'year')
    AND a.metric = 'adherence_xplead'
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xplead_final AS(
  SELECT
    *
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN (adherence_in_target + tnps_in_target + wows_in_target) 
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN (adherence_in_target + tnps_in_target + wows_in_target + qa_in_target) 
        ELSE (adherence_in_target + tnps_in_target + wows_in_target + qa_in_target + nocc_in_target) 
      END AS xpeers_in_target
    , CASE
        WHEN CAST(date_reference AS DATE) <= DATE '2026-01-01' THEN (adherence_xpeers + tnps_xpeers + wows_xpeers)  
        WHEN CAST(date_reference AS DATE) <= DATE '2026-02-01' THEN (adherence_xpeers + tnps_xpeers + wows_xpeers + qa_xpeers)
        ELSE (adherence_xpeers + tnps_xpeers + wows_xpeers + qa_xpeers + nocc_xpeers)
        END AS xpeers
  FROM xpeers_in_target_xplead_base
);

-- SELECT * FROM xpeers_in_target_xplead_base

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XPLead Calculations
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xplead' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_xplead_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xplead' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_xplead_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'xpeers_in_target_xplead' AS metric
--     , SUM(xpeers_in_target) AS numerator
--     , SUM(xpeers) AS denominator
--     , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
--   FROM xpeers_in_target_xplead_final
--   WHERE date_granularity = 'quarter'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'xpeers_in_target_xplead' AS metric
--     , SUM(xpeers_in_target) AS numerator
--     , SUM(xpeers) AS denominator
--     , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
--   FROM xpeers_in_target_xplead_final
--   WHERE date_granularity = 'semester'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'xpeers_in_target_xplead' AS metric
--     , SUM(xpeers_in_target) AS numerator
--     , SUM(xpeers) AS denominator
--     , (SUM(xpeers_in_target) / SUM(xpeers)) *100 AS metric_value
--   FROM xpeers_in_target_xplead_final
--   WHERE date_granularity = 'year'
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads AS (
  SELECT * FROM xpeers_in_target_xpleads_monthly
  UNION ALL
  SELECT * FROM xpeers_in_target_xpleads_weekly
--   UNION ALL
--   SELECT * FROM xpeers_in_target_xpleads_semesterly
--   UNION ALL
--   SELECT * FROM xpeers_in_target_xpleads_yearly
);

-- SELECT * FROM xpeers_in_target_xpleads_monthly

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XPLead Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xplead_squad' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_xplead_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xplead_squad' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_xplead_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_squad AS (
  SELECT * FROM xpeers_in_target_xpleads_squad_monthly
  UNION ALL
  SELECT * FROM xpeers_in_target_xpleads_squad_weekly
);

-- SELECT * FROM xpeers_in_target_xpleads_squad

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XPLead District Calculations
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xplead_district' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_xplead_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'xpeers_in_target_xplead_district' AS metric
    , SUM(xpeers_in_target) AS numerator
    , SUM(xpeers) AS denominator
    , TRY_DIVIDE(SUM(xpeers_in_target), SUM(xpeers)) *100 AS metric_value
  FROM xpeers_in_target_xplead_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_district AS (
  SELECT * FROM xpeers_in_target_xpleads_district_monthly
  UNION ALL
  SELECT * FROM xpeers_in_target_xpleads_district_weekly
);

-- SELECT * FROM xpeers_in_target_xpleads_district

-- COMMAND ----------

-- DBTITLE 1,Xpeers in Target for XPLead Dataset
CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xpleads_join AS (
  SELECT * FROM xpeers_in_target_xpleads
  UNION ALL
  SELECT * FROM xpeers_in_target_xpleads_squad
  UNION ALL
  SELECT * FROM xpeers_in_target_xpleads_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Average Index XForces

-- COMMAND ----------

-- DBTITLE 1,Average Index XForces Base
CREATE OR REPLACE TEMPORARY VIEW average_index_xforces_base AS(
  SELECT
    *
  FROM index_xforces
);

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
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'average_index_xforce' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , AVG(metric_value) AS metric_value
--   FROM average_index_xforces_base
--   WHERE date_granularity = 'quarter'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'average_index_xforce' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , AVG(metric_value) AS metric_value
--   FROM average_index_xforces_base
--   WHERE date_granularity = 'semester'
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'average_index_xforce' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , AVG(metric_value) AS metric_value
--   FROM average_index_xforces_base
--   WHERE date_granularity = 'year'
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW average_index_xforce AS (
  SELECT * FROM average_index_xforce_monthly
  UNION ALL
  SELECT * FROM average_index_xforce_weekly
  -- UNION ALL
  -- SELECT * FROM average_index_xforce_semesterly
  -- UNION ALL
  -- SELECT * FROM average_index_xforce_yearly
);

-- SELECT * FROM average_index_xforce

-- COMMAND ----------

-- DBTITLE 1,Average Index XForces Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'average_index_xforce_squad' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforces_base
  WHERE date_granularity = 'month'
    AND metric = 'index_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'average_index_xforce_squad' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforces_base
  WHERE date_granularity = 'week'
    AND metric = 'index_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_squad AS (
  SELECT * FROM average_index_xforce_squad_monthly
  UNION ALL
  SELECT * FROM average_index_xforce_squad_weekly
);

-- SELECT * FROM average_index_xforce_squad

-- COMMAND ----------

-- DBTITLE 1,Average Index XForces District Calculations
CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'average_index_xforce_district' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforces_base
  WHERE date_granularity = 'month'
    AND metric = 'index_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'average_index_xforce_district' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforces_base
  WHERE date_granularity = 'week'
    AND metric = 'index_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_district AS (
  SELECT * FROM average_index_xforce_district_monthly
  UNION ALL
  SELECT * FROM average_index_xforce_district_weekly
);

-- SELECT * FROM average_index_xforce_district

-- COMMAND ----------

-- DBTITLE 1,Average Index XForces Dataset
CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_join AS (
  SELECT * FROM average_index_xforce
  UNION ALL
  SELECT * FROM average_index_xforce_squad
  UNION ALL
  SELECT * FROM average_index_xforce_district
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Joins and Save

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC table = "usr.mx__cx.internal_ops_performance_2026_social_media"
-- MAGIC
-- MAGIC metrics = [
-- MAGIC     "adherence",
-- MAGIC     "tnps",
-- MAGIC     "wows",
-- MAGIC     "nocc",
-- MAGIC     "shrinkage",
-- MAGIC     "quality",
-- MAGIC     "index_agents_join",
-- MAGIC     "xpeers_in_target_xforces_join",
-- MAGIC     "average_index_agent_join",
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
-- MAGIC         CREATE OR REPLACE TABLE {table}
-- MAGIC         USING DELTA
-- MAGIC         AS SELECT * FROM adherence WHERE 1 = 0
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

-- DBTITLE 1,Joins
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW dataset AS (
-- MAGIC   SELECT * FROM adherence
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM tnps
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM nocc
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM wows
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM shrinkage
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM index_agents_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM xpeers_in_target_xforces_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM average_index_agent_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM index_xforces_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM xpeers_in_target_xpleads_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM average_index_xforce_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM nuvinhos_performance_join
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM quality
-- MAGIC );
-- MAGIC
-- MAGIC -- SELECT DISTINCT metric FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Save table
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TABLE usr.mx__cx.internal_ops_performance_2026_social_media AS
-- MAGIC SELECT * FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Table Sharing
-- GRANT SELECT ON TABLE usr.mx__cx.internal_ops_performance_2026_social_media TO `59e52f0a-0aa5-44b9-90f9-3d781cc0e097`;
-- SELECT * FROM usr.mx__cx.internal_ops_performance_2026_social_media

-- COMMAND ----------

-- SELECT 
--   DISTINCT metric
-- FROM 
--   usr.mx__cx.internal_ops_performance_2026_social_media

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Cleanup intermediate tables
-- [Temp Fix] Optional cleanup of intermediate tables (run after the table is saved).
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_agent_information;
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_adherence;
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_nocc;
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_tnps;
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_wows;
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_quality;
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_index_agents_final;
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_index_agents;
-- DROP TABLE IF EXISTS usr.danielanzures.sm_temp_social_agents;