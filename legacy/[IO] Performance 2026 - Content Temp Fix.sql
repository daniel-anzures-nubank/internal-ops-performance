-- Databricks notebook source
-- MAGIC %md
-- MAGIC # [IO] Performance 2026 - Content (Temp Fix)
-- MAGIC
-- MAGIC Auto-generated optimization of `[IO] Performance 2026 - Content`.
-- MAGIC **Same data, same output table** (`usr.mx__cx.internal_ops_performance_2026_content`).
-- MAGIC
-- MAGIC Intermediate tables live in `usr.danielanzures` with prefix `cont_temp_`; a commented
-- MAGIC cleanup cell at the end drops them. Run top-to-bottom in Databricks like the original.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Agents Informations

-- COMMAND ----------

-- DBTITLE 1,Agent Information
CREATE OR REPLACE TEMPORARY VIEW agent_information AS (
  SELECT
    LOWER(REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , LOWER(REGEXP_EXTRACT(xforce_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS xforce
    , LOWER(REGEXP_EXTRACT(xplead_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS xplead
    , LOWER(squad) AS squad
    , LOWER(district) AS squad_district
    , LOWER(status) AS status
    , LOWER(target_squad) AS target_squad
    , DATE '2024-01-01' AS valid_from
    , DATE '2099-12-31' AS valid_to
  FROM gsheets.sheets.mx_content_bdx_daniel_anz_temp
  WHERE actor_email IS NOT NULL
);

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: agent_information
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_agent_information AS SELECT * FROM agent_information;
CREATE OR REPLACE TEMPORARY VIEW agent_information AS SELECT * FROM usr.danielanzures.cont_temp_agent_information;
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_content_agents AS
  SELECT DISTINCT LOWER(agent) AS agent FROM agent_information
  WHERE agent IS NOT NULL AND agent != '';
CREATE OR REPLACE TEMPORARY VIEW content_agents AS SELECT agent FROM usr.danielanzures.cont_temp_content_agents;

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] UNION missing DIME slots
-- UNION the ETL dime table with manually-added missing slots for Content H1 2026
CREATE OR REPLACE TEMPORARY VIEW agent_dimensioned_activities_extended AS (
  SELECT * FROM etl.mx__series_contract.agent_dimensioned_activities
  UNION ALL
  SELECT
    agent
    , shuffle_status_required
    , slot_start_local AS local_timestamp_dime_slot_starts_at
    , activity_type_required
    , NULL AS series_manual_append_timestamp
    , NULL AS shift_duration
    , dimensioned_activity
    , NULL AS job_owner_squad
    , date AS dime_date
    , DATE_FORMAT(slot_start_local, 'HH:mm:ss') AS dime_time_slot_starts_at
    , 'nubank' AS affiliation
    , dime_squad AS agent_dime_squad
  FROM usr.danielanzures.missing_agents_dime_slots_content_h1
);

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
  WHERE LOWER(REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM content_agents)
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
    OR (b.agent LIKE '%lucia.mier%' AND DATE(a.timestamp) IN ('2026-01-06', '2026-01-07', '2026-01-08', '2026-01-11'))
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
  FROM agent_dimensioned_activities_extended
  WHERE
      affiliation = 'nubank'
      AND dime_date >= '2025-01-01'
      AND activity_type_required IS NOT NULL
      AND activity_type_required NOT IN ('lunch_break', 'time_off', 'shrinkage')
      AND dimensioned_activity NOT IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle', 'Licencia', 'Vacacion')
      AND agent_dime_squad IS NOT NULL
      AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote')
      AND dime_date <= DATE_SUB(DATE_TRUNC('WEEK', CURRENT_DATE()), 1)
      AND dime_date != '2026-03-10'
      AND LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM content_agents)
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

CREATE OR REPLACE TEMPORARY VIEW adherence_by_slot AS(
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
    AND DATE_TRUNC('MONTH', a.date) >= b.valid_from
    AND DATE_TRUNC('MONTH', a.date) < b.valid_to
  WHERE (a.date <= '2025-11-05' OR a.date >= '2025-11-20') 
    AND a.date >= '2025-12-01'
    AND b.status = 'active'
    AND a.date NOT IN ('2026-03-27', '2026-04-09')
  GROUP BY ALL
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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_adherence AS
  SELECT * FROM adherence
  WHERE date_reference < '2026-01-01' OR date_reference >= '2026-02-01';
CREATE OR REPLACE TEMPORARY VIEW adherence AS SELECT * FROM usr.danielanzures.cont_temp_adherence;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Normalized Time per Job

-- COMMAND ----------

-- DBTITLE 1,NTPJ Base
CREATE OR REPLACE TEMPORARY VIEW oos_jobs_ntpj AS (
  SELECT
    LOWER(REPLACE(TRIM(REPLACE(job_classification, '(OOS_CONT)', '')), ' ', '_')) AS job_classification
    , net_time_spent_seconds
    , 'oos' AS activity_type
    , local_start_date
    , LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , REGEXP_REPLACE(squad, '^:', '')   AS squad
    , REGEXP_REPLACE(status, '^:', '')  AS status
    , COALESCE(
        CASE
          WHEN UPPER(comment) RLIKE 'MOS[\\s_-]*\\d{3,}'
            THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(comment), 'MOS[\\s_-]*(\\d{3,})', 1))
          WHEN comment RLIKE '^\\s*\\d{3,}\\s*$'
            THEN CONCAT('MOS-', TRIM(comment))
          WHEN UPPER(comment) RLIKE 'TICKET\\s+(MOS-?\\s*)?\\d{3,}'
            THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(comment), 'TICKET\\s+(?:MOS-?\\s*)?(\\d{3,})', 1))
          ELSE NULL
        END,
        CASE
          WHEN UPPER(ticket__id) RLIKE 'MOS[\\s_-]*\\d{3,}'
            THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(ticket__id), 'MOS[\\s_-]*(\\d{3,})', 1))
          WHEN ticket__id RLIKE '^\\s*\\d{3,}\\s*$'
            THEN CONCAT('MOS-', TRIM(ticket__id))
          WHEN UPPER(ticket__id) RLIKE 'TICKET\\s+(MOS-?\\s*)?\\d{3,}'
            THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(ticket__id), 'TICKET\\s+(?:MOS-?\\s*)?(\\d{3,})', 1))
          ELSE NULL
        END
      ) AS content_id
  FROM etl.mx__dataset.taskmaster_consolidated_registry
  WHERE DATE(local_start_date) != '2026-03-10'
);

CREATE OR REPLACE TEMPORARY VIEW oos_jobs_agg_ntpj AS (
  SELECT
    job_classification AS job_type
    , activity_type
    , status
    , squad
    , CASE
        WHEN job_classification IN ('macros', 'faq', 'ar') THEN DATE_TRUNC('DAY', MIN(local_start_date))
        ELSE DATE_TRUNC('DAY', MIN(local_start_date))
      END AS start_date
    , agent
    , CASE
        WHEN job_classification IN ('macros', 'faq', 'ar') THEN COUNT(*)
        ELSE COUNT(DISTINCT content_id)
      END AS count
    , SUM(net_time_spent_seconds) AS duration
    , CONCAT(activity_type, ' - ', job_classification) AS job_id
    , content_id
  FROM oos_jobs_ntpj
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW expected_duration_per_job_ntpj AS (
  SELECT
    DATE_TRUNC('MONTH', a.start_date) AS start_month
    , a.job_id
    , TRY_DIVIDE(SUM(b.duration), SUM(b.count)) AS exp_duration_job
    , a.squad
  FROM oos_jobs_agg_ntpj AS a
  JOIN oos_jobs_agg_ntpj AS b
    ON a.job_id = b.job_id
    AND DATE_TRUNC('MONTH', a.start_date) >= DATE_TRUNC('MONTH', b.start_date)
    AND DATE_TRUNC('MONTH', a.start_date) - INTERVAL 4 MONTHS <= DATE_TRUNC('MONTH', b.start_date)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW dime_ntpj AS (
  SELECT 
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , dime_date AS date
    , activity_type_required AS activity_type
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE affiliation = 'nubank'
    AND dime_date >= '2024-12-30'
    AND activity_type_required IS NOT NULL
    AND activity_type_required NOT IN ('lunch_break', 'dime_invalid_notation', 'time_off')
    AND agent_dime_squad IN ('Content', 'content_content')
    AND shuffle_status_required IN ('available', 'oos')
    AND dime_date != '2026-03-10'
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_initial_base AS(
  SELECT
    a.*
    , b.exp_duration_job
  FROM oos_jobs_agg_ntpj AS a
  LEFT JOIN expected_duration_per_job_ntpj AS b
    ON a.job_id = b.job_id
    AND DATE_TRUNC('MONTH', a.start_date) = b.start_month
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_base AS(
  SELECT
    *
    , exp_duration_job * count AS total_exp_duration
  FROM ntpj_initial_base
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_calculations AS(
  SELECT
    REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) as agent
    , start_date AS date
    -- , SUM(duration) AS job_time
    -- , SUM(total_exp_duration) AS exp_job_time
    , TRY_DIVIDE(SUM(duration), COUNT(job_id)) AS job_time
    , TRY_DIVIDE(SUM(total_exp_duration), COUNT(job_id)) AS exp_job_time
    , TRY_DIVIDE(SUM(duration), SUM(total_exp_duration)) AS ntpj
  FROM ntpj_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_all_info AS(
  SELECT
    a.*
    , b.xforce
    , b.xplead
    , b.squad
    , b.squad_district
  FROM ntpj_calculations AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) >= b.valid_from
    AND DATE_TRUNC('MONTH', a.date) < b.valid_to
  WHERE b.status = 'active'
    AND a.date >= '2025-12-01'
    AND a.date NOT IN ('2026-03-10', '2026-03-27', '2026-04-09')
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_final AS(
  SELECT
    *
  FROM ntpj_all_info
);

-- SELECT
--   *
-- FROM ntpj_final
-- WHERE squad = 'content'

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: ntpj_initial_base
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_ntpj_initial_base AS SELECT * FROM ntpj_initial_base;
CREATE OR REPLACE TEMPORARY VIEW ntpj_initial_base AS SELECT * FROM usr.danielanzures.cont_temp_ntpj_initial_base;

-- COMMAND ----------

-- DBTITLE 1,NTPJ Agents Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('DAY', date) AS date_reference
    , 'day' AS date_granularity
    , 'ntpj_agent' AS metric
    , SUM(job_time) AS numerator
    , SUM(exp_job_time) AS denominator
    , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
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
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'ntpj_agent' AS metric
    , SUM(job_time) AS numerator
    , SUM(exp_job_time) AS denominator
    , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
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
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'ntpj_agent' AS metric
    , SUM(job_time) AS numerator
    , SUM(exp_job_time) AS denominator
    , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_final
    GROUP BY ALL)
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , DATE_TRUNC('QUARTER', date) AS date_reference
--     , 'quarter' AS date_granularity
--     , 'ntpj_agent' AS metric
--     , SUM(job_time) AS numerator
--     , SUM(exp_job_time) AS denominator
--     , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
--   FROM ntpj_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_semesterly AS (
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
--     , 'ntpj_agent' AS metric
--     , SUM(job_time) AS numerator
--     , SUM(exp_job_time) AS denominator
--     , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
--   FROM ntpj_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , DATE_TRUNC('YEAR', date) AS date_reference
--     , 'year' AS date_granularity
--     , 'ntpj_agent' AS metric
--     , SUM(job_time) AS numerator
--     , SUM(exp_job_time) AS denominator
--     , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
--   FROM ntpj_final
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents AS (
  SELECT * FROM ntpj_agents_daily
  UNION ALL
  SELECT * FROM ntpj_agents_weekly
  UNION ALL
  SELECT * FROM ntpj_agents_monthly
  -- UNION ALL
  -- SELECT * FROM ntpj_agents_quarterly
  -- UNION ALL
  -- SELECT * FROM ntpj_agents_semesterly
  -- UNION ALL
  -- SELECT * FROM ntpj_agents_yearly
);

-- SELECT * FROM ntpj_agents

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

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_general_quartile_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM ntpj_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_general_quartile_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM ntpj_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_general_quartile_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_agents_general_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM ntpj_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_general_quartile AS (
  SELECT * FROM ntpj_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM ntpj_agents_general_quartile_weekly
--   UNION ALL
--   SELECT * FROM ntpj_agents_general_quartile_semesterly
--   UNION ALL
--   SELECT * FROM ntpj_agents_general_quartile_yearly
);

-- SELECT * FROM ntpj_agents_general_quartile

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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) ASC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) ASC) AS metric_value
  FROM ntpj_agents_weekly
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_team_quartile_quarterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM ntpj_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_team_quartile_semesterly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM ntpj_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_team_quartile_yearly AS (
--   SELECT
--     agent
--     , xforce
--     , xplead
--     , squad
--     , squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_agents_team_quartile' AS metric
--     , NULL AS numerator
--     , NULL AS denominator
--     , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
--   FROM ntpj_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW ntpj_agents_team_quartile AS (
  SELECT * FROM ntpj_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM ntpj_agents_team_quartile_weekly
--   UNION ALL
--   SELECT * FROM ntpj_agents_team_quartile_semesterly
--   UNION ALL
--   SELECT * FROM ntpj_agents_team_quartile_yearly
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
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
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
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM ntpj_agents_weekly
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_xforces_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM ntpj_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_xforces_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM ntpj_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_xforces_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_xforce' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
--   FROM ntpj_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW ntpj_xforces AS (
  SELECT * FROM ntpj_xforces_monthly
  UNION ALL
  SELECT * FROM ntpj_xforces_weekly
--   UNION ALL
--   SELECT * FROM ntpj_xforces_semesterly
--   UNION ALL
--   SELECT * FROM ntpj_xforces_yearly
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
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
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
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) *100 AS metric_value
  FROM ntpj_agents_weekly
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_xpleads_quarterly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM ntpj_agents_quarterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_xpleads_semesterly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM ntpj_agents_semesterly
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW ntpj_xpleads_yearly AS (
--   SELECT
--     NULL AS agent
--     , NULL AS xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , date_reference
--     , date_granularity
--     , 'ntpj_xplead' AS metric
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
--   FROM ntpj_agents_yearly
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW ntpj_xpleads AS (
  SELECT * FROM ntpj_xpleads_monthly
  UNION ALL
  SELECT * FROM ntpj_xpleads_weekly
--   UNION ALL
--   SELECT * FROM ntpj_xpleads_semesterly
--   UNION ALL
--   SELECT * FROM ntpj_xpleads_yearly
);

-- SELECT * FROM ntpj_xpleads

-- COMMAND ----------

-- DBTITLE 1,NTPJ Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW ntpj_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'ntpj_squad' AS metric
    , SUM(job_time) AS numerator
    , SUM(exp_job_time) AS denominator
    , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'ntpj_squad' AS metric
    , SUM(job_time) AS numerator
    , SUM(exp_job_time) AS denominator
    , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_final
    GROUP BY ALL)
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
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'ntpj_district' AS metric
    , SUM(job_time) AS numerator
    , SUM(exp_job_time) AS denominator
    , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'ntpj_district' AS metric
    , SUM(job_time) AS numerator
    , SUM(exp_job_time) AS denominator
    , TRY_DIVIDE(SUM(job_time) , SUM(exp_job_time)) *100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_final
    GROUP BY ALL)
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_district AS (
  SELECT * FROM ntpj_district_monthly
  UNION ALL
  SELECT * FROM ntpj_district_weekly
);

-- SELECT * FROM ntpj_district

-- COMMAND ----------

-- DBTITLE 1,NTPJ Dataset
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
  UNION ALL
  SELECT * FROM ntpj_squad
  UNION ALL
  SELECT * FROM ntpj_district
);

-- SELECT * FROM ntpj

-- COMMAND ----------

-- DBTITLE 1,NTPJ v2 — SLA-Weighted Compliance (Agent Level)
-- ============================================================
-- NTPJ v2 — SLA-Weighted Compliance (Agent Level)
-- Higher is better: 100 = every job delivered within its SLA.
-- Standalone view — NOT wired into ntpj UNION or the save cell.
--
-- Probe reconciliation (distinct normalized job_classification, 2025-12-01+):
--   All 25 SLA map keys found in data — no spelling changes needed.
--   Excluded by inner join (no SLA key in map):
--     mastery_cx, sop             → explicitly no-SLA per spec
--     projects (557 rows)         → generic bucket without SLA;
--                                    only projects_a/b/c are mapped —
--                                    confirm with team whether this should be included
--     2nd_(oos_cbr_2nd), c-_reject, ddr_answered_(oos_cbr_dda),
--     ddr_cancelled_(oos_cbu_ddc), ddr_debit_(oos_cbu_ddd),
--     debit_cancelled_(oos_cbu_mdc), debit_fraud_(oos_cbu_mdf),
--     fraud_(oos_cbu_ddf)         → Core/Fraud OOS types appearing for
--                                    content agents; no content SLA defined
-- ============================================================

-- 1. Per-job grain: actual_time_seconds vs SLA (inner join excludes no-SLA types)
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_job_base AS (
  WITH sla_map (job_type, sla_seconds) AS (
    SELECT * FROM (VALUES
      ('discovery',             7200),
      ('sync',                  5400),
      ('weduka_a',              5400),
      ('weduka_b',             12600),
      ('weduka_c',             18000),
      ('weduka_optimization',  36000),
      ('macros',                1800),
      ('faq',                   2700),
      ('ar',                    2700),
      ('learning_material_a',  10800),
      ('learning_material_b',  18000),
      ('learning_material_c',  25200),
      ('purple_screens',        5400),
      ('purple_screens_review', 3600),
      ('weduka_comms',          5400),
      ('publish',               1200),
      ('emergency',             1800),
      ('projects_a',            7200),
      ('projects_b',           28800),
      ('projects_c',           57600),
      ('xpeer_xperience',      18000),
      ('deactivation',           300),
      ('weduka_bdx',            5400),
      ('sm_content',           28800),
      ('sm_comunidad_nu',       1800)
    ) AS t(job_type, sla_seconds)
  ),
  non_row_jobs AS (
    -- Non-macros/faq/ar: one "job" = one distinct content_id
    SELECT
      j.job_classification
      , j.agent
      , DATE_TRUNC('DAY', MIN(j.local_start_date)) AS start_date
      , SUM(j.net_time_spent_seconds)              AS actual_time_seconds
    FROM oos_jobs_ntpj AS j
    WHERE j.job_classification NOT IN ('macros', 'faq', 'ar')
      AND j.content_id IS NOT NULL
    GROUP BY j.job_classification, j.agent, j.content_id
  ),
  row_jobs AS (
    -- macros/faq/ar: one "job" = one source row
    SELECT
      job_classification
      , agent
      , DATE_TRUNC('DAY', local_start_date) AS start_date
      , net_time_spent_seconds              AS actual_time_seconds
    FROM oos_jobs_ntpj
    WHERE job_classification IN ('macros', 'faq', 'ar')
  ),
  all_jobs AS (
    SELECT * FROM non_row_jobs
    UNION ALL
    SELECT * FROM row_jobs
  )
  SELECT
    j.agent
    , j.start_date
    , s.sla_seconds
    , CASE WHEN j.actual_time_seconds <= s.sla_seconds THEN s.sla_seconds ELSE 0 END AS sla_seconds_met
  FROM all_jobs      AS j
  INNER JOIN sla_map AS s ON j.job_classification = s.job_type
);

-- 2. Aggregate per agent x day, attach hierarchy from agent_information
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_agent_base AS (
  SELECT
    j.agent
    , j.start_date               AS date
    , SUM(j.sla_seconds_met)     AS numerator
    , SUM(j.sla_seconds)         AS denominator
    , b.xforce
    , b.xplead
    , b.squad
    , b.squad_district
  FROM ntpj_sla_new_job_base AS j
  LEFT JOIN agent_information AS b
    ON j.agent = b.agent
    AND DATE_TRUNC('MONTH', j.start_date) >= b.valid_from
    AND DATE_TRUNC('MONTH', j.start_date) < b.valid_to
  WHERE b.status = 'active'
    AND j.start_date >= '2025-12-01'
    AND j.start_date != '2026-03-10'
  GROUP BY ALL
);

-- 3. Daily granularity
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('DAY', date)                            AS date_reference
    , 'day'                                              AS date_granularity
    , 'ntpj_sla_new'                                    AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM ntpj_sla_new_agent_base
  GROUP BY ALL
);

-- 4. Weekly granularity
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_weekly AS (
  SELECT
    agent
    , first_xforce           AS xforce
    , first_xplead           AS xplead
    , first_squad            AS squad
    , first_squad_district   AS squad_district
    , DATE_TRUNC('WEEK', date)                           AS date_reference
    , 'week'                                             AS date_granularity
    , 'ntpj_sla_new'                                    AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce)         OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead)         OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad)          OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_new_agent_base
    GROUP BY ALL
  )
  GROUP BY ALL
);

-- 5. Monthly granularity
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_monthly AS (
  SELECT
    agent
    , first_xforce           AS xforce
    , first_xplead           AS xplead
    , first_squad            AS squad
    , first_squad_district   AS squad_district
    , DATE_TRUNC('MONTH', date)                          AS date_reference
    , 'month'                                            AS date_granularity
    , 'ntpj_sla_new'                                    AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce)         OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead)         OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad)          OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_new_agent_base
    GROUP BY ALL
  )
  GROUP BY ALL
);

-- 6. Final standalone union — metric='ntpj_sla_new', same schema as ntpj_agents
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new AS (
  SELECT * FROM ntpj_sla_new_daily
  UNION ALL
  SELECT * FROM ntpj_sla_new_weekly
  UNION ALL
  SELECT * FROM ntpj_sla_new_monthly
);

-- SELECT * FROM ntpj_sla_new

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: ntpj_v2
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_ntpj_sla_new AS SELECT * FROM ntpj_sla_new;
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new AS SELECT * FROM usr.danielanzures.cont_temp_ntpj_sla_new;

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: ntpj
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_ntpj AS SELECT * FROM ntpj;
CREATE OR REPLACE TEMPORARY VIEW ntpj AS SELECT * FROM usr.danielanzures.cont_temp_ntpj;

-- COMMAND ----------

-- DBTITLE 1,NTPJ v3 — SLA-Weighted Compliance (OLD SLA, Agent Level)
-- ============================================================
-- NTPJ v3 — SLA-Weighted Compliance using OLD SLA (Agent Level)
-- Metric name in final table: ntpj_sla_old
-- Higher is better: 100 = every job delivered within its OLD SLA.
-- Standalone view — NOT wired into ntpj UNION; wired into save cell only.
--
-- Probe reconciliation (distinct normalized job_classification, 2025-12-01+):
--   All 25 OLD SLA map keys found in data — no spelling changes needed.
--   Excluded by inner join (no OLD SLA key in map):
--     mastery_cx, sop             → excluded per spec (even though they have 24h old SLA)
--     projects (557 rows)         → generic bucket without SLA;
--                                    only projects_a/b/c are mapped
--     2nd_(oos_cbr_2nd), c-_reject, ddr_answered_(oos_cbr_dda),
--     ddr_cancelled_(oos_cbu_ddc), ddr_debit_(oos_cbu_ddd),
--     debit_cancelled_(oos_cbu_mdc), debit_fraud_(oos_cbu_mdf),
--     fraud_(oos_cbu_ddf)         → Core/Fraud OOS types appearing for
--                                    content agents; no content SLA defined
-- ============================================================

-- 1. Per-job grain: actual_time_seconds vs OLD SLA (inner join excludes no-SLA types)
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_job_base AS (
  WITH sla_old_map (job_type, sla_old_seconds) AS (
    SELECT * FROM (VALUES
      ('discovery',             10800),
      ('sync',                   3600),
      ('weduka_a',              14400),
      ('weduka_b',              21600),
      ('weduka_c',              28800),
      ('weduka_optimization',   86400),
      ('macros',                 1200),
      ('faq',                    1800),
      ('ar',                     1800),
      ('learning_material_a',   28800),
      ('learning_material_b',   43200),
      ('learning_material_c',   57600),
      ('purple_screens',        14400),
      ('purple_screens_review',  7200),
      ('weduka_comms',          14400),
      ('publish',                 600),
      ('emergency',              1800),
      ('projects_a',            28800),
      ('projects_b',            86400),
      ('projects_c',           144000),
      ('xpeer_xperience',       18000),
      ('deactivation',            600),
      ('weduka_bdx',            14400),
      ('sm_content',            57600),
      ('sm_comunidad_nu',        1200)
    ) AS t(job_type, sla_old_seconds)
  ),
  non_row_jobs AS (
    -- Non-macros/faq/ar: one "job" = one distinct content_id
    SELECT
      j.job_classification
      , j.agent
      , DATE_TRUNC('DAY', MIN(j.local_start_date)) AS start_date
      , SUM(j.net_time_spent_seconds)              AS actual_time_seconds
    FROM oos_jobs_ntpj AS j
    WHERE j.job_classification NOT IN ('macros', 'faq', 'ar')
      AND j.content_id IS NOT NULL
    GROUP BY j.job_classification, j.agent, j.content_id
  ),
  row_jobs AS (
    -- macros/faq/ar: one "job" = one source row
    SELECT
      job_classification
      , agent
      , DATE_TRUNC('DAY', local_start_date) AS start_date
      , net_time_spent_seconds              AS actual_time_seconds
    FROM oos_jobs_ntpj
    WHERE job_classification IN ('macros', 'faq', 'ar')
  ),
  all_jobs AS (
    SELECT * FROM non_row_jobs
    UNION ALL
    SELECT * FROM row_jobs
  )
  SELECT
    j.agent
    , j.start_date
    , s.sla_old_seconds
    , CASE WHEN j.actual_time_seconds <= s.sla_old_seconds THEN s.sla_old_seconds ELSE 0 END AS sla_old_seconds_met
  FROM all_jobs          AS j
  INNER JOIN sla_old_map AS s ON j.job_classification = s.job_type
);

-- 2. Aggregate per agent x day, attach hierarchy from agent_information
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_agent_base AS (
  SELECT
    j.agent
    , j.start_date               AS date
    , SUM(j.sla_old_seconds_met) AS numerator
    , SUM(j.sla_old_seconds)     AS denominator
    , b.xforce
    , b.xplead
    , b.squad
    , b.squad_district
  FROM ntpj_sla_old_job_base AS j
  LEFT JOIN agent_information AS b
    ON j.agent = b.agent
    AND DATE_TRUNC('MONTH', j.start_date) >= b.valid_from
    AND DATE_TRUNC('MONTH', j.start_date) < b.valid_to
  WHERE b.status = 'active'
    AND j.start_date >= '2025-12-01'
    AND j.start_date NOT IN ('2026-03-10', '2026-03-27', '2026-04-09')
  GROUP BY ALL
);

-- 3. Daily granularity
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('DAY', date)                            AS date_reference
    , 'day'                                              AS date_granularity
    , 'ntpj_sla_old'                                     AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM ntpj_sla_old_agent_base
  GROUP BY ALL
);

-- 4. Weekly granularity
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_weekly AS (
  SELECT
    agent
    , first_xforce           AS xforce
    , first_xplead           AS xplead
    , first_squad            AS squad
    , first_squad_district   AS squad_district
    , DATE_TRUNC('WEEK', date)                           AS date_reference
    , 'week'                                             AS date_granularity
    , 'ntpj_sla_old'                                     AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce)         OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead)         OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad)          OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_old_agent_base
    GROUP BY ALL
  )
  GROUP BY ALL
);

-- 5. Monthly granularity
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_monthly AS (
  SELECT
    agent
    , first_xforce           AS xforce
    , first_xplead           AS xplead
    , first_squad            AS squad
    , first_squad_district   AS squad_district
    , DATE_TRUNC('MONTH', date)                          AS date_reference
    , 'month'                                            AS date_granularity
    , 'ntpj_sla_old'                                     AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce)         OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead)         OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad)          OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_old_agent_base
    GROUP BY ALL
  )
  GROUP BY ALL
);

-- 6. Final standalone union — metric='ntpj_sla_old', same schema as ntpj_agents
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old AS (
  SELECT * FROM ntpj_sla_old_daily
  UNION ALL
  SELECT * FROM ntpj_sla_old_weekly
  UNION ALL
  SELECT * FROM ntpj_sla_old_monthly
);

-- SELECT * FROM ntpj_sla_old

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: ntpj_v3
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_ntpj_sla_old AS SELECT * FROM ntpj_sla_old;
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old AS SELECT * FROM usr.danielanzures.cont_temp_ntpj_sla_old;

-- [Promote] ntpj_sla_old → official ntpj with the original ntpj shape
-- Build the same output form as the original ntpj dataset, while keeping the
-- explicit old-SLA metrics under the ntpj_sla_old* names.

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_agents_general_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_sla_old_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM ntpj_sla_old_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_agents_general_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_sla_old_agents_general_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY date_reference ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM ntpj_sla_old_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_agents_general_quartile AS (
  SELECT * FROM ntpj_sla_old_agents_general_quartile_monthly
  UNION ALL
  SELECT * FROM ntpj_sla_old_agents_general_quartile_weekly
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_agents_team_quartile_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_sla_old_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM ntpj_sla_old_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_agents_team_quartile_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'ntpj_sla_old_agents_team_quartile' AS metric
    , NULL AS numerator
    , NULL AS denominator
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
  FROM ntpj_sla_old_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_agents_team_quartile AS (
  SELECT * FROM ntpj_sla_old_agents_team_quartile_monthly
  UNION ALL
  SELECT * FROM ntpj_sla_old_agents_team_quartile_weekly
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_sla_old_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) * 100 AS metric_value
  FROM ntpj_sla_old_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_xforces_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_sla_old_xforce' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 95 THEN agent END), COUNT(DISTINCT agent)) * 100 AS metric_value
  FROM ntpj_sla_old_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_xforces AS (
  SELECT * FROM ntpj_sla_old_xforces_monthly
  UNION ALL
  SELECT * FROM ntpj_sla_old_xforces_weekly
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_xpleads_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_sla_old_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END), COUNT(DISTINCT agent)) * 100 AS metric_value
  FROM ntpj_sla_old_monthly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'ntpj_sla_old_xplead' AS metric
    , COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , TRY_DIVIDE(COUNT(DISTINCT CASE WHEN metric_value >= 100 THEN agent END), COUNT(DISTINCT agent)) * 100 AS metric_value
  FROM ntpj_sla_old_weekly
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_xpleads AS (
  SELECT * FROM ntpj_sla_old_xpleads_monthly
  UNION ALL
  SELECT * FROM ntpj_sla_old_xpleads_weekly
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'ntpj_sla_old_squad' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
    FROM ntpj_sla_old_agent_base
    GROUP BY ALL
  )
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'ntpj_sla_old_district' AS metric
    , SUM(numerator) AS numerator
    , SUM(denominator) AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_old_agent_base
    GROUP BY ALL
  )
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_squad AS (
  SELECT * FROM ntpj_sla_old_squad_monthly
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_district AS (
  SELECT * FROM ntpj_sla_old_district_monthly
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old AS (
  SELECT * FROM usr.danielanzures.cont_temp_ntpj_sla_old
  UNION ALL
  SELECT * FROM ntpj_sla_old_agents_general_quartile
  UNION ALL
  SELECT * FROM ntpj_sla_old_agents_team_quartile
  UNION ALL
  SELECT * FROM ntpj_sla_old_xforces
  UNION ALL
  SELECT * FROM ntpj_sla_old_xpleads
  UNION ALL
  SELECT * FROM ntpj_sla_old_squad
  UNION ALL
  SELECT * FROM ntpj_sla_old_district
);

CREATE OR REPLACE TEMPORARY VIEW ntpj AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , CASE
        WHEN metric = 'ntpj_sla_old' THEN 'ntpj_agent'
        WHEN metric = 'ntpj_sla_old_agents_general_quartile' THEN 'ntpj_agents_general_quartile'
        WHEN metric = 'ntpj_sla_old_agents_team_quartile' THEN 'ntpj_agents_team_quartile'
        WHEN metric = 'ntpj_sla_old_xforce' THEN 'ntpj_xforce'
        WHEN metric = 'ntpj_sla_old_xplead' THEN 'ntpj_xplead'
        WHEN metric = 'ntpj_sla_old_squad' THEN 'ntpj_squad'
        WHEN metric = 'ntpj_sla_old_district' THEN 'ntpj_district'
      END AS metric
    , numerator
    , denominator
    , metric_value
  FROM ntpj_sla_old
);

-- COMMAND ----------

-- DBTITLE 1,NTPJ sla_new_v2 — SLA Partial Credit, New SLA (Agent Level)
-- ============================================================
-- NTPJ sla_new_v2 — SLA-Weighted Partial Credit, NEW SLA (Agent Level)
-- Variant of ntpj_sla_new: missed jobs contribute proportionally instead of 0.
--   met SLA:    contribution = sla_seconds (full credit, 100%)
--   missed SLA: contribution = GREATEST(0, 2*sla - actual) — linear decay to 0
--               e.g. SLA=8h, actual=10h → (16-10)=6 → 75% of sla_seconds
--               e.g. SLA=8h, actual=12h → (16-12)=4 → 50% of sla_seconds
--               Range: 100% at SLA boundary, 0% at 2×SLA or beyond, never negative.
-- SLA map, job grain, and agent join are identical to ntpj_sla_new.
-- ============================================================

-- 1. Per-job grain: partial credit for overruns (new SLA values)
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_v2_job_base AS (
  WITH sla_map (job_type, sla_seconds) AS (
    SELECT * FROM (VALUES
      ('discovery',             7200),
      ('sync',                  5400),
      ('weduka_a',              5400),
      ('weduka_b',             12600),
      ('weduka_c',             18000),
      ('weduka_optimization',  36000),
      ('macros',                1800),
      ('faq',                   2700),
      ('ar',                    2700),
      ('learning_material_a',  10800),
      ('learning_material_b',  18000),
      ('learning_material_c',  25200),
      ('purple_screens',        5400),
      ('purple_screens_review', 3600),
      ('weduka_comms',          5400),
      ('publish',               1200),
      ('emergency',             1800),
      ('projects_a',            7200),
      ('projects_b',           28800),
      ('projects_c',           57600),
      ('xpeer_xperience',      18000),
      ('deactivation',           300),
      ('weduka_bdx',            5400),
      ('sm_content',           28800),
      ('sm_comunidad_nu',       1800)
    ) AS t(job_type, sla_seconds)
  ),
  non_row_jobs AS (
    SELECT
      j.job_classification
      , j.agent
      , DATE_TRUNC('DAY', MIN(j.local_start_date)) AS start_date
      , SUM(j.net_time_spent_seconds)              AS actual_time_seconds
    FROM oos_jobs_ntpj AS j
    WHERE j.job_classification NOT IN ('macros', 'faq', 'ar')
      AND j.content_id IS NOT NULL
    GROUP BY j.job_classification, j.agent, j.content_id
  ),
  row_jobs AS (
    SELECT
      job_classification
      , agent
      , DATE_TRUNC('DAY', local_start_date) AS start_date
      , net_time_spent_seconds              AS actual_time_seconds
    FROM oos_jobs_ntpj
    WHERE job_classification IN ('macros', 'faq', 'ar')
  ),
  all_jobs AS (
    SELECT * FROM non_row_jobs
    UNION ALL
    SELECT * FROM row_jobs
  )
  SELECT
    j.agent
    , j.start_date
    , s.sla_seconds
    , CASE
        WHEN j.actual_time_seconds <= s.sla_seconds THEN s.sla_seconds
        ELSE GREATEST(0, 2 * s.sla_seconds - j.actual_time_seconds)  -- linear decay: 100% at SLA, 0% at 2×SLA
      END AS sla_seconds_met
  FROM all_jobs      AS j
  INNER JOIN sla_map AS s ON j.job_classification = s.job_type
);

-- 2. Aggregate per agent x day, attach hierarchy
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_v2_agent_base AS (
  SELECT
    j.agent
    , j.start_date               AS date
    , SUM(j.sla_seconds_met)     AS numerator
    , SUM(j.sla_seconds)         AS denominator
    , b.xforce
    , b.xplead
    , b.squad
    , b.squad_district
  FROM ntpj_sla_new_v2_job_base AS j
  LEFT JOIN agent_information AS b
    ON j.agent = b.agent
    AND DATE_TRUNC('MONTH', j.start_date) >= b.valid_from
    AND DATE_TRUNC('MONTH', j.start_date) < b.valid_to
  WHERE b.status = 'active'
    AND j.start_date >= '2025-12-01'
    AND j.start_date != '2026-03-10'
  GROUP BY ALL
);

-- 3. Daily
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_v2_daily AS (
  SELECT
    agent, xforce, xplead, squad, squad_district
    , DATE_TRUNC('DAY', date)                            AS date_reference
    , 'day'                                              AS date_granularity
    , 'ntpj_sla_new_v2'                                  AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM ntpj_sla_new_v2_agent_base
  GROUP BY ALL
);

-- 4. Weekly
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_v2_weekly AS (
  SELECT
    agent
    , first_xforce AS xforce, first_xplead AS xplead, first_squad AS squad, first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date)                           AS date_reference
    , 'week'                                             AS date_granularity
    , 'ntpj_sla_new_v2'                                  AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT *
      , FIRST_VALUE(xforce)         OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead)         OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad)          OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_new_v2_agent_base GROUP BY ALL
  ) GROUP BY ALL
);

-- 5. Monthly
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_v2_monthly AS (
  SELECT
    agent
    , first_xforce AS xforce, first_xplead AS xplead, first_squad AS squad, first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date)                          AS date_reference
    , 'month'                                            AS date_granularity
    , 'ntpj_sla_new_v2'                                  AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT *
      , FIRST_VALUE(xforce)         OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead)         OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad)          OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_new_v2_agent_base GROUP BY ALL
  ) GROUP BY ALL
);

-- 6. Final union
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_v2 AS (
  SELECT * FROM ntpj_sla_new_v2_daily
  UNION ALL
  SELECT * FROM ntpj_sla_new_v2_weekly
  UNION ALL
  SELECT * FROM ntpj_sla_new_v2_monthly
);

-- SELECT * FROM ntpj_sla_new_v2

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: ntpj_sla_new_v2
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_ntpj_sla_new_v2 AS SELECT * FROM ntpj_sla_new_v2;
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_new_v2 AS SELECT * FROM usr.danielanzures.cont_temp_ntpj_sla_new_v2;

-- COMMAND ----------

-- DBTITLE 1,NTPJ sla_old_v2 — SLA Partial Credit, Old SLA (Agent Level)
-- ============================================================
-- NTPJ sla_old_v2 — SLA-Weighted Partial Credit, OLD SLA (Agent Level)
-- Variant of ntpj_sla_old: missed jobs contribute proportionally instead of 0.
--   met SLA:    contribution = sla_seconds (full credit, 100%)
--   missed SLA: contribution = GREATEST(0, 2*sla - actual) — linear decay to 0
--               e.g. SLA=8h, actual=10h → (16-10)=6 → 75% of sla_seconds
--               e.g. SLA=8h, actual=12h → (16-12)=4 → 50% of sla_seconds
--               Range: 100% at SLA boundary, 0% at 2×SLA or beyond, never negative.
-- SLA map, job grain, and agent join are identical to ntpj_sla_old.
-- ============================================================

-- 1. Per-job grain: partial credit for overruns (old SLA values)
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_v2_job_base AS (
  WITH sla_old_map (job_type, sla_old_seconds) AS (
    SELECT * FROM (VALUES
      ('discovery',             10800),
      ('sync',                   3600),
      ('weduka_a',              14400),
      ('weduka_b',              21600),
      ('weduka_c',              28800),
      ('weduka_optimization',   86400),
      ('macros',                 1200),
      ('faq',                    1800),
      ('ar',                     1800),
      ('learning_material_a',   28800),
      ('learning_material_b',   43200),
      ('learning_material_c',   57600),
      ('purple_screens',        14400),
      ('purple_screens_review',  7200),
      ('weduka_comms',          14400),
      ('publish',                 600),
      ('emergency',              1800),
      ('projects_a',            28800),
      ('projects_b',            86400),
      ('projects_c',           144000),
      ('xpeer_xperience',       18000),
      ('deactivation',            600),
      ('weduka_bdx',            14400),
      ('sm_content',            57600),
      ('sm_comunidad_nu',        1200)
    ) AS t(job_type, sla_old_seconds)
  ),
  non_row_jobs AS (
    SELECT
      j.job_classification
      , j.agent
      , DATE_TRUNC('DAY', MIN(j.local_start_date)) AS start_date
      , SUM(j.net_time_spent_seconds)              AS actual_time_seconds
    FROM oos_jobs_ntpj AS j
    WHERE j.job_classification NOT IN ('macros', 'faq', 'ar')
      AND j.content_id IS NOT NULL
    GROUP BY j.job_classification, j.agent, j.content_id
  ),
  row_jobs AS (
    SELECT
      job_classification
      , agent
      , DATE_TRUNC('DAY', local_start_date) AS start_date
      , net_time_spent_seconds              AS actual_time_seconds
    FROM oos_jobs_ntpj
    WHERE job_classification IN ('macros', 'faq', 'ar')
  ),
  all_jobs AS (
    SELECT * FROM non_row_jobs
    UNION ALL
    SELECT * FROM row_jobs
  )
  SELECT
    j.agent
    , j.start_date
    , s.sla_old_seconds
    , CASE
        WHEN j.actual_time_seconds <= s.sla_old_seconds THEN s.sla_old_seconds
        ELSE GREATEST(0, 2 * s.sla_old_seconds - j.actual_time_seconds)  -- linear decay: 100% at SLA, 0% at 2×SLA
      END AS sla_seconds_met
  FROM all_jobs          AS j
  INNER JOIN sla_old_map AS s ON j.job_classification = s.job_type
);

-- 2. Aggregate per agent x day, attach hierarchy
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_v2_agent_base AS (
  SELECT
    j.agent
    , j.start_date               AS date
    , SUM(j.sla_seconds_met)     AS numerator
    , SUM(j.sla_old_seconds)     AS denominator
    , b.xforce
    , b.xplead
    , b.squad
    , b.squad_district
  FROM ntpj_sla_old_v2_job_base AS j
  LEFT JOIN agent_information AS b
    ON j.agent = b.agent
    AND DATE_TRUNC('MONTH', j.start_date) >= b.valid_from
    AND DATE_TRUNC('MONTH', j.start_date) < b.valid_to
  WHERE b.status = 'active'
    AND j.start_date >= '2025-12-01'
    AND j.start_date != '2026-03-10'
  GROUP BY ALL
);

-- 3. Daily
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_v2_daily AS (
  SELECT
    agent, xforce, xplead, squad, squad_district
    , DATE_TRUNC('DAY', date)                            AS date_reference
    , 'day'                                              AS date_granularity
    , 'ntpj_sla_old_v2'                                  AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM ntpj_sla_old_v2_agent_base
  GROUP BY ALL
);

-- 4. Weekly
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_v2_weekly AS (
  SELECT
    agent
    , first_xforce AS xforce, first_xplead AS xplead, first_squad AS squad, first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date)                           AS date_reference
    , 'week'                                             AS date_granularity
    , 'ntpj_sla_old_v2'                                  AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT *
      , FIRST_VALUE(xforce)         OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead)         OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad)          OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_old_v2_agent_base GROUP BY ALL
  ) GROUP BY ALL
);

-- 5. Monthly
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_v2_monthly AS (
  SELECT
    agent
    , first_xforce AS xforce, first_xplead AS xplead, first_squad AS squad, first_squad_district AS squad_district
    , DATE_TRUNC('MONTH', date)                          AS date_reference
    , 'month'                                            AS date_granularity
    , 'ntpj_sla_old_v2'                                  AS metric
    , SUM(numerator)                                     AS numerator
    , SUM(denominator)                                   AS denominator
    , TRY_DIVIDE(SUM(numerator), SUM(denominator)) * 100 AS metric_value
  FROM (
    SELECT *
      , FIRST_VALUE(xforce)         OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xforce
      , FIRST_VALUE(xplead)         OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_xplead
      , FIRST_VALUE(squad)          OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date) ORDER BY date DESC) AS first_squad_district
    FROM ntpj_sla_old_v2_agent_base GROUP BY ALL
  ) GROUP BY ALL
);

-- 6. Final union
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_v2 AS (
  SELECT * FROM ntpj_sla_old_v2_daily
  UNION ALL
  SELECT * FROM ntpj_sla_old_v2_weekly
  UNION ALL
  SELECT * FROM ntpj_sla_old_v2_monthly
);

-- SELECT * FROM ntpj_sla_old_v2

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: ntpj_sla_old_v2
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_ntpj_sla_old_v2 AS SELECT * FROM ntpj_sla_old_v2;
CREATE OR REPLACE TEMPORARY VIEW ntpj_sla_old_v2 AS SELECT * FROM usr.danielanzures.cont_temp_ntpj_sla_old_v2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Normalized Occupancy

-- COMMAND ----------

-- DBTITLE 1,Normalized Occupancy Base
CREATE OR REPLACE TEMPORARY VIEW dime_table_occupancy AS(
  SELECT
    agent
    , agent_dime_squad AS squad
    , dime_date AS date
    , REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)  AS agent_name_extracted
    , unix_timestamp(local_timestamp_dime_slot_starts_at) AS slot_start
    , unix_timestamp(local_timestamp_dime_slot_starts_at) + (30 * 60) AS slot_end
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
        WHEN activity_type_required = 'dime_invalid_notation'
          THEN 'oos'
        ELSE activity_type_required
      END AS activity_type_required
    , dimensioned_activity
  FROM agent_dimensioned_activities_extended
  WHERE
      affiliation = 'nubank'
      AND dime_date >= '2024-12-30'
      AND activity_type_required IS NOT NULL
      AND activity_type_required NOT IN ('lunch_break', 'time_off', 'shrinkage')
      AND dimensioned_activity NOT IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle', 'Licencia', 'Vacacion')
      AND agent_dime_squad IS NOT NULL
      AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote', 'social')
      AND dime_date <= DATE_SUB(DATE_TRUNC('WEEK', CURRENT_DATE()), 1)
      AND dime_date != '2026-03-10'
      AND LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM content_agents)
);

CREATE OR REPLACE TEMPORARY VIEW jobs_shuffle AS (
  SELECT
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , DATE(local_start_time) AS date
    , activity_type
    , UNIX_TIMESTAMP(local_start_time) AS activity_start
    , UNIX_TIMESTAMP(local_stop_time) AS activity_end
    , net_time_spent
  FROM etl.mx__dataset.ops_canonical_time_spent_activities
  WHERE status IN ('finished', 'transferred', 'skipped')
    AND actor_affiliation = 'nubank'
    AND local_start_time >= '2025-01-01'
    AND DATE(local_start_time) != '2026-03-10'
    AND LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM content_agents)
);

-- [Correction] luis.contreras laptop clock was behind on certain periods:
--   2026-01-01 – 2026-03-08: 2 hours behind → shift +2 h
--   2026-03-09 – 2026-05-19: 1 hour behind  → shift +1 h
--   2026-05-20 – present:    correct          → no shift
CREATE OR REPLACE TEMPORARY VIEW jobs_oos AS(
  WITH raw AS (
    SELECT
      LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
      , local_start_date
      , local_stop_date
      , net_time_spent_seconds
    FROM etl.mx__dataset.taskmaster_consolidated_registry
    WHERE local_start_date >= '2025-01-01'
      AND DATE(local_start_date) != '2026-03-10'
      AND LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM content_agents)
  ),
  corrected AS (
    SELECT
      agent
      , CASE
          WHEN agent = 'luis.contreras' AND local_start_date >= '2026-01-01' AND local_start_date < '2026-03-09'
            THEN local_start_date + INTERVAL 2 HOURS
          WHEN agent = 'luis.contreras' AND local_start_date >= '2026-03-09' AND local_start_date < '2026-05-20'
            THEN local_start_date + INTERVAL 1 HOUR
          ELSE local_start_date
        END AS adj_start
      , CASE
          WHEN agent = 'luis.contreras' AND local_start_date >= '2026-01-01' AND local_start_date < '2026-03-09'
            THEN local_stop_date + INTERVAL 2 HOURS
          WHEN agent = 'luis.contreras' AND local_start_date >= '2026-03-09' AND local_start_date < '2026-05-20'
            THEN local_stop_date + INTERVAL 1 HOUR
          ELSE local_stop_date
        END AS adj_stop
      , net_time_spent_seconds
    FROM raw
  )
  SELECT
    agent
    , DATE(adj_start)             AS date
    , 'oos'                       AS activity_type
    , UNIX_TIMESTAMP(adj_start)   AS activity_start
    , UNIX_TIMESTAMP(adj_stop)    AS activity_end
    , net_time_spent_seconds      AS net_time_spent
  FROM corrected
);

CREATE OR REPLACE TEMPORARY VIEW jobs_join AS(
  SELECT * FROM jobs_shuffle
  UNION ALL
  SELECT * FROM jobs_oos
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
  WHERE activity_type_required != 'time_off'
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
  LEFT JOIN jobs_join AS b
    ON a.agent = b.agent
    AND a.date = b.date
    AND ((b.activity_start >= a.slot_start AND b.activity_start < a.slot_end)
      OR (b.activity_end > a.slot_start AND b.activity_end <= a.slot_end)
      OR (b.activity_start < a.slot_start AND b.activity_end >= a.slot_end))
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_base AS(
  SELECT
    agent
    , squad
    , date
    , slot_start
    , slot_end
    , activity_type_required
    -- , job_activity_type
    , CASE 
        WHEN activity_type_required = job_activity_type
          THEN 1
          ELSE 0
        END AS activity_occuped
    , CASE
        WHEN job_start >= slot_start
          THEN job_start
        WHEN job_start < slot_start
          THEN slot_start
        END AS job_start
    , CASE
        WHEN job_end <= slot_end
          THEN job_end
        WHEN job_end > slot_end
          THEN slot_end
        END AS job_end
    , COALESCE(
        MAX(
          CASE
            WHEN job_end <= slot_end THEN job_end
            WHEN job_end > slot_end THEN slot_end
          END
        ) OVER (
          PARTITION BY agent, squad, date, slot_start, activity_type_required, job_activity_type
          ORDER BY job_start, job_end
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        slot_start
      ) AS prev_max_end
  FROM slot_jobs
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_agg AS(
  SELECT
    REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
    , squad AS old_squad
    , date
    , slot_start
    , SUM(
        CASE WHEN activity_occuped = 1
          THEN GREATEST(0, job_end - GREATEST(job_start, prev_max_end))
        END
      ) AS occupancy_time
    , 1800 AS slot_duration
    , activity_type_required
  FROM occupancy_base
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_agents_information AS (
  SELECT
    a.*
    , b.xplead
    , b.xforce
    , b.squad_district
    , b.squad
  FROM occupancy_agg AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) >= b.valid_from
    AND DATE_TRUNC('MONTH', a.date) < b.valid_to
  WHERE b.status = 'active'
    AND a.date >= '2025-12-01'
    AND a.date != '2026-03-10'
    AND (a.agent NOT IN ('jose.velez', 'carlos.gonzalez', 'jorge.ortega', 'luisa.castaneda', 'janet.castro', 'karen.ortega')
      AND a.date NOT IN ('2026-03-25', '2026-03-26', '2026-03-27'))
    AND a.date NOT IN ('2026-03-27', '2026-04-09')
);

CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy_benchmark AS (
  SELECT
    DATE_TRUNC('MONTH', date) AS month
    , squad_district
    , TRY_DIVIDE(SUM(occupancy_time), SUM(job_time)) AS occupancy_monthly
    , squad
  FROM (
    SELECT
      agent
      , squad_district
      , squad
      , slot_start
      , date
      , LEAST(COALESCE(SUM(occupancy_time), 0), 1800) AS occupancy_time
      , SUM(slot_duration) AS job_time
    FROM occupancy_agents_information
    GROUP BY ALL
  )
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy AS (
  SELECT
    a.*
    , AVG(b.occupancy_monthly) AS occupancy_exp
  FROM occupancy_agents_information AS a
  LEFT JOIN normalized_occupancy_benchmark AS b
    ON a.squad_district = b.squad_district
    AND DATE_TRUNC('MONTH', a.date) = b.month
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
    , date
    , LEAST(COALESCE(SUM(occupancy_time), 0), 1800) AS occupancy_time
    , SUM(slot_duration) AS job_time
    , occupancy_exp AS occupancy_exp
  FROM normalized_occupancy
  WHERE date >= '2026-03-01'
    AND date != '2026-03-10'
  GROUP BY ALL
);

-- SELECT * FROM normalized_occupancy_final

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: normalized_occupancy
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_normalized_occupancy AS SELECT * FROM normalized_occupancy;
CREATE OR REPLACE TEMPORARY VIEW normalized_occupancy AS SELECT * FROM usr.danielanzures.cont_temp_normalized_occupancy;

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
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'nocc_agent' AS metric
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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_nocc AS SELECT * FROM nocc;
CREATE OR REPLACE TEMPORARY VIEW nocc AS SELECT * FROM usr.danielanzures.cont_temp_nocc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quality (CSAT) Metric

-- COMMAND ----------

-- DBTITLE 1,Quality Base
CREATE OR REPLACE TEMPORARY VIEW qa_base AS(
  SELECT
    timestamp
    , REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS requested_by
    , TO_TIMESTAMP(timestamp, 'M/d/yyyy H:mm:ss') - INTERVAL 1 MONTH AS date_reference
    , CASE
        WHEN comprension >= 4 THEN 1
        ELSE 0
      END AS comprension_promoter
    , CASE
        WHEN comunicacion >= 4 THEN 1
        ELSE 0
      END AS comunicacion_promoter
    , CASE
        WHEN calidad >= 4 THEN 1
        ELSE 0
      END AS calidad_promoter
    , CASE
        WHEN tiempo >= 4 THEN 1
        ELSE 0
      END AS tiempo_promoter
    , CASE
        WHEN expectativas >= 4 THEN 1
        ELSE 0
      END AS expectativas_promoter
    , 5 AS number_of_questions
    , CASE 
        WHEN squad IN ('E.M.I.', 'GENERAL (CHANNEL SOLUTIONS, PLANNING, SERVICE EXCELLENCE, QA, OPS DEFENSE)') THEN 'emi_general'
        ELSE LOWER(squad)
      END AS squad
  FROM gsheets.sheets.mx_content_csat_daniel_anz_temp
);

CREATE OR REPLACE TEMPORARY VIEW qa_base_agg AS(
    SELECT
      date_reference
      , requested_by
      , squad AS target_squad
      , comprension_promoter + comunicacion_promoter + calidad_promoter + tiempo_promoter + expectativas_promoter AS promoters
      , number_of_questions
    FROM qa_base
);

CREATE OR REPLACE TEMPORARY VIEW qa_base_final AS(
  SELECT
    a.*
    , b.agent
    , b.xplead
    , b.xforce
    , b.squad_district
    , b.squad
  FROM qa_base_agg AS a
  LEFT JOIN agent_information AS b
    ON a.target_squad = b.target_squad
    AND DATE_TRUNC('MONTH', a.date_reference) >= b.valid_from
    AND DATE_TRUNC('MONTH', a.date_reference) < b.valid_to
  WHERE b.status = 'active'
    AND a.date_reference >= '2025-12-01'
);

-- SELECT *
-- FROM qa_base_final

-- COMMAND ----------

-- DBTITLE 1,QA Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_agents_daily AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , DATE_TRUNC('DAY', date_reference) AS date_reference
    , 'day' AS date_granularity
    , 'qa_score_agent' AS metric
    , SUM(promoters) AS numerator
    , SUM(number_of_questions) AS denominator
    , TRY_DIVIDE(SUM(promoters), SUM(number_of_questions)) * 100 AS metric_value 
  FROM qa_base_final
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW qa_agents_weekly AS (
  SELECT
    agent
    , first_xforce AS xforce
    , first_xplead AS xplead
    , first_squad AS squad
    , first_squad_district AS squad_district
    , DATE_TRUNC('WEEK', date_reference) AS date_reference
    , 'week' AS date_granularity
    , 'qa_score_agent' AS metric
    , SUM(promoters) AS numerator
    , SUM(number_of_questions) AS denominator
    , TRY_DIVIDE(SUM(promoters), SUM(number_of_questions)) * 100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_squad_district
    FROM qa_base_final
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
    , DATE_TRUNC('MONTH', date_reference) AS date_reference
    , 'month' AS date_granularity
    , 'qa_score_agent' AS metric
    , SUM(promoters) AS numerator
    , SUM(number_of_questions) AS denominator
    , TRY_DIVIDE(SUM(promoters), SUM(number_of_questions)) * 100 AS metric_value
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_squad_district
    FROM qa_base_final
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

-- SELECT * FROM qa_agents_monthly

-- COMMAND ----------

-- DBTITLE 1,[Manual Fix] February 2026 CSAT Results
-- [Manual Fix] January 2026 CSAT results injected manually
-- Materialize original qa_agents_monthly to Delta to break circular view reference
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_qa_agents_monthly AS SELECT * FROM qa_agents_monthly;

-- Manual CSAT results for February 2026
CREATE OR REPLACE TEMPORARY VIEW qa_feb_2026_manual AS (
  SELECT
    v.agent
    , v.xforce
    , b.xplead
    , b.squad
    , b.squad_district
    , CAST('2026-02-01' AS TIMESTAMP) AS date_reference
    , 'month' AS date_granularity
    , 'qa_score_agent' AS metric
    , CAST(v.metric_value AS DOUBLE) AS numerator
    , CAST(100 AS DOUBLE) AS denominator
    , v.metric_value
  FROM (VALUES
    ('jesus.morales', 'carolina.diaz', 100.00),
    ('luis.contreras', 'carolina.diaz', 100.00),
    ('alejandra.monroy', 'cecilia.arjona', 100.00),
    ('luis.delvalle', 'cecilia.arjona', 100.00),
    ('erick.zeron', 'fatima.pocoroba', 100.00),
    ('shadia.hasbun', 'fatima.pocoroba', 100.00),
    ('jimena.valencia', 'hilda.gomez', 100.00),
    ('omar.ramirez', 'hilda.gomez', 100.00),
    ('alejandra.erazo', 'karina.gonzalez', 100.00),
    ('aura.olvera', 'karina.gonzalez', 100.00),
    ('elias.caudillo', 'karina.gonzalez', 100.00),
    ('eva.triay', 'karina.gonzalez', 100.00),
    ('mayda.vital', 'karina.gonzalez', 100.00),
    ('ariana.cortes', 'montserrat.huerta', 100.00),
    ('isaac.espinoza', 'montserrat.huerta', 100.00),
    ('kenia.hernandez', 'montserrat.huerta', 100.00),
    ('luis.rosario', 'montserrat.huerta', 66.67)
  ) AS v(agent, xforce, metric_value)
  LEFT JOIN agent_information AS b
    ON v.agent = b.agent
    AND CAST('2026-02-01' AS TIMESTAMP) >= b.valid_from
    AND CAST('2026-02-01' AS TIMESTAMP) < b.valid_to
);

-- Override qa_agents_monthly to include manual January & February 2026 data
CREATE OR REPLACE TEMPORARY VIEW qa_agents_monthly AS (
  SELECT * FROM usr.danielanzures.cont_temp_qa_agents_monthly
  WHERE date_reference < '2026-01-01' OR date_reference >= '2026-02-01'
  UNION ALL
  SELECT * FROM qa_feb_2026_manual
);

-- Re-create qa_score_agents to pick up updated monthly data
CREATE OR REPLACE TEMPORARY VIEW qa_score_agents AS (
  SELECT * FROM qa_agents_daily
  UNION ALL
  SELECT * FROM qa_agents_weekly
  UNION ALL
  SELECT * FROM qa_agents_monthly
);

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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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

-- DBTITLE 1,QA Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW qa_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , first_squad AS squad
    , NULL AS squad_district
    , DATE_TRUNC('MONTH', date_reference) AS date_reference
    , 'month' AS date_granularity
    , 'qa_squad' AS metric
    , SUM(promoters) AS numerator
    , SUM(number_of_questions) AS denominator
    , TRY_DIVIDE(SUM(promoters), SUM(number_of_questions)) AS metric_value 
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_squad_district
    FROM qa_base_final
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
    , DATE_TRUNC('WEEK', date_reference) AS date_reference
    , 'week' AS date_granularity
    , 'qa_squad' AS metric
    , SUM(promoters) AS numerator
    , SUM(number_of_questions) AS denominator
    , TRY_DIVIDE(SUM(promoters), SUM(number_of_questions)) AS metric_value 
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_squad_district
    FROM qa_base_final
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
    , DATE_TRUNC('MONTH', date_reference) AS date_reference
    , 'month' AS date_granularity
    , 'qa_district' AS metric
    , SUM(promoters) AS numerator
    , SUM(number_of_questions) AS denominator
    , TRY_DIVIDE(SUM(promoters), SUM(number_of_questions)) AS metric_value 
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('MONTH', date_reference) ORDER BY date_reference DESC) AS first_squad_district
    FROM qa_base_final
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
    , DATE_TRUNC('WEEK', date_reference) AS date_reference
    , 'week' AS date_granularity
    , 'qa_district' AS metric
    , SUM(promoters) AS numerator
    , SUM(number_of_questions) AS denominator
    , TRY_DIVIDE(SUM(promoters), SUM(number_of_questions)) AS metric_value 
  FROM (
    SELECT
      *
      , FIRST_VALUE(xforce) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_xforce
      , FIRST_VALUE(xplead) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_xplead
      , FIRST_VALUE(squad) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_squad
      , FIRST_VALUE(squad_district) OVER (PARTITION BY agent, DATE_TRUNC('WEEK', date_reference) ORDER BY date_reference DESC) AS first_squad_district
    FROM qa_base_final
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
)

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: quality
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_quality AS SELECT * FROM quality;
CREATE OR REPLACE TEMPORARY VIEW quality AS SELECT * FROM usr.danielanzures.cont_temp_quality;

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
    , ntpj
    , CASE
        WHEN nocc IS NULL THEN NULL
        WHEN nocc >= 100 THEN 100
        WHEN nocc <= 100 THEN nocc
        ELSE 0
      END AS nocc
    , quality
    FROM index_agents_base
);

-- SELECT * FROM index_agents_final

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Materialize: index_agents_final
-- [Temp Fix] Materialize reused view(s) to Delta and re-point the temp view at
-- the table, so downstream cells read cached data instead of re-deriving the
-- full lineage. Produces identical rows.
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_index_agents_final AS SELECT * FROM index_agents_final;
CREATE OR REPLACE TEMPORARY VIEW index_agents_final AS SELECT * FROM usr.danielanzures.cont_temp_index_agents_final;

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
        WHEN date_reference <= DATE '2026-02-01' AND ntpj IS NULL
          THEN adherence
        WHEN date_reference <= DATE '2026-02-01'
          THEN (adherence + ntpj)
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL AND ntpj IS NULL
          THEN (adherence + nocc)
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc)
        WHEN date_reference >= DATE '2026-03-01' AND ntpj IS NULL
          THEN (adherence + nocc + quality)
        ELSE (adherence + ntpj + nocc + quality) 
      END AS numerator
    , CASE
        WHEN date_reference <= DATE '2026-02-01' AND ntpj IS NULL
          THEN 100
        WHEN date_reference <= DATE '2026-02-01'
          THEN 200
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL AND ntpj IS NULL
          THEN 200
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL
          THEN 300
        WHEN date_reference >= DATE '2026-03-01' AND ntpj IS NULL
          THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN date_reference <= DATE '2026-02-01' AND ntpj IS NULL
          THEN adherence / 1
        WHEN date_reference <= DATE '2026-02-01'
          THEN (adherence + ntpj) / 2
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL AND ntpj IS NULL
          THEN (adherence + nocc) / 2
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc) / 3
        WHEN date_reference >= DATE '2026-03-01' AND ntpj IS NULL
          THEN (adherence + nocc + quality) / 3
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
        WHEN date_reference <= DATE '2026-02-28' AND ntpj IS NULL
          THEN adherence
        WHEN date_reference <= DATE '2026-02-28'
          THEN (adherence + ntpj)
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL AND ntpj IS NULL
          THEN (adherence + nocc)
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc)
        WHEN date_reference >= DATE '2026-03-01' AND ntpj IS NULL
          THEN (adherence + nocc + quality)
        ELSE (adherence + ntpj + nocc + quality) 
      END AS numerator
    , CASE
        WHEN date_reference <= DATE '2026-02-28' AND ntpj IS NULL
          THEN 100
        WHEN date_reference <= DATE '2026-02-28'
          THEN 200
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL AND ntpj IS NULL
          THEN 200
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL
          THEN 300
        WHEN date_reference >= DATE '2026-03-01' AND ntpj IS NULL
          THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN date_reference <= DATE '2026-02-28' AND ntpj IS NULL
          THEN adherence / 1
        WHEN date_reference <= DATE '2026-02-28'
          THEN (adherence + ntpj) / 2
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL AND ntpj IS NULL
          THEN (adherence + nocc) / 2
        WHEN date_reference >= DATE '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc) / 3
        WHEN date_reference >= DATE '2026-03-01' AND ntpj IS NULL
          THEN (adherence + nocc + quality) / 3
        ELSE (adherence + ntpj + nocc + quality) / 4
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
CREATE OR REPLACE TABLE usr.danielanzures.cont_temp_index_agents AS SELECT * FROM index_agents;
CREATE OR REPLACE TEMPORARY VIEW index_agents AS SELECT * FROM usr.danielanzures.cont_temp_index_agents;

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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , NTILE(4) OVER (PARTITION BY (date_reference, xplead, squad) ORDER BY ANY_VALUE(metric_value) DESC) AS metric_value
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
    , b.metric_value AS ntpj
    , c.metric_value AS nocc
    -- , d.metric_value AS quality
  FROM adherence AS a
  LEFT JOIN ntpj AS b
    ON a.agent = b.agent
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'ntpj_squad'
  LEFT JOIN nocc AS c
    ON a.agent = c.agent
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_squad'
  -- LEFT JOIN quality AS d
  --   ON a.agent = d.agent
  --   AND a.date_reference = d.date_reference
  --   AND a.date_granularity = d.date_granularity
  --   AND d.metric = 'qa_score_squad'
  WHERE a.date_granularity IN ('week', 'month')
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
        WHEN ntpj <= 100 THEN 100
        WHEN ntpj > 100 AND ntpj <= 200 THEN (200 - ntpj)
        WHEN ntpj > 200 THEN 0
        ELSE 0
      END AS ntpj
    , CASE
        WHEN nocc >= 100 THEN 100
        WHEN nocc <= 100 THEN nocc
        ELSE 0
      END AS nocc
    -- , COALESCE(quality, 0) AS quality
    FROM index_agents_squad_base
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_squad_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent_squad' AS metric
   , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence + ntpj) 
        ELSE (adherence + ntpj + nocc) 
      END AS numerator
    , CASE
        WHEN date_reference <= '2026-02-01' THEN 200
        ELSE 300 
      END AS denominator
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence + ntpj) / 2
        ELSE (adherence + ntpj + nocc) / 3
      END AS metric_value
  FROM index_agents_squad_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_squad_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent_squad' AS metric
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence + ntpj) 
        ELSE (adherence + ntpj + nocc) 
      END AS numerator
    , CASE
        WHEN date_reference <= '2026-02-01' THEN 200
        ELSE 300 
      END AS denominator
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence + ntpj) / 2
        ELSE (adherence + ntpj + nocc) / 3
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
    , b.metric_value AS ntpj
    , c.metric_value AS nocc
    -- , d.metric_value AS quality
  FROM adherence AS a
  LEFT JOIN ntpj AS b
    ON a.agent = b.agent
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'ntpj_district'
  LEFT JOIN nocc AS c
    ON a.agent = c.agent
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
    AND c.metric = 'nocc_district'
  -- LEFT JOIN quality AS d
  --   ON a.agent = d.agent
  --   AND a.date_reference = d.date_reference
  --   AND a.date_granularity = d.date_granularity
  --   AND d.metric = 'qa_score_district'
  WHERE a.date_granularity IN ('week', 'month')
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
        WHEN ntpj <= 100 THEN 100
        WHEN ntpj > 100 AND ntpj <= 200 THEN (200 - ntpj)
        WHEN ntpj > 200 THEN 0
        ELSE 0
      END AS ntpj
    , CASE
        WHEN nocc >= 100 THEN 100
        WHEN nocc <= 100 THEN nocc
        ELSE 0
      END AS nocc
    -- , COALESCE(quality, 0) AS quality
    FROM index_agents_district_base
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_district_monthly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent_district' AS metric
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence + ntpj) 
        ELSE (adherence + ntpj + nocc) 
      END AS numerator
    , CASE
        WHEN date_reference <= '2026-02-01' THEN 200
        ELSE 300 
      END AS denominator
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence + ntpj) / 2
        ELSE (adherence + ntpj + nocc) / 3
      END AS metric_value
  FROM index_agents_district_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_agents_district_weekly AS (
  SELECT
    agent
    , xforce
    , xplead
    , squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_agent_district' AS metric
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence + ntpj) 
        ELSE (adherence + ntpj + nocc) 
      END AS numerator
    , CASE
        WHEN date_reference <= '2026-02-01' THEN 200
        ELSE 300 
      END AS denominator
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence + ntpj) / 2
        ELSE (adherence + ntpj + nocc) / 3
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
-- MAGIC #XForces Metrics

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
    , dimensioned_activity
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE affiliation = 'nubank'
    AND dime_date >= '2025-01-01'
    AND activity_type_required IS NOT NULL
    AND activity_type_required NOT IN ('lunch_break')
    AND agent_dime_squad IS NOT NULL
    AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote')
    AND dime_date != '2026-03-10'
    AND LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) IN (SELECT agent FROM content_agents)
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_final AS(
  SELECT
    a.*
    , COUNT(CASE
        WHEN a.activity_type_required IN ('shrinkage', 'timeoff') THEN 1
        WHEN a.activity_type_required = 'dime_invalid_notation' AND a.dimensioned_activity IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle', 'Licencia', 'Vacacion') THEN 1
      END) AS shrinkage_slot
    , COUNT(CASE WHEN a.activity_type_required != 'dime_invalid_notation' THEN 1 END) AS required_slot
    , b.xplead
    , b.xforce
    , b.squad_district
    -- , b.shift
    , b.squad
  FROM shrinkage_base AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) >= b.valid_from
    AND DATE_TRUNC('MONTH', a.date) < b.valid_to
  WHERE b.status = 'active'
    AND a.date >= '2025-12-01'
    AND a.date != '2026-03-10'
  GROUP BY ALL
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
    , DATE_TRUNC('MONTH', date) AS date_reference
    , 'month' AS date_granularity
    , 'shrinkage_xplead' AS metric
    , SUM(shrinkage_slot) AS numerator
    , SUM(required_slot) AS denominator
    , TRY_DIVIDE(SUM(shrinkage_slot), SUM(required_slot)) * 100 AS metric_value
  FROM shrinkage_final
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_xpleads_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , DATE_TRUNC('WEEK', date) AS date_reference
    , 'week' AS date_granularity
    , 'shrinkage_xplead' AS metric
    , SUM(shrinkage_slot) AS numerator
    , SUM(required_slot) AS denominator
    , TRY_DIVIDE(SUM(shrinkage_slot), SUM(required_slot)) * 100 AS metric_value
  FROM shrinkage_final
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
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'shrinkage_squad' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM shrinkage_xforces_monthly
  WHERE metric = 'shrinkage_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'shrinkage_squad' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM shrinkage_xforces_weekly
  WHERE metric = 'shrinkage_xforce'
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
    , squad_district
    , date_reference
    , date_granularity
    , 'shrinkage_district' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM shrinkage_xforces_monthly
  WHERE metric = 'shrinkage_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'shrinkage_district' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM shrinkage_xforces_weekly
  WHERE metric = 'shrinkage_xforce'
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
    , (COALESCE(adherence_in_target, 0) + COALESCE(ntpj_in_target, 0) + COALESCE(nocc_in_target, 0) + COALESCE(qa_in_target, 0)) AS xpeers_in_target
    , (COALESCE(adherence_xpeers, 0) + COALESCE(ntpj_xpeers, 0) + COALESCE(nocc_xpeers, 0) + COALESCE(qa_xpeers, 0)) AS xpeers
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM xpeers_in_target_xforces_monthly
  WHERE metric = 'xpeers_in_target_xforce'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM xpeers_in_target_xforces_weekly
  WHERE metric = 'xpeers_in_target_xforce'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM xpeers_in_target_xforces_monthly
  WHERE metric = 'xpeers_in_target_xforce'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM xpeers_in_target_xforces_weekly
  WHERE metric = 'xpeers_in_target_xforce'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_agent_monthly
  WHERE metric = 'average_index_agent'
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
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_agent_weekly
  WHERE metric = 'average_index_agent'
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
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_agent_monthly
  WHERE metric = 'average_index_agent'
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
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_agent_weekly
  WHERE metric = 'average_index_agent'
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
        WHEN DATE_TRUNC('MONTH', a.date_reference) >= DATE_TRUNC('MONTH', b.valid_from) 
          AND DATE_TRUNC('MONTH', a.date_reference) <= (DATE_TRUNC('MONTH', b.valid_from) + INTERVAL 2 MONTH)
          THEN 'nuvinho'
        ELSE 'old'
      END AS nuvinho
  FROM index_agents_monthly AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date_reference) >= b.valid_from
    AND DATE_TRUNC('MONTH', a.date_reference) < b.valid_to
  WHERE a.date_reference >= '2025-12-01'
);

CREATE OR REPLACE TEMPORARY VIEW nuvinhos_performance_final AS(
  SELECT
    xforce
    , xplead
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
    , nuvinhos_average AS numerator
    , old_average AS denominator
    , TRY_DIVIDE(nuvinhos_average, old_average) * 100 AS metric_value
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
    , nuvinhos_average AS numerator
    , old_average AS denominator
    , TRY_DIVIDE(nuvinhos_average, old_average) * 100 AS metric_value
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM nuvinhos_performance_monthly
  WHERE metric = 'nuvinhos_performance'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM nuvinhos_performance_weekly
  WHERE metric = 'nuvinhos_performance'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM nuvinhos_performance_monthly
  WHERE metric = 'nuvinhos_performance'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM nuvinhos_performance_weekly
  WHERE metric = 'nuvinhos_performance'
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
-- MAGIC ## Improved Benchmarks

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark Base
CREATE OR REPLACE TEMPORARY VIEW ntpj_benchmark AS(
  SELECT
    a.job_id
    , a.agent
    , a.exp_duration_job
    , DATE_TRUNC('MONTH', a.start_date) AS benchmark_month
    , b.xforce
    , b.xplead
  FROM ntpj_initial_base AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.start_date) >= b.valid_from
    AND DATE_TRUNC('MONTH', a.start_date) < b.valid_to
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_benchmark_agg AS(
  SELECT
    job_id
    , ROUND(AVG(exp_duration_job), 5) AS benchmark
    , benchmark_month
    , xforce
    , xplead
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
  FROM ntpj_benchmark_base
);

CREATE OR REPLACE TEMPORARY VIEW occupancy_benchmark AS(
  SELECT
    xforce
    , xplead
    , squad_district AS job_id
    , ROUND(AVG(occupancy_exp), 5) AS benchmark
    , DATE_TRUNC('MONTH', date) AS benchmark_month
  FROM normalized_occupancy
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
  FROM ntpj_xforces AS a
  LEFT JOIN improved_benchmark_base AS b
    ON DATE_TRUNC('MONTH', date_reference) = b.benchmark_month
    AND a.xforce = b.xforce
  WHERE a.date_granularity IN ('week', 'month')
    AND a.date_reference >= '2026-04-01'
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
    , benchmark_month AS date_reference
    , 'month' AS date_granularity
    , 'improved_benchmark' AS metric
    , SUM(improved_jobs) AS numerator
    , SUM(jobs) AS denominator
    , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
  FROM improved_benchmark_final
  WHERE date_granularity = 'month'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_weekly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , benchmark_month AS date_reference
    , 'month' AS date_granularity
    , 'improved_benchmark' AS metric
    , SUM(improved_jobs) AS numerator
    , SUM(jobs) AS denominator
    , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
  FROM improved_benchmark_final
  WHERE date_granularity = 'week'
  GROUP BY ALL
);

-- CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_quarterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , DATE_TRUNC('QUARTER', benchmark_month) AS date_reference
--     , 'quarter' AS date_granularity
--     , 'improved_benchmark' AS metric
--     , SUM(improved_jobs) AS numerator
--     , SUM(jobs) AS denominator
--     , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
--   FROM improved_benchmark_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_semesterly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , CASE
--         WHEN benchmark_month < '2026-07-01' THEN '2026-01-01'
--         WHEN benchmark_month >= '2026-07-01' THEN '2026-07-01'
--         ELSE NULL
--       END AS date_reference
--     , 'semester' AS date_granularity
--     , 'improved_benchmark' AS metric
--     , SUM(improved_jobs) AS numerator
--     , SUM(jobs) AS denominator
--     , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
--   FROM improved_benchmark_final
--   GROUP BY ALL
-- );

-- CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_yearly AS (
--   SELECT
--     NULL AS agent
--     , xforce
--     , xplead
--     , NULL AS squad
--     , NULL AS squad_district
--     , DATE_TRUNC('YEAR', benchmark_month) AS date_reference
--     , 'year' AS date_granularity
--     , 'improved_benchmark' AS metric
--     , SUM(improved_jobs) AS numerator
--     , SUM(jobs) AS denominator
--     , TRY_DIVIDE(SUM(improved_jobs), SUM(jobs)) * 100 AS metric_value
--   FROM improved_benchmark_final
--   GROUP BY ALL
-- );

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark AS (
  SELECT * FROM improved_benchmark_monthly
  UNION ALL
  SELECT * FROM improved_benchmark_weekly
--   UNION ALL
--   SELECT * FROM improved_benchmark_semesterly
--   UNION ALL
--   SELECT * FROM improved_benchmark_yearly
);

-- SELECT * FROM improved_benchmark

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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM improved_benchmark_monthly
  WHERE metric = 'improved_benchmark'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM improved_benchmark_weekly
  WHERE metric = 'improved_benchmark'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM improved_benchmark_monthly
  WHERE metric = 'improved_benchmark'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM improved_benchmark_weekly
  WHERE metric = 'improved_benchmark'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_district AS (
  SELECT * FROM improved_benchmark_district_monthly
  UNION ALL
  SELECT * FROM improved_benchmark_district_weekly
);

-- SELECT * FROM improved_benchmark_district

-- COMMAND ----------

-- DBTITLE 1,Improved Benchmark Dataset
CREATE OR REPLACE TEMPORARY VIEW improved_benchmark_join AS (
  SELECT * FROM improved_benchmark
  UNION ALL
  SELECT * FROM improved_benchmark_squad
  UNION ALL
  SELECT * FROM improved_benchmark_district
);

-- COMMAND ----------

-- DBTITLE 1,Projects XForces Metric
-- Projects metric: value 100 for all xforces in March and June only (month granularity)
CREATE OR REPLACE TEMPORARY VIEW projects_xforces_monthly AS (
  SELECT
    NULL AS agent
    , xforce
    , xplead
    , NULL AS squad
    , NULL AS squad_district
    , date_reference
    , 'month' AS date_granularity
    , 'projects_xforce' AS metric
    , CAST(100 AS DOUBLE) AS numerator
    , CAST(100 AS DOUBLE) AS denominator
    , CAST(100 AS DOUBLE) AS metric_value
  FROM (
    SELECT DISTINCT xforce, xplead
    FROM agent_information
    WHERE status = 'active'
      AND xforce IS NOT NULL
  )
  CROSS JOIN (
    SELECT CAST('2026-03-01' AS TIMESTAMP) AS date_reference
    UNION ALL
    SELECT CAST('2026-06-01' AS TIMESTAMP) AS date_reference
  )
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
    , a.metric_value AS xpeers_in_target_xforce
    , b.metric_value AS average_index_agent
    , c.metric_value AS projects
    , e.metric_value AS shrinkage_raw
  FROM xpeers_in_target_xforces_monthly AS a
  LEFT JOIN average_index_agent_monthly AS b
    ON a.xforce = b.xforce
    AND a.date_reference = b.date_reference
    AND a.date_granularity = b.date_granularity
    AND b.metric = 'average_index_agent'
  LEFT JOIN projects_xforces_monthly AS c
    ON a.xforce = c.xforce
    AND a.date_reference = c.date_reference
    AND a.date_granularity = c.date_granularity
  LEFT JOIN shrinkage_xforces_monthly AS e
    ON a.xforce = e.xforce
    AND a.date_reference = e.date_reference
    AND a.date_granularity = e.date_granularity
    AND e.metric = 'shrinkage_xforce'
  WHERE a.date_granularity IN ('week', 'month', 'quarter', 'semester', 'year')
    AND a.metric = 'xpeers_in_target_xforce'
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
    , COALESCE(xpeers_in_target_xforce, 0) AS xpeers_in_target_xforce
    , COALESCE(average_index_agent, 0) AS average_index_agent
    , COALESCE(projects, 0) AS projects
    , CASE
        WHEN COALESCE(shrinkage_raw, 0) <= 20 THEN 100
        ELSE 120 - COALESCE(shrinkage_raw, 0)
      END AS shrinkage
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
    , NULL AS numerator
    , NULL AS denominator
    , CASE
        -- March & June: projects 10%, shrinkage/xpeers/avg_index 30% each
        WHEN projects IS NOT NULL AND date_reference IN (DATE '2026-03-01', DATE '2026-06-01')
          THEN 0.10 * projects + 0.30 * shrinkage + 0.30 * xpeers_in_target_xforce + 0.30 * average_index_agent
        -- All other months: even split 3 metrics
        ELSE (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3
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
    , NULL AS numerator
    , NULL AS denominator
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
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) AS numerator
--     , 400 AS denominator
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4 AS metric_value
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
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) AS numerator
--     , 400 AS denominator
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4 AS metric_value
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
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) AS numerator
--     , 400 AS denominator
--     , (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4 AS metric_value
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

-- SELECT * FROM index_agents_monthly

-- COMMAND ----------

-- DBTITLE 1,Index XForces Squad Calculations
CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'index_xforce_squad' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_xforces_monthly
  WHERE metric = 'index_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_squad_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , squad
    , NULL AS squad_district
    , date_reference
    , date_granularity
    , 'index_xforce_squad' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_xforces_weekly
  WHERE metric = 'index_xforce'
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
CREATE OR REPLACE TEMPORARY VIEW index_xforces_district_monthly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_xforce_district' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_xforces_monthly
  WHERE metric = 'index_xforce'
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW index_xforces_district_weekly AS (
  SELECT
    NULL AS agent
    , NULL AS xforce
    , NULL AS xplead
    , NULL AS squad
    , squad_district
    , date_reference
    , date_granularity
    , 'index_xforce_district' AS metric
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM index_xforces_weekly
  WHERE metric = 'index_xforce'
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
  WHERE a.date_granularity IN ('month', 'quarter', 'semester', 'year')
    AND a.metric = 'adherence_xplead'
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xplead_final AS(
  SELECT
    *
    , (COALESCE(adherence_in_target, 0) + COALESCE(ntpj_in_target, 0) + COALESCE(nocc_in_target, 0) + COALESCE(qa_in_target, 0)) AS xpeers_in_target
    , (COALESCE(adherence_xpeers, 0) + COALESCE(ntpj_xpeers, 0) + COALESCE(nocc_xpeers, 0) + COALESCE(qa_xpeers, 0)) AS xpeers
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM xpeers_in_target_xpleads_monthly
  WHERE metric = 'xpeers_in_target_xplead'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM xpeers_in_target_xpleads_weekly
  WHERE metric = 'xpeers_in_target_xplead'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM xpeers_in_target_xpleads_monthly
  WHERE metric = 'xpeers_in_target_xplead'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM xpeers_in_target_xpleads_weekly
  WHERE metric = 'xpeers_in_target_xplead'
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

-- SELECT * FROM average_index_xforces_base

-- COMMAND ----------

-- DBTITLE 1,Average Index XForces Calculations
CREATE OR REPLACE TEMPORARY VIEW average_index_xforce_monthly AS (
  SELECT
    NULL AS agent
    , NULL xforce
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
    , NULL xforce
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforce_monthly
  WHERE metric = 'average_index_xforce'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforce_weekly
  WHERE metric = 'average_index_xforce'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforce_monthly
  WHERE metric = 'average_index_xforce'
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
    , SUM(metric_value) AS numerator
    , COUNT(DISTINCT agent) AS denominator
    , AVG(metric_value) AS metric_value
  FROM average_index_xforce_weekly
  WHERE metric = 'average_index_xforce'
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

-- DBTITLE 1,[Temp Fix] Joins and Save (Python)
-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC table = "usr.mx__cx.internal_ops_performance_2026_content"
-- MAGIC
-- MAGIC metrics = [
-- MAGIC     "adherence",
-- MAGIC     "ntpj",
-- MAGIC     "nocc",
-- MAGIC     "shrinkage",
-- MAGIC     "index_agents_join",
-- MAGIC     "xpeers_in_target_xforces_join",
-- MAGIC     "average_index_agent_join",
-- MAGIC     "index_xforces_join",
-- MAGIC     "xpeers_in_target_xpleads_join",
-- MAGIC     "average_index_xforce_join",
-- MAGIC     "nuvinhos_performance_join",
-- MAGIC     "quality",
-- MAGIC     "projects_xforces_monthly",
-- MAGIC ]
-- MAGIC
-- MAGIC # ---------- Table creation ----------
-- MAGIC print("Creating table...")
-- MAGIC try:
-- MAGIC     spark.sql(f"""
-- MAGIC         CREATE OR REPLACE TABLE {table}
-- MAGIC         USING DELTA
-- MAGIC         AS SELECT * FROM {metrics[0]} WHERE 1 = 0
-- MAGIC     """)
-- MAGIC     print(f"OK {table} table created\n")
-- MAGIC except Exception as e:
-- MAGIC     raise RuntimeError(f"Fail to create table. Aborting.\nError: {e}")
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
-- MAGIC             WHERE date_reference < '2026-01-01' OR date_reference >= '2026-02-01'
-- MAGIC         """)
-- MAGIC         duration = (datetime.now() - start).seconds
-- MAGIC         saved_metrics.append(metric)
-- MAGIC         print(f"  OK {metric} ({duration}s)\n")
-- MAGIC     except Exception as e:
-- MAGIC         metrics_with_error.append((metric, str(e)))
-- MAGIC         print(f"  FAIL {metric} - ERROR: {e}\n")
-- MAGIC
-- MAGIC # ---------- Resume ----------
-- MAGIC print("=" * 50)
-- MAGIC print(f"Successfully saved ({len(saved_metrics)}/{len(metrics)}):")
-- MAGIC for m in saved_metrics:
-- MAGIC     print(f"  OK {m}")
-- MAGIC
-- MAGIC if metrics_with_error:
-- MAGIC     print(f"\nErrors ({len(metrics_with_error)}):")
-- MAGIC     for m, err in metrics_with_error:
-- MAGIC         print(f"  FAIL {m}: {err}")
-- MAGIC     print("\nTo run only the errors, use:")
-- MAGIC     names = [m for m, _ in metrics_with_error]
-- MAGIC     print(f"  metrics = {names}")
-- MAGIC else:
-- MAGIC     print("\nAll metrics saved!")
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Joins
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW dataset AS (
-- MAGIC   SELECT * FROM adherence
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM ntpj
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM ntpj_sla_new
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM ntpj_sla_new_v2
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM ntpj_sla_old_v2
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM nocc
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM shrinkage
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
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM quality
-- MAGIC );
-- MAGIC
-- MAGIC -- SELECT DISTINCT metric FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Save table
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TABLE usr.mx__cx.internal_ops_performance_2026_content AS
-- MAGIC SELECT * FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Table Sharing
-- GRANT SELECT ON TABLE usr.mx__cx.internal_ops_performance_2026_content TO `59e52f0a-0aa5-44b9-90f9-3d781cc0e097`;
-- SELECT * FROM usr.mx__cx.internal_ops_performance_2026_content

-- COMMAND ----------

-- DBTITLE 1,[Temp Fix] Cleanup intermediate tables
-- [Temp Fix] Optional cleanup of intermediate tables (run after the table is saved).
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_agent_information;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_adherence;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_ntpj_initial_base;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_ntpj;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_ntpj_sla_new;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_ntpj_sla_old;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_ntpj_sla_new_v2;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_ntpj_sla_old_v2;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_normalized_occupancy;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_nocc;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_quality;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_index_agents_final;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_index_agents;
-- DROP TABLE IF EXISTS usr.danielanzures.cont_temp_content_agents;