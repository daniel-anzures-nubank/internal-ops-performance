-- Databricks notebook source
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

-- MAGIC %md
-- MAGIC ## Adherence

-- COMMAND ----------

-- DBTITLE 1,Adherence Base
CREATE OR REPLACE TEMPORARY VIEW agent_id AS(
  SELECT DISTINCT
    REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent
    , actor__id
  FROM etl.mx__dataset.ops_actors
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
          WHEN UPPER(comment) RLIKE 'MOS[\\s_-]*\\d{4}'
            THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(comment), 'MOS[\\s_-]*(\\d{4})', 1))
          WHEN comment RLIKE '^\\s*\\d{4}\\s*$'
            THEN CONCAT('MOS-', TRIM(comment))
          WHEN UPPER(comment) RLIKE 'TICKET\\s+(MOS-?\\s*)?\\d{4}'
            THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(comment), 'TICKET\\s+(?:MOS-?\\s*)?(\\d{4})', 1))
          ELSE NULL
        END,
        CASE
          WHEN UPPER(ticket__id) RLIKE 'MOS[\\s_-]*\\d{4}'
            THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(ticket__id), 'MOS[\\s_-]*(\\d{4})', 1))
          WHEN ticket__id RLIKE '^\\s*\\d{4}\\s*$'
            THEN CONCAT('MOS-', TRIM(ticket__id))
          WHEN UPPER(ticket__id) RLIKE 'TICKET\\s+(MOS-?\\s*)?\\d{4}'
            THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(ticket__id), 'TICKET\\s+(?:MOS-?\\s*)?(\\d{4})', 1))
          ELSE NULL
        END
      ) AS content_id
  FROM etl.mx__dataset.taskmaster_consolidated_registry
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
    AND agent_dime_squad = 'Content'
    AND shuffle_status_required IN ('available', 'oos')
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
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
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
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
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
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) / COUNT(DISTINCT agent) *100 AS metric_value
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
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
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
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
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
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END) AS numerator
--     , COUNT(DISTINCT agent) AS denominator
--     , COUNT(DISTINCT CASE WHEN metric_value <= 100 THEN agent END)/ COUNT(DISTINCT agent) *100 AS metric_value
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
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE
      affiliation = 'nubank'
      AND dime_date >= '2024-12-30'
      AND activity_type_required IS NOT NULL
      AND activity_type_required NOT IN ('lunch_break', 'time_off', 'shrinkage')
      AND dimensioned_activity NOT IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle')
      AND agent_dime_squad IS NOT NULL
      AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote', 'social')
      AND dime_date <= DATE_SUB(DATE_TRUNC('WEEK', CURRENT_DATE()), 1)
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
    AND (a.agent NOT IN ('jose.velez', 'carlos.gonzalez', 'jorge.ortega', 'luisa.castaneda', 'janet.castro', 'karen.ortega')
      AND a.date NOT IN ('2026-03-25', '2026-03-26', '2026-03-27'))
    AND a.date != '2026-03-27'
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
  GROUP BY ALL
);

-- SELECT * FROM normalized_occupancy_final

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
        WHEN facilidad >= 4 THEN 1
        ELSE 0
      END AS facilidad_promoter
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
        WHEN manejo_de_cambios >= 4 THEN 1
        ELSE 0
      END AS manejo_de_cambios_promoter
    , CASE
        WHEN expectativas >= 4 THEN 1
        ELSE 0
      END AS expectativas_promoter
    , CASE
        WHEN aportacion_estrategica >= 4 THEN 1
        ELSE 0
      END AS aportacion_estrategica_promoter
    , 8 AS number_of_questions
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
      , facilidad_promoter + comprension_promoter + comunicacion_promoter + calidad_promoter + tiempo_promoter + manejo_de_cambios_promoter + expectativas_promoter + aportacion_estrategica_promoter AS promoters
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
        WHEN date_reference <= '2026-02-01'
          THEN (adherence + ntpj)
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc)
        ELSE (adherence + ntpj + nocc + quality) 
      END AS numerator
    , CASE
        WHEN date_reference <= '2026-02-01'
          THEN 200
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN date_reference <= '2026-02-01'
          THEN (adherence + ntpj) / 2
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
        WHEN date_reference <= '2026-02-28'
          THEN (adherence + ntpj)
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc)
        ELSE (adherence + ntpj + nocc + quality) 
      END AS numerator
    , CASE
        WHEN date_reference <= '2026-02-28'
          THEN 200
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN date_reference <= '2026-02-28'
          THEN (adherence + ntpj) / 2
        WHEN date_reference >= '2026-03-01' AND quality IS NULL
          THEN (adherence + ntpj + nocc) / 3
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
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE affiliation = 'nubank'
    AND dime_date >= '2025-01-01'
    AND activity_type_required IS NOT NULL
    AND activity_type_required NOT IN ('lunch_break', 'dime_invalid_notation')
    AND agent_dime_squad IS NOT NULL
    AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote')
);

CREATE OR REPLACE TEMPORARY VIEW shrinkage_final AS(
  SELECT
    a.*
    , COUNT(CASE WHEN a.activity_type_required IN ('shrinkage', 'timeoff') THEN 1 END) AS shrinkage_slot
    , COUNT(*) AS required_slot
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
    , a.numerator AS adherence_in_target
    , b.numerator AS ntpj_in_target
    , c.numerator AS nocc_in_target
    -- , d.numerator AS qa_in_target
    , a.denominator AS adherence_xpeers
    , b.denominator AS ntpj_xpeers
    , c.denominator AS nocc_xpeers
    -- , d.denominator AS qa_xpeers
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
  -- LEFT JOIN quality AS d
  --   ON a.xforce = d.xforce
  --   AND a.date_reference = d.date_reference
  --   AND a.date_granularity = d.date_granularity
  --   AND d.metric = 'qa_xforce'
  WHERE a.date_granularity IN ('week', 'month', 'quarter', 'semester', 'year')
    AND a.metric = 'adherence_xforce'
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_final AS(
  SELECT
    *
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence_in_target + ntpj_in_target)
        ELSE (adherence_in_target + ntpj_in_target + nocc_in_target)
      END AS xpeers_in_target
    , CASE 
        WHEN date_reference <= '2026-02-01' THEN (adherence_xpeers + ntpj_xpeers)
        ELSE (adherence_xpeers + ntpj_xpeers + nocc_xpeers)
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
  LEFT JOIN improved_benchmark_monthly AS d
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
        WHEN date_reference <= '2026-03-31'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent)
        ELSE (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark)
      END AS numerator
    , CASE
        WHEN date_reference <= '2026-03-31'
          THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN date_reference <= '2026-03-31'
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
        WHEN date_reference <= '2026-03-31'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent)
        ELSE (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark)
      END AS numerator
    , CASE
        WHEN date_reference <= '2026-03-31'
          THEN 300
        ELSE 400
      END AS denominator
    , CASE
        WHEN date_reference <= '2026-03-31'
          THEN (shrinkage + xpeers_in_target_xforce + average_index_agent) / 3
        ELSE (shrinkage + xpeers_in_target_xforce + average_index_agent + improved_benchmark) / 4
      END AS metric_value
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
    -- , d.numerator AS qa_in_target
    , a.denominator AS adherence_xpeers
    , b.denominator AS ntpj_xpeers
    , c.denominator AS nocc_xpeers
    -- , d.denominator AS qa_xpeers
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
  -- LEFT JOIN quality AS d
    -- ON a.xplead = d.xplead
    -- AND a.date_reference = d.date_reference
    -- AND a.date_granularity = d.date_granularity
    -- AND d.metric = 'qa_xplead'
  WHERE a.date_granularity IN ('month', 'quarter', 'semester', 'year')
    AND a.metric = 'adherence_xplead'
);

CREATE OR REPLACE TEMPORARY VIEW xpeers_in_target_xplead_final AS(
  SELECT
    *
    , CASE
        WHEN date_reference <= '2026-02-01' THEN (adherence_in_target + ntpj_in_target)
        ELSE (adherence_in_target + ntpj_in_target + nocc_in_target)
      END AS xpeers_in_target
    , CASE 
        WHEN date_reference <= '2026-02-01' THEN (adherence_xpeers + ntpj_xpeers)
        ELSE (adherence_xpeers + ntpj_xpeers + nocc_xpeers)
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

-- DBTITLE 1,Joins
CREATE OR REPLACE TEMPORARY VIEW dataset AS (
  SELECT * FROM adherence
  UNION ALL
  SELECT * FROM ntpj
  UNION ALL
  SELECT * FROM nocc
  UNION ALL
  SELECT * FROM shrinkage
  UNION ALL
  SELECT * FROM index_agents_join
  UNION ALL
  SELECT * FROM xpeers_in_target_xforces_join
  UNION ALL
  SELECT * FROM average_index_agent_join
  UNION ALL
  SELECT * FROM improved_benchmark_join
  UNION ALL
  SELECT * FROM index_xforces_join
  UNION ALL
  SELECT * FROM xpeers_in_target_xpleads_join
  UNION ALL
  SELECT * FROM average_index_xforce_join
  UNION ALL
  SELECT * FROM nuvinhos_performance_join
  UNION ALL
  SELECT * FROM quality
);

-- SELECT DISTINCT metric FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Save table
CREATE OR REPLACE TABLE usr.mx__cx.internal_ops_performance_2026_content AS
SELECT * FROM dataset

-- COMMAND ----------

-- DBTITLE 1,Table Sharing
-- GRANT SELECT ON TABLE usr.mx__cx.internal_ops_performance_2026_content TO `59e52f0a-0aa5-44b9-90f9-3d781cc0e097`;
-- SELECT * FROM usr.mx__cx.internal_ops_performance_2026_content