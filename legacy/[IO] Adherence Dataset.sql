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
        WHEN (agent LIKE '%maria.reyes%' AND dime_date >= '2026-02-01' AND dime_date < '2026-03-01')
          THEN 'time_off'
        WHEN (agent LIKE '%tania.enciso%' AND dime_date IN ('2026-05-08', '2026-05-09'))
          THEN 'time_off'
        WHEN (agent LIKE '%yerck.tellez%' AND dime_date IN ('2026-03-03', '2026-04-28'))
          THEN 'time_off'
        WHEN (agent LIKE '%gabriela.vega%' AND dime_date = '2026-05-12')
          THEN 'time_off'
        WHEN (agent LIKE '%dulce.rivera%' AND dime_date = '2026-03-29')
          THEN 'time_off'
        WHEN (agent LIKE '%nadia.tovias%' AND dime_date = '2026-04-23')
          THEN 'time_off'
        WHEN (agent LIKE '%rodrigo.padilla%' AND dime_date = '2026-03-10')
          THEN 'time_off'
        WHEN (agent LIKE '%israel.cadena%' AND dime_date = '2026-03-19')
          THEN 'time_off'
        WHEN (agent LIKE '%uriel.alfaro%' AND dime_date IN ('2026-04-06', '2026-04-07', '2026-05-13', '2026-05-14', '2026-05-15'))
          THEN 'time_off'
        WHEN (agent LIKE '%yuridia.agama%' AND dime_date IN ('2026-05-11', '2026-05-12', '2026-05-13', '2026-05-14'))
          THEN 'time_off'
        WHEN (agent LIKE '%alexis.torres%' AND dime_date IN ('2026-05-21', '2026-05-22'))
          THEN 'time_off'
        WHEN (agent LIKE '%lucia.espinosa%' AND dime_date = '2026-04-11')
          THEN 'time_off'
        WHEN (agent LIKE '%adriana.lopez%' AND dime_date = '2026-05-14')
          THEN 'time_off'
        WHEN (agent LIKE '%jefferson.nunes%' AND dime_date = '2026-05-01')
          THEN 'time_off'
        WHEN (agent LIKE '%patricia.gomez%' AND dime_date = '2026-05-01')
          THEN 'time_off'
        WHEN (agent LIKE '%carmina.venegas%' AND dime_date >= '2026-04-19' AND dime_date <= '2026-08-19')
          THEN 'time_off'
        WHEN ((agent LIKE '%cecilia.ortiz%' OR agent LIKE '%federico.gaona%' OR agent LIKE '%ignacio.herbert%' OR agent LIKE '%marcos.caudillo%' OR agent LIKE '%maria.castillo%') AND dime_date = '2026-05-01')
          THEN 'time_off'
        WHEN (agent LIKE '%evelyn.macedo%' AND dime_date >= '2026-04-27')
          THEN 'time_off'
        WHEN (agent LIKE '%jorge.delgado%' AND dime_date IN ('2026-05-19', '2026-05-20'))
          THEN 'time_off'
        WHEN (agent LIKE '%claudia.brigada%' AND dime_date = '2026-06-07')
          THEN 'time_off'
        WHEN (agent LIKE '%omar.morales%' AND dime_date = '2026-05-04')
          THEN 'time_off'
        WHEN (agent LIKE '%luis.delgadillo%' AND dime_date = '2026-05-28')
          THEN 'time_off'
        ELSE activity_type_required
      END AS activity_type_required
  FROM (
    SELECT * FROM etl.mx__series_contract.agent_dimensioned_activities
    UNION ALL
    SELECT * FROM usr.danielanzures.h1_missing_dime_slots
  )
  WHERE
      affiliation = 'nubank'
      AND dime_date >= '2025-01-01'
      AND activity_type_required IS NOT NULL
      AND activity_type_required NOT IN ('lunch_break', 'time_off', 'shrinkage')
      AND dimensioned_activity NOT IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle', 'Licencia', 'Vacacion')
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

CREATE OR REPLACE TEMPORARY VIEW adherence_by_slot_2026 AS(
  SELECT
    a.agent
    , a.date
    , a.slot_start
    , a.activity_type_required
    , LEAST(COALESCE(SUM(a.adherent_time_final), 0), 1800) AS adherent_time_final
    , b.xplead
    , b.xforce
    , b.squad
    , b.squad_district
  FROM data_calculations AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.date) = b.snapshot_month
  WHERE a.date >= '2025-12-01'
    AND b.status = 'active'
    AND a.date NOT IN ('2026-03-27', '2026-04-09') -- deleting data with general access problems
    AND NOT (a.agent IN ('jose.velez', 'carlos.gonzalez', 'jorge.ortega', 'luisa.castaneda', 'janet.castro', 'karen.ortega')
      AND a.date IN ('2026-03-24', '2026-03-25', '2026-03-26', '2026-03-27', '2026-03-28'))
    AND NOT (a.agent = 'jonathan.pineda' AND a.date = '2026-02-26')
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW adherence_by_slot_2025 AS(
  SELECT
    a.agent
    , a.date
    , a.slot_start
    , a.activity_type_required
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

-- SELECT * FROM adherence_final

-- COMMAND ----------

CREATE OR REPLACE TABLE usr.mx__cx.adherence_io AS
SELECT * FROM adherence_final