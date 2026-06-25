-- Databricks notebook source
-- DBTITLE 1,Agent Information Core + SM
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

CREATE OR REPLACE TEMPORARY VIEW agent_information_core AS(
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
WHERE a.squad NOT IN ('content')
);

CREATE OR REPLACE TEMPORARY VIEW agent_information_core_for_union AS (
  SELECT
    LOWER(agent) AS agent,
    LOWER(xplead) AS xplead,
    LOWER(xforce) AS xforce,
    squad,
    squad_district,
    status,
    shift,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    DATE_TRUNC('month', snapshot_month) AS snapshot_month,
    CAST(last_change_date AS DATE) AS last_change_date,
    CAST(NULL AS STRING) AS target_squad,
    CAST('core_sm' AS STRING) AS agent_source
  FROM agent_information_core
);

-- SELECT * FROM agent_information_core_for_union


-- COMMAND ----------

-- DBTITLE 1,Agent Information Content
CREATE OR REPLACE TEMPORARY VIEW agent_information_content AS (
  SELECT 
    REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent,
    squad,
    REGEXP_EXTRACT(xplead_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS xplead,
    REGEXP_EXTRACT(xforce_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS xforce,
    district AS squad_district,
    last_update AS valid_from,
    COALESCE(LEAD(last_update) OVER(PARTITION BY REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) ORDER BY last_update), '9999-12-31') AS valid_to,
    status,
    target_squad
  FROM gsheets.sheets.mx_content_bdx
  WHERE actor_email IS NOT NULL
);

CREATE OR REPLACE TEMPORARY VIEW agent_information_content_for_union AS (
SELECT
  LOWER(c.agent) AS agent,
  LOWER(c.xplead) AS xplead,
  LOWER(c.xforce) AS xforce,
  c.squad,
  c.squad_district,
  c.status,
  CAST(NULL AS STRING) AS shift,
  CAST(NULL AS DATE) AS snapshot_date,
  m.snapshot_month,
  TO_DATE(c.valid_from) AS last_change_date,
  c.target_squad,
  CAST('content' AS STRING) AS agent_source
FROM agent_information_content AS c
LATERAL VIEW OUTER posexplode(
  sequence(
    DATE_TRUNC('month', TO_DATE(c.valid_from)),
    LEAST(
      DATE_TRUNC('month', DATE_SUB(TO_DATE(c.valid_to), 1)),
      DATE_TRUNC('month', DATE '2030-12-01')
    ),
    INTERVAL 1 MONTH
  )
) m AS pos, snapshot_month
WHERE TO_DATE(c.valid_to) > TO_DATE(c.valid_from)
);

-- SELECT * FROM agent_information_content_for_union

-- COMMAND ----------

-- DBTITLE 1,Agent Information Union
CREATE OR REPLACE TEMPORARY VIEW agent_information AS (
  SELECT * FROM agent_information_core_for_union
  UNION ALL
  SELECT * FROM agent_information_content_for_union
);

-- COMMAND ----------

-- DBTITLE 1,Occupancy Base
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
        WHEN activity_type_required = 'dime_invalid_notation'
          THEN 'oos'
        ELSE activity_type_required
      END AS activity_type_required
    , dimensioned_activity
  FROM (
    SELECT * FROM etl.mx__series_contract.agent_dimensioned_activities
    UNION ALL
    SELECT * FROM usr.danielanzures.h1_missing_dime_slots
  )
  WHERE
      affiliation = 'nubank'
      AND dime_date >= '2024-12-30'
      AND activity_type_required IS NOT NULL
      AND activity_type_required NOT IN ('lunch_break', 'time_off', 'shrinkage')
      AND dimensioned_activity NOT IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle', 'Licencia', 'Vacacion')
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
    AND NOT (LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) = 'lilian.silva'
      AND job_classification = 'Deimos - Internal Issues (OOS_LCYC)'
      AND CAST(local_start_date AS DATE) = DATE '2026-05-28'
      AND customer__id = '65566904-c391-4975-8b9a-eaf46fe03818') -- excluded: outlier, task left open ~70 hours
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
    AND NOT (a.agent IN ('jose.velez', 'carlos.gonzalez', 'jorge.ortega', 'luisa.castaneda', 'janet.castro', 'karen.ortega')
      AND a.date IN ('2026-03-24', '2026-03-25', '2026-03-26', '2026-03-27', '2026-03-28'))
    AND a.date NOT IN ('2026-03-27', '2026-04-09')
    AND NOT (a.agent = 'jonathan.pineda' AND a.date = '2026-02-26')
    AND NOT (a.agent = 'maria.reyes' AND a.date >= '2026-02-01' AND a.date < '2026-03-01') -- maternity leave
    AND NOT (a.agent = 'tania.enciso' AND a.date IN ('2026-05-08', '2026-05-09')) -- vacation
    AND NOT (a.agent = 'yerck.tellez' AND a.date IN ('2026-03-03', '2026-04-28')) -- vacation
    AND NOT (a.agent = 'gabriela.vega' AND a.date = '2026-05-12') -- vacation
    AND NOT (a.agent = 'dulce.rivera' AND a.date = '2026-03-29') -- vacation
    AND NOT (a.agent = 'nadia.tovias' AND a.date = '2026-04-23') -- vacation
    AND NOT (a.agent = 'rodrigo.padilla' AND a.date = '2026-03-10') -- vacation
    AND NOT (a.agent = 'israel.cadena' AND a.date = '2026-03-19') -- vacation
    AND NOT (a.agent = 'uriel.alfaro' AND a.date IN ('2026-04-06', '2026-04-07', '2026-05-13', '2026-05-14', '2026-05-15')) -- vacation
    AND NOT (a.agent = 'yuridia.agama' AND a.date IN ('2026-05-11', '2026-05-12', '2026-05-13', '2026-05-14')) -- vacation
    AND NOT (a.agent = 'alexis.torres' AND a.date IN ('2026-05-21', '2026-05-22')) -- vacation
    AND NOT (a.agent = 'lucia.espinosa' AND a.date = '2026-04-11') -- vacation
    AND NOT (a.agent = 'adriana.lopez' AND a.date = '2026-05-14') -- vacation
    AND NOT (b.xplead = 'david.fernandez' AND a.date = '2026-03-10') -- DIME ETL doesn't match with DIME Drive
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
    , squad
  FROM (
    SELECT
      agent
      , squad_district
      , squad
      , slot_start
      , shift
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
    AND a.shift = b.shift
    -- AND DATE_TRUNC('MONTH', a.date) >= b.month 
    -- AND DATE_TRUNC('MONTH', a.date) <= b.month + INTERVAL 4 MONTHS
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
    , shift
  FROM normalized_occupancy
  WHERE date >= '2026-03-01'
  GROUP BY ALL
);

-- SELECT * FROM normalized_occupancy_final

-- COMMAND ----------

CREATE OR REPLACE TABLE usr.mx__cx.normalized_occupancy AS
SELECT * FROM normalized_occupancy_final

-- COMMAND ----------

-- GRANT SELECT ON TABLE usr.mx__cx.normalized_occupancy TO `59e52f0a-0aa5-44b9-90f9-3d781cc0e097`;
-- SELECT * FROM usr.mx__cx.normalized_occupancy