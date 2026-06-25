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

-- DBTITLE 1,Manual Adjustments
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

      /* CROSS SUPPORT → CREDIT CANCELLATION — training */
      WHEN agent IN ('javier.balanzar', 'carlos.gonzalez', 'jorge.severiano')
        AND dime_date = DATE '2026-05-26'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 16:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 17:00:00')) THEN TRUE
      WHEN agent IN ('mariana.infante', 'eden.martinez')
        AND dime_date = DATE '2026-05-27'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 12:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 13:00:00')) THEN TRUE
      WHEN agent = 'rocio.rodriguez'
        AND dime_date = DATE '2026-05-22'
        AND local_timestamp_dime_slot_starts_at >= to_timestamp(concat(cast(dime_date AS STRING), ' 12:00:00'))
        AND local_timestamp_dime_slot_starts_at <  to_timestamp(concat(cast(dime_date AS STRING), ' 13:00:00')) THEN TRUE

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

      /* HOLIDAY — May 1st 2026 */
      WHEN agent IN ('cecilia.ortiz', 'federico.gaona', 'ignacio.herbert', 'marcos.caudillo', 'maria.castillo')
        AND dime_date = DATE '2026-05-01' THEN TRUE

      /* EXCLUDE — evelyn.macedo from Apr 27 2026 onwards */
      WHEN agent = 'evelyn.macedo'
        AND dime_date >= DATE '2026-04-27' THEN TRUE

      /* DAYS OFF — jorge.delgado May 19-20 2026 */
      WHEN agent = 'jorge.delgado'
        AND dime_date IN (DATE '2026-05-19', DATE '2026-05-20') THEN TRUE

      /* DAY OFF — claudia.brigada Jun 7 2026 */
      WHEN agent = 'claudia.brigada'
        AND dime_date = DATE '2026-06-07' THEN TRUE

      /* DAY OFF — omar.morales May 4 2026 */
      WHEN agent = 'omar.morales'
        AND dime_date = DATE '2026-05-04' THEN TRUE

      /* DAY OFF — luis.delgadillo May 28 2026 */
      WHEN agent = 'luis.delgadillo'
        AND dime_date = DATE '2026-05-28' THEN TRUE

      ELSE FALSE
    END AS exclude
    , CASE
        /* ========= Vacation / Time Off (keep in required_slot, exclude from shrinkage_slot only) ========= */

        /* jessica.pimentel — Jun 08–12 2026 */
        WHEN agent = 'jessica.pimentel'
          AND dime_date BETWEEN DATE '2026-06-08' AND DATE '2026-06-12' THEN TRUE

        /* maximiliano.lopez — Jun 10 2026 */
        WHEN agent = 'maximiliano.lopez'
          AND dime_date = DATE '2026-06-10' THEN TRUE

        ELSE FALSE
      END AS not_shrinkage
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

-- DBTITLE 1,Cell 3
CREATE OR REPLACE TEMPORARY VIEW shrinkage_base AS(
  SELECT
    LOWER(REGEXP_EXTRACT(a.agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , a.agent_dime_squad AS old_squad
    , a.dime_date AS date
    , a.activity_type_required
    , a.dimensioned_activity
    , b.exclude
    , b.not_shrinkage
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
        WHEN a.date < '2026-03-01' THEN COUNT(CASE 
          WHEN a.activity_type_required = 'shrinkage' THEN 1
          WHEN a.agent = 'maria.reyes' AND a.date >= '2026-02-01' AND a.date < '2026-03-01' THEN 1 -- maternity leave: reclassify all non-null slots as shrinkage
          END)
        ELSE COUNT(CASE 
          WHEN a.activity_type_required = 'shrinkage' AND a.not_shrinkage IS NOT TRUE THEN 1 
          WHEN a.activity_type_required = 'dime_invalid_notation' AND a.dimensioned_activity IN ('Mouring', 'Weekly', 'Permiso Medico', 'Permiso medico', 'Huddle', 'Licencia', 'Vacacion') THEN 1
          WHEN a.agent = 'tania.enciso' AND a.date IN ('2026-05-08', '2026-05-09') THEN 1 -- vacation
          WHEN a.agent = 'yerck.tellez' AND a.date IN ('2026-03-03', '2026-04-28') THEN 1 -- vacation
          WHEN a.agent = 'gabriela.vega' AND a.date = '2026-05-12' THEN 1 -- vacation
          WHEN a.agent = 'dulce.rivera' AND a.date = '2026-03-29' THEN 1 -- vacation
          WHEN a.agent = 'nadia.tovias' AND a.date = '2026-04-23' THEN 1 -- vacation
          WHEN a.agent = 'rodrigo.padilla' AND a.date = '2026-03-10' THEN 1 -- vacation
          WHEN a.agent = 'israel.cadena' AND a.date = '2026-03-19' THEN 1 -- vacation
          WHEN a.agent = 'uriel.alfaro' AND a.date IN ('2026-04-06', '2026-04-07', '2026-05-13', '2026-05-14', '2026-05-15') THEN 1 -- vacation
          WHEN a.agent = 'yuridia.agama' AND a.date IN ('2026-05-11', '2026-05-12', '2026-05-13', '2026-05-14') THEN 1 -- vacation
          WHEN a.agent = 'alexis.torres' AND a.date IN ('2026-05-21', '2026-05-22') THEN 1 -- vacation
          WHEN a.agent = 'lucia.espinosa' AND a.date = '2026-04-11' THEN 1 -- vacation
          WHEN a.agent = 'adriana.lopez' AND a.date = '2026-05-14' THEN 1 -- vacation
          WHEN a.agent = 'carmina.venegas' AND a.date >= '2026-04-19' AND a.date <= '2026-08-19' THEN 1 -- licence
          END)
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
    AND NOT (a.agent IN ('jose.velez', 'carlos.gonzalez', 'jorge.ortega', 'luisa.castaneda', 'janet.castro', 'karen.ortega')
      AND a.date IN ('2026-03-24', '2026-03-25', '2026-03-26', '2026-03-27', '2026-03-28'))
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

CREATE OR REPLACE TABLE usr.mx__cx.shrinkage_io AS
SELECT * FROM shrinkage_final