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

-- DBTITLE 1,Manual Adjustments
CREATE OR REPLACE TEMPORARY VIEW manual_adjustments_ntpj AS (
  SELECT
    *,
    CASE
      /* CREDIT → LIFECYCLE (BKO_LCYC) */
      WHEN agent = 'elizabeth.martinez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-27' THEN TRUE
      WHEN agent = 'daniel.cano' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-11' AND dime_date <= DATE '2026-03-27' THEN TRUE
      WHEN agent = 'bertha.sanchez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-27' THEN TRUE
      WHEN agent = 'jonathan.pineda' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-27' THEN TRUE
      WHEN agent = 'sofia.orozco' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-27' THEN TRUE
      WHEN agent = 'jessica.gonzalez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-27' THEN TRUE
      WHEN agent = 'jorge.ortega' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-27' THEN TRUE
      WHEN agent = 'nitza.zarza' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-27' THEN TRUE

      /* CREDIT → CUENTA (BKO_CTA_TSKF) */
      WHEN agent = 'elizabeth.martinez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'daniel.cano' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'jonathan.pineda' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE

      /* COLLECTIONS → CUENTA */
      WHEN agent = 'adriana.marquez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-10' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'javier.balanzar' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-10' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'carlos.gonzalez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'eden.martinez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'mariana.infante' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'jorge.severiano' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-13' AND dime_date <= DATE '2099-12-31' THEN TRUE

      /* EMI → CUENTA */
      WHEN agent = 'fernanda.ibanez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'jose.velez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'ivette.melendez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'rocio.rodriguez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_cta_tskf' AND dime_date >= DATE '2026-04-09' AND dime_date <= DATE '2099-12-31' THEN TRUE

      /* EMI → LIFECYCLE (BKO_LCYC) */
      WHEN agent = 'fernanda.ibanez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-29' THEN TRUE
      WHEN agent = 'jose.velez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-29' THEN TRUE
      WHEN agent = 'ivette.melendez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-29' THEN TRUE
      WHEN agent = 'erik.licona' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-03-10' AND dime_date <= DATE '2026-03-29' THEN TRUE

      /* CROSS SUPPORT → CANCELLATION QUEUES (May 2026+) */
      WHEN agent = 'erik.licona' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-16' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'jessica.gonzalez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-16' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'bertha.sanchez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-15' AND dime_date <= DATE '2099-12-31' THEN TRUE

      /* CROSS SUPPORT → CREDIT CANCELLATION (May 2026+) */
      WHEN agent = 'javier.balanzar' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-26' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'mariana.infante' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-26' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'carlos.gonzalez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-26' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'eden.martinez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-26' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'jorge.severiano' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-26' AND dime_date <= DATE '2099-12-31' THEN TRUE
      WHEN agent = 'rocio.rodriguez' AND LOWER(TRIM(dimensioned_activity)) = 'bko_lcyc' AND dime_date >= DATE '2026-05-22' AND dime_date <= DATE '2099-12-31' THEN TRUE

      /* MANUAL EXCLUSION — May 1st 2026 */
      WHEN agent = 'jefferson.nunes' AND dime_date = DATE '2026-05-01' THEN TRUE
      WHEN agent = 'patricia.gomez' AND dime_date = DATE '2026-05-01' THEN TRUE

      /* LICENCE — carmina.venegas Apr-Aug 2026 */
      WHEN agent = 'carmina.venegas' AND dime_date >= DATE '2026-04-19' AND dime_date <= DATE '2026-08-19' THEN TRUE

      /* HOLIDAY — May 1st 2026 */
      WHEN agent IN ('cecilia.ortiz', 'federico.gaona', 'ignacio.herbert', 'marcos.caudillo', 'maria.castillo') AND dime_date = DATE '2026-05-01' THEN TRUE

      /* EXCLUDE — evelyn.macedo from Apr 27 2026 onwards */
      WHEN agent = 'evelyn.macedo' AND dime_date >= DATE '2026-04-27' THEN TRUE

      /* DAYS OFF — jorge.delgado May 19-20 2026 */
      WHEN agent = 'jorge.delgado' AND dime_date IN (DATE '2026-05-19', DATE '2026-05-20') THEN TRUE

      /* DAY OFF — claudia.brigada Jun 7 2026 */
      WHEN agent = 'claudia.brigada' AND dime_date = DATE '2026-06-07' THEN TRUE

      /* DAY OFF — omar.morales May 4 2026 */
      WHEN agent = 'omar.morales' AND dime_date = DATE '2026-05-04' THEN TRUE

      /* DAY OFF — luis.delgadillo May 28 2026 */
      WHEN agent = 'luis.delgadillo' AND dime_date = DATE '2026-05-28' THEN TRUE
      ELSE FALSE
    END AS exclude
  FROM (
    SELECT
      LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
      CAST(dime_date AS DATE) AS dime_date,
      unix_timestamp(local_timestamp_dime_slot_starts_at) + (6 * 60 * 60) AS slot_start,
      dimensioned_activity
    FROM (
      SELECT * FROM etl.mx__series_contract.agent_dimensioned_activities
      UNION ALL
      SELECT * FROM usr.danielanzures.h1_missing_dime_slots
    )
    WHERE affiliation = 'nubank'
      AND dime_date >= DATE '2025-01-01'
  ) AS dime_slots
);

-- Queue-specific exclusions for cross-support agents
CREATE OR REPLACE TEMPORARY VIEW manual_queue_exclusions_ntpj AS (
  -- Group A: Cancellation queues (5 queues) - March 2026
  SELECT agents.agent, agents.start_date, agents.end_date, queues.queue
  FROM (VALUES
    ('elizabeth.martinez', DATE '2026-03-10', DATE '2026-03-27'),
    ('daniel.cano', DATE '2026-03-11', DATE '2026-03-27'),
    ('bertha.sanchez', DATE '2026-03-10', DATE '2026-03-27'),
    ('jonathan.pineda', DATE '2026-03-10', DATE '2026-03-27'),
    ('sofia.orozco', DATE '2026-03-10', DATE '2026-03-27'),
    ('jessica.gonzalez', DATE '2026-03-10', DATE '2026-03-27'),
    ('jorge.ortega', DATE '2026-03-10', DATE '2026-03-27'),
    ('nitza.zarza', DATE '2026-03-10', DATE '2026-03-27')
  ) AS agents(agent, start_date, end_date)
  CROSS JOIN (VALUES
    ('backoffice-multiproduct-cuenta-account-cancellation'),
    ('backoffice-multiproduct-credit-account-cancellation'),
    ('backoffice-phone-channel-cancellations'),
    ('backoffice-multiproduct-secured-account-cancellation'),
    ('backoffice-multiproduct-multi-account-cancellation')
  ) AS queues(queue)

  UNION ALL

  -- Group B: Payment queue - April 2026+
  SELECT agents.agent, agents.start_date, agents.end_date, 'backoffice-payment-srf' AS queue
  FROM (VALUES
    ('elizabeth.martinez', DATE '2026-04-09', DATE '2099-12-31'),
    ('daniel.cano', DATE '2026-04-09', DATE '2099-12-31'),
    ('jonathan.pineda', DATE '2026-04-09', DATE '2099-12-31'),
    ('adriana.marquez', DATE '2026-04-10', DATE '2099-12-31'),
    ('javier.balanzar', DATE '2026-04-10', DATE '2099-12-31'),
    ('carlos.gonzalez', DATE '2026-04-09', DATE '2099-12-31'),
    ('eden.martinez', DATE '2026-04-09', DATE '2099-12-31'),
    ('mariana.infante', DATE '2026-04-09', DATE '2099-12-31'),
    ('jorge.severiano', DATE '2026-04-13', DATE '2099-12-31'),
    ('fernanda.ibanez', DATE '2026-04-09', DATE '2099-12-31'),
    ('jose.velez', DATE '2026-04-09', DATE '2099-12-31'),
    ('ivette.melendez', DATE '2026-04-09', DATE '2099-12-31'),
    ('rocio.rodriguez', DATE '2026-04-09', DATE '2099-12-31')
  ) AS agents(agent, start_date, end_date)

  UNION ALL

  -- Group B2: Incoming transfers queue (only agents with both queues)
  SELECT agents.agent, agents.start_date, agents.end_date, 'backoffice-incoming-transfers' AS queue
  FROM (VALUES
    ('elizabeth.martinez', DATE '2026-04-09', DATE '2099-12-31'),
    ('jonathan.pineda', DATE '2026-04-09', DATE '2099-12-31'),
    ('adriana.marquez', DATE '2026-04-10', DATE '2099-12-31'),
    ('eden.martinez', DATE '2026-04-09', DATE '2099-12-31'),
    ('mariana.infante', DATE '2026-04-09', DATE '2099-12-31'),
    ('jorge.severiano', DATE '2026-04-13', DATE '2099-12-31'),
    ('jose.velez', DATE '2026-04-09', DATE '2099-12-31'),
    ('ivette.melendez', DATE '2026-04-09', DATE '2099-12-31'),
    ('rocio.rodriguez', DATE '2026-04-09', DATE '2099-12-31')
  ) AS agents(agent, start_date, end_date)

  UNION ALL

  -- Group C: Cancellation queues (4 queues, no phone-channel) - March 2026
  SELECT agents.agent, agents.start_date, agents.end_date, queues.queue
  FROM (VALUES
    ('fernanda.ibanez', DATE '2026-03-10', DATE '2026-03-29'),
    ('jose.velez', DATE '2026-03-10', DATE '2026-03-29'),
    ('ivette.melendez', DATE '2026-03-10', DATE '2026-03-29'),
    ('erik.licona', DATE '2026-03-10', DATE '2026-03-29')
  ) AS agents(agent, start_date, end_date)
  CROSS JOIN (VALUES
    ('backoffice-multiproduct-cuenta-account-cancellation'),
    ('backoffice-multiproduct-credit-account-cancellation'),
    ('backoffice-multiproduct-secured-account-cancellation'),
    ('backoffice-multiproduct-multi-account-cancellation')
  ) AS queues(queue)

  UNION ALL

  -- Group D: Cancellation queues (5 queues) - May 2026+
  SELECT agents.agent, agents.start_date, agents.end_date, queues.queue
  FROM (VALUES
    ('erik.licona', DATE '2026-05-16', DATE '2099-12-31'),
    ('jessica.gonzalez', DATE '2026-05-16', DATE '2099-12-31'),
    ('bertha.sanchez', DATE '2026-05-15', DATE '2099-12-31')
  ) AS agents(agent, start_date, end_date)
  CROSS JOIN (VALUES
    ('backoffice-multiproduct-cuenta-account-cancellation'),
    ('backoffice-multiproduct-credit-account-cancellation'),
    ('backoffice-phone-channel-cancellations'),
    ('backoffice-multiproduct-secured-account-cancellation'),
    ('backoffice-multiproduct-multi-account-cancellation')
  ) AS queues(queue)

  UNION ALL

  -- Group E: Credit cancellation queue only - May 2026+
  SELECT agents.agent, agents.start_date, agents.end_date, 'backoffice-multiproduct-credit-account-cancellation' AS queue
  FROM (VALUES
    ('javier.balanzar', DATE '2026-05-26', DATE '2099-12-31'),
    ('mariana.infante', DATE '2026-05-26', DATE '2099-12-31'),
    ('carlos.gonzalez', DATE '2026-05-26', DATE '2099-12-31'),
    ('eden.martinez', DATE '2026-05-26', DATE '2099-12-31'),
    ('jorge.severiano', DATE '2026-05-26', DATE '2099-12-31'),
    ('rocio.rodriguez', DATE '2026-05-22', DATE '2099-12-31')
  ) AS agents(agent, start_date, end_date)

  UNION ALL

  -- Group F: Cross support → cancellation queues, May 2026 leftovers
  -- Credit-account-cancellation only
  SELECT agents.agent, agents.start_date, agents.end_date, queues.queue
  FROM (VALUES
    ('jonathan.pineda', DATE '2026-05-22', DATE '2099-12-31'),
    ('daniel.cano', DATE '2026-05-24', DATE '2026-05-31'),
    ('fernanda.ibanez', DATE '2026-05-24', DATE '2099-12-31')
  ) AS agents(agent, start_date, end_date)
  CROSS JOIN (VALUES
    ('backoffice-multiproduct-credit-account-cancellation')
  ) AS queues(queue)

  UNION ALL

  -- Credit + cuenta cancellation
  SELECT agents.agent, agents.start_date, agents.end_date, queues.queue
  FROM (VALUES
    ('jose.velez', DATE '2026-05-22', DATE '2099-12-31'),
    ('ivette.melendez', DATE '2026-05-26', DATE '2099-12-31')
  ) AS agents(agent, start_date, end_date)
  CROSS JOIN (VALUES
    ('backoffice-multiproduct-credit-account-cancellation'),
    ('backoffice-multiproduct-cuenta-account-cancellation')
  ) AS queues(queue)

  UNION ALL

  -- Credit + cuenta + secured cancellation (two specific days only)
  SELECT agents.agent, agents.start_date, agents.end_date, queues.queue
  FROM (VALUES
    ('tania.llamas', DATE '2026-05-04', DATE '2026-05-06')
  ) AS agents(agent, start_date, end_date)
  CROSS JOIN (VALUES
    ('backoffice-multiproduct-credit-account-cancellation'),
    ('backoffice-multiproduct-cuenta-account-cancellation'),
    ('backoffice-multiproduct-secured-account-cancellation')
  ) AS queues(queue)
);

-- COMMAND ----------

-- DBTITLE 1,NTPJ Base
CREATE OR REPLACE TEMPORARY VIEW shuffle_jobs_ntpj AS (
  SELECT
    a.received_source_q
    , a.activity_type
    , a.status
    , a.net_time_spent
    , a.local_start_time
    , LOWER(REGEXP_EXTRACT(b.email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
  FROM etl.mx__dataset.ops_canonical_time_spent_activities AS a
  LEFT JOIN etl.mx__dataset.ops_actors AS b
    ON a.actor__id = b.actor__id
  LEFT JOIN manual_queue_exclusions_ntpj AS excl
    ON LOWER(REGEXP_EXTRACT(b.email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) = excl.agent
    AND CAST(a.local_start_time AS DATE) >= excl.start_date
    AND CAST(a.local_start_time AS DATE) <= excl.end_date
    AND LOWER(REPLACE(REPLACE(a.received_source_q, 'incredible_machine__', ''), '_', '-')) = excl.queue
  WHERE DATE_TRUNC('MONTH', a.local_start_time) >= '2025-01-01'
    AND a.actor_affiliation = 'nubank'
    AND a.status = 'finished'
    AND b.current_row_indicator = 'current'
    AND excl.agent IS NULL -- exclude cross-support queue matches
);

CREATE OR REPLACE TEMPORARY VIEW shuffle_jobs_agg_ntpj AS (
  SELECT
    received_source_q AS job_type
    , activity_type
    , status
    , DATE_TRUNC('DAY', local_start_time) AS start_date
    , agent
    , COUNT(*) AS count
    , SUM(net_time_spent) AS duration
    , CASE
        WHEN activity_type = 'email' THEN CONCAT('email - ', received_source_q, ' - ', status)
        WHEN activity_type = 'backoffice' THEN CONCAT('bko - ', received_source_q, ' - ', status)
        ELSE CONCAT(activity_type, ' - ', status)
      END AS job_id
  FROM shuffle_jobs_ntpj
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW oos_jobs_ntpj AS (
  SELECT
    CASE
      WHEN squad LIKE '%content%'
        THEN LOWER(REPLACE(TRIM(REPLACE(job_classification, '(OOS_CONT)', '')), ' ', '_'))
      ELSE job_classification
    END AS job_classification
    , net_time_spent_seconds
    , 'oos' AS activity_type
    , 'finished' AS status
    , local_start_date
    , LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , CASE
        WHEN UPPER(comment) RLIKE 'MOS[\\s_-]*\\d{4}'
          THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(comment), 'MOS[\\s_-]*(\\d{4})', 1))
        WHEN comment RLIKE '^\\s*\\d{4}\\s*$'
          THEN CONCAT('MOS-', TRIM(comment))
        WHEN UPPER(comment) RLIKE 'TICKET\\s+(MOS-?\\s*)?\\d{4}'
          THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(comment), 'TICKET\\s+(?:MOS-?\\s*)(\\d{4})', 1))
        ELSE NULL
      END AS content_id
  FROM etl.mx__dataset.taskmaster_consolidated_registry
  WHERE NOT (LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) = 'alan.elizalde'
    AND job_classification = '99 Minute - Carrier reports (OOS_LCYC)'
    AND CAST(local_start_date AS DATE) >= DATE '2026-04-29') -- excluded: bad behavior, affects nadia.tovias ntpj
  AND NOT (LOWER(squad) LIKE '%lifecycle%'
    AND job_classification = 'Estafeta - Carrier reports (OOS_LCYC)'
    AND DATE_TRUNC('MONTH', CAST(local_start_date AS DATE)) = DATE '2026-04-01') -- excluded: lifecycle squad, all April 2026

);

CREATE OR REPLACE TEMPORARY VIEW oos_jobs_agg_ntpj AS (
  SELECT
    job_classification AS job_type
    , activity_type
    , status
    , DATE_TRUNC('DAY', local_start_date) AS start_date
    , agent
    , COUNT(*) AS count
    , SUM(net_time_spent_seconds) AS duration
    , CONCAT(activity_type, ' - ', job_classification) AS job_id
  FROM oos_jobs_ntpj
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW jobs_base_ntpj AS (
  SELECT *
  FROM shuffle_jobs_agg_ntpj
  UNION ALL
  SELECT *
  FROM oos_jobs_agg_ntpj
);

CREATE OR REPLACE TEMPORARY VIEW expected_duration_per_job_ntpj AS (
  SELECT
    DATE_TRUNC('MONTH', a.start_date) AS start_month
    , a.job_id
    , TRY_DIVIDE(SUM(b.duration), SUM(b.count)) AS exp_duration_job
    , c.exclude
  FROM jobs_base_ntpj AS a
  JOIN jobs_base_ntpj AS b
    ON a.job_id = b.job_id
    AND ((DATE_TRUNC('MONTH', a.start_date) <= DATE '2026-03-01'
      AND DATE_TRUNC('MONTH', a.start_date) >= DATE_TRUNC('MONTH', b.start_date)
      AND DATE_TRUNC('MONTH', a.start_date) - INTERVAL 4 MONTHS <= DATE_TRUNC('MONTH', b.start_date))
    OR (DATE_TRUNC('MONTH', a.start_date) >= DATE '2026-04-01'
      AND DATE_TRUNC('MONTH', a.start_date) = DATE_TRUNC('MONTH', b.start_date)))
  LEFT JOIN manual_adjustments_ntpj AS c
    ON a.agent = c.agent
    AND DATE_TRUNC('DAY', a.start_date) = c.dime_date
    AND a.start_date >= TO_TIMESTAMP(c.slot_start)
    AND a.start_date < TO_TIMESTAMP(c.slot_start) + INTERVAL 30 MINUTES
  WHERE c.exclude IS NOT TRUE
    AND CAST(a.start_date AS DATE) NOT IN (DATE '2026-03-27', DATE '2026-04-09')
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW dime_ntpj AS (
  SELECT 
    LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
    , dime_date AS date
    , activity_type_required AS activity_type
    , unix_timestamp(local_timestamp_dime_slot_starts_at) + (6 * 60 * 60) AS slot_start
  FROM etl.mx__series_contract.agent_dimensioned_activities
  WHERE affiliation = 'nubank'
    AND dime_date >= '2024-12-30'
    AND activity_type_required IS NOT NULL
    AND activity_type_required NOT IN ('lunch_break', 'shrinkage','time_off')
    AND agent_dime_squad IS NOT NULL
    AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote')
    AND shuffle_status_required IN ('available', 'oos')
    AND NOT (LOWER(agent) LIKE '%maria.reyes%' AND dime_date >= '2026-02-01' AND dime_date < '2026-03-01') -- maternity leave
    AND NOT (LOWER(agent) LIKE '%tania.enciso%' AND dime_date IN (DATE '2026-05-08', DATE '2026-05-09')) -- vacation
    AND NOT (LOWER(agent) LIKE '%yerck.tellez%' AND dime_date IN (DATE '2026-03-03', DATE '2026-04-28')) -- vacation
    AND NOT (LOWER(agent) LIKE '%gabriela.vega%' AND dime_date = DATE '2026-05-12') -- vacation
    AND NOT (LOWER(agent) LIKE '%dulce.rivera%' AND dime_date = DATE '2026-03-29') -- vacation
    AND NOT (LOWER(agent) LIKE '%nadia.tovias%' AND dime_date = DATE '2026-04-23') -- vacation
    AND NOT (LOWER(agent) LIKE '%rodrigo.padilla%' AND dime_date = DATE '2026-03-10') -- vacation
    AND NOT (LOWER(agent) LIKE '%israel.cadena%' AND dime_date = DATE '2026-03-19') -- vacation
    AND NOT (LOWER(agent) LIKE '%uriel.alfaro%' AND dime_date IN (DATE '2026-04-06', DATE '2026-04-07', DATE '2026-05-13', DATE '2026-05-14', DATE '2026-05-15')) -- vacation
    AND NOT (LOWER(agent) LIKE '%yuridia.agama%' AND dime_date IN (DATE '2026-05-11', DATE '2026-05-12', DATE '2026-05-13', DATE '2026-05-14')) -- vacation
    AND NOT (LOWER(agent) LIKE '%alexis.torres%' AND dime_date IN (DATE '2026-05-21', DATE '2026-05-22')) -- vacation
    AND NOT (LOWER(agent) LIKE '%lucia.espinosa%' AND dime_date = DATE '2026-04-11') -- vacation
    AND NOT (LOWER(agent) LIKE '%adriana.lopez%' AND dime_date = DATE '2026-05-14') -- vacation
    AND NOT (LOWER(agent) LIKE '%jose.velez%' AND dime_date >= DATE '2026-03-24' AND dime_date <= DATE '2026-03-28') -- day controls
    AND NOT (LOWER(agent) LIKE '%carlos.gonzalez%' AND dime_date >= DATE '2026-03-24' AND dime_date <= DATE '2026-03-28') -- day controls
    AND NOT (LOWER(agent) LIKE '%jorge.ortega%' AND dime_date >= DATE '2026-03-24' AND dime_date <= DATE '2026-03-28') -- day controls
);

CREATE OR REPLACE TEMPORARY VIEW requested_hours_ntpj AS (
  SELECT 
    a.agent
    , a.date
    , a.activity_type
    , COUNT(*) / 2.0 as required_hours
    , b.exclude
  FROM dime_ntpj AS a
  LEFT JOIN manual_adjustments_ntpj AS b
    ON a.agent = b.agent
    AND a.date = b.dime_date
    AND a.slot_start = b.slot_start
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_initial_base AS(
  SELECT
    a.*
    , b.exp_duration_job
    , SUM(c.required_hours) AS required_hours
  FROM jobs_base_ntpj AS a
  LEFT JOIN expected_duration_per_job_ntpj AS b
    ON a.job_id = b.job_id
    AND DATE_TRUNC('MONTH', a.start_date) = b.start_month
  LEFT JOIN requested_hours_ntpj AS c
    ON a.agent = c.agent
    AND DATE_TRUNC('DAY', c.date) = a.start_date
    AND c.activity_type = a.activity_type
  WHERE c.exclude IS NOT TRUE
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_base AS(
  SELECT
    *
    , exp_duration_job * count AS total_exp_duration
  FROM ntpj_initial_base
  WHERE required_hours IS NOT NULL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_calculations AS(
  SELECT
    job_type
    , activity_type
    , status
    , job_id
    , REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) as agent
    , start_date
    -- , SUM(duration) AS job_time
    , SUM(exp_duration_job) AS exp_duration_job
    , SUM(duration) AS duration
    , SUM(count) AS count
    , required_hours
  FROM ntpj_base
  WHERE CAST(start_date AS DATE) NOT IN (DATE '2026-03-27', DATE '2026-04-09') -- deleting data with general access problems
  GROUP BY ALL
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_all_info_2026 AS(
  SELECT
    a.*
    , b.xforce
    , b.xplead
    , b.squad
    , b.squad_district
  FROM ntpj_calculations AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
    AND DATE_TRUNC('MONTH', a.start_date) = b.snapshot_month
  WHERE b.status = 'active'
    AND a.start_date >= '2025-12-01'
    AND NOT (a.agent IN ('jose.velez', 'carlos.gonzalez', 'jorge.ortega', 'luisa.castaneda', 'janet.castro', 'karen.ortega')
      AND a.start_date IN ('2026-03-24', '2026-03-25', '2026-03-26', '2026-03-27', '2026-03-28'))
    AND NOT (a.agent = 'jonathan.pineda' AND a.start_date = '2026-02-26')
    AND NOT (a.agent = 'maria.reyes' AND a.start_date >= '2026-02-01' AND a.start_date < '2026-03-01') -- maternity leave
    AND NOT (a.agent = 'tania.enciso' AND a.start_date IN ('2026-05-08', '2026-05-09')) -- vacation
    AND NOT (a.agent = 'yerck.tellez' AND CAST(a.start_date AS DATE) IN ('2026-03-03', '2026-04-28')) -- vacation
    AND NOT (a.agent = 'gabriela.vega' AND a.start_date = '2026-05-12') -- vacation
    AND NOT (a.agent = 'dulce.rivera' AND a.start_date = '2026-03-29') -- vacation
    AND NOT (a.agent = 'nadia.tovias' AND a.start_date = '2026-04-23') -- vacation
    AND NOT (a.agent = 'rodrigo.padilla' AND a.start_date = '2026-03-10') -- vacation
    AND NOT (a.agent = 'israel.cadena' AND a.start_date = '2026-03-19') -- vacation
    AND NOT (a.agent = 'uriel.alfaro' AND CAST(a.start_date AS DATE) IN ('2026-04-06', '2026-04-07', '2026-05-13', '2026-05-14', '2026-05-15')) -- vacation
    AND NOT (a.agent = 'yuridia.agama' AND CAST(a.start_date AS DATE) IN ('2026-05-11', '2026-05-12', '2026-05-13', '2026-05-14')) -- vacation
    AND NOT (a.agent = 'alexis.torres' AND CAST(a.start_date AS DATE) IN ('2026-05-21', '2026-05-22')) -- vacation
    AND NOT (a.agent = 'lucia.espinosa' AND a.start_date = '2026-04-11') -- vacation
    AND NOT (a.agent = 'adriana.lopez' AND a.start_date = '2026-05-14') -- vacation
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_all_info_2025 AS(
  SELECT
    a.*
    , b.xforce
    , b.xplead
    , b.squad
    , b.squad_district
  FROM ntpj_calculations AS a
  LEFT JOIN agent_information AS b
    ON a.agent = b.agent
  WHERE b.status = 'active'
    AND a.start_date < '2025-12-01'
    AND a.start_date >= '2025-01-01'
    AND b.snapshot_month = '2025-12-01'
);

CREATE OR REPLACE TEMPORARY VIEW ntpj_all_info AS(
  SELECT * FROM ntpj_all_info_2025
  UNION ALL
  SELECT * FROM ntpj_all_info_2026
);

-- SELECT * FROM ntpj_all_info

-- COMMAND ----------

-- DBTITLE 1,Table Save
CREATE OR REPLACE TABLE usr.mx__cx.normalized_time_per_job AS
SELECT * FROM ntpj_all_info

-- COMMAND ----------

SELECT 
    DISTINCT(exp_duration_job)
FROM usr.mx__cx.normalized_time_per_job
WHERE DATE_TRUNC('month', start_date) = '2026-03-01'

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW shuffle_jobs_n AS (
-- MAGIC   SELECT
-- MAGIC     a.received_source_q
-- MAGIC     , a.activity_type
-- MAGIC     , a.status
-- MAGIC     , a.net_time_spent
-- MAGIC     , a.local_start_time
-- MAGIC     , LOWER(REGEXP_EXTRACT(b.email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
-- MAGIC   FROM etl.mx__dataset.ops_canonical_time_spent_activities AS a
-- MAGIC   LEFT JOIN etl.mx__dataset.ops_actors AS b
-- MAGIC     ON a.actor__id = b.actor__id
-- MAGIC   WHERE DATE_TRUNC('MONTH', a.local_start_time) >= '2025-01-01'
-- MAGIC     AND a.actor_affiliation = 'nubank'
-- MAGIC     AND a.status = 'finished'
-- MAGIC     AND b.current_row_indicator = 'current'
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW shuffle_jobs_agg_n AS (
-- MAGIC   SELECT
-- MAGIC     received_source_q AS job_type
-- MAGIC     , activity_type
-- MAGIC     , status
-- MAGIC     , DATE_TRUNC('DAY', local_start_time) AS start_date
-- MAGIC     , agent
-- MAGIC     , COUNT(*) AS count
-- MAGIC     , SUM(net_time_spent) AS duration
-- MAGIC     , CASE
-- MAGIC         WHEN activity_type = 'email' THEN CONCAT('email - ', received_source_q, ' - ', status)
-- MAGIC         WHEN activity_type = 'backoffice' THEN CONCAT('bko - ', received_source_q, ' - ', status)
-- MAGIC         ELSE CONCAT(activity_type, ' - ', status)
-- MAGIC       END AS job_id
-- MAGIC   FROM shuffle_jobs_n
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW oos_jobs_n AS (
-- MAGIC   SELECT
-- MAGIC     CASE
-- MAGIC       WHEN squad LIKE '%content%'
-- MAGIC         THEN LOWER(REPLACE(TRIM(REPLACE(job_classification, '(OOS_CONT)', '')), ' ', '_'))
-- MAGIC       ELSE job_classification
-- MAGIC     END AS job_classification
-- MAGIC     , net_time_spent_seconds
-- MAGIC     , 'oos' AS activity_type
-- MAGIC     , 'finished' AS status
-- MAGIC     , local_start_date
-- MAGIC     , LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
-- MAGIC     , CASE
-- MAGIC         WHEN UPPER(comment) RLIKE 'MOS[\\s_-]*\\d{4}'
-- MAGIC           THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(comment), 'MOS[\\s_-]*(\\d{4})', 1))
-- MAGIC         WHEN comment RLIKE '^\\s*\\d{4}\\s*$'
-- MAGIC           THEN CONCAT('MOS-', TRIM(comment))
-- MAGIC         WHEN UPPER(comment) RLIKE 'TICKET\\s+(MOS-?\\s*)?\\d{4}'
-- MAGIC           THEN CONCAT('MOS-', REGEXP_EXTRACT(UPPER(comment), 'TICKET\\s+(?:MOS-?\\s*)?(\\d{4})', 1))
-- MAGIC         ELSE NULL
-- MAGIC       END AS content_id
-- MAGIC   FROM etl.mx__dataset.taskmaster_consolidated_registry
-- MAGIC   -- WHERE DATE_TRUNC('MONTH', local_start_time) >= '2025-04-01' -- ponto de atenção, checar se não precisa de filtro de data
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW oos_jobs_agg_n AS (
-- MAGIC   SELECT
-- MAGIC     job_classification AS job_type
-- MAGIC     , activity_type
-- MAGIC     , status
-- MAGIC     , DATE_TRUNC('DAY', local_start_date) AS start_date
-- MAGIC     , agent
-- MAGIC     , CASE
-- MAGIC         WHEN job_classification IN ('publish', 'learning_material_b', 'learning_material_c', 'sync', 'projects', 'weduka_a', 'weduka_b', 'discovery', 'weduka_optimization', 'learning_material_a', 'weduka_c', 'weduka_comms', 'purple_screens', 'emergency') THEN COUNT(DISTINCT content_id)
-- MAGIC         ELSE COUNT(*)
-- MAGIC       END AS count
-- MAGIC     , SUM(net_time_spent_seconds) AS duration
-- MAGIC     , CONCAT(activity_type, ' - ', job_classification) AS job_id
-- MAGIC   FROM oos_jobs_n
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW jobs_base_n AS (
-- MAGIC   SELECT *
-- MAGIC   FROM shuffle_jobs_agg_n
-- MAGIC   UNION ALL
-- MAGIC   SELECT *
-- MAGIC   FROM oos_jobs_agg_n
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW expected_duration_per_job_n AS (
-- MAGIC   SELECT
-- MAGIC     DATE_TRUNC('MONTH', a.start_date) AS start_month
-- MAGIC     , a.job_id
-- MAGIC     , a.job_type
-- MAGIC     , a.activity_type
-- MAGIC     , SUM(b.duration) / SUM(b.count) AS exp_duration_job
-- MAGIC   FROM jobs_base_n AS a
-- MAGIC   JOIN jobs_base_n AS b
-- MAGIC     ON a.job_id = b.job_id
-- MAGIC     AND DATE_TRUNC('MONTH', a.start_date) >= DATE_TRUNC('MONTH', b.start_date)
-- MAGIC     AND DATE_TRUNC('MONTH', a.start_date) - INTERVAL 4 MONTHS <= DATE_TRUNC('MONTH', b.start_date)
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW dime_n AS (
-- MAGIC   SELECT 
-- MAGIC     LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent
-- MAGIC     , dime_date AS date
-- MAGIC     , activity_type_required AS activity_type
-- MAGIC   FROM etl.mx__series_contract.agent_dimensioned_activities
-- MAGIC   WHERE affiliation = 'nubank'
-- MAGIC     AND dime_date >= '2024-12-30'
-- MAGIC     AND activity_type_required IS NOT NULL
-- MAGIC     AND activity_type_required NOT IN ('lunch_break', 'dime_invalid_notation', 'time_off')
-- MAGIC     AND agent_dime_squad IS NOT NULL
-- MAGIC     AND agent_dime_squad NOT IN ('wfm', 'credit_evolution', 'dote', 'social')
-- MAGIC     AND shuffle_status_required IN ('available', 'oos')
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW requested_hours_n AS (
-- MAGIC SELECT 
-- MAGIC   agent
-- MAGIC   , date
-- MAGIC   , activity_type
-- MAGIC   , COUNT(*) / 2.0 as required_hours
-- MAGIC FROM dime_n
-- MAGIC GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW ntpj_initial_base_n AS(
-- MAGIC   SELECT
-- MAGIC     a.*
-- MAGIC     , b.exp_duration_job
-- MAGIC     , SUM(c.required_hours) AS required_hours
-- MAGIC   FROM jobs_base_n AS a
-- MAGIC   LEFT JOIN expected_duration_per_job_n AS b
-- MAGIC     ON a.job_id = b.job_id
-- MAGIC     AND DATE_TRUNC('MONTH', a.start_date) = b.start_month
-- MAGIC   LEFT JOIN requested_hours_n AS c
-- MAGIC     ON a.agent = c.agent
-- MAGIC     AND DATE_TRUNC('DAY', c.date) = a.start_date
-- MAGIC     AND c.activity_type = a.activity_type
-- MAGIC   GROUP BY ALL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW monthly_snapshots AS (
-- MAGIC   SELECT 
-- MAGIC     REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS actor_name,
-- MAGIC     squad,
-- MAGIC     snapshot_date,
-- MAGIC     DATE_TRUNC('month', snapshot_date) AS snapshot_month,
-- MAGIC     ROW_NUMBER() OVER (PARTITION BY REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0), DATE_TRUNC('month', snapshot_date) ORDER BY snapshot_date DESC) AS rn
-- MAGIC   FROM etl.mx__series_contract.cx_mx_bdx_snapshots
-- MAGIC   WHERE actor_name IS NOT NULL
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW latest_per_month AS (
-- MAGIC   SELECT 
-- MAGIC     REGEXP_EXTRACT(a.actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS actor_name,
-- MAGIC     a.xforce_email,
-- MAGIC     a.xplead_email,
-- MAGIC     a.squad,
-- MAGIC     a.district,
-- MAGIC     a.status,
-- MAGIC     a.shift_name,
-- MAGIC     a.snapshot_date,
-- MAGIC     a.hire_start_date,
-- MAGIC     DATE_TRUNC('month', a.snapshot_date) AS snapshot_month
-- MAGIC   FROM etl.mx__series_contract.cx_mx_bdx_snapshots AS a
-- MAGIC   INNER JOIN monthly_snapshots AS b
-- MAGIC     ON REGEXP_EXTRACT(a.actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) = b.actor_name 
-- MAGIC     AND a.snapshot_date = b.snapshot_date
-- MAGIC   WHERE b.rn = 1
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW squad_changes AS (
-- MAGIC   SELECT 
-- MAGIC     actor_name,
-- MAGIC     squad,
-- MAGIC     snapshot_date,
-- MAGIC     snapshot_month,
-- MAGIC     LAG(squad) OVER (PARTITION BY actor_name ORDER BY snapshot_month) AS previous_squad,
-- MAGIC     LAG(snapshot_month) OVER (PARTITION BY actor_name ORDER BY snapshot_month) AS previous_month
-- MAGIC   FROM monthly_snapshots
-- MAGIC   WHERE rn = 1
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW agent_information AS(
-- MAGIC   SELECT 
-- MAGIC   REGEXP_EXTRACT(a.actor_name, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS agent,
-- MAGIC   REGEXP_EXTRACT(a.xplead_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS xplead,
-- MAGIC   REGEXP_EXTRACT(a.xforce_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0) AS xforce,
-- MAGIC   a.squad,
-- MAGIC   a.district AS squad_district,
-- MAGIC   a.status,
-- MAGIC   a.shift_name AS shift,
-- MAGIC   a.snapshot_date,
-- MAGIC   a.snapshot_month,
-- MAGIC   CASE 
-- MAGIC     WHEN b.previous_squad IS NULL THEN a.hire_start_date
-- MAGIC     WHEN b.previous_squad != a.squad THEN b.snapshot_date
-- MAGIC     ELSE a.hire_start_date
-- MAGIC   END AS last_change_date
-- MAGIC FROM latest_per_month AS a
-- MAGIC LEFT JOIN squad_changes AS b 
-- MAGIC   ON a.actor_name = b.actor_name 
-- MAGIC   AND a.snapshot_month = b.snapshot_month
-- MAGIC WHERE a.squad NOT IN ('social')
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW ntpj_all_info_2026 AS(
-- MAGIC   SELECT
-- MAGIC     a.*
-- MAGIC     , b.xforce
-- MAGIC     , b.xplead
-- MAGIC     , b.squad
-- MAGIC     , b.squad_district
-- MAGIC     , b.shift
-- MAGIC   FROM ntpj_initial_base_n AS a
-- MAGIC   LEFT JOIN agent_information AS b
-- MAGIC     ON a.agent = b.agent
-- MAGIC     AND DATE_TRUNC('MONTH', a.start_date) = b.snapshot_month
-- MAGIC   WHERE b.status = 'active'
-- MAGIC     AND a.start_date >= '2025-12-01'
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW ntpj_all_info_2025 AS(
-- MAGIC   SELECT
-- MAGIC     a.*
-- MAGIC     , b.xforce
-- MAGIC     , b.xplead
-- MAGIC     , b.squad
-- MAGIC     , b.squad_district
-- MAGIC     , b.shift
-- MAGIC   FROM ntpj_initial_base_n AS a
-- MAGIC   LEFT JOIN agent_information AS b
-- MAGIC     ON a.agent = b.agent
-- MAGIC   WHERE b.status = 'active'
-- MAGIC     AND a.start_date < '2025-12-01'
-- MAGIC     AND a.start_date >= '2025-01-01'
-- MAGIC     AND b.snapshot_month = '2025-12-01'
-- MAGIC );
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW ntpj_base_n AS(
-- MAGIC   SELECT * FROM ntpj_all_info_2025
-- MAGIC   UNION ALL
-- MAGIC   SELECT * FROM ntpj_all_info_2026
-- MAGIC );
-- MAGIC
-- MAGIC -- SELECT * FROM ntpj_base_n
-- MAGIC -- WHERE squad = 'content'           