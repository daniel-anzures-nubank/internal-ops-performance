-- =====================================================================================
-- Content CSAT — raw monthly satisfaction survey extractor (Google Sheet)
-- =====================================================================================
--
-- Purpose
--   Returns one row per Content CSAT **survey response**. Each response is a squad
--   representative rating how well the Content (enablement) team supported their
--   squad that month, across 8 questions scored 1-5. This is the raw feed for the
--   Content **Quality (CSAT)** metric (`promoters / total_questions`, promoter =
--   answer >= 4; target >= 95%). CSAT only applies to Content.
--
-- Source
--   Currently `gsheets.sheets.mx_content_csat_daniel_anz_temp` (the twin the legacy
--   Content notebook reads). The canonical sheet is `gsheets.sheets.mx_content_csat`
--   — swap the FROM once SELECT access is granted.
--
-- Scope of this query (what IS done here)
--   * Parse the sheet's string `timestamp` ('M/d/yyyy H:mm:ss') into a TIMESTAMP.
--   * Derive `date_reference = survey_timestamp - 1 month` — the month the survey is
--     ABOUT (a survey filled in April rates March), matching legacy.
--   * Normalize the respondent to an email prefix (lowercased) from `email_address`.
--   * Expose the 8 raw question scores (1-5) and the separate `nps` score raw.
--   * Expose the raw `squad` (the supported squad, display form e.g. 'E.M.I.', 'TXN').
--
-- Out of scope (handled by the metrics layer)
--   * Promoter flagging (`score >= 4`) and the `promoters / number_of_questions`
--     ratio — done in `metrics_data/content_csat.py`.
--   * The `squad`->`target_squad` normalization (E.M.I. / GENERAL(...) -> emi_general,
--     else lowercase) used as the roster join key — done in the module so the raw
--     `squad` stays untouched here.
--   * The `target_squad`-based fan-out join to the content roster.
--   * Per-agent / per-period aggregation.
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `date_reference` (the survey's month)
--   :period_end   DATE  inclusive upper bound on `date_reference`
--
-- Output schema (one row per survey response)
--   survey_timestamp        TIMESTAMP  when the survey was filled (parsed)
--   date_reference          TIMESTAMP  survey_timestamp - 1 month (month rated)
--   requested_by            STRING     respondent email prefix, lowercased
--   email_address           STRING     raw respondent email
--   squad                   STRING     raw supported squad (display form)
--   mes                     STRING     raw month label from the sheet (e.g. 'Marzo 2026')
--   facilidad               BIGINT     question score 1-5
--   comprension             BIGINT     question score 1-5
--   comunicacion            BIGINT     question score 1-5
--   calidad                 BIGINT     question score 1-5
--   tiempo                  BIGINT     question score 1-5
--   manejo_de_cambios       BIGINT     question score 1-5
--   expectativas            BIGINT     question score 1-5
--   aportacion_estrategica  BIGINT     question score 1-5
--   nps                     BIGINT     separate 0-10 NPS score (not part of CSAT)
-- =====================================================================================

SELECT
  TO_TIMESTAMP(timestamp, 'M/d/yyyy H:mm:ss')                       AS survey_timestamp,
  TO_TIMESTAMP(timestamp, 'M/d/yyyy H:mm:ss') - INTERVAL 1 MONTH    AS date_reference,
  LOWER(REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS requested_by,
  email_address,
  squad,
  mes,
  facilidad,
  comprension,
  comunicacion,
  calidad,
  tiempo,
  manejo_de_cambios,
  expectativas,
  aportacion_estrategica,
  nps
FROM gsheets.sheets.mx_content_csat_daniel_anz_temp
WHERE timestamp IS NOT NULL
  AND timestamp != ''
  AND (TO_TIMESTAMP(timestamp, 'M/d/yyyy H:mm:ss') - INTERVAL 1 MONTH) >= :period_start
  AND (TO_TIMESTAMP(timestamp, 'M/d/yyyy H:mm:ss') - INTERVAL 1 MONTH) <= :period_end
