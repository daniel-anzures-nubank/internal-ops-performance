-- =====================================================================================
-- WoWs — Social-Media "WoW" experiences extractor (Google Sheet)
-- =====================================================================================
--
-- Purpose
--   Returns one row per WoW experience logged for a Social-Media Xpeer in the
--   WoWs Google Sheet. This is the raw feed for the **WoWs** metric (count of WoW
--   experiences delivered; monthly target >= 5). WoWs only apply to Social Media.
--
-- Source
--   `gsheets.sheets.mx_wows_daniel_temp` — this is what the legacy `Social Media`
--   notebook actually reads. The notebook flags `gsheets.sheets.mx_wows_social_media`
--   as the intended canonical source ("Change temporary fix"), and
--   docs/metrics_definitions.md lists it too, but that sheet is not currently
--   readable (PERMISSION_DENIED). Swap the FROM clause once access is granted.
--
-- Scope of this query (what IS done here)
--   * Pull every WoW row with a non-empty `date`.
--   * Normalize agent to email prefix (lowercased) from `agent` (the sheet stores
--     full emails — note: `@nubank.com.br` domain — but the name regex is the same).
--   * Parse the sheet's ISO-string `date` to a DATE.
--   * Expose `case_id` raw (the WoW's case identifier).
--
-- Out of scope (handled by the metrics layer)
--   * `COUNT(DISTINCT case_id)` per agent/day and the monthly target (>= 5).
--   * The outage-date exclusion (`2026-03-27`).
--   * Roster join / active filter / squad scoping.
--
-- Parameters
--   :period_start DATE  inclusive lower bound on `DATE(date)`
--   :period_end   DATE  inclusive upper bound on `DATE(date)`
--
-- Output schema (one row per WoW experience)
--   agent         STRING   email prefix, lowercased (empty if unparseable)
--   agent_email   STRING   raw agent email from the sheet
--   case_id       STRING   the WoW's case identifier
--   date          DATE     `DATE(date)` — day the WoW was logged
-- =====================================================================================

SELECT
  LOWER(REGEXP_EXTRACT(agent, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
  agent                          AS agent_email,
  CAST(case_id AS STRING)        AS case_id,
  DATE(date)                     AS date
FROM gsheets.sheets.mx_wows_daniel_temp
WHERE date != ''
  AND DATE(date) >= :period_start
  AND DATE(date) <= :period_end
