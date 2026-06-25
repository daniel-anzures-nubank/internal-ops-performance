-- =====================================================================================
-- Agent Information — BDX-based roster extractor (shared)
-- =====================================================================================
--
-- Purpose
--   Returns one row per (agent, snapshot_month) with the agent's organizational
--   attribution (xforce, xplead, squad, district, shift) for the latest snapshot in
--   that month, plus the `last_change_date` (hire date or last squad-change date).
--
--   This is the canonical roster used by Core, Fraud, Social Media, AND Content
--   metrics. Core / Fraud / Social Media come from the BDX snapshots; Content comes
--   from a dedicated Google Sheet (`gsheets.sheets.mx_content_bdx`) that is unioned
--   in below (see "Content roster" section). It is intended to be reused by every
--   extractor that needs roster info.
--
-- Scope of this query (what IS done here)
--   * Pull all BDX snapshots from `etl.mx__series_contract.cx_mx_bdx_snapshots`.
--   * Keep only the latest snapshot per agent per month (the canonical month-end view
--     of the roster).
--   * Detect squad changes month-over-month and derive `last_change_date`:
--       - If the agent has no prior month, `last_change_date = hire_start_date`.
--       - If the squad changed from the prior month, `last_change_date = snapshot_date`
--         of the current month's snapshot.
--       - Otherwise, `last_change_date = hire_start_date`.
--   * Normalize email-derived fields to lowercase.
--   * Derive the performance `team` from `squad` (official mapping in
--     docs/team_squad_mapping.md) and exclude the non-team support squads
--     `quality` and `planning` (they are not part of any performance team).
--   * Attach the agent's `actor_id` from `etl.mx__dataset.ops_actors` so downstream
--     extractors (productivity, status history, etc.) can join on `actor_id` without
--     re-deriving the email mapping. See "Notes" below for the dedup rule.
--   * **Exclude `content` from the BDX branch** — content agents also appear in the
--     BDX snapshots, but the Content team's source of truth is the Google Sheet, so
--     we drop the BDX copies and source content from the sheet instead.
--
-- Content roster (the unioned Google Sheet)
--   * Source: currently `gsheets.sheets.mx_content_bdx_daniel_anz_temp` (the twin the
--     legacy Content notebook reads). The canonical target is
--     `gsheets.sheets.mx_content_bdx` (identical schema) — swap the FROM once SELECT
--     access is granted.
--   * Columns: `actor_email`, `xforce_email`, `xplead_email`, `squad`, `district`,
--     `status`, `target_squad`.
--   * The sheet is a **static current-state roster** — it has no snapshot history
--     (legacy hardcodes `valid_from = 2024-01-01`, `valid_to = 2099-12-31`). To fit
--     this view's `(agent, snapshot_month)` grain, we **cross-join the content roster
--     against every `snapshot_month` present in the BDX universe**, i.e. each content
--     agent is treated as valid in every month (matching legacy's wide valid range).
--   * Content squads differ from the BDX view: in the sheet, `squad = 'enablement'`
--     and `district = 'content'` (they support a `target_squad`). We therefore force
--     `team = 'content'` directly for these rows rather than deriving it from `squad`.
--   * `shift`, `hire_start_date`, and `last_change_date` are NULL for content (the
--     sheet does not carry them). `actor_id` is resolved from `ops_actors` like the
--     BDX branch. `snapshot_date` is set to `snapshot_month` (no real snapshot date).
--   * `target_squad` (the squad a content agent supports) IS surfaced (lowercased)
--     for the Content CSAT join. It is NULL for all BDX rows.
--
-- Out of scope (handled by the metrics / team layer)
--   * Team-specific squad filters. The legacy notebooks apply them at this stage
--     (e.g. Adherence/Shrinkage: `squad NOT IN ('social', 'content')`; NTPJ/NOcc:
--     `squad NOT IN ('content')`). The metric / team layer applies the filter that
--     matches its team's scope.
--   * Filtering by `status = 'active'`. Some legacy queries do this when joining the
--     roster to metric data; keep it in the metric layer so we can also reason about
--     historical inactive agents from this view.
--   * Any per-agent manual overrides (e.g. corrections to squad assignment for a
--     specific month). These move to the Adjustments layer.
--
-- Parameters
--   :period_end DATE  Upper bound on `snapshot_date`. Snapshots taken after this date
--                     are excluded so the roster reflects the world as of the run.
--                     Pass `current_date()` for a full-history run, or the period end
--                     for a backfill.
--
-- Output schema (one row per agent per `snapshot_month`)
--   agent             STRING  email prefix, lowercased (e.g. 'jane.doe')
--   actor_id          STRING  agent's `actor__id` from `ops_actors` (nullable; type
--                             inherited from the source column)
--   xforce            STRING  email prefix of the agent's xforce, lowercased
--   xplead            STRING  email prefix of the agent's xplead, lowercased
--   team              STRING  performance team, lowercased ('core' / 'fraud' /
--                             'social media' / 'content'; NULL for unmapped BDX
--                             squads). Derived from `squad` for BDX rows; forced to
--                             'content' for the Google-Sheet content rows. See
--                             docs/team_squad_mapping.md.
--   squad             STRING  raw squad name (no team filter applied). For content
--                             this is the sheet's `squad` (currently 'enablement').
--   squad_district    STRING  district associated with the squad ('content' for
--                             content rows).
--   status            STRING  raw status, lowercased (e.g. 'active', 'inactive')
--   shift             STRING  shift name from BDX (NULL for content — not in sheet)
--   snapshot_date     DATE    actual snapshot date for the latest BDX snapshot in the
--                             month; = snapshot_month for content (no real snapshot)
--   snapshot_month    DATE    DATE_TRUNC('month', snapshot_date)
--   hire_start_date   DATE    hire date from BDX (NULL for content — not in sheet)
--   last_change_date  DATE    hire date OR last squad-change date (NULL for content)
--   target_squad      STRING  the squad a content agent supports (from the content
--                             sheet, lowercased); NULL for all BDX rows. Used as the
--                             join key for the Content CSAT metric.
--
-- Notes
--   * Agent name is extracted from `actor_email` via the canonical regex
--     `^[a-zA-Z]+\.[a-zA-Z]+` and lowercased. Same regex used for xforce/xplead.
--   * `snapshot_month` is a DATE truncated to the first of the month (matches legacy).
--   * Rows with `actor_email IS NULL` are dropped (they cannot be attributed to an
--     agent).
--   * `ops_actors` is an SCD2 table — we filter to `current_row_indicator = 'current'`
--     to keep only the live version of each actor. Without this, agents whose record
--     was modified would appear multiple times.
--   * `actor_id` dedup rule: if an agent still has multiple distinct `actor__id`
--     values in `ops_actors` after the SCD2 filter (rare; happens when an agent
--     account is re-created), we keep `MAX(actor__id)` so this view stays
--     one-row-per-(agent, snapshot_month). Deterministic but arbitrary. Agents not
--     present in `ops_actors` get `actor_id = NULL` (LEFT JOIN).
-- =====================================================================================

WITH monthly_snapshots AS (
  SELECT
    LOWER(REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
    squad,
    snapshot_date,
    DATE_TRUNC('month', snapshot_date) AS snapshot_month,
    ROW_NUMBER() OVER (
      PARTITION BY
        LOWER(REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)),
        DATE_TRUNC('month', snapshot_date)
      ORDER BY snapshot_date DESC
    ) AS rn
  FROM etl.mx__series_contract.cx_mx_bdx_snapshots
  WHERE actor_email IS NOT NULL
    AND snapshot_date <= :period_end
),

latest_per_month AS (
  SELECT
    LOWER(REGEXP_EXTRACT(a.actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0))  AS agent,
    LOWER(REGEXP_EXTRACT(a.xforce_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS xforce,
    LOWER(REGEXP_EXTRACT(a.xplead_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS xplead,
    a.squad,
    a.district     AS squad_district,
    a.status,
    a.shift_name   AS shift,
    a.snapshot_date,
    DATE_TRUNC('month', a.snapshot_date) AS snapshot_month,
    a.hire_start_date
  FROM etl.mx__series_contract.cx_mx_bdx_snapshots AS a
  INNER JOIN monthly_snapshots AS m
    ON LOWER(REGEXP_EXTRACT(a.actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) = m.agent
   AND a.snapshot_date = m.snapshot_date
   AND m.rn = 1
),

squad_changes AS (
  SELECT
    agent,
    squad,
    snapshot_date,
    snapshot_month,
    LAG(squad)          OVER (PARTITION BY agent ORDER BY snapshot_month) AS previous_squad,
    LAG(snapshot_month) OVER (PARTITION BY agent ORDER BY snapshot_month) AS previous_month
  FROM monthly_snapshots
  WHERE rn = 1
),

agent_to_actor_id AS (
  SELECT
    LOWER(REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
    MAX(actor__id) AS actor_id
  FROM etl.mx__dataset.ops_actors
  WHERE email_address IS NOT NULL
    AND current_row_indicator = 'current'
  GROUP BY ALL
),

-- Distinct months present in the BDX universe. Used to expand the static content
-- roster (which has no snapshot history) across every month, so content agents
-- match downstream metric data in any month.
months_universe AS (
  SELECT DISTINCT snapshot_month
  FROM monthly_snapshots
  WHERE rn = 1
),

-- BDX-sourced roster (Core / Fraud / Social Media). Content is excluded here and
-- sourced from the Google Sheet below.
bdx_roster AS (
  SELECT
    l.agent,
    i.actor_id,
    l.xforce,
    l.xplead,
    CASE
      WHEN l.squad IN ('collections', 'credit', 'engagement', 'lifecycle', 'savings') THEN 'core'
      WHEN l.squad IN ('idsec', 'txn')                                                THEN 'fraud'
      WHEN l.squad = 'social'                                                         THEN 'social media'
      ELSE NULL
    END AS team,
    l.squad,
    l.squad_district,
    l.status,
    l.shift,
    l.snapshot_date,
    l.snapshot_month,
    l.hire_start_date,
    CASE
      WHEN c.previous_squad IS NULL          THEN l.hire_start_date
      WHEN c.previous_squad <> l.squad       THEN l.snapshot_date
      ELSE l.hire_start_date
    END AS last_change_date,
    CAST(NULL AS STRING) AS target_squad
  FROM latest_per_month AS l
  LEFT JOIN squad_changes AS c
    ON l.agent = c.agent
   AND l.snapshot_month = c.snapshot_month
  LEFT JOIN agent_to_actor_id AS i
    ON l.agent = i.agent
  -- `content` is dropped here: the Content team's source of truth is the sheet.
  WHERE l.squad NOT IN ('quality', 'planning', 'content')
),

-- Content roster from the dedicated Google Sheet (static current-state roster).
content_roster AS (
  SELECT
    LOWER(REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0))  AS agent,
    LOWER(REGEXP_EXTRACT(xforce_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS xforce,
    LOWER(REGEXP_EXTRACT(xplead_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS xplead,
    LOWER(squad)        AS squad,
    LOWER(district)     AS squad_district,
    LOWER(status)       AS status,
    LOWER(target_squad) AS target_squad
  -- TEMP: canonical source is `gsheets.sheets.mx_content_bdx` (no SELECT access yet);
  -- using the identical-schema `_daniel_anz_temp` twin that the legacy notebook reads.
  FROM gsheets.sheets.mx_content_bdx_daniel_anz_temp
  WHERE actor_email IS NOT NULL
),

-- Expand the static content roster across every snapshot month and conform it to
-- the BDX output shape. `team` is forced to 'content' (their squad is 'enablement',
-- which is not in the squad->team map); shift / hire / last_change are NULL.
-- `target_squad` (the squad a content agent supports) is surfaced for the content
-- CSAT join; it is NULL for all BDX rows.
content_monthly AS (
  SELECT
    cr.agent,
    i.actor_id,
    cr.xforce,
    cr.xplead,
    'content'            AS team,
    cr.squad,
    cr.squad_district,
    cr.status,
    CAST(NULL AS STRING) AS shift,
    m.snapshot_month     AS snapshot_date,
    m.snapshot_month,
    CAST(NULL AS DATE)   AS hire_start_date,
    CAST(NULL AS DATE)   AS last_change_date,
    cr.target_squad
  FROM content_roster AS cr
  CROSS JOIN months_universe AS m
  LEFT JOIN agent_to_actor_id AS i
    ON cr.agent = i.agent
)

SELECT * FROM bdx_roster
UNION ALL
SELECT * FROM content_monthly
