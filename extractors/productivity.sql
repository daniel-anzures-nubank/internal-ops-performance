-- =====================================================================================
-- Productivity — raw extractor (agent_productivity + staffing_hero status resolution)
-- =====================================================================================
--
-- Purpose
--   Returns one row per `agent_productivity` activity within the period, with the
--   agent's email-prefix identifier already resolved via `ops_actors`, and with
--   `inferred_status` already coalesced from the multi-table `staffing_hero` status
--   history. Used by Adherence.
--
-- Why this composes multiple tables under "extraction"
--   The status of an agent at a given moment is physically split across:
--     * `agent_productivity.status` (sometimes NULL)
--     * `staffing_hero__actor_status_status_history`
--     * `staffing_hero__actor_statuses`
--     * `staffing_hero__status_options`
--   The metric layer doesn't care about that split — it wants "the status field".
--   Resolving the canonical field is identity / data-shape work, not metric logic.
--
-- Scope of this query (what IS done here)
--   * Pull `agent_productivity` rows within the period.
--   * Map `actor_id` -> `agent` (lowercased email prefix) via `ops_actors`, restricted
--     to `current_row_indicator = 'current'` (SCD2 live version only) so a single
--     actor_id maps to exactly one current email.
--   * For rows where `agent_productivity.status` IS NULL, resolve the most recent
--     prior status from `staffing_hero` history (limited to a 180-day lookback for
--     bounded scan cost).
--   * Expose both `raw_status` and `inferred_status` so the metric can choose.
--   * Final dedup on `(actor_id, timestamp)` — pick the row with the latest
--     `next_event_time` (longest activity) if duplicates somehow survive.
--
-- Out of scope (handled by the metrics layer)
--   * Filtering to "connected" statuses:
--       inferred_status IN ('available', 'oos', 'training')
--       OR (inferred_status = 'pause' AND level_3 = 'paused_with_jobs')
--       OR active_jobs > 0
--   * Filtering to a specific channel.
--   * Joining against DIME slots / computing slot overlap.
--   * The legacy treats `timestamp >= '2026-01-22' AND status IS NULL` as a special
--     fallback bucket (effectively "trust productivity even without status"). Decide
--     in the metric whether to preserve that.
--
-- Out of scope (handled by the Adjustments layer)
--   * Any per-agent status overrides.
--
-- Parameters
--   :period_start DATE  inclusive lower bound on the period
--   :period_end   DATE  inclusive upper bound on the period
--
--   NOTE: the productivity timestamp window is `:period_start − 1 day` to
--   `:period_end + 1 day` (i.e. `timestamp < :period_end + 2`). The ±1-day
--   buffer covers the local-vs-UTC timezone offset for DIME slots:
--   Mexico-City `dime_date = D` slots span `D 06:00` through `D+1 06:00` UTC,
--   so productivity overlapping a night-shift slot on the last day of the
--   period can have `timestamp` up to `D+1 06:00 UTC`. The metric layer
--   discards productivity that doesn't overlap any in-period slot.
--
-- Output schema (one row per productivity activity)
--   agent                STRING     email prefix, lowercased
--   actor_id             STRING     raw `actor_id` from agent_productivity
--   timestamp            TIMESTAMP  activity start time (UTC, raw)
--   next_event_time      TIMESTAMP  activity end time (UTC, raw)
--   activity_start_unix  BIGINT     `UNIX_TIMESTAMP(timestamp)`
--   activity_end_unix    BIGINT     `UNIX_TIMESTAMP(next_event_time)`
--   raw_status           STRING     `agent_productivity.status` as-is (NULL when unresolved)
--   inferred_status      STRING     `COALESCE(raw_status, staffing_hero status_option__name)`
--   channel              ARRAY<STRING>  `channel_active` (e.g. `["chat"]`)
--   active_jobs          INT        `active_jobs`
--   level_3              STRING     `level_3` (used to detect `paused_with_jobs`)
--
-- Timezone notes
--   * `agent_productivity.timestamp` and `next_event_time` are stored in **UTC**.
--   * When the metric layer joins this with DIME (local-time), it must offset DIME
--     by `+6h`. See `dime_slots.sql` for details.
-- =====================================================================================

WITH agent_email_to_actor_id AS (
  SELECT DISTINCT
    LOWER(REGEXP_EXTRACT(email_address, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)) AS agent,
    actor__id
  FROM etl.mx__dataset.ops_actors
  WHERE email_address IS NOT NULL
    AND current_row_indicator = 'current'
),

status_history AS (
  SELECT
    h.actor_status__id,
    h.status_option__id,
    h.db__tx_instant,
    s.actor_status__actor_id AS actor_id,
    o.status_option__name
  FROM etl.mx__contract.staffing_hero__actor_status_status_history AS h
  LEFT JOIN etl.mx__contract.staffing_hero__actor_statuses AS s
    ON h.actor_status__id = s.actor_status__id
  LEFT JOIN etl.mx__contract.staffing_hero__status_options AS o
    ON o.status_option__id = h.status_option__id
  WHERE h.db__tx_instant >= DATE_SUB(:period_end, 180)
),

productivity_status_resolution AS (
  SELECT
    p.actor_id,
    p.timestamp,
    h.status_option__name,
    ROW_NUMBER() OVER (
      PARTITION BY p.actor_id, p.timestamp
      ORDER BY h.db__tx_instant DESC
    ) AS rn
  FROM etl.mx__dataset.agent_productivity AS p
  LEFT JOIN status_history AS h
    ON h.actor_id = p.actor_id
    AND h.db__tx_instant <= p.timestamp
  WHERE p.timestamp >= DATE_SUB(:period_start, 1)
    AND p.timestamp <  DATE_ADD(:period_end, 2)
    AND p.status IS NULL
)

SELECT
  agent,
  actor_id,
  timestamp,
  next_event_time,
  activity_start_unix,
  activity_end_unix,
  raw_status,
  inferred_status,
  channel,
  active_jobs,
  level_3
FROM (
  SELECT
    e.agent,
    p.actor_id,
    p.timestamp,
    p.next_event_time,
    UNIX_TIMESTAMP(p.timestamp)                AS activity_start_unix,
    UNIX_TIMESTAMP(p.next_event_time)          AS activity_end_unix,
    p.status                                   AS raw_status,
    COALESCE(p.status, r.status_option__name)  AS inferred_status,
    p.channel_active                           AS channel,
    p.active_jobs,
    p.level_3,
    ROW_NUMBER() OVER (
      PARTITION BY p.actor_id, p.timestamp
      ORDER BY p.next_event_time DESC NULLS LAST
    ) AS _dedup_rn
  FROM etl.mx__dataset.agent_productivity AS p
  INNER JOIN agent_email_to_actor_id AS e
    ON p.actor_id = e.actor__id
  LEFT JOIN productivity_status_resolution AS r
    ON r.actor_id = p.actor_id
   AND r.timestamp = p.timestamp
   AND r.rn = 1
  WHERE p.timestamp >= DATE_SUB(:period_start, 1)
    AND p.timestamp <  DATE_ADD(:period_end, 2)
) AS deduped
WHERE _dedup_rn = 1
