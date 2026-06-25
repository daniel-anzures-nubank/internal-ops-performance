# occupancy_time

Raw per-slot occupancy / required minutes — the occupancy twin of
`adherent_time`. **One row per agent per DIME slot.**

`occupancy_minutes` measures the minutes the agent spent *actually working
jobs* whose `activity_type` matches the slot's `activity_type_required`. Feeds
the future Normalized-Occupancy metric and its district/shift benchmark.

- Module: `metrics_data/occupancy_time.py`
- Build script: `scripts/metrics_data_scripts/build_occupancy_time.py`
- Default target table: `usr.danielanzures.io_occupancy_time_raw`

## Team coverage

Occupancy is computed from a different job source per team, but they all flow
into this one table via the same per-slot overlap logic:

- **Core / Fraud**: shuffle (`shuffle_jobs`) + out-of-shuffle (`oos_jobs`) job
  executions matched to DIME slots.
- **Social Media**: social agents have no shuffle/OOS jobs — their work lives in
  Sprinklr. `sm_jobs` (`sprinklr_normalized_occupancy_data`) supplies one
  interval per social **case assignment**, unioned in as `oos`-typed jobs so
  they match social agents' `activity_type_required = 'oos'` DIME slots. This
  mirrors the legacy `Social Media` notebook, which stamps every case `'oos'`
  and compares `unix_timestamp(case_assignment_time)` directly against the DIME
  slot unix (verified: social `oos` slot hours line up with assignment hours).
  Verified live (early May 2026): with `sm_jobs`, 26 social agents get non-zero
  occupancy; without it, social occupancy was 0 everywhere.
- **Content**: content jobs are **all OOS** — they live entirely in
  `taskmaster_consolidated_registry` (via `oos_jobs`). Content agents have
  effectively no shuffle jobs, so `shuffle_jobs` contributes nothing for them.
  No dedicated source is needed (unlike Social Media): `oos_jobs` has no squad
  filter, so content's OOS jobs match their DIME slots automatically. Verified
  live (weekdays, early May 2026): 16/17 content agents get non-zero occupancy,
  ~57% of slots occupied. Note content barely works weekends — a Sat/Sun-only
  sample shows 0 occupancy simply because there are no taskmaster rows those days.

## Source tables

| extractor | underlying table(s) | role |
|-----------|---------------------|------|
| `agent_information` | `etl.mx__series_contract.cx_mx_bdx_snapshots` + `ops_actors` | roster dimensions, active filter |
| `dime_slots` | `etl.mx__series_contract.agent_dimensioned_activities` (`affiliation = 'nubank'`) | one row per 30-min scheduled slot |
| `shuffle_jobs` | `etl.mx__dataset.ops_canonical_time_spent_activities` (`actor_affiliation = 'nubank'`) | in-shuffle job executions (Core/Fraud/Content) |
| `oos_jobs` | `etl.mx__dataset.taskmaster_consolidated_registry` | out-of-shuffle job executions (Core/Fraud/Content) |
| `sm_jobs` | `usr.sprinklr_api_data_integration.sprinklr_normalized_occupancy_data` | Social-Media case assignments (occupancy source for social agents); unioned as `oos` jobs |

## Filters applied here (minimal — raw table)

- **DIME**: keep slots with `activity_type_required IS NOT NULL` only.
- **DIME systemic reclassifications are KEPT** (part of the occupancy matching
  logic, not a business exclusion):
  - `dimensioned_activity IN ('Control MC', 'xMC Debit Fraud')` →
    `activity_type_required = 'oos'`
  - `activity_type_required = 'dime_invalid_notation'` →
    `activity_type_required = 'oos'`
- **Jobs**: shuffle `status IN ('finished', 'transferred', 'skipped')`
  (occupancy counts *attempted* work, wider than NTPJ's `finished` only); OOS
  and SM rows get a synthetic `activity_type = 'oos'`. `sm_jobs` drops case rows
  with a NULL assignment/unassignment time (no measurable interval).
- **Roster**: `status = 'active'` (inner join on `(agent, snapshot_month)`).

## How occupancy_minutes is computed

Per slot, jobs whose `activity_type` matches the slot's (reclassified)
`activity_type_required` are overlapped against the slot window. Because a
single slot can have multiple overlapping jobs of the same activity type (e.g.
an agent juggling two chats), overlapping same-activity intervals are merged
with the `prev_max_end` running-max trick before summing, to avoid
double-counting. The result is capped at 30 minutes per slot.

## Date attribution (night shifts)

Slots are attributed to a `date` by the day the agent's shift started. For
agents whose roster `shift` is `'night'`, activity that crosses midnight is
rolled back onto the start day using a noon boundary (`DATE(slot_local - 12h)`),
effective `2026-07-01` onward only; non-night shifts and pre-July-2026 data keep
plain calendar-day attribution. See the shared rule in
[README → Date attribution (night shifts)](README.md#date-attribution-night-shifts).

## Deferred to the metrics layer (NOT applied here)

- Activity-type exclusions (`lunch_break` / `time_off` / `shrinkage`).
- `dimensioned_activity` exclusions (Mouring / Weekly / Permiso Medico /
  Permiso medico / Huddle / Licencia / Vacacion).
- DIME squad exclusions (`wfm` / `credit_evolution` / `dote` / …).
- The monthly district/shift occupancy benchmark (`occupancy_exp`).
- All per-agent manual adjustments / outage-date carve-outs.

## Output schema (one row per agent per DIME slot)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | |
| `xplead` | STRING | |
| `team` | STRING | performance team derived from squad (`core` / `fraud` / `social media` / `content`); see `docs/team_squad_mapping.md` |
| `squad` | STRING | roster squad |
| `district` | STRING | roster district (was `squad_district`) |
| `shift` | STRING | roster shift |
| `date` | DATE | |
| `slot_time` | STRING | local time-of-day `HH:MM:SS` of the slot start |
| `activity_type_required` | STRING | DIME activity type (after systemic reclassification) |
| `required_minutes` | DOUBLE | slot length in minutes (always 30.0) |
| `occupancy_minutes` | DOUBLE | minutes occupied (≤ 30) |
