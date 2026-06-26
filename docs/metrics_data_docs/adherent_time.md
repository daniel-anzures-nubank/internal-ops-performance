# adherent_time

Raw per-slot adherent / required minutes. **One row per agent per DIME slot.**

Feeds the future Adherence metric: Adherence = `SUM(adherent_minutes) /
SUM(required_minutes)` at whatever grain the metric layer chooses (after it
applies the business exclusions listed below).

- Module: `metrics_data/adherent_time.py`
- Build script: `scripts/metrics_data_scripts/build_adherent_time.py`
- Default target table: `usr.danielanzures.io_adherent_time_raw`

## Source tables

| extractor | underlying table(s) | role |
|-----------|---------------------|------|
| `agent_information` | `etl.mx__series_contract.cx_mx_bdx_snapshots` + `ops_actors` | roster dimensions, active filter |
| `dime_slots` | `etl.mx__series_contract.agent_dimensioned_activities` (`affiliation = 'nubank'`) | one row per 30-min scheduled slot |
| `productivity` | agent productivity / status log (UTC timestamps) | the "connected/working" intervals |

## Filters applied here (minimal — raw table)

- **DIME**: keep slots with `activity_type_required IS NOT NULL`, drop the
  meeting/leave `dimensioned_activity` slots (Mouring / Weekly / Permiso Medico /
  Permiso medico / Huddle / Licencia / Vacacion; NULL kept), and drop the
  excluded DIME squads (`agent_dime_squad` not in `wfm` / `credit_evolution` /
  `dote`, NULL dropped). Both are fixed legacy DIME filters, not manual
  adjustments.
- **Productivity**: keep "connected" rows (legacy `agent_productivity` WHERE):
  - `inferred_status IN ('available', 'oos', 'training')`, OR
  - `inferred_status = 'pause' AND level_3 = 'paused_with_jobs'`, OR
  - `active_jobs > 0`, OR
  - `timestamp >= 2026-01-22 AND inferred_status IS NULL`.
- **Roster**: `status = 'active'` (inner join on `(agent, snapshot_month)`).

## Team coverage

This table covers **all teams** (Core, Fraud, Social Media, Content) — the
roster has no squad filter and the module doesn't exclude any squad. Social
Media adherence uses the **identical** source tables and overlap logic as Core
(the legacy `Social Media` notebook just scopes the same DIME + productivity
bases to social agents via the roster join). Verified against live data: all 28
active social agents have DIME slots in May 2026, and
`build_adherent_time.py --dry-run` returns `social`-squad rows.

## How adherent_minutes is computed

Per agent, each DIME slot is overlap-joined against the connected productivity
intervals. Overlap rule: `activity_end >= slot_start AND activity_start <
slot_end`. Per pair, `overlap = LEAST(ends) - GREATEST(starts)` clipped to
`[0, 1800]` seconds; summed per slot and capped at 1800. DIME slot times are
local; productivity is UTC, so DIME is shifted +6h (Mexico City, fixed UTC-6,
no DST) before the comparison.

**Unmatched slots — legacy-phantom replication (pre-2026-07-01).** A scheduled
slot that matched no productivity scores `adherent_minutes = 0` from `2026-07-01`
onward (the correct value — unworked-but-scheduled slots still appear).
**Before** that cutover the new pipeline reproduces the legacy
**phantom-adherence bug**: an unmatched slot is counted as *fully* adherent (a
whole 1800s slot → a fake 100% adherence), so historical metrics stay
byte-for-byte with legacy. Gated on the slot's calendar date
(`LEGACY_PHANTOM_CUTOVER`) — the same 2026-07-01 migration cutover the
night-shift attribution uses.

## Date attribution (night shifts)

Slots are attributed to a `date` by the day the agent's shift started. For
agents whose roster `shift` is `'night'`, activity that crosses midnight (an
evening head + the following early-morning tail) is rolled back onto the start
day using a noon boundary (`DATE(slot_local - 12h)`), effective `2026-07-01`
onward only. Non-night shifts and all pre-July-2026 data keep plain calendar-day
attribution. See the shared rule in
[README → Date attribution (night shifts)](README.md#date-attribution-night-shifts).

## Deferred to the metrics layer (NOT applied here)

- Activity-type exclusions (`lunch_break` / `time_off` / `shrinkage`).
- All manual adjustments (agent-date carve-outs, training/shadowing windows,
  maternity leave, outage dates).

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
| `activity_type_required` | STRING | DIME activity type for the slot |
| `required_minutes` | DOUBLE | slot length in minutes (always 30.0) |
| `adherent_minutes` | DOUBLE | adherent minutes in the slot (≤ 30) |
