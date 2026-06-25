# shrinkage_slots

Raw per-slot shrinkage flags. **One row per DIME slot** for active agents, each
tagged with whether it is shrinkage and, if so, whether that shrinkage is
controllable or not.

Feeds the future Shrinkage metric: Shrinkage ratio = `SUM(shrinkage_flag) /
SUM(required_slots)`, where the denominator rule is applied by the metric layer.

- Module: `metrics_data/shrinkage_slots.py`
- Build script: `scripts/metrics_data_scripts/build_shrinkage_slots.py`
- Default target table: `usr.danielanzures.io_shrinkage_slots_raw`

## Source tables

| extractor | underlying table(s) | role |
|-----------|---------------------|------|
| `agent_information` | `etl.mx__series_contract.cx_mx_bdx_snapshots` + `ops_actors` | roster dimensions, active filter |
| `dime_slots` | `etl.mx__series_contract.agent_dimensioned_activities` (`affiliation = 'nubank'`) | one row per 30-min scheduled slot |

## Filters applied here (minimal — raw table)

- **DIME**: keep slots with `activity_type_required IS NOT NULL` only.
- **Roster**: `status = 'active'` (inner join on `(agent, snapshot_month)`).

## What the flags mean

### `shrinkage_flag` — the legacy slot-level rule (switches at 2026-03-01)

- **Pre-cutover** (`date < 2026-03-01`): `activity_type_required = 'shrinkage'`.
- **Post-cutover** (`date >= 2026-03-01`): `activity_type_required = 'shrinkage'`
  **OR** (`activity_type_required = 'dime_invalid_notation'` **AND**
  `dimensioned_activity` is a meeting/leave annotation: Mouring / Weekly /
  Permiso Medico / Permiso medico / Huddle / Licencia / Vacacion).

### `controllable_shrinkage_flag` / `uncontrollable_shrinkage_flag`

Among shrinkage slots only, the split is by `dimensioned_activity`:

- **uncontrollable** — `dimensioned_activity` in (`Licencia` / `licencia` /
  `SKR_LCNC`), matched case-insensitively. These are leave/licencia, outside
  the operation's control.
- **controllable** — every other shrinkage slot.

Non-shrinkage slots get all three flags `= 0`. By construction,
`controllable_shrinkage_flag + uncontrollable_shrinkage_flag = shrinkage_flag`
for every row.

## Date attribution (night shifts)

Slots are attributed to a `date` by the day the agent's shift started. For
agents whose roster `shift` is `'night'`, activity that crosses midnight is
rolled back onto the start day using a noon boundary (`DATE(slot_local - 12h)`),
effective `2026-07-01` onward only. This runs **after** the `shrinkage_flag`
classification (whose own 2026-03-01 formula switch keys off the calendar date),
so the flag rule is unaffected. Non-night shifts and pre-July-2026 data keep
plain calendar-day attribution. See the shared rule in
[README → Date attribution (night shifts)](README.md#date-attribution-night-shifts).

## Deferred to the metrics layer (NOT applied here)

- The required/denominator definition (legacy `required_slot`: pre-cutover
  drops `dime_invalid_notation`, post-cutover drops `time_off`). The raw slot
  universe is here; the metric layer applies the denominator rule.
- DIME squad / activity-type business exclusions.
- All training / shadowing / maternity / outage manual adjustments.

## Output schema (one row per DIME slot)

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
| `activity_type_required` | STRING | |
| `dimensioned_activity` | STRING | |
| `shrinkage_flag` | INT | 1 if the slot is shrinkage, else 0 |
| `controllable_shrinkage_flag` | INT | 1 if shrinkage AND controllable, else 0 |
| `uncontrollable_shrinkage_flag` | INT | 1 if shrinkage AND uncontrollable, else 0 |
