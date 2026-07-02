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

## Filters applied here (matching legacy at the DIME stage — raw table)

- **DIME**: keep slots with `activity_type_required IS NOT NULL`.
- **DIME systemic reclassifications are KEPT** (part of the occupancy matching
  logic, not a business exclusion):
  - `dimensioned_activity IN ('Control MC', 'xMC Debit Fraud')` →
    `activity_type_required = 'oos'`
  - `activity_type_required = 'dime_invalid_notation'` →
    `activity_type_required = 'oos'` — **except** SM DIME slots
    (`agent_dime_squad IN ('social', 'social_social')`) dated before
    `2026-07-01`, which are **dropped** instead (legacy SM scoring rule 3
    below).
- **DIME fixed legacy filters** (applied at the slot stage, so both the agent
  occupancy and the per-squad benchmark exclude them — legacy NOcc dataset
  lines 234–236):
  - `dimensioned_activity` not in (`Mouring`, `Weekly`, `Permiso Medico`,
    `Permiso medico`, `Huddle`, `Licencia`, `Vacacion`); a NULL is kept.
    **Fixed — all dates.**
  - `agent_dime_squad` non-NULL and not in (`wfm`, `credit_evolution`, `dote`).
    **Fixed — all dates.** Unlike legacy's NOcc dataset (line 236), `social` is
    NOT excluded: Social-Media occupancy is Sprinklr-sourced and intentionally
    ON for the whole history (see Social-Media occupancy below).
- **Jobs**: shuffle `status IN ('finished', 'transferred', 'skipped')`
  (occupancy counts *attempted* work, wider than NTPJ's `finished` only); OOS
  and SM rows get a synthetic `activity_type = 'oos'`. `sm_jobs` drops case rows
  with a NULL assignment/unassignment time (no measurable interval).
- **Roster**: `status = 'active'` (inner join on `(agent, snapshot_month)`).

## Social-Media occupancy (ON for all dates) and legacy SM scoring (pre-2026-07-01)

Legacy's main NOcc dataset excluded `agent_dime_squad = 'social'` DIME slots
and had no Sprinklr source, but the **Social-Media deck**
(`legacy/[IO] Performance 2026 - Social Media Temp Fix.sql`) computes SM
occupancy from Sprinklr case assignments. The new code keeps `social` DIME
slots and unions `sm_jobs` on **all dates**, so Social-Media occupancy is
populated for the whole history.

To reproduce the published legacy SM Normalized Occupancy **byte-for-byte**
for slot dates before the parity cutover, four legacy SM scoring quirks are
gated on the same condition — `agent_dime_squad IN ('social', 'social_social')`
(the squads the legacy deck scores, line 1065; constant `SM_DIME_SQUADS`) AND
slot date **before** `SM_EMPTY_SLOT_FULL_CREDIT_CUTOVER` (`2026-07-01`).
On/after the cutover the corrected behavior applies everywhere.

**1. Empty-slot full credit.** The legacy SM deck counts a dimensioned SM slot
with **no** overlapping matching-activity Sprinklr case as **fully occupied**:
`occupancy_agg` (lines 1123–1135) computes
`SUM(CASE WHEN activity_occuped = 1 THEN duration END)` — NULL when no
overlapping case matches the slot's activity type — and the downstream
`CASE WHEN SUM(occupancy_time) <= 1800 THEN SUM(occupancy_time) ELSE 1800 END`
(lines 1189 / 1223) evaluates `NULL <= 1800` to NULL, falling through to
`ELSE 1800` (seconds = the full 30-minute slot). Partially covered slots keep
their actual overlap seconds; only slots with zero matching cases get the 1800
default. Restricted to legacy's slot universe: its DIME filter (lines
1064 / 1079) drops `lunch_break` / `dime_invalid_notation` / `time_off` /
`shrinkage`, and eligibility is decided on the **pre-reclass**
`activity_type_required` (this module relabels `dime_invalid_notation` to
`'oos'`, but legacy never scored those slots). On/after the cutover an empty
slot is 0.

**2. No-dedup occupied sum.** Legacy has **no interval dedup** of overlapping
Sprinklr assignments: `occupancy_base` (lines 1103–1121) clips each job to the
slot and `occupancy_agg` (lines 1123–1135) sums those RAW clipped durations
as-is; the downstream 1800 cap (lines 1189 / 1223) bounds the slot —
`occ = LEAST(Σ clip(job), 1800)`. Reproduced for SM pre-cutover slots that
**have** a matching overlap (verified byte-exact against published legacy on
real data: half-open clipping + `Σ(cjob_end − cjob_start)` capped at 1800).
Non-SM slots (all dates) and SM slots on/after the cutover keep the
`prev_max_end` interval dedup.

**3. `dime_invalid_notation` slots are dropped, not reclassified.** Legacy's
SM DIME filter (line 1064) removes `dime_invalid_notation` slots from the
universe entirely — they never reach the numerator **or** the denominator —
whereas this module normally reclassifies them to `'oos'`. `filter_dime`
therefore filters OUT SM pre-cutover `dime_invalid_notation` slots (decided on
the pre-reclass value); non-SM squads and post-cutover SM slots keep the
reclassify behavior.

**4. Only social DIME squads are in scope for SM-team agents.** Legacy scores
ONLY slots whose `agent_dime_squad` is `social` / `social_social` for the SM
deck (line 1065), so an SM-team agent's slots from **other** DIME squads (e.g.
`collections`, `1err`) are out of scope pre-cutover. The performance `team` is
only known after the roster join, so `compute_slot_occupancy` tags each slot
with a `non_sm_dime_squad_pre_cutover` boolean (DIME squad NOT in
`SM_DIME_SQUADS` AND date < cutover) and `compute_occupancy_time` drops the
tagged rows where `team = 'social media'` post-join (the flag never reaches
the output schema). Non-SM teams keep all their slots on all dates; SM keeps
them from the cutover onward.

## luis.contreras OOS timestamp correction

Approved raw-data correction from `Correcciones Generales Datos`: his laptop
clock lagged Taskmaster during H1 2026, so his Content OOS job start/stop
timestamps are shifted **forward** before the overlap math (+2h through
2026-03-08, +1h from 2026-03-09 to 2026-05-19). Applied on `oos_jobs` rows where
`agent = 'luis.contreras'` and the squad contains `content`.

## How occupancy_minutes is computed

Per slot, jobs whose `activity_type` matches the slot's (reclassified)
`activity_type_required` are overlapped against the slot window. Because a
single slot can have multiple overlapping jobs of the same activity type (e.g.
an agent juggling two chats), overlapping same-activity intervals are merged
with the `prev_max_end` running-max trick before summing, to avoid
double-counting. The result is capped at 30 minutes per slot. Exception:
pre-cutover SM slots use legacy's raw no-dedup sum instead (rule 2 above).

## Date attribution (night shifts)

Slots are attributed to a `date` by the day the agent's shift started. For
agents whose roster `shift` is `'night'`, activity that crosses midnight is
rolled back onto the start day using a noon boundary (`DATE(slot_local - 12h)`),
effective `2026-07-01` onward only; non-night shifts and pre-July-2026 data keep
plain calendar-day attribution. See the shared rule in
[README → Date attribution (night shifts)](README.md#date-attribution-night-shifts).

## Deferred to the metrics layer (NOT applied here)

- Activity-type exclusions (`lunch_break` / `time_off` / `shrinkage`).
- The monthly district/shift occupancy benchmark (`occupancy_exp`).
- All per-agent manual adjustments / outage-date carve-outs (e.g. the
  2026-03-27 / 2026-04-09 global drops, vacation/leave windows, the
  `xplead = 'david.fernandez'` 2026-03-10 drop).

The `dimensioned_activity` meeting/leave exclusion and the DIME-squad exclusion
are **no longer deferred** — they are applied here (see the fixed legacy filters
above), matching where legacy applies them.

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
