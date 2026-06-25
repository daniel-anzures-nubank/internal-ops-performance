# jobs_raw

Raw per-job feed (shuffle + OOS). **One row per individual job**, with raw
start/end timestamps, classification fields, and a single derived flag.

Feeds the future NTPJ metric: the metrics layer aggregates to `(agent, date,
job_id)` with count / duration, computes the monthly expected-duration
benchmark, and forms the NTPJ ratio.

- Module: `metrics_data/jobs_raw.py`
- Build script: `scripts/metrics_data_scripts/build_jobs_raw.py`
- Default target table: `usr.danielanzures.io_jobs_raw`

## Source tables

| extractor | underlying table(s) | role |
|-----------|---------------------|------|
| `agent_information` | `etl.mx__series_contract.cx_mx_bdx_snapshots` + `ops_actors` | roster dimensions, active filter |
| `dime_slots` | `etl.mx__series_contract.agent_dimensioned_activities` (`affiliation = 'nubank'`) | **only** to derive `required_activity_on_day_flag` |
| `shuffle_jobs` | `etl.mx__dataset.ops_canonical_time_spent_activities` (`actor_affiliation = 'nubank'`) | one row per shuffle job execution |
| `oos_jobs` | `etl.mx__dataset.taskmaster_consolidated_registry` | one row per out-of-shuffle job execution |

> **Team note.** Content jobs are **all OOS** — they live entirely in
> `taskmaster_consolidated_registry` (`oos_jobs`); content agents have effectively
> no shuffle jobs. Core/Fraud use both shuffle and OOS. (Social Media's jobs are
> not in either source — they're in Sprinklr — so social agents have no `jobs_raw`
> rows; their occupancy is handled separately in `occupancy_time` via `sm_jobs`.)

## Filters applied here (minimal — raw table)

- **Shuffle jobs**: ALL statuses kept (finished / transferred / skipped / …).
  No status filter — that is a metric decision.
- **OOS jobs**: synthetic `activity_type = 'oos'` and `status = 'finished'`
  (taskmaster exposes neither). Content-squad `job_classification` cleanup is
  applied so `job_id` matches legacy whenever a content source exists.
- **Roster**: `status = 'active'` (inner join on `(agent, snapshot_month)`).

## `required_activity_on_day_flag` (the one derived field)

`1` if the agent was **scheduled** (had required DIME hours) for that job's
`activity_type` on that day, else `0`. "Scheduled / required" uses the NTPJ
DIME definition:

- non-null `activity_type_required` not in (`lunch_break` / `shrinkage` /
  `time_off`),
- non-null `squad` not in (`wfm` / `credit_evolution` / `dote`),
- `shuffle_status_required IN ('available', 'oos')`.

Jobs done for an activity the agent was not scheduled for that day (e.g.
cross-support) get flag `0` — but the job row is still kept.

## Date attribution (night shifts)

Each job is attributed to a `date` by the day the agent's shift started. For
agents whose roster `shift` is `'night'`, work that crosses midnight is rolled
back onto the start day using a noon boundary (`DATE(local - 12h)`), effective
`2026-07-01` onward only. **Both** the jobs (keyed off their local `start_time`)
**and** the DIME required-activity set (keyed off the slot's local start) are
re-attributed with the same rule, so the `(agent, date, activity_type)`
required-flag join stays aligned. Non-night shifts and pre-July-2026 data keep
plain calendar-day attribution. See the shared rule in
[README → Date attribution (night shifts)](README.md#date-attribution-night-shifts).

## Deferred to the metrics layer (NOT applied here)

- Aggregation to `(agent, date, job_id)` with count / duration.
- The monthly expected-duration benchmark (`exp_duration_job`) and its
  4-month-window → current-month cutover.
- The NTPJ ratio.
- All per-agent / per-date manual adjustments, cross-support queue exclusions,
  and outage-date carve-outs.

## Output schema (one row per job)

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
| `start_time` | TIMESTAMP | job start (local time) |
| `end_time` | TIMESTAMP | job end (local time) |
| `job_type` | STRING | queue / classification |
| `activity_type` | STRING | `email` / `backoffice` / `chat` / … or `oos` |
| `status` | STRING | shuffle status (OOS synthesized as `finished`) |
| `job_id` | STRING | legacy job_id naming (used for benchmark joins) |
| `duration_seconds` | BIGINT | net time spent on the job |
| `required_activity_on_day_flag` | INT | 1 if scheduled for that activity that day, else 0 |
