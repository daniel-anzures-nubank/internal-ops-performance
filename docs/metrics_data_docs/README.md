# metrics_data — raw table documentation

The scripts in `metrics_data/` produce **raw tables**. They are *not* finished
metrics: each one keeps the most granular rows (per slot, per job, per
evaluation) and applies only the minimal filtering needed to scope the data to
active agents. All business exclusions (activity-type / dimensioned-activity /
squad carve-outs, manual adjustments, outage dates, benchmarks, ratios) are
deferred to a future `metrics/` layer that reads from these tables.

## Shared dimensions

Every raw table starts with the same seven dimension columns, in this order:

| column | source | notes |
|--------|--------|-------|
| `agent` | roster | login / username |
| `xforce` | roster | team lead |
| `xplead` | roster | lead of the lead |
| `team` | roster | performance team derived from squad (`core` / `fraud` / `social media` / `content`); see `docs/team_squad_mapping.md` |
| `squad` | roster | roster squad (not DIME squad) |
| `district` | roster | roster `squad_district`, renamed to `district` |
| `shift` | roster | roster shift |

The roster comes from the `agent_information` extractor
(`etl.mx__series_contract.cx_mx_bdx_snapshots` + `ops_actors`). Most tables
inner-join on `(agent, snapshot_month)` and keep only `status = 'active'`
agents, which is also what scopes each output to active agents. The one exception
is **`content_csat`**, which is attributed by squad: it joins on
`(target_squad, snapshot_month)`, fanning each survey response out to the content
agents serving that squad (the shared dimension columns are still present).

## Team coverage

These raw tables are **cross-team**: every row carries a `team` column
(`core` / `fraud` / `social media` / `content`) derived from `squad` in the
`agent_information` extractor per `docs/team_squad_mapping.md`. The only
squad-level exclusion is the non-team support squads **`quality` and `planning`**,
which the extractor drops; all four performance teams flow through automatically
wherever their source data uses the shared tables.

- **`adherent_time`** and **`quality_evaluations`** cover Social Media using the
  same source tables as Core (verified against live data — see each doc's
  "Team coverage" section).
- **`occupancy_time`** covers Social Media via a team-specific job source:
  social agents' occupancy comes from `sm_jobs`
  (`sprinklr_normalized_occupancy_data`), unioned in as `oos` jobs, while
  Core/Fraud/Content use shuffle/OOS jobs. See its doc.
- **`tnps_responses`** is **Social Media only** — tNPS (the Human tNPS metric)
  does not apply to other teams, and its source
  (`sprinklr_tnps_data`) only contains social agents' surveys. See its doc.
- **`wows`** is **Social Media only** — WoWs do not apply to other teams, and the
  WoWs Google Sheet only contains social agents. See its doc.
- **`content_csat`** is **Content only** — the Content satisfaction survey is
  attributed by `target_squad` (one response fanned out to the content agents
  serving that squad), not by individual agent. See its doc.

## Date attribution (night shifts)

The four **time/slot-based** raw tables — `adherent_time`, `occupancy_time`,
`shrinkage_slots`, and `jobs_raw` — re-attribute night-shift activity that
crosses midnight back to the day the **shift started**. Without this, a single
night shift is split across two calendar days (the evening head on day *N*, the
early-morning tail on day *N+1*).

The rule (implemented once in `metrics_data/shift_attribution.py`):

- Only agents whose roster `shift` is `'night'` that month are touched; morning
  / mid shifts keep plain calendar-day attribution.
- The boundary is **noon**: a row's business day is `DATE(local_ts - 12h)`. Night
  shifts run ~20:00 → ~07:00 with an empty midday gap, so the evening head and the
  following early morning both land on the start day, while the next night's head
  stays put.
- **Cutover `2026-07-01`**: only activity on/after this date is re-attributed, and
  a rolled-back date is never allowed to fall before the cutover. So every
  pre-July-2026 metric is byte-for-byte unchanged and the June 30 → July 1
  boundary shift keeps its legacy (split) attribution.

In `jobs_raw` both the jobs and the DIME required-activity set are re-attributed
with the same rule, so the NTPJ `(agent, date, activity_type)` required-flag join
stays aligned. The other three raw tables (`quality_evaluations`,
`tnps_responses`, `wows`, `content_csat`) are not time/slot-based and are not
affected.

## Datasets

| doc | module | build script | default target table |
|-----|--------|--------------|-----------------------|
| [adherent_time](adherent_time.md) | `metrics_data/adherent_time.py` | `scripts/metrics_data_scripts/build_adherent_time.py` | `usr.danielanzures.io_adherent_time_raw` |
| [occupancy_time](occupancy_time.md) | `metrics_data/occupancy_time.py` | `scripts/metrics_data_scripts/build_occupancy_time.py` | `usr.danielanzures.io_occupancy_time_raw` |
| [jobs_raw](jobs_raw.md) | `metrics_data/jobs_raw.py` | `scripts/metrics_data_scripts/build_jobs_raw.py` | `usr.danielanzures.io_jobs_raw` |
| [quality_evaluations](quality_evaluations.md) | `metrics_data/quality_evaluations.py` | `scripts/metrics_data_scripts/build_quality_evaluations.py` | `usr.danielanzures.io_quality_evaluations_raw` |
| [shrinkage_slots](shrinkage_slots.md) | `metrics_data/shrinkage_slots.py` | `scripts/metrics_data_scripts/build_shrinkage_slots.py` | `usr.danielanzures.io_shrinkage_slots_raw` |
| [tnps_responses](tnps_responses.md) | `metrics_data/tnps_responses.py` | `scripts/metrics_data_scripts/build_tnps_responses.py` | `usr.danielanzures.io_tnps_responses_raw` |
| [wows](wows.md) | `metrics_data/wows.py` | `scripts/metrics_data_scripts/build_wows.py` | `usr.danielanzures.io_wows_raw` |
| [content_csat](content_csat.md) | `metrics_data/content_csat.py` | `scripts/metrics_data_scripts/build_content_csat.py` | `usr.danielanzures.io_content_csat_raw` |
