# AGENTS.md — Internal Ops Performance

Persistent guidance for AI agents working in this repository. Read it at the start of every session and apply its conventions throughout your work.

## Project Overview

This workspace contains the **Internal Operations (IO) Performance** analysis for Nubank Mexico CX. It computes agent, XForce, and XPLead performance metrics (Adherence, NTPJ, Normalized Occupancy, Quality, Shrinkage, etc.).

The project produces two layers of output:

1. **Raw data tables** (built by `metrics_data/`) — the granular building blocks (per slot, per job, per evaluation) that the performance metrics are calculated from. See [Raw Data Tables](#raw-data-tables-metrics_data).
2. **Performance tables** — the finished metrics derived from the raw data.

Both layers are produced for the **Core, Fraud, Social Media, and Content** teams. The teams share most of the pipeline, but **some metrics differ per team** (e.g. Social Media and Content have their own quality sources and dedicated performance notebooks), so always confirm which team a given calculation targets.

Teams are groupings of roster **squads**. The raw tables carry a `squad` column but no `team` column — `docs/team_squad_mapping.md` is the **official mapping of team → squad** and must be used for any team rollup (e.g. `txn` belongs to **Fraud**, not Core).

**Manual adjustments** (per-agent / per-date carve-outs, training & shadowing windows, maternity leave, outage dates, cross-support queue exclusions, etc.) are sourced from this spreadsheet, which is the source of truth for adjustments: [https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU/edit?gid=720896495#gid=720896495](https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU/edit?gid=720896495#gid=720896495). These are intentionally **not** applied in the raw data layer — they are the job of the **Adjustments layer** (`adjustments/`, currently scaffolding), which sits on top of the metric layer. See [Adjustments Layer](#adjustments-layer-adjustments) and `docs/adjustments_docs/README.md`.

Two distinct codebases live side by side:

- `legacy/` — the old monolithic Databricks SQL pipeline. Source of truth for metric definitions until each one is migrated.
- The top-level Python project — a **local-first refactor in progress**, rebuilding the legacy pipeline as small, testable, pandas-based code. We develop and validate everything locally first, but it **also runs on Databricks** via a git-sourced orchestration job — see [Databricks Deployment](#databricks-deployment-job--git-folder--a-duality).

## Repository Layout

Flat layout. No `src/` wrapper, no package directories, no `__init__.py` files.

- `extractors/` — One `.sql` file per source. Parameterized with `:period_start` / `:period_end`. No filters, no calculations, no business logic — just raw pulls from Databricks tables.
- `metrics_data/` — Pure-pandas logic modules that build the **raw data tables** (`adherent_time`, `occupancy_time`, `jobs_raw`, `quality_evaluations`, `shrinkage_slots`, `tnps_responses`, `wows`, `content_csat`). One module per raw table; minimal filtering only (business exclusions are deferred to the metrics layer). See [Raw Data Tables](#raw-data-tables-metrics_data).
- `docs/metrics_data_docs/` — One `.md` per raw table documenting its source tables, filters applied vs. deferred, derivation logic, and output schema. Plus a `README.md` index.
- `metrics/` — Pure-pandas logic modules that build the **finished metric tables** from the `io_*_raw` tables (today: `adherence`, `ntpj`, `normalized_occupancy`, `quality`). One module per metric; shared bucketing/aggregation lives in `metrics/metric_utils.py`. This is where business exclusions, benchmarks, and ratios that the raw layer deferred get applied. See [Metric Tables](#metric-tables-metrics).
- `docs/metrics_docs/` — One `.md` per metric documenting its input raw table(s), filters applied vs. deferred, derivation logic, and output schema. Plus a `README.md` index.
- `adjustments/` — Pure-pandas logic modules for the **manual adjustments layer** — the per-agent / per-date carve-outs the raw and metric layers defer (cross-support exclusions, leave reclassifications, training/shadowing windows, outage dates, DIME-squad exclusions, per-agent Index carve-outs). **Currently scaffolding** (no adjustment implemented yet). Source of truth is the adjustments Google Sheet. See [Adjustments Layer](#adjustments-layer-adjustments).
- `docs/adjustments_docs/` — One `.md` per adjustment (sources, columns read, exactly which metric rows it changes). Plus a `README.md` index with the catalog and the source-of-truth sheet.
- `gsheets.py` — Local, pure-Python Google Sheets transport (`gspread`, no Databricks/Spark/`dbutils`). Reads/writes Sheets into/from pandas. Credentials come from env vars (`GOOGLE_SERVICE_ACCOUNT_JSON` / `GOOGLE_SERVICE_ACCOUNT_FILE` / `GOOGLE_APPLICATION_CREDENTIALS`), never hardcoded. Optional `sheets` dependency group (`uv sync --group sheets`). Usage how-to: [Google Sheets access](#google-sheets-access-gsheetspy).
- `scripts/` — Runnable entry points. Each script owns its own transport (currently `databricks-sql-connector`) and its own CLI. Organized into subfolders: `scripts/metrics_data_scripts/` holds one `build_*.py` per raw table (imports the matching `metrics_data/` module and writes the result to Delta); `scripts/metrics_scripts/` holds one `build_<metric>.py` per metric (reads the `io_*_raw` table(s) via `db.read_table`, imports the matching `metrics/` module, writes the `io_*_metric` table). The data-quality runner `check_extractor_data_quality.py` stays at the `scripts/` root.
- `tests/` — Pure-pandas `test_*.py` unit tests (no warehouse), organized to mirror the code: `tests/metrics_data/` (raw-table modules), `tests/metrics/` (metric modules), `tests/extractors/` (reserved for extractor checks). The DQ engine `checks.py` + `test_checks.py` and `tests/parity/` live at the `tests/` root. Imports resolve via `pythonpath = [".", "tests", "metrics_data", "metrics"]` in `pyproject.toml`.
- `docs/metrics_definitions.md` — Canonical definitions, formulas, datasets, filters, and worked examples for every IO metric. Source of truth when migrating legacy calculations to Python.
- `docs/team_squad_mapping.md` — **Official** mapping of performance team → roster squad (used to roll the `squad` column up to Core / Fraud / Social Media / Content).
- `legacy/` — The old monolithic Databricks SQL pipeline.
- `legacy/AGENTS_LEGACY.md` — The old `AGENTS.md` from that project. Documents pipeline architecture, team hierarchy, key metrics, source datasets, and SQL conventions. **Read it whenever you work on anything inside `legacy/`** or need historical context about how the metrics are computed.

## Source-of-Truth (SOT) Tables

Several upstream **source-of-truth** tables feed the roster, schedule, SLA, and
absence logic. They are not pipeline output, but you query them constantly when
building or *auditing* metrics (e.g. "why did this district miss SLA on this
day?"). Most live in `etl.mx__series_contract` (WFM / contract SOT) or
`usr.cross_x_mx` (planning SOT). Naming gotcha: the **district** appears in two
forms — the bare form `identity_fraud` (BDX, approved-absences) and the
squad-prefixed form `idsec_identity_fraud` (DIME `agent_dime_squad`, SLA SOT).
Confirm which a given table uses before joining.

### BDX roster snapshots — `etl.mx__series_contract.cx_mx_bdx_snapshots`

The **roster source of truth** for Core / Fraud / Social Media (Content's roster
is the Google Sheet instead — see `docs/team_squad_mapping.md`). One row per
`(agent, snapshot_date)`; the `agent_information` extractor keeps the **latest
snapshot per agent per month** (the canonical month-end roster). Key columns:

- `actor_email`, `xforce_email`, `xplead_email`, `xmanager_email` — identities
(extract the email prefix with `^[a-zA-Z]+\.[a-zA-Z]+`, lowercased).
- `squad`, `district` — org attribution. Fraud's `squad = 'idsec'` splits into
districts `identity_fraud`, `gamers`, `live_channels`, `csi`, `mule_accounts`,
`victim_prevention`, `idsec`, `fraud`. The district is **bare** here
(`identity_fraud`), not the `idsec_identity_fraud` form DIME/SLA use.
- `status` — `active` / inactive. Filter to `active` for headcount.
- `shift_name`, `shift_start_time`, `shift_end_time`, `shift_lunch_time`,
`shift_day_off_1`, `shift_day_off_2` — the agent's working pattern.
- `hire_start_date`, `termination_date`, `snapshot_date`.

**Latest snapshot.** A month never "closes" mid-month, so for in-flight months
use the latest available snapshot: `snapshot_date = (SELECT MAX(snapshot_date) FROM ...)` (optionally bounded `<= :period_end`).

**Availability ≠ active headcount — use `shift_day_off_1` + `shift_day_off_2`.**
Each agent has **two fixed days off** (lowercase day names, e.g. `saturday`,
`sunday`). An agent is **available** on a given weekday iff that day is **not**
one of their two days off. Most of the roster is off on weekends, so the
rostered-available crew on a Saturday/Sunday is far smaller than the active
roster — and "distinct agents who logged work" is *throughput/coverage*, not
availability. Example (idsec `identity_fraud`, latest snapshot `2026-06-20`):
40 active, but only **11 available Saturday** and **21 available Sunday** (18 are
off Sat+Sun).

```sql
SELECT
  COUNT(*) AS roster_active,
  SUM(CASE WHEN 'saturday' NOT IN (LOWER(shift_day_off_1), LOWER(shift_day_off_2)) THEN 1 ELSE 0 END) AS avail_saturday,
  SUM(CASE WHEN 'sunday'   NOT IN (LOWER(shift_day_off_1), LOWER(shift_day_off_2)) THEN 1 ELSE 0 END) AS avail_sunday
FROM etl.mx__series_contract.cx_mx_bdx_snapshots
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM etl.mx__series_contract.cx_mx_bdx_snapshots)
  AND squad = 'idsec' AND district = 'identity_fraud' AND status = 'active';
```

### DIME schedule: Google Drive sheets ↔ ETL table (a duality)

The DIME schedule has **two representations of the same data**, and they should agree slot-for-slot:

1. **The human-authored source of truth** — WFM builds each week's schedule by hand in Google Drive: [https://drive.google.com/drive/folders/1V7lFCKqEwdqwfoYNC-hdGMiAArmaqaqv](https://drive.google.com/drive/folders/1V7lFCKqEwdqwfoYNC-hdGMiAArmaqaqv).
2. **The materialized ETL table** — `etl.mx__series_contract.agent_dimensioned_activities`, which the `extractors/dime_slots.sql` extractor reads. This is what every downstream raw table is actually built from.

The Drive sheets flow into the ETL table, so when you need to *audit / explain* a DIME value (e.g. "why is this slot `Licencia`?") the Drive sheet is authoritative; when you need to *compute metrics at scale* you use the ETL table. If the two ever disagree, trust the Drive sheet and treat the ETL row as stale/mis-ingested. The ETL table also lags (it may be missing the current in-flight week) — when the week you need isn't there yet, read the Drive sheet.

**How to read the Drive DIME** (verified via the `google-workspace` MCP — `drive_search`, `sheets_getMetadata`, `sheets_getRange`):

- The root folder holds one subfolder **per week**, named `W1 … W27`.
- Inside each week, the consolidated schedule is the spreadsheet named `**S## 2026`** (there are also per-team `… Template W##` sheets; the `S## 2026` one is the final DIME).
- The `S## 2026` workbook has **one tab per day** — `LUN, MAR, MIER, JUE, VIE, SAB, DOM` (Mon→Sun) — plus support tabs (`BDX`, `Ranges`, `VAC&UPTO`, `Glosario`, etc.) that are not the schedule.
- Within a day tab: **row 10 is the header**; agents are in **column `G` (`Xpeers`)**; `A–F` = `SHIFT / Inicio / Pausa / Fin / Squad / STEP`; `I` = `Status` (`AcDay` = working, `DayOFF` = rest day); and the **30-minute slot columns run from `J` onward** (`0h0, 0h30, … 23h30`), each cell carrying that slot's `**dimensioned_activity`** (the same value as the ETL column).
- **Week-number gotcha:** `S##` is **not** the ISO week number. Always confirm the actual Monday from the tab's date cell (`A5` / `J4`) rather than assuming — e.g. `S18`'s Monday is `2026-04-27`, `S19`'s is `2026-05-04`, `S25`'s Saturday is `2026-06-20`.

**Leave lives in `dimensioned_activity`, not `activity_type_required`.** Leave/exception slots (`Licencia`, `Vacacion`, `Permiso Medico` / `Permiso medico`, `Mouring`, `Weekly`, `Huddle`) appear in `dimensioned_activity` while their `activity_type_required` is typically `dime_invalid_notation` (not `time_off`). So a slot can look "scheduled to work" by `activity_type_required` alone yet actually be registered leave — when deciding whether a slot is productive you must also exclude this `dimensioned_activity` list. This is a **fixed DIME filter** (not a manual adjustment), applied in the raw layer (`metrics_data/adherent_time.py` → `filter_dime`, constant `MEETING_LEAVE_DIMENSIONED_ACTIVITIES`).

**Cross-support is encoded in the `dimensioned_activity` code, not in `agent_dime_squad`.** `agent_dime_squad` always carries the agent's **home** squad (e.g. `engagement_engagement`) regardless of who they actually worked for that slot. Which squad/queue a productive slot serves is encoded in the `dimensioned_activity` token, which follows a `<CHANNEL>_<TARGET>[_TSKF]` shape:

- **CHANNEL** — the work channel: `BKO` (backoffice), `OOS`, `MAIL` (email), etc. (maps to `activity_type_required` = `backoffice` / `oos` / `email` / …). A leading **`x`** prefix on the channel (e.g. `xBKO_ENG`) marks **overtime hours** — the same work channel, worked outside the agent's regular dimensioned shift.
- **TARGET** — the product/squad the work belongs to: `ENG` (engagement), `CTA` (cuenta/account), `LCYC` (lifecycle), `TXN` (transactions), `CBF`, … An agent's **home** work uses **their own** TARGET token (engagement agents → `*_ENG`: `BKO_ENG`, `OOS_ENG`, `xBKO_ENG`).
- **`_TSKF`** suffix — a **task force**: a cross-support pool that agents from several home squads are pulled into (e.g. `BKO_CTA_TSKF`, `BKO_TXN_TSKF`, `BKO_LCYC_TSKF`, `OOS_CBF_TSKF`, `OOS_LCYC_TSKF` each appear under 4–5 different `agent_dime_squad`s). A `_TSKF` slot is cross-support by construction.

So a productive slot is **cross-support for that agent** when its `dimensioned_activity` TARGET ≠ the agent's home squad token, **or** it carries `_TSKF`. For the engagement squad specifically, cross-support = every productive slot whose code is **not** `*_ENG` — in practice the cuenta/account (`BKO_CTA`, `BKO_CTA_TSKF`, `MAIL_CTA`, `OOS_CTA`) and lifecycle (`BKO_LCYC_TSKF`, `OOS_LCYC_TSKF`) task-force codes. (Worked example: May 2026 engagement had 411 cross-support slots = 205.5 dimensioned hours across 6 agents, 172.5 hours actually worked → 84% occupancy.) Cross-support **queue exclusions** themselves are an Adjustments-layer concern (sheet tab `Cross Support`); this note is just how to **identify** the slots from the DIME code.

### SLA targets — `usr.cross_x_mx.planning__sla_sot`

The **per-queue SLA source of truth** (snapshotted). Carries `sla_seconds` per
`(queue, activity_type)` plus `squad` / `district` attribution, with a
`snapshot_ts_mx`. Because it is snapshotted over time, dedupe to the latest
snapshot per `(queue, activity_type, day)` and pick the snapshot effective for
the week you're measuring (see the `week_queue_latest` pattern). Districts here
use the squad-prefixed form (`idsec_identity_fraud`). Used to judge whether a
ticket's first-response latency met its target.

### Approved absences — `usr.cross_x_mx.planning__xpeer_approved_absence_events`

WFM-**approved leave events** (distinct from DIME `dimensioned_activity` leave —
this is the approval record, not the scheduled slot). One row per
`(work_email, absence_date, …)`; key columns `absence_type`
(`XPEER VACATION`, `MEDICAL LEAVE - SHORT TERM`, `PERSONAL MATTERS`,
`MATERNITY LEAVE`, `PATERNITY LEAVE`, …), `absence_reason`, `district`, `step`,
`shift_start_date` / `shift_start_time`. District uses the **bare** form
(`identity_fraud`). Use it to explain coverage gaps — e.g. how many of the
rostered-available weekend crew (from BDX `shift_day_off`) were on approved
vacation that day.

## Raw Data Tables (`metrics_data/`)

The `metrics_data/` modules produce the granular raw tables that feed the performance-metric calculations. They are **not** finished metrics: each keeps the most granular rows and applies only the minimal filtering needed to scope to active agents. All business exclusions, manual adjustments, benchmarks, and ratios are deferred to the metrics layer.

Each table is written by `scripts/metrics_data_scripts/build_<name>.py` (default target `usr.danielanzures.io_<name>_raw`, overridable with `--target`) and documented in `docs/metrics_data_docs/<name>.md`.

**Table naming convention.** Raw `metrics_data` tables are suffixed `_raw` (e.g. `io_adherent_time_raw`); the future `metrics/` layer tables are suffixed `_metric` (e.g. `io_adherence_metric`). Both live in the `usr.danielanzures` schema. (`io_jobs_raw` keeps its single `_raw` — the dataset is literally named `jobs_raw` — rather than becoming `io_jobs_raw_raw`.) Every built table also has an append-only history twin suffixed `_snapshots` (e.g. `io_adherence_metric_snapshots`) and is recorded in the central `usr.danielanzures.pipeline_runs` registry — see [Run Snapshots & Registry](#run-snapshots--registry).

All tables share the same leading dimensions, in this order: `agent, xforce, xplead, team, squad, district, shift, date`. The roster dimensions come from the `agent_information` extractor; `district` is the roster `squad_district` renamed; `team` is derived from `squad` per `docs/team_squad_mapping.md` (the `quality` and `planning` support squads are kept to match legacy `adherence_io` but carry `team = NULL`). Core / Fraud / Social Media come from the BDX snapshots; **Content comes from a Google Sheet** (currently `gsheets.sheets.mx_content_bdx_daniel_anz_temp`; canonical `gsheets.sheets.mx_content_bdx`) unioned into the same extractor — content rows carry `squad = 'enablement'`, `district = 'content'`, and `team = 'content'` (forced). See `docs/team_squad_mapping.md`.


| table                        | grain                                                               | columns                                                                                                                                                                                          |
| ---------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `io_adherent_time_raw`       | one row per agent per DIME slot                                     | `agent, xforce, xplead, team, squad, district, shift, date, slot_time, activity_type_required, required_minutes, adherent_minutes`                                                               |
| `io_occupancy_time_raw`      | one row per agent per DIME slot                                     | `agent, xforce, xplead, team, squad, district, shift, date, slot_time, activity_type_required, required_minutes, occupancy_minutes`                                                              |
| `io_jobs_raw`                | one row per job (shuffle + OOS)                                     | `agent, xforce, xplead, team, squad, district, shift, date, start_time, end_time, job_type, activity_type, status, job_id, duration_seconds, required_activity_on_day_flag`                      |
| `io_quality_evaluations_raw` | one row per QA evaluation (Playvox + Sprinklr SM)                   | `agent, xforce, xplead, team, squad, district, shift, date, evaluation_id, team_name, source, qa_score`                                                                                          |
| `io_shrinkage_slots_raw`     | one row per DIME slot                                               | `agent, xforce, xplead, team, squad, district, shift, date, slot_time, activity_type_required, dimensioned_activity, shrinkage_flag, controllable_shrinkage_flag, uncontrollable_shrinkage_flag` |
| `io_tnps_responses_raw`      | one row per tNPS survey response (**Social Media only**)            | `agent, xforce, xplead, team, squad, district, shift, date, case_number, survey_response_date, survey_score`                                                                                     |
| `io_wows_raw`                | one row per WoW experience (**Social Media only**)                  | `agent, xforce, xplead, team, squad, district, shift, date, case_id`                                                                                                                             |
| `io_content_csat_raw`        | one row per CSAT survey response × content agent (**Content only**) | `agent, xforce, xplead, team, squad, district, shift, date, target_squad, requested_by, survey_timestamp, promoters, number_of_questions, csat_score`                                            |


`io_content_csat` is the one table not keyed by `agent`: CSAT is attributed by `target_squad` (the squad a survey rates), so each response is fanned out to every active content agent serving that squad — the join is on `(target_squad, snapshot_month)` rather than `(agent, snapshot_month)`. The content roster's `target_squad` is surfaced by the `agent_information` extractor (NULL for all non-content rows).

**Night-shift date attribution.** The four time/slot-based raw tables (`io_adherent_time_raw`, `io_occupancy_time_raw`, `io_shrinkage_slots_raw`, `io_jobs_raw`) re-attribute night-shift activity that crosses midnight back to the day the **shift started**, so one night shift isn't split across two calendar days. The rule lives in one place — `metrics_data/shift_attribution.py` — and applies only to agents whose roster `shift = 'night'`, using a noon boundary (`DATE(local_ts - 12h)`), effective from the `2026-07-01` cutover onward (pre-July-2026 metrics are byte-for-byte unchanged). In `jobs_raw` both the jobs and the DIME required-set are re-attributed identically so the NTPJ required-flag join stays aligned. See [README → Date attribution (night shifts)](docs/metrics_data_docs/README.md#date-attribution-night-shifts).

### DIME source

The DIME schedule has a Google-Drive-sheet ↔ ETL-table duality and its own
read conventions (including that leave lives in `dimensioned_activity`, not
`activity_type_required`). That documentation now lives in
[Source-of-Truth (SOT) Tables → DIME schedule](#dime-schedule-google-drive-sheets--etl-table-a-duality).

## Metric Tables (`metrics/`)

The `metrics/` modules turn the `io_*_raw` tables into **finished metrics**. This is where the business exclusions, benchmarks, ratios, and (eventually) manual adjustments the raw layer deferred get applied. Each module reads one or more `io_*_raw` tables and emits a tidy long-format metric at **day / week / month / quarter / semester / year** granularity.

Each metric is written by `scripts/metrics_scripts/build_<metric>.py` (default target `usr.danielanzures.io_<metric>_metric`, overridable with `--target`; raw input overridable with `--source`) and documented in `docs/metrics_docs/<metric>.md`.

All metric tables share the same tidy "long" shape, in this order: `agent, xforce, xplead, team, squad, district, shift, date_reference, date_granularity, metric, numerator, denominator, metric_value`. `date_granularity` is one of `day` / `week` / `month` / `quarter` / `semester` / `year` (`week` = Monday-start matching Spark `DATE_TRUNC('WEEK')`; `quarter`/`year` = first of the calendar quarter/year; `semester` = Jan 1 or Jul 1); hierarchy/dimension fields take their most-recent value within each period bucket (legacy `FIRST_VALUE(... ORDER BY date DESC)`); `metric_value` is a **percentage** (`numerator / denominator * 100`, NULL when the denominator is 0). Exception: metrics whose numerator is already on a 0-100 scale use `scale = 1` (e.g. `quality`, where `numerator = SUM(qa_score)`, `denominator = # evaluations`, so `metric_value` is the mean score directly). `tnps` keeps `scale = 100` but its `numerator` (promoters − detractors) can be **negative**, so `metric_value` is an NPS percentage in `[-100, 100]`. `wows` is a **count** metric (not a ratio): `metric_value` is the WoW count (`= numerator`) and `denominator` just carries the monthly target (`5`); it also doesn't use `aggregate_long`. Note two metric modules use a `_metric` suffix — `metrics/wows_metric.py` and `metrics/content_csat_metric.py` — to avoid colliding with the same-named raw modules (`metrics_data/wows.py`, `metrics_data/content_csat.py`); the metrics themselves are still `wows` and `content_csat`.


| table                            | metric                                                                                                                                                                    | input                                                                                                                                    | grain                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `io_adherence_metric`            | `adherence` = connected / dimensioned time (≥ 95%), all teams                                                                                                             | `io_adherent_time_raw`                                                                                                                   | one row per agent per day/week/month/quarter/semester/year                       |
| `io_ntpj_metric`                 | `ntpj` = actual / expected job time (≤ 100%), Core / Fraud / Content                                                                                                      | `io_jobs_raw`                                                                                                                            | one row per agent per day/week/month/quarter/semester/year                       |
| `io_normalized_occupancy_metric` | `normalized_occupancy` = occupancy / district+shift benchmark (≥ 100%), all teams                                                                                         | `io_occupancy_time_raw`                                                                                                                  | one row per agent per day/week/month/quarter/semester/year                       |
| `io_quality_metric`              | `quality` = mean QA score (≥ 95%), Core / Fraud / Social Media (Content uses CSAT)                                                                                        | `io_quality_evaluations_raw`                                                                                                             | one row per agent per day/week/month/quarter/semester/year                       |
| `io_shrinkage_metric`            | `shrinkage` (agent) = non-productive / required slots (≤ 20%), all teams; plus slot-weighted roll-ups `shrinkage_xforce` (per XForce) and `shrinkage_xplead` (per XPLead) | `io_shrinkage_slots_raw`                                                                                                                 | one row per agent + XForce/XPLead roll-ups, day/week/month/quarter/semester/year |
| `io_tnps_metric`                 | `tnps` = (promoters − detractors) / valid responses (NPS %, ≥ 88%), **Social Media only**                                                                                 | `io_tnps_responses_raw`                                                                                                                  | one row per agent per day/week/month/quarter/semester/year                       |
| `io_wows_metric`                 | `wows` = `COUNT(DISTINCT case_id)` (**count**, monthly target ≥ 5), **Social Media only**                                                                                 | `io_wows_raw`                                                                                                                            | one row per agent per day/week/month/quarter/semester/year                       |
| `io_content_csat_metric`         | `content_csat` = SUM(promoters) / SUM(questions) (≥ 95%), **Content only** (Content's Quality component)                                                                  | `io_content_csat_raw`                                                                                                                    | one row per agent per day/week/month/quarter/semester/year                       |
| `io_improved_benchmarks_metric`  | `improved_benchmark_squad` / `improved_benchmark_district` / `improved_benchmark_xforce` = improved / comparable monthly benchmarks (≥ 60%), Core / Fraud only            | `io_jobs_raw` + `io_occupancy_time_raw`                                                                                                  | **squad / district / xforce level**, **month only** (`agent`/`shift` NULL)       |
| `io_xpeer_index_metric`          | `xpeer_index` = mean of an agent's other metrics (≥ 95%), all teams                                                                                                       | the seven `io_*_metric` tables above (**composite**, not a raw table)                                                                    | one row per agent per day/week/month/quarter/semester/year                       |
| `io_nuvinhos_performance_metric` | `nuvinhos_performance` / `_squad` / `_district` = avg Index(Nuvinhos) / avg Index(Old), all teams                                                                         | `io_xpeer_index_metric` + `agent_information`                                                                                            | **XForce / squad / district roll-ups** (no agent grain), all six granularities   |
| `io_xpeers_in_target_metric`     | `xpeers_in_target` (per XForce) + `xpeers_in_target_xplead` (per XPLead) = targets achieved / total targets (≥ 70%), Core / Fraud / Social Media                          | the agent-level `io_*_metric` tables (**composite**, not a raw table)                                                                    | **XForce + XPLead roll-ups** (no agent grain), all six granularities             |
| `io_average_xpeer_index_metric`  | `average_xpeer_index` = mean agent Xpeer Index per XForce, all teams                                                                                                      | `io_xpeer_index_metric`                                                                                                                  | **XForce roll-up** (no agent grain), all six granularities                       |
| `io_xforce_index_metric`         | `xforce_index` = mean of up to 4 normalized components (shrinkage, xpeers_in_target, average_xpeer_index, improved_benchmark), all teams                                  | `io_shrinkage_metric` + `io_xpeers_in_target_metric` + `io_average_xpeer_index_metric` + `io_improved_benchmarks_metric` (**composite**) | **XForce roll-up** (no agent grain), all six granularities                       |
| `io_average_xforce_index_metric` | `average_xforce_index` = mean XForce `xforce_index` per XPLead, all teams                                                                                                 | `io_xforce_index_metric`                                                                                                                 | **XPLead roll-up** (no agent/xforce grain), all six granularities                |


The shared bucketing + tidy aggregation lives in `metrics/metric_utils.py` (`aggregate_long`); each metric module only filters its raw rows and names the numerator/denominator columns.

**Improved Benchmarks is the exception** to the agent-grain / six-granularity shape above. It compares each month's NTPJ (`exp_duration_job` per job type; lower = improved) and Normalized Occupancy (`district + shift` benchmark; higher = improved) benchmarks to the previous month, ties counting as improved, and rolls the improved/comparable counts up to squad, district, **and XForce** level (`improved_benchmark_xforce`, month grain only). It is **Core/Fraud only** (never Social Media / Content) and is **suppressed after each team's cutover** — Core from `2026-04`, Fraud from `2026-05` (it was dropped as an XForce-Index component then). The build script reads a benchmark look-back (6 months of `io_jobs_raw`, 2 of `io_occupancy_time_raw`).

**XForce Index is the composite headline score** (legacy `index_xforce`, renamed `xforce_index`), **XForce-level** (no agent grain), all four teams: the mean of up to four 0–100 normalized components — shrinkage (`≤20→100`, else `120−shrinkage`), `xpeers_in_target`, `average_xpeer_index`, and `improved_benchmark` (`≥60→100`, else `improved/0.6`). `shrinkage_xforce` is slot-weighted (sum the agent `io_shrinkage_metric` numerator/denominator per XForce, not an average of percentages). The improved_benchmark component is added **only where an `improved_benchmark_xforce` row exists** — i.e. Core/Fraud, month grain, before each team's cutover — so **SM/Content and all non-month grains stay 3-component**. `numerator` = Σ components, `denominator` = `100 × N` (300 or 400), `metric_value` = the mean. (This intentionally drops legacy's SM Mar+/Content Apr+ improved_benchmark addition.)

**Average Xpeer Index is XForce-level** (legacy `average_index_agent`), with **no agent grain**: the simple mean of the agent-level Xpeer Index (`io_xpeer_index_metric`) per `(team, xforce, xplead)`, for **all four teams**. Agents with a NULL index are excluded (like SQL `AVG`). We fill `numerator = Σ index` and `denominator = agent count` (legacy left them NULL and used `AVG()`); `metric_value` is the identical mean.

**Average XForce Index is XPLead-level** (legacy `average_index_xforce`), with **no agent/xforce grain**: the simple mean of the XForce-level `xforce_index` (`io_xforce_index_metric`) per `(team, xplead)`, for **all four teams**. Same NULL handling and `numerator = Σ index` / `denominator = XForce count` convention as Average Xpeer Index.

**Xpeers In Target** (legacy `xpeers_in_target_xforce` / `xpeers_in_target_xplead`), with **no agent grain**: it reads the agent-level `io_*_metric` tables, flags each Xpeer in/out of target per component (adherence ≥95, ntpj ≤100, NO ≥100, quality ≥95, tnps ≥88, wows ≥5 count), and reports `Σ targets achieved / Σ targets * 100`. It lands at **two grains in the same table**: `xpeers_in_target` (per XForce) and `xpeers_in_target_xplead` (per XPLead, `xforce` NULL — the same in-target/total counts aggregated up to the XPLead). An agent counts in a component's denominator when it has a row there and in the numerator when it clears the threshold (NULL `metric_value` fails). Components are era-gated like the Index (quality from Feb 2026, NO from March); Core/Fraud use adherence+ntpj as the always-on pair, Social Media use adherence+tnps+wows. **Content is excluded** (legacy never built it). Adherence is the driver (defines the XForce universe + `xplead`).

**Shrinkage roll-ups.** The agent-level `shrinkage` metric is the source for two slot-weighted roll-ups written into the same `io_shrinkage_metric` table: `shrinkage_xforce` (per `team, xforce, xplead`) and `shrinkage_xplead` (per `team, xplead`, `xforce` NULL). Both sum the agent numerator/denominator and divide (not a flat average of percentages) — identical to the shrinkage component inside `xforce_index`, which still rolls up from the agent rows directly (it filters `metric = 'shrinkage'`).

**Nuvinhos Performance is index-level** (legacy `nuvinhos_performance*`), with **no agent grain**: it reads `io_xpeer_index_metric` plus `agent_information` tenure and compares the average Xpeer Index of *Nuvinhos* (agents whose bucket month is in `[month(last_change_date), +2 months]`) against *old* agents — `avg Index(Nuvinhos) / avg Index(Old) * 100`. It emits three roll-ups in one table (`nuvinhos_performance` per XForce, `_squad` per squad, `_district` per district; legacy only built the XForce roll-up for Core/Fraud — we extend squad/district to all teams). It uses the **documented flat per-agent average** (`numerator` = mean Index of Nuvinhos, `denominator` = mean Index of old), deliberately diverging from legacy's two-level cohort average, which biased the ratio by the number of Nuvinho vs old cohorts (a single Nuvinho squad against many old squads diluted the result toward 0). Agents with NULL `last_change_date` (e.g. the temp Content roster) count as *old*, so Content currently yields a degenerate (no-Nuvinho) result.

**Xpeer Index is a composite** (legacy `index_agent`): it reads the other `io_*_metric` tables (not an `io_*_raw` table) and folds them into a single mean per `(agent, date_reference, date_granularity)`. Adherence is the **driver** (an agent is in the Index iff it has an Adherence row; `team`/dims come from there). Components are folded first — NTPJ around 100 (`≤100→100`, `100–200→200−x`, else 0), NO truncated at 100, WoWs `≥5→100` else `x/5*100`; tNPS / Quality / CSAT enter raw. The roster is **team- and era-dependent**: Core/Fraud = Adherence + NTPJ (+Quality from Feb, +NO from March); Content = Adherence + NTPJ (+NO and +CSAT from March); Social Media = Adherence + WoWs (+tNPS when present, +Quality from Feb, +NO from March). Quality/CSAT/tNPS drop from both sum and divisor when missing. Multi-month buckets anchor the era on the period's **end** month; pre-2026 buckets are dropped. Per-agent legacy carve-outs (e.g. `nitza.zarza`) are deferred to the Adjustments layer.

Adherence drops non-productive slots (`activity_type_required` in `lunch_break` / `time_off` / `shrinkage`) before the ratio. The legacy `dimensioned_activity` **meeting/leave exclusion** and the **DIME-squad exclusion** (`agent_dime_squad` not in `wfm` / `credit_evolution` / `dote`, NULL dropped) are both applied as **fixed DIME filters in the raw layer** (`metrics_data/adherent_time.py` → `filter_dime`); only per-agent/outage adjustments remain deferred to the Adjustments layer.

NTPJ keeps finished jobs only; the cohort-wide monthly benchmark `exp_duration_job` (`SUM(duration)/SUM(count)` per `job_id`) uses a trailing `[M-4 … M]` window for months ≤ 2026-03 and the current month for months ≥ 2026-04, so the build script reads a 4-month look-back before `period_start`. Each agent's contribution is restricted to `required_activity_on_day_flag == 1` (the benchmark is not). Social Media has no NTPJ (no shuffle/OOS jobs). Cross-support queue exclusions, outage dates, per-agent carve-outs, and the Content "always 4-month window" rule are deferred to the future Adjustments layer.

Normalized Occupancy drops the same non-productive slots as adherence, then computes per-agent `occupancy = SUM(occupancy_minutes)/SUM(required_minutes)` and a monthly **district+shift benchmark** (legacy two-step: per-`(month, district, shift, squad)` occupancy ratio, then the mean of those squad ratios per `(month, district, shift)`). `numerator` carries the agent occupancy %, `denominator` the benchmark %, so `metric_value` = NO %. All four teams; DIME-squad exclusions and per-agent/outage carve-outs are deferred.

Quality unions two feeds — **Playvox** (`qmo_playvox_consolidated`, all teams) and **Sprinklr SM** (`social_media_case_summary_information`, Social Media, `>= 2026-05-01`) — tagged by a `source` column ('playvox' / 'sprinklr_sm'). The new roster keeps social agents, so the Sprinklr SM rows actually score SM Quality (in legacy the Core/Fraud `UNION ALL` from Sprinklr was dead code, dropped by the active-roster `social` exclusion). Both are on the same 0-100 scale. The metric keeps the latest record per `evaluation_id` (legacy `ROW_NUMBER() ... ORDER BY created_at DESC`, done at day grain since the raw only carries `date`), and averages `qa_score` per agent (`numerator = SUM(qa_score)`, `denominator = # evaluations`, `scale = 1`). Covers Core / Fraud / Social Media; **Content is excluded** (its quality is the separate `content_csat`). Scorecard/evaluation blacklists and outage dates are deferred.

Shrinkage takes `numerator = SUM(shrinkage_flag)` (the raw layer's pre/post-2026-03-01 slot rule) over `denominator = SUM(required_slot)`. The required-slot rule (applied here): drop `lunch_break` always, then a slot is required unless its `activity_type_required` is `dime_invalid_notation` (pre-cutover `< 2026-03-01`) or `time_off` (post-cutover `>= 2026-03-01`). All teams. Per-agent maternity/vacation reclassifications, training/shadowing exclusions, outage dates, and DIME-squad business exclusions are deferred to the Adjustments layer; the raw also carries `controllable`/`uncontrollable` flags for a future breakdown.

## Google Sheets access (`gsheets.py`)

`gsheets.py` is the local, pure-Python transport for Google Sheets (the
counterpart to `db.py` for the warehouse). It wraps `gspread` + `google-auth`,
returns/accepts plain `pandas.DataFrame`s, and runs on a laptop with **no**
Databricks/Spark/`dbutils`. It's how the repo reads the **adjustments sheet** and
the **Content roster** sheet.

> Reading the DIME schedule from Google **Drive** is a different path — that uses
> the `google-workspace` MCP (`drive_search`, `sheets_getMetadata`,
> `sheets_getRange`), not `gsheets.py`. See [SOT → DIME schedule](#dime-schedule-google-drive-sheets--etl-table-a-duality).

**1. Auth — a Google service account (never a personal login).** Credentials are
resolved from env vars in this order (put one in a gitignored repo-root `.env`;
never hardcode the key):

1. `GOOGLE_SERVICE_ACCOUNT_JSON` — the service-account JSON, inline.
2. `GOOGLE_SERVICE_ACCOUNT_FILE` — path to the service-account JSON file.
3. `GOOGLE_APPLICATION_CREDENTIALS` — the Google-standard path env var.

**2. Share the sheet with the service account.** Open the JSON, copy its
`client_email` (`…@….iam.gserviceaccount.com`), and share the target sheet with
that address (Viewer to read, Editor to write). A sheet that isn't shared returns
`403 PERMISSION_DENIED` — that's the current state of the adjustments sheet.

**3. Dependencies.** Either `uv sync --group sheets`, or run ad hoc with
`uv run --with gspread --with google-auth …` (add
`--default-index https://pypi.org/simple` if `uv` is pinned to a private index).

**Quick CLI check** (no code needed):

```bash
uv run --with gspread --with google-auth python gsheets.py list <sheet_id_or_url>
uv run --with gspread --with google-auth python gsheets.py read <sheet_id_or_url> "Tab Name"
```

**Programmatic use** — accepts a bare sheet id or a full Sheets URL:

```python
from gsheets import read_worksheet, write_dataframe, list_worksheets

tabs = list_worksheets(SHEET_URL)                  # tab titles
df = read_worksheet(SHEET_URL, "WoWs Base")        # tab -> DataFrame (first row = header)
write_dataframe(SHEET_URL, df, "Output")           # overwrites the tab (clears, then fills)
```

Notes: reads treat the first row as the header and return **all-string** columns
(Sheets is untyped — cast as needed). `write_dataframe` **clears then overwrites**
the whole tab (NaN → empty cell, everything coerced to string), creating the tab
if missing. Pass `client=open_client()` to reuse one authorized client across
many calls. Default scopes cover read+write Sheets + Drive; narrow to
`…/auth/spreadsheets.readonly` if you only read.

## Adjustments Layer (`adjustments/`)

The third layer (on top of `metrics_data/` → `metrics/`). It applies the **manual adjustments** — per-agent / per-date carve-outs that depend on a human decision and are therefore kept out of the deterministic raw/metric layers: cross-support exclusions, agent leave (maternity/vacation) reclassifications, training & shadowing windows, outage/incident dates, DIME-squad business exclusions, and per-agent Xpeer Index carve-outs. They are applied in the **metric layer** (which raw rows count / how they're reclassified), never in `metrics_data/`.

The **source of truth** is the adjustments Google Sheet (one tab per adjustment type): [https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU/edit?gid=720896495#gid=720896495](https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU/edit?gid=720896495#gid=720896495). Read it locally with `gsheets.py` once the sheet is shared with the service account (currently returns `403`).

**Status: scaffolding** — the folders and `docs/adjustments_docs/README.md` (catalog + source of truth) exist, but no adjustment is implemented yet. When implementing, mirror the other layers: module in `adjustments/`, build script in `scripts/adjustments_scripts/`, tests in `tests/adjustments/`, and one `docs/adjustments_docs/<name>.md` per adjustment. See `docs/adjustments_docs/README.md` for the planned catalog.

## Run Snapshots & Registry

Every build script writes its table through `db.publish()` (not `write_dataframe` directly), which persists three things per run:

1. **The current table** — replaced in place via `CREATE OR REPLACE TABLE` (this preserves the Delta version history instead of dropping it).
2. **An append-only history twin `{table}_snapshots`** — the same rows tagged with `run_id` (STRING) and `run_ts` (TIMESTAMP, UTC), partitioned by `run_id`. Writing is idempotent per run: re-running the same `run_id` first deletes that run's partition, so a retry never double-counts.
3. **A row in `usr.danielanzures.pipeline_runs`** — one row per `(run_id, table)` write: `run_id, run_ts, layer, table_name, snapshot_table, period_start, period_end, row_count, status, git_sha, notes`. The registry row is written **last** (status `success`), so a crash mid-run leaves no `success` row and the incomplete run is detectable.

**run_id.** Resolved by `db.resolve_run_id()` as: explicit `--run-id` arg > `PIPELINE_RUN_ID` env var > a generated sortable UTC id (`YYYYMMDDTHHMMSSZ-<hex>`). Export `PIPELINE_RUN_ID` once before running a batch of `build_*.py` so every table in that pipeline invocation shares one id; omit it and each script mints its own. Every build script also accepts `--no-snapshot` to skip the history twin (e.g. ad-hoc dev runs).

**Querying.** Latest published run of a table = `pipeline_runs` row with the max `run_id` where `status = 'success'`; pull that run's data with `SELECT * FROM {table}_snapshots WHERE run_id = :run_id`. Diff two runs by joining their snapshots on the table's business key.

This is a transport concern: it lives entirely in `db.py`. The pure `metrics_data/` and `metrics/` modules are unaffected — they still just take a DataFrame and return a DataFrame.

## Databricks Deployment (job ↔ git folder — a duality)

The repo is wired into Databricks (workspace `nubank-e2-general`, profile `nubank-e2-general`) **two independent ways**. They do **not** share state — keep them distinct, and don't confuse "the git folder is stale" with "the job is stale."

1. **The orchestration job — `[IO] Performance Metrics Pipeline`** (job id `267598911414455`). Its tasks are `spark_python_task`s with `source: GIT`, driven by a job-level `git_source` pointing at `https://github.com/daniel-anzures-nubank/internal-ops-performance`, branch `main`. **Databricks checks out `main` fresh from GitHub on every run** — the job never reads the workspace git folder below. So it always runs whatever is on `main` at run time: **to change what the pipeline does, push to `main`** (or repoint `git_source` to another branch/tag). Each task runs a `build_*.py` straight from the checkout, parameterized `--period-start {{job.parameters.period_start}} --period-end {{job.parameters.period_end}} --run-id {{job.run_id}}` (the `period_start` / `period_end` job params currently default to a single test week). It runs on the `[IO]-Performance-Metrics-Cluster` job cluster (Spark `15.4.x` LTS). **The job currently runs only the Adherence slice** — `build_adherent_time` (raw) → `build_adherence` (metric, depends on the first); wiring up the remaining metrics with DQ checks and failure guards is in progress.

2. **The Databricks git folder** — `/Workspace/Users/daniel.anzures@nubank.com.mx/internal-ops-performance` (repo id `2657286665982577`), same GitHub repo + `main`. This is a **separate, interactive checkout** for browsing/editing the code in the Databricks UI. **It does not auto-update** and the job never reads it, so it can drift behind `main`; pull it by hand when you want the UI to reflect the latest: `databricks repos update 2657286665982577 --branch main -p nubank-e2-general`.

**Net:** GitHub `main` is the single source of truth for both. The **job** follows `main` automatically; the **git folder** is a convenience checkout you refresh manually. Because the tasks run as `spark_python_task`s, the build scripts execute against the cluster's **ambient SparkSession** — `db.py`'s transport resolves that session (`get_spark()` / `getOrCreate`) when run on Databricks, while the pure `metrics_data/` and `metrics/` logic modules stay DataFrame-in/DataFrame-out.

**Build scripts must not raise `SystemExit` on success.** Because each task runs as a git-sourced `spark_python_task` under an IPython `exec()` runner, **any `SystemExit` — even code `0` — is treated as an uncaught exception and fails the task** (which then skips every downstream task, *even though the table writes already completed*). So a `build_*.py`'s entrypoint must only exit non-zero, never `sys.exit(main())` directly:

```python
if __name__ == "__main__":
    rc = main()
    if rc:            # success (rc == 0) falls through cleanly; only real errors exit non-zero
        sys.exit(rc)
```

This bit the first real job run: `build_adherent_time` wrote its ~46k rows and *then* died on `sys.exit(0)`, marking the task failed and skipping `build_adherence` — a green pipeline that reported red.

## Working Conventions

These are load-bearing — they prevent the project from sliding back into complexity.

- **Local-first, pandas only.** DataFrames are `pandas.DataFrame`, never `pyspark.sql.DataFrame`. No `SparkSession`, no `dbutils`. Spark and Databricks-native concerns enter the codebase only when (and if) the project is later refactored to run inside Databricks Workflows.
- **Flat layout.** No `src/`, no nested packages, no `__init__.py` unless something genuinely needs to be a Python package. Earn each new folder by feeling real pain — don't scaffold empty directories.
- **Transport in `scripts/`, logic in `tests/`.** Anything that opens a network connection, reads env vars, parses CLI args, or touches the filesystem lives in `scripts/`. Anything that takes a DataFrame and returns a DataFrame lives in `tests/`. This split is what makes `pytest` fast and the codebase portable.
- **SQL extractors stay dumb.** No filters, no joins beyond identity resolution, no derived business columns. All business logic belongs in the Python layer.
- **Rule of three.** Don't extract a shared abstraction (base class, shared module, utility helper) until you have three concrete examples driving it. Two duplicate code paths is not a pattern yet.
- **Validate assumptions with real data.** Before relying on a column shape, a uniqueness assumption, or a join grain, run a small probe query against Databricks via the MCP. The cost of guessing is metric drift; the cost of probing is 30 seconds.

## Git Hygiene

This project is tracked with git. Follow these practices:

- **Never modify git config** or run destructive commands (`push --force`, `reset --hard`, etc.) unless the user explicitly asks.
- **Do not commit unless explicitly asked.** Wait for the user to request a commit before staging or committing changes.
- **Write meaningful commit messages.** Focus on the *why* of the change, not just the *what*. Keep the subject line short (≤ 72 chars) and add a body when context is helpful.
- **Avoid committing secrets.** Never stage files like `.env`, credentials, tokens, or anything that looks sensitive. Warn the user if they ask you to.
- **Keep commits focused.** One logical change per commit. If you find yourself making unrelated edits, split them.
- **Check before committing.** Always run `git status` and `git diff` before staging, and confirm only the intended files are included.
- **Branch sensibly.** Don't push directly to `main`/`master` without explicit consent. Prefer feature branches for new work.
- **Pull before pushing.** When collaborating, make sure the local branch is up to date to avoid unnecessary merge conflicts.
- **Don't skip hooks.** Avoid `--no-verify` unless the user explicitly requests it.
- **Don't amend pushed commits.** Only amend commits that are local and were created in the current session.

## General Coding Practices

- **Read before you edit.** Always inspect a file (and any related context) before making changes.
- **Prefer editing existing files over creating new ones.** Only create new files when strictly necessary.
- **Match existing style.** Follow the conventions already present in the file or folder you are working in (naming, indentation, SQL style, etc.).
- **Avoid noise comments.** Don't add comments that simply narrate what the code does. Comments should explain non-obvious intent or trade-offs.
- **Keep changes scoped.** Don't refactor or reformat unrelated code while completing a task.
- **Surface uncertainty.** If a requirement is ambiguous or a query result looks wrong, flag it instead of guessing.

## File-Specific Guidance

- `**extractors/*.sql`** — Parameterized with `:period_start` / `:period_end`. Follow the existing header structure (Purpose → Scope → Out-of-scope → Parameters → Output schema → Notes), then optional CTEs, then a final `SELECT`. Resist the temptation to add filters or computed columns; push them into the Python layer.
- `**tests/checks.py**` — Pure pandas, no IO. Check primitives take a DataFrame and return a `CheckResult`. Each extractor's expectations are declared as an `ExtractorSpec`. Adding a new extractor means appending to the `EXTRACTOR_SPECS` registry; adding a new kind of check means one small `check_*` function plus a field on `ExtractorSpec`.
- `**tests/test_*.py**` — Build synthetic DataFrames in a few lines, exercise one primitive, assert on the `CheckResult`. Fast, no network, no Databricks. Run with `uv run pytest`.
- `**scripts/*.py**` — Thin orchestrators. argparse + connection setup + a loop that calls into `tests/`. No business logic.
- `**db.py**` — The single warehouse transport (only file importing `databricks.sql`). Build scripts write via `publish()` (current table + `_snapshots` history + `pipeline_runs` registry; see [Run Snapshots & Registry](#run-snapshots--registry)), not `write_dataframe` directly. When migrating to Databricks-native execution, only this file's body changes.
- `**legacy/*.sql**` — Databricks SQL notebooks. Follow the conventions documented in `legacy/AGENTS_LEGACY.md` (temp views, `GROUP BY ALL`, `TRY_DIVIDE`, metric naming patterns, percentage scaling, etc.).

## Tooling: Which MCP to Use

Several MCP servers are available; pick the right one for the job rather than defaulting to a generic tool.

- **Google Docs** — use **`docs-writer`** for anything in a Google Doc (create, edit, format, native tables, image insertion, comments). It is far richer than the generic `google-workspace` Docs tools, which lack table/image support.
- **Google Sheets** — use **`sheets-writer`** to read/write/format Google Sheets (tabs, banding, header styling, auto-sizing, etc.).
- **Databricks table queries** — use **`databricks-sql`** to run SQL against Unity Catalog tables (probing data, validating assumptions, computing metrics from the legacy/output tables).
- **Databricks notebooks** — use **`databricks-notebooks`** to create, read, edit, and execute Databricks notebooks. Never pick a cluster yourself — call the execution tool without a `cluster_id`, then ask the user which of the returned clusters to use.
- **Discovering tables & definitions** — use **`data-discovery-mcp`** to find new tables and understand their schemas/definitions before querying them.

## Skills

When the task matches a specialized domain, **read and follow the appropriate global skill before doing the work**:

- **Data engineering tasks** (SQL, Python, pandas, pipeline design, ETL/ELT, schema changes, data quality investigations, debugging unexpected query results, or anything involving Databricks/Unity Catalog) — read and follow the global `senior-data-engineer` Claude skill located at `~/.claude/skills/senior-data-engineer/SKILL.md`. This applies to virtually all work inside `legacy/` and to most work on the new pandas pipeline.

If multiple skills apply, read all relevant ones before starting.

## How to Use This File

At the start of any session in this repo:

1. Read this `AGENTS.md` end to end.
2. If the work touches `legacy/`, also read `legacy/AGENTS_LEGACY.md`.
3. If the work involves a metric calculation, consult `docs/metrics_definitions.md` for the canonical formula and datasets.
4. If the work involves data engineering, read the `senior-data-engineer` skill linked above.
5. Apply the guidance throughout the session.

