# CLAUDE.md — Internal Ops Performance

Persistent guidance for working in this repository. This file auto-loads at the start of every session; apply its conventions throughout your work.

## Project Overview

This workspace contains the **Internal Operations (IO) Performance** analysis for Nubank Mexico CX. It computes agent, XForce, and XPLead performance metrics (Adherence, NTPJ, Normalized Occupancy, Quality, Shrinkage, tNPS, WoWs, CSAT, and the composite Indexes).

Two codebases live side by side:

- `legacy/` — the old monolithic Databricks SQL pipeline. **Source of truth for metric definitions** until each one is migrated. Re-exported from production on **2026-06-30**, so the notebooks contain the current legacy rules.
- The top-level Python project — the PySpark rebuild. Logic modules are pure `pyspark.sql` (DataFrame-in / DataFrame-out), unit-tested locally against a local SparkSession, and run **on Databricks** via a git-sourced orchestration job — see [Databricks Deployment](#databricks-deployment-job--git-folder--a-duality).

The pipeline produces three layers of output, all in the `usr.danielanzures` schema:

1. **Raw data tables** (`metrics_data/`, suffixed `_raw`) — granular building blocks (per slot / job / evaluation). See [Raw Data Tables](#raw-data-tables-metrics_data).
2. **Metric tables** (`metrics/`, suffixed `_metric`) — the finished tidy-long metrics. See [Metric Tables](#metric-tables-metrics).
3. **The consolidated table** `io_performance_metrics` — the union of all metric tables with a display `team` column, the dashboard-facing equivalent of the legacy `internal_ops_performance_2026*` tables. See [Consolidated Table](#consolidated-table-io_performance_metrics).

Both layers cover the **Core, Fraud, Social Media, and Content** teams (plus the `quality` / `planning` support squads). Teams are groupings of roster **squads**; the raw tables carry `squad` but no `team` — `docs/team_squad_mapping.md` is the **official team → squad mapping** for any rollup (e.g. `txn` belongs to **Fraud**, not Core). Some metrics differ per team (SM and Content have their own quality sources and legacy notebooks) — always confirm which team a calculation targets.

**Manual adjustments** (per-agent / per-date carve-outs, training & shadowing windows, maternity leave, outage dates, cross-support queue exclusions) are sourced from the adjustments spreadsheet, the source of truth: [https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU/edit?gid=720896495#gid=720896495](https://docs.google.com/spreadsheets/d/1Y5P6LijLxT6hFTd69DiSPBTUPKHO-m_6zzrs-PmOjfU/edit?gid=720896495#gid=720896495). They are intentionally **not** applied in the raw layer — see [Adjustments Layer](#adjustments-layer-adjustments).

## The Parity Contract

The single rule that governs every metric decision in this repo:

> **Pipeline outputs for dates before `2026-07-01` must reproduce the legacy pipeline byte-for-byte — including its bugs. Genuine fixes apply only from `2026-07-01` onward.**

When you find legacy behavior that looks wrong, you *reproduce it* for pre-cutover dates (with a comment anchoring the legacy SQL lines) and gate the corrected behavior on the cutover. Never "clean up" a legacy quirk silently — that breaks parity.

**Cutover-gated rules currently in the code** (each anchored to its legacy SQL in the module):

- **Night-shift date attribution** (`metrics_data/shift_attribution.py`) — night-shift activity crossing midnight re-attributes to the shift's start day via a noon boundary, **from 2026-07-01 onward only** (pre-cutover byte-identical to legacy's calendar-day split).
- **SM empty-slot = 1800** (`metrics_data/occupancy_time.py`) — legacy SM credits a dimensioned OOS slot with **no matching Sprinklr overlap** as fully occupied (its `SUM(CASE WHEN activity_occuped=1 …)` is NULL, and `NULL <= 1800` falls through to `ELSE 1800`). Reproduced for slot dates **< 2026-07-01**; corrected (0 when empty) from the cutover.
- **Shrinkage required-slot rule** (`metrics/shrinkage.py`) — after dropping `lunch_break` always, a slot is required unless `activity_type_required` is `dime_invalid_notation` (dates `< 2026-03-01`) or `time_off` (`>= 2026-03-01`) — a *legacy-internal* cutover, reproduced as-is.
- **SM quality source switch** — Playvox before `2026-05-01`, Sprinklr on/after (a documented *enhancement*: legacy SM quality was Playvox-only and goes dark mid-May).

**Deliberate divergences from legacy** (documented, intentional — do not "fix back"):

- **Nuvinhos Performance** uses the flat per-agent average, not legacy's cohort-count-biased two-level average.
- Legacy **quartile metrics** (`*_general_quartile`, `*_team_quartile`) and most per-squad/district/xforce/xplead roll-ups of the base metrics are **out of scope** (never ported), as is the S&D deck.
- **quarter / semester / year granularities** are extensions — legacy only has day/week/month.
- `xforce_index` intentionally drops legacy's SM-Mar+/Content-Apr+ improved_benchmark component addition.

**Known UNRESOLVED divergences** (found by the 2026-07-01 parity sweep; pending a scope decision — do not claim parity here):

- **`shrinkage_xplead` is a different metric**: legacy computes `COUNT(DISTINCT xforce with shrinkage ≤ 20) / COUNT(DISTINCT xforce)` per XPLead (share of XForces in target — main deck SQL ~L1642-1674); ours emits slot-weighted shrinkage per XPLead. ~100% mismatch.
- **SM `shrinkage_xforce` roster scope**: legacy computes over the SM deck's own roster; ours over `team = 'Social Media'` only (smaller population).

**The legacy 2026-06-30 re-export.** Production re-exported all legacy tables on 2026-06-30, recomputing history with three rule changes (all now in the repo's `legacy/*.sql`): the `xforce_index` shrinkage target relaxes to 23% for May/June-2026 *month* buckets; the `xforce_index` xpeers_in_target component rescales on-target values into the 90-100 band with a 70-cliff (**the rescale lives ONLY in `index_xforces_final` / our `metrics/xforce_index.py` component fold, applied to all history — `xpeers_in_target`'s own thresholds are UNCHANGED**, verified against the re-export diff); and `content_csat` excludes the 'tiempo de entrega' question. The re-export also re-included some outage-date rows (e.g. quality 03-27/04-09) that we drop by documented decision — expect small legacy-side drift when re-checking parity.

## Repository Layout

Flat layout. No `src/` wrapper, no nested packages, no `__init__.py` files — with one exception: `adjustments/__init__.py` must stay, because `adjustments` is imported as a package (`from adjustments.manual import ...`).

- `extractors/` — One `.sql` file per source. Parameterized with `:period_start` / `:period_end`. No filters, no calculations, no business logic — just raw pulls from Databricks tables.
- `metrics_data/` — Pure-PySpark logic modules that build the **raw data tables**. One module per raw table; minimal filtering only (business exclusions are deferred to the metrics layer). Shared fixed DIME filter constants live in `metrics_data/dime_filters.py`; the night-shift rule in `metrics_data/shift_attribution.py`.
- `metrics/` — Pure-PySpark logic modules that build the **finished metric tables** from the `io_*_raw` tables (one module per metric); shared bucketing/aggregation lives in `metrics/metric_utils.py`. This is where business exclusions, benchmarks, ratios, and sheet-backed adjustments get applied. `metrics/performance_metrics.py` builds the consolidated table.
- `adjustments/` — The **manual adjustments layer**: `adjustments/manual.py` holds the PySpark apply-helpers consumed by the metric modules and build scripts; sheet download/sync lives in `scripts/adjustments_scripts/`. See [Adjustments Layer](#adjustments-layer-adjustments).
- `scripts/` — Runnable entry points: thin orchestrators over the **shared `db.py` Spark transport**, each with its own CLI. `scripts/metrics_data_scripts/` (one `build_*.py` per raw table), `scripts/metrics_scripts/` (one per metric + `build_performance_metrics.py`), `scripts/adjustments_scripts/` (`download_adjustments.py` / `sync_adjustments.py`), `scripts/adhoc/` (dated one-off deliverable scripts — not pipeline tasks). The DQ runner `check_extractor_data_quality.py` stays at the `scripts/` root and **requires a Unity-Catalog-capable SparkSession** (run it on Databricks; a local SparkSession cannot resolve the `etl.*` / `usr.*` / `gsheets.*` catalogs — for ad-hoc local validation, run extractor SQL through the `databricks-sql` MCP instead).
- `db.py` — The single warehouse transport (`get_spark()` resolves the ambient SparkSession; `open_connection()` is its alias). Build scripts write via `publish()` — see [Run Snapshots & Registry](#run-snapshots--registry) — and resolve their period end via `resolve_period_end()` (ISO date or the `max_dime` sentinel → `MAX(dime_date)` of the DIME ETL table).
- `gsheets.py` — Local, pure-Python Google Sheets transport (`gspread`; reads/writes pandas). See [Google Sheets access](#google-sheets-access-gsheetspy).
- `tests/` — PySpark `test_*.py` unit tests building synthetic Spark DataFrames against a shared **local SparkSession** (`tests/conftest.py`) — no warehouse — mirroring the code layout (`tests/metrics_data/`, `tests/metrics/`). The DQ engine `tests/checks.py` (pandas check primitives + the `EXTRACTOR_SPECS` registry, coverage enforced by a meta-test) and `tests/parity/` live at the root. Imports resolve via `pythonpath = [".", "tests", "metrics_data", "metrics"]` in `pyproject.toml`. See [Testing](#testing).
- `docs/metrics_data_docs/`, `docs/metrics_docs/`, `docs/adjustments_docs/` — one `.md` per raw table / metric / adjustment + `README.md` indexes.
- `docs/metrics_definitions.md` — canonical definitions, formulas, datasets, filters, worked examples. Source of truth when migrating legacy calculations.
- `docs/team_squad_mapping.md` — the **official** team → squad mapping.
- `legacy/CLAUDE.md` — the legacy pipeline's own context doc (auto-loads when working under `legacy/`): architecture, source datasets, SQL conventions, and the published-table gotchas. Note: only the **Core & Fraud / Social Media / Content** decks are the pipeline — `[IO] Performance 2026 - S&D.sql` is not a pipeline component, and the runnable SM/Content SOT is the **Temp Fix** notebook variant.

## Raw Data Tables (`metrics_data/`)

The `metrics_data/` modules produce the granular raw tables that feed the metric calculations. They are **not** finished metrics: each keeps the most granular rows and applies only minimal filtering (active-agent scoping plus the **fixed DIME filters** — the meeting/leave `dimensioned_activity` exclusion and per-metric DIME-squad exclusions, shared via `metrics_data/dime_filters.py`). Business exclusions, adjustments, benchmarks, and ratios are deferred to the metrics layer.

Each table is written by `scripts/metrics_data_scripts/build_<name>.py` (default target `usr.danielanzures.io_<name>_raw`, overridable with `--target`) and documented in `docs/metrics_data_docs/<name>.md`.

**Table naming.** Raw tables are suffixed `_raw`; metric tables `_metric` (`io_jobs_raw` keeps its single `_raw` — the dataset is literally named `jobs_raw`). Every built table has an append-only history twin suffixed `_snapshots` and is recorded in `usr.danielanzures.pipeline_runs` — see [Run Snapshots & Registry](#run-snapshots--registry).

All tables share the same leading dimensions, in this order: `agent, xforce, xplead, team, squad, district, shift, date`. Roster dimensions come from the `agent_information` extractor; `district` is the roster `squad_district` renamed; `team` derives from `squad` per `docs/team_squad_mapping.md` (the `quality` and `planning` support squads are kept to match legacy `adherence_io` but carry `team = NULL`). Core / Fraud / Social Media come from the BDX snapshots; **Content comes from a Google Sheet** (currently `gsheets.sheets.mx_content_bdx_daniel_anz_temp`; canonical `gsheets.sheets.mx_content_bdx`) unioned into the same extractor — content rows carry `squad = 'enablement'`, `district = 'content'`, `team = 'content'` (forced).

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
| `io_jobs_within_sla_raw`     | one row per Content OOS job (content_id-grouped) with its SLA threshold + on-time flag (**Content only**) | `agent, xforce, xplead, team, squad, district, shift, roster_status, date, job_type, content_id, actual_seconds, sla_seconds, within_sla, sla_met_seconds`                                       |

`io_content_csat` is the one table not keyed by `agent`: CSAT is attributed by `target_squad` (the squad a survey rates), so each response fans out to every active content agent serving that squad — the join is on `(target_squad, snapshot_month)`. The content roster's `target_squad` is surfaced by the `agent_information` extractor (NULL for non-content rows).

**Night-shift date attribution.** The four slot/time-based raw tables (`adherent_time`, `occupancy_time`, `shrinkage_slots`, `jobs_raw`) re-attribute night-shift activity that crosses midnight back to the day the **shift started** (agents with roster `shift = 'night'`, noon boundary `DATE(local_ts - 12h)`), **from the 2026-07-01 cutover onward**. The rule lives in one place — `metrics_data/shift_attribution.py`. In `jobs_raw` both the jobs and the DIME required-set are re-attributed identically so the NTPJ required-flag join stays aligned. See [README → Date attribution](docs/metrics_data_docs/README.md#date-attribution-night-shifts).

**SM empty-slot 1800 rule.** For SM slots (`agent_dime_squad IN ('social','social_social')`, excluding lunch/time_off/shrinkage and pre-reclassification `dime_invalid_notation`) with **zero matching-activity overlap**, `occupancy_time` credits the full 1800s for slot dates **< 2026-07-01** (legacy parity — see [The Parity Contract](#the-parity-contract)); actual overlap (0 when empty) from the cutover. Anchored in `docs/metrics_data_docs/occupancy_time.md`.

## Metric Tables (`metrics/`)

The `metrics/` modules turn the `io_*_raw` tables into **finished metrics** — this is where business exclusions, benchmarks, ratios, and the sheet-backed manual adjustments get applied. Each module emits a tidy long-format metric at **day / week / month / quarter / semester / year** granularity, written by `scripts/metrics_scripts/build_<metric>.py` (default target `usr.danielanzures.io_<metric>_metric`; `--target` / `--source` overridable) and documented in `docs/metrics_docs/<metric>.md`.

**Shared shape.** All metric tables use the same column order: `agent, xforce, xplead, team, squad, district, shift, date_reference, date_granularity, metric, numerator, denominator, metric_value`. `week` = Monday-start (Spark `DATE_TRUNC('WEEK')`); `quarter`/`year` = first of the calendar quarter/year; `semester` = Jan 1 or Jul 1. Hierarchy fields take their most-recent value within each bucket (legacy `FIRST_VALUE(... ORDER BY date DESC)`). `metric_value = numerator / denominator * 100`, NULL when the denominator is 0 — with exceptions: `quality` uses `scale = 1` (numerator already 0-100, so metric_value is the mean score); `tnps`'s numerator (promoters − detractors) can be negative, so metric_value is an NPS in `[-100, 100]`; `wows` is a **count** (metric_value = numerator; denominator carries the monthly target `5`; doesn't use `aggregate_long`). Two modules carry a `_metric` filename suffix to avoid colliding with same-named raw modules (`metrics/wows_metric.py`, `metrics/content_csat_metric.py`) — the metrics are still `wows` / `content_csat`. Shared bucketing lives in `metrics/metric_utils.py` (`aggregate_long`).

| table                            | metric                                                                                                                                                                    | input                                                                                                                                    | grain                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `io_adherence_metric`            | `adherence` = connected / dimensioned time (≥ 95%), all teams                                                                                                             | `io_adherent_time_raw`                                                                                                                   | agent, all six granularities                                                     |
| `io_ntpj_metric`                 | `ntpj` — Core/Fraud: actual / expected job time (≤ 100%, lower-better). **Content: SLA-weighted compliance (≥ 95%, higher-better, ≤ 100)** — a different metric under the same name | `io_jobs_raw` (Core/Fraud) + `io_jobs_within_sla_raw` (Content)                                                                          | agent, all six granularities                                                     |
| `io_normalized_occupancy_metric` | `normalized_occupancy` = occupancy / district+shift benchmark (≥ 100%), all teams                                                                                         | `io_occupancy_time_raw`                                                                                                                  | agent, all six granularities                                                     |
| `io_quality_metric`              | `quality` = mean QA score (≥ 95%), Core / Fraud / Social Media (Content uses CSAT)                                                                                        | `io_quality_evaluations_raw`                                                                                                             | agent, all six granularities                                                     |
| `io_shrinkage_metric`            | `shrinkage` (agent) = non-productive / required slots (≤ 20%); plus roll-ups `shrinkage_xforce` / `shrinkage_xplead`                                                      | `io_shrinkage_slots_raw`                                                                                                                 | agent + XForce/XPLead roll-ups, all six granularities                            |
| `io_tnps_metric`                 | `tnps` = (promoters − detractors) / valid responses (≥ 88%), **SM only**                                                                                                  | `io_tnps_responses_raw`                                                                                                                  | agent, all six granularities                                                     |
| `io_wows_metric`                 | `wows` = `COUNT(DISTINCT case_id)` (**count**, monthly target ≥ 5), **SM only**                                                                                           | `io_wows_raw`                                                                                                                            | agent, all six granularities                                                     |
| `io_content_csat_metric`         | `content_csat` = SUM(promoters) / SUM(questions) (≥ 95%), **Content only** (Content's Quality)                                                                            | `io_content_csat_raw`                                                                                                                    | agent, all six granularities                                                     |
| `io_ntpj_xforce_metric`          | `ntpj_xforce` = share of an XForce's agents on-target — team-aware: Core/Fraud `ntpj ≤ 100`, Content `ntpj ≥ 95`                                                          | `io_ntpj_metric`                                                                                                                         | **XForce roll-up**, week + month only                                            |
| `io_normalized_time_per_job`     | NTPJ benchmark **substrate** (cohort-wide `exp_duration_job` per agent × job_id × month; not a tidy metric)                                                               | `io_jobs_raw`                                                                                                                            | feeds improved_benchmark                                                          |
| `io_improved_benchmarks_metric`  | `improved_benchmark_xforce` = improved / comparable monthly benchmarks (≥ 60%), Core/Fraud only                                                                           | `io_normalized_time_per_job` + `io_occupancy_time_raw` + `io_ntpj_xforce_metric`                                                         | **XForce**, month only (`agent`/`squad`/`district`/`shift` NULL)                  |
| `io_xpeer_index_metric`          | `xpeer_index` = mean of an agent's other metrics (≥ 95%), all teams                                                                                                       | the agent-level `io_*_metric` tables (**composite**)                                                                                     | agent, all six granularities                                                     |
| `io_nuvinhos_performance_metric` | `nuvinhos_performance` / `_squad` / `_district` = avg Index(Nuvinhos) / avg Index(Old), all teams                                                                         | `io_xpeer_index_metric` + `agent_information`                                                                                            | XForce / squad / district roll-ups, all six granularities                        |
| `io_xpeers_in_target_metric`     | `xpeers_in_target` (XForce) + `xpeers_in_target_xplead` (+ SM `_squad`/`_district` variants) = targets achieved / total targets (≥ 70%), Core/Fraud/SM                    | the agent-level `io_*_metric` tables (**composite**)                                                                                     | XForce + XPLead roll-ups, all six granularities                                  |
| `io_average_xpeer_index_metric`  | `average_xpeer_index` = mean agent Xpeer Index per XForce, all teams                                                                                                      | `io_xpeer_index_metric`                                                                                                                  | XForce roll-up, all six granularities                                            |
| `io_xforce_index_metric`         | `xforce_index` = mean of up to 4 normalized components, all teams                                                                                                         | `io_shrinkage_metric` + `io_xpeers_in_target_metric` + `io_average_xpeer_index_metric` + `io_improved_benchmarks_metric` (**composite**) | XForce roll-up, all six granularities                                            |
| `io_average_xforce_index_metric` | `average_xforce_index` = mean XForce `xforce_index` per XPLead, all teams                                                                                                 | `io_xforce_index_metric`                                                                                                                 | XPLead roll-up, all six granularities                                            |

### Per-metric behavior notes

Full derivations live in `docs/metrics_docs/<metric>.md` — these are the rules you must not violate:

- **Adherence** drops non-productive slots (`lunch_break` / `time_off` / `shrinkage`) before the ratio. The meeting/leave `dimensioned_activity` exclusion and the DIME-squad exclusion (`agent_dime_squad` not in `wfm` / `credit_evolution` / `dote`, NULL dropped) are **fixed raw-layer filters** (`adherent_time.filter_dime`, constants in `dime_filters.py`); per-agent/outage adjustments come from the sheet (`drop_slot_windows` / `reclassify_dime_slots`).
- **NTPJ** keeps finished jobs only; the cohort-wide monthly benchmark `exp_duration_job` uses a trailing `[M-4 … M]` window for months ≤ 2026-03 and the current month from 2026-04, so `build_ntpj.py` reads a 4-month look-back before `period_start`. Agent contributions require `required_activity_on_day_flag == 1` (the benchmark doesn't). SM has no NTPJ. Cross-support/outage/job exclusions come from the sheet, plus documented legacy hardcodes (`HARDCODED_AGENT_DATE_EXCLUSIONS` in `metrics/ntpj.py`, pending sheet migration).
- **Content NTPJ is a different metric** (SLA-weighted compliance, not duration): per Content OOS job (grouped by `content_id`, or per row for `macros`/`faq`/`ar`), a job earns its full SLA seconds iff delivered within its OLD-SLA threshold; metric = `SUM(sla_met_seconds)/SUM(sla_seconds)*100`, ≤ 100, higher-better. SLA map = `Content - SLAs` sheet tab (`adj_content_slas`); substrate = `io_jobs_within_sla_raw`; `build_ntpj.py` drops the duration-based Content rows and unions the SLA metric (`metrics/content_sla_ntpj.py`) so `io_ntpj_metric` stays one `metric='ntpj'` table. Consumers are Content-aware: `xpeer_index` adds Content NTPJ **raw**; `ntpj_xforce` uses `≥ 95`.
- **Normalized Occupancy** floors its output at **2026-03-01** for every deck (`NOCC_START_DATE` — legacy publishes no Jan/Feb NOcc; owner directive 2026-07-02, reversing the earlier SM-on-all-dates extension); drops the same non-productive slots as adherence; per-agent `occupancy = SUM(occupancy_minutes)/SUM(required_minutes)` over a monthly **district+shift benchmark** (per-`(month, district, shift, squad)` ratio, then the mean of squad ratios). `numerator` = agent %, `denominator` = benchmark %. Content's benchmark is district-only (NULL-shift forced onto a shift-agnostic key — matching the legacy Content deck). The `nitza.zarza` suppression is a documented hardcode.
- **Quality** unions Playvox (all teams) + Sprinklr SM (`>= 2026-05-01`), tagged by `source`; keeps the latest record per `evaluation_id`; averages `qa_score` (`scale = 1`). Content excluded (CSAT is its quality). Outage dates 03-27/04-09 are DROPPED (documented decision — legacy's re-export re-included them).
- **Shrinkage**: `SUM(shrinkage_flag) / SUM(required_slot)` with the pre/post-2026-03-01 required-slot rule. Sheet adjustments apply (`drop_slot_windows` / `reclassify_dime_slots` / `apply_no_shrinkage`); the shrinkage-specific DIME-squad exclusion is a raw-layer filter. The `shrinkage_xforce`/`shrinkage_xplead` roll-ups are **slot-weighted** (sum num/denom, not averaged %) — **but see the Parity Contract: legacy's `shrinkage_xplead` is a different metric** (share of XForces in target), an unresolved divergence.
- **Xpeer Index** (legacy `index_agent`) is a composite: adherence is the **driver** (an agent is in the Index iff it has an adherence row). Components fold first — NTPJ around 100 (`≤100→100`, `100–200→200−x`, else 0), NO truncated at 100, WoWs `≥5→100` else `x/5*100`; tNPS/Quality/CSAT enter raw — then average. Roster is team- and era-dependent: Core/Fraud = Adherence + NTPJ (+Quality from Feb 2026, +NO from Mar); Content = Adherence + NTPJ raw (+NO and +CSAT from Mar); SM = Adherence + WoWs (+tNPS when present, +Quality from Feb, +NO from Mar). Missing Quality/CSAT/tNPS drop from sum AND divisor. Multi-month buckets anchor the era on the period's **end** month; pre-2026 buckets drop. `nitza.zarza` carve-out is a documented hardcode.
- **Xpeers In Target** flags each agent in/out of target per component (adherence ≥95, ntpj ≤100 [Content ≥95 — but Content is excluded entirely; legacy never built it], NO ≥100, quality ≥95, tnps ≥88, wows ≥5) and reports `Σ achieved / Σ targets`. Two grains in one table (`xpeers_in_target` per XForce; `_xplead` per XPLead, `xforce` NULL) plus SM `_squad`/`_district` variants. Era-gated like the Index; adherence is the driver; `team` is NULL by design. **The on-target thresholds were NOT changed by the 2026-06-30 re-export** — the 90-100 rescale lives only in `xforce_index`'s component fold.
- **Improved Benchmarks**: month-over-month comparison of the NTPJ `exp_duration_job` (lower = improved) and NO district+shift benchmarks (higher = improved), ties improved, rolled to XForce (`improved_benchmark_xforce`), **Core/Fraud, month grain only**, gated `date_reference < 2026-05-01` plus a `david.fernandez` April carve-out; units gated to `(xforce, month)` present in `io_ntpj_xforce_metric`. The S&D squad/district roll-ups are out of scope.
- **XForce Index** (legacy `index_xforce`): mean of up to four 0-100 components — shrinkage (target 23 for May/June-2026 **month** buckets, else 20: `≤ target → 100`, else `(100+target) − shrinkage`), xpeers_in_target (**rescaled**: `≥ 70 → 90 + (x−70)/3`, `< 70 → raw` — the re-export rule, applied to all history), average_xpeer_index (raw), improved_benchmark (`≥60→100`, else `improved/0.6`). NULL components → 0. The component count is an explicit **DATE rule, not a presence test** (`metrics/xforce_index.py` docstring): a bucket is 4-component (`denominator = 400`) iff `date_reference < 2026-05-01 AND NOT (xplead = 'david.fernandez' AND date_reference >= 2026-04-01)`, else 3-component — applied to BOTH week and month grains and ALL teams, with a missing improved value folding to 0 (still counted). So pre-May SM/Content buckets ARE 4-component with a zeroed improved term — a documented known gap vs legacy's per-deck composition (Content: 3-component Jan–Mar, 4-component from Apr; and the 2026-06-30 legacy re-export now builds Content xpeers_in_target, which feeds legacy Content's index — ours folds that term to 0 too). `numerator` = Σ components, `denominator` = 100×N.
- **Average Xpeer / Average XForce Index**: simple means (of agent Index per XForce; of `xforce_index` per XPLead), all teams, NULL inputs excluded like SQL `AVG`; `numerator` = Σ, `denominator` = count.
- **Nuvinhos Performance**: avg Index(Nuvinhos: bucket month within `[month(last_change_date), +2 months]`) / avg Index(old) × 100, three roll-ups (XForce / squad / district) in one table, flat per-agent averages (deliberate divergence). NULL `last_change_date` counts as old, so the temp Content roster yields a degenerate no-Nuvinho result.

## Consolidated Table (`io_performance_metrics`)

`usr.danielanzures.io_performance_metrics` (module `metrics/performance_metrics.py`, script `build_performance_metrics.py`) is the **UNION ALL of all 16 metric tables** in the standard 13-column shape, with `team` replaced by a **display team** — the dashboard-facing equivalent of the legacy `internal_ops_performance_2026*` tables, one table for all decks. Team cascade: direct map (`core→Core`, `fraud→Fraud`, `social media→Social Media`, `content→Content`); else squad map (`quality→Quality`, `enablement→Content`; `planning` deliberately stays NULL); else **modal-team backfill** from the adherence driver at the same `(date_reference, date_granularity)` — per squad, then xforce, then xplead, then district (modal = highest row count, alphabetical tie-break; dims built only from adherence rows with a non-NULL display team). **Consumption caveat:** mixed Core/Fraud XForces produce two roll-up rows per xforce key (one per team) where legacy had one — sum num/denom or filter by team when reproducing legacy roll-ups.

## Source-of-Truth (SOT) Tables

Upstream **source-of-truth** tables feed the roster, schedule, SLA, and absence logic. They are not pipeline output, but you query them constantly when building or *auditing* metrics. Most live in `etl.mx__series_contract` (WFM / contract SOT) or `usr.cross_x_mx` (planning SOT). Naming gotcha: the **district** appears in two forms — bare `identity_fraud` (BDX, approved-absences) and squad-prefixed `idsec_identity_fraud` (DIME `agent_dime_squad`, SLA SOT). Confirm which a table uses before joining.

### BDX roster snapshots — `etl.mx__series_contract.cx_mx_bdx_snapshots`

The **roster source of truth** for Core / Fraud / Social Media (Content's roster is the Google Sheet — see `docs/team_squad_mapping.md`). One row per `(agent, snapshot_date)`; the `agent_information` extractor keeps the **latest snapshot per agent per month** (canonical month-end roster). Key columns:

- `actor_email`, `xforce_email`, `xplead_email`, `xmanager_email` — identities (extract the email prefix with `^[a-zA-Z]+\.[a-zA-Z]+`, lowercased).
- `squad`, `district` — org attribution. Fraud's `squad = 'idsec'` splits into districts `identity_fraud`, `gamers`, `live_channels`, `csi`, `mule_accounts`, `victim_prevention`, `idsec`, `fraud`. District is **bare** here, not the `idsec_identity_fraud` form DIME/SLA use.
- `status` — `active` / inactive. Filter to `active` for headcount.
- `shift_name`, `shift_start_time`, `shift_end_time`, `shift_lunch_time`, `shift_day_off_1`, `shift_day_off_2`; `hire_start_date`, `termination_date`, `snapshot_date`.

**Latest snapshot.** A month never "closes" mid-month — for in-flight months use `snapshot_date = (SELECT MAX(snapshot_date) FROM ...)` (optionally bounded `<= :period_end`).

**Availability ≠ active headcount — use the two `shift_day_off_*` columns** (lowercase day names). An agent is available on a weekday iff it's not one of their two days off; most of the roster is off on weekends, so the rostered-available weekend crew is far smaller than the active roster — and "distinct agents who logged work" is throughput, not availability. Example (idsec `identity_fraud`, snapshot `2026-06-20`): 40 active, but only 11 available Saturday and 21 Sunday (18 off Sat+Sun).

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

The DIME schedule has **two representations of the same data**:

1. **The human-authored source of truth** — WFM builds each week's schedule by hand in Google Drive: [https://drive.google.com/drive/folders/1V7lFCKqEwdqwfoYNC-hdGMiAArmaqaqv](https://drive.google.com/drive/folders/1V7lFCKqEwdqwfoYNC-hdGMiAArmaqaqv).
2. **The materialized ETL table** — `etl.mx__series_contract.agent_dimensioned_activities`, read by `extractors/dime_slots.sql`. This is what every downstream raw table is built from, and its `MAX(dime_date)` is what the `max_dime` period-end sentinel resolves to.

The Drive sheets flow into the ETL table. To *audit/explain* a DIME value, the Drive sheet is authoritative (if they disagree, trust the sheet and treat the ETL row as stale/mis-ingested); to *compute at scale*, use the ETL table. The ETL table lags (may be missing the in-flight week) — read the Drive sheet when the week isn't there yet.

**How to read the Drive DIME** (via the `google-workspace` MCP — `drive_search`, `sheets_getMetadata`, `sheets_getRange`):

- The root folder holds one subfolder per week, `W1 … W27`. Inside, the consolidated schedule is the spreadsheet named `S## 2026` (the per-team `… Template W##` sheets are not the final DIME).
- One tab per day — `LUN, MAR, MIER, JUE, VIE, SAB, DOM` — plus support tabs (`BDX`, `Ranges`, `VAC&UPTO`, `Glosario`, …) that are not the schedule.
- Within a day tab: **row 10 is the header**; agents in **column `G` (`Xpeers`)**; `A–F` = `SHIFT / Inicio / Pausa / Fin / Squad / STEP`; `I` = `Status` (`AcDay` working, `DayOFF` rest); the 30-minute slot columns run from `J` onward (`0h0 … 23h30`), each cell carrying the slot's `dimensioned_activity` (the ETL column's value).
- **Week-number gotcha:** `S##` is **not** the ISO week number — always confirm the actual Monday from the tab's date cell (`A5` / `J4`). E.g. `S18`'s Monday is `2026-04-27`, `S19`'s is `2026-05-04`, `S25`'s Saturday is `2026-06-20`.

**Leave lives in `dimensioned_activity`, not `activity_type_required`.** Leave/exception slots (`Licencia`, `Vacacion`, `Permiso Medico` / `Permiso medico`, `Mouring`, `Weekly`, `Huddle`) appear in `dimensioned_activity` while their `activity_type_required` is typically `dime_invalid_notation` (not `time_off`) — a slot can look "scheduled to work" by `activity_type_required` alone yet be registered leave. Excluding this list is a **fixed DIME filter** (not a manual adjustment), applied in the raw layer (`metrics_data/adherent_time.py` → `filter_dime`; constant `MEETING_LEAVE_DIMENSIONED_ACTIVITIES` shared via `metrics_data/dime_filters.py` — the dual `Permiso Medico`/`Permiso medico` casing is intentional, both exist in source data).

**Cross-support is encoded in the `dimensioned_activity` code, not `agent_dime_squad`.** `agent_dime_squad` always carries the agent's **home** squad. The code follows `<CHANNEL>_<TARGET>[_TSKF]`:

- **CHANNEL** — work channel: `BKO` (backoffice), `OOS`, `MAIL`, … (maps to `activity_type_required`). A leading **`x`** (e.g. `xBKO_ENG`) marks **overtime** — same channel, outside the regular dimensioned shift.
- **TARGET** — the product/squad served: `ENG`, `CTA`, `LCYC`, `TXN`, `CBF`, … Home work uses the agent's own token (engagement agents → `*_ENG`).
- **`_TSKF`** — a **task force**: a cross-support pool spanning several home squads (`BKO_CTA_TSKF`, `BKO_TXN_TSKF`, `OOS_CBF_TSKF`, …). `_TSKF` is cross-support by construction.

A productive slot is **cross-support** when its TARGET ≠ the agent's home token, or it carries `_TSKF`. For engagement specifically: everything not `*_ENG` (worked example: May 2026 engagement had 411 cross-support slots = 205.5 dimensioned hours across 6 agents, 172.5 worked → 84% occupancy). Cross-support **queue exclusions** are an Adjustments-layer concern (sheet tab `Cross Support`); this grammar is just how to identify the slots.

### SLA targets — `usr.cross_x_mx.planning__sla_sot`

Per-queue SLA source of truth (snapshotted): `sla_seconds` per `(queue, activity_type)` + `squad`/`district` attribution + `snapshot_ts_mx`. Dedupe to the latest snapshot per `(queue, activity_type, day)` and pick the snapshot effective for the measured week (`week_queue_latest` pattern). Districts use the squad-prefixed form (`idsec_identity_fraud`). Used to judge first-response latency against target.

### Approved absences — `usr.cross_x_mx.planning__xpeer_approved_absence_events`

WFM-**approved leave events** (the approval record, distinct from DIME scheduled leave). One row per `(work_email, absence_date, …)`; `absence_type` (`XPEER VACATION`, `MEDICAL LEAVE - SHORT TERM`, `PERSONAL MATTERS`, `MATERNITY LEAVE`, …), `absence_reason`, `district` (**bare** form), `step`, `shift_start_date`/`shift_start_time`. Use to explain coverage gaps (e.g. how many rostered-available weekend agents were on approved vacation).

## Google Sheets access (`gsheets.py`)

`gsheets.py` is the local, pure-Python Sheets transport (`gspread` + `google-auth`; reads/writes pandas DataFrames; no Spark/`dbutils`). It's how the repo reads the **adjustments sheet** and the **Content roster**. (Reading the DIME schedule from Google **Drive** is a different path — the `google-workspace` MCP.)

**Auth — always the `nu-mx-internal-ops` service account** (its `client_email`, form `<name>@<project>.iam.gserviceaccount.com`), never a personal login. Credentials resolve from env vars in order (put one in the gitignored repo-root `.env` locally; never hardcode the key):

1. `GOOGLE_SERVICE_ACCOUNT_JSON` (inline JSON) → 2. `GOOGLE_SERVICE_ACCOUNT_FILE` (path) → 3. `GOOGLE_APPLICATION_CREDENTIALS` → 4. per-field `GOOGLE_SA_*` vars, reassembled (`private_key` `\n`-unescaped).

**On Databricks** the key comes from the secret scope **`nu-mx-internal-ops-sa-secret`**, which stores the JSON **decomposed into one secret key per field** (11 keys — there is no single-JSON key); the job cluster's `spark_env_vars` wire each to its `GOOGLE_SA_*` var, so path 4 reassembles at runtime.

**Share the target sheet with the SA's `client_email`** (Viewer to read, Editor to write) — unshared sheets return `403 PERMISSION_DENIED`.

Usage: `read_worksheet(sheet_url, tab)` → DataFrame (first row = header, **all-string** columns — cast as needed); `write_dataframe(sheet_url, df, tab)` **clears then overwrites** the whole tab (creates it if missing); `list_worksheets(sheet_url)`; pass `client=open_client()` to reuse one authorized client. CLI: `python gsheets.py list|read <sheet_id_or_url> ["Tab"]`. Deps: `uv sync --group sheets` (note `uv` may be 401-blocked against the private index — use the existing `.venv` then). For bulk writes prefer a gspread script over the row-by-row `sheets-writer` MCP.

## Adjustments Layer (`adjustments/`)

The third layer (on top of `metrics_data/` → `metrics/`): the **manual adjustments** — per-agent / per-date carve-outs requiring a human decision (cross-support exclusions, leave reclassifications, training/shadowing windows, outage dates, per-agent Index carve-outs). Applied in the **metric layer** (which raw rows count / how they're reclassified), never in `metrics_data/`.

**Source of truth** is the adjustments Google Sheet (one tab per adjustment type; link in [Project Overview](#project-overview)). Sheet gotcha: `Exclusiones Generales` = functional slot-drops; `Correcciones Generales Datos` = tracking only (hardcoded elsewhere). Downloaded CSVs under `adjustments/data/` are gitignored (the sheet is SoT).

**Status: partially implemented.** `adjustments/manual.py` implements the apply-helpers — `reclassify_dime_slots`, `drop_slot_windows`, `apply_no_shrinkage`, `drop_cross_support_jobs`, `drop_excluded_jobs`, `append_missing_dime_slots`, plus `read_adjustment_table` for the synced `adj_*` Delta tables — and `scripts/adjustments_scripts/` (`download_adjustments.py` / `sync_adjustments.py`) syncs the sheet tabs into those tables (the job's root `sync_adjustments` task runs this every pipeline run). Applied in `metrics/adherence.py`, `normalized_occupancy.py`, `ntpj.py`, `shrinkage.py`, `improved_benchmarks.py`; tests in `tests/test_manual_adjustments.py` + `tests/test_download_adjustments.py`. **Remaining un-ported legacy hardcodes** deliberately stay in the metric modules pending sheet migration: `HARDCODED_AGENT_DATE_EXCLUSIONS` (`metrics/ntpj.py`) and the `nitza.zarza` NO-suppression constants (`metrics/normalized_occupancy.py`, `metrics/xpeer_index.py`). Cutover-date constants are intentionally per-module. New adjustments mirror the layers: helper in `adjustments/`, wiring in `scripts/adjustments_scripts/`, tests, one `docs/adjustments_docs/<name>.md`.

## Run Snapshots & Registry

Every build script writes through `db.publish()` (never `write_dataframe` directly), which persists three things per run:

1. **The current table** — replaced via `CREATE OR REPLACE TABLE` (preserves Delta version history).
2. **The append-only history twin `{table}_snapshots`** — the same rows tagged `run_id` (STRING) + `run_ts` (TIMESTAMP UTC), partitioned by `run_id`. Idempotent per run: re-running a `run_id` first deletes that partition.
3. **A row in `usr.danielanzures.pipeline_runs`** — one per `(run_id, table)`: `run_id, run_ts, layer, table_name, snapshot_table, period_start, period_end, row_count, status, git_sha, notes`. Written **last** (status `success`), so a crash mid-run leaves no success row.

**run_id** resolves as explicit `--run-id` > `PIPELINE_RUN_ID` env var > generated sortable UTC id (`YYYYMMDDTHHMMSSZ-<hex>`); operator-supplied values are validated against `[A-Za-z0-9._:-]+` (they're interpolated into the snapshot DELETE). Export `PIPELINE_RUN_ID` once so a batch of builds shares one id. `--no-snapshot` skips the history twin for ad-hoc runs.

**Querying**: latest published run = max `run_id` in `pipeline_runs` with `status='success'`; pull it via `SELECT * FROM {table}_snapshots WHERE run_id = :run_id`. Diff two runs by joining snapshots on the business key — this is also how to **spot-check parity after a risky merge** (compare the new run's pre-2026-07-01 partition against the prior run's).

## Databricks Deployment (job ↔ git folder — a duality)

The repo is wired into Databricks (workspace `nubank-e2-general`, profile `nubank-e2-general`) **two independent ways** that do not share state:

1. **The orchestration job — `[IO] Performance Metrics Pipeline`** (job id `267598911414455`). Tasks are `spark_python_task`s with `source: GIT` pointing at this GitHub repo, branch `main` — **Databricks checks out `main` fresh from GitHub on every run**, so whatever merges to `main` goes live on the next run. **27 git-sourced tasks**: a root `sync_adjustments` task (syncs the adjustments sheet into the `adj_*` Delta tables) that every raw build depends on, then the raw-table builds and the metric builds, chained so each metric runs after its inputs. Each build task is parameterized `--period-start {{job.parameters.period_start}} --period-end {{job.parameters.period_end}} --run-id {{job.run_id}}`; `period_end` should use the **`max_dime` sentinel** (resolved by `db.resolve_period_end()` to `MAX(dime_date)` of the DIME ETL table) with `period_start = 2026-01-01`, so every run rebuilds full history up to the freshest ingested DIME date — a shorter `period_start` would *truncate* the tables, since `publish()` replaces them wholesale. Cluster: `[IO]-Performance-Metrics-Cluster` (Spark `15.4.x` LTS).
2. **The Databricks git folder** — `/Workspace/Users/daniel.anzures@nubank.com.mx/internal-ops-performance` (repo id `2657286665982577`): a separate, interactive checkout for browsing in the UI. It does **not** auto-update and the job never reads it. **After every merge/push to `main`, sync it**: `databricks repos update 2657286665982577 --branch main -p nubank-e2-general`. Never edit files inside the git folder directly.

Because tasks run as git-sourced `spark_python_task`s, build scripts execute against the cluster's **ambient SparkSession** (`db.get_spark()`); the logic modules stay DataFrame-in/DataFrame-out.

**Build scripts must not raise `SystemExit` on success.** Each task runs under an IPython `exec()` runner where **any `SystemExit` — even code `0` — fails the task** (skipping every downstream task, even though the table writes already completed). Entrypoints must only exit non-zero:

```python
if __name__ == "__main__":
    rc = main()
    if rc:            # success (rc == 0) falls through cleanly
        sys.exit(rc)
```

This bit the first real job run: `build_adherent_time` wrote its ~46k rows, *then* died on `sys.exit(0)`, failing the task and skipping `build_adherence`.

## Testing

Run the full suite locally (a local SparkSession; no warehouse). `uv run` may be 401-blocked against the private index — invoke the venv interpreter directly, and **export `PYSPARK_PYTHON`** first or the Python 3.14/3.12 worker mismatch fails everything:

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
.venv/bin/python -m pytest -q          # ~5 min
```

**Baseline contract: exactly 6 known pre-existing failures** (local-Spark environment issues, not regressions). Compare failure *sets*, not counts — any new failure is yours:

- `tests/metrics_data/test_occupancy_time.py::TestBuildJobsUnion::test_luis_contreras_content_oos_plus_two_hour_correction`
- `tests/metrics_data/test_occupancy_time.py::TestComputeOccupancyTime::test_basic_end_to_end_shape`
- 4× `tests/test_manual_adjustments.py` (`test_reclassify_dime_slots_updates_activity_and_shrinkage_flags`, `test_drop_slot_windows_supports_todos_xplead_scope`, `test_apply_no_shrinkage_keeps_required_slot_but_clears_numerator_flags`, `test_job_exclusions_match_queues_and_squad_scope`)

## Working Conventions

These are load-bearing — they prevent the project from sliding back into complexity.

- **PySpark logic, transport isolated.** Logic modules (`metrics_data/`, `metrics/`, `adjustments/`) are pure `pyspark.sql` — DataFrame-in / DataFrame-out, no table IO, no env vars, no `dbutils`. Session/catalog concerns live only in `db.py` and `scripts/`.
- **Flat layout.** No `src/`, no nested packages, no `__init__.py` unless genuinely needed (today only `adjustments/`). Earn each new folder by feeling real pain.
- **Transport in `scripts/` + `db.py`, logic in the logic modules.** Anything opening a Spark session, reading env vars, parsing CLI args, or touching tables lives in `scripts/` (via `db.py`). Anything DataFrame-in/DataFrame-out lives in the logic modules. `tests/` holds the unit tests plus the DQ check primitives.
- **SQL extractors stay dumb.** No filters, no joins beyond identity resolution, no derived business columns.
- **Rule of three.** Don't extract a shared abstraction until three concrete examples drive it.
- **Validate assumptions with real data.** Before relying on a column shape, uniqueness, or join grain, probe Databricks via the MCP. Guessing costs metric drift; probing costs 30 seconds.
- **Ground metric behavior in the legacy SQL — don't guess.** The `legacy/` notebooks are the source of truth per metric until migrated. When porting, diagnosing a parity gap, or explaining a divergence, read the actual legacy view/benchmark/filter definition. Map notebook → deck via `legacy/CLAUDE.md`: **Core/Fraud** = the per-dataset notebooks (`[IO] Adherence Dataset.sql`, `[IO] Normalized Occupancy Dataset.sql`, `[IO] NTPJ Dataset.sql`, `[IO] Quality Dataset.sql`, `[IO] Shrinkage Dataset.sql`) orchestrated by `[IO] Performance 2026.sql`; **SM/Content** are self-contained in the **Temp Fix** variants (the runnable SOT that materializes `sm_temp_*` / `cont_temp_*`). The same metric can differ per deck (e.g. Content's NO benchmark is `squad_district`-only vs the main deck's `district + shift`) — read the deck that owns the rows you're checking.
- **Scope-check before parity work.** Validate a legacy metric is in scope (via `legacy/CLAUDE.md`) before porting or chasing parity; degenerate legacy output means question the scope, not reproduce it.

## Git Hygiene

- **Never modify git config** or run destructive commands (`push --force`, `reset --hard`) unless explicitly asked.
- **Do not commit unless explicitly asked.** Wait for the user to request it.
- **Meaningful commit messages** — the *why*, subject ≤ 72 chars, body when helpful.
- **Never commit secrets** (`.env`, credentials, tokens, the SA email). Warn if asked to.
- **Focused commits**; check `git status` + `git diff` before staging; feature branches + PRs, never straight to `main`; pull before pushing; don't skip hooks; don't amend pushed commits.

## General Coding Practices

- **Read before you edit.** Inspect a file and its context before changing it.
- **Prefer editing existing files over creating new ones.**
- **Match existing style** (naming, indentation, SQL conventions).
- **Avoid noise comments** — comments explain non-obvious intent or trade-offs only.
- **Keep changes scoped** — no drive-by refactors.
- **Surface uncertainty** — flag ambiguity or odd query results instead of guessing.

## File-Specific Guidance

- `extractors/*.sql` — Parameterized with `:period_start` / `:period_end`. Header structure: Purpose → Scope → Out-of-scope → Parameters → Output schema → Notes; optional CTEs; final `SELECT`. No filters or computed business columns.
- `tests/checks.py` — Pure pandas, no IO. Check primitives take a DataFrame, return a `CheckResult`. One `ExtractorSpec` per extractor in `EXTRACTOR_SPECS` (set-equality with `extractors/*.sql` is enforced by a meta-test). New check kind = one `check_*` function + one `ExtractorSpec` field.
- `tests/test_*.py` — Synthetic Spark DataFrames via the shared `spark` fixture (`tests/conftest.py`); assert on collected rows. The `checks.py` tests still use pandas frames.
- `scripts/*.py` — Thin orchestrators: argparse + `db.py` transport + one call into the matching logic module. No business logic. Respect the SystemExit rule.
- `db.py` — The single warehouse transport. Writes go through `publish()`; period ends through `resolve_period_end()`; run ids through `resolve_run_id()`.
- `legacy/*.sql` — Databricks SQL notebooks; follow `legacy/CLAUDE.md` conventions (temp views, `GROUP BY ALL`, `TRY_DIVIDE`, percentage scaling).

## Tooling: Which MCP to Use

- **Google Docs** — `docs-writer` (richer than the generic `google-workspace` Docs tools: native tables, images, comments).
- **Google Sheets** — `sheets-writer` for interactive reads/writes/formatting; a local gspread script for bulk writes.
- **Databricks SQL** — `databricks-sql` for Unity Catalog queries (probing, validation, parity checks).
- **Databricks notebooks** — `databricks-notebooks` to create/read/edit/execute notebooks. **Never pick a cluster yourself** — call without `cluster_id`, then ask the user which cluster to use. `run_databricks_notebook_cell` defaults to Scala — pass `language="python"`.
- **Table discovery** — `data-discovery-mcp` to find tables and schemas before querying.

## Skills

When the task matches a specialized domain, **read and follow the appropriate global skill before doing the work**:

- **Data engineering** (SQL, Python, PySpark, pipelines, schema changes, data-quality investigations, anything Databricks/Unity Catalog) — read `~/.claude/skills/senior-data-engineer/SKILL.md`. Applies to virtually all work in `legacy/` and most work on the pipeline.

If multiple skills apply, read all relevant ones before starting.

## How to Use This File

This file auto-loads at the start of every session. Beyond it:

1. Work touching `legacy/` also auto-loads `legacy/CLAUDE.md` — apply it.
2. Metric calculations: consult `docs/metrics_docs/<metric>.md` and `docs/metrics_definitions.md` for canonical formulas.
3. Data-engineering work: read the `senior-data-engineer` skill.
4. Apply the guidance throughout the session.
