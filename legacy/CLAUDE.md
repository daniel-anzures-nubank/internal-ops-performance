# CLAUDE.md - Internal Ops Context (legacy)

## Project Overview
This workspace contains notebooks for the **Internal Operations (IO) Performance** analysis at Nubank Mexico CX.

**Re-exported 2026-06-30.** These notebooks were re-exported from production on 2026-06-30 and contain the *current* legacy rules. That refresh changed three rules (and recomputed all history in the published tables): the `index_xforces_final` shrinkage target relaxes to **23%** for May/June-2026 month buckets; its `xpeers_in_target` component is **rescaled into the 90-100 band with a 70-cliff** (`>= 70 → 90 + (x-70)/3`) — note the `xpeers_in_target` metric's own on-target thresholds did **not** change, only this component fold; and Content CSAT excludes the *'tiempo de entrega'* question. The re-export also re-included some outage-date rows (e.g. quality 03-27 / 04-09) that earlier exports dropped.

## Pipeline Architecture

### Pipeline: "MX Internal Ops metrics update"
This is the main orchestration pipeline that updates all performance metrics.

#### Core & Fraud Agents (Performance 2026 notebook)
- **Flow**: Upstream dataset notebooks run first → produce base tables → Performance 2026 notebook orchestrates the final metrics
- **Upstream dataset notebooks** (run before Performance 2026):
  - Adherence dataset notebook
  - Normalized Occupancy dataset notebook
  - Quality dataset notebook
  - NTPJ dataset notebook
  - Shrinkage dataset notebook
- These upstream notebooks produce the tables (`usr.mx__cx.adherence_io`, `usr.mx__cx.normalized_time_per_job`, `usr.mx__cx.normalized_occupancy`, etc.) that feed into the Performance 2026 notebook.
- **Performance 2026** is the orchestrator — it reads those tables and computes final agent/xforce/xplead metrics with quartile rankings.

#### Social Media Agents (Performance 2026 Social Media notebook)
- **Flow**: Self-contained — the notebook itself produces all datasets AND orchestrates the final metrics
- No separate upstream dataset notebooks needed
- Handles metrics for social media agents only

#### Content Agents (Performance 2026 Content notebook)
- **Flow**: Self-contained — same pattern as Social Media
- The notebook itself produces all datasets AND orchestrates the final metrics
- Handles metrics for content agents only

> **Temp Fix variants are the runnable SOT.** For Social Media and Content, always read `[IO] Performance 2026 - Social Media Temp Fix.sql` / `[IO] Performance 2026 - Content Temp Fix.sql` — they materialize the `sm_temp_*` / `cont_temp_*` tables the published decks are actually built from; the non-Temp-Fix variants are older references. `[IO] Performance 2026 - Old.sql` is historical, and `[IO] Performance 2026 - S&D.sql` is **not** a pipeline component (S&D squad/district roll-ups are out of the migration's scope). The Content notebook is rebuilt **nightly**, so its published table can be transiently empty mid-rebuild.

## Team Hierarchy
- **Xpeers (agents)**: Individual customer service representatives
- **XForces**: Team leads managing a group of agents
- **XPLeads**: Senior leads managing XForces
- **Squads**: Organizational units grouped by district

## Key Metrics
| Metric | Description | Target | Level |
| --- | --- | --- | --- |
| Adherence | delivered_hours / required_hours | >= 95% | Agent |
| NTPJ (Normalized Time Per Job) | actual_duration / expected_duration (Content: SLA-weighted compliance, higher-better) | <= 100% (Content >= 95%) | Agent |
| Normalized Occupancy (NOCC) | agent occupancy / district+shift benchmark | >= 100% | Agent |
| Quality | mean QA score (Playvox; SM switches to Sprinklr from 2026-05) | >= 95% | Agent |
| Shrinkage | non-productive / required slots | <= 20% | Agent |
| tNPS | (promoters − detractors) / valid responses (SM only) | >= 88% | Agent |
| WoWs | COUNT(DISTINCT case_id) per month (SM only) | >= 5 | Agent |
| Content CSAT | promoters / questions (Content only; its Quality) | >= 95% | Agent |
| Xpeer Index | mean of the agent's other metrics (folded) | >= 95% | Agent |
| XForce Index | mean of up to 4 normalized components | >= 90% (published tables use 90, not 95) | XForce |

## Key Tables
- `etl.mx__series_contract.cx_mx_bdx_snapshots` — Agent roster snapshots (squad, district, shift, hire date)
- `usr.mx__cx.adherence_io` — Adherence hours data
- `usr.mx__cx.normalized_time_per_job` — NTPJ metric base
- `usr.mx__cx.normalized_occupancy` — Occupancy metric base

## Published Output Tables — Parity Gotchas

The final decks land in three tables: `usr.mx__cx.internal_ops_performance_2026` (Core & Fraud "main" deck), `..._2026_social_media`, and `..._2026_content`. Read these before any parity or roll-up work against them:

- `date_reference` is a **TIMESTAMP** — wrap with `DATE()` before joining on dates.
- **XForce-grain rows carry NULL `squad`** (derive Core/Fraud from the agent rows); **drop NULL-xforce roll-up rows** — they're artifacts.
- `index_xforce` has a **Jan–Mar 2026 duplicate-key fan-out artifact** (up to 18 rows per key, different values); `xpeers_in_target_xplead` has a related Jan–Mar xplead fan-out. Neither is reproduced by the new pipeline.
- **`shrinkage_xplead` is share-of-XForces-in-target** per XPLead (`COUNT(DISTINCT xforce with shrinkage <= 20) / COUNT(DISTINCT xforce)`, main deck ~L1642-1674) — *not* slot-weighted shrinkage. The new pipeline currently emits slot-weighted values here (unresolved divergence).
- **SM occupancy counts empty dimensioned OOS slots as fully occupied**: the per-slot `SUM(CASE WHEN activity_occuped = 1 THEN duration END)` is NULL when nothing matches, and `NULL <= 1800` falls through to `ELSE 1800` (SM Temp Fix ~L1129, L1189, L1223). Also SM shrinkage_xforce computes over the SM deck's own `social_agents` roster.
- The **main deck also carries cross-listed SM/Content agent rows** (e.g. the Jan-2026 `enablement` cohort's adherence/NTPJ; SM nocc rows with NULL values) — the new pipeline attributes those agents to their own teams instead; no data loss, different deck.
- The main-deck Content `ntpj_agent` rows from March onward are the **old duration-based Content NTPJ**, superseded by the SLA-based metric in the Content deck.
- Legacy grains are **day/week/month only** (improved_benchmark also has week-grain rows the new pipeline deliberately doesn't build); quartile metrics (`*_general_quartile` / `*_team_quartile`) and most per-squad/district/xforce/xplead roll-ups of base metrics were never ported.

## Source Datasets

### Agent Organizational Information (BDX)

| Table | Description | Used By |
| --- | --- | --- |
| `etl.mx__series_contract.cx_mx_bdx_snapshots` | **Primary source of truth for agent org structure.** Daily snapshots with agent email, squad, district, xforce, xplead, shift, hire date, and status. Used to build the `agent_information` temp view in every notebook. | All notebooks (Core, SM, Content) |
| `gsheets.sheets.mx_content_bdx` | **Content-specific agent roster** (Google Sheets). Contains agent email, squad, xplead, xforce, district, status, target_squad, and last_update. Uses valid_from/valid_to date ranges instead of monthly snapshots. | NTPJ Dataset, Normalized Occupancy Dataset, Content notebook |

### Scheduling & Dimensioned Activities

| Table | Description | Used By |
| --- | --- | --- |
| `etl.mx__series_contract.agent_dimensioned_activities` | **Agent scheduling (DIME) data.** Contains 30-minute time slots with scheduled activity types (shuffle, oos, shrinkage, time_off, lunch_break), dimensioned activity labels, squad assignment, and affiliation. Source of truth for required/scheduled hours. | Adherence, NTPJ, Normalized Occupancy, Shrinkage, Social Media, Content |

### Agent Activity & Productivity

| Table | Description | Used By |
| --- | --- | --- |
| `etl.mx__dataset.agent_productivity` | **Real-time agent status/activity log.** Records agent state transitions (available, oos, pause, training) with timestamps, active jobs count, channel info, and next event time. Used to determine actual time worked for adherence. | Adherence, Social Media, Content |
| `etl.mx__dataset.ops_actors` | **Agent ID ↔ email mapping.** Links `actor__id` to `email_address`. Used to join productivity data (keyed on actor_id) with other tables (keyed on agent name). | Adherence, NTPJ, Social Media, Content |
| `etl.mx__contract.staffing_hero__actor_status_status_history` | **Historical status changes.** Records when agents changed status (with timestamps). Used to infer agent status when the productivity table has null status values. | Adherence, Social Media, Content |
| `etl.mx__contract.staffing_hero__actor_statuses` | **Actor status records.** Links status history entries to actor IDs. | Adherence, Social Media, Content |
| `etl.mx__contract.staffing_hero__status_options` | **Status option definitions.** Maps status_option_id to human-readable names (available, oos, training, pause). | Adherence, Social Media, Content |

### Job & Task Data

| Table | Description | Used By |
| --- | --- | --- |
| `etl.mx__dataset.ops_canonical_time_spent_activities` | **Shuffle (chat/email/backoffice) job records.** Contains job type (received_source_q), activity type, status, net_time_spent, start/stop times, and actor affiliation. Source of truth for shuffle job durations used in NTPJ and occupancy calculations. | NTPJ, Normalized Occupancy |
| `etl.mx__dataset.taskmaster_consolidated_registry` | **OOS (Out of Shuffle) job records.** Contains job classification, net_time_spent_seconds, start/stop dates, agent, squad, and comments. Used for OOS job durations in NTPJ and occupancy. | NTPJ, Normalized Occupancy, Content |

### Quality Assurance

| Table | Description | Used By |
| --- | --- | --- |
| `etl.mx__dataset.qmo_playvox_consolidated` | **QA evaluation scores (Playvox).** Contains evaluation ID, agent email, score average, scorecard ID, team name, and timestamps. Primary QA source for Core, Fraud, and Social Media agents. | Quality Dataset, Social Media |
| `mx__series_contract.social_media_case_summary_information` | **Social media case summaries.** Contains case-level QA data including report_date, agent_name, auditor, and score_avg for Sprinklr-based SM evaluations. | Quality Dataset |
| `usr.mx__enablement.sprinklr_sm_users` | **Sprinklr user mapping.** Maps Sprinklr user names to emails. Used to join SM case data to agent identities. | Quality Dataset |

### Social Media Specific

| Table | Description | Used By |
| --- | --- | --- |
| `usr.sprinklr_api_data_integration.sprinklr_normalized_occupancy_data` | **SM case assignment data for occupancy.** Contains case_assignment_time, case_unassignment_time, and agent_email_id. Used instead of shuffle jobs for SM occupancy calculation. | Social Media |
| `usr.sprinklr_api_data_integration.sprinklr_tnps_data` | **SM tNPS survey data.** Contains survey scores, response dates, case closure times, and agent emails. Source of truth for Social Media transactional NPS. | Social Media |
| `gsheets.sheets.mx_wows_daniel_temp` | **SM WOWs tracking (Google Sheets, temporary).** Records WOW moments with case_id, agent, and date. Temporary source pending migration. | Social Media |

## Metric Calculation Patterns
- All metrics are calculated at **daily**, **weekly**, and **monthly** granularities
- Weekly/monthly aggregations use `FIRST_VALUE(...ORDER BY date DESC)` for hierarchy fields (xforce, xplead, squad)
- Quartile rankings computed via `NTILE(4)` at both general and team (squad) level
- XForce/XPlead metrics = % of agents meeting threshold
- Squads `social` and `content` are excluded from the Core & Fraud analysis

## Naming Conventions
- Agent names extracted from email via: `REGEXP_EXTRACT(actor_email, '^[a-zA-Z]+\\.[a-zA-Z]+', 0)`
- Temp views follow pattern: `{metric}_{level}_{granularity}` (e.g., `adherence_agents_monthly`)
- Final combined views: `adherence`, `ntpj`, `nocc`, etc.

## Coding Style
- SQL is the primary language
- Use `CREATE OR REPLACE TEMPORARY VIEW` for intermediate calculations
- Use `GROUP BY ALL` for brevity
- Use `TRY_DIVIDE()` to avoid division-by-zero errors
- Commented `SELECT *` at end of cells for quick debugging
- Metric values stored as percentages (multiplied by 100)

## How to Use This File
This file auto-loads when you work with files under `legacy/`. It documents the
old monolithic Databricks SQL pipeline — its architecture, team hierarchy, source
datasets, and SQL conventions. Apply this context whenever reading or porting the
legacy notebooks (the source of truth for each metric definition until migrated).
