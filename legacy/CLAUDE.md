# CLAUDE.md - Internal Ops Context (legacy)

## Project Overview
This workspace contains notebooks for the **Internal Operations (IO) Performance** analysis at Nubank Mexico CX.

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

## Team Hierarchy
- **Xpeers (agents)**: Individual customer service representatives
- **XForces**: Team leads managing a group of agents
- **XPLeads**: Senior leads managing XForces
- **Squads**: Organizational units grouped by district

## Key Metrics
| Metric | Description | Target | Level |
| --- | --- | --- | --- |
| Adherence | delivered_hours / required_hours | >= 95% | Agent |
| NTPJ (Normalized Time Per Job) | actual_duration / expected_duration | <= 100% | Agent |
| Normalized Occupancy (NOCC) | occupancy_time / job_time / expected | Varies | Agent |
| Quality | TBD | TBD | Agent |
| Shrinkage | TBD | TBD | Agent |

## Key Tables
- `etl.mx__series_contract.cx_mx_bdx_snapshots` — Agent roster snapshots (squad, district, shift, hire date)
- `usr.mx__cx.adherence_io` — Adherence hours data
- `usr.mx__cx.normalized_time_per_job` — NTPJ metric base
- `usr.mx__cx.normalized_occupancy` — Occupancy metric base

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
