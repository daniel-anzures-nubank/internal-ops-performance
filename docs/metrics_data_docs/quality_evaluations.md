# quality_evaluations

Raw QA evaluations (**Playvox + Sprinklr SM**). **One row per individual
evaluation.**

Feeds the future Quality metric: the metrics layer re-averages `qa_score` at
whatever grain it wants (agent/day, squad/month, …).

- Module: `metrics_data/quality_evaluations.py`
- Build script: `scripts/metrics_data_scripts/build_quality_evaluations.py`
- Default target table: `usr.danielanzures.io_quality_evaluations_raw`

## Source tables

| extractor | underlying source | role |
|-----------|-------------------|------|
| `agent_information` | `etl.mx__series_contract.cx_mx_bdx_snapshots` + `ops_actors` | roster dimensions, active filter |
| `playvox_evaluations` | `etl.mx__dataset.qmo_playvox_consolidated` (Playvox QA) | one row per evaluation (`source='playvox'`) |
| `sprinklr_sm_evaluations` | `etl.mx__series_contract.social_media_case_summary_information` (Sprinklr SM case QA) | one row per evaluation, `>= 2026-05-01` (`source='sprinklr_sm'`) |

Both feeds report `qa_score` on the same **0-100** scale (verified on live
data), so they `UNION ALL` directly with no rescaling. The `source` column
('playvox' / 'sprinklr_sm') tags provenance so the two can be audited/diffed.

## Team coverage

This table covers **all teams** (Core, Fraud, Social Media, Content) — the
roster has no squad filter and the module doesn't exclude any squad. Core, Fraud
and Content are scored from Playvox; **Social Media is scored from both Playvox
and (from 2026-05-01) Sprinklr SM** — see the next section.

> The downstream **Quality metric** (`metrics/quality.py`) drops Content (its
> quality of record is the separate `content_csat` metric). Content rows are
> still present in *this raw table*.

## Sprinklr SM union (Social Media, `>= 2026-05-01`)

Social Media QA is logged against **Sprinklr cases**, not Playvox. From the
`SPRINKLR_SM_CUTOVER` (`2026-05-01`) onward we `UNION ALL` the Sprinklr SM
case-QA feed on top of Playvox. The cutover is **hard-floored in the extractor**
(`report_date >= DATE '2026-05-01'`) and re-applied defensively in the module,
so earlier SM quality stays Playvox-only and nothing changes retroactively.

This differs from legacy: legacy carried the Sprinklr `UNION ALL` only in the
Core/Fraud Quality dataset, where it was **dead code** — `agent_information` was
built with `squad NOT IN ('social', 'content')`, so every (social) Sprinklr row
was dropped before output. Here the new roster keeps social agents, so the
Sprinklr SM rows actually reach output and are scored for Social Media.

Sprinklr-specific source filtering (in `extractors/sprinklr_sm_evaluations.sql`):

- `agent_name` → Nubank email → agent prefix via `usr.mx__enablement.sprinklr_sm_users`.
  Rows whose agent can't be mapped yield an empty `agent` and are dropped in shaping.
- Monitor exclusion `sm_monitor.user_email NOT IN (CONCAT('testuser', '@', 'nu.com.mx'))`. We use
  `COALESCE(mon.user_email,'')` so an **unmapped auditor no longer silently drops
  the row** (legacy's `LEFT JOIN ... NOT IN` did; for May 2026 there are 0 such
  rows, so this is equivalent today and safer going forward).
- Dedup by `evaluation_id` (the Sprinklr `case_number`) keeping the latest
  `checklist_modified_date` (analogous to Playvox's `ROW_NUMBER()` dedup).

> Note: social agents that have **both** Playvox and Sprinklr SM evaluations in a
> period contribute both to the Quality mean (this is a `UNION ALL` = additive,
> not a Sprinklr-replaces-Playvox switch).

## Filters applied here (minimal — raw table)

- **Playvox**:
  - `team_name NOT IN ('REGULATORY SOLUTIONS', 'AML')`, AND
  - Nubank-MX agent-email regex `^[a-z]+\.[a-z]+[0-9]*@nu\.com\.mx$`
    (case-insensitive).
  - These mirror legacy's source-level `qa_base` gate. Sprinklr SM rows are
    **not** run through this Playvox-specific gate (their filtering lives in the
    extractor).
- **Sprinklr SM**: defensive `date >= SPRINKLR_SM_CUTOVER (2026-05-01)` floor.
- **Shaping**: rows with empty-string `agent` (unmatched email extraction) are
  dropped. `date` is the calendar day the evaluation was logged (MX local).
- **Roster**: `status = 'active'` and non-null `squad` (inner join on
  `(agent, snapshot_month)`).

## Deferred to the metrics layer (NOT applied here)

- The `scorecard_id` / `evaluation_id` blacklists.
- The hardcoded outage-date exclusions (2026-03-27, 2026-04-09).
- Any narrower squad scoping.

## Output schema (one row per evaluation)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | |
| `xplead` | STRING | |
| `team` | STRING | performance team derived from squad (`core` / `fraud` / `social media` / `content`); see `docs/team_squad_mapping.md` |
| `squad` | STRING | roster squad |
| `district` | STRING | roster district (was `squad_district`) |
| `shift` | STRING | roster shift |
| `date` | DATE | calendar day the evaluation was logged (MX local) |
| `evaluation_id` | STRING | Playvox evaluation id, or Sprinklr `case_number` |
| `team_name` | STRING | source team / scorecard team (`'SM'` for Sprinklr) |
| `source` | STRING | `'playvox'` or `'sprinklr_sm'` |
| `qa_score` | DOUBLE | the evaluation's score (0-100) |
