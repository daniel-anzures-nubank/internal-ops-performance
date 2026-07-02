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

## Sprinklr SM union (Social Media, `>= 2026-05-01`) — a union, like legacy

Social Media QA started being logged against **Sprinklr cases** in May 2026.
From the Sprinklr feed's floor (`SPRINKLR_SM_CUTOVER`, `2026-05-01`) onward we
`UNION ALL` the Sprinklr SM case-QA feed on top of Playvox. The floor is
**hard-coded in the extractor** (`report_date >= DATE '2026-05-01'`) and
re-applied defensively in the module — matching legacy's Sprinklr branch
(`sm.report_date >= "2026-05-01"`, `[IO] Performance 2026 - Social Media Temp
Fix.sql` `qa_base` line 3025).

**The Playvox branch has no May cutoff** — exactly like legacy's, which is
unbounded above (SM Playvox evaluations keep flowing until they naturally end
after 2026-05-15). So in early May an SM agent can contribute both a Playvox
and a Sprinklr evaluation to the same period; that is legacy behavior, not
double-counting (the metric layer dedups per `(source, evaluation_id)`, and
the Playvox / Sprinklr id spaces are disjoint). An earlier revision of this
module implemented a "clean switch" that dropped Playvox SM rows on/after
2026-05-01; that was reverted for legacy parity.

One legacy nuance: the Core/Fraud Quality dataset also carries the Sprinklr
`UNION ALL`, but there it is **dead code** — its `agent_information` is built
with `squad NOT IN ('social', 'content')`, so every (social) Sprinklr row is
dropped before output. The SM deck's union is the live one, and it is what
this table reproduces.

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
- Any narrower squad scoping.

**No dates are dropped for quality — anywhere.** The 2026-06-30 legacy
re-export re-included the 2026-03-27 / 2026-04-09 outage rows (the published
`usr.mx__cx.quality_io` and `usr.danielanzures.sm_temp_quality` both carry
those dates), so neither this raw layer nor `metrics/quality.py` applies any
quality date drop.

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
| `created_at` | TIMESTAMP | raw evaluation timestamp (legacy dedup order key) |
| `evaluation_id` | STRING | Playvox evaluation id, or Sprinklr `case_number` |
| `team_name` | STRING | source team / scorecard team (`'SM'` for Sprinklr) |
| `scorecard_id` | STRING | source scorecard id (for the metric-layer blacklist) |
| `source` | STRING | `'playvox'` or `'sprinklr_sm'` |
| `qa_score` | DOUBLE | the evaluation's score (0-100) |
