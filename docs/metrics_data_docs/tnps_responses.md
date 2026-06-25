# tnps_responses

Raw Social-Media transactional-NPS (tNPS) survey responses. **One row per survey
response.**

Feeds the future **Human tNPS** metric: the metrics layer classifies each
response (promoter `>= 9` / detractor `<= 6` / neutral 7-8) and computes
`(promoters - detractors) / valid_responses` (target ≥ 88%).

- Module: `metrics_data/tnps_responses.py`
- Build script: `scripts/metrics_data_scripts/build_tnps_responses.py`
- Default target table: `usr.danielanzures.io_tnps_responses_raw`

## Team coverage (Social Media only)

tNPS **only applies to Social Media.** The source
(`sprinklr_tnps_data`) only contains surveys for cases handled by a human social
agent — there are no bot/unattributed rows (verified live: May 2026 had 0 rows
without a human agent). The roster join therefore naturally yields only
`squad = 'social'` / `team = 'social media'` rows. Verified live (May 1–15 2026):
211 responses across 26 social agents, all `social`.

This is the **"Human tNPS"** feed (NPS attributable to a human agent), as opposed
to any future bot/automated NPS source.

## Source tables

| extractor | underlying table | role |
|-----------|------------------|------|
| `agent_information` | `etl.mx__series_contract.cx_mx_bdx_snapshots` + `ops_actors` | roster dimensions, active filter |
| `tnps` | `usr.sprinklr_api_data_integration.sprinklr_tnps_data` | one row per tNPS survey response |

## Filters applied here (minimal — raw table)

- **Human filter**: drop rows whose `agent` does not resolve from `agent_email_id`
  (empty string) — i.e. unattributable / non-human surveys.
- **Roster**: `status = 'active'` and non-null `squad` (inner join on
  `(agent, snapshot_month)`, where `snapshot_month` comes from the response's
  closure-day month).

## Deferred to the metrics layer (NOT applied here)

- Promoter / detractor / neutral classification and the
  `(promoters - detractors) / valid_responses` NPS ratio.
- The validity window `survey_response_date <= date + 1 day`.
- The outage-date exclusion (`2026-03-27`).
- Dedup to one response per `case_number` (legacy counts DISTINCT case_number;
  in practice the source is already ~one row per case).

## Output schema (one row per survey response)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | |
| `xplead` | STRING | |
| `team` | STRING | performance team derived from squad (`social media` for these rows); see `docs/team_squad_mapping.md` |
| `squad` | STRING | roster squad (`social`) |
| `district` | STRING | roster district (was `squad_district`) |
| `shift` | STRING | roster shift |
| `date` | DATE | case closure day (MX local) the response is attributed to |
| `case_number` | STRING | the case / survey identifier |
| `survey_response_date` | DATE | when the customer answered the survey |
| `survey_score` | INT | raw 0-10 NPS score (nullable) |
