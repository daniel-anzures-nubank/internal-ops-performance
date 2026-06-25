# wows

Raw Social-Media **WoW experiences**. **One row per WoW experience** (per
agent / case credited in the WoWs Google Sheet).

Feeds the future **WoWs** metric: the metrics layer counts
`COUNT(DISTINCT case_id)` per agent / period against a monthly target (≥ 5).

- Module: `metrics_data/wows.py`
- Build script: `scripts/metrics_data_scripts/build_wows.py`
- Default target table: `usr.danielanzures.io_wows_raw`

## Team coverage (Social Media only)

WoWs **only apply to Social Media.** The WoWs sheet is maintained for social
Xpeers, so the roster join naturally yields only `squad = 'social'` /
`team = 'social media'` rows. There is no explicit team filter — coverage is
source-driven.

## Source tables

| extractor | underlying table | role |
|-----------|------------------|------|
| `agent_information` | `etl.mx__series_contract.cx_mx_bdx_snapshots` + `ops_actors` | roster dimensions, active filter |
| `wows` | `gsheets.sheets.mx_wows_daniel_temp` | one row per WoW experience |

> **Source note.** The legacy `Social Media` notebook reads
> `gsheets.sheets.mx_wows_daniel_temp`, and so does this extractor. The notebook
> flags `gsheets.sheets.mx_wows_social_media` as the intended canonical source
> ("Change temporary fix"), and `docs/metrics_definitions.md` lists it too — but
> that sheet is **not currently readable** (`PERMISSION_DENIED`). Swap the `FROM`
> clause in `extractors/wows.sql` once access is granted.
>
> The sheet stores full agent emails on the `@nubank.com.br` domain; the agent
> key is the lowercased `firstname.lastname` prefix (same regex as everywhere
> else), so the roster join is unaffected.

## Filters applied here (minimal — raw table)

- Drop rows with empty `date` (extractor) and rows whose `agent` does not
  resolve to a name (empty string).
- **Roster**: `status = 'active'` and non-null `squad` (inner join on
  `(agent, snapshot_month)`, where `snapshot_month` comes from the WoW's date
  month).

## Deferred to the metrics layer (NOT applied here)

- `COUNT(DISTINCT case_id)` aggregation per agent / period and the monthly
  target (≥ 5).
- The outage-date exclusion (`2026-03-27`, "general access problems").
- Any dedup of repeated `(agent, date, case_id)` rows (kept raw here; the
  metrics-layer DISTINCT count absorbs duplicates).

## Output schema (one row per WoW experience)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | |
| `xforce` | STRING | |
| `xplead` | STRING | |
| `team` | STRING | performance team derived from squad (`social media` for these rows); see `docs/team_squad_mapping.md` |
| `squad` | STRING | roster squad (`social`) |
| `district` | STRING | roster district (was `squad_district`) |
| `shift` | STRING | roster shift |
| `date` | DATE | day the WoW was logged (MX local) |
| `case_id` | STRING | the WoW's case identifier |
