# content_csat

Raw **Content CSAT** survey responses. **One row per survey response × content
agent** (the legacy fan-out — see grain note below).

Feeds the future Content **Quality (CSAT)** metric: each survey response rates how
well the Content (enablement) team supported a squad that month; legacy scores
5 of the survey's 8 questions, each 1-5 (a "promoter" is any answer `>= 4`).
The metrics layer
aggregates per agent as `SUM(promoters) / SUM(number_of_questions)` (target ≥ 95%).

- Module: `metrics_data/content_csat.py`
- Build script: `scripts/metrics_data_scripts/build_content_csat.py`
- Default target table: `usr.danielanzures.io_content_csat_raw`

## Team coverage (Content only)

CSAT **only applies to Content.** The survey is filled by representatives of the
squads the Content team supports, and is attributed to the content agents serving
those squads. Non-content teams have no CSAT.

## Grain: attributed by `target_squad`, fanned out to agents

Unlike the other raw tables (which key on `agent`), CSAT is attributed by
**`target_squad`** — the squad a survey response is about. Each response is
credited to **every active content agent whose `target_squad` matches** (content
agents each support one squad; multiple agents can support the same squad). This
reproduces the legacy Content notebook, which joins `qa_base_agg` to the roster on
`target_squad` and then rolls up per agent.

So one survey response becomes N rows (one per serving content agent). Verified
live (Mar–May 2026): 61 responses → 132 response×agent rows across 17 content
agents and 8 target squads.

## Source tables

| extractor | underlying table | role |
|-----------|------------------|------|
| `agent_information` | BDX snapshots + `gsheets.sheets.mx_content_bdx` (content roster) | roster dimensions + each content agent's `target_squad` (the join key) |
| `content_csat` | `gsheets.sheets.mx_content_csat_daniel_anz_temp` | one row per CSAT survey response |

> **Source note.** The extractor currently reads
> `gsheets.sheets.mx_content_csat_daniel_anz_temp` (the twin the legacy notebook
> reads). The canonical sheet is `gsheets.sheets.mx_content_csat` — swap the `FROM`
> in `extractors/content_csat.sql` once access is granted.

## Filters applied here (minimal — raw table)

- Extractor: keep rows with a non-empty `timestamp`; derive
  `date_reference` from the sheet's `mes` label (`'Abril 2026'` → 2026-04-01, the
  month rated — matching the OG legacy notebook; a malformed `mes` falls back to
  the old `survey_timestamp - 1 month` proxy, which broke for responses filled
  more than a month late) and filter it to the period.
- Module: `target_squad` normalization (`E.M.I.` / the long `GENERAL (...)` label
  → `emi_general`; else lowercase) so it matches the roster key.
- **Roster**: `status = 'active'` and non-null `target_squad` (inner join on
  `(target_squad, snapshot_month)`).

## How the score is computed (per response)

- The survey sheet carries 8 question columns, but legacy (`qa_base`, promoters
  sum at ~L3859 of the Content Temp Fix) scores exactly **5**: `comprension`,
  `comunicacion`, `calidad`, `tiempo`, `expectativas`. `facilidad` (question 1),
  `manejo_de_cambios` and `aportacion_estrategica` are **not** scored. (An
  earlier "first 5" reading — `facilidad` in, `expectativas` out — coincidentally
  matched legacy on the data of the day and caused the documented ±1-2pp
  residual.)
- Each scored question is a promoter if its score `>= 4` (null → not a promoter).
- `promoters` = count of promoter questions (0-5); `number_of_questions` = 5
  (constant, matching legacy; 4 for the May-2026 `tiempo` exclusion agents);
  `csat_score = promoters / number_of_questions`.

## Deferred to the metrics layer (NOT applied here)

- Per-agent / per-period aggregation (`SUM(promoters) / SUM(number_of_questions)`).
  Note: the agent metric sums numerators/denominators across responses — it is
  **not** the average of per-response `csat_score`.
- Any per-agent manual adjustments / outage-date carve-outs.

## Not included

- The separate `nps` column (0-10) in the source sheet is a different Content NPS
  signal, not part of CSAT; it is not surfaced in this table.

## Output schema (one row per survey response × content agent)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | content agent serving the `target_squad` |
| `xforce` | STRING | |
| `xplead` | STRING | |
| `team` | STRING | always `content` |
| `squad` | STRING | roster squad (content: `enablement`) |
| `district` | STRING | roster district (content: `content`) |
| `shift` | STRING | roster shift (NULL for content) |
| `date` | DATE | month rated (`DATE(date_reference)`) |
| `target_squad` | STRING | the supported squad the survey is about (join key) |
| `requested_by` | STRING | respondent email prefix (squad rep who filled the survey) |
| `survey_timestamp` | TIMESTAMP | when the survey was filled |
| `promoters` | INT | # of the 5 scored questions answered `>= 4` |
| `number_of_questions` | INT | always 5 (4 for the May-2026 `tiempo`-exclusion agents) |
| `csat_score` | DOUBLE | `promoters / number_of_questions` (0-1, per response) |
