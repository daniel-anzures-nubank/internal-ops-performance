# Team → Squad Mapping (official)

This is the **official mapping** of performance teams to roster squads.

This mapping is **encoded in the `agent_information` extractor**
(`extractors/agent_information.sql`): it emits a lowercase `team` column derived
from `squad`. The `quality` and `planning` support squads are **kept** (to match
legacy `adherence_io`) but carry `team = NULL` — they belong to no performance
team, so team roll-ups skip them. Because every `metrics_data/` module joins to
that roster, the `team` column flows into all raw tables. Keep this doc and
the extractor's `CASE` in sync.

## Mapping

| Team | Squad(s) | Roster source |
|------|----------|---------------|
| **Core** | `collections`, `credit`, `engagement`, `lifecycle`, `savings` | BDX snapshots |
| **Fraud** | `idsec`, `txn` | BDX snapshots |
| **Social Media** | `social` | BDX snapshots |
| **Content** | `enablement` (district `content`) | Google Sheet (`mx_content_bdx_daniel_anz_temp` for now; canonical `gsheets.sheets.mx_content_bdx`) |

### Content is special

Content's roster does **not** come from the BDX snapshots — it comes from a
Google Sheet. The extractor currently reads `gsheets.sheets.mx_content_bdx_daniel_anz_temp`
(the twin the legacy notebook reads); the canonical target is
`gsheets.sheets.mx_content_bdx` (identical schema), to be swapped in once access is
granted. A few consequences:

- In the sheet, content agents carry `squad = 'enablement'` and
  `district = 'content'` (plus a `target_squad` they support). So in the raw
  tables their **`squad` dimension is `enablement`, not `content`**.
- `team` is **forced to `'content'`** for these rows (it is not derived from the
  squad→team `CASE`, since `enablement` is not in it).
- Content agents also appear in the BDX snapshots (as `squad = 'content'`), but
  the extractor **drops the BDX copies** (`squad NOT IN (... 'content')`) so the
  sheet is the single source of truth.
- The sheet is a static current-state roster (no snapshot history), so the
  extractor expands it across every `snapshot_month` in the BDX universe.
- **Content jobs are all OOS**: they live entirely in
  `taskmaster_consolidated_registry` (`oos_jobs`); content agents have effectively
  no shuffle jobs. This is why content occupancy/NTPJ work through the existing
  `oos_jobs` source with no dedicated extractor. See
  `docs/metrics_data_docs/occupancy_time.md`.

## Not part of any performance team

These squads appear in the roster / raw tables but are **not** one of the four
performance teams and should be **excluded** from team rollups:

- `planning`
- `quality`

## Notes

- Coverage in the `metrics_data/` raw tables is at **squad grain**. There is no
  team filter applied anywhere in those modules, so every squad above flows
  through automatically; the team grouping is applied downstream using this table.
- `txn` belongs to **Fraud**, not Core.
- The legacy notebooks group Core and Fraud together ("Core & Fraud") and exclude
  `social` and `content`; this mapping makes the Core/Fraud split explicit.
- If a new squad shows up in the data that isn't listed here, treat it as
  unmapped and confirm its team before including it in any rollup.
